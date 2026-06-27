package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go_distributed_system/internal/queue"
	"go_distributed_system/internal/store"
)

// Consumer runs the AMQP consume → claim → execute loop.
type Consumer struct {
	queue  queue.Consumer
	client *Client
	worker *Worker
}

// NewConsumer wires a Rabbit consumer to a worker instance.
func NewConsumer(q queue.Consumer, w *Worker) *Consumer {
	return &Consumer{
		queue:  q,
		client: w.client,
		worker: w,
	}
}

// Run consumes dispatch messages until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	handler := func(ctx context.Context, msg queue.Message) error {
		return c.handleMessage(ctx, msg)
	}
	if pc, ok := c.queue.(queue.ParallelConsumer); ok {
		return pc.ConsumeParallel(ctx, c.worker.cfg.MaxParallelJobs, handler)
	}
	return c.queue.Consume(ctx, handler)
}

func (c *Consumer) handleMessage(ctx context.Context, msg queue.Message) error {
	job, err := c.client.ClaimJob(ctx, c.worker.cfg.ID, msg.JobID)
	if err != nil {
		if isClaimConflict(err) {
			return nil
		}
		return err
	}

	success, logs := c.worker.executeJob(ctx, *job)

	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.client.SubmitJobResult(submitCtx, c.worker.cfg.ID, job.ID, job.LeaseGeneration, success, logs); err != nil {
		return fmt.Errorf("submit result: %w", err)
	}
	return nil
}

func isClaimConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, store.ErrJobNotClaimable) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "409") || strings.Contains(msg, "not dispatchable")
}
