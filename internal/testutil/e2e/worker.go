//go:build integration

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"go_distributed_system/internal/worker"
)

// RunningWorker is a worker consuming Rabbit dispatch messages against the scheduler stack.
type RunningWorker struct {
	ID     string
	stop   context.CancelFunc
	errCh  chan error
}

// StartWorker registers a worker and consumes jobs until stopped.
func StartWorker(t *testing.T, stack *Stack) *RunningWorker {
	t.Helper()

	workerID := uuid.New().String()
	runCtx, cancel := context.WithCancel(context.Background())

	w := worker.New(worker.Config{
		ID:                    workerID,
		Hostname:              "e2e-test",
		CPUCores:              1,
		MaxParallelJobs:       1,
		SchedulerURL:          stack.URL,
		HeartbeatEvery:        workerHeartbeat,
		LeaseRenewInterval:    workerLeaseRenew,
		JobLeaseDuration:      workerJobLease,
		DefaultMaxJobDuration: workerMaxDuration,
	}, stack.Storage, stack.Rabbit)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(runCtx)
	}()

	waitForWorkerRegistered(t, stack, workerID)

	rw := &RunningWorker{
		ID:    workerID,
		stop:  cancel,
		errCh: errCh,
	}
	t.Cleanup(func() { rw.Stop(t) })
	return rw
}

// Stop cancels the worker loop and waits for it to exit.
func (rw *RunningWorker) Stop(t *testing.T) {
	t.Helper()
	if rw.stop == nil {
		return
	}
	rw.stop()
	select {
	case err := <-rw.errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("worker run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop")
	}
	rw.stop = nil
}

func waitForWorkerRegistered(t *testing.T, stack *Stack, workerID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := stack.Store.GetWorker(ctx, workerID); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for worker registration")
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("timeout waiting for worker registration")
}
