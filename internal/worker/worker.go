package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go_distributed_system/internal/ffmpeg"
	"go_distributed_system/internal/types"
)

// Config holds worker runtime settings.
type Config struct {
	ID              string
	Hostname        string
	CPUCores        int
	GPUAvailable    bool
	MaxParallelJobs int
	SchedulerURL    string
	HeartbeatEvery  time.Duration
	PollInterval    time.Duration
}

// Worker pulls jobs from the scheduler and runs ffmpeg.
type Worker struct {
	cfg    Config
	client *Client
}

// New creates a worker instance.
func New(cfg Config) *Worker {
	return &Worker{
		cfg:    cfg,
		client: NewClient(cfg.SchedulerURL),
	}
}

// Run registers the worker, sends heartbeats, and processes jobs until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	worker := &types.Worker{
		ID:              w.cfg.ID,
		Hostname:        w.cfg.Hostname,
		CPUCores:        w.cfg.CPUCores,
		GPUAvailable:    w.cfg.GPUAvailable,
		MaxParallelJobs: w.cfg.MaxParallelJobs,
		Status:          types.WorkerStatusActive,
	}
	if err := w.client.Register(ctx, worker); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	log.Printf("worker %s registered at %s", w.cfg.ID, w.cfg.SchedulerURL)

	hbCtx, cancelHB := context.WithCancel(ctx)
	defer cancelHB()
	go w.heartbeatLoop(hbCtx)

	slots := make(chan struct{}, w.cfg.MaxParallelJobs)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case slots <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				w.processOne(ctx)
			}()
		}
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.client.Heartbeat(ctx, w.cfg.ID); err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
		}
	}
}

func (w *Worker) processOne(ctx context.Context) {
	job, err := w.client.RequestJob(ctx, w.cfg.ID)
	if err != nil {
		log.Printf("request job failed: %v", err)
		w.sleep(ctx, w.cfg.PollInterval)
		return
	}
	if job == nil {
		w.sleep(ctx, w.cfg.PollInterval)
		return
	}

	log.Printf("running job %s (attempt %d)", job.ID, job.Attempt)
	success, logs := w.runJob(ctx, *job)

	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.client.SubmitJobResult(submitCtx, w.cfg.ID, job.ID, success, logs); err != nil {
		log.Printf("submit result for job %s failed: %v", job.ID, err)
		return
	}

	status := "failed"
	if success {
		status = "completed"
	}
	log.Printf("job %s %s", job.ID, status)
}

func (w *Worker) runJob(ctx context.Context, job types.Job) (bool, []types.JobLogEntry) {
	if err := ffmpeg.EnsureInputExists(job.InputPath); err != nil {
		log.Printf("job %s input check: %v", job.ID, err)
		return false, []types.JobLogEntry{{Stream: "stderr", Line: err.Error()}}
	}
	if err := ffmpeg.EnsureOutputDir(job.OutputPath); err != nil {
		log.Printf("job %s output dir: %v", job.ID, err)
		return false, []types.JobLogEntry{{Stream: "stderr", Line: err.Error()}}
	}

	result := ffmpeg.Run(ctx, job)
	if result.Err != nil {
		log.Printf("job %s ffmpeg error: %v", job.ID, result.Err)
		logs := result.Logs
		if logs == nil {
			logs = []types.JobLogEntry{}
		}
		logs = append(logs, types.JobLogEntry{Stream: "stderr", Line: result.Err.Error()})
		return false, logs
	}
	return true, result.Logs
}

func (w *Worker) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
