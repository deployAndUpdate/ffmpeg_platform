package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go_distributed_system/internal/ffmpeg"
	"go_distributed_system/internal/storage"
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
	TempDir         string
	HeartbeatEvery  time.Duration
	PollInterval    time.Duration
}

// Worker pulls jobs from the scheduler and runs ffmpeg.
type Worker struct {
	cfg     Config
	client  *Client
	storage storage.ObjectStorage
}

// New creates a worker instance.
func New(cfg Config, obj storage.ObjectStorage) *Worker {
	if cfg.TempDir == "" {
		cfg.TempDir = "/tmp/jobs"
	}
	return &Worker{
		cfg:     cfg,
		client:  NewClient(cfg.SchedulerURL),
		storage: obj,
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
	if job.Storage == types.StorageR2 {
		return w.runJobR2(ctx, job)
	}
	return w.runJobLocal(ctx, job)
}

func (w *Worker) runJobLocal(ctx context.Context, job types.Job) (bool, []types.JobLogEntry) {
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

func (w *Worker) runJobR2(ctx context.Context, job types.Job) (bool, []types.JobLogEntry) {
	if w.storage == nil {
		err := fmt.Errorf("R2 storage is not configured on worker")
		log.Printf("job %s: %v", job.ID, err)
		return false, []types.JobLogEntry{{Stream: "stderr", Line: err.Error()}}
	}

	workDir, err := os.MkdirTemp(w.cfg.TempDir, job.ID+"-*")
	if err != nil {
		log.Printf("job %s temp dir: %v", job.ID, err)
		return false, []types.JobLogEntry{{Stream: "stderr", Line: err.Error()}}
	}
	defer os.RemoveAll(workDir)

	inputExt := filepath.Ext(job.InputPath)
	if inputExt == "" {
		inputExt = ".bin"
	}
	outputExt := filepath.Ext(job.OutputPath)
	if outputExt == "" {
		outputExt = ".bin"
	}

	localInput := filepath.Join(workDir, "input"+inputExt)
	localOutput := filepath.Join(workDir, "output"+outputExt)

	if err := w.storage.Download(ctx, job.InputPath, localInput); err != nil {
		log.Printf("job %s download input: %v", job.ID, err)
		return false, []types.JobLogEntry{{Stream: "stderr", Line: err.Error()}}
	}

	localJob := job
	localJob.InputPath = localInput
	localJob.OutputPath = localOutput

	result := ffmpeg.Run(ctx, localJob)
	if result.Err != nil {
		log.Printf("job %s ffmpeg error: %v", job.ID, result.Err)
		logs := result.Logs
		if logs == nil {
			logs = []types.JobLogEntry{}
		}
		logs = append(logs, types.JobLogEntry{Stream: "stderr", Line: result.Err.Error()})
		return false, logs
	}

	if err := w.storage.Upload(ctx, localOutput, job.OutputPath); err != nil {
		log.Printf("job %s upload output: %v", job.ID, err)
		logs := result.Logs
		if logs == nil {
			logs = []types.JobLogEntry{}
		}
		logs = append(logs, types.JobLogEntry{Stream: "stderr", Line: err.Error()})
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
