package api

import (
	"context"
	"time"

	"go_distributed_system/internal/store"
	"go_distributed_system/internal/types"
)

// JobStore abstracts persistence for HTTP handlers (enables unit tests without PostgreSQL).
type JobStore interface {
	CreateJob(ctx context.Context, job *types.Job) error
	QueueJob(ctx context.Context, jobID string) error
	GetJob(ctx context.Context, id string) (*types.Job, error)
	GetJobLogs(ctx context.Context, jobID string) ([]types.JobLog, error)
	RegisterWorker(ctx context.Context, w *types.Worker) error
	Heartbeat(ctx context.Context, workerID string, ts time.Time) error
	AcquireJob(ctx context.Context, workerID string, lease time.Duration) (*types.Job, error)
	FinishJob(ctx context.Context, jobID, workerID string, success bool, logs []types.JobLogEntry) error
}

var _ JobStore = (*store.Store)(nil)
