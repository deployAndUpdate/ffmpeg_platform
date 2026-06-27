package api

import (
	"context"
	"time"

	"go_distributed_system/internal/store"
	"go_distributed_system/internal/types"
)

// JobStore abstracts persistence for HTTP handlers (enables unit tests without PostgreSQL).
type JobStore interface {
	Ping(ctx context.Context) error
	CreateJob(ctx context.Context, job *types.Job) error
	CreateAndDispatch(ctx context.Context, p *store.JobCreateParams) error
	QueueJob(ctx context.Context, jobID string) error
	GetJob(ctx context.Context, id string) (*types.Job, error)
	GetJobByIdempotencyKey(ctx context.Context, key string) (*types.Job, error)
	GetJobLogs(ctx context.Context, jobID string) ([]types.JobLog, error)
	RegisterWorker(ctx context.Context, w *types.Worker) error
	Heartbeat(ctx context.Context, workerID string, ts time.Time) error
	ClaimJob(ctx context.Context, jobID, workerID string, lease time.Duration) (*types.Job, error)
	RenewLease(ctx context.Context, jobID, workerID string, leaseGeneration int64, lease time.Duration) (*types.Job, error)
	FinishJob(ctx context.Context, jobID, workerID string, leaseGeneration int64, success bool, logs []types.JobLogEntry) error

	ListJobs(ctx context.Context, filter store.ListJobsFilter) (store.ListJobsResult, error)
	ListWorkers(ctx context.Context) ([]types.WorkerStats, error)
	GetWorker(ctx context.Context, id string) (*types.WorkerStats, error)
	GetAdminStats(ctx context.Context) (store.AdminStats, error)
}

var _ JobStore = (*store.Store)(nil)
