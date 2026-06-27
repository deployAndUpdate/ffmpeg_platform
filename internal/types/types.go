package types

import (
	"time"
)

// StorageMode indicates where job input/output files are stored.
type StorageMode string

const (
	StorageLocal StorageMode = "local"
	StorageR2    StorageMode = "r2"
)

// JobStatus represents the lifecycle of a compression job.
type JobStatus string

const (
	JobStatusNew        JobStatus = "NEW"
	JobStatusDispatched JobStatus = "DISPATCHED"
	JobStatusRunning    JobStatus = "RUNNING"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
)

// WorkerStatus denotes worker availability.
type WorkerStatus string

const (
	WorkerStatusActive WorkerStatus = "ACTIVE"
	WorkerStatusDead   WorkerStatus = "DEAD"
)

// Job defines the domain model of a compression task.
type Job struct {
	ID               string      `json:"id"`
	InputPath        string      `json:"input_path"`
	OutputPath       string      `json:"output_path"`
	Preset           string      `json:"preset,omitempty"`
	FFmpegArgs       string      `json:"ffmpeg_args"`
	Storage          StorageMode `json:"storage"`
	Status           JobStatus   `json:"status"`
	AssignedWorkerID *string    `json:"assigned_worker_id,omitempty"`
	Attempt            int        `json:"attempt"`
	MaxAttempts        int        `json:"max_attempts"`
	LeaseExpiresAt     *time.Time `json:"lease_expires_at,omitempty"`
	LeaseGeneration    int64      `json:"lease_generation"`
	IdempotencyKey     string     `json:"idempotency_key,omitempty"`
	MaxDurationSeconds int        `json:"max_duration_seconds,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// Worker describes a worker node that can execute jobs.
type Worker struct {
	ID              string       `json:"id"`
	Hostname        string       `json:"hostname"`
	CPUCores        int          `json:"cpu_cores"`
	GPUAvailable    bool         `json:"gpu_available"`
	MaxParallelJobs int          `json:"max_parallel_jobs"`
	LastHeartbeatAt *time.Time   `json:"last_heartbeat_at,omitempty"`
	Status          WorkerStatus `json:"status"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
}

// JobLogEntry is a single stdout/stderr line from a job run.
type JobLogEntry struct {
	Stream string `json:"stream"`
	Line   string `json:"line"`
}

// JobLog is a persisted log line returned by GET /jobs/{id}/logs.
type JobLog struct {
	ID     int64     `json:"id"`
	TS     time.Time `json:"ts"`
	Stream string    `json:"stream"`
	Line   string    `json:"line"`
}

// JobLogArtifact describes an R2 object holding logs for one job attempt.
type JobLogArtifact struct {
	ID        int64     `json:"id"`
	JobID     string    `json:"job_id"`
	Attempt   int       `json:"attempt"`
	ObjectKey string    `json:"object_key"`
	Bytes     int64     `json:"bytes"`
	Lines     int       `json:"lines"`
	CreatedAt time.Time `json:"created_at"`
}

// JobLogArtifactInput is metadata recorded after logs are uploaded to object storage.
type JobLogArtifactInput struct {
	Attempt   int
	ObjectKey string
	Bytes     int64
	Lines     int
}

// WorkerStats extends Worker with live workload counters for the admin dashboard.
type WorkerStats struct {
	Worker
	RunningJobs int `json:"running_jobs"`
}
