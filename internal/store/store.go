package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go_distributed_system/internal/types"

	_ "github.com/lib/pq"
)

// Store wraps DB accessors.
type Store struct {
	db *sql.DB
}

// New opens a PostgreSQL connection with sane defaults.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	return &Store{db: db}, nil
}

// Close closes the underlying DB.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return errors.New("store is nil")
	}
	return s.db.Close()
}

// CreateJob inserts a new job with initial status/attempts.
func (s *Store) CreateJob(ctx context.Context, job *types.Job) error {
	if job.Storage == "" {
		job.Storage = types.StorageLocal
	}
	const q = `
INSERT INTO jobs (id, input_path, output_path, preset, ffmpeg_args, storage, status, attempt, max_attempts,
                  idempotency_key, max_duration_seconds, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW())`
	_, err := s.db.ExecContext(ctx, q,
		job.ID,
		job.InputPath,
		job.OutputPath,
		nullString(job.Preset),
		job.FFmpegArgs,
		job.Storage,
		job.Status,
		job.Attempt,
		job.MaxAttempts,
		nullString(job.IdempotencyKey),
		job.MaxDurationSeconds,
	)
	return err
}

// QueueJob moves a NEW job to QUEUED after input upload is confirmed.
func (s *Store) QueueJob(ctx context.Context, jobID string) error {
	const q = `
UPDATE jobs
SET status = 'QUEUED', updated_at = NOW()
WHERE id = $1 AND status = 'NEW'`
	res, err := s.db.ExecContext(ctx, q, jobID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// GetJob returns a job by ID.
func (s *Store) GetJob(ctx context.Context, id string) (*types.Job, error) {
	const q = `SELECT ` + jobSelectColumns + ` FROM jobs WHERE id = $1`
	return scanJob(s.db.QueryRowContext(ctx, q, id))
}

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// RegisterWorker inserts or updates worker info, marking it ACTIVE and refreshing heartbeat.
func (s *Store) RegisterWorker(ctx context.Context, w *types.Worker) error {
	const q = `
INSERT INTO workers (id, hostname, cpu_cores, gpu_available, max_parallel_jobs, last_heartbeat_at, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
ON CONFLICT (id) DO UPDATE
SET hostname = EXCLUDED.hostname,
    cpu_cores = EXCLUDED.cpu_cores,
    gpu_available = EXCLUDED.gpu_available,
    max_parallel_jobs = EXCLUDED.max_parallel_jobs,
    last_heartbeat_at = EXCLUDED.last_heartbeat_at,
    status = EXCLUDED.status,
    updated_at = NOW()`
	_, err := s.db.ExecContext(ctx, q,
		w.ID,
		w.Hostname,
		w.CPUCores,
		w.GPUAvailable,
		w.MaxParallelJobs,
		w.LastHeartbeatAt,
		w.Status,
	)
	return err
}

// Heartbeat updates last_heartbeat_at and marks worker ACTIVE. Returns sql.ErrNoRows if worker not found.
func (s *Store) Heartbeat(ctx context.Context, workerID string, ts time.Time) error {
	const q = `
UPDATE workers
SET last_heartbeat_at = $2, status = 'ACTIVE', updated_at = NOW()
WHERE id = $1`
	res, err := s.db.ExecContext(ctx, q, workerID, ts)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}
