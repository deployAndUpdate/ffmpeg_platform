package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go_distributed_system/internal/queue"
	"go_distributed_system/internal/types"
)

var (
	ErrJobNotAssigned = errors.New("job is not assigned to this worker or not running")
	ErrLeaseLost      = errors.New("lease lost or generation mismatch")
)

// RenewLease extends lease_expires_at for a running job owned by the worker.
func (s *Store) RenewLease(ctx context.Context, jobID, workerID string, leaseGeneration int64, lease time.Duration) (*types.Job, error) {
	leaseSeconds := int64(lease.Seconds())
	if leaseSeconds <= 0 {
		leaseSeconds = 1800
	}

	const q = `
UPDATE jobs
SET lease_expires_at = NOW() + ($4 * interval '1 second'),
    updated_at = NOW()
WHERE id = $1
  AND assigned_worker_id = $2
  AND status = 'RUNNING'
  AND lease_generation = $3
RETURNING ` + jobSelectColumns

	row := s.db.QueryRowContext(ctx, q, jobID, workerID, leaseGeneration, leaseSeconds)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrLeaseLost
		}
		return nil, err
	}
	return job, nil
}

// FinishJob marks a running job as completed or failed/dispatched-for-retry.
// When artifact is non-nil, metadata for logs already stored in object storage is recorded.
// leaseGeneration fences stale workers after reaper/re-claim.
// On success, a late finish is accepted when the job was moved to DISPATCHED but not yet re-claimed.
func (s *Store) FinishJob(ctx context.Context, jobID, workerID string, leaseGeneration int64, success bool, artifact *types.JobLogArtifactInput) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var status types.JobStatus
	var attempt, maxAttempts int
	var gen int64
	var assigned sql.NullString

	const selectQ = `
SELECT status, attempt, max_attempts, lease_generation, assigned_worker_id
FROM jobs
WHERE id = $1
FOR UPDATE`
	if err = tx.QueryRowContext(ctx, selectQ, jobID).Scan(&status, &attempt, &maxAttempts, &gen, &assigned); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotAssigned
		}
		return err
	}

	if status == types.JobStatusCompleted {
		if err = insertLogArtifactTx(ctx, tx, jobID, attempt, artifact); err != nil {
			return err
		}
		return tx.Commit()
	}

	activeFinish := status == types.JobStatusRunning &&
		assigned.Valid && assigned.String == workerID &&
		gen == leaseGeneration
	lateFinish := success &&
		status == types.JobStatusDispatched &&
		!assigned.Valid &&
		gen == leaseGeneration

	if !activeFinish && !lateFinish {
		return ErrJobNotAssigned
	}

	if success {
		const completeQ = `
UPDATE jobs
SET status = 'COMPLETED',
    finished_at = NOW(),
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE id = $1`
		if _, err = tx.ExecContext(ctx, completeQ, jobID); err != nil {
			return err
		}
	} else if attempt >= maxAttempts {
		const failQ = `
UPDATE jobs
SET status = 'FAILED',
    finished_at = NOW(),
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE id = $1`
		if _, err = tx.ExecContext(ctx, failQ, jobID); err != nil {
			return err
		}
	} else {
		const retryQ = `
UPDATE jobs
SET status = 'DISPATCHED',
    assigned_worker_id = NULL,
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE id = $1`
		if _, err = tx.ExecContext(ctx, retryQ, jobID); err != nil {
			return err
		}
		if err = insertOutboxTx(ctx, tx, jobID, queue.TargetRetry, attempt); err != nil {
			return err
		}
	}

	if err = insertLogArtifactTx(ctx, tx, jobID, attempt, artifact); err != nil {
		return err
	}

	return tx.Commit()
}

// ListLogArtifacts returns R2 log artifact metadata for a job ordered by attempt.
func (s *Store) ListLogArtifacts(ctx context.Context, jobID string) ([]types.JobLogArtifact, error) {
	const q = `
SELECT id, job_id, attempt, object_key, bytes, lines, created_at
FROM job_log_artifacts
WHERE job_id = $1
ORDER BY attempt ASC, id ASC`

	rows, err := s.db.QueryContext(ctx, q, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []types.JobLogArtifact
	for rows.Next() {
		var art types.JobLogArtifact
		if err := rows.Scan(&art.ID, &art.JobID, &art.Attempt, &art.ObjectKey, &art.Bytes, &art.Lines, &art.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, art)
	}
	return out, rows.Err()
}

// GetJobByIdempotencyKey returns a job created with the given idempotency key.
func (s *Store) GetJobByIdempotencyKey(ctx context.Context, key string) (*types.Job, error) {
	const q = `SELECT ` + jobSelectColumns + ` FROM jobs WHERE idempotency_key = $1`
	job, err := scanJob(s.db.QueryRowContext(ctx, q, key))
	if err != nil {
		return nil, err
	}
	return job, nil
}

func insertLogArtifactTx(ctx context.Context, tx *sql.Tx, jobID string, attempt int, artifact *types.JobLogArtifactInput) error {
	if artifact == nil {
		return nil
	}
	if artifact.Attempt != attempt {
		return fmt.Errorf("log artifact attempt %d does not match job attempt %d", artifact.Attempt, attempt)
	}
	const q = `
INSERT INTO job_log_artifacts (job_id, attempt, object_key, bytes, lines)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (job_id, attempt) DO UPDATE
SET object_key = EXCLUDED.object_key,
    bytes = EXCLUDED.bytes,
    lines = EXCLUDED.lines,
    created_at = NOW()`
	_, err := tx.ExecContext(ctx, q, jobID, attempt, artifact.ObjectKey, artifact.Bytes, artifact.Lines)
	return err
}

func scanJob(row scanner) (*types.Job, error) {
	var j types.Job
	var assigned sql.NullString
	var preset sql.NullString
	var lease sql.NullTime
	var started sql.NullTime
	var finished sql.NullTime
	var idempotency sql.NullString
	var storage string

	if err := row.Scan(
		&j.ID,
		&j.InputPath,
		&j.OutputPath,
		&preset,
		&j.FFmpegArgs,
		&storage,
		&j.Status,
		&assigned,
		&j.Attempt,
		&j.MaxAttempts,
		&lease,
		&j.LeaseGeneration,
		&idempotency,
		&j.MaxDurationSeconds,
		&j.CreatedAt,
		&started,
		&finished,
		&j.UpdatedAt,
	); err != nil {
		return nil, err
	}
	j.Storage = types.StorageMode(storage)
	if preset.Valid {
		j.Preset = preset.String
	}
	if assigned.Valid {
		j.AssignedWorkerID = &assigned.String
	}
	if lease.Valid {
		j.LeaseExpiresAt = &lease.Time
	}
	if started.Valid {
		j.StartedAt = &started.Time
	}
	if finished.Valid {
		j.FinishedAt = &finished.Time
	}
	if idempotency.Valid {
		j.IdempotencyKey = idempotency.String
	}
	return &j, nil
}

type scanner interface {
	Scan(dest ...any) error
}
