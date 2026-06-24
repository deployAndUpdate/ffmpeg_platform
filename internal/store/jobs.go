package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go_distributed_system/internal/types"
)

var ErrJobNotAssigned = errors.New("job is not assigned to this worker or not running")

// AcquireJob atomically claims the next available job for a worker.
func (s *Store) AcquireJob(ctx context.Context, workerID string, lease time.Duration) (*types.Job, error) {
	const q = `
WITH next_job AS (
    SELECT id
    FROM jobs
    WHERE status IN ('QUEUED', 'RETRY')
    ORDER BY created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs j
SET status = 'RUNNING',
    assigned_worker_id = $1,
    attempt = j.attempt + 1,
    lease_expires_at = NOW() + ($2 * interval '1 second'),
    started_at = NOW(),
    updated_at = NOW()
FROM next_job
WHERE j.id = next_job.id
RETURNING j.id, j.input_path, j.output_path, j.ffmpeg_args, j.status, j.assigned_worker_id,
          j.attempt, j.max_attempts, j.lease_expires_at, j.created_at, j.started_at, j.finished_at, j.updated_at`

	leaseSeconds := int64(lease.Seconds())
	if leaseSeconds <= 0 {
		leaseSeconds = 1800
	}

	row := s.db.QueryRowContext(ctx, q, workerID, leaseSeconds)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return job, nil
}

// FinishJob marks a running job as completed or failed/retry and stores logs.
func (s *Store) FinishJob(ctx context.Context, jobID, workerID string, success bool, logs []types.JobLogEntry) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var attempt, maxAttempts int
	const selectQ = `
SELECT attempt, max_attempts
FROM jobs
WHERE id = $1 AND assigned_worker_id = $2 AND status = 'RUNNING'
FOR UPDATE`
	if err = tx.QueryRowContext(ctx, selectQ, jobID, workerID).Scan(&attempt, &maxAttempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotAssigned
		}
		return err
	}

	if success {
		const completeQ = `
UPDATE jobs
SET status = 'COMPLETED',
    finished_at = NOW(),
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE id = $1 AND assigned_worker_id = $2 AND status = 'RUNNING'`
		if _, err = tx.ExecContext(ctx, completeQ, jobID, workerID); err != nil {
			return err
		}
	} else if attempt >= maxAttempts {
		const failQ = `
UPDATE jobs
SET status = 'FAILED',
    finished_at = NOW(),
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE id = $1 AND assigned_worker_id = $2 AND status = 'RUNNING'`
		if _, err = tx.ExecContext(ctx, failQ, jobID, workerID); err != nil {
			return err
		}
	} else {
		const retryQ = `
UPDATE jobs
SET status = 'RETRY',
    assigned_worker_id = NULL,
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE id = $1 AND assigned_worker_id = $2 AND status = 'RUNNING'`
		if _, err = tx.ExecContext(ctx, retryQ, jobID, workerID); err != nil {
			return err
		}
	}

	if err = insertJobLogsTx(ctx, tx, jobID, logs); err != nil {
		return err
	}

	return tx.Commit()
}

// GetJobLogs returns log lines for a job ordered by timestamp.
func (s *Store) GetJobLogs(ctx context.Context, jobID string) ([]types.JobLog, error) {
	const q = `
SELECT id, ts, stream, line
FROM job_logs
WHERE job_id = $1
ORDER BY ts ASC, id ASC`

	rows, err := s.db.QueryContext(ctx, q, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []types.JobLog
	for rows.Next() {
		var entry types.JobLog
		if err := rows.Scan(&entry.ID, &entry.TS, &entry.Stream, &entry.Line); err != nil {
			return nil, err
		}
		logs = append(logs, entry)
	}
	return logs, rows.Err()
}

func insertJobLogsTx(ctx context.Context, tx *sql.Tx, jobID string, logs []types.JobLogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	const q = `INSERT INTO job_logs (job_id, stream, line) VALUES ($1, $2, $3)`
	for _, entry := range logs {
		if _, err := tx.ExecContext(ctx, q, jobID, entry.Stream, entry.Line); err != nil {
			return err
		}
	}
	return nil
}

func scanJob(row scanner) (*types.Job, error) {
	var j types.Job
	var assigned sql.NullString
	var lease sql.NullTime
	var started sql.NullTime
	var finished sql.NullTime

	if err := row.Scan(
		&j.ID,
		&j.InputPath,
		&j.OutputPath,
		&j.FFmpegArgs,
		&j.Status,
		&assigned,
		&j.Attempt,
		&j.MaxAttempts,
		&lease,
		&j.CreatedAt,
		&started,
		&finished,
		&j.UpdatedAt,
	); err != nil {
		return nil, err
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
	return &j, nil
}

type scanner interface {
	Scan(dest ...any) error
}
