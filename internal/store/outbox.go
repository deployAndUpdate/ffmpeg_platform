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

// OutboxEntry is a row waiting to be published to RabbitMQ.
type OutboxEntry struct {
	ID          int64
	JobID       string
	QueueTarget queue.Target
	Attempt     int
	CreatedAt   time.Time
}

var (
	ErrJobNotClaimable = errors.New("job is not dispatchable or already claimed")
)

// insertOutboxTx adds an outbox row inside an open transaction.
func insertOutboxTx(ctx context.Context, tx *sql.Tx, jobID string, target queue.Target, attempt int) error {
	const q = `
INSERT INTO job_outbox (job_id, queue_target, attempt)
VALUES ($1, $2, $3)`
	_, err := tx.ExecContext(ctx, q, jobID, string(target), attempt)
	return err
}

// InsertOutbox adds a dispatch outbox entry (standalone, for tests).
func (s *Store) InsertOutbox(ctx context.Context, jobID string, target queue.Target, attempt int) error {
	const q = `
INSERT INTO job_outbox (job_id, queue_target, attempt)
VALUES ($1, $2, $3)`
	_, err := s.db.ExecContext(ctx, q, jobID, string(target), attempt)
	return err
}

// OutboxTx is the transactional scope for outbox claim/publish.
type OutboxTx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// ClaimOutboxBatch locks unpublished rows and returns them for publishing.
func (s *Store) ClaimOutboxBatch(ctx context.Context, limit int) ([]OutboxEntry, OutboxTx, error) {
	if limit <= 0 {
		limit = 50
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	const q = `
SELECT id, job_id, queue_target, attempt, created_at
FROM job_outbox
WHERE published_at IS NULL
ORDER BY created_at ASC, id ASC
LIMIT $1
FOR UPDATE SKIP LOCKED`

	rows, err := tx.QueryContext(ctx, q, limit)
	if err != nil {
		_ = tx.Rollback()
		return nil, nil, err
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		var target string
		if err := rows.Scan(&e.ID, &e.JobID, &target, &e.Attempt, &e.CreatedAt); err != nil {
			_ = tx.Rollback()
			return nil, nil, err
		}
		e.QueueTarget = queue.Target(target)
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return nil, nil, err
	}
	return entries, tx, nil
}

// MarkOutboxPublishedTx marks a row published inside an open outbox transaction.
func (s *Store) MarkOutboxPublishedTx(ctx context.Context, tx OutboxTx, id int64) error {
	const q = `UPDATE job_outbox SET published_at = NOW(), publish_error = NULL WHERE id = $1`
	res, err := tx.ExecContext(ctx, q, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// ListUnpublishedOutbox returns unpublished outbox rows (read-only, for metrics/tests).
func (s *Store) ListUnpublishedOutbox(ctx context.Context, limit int) ([]OutboxEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	const q = `
SELECT id, job_id, queue_target, attempt, created_at
FROM job_outbox
WHERE published_at IS NULL
ORDER BY created_at ASC, id ASC
LIMIT $1`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		var target string
		if err := rows.Scan(&e.ID, &e.JobID, &target, &e.Attempt, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.QueueTarget = queue.Target(target)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// MarkOutboxPublished marks an outbox row as successfully published.
func (s *Store) MarkOutboxPublished(ctx context.Context, id int64) error {
	const q = `UPDATE job_outbox SET published_at = NOW(), publish_error = NULL WHERE id = $1`
	res, err := s.db.ExecContext(ctx, q, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// MarkOutboxPublishError records a publish failure for an outbox row.
func (s *Store) MarkOutboxPublishError(ctx context.Context, id int64, publishErr error) error {
	const q = `UPDATE job_outbox SET publish_error = $2 WHERE id = $1`
	_, err := s.db.ExecContext(ctx, q, id, publishErr.Error())
	return err
}

// CountUnpublishedOutbox returns rows not yet published.
func (s *Store) CountUnpublishedOutbox(ctx context.Context) (int, error) {
	const q = `SELECT COUNT(*) FROM job_outbox WHERE published_at IS NULL`
	var n int
	err := s.db.QueryRowContext(ctx, q).Scan(&n)
	return n, err
}

// JobCreateParams holds fields for CreateAndDispatch.
type JobCreateParams struct {
	ID                 string
	InputPath          string
	OutputPath         string
	Preset             string
	FFmpegArgs         string
	Storage            types.StorageMode
	Attempt            int
	MaxAttempts        int
	IdempotencyKey     string
	MaxDurationSeconds int
}

// CreateAndDispatch inserts a job and outbox row atomically.
func (s *Store) CreateAndDispatch(ctx context.Context, p *JobCreateParams) error {
	if p.Storage == "" {
		p.Storage = types.StorageLocal
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	const insertJob = `
INSERT INTO jobs (id, input_path, output_path, preset, ffmpeg_args, storage, status, attempt, max_attempts,
                  idempotency_key, max_duration_seconds, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, 'DISPATCHED', $7, $8, $9, $10, NOW(), NOW())`
	if _, err = tx.ExecContext(ctx, insertJob,
		p.ID,
		p.InputPath,
		p.OutputPath,
		nullString(p.Preset),
		p.FFmpegArgs,
		p.Storage,
		p.Attempt,
		p.MaxAttempts,
		nullString(p.IdempotencyKey),
		p.MaxDurationSeconds,
	); err != nil {
		return err
	}
	if err = insertOutboxTx(ctx, tx, p.ID, queue.TargetMain, p.Attempt); err != nil {
		return err
	}
	return tx.Commit()
}
