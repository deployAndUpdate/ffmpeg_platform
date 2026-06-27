package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"go_distributed_system/internal/types"
)

// ClaimJob atomically assigns a dispatched job to a worker.
func (s *Store) ClaimJob(ctx context.Context, jobID, workerID string, lease time.Duration) (*types.Job, error) {
	leaseSeconds := int64(lease.Seconds())
	if leaseSeconds <= 0 {
		leaseSeconds = 1800
	}

	const q = `
UPDATE jobs j
SET status = 'RUNNING',
    assigned_worker_id = $2,
    attempt = j.attempt + 1,
    lease_expires_at = NOW() + ($3 * interval '1 second'),
    lease_generation = j.lease_generation + 1,
    started_at = COALESCE(j.started_at, NOW()),
    updated_at = NOW()
WHERE j.id = $1
  AND j.status = 'DISPATCHED'
RETURNING ` + jobReturningColumns

	row := s.db.QueryRowContext(ctx, q, jobID, workerID, leaseSeconds)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotClaimable
		}
		return nil, err
	}
	return job, nil
}
