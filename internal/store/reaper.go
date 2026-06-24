package store

import (
	"context"
	"time"

	"go_distributed_system/internal/types"
)

// ReapResult counts jobs moved out of RUNNING by ReapOrphanJobs.
type ReapResult struct {
	Retried int64
	Failed  int64
}

// MarkStaleWorkersDead marks ACTIVE workers without a recent heartbeat as DEAD.
func (s *Store) MarkStaleWorkersDead(ctx context.Context, cutoff time.Time) (int64, error) {
	const q = `
UPDATE workers
SET status = 'DEAD', updated_at = NOW()
WHERE status = 'ACTIVE'
  AND last_heartbeat_at IS NOT NULL
  AND last_heartbeat_at < $1`
	res, err := s.db.ExecContext(ctx, q, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ReapOrphanJobs moves RUNNING jobs with an expired lease or a DEAD worker to RETRY or FAILED.
// attempt is not incremented — it was already bumped when the job was acquired.
func (s *Store) ReapOrphanJobs(ctx context.Context, now time.Time) (ReapResult, error) {
	const q = `
UPDATE jobs j
SET status = CASE WHEN j.attempt >= j.max_attempts THEN 'FAILED' ELSE 'RETRY' END,
    finished_at = CASE WHEN j.attempt >= j.max_attempts THEN NOW() ELSE j.finished_at END,
    assigned_worker_id = CASE WHEN j.attempt >= j.max_attempts THEN j.assigned_worker_id ELSE NULL END,
    lease_expires_at = NULL,
    updated_at = NOW()
FROM workers w
WHERE j.status = 'RUNNING'
  AND j.assigned_worker_id = w.id
  AND (
    j.lease_expires_at < $1
    OR w.status = 'DEAD'
  )
RETURNING j.status`

	rows, err := s.db.QueryContext(ctx, q, now)
	if err != nil {
		return ReapResult{}, err
	}
	defer rows.Close()

	var result ReapResult
	for rows.Next() {
		var status types.JobStatus
		if err := rows.Scan(&status); err != nil {
			return ReapResult{}, err
		}
		switch status {
		case types.JobStatusRetry:
			result.Retried++
		case types.JobStatusFailed:
			result.Failed++
		}
	}
	return result, rows.Err()
}
