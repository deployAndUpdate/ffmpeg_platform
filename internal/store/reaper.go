package store

import (
	"context"
	"time"

	"go_distributed_system/internal/queue"
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

// ReapOrphanJobs moves RUNNING jobs with an expired lease or a DEAD worker to DISPATCHED+outbox or FAILED.
// attempt is not incremented — it was already bumped when the job was claimed.
func (s *Store) ReapOrphanJobs(ctx context.Context, now time.Time) (ReapResult, error) {
	const selectQ = `
SELECT j.id, j.attempt, j.max_attempts
FROM jobs j
JOIN workers w ON j.assigned_worker_id = w.id
WHERE j.status = 'RUNNING'
  AND (
    j.lease_expires_at < $1
    OR w.status = 'DEAD'
  )
FOR UPDATE OF j`

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReapResult{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(ctx, selectQ, now)
	if err != nil {
		return ReapResult{}, err
	}

	type orphan struct {
		id          string
		attempt     int
		maxAttempts int
	}
	var orphans []orphan
	for rows.Next() {
		var o orphan
		if err := rows.Scan(&o.id, &o.attempt, &o.maxAttempts); err != nil {
			rows.Close()
			return ReapResult{}, err
		}
		orphans = append(orphans, o)
	}
	if err := rows.Close(); err != nil {
		return ReapResult{}, err
	}
	if err := rows.Err(); err != nil {
		return ReapResult{}, err
	}

	var result ReapResult
	for _, o := range orphans {
		if o.attempt >= o.maxAttempts {
			const failQ = `
UPDATE jobs
SET status = 'FAILED',
    finished_at = NOW(),
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE id = $1`
			if _, err = tx.ExecContext(ctx, failQ, o.id); err != nil {
				return ReapResult{}, err
			}
			result.Failed++
			continue
		}

		const retryQ = `
UPDATE jobs
SET status = 'DISPATCHED',
    assigned_worker_id = NULL,
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE id = $1`
		if _, err = tx.ExecContext(ctx, retryQ, o.id); err != nil {
			return ReapResult{}, err
		}
		if err = insertOutboxTx(ctx, tx, o.id, queue.TargetRetry, o.attempt); err != nil {
			return ReapResult{}, err
		}
		result.Retried++
	}

	if err = tx.Commit(); err != nil {
		return ReapResult{}, err
	}
	return result, nil
}
