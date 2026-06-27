package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go_distributed_system/internal/types"
)

// ListJobsFilter controls admin job listing.
type ListJobsFilter struct {
	Status   *types.JobStatus
	WorkerID *string
	Limit    int
	Offset   int
}

// ListJobsResult is a paginated job list.
type ListJobsResult struct {
	Jobs  []types.Job `json:"jobs"`
	Total int         `json:"total"`
}

// AdminStats aggregates dashboard counters.
type AdminStats struct {
	JobsByStatus  map[string]int `json:"jobs_by_status"`
	WorkersActive int            `json:"workers_active"`
	WorkersDead   int            `json:"workers_dead"`
	QueueDepth    int            `json:"queue_depth"`
}

// ListJobs returns jobs ordered by created_at desc with optional filters.
func (s *Store) ListJobs(ctx context.Context, f ListJobsFilter) (ListJobsResult, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > 200 {
		f.Limit = 200
	}
	if f.Offset < 0 {
		f.Offset = 0
	}

	var (
		where  []string
		args   []any
		argNum = 1
	)

	if f.Status != nil {
		where = append(where, fmt.Sprintf("status = $%d", argNum))
		args = append(args, string(*f.Status))
		argNum++
	}
	if f.WorkerID != nil && *f.WorkerID != "" {
		where = append(where, fmt.Sprintf("assigned_worker_id = $%d", argNum))
		args = append(args, *f.WorkerID)
		argNum++
	}

	whereSQL := ""
	if len(where) > 0 {
		whereSQL = "WHERE " + strings.Join(where, " AND ")
	}

	countQ := "SELECT COUNT(*) FROM jobs " + whereSQL
	var total int
	if err := s.db.QueryRowContext(ctx, countQ, args...).Scan(&total); err != nil {
		return ListJobsResult{}, err
	}

	listArgs := append(append([]any{}, args...), f.Limit, f.Offset)
	listQ := fmt.Sprintf(`
SELECT %s
FROM jobs
%s
ORDER BY created_at DESC
LIMIT $%d OFFSET $%d`, jobSelectColumns, whereSQL, argNum, argNum+1)

	rows, err := s.db.QueryContext(ctx, listQ, listArgs...)
	if err != nil {
		return ListJobsResult{}, err
	}
	defer rows.Close()

	jobs := make([]types.Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return ListJobsResult{}, err
		}
		jobs = append(jobs, *job)
	}
	if err := rows.Err(); err != nil {
		return ListJobsResult{}, err
	}

	return ListJobsResult{Jobs: jobs, Total: total}, nil
}

// ListWorkers returns all workers with a count of currently running jobs.
func (s *Store) ListWorkers(ctx context.Context) ([]types.WorkerStats, error) {
	const q = `
SELECT w.id, w.hostname, w.cpu_cores, w.gpu_available, w.max_parallel_jobs,
       w.last_heartbeat_at, w.status, w.created_at, w.updated_at,
       COALESCE(r.running_jobs, 0)
FROM workers w
LEFT JOIN (
    SELECT assigned_worker_id, COUNT(*) AS running_jobs
    FROM jobs
    WHERE status = 'RUNNING' AND assigned_worker_id IS NOT NULL
    GROUP BY assigned_worker_id
) r ON r.assigned_worker_id = w.id
ORDER BY w.status ASC, w.last_heartbeat_at DESC NULLS LAST, w.hostname ASC`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []types.WorkerStats
	for rows.Next() {
		ws, err := scanWorkerStats(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ws)
	}
	return out, rows.Err()
}

// GetWorker returns a worker by ID with running job count.
func (s *Store) GetWorker(ctx context.Context, id string) (*types.WorkerStats, error) {
	const q = `
SELECT w.id, w.hostname, w.cpu_cores, w.gpu_available, w.max_parallel_jobs,
       w.last_heartbeat_at, w.status, w.created_at, w.updated_at,
       COALESCE(r.running_jobs, 0)
FROM workers w
LEFT JOIN (
    SELECT assigned_worker_id, COUNT(*) AS running_jobs
    FROM jobs
    WHERE status = 'RUNNING' AND assigned_worker_id IS NOT NULL
    GROUP BY assigned_worker_id
) r ON r.assigned_worker_id = w.id
WHERE w.id = $1`

	ws, err := scanWorkerStats(s.db.QueryRowContext(ctx, q, id))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, err
	}
	return &ws, nil
}

// GetAdminStats returns aggregate counters for the admin dashboard.
func (s *Store) GetAdminStats(ctx context.Context) (AdminStats, error) {
	stats := AdminStats{JobsByStatus: make(map[string]int)}

	const jobsQ = `SELECT status, COUNT(*) FROM jobs GROUP BY status`
	rows, err := s.db.QueryContext(ctx, jobsQ)
	if err != nil {
		return stats, err
	}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return stats, err
		}
		stats.JobsByStatus[status] = count
	}
	if err := rows.Close(); err != nil {
		return stats, err
	}
	if err := rows.Err(); err != nil {
		return stats, err
	}

	const workersQ = `
SELECT
  COUNT(*) FILTER (WHERE status = 'ACTIVE'),
  COUNT(*) FILTER (WHERE status = 'DEAD')
FROM workers`
	if err := s.db.QueryRowContext(ctx, workersQ).Scan(&stats.WorkersActive, &stats.WorkersDead); err != nil {
		return stats, err
	}

	stats.QueueDepth = stats.JobsByStatus[string(types.JobStatusDispatched)]

	return stats, nil
}

func scanWorkerStats(row scanner) (types.WorkerStats, error) {
	var ws types.WorkerStats
	var lastHeartbeat sql.NullTime
	if err := row.Scan(
		&ws.ID,
		&ws.Hostname,
		&ws.CPUCores,
		&ws.GPUAvailable,
		&ws.MaxParallelJobs,
		&lastHeartbeat,
		&ws.Status,
		&ws.CreatedAt,
		&ws.UpdatedAt,
		&ws.RunningJobs,
	); err != nil {
		return ws, err
	}
	if lastHeartbeat.Valid {
		ws.LastHeartbeatAt = &lastHeartbeat.Time
	}
	return ws, nil
}
