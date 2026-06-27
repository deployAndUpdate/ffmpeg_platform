//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"go_distributed_system/internal/testutil"
	"go_distributed_system/internal/types"

	"github.com/google/uuid"
)

func TestMarkStaleWorkersDead(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	stale := time.Now().UTC().Add(-time.Hour)
	execTestSQL(t, `UPDATE workers SET last_heartbeat_at = $1 WHERE id = $2`, stale, workerID)

	n, err := st.MarkStaleWorkersDead(ctx, time.Now().UTC().Add(-30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("marked dead = %d, want 1", n)
	}

	status := workerStatus(t, workerID)
	if status != types.WorkerStatusDead {
		t.Fatalf("status = %q, want DEAD", status)
	}
}

func TestMarkStaleWorkersDeadSkipsRecentHeartbeat(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	n, err := st.MarkStaleWorkersDead(ctx, time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("marked dead = %d, want 0", n)
	}
}

func TestReapExpiredLeaseToRetry(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")
	if _, err := st.ClaimJob(ctx, jobID, workerID, time.Minute); err != nil {
		t.Fatal(err)
	}

	execTestSQL(t, `UPDATE jobs SET lease_expires_at = NOW() - interval '1 minute' WHERE id = $1`, jobID)

	result, err := st.ReapOrphanJobs(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Retried != 1 || result.Failed != 0 {
		t.Fatalf("reap = %+v, want 1 retried", result)
	}

	got, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusDispatched {
		t.Fatalf("status = %q, want DISPATCHED", got.Status)
	}
	if got.AssignedWorkerID != nil {
		t.Fatalf("assigned_worker_id = %v, want nil", got.AssignedWorkerID)
	}

	n, err := st.CountUnpublishedOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("expected retry outbox row, got %d unpublished", n)
	}
}

func TestReapExpiredLeaseToFailed(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := uuid.New().String()
	if err := st.CreateJob(ctx, &types.Job{
		ID: jobID, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", Status: types.JobStatusDispatched, MaxAttempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimJob(ctx, jobID, workerID, time.Minute); err != nil {
		t.Fatal(err)
	}

	execTestSQL(t, `UPDATE jobs SET lease_expires_at = NOW() - interval '1 minute' WHERE id = $1`, jobID)

	result, err := st.ReapOrphanJobs(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Retried != 0 || result.Failed != 1 {
		t.Fatalf("reap = %+v, want 1 failed", result)
	}

	got, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusFailed {
		t.Fatalf("status = %q, want FAILED", got.Status)
	}
}

func TestReapJobsFromDeadWorker(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")
	if _, err := st.ClaimJob(ctx, jobID, workerID, 30*time.Minute); err != nil {
		t.Fatal(err)
	}

	execTestSQL(t, `UPDATE workers SET status = 'DEAD' WHERE id = $1`, workerID)

	result, err := st.ReapOrphanJobs(ctx, time.Now().UTC().Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.Retried != 1 {
		t.Fatalf("reap = %+v, want 1 retried", result)
	}

	got, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusDispatched {
		t.Fatalf("status = %q, want DISPATCHED", got.Status)
	}
}

func TestReapNoOpWhenLeaseValid(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")
	if _, err := st.ClaimJob(ctx, jobID, workerID, 30*time.Minute); err != nil {
		t.Fatal(err)
	}

	result, err := st.ReapOrphanJobs(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.Retried != 0 || result.Failed != 0 {
		t.Fatalf("reap = %+v, want no changes", result)
	}

	got, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusRunning {
		t.Fatalf("status = %q, want RUNNING", got.Status)
	}
}

func TestReapIdempotent(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")
	if _, err := st.ClaimJob(ctx, jobID, workerID, time.Minute); err != nil {
		t.Fatal(err)
	}
	execTestSQL(t, `UPDATE jobs SET lease_expires_at = NOW() - interval '1 minute' WHERE id = $1`, jobID)

	first, err := st.ReapOrphanJobs(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if first.Retried != 1 {
		t.Fatalf("first reap = %+v", first)
	}

	second, err := st.ReapOrphanJobs(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if second.Retried != 0 || second.Failed != 0 {
		t.Fatalf("second reap = %+v, want no changes", second)
	}
}

func execTestSQL(t *testing.T, query string, args ...any) {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func workerStatus(t *testing.T, workerID string) types.WorkerStatus {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var status string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status FROM workers WHERE id = $1`, workerID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return types.WorkerStatus(status)
}
