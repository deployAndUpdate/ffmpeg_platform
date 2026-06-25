//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"go_distributed_system/internal/store"
	"go_distributed_system/internal/testutil"
	"go_distributed_system/internal/types"

	"github.com/google/uuid"
)

func TestRenewLease(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := uuid.New().String()
	if err := st.CreateJob(ctx, &types.Job{
		ID: jobID, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", Status: types.JobStatusQueued, MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}

	acquired, err := st.AcquireJob(ctx, workerID, time.Minute)
	if err != nil || acquired == nil {
		t.Fatalf("acquire: %v", err)
	}
	if acquired.LeaseGeneration != 1 {
		t.Fatalf("generation = %d, want 1", acquired.LeaseGeneration)
	}

	renewed, err := st.RenewLease(ctx, jobID, workerID, acquired.LeaseGeneration, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.LeaseExpiresAt == nil || !renewed.LeaseExpiresAt.After(*acquired.LeaseExpiresAt) {
		t.Fatal("expected extended lease")
	}
}

func TestFinishJobLateSuccess(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := uuid.New().String()
	if err := st.CreateJob(ctx, &types.Job{
		ID: jobID, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", Status: types.JobStatusQueued, MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}

	acquired, err := st.AcquireJob(ctx, workerID, time.Minute)
	if err != nil || acquired == nil {
		t.Fatalf("acquire: %v", err)
	}

	execTestSQL(t, `UPDATE jobs SET status = 'RETRY', assigned_worker_id = NULL, lease_expires_at = NULL WHERE id = $1`, jobID)

	if err := st.FinishJob(ctx, jobID, workerID, acquired.LeaseGeneration, true, nil); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusCompleted {
		t.Fatalf("status = %q, want COMPLETED", got.Status)
	}
}

func TestGetJobByIdempotencyKey(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	id := uuid.New().String()
	key := "idem-" + uuid.New().String()
	if err := st.CreateJob(ctx, &types.Job{
		ID: id, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", Status: types.JobStatusQueued,
		MaxAttempts: 3, IdempotencyKey: key,
	}); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetJobByIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != id {
		t.Fatalf("id = %s, want %s", got.ID, id)
	}
}

func TestRenewLeaseWrongGeneration(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := uuid.New().String()
	if err := st.CreateJob(ctx, &types.Job{
		ID: jobID, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", Status: types.JobStatusQueued, MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AcquireJob(ctx, workerID, time.Minute); err != nil {
		t.Fatal(err)
	}

	_, err := st.RenewLease(ctx, jobID, workerID, 999, time.Minute)
	if err == nil || err != store.ErrLeaseLost {
		t.Fatalf("err = %v, want ErrLeaseLost", err)
	}
}
