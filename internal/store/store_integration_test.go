//go:build integration

package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go_distributed_system/internal/store"
	"go_distributed_system/internal/testutil"
	"go_distributed_system/internal/types"

	"github.com/google/uuid"
)

func TestCreateJobGetJob(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	id := uuid.New().String()
	job := &types.Job{
		ID:          id,
		InputPath:   "/in.mp4",
		OutputPath:  "/out.mp4",
		FFmpegArgs:  "-c:v libx264",
		Status:      types.JobStatusQueued,
		Attempt:     0,
		MaxAttempts: 3,
	}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusQueued {
		t.Fatalf("status = %q, want QUEUED", got.Status)
	}
	if got.Attempt != 0 {
		t.Fatalf("attempt = %d, want 0", got.Attempt)
	}
}

func TestRegisterWorkerHeartbeat(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	id := uuid.New().String()
	now := time.Now().UTC()
	worker := &types.Worker{
		ID:              id,
		Hostname:        "node-1",
		CPUCores:        8,
		GPUAvailable:    false,
		MaxParallelJobs: 2,
		LastHeartbeatAt: &now,
		Status:          types.WorkerStatusActive,
	}
	if err := st.RegisterWorker(ctx, worker); err != nil {
		t.Fatal(err)
	}

	later := now.Add(time.Minute)
	if err := st.Heartbeat(ctx, id, later); err != nil {
		t.Fatal(err)
	}

	if err := st.Heartbeat(ctx, uuid.New().String(), later); err == nil {
		t.Fatal("expected error for unknown worker")
	}
}

func TestAcquireJobEmptyQueue(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	job, err := st.AcquireJob(ctx, workerID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job != nil {
		t.Fatalf("job = %+v, want nil", job)
	}
}

func TestAcquireJobClaimsQueuedJob(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := uuid.New().String()
	if err := st.CreateJob(ctx, &types.Job{
		ID: jobID, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", Status: types.JobStatusQueued,
		MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}

	acquired, err := st.AcquireJob(ctx, workerID, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if acquired == nil {
		t.Fatal("expected job")
	}
	if acquired.ID != jobID {
		t.Fatalf("id = %s, want %s", acquired.ID, jobID)
	}
	if acquired.Status != types.JobStatusRunning {
		t.Fatalf("status = %q, want RUNNING", acquired.Status)
	}
	if acquired.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", acquired.Attempt)
	}
	if acquired.AssignedWorkerID == nil || *acquired.AssignedWorkerID != workerID {
		t.Fatalf("assigned_worker_id = %v, want %s", acquired.AssignedWorkerID, workerID)
	}
	if acquired.LeaseExpiresAt == nil {
		t.Fatal("expected lease_expires_at")
	}
}

func TestAcquireJobSkipLocked(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerA := uuid.New().String()
	workerB := uuid.New().String()
	registerWorker(t, st, workerA)
	registerWorker(t, st, workerB)

	if err := st.CreateJob(ctx, &types.Job{
		ID: uuid.New().String(), InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", Status: types.JobStatusQueued, MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make(chan *types.Job, 2)
	for _, wid := range []string{workerA, workerB} {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			job, err := st.AcquireJob(ctx, workerID, time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			results <- job
		}(wid)
	}
	wg.Wait()
	close(results)

	var claimed int
	for job := range results {
		if job != nil {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("claimed jobs = %d, want 1", claimed)
	}
}

func TestFinishJobCompleted(t *testing.T) {
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

	logs := []types.JobLogEntry{
		{Stream: "stdout", Line: "frame=100"},
		{Stream: "stderr", Line: "warning"},
	}
	if err := st.FinishJob(ctx, jobID, workerID, true, logs); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusCompleted {
		t.Fatalf("status = %q, want COMPLETED", got.Status)
	}
	if got.FinishedAt == nil {
		t.Fatal("expected finished_at")
	}

	storedLogs, err := st.GetJobLogs(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(storedLogs) != 2 {
		t.Fatalf("logs len = %d, want 2", len(storedLogs))
	}
}

func TestFinishJobRetryAndFailed(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := uuid.New().String()
	if err := st.CreateJob(ctx, &types.Job{
		ID: jobID, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", Status: types.JobStatusQueued,
		MaxAttempts: 2,
	}); err != nil {
		t.Fatal(err)
	}

	acquired, err := st.AcquireJob(ctx, workerID, time.Minute)
	if err != nil || acquired == nil {
		t.Fatalf("acquire: job=%v err=%v", acquired, err)
	}
	if err := st.FinishJob(ctx, jobID, workerID, false, nil); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusRetry {
		t.Fatalf("after first fail status = %q, want RETRY", got.Status)
	}

	acquired, err = st.AcquireJob(ctx, workerID, time.Minute)
	if err != nil || acquired == nil {
		t.Fatalf("re-acquire: job=%v err=%v", acquired, err)
	}
	if err := st.FinishJob(ctx, jobID, workerID, false, nil); err != nil {
		t.Fatal(err)
	}

	got, err = st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusFailed {
		t.Fatalf("after max attempts status = %q, want FAILED", got.Status)
	}
}

func TestFinishJobWrongWorker(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	otherWorker := uuid.New().String()
	registerWorker(t, st, workerID)
	registerWorker(t, st, otherWorker)

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

	err := st.FinishJob(ctx, jobID, otherWorker, true, nil)
	if !errors.Is(err, store.ErrJobNotAssigned) {
		t.Fatalf("err = %v, want ErrJobNotAssigned", err)
	}
}

func registerWorker(t *testing.T, st *store.Store, id string) {
	t.Helper()
	now := time.Now().UTC()
	if err := st.RegisterWorker(context.Background(), &types.Worker{
		ID: id, Hostname: "test", CPUCores: 4, MaxParallelJobs: 1,
		LastHeartbeatAt: &now, Status: types.WorkerStatusActive,
	}); err != nil {
		t.Fatal(err)
	}
}
