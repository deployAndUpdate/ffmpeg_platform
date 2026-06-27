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

func TestCreateAndDispatch(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	id := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")

	got, err := st.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusDispatched {
		t.Fatalf("status = %q, want DISPATCHED", got.Status)
	}

	n, err := st.CountUnpublishedOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("unpublished outbox = %d, want 1", n)
	}
}

func TestClaimJobNotDispatched(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	_, err := st.ClaimJob(ctx, uuid.New().String(), workerID, time.Minute)
	if !errors.Is(err, store.ErrJobNotClaimable) {
		t.Fatalf("err = %v, want ErrJobNotClaimable", err)
	}
}

func TestClaimJobFromDispatched(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")

	claimed, err := st.ClaimJob(ctx, jobID, workerID, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != types.JobStatusRunning {
		t.Fatalf("status = %q, want RUNNING", claimed.Status)
	}
	if claimed.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", claimed.Attempt)
	}
	if claimed.LeaseGeneration != 1 {
		t.Fatalf("generation = %d, want 1", claimed.LeaseGeneration)
	}
}

func TestClaimJobConcurrentIdempotent(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerA := uuid.New().String()
	workerB := uuid.New().String()
	registerWorker(t, st, workerA)
	registerWorker(t, st, workerB)

	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, wid := range []string{workerA, workerB} {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			_, err := st.ClaimJob(ctx, jobID, workerID, time.Minute)
			results <- err
		}(wid)
	}
	wg.Wait()
	close(results)

	var ok, conflict int
	for err := range results {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, store.ErrJobNotClaimable):
			conflict++
		default:
			t.Errorf("unexpected err: %v", err)
		}
	}
	if ok != 1 || conflict != 1 {
		t.Fatalf("ok=%d conflict=%d, want 1 each", ok, conflict)
	}
}

func TestFinishJobCompleted(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")
	claimed, err := st.ClaimJob(ctx, jobID, workerID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	logs := []types.JobLogEntry{
		{Stream: "stdout", Line: "frame=100"},
		{Stream: "stderr", Line: "warning"},
	}
	if err := st.FinishJob(ctx, jobID, workerID, claimed.LeaseGeneration, true, logs); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusCompleted {
		t.Fatalf("status = %q, want COMPLETED", got.Status)
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

	id := uuid.New().String()
	if err := st.CreateAndDispatch(ctx, &store.JobCreateParams{
		ID: id, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", MaxAttempts: 2,
	}); err != nil {
		t.Fatal(err)
	}

	claimed, err := st.ClaimJob(ctx, id, workerID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.FinishJob(ctx, id, workerID, claimed.LeaseGeneration, false, nil); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.JobStatusDispatched {
		t.Fatalf("after first fail status = %q, want DISPATCHED", got.Status)
	}
	n, err := st.CountUnpublishedOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("outbox rows = %d, want 2 (initial + retry)", n)
	}

	claimed, err = st.ClaimJob(ctx, id, workerID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.FinishJob(ctx, id, workerID, claimed.LeaseGeneration, false, nil); err != nil {
		t.Fatal(err)
	}

	got, err = st.GetJob(ctx, id)
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

	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")
	claimed, err := st.ClaimJob(ctx, jobID, workerID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	err = st.FinishJob(ctx, jobID, otherWorker, claimed.LeaseGeneration, true, nil)
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
