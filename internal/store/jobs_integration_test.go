//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"go_distributed_system/internal/outbox"
	"go_distributed_system/internal/queue"
	"go_distributed_system/internal/store"
	"go_distributed_system/internal/testutil"
	"go_distributed_system/internal/types"

	"github.com/google/uuid"
)

func TestOutboxRelayEndToEnd(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()
	rabbit := testutil.OpenRabbit(t)

	ctx := context.Background()
	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")

	relay := outbox.New(st, rabbit, outbox.Config{Interval: time.Second, Batch: 10, Enabled: true})
	testutil.RelayUntilOutboxEmpty(t, relay, st.CountUnpublishedOutbox)

	ready, err := rabbit.MessagesReady()
	if err != nil {
		t.Fatal(err)
	}
	if ready != 1 {
		t.Fatalf("messages ready = %d, want 1", ready)
	}

	n, err := st.CountUnpublishedOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("unpublished outbox = %d, want 0", n)
	}
	_ = jobID
}

func TestOutboxRelayCrashRecovery(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()
	mem := queue.NewMemoryPublisher()

	ctx := context.Background()
	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")

	relay := outbox.New(st, mem, outbox.Config{Batch: 10, Enabled: true})
	if err := relay.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := relay.Tick(ctx); err != nil {
		t.Fatal(err)
	}

	if mem.PublishCount() != 1 {
		t.Fatalf("publish count = %d, want 1", mem.PublishCount())
	}
	published := mem.Published()
	if len(published) != 1 || published[0].Msg.JobID != jobID {
		t.Fatalf("published = %+v", published)
	}

	n, err := st.CountUnpublishedOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("unpublished outbox = %d, want 0", n)
	}
}

func TestQueueJobCreatesOutbox(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	id := uuid.New().String()
	if err := st.CreateJob(ctx, &types.Job{
		ID: id, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", Status: types.JobStatusNew, MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}

	if err := st.QueueJob(ctx, id); err != nil {
		t.Fatal(err)
	}

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
		t.Fatalf("outbox = %d, want 1", n)
	}
}

func TestRenewLease(t *testing.T) {
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

	renewed, err := st.RenewLease(ctx, jobID, workerID, claimed.LeaseGeneration, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.LeaseExpiresAt == nil || !renewed.LeaseExpiresAt.After(*claimed.LeaseExpiresAt) {
		t.Fatal("expected extended lease")
	}
}

func TestFinishJobLateSuccess(t *testing.T) {
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

	execTestSQL(t, `UPDATE jobs SET status = 'DISPATCHED', assigned_worker_id = NULL, lease_expires_at = NULL WHERE id = $1`, jobID)

	if err := st.FinishJob(ctx, jobID, workerID, claimed.LeaseGeneration, true, nil); err != nil {
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

func TestRenewLeaseWrongGeneration(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	workerID := uuid.New().String()
	registerWorker(t, st, workerID)

	jobID := testutil.CreateDispatchedJob(t, st, "/in.mp4", "/out.mp4")
	if _, err := st.ClaimJob(ctx, jobID, workerID, time.Minute); err != nil {
		t.Fatal(err)
	}

	_, err := st.RenewLease(ctx, jobID, workerID, 999, time.Minute)
	if err == nil || err != store.ErrLeaseLost {
		t.Fatalf("err = %v, want ErrLeaseLost", err)
	}
}

func TestGetJobByIdempotencyKey(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	ctx := context.Background()
	id := uuid.New().String()
	key := "idem-" + uuid.New().String()
	if err := st.CreateAndDispatch(ctx, &store.JobCreateParams{
		ID: id, InputPath: "/in.mp4", OutputPath: "/out.mp4",
		FFmpegArgs: "-c:v libx264", MaxAttempts: 3, IdempotencyKey: key,
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
