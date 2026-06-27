//go:build integration

package api_test

import (
	"context"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"go_distributed_system/internal/api"
	"go_distributed_system/internal/api/auth"
	"go_distributed_system/internal/outbox"
	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/store"
	"go_distributed_system/internal/testutil"
	"go_distributed_system/internal/types"
	"go_distributed_system/internal/worker"
)

func TestJobLifecycleE2E_RabbitOutbox(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}

	st, cleanup := testutil.OpenStore(t)
	defer cleanup()
	rabbit := testutil.OpenRabbit(t)

	workDir := t.TempDir()
	inputPath := filepath.Join(workDir, "input.mp4")
	outputPath := filepath.Join(workDir, "output.mp3")
	testutil.CopySampleVideo(t, inputPath)

	ctx := context.Background()
	jobID := uuid.New().String()
	if err := st.CreateAndDispatch(ctx, &store.JobCreateParams{
		ID:          jobID,
		InputPath:   inputPath,
		OutputPath:  outputPath,
		Preset:      "mp3_192k",
		FFmpegArgs:  "-vn -acodec libmp3lame -b:a 192k",
		Storage:     types.StorageLocal,
		MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}

	relay := outbox.New(st, rabbit, outbox.Config{Batch: 10, Enabled: true})
	testutil.RelayUntilOutboxEmpty(t, relay, st.CountUnpublishedOutbox)

	srv := httptest.NewServer(api.NewServerWithStorageAuthAndRabbit(st, nil, storage.Config{}, auth.Config{}, rabbit))
	defer srv.Close()

	workerID := uuid.New().String()
	w := worker.New(worker.Config{
		ID:                    workerID,
		Hostname:              "integration-test",
		CPUCores:              1,
		MaxParallelJobs:       1,
		SchedulerURL:          srv.URL,
		HeartbeatEvery:        2 * time.Second,
		LeaseRenewInterval:    5 * time.Second,
		JobLeaseDuration:      5 * time.Minute,
		DefaultMaxJobDuration: 10 * time.Minute,
	}, nil, rabbit)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(runCtx)
	}()

	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		job, err := st.GetJob(ctx, jobID)
		if err != nil {
			t.Fatal(err)
		}
		switch job.Status {
		case types.JobStatusCompleted:
			cancelRun()
			goto verify
		case types.JobStatusFailed:
			t.Fatalf("job failed after %d attempts", job.Attempt)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("timeout waiting for job to complete")

verify:
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("worker run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop after job completion")
	}

	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("output file missing: %v", err)
	}

	logs, err := st.GetJobLogs(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) == 0 {
		t.Fatal("expected ffmpeg logs to be stored")
	}

	final, err := st.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != types.JobStatusCompleted {
		t.Fatalf("final status = %q, want COMPLETED", final.Status)
	}
	if final.AssignedWorkerID == nil || *final.AssignedWorkerID != workerID {
		t.Fatalf("assigned_worker_id = %v, want %s", final.AssignedWorkerID, workerID)
	}
}
