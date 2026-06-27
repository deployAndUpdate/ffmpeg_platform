//go:build integration

package worker

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"go_distributed_system/internal/testutil"
)

func TestExecuteJobDoesNotDeadlock(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}

	w := New(Config{
		ID:                    "deadlock-test",
		Hostname:              "test",
		CPUCores:              1,
		MaxParallelJobs:       1,
		SchedulerURL:          "http://localhost:1",
		LeaseRenewInterval:    5 * time.Minute,
		JobLeaseDuration:      time.Minute,
		DefaultMaxJobDuration: time.Minute,
	}, nil, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = w.executeJob(context.Background(), testutil.ShortLocalMP3Job(t))
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("executeJob deadlocked after runJob (lease renewal goroutine)")
	}
}
