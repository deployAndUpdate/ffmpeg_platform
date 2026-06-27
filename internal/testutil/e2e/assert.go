//go:build integration

package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go_distributed_system/internal/testutil"
	"go_distributed_system/internal/types"
)

// RequireFFmpeg skips the test when ffmpeg is not available.
func RequireFFmpeg(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
}

// PrepareLocalMedia copies the sample fixture into a temp dir and returns input/output paths.
func PrepareLocalMedia(t *testing.T) (inputPath, outputPath string) {
	t.Helper()

	workDir := t.TempDir()
	inputPath = filepath.Join(workDir, "input.mp4")
	outputPath = filepath.Join(workDir, "output.mp3")
	testutil.CopySampleVideo(t, inputPath)
	return inputPath, outputPath
}

// AssertJobCompleted checks final job fields and local output file.
func AssertJobCompleted(t *testing.T, job *types.Job, workerID, outputPath string) {
	t.Helper()

	if job.Status != types.JobStatusCompleted {
		t.Fatalf("status = %q, want COMPLETED", job.Status)
	}
	if job.AssignedWorkerID == nil || *job.AssignedWorkerID != workerID {
		t.Fatalf("assigned_worker_id = %v, want %s", job.AssignedWorkerID, workerID)
	}
	if job.FinishedAt == nil {
		t.Fatal("finished_at is nil")
	}

	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("output file missing: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("output file is empty")
	}
}

// AssertJobLogsNonEmpty checks GET /jobs/{id}/logs returned entries.
func AssertJobLogsNonEmpty(t *testing.T, logs []types.JobLog) {
	t.Helper()

	if len(logs) == 0 {
		t.Fatal("expected job logs via GET /jobs/{id}/logs")
	}
	for i, entry := range logs {
		if strings.TrimSpace(entry.Line) == "" {
			t.Fatalf("log entry %d has empty line", i)
		}
	}
}

// AssertStatusSubsequence verifies expected statuses appear in order within seen.
func AssertStatusSubsequence(t *testing.T, seen []types.JobStatus, expected ...types.JobStatus) {
	t.Helper()

	idx := 0
	for _, status := range seen {
		if status == expected[idx] {
			idx++
			if idx == len(expected) {
				return
			}
		}
	}
	t.Fatalf("status subsequence not found: seen=%v want=%v", seen, expected)
}
