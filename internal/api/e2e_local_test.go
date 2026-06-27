//go:build integration

package api_test

import (
	"testing"

	"go_distributed_system/internal/testutil/e2e"
	"go_distributed_system/internal/types"
)

func TestE2E_LocalJSONJob_HTTPCreateToCompleted(t *testing.T) {
	e2e.RequireFFmpeg(t)

	stack := e2e.StartStack(t)
	w := e2e.StartWorker(t, stack)
	client := e2e.NewClient(stack.URL)

	inputPath, outputPath := e2e.PrepareLocalMedia(t)
	created := client.CreateLocalJob(t, e2e.CreateLocalJobRequest{
		InputPath:  inputPath,
		OutputPath: outputPath,
		Preset:     "mp3_192k",
	})
	if created.Status != types.JobStatusDispatched {
		t.Fatalf("created status = %q, want DISPATCHED", created.Status)
	}

	final := client.WaitForJob(t, stack, created.ID, types.JobStatusCompleted, e2e.DefaultJobTimeout)
	e2e.AssertJobCompleted(t, final, w.ID, outputPath)
}

func TestE2E_LocalJob_StatusTransitions(t *testing.T) {
	e2e.RequireFFmpeg(t)

	stack := e2e.StartStack(t)
	e2e.StartWorker(t, stack)
	client := e2e.NewClient(stack.URL)

	inputPath, outputPath := e2e.PrepareLocalMedia(t)
	created := client.CreateLocalJob(t, e2e.CreateLocalJobRequest{
		InputPath:  inputPath,
		OutputPath: outputPath,
		Preset:     "mp3_192k",
	})

	seen := client.WaitForJobCollectStatuses(t, stack, created.ID, e2e.DefaultJobTimeout)
	e2e.AssertStatusSubsequence(t, seen,
		types.JobStatusDispatched,
		types.JobStatusRunning,
		types.JobStatusCompleted,
	)
}

func TestE2E_LocalJob_Logs(t *testing.T) {
	e2e.RequireFFmpeg(t)

	stack := e2e.StartStack(t)
	w := e2e.StartWorker(t, stack)
	client := e2e.NewClient(stack.URL)

	inputPath, outputPath := e2e.PrepareLocalMedia(t)
	created := client.CreateLocalJob(t, e2e.CreateLocalJobRequest{
		InputPath:  inputPath,
		OutputPath: outputPath,
		Preset:     "mp3_192k",
	})

	final := client.WaitForJob(t, stack, created.ID, types.JobStatusCompleted, e2e.DefaultJobTimeout)
	e2e.AssertJobCompleted(t, final, w.ID, outputPath)

	logs := client.GetJobLogs(t, created.ID)
	e2e.AssertJobLogsNonEmpty(t, logs)
}
