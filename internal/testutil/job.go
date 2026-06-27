//go:build integration

package testutil

import (
	"context"
	"testing"

	"go_distributed_system/internal/store"
	"go_distributed_system/internal/types"

	"github.com/google/uuid"
)

// CreateDispatchedJob inserts a DISPATCHED job with outbox via CreateAndDispatch.
func CreateDispatchedJob(t *testing.T, st *store.Store, inputPath, outputPath string) string {
	t.Helper()
	id := uuid.New().String()
	ctx := context.Background()
	if err := st.CreateAndDispatch(ctx, &store.JobCreateParams{
		ID:          id,
		InputPath:   inputPath,
		OutputPath:  outputPath,
		FFmpegArgs:  "-c:v libx264",
		Storage:     types.StorageLocal,
		Attempt:     0,
		MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
	return id
}
