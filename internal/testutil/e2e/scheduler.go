//go:build integration

package e2e

import (
	"context"
	"net/http/httptest"
	"testing"

	"go_distributed_system/internal/api"
	"go_distributed_system/internal/api/auth"
	"go_distributed_system/internal/outbox"
	"go_distributed_system/internal/queue"
	"go_distributed_system/internal/reaper"
	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/store"
	"go_distributed_system/internal/testutil"
)

// Stack runs scheduler HTTP API with outbox relay and reaper (same components as cmd/scheduler).
type Stack struct {
	Store   *store.Store
	Rabbit  *queue.Rabbit
	Storage *storage.Memory
	URL     string

	cancel context.CancelFunc
}

// StartStack wires PostgreSQL, RabbitMQ, in-memory object storage, HTTP API, relay and reaper.
func StartStack(t *testing.T) *Stack {
	t.Helper()

	st, cleanupStore := testutil.OpenStore(t)
	t.Cleanup(cleanupStore)

	rabbit := testutil.OpenRabbit(t)
	mem := storage.NewMemory("e2e")

	handler := api.NewServerWithStorageAuthAndRabbit(st, mem, storage.Config{}, auth.Config{}, rabbit)
	srv := httptest.NewServer(handler)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		srv.Close()
	})

	go outbox.New(st, rabbit, outbox.Config{
		Interval: relayInterval,
		Batch:    relayBatch,
		Enabled:  true,
	}).Run(ctx)

	go reaper.New(st, reaper.Config{
		Interval:      reaperInterval,
		WorkerTimeout: reaperWorkerTTL,
		Enabled:       true,
	}).Run(ctx)

	return &Stack{
		Store:   st,
		Rabbit:  rabbit,
		Storage: mem,
		URL:     srv.URL,
		cancel:  cancel,
	}
}
