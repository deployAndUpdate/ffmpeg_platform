//go:build integration

package testutil

import (
	"context"
	"os"
	"testing"
	"time"

	"go_distributed_system/internal/queue"
)

// OpenRabbit connects to TEST_RABBITMQ_URL and purges queues.
func OpenRabbit(t *testing.T) *queue.Rabbit {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("TEST_RABBITMQ_URL not set (run: make test-integration)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		r      *queue.Rabbit
		lastErr error
	)
	for attempt := 0; attempt < 30; attempt++ {
		candidate, err := queue.NewRabbit(queue.RabbitConfig{URL: url, Prefetch: 1})
		if err != nil {
			lastErr = err
		} else if pingErr := candidate.Ping(ctx); pingErr != nil {
			lastErr = pingErr
			_ = candidate.Close()
		} else {
			r = candidate
			lastErr = nil
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("open rabbit: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	if r == nil {
		t.Fatalf("open rabbit: %v", lastErr)
	}
	if err := r.PurgeQueues(); err != nil {
		_ = r.Close()
		t.Fatalf("purge rabbit queues: %v", err)
	}

	t.Cleanup(func() { _ = r.Close() })
	return r
}

// RelayUntilOutboxEmpty runs outbox relay ticks until all rows are published or timeout.
func RelayUntilOutboxEmpty(t *testing.T, relay interface {
	Tick(context.Context) error
}, countFn func(context.Context) (int, error)) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		n, err := countFn(ctx)
		if err != nil {
			t.Fatalf("count outbox: %v", err)
		}
		if n == 0 {
			return
		}
		if err := relay.Tick(ctx); err != nil {
			t.Fatalf("relay tick: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timeout waiting for outbox to drain")
}
