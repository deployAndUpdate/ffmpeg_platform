package outbox

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"go_distributed_system/internal/queue"
	"go_distributed_system/internal/store"
)

// Store abstracts outbox persistence for the relay.
type Store interface {
	ClaimOutboxBatch(ctx context.Context, limit int) ([]store.OutboxEntry, store.OutboxTx, error)
	MarkOutboxPublishedTx(ctx context.Context, tx store.OutboxTx, id int64) error
	MarkOutboxPublishError(ctx context.Context, id int64, err error) error
}

// Config holds relay timing settings.
type Config struct {
	Interval time.Duration
	Batch    int
	Enabled  bool
}

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{
		Interval: 500 * time.Millisecond,
		Batch:    50,
		Enabled:  true,
	}
}

// ConfigFromEnv reads OUTBOX_RELAY_* environment variables.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()
	if v := os.Getenv("OUTBOX_RELAY_ENABLED"); v == "false" || v == "0" {
		cfg.Enabled = false
	}
	if v := os.Getenv("OUTBOX_RELAY_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err != nil {
			log.Fatalf("invalid OUTBOX_RELAY_INTERVAL: %q", v)
		} else {
			cfg.Interval = d
		}
	}
	if v := os.Getenv("OUTBOX_RELAY_BATCH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("invalid OUTBOX_RELAY_BATCH: %q", v)
		}
		cfg.Batch = n
	}
	return cfg
}

// Relay publishes unpublished outbox rows to RabbitMQ.
type Relay struct {
	store Store
	pub   queue.Publisher
	cfg   Config
}

// New creates a relay instance.
func New(st Store, pub queue.Publisher, cfg Config) *Relay {
	return &Relay{store: st, pub: pub, cfg: cfg}
}

// Run executes relay ticks until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	if !r.cfg.Enabled {
		log.Printf("outbox relay disabled")
		return
	}
	log.Printf("outbox relay started (interval=%s, batch=%d)", r.cfg.Interval, r.cfg.Batch)

	if err := r.Tick(ctx); err != nil && ctx.Err() == nil {
		log.Printf("outbox relay tick: %v", err)
	}

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("outbox relay stopped")
			return
		case <-ticker.C:
			if err := r.Tick(ctx); err != nil && ctx.Err() == nil {
				log.Printf("outbox relay tick: %v", err)
			}
		}
	}
}

// Tick publishes one batch of outbox entries.
func (r *Relay) Tick(ctx context.Context) error {
	entries, tx, err := r.store.ClaimOutboxBatch(ctx, r.cfg.Batch)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		_ = tx.Rollback()
		return nil
	}

	var publishErr error
	for _, e := range entries {
		msg := queue.Message{JobID: e.JobID, Attempt: e.Attempt}
		if err := r.pub.Publish(ctx, e.QueueTarget, msg); err != nil {
			_ = r.store.MarkOutboxPublishError(ctx, e.ID, err)
			publishErr = err
			continue
		}
		if err := r.store.MarkOutboxPublishedTx(ctx, tx, e.ID); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return publishErr
}
