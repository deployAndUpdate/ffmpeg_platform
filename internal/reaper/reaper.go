package reaper

import (
	"context"
	"log"
	"os"
	"time"

	"go_distributed_system/internal/store"
)

// Store abstracts reaper persistence operations.
type Store interface {
	MarkStaleWorkersDead(ctx context.Context, cutoff time.Time) (int64, error)
	ReapOrphanJobs(ctx context.Context, now time.Time) (store.ReapResult, error)
}

// Config holds reaper timing settings.
type Config struct {
	Interval      time.Duration
	WorkerTimeout time.Duration
	Enabled       bool
}

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{
		Interval:      30 * time.Second,
		WorkerTimeout: 45 * time.Second,
		Enabled:       true,
	}
}

// ConfigFromEnv reads REAPER_* environment variables.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()
	if v := os.Getenv("REAPER_ENABLED"); v == "false" || v == "0" {
		cfg.Enabled = false
	}
	if v := os.Getenv("REAPER_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err != nil {
			log.Fatalf("invalid REAPER_INTERVAL: %q", v)
		} else {
			cfg.Interval = d
		}
	}
	if v := os.Getenv("REAPER_WORKER_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err != nil {
			log.Fatalf("invalid REAPER_WORKER_TIMEOUT: %q", v)
		} else {
			cfg.WorkerTimeout = d
		}
	}
	return cfg
}

// Reaper periodically marks stale workers DEAD and reclaims orphan RUNNING jobs.
type Reaper struct {
	store Store
	cfg   Config
}

// New creates a reaper instance.
func New(st Store, cfg Config) *Reaper {
	return &Reaper{store: st, cfg: cfg}
}

// Run executes reaper ticks until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	if !r.cfg.Enabled {
		log.Printf("reaper disabled")
		return
	}

	log.Printf("reaper started (interval=%s, worker_timeout=%s)", r.cfg.Interval, r.cfg.WorkerTimeout)

	if err := r.Tick(ctx); err != nil && ctx.Err() == nil {
		log.Printf("reaper tick: %v", err)
	}

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("reaper stopped")
			return
		case <-ticker.C:
			if err := r.Tick(ctx); err != nil && ctx.Err() == nil {
				log.Printf("reaper tick: %v", err)
			}
		}
	}
}

// Tick performs one reaper pass.
func (r *Reaper) Tick(ctx context.Context) error {
	now := time.Now().UTC()
	cutoff := now.Add(-r.cfg.WorkerTimeout)

	deadWorkers, err := r.store.MarkStaleWorkersDead(ctx, cutoff)
	if err != nil {
		return err
	}

	reaped, err := r.store.ReapOrphanJobs(ctx, now)
	if err != nil {
		return err
	}

	if deadWorkers > 0 || reaped.Retried > 0 || reaped.Failed > 0 {
		log.Printf("reaper: workers_marked_dead=%d jobs_retried=%d jobs_failed=%d",
			deadWorkers, reaped.Retried, reaped.Failed)
	}
	return nil
}
