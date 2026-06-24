package reaper_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go_distributed_system/internal/reaper"
	"go_distributed_system/internal/store"
)

type mockStore struct {
	markCutoff time.Time
	reapNow    time.Time

	markErr error
	reapErr error

	markCalls int
	reapCalls int
}

func (m *mockStore) MarkStaleWorkersDead(_ context.Context, cutoff time.Time) (int64, error) {
	m.markCalls++
	m.markCutoff = cutoff
	if m.markErr != nil {
		return 0, m.markErr
	}
	return 2, nil
}

func (m *mockStore) ReapOrphanJobs(_ context.Context, now time.Time) (store.ReapResult, error) {
	m.reapCalls++
	m.reapNow = now
	if m.reapErr != nil {
		return store.ReapResult{}, m.reapErr
	}
	return store.ReapResult{Retried: 1, Failed: 1}, nil
}

func TestTick_CallsStoreMethods(t *testing.T) {
	st := &mockStore{}
	cfg := reaper.Config{
		Interval:      time.Minute,
		WorkerTimeout: 45 * time.Second,
		Enabled:       true,
	}
	r := reaper.New(st, cfg)

	before := time.Now().UTC()
	if err := r.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}

	if st.markCalls != 1 || st.reapCalls != 1 {
		t.Fatalf("calls: mark=%d reap=%d, want 1 each", st.markCalls, st.reapCalls)
	}

	wantCutoff := before.Add(-cfg.WorkerTimeout)
	if st.markCutoff.Before(wantCutoff.Add(-time.Second)) || st.markCutoff.After(wantCutoff.Add(time.Second)) {
		t.Fatalf("cutoff = %v, want ~%v", st.markCutoff, wantCutoff)
	}
	if st.reapNow.Before(before.Add(-time.Second)) || st.reapNow.After(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("reap now = %v out of range", st.reapNow)
	}
}

func TestTick_PropagatesMarkError(t *testing.T) {
	want := errors.New("mark failed")
	st := &mockStore{markErr: want}
	r := reaper.New(st, reaper.DefaultConfig())

	err := r.Tick(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if st.reapCalls != 0 {
		t.Fatalf("reap calls = %d, want 0", st.reapCalls)
	}
}

func TestTick_PropagatesReapError(t *testing.T) {
	want := errors.New("reap failed")
	st := &mockStore{reapErr: want}
	r := reaper.New(st, reaper.DefaultConfig())

	err := r.Tick(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	st := &mockStore{}
	cfg := reaper.Config{
		Interval:      20 * time.Millisecond,
		WorkerTimeout: time.Minute,
		Enabled:       true,
	}
	r := reaper.New(st, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancel")
	}

	if st.markCalls < 1 {
		t.Fatal("expected at least one tick before stop")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := reaper.DefaultConfig()
	if cfg.Interval != 30*time.Second {
		t.Fatalf("interval = %v", cfg.Interval)
	}
	if cfg.WorkerTimeout != 45*time.Second {
		t.Fatalf("worker timeout = %v", cfg.WorkerTimeout)
	}
	if !cfg.Enabled {
		t.Fatal("expected enabled by default")
	}
}
