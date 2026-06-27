package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"go_distributed_system/internal/outbox"
	"go_distributed_system/internal/queue"
	"go_distributed_system/internal/store"
)

type mockOutboxStore struct {
	entries []store.OutboxEntry
	marked  []int64
	errors  map[int64]string
}

func (m *mockOutboxStore) ClaimOutboxBatch(_ context.Context, limit int) ([]store.OutboxEntry, store.OutboxTx, error) {
	if limit <= 0 {
		limit = len(m.entries)
	}
	if len(m.entries) == 0 {
		return nil, &fakeTx{}, nil
	}
	n := limit
	if n > len(m.entries) {
		n = len(m.entries)
	}
	batch := append([]store.OutboxEntry(nil), m.entries[:n]...)
	return batch, &fakeTx{}, nil
}

func (m *mockOutboxStore) MarkOutboxPublishedTx(_ context.Context, _ store.OutboxTx, id int64) error {
	m.marked = append(m.marked, id)
	return nil
}

func (m *mockOutboxStore) MarkOutboxPublishError(_ context.Context, id int64, err error) error {
	if m.errors == nil {
		m.errors = map[int64]string{}
	}
	m.errors[id] = err.Error()
	return nil
}

type fakeTx struct{}

func (f *fakeTx) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return fakeResult(1), nil
}
func (f *fakeTx) Commit() error   { return nil }
func (f *fakeTx) Rollback() error { return nil }

type fakeResult int64

func (f fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (f fakeResult) RowsAffected() (int64, error) { return int64(f), nil }

func TestRelayTickPublishesBatch(t *testing.T) {
	mem := queue.NewMemoryPublisher()
	st := &mockOutboxStore{
		entries: []store.OutboxEntry{
			{ID: 1, JobID: "job-a", QueueTarget: queue.TargetMain, Attempt: 0},
			{ID: 2, JobID: "job-b", QueueTarget: queue.TargetRetry, Attempt: 1},
		},
	}

	relay := outbox.New(st, mem, outbox.Config{Batch: 10, Enabled: true})
	if err := relay.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if mem.PublishCount() != 2 {
		t.Fatalf("publish count = %d, want 2", mem.PublishCount())
	}
	if len(st.marked) != 2 {
		t.Fatalf("marked = %v, want 2 ids", st.marked)
	}
}

func TestRelayTickRecordsPublishError(t *testing.T) {
	failPub := &failPublisher{}
	st := &mockOutboxStore{
		entries: []store.OutboxEntry{
			{ID: 7, JobID: "job-x", QueueTarget: queue.TargetMain, Attempt: 0},
		},
	}
	relay := outbox.New(st, failPub, outbox.Config{Batch: 10, Enabled: true})
	if err := relay.Tick(context.Background()); err == nil {
		t.Fatal("expected publish error")
	}
	if st.errors[7] == "" {
		t.Fatal("expected publish error recorded on outbox row")
	}
}

type failPublisher struct{}

func (f *failPublisher) Ping(context.Context) error { return nil }
func (f *failPublisher) Publish(context.Context, queue.Target, queue.Message) error {
	return errors.New("publish failed")
}
func (f *failPublisher) Close() error { return nil }
