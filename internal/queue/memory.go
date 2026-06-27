package queue

import (
	"context"
	"sync"
)

// MemoryPublisher is an in-memory Publisher for unit tests.
type MemoryPublisher struct {
	mu       sync.Mutex
	messages []published
	closed   bool
}

type published struct {
	Target Target
	Msg    Message
}

// NewMemoryPublisher creates a test double publisher.
func NewMemoryPublisher() *MemoryPublisher {
	return &MemoryPublisher{}
}

func (m *MemoryPublisher) Ping(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return context.Canceled
	}
	return nil
}

func (m *MemoryPublisher) Publish(_ context.Context, target Target, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return context.Canceled
	}
	m.messages = append(m.messages, published{Target: target, Msg: msg})
	return nil
}

func (m *MemoryPublisher) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// Published returns a copy of all published messages.
func (m *MemoryPublisher) Published() []published {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]published, len(m.messages))
	copy(out, m.messages)
	return out
}

// PublishCount returns how many messages were published.
func (m *MemoryPublisher) PublishCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}
