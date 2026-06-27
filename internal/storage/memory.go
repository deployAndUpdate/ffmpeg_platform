package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"time"
)

// Memory is an in-memory ObjectStorage for tests and local development.
type Memory struct {
	mu     sync.RWMutex
	bucket string
	objects map[string][]byte
}

// NewMemory creates an empty in-memory object store.
func NewMemory(bucket string) *Memory {
	if bucket == "" {
		bucket = "memory"
	}
	return &Memory{bucket: bucket, objects: make(map[string][]byte)}
}

func (m *Memory) HealthCheck(context.Context) error { return nil }

func (m *Memory) Bucket() string { return m.bucket }

func (m *Memory) InputObjectKey(jobID, ext string) string  { return InputObjectKey(jobID, ext) }
func (m *Memory) OutputObjectKey(jobID, ext string) string { return OutputObjectKey(jobID, ext) }

func (m *Memory) PresignPut(context.Context, string, time.Duration) (string, error) {
	return "https://memory.example/upload", nil
}

func (m *Memory) PresignGet(context.Context, string, time.Duration) (string, error) {
	return "https://memory.example/download", nil
}

func (m *Memory) Exists(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.objects[key]
	return ok, nil
}

func (m *Memory) StatObject(_ context.Context, key string) (ObjectStat, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	body, ok := m.objects[key]
	if !ok {
		return ObjectStat{}, errNotFound(key)
	}
	return ObjectStat{Size: int64(len(body))}, nil
}

func (m *Memory) Download(_ context.Context, key, localPath string) error {
	body, err := m.read(key)
	if err != nil {
		return err
	}
	return os.WriteFile(localPath, body, 0o644)
}

func (m *Memory) OpenObject(_ context.Context, key string) (io.ReadCloser, error) {
	body, err := m.read(key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (m *Memory) Upload(_ context.Context, localPath, key string) error {
	body, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	m.write(key, body)
	return nil
}

func (m *Memory) UploadReader(_ context.Context, key string, r io.Reader, _ int64, _ string) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.write(key, body)
	return nil
}

func (m *Memory) read(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	body, ok := m.objects[key]
	if !ok {
		return nil, errNotFound(key)
	}
	out := make([]byte, len(body))
	copy(out, body)
	return out, nil
}

func (m *Memory) write(key string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dup := make([]byte, len(body))
	copy(dup, body)
	m.objects[key] = dup
}

func errNotFound(key string) error {
	return &notFoundError{key: key}
}

type notFoundError struct{ key string }

func (e *notFoundError) Error() string { return "not found: " + e.key }
