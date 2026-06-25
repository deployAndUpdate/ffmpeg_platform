package storage

import (
	"context"
	"fmt"
	"io"
	"time"
)

// ObjectStat holds object metadata from a HEAD request.
type ObjectStat struct {
	Size int64
}

type ObjectStorage interface {
	HealthCheck(ctx context.Context) error
	Bucket() string
	InputObjectKey(jobID, ext string) string
	OutputObjectKey(jobID, ext string) string
	PresignPut(ctx context.Context, key string, expiry time.Duration) (string, error)
	PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error)
	Exists(ctx context.Context, key string) (bool, error)
	StatObject(ctx context.Context, key string) (ObjectStat, error)
	Download(ctx context.Context, key, localPath string) error
	Upload(ctx context.Context, localPath, key string) error
	UploadReader(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
}

// NewFromEnv creates an R2 client when env vars are set.
func NewFromEnv() (ObjectStorage, error) {
	cfg, ok := ConfigFromEnv()
	if !ok {
		return nil, fmt.Errorf("R2 is not configured (need R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT)")
	}
	return NewR2(cfg)
}
