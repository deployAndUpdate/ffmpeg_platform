package api

import (
	"os"
	"strconv"
	"time"
)

const (
	defaultMaxUploadBytes  int64 = 512 << 20 // 512 MiB
	defaultUploadTimeout         = 30 * time.Minute
)

// MaxUploadBytesFromEnv returns the HTTP upload size limit for multipart job requests.
func MaxUploadBytesFromEnv() int64 {
	v := os.Getenv("MAX_UPLOAD_BYTES")
	if v == "" {
		return defaultMaxUploadBytes
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return defaultMaxUploadBytes
	}
	return n
}

// UploadTimeoutFromEnv returns the per-request timeout for multipart uploads.
func UploadTimeoutFromEnv() time.Duration {
	v := os.Getenv("SCHEDULER_UPLOAD_TIMEOUT")
	if v == "" {
		return defaultUploadTimeout
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return defaultUploadTimeout
	}
	return d
}
