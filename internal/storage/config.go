package storage

import (
	"os"
	"time"
)

const (
	defaultRegion            = "auto"
	defaultPresignUploadTTL  = time.Hour
	defaultPresignDownloadTTL = time.Hour
)

// Config holds Cloudflare R2 (S3-compatible) settings.
type Config struct {
	AccessKeyID        string
	SecretAccessKey    string
	Bucket             string
	Endpoint           string
	Region             string
	PresignUploadTTL   time.Duration
	PresignDownloadTTL time.Duration
}

// ConfigFromEnv reads R2 settings. The second return value is false when R2 is not configured.
func ConfigFromEnv() (Config, bool) {
	bucket := os.Getenv("R2_BUCKET")
	accessKey := os.Getenv("R2_ACCESS_KEY_ID")
	secretKey := os.Getenv("R2_SECRET_ACCESS_KEY")
	if bucket == "" || accessKey == "" || secretKey == "" {
		return Config{}, false
	}

	endpoint := os.Getenv("R2_ENDPOINT")
	if endpoint == "" {
		return Config{}, false
	}

	region := os.Getenv("R2_REGION")
	if region == "" {
		region = defaultRegion
	}

	return Config{
		AccessKeyID:        accessKey,
		SecretAccessKey:    secretKey,
		Bucket:             bucket,
		Endpoint:           endpoint,
		Region:             region,
		PresignUploadTTL:   envDuration("R2_PRESIGN_UPLOAD_TTL", defaultPresignUploadTTL),
		PresignDownloadTTL: envDuration("R2_PRESIGN_DOWNLOAD_TTL", defaultPresignDownloadTTL),
	}, true
}

func envDuration(name string, fallback time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
