package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// R2 implements ObjectStorage for Cloudflare R2.
type R2 struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
	uploadTTL     time.Duration
	downloadTTL   time.Duration
}

// NewR2 creates an R2 client from config.
func NewR2(cfg Config) (*R2, error) {
	if cfg.Bucket == "" || cfg.Endpoint == "" {
		return nil, fmt.Errorf("R2 bucket and endpoint are required")
	}

	awsCfg := aws.Config{
		Region: cfg.Region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)),
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	})

	uploadTTL := cfg.PresignUploadTTL
	if uploadTTL <= 0 {
		uploadTTL = defaultPresignUploadTTL
	}
	downloadTTL := cfg.PresignDownloadTTL
	if downloadTTL <= 0 {
		downloadTTL = defaultPresignDownloadTTL
	}

	return &R2{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		bucket:        cfg.Bucket,
		uploadTTL:     uploadTTL,
		downloadTTL:   downloadTTL,
	}, nil
}

func (r *R2) Bucket() string {
	return r.bucket
}

// HealthCheck verifies R2 bucket access (HeadBucket).
func (r *R2) HealthCheck(ctx context.Context) error {
	_, err := r.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(r.bucket),
	})
	if err != nil {
		return fmt.Errorf("head bucket %q: %w", r.bucket, err)
	}
	return nil
}

func (r *R2) InputObjectKey(jobID, ext string) string {
	return InputObjectKey(jobID, ext)
}

func (r *R2) OutputObjectKey(jobID, ext string) string {
	return OutputObjectKey(jobID, ext)
}

func (r *R2) PresignPut(ctx context.Context, key string, expiry time.Duration) (string, error) {
	if expiry <= 0 {
		expiry = r.uploadTTL
	}
	out, err := r.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("presign put %q: %w", key, err)
	}
	return out.URL, nil
}

func (r *R2) PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error) {
	if expiry <= 0 {
		expiry = r.downloadTTL
	}
	out, err := r.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("presign get %q: %w", key, err)
	}
	return out.URL, nil
}

func (r *R2) Exists(ctx context.Context, key string) (bool, error) {
	stat, err := r.StatObject(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return stat.Size > 0, nil
}

func (r *R2) StatObject(ctx context.Context, key string) (ObjectStat, error) {
	out, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return ObjectStat{}, fmt.Errorf("not found: %w", err)
		}
		return ObjectStat{}, fmt.Errorf("head object %q: %w", key, err)
	}
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return ObjectStat{Size: size}, nil
}

func (r *R2) Download(ctx context.Context, key, localPath string) error {
	out, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("get object %q: %w", key, err)
	}
	defer out.Body.Close()

	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return fmt.Errorf("create download dir: %w", err)
	}

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create local file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, out.Body); err != nil {
		return fmt.Errorf("write local file: %w", err)
	}
	return nil
}

func (r *R2) Upload(ctx context.Context, localPath, key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open local file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat local file: %w", err)
	}
	return r.UploadReader(ctx, key, f, info.Size(), "")
}

func (r *R2) UploadReader(ctx context.Context, key string, body io.Reader, size int64, contentType string) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if size > 0 {
		input.ContentLength = aws.Int64(size)
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}

	_, err := r.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("put object %q: %w", key, err)
	}
	return nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "NotFound") || strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "404")
}
