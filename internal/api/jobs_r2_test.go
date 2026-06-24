package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/types"
)

type mockObjectStorage struct {
	bucket      string
	existsFn    func(ctx context.Context, key string) (bool, error)
	presignPut  func(ctx context.Context, key string, expiry time.Duration) (string, error)
	presignGet  func(ctx context.Context, key string, expiry time.Duration) (string, error)
}

func (m *mockObjectStorage) Bucket() string { return m.bucket }
func (m *mockObjectStorage) InputObjectKey(jobID, ext string) string {
	return storage.InputObjectKey(jobID, ext)
}
func (m *mockObjectStorage) OutputObjectKey(jobID, ext string) string {
	return storage.OutputObjectKey(jobID, ext)
}
func (m *mockObjectStorage) PresignPut(ctx context.Context, key string, expiry time.Duration) (string, error) {
	if m.presignPut != nil {
		return m.presignPut(ctx, key, expiry)
	}
	return "https://example.com/upload/" + key, nil
}
func (m *mockObjectStorage) PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error) {
	if m.presignGet != nil {
		return m.presignGet(ctx, key, expiry)
	}
	return "https://example.com/download/" + key, nil
}
func (m *mockObjectStorage) Exists(ctx context.Context, key string) (bool, error) {
	if m.existsFn != nil {
		return m.existsFn(ctx, key)
	}
	return true, nil
}
func (m *mockObjectStorage) Download(context.Context, string, string) error { return nil }
func (m *mockObjectStorage) Upload(context.Context, string, string) error   { return nil }

func newTestServerWithStorage(st JobStore, obj storage.ObjectStorage) *httptest.Server {
	return httptest.NewServer(NewServerWithStorage(st, obj, storage.Config{
		PresignUploadTTL:   time.Hour,
		PresignDownloadTTL: time.Hour,
	}))
}

func TestJobsInitRequiresStorage(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/jobs/init", map[string]any{
		"ffmpeg_args":     "-vn",
		"input_filename":  "in.mp4",
		"output_ext":      "mp3",
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}

func TestJobsInitSuccess(t *testing.T) {
	var captured *types.Job
	st := &mockStore{
		createJobFn: func(_ context.Context, job *types.Job) error {
			captured = job
			return nil
		},
	}
	obj := &mockObjectStorage{bucket: "video-jobs"}
	srv := newTestServerWithStorage(st, obj)
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/jobs/init", map[string]any{
		"ffmpeg_args":     "-vn -acodec libmp3lame -b:a 192k",
		"input_filename":  "clip.mp4",
		"output_ext":      "mp3",
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var out struct {
		Job       types.Job `json:"job"`
		UploadURL string    `json:"upload_url"`
		Bucket    string    `json:"bucket"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Job.Status != types.JobStatusNew {
		t.Fatalf("status = %q, want NEW", out.Job.Status)
	}
	if out.Job.Storage != types.StorageR2 {
		t.Fatalf("storage = %q, want r2", out.Job.Storage)
	}
	if out.UploadURL == "" || out.Bucket != "video-jobs" {
		t.Fatalf("unexpected init response: %+v", out)
	}
	if captured == nil || !strings.HasPrefix(captured.InputPath, "jobs/") {
		t.Fatalf("unexpected captured job: %+v", captured)
	}
}

func TestJobSubmitRequiresUpload(t *testing.T) {
	jobID := "00000000-0000-0000-0000-000000000099"
	st := &mockStore{
		getJobFn: func(_ context.Context, id string) (*types.Job, error) {
			return &types.Job{
				ID:         jobID,
				InputPath:  "jobs/" + jobID + "/input.mp4",
				OutputPath: "jobs/" + jobID + "/output.mp3",
				Storage:    types.StorageR2,
				Status:     types.JobStatusNew,
			}, nil
		},
	}
	obj := &mockObjectStorage{
		bucket: "video-jobs",
		existsFn: func(context.Context, string) (bool, error) {
			return false, nil
		},
	}
	srv := newTestServerWithStorage(st, obj)
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/jobs/"+jobID+"/submit", map[string]any{})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestGetJobDownloadURL(t *testing.T) {
	jobID := "00000000-0000-0000-0000-000000000088"
	st := &mockStore{
		getJobFn: func(_ context.Context, id string) (*types.Job, error) {
			return &types.Job{
				ID:         jobID,
				InputPath:  "jobs/" + jobID + "/input.mp4",
				OutputPath: "jobs/" + jobID + "/output.mp3",
				Storage:    types.StorageR2,
				Status:     types.JobStatusCompleted,
			}, nil
		},
	}
	obj := &mockObjectStorage{
		bucket: "video-jobs",
		presignGet: func(_ context.Context, key string, _ time.Duration) (string, error) {
			return "https://example.com/get/" + key, nil
		},
	}
	srv := newTestServerWithStorage(st, obj)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/jobs/" + jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["download_url"] == nil {
		t.Fatal("expected download_url in completed R2 job response")
	}
}

func TestQueueJobIntegrationShape(t *testing.T) {
	st := &mockStore{
		queueJobFn: func(_ context.Context, jobID string) error {
			if jobID == "" {
				return errors.New("missing id")
			}
			return nil
		},
	}
	obj := &mockObjectStorage{bucket: "video-jobs"}
	srv := newTestServerWithStorage(st, obj)
	defer srv.Close()

	jobID := "00000000-0000-0000-0000-000000000077"
	st.getJobFn = func(_ context.Context, id string) (*types.Job, error) {
		if id == jobID {
			return &types.Job{
				ID:        jobID,
				InputPath: "jobs/" + jobID + "/input.mp4",
				Storage:   types.StorageR2,
				Status:    types.JobStatusNew,
			}, nil
		}
		return &types.Job{
			ID:      jobID,
			Storage: types.StorageR2,
			Status:  types.JobStatusQueued,
		}, nil
	}

	resp := jsonPost(t, srv.URL+"/jobs/"+jobID+"/submit", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}
