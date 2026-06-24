package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/types"
)

type recordingObjectStorage struct {
	mockObjectStorage
	uploadedKey  string
	uploadedSize int64
}

func (r *recordingObjectStorage) UploadReader(_ context.Context, key string, body io.Reader, size int64, _ string) error {
	r.uploadedKey = key
	r.uploadedSize = size
	_, err := io.Copy(io.Discard, body)
	return err
}

func TestCreateJobFromUploadSuccess(t *testing.T) {
	var captured *types.Job
	st := &mockStore{
		createJobFn: func(_ context.Context, job *types.Job) error {
			captured = job
			return nil
		},
	}
	obj := &recordingObjectStorage{mockObjectStorage: mockObjectStorage{bucket: "ffmpegfiles"}}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("ffmpeg_args", "-vn -acodec libmp3lame -b:a 192k")
	_ = writer.WriteField("output_ext", "mp3")
	part, err := writer.CreateFormFile("file", "clip.mp4")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("fake-video-bytes")
	if _, err := part.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(NewServerWithStorage(st, obj, storage.Config{}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/jobs", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	var out types.Job
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != types.JobStatusQueued {
		t.Fatalf("status = %q, want QUEUED", out.Status)
	}
	if out.Storage != types.StorageR2 {
		t.Fatalf("storage = %q, want r2", out.Storage)
	}
	if captured == nil || captured.ID != out.ID {
		t.Fatal("expected store.CreateJob with response id")
	}
	if obj.uploadedKey == "" || obj.uploadedSize != int64(len(payload)) {
		t.Fatalf("upload = key %q size %d", obj.uploadedKey, obj.uploadedSize)
	}
}

func TestCreateJobFromUploadRequiresStorage(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("ffmpeg_args", "-vn")
	_ = writer.WriteField("output_ext", "mp3")
	_ = writer.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/jobs", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}

func TestCreateJobFromUploadValidation(t *testing.T) {
	obj := &mockObjectStorage{bucket: "ffmpegfiles"}
	srv := newTestServerWithStorage(&mockStore{}, obj)
	defer srv.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("ffmpeg_args", "-vn")
	_ = writer.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/jobs", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestMaxUploadBytesFromEnvDefault(t *testing.T) {
	t.Setenv("MAX_UPLOAD_BYTES", "")
	if got := MaxUploadBytesFromEnv(); got != defaultMaxUploadBytes {
		t.Fatalf("got %d, want default %d", got, defaultMaxUploadBytes)
	}
}
