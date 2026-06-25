package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go_distributed_system/internal/store"
	"go_distributed_system/internal/types"
)

type mockStore struct {
	createJobFn            func(ctx context.Context, job *types.Job) error
	queueJobFn             func(ctx context.Context, jobID string) error
	getJobFn               func(ctx context.Context, id string) (*types.Job, error)
	getJobByIdempotencyFn  func(ctx context.Context, key string) (*types.Job, error)
	getJobLogsFn           func(ctx context.Context, jobID string) ([]types.JobLog, error)
	registerWorkerFn       func(ctx context.Context, w *types.Worker) error
	heartbeatFn            func(ctx context.Context, workerID string, ts time.Time) error
	acquireJobFn           func(ctx context.Context, workerID string, lease time.Duration) (*types.Job, error)
	renewLeaseFn           func(ctx context.Context, jobID, workerID string, leaseGeneration int64, lease time.Duration) (*types.Job, error)
	finishJobFn            func(ctx context.Context, jobID, workerID string, leaseGeneration int64, success bool, logs []types.JobLogEntry) error
	listJobsFn             func(ctx context.Context, filter store.ListJobsFilter) (store.ListJobsResult, error)
	listWorkersFn          func(ctx context.Context) ([]types.WorkerStats, error)
	getWorkerFn            func(ctx context.Context, id string) (*types.WorkerStats, error)
	getAdminStatsFn        func(ctx context.Context) (store.AdminStats, error)
}

func (m *mockStore) CreateJob(ctx context.Context, job *types.Job) error {
	if m.createJobFn != nil {
		return m.createJobFn(ctx, job)
	}
	return nil
}

func (m *mockStore) QueueJob(ctx context.Context, jobID string) error {
	if m.queueJobFn != nil {
		return m.queueJobFn(ctx, jobID)
	}
	return nil
}

func (m *mockStore) GetJob(ctx context.Context, id string) (*types.Job, error) {
	if m.getJobFn != nil {
		return m.getJobFn(ctx, id)
	}
	return nil, sql.ErrNoRows
}

func (m *mockStore) GetJobLogs(ctx context.Context, jobID string) ([]types.JobLog, error) {
	if m.getJobLogsFn != nil {
		return m.getJobLogsFn(ctx, jobID)
	}
	return nil, nil
}

func (m *mockStore) RegisterWorker(ctx context.Context, w *types.Worker) error {
	if m.registerWorkerFn != nil {
		return m.registerWorkerFn(ctx, w)
	}
	return nil
}

func (m *mockStore) Heartbeat(ctx context.Context, workerID string, ts time.Time) error {
	if m.heartbeatFn != nil {
		return m.heartbeatFn(ctx, workerID, ts)
	}
	return nil
}

func (m *mockStore) AcquireJob(ctx context.Context, workerID string, lease time.Duration) (*types.Job, error) {
	if m.acquireJobFn != nil {
		return m.acquireJobFn(ctx, workerID, lease)
	}
	return nil, nil
}

func (m *mockStore) GetJobByIdempotencyKey(ctx context.Context, key string) (*types.Job, error) {
	if m.getJobByIdempotencyFn != nil {
		return m.getJobByIdempotencyFn(ctx, key)
	}
	return nil, sql.ErrNoRows
}

func (m *mockStore) RenewLease(ctx context.Context, jobID, workerID string, leaseGeneration int64, lease time.Duration) (*types.Job, error) {
	if m.renewLeaseFn != nil {
		return m.renewLeaseFn(ctx, jobID, workerID, leaseGeneration, lease)
	}
	return nil, store.ErrLeaseLost
}

func (m *mockStore) FinishJob(ctx context.Context, jobID, workerID string, leaseGeneration int64, success bool, logs []types.JobLogEntry) error {
	if m.finishJobFn != nil {
		return m.finishJobFn(ctx, jobID, workerID, leaseGeneration, success, logs)
	}
	return nil
}

func (m *mockStore) ListJobs(ctx context.Context, filter store.ListJobsFilter) (store.ListJobsResult, error) {
	if m.listJobsFn != nil {
		return m.listJobsFn(ctx, filter)
	}
	return store.ListJobsResult{}, nil
}

func (m *mockStore) ListWorkers(ctx context.Context) ([]types.WorkerStats, error) {
	if m.listWorkersFn != nil {
		return m.listWorkersFn(ctx)
	}
	return nil, nil
}

func (m *mockStore) GetWorker(ctx context.Context, id string) (*types.WorkerStats, error) {
	if m.getWorkerFn != nil {
		return m.getWorkerFn(ctx, id)
	}
	return nil, sql.ErrNoRows
}

func (m *mockStore) GetAdminStats(ctx context.Context) (store.AdminStats, error) {
	if m.getAdminStatsFn != nil {
		return m.getAdminStatsFn(ctx)
	}
	return store.AdminStats{JobsByStatus: map[string]int{}}, nil
}

func newTestServer(st JobStore) *httptest.Server {
	return httptest.NewServer(NewServer(st))
}

func jsonPost(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestHealth(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestCreateJobValidation(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()

	tests := []struct {
		name       string
		method     string
		body       string
		wantStatus int
	}{
		{"method not allowed", http.MethodGet, "", http.StatusMethodNotAllowed},
		{"invalid json", http.MethodPost, "{", http.StatusBadRequest},
		{"missing fields", http.MethodPost, `{}`, http.StatusBadRequest},
		{"missing transcode spec", http.MethodPost, `{"input_path":"/in","output_path":"/out"}`, http.StatusBadRequest},
		{"both preset and ffmpeg_args", http.MethodPost, `{"input_path":"/in","output_path":"/out.mp4","preset":"h264_crf23","ffmpeg_args":"-c copy"}`, http.StatusBadRequest},
		{"unknown preset", http.MethodPost, `{"input_path":"/in","output_path":"/out.mp4","preset":"nope"}`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, srv.URL+"/jobs", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestCreateJobWithPreset(t *testing.T) {
	var captured *types.Job
	st := &mockStore{
		createJobFn: func(_ context.Context, job *types.Job) error {
			captured = job
			job.CreatedAt = time.Now().UTC()
			job.UpdatedAt = job.CreatedAt
			return nil
		},
	}
	srv := newTestServer(st)
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/jobs", map[string]any{
		"input_path":  "/data/in.mp4",
		"output_path": "/data/out.mp4",
		"preset":      "h264_crf23",
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var out types.Job
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Preset != "h264_crf23" {
		t.Fatalf("preset = %q", out.Preset)
	}
	if out.FFmpegArgs != "-c:v libx264 -crf 23 -preset medium" {
		t.Fatalf("ffmpeg_args = %q", out.FFmpegArgs)
	}
	if captured == nil || captured.Preset != "h264_crf23" {
		t.Fatalf("captured = %+v", captured)
	}
}

func TestListPresets(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/presets")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var out struct {
		Presets []struct {
			ID          string   `json:"id"`
			Description string   `json:"description"`
			OutputExts  []string `json:"output_exts"`
		} `json:"presets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Presets) == 0 {
		t.Fatal("expected presets")
	}
}

func TestCreateJobSuccess(t *testing.T) {
	var captured *types.Job
	st := &mockStore{
		createJobFn: func(_ context.Context, job *types.Job) error {
			captured = job
			job.CreatedAt = time.Now().UTC()
			job.UpdatedAt = job.CreatedAt
			return nil
		},
	}
	srv := newTestServer(st)
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/jobs", map[string]any{
		"input_path":   "/data/in.mp4",
		"output_path":  "/data/out.mp4",
		"ffmpeg_args":  "-c:v libx264",
		"max_attempts": 5,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var out types.Job
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != types.JobStatusQueued {
		t.Fatalf("status = %q, want QUEUED", out.Status)
	}
	if out.MaxAttempts != 5 {
		t.Fatalf("max_attempts = %d, want 5", out.MaxAttempts)
	}
	if captured == nil || captured.ID != out.ID {
		t.Fatal("store.CreateJob was not called with response id")
	}
}

func TestGetJobNotFound(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/jobs/00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestWorkerRegisterValidation(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/workers/register", map[string]any{
		"id":       "",
		"hostname": "host",
		"cpu_cores": 4,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestWorkerHeartbeatNotFound(t *testing.T) {
	st := &mockStore{
		heartbeatFn: func(context.Context, string, time.Time) error {
			return sql.ErrNoRows
		},
	}
	srv := newTestServer(st)
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/workers/heartbeat", map[string]string{
		"id": "00000000-0000-0000-0000-000000000001",
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestWorkerRequestJobValidation(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/workers/request-job", map[string]string{})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestWorkerJobResultConflict(t *testing.T) {
	st := &mockStore{
		finishJobFn: func(context.Context, string, string, int64, bool, []types.JobLogEntry) error {
			return store.ErrJobNotAssigned
		},
	}
	srv := newTestServer(st)
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/workers/job-result", map[string]any{
		"worker_id":         "00000000-0000-0000-0000-000000000001",
		"job_id":            "00000000-0000-0000-0000-000000000002",
		"lease_generation":  1,
		"success":           true,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
}

func TestWorkerJobResultStoreError(t *testing.T) {
	st := &mockStore{
		finishJobFn: func(context.Context, string, string, int64, bool, []types.JobLogEntry) error {
			return errors.New("db down")
		},
	}
	srv := newTestServer(st)
	defer srv.Close()

	resp := jsonPost(t, srv.URL+"/workers/job-result", map[string]any{
		"worker_id":         "00000000-0000-0000-0000-000000000001",
		"job_id":            "00000000-0000-0000-0000-000000000002",
		"lease_generation":  1,
		"success":           false,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}
