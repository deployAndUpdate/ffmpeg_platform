package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go_distributed_system/internal/types"
)

func TestClientRegister(t *testing.T) {
	var got types.Worker
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/workers/register" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(got)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	worker := &types.Worker{
		ID:              "00000000-0000-0000-0000-000000000001",
		Hostname:        "test-host",
		CPUCores:        4,
		MaxParallelJobs: 1,
		Status:          types.WorkerStatusActive,
	}
	if err := client.Register(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	if got.ID != worker.ID || got.Hostname != worker.Hostname {
		t.Fatalf("server received %+v, want id=%s hostname=%s", got, worker.ID, worker.Hostname)
	}
}

func TestClientRequestJobEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"job": nil})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	job, err := client.RequestJob(context.Background(), "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if job != nil {
		t.Fatalf("job = %+v, want nil", job)
	}
}

func TestClientRequestJobServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.RequestJob(context.Background(), "worker-1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClientSubmitJobResult(t *testing.T) {
	var payload map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/workers/job-result" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	logs := []types.JobLogEntry{{Stream: "stdout", Line: "done"}}
	if err := client.SubmitJobResult(context.Background(), "w1", "j1", true, logs); err != nil {
		t.Fatal(err)
	}
	if payload["worker_id"] != "w1" || payload["job_id"] != "j1" || payload["success"] != true {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}
