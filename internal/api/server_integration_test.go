//go:build integration

package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go_distributed_system/internal/api"
	"go_distributed_system/internal/testutil"
	"go_distributed_system/internal/types"

	"github.com/google/uuid"
)

func TestJobLifecycleSmoke(t *testing.T) {
	st, cleanup := testutil.OpenStore(t)
	defer cleanup()

	srv := httptest.NewServer(api.NewServer(st))
	defer srv.Close()

	workerID := uuid.New().String()

	createResp := postJSON(t, srv.URL+"/jobs", map[string]any{
		"input_path":  "/data/in.mp4",
		"output_path": "/data/out.mp4",
		"preset":      "h264_crf23",
	})
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("create job: status=%d body=%s", createResp.StatusCode, body)
	}
	var created types.Job
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}

	regResp := postJSON(t, srv.URL+"/workers/register", map[string]any{
		"id":                workerID,
		"hostname":          "smoke-test",
		"cpu_cores":         4,
		"max_parallel_jobs": 1,
	})
	regResp.Body.Close()
	if regResp.StatusCode != http.StatusOK {
		t.Fatalf("register worker: status=%d", regResp.StatusCode)
	}

	reqJobResp := postJSON(t, srv.URL+"/workers/request-job", map[string]string{
		"worker_id": workerID,
	})
	defer reqJobResp.Body.Close()
	if reqJobResp.StatusCode != http.StatusOK {
		t.Fatalf("request job: status=%d", reqJobResp.StatusCode)
	}
	var reqJobOut struct {
		Job *types.Job `json:"job"`
	}
	if err := json.NewDecoder(reqJobResp.Body).Decode(&reqJobOut); err != nil {
		t.Fatal(err)
	}
	if reqJobOut.Job == nil || reqJobOut.Job.ID != created.ID {
		t.Fatalf("acquired job = %+v, want id=%s", reqJobOut.Job, created.ID)
	}
	if reqJobOut.Job.Status != types.JobStatusRunning {
		t.Fatalf("acquired status = %q, want RUNNING", reqJobOut.Job.Status)
	}

	resultResp := postJSON(t, srv.URL+"/workers/job-result", map[string]any{
		"worker_id": workerID,
		"job_id":    created.ID,
		"success":   true,
		"logs": []types.JobLogEntry{
			{Stream: "stdout", Line: "encoded successfully"},
		},
	})
	resultResp.Body.Close()
	if resultResp.StatusCode != http.StatusOK {
		t.Fatalf("job result: status=%d", resultResp.StatusCode)
	}

	getResp, err := http.Get(srv.URL + "/jobs/" + created.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get job: status=%d", getResp.StatusCode)
	}
	var final types.Job
	if err := json.NewDecoder(getResp.Body).Decode(&final); err != nil {
		t.Fatal(err)
	}
	if final.Status != types.JobStatusCompleted {
		t.Fatalf("final status = %q, want COMPLETED", final.Status)
	}

	logsResp, err := http.Get(srv.URL + "/jobs/" + created.ID + "/logs")
	if err != nil {
		t.Fatal(err)
	}
	defer logsResp.Body.Close()
	if logsResp.StatusCode != http.StatusOK {
		t.Fatalf("get logs: status=%d", logsResp.StatusCode)
	}
	var logsOut struct {
		JobID string          `json:"job_id"`
		Logs  []types.JobLog  `json:"logs"`
	}
	if err := json.NewDecoder(logsResp.Body).Decode(&logsOut); err != nil {
		t.Fatal(err)
	}
	if logsOut.JobID != created.ID {
		t.Fatalf("logs job_id = %q, want %q", logsOut.JobID, created.ID)
	}
	if len(logsOut.Logs) != 1 || logsOut.Logs[0].Line != "encoded successfully" {
		t.Fatalf("unexpected logs: %+v", logsOut.Logs)
	}

	// Heartbeat path on real store
	hbResp := postJSON(t, srv.URL+"/workers/heartbeat", map[string]string{"id": workerID})
	hbResp.Body.Close()
	if hbResp.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat: status=%d", hbResp.StatusCode)
	}
}

func postJSON(t *testing.T, url string, body any) *http.Response {
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
