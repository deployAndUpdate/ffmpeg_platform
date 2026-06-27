//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go_distributed_system/internal/types"
)

// Client calls scheduler HTTP endpoints used by integration clients.
type Client struct {
	BaseURL string
	APIKey  string
	http    *http.Client
}

// NewClient creates an HTTP client for the scheduler base URL.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// CreateLocalJobRequest describes POST /jobs for local-path jobs.
type CreateLocalJobRequest struct {
	InputPath  string
	OutputPath string
	Preset     string
}

// CreateLocalJob submits POST /jobs with JSON body.
func (c *Client) CreateLocalJob(t *testing.T, req CreateLocalJobRequest) *types.Job {
	t.Helper()

	body, err := json.Marshal(map[string]string{
		"input_path":  req.InputPath,
		"output_path": req.OutputPath,
		"preset":      req.Preset,
	})
	if err != nil {
		t.Fatal(err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, c.BaseURL+"/jobs", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	c.setAuth(httpReq)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("POST /jobs: status %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	var job types.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	return &job
}

// GetJob fetches GET /jobs/{id}.
func (c *Client) GetJob(t *testing.T, jobID string) *types.Job {
	t.Helper()

	httpReq, err := http.NewRequest(http.MethodGet, c.BaseURL+"/jobs/"+jobID, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.setAuth(httpReq)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("GET /jobs/%s: status %d: %s", jobID, resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	return decodeJob(t, payload)
}

// GetJobLogs fetches GET /jobs/{id}/logs.
func (c *Client) GetJobLogs(t *testing.T, jobID string) []types.JobLog {
	t.Helper()

	httpReq, err := http.NewRequest(http.MethodGet, c.BaseURL+"/jobs/"+jobID+"/logs", nil)
	if err != nil {
		t.Fatal(err)
	}
	c.setAuth(httpReq)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("GET /jobs/%s/logs: status %d: %s", jobID, resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	var payload struct {
		JobID string          `json:"job_id"`
		Logs  []types.JobLog  `json:"logs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	return payload.Logs
}

// WaitForJob polls GET /jobs/{id} until the job reaches wantStatus or times out.
func (c *Client) WaitForJob(t *testing.T, stack *Stack, jobID string, wantStatus types.JobStatus, timeout time.Duration) *types.Job {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last *types.Job

	for time.Now().Before(deadline) {
		job := c.GetJob(t, jobID)
		last = job
		switch job.Status {
		case wantStatus:
			return job
		case types.JobStatusFailed:
			t.Fatalf("job %s failed after %d attempts", jobID, job.Attempt)
		}
		time.Sleep(pollInterval)
	}

	c.logDiagnostics(t, stack, jobID, last)
	t.Fatalf("timeout waiting for job %s status %s (last=%s)", jobID, wantStatus, statusOrUnknown(last))
	return nil
}

// WaitForJobCollectStatuses polls until COMPLETED and returns distinct statuses in order of first appearance.
func (c *Client) WaitForJobCollectStatuses(t *testing.T, stack *Stack, jobID string, timeout time.Duration) []types.JobStatus {
	t.Helper()

	deadline := time.Now().Add(timeout)
	seen := make([]types.JobStatus, 0, 4)
	var last *types.Job

	for time.Now().Before(deadline) {
		job := c.GetJob(t, jobID)
		last = job
		if len(seen) == 0 || seen[len(seen)-1] != job.Status {
			seen = append(seen, job.Status)
		}
		switch job.Status {
		case types.JobStatusCompleted:
			return seen
		case types.JobStatusFailed:
			t.Fatalf("job %s failed after %d attempts", jobID, job.Attempt)
		}
		time.Sleep(pollInterval)
	}

	c.logDiagnostics(t, stack, jobID, last)
	t.Fatalf("timeout waiting for job %s to complete (last=%s, seen=%v)", jobID, statusOrUnknown(last), seen)
	return nil
}

func (c *Client) setAuth(req *http.Request) {
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
}

func (c *Client) logDiagnostics(t *testing.T, stack *Stack, jobID string, last *types.Job) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if last != nil {
		t.Logf("last job status=%s attempt=%d worker=%v", last.Status, last.Attempt, last.AssignedWorkerID)
	}
	if n, err := stack.Store.CountUnpublishedOutbox(ctx); err == nil {
		t.Logf("unpublished outbox rows=%d", n)
	}
	if ready, err := stack.Rabbit.MessagesReady(); err == nil {
		t.Logf("rabbit messages ready=%d", ready)
	}
	if _, err := stack.Store.GetJob(ctx, jobID); err != nil {
		t.Logf("store get job: %v", err)
	}
}

func decodeJob(t *testing.T, payload map[string]any) *types.Job {
	t.Helper()

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var job types.Job
	if err := json.Unmarshal(raw, &job); err != nil {
		t.Fatal(err)
	}
	return &job
}

func statusOrUnknown(job *types.Job) string {
	if job == nil {
		return "unknown"
	}
	return string(job.Status)
}
