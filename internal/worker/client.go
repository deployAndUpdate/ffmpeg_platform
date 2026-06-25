package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go_distributed_system/internal/types"
)

// Client talks to the scheduler HTTP API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a scheduler API client without authentication.
func NewClient(baseURL string) *Client {
	return NewClientWithAPIKey(baseURL, "")
}

// NewClientWithAPIKey creates a scheduler API client that sends a worker API key.
func NewClientWithAPIKey(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) Register(ctx context.Context, w *types.Worker) error {
	return c.post(ctx, "/workers/register", w, w)
}

func (c *Client) Heartbeat(ctx context.Context, workerID string) error {
	body := map[string]string{"id": workerID}
	var out map[string]string
	return c.post(ctx, "/workers/heartbeat", body, &out)
}

func (c *Client) RequestJob(ctx context.Context, workerID string) (*types.Job, error) {
	body := map[string]string{"worker_id": workerID}
	var resp struct {
		Job *types.Job `json:"job"`
	}
	if err := c.post(ctx, "/workers/request-job", body, &resp); err != nil {
		return nil, err
	}
	return resp.Job, nil
}

func (c *Client) RenewLease(ctx context.Context, workerID, jobID string, leaseGeneration int64) error {
	body := map[string]any{
		"worker_id":         workerID,
		"job_id":            jobID,
		"lease_generation":  leaseGeneration,
	}
	var resp struct {
		Job *types.Job `json:"job"`
	}
	return c.post(ctx, "/workers/renew-lease", body, &resp)
}

func (c *Client) SubmitJobResult(ctx context.Context, workerID, jobID string, leaseGeneration int64, success bool, logs []types.JobLogEntry) error {
	body := map[string]any{
		"worker_id":         workerID,
		"job_id":            jobID,
		"lease_generation":  leaseGeneration,
		"success":           success,
		"logs":              logs,
	}
	var out map[string]string
	return c.post(ctx, "/workers/job-result", body, &out)
}

func (c *Client) post(ctx context.Context, path string, reqBody, respBody any) error {
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http post %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("http post %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	if respBody != nil {
		if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
