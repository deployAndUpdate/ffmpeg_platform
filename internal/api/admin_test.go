package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go_distributed_system/internal/api/auth"
	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/store"
	"go_distributed_system/internal/types"
)

func TestAdminStats(t *testing.T) {
	st := &mockStore{
		getAdminStatsFn: func(_ context.Context) (store.AdminStats, error) {
			return store.AdminStats{
				JobsByStatus:  map[string]int{"RUNNING": 2, "DISPATCHED": 5},
				WorkersActive: 3,
				WorkersDead:   1,
				QueueDepth:    5,
			}, nil
		},
	}
	s := NewServerWithStorageAndAuth(st, nil, storage.Config{}, auth.Config{
		Required: true,
		AdminKey: "admin-key",
	})

	req, _ := http.NewRequest(http.MethodGet, "/admin/api/stats", nil)
	req.Header.Set("Authorization", "Bearer admin-key")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var stats store.AdminStats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.WorkersActive != 3 || stats.QueueDepth != 5 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestAdminListJobs(t *testing.T) {
	jobID := "job-1"
	st := &mockStore{
		listJobsFn: func(_ context.Context, filter store.ListJobsFilter) (store.ListJobsResult, error) {
			if filter.Status == nil || *filter.Status != types.JobStatusRunning {
				t.Fatalf("expected RUNNING filter, got %+v", filter.Status)
			}
			return store.ListJobsResult{
				Jobs:  []types.Job{{ID: jobID, Status: types.JobStatusRunning}},
				Total: 1,
			}, nil
		},
	}
	s := NewServerWithStorageAndAuth(st, nil, storage.Config{}, auth.Config{
		Required: true,
		AdminKey: "admin-key",
	})

	req, _ := http.NewRequest(http.MethodGet, "/admin/api/jobs?status=RUNNING", nil)
	req.Header.Set("Authorization", "Bearer admin-key")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestAdminListWorkers(t *testing.T) {
	st := &mockStore{
		listWorkersFn: func(_ context.Context) ([]types.WorkerStats, error) {
			return []types.WorkerStats{{
				Worker:      types.Worker{ID: "w1", Hostname: "node-1", Status: types.WorkerStatusActive},
				RunningJobs: 1,
			}}, nil
		},
	}
	s := NewServerWithStorageAndAuth(st, nil, storage.Config{}, auth.Config{
		Required: true,
		AdminKey: "admin-key",
	})

	req, _ := http.NewRequest(http.MethodGet, "/admin/api/workers", nil)
	req.Header.Set("Authorization", "Bearer admin-key")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestAdminDashboardPublic(t *testing.T) {
	s := NewServer(&mockStore{})
	req, _ := http.NewRequest(http.MethodGet, "/admin/", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin UI status = %d", rec.Code)
	}
}
