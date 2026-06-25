package api

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go_distributed_system/internal/store"
	"go_distributed_system/internal/types"
)

func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/api/")
	if path == "" || path == r.URL.Path {
		http.NotFound(w, r)
		return
	}

	switch {
	case path == "stats" && r.Method == http.MethodGet:
		s.adminStats(w, r)
	case path == "jobs" && r.Method == http.MethodGet:
		s.adminListJobs(w, r)
	case path == "workers" && r.Method == http.MethodGet:
		s.adminListWorkers(w, r)
	default:
		s.handleAdminWorkerByID(w, r, path)
	}
}

func (s *Server) handleAdminWorkerByID(w http.ResponseWriter, r *http.Request, path string) {
	if !strings.HasPrefix(path, "workers/") {
		http.NotFound(w, r)
		return
	}
	rest := strings.TrimPrefix(path, "workers/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	workerID := parts[0]

	if len(parts) == 2 && parts[1] == "jobs" && r.Method == http.MethodGet {
		s.adminWorkerJobs(w, r, workerID)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.adminGetWorker(w, r, workerID)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) adminStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	stats, err := s.store.GetAdminStats(ctx)
	if err != nil {
		log.Printf("admin stats: %v", err)
		http.Error(w, "failed to load stats", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) adminListJobs(w http.ResponseWriter, r *http.Request) {
	filter := store.ListJobsFilter{
		Limit:  parseIntQuery(r, "limit", 50),
		Offset: parseIntQuery(r, "offset", 0),
	}
	if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
		st := types.JobStatus(status)
		filter.Status = &st
	}
	if workerID := strings.TrimSpace(r.URL.Query().Get("worker_id")); workerID != "" {
		filter.WorkerID = &workerID
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	result, err := s.store.ListJobs(ctx, filter)
	if err != nil {
		log.Printf("admin list jobs: %v", err)
		http.Error(w, "failed to list jobs", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) adminListWorkers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	workers, err := s.store.ListWorkers(ctx)
	if err != nil {
		log.Printf("admin list workers: %v", err)
		http.Error(w, "failed to list workers", http.StatusInternalServerError)
		return
	}
	if workers == nil {
		workers = []types.WorkerStats{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}

func (s *Server) adminGetWorker(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	worker, err := s.store.GetWorker(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		log.Printf("admin get worker: %v", err)
		http.Error(w, "failed to get worker", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, worker)
}

func (s *Server) adminWorkerJobs(w http.ResponseWriter, r *http.Request, workerID string) {
	filter := store.ListJobsFilter{
		WorkerID: &workerID,
		Limit:    parseIntQuery(r, "limit", 20),
		Offset:   parseIntQuery(r, "offset", 0),
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	result, err := s.store.ListJobs(ctx, filter)
	if err != nil {
		log.Printf("admin worker jobs: %v", err)
		http.Error(w, "failed to list worker jobs", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func parseIntQuery(r *http.Request, key string, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}
