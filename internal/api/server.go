package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"time"

	"go_distributed_system/internal/store"
	"go_distributed_system/internal/types"
	webdocs "go_distributed_system/web/docs"

	"github.com/google/uuid"
)

// Server bundles HTTP handlers for the scheduler.
type Server struct {
	store *store.Store
	mux   *http.ServeMux
}

// NewServer wires routes and dependencies.
func NewServer(st *store.Store) *Server {
	s := &Server{
		store: st,
		mux:   http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

// ServeHTTP allows Server to act as an http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/jobs", s.handleJobs)
	s.mux.HandleFunc("/jobs/", s.handleJobByID)

	s.mux.HandleFunc("/workers/register", s.handleWorkerRegister)
	s.mux.HandleFunc("/workers/heartbeat", s.handleWorkerHeartbeat)
	s.mux.HandleFunc("/workers/request-job", s.handleWorkerRequestJob)
	s.mux.HandleFunc("/workers/job-result", s.handleWorkerJobResult)

	s.registerDocsRoutes()
}

func (s *Server) registerDocsRoutes() {
	sub, err := fs.Sub(webdocs.FS, ".")
	if err != nil {
		log.Printf("docs fs: %v", err)
		return
	}
	fileServer := http.FileServer(http.FS(sub))
	s.mux.Handle("/docs/", http.StripPrefix("/docs/", fileServer))
	s.mux.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/docs/", http.StatusMovedPermanently)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleJobs covers POST /jobs.
func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.createJob(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleJobByID covers GET /jobs/{id} and GET /jobs/{id}/logs.
func (s *Server) handleJobByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if path == "" {
		http.NotFound(w, r)
		return
	}

	parts := strings.Split(path, "/")
	id := parts[0]

	if len(parts) == 2 && parts[1] == "logs" && r.Method == http.MethodGet {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "GET /jobs/{id}/logs not implemented yet"})
		return
	}

	if len(parts) != 1 || r.Method != http.MethodGet {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	s.getJob(w, r, id)
}

func (s *Server) handleWorkerRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.workerRegister(w, r)
}

func (s *Server) handleWorkerHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.workerHeartbeat(w, r)
}

func (s *Server) handleWorkerRequestJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.workerRequestJob(w, r)
}

func (s *Server) handleWorkerJobResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.workerJobResult(w, r)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("write json: %v", err)
	}
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	type req struct {
		InputPath   string `json:"input_path"`
		OutputPath  string `json:"output_path"`
		FFmpegArgs  string `json:"ffmpeg_args"`
		MaxAttempts int    `json:"max_attempts"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.InputPath == "" || body.OutputPath == "" || body.FFmpegArgs == "" {
		http.Error(w, "input_path, output_path, ffmpeg_args are required", http.StatusBadRequest)
		return
	}
	if body.MaxAttempts <= 0 {
		body.MaxAttempts = 3
	}

	id := uuid.New().String()
	job := &types.Job{
		ID:          id,
		InputPath:   body.InputPath,
		OutputPath:  body.OutputPath,
		FFmpegArgs:  body.FFmpegArgs,
		Status:      types.JobStatusQueued,
		Attempt:     0,
		MaxAttempts: body.MaxAttempts,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.store.CreateJob(ctx, job); err != nil {
		log.Printf("create job: %v", err)
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		log.Printf("get job: %v", err)
		http.Error(w, "failed to get job", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) workerRegister(w http.ResponseWriter, r *http.Request) {
	type req struct {
		ID              string `json:"id"`
		Hostname        string `json:"hostname"`
		CPUCores        int    `json:"cpu_cores"`
		GPUAvailable    bool   `json:"gpu_available"`
		MaxParallelJobs int    `json:"max_parallel_jobs"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.ID == "" || body.Hostname == "" || body.CPUCores <= 0 {
		http.Error(w, "id, hostname, cpu_cores are required", http.StatusBadRequest)
		return
	}
	if body.MaxParallelJobs <= 0 {
		body.MaxParallelJobs = 1
	}

	now := time.Now().UTC()
	worker := &types.Worker{
		ID:              body.ID,
		Hostname:        body.Hostname,
		CPUCores:        body.CPUCores,
		GPUAvailable:    body.GPUAvailable,
		MaxParallelJobs: body.MaxParallelJobs,
		LastHeartbeatAt: &now,
		Status:          types.WorkerStatusActive,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.store.RegisterWorker(ctx, worker); err != nil {
		log.Printf("register worker: %v", err)
		http.Error(w, "failed to register worker", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, worker)
}

func (s *Server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	type req struct {
		ID string `json:"id"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.ID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.store.Heartbeat(ctx, body.ID, now); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		log.Printf("heartbeat: %v", err)
		http.Error(w, "failed to update heartbeat", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

const defaultJobLease = 30 * time.Minute

func (s *Server) workerRequestJob(w http.ResponseWriter, r *http.Request) {
	type req struct {
		WorkerID string `json:"worker_id"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.WorkerID == "" {
		http.Error(w, "worker_id is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	job, err := s.store.AcquireJob(ctx, body.WorkerID, defaultJobLease)
	if err != nil {
		log.Printf("acquire job: %v", err)
		http.Error(w, "failed to acquire job", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Server) workerJobResult(w http.ResponseWriter, r *http.Request) {
	type req struct {
		WorkerID string              `json:"worker_id"`
		JobID    string              `json:"job_id"`
		Success  bool                `json:"success"`
		Logs     []types.JobLogEntry `json:"logs"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.WorkerID == "" || body.JobID == "" {
		http.Error(w, "worker_id and job_id are required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	if err := s.store.FinishJob(ctx, body.JobID, body.WorkerID, body.Success, body.Logs); err != nil {
		if errors.Is(err, store.ErrJobNotAssigned) {
			http.Error(w, "job not assigned to worker or not running", http.StatusConflict)
			return
		}
		log.Printf("finish job: %v", err)
		http.Error(w, "failed to finish job", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
