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

	"go_distributed_system/internal/api/auth"
	"go_distributed_system/internal/queue"
	"go_distributed_system/internal/store"
	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/types"
	webadmin "go_distributed_system/web/admin"
	webdocs "go_distributed_system/web/docs"

	"github.com/google/uuid"
)

// Server bundles HTTP handlers for the scheduler.
type Server struct {
	store           JobStore
	storage         storage.ObjectStorage
	storageConfig   storage.Config
	maxUploadBytes  int64
	uploadTimeout   time.Duration
	auth            auth.Config
	rabbit          queue.Publisher
	mux             *http.ServeMux
}

// NewServer wires routes and dependencies (local-path jobs only).
func NewServer(st JobStore) *Server {
	return NewServerWithStorage(st, nil, storage.Config{})
}

// NewServerWithStorage enables R2 object-storage job flow when storage is non-nil.
func NewServerWithStorage(st JobStore, obj storage.ObjectStorage, cfg storage.Config) *Server {
	return NewServerWithStorageAndAuth(st, obj, cfg, auth.Config{})
}

// NewServerWithStorageAndAuth is like NewServerWithStorage with explicit API key auth config.
func NewServerWithStorageAndAuth(st JobStore, obj storage.ObjectStorage, cfg storage.Config, authCfg auth.Config) *Server {
	return NewServerWithStorageAuthAndRabbit(st, obj, cfg, authCfg, nil)
}

// NewServerWithStorageAuthAndRabbit adds optional RabbitMQ readiness checks.
func NewServerWithStorageAuthAndRabbit(st JobStore, obj storage.ObjectStorage, cfg storage.Config, authCfg auth.Config, pub queue.Publisher) *Server {
	s := &Server{
		store:          st,
		storage:        obj,
		storageConfig:  cfg,
		maxUploadBytes: MaxUploadBytesFromEnv(),
		uploadTimeout:  UploadTimeoutFromEnv(),
		auth:           authCfg,
		rabbit:         pub,
		mux:            http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

// ServeHTTP allows Server to act as an http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth.NewMiddleware(s.auth, s.mux).ServeHTTP(w, r)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/ready", s.handleReady)
	s.mux.HandleFunc("/presets", s.handlePresets)
	s.mux.HandleFunc("/jobs/init", s.handleJobsInit)
	s.mux.HandleFunc("/jobs", s.handleJobs)
	s.mux.HandleFunc("/jobs/", s.handleJobByID)

	s.mux.HandleFunc("/workers/register", s.handleWorkerRegister)
	s.mux.HandleFunc("/workers/heartbeat", s.handleWorkerHeartbeat)
	s.mux.HandleFunc("/workers/claim-job", s.handleWorkerClaimJob)
	s.mux.HandleFunc("/workers/renew-lease", s.handleWorkerRenewLease)
	s.mux.HandleFunc("/workers/job-result", s.handleWorkerJobResult)

	s.mux.HandleFunc("/admin/api/", s.handleAdmin)

	s.registerDocsRoutes()
	s.registerAdminRoutes()
}

func (s *Server) registerAdminRoutes() {
	sub, err := fs.Sub(webadmin.FS, ".")
	if err != nil {
		log.Printf("admin fs: %v", err)
		return
	}
	fileServer := http.FileServer(http.FS(sub))
	s.mux.Handle("/admin/", http.StripPrefix("/admin/", fileServer))
	s.mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/", http.StatusMovedPermanently)
	})
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

// handleJobs covers POST /jobs (JSON local paths or multipart upload to R2).
func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		ct := r.Header.Get("Content-Type")
		if strings.HasPrefix(ct, "multipart/form-data") {
			s.createJobFromUpload(w, r)
			return
		}
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
		s.getJobLogs(w, r, id)
		return
	}

	if len(parts) == 2 && parts[1] == "submit" && r.Method == http.MethodPost {
		s.submitR2Job(w, r, id)
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

func (s *Server) handleWorkerClaimJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.workerClaimJob(w, r)
}

func (s *Server) handleWorkerRenewLease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.workerRenewLease(w, r)
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
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		http.Error(w, "use multipart POST /jobs only when R2 is configured", http.StatusUnsupportedMediaType)
		return
	}
	type req struct {
		InputPath          string `json:"input_path"`
		OutputPath         string `json:"output_path"`
		Preset             string `json:"preset"`
		FFmpegArgs         string `json:"ffmpeg_args"`
		MaxAttempts        int    `json:"max_attempts"`
		MaxDurationSeconds int    `json:"max_duration_seconds"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.InputPath == "" || body.OutputPath == "" {
		http.Error(w, "input_path and output_path are required", http.StatusBadRequest)
		return
	}
	spec, err := resolveTranscodeSpec(body.Preset, body.FFmpegArgs, outputExtFromPath(body.OutputPath))
	if err != nil {
		status, msg := transcodeSpecHTTPError(err)
		http.Error(w, msg, status)
		return
	}
	if body.MaxAttempts <= 0 {
		body.MaxAttempts = 3
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if idempotencyKey != "" {
		existing, err := s.store.GetJobByIdempotencyKey(ctx, idempotencyKey)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			log.Printf("get job by idempotency key: %v", err)
			http.Error(w, "failed to create job", http.StatusInternalServerError)
			return
		}
		if existing != nil {
			writeJSON(w, http.StatusOK, existing)
			return
		}
	}

	id := uuid.New().String()
	if err := s.store.CreateAndDispatch(ctx, &store.JobCreateParams{
		ID:                 id,
		InputPath:          body.InputPath,
		OutputPath:         body.OutputPath,
		Preset:             spec.Preset,
		FFmpegArgs:         spec.FFmpegArgs,
		Storage:            types.StorageLocal,
		Attempt:            0,
		MaxAttempts:        body.MaxAttempts,
		IdempotencyKey:     idempotencyKey,
		MaxDurationSeconds: resolveMaxDurationSeconds(body.MaxDurationSeconds, spec.Preset),
	}); err != nil {
		log.Printf("create job: %v", err)
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}
	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		log.Printf("get job after create: %v", err)
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
	writeJSON(w, http.StatusOK, s.jobResponse(ctx, job))
}

func (s *Server) getJobLogs(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if _, err := s.store.GetJob(ctx, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		log.Printf("get job for logs: %v", err)
		http.Error(w, "failed to get job", http.StatusInternalServerError)
		return
	}

	logs, err := s.store.GetJobLogs(ctx, id)
	if err != nil {
		log.Printf("get job logs: %v", err)
		http.Error(w, "failed to get job logs", http.StatusInternalServerError)
		return
	}
	if logs == nil {
		logs = []types.JobLog{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": id,
		"logs":   logs,
	})
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

func (s *Server) workerClaimJob(w http.ResponseWriter, r *http.Request) {
	type req struct {
		WorkerID string `json:"worker_id"`
		JobID    string `json:"job_id"`
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

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	job, err := s.store.ClaimJob(ctx, body.JobID, body.WorkerID, JobLeaseFromEnv())
	if err != nil {
		if errors.Is(err, store.ErrJobNotClaimable) {
			http.Error(w, "job not dispatchable or already claimed", http.StatusConflict)
			return
		}
		log.Printf("claim job: %v", err)
		http.Error(w, "failed to claim job", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Server) workerRenewLease(w http.ResponseWriter, r *http.Request) {
	type req struct {
		WorkerID         string `json:"worker_id"`
		JobID            string `json:"job_id"`
		LeaseGeneration  int64  `json:"lease_generation"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.WorkerID == "" || body.JobID == "" || body.LeaseGeneration <= 0 {
		http.Error(w, "worker_id, job_id and lease_generation are required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	job, err := s.store.RenewLease(ctx, body.JobID, body.WorkerID, body.LeaseGeneration, JobLeaseFromEnv())
	if err != nil {
		if errors.Is(err, store.ErrLeaseLost) {
			http.Error(w, "lease lost or generation mismatch", http.StatusConflict)
			return
		}
		log.Printf("renew lease: %v", err)
		http.Error(w, "failed to renew lease", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Server) workerJobResult(w http.ResponseWriter, r *http.Request) {
	type req struct {
		WorkerID         string              `json:"worker_id"`
		JobID            string              `json:"job_id"`
		LeaseGeneration  int64               `json:"lease_generation"`
		Success          bool                `json:"success"`
		Logs             []types.JobLogEntry `json:"logs"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.WorkerID == "" || body.JobID == "" || body.LeaseGeneration <= 0 {
		http.Error(w, "worker_id, job_id and lease_generation are required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	if err := s.store.FinishJob(ctx, body.JobID, body.WorkerID, body.LeaseGeneration, body.Success, body.Logs); err != nil {
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
