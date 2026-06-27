package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/types"

	"github.com/google/uuid"
)

func (s *Server) handleJobsInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.storage == nil {
		http.Error(w, "object storage is not configured", http.StatusServiceUnavailable)
		return
	}
	s.initR2Job(w, r)
}

func (s *Server) initR2Job(w http.ResponseWriter, r *http.Request) {
	type req struct {
		Preset        string `json:"preset"`
		FFmpegArgs    string `json:"ffmpeg_args"`
		InputFilename string `json:"input_filename"`
		OutputExt     string `json:"output_ext"`
		MaxAttempts        int    `json:"max_attempts"`
		MaxDurationSeconds int    `json:"max_duration_seconds"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.InputFilename == "" || body.OutputExt == "" {
		http.Error(w, "input_filename and output_ext are required", http.StatusBadRequest)
		return
	}
	outputExt := sanitizeOutputExt(body.OutputExt)
	if outputExt == "" || outputExt == "bin" {
		http.Error(w, "output_ext is required", http.StatusBadRequest)
		return
	}
	spec, err := resolveTranscodeSpec(body.Preset, body.FFmpegArgs, outputExt)
	if err != nil {
		status, msg := transcodeSpecHTTPError(err)
		http.Error(w, msg, status)
		return
	}
	if body.MaxAttempts <= 0 {
		body.MaxAttempts = 3
	}

	inputExt := storage.ExtFromFilename(body.InputFilename)

	id := uuid.New().String()
	inputKey := s.storage.InputObjectKey(id, inputExt)
	outputKey := s.storage.OutputObjectKey(id, outputExt)

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	uploadURL, err := s.storage.PresignPut(ctx, inputKey, 0)
	if err != nil {
		log.Printf("presign upload: %v", err)
		http.Error(w, "failed to create upload url", http.StatusInternalServerError)
		return
	}

	uploadExpires := time.Now().UTC().Add(s.uploadPresignTTL())

	job := &types.Job{
		ID:                 id,
		InputPath:          inputKey,
		OutputPath:         outputKey,
		Preset:             spec.Preset,
		FFmpegArgs:         spec.FFmpegArgs,
		Storage:            types.StorageR2,
		Status:             types.JobStatusNew,
		Attempt:            0,
		MaxAttempts:        body.MaxAttempts,
		MaxDurationSeconds: resolveMaxDurationSeconds(body.MaxDurationSeconds, spec.Preset),
	}
	if err := s.store.CreateJob(ctx, job); err != nil {
		log.Printf("create r2 job: %v", err)
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"job":               job,
		"upload_url":        uploadURL,
		"upload_expires_at": uploadExpires,
		"bucket":            s.storage.Bucket(),
	})
}

func (s *Server) submitR2Job(w http.ResponseWriter, r *http.Request, id string) {
	if s.storage == nil {
		http.Error(w, "object storage is not configured", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		log.Printf("get job for submit: %v", err)
		http.Error(w, "failed to get job", http.StatusInternalServerError)
		return
	}
	if job.Storage != types.StorageR2 {
		http.Error(w, "job is not an R2 job", http.StatusBadRequest)
		return
	}
	if job.Status != types.JobStatusNew {
		http.Error(w, "job is not awaiting upload", http.StatusConflict)
		return
	}

	exists, err := s.storage.Exists(ctx, job.InputPath)
	if err != nil {
		log.Printf("head input object: %v", err)
		http.Error(w, "failed to verify upload", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "input file not uploaded yet", http.StatusBadRequest)
		return
	}

	if err := s.store.QueueJob(ctx, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "job is not awaiting upload", http.StatusConflict)
			return
		}
		log.Printf("queue job: %v", err)
		http.Error(w, "failed to queue job", http.StatusInternalServerError)
		return
	}

	job, err = s.store.GetJob(ctx, id)
	if err != nil {
		log.Printf("get job after submit: %v", err)
		http.Error(w, "failed to get job", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) jobResponse(ctx context.Context, job *types.Job) map[string]any {
	out := map[string]any{
		"id":                 job.ID,
		"input_path":         job.InputPath,
		"output_path":        job.OutputPath,
		"preset":             job.Preset,
		"ffmpeg_args":        job.FFmpegArgs,
		"storage":            job.Storage,
		"status":             job.Status,
		"assigned_worker_id": job.AssignedWorkerID,
		"attempt":            job.Attempt,
		"max_attempts":       job.MaxAttempts,
		"lease_expires_at":   job.LeaseExpiresAt,
		"lease_generation":   job.LeaseGeneration,
		"max_duration_seconds": job.MaxDurationSeconds,
		"created_at":         job.CreatedAt,
		"started_at":         job.StartedAt,
		"finished_at":        job.FinishedAt,
		"updated_at":         job.UpdatedAt,
	}

	if s.storage != nil && job.Storage == types.StorageR2 && job.Status == types.JobStatusCompleted {
		downloadURL, err := s.storage.PresignGet(ctx, job.OutputPath, 0)
		if err != nil {
			log.Printf("presign download for job %s: %v", job.ID, err)
		} else {
			out["download_url"] = downloadURL
			out["download_expires_at"] = time.Now().UTC().Add(s.downloadPresignTTL())
		}
	}
	return out
}

func (s *Server) uploadPresignTTL() time.Duration {
	if s.storageConfig.PresignUploadTTL > 0 {
		return s.storageConfig.PresignUploadTTL
	}
	return time.Hour
}

func (s *Server) downloadPresignTTL() time.Duration {
	if s.storageConfig.PresignDownloadTTL > 0 {
		return s.storageConfig.PresignDownloadTTL
	}
	return time.Hour
}

func sanitizeOutputExt(ext string) string {
	ext = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(ext)), ".")
	if ext == "" {
		return "bin"
	}
	for _, r := range ext {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return "bin"
		}
	}
	return ext
}
