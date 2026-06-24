package api

import (
	"context"
	"log"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/types"

	"github.com/google/uuid"
)

func (s *Server) createJobFromUpload(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		http.Error(w, "object storage is not configured", http.StatusServiceUnavailable)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.maxUploadBytes)

	const maxFormMemory = 32 << 20
	if err := r.ParseMultipartForm(maxFormMemory); err != nil {
		if isRequestTooLarge(err) {
			http.Error(w, "upload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "invalid multipart form", http.StatusBadRequest)
		return
	}

	ffmpegArgs := strings.TrimSpace(r.FormValue("ffmpeg_args"))
	outputExt := sanitizeOutputExt(r.FormValue("output_ext"))
	if ffmpegArgs == "" || outputExt == "" {
		http.Error(w, "ffmpeg_args and output_ext are required", http.StatusBadRequest)
		return
	}

	maxAttempts := 3
	if v := strings.TrimSpace(r.FormValue("max_attempts")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			http.Error(w, "invalid max_attempts", http.StatusBadRequest)
			return
		}
		maxAttempts = n
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	filename := header.Filename
	if filename == "" {
		filename = "upload.bin"
	}
	inputExt := storage.ExtFromFilename(filename)

	id := uuid.New().String()
	inputKey := s.storage.InputObjectKey(id, inputExt)
	outputKey := s.storage.OutputObjectKey(id, outputExt)
	contentType := mime.TypeByExtension(filepath.Ext(filename))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.uploadTimeout)
	defer cancel()

	if err := s.storage.UploadReader(ctx, inputKey, file, header.Size, contentType); err != nil {
		log.Printf("upload input for job %s: %v", id, err)
		http.Error(w, "failed to store upload", http.StatusInternalServerError)
		return
	}

	job := &types.Job{
		ID:          id,
		InputPath:   inputKey,
		OutputPath:  outputKey,
		FFmpegArgs:  ffmpegArgs,
		Storage:     types.StorageR2,
		Status:      types.JobStatusQueued,
		Attempt:     0,
		MaxAttempts: maxAttempts,
	}
	if err := s.store.CreateJob(ctx, job); err != nil {
		log.Printf("create upload job: %v", err)
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, job)
}

func isRequestTooLarge(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "http: request body too large") ||
		strings.Contains(msg, "multipart: message too large")
}
