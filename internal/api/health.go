package api

import (
	"context"
	"net/http"
	"time"
)

const readinessCheckTimeout = 3 * time.Second

type readinessCheck struct {
	Status    string `json:"status"`
	LatencyMS int64  `json:"latency_ms,omitempty"`
	Error     string `json:"error,omitempty"`
}

type readinessResponse struct {
	Status string                    `json:"status"`
	Checks map[string]readinessCheck `json:"checks"`
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), readinessCheckTimeout)
	defer cancel()

	checks := map[string]readinessCheck{
		"postgres": s.checkPostgres(ctx),
		"r2":       s.checkR2(ctx),
	}

	status := http.StatusOK
	overall := "ok"
	for _, c := range checks {
		if c.Status == "fail" {
			overall = "degraded"
			status = http.StatusServiceUnavailable
			break
		}
	}

	writeJSON(w, status, readinessResponse{
		Status: overall,
		Checks: checks,
	})
}

func (s *Server) checkPostgres(ctx context.Context) readinessCheck {
	start := time.Now()
	if err := s.store.Ping(ctx); err != nil {
		return readinessCheck{
			Status: "fail",
			Error:  err.Error(),
		}
	}
	return readinessCheck{
		Status:    "ok",
		LatencyMS: time.Since(start).Milliseconds(),
	}
}

func (s *Server) checkR2(ctx context.Context) readinessCheck {
	if s.storage == nil {
		return readinessCheck{Status: "skipped"}
	}

	start := time.Now()
	if err := s.storage.HealthCheck(ctx); err != nil {
		return readinessCheck{
			Status: "fail",
			Error:  err.Error(),
		}
	}
	return readinessCheck{
		Status:    "ok",
		LatencyMS: time.Since(start).Milliseconds(),
	}
}
