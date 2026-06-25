package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConfigEnabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{"disabled empty", Config{}, false},
		{"required without keys", Config{Required: true}, true},
		{"client key only", Config{ClientKey: "client"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.Enabled(); got != tt.want {
				t.Fatalf("Enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	if err := (Config{Required: true, ClientKey: "a", WorkerKey: "b"}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := (Config{Required: true, ClientKey: "same", WorkerKey: "same"}).Validate(); err == nil {
		t.Fatal("expected duplicate key error")
	}
	if err := (Config{Required: true, WorkerKey: "w"}).Validate(); err == nil {
		t.Fatal("expected missing client key error")
	}
}

func TestMiddlewarePublicPaths(t *testing.T) {
	var hit bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
	})
	h := NewMiddleware(Config{
		Required:  true,
		ClientKey: "client",
		WorkerKey: "worker",
	}, next)

	for _, path := range []string{"/health", "/docs", "/docs/index.html", "/admin/", "/admin"} {
		hit = false
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || !hit {
			t.Fatalf("path %s: status=%d hit=%v", path, rec.Code, hit)
		}
	}
}

func TestMiddlewareClientAndWorkerRoles(t *testing.T) {
	cfg := Config{
		Required:  true,
		ClientKey: "client-key",
		WorkerKey: "worker-key",
		AdminKey:  "admin-key",
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name       string
		path       string
		key        string
		wantStatus int
	}{
		{"client on jobs", "/jobs", "client-key", http.StatusOK},
		{"worker rejected on jobs", "/jobs", "worker-key", http.StatusForbidden},
		{"worker on workers", "/workers/register", "worker-key", http.StatusOK},
		{"client rejected on workers", "/workers/register", "client-key", http.StatusForbidden},
		{"admin on jobs", "/jobs/1", "admin-key", http.StatusOK},
		{"admin on workers", "/workers/heartbeat", "admin-key", http.StatusOK},
		{"admin on admin api", "/admin/api/stats", "admin-key", http.StatusOK},
		{"client rejected on admin api", "/admin/api/stats", "client-key", http.StatusForbidden},
		{"missing key", "/jobs", "", http.StatusUnauthorized},
		{"invalid key", "/jobs", "wrong", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			if tt.key != "" {
				req.Header.Set("Authorization", "Bearer "+tt.key)
			}
			rec := httptest.NewRecorder()
			NewMiddleware(cfg, next).ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestExtractAPIKey(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	if got := ExtractAPIKey(req); got != "abc" {
		t.Fatalf("Bearer = %q, want abc", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "xyz")
	if got := ExtractAPIKey(req); got != "xyz" {
		t.Fatalf("X-API-Key = %q, want xyz", got)
	}
}
