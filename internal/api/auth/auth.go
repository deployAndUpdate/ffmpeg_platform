package auth

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
)

// Role identifies which API surface a key may access.
type Role int

const (
	RoleClient Role = iota + 1
	RoleWorker
	RoleAdmin
)

// Config holds scheduler API key settings (Variant B: separate keys per role).
type Config struct {
	Required  bool
	ClientKey string
	WorkerKey string
	AdminKey  string
}

// ConfigFromEnv reads SCHEDULER_* API key environment variables.
func ConfigFromEnv() Config {
	required := false
	if v := os.Getenv("SCHEDULER_API_KEY_REQUIRED"); v == "true" || v == "1" {
		required = true
	}
	return Config{
		Required:  required,
		ClientKey: os.Getenv("SCHEDULER_CLIENT_API_KEY"),
		WorkerKey: os.Getenv("SCHEDULER_WORKER_API_KEY"),
		AdminKey:  os.Getenv("SCHEDULER_ADMIN_API_KEY"),
	}
}

// Enabled reports whether incoming requests must be authenticated.
func (c Config) Enabled() bool {
	if c.Required {
		return true
	}
	return c.ClientKey != "" || c.WorkerKey != "" || c.AdminKey != ""
}

// Validate checks configuration before the scheduler starts.
func (c Config) Validate() error {
	if !c.Enabled() {
		return nil
	}
	if c.Required {
		if c.ClientKey == "" {
			return errors.New("SCHEDULER_CLIENT_API_KEY is required when SCHEDULER_API_KEY_REQUIRED=true")
		}
		if c.WorkerKey == "" {
			return errors.New("SCHEDULER_WORKER_API_KEY is required when SCHEDULER_API_KEY_REQUIRED=true")
		}
	}
	keys := nonEmptyKeys(c.ClientKey, c.WorkerKey, c.AdminKey)
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if subtle.ConstantTimeCompare([]byte(keys[i]), []byte(keys[j])) == 1 {
				return errors.New("scheduler API keys must be unique")
			}
		}
	}
	return nil
}

func nonEmptyKeys(keys ...string) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k != "" {
			out = append(out, k)
		}
	}
	return out
}

// ExtractAPIKey reads the key from Authorization: Bearer or X-API-Key.
func ExtractAPIKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}

// Middleware enforces role-based API key auth before delegating to next.
type Middleware struct {
	cfg  Config
	next http.Handler
}

// NewMiddleware wraps next with API key checks when auth is enabled.
func NewMiddleware(cfg Config, next http.Handler) http.Handler {
	if !cfg.Enabled() {
		return next
	}
	return &Middleware{cfg: cfg, next: next}
}

func (m *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requiredRole, protected := routeRole(r.URL.Path)
	if !protected {
		m.next.ServeHTTP(w, r)
		return
	}

	key := ExtractAPIKey(r)
	if key == "" {
		writeAuthError(w, http.StatusUnauthorized, "unauthorized", "missing API key")
		return
	}

	role, ok := m.cfg.roleForKey(key)
	if !ok {
		writeAuthError(w, http.StatusUnauthorized, "unauthorized", "invalid API key")
		return
	}
	if !roleAllowed(role, requiredRole) {
		writeAuthError(w, http.StatusForbidden, "forbidden", "API key not allowed for this endpoint")
		return
	}

	m.next.ServeHTTP(w, r)
}

func routeRole(path string) (Role, bool) {
	switch {
	case path == "/health", path == "/docs", strings.HasPrefix(path, "/docs/"):
		return 0, false
	case strings.HasPrefix(path, "/workers/"):
		return RoleWorker, true
	case path == "/jobs", strings.HasPrefix(path, "/jobs/"):
		return RoleClient, true
	default:
		return RoleAdmin, true
	}
}

func (c Config) roleForKey(key string) (Role, bool) {
	if c.AdminKey != "" && subtle.ConstantTimeCompare([]byte(key), []byte(c.AdminKey)) == 1 {
		return RoleAdmin, true
	}
	if c.ClientKey != "" && subtle.ConstantTimeCompare([]byte(key), []byte(c.ClientKey)) == 1 {
		return RoleClient, true
	}
	if c.WorkerKey != "" && subtle.ConstantTimeCompare([]byte(key), []byte(c.WorkerKey)) == 1 {
		return RoleWorker, true
	}
	return 0, false
}

func roleAllowed(have, need Role) bool {
	if have == RoleAdmin {
		return true
	}
	return have == need
}

func writeAuthError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":   code,
		"message": message,
	})
}
