//go:build integration

package testutil

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"go_distributed_system/internal/store"

	_ "github.com/lib/pq"
)

var migrateOnce sync.Once

// OpenStore connects to PostgreSQL using DB_DSN and returns a ready store.
// Skips the test when DB_DSN is unset (unit/short runs).
func OpenStore(t *testing.T) (*store.Store, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		t.Skip("DB_DSN not set")
	}

	migrateOnce.Do(func() {
		if err := applyMigration(dsn); err != nil {
			t.Fatalf("apply migration: %v", err)
		}
	})

	st, err := store.New(dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := resetTables(ctx, st); err != nil {
		_ = st.Close()
		t.Fatalf("reset tables: %v", err)
	}

	return st, func() { _ = st.Close() }
}

func applyMigration(dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var exists bool
	if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT FROM information_schema.tables
	WHERE table_schema = 'public' AND table_name = 'jobs'
)`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}

	sqlBytes, err := os.ReadFile(migrationPath())
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, string(sqlBytes))
	return err
}

func resetTables(ctx context.Context, st *store.Store) error {
	// Store does not expose DB; use a fresh connection with the same DSN pattern.
	// resetTables is called right after OpenStore which already validated DSN via env.
	db, err := sql.Open("postgres", os.Getenv("DB_DSN"))
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, `TRUNCATE job_logs, jobs, workers RESTART IDENTITY CASCADE`)
	return err
}

func migrationPath() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "db/migrations/001_init.sql"
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "db", "migrations", "001_init.sql")
}
