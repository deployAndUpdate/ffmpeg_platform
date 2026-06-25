//go:build integration

package testutil

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"go_distributed_system/internal/store"

	_ "github.com/lib/pq"
)

var migrateOnce sync.Once

// OpenStore connects to the dedicated test database (TEST_DB_DSN).
// Refuses non-test database names to avoid touching dev/prod data.
func OpenStore(t *testing.T) (*store.Store, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		t.Skip("TEST_DB_DSN not set (run: make test-integration)")
	}
	assertTestDatabase(t, dsn)

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
	if err := resetTables(ctx, dsn); err != nil {
		_ = st.Close()
		t.Fatalf("reset tables: %v", err)
	}

	return st, func() { _ = st.Close() }
}

func assertTestDatabase(t *testing.T, dsn string) {
	t.Helper()

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DB_DSN: %v", err)
	}
	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" || !strings.HasSuffix(dbName, "_test") {
		t.Fatalf("integration tests require a database name ending with _test, got %q", dbName)
	}
}

func applyMigration(dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := execMigrationFile(ctx, db, "001_init.sql"); err != nil {
		return err
	}
	if err := execMigrationFile(ctx, db, "002_storage.sql"); err != nil {
		return err
	}
	return execMigrationFile(ctx, db, "003_preset.sql")
}

func execMigrationFile(ctx context.Context, db *sql.DB, name string) error {
	columnChecks := map[string]string{
		"002_storage.sql": "storage",
		"003_preset.sql":  "preset",
	}
	if columnName, ok := columnChecks[name]; ok {
		var exists bool
		if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT FROM information_schema.columns
	WHERE table_schema = 'public' AND table_name = 'jobs' AND column_name = $1
)`, columnName).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
	} else if name == "001_init.sql" {
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
	}

	sqlBytes, err := os.ReadFile(filepath.Join(migrationsDir(), name))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, string(sqlBytes))
	return err
}

func resetTables(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, `TRUNCATE job_logs, jobs, workers RESTART IDENTITY CASCADE`)
	return err
}

func migrationsDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "db/migrations"
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "db", "migrations")
}
