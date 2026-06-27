//go:build integration

package testutil

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go_distributed_system/internal/migrate"
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
		if err := migrate.UpDSN(dsn); err != nil {
			t.Fatalf("migrate up: %v", err)
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

func resetTables(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, `TRUNCATE job_logs, job_outbox, jobs, workers RESTART IDENTITY CASCADE`)
	return err
}
