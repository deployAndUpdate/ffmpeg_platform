//go:build integration

package migrate_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"go_distributed_system/internal/migrate"

	_ "github.com/lib/pq"
)

func TestMigrateUpStatusAndDown(t *testing.T) {
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		t.Skip("TEST_DB_DSN not set (run: make test-integration)")
	}

	resetPublicSchema(t, dsn)

	if err := migrate.UpDSN(dsn); err != nil {
		t.Fatalf("first up: %v", err)
	}
	assertTableExists(t, dsn, "jobs")
	assertColumnExists(t, dsn, "jobs", "storage")
	assertColumnExists(t, dsn, "jobs", "preset")
	assertColumnExists(t, dsn, "jobs", "lease_generation")

	mg, err := migrate.New(dsn)
	if err != nil {
		t.Fatalf("new migrator: %v", err)
	}
	defer mg.Close()

	st, err := mg.Status()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Version != 4 {
		t.Fatalf("version = %d, want 4", st.Version)
	}
	if st.Dirty {
		t.Fatal("expected clean migration state")
	}

	if err := migrate.UpDSN(dsn); err != nil {
		t.Fatalf("second up: %v", err)
	}

	if err := mg.Down(1); err != nil {
		t.Fatalf("down 1: %v", err)
	}
	assertColumnMissing(t, dsn, "jobs", "lease_generation")

	st, err = mg.Status()
	if err != nil {
		t.Fatalf("status after down: %v", err)
	}
	if st.Version != 3 {
		t.Fatalf("version = %d, want 3", st.Version)
	}

	if err := mg.Down(1); err != nil {
		t.Fatalf("down 2: %v", err)
	}
	assertColumnMissing(t, dsn, "jobs", "preset")

	st, err = mg.Status()
	if err != nil {
		t.Fatalf("status after second down: %v", err)
	}
	if st.Version != 2 {
		t.Fatalf("version = %d, want 2", st.Version)
	}

	if err := mg.Down(2); err != nil {
		t.Fatalf("down 3+4: %v", err)
	}
	assertTableMissing(t, dsn, "jobs")

	st, err = mg.Status()
	if err != nil {
		t.Fatalf("status after full down: %v", err)
	}
	if st.Version != 0 {
		t.Fatalf("version = %d, want 0", st.Version)
	}

	if err := migrate.UpDSN(dsn); err != nil {
		t.Fatalf("re-up: %v", err)
	}
}

func TestMigrateLegacyInitSchemaBaseline(t *testing.T) {
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		t.Skip("TEST_DB_DSN not set (run: make test-integration)")
	}

	resetPublicSchema(t, dsn)
	applyLegacyInitSchema(t, dsn)

	if err := migrate.UpDSN(dsn); err != nil {
		t.Fatalf("up on legacy schema: %v", err)
	}

	assertColumnExists(t, dsn, "jobs", "storage")
	assertColumnExists(t, dsn, "jobs", "preset")
	assertColumnExists(t, dsn, "jobs", "lease_generation")

	mg, err := migrate.New(dsn)
	if err != nil {
		t.Fatalf("new migrator: %v", err)
	}
	defer mg.Close()

	st, err := mg.Status()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Version != 4 || st.Dirty {
		t.Fatalf("status = version %d dirty %t, want 4 false", st.Version, st.Dirty)
	}
}

func applyLegacyInitSchema(t *testing.T, dsn string) {
	t.Helper()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	legacyInit, err := os.ReadFile("../../db/migrations/000001_init.up.sql")
	if err != nil {
		t.Fatalf("read legacy init: %v", err)
	}
	if _, err := db.ExecContext(ctx, string(legacyInit)); err != nil {
		t.Fatalf("apply legacy init: %v", err)
	}

	legacyStorage := `ALTER TABLE jobs ADD COLUMN storage TEXT NOT NULL DEFAULT 'local' CHECK (storage IN ('local', 'r2'));`
	if _, err := db.ExecContext(ctx, legacyStorage); err != nil {
		t.Fatalf("apply legacy storage: %v", err)
	}
}

func resetPublicSchema(t *testing.T, dsn string) {
	t.Helper()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err = db.ExecContext(ctx, `
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO public;
GRANT ALL ON SCHEMA public TO CURRENT_USER;
`)
	if err != nil {
		t.Fatalf("reset schema: %v", err)
	}
}

func assertTableExists(t *testing.T, dsn, table string) {
	t.Helper()
	if !relationExists(t, dsn, table, "BASE TABLE") {
		t.Fatalf("table %q not found", table)
	}
}

func assertTableMissing(t *testing.T, dsn, table string) {
	t.Helper()
	if relationExists(t, dsn, table, "BASE TABLE") {
		t.Fatalf("table %q still exists", table)
	}
}

func assertColumnExists(t *testing.T, dsn, table, column string) {
	t.Helper()
	if !columnExists(t, dsn, table, column) {
		t.Fatalf("column %s.%s not found", table, column)
	}
}

func assertColumnMissing(t *testing.T, dsn, table, column string) {
	t.Helper()
	if columnExists(t, dsn, table, column) {
		t.Fatalf("column %s.%s still exists", table, column)
	}
}

func relationExists(t *testing.T, dsn, name, kind string) bool {
	t.Helper()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var exists bool
	err = db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM information_schema.tables
	WHERE table_schema = 'public' AND table_name = $1 AND table_type = $2
)`, name, kind).Scan(&exists)
	if err != nil {
		t.Fatalf("query table exists: %v", err)
	}
	return exists
}

func columnExists(t *testing.T, dsn, table, column string) bool {
	t.Helper()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var exists bool
	err = db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM information_schema.columns
	WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
)`, table, column).Scan(&exists)
	if err != nil {
		t.Fatalf("query column exists: %v", err)
	}
	return exists
}
