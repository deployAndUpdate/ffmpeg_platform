package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	dbmigrations "go_distributed_system/db/migrations"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/lib/pq"
)

// ErrNoChange is returned when the database schema is already at the requested version.
var ErrNoChange = migrate.ErrNoChange

// Config controls optional backup behaviour before applying migrations.
type Config struct {
	BackupDir     string
	BackupBefore  BackupPolicy
	RequireBackup bool
}

// BackupPolicy decides when to run pg_dump before migrate up.
type BackupPolicy string

const (
	BackupNever  BackupPolicy = "never"
	BackupRisky  BackupPolicy = "risky"
	BackupAlways BackupPolicy = "always"
)

// ConfigFromEnv reads migrator settings from the environment.
func ConfigFromEnv() Config {
	dir := os.Getenv("MIGRATE_BACKUP_DIR")
	if dir == "" {
		dir = "db/backups"
	}
	policy := BackupPolicy(os.Getenv("MIGRATE_BACKUP_BEFORE"))
	if policy == "" {
		policy = BackupRisky
	}
	return Config{
		BackupDir:    dir,
		BackupBefore: policy,
	}
}

// Status describes the current migration state.
type Status struct {
	Version uint
	Dirty   bool
}

// New opens a migrator for the given PostgreSQL DSN.
func New(dsn string) (*Migrator, error) {
	source, err := iofs.New(dbmigrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("migration source: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", source, dsn)
	if err != nil {
		return nil, fmt.Errorf("migration instance: %w", err)
	}

	return &Migrator{m: m}, nil
}

// Migrator wraps golang-migrate with project defaults.
type Migrator struct {
	m *migrate.Migrate
}

// Close releases migration resources.
func (mg *Migrator) Close() error {
	if mg == nil || mg.m == nil {
		return nil
	}
	srcErr, dbErr := mg.m.Close()
	return errors.Join(srcErr, dbErr)
}

// Force sets the schema version without running migrations.
func (mg *Migrator) Force(version int) error {
	if err := mg.m.Force(version); err != nil {
		return err
	}
	return nil
}

// Up applies all pending migrations.
func (mg *Migrator) Up() error {
	if err := mg.m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

// Down rolls back the last n migrations.
func (mg *Migrator) Down(steps int) error {
	if steps <= 0 {
		return fmt.Errorf("steps must be positive, got %d", steps)
	}
	if err := mg.m.Steps(-steps); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

// Status returns the current schema version.
func (mg *Migrator) Status() (Status, error) {
	version, dirty, err := mg.m.Version()
	if err != nil {
		if errors.Is(err, migrate.ErrNilVersion) {
			return Status{}, nil
		}
		return Status{}, err
	}
	return Status{Version: version, Dirty: dirty}, nil
}

// UpDSN is a convenience helper that opens a migrator, applies pending migrations, and closes it.
func UpDSN(dsn string) error {
	mg, err := New(dsn)
	if err != nil {
		return err
	}
	defer mg.Close()

	if err := reconcileLegacySchema(dsn, mg); err != nil {
		return err
	}
	return mg.Up()
}

// UpWithBackup applies pending migrations and optionally creates a pg_dump backup first.
func UpWithBackup(dsn string, cfg Config) (backupPath string, err error) {
	if shouldBackup(cfg.BackupBefore) {
		backupPath, err = Backup(dsn, cfg.BackupDir)
		if err != nil {
			return "", err
		}
	}

	mg, err := New(dsn)
	if err != nil {
		return backupPath, err
	}
	defer mg.Close()

	if err := reconcileLegacySchema(dsn, mg); err != nil {
		return backupPath, err
	}
	if err := mg.Up(); err != nil {
		return backupPath, err
	}
	return backupPath, nil
}

// reconcileLegacySchema aligns schema_migrations with an existing schema created
// by the old docker-entrypoint-initdb.d flow or a failed first migrate run.
func reconcileLegacySchema(dsn string, mg *Migrator) error {
	detected, err := detectSchemaVersion(dsn)
	if err != nil {
		return err
	}
	if detected == 0 {
		return nil
	}

	st, err := mg.Status()
	if err != nil {
		return err
	}
	if st.Version == detected && !st.Dirty {
		return nil
	}

	if err := mg.Force(int(detected)); err != nil {
		return fmt.Errorf("baseline legacy schema at version %d: %w", detected, err)
	}
	return nil
}

func detectSchemaVersion(dsn string) (uint, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if !relationExists(ctx, db, "workers", "BASE TABLE") ||
		!relationExists(ctx, db, "jobs", "BASE TABLE") ||
		!relationExists(ctx, db, "job_logs", "BASE TABLE") {
		return 0, nil
	}

	version := uint(1)
	if columnExists(ctx, db, "jobs", "storage") {
		version = 2
	}
	if columnExists(ctx, db, "jobs", "preset") {
		version = 3
	}
	if columnExists(ctx, db, "jobs", "lease_generation") {
		version = 4
	}
	return version, nil
}

func relationExists(ctx context.Context, db *sql.DB, name, kind string) bool {
	var exists bool
	err := db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM information_schema.tables
	WHERE table_schema = 'public' AND table_name = $1 AND table_type = $2
)`, name, kind).Scan(&exists)
	return err == nil && exists
}

func columnExists(ctx context.Context, db *sql.DB, table, column string) bool {
	var exists bool
	err := db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM information_schema.columns
	WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
)`, table, column).Scan(&exists)
	return err == nil && exists
}

func shouldBackup(policy BackupPolicy) bool {
	switch policy {
	case BackupAlways:
		return true
	case BackupRisky:
		// No risky migrations yet; hook for future *.risky.up.sql files.
		return false
	default:
		return false
	}
}

// Backup creates a compressed pg_dump file and returns its path.
func Backup(dsn, dir string) (string, error) {
	if _, err := exec.LookPath("pg_dump"); err != nil {
		return "", fmt.Errorf("pg_dump not found in PATH: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create backup dir: %w", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("pre_migrate_%s.dump", time.Now().UTC().Format("20060102_150405")))
	cmd := exec.Command("pg_dump", "-Fc", "-f", path, dsn)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("pg_dump: %w: %s", err, string(out))
	}
	return path, nil
}
