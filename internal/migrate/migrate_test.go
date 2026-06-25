package migrate_test

import (
	"os"
	"testing"

	"go_distributed_system/internal/migrate"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("MIGRATE_BACKUP_DIR", "/tmp/backups")
	t.Setenv("MIGRATE_BACKUP_BEFORE", "always")

	cfg := migrate.ConfigFromEnv()
	if cfg.BackupDir != "/tmp/backups" {
		t.Fatalf("BackupDir = %q, want /tmp/backups", cfg.BackupDir)
	}
	if cfg.BackupBefore != migrate.BackupAlways {
		t.Fatalf("BackupBefore = %q, want always", cfg.BackupBefore)
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	t.Setenv("MIGRATE_BACKUP_DIR", "")
	t.Setenv("MIGRATE_BACKUP_BEFORE", "")

	cfg := migrate.ConfigFromEnv()
	if cfg.BackupDir != "db/backups" {
		t.Fatalf("BackupDir = %q, want db/backups", cfg.BackupDir)
	}
	if cfg.BackupBefore != migrate.BackupRisky {
		t.Fatalf("BackupBefore = %q, want risky", cfg.BackupBefore)
	}
}

func TestBackupRequiresPgDump(t *testing.T) {
	t.Setenv("PATH", "")

	_, err := migrate.Backup("postgres://unused", t.TempDir())
	if err == nil {
		t.Fatal("expected error when pg_dump is unavailable")
	}
}

func TestUpDSNRequiresDSN(t *testing.T) {
	if err := migrate.UpDSN(""); err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestMigratorDownInvalidSteps(t *testing.T) {
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		t.Skip("TEST_DB_DSN not set")
	}

	mg, err := migrate.New(dsn)
	if err != nil {
		t.Fatalf("new migrator: %v", err)
	}
	defer mg.Close()

	if err := mg.Down(0); err == nil {
		t.Fatal("expected error for zero steps")
	}
}
