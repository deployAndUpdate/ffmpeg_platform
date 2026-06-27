package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"go_distributed_system/internal/migrate"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("DB_DSN is required")
	}

	switch os.Args[1] {
	case "up":
		runUp(dsn)
	case "down":
		runDown(dsn, os.Args[2:])
	case "status":
		runStatus(dsn)
	case "backup":
		runBackup(dsn)
	case "force":
		runForce(dsn, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func runUp(dsn string) {
	cfg := migrate.ConfigFromEnv()
	backupPath, err := migrate.UpWithBackup(dsn, cfg)
	if err != nil {
		log.Fatalf("migrate up: %v", err)
	}
	if backupPath != "" {
		log.Printf("backup saved: %s", backupPath)
	}
	log.Println("migrations applied")
}

func runDown(dsn string, args []string) {
	steps := 1
	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil || n <= 0 {
			log.Fatalf("invalid steps value: %q", args[0])
		}
		steps = n
	}

	mg, err := migrate.New(dsn)
	if err != nil {
		log.Fatalf("open migrator: %v", err)
	}
	defer mg.Close()

	if err := mg.Down(steps); err != nil {
		log.Fatalf("migrate down: %v", err)
	}
	log.Printf("rolled back %d migration(s)", steps)
}

func runStatus(dsn string) {
	mg, err := migrate.New(dsn)
	if err != nil {
		log.Fatalf("open migrator: %v", err)
	}
	defer mg.Close()

	st, err := mg.Status()
	if err != nil {
		log.Fatalf("migration status: %v", err)
	}
	if st.Version == 0 && !st.Dirty {
		fmt.Println("version: none")
		return
	}
	fmt.Printf("version: %d dirty: %t\n", st.Version, st.Dirty)
}

func runForce(dsn string, args []string) {
	if len(args) != 1 {
		log.Fatal("usage: migrate force <version>")
	}
	version, err := strconv.Atoi(args[0])
	if err != nil || version < 0 {
		log.Fatalf("invalid version: %q", args[0])
	}

	mg, err := migrate.New(dsn)
	if err != nil {
		log.Fatalf("open migrator: %v", err)
	}
	defer mg.Close()

	if err := mg.Force(version); err != nil {
		log.Fatalf("force version: %v", err)
	}
	log.Printf("forced schema version to %d", version)
}

func runBackup(dsn string) {
	cfg := migrate.ConfigFromEnv()
	path, err := migrate.Backup(dsn, cfg.BackupDir)
	if err != nil {
		log.Fatalf("backup: %v", err)
	}
	log.Printf("backup saved: %s", path)
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: migrate <command>

Commands:
  up              Apply pending migrations
  down [N]        Roll back N migrations (default 1)
  status          Show current schema version
  backup          Create pg_dump backup without migrating
  force VERSION   Set schema version without running SQL (recovery)

Environment:
  DB_DSN                  PostgreSQL connection string (required)
  MIGRATE_BACKUP_DIR      Backup directory (default: db/backups)
  MIGRATE_BACKUP_BEFORE   never|risky|always (default: risky)
`)
}
