package api

import (
	"log"
	"os"
	"time"
)

const defaultJobLease = 30 * time.Minute
const defaultJobMaxDuration = 2 * time.Hour

// JobLeaseFromEnv returns JOB_LEASE_DURATION (default 30m).
func JobLeaseFromEnv() time.Duration {
	v := os.Getenv("JOB_LEASE_DURATION")
	if v == "" {
		return defaultJobLease
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid JOB_LEASE_DURATION: %q", v)
	}
	return d
}

// JobMaxDurationFromEnv returns JOB_DEFAULT_MAX_DURATION (default 2h).
func JobMaxDurationFromEnv() time.Duration {
	v := os.Getenv("JOB_DEFAULT_MAX_DURATION")
	if v == "" {
		return defaultJobMaxDuration
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid JOB_DEFAULT_MAX_DURATION: %q", v)
	}
	return d
}
