//go:build integration

package e2e

import "time"

const (
	relayInterval     = 50 * time.Millisecond
	relayBatch        = 10
	reaperInterval    = time.Second
	reaperWorkerTTL   = 30 * time.Second
	workerHeartbeat   = 2 * time.Second
	workerLeaseRenew  = 5 * time.Second
	workerJobLease    = 5 * time.Minute
	workerMaxDuration = 10 * time.Minute
	// DefaultJobTimeout is the maximum wait for a job to reach a terminal HTTP status.
	DefaultJobTimeout = 5 * time.Minute
	pollInterval      = 200 * time.Millisecond
)
