-- Lease generation (fencing), idempotency keys, per-job max duration

ALTER TABLE jobs
    ADD COLUMN lease_generation BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN idempotency_key TEXT,
    ADD COLUMN max_duration_seconds INTEGER NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX idx_jobs_idempotency_key ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
