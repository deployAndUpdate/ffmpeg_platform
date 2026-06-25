DROP INDEX IF EXISTS idx_jobs_idempotency_key;

ALTER TABLE jobs
    DROP COLUMN IF EXISTS max_duration_seconds,
    DROP COLUMN IF EXISTS idempotency_key,
    DROP COLUMN IF EXISTS lease_generation;
