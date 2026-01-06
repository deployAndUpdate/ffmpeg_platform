-- Base schema for distributed video compression platform
-- Idempotency note: rely on migration tool to run once; no IF NOT EXISTS

-- Worker status and job status kept as constrained text to avoid custom enum migrations

CREATE TABLE workers (
    id UUID PRIMARY KEY,
    hostname TEXT NOT NULL,
    cpu_cores INTEGER NOT NULL,
    gpu_available BOOLEAN NOT NULL DEFAULT FALSE,
    max_parallel_jobs INTEGER NOT NULL DEFAULT 1,
    last_heartbeat_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'DEAD')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    input_path TEXT NOT NULL,
    output_path TEXT NOT NULL,
    ffmpeg_args TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('NEW', 'QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'RETRY')),
    assigned_worker_id UUID REFERENCES workers (id),
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE job_logs (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr')),
    line TEXT NOT NULL
);

-- Useful indexes
CREATE INDEX idx_jobs_status ON jobs (status);
CREATE INDEX idx_jobs_assigned_worker ON jobs (assigned_worker_id);
CREATE INDEX idx_jobs_lease ON jobs (lease_expires_at);
CREATE INDEX idx_job_logs_job_id_ts ON job_logs (job_id, ts);

-- Simple trigger to keep updated_at fresh
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_workers_updated_at
BEFORE UPDATE ON workers
FOR EACH ROW
EXECUTE PROCEDURE set_updated_at();

CREATE TRIGGER trg_jobs_updated_at
BEFORE UPDATE ON jobs
FOR EACH ROW
EXECUTE PROCEDURE set_updated_at();


