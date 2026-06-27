CREATE TABLE job_logs (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr')),
    line TEXT NOT NULL
);

CREATE INDEX idx_job_logs_job_id_ts ON job_logs (job_id, ts);

DROP TABLE IF EXISTS job_log_artifacts;
