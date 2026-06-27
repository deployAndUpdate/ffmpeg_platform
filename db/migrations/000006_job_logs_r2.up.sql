-- Job logs stored in R2; PostgreSQL keeps artifact metadata only.

CREATE TABLE job_log_artifacts (
    id         BIGSERIAL PRIMARY KEY,
    job_id     UUID NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt    INT NOT NULL,
    object_key TEXT NOT NULL,
    bytes      BIGINT NOT NULL DEFAULT 0,
    lines      INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (job_id, attempt)
);

CREATE INDEX idx_job_log_artifacts_job_id ON job_log_artifacts (job_id);

DROP TABLE IF EXISTS job_logs;
