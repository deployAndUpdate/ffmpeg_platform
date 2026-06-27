-- RabbitMQ dispatch: outbox table and DISPATCHED status (replaces QUEUED/RETRY)

UPDATE jobs SET status = 'DISPATCHED' WHERE status IN ('QUEUED', 'RETRY');

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('NEW', 'DISPATCHED', 'RUNNING', 'COMPLETED', 'FAILED'));

CREATE TABLE IF NOT EXISTS job_outbox (
    id            BIGSERIAL PRIMARY KEY,
    job_id        UUID NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    queue_target  TEXT NOT NULL CHECK (queue_target IN ('main', 'retry')),
    attempt       INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ,
    publish_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_outbox_unpublished ON job_outbox (created_at)
    WHERE published_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_job_outbox_job_id ON job_outbox (job_id);
