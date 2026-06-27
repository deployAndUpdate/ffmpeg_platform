DROP TABLE IF EXISTS job_outbox;

UPDATE jobs SET status = 'QUEUED' WHERE status = 'DISPATCHED';

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('NEW', 'QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'RETRY'));
