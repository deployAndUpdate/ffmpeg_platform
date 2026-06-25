DROP TRIGGER IF EXISTS trg_jobs_updated_at ON jobs;
DROP TRIGGER IF EXISTS trg_workers_updated_at ON workers;
DROP FUNCTION IF EXISTS set_updated_at();
DROP TABLE IF EXISTS job_logs;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS workers;
