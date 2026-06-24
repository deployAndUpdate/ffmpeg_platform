-- Object storage mode for jobs (local filesystem or R2/S3-compatible).
-- For R2 jobs, input_path and output_path hold object keys, not local paths.

ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS storage TEXT NOT NULL DEFAULT 'local'
CHECK (storage IN ('local', 'r2'));
