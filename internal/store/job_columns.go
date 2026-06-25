package store

const jobSelectColumns = `
id, input_path, output_path, preset, ffmpeg_args, storage, status, assigned_worker_id,
attempt, max_attempts, lease_expires_at, lease_generation, idempotency_key, max_duration_seconds,
created_at, started_at, finished_at, updated_at`

// jobReturningColumns prefixes columns for UPDATE ... FROM (avoids ambiguous "id").
const jobReturningColumns = `
j.id, j.input_path, j.output_path, j.preset, j.ffmpeg_args, j.storage, j.status, j.assigned_worker_id,
j.attempt, j.max_attempts, j.lease_expires_at, j.lease_generation, j.idempotency_key, j.max_duration_seconds,
j.created_at, j.started_at, j.finished_at, j.updated_at`
