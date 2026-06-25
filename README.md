# Distributed Video Compression Platform (Go)

Fault-tolerant scheduler and workers for running `ffmpeg` across multiple nodes. HTTP API (scheduler), CLI workers, embedded docs, and an admin dashboard for monitoring.

## Quick start

**Docker (recommended):**

```bash
cp .env.example .env
docker compose up --build -d
```

After startup:

- Documentation: [http://localhost:8080/docs/](http://localhost:8080/docs/) (EN / UA / RU)
- Admin dashboard: [http://localhost:8080/admin/](http://localhost:8080/admin/)

Optional: generate API keys with `./scripts/gen-api-keys.sh --format=env` and add them to `.env` (`SCHEDULER_API_KEY_REQUIRED=true` in production).

Scale workers:

```bash
docker compose up -d --scale worker=3
```

**Local (without containers):**

1. Start PostgreSQL and apply migrations: `export DB_DSN=...; make migrate-up`
2. Start the scheduler: `export DB_DSN=...; export SCHEDULER_ADDR=:8080; go run ./cmd/scheduler`
3. Start a worker (machine with ffmpeg and file access): `export SCHEDULER_URL=http://localhost:8080; go run ./cmd/worker`

For Cloudflare R2 object storage, set `R2_*` env vars on scheduler and workers (see `.env.example`).

## Architecture

- **Scheduler** (`cmd/scheduler`) — HTTP API: create jobs, register workers, heartbeats, dispatch jobs (`SELECT … FOR UPDATE SKIP LOCKED` + lease), lease renewal, finalize results. Background reaper for stale workers and expired leases.
- **Workers** (`cmd/worker`) — register, send heartbeats, poll for jobs, renew leases during ffmpeg, run transcode (with output skip when already valid), upload logs and results.
- **PostgreSQL** — single source of truth: `jobs`, `workers`, `job_logs`, lease TTL and `lease_generation` fencing.
- **R2 (optional)** — S3-compatible object storage for upload/download via presigned URLs; workers download input and upload output.
- **Presets** — named transcode profiles (`h264_crf23`, `mp3_192k`, …) resolved to `ffmpeg_args` at job creation.

Key principles: effectively-once delivery (lease renewal + output skip + `lease_generation`), idempotency via `Idempotency-Key` on `POST /jobs`, at-least-once as a safety net.

## Job lifecycle

```
NEW → QUEUED → RUNNING → COMPLETED
  ↑       ↑         ↓
  │       └── RETRY ← FAILED
  └── R2 flow only (after POST /jobs/init, before submit)
```

- Local jobs via `POST /jobs` (JSON) start as `QUEUED`.
- R2 jobs via `POST /jobs/init` start as `NEW` and move to `QUEUED` after `POST /jobs/{id}/submit`.
- `COMPLETED` is final; `FAILED` when `attempt ≥ max_attempts`; reaper returns stale `RUNNING` jobs to `RETRY` or `FAILED`.

## HTTP API (summary)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness probe (process up) |
| GET | `/ready` | Readiness probe (PostgreSQL + R2 if configured) |
| GET | `/presets` | List transcode presets |
| POST | `/jobs/init` | Create R2 job + presigned upload URL |
| POST | `/jobs/{id}/submit` | Confirm R2 upload, enqueue job |
| POST | `/jobs` | Create job (JSON local paths or multipart R2 upload) |
| GET | `/jobs/{id}` | Job status (+ `download_url` for completed R2 jobs) |
| GET | `/jobs/{id}/logs` | stdout/stderr logs |
| POST | `/workers/register` | Register or update a worker |
| POST | `/workers/heartbeat` | Worker heartbeat |
| POST | `/workers/request-job` | Acquire next job (SKIP LOCKED + lease) |
| POST | `/workers/renew-lease` | Extend lease while job is running |
| POST | `/workers/job-result` | Finalize job, store result and logs |
| GET | `/admin/api/*` | Read-only admin API (stats, jobs, workers) |
| GET | `/docs/` | Embedded documentation (EN / UA / RU) |
| GET | `/admin/` | Admin dashboard UI |

**Authentication:** when enabled (`SCHEDULER_API_KEY_REQUIRED` or any key set), protected routes require `Authorization: Bearer <key>` or `X-API-Key`. Roles: client (`/jobs*`, `/presets`), worker (`/workers/*`), admin (all protected routes + `/admin/api/*`). Public without key: `/health`, `/ready`, `/docs/`, `/admin/` UI shell.

Full request/response examples, env vars, Docker setup, and tests: see **[/docs/](http://localhost:8080/docs/)**.

## Trade-offs

- **Effectively-once** — lease renewal, valid-output skip, and `lease_generation` reduce duplicate ffmpeg runs; reaper remains a safety net.
- **Postgres queue** — no external broker; simpler MVP, limited throughput.
- **Logs in DB** — consistent and simple, heavier for large streams.
- **Resource model** — `max_parallel_jobs` today; GPU/CPU filters can be extended later.

## Tests

```bash
make test              # unit tests (no PostgreSQL)
make test-integration  # integration tests (docker-compose.test.yml, port 5433)
make test-all          # unit + integration
make ci                # vet, build, unit + integration
make migrate-up        # apply DB migrations
make migrate-backup    # pg_dump backup via cmd/migrate
```

## Known limitations

- GPU/CPU filters are not used when dispatching jobs
- At-least-once semantics — duplicate ffmpeg runs are still possible on failure or lease expiry
- Logs stored in PostgreSQL — not ideal for very large volumes

## Project layout

```
cmd/scheduler/     HTTP API server
cmd/worker/        ffmpeg worker
cmd/migrate/       database migrations CLI
internal/api/      HTTP handlers + auth
internal/ffmpeg/   ffmpeg wrapper
internal/migrate/  migrator wrapper (golang-migrate)
internal/presets/  transcode preset catalog
internal/reaper/   background reaper loop
internal/storage/  Cloudflare R2 (S3 API)
internal/store/    PostgreSQL layer
internal/worker/   worker client and loop
web/docs/          embedded HTML documentation
web/admin/         admin dashboard UI
scripts/           gen-api-keys.sh, submit-jobs.sh
db/migrations/     versioned SQL (.up.sql / .down.sql)
```
