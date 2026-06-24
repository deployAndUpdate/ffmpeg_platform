# Distributed Video Compression Platform (Go)

Fault-tolerant scheduler and workers for running `ffmpeg` across multiple nodes. No UI — only an HTTP API (scheduler) and CLI workers.

## Quick start

**Docker (recommended):**

```bash
cp .env.example .env
docker compose up --build -d
```

Documentation: [http://localhost:8080/docs/](http://localhost:8080/docs/) (EN / UA / RU)

**Local (without containers):**

1. Start PostgreSQL and apply the migration: `psql "$DB_DSN" -f db/migrations/001_init.sql`
2. Start the scheduler: `export DB_DSN=...; export SCHEDULER_ADDR=:8080; go run ./cmd/scheduler`
3. Start a worker (machine with ffmpeg and file access): `export SCHEDULER_URL=http://localhost:8080; go run ./cmd/worker`

## Architecture

- **Scheduler** (`cmd/scheduler`) — HTTP API: create jobs, register workers, heartbeats, dispatch jobs (`SELECT … FOR UPDATE SKIP LOCKED` + lease), finalize results.
- **Workers** (`cmd/worker`) — register, send heartbeats, poll for jobs, run ffmpeg, upload logs and results.
- **PostgreSQL** — single source of truth: `jobs`, `workers`, `job_logs`, lease TTL.

## Job lifecycle

```
QUEUED → RUNNING → COMPLETED
              ↓
           FAILED → RETRY (if attempt < max_attempts)
```

Invariants: a job is `RUNNING` on at most one worker; `COMPLETED` is final; `FAILED` may transition to `RETRY` while attempts remain.

## HTTP API (summary)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/jobs` | Create a job |
| GET | `/jobs/{id}` | Job status |
| GET | `/jobs/{id}/logs` | stdout/stderr logs |
| POST | `/workers/register` | Register or update a worker |
| POST | `/workers/heartbeat` | Worker heartbeat |
| POST | `/workers/request-job` | Acquire next job (SKIP LOCKED + lease) |
| POST | `/workers/job-result` | Finalize job, store result and logs |
| GET | `/docs/` | This documentation (embedded HTML) |

Full request/response examples, env vars, Docker setup, and tests: see **[/docs/](http://localhost:8080/docs/)**.

## Trade-offs

- **At-least-once** instead of exactly-once — duplicate ffmpeg runs are possible on failure or lease expiry.
- **Postgres queue** — no external broker; simpler MVP, limited throughput.
- **Logs in DB** — consistent and simple, heavier for large streams.
- **Resource model** — `max_parallel_jobs` today; CPU/GPU filters can be extended later.

## Tests

```bash
make test              # unit tests (no PostgreSQL)
make test-integration  # integration tests (docker-compose.test.yml, port 5433)
make ci                # full local CI suite
```

## Known limitations

- No background reaper for dead workers and expired leases
- No standalone migration tool (Docker applies init SQL on first postgres start)
- GPU/CPU filters are not used when dispatching jobs

## Project layout

```
cmd/scheduler/   HTTP API server
cmd/worker/      ffmpeg worker
internal/api/    HTTP handlers
internal/store/  PostgreSQL layer
internal/worker/ worker client and loop
internal/ffmpeg/ ffmpeg wrapper
web/docs/        embedded HTML documentation
db/migrations/   SQL schema
```
