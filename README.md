# Distributed Video Compression Platform (Go)

Fault-tolerant scheduler and workers for running `ffmpeg` across multiple nodes. HTTP API (scheduler), RabbitMQ dispatch (transactional outbox), CLI workers, embedded docs, and an admin dashboard for monitoring.

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

1. Start PostgreSQL, RabbitMQ, and apply migrations: `export DB_DSN=...; make migrate-up`
2. Start the scheduler: `export DB_DSN=...; export RABBITMQ_URL=amqp://guest:guest@localhost:5672/; export SCHEDULER_ADDR=:8080; go run ./cmd/scheduler`
3. Start a worker: `export SCHEDULER_URL=http://localhost:8080; export RABBITMQ_URL=amqp://guest:guest@localhost:5672/; go run ./cmd/worker`

For Cloudflare R2 object storage, set `R2_*` env vars on scheduler and workers (see `.env.example`).

## Architecture

- **Scheduler** (`cmd/scheduler`) — HTTP API, transactional outbox → RabbitMQ relay, background reaper for stale workers and expired leases.
- **Workers** (`cmd/worker`) — AMQP consumer, claim job via HTTP, renew leases during ffmpeg, submit results.
- **RabbitMQ** — dispatch transport (`jobs.main`, retry queue, DLQ).
- **PostgreSQL** — source of truth: `jobs`, `job_outbox`, `workers`, `job_log_artifacts`, lease TTL and `lease_generation`. Job log bodies are stored in R2.
- **R2 (optional)** — S3-compatible object storage for upload/download via presigned URLs.

## Job lifecycle

```
NEW → DISPATCHED → RUNNING → COMPLETED
  ↑        ↑           ↓
  R2 only  outbox retry  FAILED (attempt ≥ max_attempts)
```

- Local jobs via `POST /jobs` (JSON) → `DISPATCHED` + outbox → RabbitMQ.
- R2 jobs via `POST /jobs/init` start as `NEW` and move to `DISPATCHED` after `POST /jobs/{id}/submit`.
- Reaper returns orphan `RUNNING` jobs to `DISPATCHED` + outbox retry or `FAILED`.

## HTTP API (summary)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness probe |
| GET | `/ready` | Readiness (PostgreSQL + RabbitMQ + R2 if configured) |
| POST | `/jobs` | Create job (JSON local or multipart R2) |
| POST | `/workers/claim-job` | Claim job by `job_id` (after AMQP delivery) |
| POST | `/workers/job-result` | Finalize job |

Full API: see **[/docs/](http://localhost:8080/docs/)**.

## Tests

```bash
make test              # unit tests
make test-integration  # PostgreSQL + RabbitMQ (docker-compose.test.yml)
make test-all
make ci
```

## Project layout

```
internal/queue/     RabbitMQ publisher/consumer
internal/outbox/    Transactional outbox relay
internal/store/     PostgreSQL + outbox + claim
cmd/scheduler/      HTTP API + relay + reaper
cmd/worker/         AMQP consumer + ffmpeg
```
