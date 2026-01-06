# Distributed Video Compression Platform (Go)

Набросок MVP отказоустойчивого планировщика и воркеров для запуска `ffmpeg` на нескольких узлах. UI отсутствует, только HTTP API (Scheduler) и CLI/воркеры.

## Архитектура (логика)
- Scheduler (HTTP API): принимает и выдает задания, отслеживает heartbeat воркеров, перевывает зависшие задания.
- Workers: регистрируются, шлют heartbeat, запрашивают job, запускают `ffmpeg`, отправляют логи и результат.
- PostgreSQL: единственный источник истины (jobs, workers, job_logs, lease TTL).
- Очередь: `SELECT ... FOR UPDATE SKIP LOCKED` для выдачи job без внешнего брокера.

## Состояния Job
```
NEW → QUEUED → RUNNING → COMPLETED
              ↓
           FAILED → RETRY (если attempt < max_attempts)
```
Инварианты: RUNNING только на одном worker; COMPLETED финально; FAILED может уйти в RETRY при лимите попыток.

## Основные таблицы
- `workers`: ресурсы, статус (ACTIVE/DEAD), heartbeat.
- `jobs`: вход/выход, ffmpeg_args, статус, попытки, lease_expires_at, assigned_worker_id.
- `job_logs`: stdout/stderr с таймштампами.

## API (минимальный контур)
- `POST /jobs` — создать job.
- `GET /jobs/{id}` — статус job.
- `GET /jobs/{id}/logs` — логи.
- `POST /workers/register` — регистрация воркера.
- `POST /workers/heartbeat` — heartbeat.
- `POST /workers/request-job` — выдача job (SKIP LOCKED + lease).
- `POST /workers/job-result` — финализация job, запись результата и логов.

## Компромиссы
- At-least-once вместо exactly-once; возможны повторные выполнения.
- Очередь в Postgres без отдельного брокера — проще для MVP, ограниченная пропускная способность.
- Логи в БД — консистентно и просто, но тяжелее для больших потоков.
- Простая модель ресурсов: пока `max_parallel_jobs`, CPU/GPU фильтры можно расширять позже.

## Известные проблемы / TODO
- Хендлеры API пока заглушки (нет бизнес-логики и валидации).
- Воркеры не реализованы; нет запуска `ffmpeg`.
- Нет фонового reaper'а для DEAD воркеров и истекших lease.
- Нет мигратора; sql-файл лежит в `db/migrations/001_init.sql`.

## Запуск (набросок)
1. Поднять PostgreSQL, получить `DB_DSN`, применить миграцию из `db/migrations/001_init.sql`.
2. `export DB_DSN=...; export SCHEDULER_ADDR=:8080`
3. `go run ./cmd/scheduler`
4. Запуск воркеров будет добавлен позже (`cmd/worker`).



