.PHONY: vet build test test-db-up test-db-down test-integration test-all ci migrate-up migrate-down migrate-status migrate-backup

COMPOSE := docker-compose
COMPOSE_TEST := docker-compose -f docker-compose.test.yml
DB_DSN ?= postgres://video:video@127.0.0.1:5432/video?sslmode=disable
TEST_DB_DSN ?= postgres://video_test:video_test@127.0.0.1:5433/video_test?sslmode=disable

vet:
	go vet ./...

build:
	go build -trimpath ./...

test:
	go test -race -count=1 -short ./...

test-db-up:
	$(COMPOSE_TEST) up -d --wait

test-db-down:
	$(COMPOSE_TEST) down -v

migrate-up:
	DB_DSN=$(DB_DSN) go run ./cmd/migrate up

migrate-down:
	DB_DSN=$(DB_DSN) go run ./cmd/migrate down $(STEPS)

migrate-status:
	DB_DSN=$(DB_DSN) go run ./cmd/migrate status

migrate-backup:
	DB_DSN=$(DB_DSN) go run ./cmd/migrate backup

test-integration: test-db-up
	TEST_DB_DSN=$(TEST_DB_DSN) go test -race -count=1 -p 1 -tags=integration ./internal/migrate/... ./internal/store/... ./internal/api/... ./internal/worker/... ./internal/reaper/...

test-all: test test-integration

ci: vet build test test-integration
