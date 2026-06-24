.PHONY: vet build test test-db-up test-db-down test-integration test-all ci

COMPOSE_TEST := docker-compose -f docker-compose.test.yml
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

test-integration: test-db-up
	TEST_DB_DSN=$(TEST_DB_DSN) go test -race -count=1 -p 1 -tags=integration ./internal/store/... ./internal/api/...

test-all: test test-integration

ci: vet build test test-integration
