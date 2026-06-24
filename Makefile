.PHONY: vet build test test-integration test-all ci

vet:
	go vet ./...

build:
	go build -trimpath ./...

test:
	go test -race -count=1 -short ./...

test-integration:
	DB_DSN=$${DB_DSN:-postgres://video:video@localhost:5432/video_test?sslmode=disable} \
	go test -race -count=1 -p 1 -tags=integration ./internal/store/... ./internal/api/...

test-all: test test-integration

ci: vet build test test-integration
