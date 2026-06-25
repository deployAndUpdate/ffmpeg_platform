# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/scheduler ./cmd/scheduler

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

# --- Scheduler runtime (HTTP API, без ffmpeg) ---

FROM alpine:3.20 AS scheduler

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S app \
    && adduser -S app -G app

COPY --from=builder /out/scheduler /usr/local/bin/scheduler

USER app

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q --spider http://127.0.0.1:8080/ready || exit 1

ENTRYPOINT ["/usr/local/bin/scheduler"]

# --- Migrate runtime (one-shot schema migrations) ---

FROM alpine:3.20 AS migrate

RUN apk add --no-cache ca-certificates postgresql-client \
    && addgroup -S app \
    && adduser -S app -G app

COPY --from=builder /out/migrate /usr/local/bin/migrate

USER app

ENTRYPOINT ["/usr/local/bin/migrate"]
CMD ["up"]

# --- Worker runtime (ffmpeg + общий том /data) ---

FROM alpine:3.20 AS worker

RUN apk add --no-cache ca-certificates ffmpeg su-exec \
    && addgroup -S app \
    && adduser -S app -G app \
    && mkdir -p /data \
    && chown app:app /data

COPY --from=builder /out/worker /usr/local/bin/worker
COPY docker/worker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

WORKDIR /data
VOLUME ["/data"]

ENTRYPOINT ["/entrypoint.sh"]
