package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"go_distributed_system/internal/queue"
	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/worker"

	"github.com/google/uuid"
)

func main() {
	schedulerURL := os.Getenv("SCHEDULER_URL")
	if schedulerURL == "" {
		log.Fatal("SCHEDULER_URL is required, e.g. http://localhost:8080")
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL is required")
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = uuid.New().String()
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("hostname: %v", err)
	}
	if envHost := os.Getenv("WORKER_HOSTNAME"); envHost != "" {
		hostname = envHost
	}

	cpuCores := runtime.NumCPU()
	if v := os.Getenv("WORKER_CPU_CORES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("invalid WORKER_CPU_CORES: %q", v)
		}
		cpuCores = n
	}

	maxParallel := 1
	if v := os.Getenv("WORKER_MAX_PARALLEL_JOBS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("invalid WORKER_MAX_PARALLEL_JOBS: %q", v)
		}
		maxParallel = n
	}

	gpuAvailable := os.Getenv("WORKER_GPU_AVAILABLE") == "true" || os.Getenv("WORKER_GPU_AVAILABLE") == "1"

	cfg := worker.Config{
		ID:                    workerID,
		Hostname:              hostname,
		CPUCores:              cpuCores,
		GPUAvailable:          gpuAvailable,
		MaxParallelJobs:       maxParallel,
		SchedulerURL:          schedulerURL,
		SchedulerAPIKey:       os.Getenv("SCHEDULER_WORKER_API_KEY"),
		RabbitMQURL:           rabbitURL,
		TempDir:               os.Getenv("WORKER_TEMP_DIR"),
		HeartbeatEvery:        envDuration("WORKER_HEARTBEAT_INTERVAL", 10*time.Second),
		LeaseRenewInterval:    envDuration("WORKER_LEASE_RENEW_INTERVAL", 0),
		JobLeaseDuration:      envDuration("WORKER_JOB_LEASE_DURATION", envDuration("JOB_LEASE_DURATION", 30*time.Minute)),
		DefaultMaxJobDuration: envDuration("WORKER_DEFAULT_MAX_DURATION", envDuration("JOB_DEFAULT_MAX_DURATION", 2*time.Hour)),
	}

	rabbitCfg := queue.RabbitConfig{
		URL:      rabbitURL,
		Prefetch: maxParallel,
	}
	rabbit, err := queue.NewRabbitWithRetry(rabbitCfg, 30, 500*time.Millisecond)
	if err != nil {
		log.Fatalf("init rabbit: %v", err)
	}
	defer func() {
		if err := rabbit.Close(); err != nil {
			log.Printf("close rabbit: %v", err)
		}
	}()

	var objStorage storage.ObjectStorage
	if _, ok := storage.ConfigFromEnv(); ok {
		obj, err := storage.NewFromEnv()
		if err != nil {
			log.Fatalf("init R2 storage: %v", err)
		}
		objStorage = obj
		log.Printf("R2 storage enabled (bucket=%s)", obj.Bucket())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	w := worker.New(cfg, objStorage, rabbit)
	log.Printf("starting worker %s (%s), parallel=%d", cfg.ID, cfg.Hostname, cfg.MaxParallelJobs)
	if err := w.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("worker stopped: %v", err)
	}
	log.Println("worker stopped")
}

func envDuration(name string, fallback time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid %s: %q", name, v)
	}
	return d
}
