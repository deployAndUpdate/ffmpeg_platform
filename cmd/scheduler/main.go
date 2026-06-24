package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"go_distributed_system/internal/api"
	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/store"
)

func main() {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("DB_DSN is required")
	}

	addr := os.Getenv("SCHEDULER_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	st, err := store.New(dsn)
	if err != nil {
		log.Fatalf("init store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Printf("close store: %v", err)
		}
	}()

	var handler http.Handler
	if cfg, ok := storage.ConfigFromEnv(); ok {
		obj, err := storage.NewR2(cfg)
		if err != nil {
			log.Fatalf("init R2 storage: %v", err)
		}
		log.Printf("R2 storage enabled (bucket=%s)", obj.Bucket())
		handler = api.NewServerWithStorage(st, obj, cfg)
	} else {
		log.Printf("R2 storage not configured — only local-path JSON jobs are available")
		handler = api.NewServer(st)
	}

	uploadTimeout := api.UploadTimeoutFromEnv()
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       uploadTimeout + 30*time.Second,
		WriteTimeout:      uploadTimeout + 30*time.Second,
	}

	log.Printf("scheduler listening on %s (upload timeout %s, max upload %d bytes)",
		addr, uploadTimeout, api.MaxUploadBytesFromEnv())
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("http server error: %v", err)
	}
}
