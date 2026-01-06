package main

import (
	"log"
	"net/http"
	"os"

	"go_distributed_system/internal/api"
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

	server := api.NewServer(st)

	log.Printf("scheduler listening on %s", addr)
	if err := http.ListenAndServe(addr, server); err != nil {
		log.Fatalf("http server error: %v", err)
	}
}

