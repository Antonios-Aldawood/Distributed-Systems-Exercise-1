package main

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "users.db")
	if err != nil {
		slog.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	// WAL mode: readers and writers don't block each other.
	// busy_timeout: retry for up to 5 s instead of failing immediately on lock.
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA busy_timeout=5000")

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		name       TEXT NOT NULL,
		email      TEXT NOT NULL UNIQUE,
		created_at TEXT NOT NULL
	)`)
	if err != nil {
		slog.Error("create table", "err", err)
		os.Exit(1)
	}

	s := &store{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /users", s.handleCreate)
	mux.HandleFunc("GET /users/{id}", s.handleGet)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "user"})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	slog.Info("user-service up", "port", port)
	if err := http.ListenAndServe(":"+port, withLatency(mux)); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

// withLatency injects artificial delay when LATENCY_MS is set.
// This lets us simulate slow services to observe timeout and circuit-breaker behaviour.
func withLatency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ms, err := strconv.Atoi(os.Getenv("LATENCY_MS")); err == nil && ms > 0 {
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		next.ServeHTTP(w, r)
	})
}
