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
	db, err := sql.Open("sqlite", "products.db")
	if err != nil {
		slog.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA busy_timeout=5000")

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS products (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		name       TEXT NOT NULL,
		price      REAL NOT NULL,
		created_at TEXT NOT NULL
	)`)
	if err != nil {
		slog.Error("create table", "err", err)
		os.Exit(1)
	}

	s := &store{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /products", s.handleCreate)
	mux.HandleFunc("GET /products/{id}", s.handleGet)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8082"
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "product", "port": port})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	slog.Info("product-service up", "port", port)
	if err := http.ListenAndServe(":"+port, withLatency(mux)); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

func withLatency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ms, err := strconv.Atoi(os.Getenv("LATENCY_MS")); err == nil && ms > 0 {
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		next.ServeHTTP(w, r)
	})
}
