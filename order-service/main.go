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
	db, err := sql.Open("sqlite", "orders.db")
	if err != nil {
		slog.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA busy_timeout=5000")

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id    INTEGER NOT NULL,
			status     TEXT NOT NULL DEFAULT 'pending',
			created_at TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS order_items (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			order_id     INTEGER NOT NULL,
			product_id   INTEGER NOT NULL,
			quantity     INTEGER NOT NULL,
			price_at_time REAL NOT NULL,
			FOREIGN KEY (order_id) REFERENCES orders(id)
		);
	`)
	if err != nil {
		slog.Error("create tables", "err", err)
		os.Exit(1)
	}

	s := &orderStore{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.handleCreate)
	mux.HandleFunc("GET /orders/{id}", s.handleGet)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "order"})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8084"
	}
	slog.Info("order-service up", "port", port)
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
