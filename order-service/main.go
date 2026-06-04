package main

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

var latencyMs atomic.Int64

func main() {
	if ms, err := strconv.Atoi(os.Getenv("LATENCY_MS")); err == nil && ms >= 0 {
		latencyMs.Store(int64(ms))
	}

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
			id            INTEGER PRIMARY KEY AUTOINCREMENT,
			order_id      INTEGER NOT NULL,
			product_id    INTEGER NOT NULL,
			quantity      INTEGER NOT NULL,
			price_at_time REAL NOT NULL,
			FOREIGN KEY (order_id) REFERENCES orders(id)
		);
	`)
	if err != nil {
		slog.Error("create tables", "err", err)
		os.Exit(1)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8084"
	}

	s := &orderStore{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.handleCreate)
	mux.HandleFunc("GET /orders/{id}", s.handleGet)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status": "ok", "service": "order",
			"port": port, "latency_ms": latencyMs.Load(),
		})
	})
	mux.HandleFunc("POST /admin/latency", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Ms int64 `json:"ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json")
			return
		}
		if req.Ms < 0 {
			req.Ms = 0
		}
		latencyMs.Store(req.Ms)
		slog.Info("latency updated", "ms", req.Ms)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ms": req.Ms, "port": port})
	})

	slog.Info("order-service up", "port", port)
	if err := http.ListenAndServe(":"+port, withLatency(mux)); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

func withLatency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/latency" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		if ms := latencyMs.Load(); ms > 0 {
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		next.ServeHTTP(w, r)
	})
}
