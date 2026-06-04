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

// latencyMs is the artificial delay injected on every request.
// Initialised from LATENCY_MS env var but writable at runtime via
// POST /admin/latency so the frontend can change it without restarting.
var latencyMs atomic.Int64

func main() {
	// Seed latency from environment variable.
	if ms, err := strconv.Atoi(os.Getenv("LATENCY_MS")); err == nil && ms >= 0 {
		latencyMs.Store(int64(ms))
	}

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

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	s := &store{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /products", s.handleCreate)
	mux.HandleFunc("GET /products/{id}", s.handleGet)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":     "ok",
			"service":    "product",
			"port":       port,
			"latency_ms": latencyMs.Load(),
		})
	})

	// POST /admin/latency {"ms": 400}
	// Lets the gateway (and the frontend via the gateway) change the simulated
	// latency of this specific instance at runtime.
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
		slog.Info("latency updated", "ms", req.Ms, "port", port)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ms": req.Ms, "port": port})
	})

	slog.Info("product-service up", "port", port, "latency_ms", latencyMs.Load())
	if err := http.ListenAndServe(":"+port, withLatency(mux)); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

// withLatency reads the atomic latencyMs on every request so changes made via
// /admin/latency take effect immediately without a restart.
func withLatency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip latency for the admin control call and for health probes.
		// Health probes must respond quickly so the health-check poller (2 s timeout)
		// is not fooled by artificial latency — the service is genuinely alive even
		// when we are simulating slowness on the data plane.
		// The circuit breaker is tripped separately via actual timed-out requests.
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
