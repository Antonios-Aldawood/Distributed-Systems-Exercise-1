package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	// Switch between algorithms via LB_ALGO=least-connections env var.
	algo := RoundRobin
	if os.Getenv("LB_ALGO") == "least-connections" {
		algo = LeastConnections
	}

	// One LoadBalancer per service.
	// product-service has TWO backend URLs — we run two instances on different
	// ports to demonstrate load balancing. user-service and order-service each
	// have one instance (single backend, but still goes through CB and retry).
	userLB := NewLoadBalancer(
		[]string{"http://localhost:8081"},
		algo,
	)
	productLB := NewLoadBalancer(
		[]string{
			"http://localhost:8082", // product-service instance A
			"http://localhost:8083", // product-service instance B (can have LATENCY_MS set)
		},
		algo,
	)
	orderLB := NewLoadBalancer(
		[]string{"http://localhost:8084"},
		algo,
	)

	// Health checks run every 10 s in the background.
	// They are independent of the circuit breaker:
	//   - Health checks = proactive polling ("are you alive?")
	//   - Circuit breaker = reactive pattern ("you've been failing my requests")
	// Both are necessary. Health checks catch a service that went down
	// silently; the CB handles a service that is alive but misbehaving.
	userLB.StartHealthChecks(10 * time.Second)
	productLB.StartHealthChecks(10 * time.Second)
	orderLB.StartHealthChecks(10 * time.Second)

	userProxy := &Proxy{
		lb:         userLB,
		timeout:    5 * time.Second,
		maxRetries: 2,
	}
	productProxy := &Proxy{
		lb:         productLB,
		timeout:    5 * time.Second,
		maxRetries: 2,
	}
	// Order service gets a longer budget because it makes its own downstream
	// call to product-service, which can take up to 3 s.
	orderProxy := &Proxy{
		lb:         orderLB,
		timeout:    10 * time.Second,
		maxRetries: 2,
	}

	mux := http.NewServeMux()

	// Route table.
	// Go 1.22 ServeMux distinguishes method-qualified patterns (exact match)
	// from plain prefix patterns (trailing slash = prefix).
	// We register both because POST /users has no trailing slash.
	mux.Handle("POST /users", userProxy)
	mux.Handle("/users/", userProxy)

	mux.Handle("POST /products", productProxy)
	mux.Handle("/products/", productProxy)

	mux.Handle("POST /orders", orderProxy)
	mux.Handle("/orders/", orderProxy)

	// Gateway self-health.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "gateway"})
	})

	// /gateway/status — real-time view of all backends.
	// Hit this in a browser or with curl while running load to watch the load
	// balancer distribute connections and the circuit breaker change state.
	mux.HandleFunc("GET /gateway/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"algorithm": algo.String(),
			"services": map[string]any{
				"user":    userLB.Status(),
				"product": productLB.Status(),
				"order":   orderLB.Status(),
			},
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	slog.Info("gateway up", "port", port, "lb_algorithm", algo.String())
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}
