package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"
)

//go:embed static/index.html
var indexHTML []byte

func main() {
	algo := RoundRobin
	if a, ok := ParseAlgorithm(os.Getenv("LB_ALGO")); ok {
		algo = a
	}

	userLB := NewLoadBalancer([]BackendConfig{
		{URL: "http://localhost:8081", Weight: 1},
	}, algo)

	productLB := NewLoadBalancer([]BackendConfig{
		{URL: "http://localhost:8082", Weight: 3},
		{URL: "http://localhost:8083", Weight: 1},
		{URL: "http://localhost:8085", Weight: 2},
		{URL: "http://localhost:8086", Weight: 3},
	}, algo)

	orderLB := NewLoadBalancer([]BackendConfig{
		{URL: "http://localhost:8084", Weight: 1},
	}, algo)

	// 5 s interval: with CB threshold=3, a sustained failure opens the circuit
	// in ~15 s — fast enough to observe in a demo without being noisy.
	userLB.StartHealthChecks(5 * time.Second)
	productLB.StartHealthChecks(5 * time.Second)
	orderLB.StartHealthChecks(5 * time.Second)

	userProxy    := &Proxy{lb: userLB,    timeout: 5 * time.Second,  maxRetries: 2}
	productProxy := &Proxy{lb: productLB, timeout: 5 * time.Second,  maxRetries: 2}
	orderProxy   := &Proxy{lb: orderLB,   timeout: 10 * time.Second, maxRetries: 2}

	mux := http.NewServeMux()

	// ── Service routes ────────────────────────────────────────────────────────
	mux.Handle("POST /users", userProxy)
	mux.Handle("/users/", userProxy)
	mux.Handle("POST /products", productProxy)
	mux.Handle("/products/", productProxy)
	mux.Handle("POST /orders", orderProxy)
	mux.Handle("/orders/", orderProxy)

	// ── Observability ─────────────────────────────────────────────────────────

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "gateway"})
	})

	mux.HandleFunc("GET /gateway/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"algorithm": productLB.GetAlgorithm().String(),
			"services": map[string]any{
				"user":    userLB.Status(),
				"product": productLB.Status(),
				"order":   orderLB.Status(),
			},
		})
	})

	// GET /events — Server-Sent Events stream.
	// The browser subscribes once; the gateway pushes health-probe results and
	// circuit-breaker state transitions in real time without polling.
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "SSE not supported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Immediate confirmation so the browser knows the stream is live.
		fmt.Fprintf(w, "data: %s\n\n",
			Event{Type: "connected", Message: "SSE stream connected"}.JSON())
		flusher.Flush()

		ch := globalBus.Subscribe()
		defer globalBus.Unsubscribe(ch)

		// SSE comment lines (":" prefix) are invisible to onmessage but keep the
		// TCP connection alive and prevent intermediate proxies from closing it.
		keepalive := time.NewTicker(15 * time.Second)
		defer keepalive.Stop()

		for {
			select {
			case evt := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", evt.JSON())
				flusher.Flush()
			case <-keepalive.C:
				fmt.Fprintf(w, ": keepalive\n\n")
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	})

	// ── Runtime controls ──────────────────────────────────────────────────────

	// POST /gateway/algorithm {"algorithm": "least-connections"}
	mux.HandleFunc("POST /gateway/algorithm", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Algorithm string `json:"algorithm"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		a, ok := ParseAlgorithm(req.Algorithm)
		if !ok {
			http.Error(w, "unknown algorithm", http.StatusBadRequest)
			return
		}
		userLB.SetAlgorithm(a)
		productLB.SetAlgorithm(a)
		orderLB.SetAlgorithm(a)
		slog.Info("algorithm switched", "algorithm", a.String())
		globalBus.Publish(Event{
			Type:    "admin",
			Message: fmt.Sprintf("load-balancing algorithm switched to %q", a.String()),
		})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"algorithm": a.String()})
	})

	// POST /admin/latency {"port": "8083", "ms": 400}
	// Forwards directly to the named backend's /admin/latency endpoint, bypassing
	// the load balancer — you're targeting a specific instance, not a service group.
	// Allowed ports are the four product-service instances.
	mux.HandleFunc("POST /admin/latency", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Port string `json:"port"`
			Ms   int64  `json:"ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		allowed := map[string]bool{
			"8082": true, "8083": true, "8085": true, "8086": true,
		}
		if !allowed[req.Port] {
			http.Error(w, "port must be one of: 8082 8083 8085 8086", http.StatusBadRequest)
			return
		}
		if req.Ms < 0 {
			req.Ms = 0
		}

		target := fmt.Sprintf("http://localhost:%s/admin/latency", req.Port)
		body, _ := json.Marshal(map[string]int64{"ms": req.Ms})
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, target,
			bytes.NewReader(body))
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			http.Error(w, "cannot reach service: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		slog.Info("latency set on backend", "port", req.Port, "ms", req.Ms)
		globalBus.Publish(Event{
			Type:    "admin",
			Message: fmt.Sprintf("latency set to %dms", req.Ms),
			Backend: "http://localhost:" + req.Port,
			Ms:      req.Ms,
		})

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	})

	// ── Simulation ────────────────────────────────────────────────────────────
	mux.Handle("POST /simulate/flood", handleFlood(productLB))
	mux.Handle("POST /simulate/warmup", handleWarmup(productLB))

	// ── Frontend ──────────────────────────────────────────────────────────────
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	slog.Info("gateway up", "port", port, "algorithm", productLB.GetAlgorithm().String())
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}
