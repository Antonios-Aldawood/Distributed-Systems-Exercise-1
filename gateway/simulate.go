package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type floodReq struct {
	Count     int    `json:"count"`
	ProductID int64  `json:"product_id"`
	ClientIP  string `json:"client_ip"` // override for IP-Hash demos; empty = use real IP
}

type floodEntry struct {
	Backend    string `json:"backend"`
	DurationMs int64  `json:"duration_ms"`
	Status     int    `json:"status"`
}

// handleFlood fires Count GET /products/{id} requests simultaneously against
// the product load balancer and returns a full per-request trace plus a
// per-backend distribution summary.
//
// The barrier channel ensures ALL goroutines start at the exact same instant.
// This is what makes the Least-Connections demo work: with a 20-goroutine burst,
// the slow backend (:8083) immediately accumulates active connections while
// the fast backends complete and free up. Least-Connections sees this and routes
// subsequent picks away from the slow one. Round-Robin distributes blindly.
func handleFlood(productLB *LoadBalancer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req floodReq
		req.Count = 20
		req.ProductID = 1
		json.NewDecoder(r.Body).Decode(&req)

		if req.Count < 1 {
			req.Count = 1
		}
		if req.Count > 300 {
			req.Count = 300
		}

		// Resolve the sticky key for IP-Hash.
		stickyKey := req.ClientIP
		if stickyKey == "" {
			stickyKey = extractIP(r)
		}

		results := make([]floodEntry, req.Count)
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Closed barrier releases all goroutines simultaneously.
		barrier := make(chan struct{})

		wg.Add(req.Count)
		for i := 0; i < req.Count; i++ {
			go func(idx int) {
				defer wg.Done()
				<-barrier // wait for the starting gun

				backend := productLB.Pick(stickyKey)
				if backend == nil {
					mu.Lock()
					results[idx] = floodEntry{Backend: "none", Status: 503}
					mu.Unlock()
					return
				}

				url := fmt.Sprintf("%s/products/%d", backend.URL, req.ProductID)
				// Mirror the gateway's real productProxy timeout (5s) so the flood
				// genuinely reproduces production behaviour: a backend whose injected
				// latency exceeds this budget will time out — a real failure — and
				// RecordFailure() below will (correctly) push its breaker towards Open.
				// The previous 8s budget was more generous than the real proxy, so a
				// backend slowed to 5500ms would still "succeed" here (just slowly),
				// which fed RecordSuccess() and the breaker could never trip.
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				httpReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

				start := time.Now()
				backend.active.Add(1)
				resp, err := http.DefaultClient.Do(httpReq)
				elapsed := time.Since(start)
				backend.active.Add(-1)

				status := 0
				if err != nil {
					backend.cb.RecordFailure()
				} else {
					resp.Body.Close()
					status = resp.StatusCode
					backend.cb.RecordSuccess()
					backend.RecordLatency(elapsed)
				}

				mu.Lock()
				results[idx] = floodEntry{
					Backend:    backend.URL,
					DurationMs: elapsed.Milliseconds(),
					Status:     status,
				}
				mu.Unlock()
			}(i)
		}

		wallStart := time.Now()
		close(barrier) // fire
		wg.Wait()
		wallMs := time.Since(wallStart).Milliseconds()

		// Aggregate distribution and per-backend average latency.
		dist := make(map[string]int)
		latSum := make(map[string]int64)
		for _, e := range results {
			dist[e.Backend]++
			latSum[e.Backend] += e.DurationMs
		}
		avgLat := make(map[string]float64)
		for backend, count := range dist {
			if count > 0 {
				avgLat[backend] = float64(latSum[backend]) / float64(count)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"count":           req.Count,
			"wall_ms":         wallMs,
			"algorithm":       productLB.GetAlgorithm().String(),
			"results":         results,
			"distribution":    dist,
			"avg_latency_ms":  avgLat,
		})
	}
}

// ── Warm-up (Least-Connections demo primer) ──────────────────────────────────

type warmupReq struct {
	Port  string `json:"port"`
	Count int    `json:"count"`
}

// handleWarmup fires Count concurrent GET requests DIRECTLY at one named
// backend — bypassing the load balancer entirely — and lets them run in the
// background. Each one increments that backend's `active` in-flight counter
// for as long as the request takes.
//
// This exists purely to make Least-Connections "visible": raise the target
// backend's artificial latency first (via the latency slider), then warm it up
// with a handful of long-running requests, then immediately fire the flood.
// Least-Connections will see the elevated active-connection count and steer
// the new burst toward the other (idle) backends — Round Robin, by contrast,
// will blindly keep sending it a fair share. Comparing the two distributions
// side by side is the clearest possible illustration of what the algorithm
// actually does differently.
//
// The handler returns immediately (HTTP 202-equivalent JSON ack); the warm-up
// requests keep running in their own goroutines after the response is sent.
func handleWarmup(productLB *LoadBalancer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req warmupReq
		req.Count = 6
		json.NewDecoder(r.Body).Decode(&req)

		if req.Count < 1 {
			req.Count = 1
		}
		if req.Count > 50 {
			req.Count = 50
		}

		var target *Backend
		for _, b := range productLB.Backends() {
			if strings.HasSuffix(b.URL, ":"+req.Port) {
				target = b
				break
			}
		}
		if target == nil {
			http.Error(w, "unknown backend port — must be one of the product-service instances", http.StatusBadRequest)
			return
		}

		for i := 0; i < req.Count; i++ {
			go func() {
				// Mimic exactly what the proxy does: bump `active` before the call,
				// drop it once the (slow) response finally completes. This is what
				// pickLeastConnections actually reads when choosing a backend.
				target.active.Add(1)
				defer target.active.Add(-1)

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				url := target.URL + "/products/1"
				hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
				if err != nil {
					return
				}
				resp, err := http.DefaultClient.Do(hreq)
				if err != nil {
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
		}

		slog.Info("warm-up launched", "backend", target.URL, "count", req.Count)
		globalBus.Publish(Event{
			Type:    "admin",
			Message: fmt.Sprintf("warm-up: %d long-running requests launched directly against backend — its active-connection count will stay elevated for as long as its configured latency lasts", req.Count),
			Backend: target.URL,
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"backend": target.URL,
			"count":   req.Count,
			"note":    "requests are running in the background — fire the flood now to see Least-Connections route around this backend",
		})
	}
}
