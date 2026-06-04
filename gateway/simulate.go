package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
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
