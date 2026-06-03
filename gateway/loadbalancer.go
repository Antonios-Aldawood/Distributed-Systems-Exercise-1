package main

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Algorithm selects how the load balancer picks a backend.
type Algorithm int

const (
	RoundRobin       Algorithm = iota
	LeastConnections Algorithm = iota
)

func (a Algorithm) String() string {
	if a == LeastConnections {
		return "least-connections"
	}
	return "round-robin"
}

// Backend is one upstream service instance.
type Backend struct {
	URL     string
	cb      *CircuitBreaker
	active  atomic.Int32 // in-flight request count, used by LeastConnections
	healthy atomic.Bool  // set by the health-check goroutine
}

// LoadBalancer distributes requests across a pool of backends.
// It integrates health checking and circuit breaking so that dead or
// degraded backends are automatically skipped.
type LoadBalancer struct {
	mu        sync.Mutex
	backends  []*Backend
	rrIdx     int
	algorithm Algorithm
}

func NewLoadBalancer(urls []string, algo Algorithm) *LoadBalancer {
	backends := make([]*Backend, len(urls))
	for i, u := range urls {
		b := &Backend{
			URL: u,
			cb:  NewCircuitBreaker(5, 10*time.Second),
		}
		b.healthy.Store(true)
		backends[i] = b
	}
	return &LoadBalancer{backends: backends, algorithm: algo}
}

// Pick selects the best available backend according to the configured algorithm.
// It only considers backends that are marked healthy AND whose circuit breaker
// allows a request. Returns nil if no backend is currently usable.
func (lb *LoadBalancer) Pick() *Backend {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	var healthy []*Backend
	for _, b := range lb.backends {
		if b.healthy.Load() {
			healthy = append(healthy, b)
		}
	}
	if len(healthy) == 0 {
		return nil
	}

	switch lb.algorithm {
	case RoundRobin:
		// Walk the list starting from rrIdx, skip any backend whose CB is open.
		n := len(healthy)
		for i := 0; i < n; i++ {
			b := healthy[lb.rrIdx%n]
			lb.rrIdx++
			if b.cb.Allow() {
				return b
			}
		}

	case LeastConnections:
		// Find the backend with the fewest in-flight requests.
		// If its CB blocks, try the others in order.
		var best *Backend
		for _, b := range healthy {
			if best == nil || b.active.Load() < best.active.Load() {
				best = b
			}
		}
		if best != nil && best.cb.Allow() {
			return best
		}
		for _, b := range healthy {
			if b != best && b.cb.Allow() {
				return b
			}
		}
	}

	return nil
}

// StartHealthChecks polls every backend's /health endpoint on a fixed interval.
// This is proactive (we ask the service how it feels), whereas the circuit
// breaker is reactive (we watch what happens when we actually send requests).
// Both mechanisms are needed: health checks catch a service that went down
// between requests; circuit breakers handle a degraded-but-alive service.
func (lb *LoadBalancer) StartHealthChecks(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			for _, b := range lb.backends {
				go lb.checkBackend(b)
			}
		}
	}()
}

func (lb *LoadBalancer) checkBackend(b *Backend) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.URL+"/health", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if b.healthy.Swap(false) {
			slog.Warn("backend unhealthy", "url", b.URL)
		}
		if err == nil {
			resp.Body.Close()
		}
		return
	}
	resp.Body.Close()
	if !b.healthy.Swap(true) {
		slog.Info("backend recovered", "url", b.URL)
	}
}

// Status returns a JSON-serialisable snapshot of all backends.
// Used by the /gateway/status endpoint so you can observe the system live.
func (lb *LoadBalancer) Status() []map[string]any {
	out := make([]map[string]any, len(lb.backends))
	for i, b := range lb.backends {
		out[i] = map[string]any{
			"url":          b.URL,
			"healthy":      b.healthy.Load(),
			"cb_state":     b.cb.State(),
			"active_conns": b.active.Load(),
		}
	}
	return out
}
