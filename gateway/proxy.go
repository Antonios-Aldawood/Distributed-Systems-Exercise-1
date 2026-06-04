package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"time"
)

// hopHeaders must not be forwarded to or from upstream services.
var hopHeaders = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailers":            true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// Proxy is an http.Handler that forwards requests to a LoadBalancer-managed
// pool of backends, with automatic retries, circuit breaking, and a shared
// per-request timeout budget.
type Proxy struct {
	lb         *LoadBalancer
	timeout    time.Duration
	maxRetries int
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := r.Header.Get("X-Request-ID")
	if reqID == "" {
		reqID = fmt.Sprintf("%016x", rand.Uint64())
	}
	w.Header().Set("X-Request-ID", reqID)

	// Buffer the body so we can replay it on every retry attempt.
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			http.Error(w, "cannot read request body", http.StatusBadRequest)
			return
		}
	}

	// Extract the real client IP for IP-Hash load balancing.
	clientIP := extractIP(r)

	ctx, cancel := context.WithTimeout(r.Context(), p.timeout)
	defer cancel()

	var lastErr string
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if ctx.Err() != nil {
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			return
		}

		if attempt > 0 {
			delay := backoff(attempt)
			slog.Info("backing off before retry",
				"request_id", reqID,
				"path", r.URL.Path,
				"attempt", attempt,
				"delay", delay,
			)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
				return
			}
		}

		// Pass the client IP so IP-Hash can route consistently.
		backend := p.lb.Pick(clientIP)
		if backend == nil {
			http.Error(w, "no backends available", http.StatusServiceUnavailable)
			return
		}

		start := time.Now()
		status, transportErr := p.forward(ctx, w, r, backend, body, reqID, attempt)
		elapsed := time.Since(start)

		slog.Info("upstream response",
			"request_id", reqID,
			"method", r.Method,
			"path", r.URL.Path,
			"backend", backend.URL,
			"attempt", attempt,
			"status", status,
			"duration", elapsed,
			"err", transportErr,
		)

		if transportErr != nil {
			lastErr = transportErr.Error()
			continue
		}

		if !isRetriable(status) {
			// Feed the latency sample back so LeastResponseTime stays accurate.
			backend.RecordLatency(elapsed)
			return
		}

		lastErr = fmt.Sprintf("status %d", status)
	}

	slog.Error("all retries exhausted",
		"request_id", reqID,
		"path", r.URL.Path,
		"last_error", lastErr,
	)
	http.Error(w, "service unavailable after retries: "+lastErr, http.StatusServiceUnavailable)
}

func (p *Proxy) forward(
	ctx context.Context,
	w http.ResponseWriter,
	orig *http.Request,
	backend *Backend,
	body []byte,
	reqID string,
	attempt int,
) (status int, err error) {
	targetURL := backend.URL + orig.URL.RequestURI()
	req, err := http.NewRequestWithContext(ctx, orig.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		backend.cb.RecordFailure()
		return 0, err
	}

	for k, vs := range orig.Header {
		if !hopHeaders[k] {
			req.Header[k] = vs
		}
	}
	req.Header.Set("X-Request-ID", reqID)
	req.Header.Set("X-Forwarded-For", orig.RemoteAddr)
	if attempt > 0 {
		req.Header.Set("X-Retry-Attempt", fmt.Sprintf("%d", attempt))
	}

	backend.active.Add(1)
	resp, err := http.DefaultClient.Do(req)
	backend.active.Add(-1)

	if err != nil {
		backend.cb.RecordFailure()
		return 0, err
	}
	defer resp.Body.Close()

	if isRetriable(resp.StatusCode) {
		io.Copy(io.Discard, resp.Body)
		backend.cb.RecordFailure()
		return resp.StatusCode, nil
	}

	backend.cb.RecordSuccess()

	for k, vs := range resp.Header {
		if !hopHeaders[k] {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return resp.StatusCode, nil
}

// extractIP pulls the real client IP from a request.
// Checks X-Forwarded-For first (set by upstream proxies), falls back to RemoteAddr.
func extractIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.SplitN(xff, ",", 2)
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
