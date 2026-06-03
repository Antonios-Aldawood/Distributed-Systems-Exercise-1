package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"
)

// hopHeaders must not be forwarded to or from upstream services.
// They describe a single transport hop, not the end-to-end request.
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
	timeout    time.Duration // total budget for all attempts combined
	maxRetries int
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Propagate or generate a request ID. In a real system this would flow
	// through every log line across every service, making distributed traces
	// trivial to reconstruct.
	reqID := r.Header.Get("X-Request-ID")
	if reqID == "" {
		reqID = fmt.Sprintf("%016x", rand.Uint64())
	}
	w.Header().Set("X-Request-ID", reqID)

	// Buffer the entire body now. A request body is a one-shot stream — once
	// read it's gone. We need to replay it identically on every retry attempt.
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

	// One deadline shared across all retry attempts.
	// Inheriting r.Context() means a client disconnect cancels everything
	// automatically — we don't keep hammering a backend for a client that
	// already left.
	ctx, cancel := context.WithTimeout(r.Context(), p.timeout)
	defer cancel()

	var lastErr string
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		// Fast-exit if the overall deadline already fired.
		if ctx.Err() != nil {
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			return
		}

		// Exponential backoff between retries (not before the first attempt).
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

		backend := p.lb.Pick()
		if backend == nil {
			http.Error(w, "no backends available", http.StatusServiceUnavailable)
			return
		}

		start := time.Now()
		status, transportErr := p.forward(ctx, w, r, backend, body, reqID, attempt)
		dur := time.Since(start)

		if transportErr != nil {
			lastErr = transportErr.Error()
			slog.Warn("upstream transport error",
				"request_id", reqID,
				"method", r.Method,
				"path", r.URL.Path,
				"backend", backend.URL,
				"attempt", attempt,
				"duration", dur,
				"err", transportErr,
			)
			continue // retry
		}

		slog.Info("upstream response",
			"request_id", reqID,
			"method", r.Method,
			"path", r.URL.Path,
			"backend", backend.URL,
			"attempt", attempt,
			"status", status,
			"duration", dur,
		)

		if !isRetriable(status) {
			return // response already written inside forward()
		}

		// Retriable status — body was discarded inside forward(), try again.
		lastErr = fmt.Sprintf("status %d", status)
	}

	slog.Error("all retries exhausted",
		"request_id", reqID,
		"path", r.URL.Path,
		"last_error", lastErr,
	)
	http.Error(w, "service unavailable after retries: "+lastErr, http.StatusServiceUnavailable)
}

// forward sends one attempt to the given backend.
//
// On success (non-retriable status) it writes the full response to w and
// returns (status, nil). On a retriable status it drains and discards the
// body (so the connection returns to the pool) and returns (status, nil)
// without writing to w. On a transport error it returns (0, err).
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

	// Copy original headers, skip hop-by-hop.
	for k, vs := range orig.Header {
		if !hopHeaders[k] {
			req.Header[k] = vs
		}
	}
	req.Header.Set("X-Request-ID", reqID)
	req.Header.Set("X-Forwarded-For", orig.RemoteAddr)
	if attempt > 0 {
		// This header tells the service "this is retry N, apply idempotency logic."
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
		// Drain so the underlying TCP connection can be reused.
		io.Copy(io.Discard, resp.Body)
		backend.cb.RecordFailure()
		return resp.StatusCode, nil
	}

	backend.cb.RecordSuccess()

	// Write the upstream response to the client.
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
