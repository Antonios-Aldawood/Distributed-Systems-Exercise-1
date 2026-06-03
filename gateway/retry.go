package main

import (
	"math/rand/v2"
	"net/http"
	"time"
)

// backoff returns the delay to wait before retry attempt n (1-indexed).
//
// Formula: random value in [0, min(base * 2^n, cap))
// This is "full jitter" exponential backoff.
//
// Why full jitter instead of pure exponential?
// If ten clients all hit the same service failure at the same moment, pure
// exponential would make them all retry at t+200ms, then t+400ms, etc. —
// perfectly synchronised. That synchronised wave hits the recovering service
// all at once (the "thundering herd") and can knock it down again.
// Full jitter spreads those retries randomly across the window, so the
// recovering service sees a gradual ramp-up instead of a spike.
//
// Delays (approximate):
//
//	attempt 1: 0–200 ms
//	attempt 2: 0–400 ms
//	attempt 3: 0–800 ms (capped at 2 s)
func backoff(attempt int) time.Duration {
	const (
		base = 100 * time.Millisecond
		cap  = 2 * time.Second
	)
	d := base * time.Duration(1<<uint(attempt))
	if d > cap {
		d = cap
	}
	return time.Duration(rand.Int64N(int64(d)))
}

// isRetriable reports whether an HTTP status code warrants another attempt.
//
// We only retry server-side and gateway errors. Client errors (4xx) reflect
// a bad request — retrying the exact same request won't fix a 400 or 404.
// Note: 429 (Too Many Requests) is an exception; the server is telling us
// to slow down, so we retry with backoff.
func isRetriable(status int) bool {
	switch status {
	case http.StatusTooManyRequests,       // 429
		http.StatusInternalServerError,    // 500
		http.StatusBadGateway,             // 502
		http.StatusServiceUnavailable,     // 503
		http.StatusGatewayTimeout:         // 504
		return true
	}
	return false
}
