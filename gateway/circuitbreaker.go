package main

import (
	"sync"
	"time"
)

type cbState int

const (
	// cbClosed is the normal operating state. All requests pass through.
	cbClosed cbState = iota

	// cbOpen means the downstream service is considered unhealthy.
	// All requests are rejected immediately (fail fast) so callers get a fast
	// error instead of waiting for a timeout. This prevents one slow service
	// from blocking threads/goroutines across the whole system.
	cbOpen

	// cbHalfOpen is the recovery probe state. After openDuration has elapsed,
	// exactly one request is allowed through to test whether the service
	// has recovered. All other concurrent requests are still rejected.
	cbHalfOpen
)

// CircuitBreaker is a per-backend failure detector.
//
// State machine:
//
//	Closed ──(failures >= threshold)──► Open
//	Open   ──(openDuration elapsed)───► HalfOpen (one probe allowed)
//	HalfOpen ──(probe succeeds)───────► Closed
//	HalfOpen ──(probe fails)──────────► Open (timer reset)
type CircuitBreaker struct {
	mu           sync.Mutex
	state        cbState
	failures     int
	threshold    int           // consecutive failures before opening
	openDuration time.Duration // how long to stay Open before probing
	lastFailure  time.Time
}

func NewCircuitBreaker(threshold int, openDuration time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		openDuration: openDuration,
		state:        cbClosed,
	}
}

// Allow reports whether a request to this backend should be attempted.
// Calling Allow() in HalfOpen "consumes" the probe slot — subsequent
// callers will receive false until the probe completes.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case cbClosed:
		return true
	case cbOpen:
		if time.Since(cb.lastFailure) >= cb.openDuration {
			cb.state = cbHalfOpen
			return true // let the probe through
		}
		return false
	case cbHalfOpen:
		return false // probe already in-flight
	}
	return false
}

// RecordSuccess resets the breaker to Closed on any successful response.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = cbClosed
}

// RecordFailure increments the failure counter and opens the breaker
// once the threshold is crossed.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()
	if cb.failures >= cb.threshold {
		cb.state = cbOpen
	}
}

func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case cbClosed:
		return "closed"
	case cbOpen:
		return "open"
	case cbHalfOpen:
		return "half-open"
	}
	return "unknown"
}
