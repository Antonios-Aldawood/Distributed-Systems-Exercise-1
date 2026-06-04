package main

import (
	"fmt"
	"sync"
	"time"
)

type cbState int

const (
	cbClosed   cbState = iota
	cbOpen
	cbHalfOpen
)

// CircuitBreaker is a per-backend failure detector.
//
// State machine:
//
//	Closed ──(failures >= threshold)──► Open
//	Open   ──(openDuration elapsed)───► HalfOpen  (one probe allowed)
//	HalfOpen ──(probe succeeds)───────► Closed
//	HalfOpen ──(probe fails)──────────► Open       (timer reset)
//
// Every transition publishes an Event to the global SSE bus so the frontend
// sees circuit-breaker changes in real time without polling.
type CircuitBreaker struct {
	mu           sync.Mutex
	name         string        // backend URL — used in event messages
	state        cbState
	failures     int
	threshold    int
	openDuration time.Duration
	lastFailure  time.Time
}

func NewCircuitBreaker(name string, threshold int, openDuration time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		name:         name,
		threshold:    threshold,
		openDuration: openDuration,
		state:        cbClosed,
	}
}

// Allow reports whether a request to this backend should be attempted.
// Transitions Open → HalfOpen when the cooldown has elapsed.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case cbClosed:
		return true
	case cbOpen:
		if time.Since(cb.lastFailure) >= cb.openDuration {
			cb.state = cbHalfOpen
			globalBus.Publish(Event{
				Type:    "cb",
				Message: "→ HALF-OPEN: sending probe request",
				Backend: cb.name,
				Status:  "half-open",
			})
			return true
		}
		return false
	case cbHalfOpen:
		return false // probe already in-flight
	}
	return false
}

// RecordSuccess resets the breaker to Closed.
// Emits an event when recovering from Open or HalfOpen.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	prev := cb.state
	cb.failures = 0
	cb.state = cbClosed
	if prev != cbClosed {
		msg := "→ CLOSED (probe succeeded — service recovered)"
		if prev == cbOpen {
			msg = "→ CLOSED (recovered)"
		}
		globalBus.Publish(Event{
			Type:    "cb",
			Message: msg,
			Backend: cb.name,
			Status:  "closed",
		})
	}
}

// RecordFailure increments the failure counter and opens the breaker when the
// threshold is crossed. Also handles HalfOpen → Open (probe failed).
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()

	prev := cb.state
	if cb.failures >= cb.threshold {
		cb.state = cbOpen
	}

	// Emit only on transition into Open to avoid flooding the event bus.
	if cb.state == cbOpen && prev != cbOpen {
		msg := fmt.Sprintf("→ OPEN (%d consecutive failures)", cb.failures)
		if prev == cbHalfOpen {
			msg = "→ re-OPEN (probe failed)"
		}
		globalBus.Publish(Event{
			Type:    "cb",
			Message: msg,
			Backend: cb.name,
			Status:  "open",
		})
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
