package main

import (
	"sync"
	"time"
)

// idemStore is an in-memory idempotency cache.
//
// Why does this exist?
//
// When the gateway retries a POST /orders (because it got a timeout or a 5xx
// and doesn't know whether the order was actually created), the order service
// could process the same logical request twice, producing duplicate orders.
//
// The fix: the client attaches a unique Idempotency-Key header to every
// mutating request. Before doing any work, the order service checks whether
// it has seen this key. If yes, it returns the stored result immediately
// without touching the database. The second call is provably identical to the
// first — same key, same cached response.
//
// This is the same mechanism Stripe, PayPal, and most payment processors use.
// The header "X-Idempotent-Replayed: true" on the response tells the caller
// it received a cached result, not a fresh creation.
type idemStore struct {
	mu      sync.RWMutex
	entries map[string]idemEntry
}

type idemEntry struct {
	statusCode int
	body       []byte
	expiry     time.Time
}

var globalIdem = &idemStore{entries: make(map[string]idemEntry)}

func (s *idemStore) get(key string) (idemEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	if !ok || time.Now().After(e.expiry) {
		return idemEntry{}, false
	}
	return e, true
}

func (s *idemStore) set(key string, statusCode int, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = idemEntry{
		statusCode: statusCode,
		body:       body,
		expiry:     time.Now().Add(24 * time.Hour),
	}
}
