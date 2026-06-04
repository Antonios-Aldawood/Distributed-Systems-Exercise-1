package main

import (
	"encoding/json"
	"sync"
)

// Event is pushed over Server-Sent Events to every connected browser tab,
// giving real-time visibility into health probes, circuit-breaker transitions,
// and admin actions without the frontend having to poll aggressively.
type Event struct {
	Type    string `json:"type"`              // "health" | "cb" | "admin" | "connected"
	Message string `json:"message"`           // human-readable description
	Backend string `json:"backend,omitempty"` // full backend URL, e.g. http://localhost:8083
	Status  string `json:"status,omitempty"`  // healthy | unhealthy | recovered | open | half-open | closed
	Ms      int64  `json:"ms,omitempty"`      // latency value or probe round-trip time
}

func (e Event) JSON() []byte {
	b, _ := json.Marshal(e)
	return b
}

// EventBus is a simple fan-out pub/sub. Publish never blocks: if a subscriber's
// buffer is full the event is dropped for that subscriber (the frontend will
// recover on its next status poll).
type EventBus struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

var globalBus = &EventBus{subs: make(map[chan Event]struct{})}

func (b *EventBus) Publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

func (b *EventBus) Subscribe() chan Event {
	ch := make(chan Event, 64)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *EventBus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	delete(b.subs, ch)
	b.mu.Unlock()
}
