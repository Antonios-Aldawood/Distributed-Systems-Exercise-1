package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Algorithm enumerates the seven supported load-balancing strategies.
type Algorithm int

const (
	RoundRobin         Algorithm = iota // strict rotation among healthy backends
	LeastConnections                    // fewest active in-flight requests wins
	Random                              // uniform random selection
	WeightedRoundRobin                  // smooth WRR — Nginx algorithm, weight-proportional
	IPHash                              // client IP hashes to a consistent backend (sticky)
	LeastResponseTime                   // lowest EWMA response latency wins
	PowerOfTwoChoices                   // pick 2 at random, take the one with fewer connections
)

var algorithmNames = [...]string{
	"round-robin",
	"least-connections",
	"random",
	"weighted-round-robin",
	"ip-hash",
	"least-response-time",
	"power-of-two-choices",
}

func (a Algorithm) String() string {
	if int(a) < len(algorithmNames) {
		return algorithmNames[a]
	}
	return "unknown"
}

// ParseAlgorithm converts a string name to an Algorithm.
func ParseAlgorithm(s string) (Algorithm, bool) {
	for i, name := range algorithmNames {
		if name == s {
			return Algorithm(i), true
		}
	}
	return RoundRobin, false
}

// BackendConfig declares one upstream instance.
type BackendConfig struct {
	URL    string
	Weight int // relative weight for WeightedRoundRobin; defaults to 1
}

// Backend is one upstream service instance managed by a LoadBalancer.
type Backend struct {
	URL           string
	weight        int          // static, set at construction
	currentWeight int          // mutable smooth-WRR state; protected by LB mutex
	cb            *CircuitBreaker
	active        atomic.Int32 // in-flight request counter
	healthy       atomic.Bool  // set by health-check goroutine
	ewmaUs        atomic.Int64 // EWMA response time in microseconds (LeastResponseTime)
}

// RecordLatency updates the exponential moving-average latency.
// α = 0.5  →  new_ewma = 0.5·sample + 0.5·old_ewma
// Uses a CAS loop so it is safe to call concurrently from many goroutines.
func (b *Backend) RecordLatency(d time.Duration) {
	us := d.Microseconds()
	for {
		old := b.ewmaUs.Load()
		next := us
		if old != 0 {
			next = (us + old) / 2
		}
		if b.ewmaUs.CompareAndSwap(old, next) {
			return
		}
	}
}

func (b *Backend) avgResponseMs() float64 {
	return float64(b.ewmaUs.Load()) / 1000.0
}

// LoadBalancer distributes requests across a backend pool using the configured
// algorithm. It integrates circuit breaking (per-backend) and health checks
// (background poller). Both mechanisms are intentionally independent:
//
//   - Health check = proactive polling ("are you alive right now?")
//   - Circuit breaker = reactive pattern ("you've been failing my requests")
type LoadBalancer struct {
	mu        sync.Mutex
	backends  []*Backend
	rrIdx     int
	tieIdx    int // rotates the scan order for "pick the best" algorithms so ties don't always favour backends[0]
	algorithm Algorithm
}

// rotateStart returns a rotating starting offset into a healthy-backend slice
// of length n, advancing the rotation each call.
//
// Why this matters: pickLeastConnections, pickWeightedRoundRobin and
// pickLeastResponseTime all scan the backend list and keep the "best" one
// seen so far, breaking ties with a strict < / > comparison. A naive
// left-to-right scan therefore ALWAYS resolves ties in favour of the first
// backend in the slice — here, :8082. At idle/cold-start every backend looks
// identical (0 active connections, 0 EWMA), so :8082 would systematically win
// every tie and absorb a disproportionate share of traffic — exactly the bias
// you observed. Rotating the scan's starting point each call spreads tie-wins
// evenly across all backends over time, while leaving genuine (non-tied)
// decisions completely untouched.
func (lb *LoadBalancer) rotateStart(n int) int {
	if n <= 0 {
		return 0
	}
	start := lb.tieIdx % n
	lb.tieIdx++
	return start
}

func NewLoadBalancer(configs []BackendConfig, algo Algorithm) *LoadBalancer {
	backends := make([]*Backend, len(configs))
	for i, c := range configs {
		w := c.Weight
		if w <= 0 {
			w = 1
		}
		b := &Backend{
			URL:    c.URL,
			weight: w,
			// threshold=3: three consecutive failures open the circuit.
			// With health checks every 5 s that means the CB opens after ~15 s of
			// sustained failure — fast enough for a live demo.
			cb: NewCircuitBreaker(c.URL, 3, 10*time.Second),
		}
		b.healthy.Store(true)
		backends[i] = b
	}
	return &LoadBalancer{backends: backends, algorithm: algo}
}

// SetAlgorithm changes the algorithm at runtime (used by /gateway/algorithm).
func (lb *LoadBalancer) SetAlgorithm(a Algorithm) {
	lb.mu.Lock()
	lb.algorithm = a
	for _, b := range lb.backends { // reset WRR state
		b.currentWeight = 0
	}
	lb.mu.Unlock()
}

func (lb *LoadBalancer) GetAlgorithm() Algorithm {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.algorithm
}

// Pick selects the best available backend.
// stickyKey is the client IP; only used by IPHash — ignored by all others.
func (lb *LoadBalancer) Pick(stickyKey string) *Backend {
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
		return lb.pickRoundRobin(healthy)
	case LeastConnections:
		return lb.pickLeastConnections(healthy)
	case Random:
		return lb.pickRandom(healthy)
	case WeightedRoundRobin:
		return lb.pickWeightedRoundRobin(healthy)
	case IPHash:
		return lb.pickIPHash(healthy, stickyKey)
	case LeastResponseTime:
		return lb.pickLeastResponseTime(healthy)
	case PowerOfTwoChoices:
		return lb.pickPowerOfTwo(healthy)
	}
	return lb.pickRoundRobin(healthy)
}

// ── Algorithm implementations ─────────────────────────────────────────────────

func (lb *LoadBalancer) pickRoundRobin(healthy []*Backend) *Backend {
	n := len(healthy)
	for i := 0; i < n; i++ {
		b := healthy[lb.rrIdx%n]
		lb.rrIdx++
		if b.cb.Allow() {
			return b
		}
	}
	return nil
}

func (lb *LoadBalancer) pickLeastConnections(healthy []*Backend) *Backend {
	n := len(healthy)
	start := lb.rotateStart(n)
	var best *Backend
	for i := 0; i < n; i++ {
		b := healthy[(start+i)%n]
		if best == nil || b.active.Load() < best.active.Load() {
			best = b
		}
	}
	if best != nil && best.cb.Allow() {
		return best
	}
	for i := 0; i < n; i++ {
		b := healthy[(start+i)%n]
		if b != best && b.cb.Allow() {
			return b
		}
	}
	return nil
}

func (lb *LoadBalancer) pickRandom(healthy []*Backend) *Backend {
	n := len(healthy)
	start := rand.IntN(n)
	for i := 0; i < n; i++ {
		b := healthy[(start+i)%n]
		if b.cb.Allow() {
			return b
		}
	}
	return nil
}

// pickWeightedRoundRobin uses the smooth WRR algorithm from Nginx.
//
// Each round: add each backend's static weight to its currentWeight,
// select the backend with the highest currentWeight, subtract total weight
// from the winner. This produces a smooth, interleaved sequence matching
// the declared weight ratios without clumping.
//
// Example — weights [A=3, B=1, C=2], total=6 over 6 rounds:
//
//	Rnd1: A(3) B(1) C(2) → pick A(→-3)   sequence: A
//	Rnd2: A(0) B(2) C(4) → pick C(→-2)   sequence: A C
//	Rnd3: A(3) B(3) C(0) → pick A(→-3)   sequence: A C A
//	Rnd4: A(0) B(4) C(2) → pick B(→-2)   sequence: A C A B
//	Rnd5: A(3) B(-1)C(4) → pick C(→-2)   sequence: A C A B C
//	Rnd6: A(6) B(0) C(0) → pick A(→ 0)   sequence: A C A B C A  ✓ (3:1:2)
func (lb *LoadBalancer) pickWeightedRoundRobin(healthy []*Backend) *Backend {
	n := len(healthy)
	start := lb.rotateStart(n)
	total := 0
	for _, b := range healthy {
		b.currentWeight += b.weight
		total += b.weight
	}
	var best *Backend
	for i := 0; i < n; i++ {
		b := healthy[(start+i)%n]
		if best == nil || b.currentWeight > best.currentWeight {
			best = b
		}
	}
	if best != nil {
		best.currentWeight -= total
		if best.cb.Allow() {
			return best
		}
	}
	for _, b := range healthy {
		if b != best && b.cb.Allow() {
			return b
		}
	}
	return nil
}

// pickIPHash maps the stickyKey (client IP) to a consistent backend using FNV-1a.
// All requests from the same client always hit the same backend — useful for
// session affinity. If the mapped backend's CB is open, we walk forward.
//
// Note: this uses modular arithmetic, not consistent hashing. When backends
// are added or removed, some sessions will remap. A production system would
// use a consistent hash ring (e.g. ketama) to minimise disruption.
func (lb *LoadBalancer) pickIPHash(healthy []*Backend, key string) *Backend {
	h := fnv32(key) % uint32(len(healthy))
	n := len(healthy)
	for i := 0; i < n; i++ {
		b := healthy[(int(h)+i)%n]
		if b.cb.Allow() {
			return b
		}
	}
	return nil
}

// fnv32 is the FNV-1a 32-bit non-cryptographic hash.
func fnv32(s string) uint32 {
	const (
		offset = uint32(2166136261)
		prime  = uint32(16777619)
	)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}

// pickLeastResponseTime picks the backend with the lowest EWMA response time.
// Backends with no recorded latency (ewma==0) are treated as fastest to
// avoid cold-start penalisation of new instances.
func (lb *LoadBalancer) pickLeastResponseTime(healthy []*Backend) *Backend {
	n := len(healthy)
	start := lb.rotateStart(n)
	var best *Backend
	for i := 0; i < n; i++ {
		b := healthy[(start+i)%n]
		if best == nil {
			best = b
			continue
		}
		bu, beu := b.ewmaUs.Load(), best.ewmaUs.Load()
		// 0 means "never measured" — prefer it (cold-start bias). Rotating the
		// scan start means that during cold start (everyone at 0) each backend
		// gets a turn at being the "last unmeasured one wins" pick, instead of
		// the same one winning every single time.
		if bu == 0 || (beu != 0 && bu < beu) {
			best = b
		}
	}
	if best != nil && best.cb.Allow() {
		return best
	}
	for i := 0; i < n; i++ {
		b := healthy[(start+i)%n]
		if b != best && b.cb.Allow() {
			return b
		}
	}
	return nil
}

// pickPowerOfTwo implements the "Power of Two Choices" algorithm.
//
// Why it works: picking the minimum of two random samples gives a distribution
// that is exponentially better than pure random. The maximum load on any
// backend drops from O(log n / log log n) with random to O(log log n) with P2C.
// It approximates full Least Connections with only 2 comparisons per pick.
func (lb *LoadBalancer) pickPowerOfTwo(healthy []*Backend) *Backend {
	n := len(healthy)
	if n == 1 {
		if healthy[0].cb.Allow() {
			return healthy[0]
		}
		return nil
	}
	// Two distinct random indices.
	i := rand.IntN(n)
	j := rand.IntN(n - 1)
	if j >= i {
		j++
	}
	a, b := healthy[i], healthy[j]
	// Choose whichever has fewer in-flight connections.
	chosen, other := a, b
	if b.active.Load() < a.active.Load() {
		chosen, other = b, a
	}
	if chosen.cb.Allow() {
		return chosen
	}
	if other.cb.Allow() {
		return other
	}
	// Both random picks are CB-blocked; walk remaining backends.
	for _, b := range healthy {
		if b != chosen && b != other && b.cb.Allow() {
			return b
		}
	}
	return nil
}

// ── Health checking ───────────────────────────────────────────────────────────

func (lb *LoadBalancer) StartHealthChecks(interval time.Duration) {
	go func() {
		// Start on the ticker only — no immediate first check.
		// With `go run .` each service takes several seconds to compile; firing
		// health probes before they are listening would cause spurious failures
		// that could trip the circuit breaker before a single real request is sent.
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

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	ms := time.Since(start).Milliseconds()

	if err != nil || resp.StatusCode != http.StatusOK {
		if err == nil {
			resp.Body.Close()
		}
		wasHealthy := b.healthy.Swap(false)
		// Health checks control pool membership (healthy flag) only.
		// The circuit breaker is deliberately kept independent — it reacts to
		// actual request failures from the proxy, not to health-poll results.
		// Coupling them caused spurious CB trips during service startup.
		var msg, status string
		if wasHealthy {
			msg = fmt.Sprintf("health probe FAILED → marked DOWN (%dms)", ms)
			status = "unhealthy"
			slog.Warn("backend unhealthy", "url", b.URL, "ms", ms)
		} else {
			msg = fmt.Sprintf("health probe FAILED, still DOWN (%dms)", ms)
			status = "unhealthy"
			slog.Info("health probe failed", "url", b.URL, "ms", ms)
		}
		globalBus.Publish(Event{Type: "health", Message: msg, Backend: b.URL, Status: status, Ms: ms})
		return
	}
	resp.Body.Close()

	wasUnhealthy := !b.healthy.Swap(true)
	var msg, status string
	if wasUnhealthy {
		msg = fmt.Sprintf("health probe OK → RECOVERED (%dms)", ms)
		status = "recovered"
		slog.Info("backend recovered", "url", b.URL, "ms", ms)
	} else {
		msg = fmt.Sprintf("health probe OK (%dms)", ms)
		status = "healthy"
		slog.Info("health probe OK", "url", b.URL, "ms", ms)
	}
	globalBus.Publish(Event{Type: "health", Message: msg, Backend: b.URL, Status: status, Ms: ms})
}

// ── Status snapshot ───────────────────────────────────────────────────────────

func (lb *LoadBalancer) Status() []map[string]any {
	out := make([]map[string]any, len(lb.backends))
	for i, b := range lb.backends {
		cbState := b.cb.State()
		isHealthy := b.healthy.Load()

		// Composite status makes the distinction between CB and health-check
		// failures visible without requiring the reader to combine two fields.
		var status string
		switch {
		case cbState == "open":
			status = "circuit-open"
		case cbState == "half-open":
			status = "probing"
		case !isHealthy:
			status = "down (health-check)"
		default:
			status = "healthy"
		}

		out[i] = map[string]any{
			"url":             b.URL,
			"weight":          b.weight,
			"healthy":         isHealthy,
			"cb_state":        cbState,
			"status":          status,
			"active_conns":    b.active.Load(),
			"avg_response_ms": b.avgResponseMs(),
		}
	}
	return out
}

// Backends exposes the raw backend slice for the simulate handler.
func (lb *LoadBalancer) Backends() []*Backend {
	return lb.backends
}

