// Package circuitbreaker implements a concurrency-safe closed/open/half-open breaker.
package circuitbreaker

import (
	"sync"
	"time"
)

// State is the observable circuit state.
type State string

const (
	Closed   State = "closed"
	Open     State = "open"
	HalfOpen State = "half_open"
)

// Config bounds the failure window and recovery probes.
type Config struct {
	FailureThreshold int
	Window           time.Duration
	OpenDuration     time.Duration
	HalfOpenProbes   int
}

// Breaker does not hold its mutex during the protected operation.
type Breaker struct {
	mu             sync.Mutex
	config         Config
	state          State
	failures       []time.Time
	openedAt       time.Time
	halfOpenActive int
}

// New creates a closed breaker.
func New(config Config) *Breaker {
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 5
	}
	if config.Window <= 0 {
		config.Window = 30 * time.Second
	}
	if config.OpenDuration <= 0 {
		config.OpenDuration = 15 * time.Second
	}
	if config.HalfOpenProbes <= 0 {
		config.HalfOpenProbes = 1
	}
	return &Breaker{config: config, state: Closed}
}

// Allow reserves a half-open probe when necessary.
func (breaker *Breaker) Allow(now time.Time) bool {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if breaker.state == Open && now.Sub(breaker.openedAt) >= breaker.config.OpenDuration {
		breaker.state = HalfOpen
		breaker.halfOpenActive = 0
	}
	switch breaker.state {
	case Closed:
		return true
	case HalfOpen:
		if breaker.halfOpenActive >= breaker.config.HalfOpenProbes {
			return false
		}
		breaker.halfOpenActive++
		return true
	default:
		return false
	}
}

// Success closes the breaker and releases a half-open probe.
func (breaker *Breaker) Success() {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	breaker.state = Closed
	breaker.failures = nil
	breaker.halfOpenActive = 0
}

// Failure records one retry-relevant failure.
func (breaker *Breaker) Failure(now time.Time) {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if breaker.state == HalfOpen {
		breaker.open(now)
		return
	}
	cutoff := now.Add(-breaker.config.Window)
	retained := breaker.failures[:0]
	for _, occurred := range breaker.failures {
		if occurred.After(cutoff) {
			retained = append(retained, occurred)
		}
	}
	breaker.failures = append(retained, now)
	if len(breaker.failures) >= breaker.config.FailureThreshold {
		breaker.open(now)
	}
}

// Release abandons an unused half-open probe without recording a failure.
func (breaker *Breaker) Release() {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if breaker.state == HalfOpen && breaker.halfOpenActive > 0 {
		breaker.halfOpenActive--
	}
}

// State returns the current state, transitioning to half-open when time permits.
func (breaker *Breaker) State(now time.Time) State {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()
	if breaker.state == Open && now.Sub(breaker.openedAt) >= breaker.config.OpenDuration {
		return HalfOpen
	}
	return breaker.state
}

func (breaker *Breaker) open(now time.Time) {
	breaker.state = Open
	breaker.openedAt = now
	breaker.halfOpenActive = 0
}
