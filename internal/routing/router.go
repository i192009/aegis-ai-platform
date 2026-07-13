// Package routing filters providers by policy and applies configurable load-balancing strategies.
package routing

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/circuitbreaker"
	"github.com/i192009/aegis-ai-platform/internal/provider"
)

// Strategy identifies the final selection algorithm after eligibility filtering.
type Strategy string

const (
	WeightedRoundRobin Strategy = "weighted_round_robin"
	LeastOutstanding   Strategy = "least_outstanding"
	LeastLatency       Strategy = "least_latency"
	PriorityFallback   Strategy = "priority_fallback"
)

var ErrNoEligibleProvider = errors.New("no eligible provider")

// ProviderConfig contains routing and price metadata for one registered provider.
type ProviderConfig struct {
	Provider                  provider.Provider
	Weight                    int
	Priority                  int
	MaxConcurrency            int
	Timeout                   time.Duration
	InputCostPerMillionMicro  int64
	OutputCostPerMillionMicro int64
	Enabled                   bool
}

type managed struct {
	config      ProviderConfig
	semaphore   chan struct{}
	breaker     *circuitbreaker.Breaker
	outstanding atomic.Int64
	healthy     atomic.Bool
	latencyMu   sync.RWMutex
	ewmaMS      float64
}

// Router is safe for concurrent selection across gateway goroutines.
type Router struct {
	providers []*managed
	sequence  atomic.Uint64
	alpha     float64
}

// New creates a registry. Provider order is stable for deterministic tie-breaking.
func New(configs []ProviderConfig, breakerConfig circuitbreaker.Config) (*Router, error) {
	if len(configs) == 0 {
		return nil, errors.New("at least one provider is required")
	}
	router := &Router{alpha: 0.2}
	for _, config := range configs {
		if config.Provider == nil {
			return nil, errors.New("provider is required")
		}
		if config.Weight <= 0 {
			config.Weight = 1
		}
		if config.MaxConcurrency <= 0 {
			config.MaxConcurrency = 20
		}
		if config.Timeout <= 0 {
			config.Timeout = 30 * time.Second
		}
		entry := &managed{config: config, semaphore: make(chan struct{}, config.MaxConcurrency), breaker: circuitbreaker.New(breakerConfig)}
		entry.healthy.Store(true)
		router.providers = append(router.providers, entry)
	}
	return router, nil
}

// Selection defines request eligibility inputs.
type Selection struct {
	Model              string
	DataClassification string
	AllowedProviders   map[string]struct{}
	ExcludedProviders  map[string]struct{}
	Strategy           Strategy
	Now                time.Time
}

// Lease holds provider capacity until the caller reports an outcome.
type Lease struct {
	Provider                  provider.Provider
	Timeout                   time.Duration
	InputCostPerMillionMicro  int64
	OutputCostPerMillionMicro int64
	entry                     *managed
	released                  atomic.Bool
}

// Select filters policy before applying the configured balancing strategy.
func (router *Router) Select(selection Selection) (*Lease, error) {
	if selection.Now.IsZero() {
		selection.Now = time.Now()
	}
	eligible := make([]*managed, 0, len(router.providers))
	for _, entry := range router.providers {
		name := entry.config.Provider.Name()
		if !entry.config.Enabled || !entry.healthy.Load() {
			continue
		}
		if selection.AllowedProviders != nil {
			if _, ok := selection.AllowedProviders[name]; !ok {
				continue
			}
		}
		if _, excluded := selection.ExcludedProviders[name]; excluded {
			continue
		}
		capabilities := entry.config.Provider.Capabilities()
		if _, ok := capabilities.Models[selection.Model]; !ok {
			continue
		}
		if _, ok := capabilities.DataClassifications[selection.DataClassification]; !ok {
			continue
		}
		if entry.breaker.State(selection.Now) == circuitbreaker.Open {
			continue
		}
		if len(entry.semaphore) >= cap(entry.semaphore) {
			continue
		}
		eligible = append(eligible, entry)
	}
	if len(eligible) == 0 {
		return nil, ErrNoEligibleProvider
	}

	ordered := router.order(eligible, selection.Strategy)
	for _, entry := range ordered {
		if !entry.breaker.Allow(selection.Now) {
			continue
		}
		select {
		case entry.semaphore <- struct{}{}:
			entry.outstanding.Add(1)
			return &Lease{
				Provider:                  entry.config.Provider,
				Timeout:                   entry.config.Timeout,
				InputCostPerMillionMicro:  entry.config.InputCostPerMillionMicro,
				OutputCostPerMillionMicro: entry.config.OutputCostPerMillionMicro,
				entry:                     entry,
			}, nil
		default:
			entry.breaker.Release()
		}
	}
	return nil, ErrNoEligibleProvider
}

// HighestPrices returns conservative prices across providers allowed by static policy.
// It intentionally ignores current health and capacity so budget admission remains safe
// if a request fails over to a temporarily busier or less healthy provider.
func (router *Router) HighestPrices(selection Selection) (input, output int64, err error) {
	found := false
	for _, entry := range router.providers {
		name := entry.config.Provider.Name()
		if !entry.config.Enabled {
			continue
		}
		if selection.AllowedProviders != nil {
			if _, ok := selection.AllowedProviders[name]; !ok {
				continue
			}
		}
		capabilities := entry.config.Provider.Capabilities()
		if _, ok := capabilities.Models[selection.Model]; !ok {
			continue
		}
		if _, ok := capabilities.DataClassifications[selection.DataClassification]; !ok {
			continue
		}
		found = true
		input = max(input, entry.config.InputCostPerMillionMicro)
		output = max(output, entry.config.OutputCostPerMillionMicro)
	}
	if !found {
		return 0, 0, ErrNoEligibleProvider
	}
	return input, output, nil
}

// Success releases capacity, updates the latency EWMA, and closes the circuit.
func (lease *Lease) Success(latency time.Duration) {
	if !lease.release() {
		return
	}
	lease.entry.latencyMu.Lock()
	measurement := float64(latency.Milliseconds())
	if lease.entry.ewmaMS == 0 {
		lease.entry.ewmaMS = measurement
	} else {
		lease.entry.ewmaMS = 0.2*measurement + 0.8*lease.entry.ewmaMS
	}
	lease.entry.latencyMu.Unlock()
	lease.entry.breaker.Success()
}

// Failure releases capacity and records a breaker failure only for provider faults.
func (lease *Lease) Failure(retryable bool, now time.Time) {
	if !lease.release() {
		return
	}
	if retryable {
		lease.entry.breaker.Failure(now)
	} else {
		lease.entry.breaker.Release()
	}
}

func (lease *Lease) release() bool {
	if !lease.released.CompareAndSwap(false, true) {
		return false
	}
	<-lease.entry.semaphore
	lease.entry.outstanding.Add(-1)
	return true
}

func (router *Router) order(entries []*managed, strategy Strategy) []*managed {
	ordered := append([]*managed(nil), entries...)
	switch strategy {
	case LeastOutstanding:
		sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].outstanding.Load() < ordered[j].outstanding.Load() })
	case LeastLatency:
		sort.SliceStable(ordered, func(i, j int) bool { return latency(ordered[i]) < latency(ordered[j]) })
	case PriorityFallback:
		sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].config.Priority < ordered[j].config.Priority })
	default:
		total := 0
		for _, entry := range ordered {
			total += entry.config.Weight
		}
		position := int(router.sequence.Add(1)-1) % total
		selected := 0
		for index, entry := range ordered {
			position -= entry.config.Weight
			if position < 0 {
				selected = index
				break
			}
		}
		ordered = append(ordered[selected:], ordered[:selected]...)
	}
	return ordered
}

func latency(entry *managed) float64 {
	entry.latencyMu.RLock()
	defer entry.latencyMu.RUnlock()
	if entry.ewmaMS == 0 {
		return 1e15
	}
	return entry.ewmaMS
}
