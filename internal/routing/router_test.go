package routing

import (
	"testing"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/circuitbreaker"
	"github.com/i192009/aegis-ai-platform/internal/provider"
)

func TestWeightedRoundRobinAndModelFilter(t *testing.T) {
	first := provider.NewMock(provider.MockConfig{Name: "first", Models: []string{"m"}})
	second := provider.NewMock(provider.MockConfig{Name: "second", Models: []string{"m"}})
	router, err := New([]ProviderConfig{
		{Provider: first, Weight: 2, Enabled: true},
		{Provider: second, Weight: 1, Enabled: true},
	}, circuitbreaker.Config{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"first", "first", "second"}
	for index, name := range want {
		lease, err := router.Select(Selection{Model: "m", DataClassification: "INTERNAL", Strategy: WeightedRoundRobin})
		if err != nil {
			t.Fatal(err)
		}
		if lease.Provider.Name() != name {
			t.Fatalf("selection %d = %s, want %s", index, lease.Provider.Name(), name)
		}
		lease.Success(time.Millisecond)
	}
	if _, err := router.Select(Selection{Model: "unsupported", DataClassification: "INTERNAL"}); err != ErrNoEligibleProvider {
		t.Fatalf("unsupported model error = %v", err)
	}
}

func TestConcurrencyCapacity(t *testing.T) {
	mock := provider.NewMock(provider.MockConfig{Name: "only", Models: []string{"m"}})
	router, _ := New([]ProviderConfig{{Provider: mock, Enabled: true, MaxConcurrency: 1}}, circuitbreaker.Config{})
	lease, err := router.Select(Selection{Model: "m", DataClassification: "INTERNAL"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Select(Selection{Model: "m", DataClassification: "INTERNAL"}); err != ErrNoEligibleProvider {
		t.Fatalf("capacity error = %v", err)
	}
	lease.Failure(false, time.Now())
}
