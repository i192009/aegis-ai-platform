package budget

import (
	"errors"
	"testing"
)

func TestCostMicroUSD(t *testing.T) {
	cost, err := CostMicroUSD(1_000_000, 500_000, 10, 20)
	if err != nil || cost != 20 {
		t.Fatalf("CostMicroUSD() = %d, %v; want 20", cost, err)
	}
	cost, _ = CostMicroUSD(1, 0, 1, 0)
	if cost != 1 {
		t.Fatalf("fractional cost = %d, want rounded 1", cost)
	}
}

func TestCanReserve(t *testing.T) {
	if err := CanReserve(100, 60, 20, 20); err != nil {
		t.Fatal(err)
	}
	if err := CanReserve(100, 60, 20, 21); !errors.Is(err, ErrExceeded) {
		t.Fatalf("overspend error = %v", err)
	}
}
