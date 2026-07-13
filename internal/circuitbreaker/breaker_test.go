package circuitbreaker

import (
	"testing"
	"time"
)

func TestOpenAndHalfOpenRecovery(t *testing.T) {
	now := time.Unix(100, 0)
	breaker := New(Config{FailureThreshold: 2, Window: time.Minute, OpenDuration: 10 * time.Second, HalfOpenProbes: 1})
	breaker.Failure(now)
	breaker.Failure(now.Add(time.Second))
	if breaker.Allow(now.Add(2 * time.Second)) {
		t.Fatal("open breaker allowed request")
	}
	if !breaker.Allow(now.Add(12 * time.Second)) {
		t.Fatal("half-open breaker denied first probe")
	}
	if breaker.Allow(now.Add(12 * time.Second)) {
		t.Fatal("half-open breaker exceeded probe limit")
	}
	breaker.Success()
	if breaker.State(now) != Closed {
		t.Fatal("successful probe did not close breaker")
	}
}
