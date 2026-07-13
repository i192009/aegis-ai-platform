package persistence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/budget"
	"github.com/i192009/aegis-ai-platform/internal/request"
)

func TestMemoryConcurrentIdempotency(t *testing.T) {
	store := NewMemory()
	params := CreateRequestParams{TenantID: "tenant", APIKeyID: "key", IdempotencyKey: "same-key", CanonicalHash: []byte("same"), Input: request.ChatInput{Model: "m"}}
	var created atomic.Int64
	var wait sync.WaitGroup
	for range 100 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, wasCreated, err := store.CreateOrGetRequest(context.Background(), params)
			if err != nil {
				t.Errorf("CreateOrGetRequest() error = %v", err)
			}
			if wasCreated {
				created.Add(1)
			}
		}()
	}
	wait.Wait()
	if created.Load() != 1 {
		t.Fatalf("created count = %d, want 1", created.Load())
	}
	params.CanonicalHash = []byte("different")
	if _, _, err := store.CreateOrGetRequest(context.Background(), params); !errors.Is(err, ErrConflict) {
		t.Fatalf("payload mismatch error = %v", err)
	}
}

func TestMemoryOnlyOneAttemptFinalizes(t *testing.T) {
	store := NewMemory()
	record, _, _ := store.CreateOrGetRequest(context.Background(), CreateRequestParams{TenantID: "tenant", APIKeyID: "key", IdempotencyKey: "key", CanonicalHash: []byte("hash"), Input: request.ChatInput{Model: "m"}})
	_ = store.TransitionRequest(context.Background(), "tenant", record.ID, request.Received, request.Validated)
	_ = store.TransitionRequest(context.Background(), "tenant", record.ID, request.Validated, request.Routing)
	_ = store.TransitionRequest(context.Background(), "tenant", record.ID, request.Routing, request.InProgress)
	_ = store.StartAttempt(context.Background(), AttemptParams{RequestID: record.ID, TenantID: "tenant", Number: 1})
	_ = store.StartAttempt(context.Background(), AttemptParams{RequestID: record.ID, TenantID: "tenant", Number: 2})
	var successes atomic.Int64
	var wait sync.WaitGroup
	for attempt := 1; attempt <= 2; attempt++ {
		wait.Add(1)
		go func(number int) {
			defer wait.Done()
			err := store.CompleteRequest(context.Background(), CompletionParams{RequestID: record.ID, TenantID: "tenant", AttemptNumber: number, Response: request.ChatResponse{Usage: request.Usage{}}})
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrAlreadyFinal) {
				t.Errorf("unexpected completion error: %v", err)
			}
		}(attempt)
	}
	wait.Wait()
	if successes.Load() != 1 {
		t.Fatalf("final successes = %d, want 1", successes.Load())
	}
}

func TestMemoryConcurrentBudgetReservations(t *testing.T) {
	store := NewMemory()
	store.SetTenantPolicy("tenant", TenantPolicy{MonthlyBudget: 100})
	var admitted atomic.Int64
	var wait sync.WaitGroup
	for index := range 10 {
		record, _, _ := store.CreateOrGetRequest(context.Background(), CreateRequestParams{TenantID: "tenant", APIKeyID: "key", IdempotencyKey: fmt.Sprint(index), CanonicalHash: []byte{byte(index)}, Input: request.ChatInput{Model: "m"}})
		wait.Add(1)
		go func(id string) {
			defer wait.Done()
			if err := store.ReserveBudget(context.Background(), "tenant", id, 30, time.Now()); err == nil {
				admitted.Add(1)
			} else if !errors.Is(err, budget.ErrExceeded) {
				t.Errorf("unexpected reservation error: %v", err)
			}
		}(record.ID)
	}
	wait.Wait()
	if admitted.Load() != 3 {
		t.Fatalf("admitted reservations = %d, want 3", admitted.Load())
	}
}

func TestMemoryTenantIsolation(t *testing.T) {
	store := NewMemory()
	record, _, _ := store.CreateOrGetRequest(context.Background(), CreateRequestParams{TenantID: "tenant-a", APIKeyID: "key", IdempotencyKey: "key", CanonicalHash: []byte("hash"), Input: request.ChatInput{Model: "m"}})
	if _, err := store.GetRequest(context.Background(), "tenant-b", record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant lookup error = %v", err)
	}
}
