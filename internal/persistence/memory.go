package persistence

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/auth"
	"github.com/i192009/aegis-ai-platform/internal/budget"
	"github.com/i192009/aegis-ai-platform/internal/request"
)

type memoryAttempt struct {
	params AttemptParams
	final  bool
}

// Memory is a deterministic, race-safe repository used in unit and transport tests.
type Memory struct {
	mu           sync.RWMutex
	keys         map[string]auth.StoredKey
	requests     map[string]request.Record
	idempotency  map[string]string
	attempts     map[string]map[int]memoryAttempt
	policies     map[string]TenantPolicy
	reservations map[string]int64
	used         map[string]int64
}

// NewMemory creates an empty store.
func NewMemory() *Memory {
	return &Memory{
		keys: make(map[string]auth.StoredKey), requests: make(map[string]request.Record), idempotency: make(map[string]string), attempts: make(map[string]map[int]memoryAttempt), policies: make(map[string]TenantPolicy), reservations: make(map[string]int64), used: make(map[string]int64),
	}
}

// AddAPIKey installs a pre-hashed test or development credential.
func (memory *Memory) AddAPIKey(key auth.StoredKey) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	memory.keys[key.Prefix] = key
}

// SetTenantPolicy installs test admission policy.
func (memory *Memory) SetTenantPolicy(tenantID string, policy TenantPolicy) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	memory.policies[tenantID] = policy
}

func (memory *Memory) FindAPIKeyByPrefix(_ context.Context, prefix string) (auth.StoredKey, error) {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	key, ok := memory.keys[prefix]
	if !ok {
		return auth.StoredKey{}, ErrNotFound
	}
	return key, nil
}

func (memory *Memory) CreateOrGetRequest(_ context.Context, params CreateRequestParams) (request.Record, bool, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	idempotencyKey := params.TenantID + "\x00" + params.IdempotencyKey
	if existingID, ok := memory.idempotency[idempotencyKey]; ok {
		existing := memory.requests[existingID]
		if !bytes.Equal(existing.CanonicalHash, params.CanonicalHash) {
			return request.Record{}, false, ErrConflict
		}
		return cloneRecord(existing), false, nil
	}
	now := time.Now().UTC()
	record := request.Record{
		ID: randomID(), TenantID: params.TenantID, APIKeyID: params.APIKeyID, IdempotencyKey: params.IdempotencyKey, CanonicalHash: bytes.Clone(params.CanonicalHash), Model: params.Input.Model, State: request.Received, Stream: params.Input.Stream, CorrelationID: params.CorrelationID, CreatedAt: now, UpdatedAt: now,
	}
	memory.requests[record.ID] = record
	memory.idempotency[idempotencyKey] = record.ID
	return cloneRecord(record), true, nil
}

func (memory *Memory) GetRequest(_ context.Context, tenantID, requestID string) (request.Record, error) {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	record, ok := memory.requests[requestID]
	if !ok || record.TenantID != tenantID {
		return request.Record{}, ErrNotFound
	}
	return cloneRecord(record), nil
}

func (memory *Memory) TransitionRequest(_ context.Context, tenantID, requestID string, from, to request.State) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	record, ok := memory.requests[requestID]
	if !ok || record.TenantID != tenantID {
		return ErrNotFound
	}
	if record.State != from {
		return fmt.Errorf("expected %s: %w", from, ErrAlreadyFinal)
	}
	if err := request.Transition(from, to); err != nil {
		return err
	}
	record.State, record.UpdatedAt = to, time.Now().UTC()
	memory.requests[requestID] = record
	return nil
}

func (memory *Memory) StartAttempt(_ context.Context, params AttemptParams) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	record, ok := memory.requests[params.RequestID]
	if !ok || record.TenantID != params.TenantID {
		return ErrNotFound
	}
	if memory.attempts[params.RequestID] == nil {
		memory.attempts[params.RequestID] = make(map[int]memoryAttempt)
	}
	if _, exists := memory.attempts[params.RequestID][params.Number]; exists {
		return ErrConflict
	}
	memory.attempts[params.RequestID][params.Number] = memoryAttempt{params: params}
	return nil
}

func (memory *Memory) CompleteRequest(_ context.Context, params CompletionParams) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	record, ok := memory.requests[params.RequestID]
	if !ok || record.TenantID != params.TenantID {
		return ErrNotFound
	}
	if record.State != request.InProgress {
		return ErrAlreadyFinal
	}
	now := time.Now().UTC()
	record.State, record.Response, record.CompletedAt, record.UpdatedAt = request.Completed, &params.Response, &now, now
	record.PromptTokens, record.CompletionTokens = params.Response.Usage.PromptTokens, params.Response.Usage.CompletionTokens
	record.CostMicroUSD = params.CostMicroUSD
	memory.requests[record.ID] = record
	attempt := memory.attempts[params.RequestID][params.AttemptNumber]
	attempt.final = true
	memory.attempts[params.RequestID][params.AttemptNumber] = attempt
	return nil
}

func (memory *Memory) FailAttempt(_ context.Context, params FailureParams) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	record, ok := memory.requests[params.RequestID]
	if !ok || record.TenantID != params.TenantID {
		return ErrNotFound
	}
	if params.Final {
		if record.State != request.InProgress {
			return ErrAlreadyFinal
		}
		now := time.Now().UTC()
		record.State, record.FailureCategory, record.PartialStreamed, record.CompletedAt, record.UpdatedAt = request.Failed, params.Category, params.Partial, &now, now
		memory.requests[record.ID] = record
	}
	return nil
}

func (memory *Memory) CancelRequest(_ context.Context, tenantID, requestID, _ string) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	record, ok := memory.requests[requestID]
	if !ok || record.TenantID != tenantID {
		return ErrNotFound
	}
	if record.State == request.Completed || record.State == request.Failed || record.State == request.Cancelled || record.State == request.BudgetRejected {
		return ErrAlreadyFinal
	}
	now := time.Now().UTC()
	record.State, record.CompletedAt, record.UpdatedAt = request.Cancelled, &now, now
	memory.requests[requestID] = record
	return nil
}

func (memory *Memory) TenantPolicy(_ context.Context, tenantID, _ string) (TenantPolicy, error) {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	policy, ok := memory.policies[tenantID]
	if !ok {
		return TenantPolicy{}, ErrModelForbidden
	}
	return policy, nil
}

func (memory *Memory) ReserveBudget(_ context.Context, tenantID, requestID string, amount int64, _ time.Time) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	policy, ok := memory.policies[tenantID]
	if !ok {
		return ErrNotFound
	}
	if err := budget.CanReserve(policy.MonthlyBudget, memory.used[tenantID], sumMap(memory.reservations, memory.requests, tenantID), amount); err != nil {
		return err
	}
	memory.reservations[requestID] = amount
	return nil
}

func (memory *Memory) RejectBudget(ctx context.Context, tenantID, requestID, _ string, _ int64) error {
	return memory.TransitionRequest(ctx, tenantID, requestID, request.Validated, request.BudgetRejected)
}

func (memory *Memory) ReconcileBudget(_ context.Context, requestID string, actual int64) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	record, ok := memory.requests[requestID]
	if !ok {
		return ErrNotFound
	}
	delete(memory.reservations, requestID)
	memory.used[record.TenantID] += actual
	return nil
}

func (memory *Memory) Close() {}

func cloneRecord(record request.Record) request.Record {
	record.CanonicalHash = bytes.Clone(record.CanonicalHash)
	if record.Response != nil {
		encoded, _ := json.Marshal(record.Response)
		var response request.ChatResponse
		_ = json.Unmarshal(encoded, &response)
		record.Response = &response
	}
	return record
}

func randomID() string {
	value := make([]byte, 16)
	_, _ = rand.Read(value)
	return hex.EncodeToString(value)
}

func sumMap(reservations map[string]int64, requests map[string]request.Record, tenantID string) int64 {
	var total int64
	for requestID, amount := range reservations {
		if requests[requestID].TenantID == tenantID {
			total += amount
		}
	}
	return total
}
