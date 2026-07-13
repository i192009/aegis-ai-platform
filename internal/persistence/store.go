// Package persistence defines explicit repositories and PostgreSQL transaction boundaries.
package persistence

import (
	"context"
	"errors"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/auth"
	"github.com/i192009/aegis-ai-platform/internal/request"
)

var (
	ErrNotFound       = errors.New("record not found")
	ErrConflict       = errors.New("idempotency key payload conflict")
	ErrAlreadyFinal   = errors.New("request already finalized")
	ErrModelForbidden = errors.New("model is not enabled for tenant")
)

// TenantPolicy is the authenticated tenant's gateway admission configuration.
type TenantPolicy struct {
	DataClassification string
	AllowedProviders   map[string]struct{}
	RequestsPerMinute  int64
	TokensPerMinute    int64
	MaxConcurrent      int64
	MonthlyBudget      int64
	DailyBudget        int64
}

// CreateRequestParams is all client-owned data needed to establish idempotency.
type CreateRequestParams struct {
	TenantID       string
	APIKeyID       string
	UserID         string
	IdempotencyKey string
	CanonicalHash  []byte
	Input          request.ChatInput
	CorrelationID  string
}

// AttemptParams starts one physical provider attempt.
type AttemptParams struct {
	RequestID     string
	TenantID      string
	Number        int
	ProviderName  string
	Model         string
	CorrelationID string
}

// CompletionParams atomically accepts one final result and writes its outbox fact.
type CompletionParams struct {
	RequestID         string
	TenantID          string
	AttemptNumber     int
	ProviderRequestID string
	Response          request.ChatResponse
	CostMicroUSD      int64
	StartedAt         time.Time
	CorrelationID     string
}

// FailureParams records one attempt and optionally finalizes the logical request.
type FailureParams struct {
	RequestID     string
	TenantID      string
	AttemptNumber int
	Category      string
	Retryable     bool
	Partial       bool
	Final         bool
	StartedAt     time.Time
	CorrelationID string
}

// Store is implemented by explicit SQL and a race-safe test store.
type Store interface {
	auth.Lookup
	CreateOrGetRequest(context.Context, CreateRequestParams) (request.Record, bool, error)
	GetRequest(context.Context, string, string) (request.Record, error)
	TransitionRequest(context.Context, string, string, request.State, request.State) error
	StartAttempt(context.Context, AttemptParams) error
	CompleteRequest(context.Context, CompletionParams) error
	FailAttempt(context.Context, FailureParams) error
	CancelRequest(context.Context, string, string, string) error
	TenantPolicy(context.Context, string, string) (TenantPolicy, error)
	ReserveBudget(context.Context, string, string, int64, time.Time) error
	RejectBudget(context.Context, string, string, string, int64) error
	ReconcileBudget(context.Context, string, int64) error
	Close()
}
