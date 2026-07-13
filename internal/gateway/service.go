// Package gateway orchestrates authenticated logical requests without owning transport concerns.
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/auth"
	"github.com/i192009/aegis-ai-platform/internal/budget"
	"github.com/i192009/aegis-ai-platform/internal/idempotency"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/provider"
	"github.com/i192009/aegis-ai-platform/internal/ratelimit"
	"github.com/i192009/aegis-ai-platform/internal/request"
	"github.com/i192009/aegis-ai-platform/internal/routing"
)

// Config bounds retries, backoff, and token buffering.
type Config struct {
	Strategy           routing.Strategy
	MaxAttempts        int
	InitialBackoff     time.Duration
	MaximumBackoff     time.Duration
	DefaultMaxTokens   int
	MaximumStreamBytes int
}

// Service coordinates repositories, rate admission, budgets, and provider calls.
type Service struct {
	store   persistence.Store
	router  *routing.Router
	limiter *ratelimit.Limiter
	config  Config
}

// NewService creates a gateway service.
func NewService(store persistence.Store, router *routing.Router, limiter *ratelimit.Limiter, config Config) *Service {
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.InitialBackoff <= 0 {
		config.InitialBackoff = 50 * time.Millisecond
	}
	if config.MaximumBackoff <= 0 {
		config.MaximumBackoff = time.Second
	}
	if config.DefaultMaxTokens <= 0 {
		config.DefaultMaxTokens = 1024
	}
	if config.MaximumStreamBytes <= 0 {
		config.MaximumStreamBytes = 1 << 20
	}
	if config.Strategy == "" {
		config.Strategy = routing.WeightedRoundRobin
	}
	return &Service{store: store, router: router, limiter: limiter, config: config}
}

// Outcome represents a completed response or a repeated request still in progress.
type Outcome struct {
	Record   request.Record
	Response *request.ChatResponse
	Replayed bool
	Pending  bool
}

// Submit performs a non-streaming request.
func (service *Service) Submit(ctx context.Context, principal auth.Principal, idempotencyKey, correlationID string, input request.ChatInput) (Outcome, error) {
	return service.execute(ctx, principal, idempotencyKey, correlationID, input, nil)
}

// Stream performs a request and synchronously emits bounded provider chunks.
func (service *Service) Stream(ctx context.Context, principal auth.Principal, idempotencyKey, correlationID string, input request.ChatInput, emit func(provider.Chunk) error) (Outcome, error) {
	if emit == nil {
		return Outcome{}, errors.New("stream emitter is required")
	}
	return service.execute(ctx, principal, idempotencyKey, correlationID, input, emit)
}

func (service *Service) execute(ctx context.Context, principal auth.Principal, idempotencyKey, correlationID string, input request.ChatInput, emit func(provider.Chunk) error) (Outcome, error) {
	if err := input.Validate(); err != nil {
		return Outcome{}, err
	}
	hash, err := idempotency.HashChat(input)
	if err != nil {
		return Outcome{}, err
	}
	record, created, err := service.store.CreateOrGetRequest(ctx, persistence.CreateRequestParams{
		TenantID: principal.TenantID, APIKeyID: principal.APIKeyID, UserID: principal.UserID, IdempotencyKey: idempotencyKey, CanonicalHash: hash, Input: input, CorrelationID: correlationID,
	})
	if err != nil {
		return Outcome{}, err
	}
	if !created {
		if record.Response != nil && record.State == request.Completed {
			return Outcome{Record: record, Response: record.Response, Replayed: true}, nil
		}
		return Outcome{Record: record, Replayed: true, Pending: true}, nil
	}
	if err := service.store.TransitionRequest(ctx, principal.TenantID, record.ID, request.Received, request.Validated); err != nil {
		return Outcome{}, err
	}
	policy, err := service.store.TenantPolicy(ctx, principal.TenantID, input.Model)
	if err != nil {
		_ = service.store.TransitionRequest(ctx, principal.TenantID, record.ID, request.Validated, request.Failed)
		return Outcome{}, err
	}

	estimatedPrompt := estimatePromptTokens(input.Messages)
	maxCompletion := int64(input.MaxTokens)
	if maxCompletion == 0 {
		maxCompletion = int64(service.config.DefaultMaxTokens)
	}
	permits, err := service.admit(ctx, principal, policy, estimatedPrompt+maxCompletion)
	if err != nil {
		_ = service.store.TransitionRequest(ctx, principal.TenantID, record.ID, request.Validated, request.Failed)
		return Outcome{}, err
	}
	defer releasePermits(permits)

	selection := routing.Selection{Model: input.Model, DataClassification: policy.DataClassification, AllowedProviders: policy.AllowedProviders, Strategy: service.config.Strategy}
	maxInputPrice, maxOutputPrice, err := service.router.HighestPrices(selection)
	if err != nil {
		_ = service.store.TransitionRequest(ctx, principal.TenantID, record.ID, request.Validated, request.Failed)
		return Outcome{}, err
	}
	reservation, err := budget.CostMicroUSD(estimatedPrompt, maxCompletion, maxInputPrice, maxOutputPrice)
	if err != nil {
		return Outcome{}, err
	}
	if err := service.store.ReserveBudget(ctx, principal.TenantID, record.ID, reservation, time.Now()); err != nil {
		if errors.Is(err, budget.ErrExceeded) {
			_ = service.store.RejectBudget(context.WithoutCancel(ctx), principal.TenantID, record.ID, correlationID, reservation)
		}
		return Outcome{}, err
	}
	if err := service.store.TransitionRequest(ctx, principal.TenantID, record.ID, request.Validated, request.Routing); err != nil {
		return Outcome{}, err
	}
	if err := service.store.TransitionRequest(ctx, principal.TenantID, record.ID, request.Routing, request.InProgress); err != nil {
		return Outcome{}, err
	}

	excluded := make(map[string]struct{})
	var lastErr error
	finalized := false
	for attemptNumber := 1; attemptNumber <= service.config.MaxAttempts; attemptNumber++ {
		selection.ExcludedProviders = excluded
		selection.Now = time.Now()
		lease, err := service.router.Select(selection)
		if err != nil {
			lastErr = err
			break
		}
		excluded[lease.Provider.Name()] = struct{}{}
		if err := service.store.StartAttempt(ctx, persistence.AttemptParams{RequestID: record.ID, TenantID: principal.TenantID, Number: attemptNumber, ProviderName: lease.Provider.Name(), Model: input.Model, CorrelationID: correlationID}); err != nil {
			lease.Failure(false, time.Now())
			return Outcome{}, err
		}

		started := time.Now()
		providerCtx, cancel := context.WithTimeout(ctx, lease.Timeout)
		completion, partial, callErr := service.call(providerCtx, lease.Provider, record.ID, input, emit)
		cancel()
		if callErr == nil {
			cost, costErr := budget.CostMicroUSD(completion.Usage.PromptTokens, completion.Usage.CompletionTokens, lease.InputCostPerMillionMicro, lease.OutputCostPerMillionMicro)
			if costErr != nil {
				lease.Failure(false, time.Now())
				return Outcome{}, costErr
			}
			response := buildResponse(record.ID, input.Model, completion)
			if err := service.store.CompleteRequest(ctx, persistence.CompletionParams{RequestID: record.ID, TenantID: principal.TenantID, AttemptNumber: attemptNumber, ProviderRequestID: completion.ProviderRequestID, Response: response, CostMicroUSD: cost, StartedAt: started, CorrelationID: correlationID}); err != nil {
				lease.Failure(false, time.Now())
				return Outcome{}, err
			}
			lease.Success(time.Since(started))
			_ = service.store.ReconcileBudget(context.WithoutCancel(ctx), record.ID, cost)
			record.State, record.Response, record.CostMicroUSD = request.Completed, &response, cost
			return Outcome{Record: record, Response: &response}, nil
		}

		retryable := provider.IsRetryable(callErr) && !partial && attemptNumber < service.config.MaxAttempts
		lease.Failure(retryable, time.Now())
		category := errorCategory(callErr)
		if err := service.store.FailAttempt(context.WithoutCancel(ctx), persistence.FailureParams{RequestID: record.ID, TenantID: principal.TenantID, AttemptNumber: attemptNumber, Category: category, Retryable: retryable, Partial: partial, Final: !retryable, StartedAt: started, CorrelationID: correlationID}); err != nil {
			return Outcome{}, err
		}
		lastErr = callErr
		if !retryable {
			finalized = true
			break
		}
		if err := waitBackoff(ctx, service.config.InitialBackoff, service.config.MaximumBackoff, attemptNumber); err != nil {
			lastErr = err
			break
		}
	}
	if lastErr == nil {
		lastErr = routing.ErrNoEligibleProvider
	}
	if !finalized {
		_ = service.store.FailAttempt(context.WithoutCancel(ctx), persistence.FailureParams{RequestID: record.ID, TenantID: principal.TenantID, Category: errorCategory(lastErr), Final: true, StartedAt: time.Now(), CorrelationID: correlationID})
	}
	_ = service.store.ReconcileBudget(context.WithoutCancel(ctx), record.ID, 0)
	return Outcome{}, lastErr
}

func (service *Service) call(ctx context.Context, selected provider.Provider, requestID string, input request.ChatInput, emit func(provider.Chunk) error) (provider.Completion, bool, error) {
	providerInput := provider.CompletionRequest{RequestID: requestID, Model: input.Model, Messages: input.Messages, Temperature: input.Temperature, MaxTokens: input.MaxTokens}
	if emit == nil {
		completion, err := selected.Complete(ctx, providerInput)
		return completion, false, err
	}
	var content strings.Builder
	partial := false
	usage, err := selected.Stream(ctx, providerInput, func(chunk provider.Chunk) error {
		if content.Len()+len(chunk.Content) > service.config.MaximumStreamBytes {
			return errors.New("stream exceeded bounded response size")
		}
		if chunk.Content != "" {
			partial = true
			content.WriteString(chunk.Content)
		}
		return emit(chunk)
	})
	if err != nil {
		return provider.Completion{}, partial, err
	}
	return provider.Completion{Model: input.Model, Content: content.String(), FinishReason: "stop", Usage: usage}, partial, nil
}

func (service *Service) admit(ctx context.Context, principal auth.Principal, policy persistence.TenantPolicy, estimatedTokens int64) ([]*ratelimit.Permit, error) {
	if service.limiter == nil {
		return nil, nil
	}
	limits := ratelimit.Limits{RequestsPerMinute: policy.RequestsPerMinute, TokensPerMinute: policy.TokensPerMinute, MaxConcurrent: policy.MaxConcurrent}
	permits := make([]*ratelimit.Permit, 0, 2)
	for _, bucket := range []string{"tenant:" + principal.TenantID, "key:" + principal.APIKeyID} {
		decision, permit, err := service.limiter.Allow(ctx, bucket, limits, estimatedTokens, 2*time.Minute)
		if err != nil {
			releasePermits(permits)
			return nil, err
		}
		if !decision.Allowed {
			releasePermits(permits)
			return nil, fmt.Errorf("rate limit rejected: %s", decision.Reason)
		}
		permits = append(permits, permit)
	}
	return permits, nil
}

func (service *Service) Get(ctx context.Context, tenantID, requestID string) (request.Record, error) {
	return service.store.GetRequest(ctx, tenantID, requestID)
}

func (service *Service) Cancel(ctx context.Context, tenantID, requestID, correlationID string) error {
	return service.store.CancelRequest(ctx, tenantID, requestID, correlationID)
}

func buildResponse(requestID, model string, completion provider.Completion) request.ChatResponse {
	return request.ChatResponse{ID: "chatcmpl-" + requestID, Object: "chat.completion", Created: time.Now().Unix(), Model: model, Choices: []request.Choice{{Index: 0, Message: request.Message{Role: "assistant", Content: completion.Content}, FinishReason: completion.FinishReason}}, Usage: completion.Usage, Aegis: request.ResponseMeta{RequestID: requestID, Status: request.Completed}}
}

func estimatePromptTokens(messages []request.Message) int64 {
	characters := 0
	for _, message := range messages {
		characters += len(message.Content)
	}
	return int64((characters + 3) / 4)
}

func errorCategory(err error) string {
	var providerErr *provider.Error
	if errors.As(err, &providerErr) {
		return providerErr.Category
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "provider_unavailable"
}

func waitBackoff(ctx context.Context, initial, maximum time.Duration, attempt int) error {
	delay := initial << (attempt - 1)
	if delay > maximum {
		delay = maximum
	}
	var random [8]byte
	_, _ = rand.Read(random[:])
	jitter := time.Duration(binary.BigEndian.Uint64(random[:]) % uint64(max(time.Nanosecond, delay/2)))
	timer := time.NewTimer(delay/2 + jitter)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func releasePermits(permits []*ratelimit.Permit) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, permit := range permits {
		_ = permit.Release(ctx)
	}
}
