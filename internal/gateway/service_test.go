package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/auth"
	"github.com/i192009/aegis-ai-platform/internal/circuitbreaker"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/provider"
	"github.com/i192009/aegis-ai-platform/internal/request"
	"github.com/i192009/aegis-ai-platform/internal/routing"
)

func TestSubmitAndIdempotentReplay(t *testing.T) {
	store := persistence.NewMemory()
	store.SetTenantPolicy("tenant", persistence.TenantPolicy{DataClassification: "INTERNAL", AllowedProviders: map[string]struct{}{"mock": {}}, RequestsPerMinute: 100, TokensPerMinute: 10000, MaxConcurrent: 10})
	mock := provider.NewMock(provider.MockConfig{Name: "mock", Models: []string{"m"}})
	router, _ := routing.New([]routing.ProviderConfig{{Provider: mock, Enabled: true, InputCostPerMillionMicro: 1, OutputCostPerMillionMicro: 1}}, circuitbreaker.Config{})
	service := NewService(store, router, nil, Config{})
	principal := auth.Principal{TenantID: "tenant", APIKeyID: "key"}
	input := request.ChatInput{Model: "m", Messages: []request.Message{{Role: "user", Content: "hello"}}}
	first, err := service.Submit(context.Background(), principal, "idempotency-key", "correlation", input)
	if err != nil || first.Response == nil {
		t.Fatalf("first submit = %+v, %v", first, err)
	}
	second, err := service.Submit(context.Background(), principal, "idempotency-key", "correlation", input)
	if err != nil || !second.Replayed || second.Response == nil || second.Response.ID != first.Response.ID {
		t.Fatalf("replay = %+v, %v", second, err)
	}
	input.Messages[0].Content = "different"
	if _, err := service.Submit(context.Background(), principal, "idempotency-key", "correlation", input); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("mismatched replay error = %v", err)
	}
}

func TestProviderFailover(t *testing.T) {
	store := persistence.NewMemory()
	store.SetTenantPolicy("tenant", persistence.TenantPolicy{DataClassification: "INTERNAL", AllowedProviders: map[string]struct{}{"first": {}, "second": {}}, RequestsPerMinute: 100, TokensPerMinute: 10000, MaxConcurrent: 10})
	first := provider.NewMock(provider.MockConfig{Name: "first", Models: []string{"m"}, FailFirst: 1})
	second := provider.NewMock(provider.MockConfig{Name: "second", Models: []string{"m"}})
	router, _ := routing.New([]routing.ProviderConfig{{Provider: first, Enabled: true}, {Provider: second, Enabled: true}}, circuitbreaker.Config{})
	service := NewService(store, router, nil, Config{InitialBackoff: time.Millisecond, MaximumBackoff: time.Millisecond})
	outcome, err := service.Submit(context.Background(), auth.Principal{TenantID: "tenant", APIKeyID: "key"}, "failover-key", "correlation", request.ChatInput{Model: "m", Messages: []request.Message{{Role: "user", Content: "hello"}}})
	if err != nil || outcome.Response == nil {
		t.Fatalf("failover submit = %+v, %v", outcome, err)
	}
}

func TestStreamingEmitterFailureStopsWithoutFailover(t *testing.T) {
	store := persistence.NewMemory()
	store.SetTenantPolicy("tenant", persistence.TenantPolicy{DataClassification: "INTERNAL", AllowedProviders: map[string]struct{}{"mock": {}}, RequestsPerMinute: 100, TokensPerMinute: 10000, MaxConcurrent: 10})
	mock := provider.NewMock(provider.MockConfig{Name: "mock", Models: []string{"m"}})
	router, _ := routing.New([]routing.ProviderConfig{{Provider: mock, Enabled: true}}, circuitbreaker.Config{})
	service := NewService(store, router, nil, Config{})
	_, err := service.Stream(context.Background(), auth.Principal{TenantID: "tenant", APIKeyID: "key"}, "stream-key", "correlation", request.ChatInput{Model: "m", Stream: true, Messages: []request.Message{{Role: "user", Content: "hello"}}}, func(provider.Chunk) error { return errors.New("client disconnected") })
	if err == nil {
		t.Fatal("stream emitter failure was ignored")
	}
}
