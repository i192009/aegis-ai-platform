package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/i192009/aegis-ai-platform/internal/api"
	"github.com/i192009/aegis-ai-platform/internal/app"
	"github.com/i192009/aegis-ai-platform/internal/auth"
	"github.com/i192009/aegis-ai-platform/internal/circuitbreaker"
	"github.com/i192009/aegis-ai-platform/internal/config"
	"github.com/i192009/aegis-ai-platform/internal/gateway"
	"github.com/i192009/aegis-ai-platform/internal/observability"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/provider"
	"github.com/i192009/aegis-ai-platform/internal/ratelimit"
	"github.com/i192009/aegis-ai-platform/internal/routing"
	"github.com/i192009/aegis-ai-platform/internal/version"
	"github.com/i192009/aegis-ai-platform/pkg/middleware"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("gateway")
	if err != nil {
		return err
	}
	logger := observability.NewLogger("gateway", version.Version, cfg.Environment, cfg.LogLevel)
	shutdownTracing, err := observability.SetupTracing(context.Background(), "gateway", cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	store, err := gatewayStore(context.Background(), cfg)
	if err != nil {
		return err
	}
	var redisClient *redis.Client
	var limiter *ratelimit.Limiter
	if cfg.RedisAddress != "" {
		redisClient = redis.NewClient(&redis.Options{Addr: cfg.RedisAddress, DialTimeout: 3 * time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
		limiter = ratelimit.New(redisClient, "aegis:limit")
	}
	selectedProvider, err := configuredProvider(cfg)
	if err != nil {
		return err
	}
	router, err := routing.New([]routing.ProviderConfig{{Provider: selectedProvider, Weight: 1, Priority: 10, MaxConcurrency: 50, Timeout: 30 * time.Second, InputCostPerMillionMicro: 1000, OutputCostPerMillionMicro: 2000, Enabled: true}}, circuitbreaker.Config{FailureThreshold: 5, Window: 30 * time.Second, OpenDuration: 15 * time.Second, HalfOpenProbes: 1})
	if err != nil {
		return err
	}
	service := gateway.NewService(store, router, limiter, gateway.Config{Strategy: routing.Strategy(cfg.RoutingStrategy), MaxAttempts: 3})
	businessMux := http.NewServeMux()
	api.NewGateway(service, store, []byte(cfg.APIKeyPepper)).Register(businessMux)
	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics("gateway", registry)
	handler := middleware.Chain(businessMux,
		middleware.RequestContext,
		func(next http.Handler) http.Handler { return middleware.Recover(logger, next) },
		func(next http.Handler) http.Handler { return middleware.BodyLimit(cfg.MaxBodyBytes, next) },
		metrics.HTTPMiddleware,
		func(next http.Handler) http.Handler { return otelhttp.NewHandler(next, "gateway.http") },
	)
	ready := func(ctx context.Context) error {
		if redisClient != nil {
			return redisClient.Ping(ctx).Err()
		}
		return nil
	}
	closeDependencies := func(ctx context.Context) error {
		store.Close()
		if redisClient != nil {
			_ = redisClient.Close()
		}
		return shutdownTracing(ctx)
	}
	return app.Serve(cfg, logger, handler, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), ready, closeDependencies)
}

func gatewayStore(ctx context.Context, cfg config.Config) (persistence.Store, error) {
	if !cfg.DevMemory {
		return persistence.NewPostgres(ctx, cfg.DatabaseURL)
	}
	memory := persistence.NewMemory()
	prefix, err := auth.ParsePrefix(cfg.DevAPIKey)
	if err != nil {
		return nil, err
	}
	memory.AddAPIKey(auth.StoredKey{ID: "00000000-0000-0000-0000-000000000003", TenantID: "00000000-0000-0000-0000-000000000001", Prefix: prefix, Hash: auth.Hash([]byte(cfg.APIKeyPepper), cfg.DevAPIKey), Scopes: []string{"*"}})
	memory.SetTenantPolicy("00000000-0000-0000-0000-000000000001", persistence.TenantPolicy{DataClassification: "INTERNAL", AllowedProviders: map[string]struct{}{cfg.ProviderName: {}}, RequestsPerMinute: 1000, TokensPerMinute: 1_000_000, MaxConcurrent: 100, MonthlyBudget: 100_000_000})
	return memory, nil
}

func configuredProvider(cfg config.Config) (provider.Provider, error) {
	if cfg.ProviderEndpoint == "" {
		return provider.NewMock(provider.MockConfig{Name: cfg.ProviderName, Models: cfg.ProviderModels}), nil
	}
	return provider.NewOpenAICompatible(provider.OpenAIConfig{Name: cfg.ProviderName, Endpoint: cfg.ProviderEndpoint, APIKey: cfg.ProviderAPIKey, Models: cfg.ProviderModels, DataClassifications: []string{"PUBLIC", "INTERNAL", "CONFIDENTIAL"}, Timeout: 30 * time.Second})
}
