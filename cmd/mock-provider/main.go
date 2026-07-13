package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/i192009/aegis-ai-platform/internal/api"
	"github.com/i192009/aegis-ai-platform/internal/app"
	"github.com/i192009/aegis-ai-platform/internal/config"
	"github.com/i192009/aegis-ai-platform/internal/observability"
	"github.com/i192009/aegis-ai-platform/internal/provider"
	"github.com/i192009/aegis-ai-platform/internal/version"
	"github.com/i192009/aegis-ai-platform/pkg/middleware"
)

func main() {
	cfg, err := config.Load("mock-provider")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger := observability.NewLogger("mock-provider", version.Version, cfg.Environment, cfg.LogLevel)
	mux := http.NewServeMux()
	api.NewMockProvider(provider.NewMock(provider.MockConfig{Name: cfg.ProviderName, Models: cfg.ProviderModels})).Register(mux)
	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics("mock-provider", registry)
	handler := middleware.Chain(mux, middleware.RequestContext, func(next http.Handler) http.Handler { return middleware.Recover(logger, next) }, metrics.HTTPMiddleware)
	if err := app.Serve(cfg, logger, handler, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), nil, func(context.Context) error { return nil }); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
