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
	"github.com/i192009/aegis-ai-platform/internal/evaluationjob"
	"github.com/i192009/aegis-ai-platform/internal/observability"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/rabbitmq"
	"github.com/i192009/aegis-ai-platform/internal/version"
	"github.com/i192009/aegis-ai-platform/pkg/middleware"
)

func main() {
	cfg, err := config.Load("evaluation-api")
	if err != nil {
		fatal(err)
	}
	logger := observability.NewLogger("evaluation-api", version.Version, cfg.Environment, cfg.LogLevel)
	postgres, err := persistence.NewPostgres(context.Background(), cfg.DatabaseURL)
	if err != nil {
		fatal(err)
	}
	rabbit, err := rabbitmq.Connect(cfg.RabbitMQURL)
	if err != nil {
		fatal(err)
	}
	service := evaluationjob.New(postgres, rabbit)
	mux := http.NewServeMux()
	api.NewEvaluation(service, postgres, []byte(cfg.APIKeyPepper)).Register(mux)
	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics("evaluation-api", registry)
	handler := middleware.Chain(mux, middleware.RequestContext, func(next http.Handler) http.Handler { return middleware.Recover(logger, next) }, func(next http.Handler) http.Handler { return middleware.BodyLimit(cfg.MaxBodyBytes, next) }, metrics.HTTPMiddleware)
	closeDependencies := func(context.Context) error {
		_ = rabbit.Close()
		postgres.Close()
		return nil
	}
	if err := app.Serve(cfg, logger, handler, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), postgres.Ping, closeDependencies); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
