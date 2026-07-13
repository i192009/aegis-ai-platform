package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/i192009/aegis-ai-platform/internal/app"
	"github.com/i192009/aegis-ai-platform/internal/config"
	"github.com/i192009/aegis-ai-platform/internal/evaluation"
	"github.com/i192009/aegis-ai-platform/internal/observability"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/rabbitmq"
	"github.com/i192009/aegis-ai-platform/internal/version"
)

func main() {
	cfg, err := config.Load("evaluation-worker")
	if err != nil {
		fatal(err)
	}
	logger := observability.NewLogger("evaluation-worker", version.Version, cfg.Environment, cfg.LogLevel)
	postgres, err := persistence.NewPostgres(context.Background(), cfg.DatabaseURL)
	if err != nil {
		fatal(err)
	}
	rabbit, err := rabbitmq.Connect(cfg.RabbitMQURL)
	if err != nil {
		fatal(err)
	}
	evaluators := []evaluation.Evaluator{evaluation.PII{}, evaluation.Quality{}, evaluation.Safety{}, evaluation.Performance{}}
	worker := rabbitmq.NewWorker(rabbit, postgres, evaluators, cfg.WorkerConcurrency, 3, logger)
	workerCtx, cancel := context.WithCancel(context.Background())
	var workerFailed atomic.Bool
	go func() {
		if err := worker.Run(workerCtx); err != nil {
			workerFailed.Store(true)
			logger.Error("evaluation worker stopped", "operation", "rabbitmq.consume", "error_category", "worker_failure")
		}
	}()
	registry := prometheus.NewRegistry()
	_ = observability.NewMetrics("evaluation-worker", registry)
	ready := func(ctx context.Context) error {
		if workerFailed.Load() {
			return fmt.Errorf("worker stopped")
		}
		return postgres.Ping(ctx)
	}
	closeDependencies := func(context.Context) error {
		cancel()
		_ = rabbit.Close()
		postgres.Close()
		return nil
	}
	if err := app.Serve(cfg, logger, nil, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), ready, closeDependencies); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
