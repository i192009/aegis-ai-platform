package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/i192009/aegis-ai-platform/internal/app"
	"github.com/i192009/aegis-ai-platform/internal/config"
	broker "github.com/i192009/aegis-ai-platform/internal/kafka"
	"github.com/i192009/aegis-ai-platform/internal/observability"
	"github.com/i192009/aegis-ai-platform/internal/outbox"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/version"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("outbox-relay")
	if err != nil {
		return err
	}
	logger := observability.NewLogger("outbox-relay", version.Version, cfg.Environment, cfg.LogLevel)
	postgres, err := persistence.NewPostgres(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return err
	}
	publisher := broker.NewPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	workerID, _ := os.Hostname()
	relay := outbox.NewRelay(outbox.NewRepository(postgres.Pool()), publisher, workerID, 100, 500*time.Millisecond)
	workerCtx, cancel := context.WithCancel(context.Background())
	var workerFailed atomic.Bool
	go func() {
		if err := relay.Run(workerCtx); err != nil {
			workerFailed.Store(true)
			logger.Error("outbox relay stopped", "operation", "outbox.run", "error_category", "relay_failure")
		}
	}()
	registry := prometheus.NewRegistry()
	_ = observability.NewMetrics("outbox-relay", registry)
	ready := func(ctx context.Context) error {
		if workerFailed.Load() {
			return fmt.Errorf("relay worker stopped")
		}
		return postgres.Ping(ctx)
	}
	closeDependencies := func(context.Context) error {
		cancel()
		_ = publisher.Close()
		postgres.Close()
		return nil
	}
	return app.Serve(cfg, logger, nil, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), ready, closeDependencies)
}
