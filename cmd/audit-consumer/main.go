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
	broker "github.com/i192009/aegis-ai-platform/internal/kafka"
	"github.com/i192009/aegis-ai-platform/internal/observability"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/version"
)

func main() {
	cfg, err := config.Load("audit-consumer")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger := observability.NewLogger("audit-consumer", version.Version, cfg.Environment, cfg.LogLevel)
	postgres, err := persistence.NewPostgres(context.Background(), cfg.DatabaseURL)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	consumer := broker.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaConsumerGroup, "audit-usage-v1", postgres, logger)
	workerCtx, cancel := context.WithCancel(context.Background())
	var workerFailed atomic.Bool
	go func() {
		if err := consumer.Run(workerCtx); err != nil {
			workerFailed.Store(true)
			logger.Error("Kafka consumer stopped", "operation", "kafka.consume", "error_category", "consumer_failure")
		}
	}()
	registry := prometheus.NewRegistry()
	_ = observability.NewMetrics("audit-consumer", registry)
	ready := func(ctx context.Context) error {
		if workerFailed.Load() {
			return fmt.Errorf("consumer stopped")
		}
		return postgres.Ping(ctx)
	}
	closeDependencies := func(context.Context) error {
		cancel()
		_ = consumer.Close()
		postgres.Close()
		return nil
	}
	if err := app.Serve(cfg, logger, nil, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), ready, closeDependencies); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
