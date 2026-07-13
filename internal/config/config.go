// Package config loads and validates process configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPAddress       = ":8080"
	defaultShutdownTimeout   = 20 * time.Second
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 15 * time.Second
	defaultWriteTimeout      = 30 * time.Second
	defaultIdleTimeout       = 60 * time.Second
	defaultMaxHeaderBytes    = 1 << 20
)

// Config contains common configuration shared by AegisAI services.
type Config struct {
	Service            string
	Environment        string
	LogLevel           string
	HTTPAddress        string
	ShutdownTimeout    time.Duration
	ReadHeaderTimeout  time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	IdleTimeout        time.Duration
	MaxHeaderBytes     int
	DatabaseURL        string
	RedisAddress       string
	KafkaBrokers       []string
	RabbitMQURL        string
	APIKeyPepper       string
	DevAPIKey          string
	ProviderName       string
	ProviderEndpoint   string
	ProviderAPIKey     string
	ProviderModels     []string
	RoutingStrategy    string
	KafkaTopic         string
	KafkaConsumerGroup string
	OTLPEndpoint       string
	MaxBodyBytes       int64
	WorkerConcurrency  int
	DevMemory          bool
}

// Load reads configuration for service. It does not connect to dependencies.
func Load(service string) (Config, error) {
	if strings.TrimSpace(service) == "" {
		return Config{}, errors.New("service name is required")
	}

	var err error
	cfg := Config{
		Service:            service,
		Environment:        envOr("AEGIS_ENVIRONMENT", "development"),
		LogLevel:           strings.ToUpper(envOr("AEGIS_LOG_LEVEL", "INFO")),
		HTTPAddress:        envOr("AEGIS_HTTP_ADDRESS", defaultHTTPAddress),
		ShutdownTimeout:    defaultShutdownTimeout,
		ReadHeaderTimeout:  defaultReadHeaderTimeout,
		ReadTimeout:        defaultReadTimeout,
		WriteTimeout:       defaultWriteTimeout,
		IdleTimeout:        defaultIdleTimeout,
		MaxHeaderBytes:     defaultMaxHeaderBytes,
		DatabaseURL:        strings.TrimSpace(os.Getenv("AEGIS_DATABASE_URL")),
		RedisAddress:       strings.TrimSpace(os.Getenv("AEGIS_REDIS_ADDRESS")),
		RabbitMQURL:        strings.TrimSpace(os.Getenv("AEGIS_RABBITMQ_URL")),
		APIKeyPepper:       envOr("AEGIS_API_KEY_PEPPER", "development-only-pepper-change-me-32-bytes"),
		DevAPIKey:          envOr("AEGIS_DEV_API_KEY", "aegis_devkey0000_0123456789abcdefghijklmnopqrstuvwxyzABCDEFG"),
		ProviderName:       envOr("AEGIS_PROVIDER_NAME", "mock-primary"),
		ProviderEndpoint:   strings.TrimSpace(os.Getenv("AEGIS_PROVIDER_ENDPOINT")),
		ProviderAPIKey:     strings.TrimSpace(os.Getenv("AEGIS_PROVIDER_API_KEY")),
		RoutingStrategy:    envOr("AEGIS_ROUTING_STRATEGY", "weighted_round_robin"),
		KafkaTopic:         envOr("AEGIS_KAFKA_TOPIC", "aegis.events.v1"),
		KafkaConsumerGroup: envOr("AEGIS_KAFKA_CONSUMER_GROUP", "aegis-audit-v1"),
		OTLPEndpoint:       strings.TrimSpace(os.Getenv("AEGIS_OTLP_ENDPOINT")),
		MaxBodyBytes:       1 << 20,
		WorkerConcurrency:  4,
	}
	cfg.DevMemory, err = boolEnv("AEGIS_DEV_MEMORY", cfg.DatabaseURL == "")
	if err != nil {
		return Config{}, err
	}

	if cfg.ShutdownTimeout, err = durationEnv("AEGIS_SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ReadHeaderTimeout, err = durationEnv("AEGIS_READ_HEADER_TIMEOUT", cfg.ReadHeaderTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ReadTimeout, err = durationEnv("AEGIS_READ_TIMEOUT", cfg.ReadTimeout); err != nil {
		return Config{}, err
	}
	if cfg.WriteTimeout, err = durationEnv("AEGIS_WRITE_TIMEOUT", cfg.WriteTimeout); err != nil {
		return Config{}, err
	}
	if cfg.IdleTimeout, err = durationEnv("AEGIS_IDLE_TIMEOUT", cfg.IdleTimeout); err != nil {
		return Config{}, err
	}
	if cfg.MaxHeaderBytes, err = intEnv("AEGIS_MAX_HEADER_BYTES", cfg.MaxHeaderBytes); err != nil {
		return Config{}, err
	}
	if maxBody, parseErr := intEnv("AEGIS_MAX_BODY_BYTES", int(cfg.MaxBodyBytes)); parseErr != nil {
		return Config{}, parseErr
	} else {
		cfg.MaxBodyBytes = int64(maxBody)
	}
	if cfg.WorkerConcurrency, err = intEnv("AEGIS_WORKER_CONCURRENCY", cfg.WorkerConcurrency); err != nil {
		return Config{}, err
	}
	for _, model := range strings.Split(envOr("AEGIS_PROVIDER_MODELS", "aegis-small,aegis-medium"), ",") {
		if model = strings.TrimSpace(model); model != "" {
			cfg.ProviderModels = append(cfg.ProviderModels, model)
		}
	}

	for _, broker := range strings.Split(os.Getenv("AEGIS_KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
		}
	}

	if cfg.ShutdownTimeout <= 0 || cfg.ReadHeaderTimeout <= 0 || cfg.MaxHeaderBytes <= 0 || cfg.MaxBodyBytes <= 0 || cfg.WorkerConcurrency <= 0 {
		return Config{}, errors.New("timeouts and maximum header bytes must be positive")
	}
	if len(cfg.APIKeyPepper) < 32 {
		return Config{}, errors.New("AEGIS_API_KEY_PEPPER must contain at least 32 bytes")
	}
	if len(cfg.ProviderModels) == 0 {
		return Config{}, errors.New("at least one provider model is required")
	}
	return cfg, nil
}

func boolEnv(name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func intEnv(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}
