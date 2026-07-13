// Package observability configures logs, metrics, and traces.
package observability

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger returns a JSON logger with stable service-level fields.
func NewLogger(service, version, environment, level string) *slog.Logger {
	var slogLevel slog.Level
	switch strings.ToUpper(level) {
	case "DEBUG":
		slogLevel = slog.LevelDebug
	case "WARN", "WARNING":
		slogLevel = slog.LevelWarn
	case "ERROR":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel, ReplaceAttr: func(_ []string, attribute slog.Attr) slog.Attr {
		switch attribute.Key {
		case slog.TimeKey:
			attribute.Key = "timestamp"
		case slog.LevelKey:
			attribute.Key = "severity"
		case slog.MessageKey:
			attribute.Key = "message"
		}
		return attribute
	}})
	return slog.New(handler).With(
		"service", service,
		"version", version,
		"environment", environment,
	)
}
