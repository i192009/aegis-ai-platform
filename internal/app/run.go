// Package app wires process-level concerns shared by AegisAI commands.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/config"
	"github.com/i192009/aegis-ai-platform/internal/health"
	"github.com/i192009/aegis-ai-platform/internal/version"
)

// Serve runs a configured HTTP service and applies the shared graceful-shutdown order.
func Serve(cfg config.Config, logger *slog.Logger, business http.Handler, metricsHandler http.Handler, readyCheck func(context.Context) error, closeDependencies func(context.Context) error) error {
	state := health.NewState()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", state.Live)
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		if !state.IsReady() {
			state.Ready(w, r)
			return
		}
		if readyCheck != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := readyCheck(ctx); err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"status":"dependency_unready"}`))
				return
			}
		}
		state.Ready(w, r)
	})
	mux.HandleFunc("GET /version", versionHandler)
	if metricsHandler != nil {
		mux.Handle("GET /metrics", metricsHandler)
	} else {
		mux.HandleFunc("GET /metrics", baseMetricsHandler)
	}
	if business != nil {
		mux.Handle("/", business)
	}
	server := &http.Server{Addr: cfg.HTTPAddress, Handler: accessLog(logger, mux), ReadHeaderTimeout: cfg.ReadHeaderTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout, IdleTimeout: cfg.IdleTimeout, MaxHeaderBytes: cfg.MaxHeaderBytes}
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server starting", "operation", "server.start", "address", cfg.HTTPAddress)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	state.SetReady(true)
	select {
	case <-rootCtx.Done():
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("serve HTTP: %w", err)
		}
	}
	state.SetReady(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	serverErr := server.Shutdown(shutdownCtx)
	if closeDependencies != nil {
		if err := closeDependencies(shutdownCtx); err != nil && serverErr == nil {
			serverErr = err
		}
	}
	if serverErr != nil {
		_ = server.Close()
		return fmt.Errorf("graceful shutdown: %w", serverErr)
	}
	logger.Info("shutdown complete", "operation", "server.shutdown", "outcome", "success")
	return nil
}

func versionHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(version.Current())
}

func baseMetricsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte("# HELP aegis_build_info Static build information.\n# TYPE aegis_build_info gauge\naegis_build_info 1\n"))
}

func accessLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		outcome := "success"
		if recorder.status >= 500 {
			outcome = "server_error"
		} else if recorder.status >= 400 {
			outcome = "client_error"
		}
		logger.Info("http request",
			"operation", "http.request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"request_id", recorder.Header().Get("X-Request-ID"),
			"correlation_id", recorder.Header().Get("X-Correlation-ID"),
			"duration_ms", time.Since(started).Milliseconds(),
			"outcome", outcome,
		)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *responseRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

func (recorder *responseRecorder) Flush() {
	if flusher, ok := recorder.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
