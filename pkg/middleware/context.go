// Package middleware provides low-level HTTP safety and request-context middleware.
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/i192009/aegis-ai-platform/pkg/problem"
)

type contextKey uint8

const (
	requestIDKey contextKey = iota + 1
	correlationIDKey
)

// RequestID returns the server request identifier assigned by RequestContext.
func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}

// CorrelationID returns the validated correlation identifier assigned by RequestContext.
func CorrelationID(ctx context.Context) string {
	value, _ := ctx.Value(correlationIDKey).(string)
	return value
}

// RequestContext assigns low-cardinality opaque identifiers and echoes them to the client.
func RequestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := newID()
		correlationID := sanitizeID(r.Header.Get("X-Correlation-ID"))
		if correlationID == "" {
			correlationID = requestID
		}
		ctx := context.WithValue(r.Context(), requestIDKey, requestID)
		ctx = context.WithValue(ctx, correlationIDKey, correlationID)
		w.Header().Set("X-Request-ID", requestID)
		w.Header().Set("X-Correlation-ID", correlationID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// BodyLimit rejects requests larger than maxBytes and limits reads even without Content-Length.
func BodyLimit(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxBytes {
			problem.Write(w, problem.Detail{Status: http.StatusRequestEntityTooLarge, Code: "request_too_large", RequestID: RequestID(r.Context())})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		next.ServeHTTP(w, r)
	})
}

// Timeout bounds non-streaming handlers. Streaming handlers must apply their own deadline.
func Timeout(duration time.Duration, next http.Handler) http.Handler {
	return http.TimeoutHandler(next, duration, `{"type":"about:blank","title":"Gateway Timeout","status":504,"code":"request_timeout"}`)
}

// Recover converts panics to a stable response and logs only server-side diagnostic data.
func Recover(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("panic recovered", "operation", "http.request", "request_id", RequestID(r.Context()), "panic", recovered, "stack", string(debug.Stack()))
				problem.Write(w, problem.Detail{Status: http.StatusInternalServerError, Code: "internal_error", RequestID: RequestID(r.Context()), CorrelationID: CorrelationID(r.Context())})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// Chain applies middleware in declaration order.
func Chain(handler http.Handler, wrappers ...func(http.Handler) http.Handler) http.Handler {
	for index := len(wrappers) - 1; index >= 0; index-- {
		handler = wrappers[index](handler)
	}
	return handler
}

func newID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(value[:])
}

func sanitizeID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > 128 {
		return ""
	}
	for _, character := range value {
		if !(character == '-' || character == '_' || character == '.' || character >= '0' && character <= '9' || character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z') {
			return ""
		}
	}
	return value
}
