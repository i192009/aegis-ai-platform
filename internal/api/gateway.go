// Package api implements versioned HTTP transports for AegisAI services.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/auth"
	"github.com/i192009/aegis-ai-platform/internal/budget"
	"github.com/i192009/aegis-ai-platform/internal/gateway"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/provider"
	"github.com/i192009/aegis-ai-platform/internal/request"
	"github.com/i192009/aegis-ai-platform/internal/routing"
	"github.com/i192009/aegis-ai-platform/pkg/middleware"
	"github.com/i192009/aegis-ai-platform/pkg/problem"
)

// Gateway contains thin HTTP handlers and API-key middleware.
type Gateway struct {
	service *gateway.Service
	keys    auth.Lookup
	pepper  []byte
	now     func() time.Time
}

// NewGateway creates the versioned gateway transport.
func NewGateway(service *gateway.Service, keys auth.Lookup, pepper []byte) *Gateway {
	return &Gateway{service: service, keys: keys, pepper: pepper, now: time.Now}
}

// Register attaches authenticated routes to mux.
func (gatewayAPI *Gateway) Register(mux *http.ServeMux) {
	mux.Handle("POST /v1/chat/completions", gatewayAPI.authenticate("chat:write", http.HandlerFunc(gatewayAPI.chat)))
	mux.Handle("GET /v1/requests/{request_id}", gatewayAPI.authenticate("requests:read", http.HandlerFunc(gatewayAPI.getRequest)))
	mux.Handle("POST /v1/requests/{request_id}/cancel", gatewayAPI.authenticate("requests:cancel", http.HandlerFunc(gatewayAPI.cancelRequest)))
}

func (gatewayAPI *Gateway) authenticate(scope string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		plain := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if plain == "" {
			authorization := strings.TrimSpace(r.Header.Get("Authorization"))
			if strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
				plain = strings.TrimSpace(authorization[7:])
			}
		}
		principal, err := auth.Verify(r.Context(), gatewayAPI.keys, gatewayAPI.pepper, plain, gatewayAPI.now())
		if err != nil {
			writeProblem(w, r, http.StatusUnauthorized, "authentication_failed", "Valid API-key authentication is required")
			return
		}
		if err := auth.RequireScope(principal, scope); err != nil {
			writeProblem(w, r, http.StatusForbidden, "scope_denied", "The API key is not authorised for this operation")
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), principal)))
	})
}

func (gatewayAPI *Gateway) chat(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) < 8 || len(idempotencyKey) > 128 {
		writeProblem(w, r, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key must contain between 8 and 128 characters")
		return
	}
	var input request.ChatInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", "The request body is not a valid supported chat-completions request")
		return
	}
	if err := input.Validate(); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	principal, _ := auth.FromContext(r.Context())
	correlationID := middleware.CorrelationID(r.Context())
	if !input.Stream {
		outcome, err := gatewayAPI.service.Submit(r.Context(), principal, idempotencyKey, correlationID, input)
		if err != nil {
			gatewayAPI.writeServiceError(w, r, err)
			return
		}
		gatewayAPI.writeOutcome(w, outcome)
		return
	}

	stream := &sseWriter{writer: w, requestID: middleware.RequestID(r.Context()), model: input.Model}
	outcome, err := gatewayAPI.service.Stream(r.Context(), principal, idempotencyKey, correlationID, input, stream.Emit)
	if err != nil {
		if stream.started {
			_ = stream.Error(errorCode(err))
			return
		}
		gatewayAPI.writeServiceError(w, r, err)
		return
	}
	if outcome.Pending {
		gatewayAPI.writeOutcome(w, outcome)
		return
	}
	if !stream.started {
		stream.start(outcome.Record.ID, input.Model)
	}
	_ = stream.Done()
}

func (gatewayAPI *Gateway) getRequest(w http.ResponseWriter, r *http.Request) {
	principal, _ := auth.FromContext(r.Context())
	record, err := gatewayAPI.service.Get(r.Context(), principal.TenantID, r.PathValue("request_id"))
	if err != nil {
		gatewayAPI.writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, requestView(record))
}

func (gatewayAPI *Gateway) cancelRequest(w http.ResponseWriter, r *http.Request) {
	principal, _ := auth.FromContext(r.Context())
	if err := gatewayAPI.service.Cancel(r.Context(), principal.TenantID, r.PathValue("request_id"), middleware.CorrelationID(r.Context())); err != nil {
		gatewayAPI.writeServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (gatewayAPI *Gateway) writeOutcome(w http.ResponseWriter, outcome gateway.Outcome) {
	if outcome.Response != nil {
		if outcome.Replayed {
			w.Header().Set("Idempotency-Replayed", "true")
		}
		writeJSON(w, http.StatusOK, outcome.Response)
		return
	}
	writeJSON(w, http.StatusAccepted, requestView(outcome.Record))
}

func (gatewayAPI *Gateway) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, persistence.ErrConflict):
		writeProblem(w, r, http.StatusConflict, "idempotency_conflict", "The idempotency key was already used with different input")
	case errors.Is(err, persistence.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, "not_found", "The requested resource was not found")
	case errors.Is(err, persistence.ErrModelForbidden):
		writeProblem(w, r, http.StatusForbidden, "model_forbidden", "The requested model is not enabled for this tenant")
	case errors.Is(err, persistence.ErrAlreadyFinal):
		writeProblem(w, r, http.StatusConflict, "invalid_request_state", "The logical request is already final or changed concurrently")
	case errors.Is(err, budget.ErrExceeded):
		writeProblem(w, r, http.StatusPaymentRequired, "budget_exceeded", "The tenant budget cannot reserve this request")
	case strings.Contains(err.Error(), "rate limit rejected"):
		w.Header().Set("Retry-After", "60")
		writeProblem(w, r, http.StatusTooManyRequests, "rate_limit_exceeded", "The distributed tenant or API-key limit was exceeded")
	case errors.Is(err, routing.ErrNoEligibleProvider):
		writeProblem(w, r, http.StatusServiceUnavailable, "provider_unavailable", "No eligible AI provider is currently available")
	default:
		writeProblem(w, r, http.StatusBadGateway, errorCode(err), "The completion could not be produced")
	}
}

func errorCode(err error) string {
	var providerErr *provider.Error
	if errors.As(err, &providerErr) {
		return providerErr.Category
	}
	return "provider_unavailable"
}

func requestView(record request.Record) map[string]any {
	return map[string]any{
		"id": record.ID, "model": record.Model, "status": record.State, "stream": record.Stream,
		"partial_response_streamed": record.PartialStreamed, "failure_category": record.FailureCategory,
		"created_at": record.CreatedAt, "updated_at": record.UpdatedAt, "completed_at": record.CompletedAt,
		"usage":          request.Usage{PromptTokens: record.PromptTokens, CompletionTokens: record.CompletionTokens, TotalTokens: record.PromptTokens + record.CompletionTokens},
		"cost_micro_usd": record.CostMicroUSD,
	}
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, detail string) {
	problem.Write(w, problem.Detail{Type: "https://aegis.example/problems/" + code, Status: status, Code: code, Detail: detail, Instance: r.URL.Path, RequestID: middleware.RequestID(r.Context()), CorrelationID: middleware.CorrelationID(r.Context())})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type sseWriter struct {
	writer    http.ResponseWriter
	flusher   http.Flusher
	started   bool
	requestID string
	model     string
}

func (stream *sseWriter) start(requestID, model string) {
	if stream.started {
		return
	}
	stream.started, stream.requestID, stream.model = true, requestID, model
	stream.writer.Header().Set("Content-Type", "text/event-stream")
	stream.writer.Header().Set("Cache-Control", "no-cache, no-transform")
	stream.writer.Header().Set("X-Accel-Buffering", "no")
	stream.writer.WriteHeader(http.StatusOK)
	stream.flusher, _ = stream.writer.(http.Flusher)
}

func (stream *sseWriter) Emit(chunk provider.Chunk) error {
	stream.start(stream.requestID, stream.model)
	event := map[string]any{
		"id": "chatcmpl-" + stream.requestID, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": stream.model,
		"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": chunk.Content}, "finish_reason": nilIfBlank(chunk.FinishReason)}},
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(stream.writer, "data: %s\n\n", encoded); err != nil {
		return err
	}
	if stream.flusher != nil {
		stream.flusher.Flush()
	}
	return nil
}

func (stream *sseWriter) Error(code string) error {
	encoded, _ := json.Marshal(map[string]string{"code": code, "message": "stream terminated before completion"})
	_, err := fmt.Fprintf(stream.writer, "event: error\ndata: %s\n\n", encoded)
	if stream.flusher != nil {
		stream.flusher.Flush()
	}
	return err
}

func (stream *sseWriter) Done() error {
	_, err := fmt.Fprint(stream.writer, "data: [DONE]\n\n")
	if stream.flusher != nil {
		stream.flusher.Flush()
	}
	return err
}

func nilIfBlank(value string) any {
	if value == "" {
		return nil
	}
	return value
}
