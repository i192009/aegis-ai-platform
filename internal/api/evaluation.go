package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/auth"
	"github.com/i192009/aegis-ai-platform/internal/evaluationjob"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/pkg/middleware"
)

// Evaluation exposes tenant-scoped evaluation job APIs.
type Evaluation struct {
	service *evaluationjob.Service
	keys    auth.Lookup
	pepper  []byte
}

func NewEvaluation(service *evaluationjob.Service, keys auth.Lookup, pepper []byte) *Evaluation {
	return &Evaluation{service: service, keys: keys, pepper: pepper}
}

func (api *Evaluation) Register(mux *http.ServeMux) {
	mux.Handle("POST /v1/evaluations", api.authenticate("evaluations:write", http.HandlerFunc(api.submit)))
	mux.Handle("GET /v1/evaluations/{evaluation_id}", api.authenticate("evaluations:read", http.HandlerFunc(api.get)))
	mux.Handle("GET /v1/evaluations", api.authenticate("evaluations:read", http.HandlerFunc(api.list)))
	mux.Handle("POST /v1/evaluations/{evaluation_id}/retry", api.authenticate("evaluations:write", http.HandlerFunc(api.retry)))
}

func (api *Evaluation) authenticate(scope string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		plain := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if plain == "" && strings.HasPrefix(strings.ToLower(r.Header.Get("Authorization")), "bearer ") {
			plain = strings.TrimSpace(r.Header.Get("Authorization")[7:])
		}
		principal, err := auth.Verify(r.Context(), api.keys, api.pepper, plain, time.Now())
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

func (api *Evaluation) submit(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) < 8 || len(idempotencyKey) > 128 {
		writeProblem(w, r, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key must contain between 8 and 128 characters")
		return
	}
	var input evaluationjob.SubmitInput
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", "The evaluation request is invalid")
		return
	}
	principal, _ := auth.FromContext(r.Context())
	job, created, err := api.service.Submit(r.Context(), principal.TenantID, idempotencyKey, middleware.CorrelationID(r.Context()), input)
	if err != nil {
		api.error(w, r, err)
		return
	}
	if !created {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (api *Evaluation) get(w http.ResponseWriter, r *http.Request) {
	principal, _ := auth.FromContext(r.Context())
	job, err := api.service.Get(r.Context(), principal.TenantID, r.PathValue("evaluation_id"))
	if err != nil {
		api.error(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (api *Evaluation) list(w http.ResponseWriter, r *http.Request) {
	principal, _ := auth.FromContext(r.Context())
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	jobs, err := api.service.List(r.Context(), principal.TenantID, limit, offset)
	if err != nil {
		api.error(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": jobs, "limit": limit, "offset": offset})
}

func (api *Evaluation) retry(w http.ResponseWriter, r *http.Request) {
	principal, _ := auth.FromContext(r.Context())
	job, err := api.service.Retry(r.Context(), principal.TenantID, r.PathValue("evaluation_id"), middleware.CorrelationID(r.Context()))
	if err != nil {
		api.error(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (api *Evaluation) error(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, "not_found", "The evaluation or source request was not found")
	case errors.Is(err, persistence.ErrConflict):
		writeProblem(w, r, http.StatusConflict, "idempotency_conflict", "The idempotency key was used with different evaluation input")
	case errors.Is(err, evaluationjob.ErrNotRetryable):
		writeProblem(w, r, http.StatusConflict, "evaluation_not_retryable", "The evaluation is not in a retryable state")
	default:
		writeProblem(w, r, http.StatusServiceUnavailable, "evaluation_unavailable", "The evaluation request could not be processed")
	}
}
