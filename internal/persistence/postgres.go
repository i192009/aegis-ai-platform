package persistence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/i192009/aegis-ai-platform/internal/auth"
	"github.com/i192009/aegis-ai-platform/internal/budget"
	"github.com/i192009/aegis-ai-platform/internal/events"
	"github.com/i192009/aegis-ai-platform/internal/request"
)

// Postgres is the production Store implementation using explicit SQL.
type Postgres struct{ pool *pgxpool.Pool }

// NewPostgres opens and verifies a bounded pool.
func NewPostgres(ctx context.Context, databaseURL string) (*Postgres, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	config.MaxConns = 30
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 10 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Pool exposes the pool to broker repositories that share the same transaction boundary.
func (postgres *Postgres) Pool() *pgxpool.Pool { return postgres.pool }

// Ping verifies that a database connection can be acquired.
func (postgres *Postgres) Ping(ctx context.Context) error { return postgres.pool.Ping(ctx) }

func (postgres *Postgres) Close() { postgres.pool.Close() }

func (postgres *Postgres) FindAPIKeyByPrefix(ctx context.Context, prefix string) (auth.StoredKey, error) {
	var key auth.StoredKey
	var userID *string
	err := postgres.pool.QueryRow(ctx, `
SELECT id::text, tenant_id::text, user_id::text, key_prefix, key_hash, scopes, expires_at, revoked_at
FROM api_keys WHERE key_prefix = $1`, prefix).Scan(&key.ID, &key.TenantID, &userID, &key.Prefix, &key.Hash, &key.Scopes, &key.ExpiresAt, &key.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return auth.StoredKey{}, ErrNotFound
	}
	if err != nil {
		return auth.StoredKey{}, fmt.Errorf("find API key: %w", err)
	}
	if userID != nil {
		key.UserID = *userID
	}
	return key, nil
}

func (postgres *Postgres) CreateOrGetRequest(ctx context.Context, params CreateRequestParams) (request.Record, bool, error) {
	ctx, span := otel.Tracer("aegis/postgres").Start(ctx, "postgres.create_or_get_request")
	defer span.End()
	tx, err := postgres.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return request.Record{}, false, fmt.Errorf("begin create request: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var modelID string
	err = tx.QueryRow(ctx, `
SELECT m.id::text
FROM models m
JOIN tenant_model_policies p ON p.model_id = m.id AND p.tenant_id = $1 AND p.enabled
WHERE m.public_name = $2 AND m.enabled`, params.TenantID, params.Input.Model).Scan(&modelID)
	if errors.Is(err, pgx.ErrNoRows) {
		return request.Record{}, false, ErrModelForbidden
	}
	if err != nil {
		return request.Record{}, false, fmt.Errorf("authorize model: %w", err)
	}

	metadata, err := json.Marshal(params.Input.Metadata)
	if err != nil {
		return request.Record{}, false, fmt.Errorf("marshal request metadata: %w", err)
	}
	var id string
	err = tx.QueryRow(ctx, `
INSERT INTO ai_requests (
    tenant_id, api_key_id, user_id, idempotency_key, canonical_request_hash,
    model_id, stream_requested, request_metadata, correlation_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
RETURNING id::text`, params.TenantID, params.APIKeyID, nilIfEmpty(params.UserID), params.IdempotencyKey, params.CanonicalHash, modelID, params.Input.Stream, metadata, params.CorrelationID).Scan(&id)
	created := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return request.Record{}, false, fmt.Errorf("insert logical request: %w", err)
	}
	if !created {
		err = tx.QueryRow(ctx, `SELECT id::text FROM ai_requests WHERE tenant_id = $1 AND idempotency_key = $2`, params.TenantID, params.IdempotencyKey).Scan(&id)
		if err != nil {
			return request.Record{}, false, fmt.Errorf("load conflicting logical request: %w", err)
		}
	}
	record, err := queryRequest(ctx, tx, params.TenantID, id)
	if err != nil {
		return request.Record{}, false, err
	}
	if !created && !bytes.Equal(record.CanonicalHash, params.CanonicalHash) {
		return request.Record{}, false, ErrConflict
	}
	if created {
		if err := insertOutbox(ctx, tx, events.RequestAccepted, record.ID, params.TenantID, params.CorrelationID, map[string]any{"request_id": record.ID, "model": params.Input.Model, "stream": params.Input.Stream}); err != nil {
			return request.Record{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return request.Record{}, false, fmt.Errorf("commit create request: %w", err)
	}
	return record, created, nil
}

func (postgres *Postgres) GetRequest(ctx context.Context, tenantID, requestID string) (request.Record, error) {
	return queryRequest(ctx, postgres.pool, tenantID, requestID)
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func queryRequest(ctx context.Context, query rowQuerier, tenantID, requestID string) (request.Record, error) {
	var record request.Record
	var responseBytes []byte
	err := query.QueryRow(ctx, `
SELECT r.id::text, r.tenant_id::text, r.api_key_id::text, r.idempotency_key,
       r.canonical_request_hash, m.public_name, r.status::text, r.stream_requested,
       r.partial_response_streamed, r.response_body, COALESCE(r.failure_category, ''),
       r.correlation_id, r.created_at, r.updated_at, r.completed_at,
       COALESCE(r.prompt_tokens, 0), COALESCE(r.completion_tokens, 0), COALESCE(r.actual_cost_micro_usd, 0)
FROM ai_requests r JOIN models m ON m.id = r.model_id
WHERE r.tenant_id = $1 AND r.id = $2`, tenantID, requestID).Scan(
		&record.ID, &record.TenantID, &record.APIKeyID, &record.IdempotencyKey,
		&record.CanonicalHash, &record.Model, &record.State, &record.Stream,
		&record.PartialStreamed, &responseBytes, &record.FailureCategory,
		&record.CorrelationID, &record.CreatedAt, &record.UpdatedAt, &record.CompletedAt,
		&record.PromptTokens, &record.CompletionTokens, &record.CostMicroUSD,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return request.Record{}, ErrNotFound
	}
	if err != nil {
		return request.Record{}, fmt.Errorf("get logical request: %w", err)
	}
	if len(responseBytes) > 0 {
		var response request.ChatResponse
		if err := json.Unmarshal(responseBytes, &response); err != nil {
			return request.Record{}, fmt.Errorf("decode stored response: %w", err)
		}
		record.Response = &response
	}
	return record, nil
}

func (postgres *Postgres) TransitionRequest(ctx context.Context, tenantID, requestID string, from, to request.State) error {
	if err := request.Transition(from, to); err != nil {
		return err
	}
	tag, err := postgres.pool.Exec(ctx, `UPDATE ai_requests SET status = $1, updated_at = now() WHERE tenant_id = $2 AND id = $3 AND status = $4`, to, tenantID, requestID, from)
	if err != nil {
		return fmt.Errorf("transition logical request: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrAlreadyFinal
	}
	return nil
}

func (postgres *Postgres) StartAttempt(ctx context.Context, params AttemptParams) error {
	tx, err := postgres.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin provider attempt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
INSERT INTO provider_attempts (request_id, tenant_id, attempt_number, provider_id, model_id)
SELECT $1, $2, $3, p.id, m.id FROM providers p CROSS JOIN models m
WHERE p.name = $4 AND m.public_name = $5`, params.RequestID, params.TenantID, params.Number, params.ProviderName, params.Model)
	if err != nil {
		return fmt.Errorf("start provider attempt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	if err := insertOutbox(ctx, tx, events.ProviderSelected, params.RequestID, params.TenantID, params.CorrelationID, map[string]any{"request_id": params.RequestID, "provider": params.ProviderName, "model": params.Model, "attempt_number": params.Number}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit provider attempt: %w", err)
	}
	return nil
}

func (postgres *Postgres) CompleteRequest(ctx context.Context, params CompletionParams) error {
	ctx, span := otel.Tracer("aegis/postgres").Start(ctx, "postgres.complete_request")
	defer span.End()
	responseBytes, err := json.Marshal(params.Response)
	if err != nil {
		return fmt.Errorf("marshal final response: %w", err)
	}
	tx, err := postgres.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin request completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	latency := time.Since(params.StartedAt).Milliseconds()
	_, err = tx.Exec(ctx, `
UPDATE provider_attempts SET status='SUCCEEDED', finished_at=now(), provider_request_id=$1,
    prompt_tokens=$2, completion_tokens=$3, cost_micro_usd=$4, latency_ms=$5, retryable=false
WHERE request_id=$6 AND tenant_id=$7 AND attempt_number=$8 AND status='STARTED'`,
		params.ProviderRequestID, params.Response.Usage.PromptTokens, params.Response.Usage.CompletionTokens, params.CostMicroUSD, latency, params.RequestID, params.TenantID, params.AttemptNumber)
	if err != nil {
		return fmt.Errorf("complete provider attempt: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE ai_requests SET status='COMPLETED', response_body=$1, prompt_tokens=$2,
    completion_tokens=$3, actual_cost_micro_usd=$4, completed_at=now(), updated_at=now()
WHERE id=$5 AND tenant_id=$6 AND status='IN_PROGRESS'`, responseBytes, params.Response.Usage.PromptTokens, params.Response.Usage.CompletionTokens, params.CostMicroUSD, params.RequestID, params.TenantID)
	if err != nil {
		return fmt.Errorf("finalize logical request: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrAlreadyFinal
	}
	payload := map[string]any{"request_id": params.RequestID, "model": params.Response.Model, "prompt_tokens": params.Response.Usage.PromptTokens, "completion_tokens": params.Response.Usage.CompletionTokens, "cost_micro_usd": params.CostMicroUSD, "latency_ms": latency}
	if err := insertOutbox(ctx, tx, events.RequestCompleted, params.RequestID, params.TenantID, params.CorrelationID, payload); err != nil {
		return err
	}
	if err := insertOutbox(ctx, tx, events.UsageRecorded, params.RequestID, params.TenantID, params.CorrelationID, payload); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit request completion: %w", err)
	}
	return nil
}

func (postgres *Postgres) FailAttempt(ctx context.Context, params FailureParams) error {
	tx, err := postgres.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin attempt failure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if params.AttemptNumber > 0 {
		_, err = tx.Exec(ctx, `
UPDATE provider_attempts SET status='FAILED', finished_at=now(), error_category=$1,
    retryable=$2, latency_ms=$3 WHERE request_id=$4 AND tenant_id=$5 AND attempt_number=$6 AND status='STARTED'`,
			params.Category, params.Retryable, time.Since(params.StartedAt).Milliseconds(), params.RequestID, params.TenantID, params.AttemptNumber)
		if err != nil {
			return fmt.Errorf("fail provider attempt: %w", err)
		}
	}
	if params.Final {
		tag, err := tx.Exec(ctx, `
UPDATE ai_requests SET status='FAILED', failure_category=$1, partial_response_streamed=$2,
    completed_at=now(), updated_at=now() WHERE id=$3 AND tenant_id=$4 AND status='IN_PROGRESS'`, params.Category, params.Partial, params.RequestID, params.TenantID)
		if err != nil {
			return fmt.Errorf("fail logical request: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return ErrAlreadyFinal
		}
		if err := insertOutbox(ctx, tx, events.RequestFailed, params.RequestID, params.TenantID, params.CorrelationID, map[string]any{"request_id": params.RequestID, "error_category": params.Category, "partial_streamed": params.Partial}); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit attempt failure: %w", err)
	}
	return nil
}

func (postgres *Postgres) CancelRequest(ctx context.Context, tenantID, requestID, correlationID string) error {
	tag, err := postgres.pool.Exec(ctx, `
UPDATE ai_requests SET status='CANCELLED', completed_at=now(), updated_at=now()
WHERE tenant_id=$1 AND id=$2 AND status IN ('RECEIVED','VALIDATED','ROUTING','IN_PROGRESS')`, tenantID, requestID)
	if err != nil {
		return fmt.Errorf("cancel request: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrAlreadyFinal
	}
	_ = correlationID
	return nil
}

func (postgres *Postgres) TenantPolicy(ctx context.Context, tenantID, model string) (TenantPolicy, error) {
	var policy TenantPolicy
	err := postgres.pool.QueryRow(ctx, `
SELECT t.data_classification, l.requests_per_minute, l.tokens_per_minute,
       l.max_concurrent_requests, l.monthly_budget_micro_usd, COALESCE(l.daily_budget_micro_usd, 0)
FROM tenants t JOIN tenant_limits l ON l.tenant_id=t.id
JOIN tenant_model_policies mp ON mp.tenant_id=t.id AND mp.enabled
JOIN models m ON m.id=mp.model_id AND m.enabled
WHERE t.id=$1 AND t.status='ACTIVE' AND m.public_name=$2`, tenantID, model).Scan(
		&policy.DataClassification, &policy.RequestsPerMinute, &policy.TokensPerMinute, &policy.MaxConcurrent, &policy.MonthlyBudget, &policy.DailyBudget)
	if errors.Is(err, pgx.ErrNoRows) {
		return TenantPolicy{}, ErrModelForbidden
	}
	if err != nil {
		return TenantPolicy{}, fmt.Errorf("load tenant policy: %w", err)
	}
	rows, err := postgres.pool.Query(ctx, `
SELECT p.name FROM tenant_provider_policies tp JOIN providers p ON p.id=tp.provider_id
WHERE tp.tenant_id=$1 AND tp.enabled AND p.enabled`, tenantID)
	if err != nil {
		return TenantPolicy{}, fmt.Errorf("load tenant provider policy: %w", err)
	}
	defer rows.Close()
	policy.AllowedProviders = make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return TenantPolicy{}, fmt.Errorf("scan tenant provider policy: %w", err)
		}
		policy.AllowedProviders[name] = struct{}{}
	}
	return policy, rows.Err()
}

func (postgres *Postgres) ReserveBudget(ctx context.Context, tenantID, requestID string, amount int64, now time.Time) error {
	ctx, span := otel.Tracer("aegis/postgres").Start(ctx, "postgres.reserve_budget")
	defer span.End()
	tx, err := postgres.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin budget reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var monthlyLimit, dailyLimit int64
	err = tx.QueryRow(ctx, `SELECT monthly_budget_micro_usd, COALESCE(daily_budget_micro_usd,0) FROM tenant_limits WHERE tenant_id=$1 FOR UPDATE`, tenantID).Scan(&monthlyLimit, &dailyLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock tenant budget: %w", err)
	}
	month := time.Date(now.UTC().Year(), now.UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	day := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
	var monthUsed, monthReserved, dayUsed, dayReserved int64
	err = tx.QueryRow(ctx, `
SELECT
 COALESCE((SELECT SUM(cost_micro_usd) FROM usage_ledger WHERE tenant_id=$1 AND occurred_at >= $2),0),
 COALESCE((SELECT SUM(reserved_micro_usd) FROM budget_reservations WHERE tenant_id=$1 AND period_month=$2 AND state='RESERVED'),0),
 COALESCE((SELECT SUM(cost_micro_usd) FROM usage_ledger WHERE tenant_id=$1 AND occurred_at >= $3),0),
 COALESCE((SELECT SUM(reserved_micro_usd) FROM budget_reservations WHERE tenant_id=$1 AND period_day=$3 AND state='RESERVED'),0)`, tenantID, month, day).Scan(&monthUsed, &monthReserved, &dayUsed, &dayReserved)
	if err != nil {
		return fmt.Errorf("calculate available budget: %w", err)
	}
	if err := budget.CanReserve(monthlyLimit, monthUsed, monthReserved, amount); err != nil {
		return err
	}
	if err := budget.CanReserve(dailyLimit, dayUsed, dayReserved, amount); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
INSERT INTO budget_reservations (tenant_id, request_id, period_month, period_day, reserved_micro_usd, state)
VALUES ($1,$2,$3,$4,$5,'RESERVED') ON CONFLICT (request_id) DO NOTHING`, tenantID, requestID, month, day, amount)
	if err != nil {
		return fmt.Errorf("insert budget reservation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit budget reservation: %w", err)
	}
	return nil
}

func (postgres *Postgres) RejectBudget(ctx context.Context, tenantID, requestID, correlationID string, requested int64) error {
	tx, err := postgres.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin budget rejection: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE ai_requests SET status='BUDGET_REJECTED',failure_category='budget_exceeded',completed_at=now(),updated_at=now() WHERE tenant_id=$1 AND id=$2 AND status='VALIDATED'`, tenantID, requestID)
	if err != nil {
		return fmt.Errorf("reject request budget: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrAlreadyFinal
	}
	if err := insertOutbox(ctx, tx, events.BudgetExceeded, requestID, tenantID, correlationID, map[string]any{"request_id": requestID, "requested_micro_usd": requested}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (postgres *Postgres) ReconcileBudget(ctx context.Context, requestID string, actual int64) error {
	tag, err := postgres.pool.Exec(ctx, `
UPDATE budget_reservations SET actual_micro_usd=$1,
 released_micro_usd=GREATEST(reserved_micro_usd-$1,0), state='RECONCILED', updated_at=now()
WHERE request_id=$2 AND state='RESERVED'`, actual, requestID)
	if err != nil {
		return fmt.Errorf("reconcile budget: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, eventType, aggregateID, tenantID, correlationID string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO outbox_events (event_type,event_version,aggregate_id,tenant_id,correlation_id,payload,occurred_at)
VALUES ($1,1,$2,$3,$4,$5,now())`, eventType, aggregateID, tenantID, correlationID, encoded)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func nilIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
