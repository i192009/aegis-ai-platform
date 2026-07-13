BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE tenant_status AS ENUM ('ACTIVE', 'SUSPENDED', 'CLOSED');
CREATE TYPE request_status AS ENUM (
    'RECEIVED', 'VALIDATED', 'ROUTING', 'IN_PROGRESS',
    'COMPLETED', 'FAILED', 'CANCELLED', 'BUDGET_REJECTED'
);
CREATE TYPE attempt_status AS ENUM ('STARTED', 'SUCCEEDED', 'FAILED', 'CANCELLED');
CREATE TYPE evaluation_status AS ENUM ('PENDING', 'QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'DEAD_LETTERED');

CREATE TABLE tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    status tenant_status NOT NULL DEFAULT 'ACTIVE',
    data_classification TEXT NOT NULL DEFAULT 'INTERNAL',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenants_slug_format CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$')
);

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    external_subject TEXT NOT NULL,
    email TEXT,
    roles TEXT[] NOT NULL DEFAULT '{}',
    disabled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, external_subject)
);

CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    user_id UUID REFERENCES users(id),
    name TEXT NOT NULL,
    key_prefix TEXT NOT NULL,
    key_hash BYTEA NOT NULL,
    scopes TEXT[] NOT NULL DEFAULT '{}',
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name),
    UNIQUE (key_prefix)
);

CREATE INDEX api_keys_tenant_active_idx ON api_keys (tenant_id, key_prefix)
    WHERE revoked_at IS NULL;

CREATE TABLE models (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    public_name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE providers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    endpoint TEXT NOT NULL,
    api_key_secret_ref TEXT,
    weight INTEGER NOT NULL DEFAULT 1 CHECK (weight > 0),
    priority INTEGER NOT NULL DEFAULT 100 CHECK (priority >= 0),
    max_concurrency INTEGER NOT NULL DEFAULT 20 CHECK (max_concurrency > 0),
    timeout_ms INTEGER NOT NULL DEFAULT 30000 CHECK (timeout_ms > 0),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    data_classifications TEXT[] NOT NULL DEFAULT ARRAY['PUBLIC', 'INTERNAL'],
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE provider_models (
    provider_id UUID NOT NULL REFERENCES providers(id),
    model_id UUID NOT NULL REFERENCES models(id),
    provider_model_name TEXT NOT NULL,
    input_cost_micro_usd_per_million_tokens BIGINT NOT NULL DEFAULT 0 CHECK (input_cost_micro_usd_per_million_tokens >= 0),
    output_cost_micro_usd_per_million_tokens BIGINT NOT NULL DEFAULT 0 CHECK (output_cost_micro_usd_per_million_tokens >= 0),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (provider_id, model_id)
);

COMMENT ON COLUMN provider_models.input_cost_micro_usd_per_million_tokens IS
    'Integer millionths of one US dollar per one million tokens; no floating point is used.';

CREATE TABLE tenant_model_policies (
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    model_id UUID NOT NULL REFERENCES models(id),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    max_tokens_per_request INTEGER CHECK (max_tokens_per_request > 0),
    PRIMARY KEY (tenant_id, model_id)
);

CREATE TABLE tenant_provider_policies (
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    provider_id UUID NOT NULL REFERENCES providers(id),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority_override INTEGER CHECK (priority_override >= 0),
    PRIMARY KEY (tenant_id, provider_id)
);

CREATE TABLE tenant_limits (
    tenant_id UUID PRIMARY KEY REFERENCES tenants(id),
    requests_per_minute INTEGER NOT NULL DEFAULT 60 CHECK (requests_per_minute > 0),
    tokens_per_minute BIGINT NOT NULL DEFAULT 100000 CHECK (tokens_per_minute > 0),
    max_concurrent_requests INTEGER NOT NULL DEFAULT 10 CHECK (max_concurrent_requests > 0),
    monthly_budget_micro_usd BIGINT NOT NULL DEFAULT 0 CHECK (monthly_budget_micro_usd >= 0),
    daily_budget_micro_usd BIGINT CHECK (daily_budget_micro_usd >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ai_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    api_key_id UUID NOT NULL REFERENCES api_keys(id),
    user_id UUID REFERENCES users(id),
    idempotency_key TEXT NOT NULL,
    canonical_request_hash BYTEA NOT NULL,
    model_id UUID NOT NULL REFERENCES models(id),
    status request_status NOT NULL DEFAULT 'RECEIVED',
    stream_requested BOOLEAN NOT NULL DEFAULT FALSE,
    partial_response_streamed BOOLEAN NOT NULL DEFAULT FALSE,
    request_metadata JSONB NOT NULL DEFAULT '{}',
    response_body JSONB,
    prompt_tokens BIGINT CHECK (prompt_tokens >= 0),
    completion_tokens BIGINT CHECK (completion_tokens >= 0),
    actual_cost_micro_usd BIGINT CHECK (actual_cost_micro_usd >= 0),
    failure_category TEXT,
    correlation_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (tenant_id, id)
);

CREATE INDEX ai_requests_tenant_status_created_idx ON ai_requests (tenant_id, status, created_at DESC);
CREATE INDEX ai_requests_status_updated_idx ON ai_requests (status, updated_at);

CREATE TABLE provider_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id UUID NOT NULL REFERENCES ai_requests(id),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    provider_id UUID NOT NULL REFERENCES providers(id),
    model_id UUID NOT NULL REFERENCES models(id),
    status attempt_status NOT NULL DEFAULT 'STARTED',
    provider_request_id TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    prompt_tokens BIGINT CHECK (prompt_tokens >= 0),
    completion_tokens BIGINT CHECK (completion_tokens >= 0),
    cost_micro_usd BIGINT CHECK (cost_micro_usd >= 0),
    error_category TEXT,
    retryable BOOLEAN,
    latency_ms BIGINT CHECK (latency_ms >= 0),
    UNIQUE (request_id, attempt_number)
);

CREATE INDEX provider_attempts_request_idx ON provider_attempts (request_id, attempt_number);
CREATE INDEX provider_attempts_provider_started_idx ON provider_attempts (provider_id, started_at DESC);

CREATE TABLE budget_reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    request_id UUID NOT NULL UNIQUE REFERENCES ai_requests(id),
    period_month DATE NOT NULL,
    period_day DATE NOT NULL,
    reserved_micro_usd BIGINT NOT NULL CHECK (reserved_micro_usd >= 0),
    actual_micro_usd BIGINT CHECK (actual_micro_usd >= 0),
    released_micro_usd BIGINT CHECK (released_micro_usd >= 0),
    state TEXT NOT NULL CHECK (state IN ('RESERVED', 'RECONCILED', 'RELEASED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX budget_reservations_tenant_month_idx ON budget_reservations (tenant_id, period_month, state);
CREATE INDEX budget_reservations_tenant_day_idx ON budget_reservations (tenant_id, period_day, state);

CREATE TABLE usage_ledger (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    request_id UUID NOT NULL REFERENCES ai_requests(id),
    provider_attempt_id UUID REFERENCES provider_attempts(id),
    event_id UUID NOT NULL,
    prompt_tokens BIGINT NOT NULL CHECK (prompt_tokens >= 0),
    completion_tokens BIGINT NOT NULL CHECK (completion_tokens >= 0),
    cost_micro_usd BIGINT NOT NULL CHECK (cost_micro_usd >= 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, event_id)
);

CREATE INDEX usage_ledger_tenant_occurred_idx ON usage_ledger (tenant_id, occurred_at DESC);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    aggregate_id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ,
    claimed_by TEXT,
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    published_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE INDEX outbox_unpublished_idx ON outbox_events (available_at, created_at)
    WHERE published_at IS NULL;
CREATE INDEX outbox_tenant_replay_idx ON outbox_events (tenant_id, occurred_at, id);

CREATE TABLE processed_kafka_events (
    consumer_name TEXT NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE audit_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL,
    aggregate_id UUID NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX audit_events_tenant_occurred_idx ON audit_events (tenant_id, occurred_at DESC, id);
CREATE INDEX audit_events_aggregate_idx ON audit_events (aggregate_id, occurred_at, id);

CREATE TABLE evaluation_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    request_id UUID NOT NULL REFERENCES ai_requests(id),
    idempotency_key TEXT NOT NULL,
    canonical_request_hash BYTEA NOT NULL,
    job_type TEXT NOT NULL,
    status evaluation_status NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    correlation_id TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error_category TEXT,
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (tenant_id, id)
);

CREATE INDEX evaluation_jobs_tenant_status_idx ON evaluation_jobs (tenant_id, status, requested_at DESC);
CREATE INDEX evaluation_jobs_queue_idx ON evaluation_jobs (status, requested_at) WHERE status IN ('PENDING', 'QUEUED');

CREATE TABLE evaluation_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    evaluation_job_id UUID NOT NULL UNIQUE REFERENCES evaluation_jobs(id),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    execution_id UUID NOT NULL UNIQUE,
    evaluator_version TEXT NOT NULL,
    score_milli INTEGER NOT NULL CHECK (score_milli BETWEEN 0 AND 1000),
    outcome TEXT NOT NULL,
    findings JSONB NOT NULL DEFAULT '[]',
    duration_ms BIGINT NOT NULL CHECK (duration_ms >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX evaluation_results_tenant_created_idx ON evaluation_results (tenant_id, created_at DESC);

COMMIT;
