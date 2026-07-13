BEGIN;

DROP TABLE IF EXISTS evaluation_results;
DROP TABLE IF EXISTS evaluation_jobs;
DROP TABLE IF EXISTS audit_events;
DROP TABLE IF EXISTS processed_kafka_events;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS usage_ledger;
DROP TABLE IF EXISTS budget_reservations;
DROP TABLE IF EXISTS provider_attempts;
DROP TABLE IF EXISTS ai_requests;
DROP TABLE IF EXISTS tenant_limits;
DROP TABLE IF EXISTS tenant_provider_policies;
DROP TABLE IF EXISTS tenant_model_policies;
DROP TABLE IF EXISTS provider_models;
DROP TABLE IF EXISTS providers;
DROP TABLE IF EXISTS models;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS tenants;

DROP TYPE IF EXISTS evaluation_status;
DROP TYPE IF EXISTS attempt_status;
DROP TYPE IF EXISTS request_status;
DROP TYPE IF EXISTS tenant_status;

COMMIT;
