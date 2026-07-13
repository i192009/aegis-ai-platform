package persistence

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/i192009/aegis-ai-platform/internal/events"
)

// ProcessKafkaEvent applies an event and its deduplication marker in one transaction.
// It returns false when this consumer has already committed the same event ID.
func (postgres *Postgres) ProcessKafkaEvent(ctx context.Context, consumerName string, event events.Envelope) (bool, error) {
	if err := event.Validate(); err != nil {
		return false, err
	}
	tx, err := postgres.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin Kafka event processing: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
INSERT INTO processed_kafka_events (consumer_name,event_id,event_type,event_version)
VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, consumerName, event.EventID, event.EventType, event.Version)
	if err != nil {
		return false, fmt.Errorf("insert processed event marker: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate event: %w", err)
		}
		return false, nil
	}
	_, err = tx.Exec(ctx, `
INSERT INTO audit_events (id,tenant_id,event_type,event_version,aggregate_id,correlation_id,causation_id,payload,occurred_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO NOTHING`, event.EventID, event.TenantID, event.EventType, event.Version, event.AggregateID, event.CorrelationID, nilIfEmpty(event.CausationID), event.Payload, event.Timestamp)
	if err != nil {
		return false, fmt.Errorf("insert immutable audit event: %w", err)
	}
	if event.EventType == events.UsageRecorded {
		var usage struct {
			RequestID        string `json:"request_id"`
			PromptTokens     int64  `json:"prompt_tokens"`
			CompletionTokens int64  `json:"completion_tokens"`
			CostMicroUSD     int64  `json:"cost_micro_usd"`
		}
		if err := json.Unmarshal(event.Payload, &usage); err != nil {
			return false, fmt.Errorf("decode usage event: %w", err)
		}
		_, err = tx.Exec(ctx, `
INSERT INTO usage_ledger (tenant_id,request_id,event_id,prompt_tokens,completion_tokens,cost_micro_usd,occurred_at)
VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (tenant_id,event_id) DO NOTHING`, event.TenantID, usage.RequestID, event.EventID, usage.PromptTokens, usage.CompletionTokens, usage.CostMicroUSD, event.Timestamp)
		if err != nil {
			return false, fmt.Errorf("insert usage ledger entry: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit Kafka event processing: %w", err)
	}
	return true, nil
}
