// Package outbox safely claims and publishes transactional outbox rows.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/i192009/aegis-ai-platform/internal/events"
)

// Row is a claimed unpublished outbox fact.
type Row struct {
	ID            string
	EventType     string
	EventVersion  int
	AggregateID   string
	TenantID      string
	CorrelationID string
	CausationID   string
	Payload       json.RawMessage
	OccurredAt    time.Time
}

// Repository uses SKIP LOCKED and expiring claims so multiple relays are safe.
type Repository struct{ pool *pgxpool.Pool }

func NewRepository(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

// Claim assigns a bounded batch to one relay. Abandoned claims become eligible again.
func (repository *Repository) Claim(ctx context.Context, workerID string, limit int, claimTTL time.Duration) ([]Row, error) {
	rows, err := repository.pool.Query(ctx, `
WITH candidates AS (
    SELECT id FROM outbox_events
    WHERE published_at IS NULL AND available_at <= now()
      AND (claimed_at IS NULL OR claimed_at < now() - ($3 * interval '1 second'))
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT $2
)
UPDATE outbox_events o SET claimed_at=now(), claimed_by=$1, publish_attempts=publish_attempts+1
FROM candidates c WHERE o.id=c.id
RETURNING o.id::text,o.event_type,o.event_version,o.aggregate_id::text,o.tenant_id::text,
          o.correlation_id,COALESCE(o.causation_id,''),o.payload,o.occurred_at`, workerID, limit, int64(claimTTL.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("claim outbox rows: %w", err)
	}
	defer rows.Close()
	claimed := make([]Row, 0, limit)
	for rows.Next() {
		var row Row
		if err := rows.Scan(&row.ID, &row.EventType, &row.EventVersion, &row.AggregateID, &row.TenantID, &row.CorrelationID, &row.CausationID, &row.Payload, &row.OccurredAt); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		claimed = append(claimed, row)
	}
	return claimed, rows.Err()
}

func (repository *Repository) MarkPublished(ctx context.Context, eventID, workerID string) error {
	_, err := repository.pool.Exec(ctx, `UPDATE outbox_events SET published_at=now(),claimed_at=NULL,claimed_by=NULL,last_error=NULL WHERE id=$1 AND claimed_by=$2 AND published_at IS NULL`, eventID, workerID)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	return nil
}

func (repository *Repository) MarkFailed(ctx context.Context, eventID, workerID, category string, delay time.Duration) error {
	_, err := repository.pool.Exec(ctx, `UPDATE outbox_events SET claimed_at=NULL,claimed_by=NULL,last_error=$3,available_at=now()+($4 * interval '1 second') WHERE id=$1 AND claimed_by=$2 AND published_at IS NULL`, eventID, workerID, category, int64(delay.Seconds()))
	if err != nil {
		return fmt.Errorf("release failed outbox event: %w", err)
	}
	return nil
}

// Publisher publishes one retained fact and waits for broker acknowledgement.
type Publisher interface {
	Publish(context.Context, events.Envelope) error
}

// Relay polls, publishes, and records acknowledgement without claiming exactly-once behavior.
type Relay struct {
	repository *Repository
	publisher  Publisher
	workerID   string
	batchSize  int
	poll       time.Duration
}

func NewRelay(repository *Repository, publisher Publisher, workerID string, batchSize int, poll time.Duration) *Relay {
	if batchSize <= 0 {
		batchSize = 100
	}
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	return &Relay{repository: repository, publisher: publisher, workerID: workerID, batchSize: batchSize, poll: poll}
}

func (relay *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(relay.poll)
	defer ticker.Stop()
	for {
		if err := relay.runBatch(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (relay *Relay) runBatch(ctx context.Context) error {
	rows, err := relay.repository.Claim(ctx, relay.workerID, relay.batchSize, time.Minute)
	if err != nil {
		return err
	}
	for _, row := range rows {
		event := events.Envelope{EventID: row.ID, EventType: row.EventType, Version: row.EventVersion, AggregateID: row.AggregateID, TenantID: row.TenantID, Timestamp: row.OccurredAt, CorrelationID: row.CorrelationID, CausationID: row.CausationID, Payload: row.Payload}
		if err := relay.publisher.Publish(ctx, event); err != nil {
			_ = relay.repository.MarkFailed(context.WithoutCancel(ctx), row.ID, relay.workerID, "kafka_publish", time.Second)
			continue
		}
		if err := relay.repository.MarkPublished(ctx, row.ID, relay.workerID); err != nil {
			return err
		}
	}
	return nil
}
