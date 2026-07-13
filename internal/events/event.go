// Package events defines versioned broker contracts and validation.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	RequestAccepted    = "ai.request.accepted.v1"
	ProviderSelected   = "ai.provider.selected.v1"
	RequestCompleted   = "ai.request.completed.v1"
	RequestFailed      = "ai.request.failed.v1"
	UsageRecorded      = "ai.usage.recorded.v1"
	BudgetExceeded     = "ai.budget.exceeded.v1"
	SecurityViolation  = "ai.security.violation.v1"
	EvaluationComplete = "evaluation.completed.v1"
	EvaluationFailed   = "evaluation.failed.v1"
)

var supported = map[string]struct{}{
	RequestAccepted: {}, ProviderSelected: {}, RequestCompleted: {}, RequestFailed: {}, UsageRecorded: {}, BudgetExceeded: {}, SecurityViolation: {}, EvaluationComplete: {}, EvaluationFailed: {},
}

// Envelope is the stable Kafka event contract.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	Version       int             `json:"version"`
	AggregateID   string          `json:"aggregate_id"`
	TenantID      string          `json:"tenant_id"`
	Timestamp     time.Time       `json:"timestamp"`
	CorrelationID string          `json:"correlation_id"`
	CausationID   string          `json:"causation_id,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

// Validate rejects malformed or unknown events before side effects.
func (event Envelope) Validate() error {
	if event.EventID == "" || event.AggregateID == "" || event.TenantID == "" || event.CorrelationID == "" {
		return errors.New("event identifiers are required")
	}
	if _, ok := supported[event.EventType]; !ok {
		return fmt.Errorf("unsupported event type %q", event.EventType)
	}
	if event.Version != 1 {
		return fmt.Errorf("unsupported event version %d", event.Version)
	}
	if event.Timestamp.IsZero() || !json.Valid(event.Payload) {
		return errors.New("event timestamp and valid payload are required")
	}
	return nil
}
