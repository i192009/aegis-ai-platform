// Package request contains the logical AI request domain model and state machine.
package request

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// State is the durable state of one logical request.
type State string

const (
	Received       State = "RECEIVED"
	Validated      State = "VALIDATED"
	Routing        State = "ROUTING"
	InProgress     State = "IN_PROGRESS"
	Completed      State = "COMPLETED"
	Failed         State = "FAILED"
	Cancelled      State = "CANCELLED"
	BudgetRejected State = "BUDGET_REJECTED"
)

var allowedTransitions = map[State]map[State]struct{}{
	Received:   {Validated: {}, Failed: {}, Cancelled: {}},
	Validated:  {Routing: {}, BudgetRejected: {}, Failed: {}, Cancelled: {}},
	Routing:    {InProgress: {}, Failed: {}, Cancelled: {}},
	InProgress: {Completed: {}, Failed: {}, Cancelled: {}},
}

// CanTransition reports whether moving from current to next preserves the state machine.
func CanTransition(current, next State) bool {
	_, ok := allowedTransitions[current][next]
	return ok
}

// Transition validates a state change.
func Transition(current, next State) error {
	if !CanTransition(current, next) {
		return fmt.Errorf("invalid request transition %s -> %s", current, next)
	}
	return nil
}

// Message is an OpenAI-compatible chat message subset.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatInput is the semantic input used by the gateway and idempotency hash.
type ChatInput struct {
	Model       string         `json:"model"`
	Messages    []Message      `json:"messages"`
	Temperature *float64       `json:"temperature,omitempty"`
	MaxTokens   int            `json:"max_tokens,omitempty"`
	Stream      bool           `json:"stream,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// Validate applies transport-independent safety and compatibility validation.
func (in ChatInput) Validate() error {
	if strings.TrimSpace(in.Model) == "" {
		return errors.New("model is required")
	}
	if len(in.Model) > 128 {
		return errors.New("model is too long")
	}
	if len(in.Messages) == 0 || len(in.Messages) > 100 {
		return errors.New("messages must contain between 1 and 100 items")
	}
	for index, message := range in.Messages {
		switch message.Role {
		case "system", "user", "assistant", "tool":
		default:
			return fmt.Errorf("messages[%d].role is unsupported", index)
		}
		if strings.TrimSpace(message.Content) == "" {
			return fmt.Errorf("messages[%d].content is required", index)
		}
		if len(message.Content) > 65536 {
			return fmt.Errorf("messages[%d].content is too long", index)
		}
	}
	if in.Temperature != nil && (*in.Temperature < 0 || *in.Temperature > 2) {
		return errors.New("temperature must be between 0 and 2")
	}
	if in.MaxTokens < 0 || in.MaxTokens > 32768 {
		return errors.New("max_tokens must be between 0 and 32768")
	}
	return nil
}

// Usage is provider-reported token usage.
type Usage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

// ChatResponse is the supported OpenAI-compatible completion response.
type ChatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []Choice     `json:"choices"`
	Usage   Usage        `json:"usage"`
	Aegis   ResponseMeta `json:"aegis"`
}

// Choice is one assistant response choice.
type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

// ResponseMeta exposes safe platform status without provider secrets.
type ResponseMeta struct {
	RequestID string `json:"request_id"`
	Status    State  `json:"status"`
}

// Record is a logical request returned by persistence.
type Record struct {
	ID               string
	TenantID         string
	APIKeyID         string
	IdempotencyKey   string
	CanonicalHash    []byte
	Model            string
	State            State
	Stream           bool
	PartialStreamed  bool
	Response         *ChatResponse
	FailureCategory  string
	CorrelationID    string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	CompletedAt      *time.Time
	PromptTokens     int64
	CompletionTokens int64
	CostMicroUSD     int64
}
