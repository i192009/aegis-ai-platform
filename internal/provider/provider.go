// Package provider defines AI provider capabilities without coupling gateway logic to one vendor.
package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/request"
)

// Error is a normalized provider failure safe for retry classification.
type Error struct {
	Provider  string
	Category  string
	Status    int
	Retryable bool
	Partial   bool
	Cause     error
}

func (err *Error) Error() string {
	return fmt.Sprintf("provider %s: %s", err.Provider, err.Category)
}

func (err *Error) Unwrap() error { return err.Cause }

// CompletionRequest is provider-neutral input.
type CompletionRequest struct {
	RequestID   string
	Model       string
	Messages    []request.Message
	Temperature *float64
	MaxTokens   int
}

// Completion is one final provider result.
type Completion struct {
	ProviderRequestID string
	Model             string
	Content           string
	FinishReason      string
	Usage             request.Usage
}

// Chunk is one bounded streaming unit.
type Chunk struct {
	Content      string
	FinishReason string
}

// Capabilities describes the models and classifications permitted for one provider.
type Capabilities struct {
	Models              map[string]struct{}
	DataClassifications map[string]struct{}
}

// Provider performs normal and streaming completions. Stream calls emit synchronously,
// which propagates client backpressure and avoids an unbounded internal token queue.
type Provider interface {
	Name() string
	Complete(ctx context.Context, input CompletionRequest) (Completion, error)
	Stream(ctx context.Context, input CompletionRequest, emit func(Chunk) error) (request.Usage, error)
	Health(ctx context.Context) error
	Capabilities() Capabilities
}

// IsRetryable classifies normalized and common transport failures.
func IsRetryable(err error) bool {
	var providerErr *Error
	if errors.As(err, &providerErr) {
		return providerErr.Retryable && !providerErr.Partial
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) && networkErr.Timeout()
}

// HTTPError converts an upstream status to a bounded category.
func HTTPError(providerName string, status int) error {
	category := "upstream_error"
	retryable := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
	if status == http.StatusTooManyRequests {
		category = "rate_limited"
	} else if status == http.StatusUnauthorized || status == http.StatusForbidden {
		category = "provider_authentication"
	} else if status >= 400 && status < 500 {
		category = "invalid_provider_request"
	}
	return &Error{Provider: providerName, Category: category, Status: status, Retryable: retryable}
}

func estimateTokens(messages []request.Message, content string) request.Usage {
	promptCharacters := 0
	for _, message := range messages {
		promptCharacters += len(message.Content)
	}
	prompt := int64((promptCharacters + 3) / 4)
	completion := int64((len(content) + 3) / 4)
	return request.Usage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: prompt + completion}
}

func normalizeModel(value string) string { return strings.TrimSpace(value) }

func remaining(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Hour
	}
	return time.Until(deadline)
}
