package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/request"
)

// MockConfig controls deterministic local behavior and failure injection.
type MockConfig struct {
	Name            string
	Models          []string
	Latency         time.Duration
	FailFirst       int64
	FailureCategory string
}

// Mock is a deterministic provider used by local development and tests.
type Mock struct {
	config MockConfig
	calls  atomic.Int64
}

// NewMock creates a concurrency-safe deterministic provider.
func NewMock(config MockConfig) *Mock {
	if config.Name == "" {
		config.Name = "mock-primary"
	}
	if len(config.Models) == 0 {
		config.Models = []string{"aegis-small", "aegis-medium"}
	}
	return &Mock{config: config}
}

func (mock *Mock) Name() string { return mock.config.Name }

func (mock *Mock) Capabilities() Capabilities {
	models := make(map[string]struct{}, len(mock.config.Models))
	for _, model := range mock.config.Models {
		models[model] = struct{}{}
	}
	return Capabilities{Models: models, DataClassifications: map[string]struct{}{"PUBLIC": {}, "INTERNAL": {}, "CONFIDENTIAL": {}}}
}

func (mock *Mock) Health(context.Context) error { return nil }

func (mock *Mock) Complete(ctx context.Context, input CompletionRequest) (Completion, error) {
	if err := mock.wait(ctx); err != nil {
		return Completion{}, err
	}
	if err := mock.injectFailure(); err != nil {
		return Completion{}, err
	}
	content := mock.content(input)
	return Completion{
		ProviderRequestID: fmt.Sprintf("mock-%d", mock.calls.Load()),
		Model:             input.Model,
		Content:           content,
		FinishReason:      "stop",
		Usage:             estimateTokens(input.Messages, content),
	}, nil
}

func (mock *Mock) Stream(ctx context.Context, input CompletionRequest, emit func(Chunk) error) (request.Usage, error) {
	if err := mock.wait(ctx); err != nil {
		return request.Usage{}, err
	}
	if err := mock.injectFailure(); err != nil {
		return request.Usage{}, err
	}
	content := mock.content(input)
	words := strings.Fields(content)
	for index, word := range words {
		select {
		case <-ctx.Done():
			return request.Usage{}, ctx.Err()
		default:
		}
		if index < len(words)-1 {
			word += " "
		}
		if err := emit(Chunk{Content: word}); err != nil {
			return request.Usage{}, &Error{Provider: mock.Name(), Category: "stream_write", Partial: index > 0, Cause: err}
		}
	}
	if err := emit(Chunk{FinishReason: "stop"}); err != nil {
		return request.Usage{}, &Error{Provider: mock.Name(), Category: "stream_write", Partial: true, Cause: err}
	}
	return estimateTokens(input.Messages, content), nil
}

func (mock *Mock) content(input CompletionRequest) string {
	last := input.Messages[len(input.Messages)-1].Content
	return "Deterministic response to: " + last
}

func (mock *Mock) wait(ctx context.Context) error {
	if mock.config.Latency <= 0 {
		return nil
	}
	timer := time.NewTimer(mock.config.Latency)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (mock *Mock) injectFailure() error {
	call := mock.calls.Add(1)
	if call <= mock.config.FailFirst {
		category := mock.config.FailureCategory
		if category == "" {
			category = "injected_transient"
		}
		return &Error{Provider: mock.Name(), Category: category, Retryable: true, Cause: errors.New("deterministic failure injection")}
	}
	return nil
}
