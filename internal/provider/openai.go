package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/i192009/aegis-ai-platform/internal/request"
)

// OpenAIConfig configures an OpenAI-compatible HTTP provider.
type OpenAIConfig struct {
	Name                string
	Endpoint            string
	APIKey              string
	Models              []string
	DataClassifications []string
	Timeout             time.Duration
	MaxResponseBytes    int64
}

// OpenAICompatible adapts a vendor that implements the supported OpenAI HTTP subset.
type OpenAICompatible struct {
	config OpenAIConfig
	client *http.Client
}

// NewOpenAICompatible creates an adapter with bounded network behavior.
func NewOpenAICompatible(config OpenAIConfig) (*OpenAICompatible, error) {
	if config.Name == "" || config.Endpoint == "" {
		return nil, errors.New("provider name and endpoint are required")
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxResponseBytes <= 0 {
		config.MaxResponseBytes = 8 << 20
	}
	return &OpenAICompatible{
		config: config,
		client: &http.Client{Timeout: config.Timeout, Transport: otelhttp.NewTransport(&http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   20,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: config.Timeout,
		})},
	}, nil
}

func (adapter *OpenAICompatible) Name() string { return adapter.config.Name }

func (adapter *OpenAICompatible) Capabilities() Capabilities {
	models := make(map[string]struct{}, len(adapter.config.Models))
	for _, model := range adapter.config.Models {
		models[model] = struct{}{}
	}
	classes := make(map[string]struct{}, len(adapter.config.DataClassifications))
	for _, class := range adapter.config.DataClassifications {
		classes[class] = struct{}{}
	}
	return Capabilities{Models: models, DataClassifications: classes}
}

func (adapter *OpenAICompatible) Health(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(adapter.config.Endpoint, "/")+"/health/ready", nil)
	if err != nil {
		return err
	}
	response, err := adapter.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 400 {
		return HTTPError(adapter.Name(), response.StatusCode)
	}
	return nil
}

func (adapter *OpenAICompatible) Complete(ctx context.Context, input CompletionRequest) (Completion, error) {
	response, err := adapter.call(ctx, input, false)
	if err != nil {
		return Completion{}, err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, adapter.config.MaxResponseBytes)
	var body struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message      request.Message `json:"message"`
			FinishReason string          `json:"finish_reason"`
		} `json:"choices"`
		Usage request.Usage `json:"usage"`
	}
	if err := json.NewDecoder(limited).Decode(&body); err != nil {
		return Completion{}, &Error{Provider: adapter.Name(), Category: "malformed_response", Cause: err}
	}
	if len(body.Choices) == 0 {
		return Completion{}, &Error{Provider: adapter.Name(), Category: "empty_response"}
	}
	return Completion{ProviderRequestID: body.ID, Model: body.Model, Content: body.Choices[0].Message.Content, FinishReason: body.Choices[0].FinishReason, Usage: body.Usage}, nil
}

func (adapter *OpenAICompatible) Stream(ctx context.Context, input CompletionRequest, emit func(Chunk) error) (request.Usage, error) {
	response, err := adapter.call(ctx, input, true)
	if err != nil {
		return request.Usage{}, err
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(io.LimitReader(response.Body, adapter.config.MaxResponseBytes))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	partial := false
	var usage request.Usage
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			return usage, nil
		}
		var event struct {
			Choices []struct {
				Delta        request.Message `json:"delta"`
				FinishReason string          `json:"finish_reason"`
			} `json:"choices"`
			Usage *request.Usage `json:"usage"`
		}
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return usage, &Error{Provider: adapter.Name(), Category: "malformed_stream", Partial: partial, Cause: err}
		}
		if event.Usage != nil {
			usage = *event.Usage
		}
		if len(event.Choices) == 0 {
			continue
		}
		chunk := Chunk{Content: event.Choices[0].Delta.Content, FinishReason: event.Choices[0].FinishReason}
		if chunk.Content != "" {
			partial = true
		}
		if err := emit(chunk); err != nil {
			return usage, &Error{Provider: adapter.Name(), Category: "stream_write", Partial: partial, Cause: err}
		}
	}
	if err := scanner.Err(); err != nil {
		return usage, &Error{Provider: adapter.Name(), Category: "stream_read", Retryable: !partial, Partial: partial, Cause: err}
	}
	return usage, &Error{Provider: adapter.Name(), Category: "stream_ended_without_done", Retryable: !partial, Partial: partial}
}

func (adapter *OpenAICompatible) call(ctx context.Context, input CompletionRequest, stream bool) (*http.Response, error) {
	payload := struct {
		Model       string            `json:"model"`
		Messages    []request.Message `json:"messages"`
		Temperature *float64          `json:"temperature,omitempty"`
		MaxTokens   int               `json:"max_tokens,omitempty"`
		Stream      bool              `json:"stream"`
	}{Model: normalizeModel(input.Model), Messages: input.Messages, Temperature: input.Temperature, MaxTokens: input.MaxTokens, Stream: stream}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal provider request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(adapter.config.Endpoint, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create provider request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Request-ID", input.RequestID)
	if adapter.config.APIKey != "" {
		request.Header.Set("Authorization", "Bearer "+adapter.config.APIKey)
	}
	response, err := adapter.client.Do(request)
	if err != nil {
		return nil, &Error{Provider: adapter.Name(), Category: "transport", Retryable: remaining(ctx) > 0, Cause: err}
	}
	if response.StatusCode >= 400 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		response.Body.Close()
		return nil, HTTPError(adapter.Name(), response.StatusCode)
	}
	return response, nil
}
