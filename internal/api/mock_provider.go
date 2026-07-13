package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/provider"
	"github.com/i192009/aegis-ai-platform/internal/request"
)

// MockProvider exposes a deterministic OpenAI-compatible HTTP provider.
type MockProvider struct{ provider *provider.Mock }

func NewMockProvider(mock *provider.Mock) *MockProvider { return &MockProvider{provider: mock} }

func (mockAPI *MockProvider) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/chat/completions", mockAPI.complete)
}

func (mockAPI *MockProvider) complete(w http.ResponseWriter, r *http.Request) {
	var input request.ChatInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.Validate() != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	providerInput := provider.CompletionRequest{RequestID: r.Header.Get("X-Request-ID"), Model: input.Model, Messages: input.Messages, Temperature: input.Temperature, MaxTokens: input.MaxTokens}
	if !input.Stream {
		completion, err := mockAPI.provider.Complete(r.Context(), providerInput)
		if err != nil {
			http.Error(w, "mock provider failure", http.StatusServiceUnavailable)
			return
		}
		response := request.ChatResponse{ID: completion.ProviderRequestID, Object: "chat.completion", Created: time.Now().Unix(), Model: completion.Model, Choices: []request.Choice{{Index: 0, Message: request.Message{Role: "assistant", Content: completion.Content}, FinishReason: completion.FinishReason}}, Usage: completion.Usage}
		writeJSON(w, http.StatusOK, response)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	usage, err := mockAPI.provider.Stream(r.Context(), providerInput, func(chunk provider.Chunk) error {
		event := map[string]any{"id": "mock-stream", "object": "chat.completion.chunk", "model": input.Model, "choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": chunk.Content}, "finish_reason": nilIfBlank(chunk.FinishReason)}}}
		encoded, _ := json.Marshal(event)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", encoded); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	})
	_ = usage
	if err == nil {
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	}
}
