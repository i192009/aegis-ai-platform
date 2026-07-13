package idempotency

import (
	"bytes"
	"testing"

	"github.com/i192009/aegis-ai-platform/internal/request"
)

func TestHashChatStableAcrossMetadataOrder(t *testing.T) {
	left := request.ChatInput{Model: "m", Messages: []request.Message{{Role: "user", Content: "hello"}}, Metadata: map[string]any{"b": 2, "a": 1}}
	right := request.ChatInput{Model: "m", Messages: []request.Message{{Role: "user", Content: "hello"}}, Metadata: map[string]any{"a": 1, "b": 2}}
	leftHash, _ := HashChat(left)
	rightHash, _ := HashChat(right)
	if !bytes.Equal(leftHash, rightHash) {
		t.Fatal("equivalent metadata produced different hashes")
	}
	right.Messages[0].Content = "different"
	rightHash, _ = HashChat(right)
	if bytes.Equal(leftHash, rightHash) {
		t.Fatal("different semantic input produced same hash")
	}
}
