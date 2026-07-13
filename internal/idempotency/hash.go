// Package idempotency canonicalizes semantic request input for conflict detection.
package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/i192009/aegis-ai-platform/internal/request"
)

// HashChat returns a stable SHA-256 hash. encoding/json sorts string map keys.
func HashChat(input request.ChatInput) ([]byte, error) {
	canonical, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical chat request: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return sum[:], nil
}

// Hex returns the human-readable representation used only in diagnostics and tests.
func Hex(hash []byte) string { return hex.EncodeToString(hash) }
