// Package auth creates, hashes, verifies, and propagates tenant API-key principals.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

const keyPrefix = "aegis"

var (
	// ErrInvalidKey indicates malformed or unknown credentials without disclosing which.
	ErrInvalidKey = errors.New("invalid API key")
	// ErrExpiredKey indicates a known credential outside its validity window.
	ErrExpiredKey = errors.New("API key expired")
	// ErrRevokedKey indicates a known credential that was revoked.
	ErrRevokedKey = errors.New("API key revoked")
	// ErrScope indicates an authenticated key lacks an operation scope.
	ErrScope = errors.New("API key scope denied")
)

// StoredKey is the non-secret credential record loaded from persistence.
type StoredKey struct {
	ID        string
	TenantID  string
	UserID    string
	Prefix    string
	Hash      []byte
	Scopes    []string
	ExpiresAt *time.Time
	RevokedAt *time.Time
}

// Principal is safe authentication context used for tenant-scoped authorization.
type Principal struct {
	APIKeyID string
	TenantID string
	UserID   string
	Scopes   map[string]struct{}
}

// Lookup resolves a non-secret prefix to a stored key candidate.
type Lookup interface {
	FindAPIKeyByPrefix(ctx context.Context, prefix string) (StoredKey, error)
}

// Generate returns a display-once key, its lookup prefix, and HMAC-SHA-256 hash.
func Generate(pepper []byte) (plain, prefix string, hash []byte, err error) {
	if len(pepper) < 32 {
		return "", "", nil, errors.New("API key pepper must be at least 32 bytes")
	}
	random := make([]byte, 32)
	if _, err = rand.Read(random); err != nil {
		return "", "", nil, fmt.Errorf("generate API key: %w", err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(random)
	prefix = encoded[:10]
	plain = keyPrefix + "_" + prefix + "_" + encoded
	return plain, prefix, Hash(pepper, plain), nil
}

// Hash creates the keyed digest stored in PostgreSQL.
func Hash(pepper []byte, plain string) []byte {
	digest := hmac.New(sha256.New, pepper)
	_, _ = digest.Write([]byte(plain))
	return digest.Sum(nil)
}

// Verify authenticates a display-once secret using a prefix lookup and constant-time hash comparison.
func Verify(ctx context.Context, lookup Lookup, pepper []byte, plain string, now time.Time) (Principal, error) {
	prefix, err := ParsePrefix(plain)
	if err != nil {
		return Principal{}, ErrInvalidKey
	}
	stored, err := lookup.FindAPIKeyByPrefix(ctx, prefix)
	if err != nil {
		return Principal{}, ErrInvalidKey
	}
	computed := Hash(pepper, plain)
	if len(computed) != len(stored.Hash) || subtle.ConstantTimeCompare(computed, stored.Hash) != 1 {
		return Principal{}, ErrInvalidKey
	}
	if stored.RevokedAt != nil {
		return Principal{}, ErrRevokedKey
	}
	if stored.ExpiresAt != nil && !now.Before(*stored.ExpiresAt) {
		return Principal{}, ErrExpiredKey
	}
	principal := Principal{APIKeyID: stored.ID, TenantID: stored.TenantID, UserID: stored.UserID, Scopes: make(map[string]struct{}, len(stored.Scopes))}
	for _, scope := range stored.Scopes {
		principal.Scopes[scope] = struct{}{}
	}
	return principal, nil
}

// ParsePrefix returns the indexed non-secret portion of a key.
func ParsePrefix(plain string) (string, error) {
	parts := strings.SplitN(plain, "_", 3)
	if len(parts) != 3 || parts[0] != keyPrefix || len(parts[1]) != 10 || len(parts[2]) < 40 {
		return "", ErrInvalidKey
	}
	return parts[1], nil
}

// RequireScope verifies one operation permission.
func RequireScope(principal Principal, scope string) error {
	if _, ok := principal.Scopes["*"]; ok {
		return nil
	}
	if _, ok := principal.Scopes[scope]; !ok {
		return ErrScope
	}
	return nil
}

type principalKey struct{}

// WithPrincipal attaches an authenticated principal to a request context.
func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

// FromContext retrieves the authenticated principal.
func FromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalKey{}).(Principal)
	return principal, ok
}
