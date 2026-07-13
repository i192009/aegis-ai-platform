package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

type keyLookup struct{ key StoredKey }

func (lookup keyLookup) FindAPIKeyByPrefix(context.Context, string) (StoredKey, error) {
	if lookup.key.ID == "" {
		return StoredKey{}, errors.New("not found")
	}
	return lookup.key, nil
}

func TestGenerateAndVerify(t *testing.T) {
	pepper := []byte("0123456789abcdef0123456789abcdef")
	plain, prefix, hash, err := Generate(pepper)
	if err != nil {
		t.Fatal(err)
	}
	lookup := keyLookup{key: StoredKey{ID: "key-1", TenantID: "tenant-1", Prefix: prefix, Hash: hash, Scopes: []string{"chat:write"}}}
	principal, err := Verify(context.Background(), lookup, pepper, plain, time.Now())
	if err != nil || principal.TenantID != "tenant-1" {
		t.Fatalf("Verify() = %+v, %v", principal, err)
	}
	if err := RequireScope(principal, "chat:write"); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(context.Background(), lookup, pepper, plain+"bad", time.Now()); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("bad key error = %v", err)
	}
}
