# ADR 0007: Tenant-scoped idempotency

- Status: Accepted
- Date: 2026-07-13

## Context

Clients retry on timeouts, while different tenants may legitimately choose the same idempotency value. Reusing one key for a different payload must not silently return unrelated output.

## Decision

Enforce uniqueness on `(tenant_id, idempotency_key)`. Store a SHA-256 hash of canonical semantic input. An identical retry returns the existing logical request; a mismatched hash returns `409 Conflict`.

## Consequences

Retries converge across gateway replicas. Canonicalization must be stable and exclude transport-only fields while retaining every field that changes model behavior.
