# ADR 0004: Redis is not a source of truth

- Status: Accepted
- Date: 2026-07-13

## Context

Distributed request/token windows and short-lived concurrency leases need fast atomic operations, but eviction or loss must not erase durable business state.

## Decision

Use Redis for rate limiting, expiring coordination, and cached health state only. Durable request, budget, charge, and audit decisions remain in PostgreSQL.

## Consequences

Redis can be rebuilt without reconstructing financial truth. Its failure policy must still be explicit because fail-open and fail-closed each have availability or control consequences.
