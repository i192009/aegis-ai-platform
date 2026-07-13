# ADR 0003: PostgreSQL as source of truth

- Status: Accepted
- Date: 2026-07-13

## Context

Request ownership, state transitions, budget reservation, usage accounting, outbox insertion, and idempotency require visible transactions and constraints.

## Decision

Use PostgreSQL as the authoritative store with explicit SQL, foreign keys, uniqueness constraints, conditional updates, and row locking at correctness boundaries.

## Consequences

Correctness does not depend on one process replica. The primary database becomes a critical dependency and must be operated with backups, point-in-time recovery, connection limits, and tested failover.
