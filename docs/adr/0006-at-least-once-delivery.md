# ADR 0006: Assume at-least-once delivery

- Status: Accepted
- Date: 2026-07-13

## Context

A relay or worker can crash after an external side effect succeeds but before its acknowledgement state is saved.

## Decision

Do not claim broker exactly-once delivery. Give every event and job a stable identifier and make database effects idempotent with unique constraints and same-transaction deduplication.

## Consequences

Redelivery is safe and testable. Handlers require more deliberate transaction design, and non-idempotent downstream effects need their own idempotency keys.
