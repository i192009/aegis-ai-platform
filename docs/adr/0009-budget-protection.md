# ADR 0009: Transactional budget reservations

- Status: Accepted
- Date: 2026-07-13

## Context

Checking a remaining balance without locking allows concurrent requests to spend the same money.

## Decision

Before provider execution, lock the tenant limit row, calculate committed use plus active reservations, and insert a conservative reservation in one PostgreSQL transaction. Reconcile actual cost afterward and release unused value.

## Consequences

Concurrent requests cannot all pass the same remainder check. Admission serializes per tenant at a short transaction boundary, and abandoned reservations need safe expiry/recovery rules.
