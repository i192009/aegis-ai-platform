# ADR 0010: Limited independently deployable services

- Status: Accepted
- Date: 2026-07-13

## Context

Latency-sensitive API work, retained event projection, and CPU-bound evaluation have different scaling and failure characteristics, but excessive microservices would add operational cost without clearer ownership.

## Decision

Deploy gateway, outbox relay, audit consumer, evaluation API, and evaluation worker as separate commands from one Go module. Keep domain boundaries as packages until independent deployment provides a measurable benefit.

## Consequences

Processes can scale and fail independently while sharing reviewed contracts and tooling. Releases require compatibility discipline around the database and broker schemas.
