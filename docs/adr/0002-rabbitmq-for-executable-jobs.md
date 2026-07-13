# ADR 0002: RabbitMQ for executable jobs

- Status: Accepted
- Date: 2026-07-13

## Context

An evaluation command should normally be executed by one worker and needs explicit acknowledgement, prefetch, delayed retry, and dead-letter routing.

## Decision

Use durable RabbitMQ queues and persistent messages for evaluation work. Workers acknowledge only after the result commits. Transient failures move through TTL retry queues; permanent or exhausted work reaches a dead-letter queue.

## Consequences

Queue semantics match executable work and allow depth-based KEDA scaling. Redelivery remains possible, so database uniqueness is required. RabbitMQ is not used as the replayable audit record.
