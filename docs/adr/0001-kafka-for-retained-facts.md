# ADR 0001: Kafka for retained facts

- Status: Accepted
- Date: 2026-07-13

## Context

Usage, audit, security, and completion facts need retention, ordered partition processing, replay, and multiple independent consumers.

## Decision

Publish versioned facts to Kafka after committing them through a PostgreSQL outbox. Use tenant ID as the default record key so a tenant's events in one topic retain partition order. Consumers assume at-least-once delivery and deduplicate by event ID.

## Consequences

New projections can replay retained history. Kafka availability is removed from the gateway commit path. Duplicate publication is possible and expected, and a very hot tenant may limit partition distribution.
