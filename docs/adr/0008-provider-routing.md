# ADR 0008: Policy-filtered provider routing

- Status: Accepted
- Date: 2026-07-13

## Context

Routing must respect tenant policy and data classification before considering load, latency, weight, or price.

## Decision

First filter by enabled state, model capability, health, circuit state, capacity, tenant allow-list, classification, retry exclusions, and remaining deadline. Then apply the configured weighted-round-robin, least-outstanding, EWMA-latency, or priority strategy.

## Consequences

An ineligible provider can never win through a favorable score. Dynamic state requires race-safe local structures and eventual convergence between replicas; network calls occur outside locks.
