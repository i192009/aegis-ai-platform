# ADR 0005: SSE for token streaming

- Status: Accepted
- Date: 2026-07-13

## Context

Chat completion tokens flow from server to client over an OpenAI-compatible HTTP API; client-to-server messages after request submission are unnecessary.

## Decision

Use Server-Sent Events with bounded buffers, flush after each event, context cancellation on disconnect, and no automatic failover after the first delivered token.

## Consequences

SSE works with normal HTTP infrastructure and simple clients. It is unidirectional, and proxy buffering and idle timeouts must be configured. Partial-stream failure must be recorded separately from a complete response.
