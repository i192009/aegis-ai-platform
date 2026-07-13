# Observability

## Logs

All services emit JSON through `log/slog` with service, version, environment, operation, duration, outcome, and bounded error category where available. Request and correlation identifiers are carried in HTTP and broker metadata. Tenant identifiers may appear only as controlled diagnostic attributes, never as metric labels.

The following are deliberately excluded: raw keys, authorization headers, prompts, responses, provider secrets, token contents, and unmasked PII.

## Metrics

Every process exposes `/metrics`. Registered collectors include:

- HTTP count, duration, and active requests
- streaming connections
- provider requests, latency, errors, circuit state, and retries
- rate-limit and budget rejections
- Kafka publication failures and consumer lag
- RabbitMQ jobs, evaluation duration, and dead letters
- outbox backlog and database errors

Labels use service, route template, status, bounded provider, and bounded category. IDs and tenants are excluded. The included Grafana dashboard shows request rate, p95 latency, provider errors, and outbox backlog.

## Traces

The gateway uses OpenTelemetry HTTP server instrumentation. Provider HTTP uses an instrumented transport. Core PostgreSQL, Redis, Kafka, and RabbitMQ operations create spans. W3C `traceparent`, `tracestate`, and baggage are injected into Kafka and RabbitMQ headers and extracted by consumers.

The default sampler records 10% of root traces while retaining parent decisions. Compose sends OTLP/HTTP to the collector, whose development exporter logs bounded trace summaries. Production should export to an authenticated managed backend and tune sampling by risk and volume.

## Suggested alerts

- Gateway 5xx ratio above 2% for five minutes
- Provider circuit open or provider failures above baseline
- Outbox oldest unpublished age above two minutes
- Kafka lag growing for fifteen minutes
- Rabbit work queue above autoscaling capacity
- DLQ non-empty
- Budget or rate rejection anomaly
- Database pool saturation or p95 transaction latency above target
- Readiness failures or repeated restarts

Operational objectives must be based on measured capacity tests; this repository does not invent latency or throughput claims.
