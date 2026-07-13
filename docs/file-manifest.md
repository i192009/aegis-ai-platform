# File manifest

Updated: 2026-07-13

## Implemented phases

- **Phase 0:** module bootstrap, configuration, lifecycle, schema, container base, CI, architecture, and ADRs
- **Phase 1:** API-key authentication, chat API, mock/OpenAI-compatible providers, request persistence, idempotency, state machine, and SSE
- **Phase 2:** provider registry, four routing strategies, EWMA latency, semaphores, retries, failover, cancellation, and circuit breaker
- **Phase 3:** Redis atomic limits and PostgreSQL budget reservation/reconciliation
- **Phase 4:** transactional outbox, Kafka publisher, retained event contracts, audit/usage consumer, and replay safety
- **Phase 5:** evaluation API, RabbitMQ confirms/prefetch/retries/DLQ, bounded worker pool, deterministic evaluators, and idempotent results
- **Phase 6:** structured logs, Prometheus collectors, OpenTelemetry spans/propagation, collector, and Grafana dashboard
- **Phase 7:** full readiness-ordered Docker Compose stack, migrations, seed process, development key, and admin CLI
- **Phase 8:** restricted Kubernetes/Helm workloads, services, ingress/Gateway API, HPA, PDB, network policy, topology spread, and KEDA
- **Phase 9:** unit, race, concurrent invariant, Testcontainers, k6, failure-injection, vulnerability, image, and manifest CI gates
- **Phase 10:** OpenAPI, examples, security/failure/operations documentation, runbooks, ADRs, and evidence-based interview guide

## Main file groups

- `cmd/`: gateway, mock provider, outbox relay, audit consumer, evaluation API/worker, admin CLI, and container healthcheck
- `internal/`: auth, request/idempotency, provider/routing/breaker, rate/budget, persistence, events/outbox/Kafka/RabbitMQ, evaluation, API, observability, health, config, and lifecycle
- `pkg/`: RFC 9457 problem responses and reusable HTTP middleware
- `migrations/`: complete up/down PostgreSQL migrations
- `api/`: OpenAPI 3.1 and executable curl examples
- `deployments/`: Docker, Helm, Kubernetes, KEDA, Prometheus, Grafana, and OpenTelemetry
- `tests/`: Testcontainers integration, k6 load, and failure injection
- `docs/`: architecture, data model, security, failure modes, operations, ADRs, runbooks, and interview story
- `.github/workflows/`: formatting, vet, static analysis, unit/race/integration, vulnerability, dependency, image, Compose, Helm, and Kubernetes validation

## Validation status

See [validation.md](validation.md) for the executed check matrix, results, and the
explicit Docker-daemon limitation of the delivery workspace.
