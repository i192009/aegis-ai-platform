# Interview story

## Honest framing

AegisAI is an original enterprise-style reference implementation built to explore bank-relevant platform concerns. It is not a system deployed at Citi or another bank. In interviews, say “I implemented and tested in this repository,” not “we operated this in production,” unless separate commercial experience supports that statement.

## 1. Tell me about the hardest problem you solved

The hardest part was making retries safe across several independent failure boundaries. A client retry, provider retry, outbox retry, Kafka redelivery, and RabbitMQ redelivery must not create a second logical request, final response, charge, audit projection, or evaluation result.

- **Implemented:** tenant-scoped idempotency, canonical hashes, logical requests versus attempts, conditional finalisation, outbox events, consumer deduplication, and commit-before-ack evaluation results.
- **Tested:** 100 concurrent in-memory submissions create one request; two attempts produce one final result; container tests cover PostgreSQL concurrency and Rabbit redelivery.
- **Production recommendation:** add multi-AZ failure testing, reconciliation jobs, and continuous invariant monitoring.
- **Assumptions:** brokers deliver at least once and PostgreSQL is the authoritative writer.

## 2. Tell me about a critical system you designed

I designed a multi-tenant AI gateway where authentication, policy, budget, provider routing, usage, and audit evidence form one controlled request lifecycle. The latency path is separated from asynchronous projection and evaluation work.

- **Implemented:** independently deployable gateway, outbox relay, audit consumer, evaluation API, and evaluation worker.
- **Tested:** unit/race tests and local runtime smoke tests; Compose and container integration assets are included.
- **Production recommendation:** managed brokers/databases, workload identity, regional design, SLOs, and a formal threat model.
- **Assumptions:** one primary write region and approved providers reachable through private egress.

## 3. Why did you use Kafka?

Kafka carries facts that happened and may need multiple consumers or replay: completion, usage, audit, security, and evaluation outcomes. Tenant ID is the partition key, preserving order for one tenant within a topic partition.

- **Implemented:** versioned envelopes, tenant keys, required acknowledgements, outbox relay, and processed-event uniqueness.
- **Tested:** event validation and deduplication transaction code; broker integration runs in Docker-enabled CI.
- **Production recommendation:** size partitions from measured throughput and document retention/compaction policy.
- **Assumptions:** total tenant ordering is more valuable than distributing one hot tenant across partitions.

## 4. Why did you also use RabbitMQ?

Evaluation is executable work that normally belongs to one worker and benefits from prefetch, explicit ack, TTL retry, and DLQ routing. That is different from a retained business fact.

- **Implemented:** durable exchanges/queues, persistent messages, confirmed publication, manual ack, prefetch, retry queue, DLX, and DLQ.
- **Tested:** a container test nacks and receives the same execution again; result uniqueness handles it.
- **Production recommendation:** quorum queues, broker policies, alerting on age/depth, and controlled DLQ tooling.
- **Assumptions:** one evaluation suite command can run the deterministic evaluators together.

## 5. How did you guarantee correctness during retries?

I did not rely on timing or process memory. Every repeated operation carries a stable key, and the database enforces uniqueness at the side-effect boundary.

- **Implemented:** unique tenant/idempotency, attempt numbers, event IDs, consumer names, job IDs, execution IDs, and conditional state updates.
- **Tested:** concurrent idempotency, competing final attempts, budget races, and broker redelivery.
- **Production recommendation:** reconciliation dashboards and property-based failure tests around commit/ack gaps.
- **Assumptions:** provider-side idempotency should also be used where a vendor supports it.

## 6. Why did you not claim exactly-once delivery?

A relay can publish and crash before marking the row, or a worker can commit and crash before ack. Calling that exactly once would hide a real duplicate-delivery window.

- **Implemented:** at-least-once handling with idempotent database effects.
- **Tested:** duplicate request/event/job paths converge.
- **Production recommendation:** explain guarantees per boundary rather than using one end-to-end slogan.
- **Assumptions:** duplicate messages are normal, not exceptional corruption.

## 7. How does the system scale across Kubernetes Pods?

Correctness state is external, so gateway and worker replicas do not depend on local maps. Kubernetes Services balance incoming traffic, HPA scales gateways, and KEDA scales broker consumers.

- **Implemented:** stateless commands, PostgreSQL constraints, Redis distributed limits, HPA, topology spread, PDBs, and KEDA.
- **Tested:** race-safe local structures and shared-store integration tests.
- **Production recommendation:** scale against active requests/provider saturation, not CPU alone.
- **Assumptions:** Kafka replicas above partition count provide no extra useful consumption parallelism.

## 8. How does load balancing work at platform and provider levels?

The platform load balancer distributes HTTP across gateway Pods. Inside a Pod, routing first filters ineligible providers, then applies weighted round robin, least outstanding, EWMA latency, or priority fallback.

- **Implemented:** model/classification/policy/health/circuit/capacity/deadline filters and four strategies.
- **Tested:** weights, capacity, model filtering, latency state, and circuit behavior.
- **Production recommendation:** share health signals carefully while keeping concurrency local to each adapter instance.
- **Assumptions:** provider quotas and platform concurrency are both configured accurately.

## 9. How did you prevent duplicate charging?

Usage is an immutable ledger entry keyed by tenant and event ID. Replayed Kafka facts cannot insert a second charge.

- **Implemented:** integer micro-USD calculation, request actual cost, usage event, consumer marker, and unique usage ledger event.
- **Tested:** fixed-precision cost and idempotency paths.
- **Production recommendation:** periodic provider-invoice reconciliation and separate adjustment entries rather than updates.
- **Assumptions:** token counts come from the provider or an approved tokenizer.

## 10. How did you protect tenant budgets?

A check without a reservation is unsafe because concurrent requests see the same balance. I lock the tenant limit row, calculate committed use plus active reservations, and insert a worst-case reservation in one short transaction.

- **Implemented:** monthly/daily limits, conservative maximum provider price, reservation, reconciliation, and release.
- **Tested:** concurrent reservations cannot exceed the configured limit.
- **Production recommendation:** expiry/recovery for abandoned reservations and alerts on reconciliation lag.
- **Assumptions:** maximum completion tokens bound cost and configured prices are current.

## 11. How did you implement backpressure?

Provider streaming invokes the HTTP emitter synchronously. A slow client therefore slows provider reads instead of filling an unbounded channel. Rabbit prefetch bounds deliveries per process.

- **Implemented:** direct streaming callback, maximum accumulated response size, provider semaphores, and worker prefetch.
- **Tested:** emitter failure terminates a partial stream without failover; race tests detect unsafe sharing.
- **Production recommendation:** measure slow-client duration and enforce ingress/write deadlines.
- **Assumptions:** upstream clients respect cancellation and network proxies do not buffer SSE.

## 12. How did you avoid unbounded goroutines?

Requests do not create background goroutines for tokens. Evaluation uses a fixed worker pool, and service loops own their goroutines through cancellation contexts.

- **Implemented:** bounded pool, bounded semaphores, signal cancellation, and dependency close order.
- **Tested:** race detector and cancellation paths.
- **Production recommendation:** add continuous goroutine/leak profiles during soak tests.
- **Assumptions:** broker client libraries release blocked calls when their connection or context closes.

## 13. How did you handle partial streaming failures?

Once any token is written, a different model cannot safely continue the semantic response. The request is marked failed with `partial_response_streamed=true`, and the SSE stream sends a bounded error event.

- **Implemented:** partial error classification, no retry after emitted output, and persisted partial flag.
- **Tested:** client/emitter failure stops the request without provider failover.
- **Production recommendation:** define client UX for discarding or explicitly retaining partial content.
- **Assumptions:** retries before the first emitted token remain safe.

## 14. How did you make consumers idempotent?

The consumer inserts `(consumer_name, event_id)` and its audit/usage effect in the same transaction. A conflict means the effect already committed and the offset can be acknowledged.

- **Implemented:** processed-event primary key and same-transaction effects.
- **Tested:** SQL/integration paths compile and run in Docker-enabled CI.
- **Production recommendation:** maintain event-version compatibility and replay runbooks.
- **Assumptions:** every producer supplies a stable globally unique event ID.

## 15. How did you design graceful shutdown?

The process becomes unready, stops accepting work, cancels broker loops, drains HTTP within a deadline, closes clients, flushes tracing, and exits inside the Pod grace period.

- **Implemented:** signal context, readiness state, HTTP `Shutdown`, worker cancellation, and dependency close callbacks.
- **Tested:** gateway `SIGTERM` smoke test and race suite.
- **Production recommendation:** measure drain time under peak streaming and queue load.
- **Assumptions:** the 45-second Kubernetes grace period exceeds the configured 20-second application drain.

## 16. What would you change for a real bank deployment?

I would add institutional identity/workload identity, service-specific database roles, managed multi-AZ infrastructure, KMS/HSM secrets, egress controls, enterprise DLP, signed images/SBOMs, audited operator workflows, residency enforcement, and formal SLO/capacity evidence.

- **Implemented:** secure reference defaults and explicit secret references.
- **Tested:** static, unit, race, integration, image, and manifest CI gates are defined.
- **Production recommendation:** complete threat modelling, penetration testing, compliance mapping, and disaster recovery exercises.
- **Assumptions:** bank standards and approved platforms determine the final controls.

## 17. What trade-offs kept the project manageable?

I used five deployable responsibilities in one Go module, one deterministic evaluation suite, explicit SQL, SSE instead of WebSockets, and one primary region. I avoided a service for every package.

- **Implemented:** small process set with shared reviewed contracts.
- **Tested:** all commands build together and preserve package boundaries.
- **Production recommendation:** split only where ownership, scaling, or risk provides measurable benefit.
- **Assumptions:** repository-scale development benefits from atomic contract changes.

## 18. What performance metrics would you measure?

I would measure throughput; p50/p95/p99 end-to-end and provider latency; active/streaming requests; provider saturation; DB pool wait; Redis latency; outbox age; Kafka lag; queue age/depth; evaluation duration; retries; errors; cancellations; memory; and goroutines.

- **Implemented:** low-cardinality Prometheus collectors and a Grafana overview.
- **Tested:** metrics endpoint runtime smoke test.
- **Production recommendation:** derive SLOs from representative capacity tests, not invented numbers.
- **Assumptions:** telemetry sampling and retention comply with privacy policy.

## 19. How would you handle regional data residency?

I would route tenants to a home region before the gateway, keep prompts/responses and authoritative records in-region, constrain eligible providers by classification/region, and replicate only approved metadata.

- **Implemented:** classification-aware provider eligibility; regional storage is not implemented.
- **Tested:** provider classification filtering.
- **Production recommendation:** regional keys, residency policy engine, isolated Kafka/Rabbit clusters, and documented failover restrictions.
- **Assumptions:** legal policy defines which metadata may cross regions.

## 20. How would you operate this during an incident?

I would establish incident roles, classify impact, stabilise without bypassing correctness, trace representative requests, control provider/broker traffic, verify backlog recovery, and reconcile usage/budgets afterward.

- **Implemented:** health endpoints, structured logs, metrics, tracing, circuit controls, DLQ/outbox state, and runbooks.
- **Tested:** deterministic fault commands and failure-focused tests are included.
- **Production recommendation:** rehearsed game days, paging ownership, status communication, and post-incident corrective actions.
- **Assumptions:** operators have audited read access but not prompt contents by default.
