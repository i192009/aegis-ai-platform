# Failure modes

| Failure | Expected behavior | Correctness mechanism | Operator signal |
| --- | --- | --- | --- |
| Client retries after timeout | Same logical request or current status is returned | Tenant/idempotency uniqueness and canonical hash | Replay header and request status |
| Two gateway replicas submit simultaneously | One insert wins; both resolve the same record | PostgreSQL unique constraint | Conflict only for different payload |
| Provider times out before output | Bounded retry with jitter and provider exclusion | Attempt records and remaining deadline | Provider error/retry metrics |
| Provider fails after streaming starts | Stream ends with error; no provider switch | Partial-stream flag and synchronous emitter | Failed request with partial flag |
| Gateway dies after request commit | Client retry reads committed response | Durable logical request | Kubernetes restart, replay response |
| Kafka unavailable | Request completion still commits; outbox backlog grows | Transactional outbox | Outbox backlog and publish failures |
| Relay dies after Kafka ack but before mark | Event may publish twice | Consumer event-ID deduplication | Duplicate marked processed once |
| Kafka consumer dies before offset commit | Kafka redelivers; effects are not duplicated | Same-transaction processed marker | Consumer restarts and lag recovers |
| Rabbit worker dies before result commit | Message redelivers | Manual ack remains outstanding | Queue depth/redelivery |
| Rabbit worker dies after commit before ack | Redelivery sees existing result | Unique job/result identifiers | Duplicate completes without second result |
| Rabbit transient error | Job moves through five-second retry queue | Confirmed republish then original ack | Retry count and job attempt |
| Permanent evaluation failure | Message reaches DLQ and job is dead-lettered | DLX/DLQ plus durable job status | Dead-letter metric and runbook |
| Redis unavailable | Gateway returns an admission error | Redis is not source of truth | Readiness and Redis span error |
| Concurrent budget requests | Reservations serialize per tenant | PostgreSQL row lock and serializable transaction | Budget rejection count |
| PostgreSQL unavailable | Services become unready and do not accept correctness-sensitive work | PostgreSQL is authoritative | Readiness failure and DB errors |
| Pod termination | Readiness drops, HTTP drains, polling stops, dependencies close | Bounded graceful shutdown | Termination duration |

## Retry-storm controls

Provider retries are capped, deadline-aware, jittered, and exclude the failed provider. Rabbit retries use broker TTL rather than sleeping worker goroutines. Outbox failures release a claim with delayed availability. Kubernetes scaling has maximum replicas and cooldown periods.

## Data recovery

PostgreSQL requires automated backups and tested point-in-time recovery. Kafka retained events can rebuild audit/usage projections but do not replace the primary request record. Redis state is disposable. RabbitMQ contains work, while evaluation job and result state remains durable in PostgreSQL.
