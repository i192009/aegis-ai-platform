# Load and failure testing

Install k6 and export the development key:

```bash
export AEGIS_API_KEY='aegis_devkey0000_0123456789abcdefghijklmnopqrstuvwxyzABCDEFG'
k6 run tests/load/chat.js
k6 run tests/load/streaming.js
k6 run tests/load/idempotency.js
k6 run tests/load/rate-limit.js
k6 run tests/load/failover.js
SOURCE_REQUEST_ID=<completed-id> k6 run tests/load/evaluation.js
```

The thresholds are initial test assertions, not production SLO claims. Measure request rate, p50/p95/p99 latency, active requests, goroutines, memory, provider concurrency, database pool wait, Redis latency, outbox age, Kafka lag, queue depth, evaluation duration, errors, and cancellations.

Run fault commands from the repository root:

```bash
tests/failure/inject.sh stop-provider
tests/failure/inject.sh start-provider
tests/failure/inject.sh kill-worker
tests/failure/inject.sh restart-kafka
tests/failure/inject.sh restart-rabbitmq
tests/failure/inject.sh terminate-gateway-pod
```

For database network latency use an approved staging fault proxy such as Toxiproxy or a cluster network-chaos tool. The script deliberately does not mutate production database settings.

Success means invariants remain true: one logical request per idempotency key, one final result, no duplicate usage or evaluation result, bounded recovery, no goroutine growth after cancellation, and backlog recovery after dependencies return.
