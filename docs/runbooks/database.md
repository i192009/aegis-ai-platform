# Runbook: PostgreSQL degradation

1. Confirm readiness failures, connection-pool wait, transaction latency, locks, and storage health.
2. Stop nonessential replay or administrative work before increasing connection limits.
3. Protect budget and idempotency correctness by failing closed; do not redirect writes to Redis.
4. Follow the managed database failover procedure and verify the writer endpoint before restoring traffic.
5. Validate request/outbox atomicity, active budget reservations, and replication recovery.
6. Reconcile reservations left active by interrupted requests through an audited maintenance job.
