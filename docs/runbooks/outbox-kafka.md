# Runbook: outbox or Kafka backlog

1. Compare unpublished outbox count/age with Kafka broker health and relay errors.
2. If Kafka is unavailable, leave committed outbox rows intact; do not mark them published manually.
3. Restore broker connectivity and verify relay claims advance.
4. Confirm consumer lag falls and processed-event uniqueness prevents duplicate projections.
5. Scale consumers only up to partition count; add partitions only after reviewing ordering implications.
6. Rebuild projections by using a new consumer group or controlled offset reset, never by deleting immutable audit records.
