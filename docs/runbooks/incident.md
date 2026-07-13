# Runbook: platform incident

1. Assign incident lead, communications owner, and operator; record a shared timeline.
2. Classify customer impact by tenant, model, operation, and region without exposing prompt data.
3. Stabilise: reduce traffic, disable a provider, pause workers, or stop deployment rollout as evidence dictates.
4. Preserve correctness first: do not bypass tenant checks, budget locks, outbox state, or consumer deduplication.
5. Use correlation and trace IDs to follow representative requests across HTTP and brokers.
6. Verify recovery through live synthetic requests and backlog drain, not only green Pods.
7. After recovery, reconcile usage/budgets, review DLQ/outbox rows, rotate exposed secrets if relevant, and write corrective actions with owners.
