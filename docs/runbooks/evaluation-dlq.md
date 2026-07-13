# Runbook: evaluation DLQ

1. Pause bulk retries and inspect bounded error category, job type, attempt count, and deployment version.
2. Determine whether failure is malformed input, source data, database availability, evaluator defect, or schema incompatibility.
3. Fix the underlying cause and deploy a backward-compatible worker.
4. Use the evaluation retry API for selected jobs; retain the original job ID.
5. Confirm result uniqueness and that RabbitMQ acknowledgements occur only after commit.
6. Record the incident and add a deterministic regression test before draining the remaining DLQ.
