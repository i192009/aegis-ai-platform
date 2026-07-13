#!/usr/bin/env bash
set -euo pipefail

api_key="${AEGIS_API_KEY:?AEGIS_API_KEY is required}"
request_id="${1:?usage: evaluate.sh REQUEST_ID}"
curl --fail-with-body http://localhost:8083/v1/evaluations \
  -H "X-API-Key: ${api_key}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: evaluation-${request_id}" \
  --data "{\"request_id\":\"${request_id}\",\"job_type\":\"response-evaluation-suite\",\"parameters\":{\"max_total_latency_ms\":5000}}"
