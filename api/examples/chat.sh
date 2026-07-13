#!/usr/bin/env bash
set -euo pipefail

api_key="${AEGIS_API_KEY:?AEGIS_API_KEY is required}"
curl --fail-with-body http://localhost:8080/v1/chat/completions \
  -H "X-API-Key: ${api_key}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: example-$(date +%s)" \
  --data '{"model":"aegis-small","messages":[{"role":"user","content":"Explain idempotency in one paragraph."}],"max_tokens":256}'
