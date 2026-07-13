#!/usr/bin/env bash
set -euo pipefail

api_key="${AEGIS_API_KEY:?AEGIS_API_KEY is required}"
curl --no-buffer --fail-with-body http://localhost:8080/v1/chat/completions \
  -H "X-API-Key: ${api_key}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: stream-$(date +%s)" \
  --data '{"model":"aegis-small","messages":[{"role":"user","content":"Stream a deterministic response."}],"stream":true}'
