#!/usr/bin/env bash
set -euo pipefail

action="${1:-}"
case "$action" in
  stop-provider)
    docker compose stop mock-provider
    ;;
  start-provider)
    docker compose start mock-provider
    ;;
  kill-worker)
    docker compose kill evaluation-worker
    docker compose up -d evaluation-worker
    ;;
  restart-kafka)
    docker compose restart kafka
    ;;
  restart-rabbitmq)
    docker compose restart rabbitmq
    ;;
  queue-depth)
    echo "Run: k6 run tests/load/evaluation.js with a completed SOURCE_REQUEST_ID"
    ;;
  terminate-gateway-pod)
    kubectl -n aegis-ai delete pod -l app.kubernetes.io/component=gateway --wait=false
    ;;
  database-latency)
    echo "Apply latency with a staging-network fault tool or Toxiproxy; do not alter production PostgreSQL settings."
    ;;
  *)
    echo "usage: $0 {stop-provider|start-provider|kill-worker|restart-kafka|restart-rabbitmq|queue-depth|terminate-gateway-pod|database-latency}" >&2
    exit 2
    ;;
esac
