# Kubernetes deployment

## Install

Create the namespace and a real secret through the organisation's secret-management process:

```bash
kubectl apply -f deployments/kubernetes/namespace.yaml
kubectl -n aegis-ai create secret generic aegis-ai-secrets \
  --from-literal=database-url="$DATABASE_URL" \
  --from-literal=api-key-pepper="$API_KEY_PEPPER" \
  --from-literal=rabbitmq-url="$RABBITMQ_URL" \
  --from-literal=provider-api-key="$PROVIDER_API_KEY"
helm upgrade --install aegis deployments/helm/aegis-ai --namespace aegis-ai
```

Apply migrations as a separately authorised release job before rolling application Pods. The chart intentionally does not run schema changes from every replica.

## Security and availability

The chart configures restricted workload contexts, immutable service-account posture, probes, resource requests/limits, rolling updates with zero unavailable replicas, topology spread, disruption budgets, 45-second termination grace, and default-deny network policy with required egress ports.

The network policy is a starting point. Namespace-wide selectors should be narrowed to actual database, broker, ingress, DNS, and telemetry identities in each cluster.

## Scaling

Gateway HPA starts with CPU at 65%. Request-rate, active-request, or latency metrics are better long-term signals after the Prometheus adapter is installed. Avoid scaling solely on CPU for slow provider calls.

KEDA scales evaluation workers on RabbitMQ queue depth and audit consumers on Kafka lag. Kafka useful consumer parallelism is capped by partition count. Tenant-key partitioning preserves a tenant's order within the topic; adding partitions changes future key distribution and needs an ordering review.

## Streaming

The NGINX Ingress annotations disable response buffering and permit a five-minute read timeout. Gateway API `HTTPRoute` is available as an alternative. Load balancers must propagate disconnect cancellation and allow SSE idle intervals or application heartbeats.

## Rollback

Application images can be rolled back when database migrations remain backward compatible. Destructive down migrations are not an automatic production rollback. Use expand/migrate/contract schema changes for real deployments.
