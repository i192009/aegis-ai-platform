# Security model

## Trust boundaries

The public HTTP boundary authenticates every business endpoint. Operational endpoints are intentionally unauthenticated for Kubernetes probes and Prometheus scraping and must be protected by cluster/network policy. PostgreSQL, Redis, Kafka, RabbitMQ, and provider networks are private dependencies, not public services.

## Tenant isolation

- The authenticated API key establishes `tenant_id`; request bodies cannot override it.
- Reads and writes include both object ID and tenant ID, preventing cross-tenant direct-object access.
- Idempotency uniqueness is `(tenant_id, idempotency_key)`, so tenants cannot collide.
- Model and provider policy are loaded after authentication and before provider selection.
- Prometheus labels never include tenant, request, user, or correlation IDs.

The integration and memory tests include a negative Tenant A/Tenant B lookup. Production should add PostgreSQL row-level security as defence in depth after operational migration procedures are designed.

## API keys

Keys contain a non-secret indexed prefix and 256 bits of randomness. They are displayed once. PostgreSQL stores only an HMAC-SHA-256 digest keyed by a separate pepper. Verification performs constant-time digest comparison. Keys support scopes, expiry, and revocation.

The pepper belongs in a secret manager, not source control or a ConfigMap. Rotation needs a versioned pepper strategy or planned key regeneration. The development key in Compose is intentionally non-production.

## Data minimisation

Prompts, responses, bearer headers, raw API keys, and provider secrets are excluded from logs and metrics. The platform stores the final response because asynchronous evaluation requires it; a real bank deployment should apply encryption, retention, residency, and access policy based on classification.

PII findings contain masked values only. The deterministic detector is a transparent heuristic and is not represented as complete DLP coverage.

## HTTP protections

- Bounded body and header sizes
- Server read-header, read, write, and idle timeouts
- Strict JSON schema subset
- Panic recovery with stable RFC 9457-style errors
- Opaque request/correlation identifiers
- Cancellation propagation on disconnect
- Bounded stream accumulation and direct backpressure
- No provider failover once output has been delivered

Ingress must disable response buffering for SSE and enforce TLS. HTTP/1 request parsing and smuggling controls are delegated to maintained Go and ingress implementations; inconsistent proxy chains should be avoided.

## Workload security

The container runs as UID/GID 65532 in a scratch image. Kubernetes sets non-root enforcement, read-only root filesystems, dropped capabilities, disabled privilege escalation, RuntimeDefault seccomp, no service-account token mounting, resource limits, and default-deny network policy.

Committed Kubernetes files contain only secret references. `secret.example.yaml` is a creation template and must never be applied with its example values.

## Production recommendations not implemented

- Institutional OIDC/JWT issuer validation and signing-key rotation
- Mutual TLS or workload identity between services
- Managed KMS/HSM-backed secret lifecycle
- Database row-level security and per-service database roles
- Egress proxy and provider certificate pinning policy where appropriate
- WAF/DDoS protection, central policy engine, and enterprise DLP
- Signed images, SBOM attestation, admission policy, and supply-chain provenance
- Regional encryption keys, residency enforcement, and legal retention schedules
