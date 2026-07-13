# Risks and simplifications

| Area | Current decision | Risk or follow-up |
| --- | --- | --- |
| Repository breadth | Ordered phases with explicit status | Later phases must not be represented as implemented early |
| SQL | Hand-written migrations and repositories | More review effort, but transaction behavior stays visible |
| Money | Integer micro-USD | Currency conversion and tax are outside the reference scope |
| API keys | Planned SHA-256 keyed hashes plus random secrets | Production should use managed secret rotation and HSM/KMS controls where required |
| JWT | Deferred until API-key path is correct | Production issuer discovery and key rotation require an institutional IdP |
| Providers | Deterministic mock plus OpenAI-compatible adapter | Token counts and error taxonomies differ between real vendors |
| Streaming | SSE, no provider failover after first token | Partial responses require explicit client-visible status and accounting |
| Kafka order | Tenant ID partition key | Hot tenants may require aggregate sharding with weaker total tenant order |
| Redis failure | Policy must be configurable | Fail-open harms limits; fail-closed harms availability |
| Multi-region | Single writer region initially | Residency and active-active conflict design are production recommendations |
| Observability | No prompt/response logging | Debugging content failures needs separately authorised, redacted workflows |
| Local infrastructure | Single-node Compose services | Demonstrates flows but not broker/database high availability |
