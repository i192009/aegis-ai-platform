# Runbook: provider outage

1. Confirm provider error category, latency, health, and circuit-state metrics.
2. Check whether failures occur before or after the first streamed token.
3. Disable the provider in configuration or tenant policy if errors are sustained.
4. Confirm alternative providers support the model and data classification and have capacity.
5. Watch retry rate to ensure bounded retries are not amplifying the incident.
6. Do not replay partially streamed requests automatically; clients must decide whether to start a new logical request.
7. Re-enable with limited half-open probes, then verify latency and error recovery.
