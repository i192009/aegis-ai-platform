# Validation record

Validated: 2026-07-13

## Passed in the delivery workspace

- `gofmt` and `go mod tidy`
- `go vet ./...`
- `staticcheck ./...`
- `go test ./...`
- `go test -race ./...`
- `go build ./...`
- `govulncheck ./...` with no known vulnerabilities reported
- Integration suite compilation with the `integration` build tag
- YAML, JSON, shell, and k6 JavaScript syntax checks
- `docker compose config --quiet`
- `helm lint` and `helm template`, including parsing every rendered object as YAML
- Gateway memory-mode smoke test covering readiness, authentication, completion,
  idempotent replay, conflicting payload rejection, SSE completion, metrics, and
  graceful termination
- Archive integrity verification after packaging

## Environment-limited checks

The delivery workspace does not expose a Docker daemon. The Testcontainers test
bodies, container image builds/scans, full Docker Compose startup, and live
PostgreSQL/Redis/Kafka/RabbitMQ failure experiments therefore could not execute
here. Those checks are defined in the repository and CI and should be run on a
Docker-capable host before treating a specific image set as release-qualified.

The tagged Testcontainers packages were compiled in this workspace to catch API,
type, and build errors despite that runtime limitation.

## Reproduce

```bash
make format-check
go mod tidy
go vet ./...
staticcheck ./...
go test ./...
go test -race ./...
go build ./...
govulncheck ./...
go test -tags=integration -run '^$' ./tests/integration/...
docker compose config --quiet
helm lint deployments/helm/aegis-ai
helm template aegis deployments/helm/aegis-ai --namespace aegis-ai
```

On a Docker-capable host, also run:

```bash
go test -tags=integration -count=1 ./tests/integration/...
docker compose up --build -d
docker compose ps
```
