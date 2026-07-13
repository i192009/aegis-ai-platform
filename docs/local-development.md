# Local development

## Tooling

- Go 1.26.5
- Docker with Compose v2
- Make 4.x or equivalent direct commands
- Helm 3.19 or compatible
- k6 for load tests
- WSL2 on Windows is recommended

## Go-only development

The gateway automatically uses a race-safe memory repository and in-process deterministic provider when database/provider endpoints are empty:

```bash
export AEGIS_DEV_MEMORY=true
export AEGIS_API_KEY_PEPPER='development-only-pepper-change-me-32-bytes'
go run ./cmd/gateway
```

Use the development API key from `.env.example`. This mode validates HTTP, authentication, routing, retries, streaming, and state logic but does not exercise PostgreSQL, Redis, or brokers.

## Full stack

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
curl --fail http://localhost:8080/health/ready
```

Startup is dependency-based: PostgreSQL health → migrations → seed, plus healthy Redis/Kafka/RabbitMQ/mock provider before application services. No fixed sleeps are used.

Stop while retaining PostgreSQL data:

```bash
docker compose down
```

Remove local data explicitly:

```bash
docker compose down --volumes
```

## Tests

```bash
make format-check
make lint
make test
make test-race
make test-integration
```

Integration tests are build-tagged and use Testcontainers for PostgreSQL, Redis, and RabbitMQ. A Docker daemon is required. The ordinary unit/race suite does not require external services.

## Service ports

| Service | Port |
| --- | ---: |
| Gateway | 8080 |
| Outbox relay operations | 8081 |
| Audit consumer operations | 8082 |
| Evaluation API | 8083 |
| Mock provider | 8084 |
| Evaluation worker operations | 8085 |
| Prometheus | 9090 |
| Grafana | 3000 |
| RabbitMQ management | 15672 |

## Windows alternatives

Run Make targets inside WSL2, or issue `go test ./...`, `go test -race ./...`, `docker compose up --build -d`, and `helm lint deployments/helm/aegis-ai` directly from PowerShell with the tools installed.
