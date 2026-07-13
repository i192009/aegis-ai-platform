SHELL := /bin/sh
GO ?= go
SERVICE ?= gateway
VERSION ?= dev
COMMIT ?= $$(git rev-parse --short HEAD 2>/dev/null || printf unknown)
BUILD_TIME ?= $$(date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X github.com/i192009/aegis-ai-platform/internal/version.Version=$(VERSION) -X github.com/i192009/aegis-ai-platform/internal/version.Commit=$(COMMIT) -X github.com/i192009/aegis-ai-platform/internal/version.BuildTime=$(BUILD_TIME)

.PHONY: bootstrap generate format format-check lint test test-race test-integration build compose-up compose-down compose-config migrate-up migrate-down seed run-gateway load-test helm-lint helm-template clean

bootstrap:
	$(GO) mod download
	$(GO) mod tidy

generate:
	$(GO) generate ./...

format:
	gofmt -w $$(find . -name '*.go' -type f)

format-check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

lint: format-check
	$(GO) vet ./...

test:
	$(GO) test -coverprofile=coverage.out ./...

test-race:
	$(GO) test -race ./...

test-integration:
	$(GO) test -tags=integration -count=1 ./tests/integration/...

build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(SERVICE) ./cmd/$(SERVICE)

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down --remove-orphans

compose-config:
	docker compose config --quiet

migrate-up:
	docker compose run --rm migrate

migrate-down:
	docker compose run --rm migrate -path=/migrations -database=postgres://aegis:aegis@postgres:5432/aegis?sslmode=disable down 1

seed:
	$(GO) run ./cmd/admin-cli seed

run-gateway:
	$(GO) run ./cmd/gateway

load-test:
	k6 run tests/load/chat.js

helm-lint:
	helm lint deployments/helm/aegis-ai

helm-template:
	helm template aegis deployments/helm/aegis-ai --namespace aegis-ai

clean:
	rm -rf bin build coverage coverage.out
