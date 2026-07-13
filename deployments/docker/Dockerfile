# syntax=docker/dockerfile:1.7
ARG GO_IMAGE=golang:1.26.5-alpine3.23
FROM ${GO_IMAGE} AS build
WORKDIR /src
ARG SERVICE=gateway
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X github.com/i192009/aegis-ai-platform/internal/version.Version=${VERSION} -X github.com/i192009/aegis-ai-platform/internal/version.Commit=${COMMIT} -X github.com/i192009/aegis-ai-platform/internal/version.BuildTime=${BUILD_TIME}" \
    -o /out/service ./cmd/${SERVICE}
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck

FROM scratch
ARG SERVICE=gateway
LABEL org.opencontainers.image.title="AegisAI ${SERVICE}" \
      org.opencontainers.image.source="https://github.com/i192009/aegis-ai-platform"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/service /service
COPY --from=build /out/healthcheck /healthcheck
USER 65532:65532
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/healthcheck"]
ENTRYPOINT ["/service"]
