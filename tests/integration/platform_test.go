//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/i192009/aegis-ai-platform/internal/budget"
	"github.com/i192009/aegis-ai-platform/internal/events"
	"github.com/i192009/aegis-ai-platform/internal/outbox"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/ratelimit"
	"github.com/i192009/aegis-ai-platform/internal/request"
)

func TestPostgresConcurrentIdempotency(t *testing.T) {
	ctx := context.Background()
	container, err := postgrescontainer.Run(ctx, "postgres:17.6-alpine3.22",
		postgrescontainer.WithDatabase("aegis"), postgrescontainer.WithUsername("aegis"), postgrescontainer.WithPassword("aegis"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	databaseURL, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	store, err := persistence.NewPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	applyMigrations(t, ctx, store)
	seedIntegration(t, ctx, store)

	params := persistence.CreateRequestParams{TenantID: "00000000-0000-0000-0000-000000000001", APIKeyID: "00000000-0000-0000-0000-000000000003", IdempotencyKey: "concurrent-key", CanonicalHash: []byte("same-hash"), Input: request.ChatInput{Model: "aegis-small"}, CorrelationID: "test"}
	var created atomic.Int64
	var waitGroup sync.WaitGroup
	for range 100 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			_, wasCreated, err := store.CreateOrGetRequest(ctx, params)
			if err != nil {
				t.Errorf("CreateOrGetRequest() error = %v", err)
			}
			if wasCreated {
				created.Add(1)
			}
		}()
	}
	waitGroup.Wait()
	if created.Load() != 1 {
		t.Fatalf("created count = %d, want 1", created.Load())
	}
	params.CanonicalHash = []byte("different-hash")
	if _, _, err := store.CreateOrGetRequest(ctx, params); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("different payload error = %v", err)
	}

	params.IdempotencyKey = "final-attempt-key"
	params.CanonicalHash = []byte("final-attempt-hash")
	finalRecord, _, err := store.CreateOrGetRequest(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	for _, transition := range [][2]request.State{{request.Received, request.Validated}, {request.Validated, request.Routing}, {request.Routing, request.InProgress}} {
		if err := store.TransitionRequest(ctx, params.TenantID, finalRecord.ID, transition[0], transition[1]); err != nil {
			t.Fatal(err)
		}
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := store.StartAttempt(ctx, persistence.AttemptParams{RequestID: finalRecord.ID, TenantID: params.TenantID, Number: attempt, ProviderName: "mock-primary", Model: "aegis-small"}); err != nil {
			t.Fatal(err)
		}
	}
	var finalSuccess atomic.Int64
	waitGroup = sync.WaitGroup{}
	for attempt := 1; attempt <= 2; attempt++ {
		waitGroup.Add(1)
		go func(number int) {
			defer waitGroup.Done()
			response := request.ChatResponse{ID: fmt.Sprintf("response-%d", number), Model: "aegis-small", Usage: request.Usage{PromptTokens: 10, CompletionTokens: 10, TotalTokens: 20}}
			err := store.CompleteRequest(ctx, persistence.CompletionParams{RequestID: finalRecord.ID, TenantID: params.TenantID, AttemptNumber: number, Response: response, StartedAt: time.Now(), CorrelationID: "integration", CostMicroUSD: 1})
			if err == nil {
				finalSuccess.Add(1)
			} else if !errors.Is(err, persistence.ErrAlreadyFinal) {
				t.Errorf("unexpected finalisation error: %v", err)
			}
		}(attempt)
	}
	waitGroup.Wait()
	if finalSuccess.Load() != 1 {
		t.Fatalf("final success count = %d, want 1", finalSuccess.Load())
	}

	repository := outbox.NewRepository(store.Pool())
	var expectedOutbox int
	if err := store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&expectedOutbox); err != nil {
		t.Fatal(err)
	}
	var claims [2][]outbox.Row
	waitGroup = sync.WaitGroup{}
	for index := range 2 {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			claimed, err := repository.Claim(ctx, fmt.Sprintf("worker-%d", worker), 10, time.Minute)
			if err != nil {
				t.Errorf("claim error: %v", err)
			}
			claims[worker] = claimed
		}(index)
	}
	waitGroup.Wait()
	seen := map[string]struct{}{}
	for _, rows := range claims {
		for _, row := range rows {
			if _, duplicate := seen[row.ID]; duplicate {
				t.Fatalf("outbox row %s claimed twice", row.ID)
			}
			seen[row.ID] = struct{}{}
		}
	}
	if len(seen) != expectedOutbox {
		t.Fatalf("claimed outbox rows = %d, want %d", len(seen), expectedOutbox)
	}

	usageEvent := events.Envelope{EventID: "00000000-0000-0000-0000-000000000099", EventType: events.UsageRecorded, Version: 1, AggregateID: finalRecord.ID, TenantID: params.TenantID, Timestamp: time.Now(), CorrelationID: "integration", Payload: json.RawMessage(fmt.Sprintf(`{"request_id":%q,"prompt_tokens":10,"completion_tokens":10,"cost_micro_usd":1}`, finalRecord.ID))}
	firstProcessed, err := store.ProcessKafkaEvent(ctx, "integration-consumer", usageEvent)
	if err != nil || !firstProcessed {
		t.Fatalf("first event processing = %v, %v", firstProcessed, err)
	}
	secondProcessed, err := store.ProcessKafkaEvent(ctx, "integration-consumer", usageEvent)
	if err != nil || secondProcessed {
		t.Fatalf("duplicate event processing = %v, %v", secondProcessed, err)
	}
	var usageRows int
	if err := store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM usage_ledger WHERE event_id=$1`, usageEvent.EventID).Scan(&usageRows); err != nil || usageRows != 1 {
		t.Fatalf("usage rows = %d, %v", usageRows, err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE tenant_limits SET monthly_budget_micro_usd=100 WHERE tenant_id=$1`, params.TenantID); err != nil {
		t.Fatal(err)
	}
	var reservations atomic.Int64
	waitGroup = sync.WaitGroup{}
	for index := range 10 {
		budgetParams := params
		budgetParams.IdempotencyKey = fmt.Sprintf("budget-%d", index)
		budgetParams.CanonicalHash = []byte(fmt.Sprintf("budget-hash-%d", index))
		budgetRecord, _, err := store.CreateOrGetRequest(ctx, budgetParams)
		if err != nil {
			t.Fatal(err)
		}
		waitGroup.Add(1)
		go func(requestID string) {
			defer waitGroup.Done()
			if err := store.ReserveBudget(ctx, params.TenantID, requestID, 30, time.Now()); err == nil {
				reservations.Add(1)
			} else if !errors.Is(err, budget.ErrExceeded) {
				t.Errorf("unexpected budget error: %v", err)
			}
		}(budgetRecord.ID)
	}
	waitGroup.Wait()
	if reservations.Load() != 3 {
		t.Fatalf("concurrent budget reservations = %d, want 3", reservations.Load())
	}

	if _, err := store.GetRequest(ctx, "00000000-0000-0000-0000-000000000002", finalRecord.ID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("cross-tenant request read error = %v", err)
	}

	if !bytes.Equal(params.CanonicalHash, []byte("final-attempt-hash")) {
		t.Fatal("test canonical hash mutated")
	}
}

func TestRedisLimitsAcrossInstances(t *testing.T) {
	ctx := context.Background()
	container, err := testcontainers.Run(ctx, "redis:8.2.1-alpine3.22", testcontainers.WithExposedPorts("6379/tcp"), testcontainers.WithWaitStrategy(wait.ForLog("Ready to accept connections").WithStartupTimeout(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	firstClient := redis.NewClient(&redis.Options{Addr: endpoint})
	secondClient := redis.NewClient(&redis.Options{Addr: endpoint})
	t.Cleanup(func() { _ = firstClient.Close(); _ = secondClient.Close() })
	first := ratelimit.New(firstClient, "integration")
	second := ratelimit.New(secondClient, "integration")
	limits := ratelimit.Limits{RequestsPerMinute: 100, TokensPerMinute: 1000, MaxConcurrent: 1}
	decision, permit, err := first.Allow(ctx, "tenant", limits, 10, time.Minute)
	if err != nil || !decision.Allowed {
		t.Fatalf("first admission = %+v, %v", decision, err)
	}
	decision, _, err = second.Allow(ctx, "tenant", limits, 10, time.Minute)
	if err != nil || decision.Allowed || decision.Reason != "maximum_concurrency" {
		t.Fatalf("second admission = %+v, %v", decision, err)
	}
	if err := permit.Release(ctx); err != nil {
		t.Fatal(err)
	}
	decision, permit, err = second.Allow(ctx, "tenant", limits, 10, time.Minute)
	if err != nil || !decision.Allowed {
		t.Fatalf("admission after release = %+v, %v", decision, err)
	}
	_ = permit.Release(ctx)
}

func applyMigrations(t *testing.T, ctx context.Context, store *persistence.Postgres) {
	t.Helper()
	for _, name := range []string{"000001_initial.up.sql", "000002_evaluation_parameters.up.sql"} {
		path := filepath.Join("..", "..", "migrations", name)
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Pool().Exec(ctx, string(contents)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
}

func seedIntegration(t *testing.T, ctx context.Context, store *persistence.Postgres) {
	t.Helper()
	statements := []string{
		`INSERT INTO tenants (id,name,slug) VALUES ('00000000-0000-0000-0000-000000000001','Test','test')`,
		`INSERT INTO tenants (id,name,slug) VALUES ('00000000-0000-0000-0000-000000000002','Other','other')`,
		`INSERT INTO api_keys (id,tenant_id,name,key_prefix,key_hash,scopes) VALUES ('00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000001','test','testprefix',decode('00','hex'),ARRAY['*'])`,
		`INSERT INTO models (id,public_name) VALUES ('00000000-0000-0000-0000-000000000010','aegis-small')`,
		`INSERT INTO tenant_model_policies (tenant_id,model_id) VALUES ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000010')`,
		`INSERT INTO providers (id,name,endpoint) VALUES ('00000000-0000-0000-0000-000000000020','mock-primary','http://mock')`,
		`INSERT INTO provider_models (provider_id,model_id,provider_model_name) VALUES ('00000000-0000-0000-0000-000000000020','00000000-0000-0000-0000-000000000010','aegis-small')`,
		`INSERT INTO tenant_limits (tenant_id,monthly_budget_micro_usd) VALUES ('00000000-0000-0000-0000-000000000001',1000000)`,
	}
	for index, statement := range statements {
		if _, err := store.Pool().Exec(ctx, statement); err != nil {
			t.Fatal(fmt.Errorf("seed statement %d: %w", index, err))
		}
	}
}
