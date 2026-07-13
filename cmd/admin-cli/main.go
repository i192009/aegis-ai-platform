package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/i192009/aegis-ai-platform/internal/auth"
	"github.com/i192009/aegis-ai-platform/internal/config"
	"github.com/i192009/aegis-ai-platform/internal/persistence"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: admin-cli <seed|create-tenant|create-api-key|revoke-api-key|set-budget|inspect-request> [flags]")
	}
	cfg, err := config.Load("admin-cli")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	postgres, err := persistence.NewPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer postgres.Close()
	switch args[0] {
	case "seed":
		return seed(ctx, postgres, cfg)
	case "create-tenant":
		return createTenant(ctx, postgres, args[1:])
	case "create-api-key":
		return createAPIKey(ctx, postgres, cfg, args[1:])
	case "revoke-api-key":
		return revokeAPIKey(ctx, postgres, args[1:])
	case "set-budget":
		return setBudget(ctx, postgres, args[1:])
	case "inspect-request":
		return inspectRequest(ctx, postgres, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func seed(ctx context.Context, postgres *persistence.Postgres, cfg config.Config) error {
	prefix, err := auth.ParsePrefix(cfg.DevAPIKey)
	if err != nil {
		return err
	}
	tx, err := postgres.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO tenants (id,name,slug,data_classification) VALUES ('00000000-0000-0000-0000-000000000001','Aegis Development','aegis-development','INTERNAL') ON CONFLICT (id) DO NOTHING`, nil},
		{`INSERT INTO models (id,public_name,description) VALUES ('00000000-0000-0000-0000-000000000010','aegis-small','Deterministic development model'),('00000000-0000-0000-0000-000000000011','aegis-medium','Deterministic development model') ON CONFLICT (id) DO NOTHING`, nil},
		{`INSERT INTO providers (id,name,endpoint,weight,priority,max_concurrency,timeout_ms,data_classifications) VALUES ('00000000-0000-0000-0000-000000000020',$1,$2,1,10,50,30000,ARRAY['PUBLIC','INTERNAL','CONFIDENTIAL']) ON CONFLICT (id) DO UPDATE SET endpoint=EXCLUDED.endpoint`, []any{cfg.ProviderName, cfg.ProviderEndpoint}},
		{`INSERT INTO provider_models (provider_id,model_id,provider_model_name,input_cost_micro_usd_per_million_tokens,output_cost_micro_usd_per_million_tokens) VALUES ('00000000-0000-0000-0000-000000000020','00000000-0000-0000-0000-000000000010','aegis-small',1000,2000),('00000000-0000-0000-0000-000000000020','00000000-0000-0000-0000-000000000011','aegis-medium',2000,4000) ON CONFLICT DO NOTHING`, nil},
		{`INSERT INTO tenant_model_policies (tenant_id,model_id) VALUES ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000010'),('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000011') ON CONFLICT DO NOTHING`, nil},
		{`INSERT INTO tenant_provider_policies (tenant_id,provider_id) VALUES ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000020') ON CONFLICT DO NOTHING`, nil},
		{`INSERT INTO tenant_limits (tenant_id,requests_per_minute,tokens_per_minute,max_concurrent_requests,monthly_budget_micro_usd,daily_budget_micro_usd) VALUES ('00000000-0000-0000-0000-000000000001',600,1000000,100,100000000,10000000) ON CONFLICT (tenant_id) DO NOTHING`, nil},
		{`INSERT INTO api_keys (id,tenant_id,name,key_prefix,key_hash,scopes) VALUES ('00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000001','development',$1,$2,ARRAY['*']) ON CONFLICT (id) DO UPDATE SET key_hash=EXCLUDED.key_hash,key_prefix=EXCLUDED.key_prefix`, []any{prefix, auth.Hash([]byte(cfg.APIKeyPepper), cfg.DevAPIKey)}},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.sql, statement.args...); err != nil {
			return fmt.Errorf("seed database: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return printJSON(map[string]any{"tenant_slug": "aegis-development", "development_api_key": cfg.DevAPIKey, "warning": "development credential only; rotate outside local environments"})
}

func createTenant(ctx context.Context, postgres *persistence.Postgres, args []string) error {
	set := flag.NewFlagSet("create-tenant", flag.ContinueOnError)
	name := set.String("name", "", "tenant display name")
	slug := set.String("slug", "", "tenant slug")
	if err := set.Parse(args); err != nil {
		return err
	}
	if *name == "" || *slug == "" {
		return errors.New("--name and --slug are required")
	}
	var id string
	err := postgres.Pool().QueryRow(ctx, `WITH tenant AS (INSERT INTO tenants (name,slug) VALUES ($1,$2) RETURNING id) INSERT INTO tenant_limits (tenant_id) SELECT id FROM tenant RETURNING tenant_id::text`, *name, *slug).Scan(&id)
	if err != nil {
		return err
	}
	return printJSON(map[string]string{"tenant_id": id, "slug": *slug})
}

func createAPIKey(ctx context.Context, postgres *persistence.Postgres, cfg config.Config, args []string) error {
	set := flag.NewFlagSet("create-api-key", flag.ContinueOnError)
	tenant := set.String("tenant", "", "tenant slug")
	name := set.String("name", "", "key name")
	scopesValue := set.String("scopes", "chat:write,requests:read,requests:cancel,evaluations:write,evaluations:read", "comma-separated scopes")
	if err := set.Parse(args); err != nil {
		return err
	}
	if *tenant == "" || *name == "" {
		return errors.New("--tenant and --name are required")
	}
	plain, prefix, hash, err := auth.Generate([]byte(cfg.APIKeyPepper))
	if err != nil {
		return err
	}
	scopes := splitCSV(*scopesValue)
	var id string
	err = postgres.Pool().QueryRow(ctx, `INSERT INTO api_keys (tenant_id,name,key_prefix,key_hash,scopes) SELECT id,$2,$3,$4,$5 FROM tenants WHERE slug=$1 RETURNING id::text`, *tenant, *name, prefix, hash, scopes).Scan(&id)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"api_key_id": id, "api_key": plain, "shown_once": true, "scopes": scopes})
}

func revokeAPIKey(ctx context.Context, postgres *persistence.Postgres, args []string) error {
	set := flag.NewFlagSet("revoke-api-key", flag.ContinueOnError)
	id := set.String("id", "", "API key ID")
	if err := set.Parse(args); err != nil {
		return err
	}
	tag, err := postgres.Pool().Exec(ctx, `UPDATE api_keys SET revoked_at=now() WHERE id=$1 AND revoked_at IS NULL`, *id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return persistence.ErrNotFound
	}
	return printJSON(map[string]any{"api_key_id": *id, "revoked": true})
}

func setBudget(ctx context.Context, postgres *persistence.Postgres, args []string) error {
	set := flag.NewFlagSet("set-budget", flag.ContinueOnError)
	tenant := set.String("tenant", "", "tenant slug")
	monthly := set.Int64("monthly-micro-usd", 0, "monthly budget in micro-USD")
	daily := set.Int64("daily-micro-usd", 0, "daily budget in micro-USD")
	if err := set.Parse(args); err != nil {
		return err
	}
	_, err := postgres.Pool().Exec(ctx, `UPDATE tenant_limits SET monthly_budget_micro_usd=$2,daily_budget_micro_usd=$3,updated_at=now() FROM tenants WHERE tenant_limits.tenant_id=tenants.id AND tenants.slug=$1`, *tenant, *monthly, *daily)
	return err
}

func inspectRequest(ctx context.Context, postgres *persistence.Postgres, args []string) error {
	set := flag.NewFlagSet("inspect-request", flag.ContinueOnError)
	tenant := set.String("tenant-id", "", "tenant UUID")
	id := set.String("id", "", "request UUID")
	if err := set.Parse(args); err != nil {
		return err
	}
	record, err := postgres.GetRequest(ctx, *tenant, *id)
	if err != nil {
		return err
	}
	return printJSON(record)
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
