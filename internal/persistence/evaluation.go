package persistence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/i192009/aegis-ai-platform/internal/evaluation"
	"github.com/i192009/aegis-ai-platform/internal/events"
)

// EvaluationJob is a tenant-owned executable evaluation request.
type EvaluationJob struct {
	ID                string          `json:"id"`
	TenantID          string          `json:"-"`
	RequestID         string          `json:"request_id"`
	IdempotencyKey    string          `json:"-"`
	CanonicalHash     []byte          `json:"-"`
	JobType           string          `json:"job_type"`
	Status            string          `json:"status"`
	AttemptCount      int             `json:"attempt_count"`
	CorrelationID     string          `json:"correlation_id"`
	Parameters        json.RawMessage `json:"parameters"`
	RequestedAt       time.Time       `json:"requested_at"`
	StartedAt         *time.Time      `json:"started_at,omitempty"`
	CompletedAt       *time.Time      `json:"completed_at,omitempty"`
	LastErrorCategory string          `json:"last_error_category,omitempty"`
	Result            json.RawMessage `json:"result,omitempty"`
}

// CreateEvaluationParams establishes tenant idempotency before broker publication.
type CreateEvaluationParams struct {
	TenantID       string
	RequestID      string
	IdempotencyKey string
	CanonicalHash  []byte
	JobType        string
	CorrelationID  string
	Parameters     json.RawMessage
}

func (postgres *Postgres) CreateOrGetEvaluation(ctx context.Context, params CreateEvaluationParams) (EvaluationJob, bool, error) {
	var id string
	err := postgres.pool.QueryRow(ctx, `
INSERT INTO evaluation_jobs (tenant_id,request_id,idempotency_key,canonical_request_hash,job_type,correlation_id,parameters)
SELECT $1,r.id,$3,$4,$5,$6,$7 FROM ai_requests r
WHERE r.id=$2 AND r.tenant_id=$1 AND r.status='COMPLETED'
ON CONFLICT (tenant_id,idempotency_key) DO NOTHING RETURNING id::text`, params.TenantID, params.RequestID, params.IdempotencyKey, params.CanonicalHash, params.JobType, params.CorrelationID, params.Parameters).Scan(&id)
	created := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return EvaluationJob{}, false, fmt.Errorf("create evaluation job: %w", err)
	}
	if !created {
		err = postgres.pool.QueryRow(ctx, `SELECT id::text FROM evaluation_jobs WHERE tenant_id=$1 AND idempotency_key=$2`, params.TenantID, params.IdempotencyKey).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return EvaluationJob{}, false, ErrNotFound
		}
		if err != nil {
			return EvaluationJob{}, false, fmt.Errorf("load existing evaluation job: %w", err)
		}
	}
	job, err := postgres.GetEvaluation(ctx, params.TenantID, id)
	if err != nil {
		return EvaluationJob{}, false, err
	}
	if !created && !bytes.Equal(job.CanonicalHash, params.CanonicalHash) {
		return EvaluationJob{}, false, ErrConflict
	}
	return job, created, nil
}

func (postgres *Postgres) GetEvaluation(ctx context.Context, tenantID, evaluationID string) (EvaluationJob, error) {
	var job EvaluationJob
	err := postgres.pool.QueryRow(ctx, `
SELECT j.id::text,j.tenant_id::text,j.request_id::text,j.idempotency_key,j.canonical_request_hash,
 j.job_type,j.status::text,j.attempt_count,j.correlation_id,j.parameters,j.requested_at,j.started_at,
 j.completed_at,COALESCE(j.last_error_category,''),COALESCE(r.result_payload,'{}')
FROM evaluation_jobs j LEFT JOIN evaluation_results r ON r.evaluation_job_id=j.id
WHERE j.tenant_id=$1 AND j.id=$2`, tenantID, evaluationID).Scan(
		&job.ID, &job.TenantID, &job.RequestID, &job.IdempotencyKey, &job.CanonicalHash,
		&job.JobType, &job.Status, &job.AttemptCount, &job.CorrelationID, &job.Parameters, &job.RequestedAt,
		&job.StartedAt, &job.CompletedAt, &job.LastErrorCategory, &job.Result,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return EvaluationJob{}, ErrNotFound
	}
	if err != nil {
		return EvaluationJob{}, fmt.Errorf("get evaluation job: %w", err)
	}
	return job, nil
}

func (postgres *Postgres) ListEvaluations(ctx context.Context, tenantID string, limit, offset int) ([]EvaluationJob, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := postgres.pool.Query(ctx, `
SELECT id::text,tenant_id::text,request_id::text,idempotency_key,canonical_request_hash,job_type,
 status::text,attempt_count,correlation_id,parameters,requested_at,started_at,completed_at,COALESCE(last_error_category,'')
FROM evaluation_jobs WHERE tenant_id=$1 ORDER BY requested_at DESC LIMIT $2 OFFSET $3`, tenantID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list evaluation jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]EvaluationJob, 0, limit)
	for rows.Next() {
		var job EvaluationJob
		if err := rows.Scan(&job.ID, &job.TenantID, &job.RequestID, &job.IdempotencyKey, &job.CanonicalHash, &job.JobType, &job.Status, &job.AttemptCount, &job.CorrelationID, &job.Parameters, &job.RequestedAt, &job.StartedAt, &job.CompletedAt, &job.LastErrorCategory); err != nil {
			return nil, fmt.Errorf("scan evaluation job: %w", err)
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (postgres *Postgres) MarkEvaluationQueued(ctx context.Context, tenantID, jobID string) error {
	_, err := postgres.pool.Exec(ctx, `UPDATE evaluation_jobs SET status='QUEUED' WHERE tenant_id=$1 AND id=$2 AND status IN ('PENDING','FAILED')`, tenantID, jobID)
	return err
}

func (postgres *Postgres) MarkEvaluationRunning(ctx context.Context, tenantID, jobID string) error {
	_, err := postgres.pool.Exec(ctx, `UPDATE evaluation_jobs SET status='RUNNING',started_at=COALESCE(started_at,now()),attempt_count=attempt_count+1 WHERE tenant_id=$1 AND id=$2 AND status IN ('PENDING','QUEUED','RUNNING','FAILED')`, tenantID, jobID)
	return err
}

// LoadEvaluationInput reads the completed assistant response without logging it.
func (postgres *Postgres) LoadEvaluationInput(ctx context.Context, tenantID, jobID string) (evaluation.Input, EvaluationJob, error) {
	job, err := postgres.GetEvaluation(ctx, tenantID, jobID)
	if err != nil {
		return evaluation.Input{}, EvaluationJob{}, err
	}
	var responseBytes []byte
	var promptTokens, completionTokens, cost, totalLatency, providerLatency int64
	err = postgres.pool.QueryRow(ctx, `
SELECT r.response_body,COALESCE(r.prompt_tokens,0),COALESCE(r.completion_tokens,0),COALESCE(r.actual_cost_micro_usd,0),
 (EXTRACT(EPOCH FROM (r.completed_at-r.created_at))*1000)::bigint,
 COALESCE((SELECT MAX(latency_ms) FROM provider_attempts WHERE request_id=r.id AND status='SUCCEEDED'),0)
FROM ai_requests r WHERE r.tenant_id=$1 AND r.id=$2 AND r.status='COMPLETED'`, tenantID, job.RequestID).Scan(&responseBytes, &promptTokens, &completionTokens, &cost, &totalLatency, &providerLatency)
	if err != nil {
		return evaluation.Input{}, EvaluationJob{}, fmt.Errorf("load evaluation source: %w", err)
	}
	var response struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(responseBytes, &response); err != nil || len(response.Choices) == 0 {
		return evaluation.Input{}, EvaluationJob{}, errors.New("stored response is malformed")
	}
	input := evaluation.Input{Response: response.Choices[0].Message.Content, PromptTokens: promptTokens, CompletionTokens: completionTokens, CostMicroUSD: cost, TotalLatency: time.Duration(totalLatency) * time.Millisecond, ProviderLatency: time.Duration(providerLatency) * time.Millisecond}
	if len(job.Parameters) > 0 {
		var parameters struct {
			RequestedFormat string   `json:"requested_format"`
			BlockedTerms    []string `json:"blocked_terms"`
			MaxProviderMS   int64    `json:"max_provider_latency_ms"`
			MaxTotalMS      int64    `json:"max_total_latency_ms"`
			MaxTokens       int64    `json:"max_tokens"`
			MaxCostMicroUSD int64    `json:"max_cost_micro_usd"`
		}
		_ = json.Unmarshal(job.Parameters, &parameters)
		input.RequestedFormat, input.BlockedTerms = parameters.RequestedFormat, parameters.BlockedTerms
		input.MaxProviderLatency, input.MaxTotalLatency = time.Duration(parameters.MaxProviderMS)*time.Millisecond, time.Duration(parameters.MaxTotalMS)*time.Millisecond
		input.MaxTokens, input.MaxCostMicroUSD = parameters.MaxTokens, parameters.MaxCostMicroUSD
	}
	return input, job, nil
}

// LoadInput adapts the richer API for the RabbitMQ worker boundary.
func (postgres *Postgres) LoadInput(ctx context.Context, tenantID, jobID string) (evaluation.Input, error) {
	input, _, err := postgres.LoadEvaluationInput(ctx, tenantID, jobID)
	return input, err
}

// CompleteEvaluation persists one result per job/execution before the worker acknowledges.
func (postgres *Postgres) CompleteEvaluation(ctx context.Context, tenantID, jobID, executionID string, results []evaluation.Result) error {
	encoded, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("marshal evaluation results: %w", err)
	}
	score := 0
	duration := int64(0)
	status := "pass"
	for _, result := range results {
		score += result.ScoreMilli
		duration += result.Duration.Milliseconds()
		if result.Status == "fail" {
			status = "fail"
		}
	}
	if len(results) > 0 {
		score /= len(results)
	}
	tx, err := postgres.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `
INSERT INTO evaluation_results (evaluation_job_id,tenant_id,execution_id,evaluator_version,score_milli,outcome,findings,duration_ms,result_payload)
VALUES ($1,$2,$3,'suite-1.0.0',$4,$5,'[]',$6,$7)
ON CONFLICT (evaluation_job_id) DO NOTHING`, jobID, tenantID, executionID, score, status, duration, encoded)
	if err != nil {
		return fmt.Errorf("persist evaluation result: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE evaluation_jobs SET status='COMPLETED',completed_at=now(),last_error_category=NULL WHERE id=$1 AND tenant_id=$2`, jobID, tenantID)
	if err != nil {
		return fmt.Errorf("complete evaluation job: %w", err)
	}
	if err := insertOutbox(ctx, tx, events.EvaluationComplete, jobID, tenantID, jobID, map[string]any{"evaluation_id": jobID, "status": status, "score_milli": score}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (postgres *Postgres) FailEvaluation(ctx context.Context, tenantID, jobID, category string, permanent bool) error {
	status := "FAILED"
	if permanent {
		status = "DEAD_LETTERED"
	}
	tx, err := postgres.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var correlationID string
	err = tx.QueryRow(ctx, `UPDATE evaluation_jobs SET status=$1,last_error_category=$2,completed_at=CASE WHEN $3 THEN now() ELSE completed_at END WHERE id=$4 AND tenant_id=$5 RETURNING correlation_id`, status, category, permanent, jobID, tenantID).Scan(&correlationID)
	if err != nil {
		return err
	}
	if permanent {
		if err := insertOutbox(ctx, tx, events.EvaluationFailed, jobID, tenantID, correlationID, map[string]any{"evaluation_id": jobID, "error_category": category}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
