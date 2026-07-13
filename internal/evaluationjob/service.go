// Package evaluationjob coordinates durable job records and RabbitMQ publication.
package evaluationjob

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/i192009/aegis-ai-platform/internal/persistence"
	"github.com/i192009/aegis-ai-platform/internal/rabbitmq"
)

var ErrNotRetryable = errors.New("evaluation job is not retryable")

// Store is the Evaluation API persistence boundary.
type Store interface {
	CreateOrGetEvaluation(context.Context, persistence.CreateEvaluationParams) (persistence.EvaluationJob, bool, error)
	GetEvaluation(context.Context, string, string) (persistence.EvaluationJob, error)
	ListEvaluations(context.Context, string, int, int) ([]persistence.EvaluationJob, error)
	MarkEvaluationQueued(context.Context, string, string) error
}

// Publisher confirms durable RabbitMQ publication.
type Publisher interface {
	Publish(context.Context, rabbitmq.Job, bool) error
}

// Service prevents broker publication from becoming the source of truth.
type Service struct {
	store     Store
	publisher Publisher
}

func New(store Store, publisher Publisher) *Service {
	return &Service{store: store, publisher: publisher}
}

// SubmitInput is the supported deterministic evaluation request.
type SubmitInput struct {
	RequestID  string         `json:"request_id"`
	JobType    string         `json:"job_type"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

func (service *Service) Submit(ctx context.Context, tenantID, idempotencyKey, correlationID string, input SubmitInput) (persistence.EvaluationJob, bool, error) {
	if input.RequestID == "" || input.JobType == "" {
		return persistence.EvaluationJob{}, false, errors.New("request_id and job_type are required")
	}
	canonical, err := json.Marshal(input)
	if err != nil {
		return persistence.EvaluationJob{}, false, err
	}
	hash := sha256.Sum256(canonical)
	parameters, err := json.Marshal(input.Parameters)
	if err != nil {
		return persistence.EvaluationJob{}, false, err
	}
	job, created, err := service.store.CreateOrGetEvaluation(ctx, persistence.CreateEvaluationParams{TenantID: tenantID, RequestID: input.RequestID, IdempotencyKey: idempotencyKey, CanonicalHash: hash[:], JobType: input.JobType, CorrelationID: correlationID, Parameters: parameters})
	if err != nil || !created {
		return job, created, err
	}
	command := rabbitmq.Job{JobID: job.ID, TenantID: tenantID, ExecutionID: executionID(), JobType: job.JobType, CorrelationID: correlationID}
	if err := service.publisher.Publish(ctx, command, false); err != nil {
		return job, true, fmt.Errorf("publish evaluation command: %w", err)
	}
	if err := service.store.MarkEvaluationQueued(ctx, tenantID, job.ID); err != nil {
		return job, true, fmt.Errorf("mark evaluation queued: %w", err)
	}
	job.Status = "QUEUED"
	return job, true, nil
}

func (service *Service) Get(ctx context.Context, tenantID, id string) (persistence.EvaluationJob, error) {
	return service.store.GetEvaluation(ctx, tenantID, id)
}

func (service *Service) List(ctx context.Context, tenantID string, limit, offset int) ([]persistence.EvaluationJob, error) {
	return service.store.ListEvaluations(ctx, tenantID, limit, offset)
}

func (service *Service) Retry(ctx context.Context, tenantID, id, correlationID string) (persistence.EvaluationJob, error) {
	job, err := service.store.GetEvaluation(ctx, tenantID, id)
	if err != nil {
		return persistence.EvaluationJob{}, err
	}
	if job.Status != "FAILED" && job.Status != "DEAD_LETTERED" && job.Status != "PENDING" {
		return persistence.EvaluationJob{}, ErrNotRetryable
	}
	command := rabbitmq.Job{JobID: job.ID, TenantID: tenantID, ExecutionID: executionID(), JobType: job.JobType, CorrelationID: correlationID}
	if err := service.publisher.Publish(ctx, command, false); err != nil {
		return persistence.EvaluationJob{}, err
	}
	if err := service.store.MarkEvaluationQueued(ctx, tenantID, id); err != nil {
		return persistence.EvaluationJob{}, err
	}
	job.Status = "QUEUED"
	return job, nil
}

func executionID() string {
	value := make([]byte, 16)
	_, _ = rand.Read(value)
	return hex.EncodeToString(value)
}
