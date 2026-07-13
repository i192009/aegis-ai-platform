// Package rabbitmq implements durable evaluation commands, retries, and dead-letter routing.
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/i192009/aegis-ai-platform/internal/evaluation"
)

const (
	MainExchange  = "aegis.evaluation"
	RetryExchange = "aegis.evaluation.retry"
	DeadExchange  = "aegis.evaluation.dlx"
	WorkQueue     = "aegis.evaluation.work"
	RetryQueue    = "aegis.evaluation.retry.5s"
	DeadQueue     = "aegis.evaluation.dead"
)

// Job is the stable executable command. Attempt starts at zero.
type Job struct {
	JobID         string `json:"job_id"`
	TenantID      string `json:"tenant_id"`
	ExecutionID   string `json:"execution_id"`
	JobType       string `json:"job_type"`
	CorrelationID string `json:"correlation_id"`
	Attempt       int    `json:"attempt"`
}

// Manager owns one connection and confirm-mode publishing channel.
type Manager struct {
	connection *amqp.Connection
	publish    *amqp.Channel
	confirm    chan amqp.Confirmation
	mu         sync.Mutex
}

// Connect declares the complete durable topology.
func Connect(url string) (*Manager, error) {
	connection, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 10 * time.Second, Locale: "en_US"})
	if err != nil {
		return nil, fmt.Errorf("connect RabbitMQ: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	manager := &Manager{connection: connection, publish: channel}
	if err := manager.declare(channel); err != nil {
		manager.Close()
		return nil, err
	}
	if err := channel.Confirm(false); err != nil {
		manager.Close()
		return nil, fmt.Errorf("enable RabbitMQ confirms: %w", err)
	}
	manager.confirm = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	return manager, nil
}

func (manager *Manager) declare(channel *amqp.Channel) error {
	for _, exchange := range []string{MainExchange, RetryExchange, DeadExchange} {
		if err := channel.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", exchange, err)
		}
	}
	workArgs := amqp.Table{"x-dead-letter-exchange": DeadExchange, "x-dead-letter-routing-key": "dead"}
	if _, err := channel.QueueDeclare(WorkQueue, true, false, false, false, workArgs); err != nil {
		return fmt.Errorf("declare work queue: %w", err)
	}
	retryArgs := amqp.Table{"x-message-ttl": int32(5000), "x-dead-letter-exchange": MainExchange, "x-dead-letter-routing-key": "evaluate"}
	if _, err := channel.QueueDeclare(RetryQueue, true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("declare retry queue: %w", err)
	}
	if _, err := channel.QueueDeclare(DeadQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead queue: %w", err)
	}
	bindings := []struct{ queue, key, exchange string }{{WorkQueue, "evaluate", MainExchange}, {RetryQueue, "retry", RetryExchange}, {DeadQueue, "dead", DeadExchange}}
	for _, binding := range bindings {
		if err := channel.QueueBind(binding.queue, binding.key, binding.exchange, false, nil); err != nil {
			return fmt.Errorf("bind queue %s: %w", binding.queue, err)
		}
	}
	return nil
}

// Publish waits for broker acknowledgement before returning success.
func (manager *Manager) Publish(ctx context.Context, job Job, retry bool) error {
	ctx, span := otel.Tracer("aegis/rabbitmq").Start(ctx, "rabbitmq.publish")
	defer span.End()
	encoded, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal evaluation job: %w", err)
	}
	exchange, key := MainExchange, "evaluate"
	if retry {
		exchange, key = RetryExchange, "retry"
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	headers := amqp.Table{"attempt": int32(job.Attempt)}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for name, value := range carrier {
		headers[name] = value
	}
	err = manager.publish.PublishWithContext(ctx, exchange, key, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", MessageId: job.ExecutionID,
		CorrelationId: job.CorrelationID, Timestamp: time.Now().UTC(), Headers: headers, Body: encoded,
	})
	if err != nil {
		return fmt.Errorf("publish evaluation job: %w", err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case confirmation, ok := <-manager.confirm:
		if !ok || !confirmation.Ack {
			return errors.New("RabbitMQ did not acknowledge publication")
		}
		return nil
	}
}

func (manager *Manager) ConsumerChannel(prefetch int) (*amqp.Channel, <-chan amqp.Delivery, error) {
	channel, err := manager.connection.Channel()
	if err != nil {
		return nil, nil, err
	}
	if err := manager.declare(channel); err != nil {
		channel.Close()
		return nil, nil, err
	}
	if err := channel.Qos(prefetch, 0, false); err != nil {
		channel.Close()
		return nil, nil, fmt.Errorf("set RabbitMQ prefetch: %w", err)
	}
	deliveries, err := channel.Consume(WorkQueue, "", false, false, false, false, nil)
	if err != nil {
		channel.Close()
		return nil, nil, fmt.Errorf("consume evaluation queue: %w", err)
	}
	return channel, deliveries, nil
}

func (manager *Manager) Close() error {
	if manager.publish != nil {
		_ = manager.publish.Close()
	}
	if manager.connection != nil {
		return manager.connection.Close()
	}
	return nil
}

// Store adapts concrete persistence without forcing its job type into the worker interface.
type Store interface {
	MarkEvaluationRunning(context.Context, string, string) error
	LoadInput(context.Context, string, string) (evaluation.Input, error)
	CompleteEvaluation(context.Context, string, string, string, []evaluation.Result) error
	FailEvaluation(context.Context, string, string, string, bool) error
}

// Worker uses a bounded goroutine pool equal to RabbitMQ prefetch.
type Worker struct {
	manager     *Manager
	store       Store
	evaluators  []evaluation.Evaluator
	concurrency int
	maxAttempts int
	logger      *slog.Logger
}

func NewWorker(manager *Manager, store Store, evaluators []evaluation.Evaluator, concurrency, maxAttempts int, logger *slog.Logger) *Worker {
	if concurrency <= 0 {
		concurrency = 4
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	return &Worker{manager: manager, store: store, evaluators: evaluators, concurrency: concurrency, maxAttempts: maxAttempts, logger: logger}
}

func (worker *Worker) Run(ctx context.Context) error {
	channel, deliveries, err := worker.manager.ConsumerChannel(worker.concurrency)
	if err != nil {
		return err
	}
	defer channel.Close()
	var wait sync.WaitGroup
	for range worker.concurrency {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case delivery, ok := <-deliveries:
					if !ok {
						return
					}
					worker.process(ctx, delivery)
				}
			}
		}()
	}
	<-ctx.Done()
	wait.Wait()
	return nil
}

func (worker *Worker) process(ctx context.Context, delivery amqp.Delivery) {
	carrier := propagation.MapCarrier{}
	for name, value := range delivery.Headers {
		if text, ok := value.(string); ok {
			carrier.Set(name, text)
		}
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	ctx, span := otel.Tracer("aegis/rabbitmq").Start(ctx, "rabbitmq.consume")
	defer span.End()
	var job Job
	if err := json.Unmarshal(delivery.Body, &job); err != nil || job.JobID == "" || job.TenantID == "" || job.ExecutionID == "" {
		_ = delivery.Nack(false, false)
		return
	}
	if err := worker.store.MarkEvaluationRunning(ctx, job.TenantID, job.JobID); err != nil {
		worker.retry(ctx, delivery, job, "database_transient")
		return
	}
	input, err := worker.store.LoadInput(ctx, job.TenantID, job.JobID)
	if err != nil {
		worker.retry(ctx, delivery, job, "evaluation_source")
		return
	}
	results := make([]evaluation.Result, 0, len(worker.evaluators))
	for _, evaluator := range worker.evaluators {
		results = append(results, evaluator.Evaluate(input))
	}
	if err := worker.store.CompleteEvaluation(ctx, job.TenantID, job.JobID, job.ExecutionID, results); err != nil {
		worker.retry(ctx, delivery, job, "database_transient")
		return
	}
	if err := delivery.Ack(false); err != nil {
		worker.logger.Error("evaluation acknowledgement failed", "operation", "rabbitmq.ack", "job_id", job.JobID, "error_category", "broker_ack")
	}
}

func (worker *Worker) retry(ctx context.Context, delivery amqp.Delivery, job Job, category string) {
	job.Attempt++
	if job.Attempt >= worker.maxAttempts {
		_ = worker.store.FailEvaluation(context.WithoutCancel(ctx), job.TenantID, job.JobID, category, true)
		_ = delivery.Nack(false, false)
		return
	}
	if err := worker.manager.Publish(ctx, job, true); err != nil {
		_ = delivery.Nack(false, true)
		return
	}
	_ = worker.store.FailEvaluation(context.WithoutCancel(ctx), job.TenantID, job.JobID, category, false)
	_ = delivery.Ack(false)
}
