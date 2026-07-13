//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/i192009/aegis-ai-platform/internal/rabbitmq"
)

func TestRabbitMQNackRedeliversSameExecution(t *testing.T) {
	ctx := context.Background()
	container, err := testcontainers.Run(ctx, "rabbitmq:4.1.4-management-alpine",
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithEnv(map[string]string{"RABBITMQ_DEFAULT_USER": "aegis", "RABBITMQ_DEFAULT_PASS": "aegis"}),
		testcontainers.WithWaitStrategy(wait.ForLog("Server startup complete").WithStartupTimeout(2*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5672/tcp")
	manager, err := rabbitmq.Connect("amqp://aegis:aegis@" + host + ":" + port.Port() + "/")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	channel, deliveries, err := manager.ConsumerChannel(1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	job := rabbitmq.Job{JobID: "job-1", TenantID: "tenant-1", ExecutionID: "execution-1", JobType: "response-quality", CorrelationID: "correlation-1"}
	if err := manager.Publish(ctx, job, false); err != nil {
		t.Fatal(err)
	}
	first := receive(t, deliveries)
	if err := first.Nack(false, true); err != nil {
		t.Fatal(err)
	}
	second := receive(t, deliveries)
	if first.MessageId != second.MessageId || second.MessageId != job.ExecutionID {
		t.Fatalf("redelivery IDs = %q, %q", first.MessageId, second.MessageId)
	}
	_ = second.Ack(false)
}

func receive(t *testing.T, deliveries <-chan amqp.Delivery) amqp.Delivery {
	t.Helper()
	select {
	case delivery := <-deliveries:
		return delivery
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for RabbitMQ delivery")
		return amqp.Delivery{}
	}
}
