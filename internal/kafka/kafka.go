// Package kafka adapts retained Kafka facts to AegisAI event contracts.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/i192009/aegis-ai-platform/internal/events"
)

// Publisher requires all in-sync replicas to acknowledge each retained fact.
type Publisher struct {
	writer *kafkago.Writer
	topic  string
}

func NewPublisher(brokers []string, topic string) *Publisher {
	writer := &kafkago.Writer{
		Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}, RequiredAcks: kafkago.RequireAll,
		Async: false, BatchTimeout: 10 * time.Millisecond, WriteTimeout: 10 * time.Second, ReadTimeout: 10 * time.Second,
	}
	return &Publisher{writer: writer, topic: topic}
}

func (publisher *Publisher) Publish(ctx context.Context, event events.Envelope) error {
	ctx, span := otel.Tracer("aegis/kafka").Start(ctx, "kafka.publish")
	defer span.End()
	if err := event.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal Kafka event: %w", err)
	}
	headers := []kafkago.Header{{Key: "event_id", Value: []byte(event.EventID)}, {Key: "correlation_id", Value: []byte(event.CorrelationID)}}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for key, value := range carrier {
		headers = append(headers, kafkago.Header{Key: key, Value: []byte(value)})
	}
	message := kafkago.Message{Key: []byte(event.TenantID), Value: encoded, Time: event.Timestamp, Headers: headers}
	if err := publisher.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish Kafka event: %w", err)
	}
	return nil
}

func (publisher *Publisher) Close() error { return publisher.writer.Close() }
