package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/i192009/aegis-ai-platform/internal/events"
)

// EventProcessor commits idempotent effects before a Kafka offset is acknowledged.
type EventProcessor interface {
	ProcessKafkaEvent(context.Context, string, events.Envelope) (bool, error)
}

// Consumer processes one consumer group serially per assigned partition.
type Consumer struct {
	reader    *kafkago.Reader
	processor EventProcessor
	name      string
	logger    *slog.Logger
}

func NewConsumer(brokers []string, topic, groupID, name string, processor EventProcessor, logger *slog.Logger) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: groupID, MinBytes: 1, MaxBytes: 10 << 20,
		MaxWait: time.Second, CommitInterval: 0, StartOffset: kafkago.FirstOffset,
	})
	return &Consumer{reader: reader, processor: processor, name: name, logger: logger}
}

func (consumer *Consumer) Run(ctx context.Context) error {
	for {
		message, err := consumer.reader.FetchMessage(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("fetch Kafka message: %w", err)
		}
		var event events.Envelope
		if err := json.Unmarshal(message.Value, &event); err != nil {
			return fmt.Errorf("decode Kafka event at partition %d offset %d: %w", message.Partition, message.Offset, err)
		}
		carrier := propagation.MapCarrier{}
		for _, header := range message.Headers {
			carrier.Set(header.Key, string(header.Value))
		}
		processCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)
		processCtx, span := otel.Tracer("aegis/kafka").Start(processCtx, "kafka.consume")
		processed, err := consumer.processor.ProcessKafkaEvent(processCtx, consumer.name, event)
		span.End()
		if err != nil {
			return fmt.Errorf("process Kafka event %s: %w", event.EventID, err)
		}
		if err := consumer.reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit Kafka offset: %w", err)
		}
		consumer.logger.Info("Kafka event committed", "operation", "kafka.consume", "event_type", event.EventType, "processed", processed, "partition", message.Partition)
	}
}

func (consumer *Consumer) Close() error { return consumer.reader.Close() }
