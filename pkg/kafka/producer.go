package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/m5tshift/weigate/pkg/kafka"

// Producer publishes events. The client is idempotent: franz-go enables idempotent writes with
// acks=all, so a retried batch does not create duplicates in the partition (SRS 4.1.5).
type Producer struct {
	client *kgo.Client
	codec  *Codec
}

// NewProducer connects to the brokers. opts are extra franz-go options (client id, TLS, ...).
func NewProducer(codec *Codec, brokers []string, opts ...kgo.Opt) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errNoBrokers
	}
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<10, true, true, nil)),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: producer: %w", err)
	}
	return &Producer{client: cl, codec: codec}, nil
}

// newRecord builds the Kafka record of an event: key = subject, value = envelope, headers =
// content type and the trace context of ctx.
func newRecord(ctx context.Context, codec *Codec, topic string, ev Event) (*kgo.Record, error) {
	val, err := codec.Encode(ev)
	if err != nil {
		return nil, err
	}
	rec := &kgo.Record{
		Topic:   topic,
		Key:     []byte(ev.Subject),
		Value:   val,
		Headers: []kgo.RecordHeader{{Key: "content-type", Value: []byte(ContentType)}},
	}
	InjectTrace(ctx, &rec.Headers)
	return rec, nil
}

// Publish validates the event and writes it to the topic, returning after the brokers acknowledged it.
// A producer span is started, so the trace continues in consumers.
func (p *Producer) Publish(ctx context.Context, topic string, ev Event) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation.type", "publish"),
			attribute.String("cloudevents.type", ev.Type),
			attribute.String("cloudevents.id", ev.ID)))
	defer span.End()

	rec, err := newRecord(ctx, p.codec, topic, ev)
	if err == nil {
		err = p.client.ProduceSync(ctx, rec).FirstErr()
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		return fmt.Errorf("kafka: publish %s to %s: %w", ev.Type, topic, err)
	}
	return nil
}

// Close flushes buffered records and closes the client.
func (p *Producer) Close() { p.client.Close() }

var errNoBrokers = errors.New("kafka: no brokers")
