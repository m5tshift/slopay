package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Handler processes one event. ctx carries the remote trace context of the producer. A returned error
// stops the consumer without committing the event, so it is delivered again after a restart.
type Handler func(ctx context.Context, ev Event) error

// Consumer is a consumer group member with manual offset commits: an offset is committed only after
// the handler returned nil, so delivery is at-least-once. Handlers must be idempotent (see Deduper).
type Consumer struct {
	client *kgo.Client
	codec  *Codec
	log    *slog.Logger
}

// NewConsumer joins the group and subscribes to topics. Without a committed offset the group starts
// from the oldest record. opts are extra franz-go options.
func NewConsumer(codec *Codec, log *slog.Logger, brokers []string, group string, topics []string, opts ...kgo.Opt) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, errNoBrokers
	}
	if group == "" || len(topics) == 0 {
		return nil, errors.New("kafka: consumer needs a group and topics")
	}
	if log == nil {
		log = slog.Default()
	}
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: consumer: %w", err)
	}
	return &Consumer{client: cl, codec: codec, log: log}, nil
}

// Run polls and handles records until ctx is cancelled (returns nil), the client is closed (nil)
// or a handler fails (returns its error). Records are handled in order per partition, and each
// batch is committed after all of its records were handled.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		done, herr := c.handleBatch(ctx, fetches, h)
		if len(done) > 0 {
			if err := c.client.CommitRecords(context.WithoutCancel(ctx), done...); err != nil {
				return fmt.Errorf("kafka: commit: %w", err)
			}
		}
		if herr != nil {
			return herr
		}
	}
}

// handleBatch handles the fetched records partition by partition, in order, and stops at the first
// handler error. It returns the records that were handled, which are safe to commit.
func (c *Consumer) handleBatch(ctx context.Context, fetches kgo.Fetches, h Handler) ([]*kgo.Record, error) {
	for _, fe := range fetches.Errors() {
		c.log.Warn("kafka fetch error", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
	}
	var done []*kgo.Record
	var herr error
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		for _, rec := range p.Records {
			if herr != nil {
				return
			}
			if herr = c.process(ctx, rec, h); herr == nil {
				done = append(done, rec)
			}
		}
	})
	return done, herr
}

// process decodes one record and calls the handler under a consumer span linked to the producer's
// trace. A record that is not a valid event can never succeed, so it is logged and skipped (nil):
// retrying it would block the partition forever.
func (c *Consumer) process(ctx context.Context, rec *kgo.Record, h Handler) error {
	ctx = ExtractTrace(ctx, rec.Headers)
	ctx, span := otel.Tracer(tracerName).Start(ctx, rec.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.String("messaging.operation.type", "process"),
			attribute.Int("messaging.destination.partition.id", int(rec.Partition)),
			attribute.Int64("messaging.kafka.offset", rec.Offset)))
	defer span.End()

	ev, err := c.codec.Decode(rec.Value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid event")
		c.log.ErrorContext(ctx, "kafka: skipping invalid event",
			"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "error", err)
		return nil
	}
	span.SetAttributes(attribute.String("cloudevents.type", ev.Type), attribute.String("cloudevents.id", ev.ID))
	ctx = WithPosition(ctx, Position{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset})
	if err := h(ctx, ev); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "handler failed")
		return fmt.Errorf("kafka: handle %s %s: %w", ev.Type, ev.ID, err)
	}
	return nil
}

// Position is the place of the record being handled in its topic.
type Position struct {
	Topic     string
	Partition int32
	Offset    int64
}

type positionKey struct{}

// PositionFrom returns the position of the record a Handler of Consumer is called for. A handler that
// keeps its own exactly-once state in a database (an ordered single-partition topic) stores the offset
// in the same transaction and skips records at or below it.
func PositionFrom(ctx context.Context) (Position, bool) {
	p, ok := ctx.Value(positionKey{}).(Position)
	return p, ok
}

// WithPosition returns ctx carrying p, as Consumer passes it to a Handler (tests of handlers).
func WithPosition(ctx context.Context, p Position) context.Context {
	return context.WithValue(ctx, positionKey{}, p)
}

// Close leaves the group and closes the client.
func (c *Consumer) Close() { c.client.Close() }
