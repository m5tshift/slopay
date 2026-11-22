// Package consume reads a topic in a consumer group and handles its records concurrently by key: the records of one
// key (one invoice, one merchant) are handled in order, records of different keys at the same time. So a slow or
// failing merchant endpoint does not hold up the webhooks of other merchants (SRS 4.1.9). A batch is committed
// after all of its records were handled; delivery is at-least-once.
package consume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/weigate/pkg/kafka"
)

// Backoff between repeats of a failed handler.
const (
	RetryInitial = 500 * time.Millisecond
	RetryMax     = 30 * time.Second
)

// Loop is one consumer group member.
type Loop struct {
	client *kgo.Client
	codec  *kafka.Codec
	log    *slog.Logger
	limit  int
}

// New joins the group and subscribes to topics. limit bounds the keys handled at the same time. Without a committed
// offset the group starts from the oldest record. opts are extra franz-go options.
func New(codec *kafka.Codec, log *slog.Logger, brokers []string, group string, topics []string, limit int, opts ...kgo.Opt) (*Loop, error) {
	if len(brokers) == 0 || group == "" || len(topics) == 0 || limit < 1 {
		return nil, errors.New("consume: brokers, a group, topics and a limit >= 1 are required")
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
		return nil, fmt.Errorf("consume: %w", err)
	}
	return &Loop{client: cl, codec: codec, log: log, limit: limit}, nil
}

// Close leaves the group.
func (l *Loop) Close() { l.client.Close() }

// Run polls and handles records until ctx is cancelled or the client is closed. A failed handler is repeated with
// backoff; when ctx ends first, the batch is not committed and is read again after a restart.
func (l *Loop) Run(ctx context.Context, h kafka.Handler) error {
	for {
		fetches := l.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			l.log.Warn("kafka fetch error", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
		}
		recs := fetches.Records()
		if !Handle(ctx, l.log, l.codec, recs, l.limit, h) {
			return nil
		}
		if len(recs) > 0 {
			// A failed commit (rebalance) means a redelivery, which the handlers tolerate.
			if err := l.client.CommitRecords(context.WithoutCancel(ctx), recs...); err != nil {
				l.log.Warn("kafka commit failed", "error", err)
			}
		}
	}
}

// Handle handles recs: in order within a topic, partition and key, concurrently across keys, at most limit keys at
// a time. It returns false when ctx ended before every record was handled.
func Handle(ctx context.Context, log *slog.Logger, codec *kafka.Codec, recs []*kgo.Record, limit int, h kafka.Handler) bool {
	type groupKey struct {
		topic     string
		partition int32
		key       string
	}
	var order []groupKey
	groups := map[groupKey][]*kgo.Record{}
	for _, r := range recs {
		k := groupKey{r.Topic, r.Partition, string(r.Key)}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	var mu sync.Mutex
	complete := true
	for _, k := range order {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return false
		}
		wg.Go(func() {
			defer func() { <-sem }()
			for _, r := range groups[k] {
				if !process(ctx, log, codec, r, h) {
					mu.Lock()
					complete = false
					mu.Unlock()
					return
				}
			}
		})
	}
	wg.Wait()
	return complete
}

// process handles one record under a consumer span linked to the producer's trace, repeating a failed handler with
// backoff. A record that is not a valid event can never succeed: it is logged and skipped. It returns false when
// ctx ended before the record was handled.
func process(ctx context.Context, log *slog.Logger, codec *kafka.Codec, rec *kgo.Record, h kafka.Handler) bool {
	ctx = kafka.ExtractTrace(ctx, rec.Headers)
	ctx, span := otel.Tracer("github.com/m5tshift/weigate/services/webhook-service").Start(ctx, rec.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.String("messaging.operation.type", "process"),
			attribute.Int("messaging.destination.partition.id", int(rec.Partition)),
			attribute.Int64("messaging.kafka.offset", rec.Offset)))
	defer span.End()

	ev, err := codec.Decode(rec.Value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid event")
		log.ErrorContext(ctx, "skipping invalid event", "topic", rec.Topic, "partition", rec.Partition,
			"offset", rec.Offset, "error", err)
		return true
	}
	span.SetAttributes(attribute.String("cloudevents.type", ev.Type), attribute.String("cloudevents.id", ev.ID))
	for delay := RetryInitial; ; delay = min(delay*2, RetryMax) {
		err := h(ctx, ev)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		span.RecordError(err)
		log.ErrorContext(ctx, "handler failed, repeating", "topic", rec.Topic, "event_id", ev.ID, "error", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}
	}
}
