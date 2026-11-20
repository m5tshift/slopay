package events

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/rate-service/internal/rate"
)

const tracerName = "github.com/m5tshift/slopay/services/rate-service/internal/events"

// Reader keeps the replica cache in step with rate.latest. Every replica reads the whole topic, so it is not
// a consumer group member: the topic is read from the start without committing offsets, and the compacted
// topic gives the latest rate of every pair after a restart (SRS 4.1.3).
type Reader struct {
	client *kgo.Client
	codec  *kafka.Codec
	cache  *rate.Cache
	log    *slog.Logger
}

// NewReader connects to the brokers. opts are extra franz-go options.
func NewReader(codec *kafka.Codec, cache *rate.Cache, log *slog.Logger, brokers []string, opts ...kgo.Opt) (*Reader, error) {
	if log == nil {
		log = slog.Default()
	}
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(TopicLatest),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("events: reader: %w", err)
	}
	return &Reader{client: cl, codec: codec, cache: cache, log: log}, nil
}

// Run reads rate.latest until ctx is cancelled or the client is closed.
func (r *Reader) Run(ctx context.Context) {
	for {
		fetches := r.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		for _, fe := range fetches.Errors() {
			r.log.WarnContext(ctx, "rate.latest fetch error", "partition", fe.Partition, "error", fe.Err)
		}
		fetches.EachRecord(func(rec *kgo.Record) { r.Apply(ctx, rec) })
	}
}

// Apply puts one rate.latest record into the cache under a consumer span that continues the producer's trace.
// An invalid record is logged and skipped; an older snapshot does not replace a newer one.
func (r *Reader) Apply(ctx context.Context, rec *kgo.Record) {
	ctx = kafka.ExtractTrace(ctx, rec.Headers)
	ctx, span := otel.Tracer(tracerName).Start(ctx, rec.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.String("messaging.operation.type", "process"),
			attribute.Int64("messaging.kafka.offset", rec.Offset)))
	defer span.End()

	ev, err := r.codec.Decode(rec.Value)
	if err == nil {
		var s rate.Snapshot
		if s, err = Snapshot(ev); err == nil {
			if r.cache.Put(s.Rate.Pair, s) {
				r.log.DebugContext(ctx, "rate restored from rate.latest", "pair", s.Rate.Pair, "price", s.Rate.Price.String())
			}
			return
		}
	}
	span.RecordError(err)
	r.log.ErrorContext(ctx, "skipping invalid rate.latest record", "offset", rec.Offset, "error", err)
}

// Close closes the client.
func (r *Reader) Close() { r.client.Close() }
