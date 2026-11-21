package projection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/weigate/pkg/kafka"
)

// RebuildConfig configures Rebuild.
type RebuildConfig struct {
	Brokers []string
	Topic   string
	// Since is the oldest record time to read: now - 26 h (SRS 4.1.7).
	Since   time.Time
	Codec   *kafka.Codec
	Handler kafka.Handler
	Log     *slog.Logger
	// Idle bounds the wait for the next record; a longer pause fails the rebuild, which is then repeated.
	Idle time.Duration
	// Opts are extra franz-go options (client id).
	Opts []kgo.Opt
}

// Rebuild reads the records of the topic written since cfg.Since, up to the end offsets the topic had when
// Rebuild started, and calls the handler for each of them in partition order. It does not use a consumer group:
// the group of the service keeps its own offsets, and the records read by both are applied idempotently.
func Rebuild(ctx context.Context, cfg RebuildConfig) error {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Idle <= 0 {
		cfg.Idle = 30 * time.Second
	}
	admin, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}, cfg.Opts...)...)
	if err != nil {
		return fmt.Errorf("projection: rebuild: %w", err)
	}
	defer admin.Close()
	parts, err := partitions(ctx, admin, cfg.Topic)
	if err != nil || len(parts) == 0 {
		return err
	}
	starts, err := listOffsets(ctx, admin, cfg.Topic, parts, cfg.Since.UnixMilli())
	if err != nil {
		return err
	}
	ends, err := listOffsets(ctx, admin, cfg.Topic, parts, -1)
	if err != nil {
		return err
	}
	from := map[int32]kgo.Offset{}
	for _, p := range parts {
		// -1: no record since cfg.Since in the partition.
		if starts[p] >= 0 && starts[p] < ends[p] {
			from[p] = kgo.NewOffset().At(starts[p])
		}
	}
	cfg.Log.InfoContext(ctx, "rebuilding the projection of tracked addresses", "topic", cfg.Topic,
		"since", cfg.Since.UTC(), "partitions", len(from))
	if len(from) == 0 {
		return nil
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topic: from}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}, cfg.Opts...)...)
	if err != nil {
		return fmt.Errorf("projection: rebuild: %w", err)
	}
	defer cl.Close()

	records := 0
	for len(from) > 0 {
		pctx, cancel := context.WithTimeout(ctx, cfg.Idle)
		fetches := cl.PollFetches(pctx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if pctx.Err() != nil && fetches.NumRecords() == 0 {
			return fmt.Errorf("projection: rebuild: no records for %s, %d partitions left", cfg.Idle, len(from))
		}
		for _, fe := range fetches.Errors() {
			if !errors.Is(fe.Err, context.DeadlineExceeded) {
				cfg.Log.WarnContext(ctx, "kafka fetch error", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
			}
		}
		var herr error
		fetches.EachRecord(func(rec *kgo.Record) {
			if _, open := from[rec.Partition]; !open || herr != nil {
				return
			}
			ev, err := cfg.Codec.Decode(rec.Value)
			if err != nil {
				cfg.Log.ErrorContext(ctx, "projection: skipping invalid event", "topic", rec.Topic,
					"partition", rec.Partition, "offset", rec.Offset, "error", err)
			} else if herr = cfg.Handler(kafka.ExtractTrace(ctx, rec.Headers), ev); herr != nil {
				return
			}
			records++
			if rec.Offset+1 >= ends[rec.Partition] {
				delete(from, rec.Partition)
			}
		})
		if herr != nil {
			return fmt.Errorf("projection: rebuild: %w", herr)
		}
	}
	cfg.Log.InfoContext(ctx, "projection of tracked addresses rebuilt", "records", records)
	return nil
}

func partitions(ctx context.Context, cl *kgo.Client, topic string) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("projection: metadata of %s: %w", topic, err)
	}
	var parts []int32
	for _, t := range resp.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				return nil, nil // no topic, no invoices
			}
			return nil, fmt.Errorf("projection: metadata of %s: %w", topic, err)
		}
		for _, p := range t.Partitions {
			parts = append(parts, p.Partition)
		}
	}
	return parts, nil
}

// listOffsets returns the offsets of the partitions at the timestamp ts in milliseconds (-1 where no record is
// that new) or at the end (ts = -1), as a read-committed consumer sees them.
func listOffsets(ctx context.Context, cl *kgo.Client, topic string, parts []int32, ts int64) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 1
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = topic
	for _, p := range parts {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition, rp.Timestamp = p, ts
		t.Partitions = append(t.Partitions, rp)
	}
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("projection: offsets of %s: %w", topic, err)
	}
	out := make(map[int32]int64, len(parts))
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("projection: offsets of %s/%d: %w", topic, p.Partition, err)
			}
			out[p.Partition] = p.Offset
		}
	}
	if len(out) != len(parts) {
		return nil, fmt.Errorf("projection: offsets of %s: %d of %d partitions", topic, len(out), len(parts))
	}
	return out, nil
}

// StateStore is the part of Store EnsureBuilt needs.
type StateStore interface {
	Built(ctx context.Context) (bool, error)
	MarkBuilt(ctx context.Context, at time.Time) error
}

// EnsureBuilt rebuilds the projection when it does not exist: on the first start and after the data in MongoDB
// was lost. rebuild is repeated with backoff until it succeeds or ctx is done; then the projection is marked as
// built.
func EnsureBuilt(ctx context.Context, s StateStore, rebuild func(context.Context) error, b Backoff, log *slog.Logger) error {
	if b.Min <= 0 || b.Max < b.Min {
		b = DefaultBackoff
	}
	if log == nil {
		log = slog.Default()
	}
	wait := b.Min
	for {
		err := ensure(ctx, s, rebuild)
		if err == nil || ctx.Err() != nil {
			return err
		}
		log.WarnContext(ctx, "projection rebuild failed, retrying", "error", err, "retry_in", wait.String())
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		wait = min(wait*2, b.Max)
	}
}

func ensure(ctx context.Context, s StateStore, rebuild func(context.Context) error) error {
	built, err := s.Built(ctx)
	if err != nil || built {
		return err
	}
	if err := rebuild(ctx); err != nil {
		return err
	}
	return s.MarkBuilt(ctx, time.Now())
}
