package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Waiter blocks until the transfers published before now are applied.
type Waiter interface {
	Wait(ctx context.Context) error
}

// Barrier is the Waiter of the chain.blocks consumer. chain-watcher publishes the transfers of a block before
// its chain.new_head (contracts/asyncapi/chain-watcher.yaml), so when the consumer group of chain.transfers has
// committed the end offsets the topic had at the start of Wait, every transfer of the block and of the earlier
// ones is stored. The group may be spread over several replicas, so the offsets are read from Kafka.
type Barrier struct {
	client *kgo.Client
	topic  string
	group  string
	poll   time.Duration
	log    *slog.Logger
}

// NewBarrier returns a Barrier for the consumer group of topic; client needs no group of its own.
func NewBarrier(client *kgo.Client, topic, group string, poll time.Duration, log *slog.Logger) *Barrier {
	if log == nil {
		log = slog.Default()
	}
	return &Barrier{client: client, topic: topic, group: group, poll: poll, log: log}
}

// Wait implements Waiter.
func (b *Barrier) Wait(ctx context.Context) error {
	parts, err := b.partitions(ctx)
	if err != nil || len(parts) == 0 {
		return err
	}
	ends, err := b.listOffsets(ctx, parts, -1)
	if err != nil {
		return err
	}
	starts, err := b.listOffsets(ctx, parts, -2)
	if err != nil {
		return err
	}
	started, warned := time.Now(), time.Now()
	for {
		committed, err := b.committed(ctx, parts)
		if err != nil {
			return err
		}
		behind := 0
		for _, p := range parts {
			if max(committed[p], starts[p]) < ends[p] {
				behind++
			}
		}
		if behind == 0 {
			return nil
		}
		if time.Since(warned) > 10*time.Second {
			warned = time.Now()
			b.log.WarnContext(ctx, "chain.blocks waits for chain.transfers", "group", b.group,
				"partitions_behind", behind, "waiting", time.Since(started).Round(time.Second))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(b.poll):
		}
	}
}

func (b *Barrier) partitions(ctx context.Context) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(b.topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, b.client)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: metadata of %s: %w", b.topic, err)
	}
	var parts []int32
	for _, t := range resp.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				return nil, nil // no topic, no transfers
			}
			return nil, fmt.Errorf("lifecycle: metadata of %s: %w", b.topic, err)
		}
		for _, p := range t.Partitions {
			parts = append(parts, p.Partition)
		}
	}
	return parts, nil
}

// listOffsets returns the latest (-1) or the earliest (-2) committed-read offsets of the partitions.
func (b *Barrier) listOffsets(ctx context.Context, parts []int32, ts int64) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 1 // read committed, as the consumers read
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = b.topic
	for _, p := range parts {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition, rp.Timestamp = p, ts
		t.Partitions = append(t.Partitions, rp)
	}
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, b.client)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: offsets of %s: %w", b.topic, err)
	}
	out := make(map[int32]int64, len(parts))
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("lifecycle: offsets of %s/%d: %w", b.topic, p.Partition, err)
			}
			out[p.Partition] = p.Offset
		}
	}
	if len(out) != len(parts) {
		return nil, fmt.Errorf("lifecycle: offsets of %s: %d of %d partitions", b.topic, len(out), len(parts))
	}
	return out, nil
}

// committed returns the committed offsets of the group; -1 where it has none.
func (b *Barrier) committed(ctx context.Context, parts []int32) (map[int32]int64, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.RequireStable = true
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = b.group
	t := kmsg.NewOffsetFetchRequestGroupTopic()
	t.Topic, t.Partitions = b.topic, parts
	g.Topics = append(g.Topics, t)
	req.Groups = append(req.Groups, g)
	resp, err := req.RequestWith(ctx, b.client)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: committed offsets of %s: %w", b.group, err)
	}
	out := make(map[int32]int64, len(parts))
	for _, p := range parts {
		out[p] = -1
	}
	for _, g := range resp.Groups {
		if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
			return nil, fmt.Errorf("lifecycle: committed offsets of %s: %w", b.group, err)
		}
		for _, t := range g.Topics {
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					return nil, fmt.Errorf("lifecycle: committed offset of %s/%d: %w", b.group, p.Partition, err)
				}
				out[p.Partition] = p.Offset
			}
		}
	}
	return out, nil
}
