// Package events maps processed blocks and reorgs to the events of contracts/asyncapi/chain-watcher.yaml:
// chain.new_head and chain.reorg on chain.blocks, key and subject chainId.
package events

import (
	"context"
	"fmt"
	"strconv"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/chain"
)

// Event source, types and topic of the contract.
const (
	EventSource = "/slopay/chain-watcher"
	TypeNewHead = "chain.new_head"
	TypeReorg   = "chain.reorg"
	TopicBlocks = "chain.blocks"
)

type newHeadData struct {
	ChainID    uint64 `json:"chainId"`
	Number     uint64 `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
	Timestamp  uint64 `json:"timestamp"`
}

type reorgData struct {
	ChainID   uint64 `json:"chainId"`
	FromBlock uint64 `json:"fromBlock"`
}

// NewHead builds chain.new_head of a processed block. The ID is deterministic, <chainId>:<hash>, so a block
// published again after a restart is deduplicated by consumers.
func NewHead(chainID uint64, h chain.Header) (kafka.Event, error) {
	ev, err := kafka.NewEvent(EventSource, TypeNewHead, subject(chainID), 1, newHeadData{
		ChainID: chainID, Number: h.Number, Hash: h.Hash, ParentHash: h.ParentHash, Timestamp: h.Timestamp,
	})
	ev.ID = subject(chainID) + ":" + h.Hash
	return ev, err
}

// Reorg builds chain.reorg: blocks from fromBlock on are no longer canonical. orphan is the hash of the
// dropped block fromBlock; with it the ID is deterministic for the reorg, so a reorg published again
// after a restart is deduplicated by consumers.
func Reorg(chainID, fromBlock uint64, orphan string) (kafka.Event, error) {
	ev, err := kafka.NewEvent(EventSource, TypeReorg, subject(chainID), 1, reorgData{
		ChainID: chainID, FromBlock: fromBlock,
	})
	ev.ID = fmt.Sprintf("%s:reorg:%d:%s", subject(chainID), fromBlock, orphan)
	return ev, err
}

func subject(chainID uint64) string { return strconv.FormatUint(chainID, 10) }

// Producer is the part of kafka.Producer the publisher needs.
type Producer interface {
	Publish(ctx context.Context, topic string, ev kafka.Event) error
}

// Publisher publishes block events directly, without an outbox: chain-watcher keeps no state in PostgreSQL,
// the producer is idempotent and the event IDs are deterministic (AGENTS.md, SRS 4.1.5).
type Publisher struct {
	p     Producer
	topic string
}

// NewPublisher publishes to topic, normally TopicBlocks.
func NewPublisher(p Producer, topic string) *Publisher { return &Publisher{p: p, topic: topic} }

// PublishNewHead publishes chain.new_head.
func (pb *Publisher) PublishNewHead(ctx context.Context, chainID uint64, h chain.Header) error {
	ev, err := NewHead(chainID, h)
	if err != nil {
		return err
	}
	return pb.p.Publish(ctx, pb.topic, ev)
}

// PublishReorg publishes chain.reorg.
func (pb *Publisher) PublishReorg(ctx context.Context, chainID, fromBlock uint64, orphan string) error {
	ev, err := Reorg(chainID, fromBlock, orphan)
	if err != nil {
		return err
	}
	return pb.p.Publish(ctx, pb.topic, ev)
}
