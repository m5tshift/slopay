// Package events maps processed blocks, reorgs and transfers to the events of
// contracts/asyncapi/chain-watcher.yaml: chain.new_head and chain.reorg on chain.blocks (key and subject
// chainId), chain.transfer on chain.transfers (key and subject the recipient address).
package events

import (
	"context"
	"fmt"
	"strconv"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/chain"
)

// Event source, types and topic of the contract.
const (
	EventSource    = "/weigate/chain-watcher"
	TypeNewHead    = "chain.new_head"
	TypeReorg      = "chain.reorg"
	TypeTransfer   = "chain.transfer"
	TopicBlocks    = "chain.blocks"
	TopicTransfers = "chain.transfers"
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

type asset struct {
	Kind     string `json:"kind"`
	Decimals int    `json:"decimals"`
}

type transferData struct {
	ChainID        uint64 `json:"chainId"`
	BlockNumber    uint64 `json:"blockNumber"`
	BlockHash      string `json:"blockHash"`
	BlockTimestamp uint64 `json:"blockTimestamp"`
	TxHash         string `json:"txHash"`
	LogIndex       *uint  `json:"logIndex"`
	From           string `json:"from"`
	To             string `json:"to"`
	Amount         string `json:"amount"`
	Asset          asset  `json:"asset"`
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

// Transfer builds chain.transfer of a native ETH transfer: logIndex null, asset ETH with 18 decimals. The ID is
// the idempotency key of the contract, <blockHash>:<txHash>:, so a transfer published again, after a restart or
// by a rescan, is deduplicated by consumers; after a reorg the same txHash in another block gets another ID.
func Transfer(chainID uint64, t chain.Transfer) (kafka.Event, error) {
	if t.Tx.Value == nil {
		return kafka.Event{}, fmt.Errorf("events: transfer %s without a value", t.Tx.Hash)
	}
	ev, err := kafka.NewEvent(EventSource, TypeTransfer, t.Tx.To, 1, transferData{
		ChainID: chainID, BlockNumber: t.Block.Number, BlockHash: t.Block.Hash, BlockTimestamp: t.Block.Timestamp,
		TxHash: t.Tx.Hash, From: t.Tx.From, To: t.Tx.To, Amount: t.Tx.Value.String(),
		Asset: asset{Kind: "ETH", Decimals: 18},
	})
	ev.ID = t.Block.Hash + ":" + t.Tx.Hash + ":"
	return ev, err
}

func subject(chainID uint64) string { return strconv.FormatUint(chainID, 10) }

// Producer is the part of kafka.Producer the publisher needs.
type Producer interface {
	Publish(ctx context.Context, topic string, ev kafka.Event) error
}

// Publisher publishes the events directly, without an outbox: chain-watcher keeps no state in PostgreSQL,
// the producer is idempotent and the event IDs are deterministic (AGENTS.md, SRS 4.1.5).
type Publisher struct {
	p         Producer
	topic     string
	transfers string
}

// NewPublisher publishes block events to blocks and transfers to transfers, normally TopicBlocks and
// TopicTransfers.
func NewPublisher(p Producer, blocks, transfers string) *Publisher {
	return &Publisher{p: p, topic: blocks, transfers: transfers}
}

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

// PublishTransfer publishes chain.transfer and returns after the brokers acknowledged it.
func (pb *Publisher) PublishTransfer(ctx context.Context, chainID uint64, t chain.Transfer) error {
	ev, err := Transfer(chainID, t)
	if err != nil {
		return err
	}
	return pb.p.Publish(ctx, pb.transfers, ev)
}
