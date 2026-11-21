package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/invoice-service/internal/projection"
)

// Topics and event types of chain-watcher (contracts/asyncapi/chain-watcher.yaml).
const (
	TopicBlocks    = "chain.blocks"
	TopicTransfers = "chain.transfers"
	EventNewHead   = "chain.new_head"
	EventReorg     = "chain.reorg"
	EventTransfer  = "chain.transfer"
)

// retry repeats fn with backoff until it succeeds or ctx is cancelled: the partition waits, so no event is
// lost and the order is kept.
func retry(ctx context.Context, b projection.Backoff, log *slog.Logger, what string, fn func() error) error {
	wait := b.Min
	for {
		err := fn()
		if err == nil {
			return nil
		}
		log.WarnContext(ctx, what+" failed, retrying", "error", err, "wait", wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, b.Max)
	}
}

// BlocksHandler is the kafka.Handler of chain.blocks. The topic has one partition: the offset of the record is
// stored with the chain state, so a redelivered record is skipped and every block event is applied once, in
// order. Before a chain.new_head it waits for the transfers (w).
func BlocksHandler(e *Engine, w Waiter, b projection.Backoff, log *slog.Logger) kafka.Handler {
	return func(ctx context.Context, ev kafka.Event) error {
		pos, ok := kafka.PositionFrom(ctx)
		if !ok {
			return errors.New("lifecycle: chain.blocks record without a position")
		}
		switch ev.Type {
		case EventNewHead:
			var h NewHead
			if err := json.Unmarshal(ev.Data, &h); err != nil {
				return fmt.Errorf("lifecycle: chain.new_head %s: %w", ev.ID, err)
			}
			if err := retry(ctx, b, log, "waiting for chain.transfers", func() error { return w.Wait(ctx) }); err != nil {
				return err
			}
			return retry(ctx, b, log, "chain.new_head", func() error { return e.NewHead(ctx, pos.Offset, h) })
		case EventReorg:
			var r Reorg
			if err := json.Unmarshal(ev.Data, &r); err != nil {
				return fmt.Errorf("lifecycle: chain.reorg %s: %w", ev.ID, err)
			}
			return retry(ctx, b, log, "chain.reorg", func() error { return e.Reorg(ctx, pos.Offset, r) })
		}
		log.DebugContext(ctx, "chain.blocks: event skipped", "type", ev.Type, "event_id", ev.ID)
		return nil
	}
}

// TransfersHandler is the kafka.Handler of chain.transfers. A repeated transfer is skipped by its key.
func TransfersHandler(e *Engine, b projection.Backoff, log *slog.Logger) kafka.Handler {
	return func(ctx context.Context, ev kafka.Event) error {
		if ev.Type != EventTransfer {
			log.DebugContext(ctx, "chain.transfers: event skipped", "type", ev.Type, "event_id", ev.ID)
			return nil
		}
		var t TransferEvent
		if err := json.Unmarshal(ev.Data, &t); err != nil {
			return fmt.Errorf("lifecycle: chain.transfer %s: %w", ev.ID, err)
		}
		return retry(ctx, b, log, "chain.transfer", func() error { return e.Transfer(ctx, t) })
	}
}
