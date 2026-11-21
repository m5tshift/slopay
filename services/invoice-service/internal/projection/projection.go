// Package projection keeps the merchant projection of invoice-service from merchant.events (SRS 4.1.2:
// event-carried state transfer, no synchronous call to merchant-service). The topic is compacted by merchant
// id; a newer version replaces the row, an older or repeated one is ignored, so redelivery is harmless.
package projection

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
)

// Applier applies a merchant.updated (invoice.Service implements it).
type Applier interface {
	ApplyMerchant(ctx context.Context, m invoice.Merchant) error
}

// Backoff of a failed apply (PostgreSQL unavailable): the pause starts at Min and doubles up to Max.
type Backoff struct {
	Min, Max time.Duration
}

// DefaultBackoff is used by NewConsumer.
var DefaultBackoff = Backoff{Min: 500 * time.Millisecond, Max: 30 * time.Second}

// Handler returns the kafka.Handler of merchant.events. Events of other types on the topic are skipped.
// A failed apply is repeated with backoff until it succeeds or ctx is cancelled: the partition waits, so the
// versions of a merchant are applied in order and no event is lost.
func Handler(a Applier, b Backoff, log *slog.Logger) kafka.Handler {
	return func(ctx context.Context, ev kafka.Event) error {
		m, err := invoice.MerchantFromEvent(ev)
		if errors.Is(err, invoice.ErrNotMerchantEvent) {
			log.DebugContext(ctx, "merchant.events: event skipped", "type", ev.Type, "event_id", ev.ID)
			return nil
		}
		if err != nil {
			// The codec validated the event against the schema, so this is a bug; skipping would lose state.
			return err
		}
		wait := b.Min
		for {
			err := a.ApplyMerchant(ctx, m)
			if err == nil {
				return nil
			}
			log.WarnContext(ctx, "merchant projection update failed, retrying", "merchant_id", m.ID,
				"version", m.Version, "error", err, "wait", wait)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			wait = min(wait*2, b.Max)
		}
	}
}

// Consumer is the consumer group of merchant.events.
type Consumer struct {
	c   *kafka.Consumer
	h   kafka.Handler
	log *slog.Logger
}

// NewConsumer joins group and subscribes to merchant.events. Without a committed offset the group reads the
// topic from the start, which restores the whole projection.
func NewConsumer(codec *kafka.Codec, a Applier, log *slog.Logger, brokers []string, group string, opts ...kgo.Opt) (*Consumer, error) {
	c, err := kafka.NewConsumer(codec, log, brokers, group, []string{invoice.TopicMerchants}, opts...)
	if err != nil {
		return nil, err
	}
	return &Consumer{c: c, h: Handler(a, DefaultBackoff, log), log: log}, nil
}

// Run consumes until ctx is cancelled (nil). Any other return is a failure of the consumer (an offset commit)
// after which the process must restart; uncommitted events are then read again.
func (c *Consumer) Run(ctx context.Context) error { return c.c.Run(ctx, c.h) }

// Close leaves the group.
func (c *Consumer) Close() { c.c.Close() }
