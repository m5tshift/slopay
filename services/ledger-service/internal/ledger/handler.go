package ledger

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
)

// Backoff of a failed write (PostgreSQL unavailable): the pause starts at Min and doubles up to Max.
type Backoff struct {
	Min, Max time.Duration
}

// DefaultBackoff is the backoff of the invoice.events consumer.
var DefaultBackoff = Backoff{Min: 500 * time.Millisecond, Max: 30 * time.Second}

// Handler returns the kafka.Handler of invoice.events. Events of other types on the topic are skipped.
// A failed write is repeated with backoff until it succeeds or ctx is cancelled: the partition waits, so no
// receipt is lost. An event that can never be posted is returned as an error: the codec has validated it
// against the schema, so this is a bug, and skipping it would lose a receipt.
func Handler(s *Service, b Backoff, log *slog.Logger) kafka.Handler {
	return func(ctx context.Context, ev kafka.Event) error {
		batch, err := s.Prepare(ev)
		if errors.Is(err, ErrNotInvoiceEvent) {
			log.DebugContext(ctx, "invoice.events: event skipped", "type", ev.Type, "event_id", ev.ID)
			return nil
		}
		if err != nil {
			return err
		}
		wait := b.Min
		for {
			err := s.Record(ctx, batch)
			if err == nil {
				return nil
			}
			log.WarnContext(ctx, "ledger write failed, retrying", "event_type", ev.Type, "event_id", ev.ID,
				"error", err, "wait", wait)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			wait = min(wait*2, b.Max)
		}
	}
}
