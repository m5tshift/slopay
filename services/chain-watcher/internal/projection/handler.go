package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/m5tshift/slopay/pkg/kafka"
)

// Topic and event types of invoice-service (contracts/asyncapi/invoice-service.yaml).
const (
	TopicInvoices = "invoice.events"
	EventCreated  = "invoice.created"
	EventPaid     = "invoice.paid"
)

// Writer is the part of Store the handler needs.
type Writer interface {
	Track(ctx context.Context, address, invoiceID string, createdAt, expiresAt, trackUntil time.Time) error
	Extend(ctx context.Context, address string, trackUntil time.Time) error
}

// Backoff bounds the pause between retries of a failed write; it doubles up to Max.
type Backoff struct {
	Min, Max time.Duration
}

// DefaultBackoff is used when a Backoff is zero.
var DefaultBackoff = Backoff{Min: 200 * time.Millisecond, Max: 10 * time.Second}

type createdData struct {
	InvoiceID string    `json:"invoiceId"`
	Address   string    `json:"address"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type paidData struct {
	Address string    `json:"address"`
	PaidAt  time.Time `json:"paidAt"`
}

// Handler is the kafka.Handler of invoice.events. invoice.created starts tracking the address; the CloudEvent
// time of it is the creation time of the invoice, from which the earlier blocks are searched. invoice.paid moves
// the end of the window to paidAt + window. Other events do not change the window and are skipped. A failed write
// is retried with backoff, so the partition waits and no event is lost.
func Handler(w Writer, window time.Duration, b Backoff, log *slog.Logger) kafka.Handler {
	if b.Min <= 0 || b.Max < b.Min {
		b = DefaultBackoff
	}
	if log == nil {
		log = slog.Default()
	}
	return func(ctx context.Context, ev kafka.Event) error {
		switch ev.Type {
		case EventCreated:
			var d createdData
			if err := json.Unmarshal(ev.Data, &d); err != nil {
				return fmt.Errorf("projection: %s %s: %w", ev.Type, ev.ID, err)
			}
			addr := strings.ToLower(d.Address)
			return retry(ctx, b, log, ev, func() error {
				return w.Track(ctx, addr, d.InvoiceID, ev.Time, d.ExpiresAt, d.ExpiresAt.Add(window))
			})
		case EventPaid:
			var d paidData
			if err := json.Unmarshal(ev.Data, &d); err != nil {
				return fmt.Errorf("projection: %s %s: %w", ev.Type, ev.ID, err)
			}
			addr := strings.ToLower(d.Address)
			return retry(ctx, b, log, ev, func() error { return w.Extend(ctx, addr, d.PaidAt.Add(window)) })
		}
		return nil
	}
}

func retry(ctx context.Context, b Backoff, log *slog.Logger, ev kafka.Event, fn func() error) error {
	wait := b.Min
	for {
		err := fn()
		if err == nil {
			return nil
		}
		log.WarnContext(ctx, "tracked address not stored, retrying", "event_type", ev.Type, "event_id", ev.ID,
			"error", err, "retry_in", wait.String())
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
