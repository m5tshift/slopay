// Package projection keeps the invoices of reconciliation-service: a projection of invoice.events in MongoDB, one
// document per invoice with its address, asset, tracking window and the transfers accounted in it (UC-08). Every
// replica consumes the topic, so a new leader has the projection at once.
package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/reconciliation-service/internal/recon"
)

// Topic and event types of invoice-service (contracts/asyncapi/invoice-service.yaml).
const (
	TopicInvoices     = "invoice.events"
	EventCreated      = "invoice.created"
	EventPaid         = "invoice.paid"
	EventUnderpaid    = "invoice.underpaid"
	EventLatePaid     = "invoice.late_paid"
	EventLateTransfer = "invoice.late_transfer"
	EventReverted     = "invoice.reverted"
)

// Update is the change of one invoice made by an event. Nil times are left as they are.
type Update struct {
	InvoiceID  string
	MerchantID string
	Address    string
	Asset      recon.Asset
	Sequence   int
	CreatedAt  *time.Time
	ExpiresAt  *time.Time
	PaidAt     *time.Time
	// Add are the transfers accounted by the event; Remove are the keys of the transfers a reorg reverted.
	Add    []recon.Transfer
	Remove []string
}

// Writer is the part of Store the handler needs.
type Writer interface {
	Apply(ctx context.Context, u Update) error
}

// Backoff bounds the pause between retries of a failed write; it doubles up to Max.
type Backoff struct {
	Min, Max time.Duration
}

// DefaultBackoff is used when a Backoff is zero.
var DefaultBackoff = Backoff{Min: 200 * time.Millisecond, Max: 10 * time.Second}

type assetData struct {
	Kind     string `json:"kind"`
	Contract string `json:"contract"`
	Decimals int    `json:"decimals"`
}

type transferData struct {
	BlockNumber uint64 `json:"blockNumber"`
	BlockHash   string `json:"blockHash"`
	TxHash      string `json:"txHash"`
	LogIndex    *uint  `json:"logIndex"`
	Amount      string `json:"amount"`
}

type eventData struct {
	InvoiceID         string         `json:"invoiceId"`
	MerchantID        string         `json:"merchantId"`
	Sequence          int            `json:"sequence"`
	Address           string         `json:"address"`
	Asset             assetData      `json:"asset"`
	ExpiresAt         *time.Time     `json:"expiresAt"`
	PaidAt            *time.Time     `json:"paidAt"`
	Transfers         []transferData `json:"transfers"`
	RevertedTransfers []transferData `json:"revertedTransfers"`
}

// Decode maps an invoice event to an Update. The second result is false for events that change nothing the
// reconciliation reads (detected, confirming, expired): their transfers are not accounted yet.
func Decode(ev kafka.Event) (Update, bool, error) {
	switch ev.Type {
	case EventCreated, EventPaid, EventUnderpaid, EventLatePaid, EventLateTransfer, EventReverted:
	default:
		return Update{}, false, nil
	}
	var d eventData
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		return Update{}, false, fmt.Errorf("projection: %s %s: %w", ev.Type, ev.ID, err)
	}
	u := Update{
		InvoiceID: d.InvoiceID, MerchantID: d.MerchantID, Address: strings.ToLower(d.Address), Sequence: d.Sequence,
		Asset: recon.Asset{Kind: d.Asset.Kind, Contract: strings.ToLower(d.Asset.Contract), Decimals: d.Asset.Decimals},
	}
	switch ev.Type {
	case EventCreated:
		// The CloudEvent time of invoice.created is the creation time of the invoice.
		created := ev.Time.UTC()
		u.CreatedAt, u.ExpiresAt = &created, d.ExpiresAt
	case EventReverted:
		for _, t := range d.RevertedTransfers {
			u.Remove = append(u.Remove, recon.TransferKey(t.BlockHash, t.TxHash, t.LogIndex))
		}
		return u, true, nil
	case EventPaid:
		u.PaidAt = d.PaidAt
	}
	for _, t := range d.Transfers {
		amount, ok := new(big.Int).SetString(t.Amount, 10)
		if !ok {
			return Update{}, false, fmt.Errorf("projection: %s %s: invalid amount %q", ev.Type, ev.ID, t.Amount)
		}
		u.Add = append(u.Add, recon.Transfer{
			Key: recon.TransferKey(t.BlockHash, t.TxHash, t.LogIndex), BlockNumber: t.BlockNumber, Amount: amount,
		})
	}
	return u, true, nil
}

// Handler is the kafka.Handler of invoice.events. The transfers are accounted by invoice.paid, .underpaid,
// .late_paid and .late_transfer and removed by invoice.reverted; every write is idempotent, so a repeated event changes
// nothing. A failed write is retried with backoff, so the partition waits and no event is lost.
func Handler(w Writer, b Backoff, log *slog.Logger) kafka.Handler {
	if b.Min <= 0 || b.Max < b.Min {
		b = DefaultBackoff
	}
	if log == nil {
		log = slog.Default()
	}
	return func(ctx context.Context, ev kafka.Event) error {
		u, ok, err := Decode(ev)
		if err != nil || !ok {
			return err
		}
		wait := b.Min
		for {
			err := w.Apply(ctx, u)
			if err == nil {
				return nil
			}
			log.WarnContext(ctx, "invoice not stored, retrying", "event_type", ev.Type, "event_id", ev.ID,
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
}
