package invoice

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/kafka"
)

// Filter selects invoices of a merchant. The order is createdAt descending, ties by id descending.
type Filter struct {
	// Statuses keeps invoices with any of them; empty means every status.
	Statuses []Status
	// OrderID keeps invoices of the order; empty means any.
	OrderID string
	// CreatedFrom is inclusive, CreatedTo is exclusive; zero means no bound.
	CreatedFrom time.Time
	CreatedTo   time.Time
	// Limit is the page size.
	Limit int
	// After continues after the last invoice of the previous page.
	After *Cursor
}

// Cursor is the position after the last invoice of a page.
type Cursor struct {
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"i"`
}

// ErrInvalidCursor: the cursor was not returned by the service.
var ErrInvalidCursor = &ValidationError{Field: "cursor", Message: "invalid cursor"}

// EncodeCursor returns the opaque nextCursor.
func EncodeCursor(c Cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a cursor of EncodeCursor; any other string is ErrInvalidCursor.
func DecodeCursor(s string) (Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil || c.CreatedAt.IsZero() || c.ID == uuid.Nil {
		return Cursor{}, ErrInvalidCursor
	}
	return c, nil
}

// Before reports whether inv comes after the cursor in the list order (createdAt desc, id desc).
func (c Cursor) Before(inv Invoice) bool {
	if !inv.CreatedAt.Equal(c.CreatedAt) {
		return inv.CreatedAt.Before(c.CreatedAt)
	}
	return compareUUID(inv.ID, c.ID) < 0
}

func compareUUID(a, b uuid.UUID) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// Matches reports whether inv passes the filter, the cursor included.
func (f Filter) Matches(inv Invoice) bool {
	if len(f.Statuses) > 0 {
		ok := false
		for _, s := range f.Statuses {
			ok = ok || s == inv.Status
		}
		if !ok {
			return false
		}
	}
	if f.OrderID != "" && inv.OrderID != f.OrderID {
		return false
	}
	if !f.CreatedFrom.IsZero() && inv.CreatedAt.Before(f.CreatedFrom) {
		return false
	}
	if !f.CreatedTo.IsZero() && !inv.CreatedAt.Before(f.CreatedTo) {
		return false
	}
	return f.After == nil || f.After.Before(inv)
}

// Event source, type and topic of invoice.created (contracts/asyncapi/invoice-service.yaml).
const (
	EventSource      = "/weigate/invoice-service"
	EventCreated     = "invoice.created"
	TopicInvoices    = "invoice.events"
	TopicMerchants   = "merchant.events"
	EventMerchantUpd = "merchant.updated"
)

// CreatedData is the data of invoice.created v1 (contracts/schemas/invoice.created.v1.json).
type CreatedData struct {
	InvoiceID      uuid.UUID `json:"invoiceId"`
	MerchantID     uuid.UUID `json:"merchantId"`
	Sequence       int       `json:"sequence"`
	Status         Status    `json:"status"`
	Address        string    `json:"address"`
	Asset          Asset     `json:"asset"`
	Amount         string    `json:"amount"`
	OrderID        string    `json:"orderId"`
	AmountUSDCents int64     `json:"amountUsdCents"`
	Rate           string    `json:"rate"`
	ExpiresAt      time.Time `json:"expiresAt"`
}

// CreatedEvent builds the invoice.created of a new invoice; the subject, and the Kafka key, is the invoice id.
func CreatedEvent(inv Invoice) (kafka.Event, error) {
	return kafka.NewEvent(EventSource, EventCreated, inv.ID.String(), 1, CreatedData{
		InvoiceID: inv.ID, MerchantID: inv.MerchantID, Sequence: inv.Sequence, Status: inv.Status,
		Address: inv.Address, Asset: inv.Asset, Amount: inv.Amount.String(), OrderID: inv.OrderID,
		AmountUSDCents: inv.AmountUSDCents, Rate: inv.Rate, ExpiresAt: inv.ExpiresAt,
	})
}

// MerchantData is the part of merchant.updated v1 the projection keeps.
type MerchantData struct {
	MerchantID   uuid.UUID `json:"merchantId"`
	Version      int64     `json:"version"`
	Xpub         string    `json:"xpub"`
	ToleranceBps int       `json:"toleranceBps"`
}

// ErrNotMerchantEvent: the event is not a merchant.updated.
var ErrNotMerchantEvent = errors.New("invoice: not a merchant.updated event")

// MerchantFromEvent reads the projection from a merchant.updated validated by the codec.
func MerchantFromEvent(ev kafka.Event) (Merchant, error) {
	if ev.Type != EventMerchantUpd {
		return Merchant{}, ErrNotMerchantEvent
	}
	var d MerchantData
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		return Merchant{}, err
	}
	return Merchant{ID: d.MerchantID, Version: d.Version, Xpub: d.Xpub, ToleranceBps: d.ToleranceBps}, nil
}

func newZero() *big.Int { return new(big.Int) }
