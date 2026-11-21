package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/kafka"
)

// Topics and the CloudEvents source of the service (contracts/asyncapi/ledger-service.yaml).
const (
	TopicInvoices = "invoice.events"
	TopicLedger   = "ledger.events"
	Source        = "/slopay/ledger-service"

	TypeReceiptPosted = "ledger.receipt_posted"
	TypeFeeAccrued    = "ledger.fee_accrued"
)

// postingTypes are the invoice events whose transfers have N confirmations and are posted (SRS 4.1.3).
var postingTypes = map[string]bool{
	"invoice.paid":          true,
	"invoice.underpaid":     true,
	"invoice.late_paid":     true,
	"invoice.late_transfer": true,
}

// ErrNotInvoiceEvent: the event is not an invoice event; the consumer skips it.
var ErrNotInvoiceEvent = errors.New("ledger: not an invoice event")

// Invoice is what the ledger takes from an invoice event: the merchant and, for the events of postingTypes,
// the transfers with N confirmations.
type Invoice struct {
	MerchantID uuid.UUID
	// Receipts is empty for an event that posts nothing (invoice.created, invoice.detected, ...).
	Receipts []Receipt
}

type invoiceData struct {
	InvoiceID  uuid.UUID  `json:"invoiceId"`
	MerchantID uuid.UUID  `json:"merchantId"`
	Address    string     `json:"address"`
	Transfers  []Transfer `json:"transfers"`
}

// FromEvent reads an invoice event of contracts/asyncapi/invoice-service.yaml.
func FromEvent(ev kafka.Event) (Invoice, error) {
	if !strings.HasPrefix(ev.Type, "invoice.") {
		return Invoice{}, ErrNotInvoiceEvent
	}
	var d invoiceData
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		return Invoice{}, fmt.Errorf("ledger: %s %s: %w", ev.Type, ev.ID, err)
	}
	if d.MerchantID == uuid.Nil {
		return Invoice{}, fmt.Errorf("ledger: %s %s has no merchantId", ev.Type, ev.ID)
	}
	inv := Invoice{MerchantID: d.MerchantID}
	if !postingTypes[ev.Type] {
		return inv, nil
	}
	if len(d.Transfers) == 0 {
		return Invoice{}, fmt.Errorf("ledger: %s %s has no transfers", ev.Type, ev.ID)
	}
	for _, t := range d.Transfers {
		inv.Receipts = append(inv.Receipts, Receipt{
			MerchantID: d.MerchantID, InvoiceID: d.InvoiceID, Address: d.Address, Transfer: t,
		})
	}
	return inv, nil
}

type receiptPostedData struct {
	MerchantID    uuid.UUID `json:"merchantId"`
	InvoiceID     uuid.UUID `json:"invoiceId"`
	Address       string    `json:"address"`
	Transfer      Transfer  `json:"transfer"`
	DebitAccount  string    `json:"debitAccount"`
	CreditAccount string    `json:"creditAccount"`
	PostedAt      time.Time `json:"postedAt"`
}

type feeAccruedData struct {
	MerchantID    uuid.UUID `json:"merchantId"`
	InvoiceID     uuid.UUID `json:"invoiceId"`
	Transfer      Transfer  `json:"transfer"`
	FeeBps        int       `json:"feeBps"`
	Amount        string    `json:"amount"`
	DebitAccount  string    `json:"debitAccount"`
	CreditAccount string    `json:"creditAccount"`
	PostedAt      time.Time `json:"postedAt"`
}

// Events returns ledger.receipt_posted and ledger.fee_accrued of the posting. Their IDs repeat the
// idempotency key of the transfer, so a repeated posting produces the same events; the key is the merchant.
func (p Posting) Events() ([]kafka.Event, error) {
	receipt, err := p.event(TypeReceiptPosted, p.ID, receiptPostedData{
		MerchantID: p.MerchantID, InvoiceID: p.InvoiceID, Address: p.Address, Transfer: p.Transfer,
		DebitAccount: p.DebitAccount(), CreditAccount: p.CreditAccount(), PostedAt: p.PostedAt,
	})
	if err != nil {
		return nil, err
	}
	fee, err := p.event(TypeFeeAccrued, p.FeeEventID(), feeAccruedData{
		MerchantID: p.MerchantID, InvoiceID: p.InvoiceID, Transfer: p.Transfer, FeeBps: p.FeeBps,
		Amount: p.Fee.String(), DebitAccount: FeeDueAccount(p.MerchantID), CreditAccount: FeeIncomeAccount,
		PostedAt: p.PostedAt,
	})
	if err != nil {
		return nil, err
	}
	return []kafka.Event{receipt, fee}, nil
}

func (p Posting) event(eventType, id string, data any) (kafka.Event, error) {
	ev, err := kafka.NewEvent(Source, eventType, p.MerchantID.String(), 1, data)
	if err != nil {
		return kafka.Event{}, err
	}
	ev.ID = id
	ev.Time = p.PostedAt
	return ev, nil
}
