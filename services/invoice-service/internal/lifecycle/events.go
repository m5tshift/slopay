package lifecycle

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
)

// AccountedTransfer is defs.v1.json#/definitions/accountedTransfer.
type AccountedTransfer struct {
	BlockNumber uint64        `json:"blockNumber"`
	BlockHash   string        `json:"blockHash"`
	TxHash      string        `json:"txHash"`
	LogIndex    *int64        `json:"logIndex"`
	Amount      string        `json:"amount"`
	Asset       invoice.Asset `json:"asset"`
}

// EventData is the data of the invoice.* events of the lifecycle (contracts/schemas/invoice.<type>.v1.json).
// The fields an event type does not have are left empty and omitted.
type EventData struct {
	InvoiceID         uuid.UUID           `json:"invoiceId"`
	MerchantID        uuid.UUID           `json:"merchantId"`
	Sequence          int                 `json:"sequence"`
	Status            invoice.Status      `json:"status"`
	PreviousStatus    invoice.Status      `json:"previousStatus,omitempty"`
	Address           string              `json:"address"`
	Asset             invoice.Asset       `json:"asset"`
	Amount            string              `json:"amount"`
	Received          *string             `json:"received,omitempty"`
	Overpaid          *bool               `json:"overpaid,omitempty"`
	Confirmations     *int                `json:"confirmations,omitempty"`
	Transfers         []AccountedTransfer `json:"transfers,omitempty"`
	PaidAt            *time.Time          `json:"paidAt,omitempty"`
	FromBlock         *uint64             `json:"fromBlock,omitempty"`
	RevertedTransfers []AccountedTransfer `json:"revertedTransfers,omitempty"`
}

func accounted(ts []Transfer) []AccountedTransfer {
	out := make([]AccountedTransfer, len(ts))
	for i, t := range ts {
		out[i] = AccountedTransfer{BlockNumber: t.BlockNumber, BlockHash: t.BlockHash, TxHash: t.TxHash,
			LogIndex: t.LogIndex, Amount: t.Amount.String(), Asset: t.Asset}
	}
	return out
}

// Data returns the event data of a transition of the invoice.
func Data(inv invoice.Invoice, tr Transition) EventData {
	d := EventData{InvoiceID: inv.ID, MerchantID: inv.MerchantID, Sequence: tr.Sequence, Status: tr.To,
		Address: inv.Address, Asset: inv.Asset, Amount: inv.Amount.String()}
	received := func() {
		s := tr.Received.String()
		d.Received = &s
	}
	confirmations := func() {
		c := tr.Confirmations
		d.Confirmations = &c
	}
	switch tr.Type {
	case EventDetected, EventConfirming:
		received()
		confirmations()
	case EventPaid:
		received()
		confirmations()
		overpaid, paidAt := tr.Overpaid, tr.PaidAt.UTC()
		d.Overpaid, d.PaidAt = &overpaid, &paidAt
		d.Transfers = accounted(tr.Transfers)
	case EventUnderpaid, EventLatePaid:
		received()
		confirmations()
		d.Transfers = accounted(tr.Transfers)
	case EventReverted:
		received()
		d.PreviousStatus = tr.From
		from := tr.FromBlock
		d.FromBlock = &from
		d.RevertedTransfers = accounted(tr.Transfers)
	case EventLateTransfer:
		d.Transfers = accounted(tr.Transfers)
	}
	return d
}

// NewEvent builds the invoice.* event of a transition. The id is EventID of the invoice and the sequence, so
// the same transition always has the same id; time is the processing time.
func NewEvent(inv invoice.Invoice, tr Transition, now time.Time) (kafka.Event, error) {
	ev, err := kafka.NewEvent(invoice.EventSource, tr.Type, inv.ID.String(), 1, Data(inv, tr))
	if err != nil {
		return kafka.Event{}, fmt.Errorf("lifecycle: %s event: %w", tr.Type, err)
	}
	ev.ID, ev.Time = EventID(inv.ID, tr.Sequence), now.UTC()
	return ev, nil
}
