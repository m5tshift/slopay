// Package invoicetest builds invoice.events of contracts/asyncapi/invoice-service.yaml for the tests of
// reconciliation-service. It is imported only by tests.
package invoicetest

import (
	"time"

	"github.com/m5tshift/slopay/pkg/kafka"
)

// Source is the source of invoice-service events.
const Source = "/slopay/invoice-service"

// ETH is the asset of native ETH invoices.
var ETH = map[string]any{"kind": "ETH", "decimals": 18}

// Transfer is an accounted transfer of an invoice event.
type Transfer struct {
	BlockNumber uint64
	BlockHash   string
	TxHash      string
	Amount      string
}

func (t Transfer) data() map[string]any {
	return map[string]any{"blockNumber": t.BlockNumber, "blockHash": t.BlockHash, "txHash": t.TxHash,
		"logIndex": nil, "amount": t.Amount, "asset": ETH}
}

func transfers(ts []Transfer) []map[string]any {
	out := make([]map[string]any, len(ts))
	for i, t := range ts {
		out[i] = t.data()
	}
	return out
}

func event(typ, invoiceID string, at time.Time, data map[string]any) kafka.Event {
	ev, err := kafka.NewEvent(Source, typ, invoiceID, 1, data)
	if err != nil {
		panic(err)
	}
	ev.Time = at.UTC()
	return ev
}

// Created builds invoice.created of an ETH invoice of amount wei created at createdAt.
func Created(invoiceID, merchantID, address, amount string, createdAt, expiresAt time.Time) kafka.Event {
	return event("invoice.created", invoiceID, createdAt, map[string]any{
		"invoiceId": invoiceID, "merchantId": merchantID, "sequence": 1, "status": "NEW", "address": address,
		"asset": ETH, "amount": amount, "orderId": "order-" + invoiceID[:8], "amountUsdCents": 1000, "rate": "3000",
		"expiresAt": expiresAt.UTC().Format(time.RFC3339),
	})
}

// Paid builds invoice.paid with the transfers ts, all of the invoice amount.
func Paid(invoiceID, merchantID, address, amount string, seq int, paidAt time.Time, ts ...Transfer) kafka.Event {
	return event("invoice.paid", invoiceID, paidAt, map[string]any{
		"invoiceId": invoiceID, "merchantId": merchantID, "sequence": seq, "status": "PAID", "address": address,
		"asset": ETH, "amount": amount, "received": amount, "overpaid": false, "confirmations": 7,
		"transfers": transfers(ts), "paidAt": paidAt.UTC().Format(time.RFC3339),
	})
}

// LateTransfer builds invoice.late_transfer with the transfers ts.
func LateTransfer(invoiceID, merchantID, address, amount string, seq int, at time.Time, ts ...Transfer) kafka.Event {
	return event("invoice.late_transfer", invoiceID, at, map[string]any{
		"invoiceId": invoiceID, "merchantId": merchantID, "sequence": seq, "status": "PAID", "address": address,
		"asset": ETH, "amount": amount, "transfers": transfers(ts),
	})
}

// Reverted builds invoice.reverted of the transfers ts.
func Reverted(invoiceID, merchantID, address, amount string, seq int, at time.Time, ts ...Transfer) kafka.Event {
	return event("invoice.reverted", invoiceID, at, map[string]any{
		"invoiceId": invoiceID, "merchantId": merchantID, "sequence": seq, "status": "NEW",
		"previousStatus": "CONFIRMING", "address": address, "asset": ETH, "amount": amount, "received": "0",
		"fromBlock": 1, "revertedTransfers": transfers(ts),
	})
}

// Detected builds invoice.detected, which the reconciliation ignores.
func Detected(invoiceID, merchantID, address, amount string, seq int, at time.Time) kafka.Event {
	return event("invoice.detected", invoiceID, at, map[string]any{
		"invoiceId": invoiceID, "merchantId": merchantID, "sequence": seq, "status": "DETECTED", "address": address,
		"asset": ETH, "amount": amount, "received": amount, "confirmations": 1,
	})
}
