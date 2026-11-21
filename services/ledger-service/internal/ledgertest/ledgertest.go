// Package ledgertest builds invoice events of contracts/asyncapi/invoice-service.yaml for the tests of
// ledger-service. Every event is checked against its schema by the codec.
package ledgertest

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledger"
)

// ERC20Contract is the contract of the test ERC-20 token.
const ERC20Contract = "0x5fbdb2315678afecb367f032d93f642f64180aa3"

// Hash returns a 32-byte hex hash derived from n.
func Hash(prefix byte, n uint64) string {
	return fmt.Sprintf("0x%02x%062x", prefix, n)
}

// Address returns an address derived from n.
func Address(n uint64) string {
	return fmt.Sprintf("0x%040x", n+1)
}

// ETH returns a native ETH transfer of amount wei in block n.
func ETH(block uint64, tx uint64, amount int64) ledger.Transfer {
	return ledger.Transfer{
		BlockNumber: block, BlockHash: Hash(0xb0, block), TxHash: Hash(0x70, tx),
		Amount: big.NewInt(amount), Asset: ledger.Asset{Kind: ledger.KindETH, Decimals: 18},
	}
}

// ERC20 returns a transfer of the test token with the log index idx.
func ERC20(block uint64, tx uint64, idx uint, amount int64) ledger.Transfer {
	return ledger.Transfer{
		BlockNumber: block, BlockHash: Hash(0xb0, block), TxHash: Hash(0x70, tx), LogIndex: &idx,
		Amount: big.NewInt(amount), Asset: ledger.Asset{Kind: ledger.KindERC20, Contract: ERC20Contract, Decimals: 6},
	}
}

// Invoice is the invoice an event is about.
type Invoice struct {
	ID, Merchant uuid.UUID
	Address      string
	Asset        ledger.Asset
}

// TB is the part of testing.TB the helpers use; *rapid.T implements it too.
type TB interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// Event returns the invoice event of type eventType ("invoice.paid", "invoice.expired", ...) with the
// transfers, encoded and decoded by codec. The event ID is fresh. Only the events with N confirmations list
// the transfers; the others (invoice.detected, ...) count them in received.
func Event(t TB, codec *kafka.Codec, eventType string, inv Invoice, sequence int, transfers ...ledger.Transfer) kafka.Event {
	t.Helper()
	sum := new(big.Int)
	for _, tr := range transfers {
		sum.Add(sum, tr.Amount)
	}
	data := map[string]any{
		"invoiceId": inv.ID, "merchantId": inv.Merchant, "sequence": sequence, "address": inv.Address,
		"asset": inv.Asset, "amount": "1000000",
	}
	status := strings.ToUpper(strings.TrimPrefix(eventType, "invoice."))
	switch eventType {
	case "invoice.paid":
		data["received"], data["overpaid"], data["confirmations"] = sum.String(), false, 3
		data["paidAt"] = "2026-11-15T10:03:00Z"
	case "invoice.underpaid", "invoice.late_paid":
		data["received"], data["confirmations"] = sum.String(), 3
	case "invoice.late_transfer":
		status = "PAID"
	case "invoice.detected", "invoice.confirming":
		data["received"] = sum.String()
		data["confirmations"] = 1
	}
	data["status"] = status
	switch eventType {
	case "invoice.paid", "invoice.underpaid", "invoice.late_paid", "invoice.late_transfer":
		data["transfers"] = transfers
	}
	ev, err := kafka.NewEvent("/slopay/invoice-service", eventType, inv.ID.String(), 1, data)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := codec.Encode(ev)
	if err != nil {
		t.Fatalf("%s does not match its schema: %v", eventType, err)
	}
	back, err := codec.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return back
}
