//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/money"
)

// newInvoice registers a merchant and creates an invoice for cents; it checks FR-01 (idempotent creation),
// FR-02 (the amount from the fixed rate, rounded up) and FR-03 (the address from the merchant xpub), then waits
// until chain-watcher tracks the address.
func newInvoice(ctx context.Context, t *testing.T, name string, cents int64) (merchant, invoice) {
	t.Helper()
	m := registerMerchant(ctx, t, name)
	key := uuid.NewString()
	inv, code := createInvoice(ctx, t, m, key, cents)
	if code != http.StatusCreated {
		t.Fatalf("create invoice: status %d, want 201", code)
	}
	if inv.Status != "NEW" || inv.MerchantID != m.id || inv.AmountUSDCents != cents || inv.Asset.Kind != "ETH" ||
		inv.Asset.Decimals != 18 {
		t.Fatalf("created invoice %+v", inv)
	}
	again, code := createInvoice(ctx, t, m, key, cents)
	if code != http.StatusOK || again.InvoiceID != inv.InvoiceID {
		t.Fatalf("repeated create: status %d, invoice %s, want 200 and %s", code, again.InvoiceID, inv.InvoiceID)
	}
	rate, err := money.ParseRate(inv.Rate)
	if err != nil {
		t.Fatalf("rate %q: %v", inv.Rate, err)
	}
	want, err := money.USDToAsset(cents, rate, 18)
	if err != nil {
		t.Fatal(err)
	}
	if bigInt(t, inv.Amount).Cmp(want) != 0 {
		t.Fatalf("amount %s, want %s for %d cents at %s", inv.Amount, want, cents, inv.Rate)
	}
	if !strings.EqualFold(inv.Address, m.firstAddress) {
		t.Fatalf("address %s, want the first address of the xpub %s", inv.Address, m.firstAddress)
	}
	waitEvents(t, inv.InvoiceID, "invoice.created")
	waitTracked(ctx, t, inv.Address)
	return m, inv
}

// TestPaid is the slice up to PAID: the merchant registers, gets a JWT, creates an invoice, payer-sim pays it in
// full and the invoice reaches PAID after N confirmations.
func TestPaid(t *testing.T) {
	ctx := testContext(t)
	m, inv := newInvoice(ctx, t, "E2E Paid", 2_500)

	p, receipt := pay(ctx, t, payerFull, inv.InvoiceID)
	if p.Outcome != "pay" || p.Amount != inv.Amount || receipt.Status != 1 {
		t.Fatalf("payment %+v, receipt status %d, want a full payment of %s", p, receipt.Status, inv.Amount)
	}
	paid := waitStatus(ctx, t, m, inv.InvoiceID, "PAID", true)
	if paid.ReceivedAmount != inv.Amount || paid.Overpaid {
		t.Errorf("paid invoice: received %s overpaid %v, want %s", paid.ReceivedAmount, paid.Overpaid, inv.Amount)
	}
	if paid.Confirmations < 7 {
		t.Errorf("paid with %d confirmations, want at least 7", paid.Confirmations)
	}
	waitEvents(t, inv.InvoiceID, "invoice.created", "invoice.detected", "invoice.paid")
}

// TestUnderpaid: payer-sim pays half of the amount, below the tolerance, and the invoice becomes UNDERPAID
// after N confirmations with the received amount.
func TestUnderpaid(t *testing.T) {
	ctx := testContext(t)
	m, inv := newInvoice(ctx, t, "E2E Underpaid", 4_000)

	p, receipt := pay(ctx, t, payerUnder, inv.InvoiceID)
	half := new(big.Int).Div(bigInt(t, inv.Amount), big.NewInt(2))
	if p.Outcome != "underpay" || bigInt(t, p.Amount).Cmp(half) != 0 || receipt.Status != 1 {
		t.Fatalf("payment %+v, receipt status %d, want an underpayment of %s", p, receipt.Status, half)
	}
	under := waitStatus(ctx, t, m, inv.InvoiceID, "UNDERPAID", true)
	if under.ReceivedAmount != p.Amount || under.Overpaid {
		t.Errorf("underpaid invoice: received %s overpaid %v, want %s", under.ReceivedAmount, under.Overpaid, p.Amount)
	}
	waitEvents(t, inv.InvoiceID, "invoice.created", "invoice.detected", "invoice.underpaid")
}

// TestReorg: a paid transfer with 3 confirmations is removed by a reorg of depth 3, the invoice goes back to NEW,
// then payer-sim sends the same signed transaction again and the invoice reaches PAID (SRS 8.5).
func TestReorg(t *testing.T) {
	ctx := testContext(t)
	m, inv := newInvoice(ctx, t, "E2E Reorg", 3_300)
	ec := ethClient(ctx, t)

	head := headNumber(ctx, t)
	var snap string
	rpcCall(ctx, t, &snap, "evm_snapshot")
	p, receipt := pay(ctx, t, payerReplayer, inv.InvoiceID)
	block := receipt.BlockNumber.Uint64()
	if block != head+1 {
		t.Fatalf("payment in block %d, want %d", block, head+1)
	}
	const depth = 3
	mineBlocks(ctx, t, depth-1)
	before := waitConfirmations(ctx, t, m, inv.InvoiceID, depth)
	if before.Status != "DETECTED" && before.Status != "CONFIRMING" {
		t.Fatalf("status before the reorg %s", before.Status)
	}

	// The new branch from the snapshot is one block longer than the old one: blocks head+1..head+3 are replaced.
	var ok bool
	rpcCall(ctx, t, &ok, "evm_revert", snap)
	if !ok {
		t.Fatal("evm_revert failed")
	}
	rpcCall(ctx, t, nil, "evm_increaseTime", hexutil.Uint64(13))
	mineBlocks(ctx, t, depth+1)
	if _, err := ec.TransactionReceipt(ctx, common.HexToHash(p.TxHash)); !errors.Is(err, ethereum.NotFound) {
		t.Fatalf("the transfer survived the reorg: %v", err)
	}

	reverted := waitStatus(ctx, t, m, inv.InvoiceID, "NEW", false)
	if reverted.ReceivedAmount != "0" || reverted.Confirmations != 0 {
		t.Errorf("after the reorg: received %s, confirmations %d", reverted.ReceivedAmount, reverted.Confirmations)
	}
	evs := waitEvents(t, inv.InvoiceID, "invoice.created", "invoice.detected", "invoice.reverted")
	var rev struct {
		Status         string `json:"status"`
		PreviousStatus string `json:"previousStatus"`
		FromBlock      uint64 `json:"fromBlock"`
		Received       string `json:"received"`
	}
	eventData(t, evs, "invoice.reverted", &rev)
	if rev.Status != "NEW" || rev.FromBlock != block || rev.Received != "0" || rev.PreviousStatus != before.Status {
		t.Errorf("invoice.reverted %+v, want status NEW from block %d after %s", rev, block, before.Status)
	}

	if sent := replay(ctx, t, payerReplayer, inv.InvoiceID); sent != 1 {
		t.Fatalf("replay sent %d transactions, want 1", sent)
	}
	again := waitReceipt(ctx, t, common.HexToHash(p.TxHash))
	if again.Status != 1 || again.BlockNumber.Uint64() <= head+depth {
		t.Fatalf("replayed transfer: status %d in block %d", again.Status, again.BlockNumber.Uint64())
	}
	paid := waitStatus(ctx, t, m, inv.InvoiceID, "PAID", true)
	if paid.ReceivedAmount != inv.Amount || paid.Overpaid {
		t.Errorf("paid invoice: received %s overpaid %v, want %s", paid.ReceivedAmount, paid.Overpaid, inv.Amount)
	}
	waitEvents(t, inv.InvoiceID, "invoice.created", "invoice.detected", "invoice.reverted", "invoice.detected",
		"invoice.paid")
}
