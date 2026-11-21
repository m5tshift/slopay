package recon

import (
	"math/big"
	"testing"
	"time"
)

var (
	eth   = Asset{Kind: KindETH, Decimals: 18}
	token = Asset{Kind: KindERC20, Contract: "0x5fbdb2315678afecb367f032d93f642f64180aa3", Decimals: 6}
	t0    = time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
)

func tr(key string, block uint64, amount int64) Transfer {
	return Transfer{Key: key, BlockNumber: block, Amount: big.NewInt(amount)}
}

func TestTransferKey(t *testing.T) {
	idx := uint(3)
	if got := TransferKey("0xAB", "0xCD", nil); got != "0xab:0xcd:" {
		t.Errorf("eth key %s", got)
	}
	if got := TransferKey("0xab", "0xcd", &idx); got != "0xab:0xcd:3" {
		t.Errorf("token key %s", got)
	}
	if eth.Key() != "ETH" || token.Key() != token.Contract {
		t.Errorf("asset keys %s %s", eth.Key(), token.Key())
	}
}

func TestTrackUntil(t *testing.T) {
	inv := Invoice{ExpiresAt: t0.Add(time.Hour)}
	if got := inv.TrackUntil(24 * time.Hour); !got.Equal(t0.Add(25 * time.Hour)) {
		t.Errorf("unpaid: %v", got)
	}
	inv.PaidAt = t0.Add(30 * time.Minute)
	if got := inv.TrackUntil(24 * time.Hour); !got.Equal(t0.Add(25 * time.Hour)) {
		t.Errorf("paid before expiry: %v", got)
	}
	inv.PaidAt = t0.Add(2 * time.Hour)
	if got := inv.TrackUntil(24 * time.Hour); !got.Equal(t0.Add(26 * time.Hour)) {
		t.Errorf("paid after expiry (late): %v", got)
	}
}

func TestAccounts(t *testing.T) {
	a, b := "0x00000000000000000000000000000000000000aa", "0x00000000000000000000000000000000000000bb"
	accts := Accounts([]Invoice{
		{InvoiceID: "i2", MerchantID: "m", Address: a, Asset: eth, ExpiresAt: t0.Add(2 * time.Hour),
			Transfers: []Transfer{tr("k1", 10, 100), tr("k2", 20, 50)}},
		{InvoiceID: "i1", MerchantID: "m", Address: a, Asset: eth, ExpiresAt: t0,
			// k1 accounted in both invoices of the address counts once; k3 is above the checked block.
			Transfers: []Transfer{tr("k1", 10, 100), tr("k3", 31, 7)}},
		{InvoiceID: "i3", MerchantID: "m", Address: a, Asset: token, ExpiresAt: t0},
		{InvoiceID: "i4", MerchantID: "n", Address: b, Asset: eth, ExpiresAt: t0, Transfers: []Transfer{tr("k4", 30, 1)}},
	}, 30, time.Hour)
	if len(accts) != 3 {
		t.Fatalf("accounts %d", len(accts))
	}
	// Accounts are sorted by address and asset key: the token contract sorts before ETH.
	first := accts[1]
	if first.Address != a || first.Asset != eth || first.InvoicesTotal.Cmp(big.NewInt(150)) != 0 || first.InvoicesCount != 2 ||
		len(first.Accounted) != 3 || first.InvoiceIDs[0] != "i1" || !first.TrackUntil.Equal(t0.Add(3*time.Hour)) {
		t.Errorf("first: %+v", first)
	}
	if accts[0].Asset != token || accts[0].InvoicesTotal.Sign() != 0 || accts[0].InvoicesCount != 0 {
		t.Errorf("token account: %+v", accts[0])
	}
	if accts[2].Address != b || accts[2].MerchantID != "n" || accts[2].InvoicesTotal.Cmp(big.NewInt(1)) != 0 {
		t.Errorf("block boundary is inclusive: %+v", accts[2])
	}
}

func TestEvaluate(t *testing.T) {
	n := big.NewInt
	for _, tc := range []struct {
		name               string
		chain, inv, ledger int64
		afterWindow        bool
		kind               string
		excess             int64 // 0: none
	}{
		{"all equal", 100, 100, 100, false, "", 0},
		{"all equal after window", 100, 100, 100, true, "", 0},
		{"unaccounted transfer in window", 150, 100, 100, false, KindChainVsInvoices, 0},
		{"transfer after window", 150, 100, 100, true, "", 50},
		{"balance below invoices in window", 40, 100, 100, false, KindChainVsInvoices, 0},
		{"balance below invoices after window", 40, 100, 100, true, KindChainVsInvoices, 0},
		{"posting missing in ledger", 100, 100, 60, false, KindInvoicesVsLedger, 0},
		{"posting missing after window", 150, 100, 60, true, KindInvoicesVsLedger, 50},
		{"chain discrepancy takes precedence", 150, 100, 60, false, KindChainVsInvoices, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := Evaluate(n(tc.chain), n(tc.inv), n(tc.ledger), tc.afterWindow)
			if f.Kind != tc.kind || f.Mismatch() != (tc.kind != "") {
				t.Errorf("kind %q, want %q", f.Kind, tc.kind)
			}
			if (tc.excess == 0) != (f.AfterWindow == nil) || (f.AfterWindow != nil && f.AfterWindow.Int64() != tc.excess) {
				t.Errorf("after window %v, want %d", f.AfterWindow, tc.excess)
			}
		})
	}
}

func TestNextRun(t *testing.T) {
	at := time.Date(2026, 11, 21, 12, 3, 20, 0, time.UTC)
	if got := NextRun(at, 10*time.Minute); !got.Equal(time.Date(2026, 11, 21, 12, 10, 0, 0, time.UTC)) {
		t.Errorf("next %v", got)
	}
	at = time.Date(2026, 11, 21, 12, 10, 0, 0, time.UTC)
	if got := NextRun(at, 10*time.Minute); !got.Equal(at.Add(10 * time.Minute)) {
		t.Errorf("on the boundary: %v", got)
	}
}
