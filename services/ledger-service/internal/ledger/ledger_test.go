package ledger_test

import (
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/money"
	"github.com/m5tshift/weigate/services/ledger-service/internal/ledger"
	"github.com/m5tshift/weigate/services/ledger-service/internal/ledgertest"
)

var (
	merchantA = uuid.MustParse("0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b")
	invoiceA  = uuid.MustParse("5d7c1a52-3b1e-4f0a-9a43-2f6f0c9b8e11")
	addressA  = "0x8ba1f109551bd432803012645ac136ddd64dba72"
	t0        = time.Date(2026, 11, 15, 10, 3, 0, 0, time.UTC)
)

func receipt(tr ledger.Transfer) ledger.Receipt {
	return ledger.Receipt{MerchantID: merchantA, InvoiceID: invoiceA, Address: addressA, Transfer: tr}
}

func TestTransferKey(t *testing.T) {
	eth := ledgertest.ETH(100, 1, 1)
	if got, want := eth.Key(), eth.BlockHash+":"+eth.TxHash+":"; got != want {
		t.Errorf("ETH key %q, want %q", got, want)
	}
	tok := ledgertest.ERC20(100, 1, 7, 1)
	if got, want := tok.Key(), tok.BlockHash+":"+tok.TxHash+":7"; got != want {
		t.Errorf("ERC-20 key %q, want %q", got, want)
	}
	if ledger.PostingID(tok) != "receipt:"+tok.Key() {
		t.Error("posting id")
	}
	// The same transaction in another block (after a reorg) is a different transfer.
	other := ledgertest.ETH(101, 1, 1)
	if other.Key() == eth.Key() {
		t.Error("block hash is part of the key")
	}
}

func TestNewPostingETH(t *testing.T) {
	tr := ledgertest.ETH(100, 1, 10_000_000_000_000_000)
	p, err := ledger.NewPosting(receipt(tr), 50, t0.Add(123456789*time.Nanosecond).In(time.FixedZone("x", 3600)))
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != "receipt:"+tr.Key() || p.FeeEventID() != "fee:"+tr.Key() {
		t.Errorf("ids %q %q", p.ID, p.FeeEventID())
	}
	if p.Fee.String() != "50000000000000" || p.FeeBps != 50 {
		t.Errorf("fee %s at %d bps", p.Fee, p.FeeBps)
	}
	if p.PostedAt.Location() != time.UTC || p.PostedAt.Nanosecond()%1000 != 0 {
		t.Errorf("postedAt %v must be UTC with microseconds", p.PostedAt)
	}
	want := []ledger.Entry{
		{Account: "address:" + addressA, Side: ledger.Debit, Amount: tr.Amount},
		{Account: "merchant:" + merchantA.String() + ":received", Side: ledger.Credit, Amount: tr.Amount},
		{Account: "merchant:" + merchantA.String() + ":fee_due", Side: ledger.Debit, Amount: p.Fee},
		{Account: "platform:fee_income", Side: ledger.Credit, Amount: p.Fee},
	}
	if len(p.Entries) != len(want) {
		t.Fatalf("entries %+v", p.Entries)
	}
	for i, e := range p.Entries {
		if e.Account != want[i].Account || e.Side != want[i].Side || e.Amount.Cmp(want[i].Amount) != 0 {
			t.Errorf("entry %d: %+v, want %+v", i, e, want[i])
		}
	}
	if !p.Balanced() {
		t.Error("posting is not balanced")
	}
	if p.DebitAccount() != want[0].Account || p.CreditAccount() != want[1].Account {
		t.Error("receipt accounts")
	}
	// The posting owns its amounts: changing the receipt afterwards does not change it.
	tr.Amount.SetInt64(1)
	if p.Transfer.Amount.String() != "10000000000000000" {
		t.Error("posting shares the amount of the receipt")
	}
}

func TestFeeRoundsDown(t *testing.T) {
	for _, c := range []struct {
		amount int64
		bps    int
		fee    string
	}{
		{199, 50, "0"}, // 0.995 -> 0
		{200, 50, "1"}, // exactly 1
		{399, 50, "1"}, // 1.995 -> 1
		{20_000_000, 50, "100000"},
		{0, 50, "0"},
		{12345, 0, "0"},
		{12345, 10000, "12345"},
	} {
		p, err := ledger.NewPosting(receipt(ledgertest.ERC20(1, 1, 0, c.amount)), c.bps, t0)
		if err != nil {
			t.Fatal(err)
		}
		if p.Fee.String() != c.fee {
			t.Errorf("fee of %d at %d bps = %s, want %s", c.amount, c.bps, p.Fee, c.fee)
		}
		if !p.Balanced() {
			t.Errorf("%d: not balanced", c.amount)
		}
	}
	if _, err := ledger.NewPosting(receipt(ledgertest.ETH(1, 1, 1)), 10001, t0); !errors.Is(err, money.ErrInvalidBps) {
		t.Errorf("fee above 100 %%: %v", err)
	}
}

func TestNewPostingRejectsInvalidReceipts(t *testing.T) {
	idx := uint(0)
	for name, mutate := range map[string]func(*ledger.Receipt){
		"no merchant":        func(r *ledger.Receipt) { r.MerchantID = uuid.Nil },
		"no invoice":         func(r *ledger.Receipt) { r.InvoiceID = uuid.Nil },
		"upper-case address": func(r *ledger.Receipt) { r.Address = strings.ToUpper(addressA) },
		"bad block hash":     func(r *ledger.Receipt) { r.Transfer.BlockHash = "0x1" },
		"bad tx hash":        func(r *ledger.Receipt) { r.Transfer.TxHash = "" },
		"no amount":          func(r *ledger.Receipt) { r.Transfer.Amount = nil },
		"negative amount":    func(r *ledger.Receipt) { r.Transfer.Amount = big.NewInt(-1) },
		"ETH with log index": func(r *ledger.Receipt) { r.Transfer.LogIndex = &idx },
		"ETH with contract":  func(r *ledger.Receipt) { r.Transfer.Asset.Contract = ledgertest.ERC20Contract },
		"ETH decimals":       func(r *ledger.Receipt) { r.Transfer.Asset.Decimals = 6 },
		"unknown asset":      func(r *ledger.Receipt) { r.Transfer.Asset.Kind = "BTC" },
		"ERC-20 without log": func(r *ledger.Receipt) {
			r.Transfer.Asset = ledger.Asset{Kind: ledger.KindERC20, Contract: ledgertest.ERC20Contract, Decimals: 6}
		},
		"ERC-20 bad contract": func(r *ledger.Receipt) {
			r.Transfer.LogIndex = &idx
			r.Transfer.Asset = ledger.Asset{Kind: ledger.KindERC20, Contract: "0x", Decimals: 6}
		},
		"ERC-20 decimals": func(r *ledger.Receipt) {
			r.Transfer.LogIndex = &idx
			r.Transfer.Asset = ledger.Asset{Kind: ledger.KindERC20, Contract: ledgertest.ERC20Contract, Decimals: 78}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := receipt(ledgertest.ETH(1, 1, 100))
			mutate(&r)
			if _, err := ledger.NewPosting(r, 50, t0); !errors.Is(err, ledger.ErrInvalidReceipt) {
				t.Errorf("got %v, want ErrInvalidReceipt", err)
			}
		})
	}
}

func TestBalance(t *testing.T) {
	e := func(side ledger.Side, n int64) ledger.Entry {
		return ledger.Entry{Account: "a", Side: side, Amount: big.NewInt(n)}
	}
	if b := ledger.Balance([]ledger.Entry{e(ledger.Debit, 5), e(ledger.Credit, 3)}); b.Int64() != 2 {
		t.Errorf("balance %s", b)
	}
	unbalanced := ledger.Posting{Entries: []ledger.Entry{e(ledger.Debit, 5), e(ledger.Credit, 4)}}
	if unbalanced.Balanced() {
		t.Error("unbalanced posting reported balanced")
	}
	if (ledger.Posting{Entries: []ledger.Entry{e(ledger.Debit, 0)}}).Balanced() {
		t.Error("a single line is not a double entry")
	}
}

func TestTransferJSON(t *testing.T) {
	for _, tr := range []ledger.Transfer{ledgertest.ETH(5, 6, 7), ledgertest.ERC20(5, 6, 0, 7)} {
		raw, err := json.Marshal(tr)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["logIndex"]; !ok {
			t.Errorf("logIndex must be present (null for ETH): %s", raw)
		}
		if m["amount"] != "7" {
			t.Errorf("amount must be a decimal string: %s", raw)
		}
		var back ledger.Transfer
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatal(err)
		}
		if back.Key() != tr.Key() || back.Amount.Cmp(tr.Amount) != 0 || back.Asset != tr.Asset {
			t.Errorf("round trip %+v, want %+v", back, tr)
		}
	}
	raw, _ := json.Marshal(ledger.Transfer{Asset: ledger.Asset{Kind: ledger.KindETH, Decimals: 18}})
	if !strings.Contains(string(raw), `"amount":"0"`) {
		t.Errorf("nil amount: %s", raw)
	}
	for _, bad := range []string{`{"amount":"1.5"}`, `{"amount":"-1"}`, `{"amount":1}`, `[]`} {
		var tr ledger.Transfer
		if err := json.Unmarshal([]byte(bad), &tr); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}
