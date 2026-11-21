// Package recon is the reconciliation of reconciliation-service (SRS 4.1.3, UC-08, FR-15): at a block N below
// the head it compares, for every invoice address issued in the lookback period, the balance read from the chain
// with the transfers accounted in the invoices (projection of invoice.events) and with the receipt postings of the
// ledger-service statement.
package recon

import (
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Asset kinds of the contracts.
const (
	KindETH   = "ETH"
	KindERC20 = "ERC20"
)

// Discrepancy kinds of reconciliation.mismatch, and the report row of an address whose tracking window has ended.
const (
	KindChainVsInvoices  = "chain_vs_invoices"
	KindInvoicesVsLedger = "invoices_vs_ledger"
	KindAfterWindow      = "after_window"
)

// Asset is native ETH or an ERC-20 token. Contract is lower-case hex and empty for ETH.
type Asset struct {
	Kind     string
	Contract string
	Decimals int
}

// Key identifies the asset: ETH or the token contract, as in the reconciliation.mismatch event ID.
func (a Asset) Key() string {
	if a.Kind == KindETH {
		return KindETH
	}
	return strings.ToLower(a.Contract)
}

// Transfer is a transfer accounted in an invoice. Key is blockHash:txHash:logIndex (empty logIndex for ETH), the
// idempotency key shared with the ledger postings.
type Transfer struct {
	Key         string
	BlockNumber uint64
	Amount      *big.Int
}

// TransferKey returns the idempotency key of a transfer.
func TransferKey(blockHash, txHash string, logIndex *uint) string {
	idx := ""
	if logIndex != nil {
		idx = strconv.FormatUint(uint64(*logIndex), 10)
	}
	return strings.ToLower(blockHash) + ":" + strings.ToLower(txHash) + ":" + idx
}

// Invoice is an invoice of the projection.
type Invoice struct {
	InvoiceID  string
	MerchantID string
	Address    string
	Asset      Asset
	// CreatedAt is the time of invoice.created; ExpiresAt and PaidAt are chain time (PaidAt is zero unless PAID).
	CreatedAt time.Time
	ExpiresAt time.Time
	PaidAt    time.Time
	Transfers []Transfer
}

// TrackUntil is the end of the address tracking window (SRS 4.1.4): window after the later of expiresAt and paidAt.
func (i Invoice) TrackUntil(window time.Duration) time.Time {
	end := i.ExpiresAt
	if i.PaidAt.After(end) {
		end = i.PaidAt
	}
	return end.Add(window)
}

// Account is what is checked for one address and asset at a block: the invoices of the address are summed.
type Account struct {
	Address    string
	Asset      Asset
	MerchantID string
	InvoiceIDs []string
	// TrackUntil is the latest end of the tracking window of the invoices of the address.
	TrackUntil time.Time
	// Accounted are the accounted transfers by key; Invoices* cover those in blocks up to the checked block.
	Accounted     map[string]Transfer
	InvoicesTotal *big.Int
	InvoicesCount int
}

// AccountKey identifies an account.
func AccountKey(address string, a Asset) string { return address + ":" + a.Key() }

// Accounts groups the invoices by address and asset. A transfer counts only when its block is at most block: the
// balance is read at that block. A transfer accounted in several invoices of the address counts once.
func Accounts(invoices []Invoice, block uint64, window time.Duration) []*Account {
	byKey := map[string]*Account{}
	for _, inv := range invoices {
		k := AccountKey(inv.Address, inv.Asset)
		a := byKey[k]
		if a == nil {
			a = &Account{Address: inv.Address, Asset: inv.Asset, MerchantID: inv.MerchantID,
				Accounted: map[string]Transfer{}, InvoicesTotal: new(big.Int)}
			byKey[k] = a
		}
		a.InvoiceIDs = append(a.InvoiceIDs, inv.InvoiceID)
		if t := inv.TrackUntil(window); t.After(a.TrackUntil) {
			a.TrackUntil = t
		}
		for _, t := range inv.Transfers {
			if _, dup := a.Accounted[t.Key]; dup {
				continue
			}
			a.Accounted[t.Key] = t
			if t.BlockNumber <= block {
				a.InvoicesTotal.Add(a.InvoicesTotal, t.Amount)
				a.InvoicesCount++
			}
		}
	}
	out := make([]*Account, 0, len(byKey))
	for _, a := range byKey {
		sort.Strings(a.InvoiceIDs)
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		return AccountKey(out[i].Address, out[i].Asset) < AccountKey(out[j].Address, out[j].Asset)
	})
	return out
}

// Finding is the outcome of the check of one account.
type Finding struct {
	// Kind is KindChainVsInvoices, KindInvoicesVsLedger or empty when the sums agree.
	Kind string
	// AfterWindow is the part of the balance above the accounted transfers of an address whose tracking window
	// has ended; nil when there is none.
	AfterWindow *big.Int
}

// Mismatch reports whether the finding is a discrepancy.
func (f Finding) Mismatch() bool { return f.Kind != "" }

// Evaluate compares the three sums of an account (UC-08). Within the tracking window any difference between the
// chain balance and the accounted transfers is a discrepancy. After the window a balance above the accounted
// transfers is not: the excess is reported as a row "after the tracking window", since the system does not
// process such transfers (SRS 4.1.4); a balance below them still is. Independently the accounted transfers must
// equal the ledger postings. One event is published per address and asset, so a chain discrepancy takes
// precedence; the event carries all three sums.
func Evaluate(chain, invoices, ledger *big.Int, afterWindow bool) Finding {
	var f Finding
	switch c := chain.Cmp(invoices); {
	case c == 0:
	case c > 0 && afterWindow:
		f.AfterWindow = new(big.Int).Sub(chain, invoices)
	default:
		f.Kind = KindChainVsInvoices
		return f
	}
	if invoices.Cmp(ledger) != 0 {
		f.Kind = KindInvoicesVsLedger
	}
	return f
}
