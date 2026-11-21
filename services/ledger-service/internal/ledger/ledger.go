// Package ledger is the double-entry ledger of ledger-service (SRS 4.1.3, FR-12): one posting per transfer
// that reached N confirmations, the platform fee accrued on it, and the statements built from the postings.
//
// A posting is the journal entry of one transfer. Its lines are, in the asset of the transfer:
//
//	debit  address:<address>              amount   the invoice address received the transfer
//	credit merchant:<merchantId>:received amount   the merchant received the funds
//	debit  merchant:<merchantId>:fee_due  fee      the merchant owes the platform the fee
//	credit platform:fee_income            fee      the platform earned the fee
//
// so the debits of every posting equal its credits. The platform never holds the funds; the fee is billed to
// the merchant separately. There are no reversing postings: a transfer with N confirmations is never undone.
package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/money"
)

// Asset kinds of contracts/schemas/defs.v1.json.
const (
	KindETH   = "ETH"
	KindERC20 = "ERC20"
)

// Asset is native ETH or an ERC-20 token. Amounts of different assets are never summed.
type Asset struct {
	Kind string `json:"kind"`
	// Contract is the token contract, lower-case hex; empty for ETH.
	Contract string `json:"contract,omitempty"`
	Decimals int    `json:"decimals"`
}

// Key identifies the asset in sums: the kind and the contract.
func (a Asset) Key() string { return a.Kind + ":" + a.Contract }

// Transfer is an accounted transfer of an invoice event.
type Transfer struct {
	BlockNumber uint64
	BlockHash   string
	TxHash      string
	// LogIndex is the index of the Transfer log for ERC-20; nil for a native ETH transfer.
	LogIndex *uint
	// Amount is in minimal units of Asset.
	Amount *big.Int
	Asset  Asset
}

// transferJSON is the wire form of defs.v1.json#/definitions/accountedTransfer.
type transferJSON struct {
	BlockNumber uint64 `json:"blockNumber"`
	BlockHash   string `json:"blockHash"`
	TxHash      string `json:"txHash"`
	LogIndex    *uint  `json:"logIndex"`
	Amount      string `json:"amount"`
	Asset       Asset  `json:"asset"`
}

// MarshalJSON writes the accountedTransfer of the event schemas; logIndex is null for ETH.
func (t Transfer) MarshalJSON() ([]byte, error) {
	amount := "0"
	if t.Amount != nil {
		amount = t.Amount.String()
	}
	return json.Marshal(transferJSON{
		BlockNumber: t.BlockNumber, BlockHash: t.BlockHash, TxHash: t.TxHash, LogIndex: t.LogIndex,
		Amount: amount, Asset: t.Asset,
	})
}

// UnmarshalJSON reads an accountedTransfer; the amount must be a non-negative integer.
func (t *Transfer) UnmarshalJSON(b []byte) error {
	var w transferJSON
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	amount, ok := new(big.Int).SetString(w.Amount, 10)
	if !ok || amount.Sign() < 0 {
		return fmt.Errorf("ledger: transfer amount %q is not a non-negative integer", w.Amount)
	}
	*t = Transfer{
		BlockNumber: w.BlockNumber, BlockHash: w.BlockHash, TxHash: w.TxHash, LogIndex: w.LogIndex,
		Amount: amount, Asset: w.Asset,
	}
	return nil
}

// Key is the idempotency key of the transfer: <blockHash>:<txHash>:<logIndex or empty> (SRS 4.1.3).
func (t Transfer) Key() string {
	idx := ""
	if t.LogIndex != nil {
		idx = strconv.FormatUint(uint64(*t.LogIndex), 10)
	}
	return t.BlockHash + ":" + t.TxHash + ":" + idx
}

// Receipt is a transfer with N confirmations to an invoice address, the input of a posting.
type Receipt struct {
	MerchantID uuid.UUID
	InvoiceID  uuid.UUID
	// Address is the invoice address, lower-case hex.
	Address  string
	Transfer Transfer
}

// Side of a journal line.
type Side string

// Sides of a journal line.
const (
	Debit  Side = "D"
	Credit Side = "C"
)

// Entry is one line of a posting.
type Entry struct {
	Account string
	Side    Side
	Amount  *big.Int
}

// Posting is the journal entry of one receipt.
type Posting struct {
	// ID is receipt:<transfer key>, the ID of the ledger.receipt_posted event.
	ID         string
	MerchantID uuid.UUID
	InvoiceID  uuid.UUID
	Address    string
	Transfer   Transfer
	// FeeBps is the fee rate the posting was made with (50 = 0.5 %).
	FeeBps int
	// Fee is floor(Transfer.Amount * FeeBps / 10000) in the asset of the transfer.
	Fee      *big.Int
	PostedAt time.Time
	// Entries are the lines in the order of the package documentation.
	Entries []Entry
}

// FeeEventID is the ID of the ledger.fee_accrued event of the posting: fee:<transfer key>.
func (p Posting) FeeEventID() string { return "fee:" + p.Transfer.Key() }

// DebitAccount is the account the receipt is debited to: address:<address>.
func (p Posting) DebitAccount() string { return AddressAccount(p.Address) }

// CreditAccount is the account the receipt is credited to: merchant:<merchantId>:received.
func (p Posting) CreditAccount() string { return ReceivedAccount(p.MerchantID) }

// AddressAccount is the account of an invoice address.
func AddressAccount(address string) string { return "address:" + address }

// ReceivedAccount is the merchant account of received funds.
func ReceivedAccount(merchant uuid.UUID) string { return "merchant:" + merchant.String() + ":received" }

// FeeDueAccount is the merchant debt to the platform.
func FeeDueAccount(merchant uuid.UUID) string { return "merchant:" + merchant.String() + ":fee_due" }

// FeeIncomeAccount is the platform fee income.
const FeeIncomeAccount = "platform:fee_income"

// PostingID returns the posting ID of a transfer: receipt:<transfer key>.
func PostingID(t Transfer) string { return "receipt:" + t.Key() }

var (
	addressRe = regexp.MustCompile(`^0x[0-9a-f]{40}$`)
	hashRe    = regexp.MustCompile(`^0x[0-9a-f]{64}$`)
)

// ErrInvalidReceipt is returned for a receipt that cannot be posted.
var ErrInvalidReceipt = errors.New("ledger: invalid receipt")

func (r Receipt) validate() error {
	t := r.Transfer
	switch {
	case r.MerchantID == uuid.Nil || r.InvoiceID == uuid.Nil:
		return fmt.Errorf("%w: merchant and invoice are required", ErrInvalidReceipt)
	case !addressRe.MatchString(r.Address):
		return fmt.Errorf("%w: address %q", ErrInvalidReceipt, r.Address)
	case !hashRe.MatchString(t.BlockHash) || !hashRe.MatchString(t.TxHash):
		return fmt.Errorf("%w: block or transaction hash", ErrInvalidReceipt)
	case t.Amount == nil || t.Amount.Sign() < 0:
		return fmt.Errorf("%w: amount", ErrInvalidReceipt)
	}
	switch t.Asset.Kind {
	case KindETH:
		if t.LogIndex != nil || t.Asset.Contract != "" || t.Asset.Decimals != 18 {
			return fmt.Errorf("%w: ETH transfer has a log index, a contract or decimals other than 18", ErrInvalidReceipt)
		}
	case KindERC20:
		if t.LogIndex == nil || !addressRe.MatchString(t.Asset.Contract) || t.Asset.Decimals < 0 || t.Asset.Decimals > 77 {
			return fmt.Errorf("%w: ERC-20 transfer needs a log index, a contract and decimals 0..77", ErrInvalidReceipt)
		}
	default:
		return fmt.Errorf("%w: asset kind %q", ErrInvalidReceipt, t.Asset.Kind)
	}
	return nil
}

// NewPosting builds the posting of a receipt with the fee rate feeBps at time at.
func NewPosting(r Receipt, feeBps int, at time.Time) (Posting, error) {
	if err := r.validate(); err != nil {
		return Posting{}, err
	}
	fee, err := money.Fee(r.Transfer.Amount, feeBps)
	if err != nil {
		return Posting{}, err
	}
	amount := new(big.Int).Set(r.Transfer.Amount)
	t := r.Transfer
	t.Amount = amount
	return Posting{
		ID:         PostingID(t),
		MerchantID: r.MerchantID,
		InvoiceID:  r.InvoiceID,
		Address:    r.Address,
		Transfer:   t,
		FeeBps:     feeBps,
		Fee:        fee,
		PostedAt:   at.UTC().Truncate(time.Microsecond),
		Entries: []Entry{
			{Account: AddressAccount(r.Address), Side: Debit, Amount: new(big.Int).Set(amount)},
			{Account: ReceivedAccount(r.MerchantID), Side: Credit, Amount: new(big.Int).Set(amount)},
			{Account: FeeDueAccount(r.MerchantID), Side: Debit, Amount: new(big.Int).Set(fee)},
			{Account: FeeIncomeAccount, Side: Credit, Amount: new(big.Int).Set(fee)},
		},
	}, nil
}

// Balance returns the sum of debits minus the sum of credits of entries; zero for a balanced journal.
func Balance(entries []Entry) *big.Int {
	sum := new(big.Int)
	for _, e := range entries {
		switch e.Side {
		case Debit:
			sum.Add(sum, e.Amount)
		case Credit:
			sum.Sub(sum, e.Amount)
		}
	}
	return sum
}

// Balanced reports whether the debits of the posting equal its credits.
func (p Posting) Balanced() bool { return len(p.Entries) >= 2 && Balance(p.Entries).Sign() == 0 }
