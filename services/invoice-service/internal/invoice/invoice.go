// Package invoice is the invoice API of invoice-service (SRS 4.1.3, 4.1.4; FR-01, FR-02, FR-03, FR-08):
// creation with an idempotency key, the rate fixed through rate-service, the address derived from the merchant
// xpub, the merchant projection of merchant.events and the status cache of the payment page.
package invoice

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
)

// Status is the invoice status of SRS 4.1.4.
type Status string

// Statuses of SRS 4.1.4.
const (
	StatusNew        Status = "NEW"
	StatusDetected   Status = "DETECTED"
	StatusConfirming Status = "CONFIRMING"
	StatusPaid       Status = "PAID"
	StatusUnderpaid  Status = "UNDERPAID"
	StatusExpired    Status = "EXPIRED"
	StatusLatePaid   Status = "LATE_PAID"
)

// Asset is the payment asset of an invoice (contracts/schemas/defs.v1.json#/definitions/asset).
type Asset struct {
	// Kind is ETH or ERC20.
	Kind string `json:"kind"`
	// Contract is the lower-case token address; empty for ETH.
	Contract string `json:"contract,omitempty"`
	// Decimals of the minimal unit: 18 for ETH.
	Decimals int `json:"decimals"`
}

// Asset kinds.
const (
	KindETH   = "ETH"
	KindERC20 = "ERC20"
)

// ETH is native ether, amounts in wei.
var ETH = Asset{Kind: KindETH, Decimals: 18}

// Requested assets of CreateInvoiceRequest.asset.
const (
	RequestETH  = "ETH"
	RequestUSDT = "USDT"
)

// Invoice is the stored invoice.
type Invoice struct {
	ID             uuid.UUID
	MerchantID     uuid.UUID
	OrderID        string
	AmountUSDCents int64
	Asset          Asset
	// Rate is USD for one whole unit of the asset as a decimal string, fixed at creation.
	Rate         string
	RateQuotedAt time.Time
	// Amount is the amount to pay in minimal units of the asset, USD converted with rounding up (SRS 4.1.3).
	Amount *big.Int
	// Address is the lower-case address derived from the merchant xpub with DerivationIndex.
	Address         string
	DerivationIndex uint32
	Status          Status
	// Sequence is the invoice version, the sequence of its invoice.events (1 at creation).
	Sequence      int
	Confirmations int
	Received      *big.Int
	Overpaid      bool
	CreatedAt     time.Time
	// ExpiresAt is the deadline for the block timestamp of the transfer (chain time).
	ExpiresAt time.Time
}

// Page is the payment page data (contracts/openapi/invoice-service.yaml#/components/schemas/PaymentPage),
// also the JSON of the status cache. It holds no merchant data other than the order id.
type Page struct {
	InvoiceID uuid.UUID `json:"invoiceId"`
	Status    Status    `json:"status"`
	Address   string    `json:"address"`
	Amount    string    `json:"amount"`
	Asset     Asset     `json:"asset"`
	OrderID   string    `json:"orderId"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// Page returns the payment page data of the invoice.
func (inv Invoice) Page() Page {
	return Page{
		InvoiceID: inv.ID, Status: inv.Status, Address: inv.Address, Amount: inv.Amount.String(),
		Asset: inv.Asset, OrderID: inv.OrderID, ExpiresAt: inv.ExpiresAt,
	}
}

// Merchant is the projection of merchant.updated that invoice-service needs.
type Merchant struct {
	ID           uuid.UUID
	Version      int64
	Xpub         string
	ToleranceBps int
}

// Errors of the service.
var (
	// ErrNotFound: no such invoice, or it belongs to another merchant.
	ErrNotFound = errors.New("invoice: not found")
	// ErrNoXpub: the merchant projection has no xpub yet, an address cannot be derived.
	ErrNoXpub = errors.New("invoice: merchant has no xpub")
	// ErrKeyReuse: the idempotency key was used with another request body.
	ErrKeyReuse = errors.New("invoice: idempotency key reused with another request")
	// ErrIndexExhausted: the merchant has used every non-hardened derivation index.
	ErrIndexExhausted = errors.New("invoice: derivation indexes of the merchant are exhausted")
)

// ValidationError is a request the contract accepts but the service cannot serve.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string { return fmt.Sprintf("invoice: %s: %s", e.Field, e.Message) }

// Rate unavailability reasons of the contract (Error.reason).
const (
	ReasonUnavailable = "unavailable"
	ReasonStale       = "stale"
	ReasonDiverged    = "diverged"
)

// RateError means no usable rate: a new invoice is not created (503, SRS 4.1.9).
type RateError struct {
	// Reason is ReasonUnavailable, ReasonStale or ReasonDiverged.
	Reason string
	// RetryAfter is the pause suggested by rate-service; zero means the configured default.
	RetryAfter time.Duration
	Err        error
}

func (e *RateError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("invoice: rate %s: %v", e.Reason, e.Err)
	}
	return "invoice: rate " + e.Reason
}

func (e *RateError) Unwrap() error { return e.Err }

// Quote is the rate returned by rate-service.
type Quote struct {
	// Price is USD for one whole unit, a decimal string.
	Price    string
	QuotedAt time.Time
	// Age is ageSeconds of the answer: the age rate-service measured.
	Age time.Duration
}

// RateSource returns the latest rate of a pair (rate-service GET /rates/{pair}). Unusable rates are *RateError.
type RateSource interface {
	Rate(ctx context.Context, pair string) (Quote, error)
}

// StatusCache keeps payment pages for polling (SRS 4.1.7: STRING with JSON, TTL 30 s). It is a cache only:
// Get misses on any error and Set ignores errors, PostgreSQL stays the source of truth.
type StatusCache interface {
	Get(ctx context.Context, id uuid.UUID) (Page, bool)
	Set(ctx context.Context, p Page)
}

// NopCache is a StatusCache that holds nothing.
type NopCache struct{}

// Get implements StatusCache.
func (NopCache) Get(context.Context, uuid.UUID) (Page, bool) { return Page{}, false }

// Set implements StatusCache.
func (NopCache) Set(context.Context, Page) {}
