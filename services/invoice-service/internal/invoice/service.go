package invoice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/m5tshift/slopay/pkg/hdwallet"
	"github.com/m5tshift/slopay/pkg/money"
)

const meterName = "github.com/m5tshift/slopay/services/invoice-service/internal/invoice"

// Store keeps invoices and the merchant projection. Implementations: store.Postgres and MemStore.
type Store interface {
	// FindByKey returns the invoice created with the idempotency key and the hash of its request,
	// ErrNotFound if there is none.
	FindByKey(ctx context.Context, merchantID uuid.UUID, key string) (Invoice, []byte, error)
	// Merchant returns the projection of the merchant, ErrNoXpub if invoice-service has not received it.
	Merchant(ctx context.Context, id uuid.UUID) (Merchant, error)
	// Create stores a new invoice atomically with its invoice.created event. Under a lock of the merchant it
	// checks the key again: if an invoice with the key exists, it returns that one, its request hash and
	// created=false. Otherwise it takes the next derivation index of the merchant, derives the address with
	// derive (an index that returns hdwallet.ErrInvalidChildKey is skipped), stores the invoice and returns
	// it with created=true.
	Create(ctx context.Context, d Draft, derive DeriveFunc) (inv Invoice, hash []byte, created bool, err error)
	// Get returns an invoice of the merchant, ErrNotFound for an unknown id or another merchant.
	Get(ctx context.Context, merchantID, id uuid.UUID) (Invoice, error)
	// GetPublic returns an invoice by id only, for the payment page.
	GetPublic(ctx context.Context, id uuid.UUID) (Invoice, error)
	// List returns up to f.Limit+1 invoices of the merchant in the order of Filter.
	List(ctx context.Context, merchantID uuid.UUID, f Filter) ([]Invoice, error)
	// UpsertMerchant applies a merchant.updated: it replaces the projection only when m.Version is higher than
	// the stored one and reports whether it did. The derivation counter is never changed by it.
	UpsertMerchant(ctx context.Context, m Merchant) (bool, error)
}

// DeriveFunc derives the lower-case address with index from the merchant xpub.
type DeriveFunc func(xpub string, index uint32) (string, error)

// Draft is a new invoice without the address, which the store derives under the merchant lock.
type Draft struct {
	Invoice
	IdempotencyKey string
	// RequestHash identifies the request body: a repeat with another body is ErrKeyReuse.
	RequestHash []byte
}

// CreateRequest is the body of POST /api/v1/invoices.
type CreateRequest struct {
	AmountUSDCents int64
	OrderID        string
	// Asset is RequestETH or RequestUSDT; empty means RequestETH.
	Asset string
}

// Config configures the Service. Every value comes from the service configuration (SRS numbers are not
// constants in code).
type Config struct {
	// InvoiceTTL is the lifetime of an invoice: expiresAt = creation + InvoiceTTL (SRS 4.1.4: 15 min).
	InvoiceTTL time.Duration
	// RateMaxAge is the oldest rate an invoice is created with (SRS 4.1.3: 60 s).
	RateMaxAge time.Duration
	// USDT is the test ERC-20 token of USDT invoices (FR-10): its contract and decimals come from the service
	// configuration. Nil: USDT invoices are not served and the request is a validation error.
	USDT *Asset
	// Now is the clock (default time.Now).
	Now func() time.Time
	// Meter receives the metrics (default: the global OTel meter).
	Meter metric.Meter
}

// Service implements the invoice API.
type Service struct {
	store Store
	rates RateSource
	cache StatusCache
	cfg   Config
	log   *slog.Logger

	rateAge metric.Float64Histogram
	created metric.Int64Counter
	cacheOp metric.Int64Counter
}

// NewService returns the service. cache may be nil.
func NewService(store Store, rates RateSource, cache StatusCache, cfg Config, log *slog.Logger) (*Service, error) {
	if cfg.InvoiceTTL <= 0 || cfg.RateMaxAge <= 0 {
		return nil, errors.New("invoice: InvoiceTTL and RateMaxAge must be positive")
	}
	if cfg.USDT != nil {
		u := *cfg.USDT
		u.Contract = strings.ToLower(u.Contract)
		if u.Kind != KindERC20 || !addressRE.MatchString(u.Contract) || u.Decimals < 0 || u.Decimals > 77 {
			return nil, errors.New("invoice: USDT must be an ERC20 asset with a 20-byte contract address and decimals 0..77")
		}
		cfg.USDT = &u
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cache == nil {
		cache = NopCache{}
	}
	m := cfg.Meter
	if m == nil {
		m = otel.Meter(meterName)
	}
	s := &Service{store: store, rates: rates, cache: cache, cfg: cfg, log: log}
	var err error
	if s.rateAge, err = m.Float64Histogram("slopay_invoice_rate_age_seconds",
		metric.WithDescription("Age of the rate fixed in a new invoice (SRS 4.1.3: at most 60 s)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(1, 2, 5, 10, 15, 20, 30, 45, 60)); err != nil {
		return nil, err
	}
	if s.created, err = m.Int64Counter("slopay_invoices_created",
		metric.WithDescription("Invoices created, by asset kind")); err != nil {
		return nil, err
	}
	if s.cacheOp, err = m.Int64Counter("slopay_invoice_status_cache",
		metric.WithDescription("Status cache lookups, by result: hit or miss")); err != nil {
		return nil, err
	}
	return s, nil
}

var addressRE = regexp.MustCompile(`^0x[0-9a-f]{40}$`)

// pairOf maps the requested asset to the rate-service pair and the invoice asset. The USDT amount is
// converted at the USDT-USD rate (SRS 4.1.3, FR-10) with the decimals of the configured token.
func (s *Service) pairOf(asset string) (string, Asset, error) {
	switch asset {
	case "", RequestETH:
		return "ETH-USD", ETH, nil
	case RequestUSDT:
		if s.cfg.USDT == nil {
			return "", Asset{}, &ValidationError{Field: "asset", Message: "USDT is not configured"}
		}
		return "USDT-USD", *s.cfg.USDT, nil
	default:
		return "", Asset{}, &ValidationError{Field: "asset", Message: "unknown asset"}
	}
}

// RequestHash is the SHA-256 of the canonical request: a repeat is the same request when the amount, the
// order id and the asset (with the default applied) are equal, whatever the JSON formatting.
func RequestHash(r CreateRequest) []byte {
	asset := r.Asset
	if asset == "" {
		asset = RequestETH
	}
	b, _ := json.Marshal(struct {
		AmountUSDCents int64  `json:"amountUsdCents"`
		OrderID        string `json:"orderId"`
		Asset          string `json:"asset"`
	}{r.AmountUSDCents, r.OrderID, asset})
	h := sha256.Sum256(b)
	return h[:]
}

// Create creates an invoice or returns the one created earlier with the same idempotency key
// (created=false). It returns ErrKeyReuse for the key with another request, ErrNoXpub without the merchant
// projection, *ValidationError for an unsupported or unconfigured asset and *RateError without a usable rate.
func (s *Service) Create(ctx context.Context, merchantID uuid.UUID, key string, r CreateRequest) (Invoice, bool, error) {
	hash := RequestHash(r)
	// The PostgreSQL unique index is the source of truth for idempotency, also after the Valkey cache was
	// lost (NFR-06). The lookup comes before the rate so that a repeat never depends on rate-service.
	if inv, h, err := s.store.FindByKey(ctx, merchantID, key); err == nil {
		return s.repeat(inv, h, hash)
	} else if !errors.Is(err, ErrNotFound) {
		return Invoice{}, false, err
	}
	if _, err := s.store.Merchant(ctx, merchantID); err != nil {
		return Invoice{}, false, err
	}
	pair, asset, err := s.pairOf(r.Asset)
	if err != nil {
		return Invoice{}, false, err
	}
	now := s.cfg.Now().UTC().Truncate(time.Microsecond)
	q, rate, err := s.quote(ctx, pair, now)
	if err != nil {
		return Invoice{}, false, err
	}
	amount, err := money.USDToAsset(r.AmountUSDCents, rate, asset.Decimals)
	if err != nil {
		return Invoice{}, false, fmt.Errorf("invoice: convert amount: %w", err)
	}
	d := Draft{
		Invoice: Invoice{
			ID: uuid.New(), MerchantID: merchantID, OrderID: r.OrderID, AmountUSDCents: r.AmountUSDCents,
			Asset: asset, Rate: q.Price, RateQuotedAt: q.QuotedAt.UTC().Truncate(time.Microsecond), Amount: amount,
			Status: StatusNew, Sequence: 1, Received: newZero(), CreatedAt: now, ExpiresAt: now.Add(s.cfg.InvoiceTTL),
		},
		IdempotencyKey: key,
		RequestHash:    hash,
	}
	inv, h, created, err := s.store.Create(ctx, d, Derive)
	if err != nil {
		return Invoice{}, false, err
	}
	if !created {
		// A concurrent request with the key won the merchant lock.
		return s.repeat(inv, h, hash)
	}
	s.rateAge.Record(ctx, now.Sub(q.QuotedAt).Seconds())
	s.created.Add(ctx, 1, metric.WithAttributes(attribute.String("asset", asset.Kind)))
	s.cache.Set(ctx, inv.Page())
	s.log.InfoContext(ctx, "invoice created", "invoice_id", inv.ID, "merchant_id", merchantID,
		"derivation_index", inv.DerivationIndex)
	return inv, true, nil
}

func (s *Service) repeat(inv Invoice, stored, hash []byte) (Invoice, bool, error) {
	if !bytes.Equal(stored, hash) {
		return Invoice{}, false, ErrKeyReuse
	}
	return inv, false, nil
}

// quote returns the rate of the pair if it is not older than RateMaxAge by both the age rate-service
// measured and the clock of this service.
func (s *Service) quote(ctx context.Context, pair string, now time.Time) (Quote, money.Rate, error) {
	q, err := s.rates.Rate(ctx, pair)
	if err != nil {
		var re *RateError
		if errors.As(err, &re) {
			return Quote{}, money.Rate{}, err
		}
		return Quote{}, money.Rate{}, &RateError{Reason: ReasonUnavailable, Err: err}
	}
	if q.Age > s.cfg.RateMaxAge || now.Sub(q.QuotedAt) > s.cfg.RateMaxAge {
		return Quote{}, money.Rate{}, &RateError{Reason: ReasonStale,
			Err: fmt.Errorf("rate of %s quoted at %s is older than %s", pair, q.QuotedAt.Format(time.RFC3339), s.cfg.RateMaxAge)}
	}
	rate, err := money.ParseRate(q.Price)
	if err != nil {
		return Quote{}, money.Rate{}, &RateError{Reason: ReasonUnavailable, Err: err}
	}
	return q, rate, nil
}

// Derive is the DeriveFunc of the service: the address m/44'/60'/0'/0/index of the xpub in lower case, the
// form of the contracts.
func Derive(xpub string, index uint32) (string, error) {
	a, err := hdwallet.Derive(xpub, index)
	if err != nil {
		return "", err
	}
	return strings.ToLower(a), nil
}

// Get returns an invoice of the merchant.
func (s *Service) Get(ctx context.Context, merchantID, id uuid.UUID) (Invoice, error) {
	return s.store.Get(ctx, merchantID, id)
}

// List returns a page of the merchant invoices and the cursor of the next page ("" for the last one).
func (s *Service) List(ctx context.Context, merchantID uuid.UUID, f Filter, cursor string) ([]Invoice, string, error) {
	if cursor != "" {
		c, err := DecodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		f.After = &c
	}
	if f.Limit <= 0 {
		return nil, "", &ValidationError{Field: "limit", Message: "must be positive"}
	}
	items, err := s.store.List(ctx, merchantID, f)
	if err != nil {
		return nil, "", err
	}
	if len(items) <= f.Limit {
		return items, "", nil
	}
	items = items[:f.Limit]
	last := items[len(items)-1]
	return items, EncodeCursor(Cursor{CreatedAt: last.CreatedAt, ID: last.ID}), nil
}

// PaymentPage returns the payment page: it is read from PostgreSQL and refreshes the status cache.
func (s *Service) PaymentPage(ctx context.Context, id uuid.UUID) (Page, error) {
	inv, err := s.store.GetPublic(ctx, id)
	if err != nil {
		return Page{}, err
	}
	p := inv.Page()
	s.cache.Set(ctx, p)
	return p, nil
}

// PaymentStatus returns the payment page for polling: from the status cache, or from PostgreSQL on a miss.
func (s *Service) PaymentStatus(ctx context.Context, id uuid.UUID) (Page, error) {
	if p, ok := s.cache.Get(ctx, id); ok {
		s.cacheOp.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "hit")))
		return p, nil
	}
	s.cacheOp.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "miss")))
	return s.PaymentPage(ctx, id)
}

// ApplyMerchant applies a merchant.updated to the projection; an older version than the stored one is
// ignored.
func (s *Service) ApplyMerchant(ctx context.Context, m Merchant) error {
	applied, err := s.store.UpsertMerchant(ctx, m)
	if err != nil {
		return err
	}
	if applied {
		s.log.InfoContext(ctx, "merchant projection updated", "merchant_id", m.ID, "version", m.Version)
	} else {
		s.log.DebugContext(ctx, "merchant event ignored: not newer than the projection", "merchant_id", m.ID,
			"version", m.Version)
	}
	return nil
}
