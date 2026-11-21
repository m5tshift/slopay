package invoice_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/pkg/hdwallet"
	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoice"
	"github.com/m5tshift/weigate/services/invoice-service/internal/invoicetest"
)

var (
	t0       = time.Date(2026, 11, 20, 12, 0, 0, 123456789, time.UTC)
	merchant = uuid.MustParse("6f1d7d7e-2b4f-4a7c-9a51-1c2d3e4f5a6b")
	quiet    = slog.New(slog.NewTextHandler(io.Discard, nil))
)

type fixture struct {
	store *invoice.MemStore
	rates *invoicetest.Rates
	cache *invoicetest.Cache
	svc   *invoice.Service
	now   time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{store: invoice.NewMemStore(), rates: invoicetest.NewRates("3000", t0.Add(-5*time.Second)),
		cache: invoicetest.NewCache(), now: t0}
	var err error
	f.svc, err = invoice.NewService(f.store, f.rates, f.cache, invoice.Config{
		InvoiceTTL: 15 * time.Minute, RateMaxAge: 60 * time.Second, Now: func() time.Time { return f.now },
	}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.UpsertMerchant(context.Background(), invoice.Merchant{ID: merchant, Version: 1, Xpub: invoicetest.Xpub, ToleranceBps: 100}); err != nil {
		t.Fatal(err)
	}
	return f
}

func req(cents int64, order string) invoice.CreateRequest {
	return invoice.CreateRequest{AmountUSDCents: cents, OrderID: order}
}

func address(t *testing.T, index uint32) string {
	t.Helper()
	a, err := hdwallet.Derive(invoicetest.Xpub, index)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ToLower(a)
}

func TestNewServiceConfig(t *testing.T) {
	if _, err := invoice.NewService(invoice.NewMemStore(), nil, nil, invoice.Config{RateMaxAge: time.Minute}, quiet); err == nil {
		t.Error("zero InvoiceTTL accepted")
	}
	if _, err := invoice.NewService(invoice.NewMemStore(), nil, nil, invoice.Config{InvoiceTTL: time.Minute}, quiet); err == nil {
		t.Error("zero RateMaxAge accepted")
	}
	if _, err := invoice.NewService(invoice.NewMemStore(), nil, nil, invoice.Config{InvoiceTTL: time.Minute, RateMaxAge: time.Minute}, quiet); err != nil {
		t.Errorf("nil cache and clock: %v", err)
	}
}

func TestCreate(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	inv, created, err := f.svc.Create(ctx, merchant, "k1", req(1000, "order-1"))
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	now := t0.Truncate(time.Microsecond)
	// 10 USD / 3000 USD per ETH = 0.00333... ETH, rounded up in wei (SRS 4.1.3).
	if inv.Amount.String() != "3333333333333334" {
		t.Errorf("amount %s", inv.Amount)
	}
	if inv.Asset != invoice.ETH || inv.Rate != "3000" || inv.Status != invoice.StatusNew || inv.Sequence != 1 ||
		inv.MerchantID != merchant || inv.OrderID != "order-1" || inv.AmountUSDCents != 1000 ||
		inv.Received.Sign() != 0 || inv.Confirmations != 0 || inv.Overpaid {
		t.Errorf("invoice %+v", inv)
	}
	if !inv.CreatedAt.Equal(now) || !inv.ExpiresAt.Equal(now.Add(15*time.Minute)) {
		t.Errorf("times %s %s", inv.CreatedAt, inv.ExpiresAt)
	}
	if inv.DerivationIndex != 0 || inv.Address != address(t, 0) {
		t.Errorf("address %d %s", inv.DerivationIndex, inv.Address)
	}
	if p, ok := f.cache.Get(ctx, inv.ID); !ok || p != inv.Page() {
		t.Errorf("status cache not filled: %+v %v", p, ok)
	}
	evs := f.store.Events()
	if len(evs) != 1 {
		t.Fatalf("events %d", len(evs))
	}
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(evs[0]); err != nil {
		t.Fatalf("invoice.created does not match its schema: %v", err)
	}
	var d invoice.CreatedData
	if err := json.Unmarshal(evs[0].Data, &d); err != nil {
		t.Fatal(err)
	}
	if evs[0].Subject != inv.ID.String() || evs[0].Type != "invoice.created" || d.Address != inv.Address ||
		d.Amount != "3333333333333334" || !d.ExpiresAt.Equal(inv.ExpiresAt) {
		t.Errorf("event %+v %+v", evs[0], d)
	}

	second, _, err := f.svc.Create(ctx, merchant, "k2", req(1999, "order-2"))
	if err != nil {
		t.Fatal(err)
	}
	if second.DerivationIndex != 1 || second.Address != address(t, 1) || second.Amount.String() != "6663333333333334" {
		t.Errorf("second invoice %d %s %s", second.DerivationIndex, second.Address, second.Amount)
	}
}

func TestCreateIdempotent(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	first, _, err := f.svc.Create(ctx, merchant, "k1", req(1000, "order-1"))
	if err != nil {
		t.Fatal(err)
	}
	calls := f.rates.Calls()
	// A repeat needs neither rate-service nor the cache: the store is the source of truth (NFR-06).
	f.rates.Set(invoice.Quote{}, errors.New("rate-service down"))
	f.cache.Flush()
	f.now = t0.Add(time.Hour)
	again, created, err := f.svc.Create(ctx, merchant, "k1", invoice.CreateRequest{AmountUSDCents: 1000, OrderID: "order-1", Asset: invoice.RequestETH})
	if err != nil || created {
		t.Fatalf("repeat: %v %v", created, err)
	}
	if again.ID != first.ID || again.Address != first.Address || f.rates.Calls() != calls || len(f.store.Events()) != 1 {
		t.Errorf("repeat created a new invoice or called rate-service: %+v", again)
	}
	for name, r := range map[string]invoice.CreateRequest{
		"amount": req(1001, "order-1"),
		"order":  req(1000, "order-2"),
		"asset":  {AmountUSDCents: 1000, OrderID: "order-1", Asset: invoice.RequestUSDT},
	} {
		if _, _, err := f.svc.Create(ctx, merchant, "k1", r); !errors.Is(err, invoice.ErrKeyReuse) {
			t.Errorf("%s: %v, want ErrKeyReuse", name, err)
		}
	}
	// The key is per merchant.
	other := uuid.New()
	if _, err := f.store.UpsertMerchant(ctx, invoice.Merchant{ID: other, Version: 1, Xpub: invoicetest.Xpub}); err != nil {
		t.Fatal(err)
	}
	f.rates.Set(invoice.Quote{Price: "3000", QuotedAt: f.now}, nil)
	inv, created, err := f.svc.Create(ctx, other, "k1", req(1000, "order-1"))
	if err != nil || !created || inv.ID == first.ID {
		t.Errorf("other merchant with the same key: %v %v", created, err)
	}
}

func TestRequestHash(t *testing.T) {
	a := invoice.RequestHash(req(1000, "o"))
	if string(a) != string(invoice.RequestHash(invoice.CreateRequest{AmountUSDCents: 1000, OrderID: "o", Asset: "ETH"})) {
		t.Error("default asset changes the hash")
	}
	if string(a) == string(invoice.RequestHash(req(1000, "o2"))) || len(a) != 32 {
		t.Error("hash does not identify the request")
	}
}

func TestCreateConcurrentSameKey(t *testing.T) {
	f := newFixture(t)
	const n = 32
	var wg sync.WaitGroup
	ids := make([]uuid.UUID, n)
	createdCount := make([]bool, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			inv, created, err := f.svc.Create(context.Background(), merchant, "same", req(500, "order"))
			ids[i], createdCount[i], errs[i] = inv.ID, created, err
		})
	}
	wg.Wait()
	created := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Errorf("request %d got invoice %s, want %s", i, ids[i], ids[0])
		}
		if createdCount[i] {
			created++
		}
	}
	if created != 1 || len(f.store.Events()) != 1 {
		t.Errorf("created %d invoices, %d events; want 1", created, len(f.store.Events()))
	}
}

func TestCreateConcurrentKeysGetDistinctIndexes(t *testing.T) {
	f := newFixture(t)
	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[uint32]string{}
	for i := range n {
		wg.Go(func() {
			inv, _, err := f.svc.Create(context.Background(), merchant, fmt.Sprintf("k%d", i), req(500, "order"))
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if prev, ok := seen[inv.DerivationIndex]; ok {
				t.Errorf("index %d given twice (%s)", inv.DerivationIndex, prev)
			}
			seen[inv.DerivationIndex] = inv.Address
		})
	}
	wg.Wait()
	for i := range uint32(n) {
		if _, ok := seen[i]; !ok {
			t.Errorf("index %d skipped", i)
		}
	}
}

func TestCreateErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("no merchant projection", func(t *testing.T) {
		f := newFixture(t)
		if _, _, err := f.svc.Create(ctx, uuid.New(), "k", req(100, "o")); !errors.Is(err, invoice.ErrNoXpub) {
			t.Errorf("%v, want ErrNoXpub", err)
		}
	})
	for name, tc := range map[string]struct {
		asset string
		field string
	}{"usdt not configured": {invoice.RequestUSDT, "asset"}, "unknown": {"BTC", "asset"}} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			_, _, err := f.svc.Create(ctx, merchant, "k", invoice.CreateRequest{AmountUSDCents: 100, OrderID: "o", Asset: tc.asset})
			var ve *invoice.ValidationError
			if !errors.As(err, &ve) || ve.Field != tc.field || ve.Error() == "" {
				t.Errorf("%v, want a validation error of %s", err, tc.field)
			}
			if f.rates.Calls() != 0 {
				t.Error("rate-service called for an unsupported asset")
			}
		})
	}
}

func TestCreateRate(t *testing.T) {
	ctx := context.Background()
	upstream := &invoice.RateError{Reason: invoice.ReasonDiverged, RetryAfter: 7 * time.Second}
	cases := map[string]struct {
		quote  invoice.Quote
		err    error
		reason string
		retry  time.Duration
	}{
		"rate-service reason kept": {err: upstream, reason: invoice.ReasonDiverged, retry: 7 * time.Second},
		"other error":              {err: errors.New("boom"), reason: invoice.ReasonUnavailable},
		"stale by ageSeconds":      {quote: invoice.Quote{Price: "3000", QuotedAt: t0, Age: 61 * time.Second}, reason: invoice.ReasonStale},
		"stale by quotedAt":        {quote: invoice.Quote{Price: "3000", QuotedAt: t0.Add(-61 * time.Second)}, reason: invoice.ReasonStale},
		"invalid price":            {quote: invoice.Quote{Price: "-1", QuotedAt: t0}, reason: invoice.ReasonUnavailable},
		"zero price":               {quote: invoice.Quote{Price: "0", QuotedAt: t0}, reason: invoice.ReasonUnavailable},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.rates.Set(tc.quote, tc.err)
			_, _, err := f.svc.Create(ctx, merchant, "k", req(100, "o"))
			var re *invoice.RateError
			if !errors.As(err, &re) || re.Reason != tc.reason || re.RetryAfter != tc.retry || re.Error() == "" {
				t.Fatalf("%v, want rate %s", err, tc.reason)
			}
			if len(f.store.Events()) != 0 {
				t.Error("invoice created without a usable rate")
			}
		})
	}
	t.Run("exactly the max age", func(t *testing.T) {
		f := newFixture(t)
		f.rates.Set(invoice.Quote{Price: "3000", QuotedAt: t0.Add(-60 * time.Second), Age: 60 * time.Second}, nil)
		if _, created, err := f.svc.Create(ctx, merchant, "k", req(100, "o")); err != nil || !created {
			t.Errorf("rate of 60 s refused: %v", err)
		}
	})
	if (&invoice.RateError{Reason: invoice.ReasonStale}).Error() != "invoice: rate stale" {
		t.Error("RateError text")
	}
}

type failingStore struct {
	*invoice.MemStore
	findErr, createErr error
	raceKey            bool
}

func (s *failingStore) FindByKey(ctx context.Context, m uuid.UUID, key string) (invoice.Invoice, []byte, error) {
	if s.findErr != nil {
		return invoice.Invoice{}, nil, s.findErr
	}
	return s.MemStore.FindByKey(ctx, m, key)
}

func (s *failingStore) Create(ctx context.Context, d invoice.Draft, derive invoice.DeriveFunc) (invoice.Invoice, []byte, bool, error) {
	if s.createErr != nil {
		return invoice.Invoice{}, nil, false, s.createErr
	}
	if s.raceKey {
		// Another request with the key and another body committed between the lookup and the lock.
		other := d
		other.ID = uuid.New()
		other.RequestHash = invoice.RequestHash(req(1, "other"))
		if _, _, _, err := s.MemStore.Create(ctx, other, derive); err != nil {
			return invoice.Invoice{}, nil, false, err
		}
	}
	return s.MemStore.Create(ctx, d, derive)
}

func TestCreateStoreErrors(t *testing.T) {
	ctx := context.Background()
	mem := invoice.NewMemStore()
	if _, err := mem.UpsertMerchant(ctx, invoice.Merchant{ID: merchant, Version: 1, Xpub: invoicetest.Xpub}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("db down")
	for name, st := range map[string]*failingStore{
		"lookup": {MemStore: mem, findErr: boom},
		"create": {MemStore: mem, createErr: boom},
	} {
		svc, err := invoice.NewService(st, invoicetest.NewRates("3000", time.Now()), nil,
			invoice.Config{InvoiceTTL: time.Minute, RateMaxAge: time.Minute}, quiet)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := svc.Create(ctx, merchant, name, req(100, "o")); !errors.Is(err, boom) {
			t.Errorf("%s: %v", name, err)
		}
	}
	svc, err := invoice.NewService(&failingStore{MemStore: mem, raceKey: true}, invoicetest.NewRates("3000", time.Now()), nil,
		invoice.Config{InvoiceTTL: time.Minute, RateMaxAge: time.Minute}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.Create(ctx, merchant, "raced", req(100, "o")); !errors.Is(err, invoice.ErrKeyReuse) {
		t.Errorf("key taken under the lock with another body: %v", err)
	}
}

func TestNextAddress(t *testing.T) {
	calls := 0
	derive := func(_ string, i uint32) (string, error) {
		calls++
		if i < 3 {
			return "", hdwallet.ErrInvalidChildKey
		}
		return fmt.Sprintf("addr-%d", i), nil
	}
	addr, index, next, err := invoice.NextAddress("x", 1, derive)
	if err != nil || addr != "addr-3" || index != 3 || next != 4 || calls != 3 {
		t.Errorf("skip invalid child keys: %s %d %d %v (%d calls)", addr, index, next, err, calls)
	}
	boom := errors.New("bad xpub")
	if _, _, _, err := invoice.NextAddress("x", 0, func(string, uint32) (string, error) { return "", boom }); !errors.Is(err, boom) {
		t.Errorf("derive error: %v", err)
	}
	if _, _, _, err := invoice.NextAddress("x", invoice.MaxDerivationIndex+1, derive); !errors.Is(err, invoice.ErrIndexExhausted) {
		t.Errorf("exhausted: %v", err)
	}
	if _, _, _, err := invoice.NextAddress("x", invoice.MaxDerivationIndex, func(string, uint32) (string, error) {
		return "", hdwallet.ErrInvalidChildKey
	}); !errors.Is(err, invoice.ErrIndexExhausted) {
		t.Errorf("last index invalid: %v", err)
	}
}

func TestDerive(t *testing.T) {
	a, err := invoice.Derive(invoicetest.Xpub, 5)
	if err != nil || a != address(t, 5) || a != strings.ToLower(a) || !strings.HasPrefix(a, "0x") {
		t.Errorf("derive: %s %v", a, err)
	}
	if _, err := invoice.Derive("not-an-xpub", 0); err == nil {
		t.Error("invalid xpub accepted")
	}
}

func TestGetAndPages(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	inv, _, err := f.svc.Create(ctx, merchant, "k", req(100, "o"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.svc.Get(ctx, merchant, inv.ID); err != nil || got.ID != inv.ID {
		t.Errorf("get: %v", err)
	}
	if _, err := f.svc.Get(ctx, uuid.New(), inv.ID); !errors.Is(err, invoice.ErrNotFound) {
		t.Errorf("other merchant: %v", err)
	}
	if _, err := f.svc.Get(ctx, merchant, uuid.New()); !errors.Is(err, invoice.ErrNotFound) {
		t.Errorf("unknown id: %v", err)
	}

	// The status of polling comes from the cache while the entry lives.
	f.store.SetStatus(inv.ID, invoice.StatusDetected)
	if p, err := f.svc.PaymentStatus(ctx, inv.ID); err != nil || p.Status != invoice.StatusNew {
		t.Errorf("cache hit: %+v %v", p, err)
	}
	// The payment page reads PostgreSQL and refreshes the cache.
	if p, err := f.svc.PaymentPage(ctx, inv.ID); err != nil || p.Status != invoice.StatusDetected {
		t.Errorf("payment page: %+v %v", p, err)
	}
	if p, err := f.svc.PaymentStatus(ctx, inv.ID); err != nil || p.Status != invoice.StatusDetected {
		t.Errorf("refreshed cache: %+v %v", p, err)
	}
	// A miss reads the store and fills the cache.
	f.cache.Flush()
	f.store.SetStatus(inv.ID, invoice.StatusConfirming)
	sets := f.cache.Sets()
	if p, err := f.svc.PaymentStatus(ctx, inv.ID); err != nil || p.Status != invoice.StatusConfirming || f.cache.Sets() != sets+1 {
		t.Errorf("cache miss: %+v %v", p, err)
	}
	if _, err := f.svc.PaymentStatus(ctx, uuid.New()); !errors.Is(err, invoice.ErrNotFound) {
		t.Errorf("unknown status: %v", err)
	}
	if _, err := f.svc.PaymentPage(ctx, uuid.New()); !errors.Is(err, invoice.ErrNotFound) {
		t.Errorf("unknown page: %v", err)
	}
}

func TestNopCache(t *testing.T) {
	var c invoice.NopCache
	c.Set(context.Background(), invoice.Page{})
	if _, ok := c.Get(context.Background(), uuid.New()); ok {
		t.Error("NopCache hit")
	}
}

func TestApplyMerchant(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, _, err := f.svc.Create(ctx, merchant, "k1", req(100, "o")); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.ApplyMerchant(ctx, invoice.Merchant{ID: merchant, Version: 3, Xpub: invoicetest.Xpub, ToleranceBps: 200}); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.ApplyMerchant(ctx, invoice.Merchant{ID: merchant, Version: 2, Xpub: "older", ToleranceBps: 0}); err != nil {
		t.Fatal(err)
	}
	m, err := f.store.Merchant(ctx, merchant)
	if err != nil || m.Version != 3 || m.ToleranceBps != 200 || m.Xpub != invoicetest.Xpub {
		t.Errorf("projection %+v %v", m, err)
	}
	// The derivation counter survives the update.
	inv, _, err := f.svc.Create(ctx, merchant, "k2", req(100, "o"))
	if err != nil || inv.DerivationIndex != 1 {
		t.Errorf("index after update: %d %v", inv.DerivationIndex, err)
	}
	if err := f.svc.ApplyMerchant(ctx, invoice.Merchant{ID: merchant, Version: 3}); err != nil {
		t.Errorf("repeat: %v", err)
	}
}

type brokenUpsert struct{ *invoice.MemStore }

func (brokenUpsert) UpsertMerchant(context.Context, invoice.Merchant) (bool, error) {
	return false, errors.New("db down")
}

func TestApplyMerchantError(t *testing.T) {
	svc, err := invoice.NewService(brokenUpsert{invoice.NewMemStore()}, nil, nil,
		invoice.Config{InvoiceTTL: time.Minute, RateMaxAge: time.Minute}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ApplyMerchant(context.Background(), invoice.Merchant{ID: merchant, Version: 1}); err == nil {
		t.Error("error swallowed")
	}
}

const usdtContract = "0x5fbdb2315678afecb367f032d93f642f64180aa3"

func usdtFixture(t *testing.T, price string) *fixture {
	t.Helper()
	f := &fixture{store: invoice.NewMemStore(), rates: invoicetest.NewRates(price, t0.Add(-5*time.Second)),
		cache: invoicetest.NewCache(), now: t0}
	var err error
	f.svc, err = invoice.NewService(f.store, f.rates, f.cache, invoice.Config{
		InvoiceTTL: 15 * time.Minute, RateMaxAge: 60 * time.Second, Now: func() time.Time { return f.now },
		// Upper case on purpose: the service stores the contract in lower case.
		USDT: &invoice.Asset{Kind: invoice.KindERC20, Contract: strings.ToUpper(usdtContract[:2]) + usdtContract[2:], Decimals: 6},
	}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.UpsertMerchant(context.Background(), invoice.Merchant{ID: merchant, Version: 1, Xpub: invoicetest.Xpub, ToleranceBps: 100}); err != nil {
		t.Fatal(err)
	}
	return f
}

// A USDT invoice is priced at the USDT-USD rate in token base units, rounded up (SRS 4.1.3, FR-10).
func TestCreateUSDT(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		cents  int64
		price  string
		amount string
	}{
		"par":           {1000, "1", "10000000"},
		"rounded up":    {1000, "1.0003", "9997001"}, // 10 / 1.0003 = 9.99700089... USDT
		"below par":     {1, "0.9998", "10003"},      // 0.01 / 0.9998 = 0.010002000...
		"exact":         {250, "1.25", "2000000"},    // 2.50 / 1.25
		"one base unit": {1, "1000000", "1"},         // 0.01 / 1000000 = 1e-8 USDT -> 1e-6
	} {
		t.Run(name, func(t *testing.T) {
			f := usdtFixture(t, tc.price)
			inv, created, err := f.svc.Create(ctx, merchant, "k", invoice.CreateRequest{AmountUSDCents: tc.cents, OrderID: "o", Asset: invoice.RequestUSDT})
			if err != nil || !created {
				t.Fatalf("%v %v", created, err)
			}
			if inv.Amount.String() != tc.amount {
				t.Errorf("amount %s, want %s", inv.Amount, tc.amount)
			}
			want := invoice.Asset{Kind: invoice.KindERC20, Contract: usdtContract, Decimals: 6}
			if inv.Asset != want || inv.Rate != tc.price {
				t.Errorf("asset %+v rate %s", inv.Asset, inv.Rate)
			}
			if f.rates.Pair() != "USDT-USD" {
				t.Errorf("pair %q", f.rates.Pair())
			}
			if p := inv.Page(); p.Asset != want || p.Amount != tc.amount {
				t.Errorf("page %+v", p)
			}
		})
	}
}

// ETH invoices of a service with USDT configured still use ETH-USD, and the asset is part of the request hash.
func TestUSDTDoesNotChangeETH(t *testing.T) {
	ctx := context.Background()
	f := usdtFixture(t, "3000")
	inv, _, err := f.svc.Create(ctx, merchant, "k", req(1000, "o"))
	if err != nil || inv.Asset != invoice.ETH || f.rates.Pair() != "ETH-USD" || inv.Amount.String() != "3333333333333334" {
		t.Fatalf("%+v %q %v", inv, f.rates.Pair(), err)
	}
	_, _, err = f.svc.Create(ctx, merchant, "k", invoice.CreateRequest{AmountUSDCents: 1000, OrderID: "o", Asset: invoice.RequestUSDT})
	if !errors.Is(err, invoice.ErrKeyReuse) {
		t.Errorf("%v, want ErrKeyReuse", err)
	}
}

func TestUSDTRateErrors(t *testing.T) {
	f := usdtFixture(t, "1")
	f.rates.Set(invoice.Quote{Price: "1", QuotedAt: t0.Add(-2 * time.Minute)}, nil)
	_, _, err := f.svc.Create(context.Background(), merchant, "k", invoice.CreateRequest{AmountUSDCents: 100, OrderID: "o", Asset: invoice.RequestUSDT})
	var re *invoice.RateError
	if !errors.As(err, &re) || re.Reason != invoice.ReasonStale {
		t.Errorf("%v, want a stale rate", err)
	}
}

func TestNewServiceUSDTConfig(t *testing.T) {
	for name, a := range map[string]invoice.Asset{
		"eth kind":      {Kind: invoice.KindETH, Decimals: 18},
		"bad address":   {Kind: invoice.KindERC20, Contract: "0x12", Decimals: 6},
		"no 0x":         {Kind: invoice.KindERC20, Contract: strings.Repeat("a", 40), Decimals: 6},
		"decimals low":  {Kind: invoice.KindERC20, Contract: usdtContract, Decimals: -1},
		"decimals high": {Kind: invoice.KindERC20, Contract: usdtContract, Decimals: 78},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := invoice.NewService(invoice.NewMemStore(), nil, nil,
				invoice.Config{InvoiceTTL: time.Minute, RateMaxAge: time.Minute, USDT: &a}, quiet)
			if err == nil {
				t.Error("accepted")
			}
		})
	}
}
