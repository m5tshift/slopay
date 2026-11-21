package ledger_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledger"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledgertest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newCodec(t testing.TB) *kafka.Codec {
	t.Helper()
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

// clock returns a clock that advances by a second on every call, starting at t0.
func clock() func() time.Time {
	var mu sync.Mutex
	at := t0
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		at = at.Add(time.Second)
		return at
	}
}

func newService(t testing.TB, store ledger.Store) *ledger.Service {
	t.Helper()
	s, err := ledger.NewService(store, ledger.Config{FeeBps: 50}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	ledger.SetNow(s, clock())
	return s
}

func ethInvoice(n uint64) ledgertest.Invoice {
	return ledgertest.Invoice{
		ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(strconv.FormatUint(n, 10))), Merchant: merchantA,
		Address: ledgertest.Address(n), Asset: ledger.Asset{Kind: ledger.KindETH, Decimals: 18},
	}
}

func tokenInvoice(n uint64) ledgertest.Invoice {
	inv := ethInvoice(n)
	inv.Asset = ledger.Asset{Kind: ledger.KindERC20, Contract: ledgertest.ERC20Contract, Decimals: 6}
	return inv
}

var errDown = errors.New("postgres is down")

// faultyStore fails the selected methods; Record fails the first failRecord calls.
type faultyStore struct {
	ledger.Store
	mu                                   sync.Mutex
	failRecord                           int
	records                              int
	exists, totals, postings, addrTotals bool
}

func (f *faultyStore) Record(ctx context.Context, m uuid.UUID, ps []ledger.Posting) (int, error) {
	f.mu.Lock()
	f.records++
	fail := f.records <= f.failRecord
	f.mu.Unlock()
	if fail {
		return 0, errDown
	}
	return f.Store.Record(ctx, m, ps)
}

func (f *faultyStore) MerchantExists(ctx context.Context, m uuid.UUID) (bool, error) {
	if f.exists {
		return false, errDown
	}
	return f.Store.MerchantExists(ctx, m)
}

func (f *faultyStore) MerchantTotals(ctx context.Context, m uuid.UUID, from, to time.Time) ([]ledger.Totals, error) {
	if f.totals {
		return nil, errDown
	}
	return f.Store.MerchantTotals(ctx, m, from, to)
}

func (f *faultyStore) MerchantPostings(ctx context.Context, m uuid.UUID, from, to time.Time, after *ledger.Cursor, limit int) ([]ledger.Posting, error) {
	if f.postings {
		return nil, errDown
	}
	return f.Store.MerchantPostings(ctx, m, from, to, after, limit)
}

func (f *faultyStore) AddressTotals(ctx context.Context, a []string) (map[string][]ledger.Totals, error) {
	if f.addrTotals {
		return nil, errDown
	}
	return f.Store.AddressTotals(ctx, a)
}

func (f *faultyStore) AddressPostings(ctx context.Context, a string, after *ledger.Cursor, limit int) ([]ledger.Posting, error) {
	if f.postings {
		return nil, errDown
	}
	return f.Store.AddressPostings(ctx, a, after, limit)
}
