package invoice_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoicetest"
)

// seed creates n invoices one second apart, the newest last; two of them share a createdAt to test ties.
func seed(t *testing.T, f *fixture, n int) []invoice.Invoice {
	t.Helper()
	var out []invoice.Invoice
	for i := range n {
		f.now = t0.Add(time.Duration(i) * time.Second)
		if i == 3 {
			f.now = t0.Add(2 * time.Second)
		}
		f.rates.Set(invoice.Quote{Price: "3000", QuotedAt: f.now}, nil)
		inv, _, err := f.svc.Create(context.Background(), merchant, fmt.Sprintf("k%d", i), req(100, fmt.Sprintf("order-%d", i%2)))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, inv)
	}
	return out
}

func TestListPagination(t *testing.T) {
	f := newFixture(t)
	seed(t, f, 7)
	ctx := context.Background()
	var all []invoice.Invoice
	cursor := ""
	pages := 0
	for {
		items, next, err := f.svc.List(ctx, merchant, invoice.Filter{Limit: 3}, cursor)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, items...)
		pages++
		if next == "" {
			break
		}
		cursor = next
	}
	if len(all) != 7 || pages != 3 {
		t.Fatalf("got %d invoices in %d pages", len(all), pages)
	}
	seen := map[uuid.UUID]bool{}
	for i, inv := range all {
		if seen[inv.ID] {
			t.Errorf("invoice %s twice", inv.ID)
		}
		seen[inv.ID] = true
		if i > 0 && !(invoice.Cursor{CreatedAt: all[i-1].CreatedAt, ID: all[i-1].ID}).Before(inv) {
			t.Errorf("order broken at %d", i)
		}
	}
	// Exactly a page: no next cursor.
	if _, next, err := f.svc.List(ctx, merchant, invoice.Filter{Limit: 7}, ""); err != nil || next != "" {
		t.Errorf("full page: %q %v", next, err)
	}
	// Another merchant sees nothing.
	if items, _, err := f.svc.List(ctx, uuid.New(), invoice.Filter{Limit: 10}, ""); err != nil || len(items) != 0 {
		t.Errorf("other merchant: %d %v", len(items), err)
	}
}

func TestListFilters(t *testing.T) {
	f := newFixture(t)
	invs := seed(t, f, 6)
	f.store.SetStatus(invs[1].ID, invoice.StatusPaid)
	f.store.SetStatus(invs[4].ID, invoice.StatusExpired)
	ctx := context.Background()
	base := t0.Truncate(time.Microsecond) // creation times are stored with microseconds
	cases := map[string]struct {
		filter invoice.Filter
		want   int
	}{
		"status":       {invoice.Filter{Statuses: []invoice.Status{invoice.StatusPaid}}, 1},
		"two statuses": {invoice.Filter{Statuses: []invoice.Status{invoice.StatusPaid, invoice.StatusExpired}}, 2},
		"order":        {invoice.Filter{OrderID: "order-1"}, 3},
		"from":         {invoice.Filter{CreatedFrom: base.Add(4 * time.Second)}, 2},
		"to exclusive": {invoice.Filter{CreatedTo: base.Add(2 * time.Second)}, 2},
		"combined":     {invoice.Filter{OrderID: "order-0", CreatedFrom: base.Add(time.Second), Statuses: []invoice.Status{invoice.StatusNew}}, 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tc.filter.Limit = 100
			items, _, err := f.svc.List(ctx, merchant, tc.filter, "")
			if err != nil || len(items) != tc.want {
				t.Errorf("got %d, want %d (%v)", len(items), tc.want, err)
			}
		})
	}
}

func TestListInvalid(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for name, c := range map[string]string{
		"not base64":   "!!!",
		"not json":     base64.RawURLEncoding.EncodeToString([]byte("nope")),
		"empty object": base64.RawURLEncoding.EncodeToString([]byte("{}")),
	} {
		var ve *invoice.ValidationError
		if _, _, err := f.svc.List(ctx, merchant, invoice.Filter{Limit: 1}, c); !errors.As(err, &ve) || ve.Field != "cursor" {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, _, err := f.svc.List(ctx, merchant, invoice.Filter{}, ""); err == nil {
		t.Error("zero limit accepted")
	}
}

type brokenList struct{ *invoice.MemStore }

func (brokenList) List(context.Context, uuid.UUID, invoice.Filter) ([]invoice.Invoice, error) {
	return nil, errors.New("db down")
}

func TestListStoreError(t *testing.T) {
	svc, err := invoice.NewService(brokenList{invoice.NewMemStore()}, nil, nil,
		invoice.Config{InvoiceTTL: time.Minute, RateMaxAge: time.Minute}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.List(context.Background(), merchant, invoice.Filter{Limit: 1}, ""); err == nil {
		t.Error("error swallowed")
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := invoice.Cursor{CreatedAt: t0, ID: uuid.New()}
	got, err := invoice.DecodeCursor(invoice.EncodeCursor(c))
	if err != nil || !got.CreatedAt.Equal(c.CreatedAt) || got.ID != c.ID {
		t.Errorf("round trip: %+v %v", got, err)
	}
}

func TestMerchantFromEvent(t *testing.T) {
	id := uuid.New()
	ev, err := kafka.NewEvent("/slopay/merchant-service", "merchant.updated", id.String(), 1, map[string]any{
		"merchantId": id, "version": 4, "xpub": invoicetest.Xpub, "webhookUrl": nil, "toleranceBps": 150,
		"webhookSecretVersion": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	m, err := invoice.MerchantFromEvent(ev)
	if err != nil || m.ID != id || m.Version != 4 || m.Xpub != invoicetest.Xpub || m.ToleranceBps != 150 {
		t.Errorf("%+v %v", m, err)
	}
	ev.Type = "merchant.deleted"
	if _, err := invoice.MerchantFromEvent(ev); !errors.Is(err, invoice.ErrNotMerchantEvent) {
		t.Errorf("other type: %v", err)
	}
	ev.Type, ev.Data = "merchant.updated", []byte(`[`)
	if _, err := invoice.MerchantFromEvent(ev); err == nil {
		t.Error("broken data accepted")
	}
}
