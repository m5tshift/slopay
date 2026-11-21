package ledger_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledger"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledgertest"
)

func ptr[T any](v T) *T { return &v }

func TestNewServiceBounds(t *testing.T) {
	store := ledger.NewMemStore(newCodec(t))
	for _, bps := range []int{-1, 10001} {
		if _, err := ledger.NewService(store, ledger.Config{FeeBps: bps}, nil); err == nil {
			t.Errorf("fee %d bps accepted", bps)
		}
	}
	for _, bps := range []int{0, 50, 10000} {
		if _, err := ledger.NewService(store, ledger.Config{FeeBps: bps}, nil); err != nil {
			t.Errorf("fee %d bps: %v", bps, err)
		}
	}
	// Defaults of the contract: 200 per page, at most 1000.
	s, err := ledger.NewService(store, ledger.Config{FeeBps: 50}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddressPostings(context.Background(), ledgertest.Address(1), ptr(1000), nil); err != nil {
		t.Errorf("limit 1000: %v", err)
	}
	if _, err := s.AddressPostings(context.Background(), ledgertest.Address(1), ptr(1001), nil); err == nil {
		t.Error("limit 1001 accepted")
	}
	// A larger default raises the maximum.
	s, err = ledger.NewService(store, ledger.Config{FeeBps: 50, DefaultLimit: 2000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddressPostings(context.Background(), ledgertest.Address(1), ptr(2000), nil); err != nil {
		t.Errorf("limit 2000 with default 2000: %v", err)
	}
}

func TestApplyPostsEveryTransferOnce(t *testing.T) {
	ctx := context.Background()
	codec := newCodec(t)
	store := ledger.NewMemStore(codec)
	s := newService(t, store)
	inv := ethInvoice(1)
	first, second, late := ledgertest.ETH(100, 1, 6_000), ledgertest.ETH(101, 2, 4_000), ledgertest.ETH(120, 3, 1_000)

	// Events without confirmed transfers post nothing but make the merchant known.
	if err := s.Apply(ctx, ledgertest.Event(t, codec, "invoice.detected", inv, 2, first)); err != nil {
		t.Fatal(err)
	}
	if len(store.Postings()) != 0 {
		t.Fatal("invoice.detected posted a receipt")
	}
	if ok, _ := store.MerchantExists(ctx, merchantA); !ok {
		t.Fatal("merchant unknown after an invoice event")
	}

	paid := ledgertest.Event(t, codec, "invoice.paid", inv, 4, first, second)
	for range 3 { // at-least-once delivery
		if err := s.Apply(ctx, paid); err != nil {
			t.Fatal(err)
		}
	}
	// The repeated transfers of invoice.late_transfer are skipped, the new one is posted.
	if err := s.Apply(ctx, ledgertest.Event(t, codec, "invoice.late_transfer", inv, 5, first, second, late)); err != nil {
		t.Fatal(err)
	}
	postings := store.Postings()
	if len(postings) != 3 {
		t.Fatalf("%d postings, want 3", len(postings))
	}
	if events := store.Events(); len(events) != 6 {
		t.Fatalf("%d events in the outbox, want 6", len(events))
	}
	for _, p := range postings {
		if !p.Balanced() {
			t.Errorf("%s is not balanced", p.ID)
		}
	}
	if postings[0].ID != ledger.PostingID(first) || postings[2].ID != ledger.PostingID(late) {
		t.Errorf("postings %v", postings)
	}
}

func TestApplyPostingEventTypes(t *testing.T) {
	codec := newCodec(t)
	for _, c := range []struct {
		eventType string
		posts     bool
	}{
		{"invoice.paid", true},
		{"invoice.underpaid", true},
		{"invoice.late_paid", true},
		{"invoice.late_transfer", true},
		{"invoice.detected", false},
		{"invoice.confirming", false},
	} {
		t.Run(c.eventType, func(t *testing.T) {
			store := ledger.NewMemStore(codec)
			s := newService(t, store)
			tr := ledgertest.ERC20(7, 7, 3, 20_000_000)
			if err := s.Apply(context.Background(), ledgertest.Event(t, codec, c.eventType, tokenInvoice(7), 3, tr)); err != nil {
				t.Fatal(err)
			}
			if got := len(store.Postings()) == 1; got != c.posts {
				t.Errorf("posted %v, want %v", got, c.posts)
			}
		})
	}
}

func TestPrepareDropsDuplicatesWithinEvent(t *testing.T) {
	codec := newCodec(t)
	s := newService(t, ledger.NewMemStore(codec))
	tr := ledgertest.ETH(1, 1, 100)
	b, err := s.Prepare(ledgertest.Event(t, codec, "invoice.paid", ethInvoice(1), 3, tr, tr))
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Postings) != 1 || b.MerchantID != merchantA || b.EventType != "invoice.paid" {
		t.Errorf("batch %+v", b)
	}
}

func rawEvent(eventType, data string) kafka.Event {
	return kafka.Event{ID: uuid.NewString(), Type: eventType, Data: json.RawMessage(data)}
}

func TestPrepareRejects(t *testing.T) {
	s := newService(t, ledger.NewMemStore(newCodec(t)))
	transfer := `{"blockNumber":1,"blockHash":"` + ledgertest.Hash(1, 1) + `","txHash":"` + ledgertest.Hash(2, 1) +
		`","logIndex":null,"amount":"1","asset":{"kind":"ETH","decimals":18}}`
	head := `"invoiceId":"` + invoiceA.String() + `","merchantId":"` + merchantA.String() + `"`
	if _, err := s.Prepare(rawEvent("merchant.updated", `{}`)); !errors.Is(err, ledger.ErrNotInvoiceEvent) {
		t.Errorf("merchant.updated: %v", err)
	}
	for name, c := range map[string]struct {
		eventType, data string
		want            error
	}{
		"malformed data":   {"invoice.paid", `{"merchantId":1}`, nil},
		"no merchant":      {"invoice.created", `{"invoiceId":"` + invoiceA.String() + `"}`, nil},
		"paid without any": {"invoice.paid", `{` + head + `,"address":"` + addressA + `"}`, nil},
		"invalid address": {"invoice.paid", `{` + head + `,"address":"` + strings.ToUpper(addressA) +
			`","transfers":[` + transfer + `]}`, ledger.ErrInvalidReceipt},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := s.Prepare(rawEvent(c.eventType, c.data))
			if err == nil || errors.Is(err, ledger.ErrNotInvoiceEvent) {
				t.Fatalf("got %v", err)
			}
			if c.want != nil && !errors.Is(err, c.want) {
				t.Errorf("got %v, want %v", err, c.want)
			}
		})
	}
	// The same transfer is accepted in a valid event.
	b, err := s.Prepare(rawEvent("invoice.paid", `{`+head+`,"address":"`+addressA+`","transfers":[`+transfer+`]}`))
	if err != nil || len(b.Postings) != 1 {
		t.Errorf("valid event: %+v, %v", b, err)
	}
}

func TestApplyStoreError(t *testing.T) {
	codec := newCodec(t)
	store := ledger.NewMemStore(codec)
	store.Fail = errDown
	s := newService(t, store)
	if err := s.Apply(context.Background(), ledgertest.Event(t, codec, "invoice.paid", ethInvoice(1), 3, ledgertest.ETH(1, 1, 1))); !errors.Is(err, errDown) {
		t.Errorf("got %v", err)
	}
	if err := s.Apply(context.Background(), rawEvent("invoice.paid", `[]`)); err == nil {
		t.Error("malformed event accepted")
	}
}

func TestCursor(t *testing.T) {
	c := ledger.Cursor{PostedAt: t0.Add(time.Microsecond), ID: "receipt:x"}
	back, err := ledger.DecodeCursor(ledger.EncodeCursor(c))
	if err != nil || !back.PostedAt.Equal(c.PostedAt) || back.ID != c.ID {
		t.Fatalf("round trip %+v, %v", back, err)
	}
	for _, bad := range []string{"!", "e30", "bm90IGpzb24", ledger.EncodeCursor(ledger.Cursor{ID: "x"}), ledger.EncodeCursor(ledger.Cursor{PostedAt: t0})} {
		var ve *ledger.ValidationError
		if _, err := ledger.DecodeCursor(bad); !errors.As(err, &ve) || ve.Field != "cursor" {
			t.Errorf("%q: %v", bad, err)
		}
	}
	if !c.Less(ledger.Posting{PostedAt: t0, ID: "receipt:z"}) || !c.Less(ledger.Posting{PostedAt: c.PostedAt, ID: "receipt:x"}) {
		t.Error("earlier posting or the cursor posting is on the next page")
	}
	if c.Less(ledger.Posting{PostedAt: c.PostedAt, ID: "receipt:y"}) || c.Less(ledger.Posting{PostedAt: t0.Add(time.Second), ID: "a"}) {
		t.Error("later posting is not on the next page")
	}
}

// seed posts n ETH receipts to the address of invoice 1 and one token receipt to invoice 2, one per second.
func seed(t *testing.T, n int) (*ledger.Service, *ledger.MemStore) {
	t.Helper()
	codec := newCodec(t)
	store := ledger.NewMemStore(codec)
	s := newService(t, store)
	for i := range n {
		ev := ledgertest.Event(t, codec, "invoice.late_transfer", ethInvoice(1), i+1, ledgertest.ETH(uint64(100+i), uint64(i), 1_000_000))
		if err := s.Apply(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Apply(context.Background(), ledgertest.Event(t, codec, "invoice.paid", tokenInvoice(2), 3, ledgertest.ERC20(200, 99, 0, 20_000_000))); err != nil {
		t.Fatal(err)
	}
	return s, store
}

func TestMerchantStatement(t *testing.T) {
	ctx := context.Background()
	s, store := seed(t, 5)
	from, to := t0, t0.Add(time.Hour)

	var ids []string
	var cursor *string
	for page := 0; ; page++ {
		st, err := s.MerchantStatement(ctx, merchantA, from, to, ptr(2), cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Totals) != 2 {
			t.Fatalf("totals %+v", st.Totals)
		}
		// Totals cover the whole period on every page, ordered by asset kind.
		token, eth := st.Totals[0], st.Totals[1]
		if eth.Asset.Kind != ledger.KindETH || eth.Received.String() != "5000000" || eth.Fee.String() != "25000" || eth.Postings != 5 {
			t.Errorf("ETH totals %+v", eth)
		}
		if token.Asset.Kind != ledger.KindERC20 || token.Received.String() != "20000000" || token.Fee.String() != "100000" || token.Postings != 1 {
			t.Errorf("token totals %+v", token)
		}
		for _, p := range st.Items {
			ids = append(ids, p.ID)
		}
		if st.Next == "" {
			if page != 2 || len(st.Items) != 2 {
				t.Errorf("last page %d has %d items", page, len(st.Items))
			}
			break
		}
		cursor = &st.Next
	}
	all := store.Postings()
	if len(ids) != len(all) {
		t.Fatalf("pages returned %d postings, want %d", len(ids), len(all))
	}
	for i, p := range all {
		if ids[i] != p.ID {
			t.Errorf("item %d: %s, want %s", i, ids[i], p.ID)
		}
	}

	// The period is [from, to): the first posting is at t0+1s.
	st, err := s.MerchantStatement(ctx, merchantA, t0.Add(2*time.Second), t0.Add(4*time.Second), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Items) != 2 || len(st.Totals) != 1 || st.Totals[0].Postings != 2 || st.Next != "" {
		t.Errorf("period statement %+v", st)
	}
	// A known merchant without postings in the period has an empty statement.
	st, err = s.MerchantStatement(ctx, merchantA, t0.Add(-time.Hour), t0, nil, nil)
	if err != nil || len(st.Items) != 0 || len(st.Totals) != 0 {
		t.Errorf("empty period: %+v, %v", st, err)
	}
}

func TestMerchantStatementErrors(t *testing.T) {
	ctx := context.Background()
	s, store := seed(t, 1)
	from, to := t0, t0.Add(time.Hour)
	for name, c := range map[string]struct {
		from, to time.Time
		limit    *int
		cursor   *string
		field    string
	}{
		"empty period":    {to, to, nil, nil, "to"},
		"reversed period": {to, from, nil, nil, "to"},
		"limit 0":         {from, to, ptr(0), nil, "limit"},
		"limit 1001":      {from, to, ptr(1001), nil, "limit"},
		"bad cursor":      {from, to, nil, ptr("x"), "cursor"},
	} {
		t.Run(name, func(t *testing.T) {
			var ve *ledger.ValidationError
			if _, err := s.MerchantStatement(ctx, merchantA, c.from, c.to, c.limit, c.cursor); !errors.As(err, &ve) || ve.Field != c.field {
				t.Errorf("got %v, want a validation error of %s", err, c.field)
			}
		})
	}
	if _, err := s.MerchantStatement(ctx, uuid.New(), from, to, nil, nil); !errors.Is(err, ledger.ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}
	for name, f := range map[string]*faultyStore{
		"exists":   {Store: store, exists: true},
		"totals":   {Store: store, totals: true},
		"postings": {Store: store, postings: true},
	} {
		if _, err := newService(t, f).MerchantStatement(ctx, merchantA, from, to, nil, nil); !errors.Is(err, errDown) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestAddressesStatement(t *testing.T) {
	ctx := context.Background()
	s, store := seed(t, 3)
	unknown := ledgertest.Address(50)
	out, err := s.AddressesStatement(ctx, []string{ledgertest.Address(2), unknown, ledgertest.Address(1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 || out[0].Address != ledgertest.Address(2) || out[1].Address != unknown || out[2].Address != ledgertest.Address(1) {
		t.Fatalf("order %+v", out)
	}
	if out[1].Totals == nil || len(out[1].Totals) != 0 {
		t.Errorf("unknown address totals %#v, want empty", out[1].Totals)
	}
	if len(out[0].Totals) != 1 || out[0].Totals[0].Received.String() != "20000000" {
		t.Errorf("token address %+v", out[0].Totals)
	}
	if len(out[2].Totals) != 1 || out[2].Totals[0].Postings != 3 || out[2].Totals[0].Fee.String() != "15000" {
		t.Errorf("ETH address %+v", out[2].Totals)
	}

	many := make([]string, 101)
	for i := range many {
		many[i] = ledgertest.Address(uint64(i))
	}
	for name, addrs := range map[string][]string{
		"none":       nil,
		"101":        many,
		"duplicate":  {unknown, unknown},
		"upper-case": {strings.ToUpper(unknown)},
		"short":      {"0x1"},
	} {
		var ve *ledger.ValidationError
		if _, err := s.AddressesStatement(ctx, addrs); !errors.As(err, &ve) || ve.Field != "address" {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := s.AddressesStatement(ctx, many[:100]); err != nil {
		t.Errorf("100 addresses: %v", err)
	}
	if _, err := newService(t, &faultyStore{Store: store, addrTotals: true}).AddressesStatement(ctx, []string{unknown}); !errors.Is(err, errDown) {
		t.Errorf("store error: %v", err)
	}
}

func TestAddressPostings(t *testing.T) {
	ctx := context.Background()
	s, store := seed(t, 3)
	addr := ledgertest.Address(1)
	p1, err := s.AddressPostings(ctx, addr, ptr(2), nil)
	if err != nil || len(p1.Items) != 2 || p1.Next == "" {
		t.Fatalf("page 1 %+v, %v", p1, err)
	}
	p2, err := s.AddressPostings(ctx, addr, ptr(2), &p1.Next)
	if err != nil || len(p2.Items) != 1 || p2.Next != "" {
		t.Fatalf("page 2 %+v, %v", p2, err)
	}
	if p2.Items[0].ID == p1.Items[1].ID {
		t.Error("the cursor posting is repeated")
	}
	all, err := s.AddressPostings(ctx, addr, nil, nil)
	if err != nil || len(all.Items) != 3 {
		t.Errorf("default limit %+v, %v", all, err)
	}
	for _, p := range all.Items {
		if p.Address != addr {
			t.Errorf("foreign posting %s", p.ID)
		}
	}
	var ve *ledger.ValidationError
	if _, err := s.AddressPostings(ctx, "0xABC", nil, nil); !errors.As(err, &ve) || ve.Field != "address" {
		t.Errorf("bad address: %v", err)
	}
	if _, err := s.AddressPostings(ctx, addr, ptr(0), nil); !errors.As(err, &ve) || ve.Field != "limit" {
		t.Errorf("bad limit: %v", err)
	}
	if _, err := s.AddressPostings(ctx, addr, nil, ptr("@")); !errors.As(err, &ve) || ve.Field != "cursor" {
		t.Errorf("bad cursor: %v", err)
	}
	if _, err := newService(t, &faultyStore{Store: store, postings: true}).AddressPostings(ctx, addr, nil, nil); !errors.Is(err, errDown) {
		t.Errorf("store error: %v", err)
	}
	if ve.Error() == "" {
		t.Error("empty message")
	}
}
