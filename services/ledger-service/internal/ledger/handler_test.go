package ledger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/m5tshift/slopay/services/ledger-service/internal/ledger"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledgertest"
)

var fast = ledger.Backoff{Min: time.Millisecond, Max: 2 * time.Millisecond}

func TestHandlerSkipsOtherEvents(t *testing.T) {
	store := ledger.NewMemStore(newCodec(t))
	h := ledger.Handler(newService(t, store), fast, quiet)
	if err := h(context.Background(), rawEvent("merchant.updated", `{}`)); err != nil {
		t.Errorf("got %v", err)
	}
	if ok, _ := store.MerchantExists(context.Background(), merchantA); ok {
		t.Error("a foreign event changed the ledger")
	}
}

func TestHandlerReturnsPermanentErrors(t *testing.T) {
	h := ledger.Handler(newService(t, ledger.NewMemStore(newCodec(t))), fast, quiet)
	if err := h(context.Background(), rawEvent("invoice.paid", `[]`)); err == nil {
		t.Error("an event that can never be posted was skipped")
	}
}

func TestHandlerRetriesStoreErrors(t *testing.T) {
	codec := newCodec(t)
	mem := ledger.NewMemStore(codec)
	store := &faultyStore{Store: mem, failRecord: 3}
	h := ledger.Handler(newService(t, store), fast, quiet)
	ev := ledgertest.Event(t, codec, "invoice.paid", ethInvoice(1), 3, ledgertest.ETH(1, 1, 1000))
	if err := h(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if store.records != 4 || len(mem.Postings()) != 1 || len(mem.Events()) != 2 {
		t.Errorf("%d attempts, %d postings, %d events", store.records, len(mem.Postings()), len(mem.Events()))
	}
}

func TestHandlerStopsOnCancel(t *testing.T) {
	codec := newCodec(t)
	store := &faultyStore{Store: ledger.NewMemStore(codec), failRecord: 1 << 30}
	h := ledger.Handler(newService(t, store), ledger.Backoff{Min: time.Hour, Max: time.Hour}, quiet)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	ev := ledgertest.Event(t, codec, "invoice.paid", ethInvoice(1), 3, ledgertest.ETH(1, 1, 1000))
	if err := h(ctx, ev); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v", err)
	}
}
