package projection

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/invoicetest"
)

const (
	invoiceID  = "0b9f3c2e-8a1d-4c3e-9f7a-1e2d3c4b5a69"
	merchantID = "7c0b6a51-5f0e-4f57-9d35-3f7f2d2b1a10"
	address    = "0x00000000000000000000000000000000000000aa"
)

var (
	created = time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	tr      = invoicetest.Transfer{BlockNumber: 42, BlockHash: "0x" + strings.Repeat("ab", 32),
		TxHash: "0x" + strings.Repeat("cd", 32), Amount: "1000"}
	trKey = "0x" + strings.Repeat("ab", 32) + ":0x" + strings.Repeat("cd", 32) + ":"
)

// valid checks the fixture against the contract, as the consumer would.
func valid(t *testing.T, ev kafka.Event) kafka.Event {
	t.Helper()
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := codec.Encode(ev)
	if err != nil {
		t.Fatalf("fixture %s does not match the contract: %v", ev.Type, err)
	}
	out, err := codec.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDecode(t *testing.T) {
	expires := created.Add(time.Hour)
	paidAt := created.Add(10 * time.Minute)

	u, ok, err := Decode(valid(t, invoicetest.Created(invoiceID, merchantID, address, "1000", created, expires)))
	if err != nil || !ok || u.CreatedAt == nil || !u.CreatedAt.Equal(created) || !u.ExpiresAt.Equal(expires) ||
		u.Address != address || u.MerchantID != merchantID || u.Asset.Key() != "ETH" || u.Asset.Decimals != 18 || u.Sequence != 1 {
		t.Fatalf("created: %+v %v %v", u, ok, err)
	}

	u, ok, err = Decode(valid(t, invoicetest.Paid(invoiceID, merchantID, address, "1000", 4, paidAt, tr)))
	if err != nil || !ok || u.PaidAt == nil || !u.PaidAt.Equal(paidAt) || u.CreatedAt != nil || len(u.Add) != 1 ||
		u.Add[0].Key != trKey || u.Add[0].BlockNumber != 42 || u.Add[0].Amount.Cmp(big.NewInt(1000)) != 0 || u.Sequence != 4 {
		t.Fatalf("paid: %+v %v %v", u, ok, err)
	}

	u, ok, err = Decode(valid(t, invoicetest.LateTransfer(invoiceID, merchantID, address, "1000", 5, paidAt, tr)))
	if err != nil || !ok || u.PaidAt != nil || len(u.Add) != 1 || u.Add[0].Key != trKey {
		t.Fatalf("late_transfer: %+v %v %v", u, ok, err)
	}

	u, ok, err = Decode(valid(t, invoicetest.Reverted(invoiceID, merchantID, address, "1000", 3, paidAt, tr)))
	if err != nil || !ok || len(u.Add) != 0 || len(u.Remove) != 1 || u.Remove[0] != trKey {
		t.Fatalf("reverted: %+v %v %v", u, ok, err)
	}

	if _, ok, err := Decode(valid(t, invoicetest.Detected(invoiceID, merchantID, address, "1000", 2, paidAt))); ok || err != nil {
		t.Fatalf("detected: %v %v", ok, err)
	}

	bad := invoicetest.Paid(invoiceID, merchantID, address, "1000", 4, paidAt, tr)
	bad.Data = []byte(`{"transfers":[{"amount":"x"}]}`)
	if _, _, err := Decode(bad); err == nil {
		t.Fatal("invalid amount: want error")
	}
	bad.Data = []byte(`{`)
	if _, _, err := Decode(bad); err == nil {
		t.Fatal("invalid json: want error")
	}
}

type flakyWriter struct {
	mu    sync.Mutex
	fails int
	got   []Update
}

func (w *flakyWriter) Apply(_ context.Context, u Update) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fails > 0 {
		w.fails--
		return errors.New("mongo down")
	}
	w.got = append(w.got, u)
	return nil
}

func TestHandlerRetriesWrites(t *testing.T) {
	w := &flakyWriter{fails: 2}
	h := Handler(w, Backoff{Min: time.Millisecond, Max: 2 * time.Millisecond}, nil)
	if err := h(context.Background(), invoicetest.Paid(invoiceID, merchantID, address, "1000", 4, created, tr)); err != nil {
		t.Fatal(err)
	}
	if len(w.got) != 1 || w.fails != 0 {
		t.Fatalf("writes %d, fails left %d", len(w.got), w.fails)
	}
	// Ignored events do not write.
	if err := h(context.Background(), invoicetest.Detected(invoiceID, merchantID, address, "1000", 2, created)); err != nil || len(w.got) != 1 {
		t.Fatalf("detected: %v, writes %d", err, len(w.got))
	}
}

func TestHandlerStopsOnCancel(t *testing.T) {
	w := &flakyWriter{fails: 1 << 30}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	h := Handler(w, Backoff{Min: time.Millisecond, Max: 5 * time.Millisecond}, nil)
	if err := h(ctx, invoicetest.Paid(invoiceID, merchantID, address, "1000", 4, created, tr)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}
