package projection

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type call struct {
	method    string
	address   string
	invoiceID string
	createdAt time.Time
	expiresAt time.Time
	until     time.Time
}

type fakeWriter struct {
	mu    sync.Mutex
	calls []call
	fails int // the next writes fail
}

func (w *fakeWriter) fail() error {
	if w.fails > 0 {
		w.fails--
		return errors.New("mongodb down")
	}
	return nil
}

func (w *fakeWriter) Track(_ context.Context, address, invoiceID string, createdAt, expiresAt, trackUntil time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail(); err != nil {
		return err
	}
	w.calls = append(w.calls, call{"track", address, invoiceID, createdAt, expiresAt, trackUntil})
	return nil
}

func (w *fakeWriter) Extend(_ context.Context, address string, trackUntil time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fail(); err != nil {
		return err
	}
	w.calls = append(w.calls, call{method: "extend", address: address, until: trackUntil})
	return nil
}

var (
	created = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	expires = created.Add(15 * time.Minute)
	paid    = created.Add(5 * time.Minute)
)

func event(typ, data string) kafka.Event {
	return kafka.Event{ID: "ev-1", Type: typ, Time: created, Data: []byte(data)}
}

func TestHandlerTracksAndExtends(t *testing.T) {
	w := &fakeWriter{}
	h := Handler(w, 24*time.Hour, Backoff{Min: time.Millisecond, Max: time.Millisecond}, quiet)
	ctx := context.Background()
	if err := h(ctx, event(EventCreated, `{"invoiceId":"inv-1","address":"0x71C7656EC7ab88b098defB751B7401B5f6d8976F",`+
		`"expiresAt":"`+expires.Format(time.RFC3339)+`"}`)); err != nil {
		t.Fatal(err)
	}
	if err := h(ctx, event(EventPaid, `{"address":"0x71C7656EC7ab88b098defB751B7401B5f6d8976F",`+
		`"paidAt":"`+paid.Format(time.RFC3339)+`"}`)); err != nil {
		t.Fatal(err)
	}
	if err := h(ctx, event("invoice.expired", `{}`)); err != nil {
		t.Fatal(err)
	}
	addr := "0x71c7656ec7ab88b098defb751b7401b5f6d8976f"
	want := []call{
		{"track", addr, "inv-1", created, expires, expires.Add(24 * time.Hour)},
		{method: "extend", address: addr, until: paid.Add(24 * time.Hour)},
	}
	if len(w.calls) != len(want) {
		t.Fatalf("calls %+v", w.calls)
	}
	for i := range want {
		got := w.calls[i]
		if got.method != want[i].method || got.address != want[i].address || got.invoiceID != want[i].invoiceID ||
			!got.createdAt.Equal(want[i].createdAt) || !got.expiresAt.Equal(want[i].expiresAt) || !got.until.Equal(want[i].until) {
			t.Errorf("call %d = %+v, want %+v", i, got, want[i])
		}
	}
}

func TestHandlerRetriesWrites(t *testing.T) {
	w := &fakeWriter{fails: 3}
	h := Handler(w, time.Hour, Backoff{Min: time.Millisecond, Max: 2 * time.Millisecond}, quiet)
	if err := h(context.Background(), event(EventPaid, `{"address":"0xaa","paidAt":"`+paid.Format(time.RFC3339)+`"}`)); err != nil {
		t.Fatal(err)
	}
	if len(w.calls) != 1 || w.fails != 0 {
		t.Fatalf("calls %+v, fails %d", w.calls, w.fails)
	}
	w.fails = 1 << 30
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := h(ctx, event(EventCreated, `{"address":"0xaa"}`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestHandlerRejectsBadData(t *testing.T) {
	h := Handler(&fakeWriter{}, time.Hour, Backoff{}, nil)
	for _, typ := range []string{EventCreated, EventPaid} {
		if err := h(context.Background(), event(typ, `{"address":1}`)); err == nil {
			t.Errorf("%s: accepted", typ)
		}
	}
}

type fakeState struct {
	built     bool
	builtErr  error
	marks     int
	markFails int
}

func (s *fakeState) Built(context.Context) (bool, error) { return s.built, s.builtErr }

func (s *fakeState) MarkBuilt(context.Context, time.Time) error {
	if s.markFails > 0 {
		s.markFails--
		return errors.New("mongodb down")
	}
	s.marks++
	s.built = true
	return nil
}

func TestEnsureBuilt(t *testing.T) {
	b := Backoff{Min: time.Millisecond, Max: 2 * time.Millisecond}
	ctx := context.Background()

	// An existing projection is not rebuilt.
	s := &fakeState{built: true}
	rebuilds := 0
	rebuild := func(context.Context) error { rebuilds++; return nil }
	if err := EnsureBuilt(ctx, s, rebuild, b, quiet); err != nil || rebuilds != 0 || s.marks != 0 {
		t.Fatalf("existing: err = %v, rebuilds %d, marks %d", err, rebuilds, s.marks)
	}

	// A missing one is rebuilt until it succeeds and then marked.
	s = &fakeState{markFails: 1}
	rebuilds = 0
	rebuild = func(context.Context) error {
		if rebuilds++; rebuilds < 3 {
			return errors.New("kafka down")
		}
		return nil
	}
	if err := EnsureBuilt(ctx, s, rebuild, b, nil); err != nil || rebuilds != 4 || s.marks != 1 {
		t.Fatalf("missing: err = %v, rebuilds %d, marks %d", err, rebuilds, s.marks)
	}

	// The state read fails until ctx is done.
	s = &fakeState{builtErr: errors.New("mongodb down")}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := EnsureBuilt(cctx, s, rebuild, Backoff{}, quiet); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}
