package dedup

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m5tshift/weigate/pkg/kafka"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type failing struct{ claim, release error }

func (f failing) Claim(context.Context, string) (bool, error) { return false, f.claim }
func (f failing) Release(context.Context, string) error       { return f.release }

func counter() (kafka.Handler, *atomic.Int32) {
	var n atomic.Int32
	return func(context.Context, kafka.Event) error { n.Add(1); return nil }, &n
}

func TestWithoutBackendEveryEventIsDelivered(t *testing.T) {
	s := New(time.Second, quiet)
	h, n := counter()
	w := s.Wrap(h, time.Millisecond, time.Millisecond)
	for range 2 {
		if err := w(context.Background(), kafka.Event{ID: "e1"}); err != nil {
			t.Fatal(err)
		}
	}
	if n.Load() != 2 {
		t.Errorf("delivered %d times, want 2", n.Load())
	}
}

func TestWithBackendDuplicateSkipped(t *testing.T) {
	s := New(time.Second, quiet)
	s.Attach(&kafka.MemoryStore{})
	h, n := counter()
	w := s.Wrap(h, time.Millisecond, time.Millisecond)
	for _, id := range []string{"e1", "e1", "e2"} {
		if err := w(context.Background(), kafka.Event{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if n.Load() != 2 {
		t.Errorf("delivered %d times, want 2", n.Load())
	}
}

// A failing Valkey does not stop delivery (SRS 4.1.7).
func TestBackendErrorsFailOpen(t *testing.T) {
	s := New(time.Second, quiet)
	s.Attach(failing{claim: errors.New("down"), release: errors.New("down")})
	h, n := counter()
	if err := s.Wrap(h, time.Millisecond, time.Millisecond)(context.Background(), kafka.Event{ID: "e1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(context.Background(), "e1"); err != nil || n.Load() != 1 {
		t.Errorf("release %v, delivered %d", err, n.Load())
	}
}

// A failed handler is repeated without claiming again, so the repeat is not taken for a duplicate.
func TestFailureRepeatedWithoutNewClaim(t *testing.T) {
	s := New(time.Second, quiet)
	s.Attach(&kafka.MemoryStore{})
	var calls atomic.Int32
	h := func(context.Context, kafka.Event) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}
	if err := s.Wrap(h, time.Millisecond, 2*time.Millisecond)(context.Background(), kafka.Event{ID: "e1"}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Errorf("calls %d, want 3", calls.Load())
	}
}

// On shutdown the claim is released, so the event is delivered after a restart.
func TestCancelReleasesClaim(t *testing.T) {
	s := New(time.Second, quiet)
	mem := &kafka.MemoryStore{}
	s.Attach(mem)
	ctx, cancel := context.WithCancel(context.Background())
	h := func(ctx context.Context, _ kafka.Event) error {
		cancel()
		return ctx.Err()
	}
	if err := s.Wrap(h, time.Millisecond, time.Millisecond)(ctx, kafka.Event{ID: "e1"}); err == nil {
		t.Fatal("expected the cancellation")
	}
	if first, _ := mem.Claim(context.Background(), "e1"); !first {
		t.Error("the claim must be released")
	}
}

func TestConnectRetriesThenAttaches(t *testing.T) {
	s := New(time.Second, quiet)
	var tries atomic.Int32
	s.Connect(context.Background(), func(context.Context) (kafka.DedupStore, error) {
		if tries.Add(1) < 2 {
			return nil, errors.New("down")
		}
		return &kafka.MemoryStore{}, nil
	}, time.Millisecond)
	if s.get() == nil || tries.Load() != 2 {
		t.Errorf("tries %d, attached %v", tries.Load(), s.get() != nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s2 := New(time.Second, quiet)
	s2.Connect(ctx, func(context.Context) (kafka.DedupStore, error) { return nil, errors.New("down") }, time.Hour)
	if s2.get() != nil {
		t.Error("nothing must be attached")
	}
}
