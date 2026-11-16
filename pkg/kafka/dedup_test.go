package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestDeduperSkipsRepeats(t *testing.T) {
	var calls int
	h := Deduper{Store: &MemoryStore{}}.Wrap(func(context.Context, Event) error { calls++; return nil })
	ev := validEvent(t)
	for range 3 {
		if err := h(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("handler called %d times", calls)
	}
	if err := h(context.Background(), validEvent(t)); err != nil || calls != 2 {
		t.Fatalf("a new event ID must be handled: calls=%d err=%v", calls, err)
	}
}

func TestDeduperReleasesOnFailure(t *testing.T) {
	boom := errors.New("boom")
	fail := true
	var calls int
	h := Deduper{Store: &MemoryStore{}}.Wrap(func(context.Context, Event) error {
		calls++
		if fail {
			return boom
		}
		return nil
	})
	ev := validEvent(t)
	if err := h(context.Background(), ev); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
	fail = false
	if err := h(context.Background(), ev); err != nil || calls != 2 {
		t.Fatalf("redelivery after failure must be handled: calls=%d err=%v", calls, err)
	}
}

type brokenStore struct{ claimErr, releaseErr error }

func (b brokenStore) Claim(context.Context, string) (bool, error) {
	return b.claimErr == nil, b.claimErr
}
func (b brokenStore) Release(context.Context, string) error { return b.releaseErr }

func TestDeduperStoreErrors(t *testing.T) {
	storeErr, boom := errors.New("store down"), errors.New("boom")
	called := false
	next := func(context.Context, Event) error { called = true; return boom }

	err := Deduper{Store: brokenStore{claimErr: storeErr}}.Wrap(next)(context.Background(), validEvent(t))
	if !errors.Is(err, storeErr) || called {
		t.Fatalf("claim error must be returned without calling the handler: %v", err)
	}

	err = Deduper{Store: brokenStore{releaseErr: storeErr}}.Wrap(next)(context.Background(), validEvent(t))
	if !errors.Is(err, boom) || !errors.Is(err, storeErr) || err.Error() == "" {
		t.Fatalf("both errors must be reported: %v", err)
	}
}

func TestMemoryStoreConcurrentClaim(t *testing.T) {
	var s MemoryStore
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, err := s.Claim(context.Background(), "id"); err == nil && ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d winners", wins.Load())
	}
	if err := s.Release(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Claim(context.Background(), "id"); !ok {
		t.Fatal("claim after release must succeed")
	}
}
