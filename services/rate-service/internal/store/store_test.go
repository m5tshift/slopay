package store

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/m5tshift/slopay/services/rate-service/internal/rate"
)

var t0 = time.Date(2026, 11, 20, 12, 0, 0, 123456789, time.UTC)

func ethRate(price string, at time.Time) rate.Rate {
	return rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice(price), QuotedAt: at, Sources: []rate.Source{rate.Coinbase, rate.OKX}}
}

func toMap(f [][2]string) map[string]string {
	m := map[string]string{}
	for _, kv := range f {
		m[kv[0]] = kv[1]
	}
	return m
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := map[string]rate.Snapshot{
		"ok":                {Rate: ethRate("3700.125", t0), UpdatedAt: t0.Add(time.Second)},
		"diverged with old": {Rate: ethRate("3700", t0), Diverged: true, UpdatedAt: t0.Add(5 * time.Second)},
		"diverged no rate":  {Diverged: true, UpdatedAt: t0},
		"one source":        {Rate: rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice("1"), QuotedAt: t0, Sources: []rate.Source{rate.OKX}}, UpdatedAt: t0},
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok, err := decode(rate.ETHUSD, toMap(encode(s)))
			if err != nil || !ok {
				t.Fatalf("decode: %v %v", ok, err)
			}
			if got.Diverged != s.Diverged || !got.UpdatedAt.Equal(s.UpdatedAt) || got.Rate.Price.String() != s.Rate.Price.String() ||
				!got.Rate.QuotedAt.Equal(s.Rate.QuotedAt) || len(got.Rate.Sources) != len(s.Rate.Sources) {
				t.Errorf("got %+v, want %+v", got, s)
			}
			if !s.Rate.Price.IsZero() && (got.Rate.Pair != rate.ETHUSD || got.Rate.Price.String() != s.Rate.Price.String()) {
				t.Errorf("rate %+v", got.Rate)
			}
		})
	}
}

func TestDecodeEmptyAndInvalid(t *testing.T) {
	if _, ok, err := decode(rate.ETHUSD, map[string]string{}); ok || err != nil {
		t.Errorf("empty: %v %v", ok, err)
	}
	good := toMap(encode(rate.Snapshot{Rate: ethRate("3700", t0), UpdatedAt: t0}))
	bad := map[string]func(m map[string]string){
		"status":    func(m map[string]string) { m["status"] = "maybe" },
		"updatedAt": func(m map[string]string) { m["updatedAt"] = "yesterday" },
		"no price":  func(m map[string]string) { delete(m, "price") },
		"price":     func(m map[string]string) { m["price"] = "-1" },
		"quotedAt":  func(m map[string]string) { m["quotedAt"] = "" },
		"source":    func(m map[string]string) { m["sources"] = "coinbase,kraken" },
	}
	for name, mutate := range bad {
		t.Run(name, func(t *testing.T) {
			m := map[string]string{}
			for k, v := range good {
				m[k] = v
			}
			mutate(m)
			if _, ok, err := decode(rate.ETHUSD, m); ok || err == nil {
				t.Errorf("accepted %v", m)
			}
		})
	}
}

func TestNotConnected(t *testing.T) {
	v := New(time.Second)
	v.Close() // no client: nothing to close
	ctx := context.Background()
	if err := v.Put(ctx, rate.ETHUSD, rate.Snapshot{}); !errors.Is(err, ErrNotConnected) {
		t.Errorf("put: %v", err)
	}
	if _, _, err := v.Get(ctx, rate.ETHUSD); !errors.Is(err, ErrNotConnected) {
		t.Errorf("get: %v", err)
	}
}

func TestConnectRetriesUntilOpen(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	v := New(time.Second)
	calls := 0
	open := func(context.Context) (valkey.Client, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("connection refused")
		}
		// valkey.NewClient fails without a server, so a nil client stands for a connected one.
		return nil, nil
	}
	v.Connect(context.Background(), open, time.Millisecond, log)
	if calls != 3 || !strings.Contains(logs.String(), "valkey connected") || strings.Count(logs.String(), "valkey unavailable") != 2 {
		t.Errorf("calls %d, logs %s", calls, logs.String())
	}
}

func TestConnectStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	v := New(time.Second)
	open := func(context.Context) (valkey.Client, error) {
		cancel()
		return nil, errors.New("connection refused")
	}
	done := make(chan struct{})
	go func() {
		v.Connect(ctx, open, time.Hour, slog.New(slog.DiscardHandler))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not stop")
	}
}

type fakeGetter struct {
	s   rate.Snapshot
	ok  bool
	err error
}

func (f fakeGetter) Get(context.Context, rate.Pair) (rate.Snapshot, bool, error) {
	return f.s, f.ok, f.err
}

func TestLatestPicksNewer(t *testing.T) {
	older := rate.Snapshot{Rate: ethRate("3700", t0), UpdatedAt: t0}
	newer := rate.Snapshot{Diverged: true, Rate: ethRate("3700", t0), UpdatedAt: t0.Add(5 * time.Second)}
	ctx := context.Background()
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cache := rate.NewCache()
	cache.Put(rate.ETHUSD, older)
	// Valkey has the divergence mark written by the leader after the last rate.latest.
	if s, ok := NewLatest(fakeGetter{s: newer, ok: true}, cache, log).Get(ctx, rate.ETHUSD); !ok || !s.Diverged {
		t.Errorf("valkey newer: %+v %v", s, ok)
	}
	// Memory is newer than a Valkey that lost writes.
	cache2 := rate.NewCache()
	cache2.Put(rate.ETHUSD, newer)
	if s, ok := NewLatest(fakeGetter{s: older, ok: true}, cache2, log).Get(ctx, rate.ETHUSD); !ok || !s.Diverged {
		t.Errorf("memory newer: %+v %v", s, ok)
	}
	// Valkey down: memory is served, the error is logged at debug.
	if s, ok := NewLatest(fakeGetter{s: newer, ok: true, err: errors.New("timeout")}, cache, log).Get(ctx, rate.ETHUSD); !ok || s.Diverged {
		t.Errorf("valkey error: %+v %v", s, ok)
	}
	if !strings.Contains(logs.String(), "serving from memory") {
		t.Errorf("logs: %s", logs.String())
	}
	logs.Reset()
	// Not connected yet: memory, without a log line per request.
	if _, ok := NewLatest(fakeGetter{err: ErrNotConnected}, cache, log).Get(ctx, rate.ETHUSD); !ok || logs.Len() != 0 {
		t.Errorf("not connected: %v %s", ok, logs.String())
	}
	// Nothing anywhere.
	if _, ok := NewLatest(fakeGetter{}, rate.NewCache(), log).Get(ctx, rate.USDTUSD); ok {
		t.Error("empty: ok")
	}
}
