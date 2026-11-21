package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/m5tshift/weigate/pkg/kafka"
)

type fakeStore struct {
	mu        sync.Mutex
	rows      []row
	published []int64
	locked    bool // another relay holds the lock
	batchErr  error
	statsErr  error
	purgeErr  error
	pending   int64
	oldest    time.Duration
	purges    int
	batches   int
	limit     int
}

func (s *fakeStore) withBatch(_ context.Context, limit int, fn func([]row) ([]int64, error)) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches++
	s.limit = limit
	if s.batchErr != nil {
		return false, s.batchErr
	}
	if s.locked {
		return false, nil
	}
	n := min(limit, len(s.rows))
	if n == 0 {
		return true, nil
	}
	done, err := fn(s.rows[:n])
	s.published = append(s.published, done...)
	s.rows = s.rows[len(done):]
	return true, err
}

func (s *fakeStore) stats(context.Context) (int64, time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending, s.oldest, s.statsErr
}

func (s *fakeStore) purge(context.Context, time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purges++
	return 0, s.purgeErr
}

type fakePub struct {
	mu     sync.Mutex
	got    []string
	failAt int // fail the Nth call (1-based); 0 = never
	calls  int
}

func (p *fakePub) Publish(_ context.Context, topic string, ev kafka.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.failAt == p.calls {
		return errors.New("broker down")
	}
	p.got = append(p.got, topic+"/"+ev.ID)
	return nil
}

func rows(n int) []row {
	out := make([]row, n)
	for i := range out {
		out[i] = row{id: int64(i + 1), topic: "t", event: kafka.Event{ID: string(rune('a' + i))}}
	}
	return out
}

func newTestRelay(t *testing.T, st store, pub Publisher, mut func(*Config)) *Relay {
	t.Helper()
	cfg := Config{Publisher: pub, Interval: time.Millisecond, MaxBackoff: 4 * time.Millisecond, BatchSize: 2,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if mut != nil {
		mut(&cfg)
	}
	cfg.applyDefaults()
	r, err := newRelay(cfg, st)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestNewRelayRequiresDeps(t *testing.T) {
	if _, err := NewRelay(Config{}); err == nil {
		t.Fatal("expected error")
	}
	if _, err := NewRelay(Config{Publisher: &fakePub{}}); err == nil {
		t.Fatal("pool is required")
	}
}

func TestDefaults(t *testing.T) {
	var c Config
	c.applyDefaults()
	if c.Interval != 500*time.Millisecond || c.MaxBackoff != 30*time.Second || c.BatchSize != 100 ||
		c.Retention != 24*time.Hour || c.Logger == nil {
		t.Fatalf("%+v", c)
	}
	c = Config{Interval: time.Second, BatchSize: 5}
	c.applyDefaults()
	if c.Interval != time.Second || c.BatchSize != 5 {
		t.Fatal("explicit values must be kept")
	}
}

func TestFlushPublishesInOrder(t *testing.T) {
	st, pub := &fakeStore{rows: rows(2)}, &fakePub{}
	n, err := newTestRelay(t, st, pub, nil).Flush(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if len(pub.got) != 2 || pub.got[0] != "t/a" || pub.got[1] != "t/b" {
		t.Fatalf("got %v", pub.got)
	}
	if len(st.published) != 2 || st.limit != 2 {
		t.Fatalf("published=%v limit=%d", st.published, st.limit)
	}
}

func TestFlushStopsAtFirstFailure(t *testing.T) {
	st, pub := &fakeStore{rows: rows(2)}, &fakePub{failAt: 2}
	n, err := newTestRelay(t, st, pub, nil).Flush(context.Background())
	if err == nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if len(st.published) != 1 || st.published[0] != 1 || len(st.rows) != 1 || st.rows[0].id != 2 {
		t.Fatalf("published=%v rows=%v", st.published, st.rows)
	}
	// The failed event is retried, nothing is skipped.
	if n, err = newTestRelay(t, st, pub, nil).Flush(context.Background()); err != nil || n != 1 {
		t.Fatalf("retry n=%d err=%v", n, err)
	}
	if pub.got[len(pub.got)-1] != "t/b" {
		t.Fatalf("got %v", pub.got)
	}
}

func TestFlushLockedAndEmpty(t *testing.T) {
	pub := &fakePub{}
	st := &fakeStore{rows: rows(1), locked: true}
	if n, err := newTestRelay(t, st, pub, nil).Flush(context.Background()); n != 0 || err != nil || pub.calls != 0 {
		t.Fatalf("locked: n=%d err=%v calls=%d", n, err, pub.calls)
	}
	st = &fakeStore{}
	if n, err := newTestRelay(t, st, pub, nil).Flush(context.Background()); n != 0 || err != nil {
		t.Fatalf("empty: n=%d err=%v", n, err)
	}
}

func TestFlushStoreError(t *testing.T) {
	st := &fakeStore{batchErr: errors.New("db down")}
	if _, err := newTestRelay(t, st, &fakePub{}, nil).Flush(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunDrainsAndStops(t *testing.T) {
	st, pub := &fakeStore{rows: rows(5), pending: 7, oldest: 3 * time.Second}, &fakePub{}
	r := newTestRelay(t, st, pub, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitFor(t, func() bool { pub.mu.Lock(); defer pub.mu.Unlock(); return len(pub.got) == 5 })
	waitFor(t, func() bool { st.mu.Lock(); defer st.mu.Unlock(); return st.purges >= 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if r.pending.Load() != 7 || time.Duration(r.oldestNs.Load()) != 3*time.Second {
		t.Fatalf("lag gauges: %d %v", r.pending.Load(), time.Duration(r.oldestNs.Load()))
	}
}

func TestRunRetriesAfterErrors(t *testing.T) {
	st := &fakeStore{rows: rows(1), statsErr: errors.New("stats down"), purgeErr: errors.New("purge down")}
	pub := &fakePub{failAt: 1}
	r := newTestRelay(t, st, pub, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitFor(t, func() bool { pub.mu.Lock(); defer pub.mu.Unlock(); return len(pub.got) == 1 })
	waitFor(t, func() bool { st.mu.Lock(); defer st.mu.Unlock(); return st.purges >= 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunStopsWhenCancelledDuringFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	st := &fakeStore{batchErr: context.Canceled}
	r := newTestRelay(t, st, &fakePub{}, nil)
	cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	// Failure with a live context, then cancel while it backs off.
	ctx, cancel = context.WithCancel(context.Background())
	st = &fakeStore{batchErr: errors.New("db down")}
	r = newTestRelay(t, st, &fakePub{}, func(c *Config) { c.Interval = time.Hour; c.MaxBackoff = time.Hour })
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitFor(t, func() bool { st.mu.Lock(); defer st.mu.Unlock(); return st.batches >= 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()

	st, pub := &fakeStore{rows: rows(2), pending: 4, oldest: 1500 * time.Millisecond}, &fakePub{failAt: 2}
	r := newTestRelay(t, st, pub, func(c *Config) { c.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)) })
	_, _ = r.Flush(context.Background()) // 1 published, 1 failure
	r.refresh(context.Background())

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				got[m.Name] = float64(d.DataPoints[0].Value)
			case metricdata.Gauge[int64]:
				got[m.Name] = float64(d.DataPoints[0].Value)
			case metricdata.Gauge[float64]:
				got[m.Name] = d.DataPoints[0].Value
			}
		}
	}
	want := map[string]float64{"outbox.published": 1, "outbox.publish.errors": 1, "outbox.pending": 4, "outbox.oldest_age_seconds": 1.5}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v (all: %v)", k, got[k], v, got)
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
