package breaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

var errBoom = errors.New("boom")

func newTest(t *testing.T, mod func(*Config)) (*Breaker, *clock) {
	t.Helper()
	clk := &clock{t: time.Unix(1000, 0)}
	cfg := Config{Dependency: "dep", FailureThreshold: 3, OpenTimeout: 10 * time.Second, HalfOpenMax: 2, Now: clk.now}
	if mod != nil {
		mod(&cfg)
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b, clk
}

func fail(b *Breaker) error { return b.Do(func() error { return errBoom }) }
func ok(b *Breaker) error   { return b.Do(func() error { return nil }) }

func TestStateString(t *testing.T) {
	for s, w := range map[State]string{Closed: "closed", HalfOpen: "half-open", Open: "open", State(9): "unknown"} {
		if s.String() != w {
			t.Errorf("%d: %s", s, s)
		}
	}
}

func TestOpensAfterThreshold(t *testing.T) {
	b, _ := newTest(t, nil)
	for i := 0; i < 2; i++ {
		if err := fail(b); !errors.Is(err, errBoom) {
			t.Fatal(err)
		}
	}
	if b.State() != Closed {
		t.Fatal("must stay closed below threshold")
	}
	_ = fail(b)
	if b.State() != Open {
		t.Fatal("must open at threshold")
	}
	called := false
	if err := b.Do(func() error { called = true; return nil }); !errors.Is(err, ErrOpen) || called {
		t.Fatalf("open breaker must reject: %v %v", err, called)
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	b, _ := newTest(t, nil)
	_ = fail(b)
	_ = fail(b)
	_ = ok(b)
	_ = fail(b)
	_ = fail(b)
	if b.State() != Closed {
		t.Fatal("success must reset the consecutive failure count")
	}
}

func TestHalfOpenAfterTimeoutThenClose(t *testing.T) {
	b, clk := newTest(t, nil)
	for i := 0; i < 3; i++ {
		_ = fail(b)
	}
	clk.add(9 * time.Second)
	if b.State() != Open {
		t.Fatal("still open before timeout")
	}
	clk.add(time.Second)
	if b.State() != HalfOpen {
		t.Fatal("half-open after timeout")
	}
	_ = ok(b)
	if b.State() != HalfOpen {
		t.Fatal("needs HalfOpenMax successes")
	}
	_ = ok(b)
	if b.State() != Closed {
		t.Fatal("closed after probe successes")
	}
	// failure counter starts from zero after closing
	_ = fail(b)
	if b.State() != Closed {
		t.Fatal("counter must be reset")
	}
}

func TestHalfOpenFailureReopens(t *testing.T) {
	b, clk := newTest(t, nil)
	for i := 0; i < 3; i++ {
		_ = fail(b)
	}
	clk.add(10 * time.Second)
	_ = fail(b)
	if b.State() != Open {
		t.Fatal("probe failure must reopen")
	}
	if err := ok(b); !errors.Is(err, ErrOpen) {
		t.Fatal("reopened breaker must reject until a new timeout")
	}
	clk.add(10 * time.Second)
	if b.State() != HalfOpen {
		t.Fatal("new timeout counts from the reopen")
	}
}

func TestHalfOpenLimitsConcurrentProbes(t *testing.T) {
	b, clk := newTest(t, nil)
	for i := 0; i < 3; i++ {
		_ = fail(b)
	}
	clk.add(10 * time.Second)
	d1, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	d2, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatal("third probe must be rejected")
	}
	d1(true)
	d2(true)
	if b.State() != Closed {
		t.Fatal("must close")
	}
}

func TestLateResultsIgnored(t *testing.T) {
	b, clk := newTest(t, nil)
	late, _ := b.Allow() // admitted while closed
	for i := 0; i < 3; i++ {
		_ = fail(b)
	}
	late(false) // finishes while open: ignored
	late(true)
	if b.State() != Open {
		t.Fatal("late closed-era result must not change state")
	}
	clk.add(10 * time.Second)
	probe, _ := b.Allow()
	_ = fail(b) // rejected? no: second probe, fails and reopens
	if b.State() != Open {
		t.Fatal("expected open")
	}
	probe(true) // probe from earlier half-open period
	if b.State() != Open {
		t.Fatal("stale probe result must be ignored")
	}
	clk.add(10 * time.Second)
	if b.State() != HalfOpen {
		t.Fatal("half-open again")
	}
	// stale probe with a state that is half-open but a different generation
	probe(true)
	if b.State() != HalfOpen {
		t.Fatal("stale probe from older generation must be ignored")
	}
}

func TestIsFailure(t *testing.T) {
	b, _ := newTest(t, func(c *Config) {
		c.FailureThreshold = 1
		c.IsFailure = func(err error) bool { return errors.Is(err, errBoom) }
	})
	_ = b.Do(func() error { return context.Canceled })
	if b.State() != Closed {
		t.Fatal("non-failure error must not count")
	}
	_ = fail(b)
	if b.State() != Open {
		t.Fatal("failure must open")
	}
}

func TestOpensWithin30s(t *testing.T) {
	// A dependency failing continuously with one call per second trips the breaker well inside NFR-09's 30 s.
	b, clk := newTest(t, func(c *Config) { c.FailureThreshold = 5 })
	start := clk.now()
	for b.State() == Closed {
		_ = fail(b)
		clk.add(time.Second)
	}
	if clk.now().Sub(start) > 30*time.Second {
		t.Fatal("too slow")
	}
}

func TestNewValidation(t *testing.T) {
	good := Config{Dependency: "d", FailureThreshold: 1, OpenTimeout: time.Second, HalfOpenMax: 1}
	for name, mod := range map[string]func(*Config){
		"dep":       func(c *Config) { c.Dependency = "" },
		"threshold": func(c *Config) { c.FailureThreshold = 0 },
		"timeout":   func(c *Config) { c.OpenTimeout = 0 },
		"half":      func(c *Config) { c.HalfOpenMax = 0 },
	} {
		c := good
		mod(&c)
		if _, err := New(c); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := New(good); err != nil {
		t.Fatal(err)
	}
}

func TestConfigFromEnv(t *testing.T) {
	def := Config{FailureThreshold: 5, OpenTimeout: time.Second, HalfOpenMax: 1}
	c, err := ConfigFromEnv("CB", "x", def)
	if err != nil || c.FailureThreshold != 5 || c.Dependency != "x" {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	t.Setenv("CB_FAILURE_THRESHOLD", "7")
	t.Setenv("CB_OPEN_TIMEOUT", "15s")
	t.Setenv("CB_HALF_OPEN_MAX", "3")
	c, err = ConfigFromEnv("CB", "x", def)
	if err != nil || c.FailureThreshold != 7 || c.OpenTimeout != 15*time.Second || c.HalfOpenMax != 3 {
		t.Fatalf("env: %+v %v", c, err)
	}
	for _, kv := range [][2]string{{"CB_FAILURE_THRESHOLD", "x"}, {"CB_OPEN_TIMEOUT", "x"}, {"CB_HALF_OPEN_MAX", "x"}} {
		t.Setenv("CB_FAILURE_THRESHOLD", "7")
		t.Setenv("CB_OPEN_TIMEOUT", "15s")
		t.Setenv("CB_HALF_OPEN_MAX", "3")
		t.Setenv(kv[0], kv[1])
		if _, err := ConfigFromEnv("CB", "x", def); err == nil {
			t.Errorf("%s: expected error", kv[0])
		}
	}
}

func TestStateMetric(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	b, _ := newTest(t, func(c *Config) { c.Meter = mp.Meter("test"); c.Dependency = "okx" })
	for i := 0; i < 3; i++ {
		_ = fail(b)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	m := rm.ScopeMetrics[0].Metrics[0]
	if m.Name != "slopay_circuit_breaker_state" {
		t.Fatal(m.Name)
	}
	dp := m.Data.(metricdata.Gauge[int64]).DataPoints[0]
	if dp.Value != int64(Open) {
		t.Fatalf("value %d", dp.Value)
	}
	if v, _ := dp.Attributes.Value("dependency"); v.AsString() != "okx" {
		t.Fatal("missing dependency label")
	}
}

func TestConcurrent(t *testing.T) {
	b, _ := newTest(t, nil)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Do(func() error {
				if i%2 == 0 {
					return errBoom
				}
				return nil
			})
			_ = b.State()
		}()
	}
	wg.Wait()
}
