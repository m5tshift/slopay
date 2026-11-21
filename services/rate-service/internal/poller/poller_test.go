package poller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
)

var t0 = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

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

// fetcher answers each request with a price or an error; the quote time is the clock at the call.
type fetcher struct {
	mu     sync.Mutex
	clk    *clock
	prices map[request]string
	errs   map[request]error
	calls  map[request]int
	hook   func(ctx context.Context)
}

func (f *fetcher) set(src rate.Source, pair rate.Pair, price string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prices[request{src, pair}] = price
	f.errs[request{src, pair}] = err
}

func (f *fetcher) Quote(ctx context.Context, src rate.Source, pair rate.Pair) (rate.Quote, error) {
	if f.hook != nil {
		f.hook(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rq := request{src, pair}
	f.calls[rq]++
	if err := f.errs[rq]; err != nil {
		return rate.Quote{}, err
	}
	if err := ctx.Err(); err != nil {
		return rate.Quote{}, err
	}
	return rate.Quote{Source: src, Pair: pair, Price: rate.MustPrice(f.prices[rq]), QuotedAt: f.clk.now()}, nil
}

type latest struct {
	r  rate.Rate
	at time.Time
}

type publisher struct {
	mu      sync.Mutex
	ticks   []rate.Quote
	latests []latest
	err     error
}

func (p *publisher) PublishTick(_ context.Context, q rate.Quote) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ticks = append(p.ticks, q)
	return p.err
}

func (p *publisher) PublishLatest(_ context.Context, r rate.Rate, at time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latests = append(p.latests, latest{r, at})
	return p.err
}

func (p *publisher) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ticks, p.latests = nil, nil
}

type store struct {
	mu    sync.Mutex
	pairs map[rate.Pair]rate.Snapshot
	err   error
}

func (s *store) Put(_ context.Context, pair rate.Pair, snap rate.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.pairs[pair] = snap
	return nil
}

type env struct {
	p      *Poller
	clk    *clock
	f      *fetcher
	pub    *publisher
	st     *store
	cache  *rate.Cache
	reader *sdkmetric.ManualReader
	logs   *bytes.Buffer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	clk := &clock{t: t0}
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	e := &env{
		clk:    clk,
		f:      &fetcher{clk: clk, prices: map[request]string{}, errs: map[request]error{}, calls: map[request]int{}},
		pub:    &publisher{},
		st:     &store{pairs: map[rate.Pair]rate.Snapshot{}},
		cache:  rate.NewCache(),
		reader: reader,
		logs:   &bytes.Buffer{},
	}
	e.f.set(rate.Coinbase, rate.ETHUSD, "3700", nil)
	e.f.set(rate.Coinbase, rate.USDTUSD, "1", nil)
	e.f.set(rate.OKX, rate.ETHUSDT, "3710", nil)
	brs := map[rate.Source]*breaker.Breaker{}
	for _, src := range []rate.Source{rate.Coinbase, rate.OKX} {
		b, err := breaker.New(breaker.Config{Dependency: string(src), FailureThreshold: 3, OpenTimeout: 30 * time.Second,
			HalfOpenMax: 1, Now: clk.now, Meter: meter})
		if err != nil {
			t.Fatal(err)
		}
		brs[src] = b
	}
	p, err := New(Config{
		Fetcher: e.f, Breakers: brs, Publisher: e.pub, Store: e.st, Cache: e.cache,
		Interval: 5 * time.Second, USDTMaxAge: time.Hour, DivergenceMaxBps: 100,
		Now: clk.now, Logger: slog.New(slog.NewJSONHandler(e.logs, nil)), Meter: meter,
	})
	if err != nil {
		t.Fatal(err)
	}
	p.leading.Store(true)
	e.p = p
	return e
}

func (e *env) served(t *testing.T, pair rate.Pair) (rate.Rate, error) {
	t.Helper()
	s, ok := e.cache.Get(pair)
	r, _, err := rate.Evaluate(s, ok, e.clk.now(), 60*time.Second)
	if ok && e.st.err == nil {
		// Valkey always holds what the cache holds.
		if v := e.st.pairs[pair]; !v.UpdatedAt.Equal(s.UpdatedAt) || v.Diverged != s.Diverged {
			t.Errorf("valkey %+v, cache %+v", v, s)
		}
	}
	return r, err
}

// metrics collects the gauges and counters of the poller.
func (e *env) metrics(t *testing.T) map[string][]metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := e.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string][]metricdata.DataPoint[int64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				out[m.Name] = d.DataPoints
			case metricdata.Sum[int64]:
				out[m.Name] = d.DataPoints
			}
		}
	}
	return out
}

func gauge(t *testing.T, m map[string][]metricdata.DataPoint[int64], name string) (int64, bool) {
	t.Helper()
	dps := m[name]
	if len(dps) == 0 {
		return 0, false
	}
	return dps[0].Value, true
}

func requestsWith(m map[string][]metricdata.DataPoint[int64], src, result string) int64 {
	var n int64
	for _, dp := range m["weigate_rate_source_requests"] {
		s, _ := dp.Attributes.Value("source")
		r, _ := dp.Attributes.Value("result")
		if s.AsString() == src && r.AsString() == result {
			n += dp.Value
		}
	}
	return n
}

func TestBothSourcesAgreeGiveTheMean(t *testing.T) {
	e := newEnv(t)
	e.p.Poll(context.Background())

	r, err := e.served(t, rate.ETHUSD)
	if err != nil || r.Price.String() != "3705" || len(r.Sources) != 2 {
		t.Fatalf("ETH-USD %+v %v", r, err)
	}
	u, err := e.served(t, rate.USDTUSD)
	if err != nil || u.Price.String() != "1" || u.Sources[0] != rate.Coinbase {
		t.Fatalf("USDT-USD %+v %v", u, err)
	}
	if len(e.pub.ticks) != 3 || len(e.pub.latests) != 2 {
		t.Errorf("published %d ticks, %d latest", len(e.pub.ticks), len(e.pub.latests))
	}
	for _, l := range e.pub.latests {
		if !l.at.Equal(t0) {
			t.Errorf("latest at %v", l.at)
		}
	}
	m := e.metrics(t)
	if v, ok := gauge(t, m, "weigate_rate_diverged"); !ok || v != 0 {
		t.Errorf("diverged %d %v", v, ok)
	}
	if v, ok := gauge(t, m, "weigate_rate_divergence_bps"); !ok || v != 27 { // 10 / 3700
		t.Errorf("divergence %d %v", v, ok)
	}
	if requestsWith(m, "coinbase", "ok") != 2 || requestsWith(m, "okx", "ok") != 1 {
		t.Errorf("requests %+v", m["weigate_rate_source_requests"])
	}
}

func TestOKXIsConvertedThroughUSDT(t *testing.T) {
	e := newEnv(t)
	e.f.set(rate.Coinbase, rate.ETHUSD, "", errors.New("down"))
	e.f.set(rate.Coinbase, rate.USDTUSD, "0.998", nil)
	e.f.set(rate.OKX, rate.ETHUSDT, "3700", nil)
	e.p.Poll(context.Background())
	r, err := e.served(t, rate.ETHUSD)
	if err != nil || r.Price.String() != "3692.6" || len(r.Sources) != 1 || r.Sources[0] != rate.OKX {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestDivergenceMakesTheRateUnreliable(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.p.Poll(ctx)
	e.pub.reset()

	e.clk.add(5 * time.Second)
	e.f.set(rate.OKX, rate.ETHUSDT, "3750", nil) // 135 bps above 3700
	e.p.Poll(ctx)
	if _, err := e.served(t, rate.ETHUSD); !errors.Is(err, rate.ErrDiverged) {
		t.Fatalf("err %v", err)
	}
	s, _ := e.cache.Get(rate.ETHUSD)
	if s.Rate.Price.String() != "3705" {
		t.Errorf("the last reliable rate is kept for its age: %+v", s.Rate)
	}
	for _, l := range e.pub.latests {
		if l.r.Pair == rate.ETHUSD {
			t.Errorf("a diverged rate must not be published: %+v", l)
		}
	}
	m := e.metrics(t)
	if v, _ := gauge(t, m, "weigate_rate_diverged"); v != 1 {
		t.Errorf("diverged gauge %d", v)
	}
	if v, _ := gauge(t, m, "weigate_rate_divergence_bps"); v != 135 {
		t.Errorf("divergence %d", v)
	}
	if !strings.Contains(e.logs.String(), "rate sources diverge") {
		t.Error("no log")
	}

	// One source left: its price is the rate again (SRS 4.1.9), the mark is cleared.
	e.clk.add(5 * time.Second)
	e.f.set(rate.OKX, rate.ETHUSDT, "", errors.New("down"))
	e.p.Poll(ctx)
	if r, err := e.served(t, rate.ETHUSD); err != nil || r.Price.String() != "3700" {
		t.Fatalf("%+v %v", r, err)
	}
	if v, _ := gauge(t, e.metrics(t), "weigate_rate_diverged"); v != 0 {
		t.Errorf("diverged gauge %d", v)
	}
}

func TestDivergenceWithoutPreviousRate(t *testing.T) {
	e := newEnv(t)
	e.f.set(rate.OKX, rate.ETHUSDT, "3800", nil)
	e.p.Poll(context.Background())
	s, ok := e.cache.Get(rate.ETHUSD)
	if !ok || !s.Diverged || !s.Rate.Price.IsZero() {
		t.Fatalf("%+v %v", s, ok)
	}
	if _, ok := e.st.pairs[rate.ETHUSD]; !ok {
		t.Error("the mark is not in valkey")
	}
}

func TestNoQuoteKeepsTheLastRateUntilStale(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.p.Poll(ctx)
	for _, rq := range requests {
		e.f.set(rq.src, rq.pair, "", errors.New("down"))
	}
	// The breakers open after 3 failed rounds; the rate is served until it is 60 s old.
	for range 12 {
		e.clk.add(5 * time.Second)
		e.p.Poll(ctx)
	}
	if r, err := e.served(t, rate.ETHUSD); err != nil || !r.QuotedAt.Equal(t0) {
		t.Fatalf("at 60 s: %+v %v", r, err)
	}
	e.clk.add(time.Second)
	if _, err := e.served(t, rate.ETHUSD); !errors.Is(err, rate.ErrStale) {
		t.Fatalf("at 61 s: %v", err)
	}
	m := e.metrics(t)
	// okx: failures at 5, 10, 15 s open the breaker; the probe at 45 s fails and opens it again.
	if requestsWith(m, "okx", "error") != 4 || requestsWith(m, "okx", "rejected") != 8 {
		t.Errorf("okx requests: %+v", m["weigate_rate_source_requests"])
	}
	if e.f.calls[request{rate.OKX, rate.ETHUSDT}] != 5 {
		t.Errorf("okx was called %d times while open", e.f.calls[request{rate.OKX, rate.ETHUSDT}])
	}
	if !strings.Contains(e.logs.String(), "the last rate ages") {
		t.Error("no log")
	}
}

func TestOneSourceDownTheOtherServes(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.f.set(rate.OKX, rate.ETHUSDT, "", errors.New("down"))
	for range 4 {
		e.p.Poll(ctx)
		e.clk.add(5 * time.Second)
	}
	if r, err := e.served(t, rate.ETHUSD); err != nil || r.Sources[0] != rate.Coinbase || len(r.Sources) != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	m := e.metrics(t)
	for _, dp := range m["weigate_circuit_breaker_state"] {
		dep, _ := dp.Attributes.Value("dependency")
		want := int64(breaker.Closed)
		if dep.AsString() == "okx" {
			want = int64(breaker.Open)
		}
		if dp.Value != want {
			t.Errorf("breaker %s: %d", dep.AsString(), dp.Value)
		}
	}
	// The breaker lets a probe through after the open timeout; a good answer closes it.
	e.clk.add(30 * time.Second)
	e.f.set(rate.OKX, rate.ETHUSDT, "3700", nil)
	e.p.Poll(ctx)
	if r, _ := e.served(t, rate.ETHUSD); len(r.Sources) != 2 {
		t.Errorf("okx is back: %+v", r)
	}
}

func TestUSDTFromCacheWithinAnHour(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.p.Poll(ctx) // USDT/USD 1 at t0
	e.f.set(rate.Coinbase, rate.USDTUSD, "", errors.New("down"))
	e.f.set(rate.Coinbase, rate.ETHUSD, "", errors.New("down"))

	e.clk.add(time.Hour)
	e.p.Poll(ctx)
	if r, err := e.served(t, rate.ETHUSD); err != nil || r.Sources[0] != rate.OKX {
		t.Fatalf("at 1 h: %+v %v", r, err)
	}
	e.clk.add(time.Second)
	e.p.Poll(ctx)
	s, _ := e.cache.Get(rate.ETHUSD)
	if !s.UpdatedAt.Equal(t0.Add(time.Hour)) {
		t.Errorf("an OKX quote without USDT/USD must not update the rate: %+v", s)
	}
	if !strings.Contains(e.logs.String(), "no USDT/USD rate") {
		t.Error("no log")
	}
}

func TestUSDTRestoredFromRateLatest(t *testing.T) {
	e := newEnv(t)
	// A new leader after a restart: the cache holds what rate.latest gave.
	e.cache.Put(rate.USDTUSD, rate.Snapshot{
		Rate:      rate.Rate{Pair: rate.USDTUSD, Price: rate.MustPrice("0.5"), QuotedAt: t0.Add(-30 * time.Minute), Sources: []rate.Source{rate.Coinbase}},
		UpdatedAt: t0.Add(-30 * time.Minute),
	})
	e.f.set(rate.Coinbase, rate.USDTUSD, "", errors.New("down"))
	e.f.set(rate.Coinbase, rate.ETHUSD, "", errors.New("down"))
	e.p.Poll(context.Background())
	if r, err := e.served(t, rate.ETHUSD); err != nil || r.Price.String() != "1855" {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestUSDTDivergedSnapshotIsNotUsed(t *testing.T) {
	e := newEnv(t)
	e.cache.Put(rate.USDTUSD, rate.Snapshot{Diverged: true, UpdatedAt: t0})
	if _, ok := e.p.convert(rate.Quote{Source: rate.OKX, Pair: rate.ETHUSDT, Price: rate.MustPrice("1"), QuotedAt: t0}, t0); ok {
		t.Error("converted")
	}
}

func TestStoreAndPublishErrorsAreLogged(t *testing.T) {
	e := newEnv(t)
	e.st.err = errors.New("valkey down")
	e.pub.err = errors.New("kafka down")
	e.p.Poll(context.Background())
	if _, err := e.served(t, rate.ETHUSD); err != nil {
		t.Fatalf("the cache serves: %v", err)
	}
	for _, msg := range []string{"valkey write failed", "publish rate.tick failed", "publish rate.latest failed"} {
		if !strings.Contains(e.logs.String(), msg) {
			t.Errorf("no %q", msg)
		}
	}
}

func TestCancelledRoundWritesNothing(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	e.f.hook = func(context.Context) { cancel() }
	e.p.Poll(ctx)
	if _, ok := e.cache.Get(rate.ETHUSD); ok || len(e.pub.ticks) != 0 {
		t.Error("a cancelled round wrote")
	}
	m := e.metrics(t)
	if requestsWith(m, "okx", "error") != 0 {
		t.Error("cancellation counted as a source failure")
	}
	for _, dp := range m["weigate_circuit_breaker_state"] {
		if dp.Value != int64(breaker.Closed) {
			t.Errorf("breaker %v", dp)
		}
	}
}

func TestGaugesOnlyOnTheLeader(t *testing.T) {
	e := newEnv(t)
	if _, ok := gauge(t, e.metrics(t), "weigate_rate_diverged"); ok {
		t.Error("reported before the first round")
	}
	e.p.Poll(context.Background())
	e.p.leading.Store(false)
	m := e.metrics(t)
	if _, ok := gauge(t, m, "weigate_rate_diverged"); ok {
		t.Error("diverged reported by a follower")
	}
	if _, ok := gauge(t, m, "weigate_rate_divergence_bps"); ok {
		t.Error("divergence reported by a follower")
	}
}

func TestRunPollsUntilCancelled(t *testing.T) {
	e := newEnv(t)
	e.p.cfg.Interval = 10 * time.Millisecond
	e.p.leading.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.p.Run(ctx)
		close(done)
	}()
	deadline := time.After(5 * time.Second)
	for {
		e.f.mu.Lock()
		n := e.f.calls[request{rate.OKX, rate.ETHUSDT}]
		e.f.mu.Unlock()
		if n >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d rounds", n)
		case <-time.After(time.Millisecond):
		}
	}
	if !e.p.leading.Load() {
		t.Error("not leading while running")
	}
	cancel()
	<-done
	if e.p.leading.Load() {
		t.Error("still leading after Run")
	}
}

func TestNewValidation(t *testing.T) {
	e := newEnv(t)
	good := e.p.cfg
	cases := map[string]func(c *Config){
		"no fetcher": func(c *Config) { c.Fetcher = nil },
		"no breaker": func(c *Config) {
			c.Breakers = map[rate.Source]*breaker.Breaker{rate.Coinbase: c.Breakers[rate.Coinbase]}
		},
		"no interval":   func(c *Config) { c.Interval = 0 },
		"negative bps":  func(c *Config) { c.DivergenceMaxBps = -1 },
		"no usdt limit": func(c *Config) { c.USDTMaxAge = 0 },
	}
	for name, mod := range cases {
		c := good
		mod(&c)
		if _, err := New(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	c := good
	c.Now, c.Logger, c.Meter = nil, nil, nil
	p, err := New(c)
	if err != nil || p.cfg.Now == nil || p.log == nil {
		t.Errorf("defaults: %v", err)
	}
}

func TestAgeGauge(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	cache := rate.NewCache()
	clk := &clock{t: t0}
	if err := RegisterAgeGauge(meter, cache, clk.now); err != nil {
		t.Fatal(err)
	}
	cache.Put(rate.ETHUSD, rate.Snapshot{Rate: rate.Rate{Pair: rate.ETHUSD, Price: rate.MustPrice("1"), QuotedAt: t0.Add(-42 * time.Second)}, UpdatedAt: t0})
	cache.Put(rate.USDTUSD, rate.Snapshot{Diverged: true, UpdatedAt: t0}) // no rate: no age
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	dps := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[float64]).DataPoints
	if rm.ScopeMetrics[0].Metrics[0].Name != "weigate_rate_age_seconds" || len(dps) != 1 || dps[0].Value != 42 {
		t.Fatalf("%+v", dps)
	}
	if v, _ := dps[0].Attributes.Value("pair"); v.AsString() != "ETH-USD" {
		t.Errorf("pair %s", v.AsString())
	}
	if err := RegisterAgeGauge(nil, cache, clk.now); err != nil {
		t.Error(err)
	}
}

// bucket grants the first n requests of each source and refuses the rest.
type limit struct {
	mu    sync.Mutex
	left  map[string]int
	asked map[string]int
}

func (l *limit) Allow(_ context.Context, src string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asked[src]++
	if l.left[src] <= 0 {
		return false
	}
	l.left[src]--
	return true
}

func TestOutgoingLimitSkipsRequestsWithoutTrippingTheBreaker(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	lim := &limit{left: map[string]int{"coinbase": 100, "okx": 0}, asked: map[string]int{}}
	e.p.cfg.Limiter = lim
	for range 5 {
		e.p.Poll(ctx)
		e.clk.add(5 * time.Second)
	}
	if n := e.f.calls[request{rate.OKX, rate.ETHUSDT}]; n != 0 {
		t.Fatalf("okx was asked %d times past its limit", n)
	}
	if lim.asked["okx"] != 5 || lim.asked["coinbase"] != 10 {
		t.Fatalf("limiter asked %v", lim.asked)
	}
	m := e.metrics(t)
	if got := requestsWith(m, "okx", "limited"); got != 5 {
		t.Errorf("limited okx = %d", got)
	}
	if got := requestsWith(m, "okx", "error"); got != 0 {
		t.Errorf("a limited request counted as an error: %d", got)
	}
	for _, dp := range m["weigate_circuit_breaker_state"] {
		if dp.Value != int64(breaker.Closed) {
			t.Errorf("breaker left closed state: %+v", dp)
		}
	}
	// Coinbase alone still serves the rate.
	if r, err := e.served(t, rate.ETHUSD); err != nil || len(r.Sources) != 1 || r.Sources[0] != rate.Coinbase {
		t.Fatalf("%+v %v", r, err)
	}

	// Tokens come back: OKX joins the aggregation.
	lim.mu.Lock()
	lim.left["okx"] = 1
	lim.mu.Unlock()
	e.p.Poll(ctx)
	if r, _ := e.served(t, rate.ETHUSD); len(r.Sources) != 2 {
		t.Errorf("okx after refill: %+v", r)
	}
}

func TestNoLimiterMeansNoLimit(t *testing.T) {
	e := newEnv(t)
	e.p.Poll(context.Background())
	if n := e.f.calls[request{rate.OKX, rate.ETHUSDT}]; n != 1 {
		t.Fatalf("okx asked %d times", n)
	}
}
