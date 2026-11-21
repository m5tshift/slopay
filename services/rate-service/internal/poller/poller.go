// Package poller is the work of the leader replica (SRS 4.1.3): every interval it asks the sources for
// quotes through a Circuit Breaker per source (SRS 4.1.9), publishes every quote to rate.ticks, aggregates
// the ETH/USD rate, and writes the result to the replica cache, to Valkey and to rate.latest.
package poller

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/m5tshift/slopay/pkg/breaker"
	"github.com/m5tshift/slopay/services/rate-service/internal/rate"
	"github.com/m5tshift/slopay/services/rate-service/internal/source"
)

const meterName = "github.com/m5tshift/slopay/services/rate-service/internal/poller"

// Publisher publishes ticks and latest rates (events.Publisher).
type Publisher interface {
	PublishTick(ctx context.Context, q rate.Quote) error
	PublishLatest(ctx context.Context, r rate.Rate, at time.Time) error
}

// Store writes the snapshot of a pair to Valkey (store.Valkey).
type Store interface {
	Put(ctx context.Context, pair rate.Pair, s rate.Snapshot) error
}

// Limiter is the outgoing rate limit of the sources (limiter.Limiter): it takes a token of the bucket
// named after the source.
type Limiter interface {
	Allow(ctx context.Context, source string) bool
}

// Config holds the dependencies and the numbers of SRS 4.1.7 and 4.1.9.
type Config struct {
	Fetcher   source.Fetcher
	Breakers  map[rate.Source]*breaker.Breaker
	Publisher Publisher
	Store     Store
	Cache     *rate.Cache
	// Limiter limits the requests to the sources; nil means no limit (the mock mode).
	Limiter Limiter
	// Interval is the polling period (5s).
	Interval time.Duration
	// USDTMaxAge is how long a USDT/USD rate converts OKX prices (1h).
	USDTMaxAge time.Duration
	// DivergenceMaxBps is the largest divergence of a reliable rate (100 = 1 %).
	DivergenceMaxBps int64
	// Now substitutes the clock in tests; defaults to time.Now.
	Now    func() time.Time
	Logger *slog.Logger
	// Meter defaults to the global OTel meter.
	Meter metric.Meter
}

// request is one quote asked in a round. Coinbase quotes ETH/USD and USDT/USD, OKX quotes ETH/USDT (SRS 4.1.3).
type request struct {
	src  rate.Source
	pair rate.Pair
}

var requests = []request{{rate.Coinbase, rate.ETHUSD}, {rate.Coinbase, rate.USDTUSD}, {rate.OKX, rate.ETHUSDT}}

// Results of a source request in slopay_rate_source_requests_total.
const (
	resultOK       = "ok"
	resultError    = "error"
	resultRejected = "rejected" // the breaker of the source is open
	resultLimited  = "limited"  // no token in the outgoing limit of the source: the request is not sent
)

// Poller polls the sources. Run it only on the leader.
type Poller struct {
	cfg      Config
	log      *slog.Logger
	requests metric.Int64Counter
	leading  atomic.Bool

	mu            sync.Mutex
	diverged      bool
	evaluated     bool // the ETH/USD rate was evaluated at least once
	divergenceBps int64
	compared      bool // two quotes were compared at least once
}

// New validates cfg and registers the metrics.
func New(cfg Config) (*Poller, error) {
	switch {
	case cfg.Fetcher == nil || cfg.Publisher == nil || cfg.Store == nil || cfg.Cache == nil:
		return nil, errors.New("poller: fetcher, publisher, store and cache are required")
	case cfg.Breakers[rate.Coinbase] == nil || cfg.Breakers[rate.OKX] == nil:
		return nil, errors.New("poller: a breaker per source is required")
	case cfg.Interval <= 0 || cfg.USDTMaxAge <= 0 || cfg.DivergenceMaxBps < 0:
		return nil, errors.New("poller: interval and USDT max age must be positive, divergence non-negative")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	p := &Poller{cfg: cfg, log: cfg.Logger}
	if p.log == nil {
		p.log = slog.Default()
	}
	m := cfg.Meter
	if m == nil {
		m = otel.Meter(meterName)
	}
	var err error
	if p.requests, err = m.Int64Counter("slopay_rate_source_requests",
		metric.WithDescription("Quote requests to the rate sources by result: ok, error, rejected (breaker open), limited (outgoing rate limit)")); err != nil {
		return nil, err
	}
	pairAttr := metric.WithAttributes(attribute.String("pair", string(rate.ETHUSD)))
	if _, err = m.Int64ObservableGauge("slopay_rate_diverged",
		metric.WithDescription("1 while the sources diverge beyond the limit and the rate is unreliable; reported by the leader"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if d, ok := p.divergedState(); ok {
				v := int64(0)
				if d {
					v = 1
				}
				o.Observe(v, pairAttr)
			}
			return nil
		})); err != nil {
		return nil, err
	}
	if _, err = m.Int64ObservableGauge("slopay_rate_divergence_bps",
		metric.WithDescription("Divergence of the last two compared quotes, basis points; reported by the leader"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if bps, ok := p.divergence(); ok {
				o.Observe(bps, pairAttr)
			}
			return nil
		})); err != nil {
		return nil, err
	}
	return p, nil
}

// divergedState reports whether the rate is unreliable; ok is false on a replica that does not lead or has
// not evaluated the rate yet.
func (p *Poller) divergedState() (diverged, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.diverged, p.evaluated && p.leading.Load()
}

// divergence is the divergence of the last two compared quotes; ok as in divergedState.
func (p *Poller) divergence() (bps int64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.divergenceBps, p.compared && p.leading.Load()
}

// Run polls every interval until ctx is done; the first round starts at once. It is the work of leader.Run.
func (p *Poller) Run(ctx context.Context) {
	p.leading.Store(true)
	defer p.leading.Store(false)
	p.log.InfoContext(ctx, "polling the rate sources", "interval", p.cfg.Interval)
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	for {
		rctx, cancel := context.WithTimeout(ctx, p.cfg.Interval)
		p.Poll(rctx)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Poll runs one round.
func (p *Poller) Poll(ctx context.Context) {
	quotes := p.fetchAll(ctx)
	if ctx.Err() != nil {
		return // leadership lost or shutdown: the next leader writes the rate
	}
	now := p.cfg.Now()
	for _, q := range quotes {
		if err := p.cfg.Publisher.PublishTick(ctx, q); err != nil {
			p.log.ErrorContext(ctx, "publish rate.tick failed", "source", q.Source, "pair", q.Pair, "error", err)
		}
	}

	if q, ok := quotes[request{rate.Coinbase, rate.USDTUSD}]; ok {
		r, _, _ := rate.Aggregate(rate.USDTUSD, []rate.Quote{q}, p.cfg.DivergenceMaxBps)
		p.save(ctx, rate.Snapshot{Rate: r, UpdatedAt: now})
	}

	var eth []rate.Quote
	if q, ok := quotes[request{rate.Coinbase, rate.ETHUSD}]; ok {
		eth = append(eth, q)
	}
	if q, ok := quotes[request{rate.OKX, rate.ETHUSDT}]; ok {
		if usd, ok := p.convert(q, now); ok {
			eth = append(eth, usd)
		} else {
			p.log.WarnContext(ctx, "okx quote skipped: no USDT/USD rate within the limit", "usdt_max_age", p.cfg.USDTMaxAge)
		}
	}
	r, outcome, bps := rate.Aggregate(rate.ETHUSD, eth, p.cfg.DivergenceMaxBps)
	p.mu.Lock()
	if len(eth) == 2 {
		p.compared, p.divergenceBps = true, bps
	}
	if outcome != rate.NoQuote {
		p.evaluated, p.diverged = true, outcome == rate.Diverged
	}
	p.mu.Unlock()
	switch outcome {
	case rate.NoQuote:
		p.log.WarnContext(ctx, "no ETH/USD quote, the last rate ages")
	case rate.Aggregated:
		p.save(ctx, rate.Snapshot{Rate: r, UpdatedAt: now})
	case rate.Diverged:
		// The last reliable rate is kept for its age only; the mark makes it unreliable (SRS 4.1.9).
		last, _ := p.cfg.Cache.Get(rate.ETHUSD)
		p.log.WarnContext(ctx, "rate sources diverge, the rate is unreliable", "divergence_bps", bps, "limit_bps", p.cfg.DivergenceMaxBps)
		p.save(ctx, rate.Snapshot{Rate: last.Rate, Diverged: true, UpdatedAt: now})
	}
}

// convert turns the OKX quote into USD with the latest USDT/USD rate, which is in the cache of this replica:
// written by this round, a previous leader round or restored from rate.latest.
func (p *Poller) convert(q rate.Quote, now time.Time) (rate.Quote, bool) {
	s, ok := p.cfg.Cache.Get(rate.USDTUSD)
	if !ok || s.Diverged {
		return rate.Quote{}, false
	}
	usdt := rate.Quote{Source: rate.Coinbase, Pair: rate.USDTUSD, Price: s.Rate.Price, QuotedAt: s.Rate.QuotedAt}
	return rate.Convert(q, usdt, now, p.cfg.USDTMaxAge)
}

// save writes a snapshot to the cache, Valkey and, for a reliable rate, rate.latest. Failures of Valkey and
// Kafka are logged: the other copies still serve.
func (p *Poller) save(ctx context.Context, s rate.Snapshot) {
	pair := s.Rate.Pair
	if s.Diverged {
		pair = rate.ETHUSD
	}
	p.cfg.Cache.Put(pair, s)
	if err := p.cfg.Store.Put(ctx, pair, s); err != nil {
		p.log.WarnContext(ctx, "valkey write failed", "pair", pair, "error", err)
	}
	if s.Diverged {
		return
	}
	if err := p.cfg.Publisher.PublishLatest(ctx, s.Rate, s.UpdatedAt); err != nil {
		p.log.ErrorContext(ctx, "publish rate.latest failed", "pair", pair, "error", err)
	}
}

// fetchAll asks all sources in parallel and returns the quotes that came back.
func (p *Poller) fetchAll(ctx context.Context) map[request]rate.Quote {
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		out = map[request]rate.Quote{}
	)
	for _, rq := range requests {
		wg.Go(func() {
			if q, ok := p.fetch(ctx, rq); ok {
				mu.Lock()
				out[rq] = q
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return out
}

func (p *Poller) fetch(ctx context.Context, rq request) (rate.Quote, bool) {
	attrs := func(result string) metric.MeasurementOption {
		return metric.WithAttributes(attribute.String("source", string(rq.src)), attribute.String("pair", string(rq.pair)),
			attribute.String("result", result))
	}
	if p.cfg.Limiter != nil && !p.cfg.Limiter.Allow(ctx, string(rq.src)) {
		p.requests.Add(ctx, 1, attrs(resultLimited))
		p.log.WarnContext(ctx, "outgoing rate limit reached, request skipped", "source", rq.src, "pair", rq.pair)
		return rate.Quote{}, false
	}
	done, err := p.cfg.Breakers[rq.src].Allow()
	if err != nil {
		p.requests.Add(ctx, 1, attrs(resultRejected))
		return rate.Quote{}, false
	}
	q, err := p.cfg.Fetcher.Quote(ctx, rq.src, rq.pair)
	if err != nil && ctx.Err() != nil {
		done(true) // cancelled by us, not a failure of the source
		return rate.Quote{}, false
	}
	done(err == nil)
	if err != nil {
		p.requests.Add(ctx, 1, attrs(resultError))
		p.log.WarnContext(ctx, "rate source request failed", "source", rq.src, "pair", rq.pair, "error", err)
		return rate.Quote{}, false
	}
	p.requests.Add(ctx, 1, attrs(resultOK))
	return q, true
}

// RegisterAgeGauge registers slopay_rate_age_seconds{pair}: the age of the latest rate in the cache of this
// replica (NFR-08, alert RateStale). Every replica reports it.
func RegisterAgeGauge(m metric.Meter, cache *rate.Cache, now func() time.Time) error {
	if m == nil {
		m = otel.Meter(meterName)
	}
	_, err := m.Float64ObservableGauge("slopay_rate_age_seconds",
		metric.WithDescription("Age of the latest rate of the pair in this replica, seconds from quotedAt"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			for _, pair := range rate.Served {
				if s, ok := cache.Get(pair); ok && !s.Rate.Price.IsZero() {
					o.Observe(max(now().Sub(s.Rate.QuotedAt), 0).Seconds(), metric.WithAttributes(attribute.String("pair", string(pair))))
				}
			}
			return nil
		}))
	return err
}
