// Package rate is the domain of rate-service: quotes of the sources, aggregation of the ETH/USD rate
// (SRS 4.1.9), conversion of the OKX price through USDT/USD, the in-memory cache of the latest rates and
// the freshness rules of a served rate (SRS 4.1.7, NFR-08). Time is always passed in, so the rules are
// tested with a substituted clock.
package rate

import (
	"errors"
	"sync"
	"time"
)

// Source is a quote source.
type Source string

// Sources of the SRS 4.1.3: Coinbase quotes ETH/USD and USDT/USD, OKX quotes ETH/USDT.
const (
	Coinbase Source = "coinbase"
	OKX      Source = "okx"
)

// Pair is a currency pair, BASE-QUOTE.
type Pair string

// Pairs. ETH-USD and USDT-USD are served; ETH-USDT is only quoted by OKX.
const (
	ETHUSD  Pair = "ETH-USD"
	USDTUSD Pair = "USDT-USD"
	ETHUSDT Pair = "ETH-USDT"
)

// Served are the pairs of GET /rates/{pair} and rate.latest.
var Served = []Pair{ETHUSD, USDTUSD}

// IsServed reports whether the pair is served.
func IsServed(p Pair) bool { return p == ETHUSD || p == USDTUSD }

// Quote is a price of one source.
type Quote struct {
	Source   Source
	Pair     Pair
	Price    Price
	QuotedAt time.Time
}

// Rate is an aggregated USD rate of a served pair.
type Rate struct {
	Pair     Pair
	Price    Price
	QuotedAt time.Time
	Sources  []Source
}

// Snapshot is the state of a pair at UpdatedAt: either a reliable rate or the divergence mark, which
// makes the pair unreliable until the next rate (SRS 4.1.9).
type Snapshot struct {
	Rate      Rate
	Diverged  bool
	UpdatedAt time.Time
}

// Newer picks the later of two snapshots; ok reports whether each one is present.
func Newer(a Snapshot, aok bool, b Snapshot, bok bool) (Snapshot, bool) {
	switch {
	case !aok && !bok:
		return Snapshot{}, false
	case !aok:
		return b, true
	case !bok:
		return a, true
	case b.UpdatedAt.After(a.UpdatedAt):
		return b, true
	}
	return a, true
}

// Errors of Evaluate, the reasons of the contract's 503.
var (
	ErrUnavailable = errors.New("rate: no rate")
	ErrStale       = errors.New("rate: stale")
	ErrDiverged    = errors.New("rate: sources diverge")
)

// Evaluate decides whether a snapshot can be served at now: the rate must exist, the sources must not diverge,
// and the rate must be at most maxAge old (SRS 4.1.7: older than 60 s is stale). It returns the whole seconds of age.
func Evaluate(s Snapshot, ok bool, now time.Time, maxAge time.Duration) (Rate, int, error) {
	switch {
	case !ok:
		return Rate{}, 0, ErrUnavailable
	case s.Diverged:
		return Rate{}, 0, ErrDiverged
	case s.Rate.Price.IsZero():
		return Rate{}, 0, ErrUnavailable
	}
	age := max(now.Sub(s.Rate.QuotedAt), 0) // a source clock ahead of ours gives a negative age
	if age > maxAge {
		return Rate{}, 0, ErrStale
	}
	return s.Rate, int(age / time.Second), nil
}

// Convert turns the OKX ETH/USDT quote into an ETH/USD quote with the USDT/USD rate of Coinbase. The USDT/USD
// rate is used while it is at most usdtMaxAge old at now (SRS 4.1.9: 1 h); ok is false otherwise. The result
// keeps the source and the quote time of the ETH/USDT quote.
func Convert(ethUSDT, usdtUSD Quote, now time.Time, usdtMaxAge time.Duration) (Quote, bool) {
	if usdtUSD.Price.IsZero() || now.Sub(usdtUSD.QuotedAt) > usdtMaxAge {
		return Quote{}, false
	}
	return Quote{Source: ethUSDT.Source, Pair: ETHUSD, Price: ethUSDT.Price.Mul(usdtUSD.Price), QuotedAt: ethUSDT.QuotedAt}, true
}

// Outcome of an aggregation.
type Outcome int

const (
	// NoQuote: no source gave a price, the last rate stays and ages.
	NoQuote Outcome = iota
	// Aggregated: the rate is the mean of both sources or the price of the one left.
	Aggregated
	// Diverged: both sources answered and diverge by more than the limit; the rate is unreliable.
	Diverged
)

// Aggregate builds the ETH/USD rate from the USD quotes of the sources that answered (SRS 4.1.9):
// two quotes within maxBps of each other give the mean, one quote gives its price, two quotes beyond
// maxBps give Diverged. The quote time of a mean is the older one, so the age is never understated.
// divergenceBps is the divergence of two quotes, 0 otherwise.
func Aggregate(pair Pair, quotes []Quote, maxBps int64) (r Rate, out Outcome, divergenceBps int64) {
	if len(quotes) == 0 {
		return Rate{}, NoQuote, 0
	}
	a := quotes[0]
	if len(quotes) < 2 {
		return Rate{Pair: pair, Price: a.Price, QuotedAt: a.QuotedAt, Sources: []Source{a.Source}}, Aggregated, 0
	}
	b := quotes[1]
	divergenceBps = DivergenceBps(a.Price, b.Price)
	if !WithinBps(a.Price, b.Price, maxBps) {
		return Rate{}, Diverged, divergenceBps
	}
	at := a.QuotedAt
	if b.QuotedAt.Before(at) {
		at = b.QuotedAt
	}
	return Rate{Pair: pair, Price: Mean(a.Price, b.Price), QuotedAt: at, Sources: []Source{a.Source, b.Source}}, Aggregated, divergenceBps
}

// Cache keeps the latest snapshot of every pair in memory. A replica serves from it when Valkey is
// unavailable (SRS 4.1.7). Safe for concurrent use.
type Cache struct {
	mu    sync.RWMutex
	pairs map[Pair]Snapshot
}

// NewCache returns an empty cache.
func NewCache() *Cache { return &Cache{pairs: map[Pair]Snapshot{}} }

// Put stores s unless the cache already holds a snapshot of the pair that is not older. It reports
// whether s was stored: replayed or reordered rate.latest events never move a pair back in time.
func (c *Cache) Put(pair Pair, s Snapshot) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.pairs[pair]; ok && !s.UpdatedAt.After(cur.UpdatedAt) {
		return false
	}
	c.pairs[pair] = s
	return true
}

// Get returns the snapshot of the pair.
func (c *Cache) Get(pair Pair) (Snapshot, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.pairs[pair]
	return s, ok
}
