// Package market simulates two independent rate sources, coinbase and okx, as bounded random walks around
// configured prices. Prices are integers in millionths of a unit (no floats, AGENTS.md).
package market

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Scale is the number of price units in one whole unit: prices have six decimal digits.
const Scale = 1_000_000

// MaxDivergenceBps bounds the shift of okx against coinbase (50 %).
const MaxDivergenceBps = 5000

// Source is an imitated exchange.
type Source string

// Sources.
const (
	Coinbase Source = "coinbase"
	OKX      Source = "okx"
)

// Pair is a traded pair.
type Pair string

// Pairs. Coinbase quotes ETH-USD and USDT-USD, OKX quotes ETH-USDT (SRS 4.1.3).
const (
	ETHUSD  Pair = "ETH-USD"
	ETHUSDT Pair = "ETH-USDT"
	USDTUSD Pair = "USDT-USD"
)

var (
	// ErrUnknownSource is returned for a source that does not exist.
	ErrUnknownSource = errors.New("market: unknown source")
	// ErrUnknownPair is returned when the source does not quote the pair.
	ErrUnknownPair = errors.New("market: source does not quote the pair")
	// ErrDisabled is returned while the source is disabled.
	ErrDisabled = errors.New("market: source is disabled")
	// ErrDivergenceRange is returned for a divergence outside [-MaxDivergenceBps, MaxDivergenceBps].
	ErrDivergenceRange = errors.New("market: divergence out of range")
)

// Config configures the simulation.
type Config struct {
	// ETHUSD and USDTUSD are the prices the walks circle around, in millionths.
	ETHUSD, USDTUSD int64
	// StepBps is the largest change of a price per tick, in basis points.
	StepBps int
	// BandBps is how far a price may leave its centre, in basis points.
	BandBps int
	// Now returns the current time; time.Now by default.
	Now func() time.Time
	// Intn returns a random number in [0, n); math/rand/v2 by default.
	Intn func(n int) int
}

// Quote is the latest price of a pair at a source.
type Quote struct {
	Source   Source
	Pair     Pair
	Price    int64
	QuotedAt time.Time
}

// State is the state of the commands.
type State struct {
	Enabled       map[Source]bool
	DivergenceBps int
}

type series struct {
	source Source
	pair   Pair
}

// Market holds the two sources.
type Market struct {
	cfg Config

	mu         sync.Mutex
	prices     map[series]int64
	quotedAt   map[series]time.Time
	disabled   map[Source]bool
	divergence int
}

// New creates a market with every price at its centre.
func New(cfg Config) (*Market, error) {
	if cfg.ETHUSD <= 0 || cfg.USDTUSD <= 0 {
		return nil, errors.New("market: prices must be positive")
	}
	if cfg.StepBps < 0 || cfg.BandBps < 0 {
		return nil, errors.New("market: step and band must not be negative")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Intn == nil {
		cfg.Intn = rand.IntN //nolint:gosec // simulated quotes, not a secret
	}
	m := &Market{
		cfg:      cfg,
		prices:   map[series]int64{},
		quotedAt: map[series]time.Time{},
		disabled: map[Source]bool{},
	}
	now := cfg.Now()
	for _, s := range allSeries {
		m.prices[s] = m.center(s)
		m.quotedAt[s] = now
	}
	return m, nil
}

var allSeries = []series{{Coinbase, ETHUSD}, {Coinbase, USDTUSD}, {OKX, ETHUSDT}}

// center is the price a series is attracted to. OKX quotes ETH in USDT, so its centre is the USD price divided by
// USDT/USD, moved by the divergence.
func (m *Market) center(s series) int64 {
	switch s {
	case series{Coinbase, ETHUSD}:
		return m.cfg.ETHUSD
	case series{Coinbase, USDTUSD}:
		return m.cfg.USDTUSD
	default:
		c := m.cfg.ETHUSD * Scale / m.cfg.USDTUSD
		return c * int64(10000+m.divergence) / 10000
	}
}

// Tick makes one step of every walk. A walk that is far from its centre (after a divergence command) is pulled
// into the band around it.
func (m *Market) Tick() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.cfg.Now()
	for _, s := range allSeries {
		p := m.prices[s]
		if m.cfg.StepBps > 0 {
			p += p * int64(m.cfg.Intn(2*m.cfg.StepBps+1)-m.cfg.StepBps) / 10000
		}
		c := m.center(s)
		lo, hi := c*int64(10000-m.cfg.BandBps)/10000, c*int64(10000+m.cfg.BandBps)/10000
		p = min(max(p, lo), hi)
		m.prices[s], m.quotedAt[s] = p, now
	}
}

// Run ticks every interval until ctx is done.
func (m *Market) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Tick()
		}
	}
}

// Quote returns the latest price of a pair at a source.
func (m *Market) Quote(src Source, pair Pair) (Quote, error) {
	if src != Coinbase && src != OKX {
		return Quote{}, ErrUnknownSource
	}
	s := series{src, pair}
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.prices[s]
	if !ok {
		return Quote{}, ErrUnknownPair
	}
	if m.disabled[src] {
		return Quote{}, ErrDisabled
	}
	return Quote{Source: src, Pair: pair, Price: p, QuotedAt: m.quotedAt[s]}, nil
}

// SetEnabled enables or disables a source.
func (m *Market) SetEnabled(src Source, enabled bool) error {
	if src != Coinbase && src != OKX {
		return ErrUnknownSource
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disabled[src] = !enabled
	return nil
}

// SetDivergence shifts OKX against Coinbase by bps basis points; 0 removes the shift.
func (m *Market) SetDivergence(bps int) error {
	if bps < -MaxDivergenceBps || bps > MaxDivergenceBps {
		return ErrDivergenceRange
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.divergence = bps
	return nil
}

// Reset enables both sources and removes the divergence.
func (m *Market) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disabled = map[Source]bool{}
	m.divergence = 0
}

// State returns the state of the commands.
func (m *Market) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return State{
		Enabled:       map[Source]bool{Coinbase: !m.disabled[Coinbase], OKX: !m.disabled[OKX]},
		DivergenceBps: m.divergence,
	}
}

// FormatPrice renders a price as a decimal string with six digits after the point.
func FormatPrice(p int64) string {
	return fmt.Sprintf("%d.%06d", p/Scale, p%Scale)
}

// ParsePrice parses a positive decimal string with at most six digits after the point.
func ParsePrice(s string) (int64, error) {
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" || len(frac) > 6 || strings.ContainsAny(whole+frac, "+- _") {
		return 0, fmt.Errorf("market: invalid price %q", s)
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("market: invalid price %q", s)
	}
	var f int64
	if frac != "" {
		if f, err = strconv.ParseInt((frac + "000000")[:6], 10, 64); err != nil {
			return 0, fmt.Errorf("market: invalid price %q", s)
		}
	}
	p := w*Scale + f
	if p <= 0 || w > 1_000_000_000 {
		return 0, fmt.Errorf("market: price %q out of range", s)
	}
	return p, nil
}
