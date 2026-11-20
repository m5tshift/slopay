package market

import (
	"context"
	"errors"
	"testing"
	"time"

	"pgregory.net/rapid"
)

var t0 = time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)

func newMarket(t *testing.T, intn func(int) int) (*Market, *time.Time) {
	t.Helper()
	now := t0
	m, err := New(Config{ETHUSD: 3700 * Scale, USDTUSD: Scale, StepBps: 5, BandBps: 10,
		Now: func() time.Time { return now }, Intn: intn})
	if err != nil {
		t.Fatal(err)
	}
	return m, &now
}

func TestNewValidates(t *testing.T) {
	for name, cfg := range map[string]Config{
		"eth":      {ETHUSD: 0, USDTUSD: Scale},
		"usdt":     {ETHUSD: Scale, USDTUSD: -1},
		"negative": {ETHUSD: Scale, USDTUSD: Scale, StepBps: -1},
		"band":     {ETHUSD: Scale, USDTUSD: Scale, BandBps: -1},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := New(Config{ETHUSD: Scale, USDTUSD: Scale}); err != nil {
		t.Fatalf("defaults: %v", err)
	}
}

func TestInitialQuotes(t *testing.T) {
	m, _ := newMarket(t, func(int) int { return 0 })
	for _, c := range []struct {
		src  Source
		pair Pair
		want string
	}{
		{Coinbase, ETHUSD, "3700.000000"}, {Coinbase, USDTUSD, "1.000000"}, {OKX, ETHUSDT, "3700.000000"},
	} {
		q, err := m.Quote(c.src, c.pair)
		if err != nil {
			t.Fatal(err)
		}
		if got := FormatPrice(q.Price); got != c.want || !q.QuotedAt.Equal(t0) {
			t.Errorf("%s %s = %s at %v, want %s", c.src, c.pair, got, q.QuotedAt, c.want)
		}
	}
}

func TestQuoteErrors(t *testing.T) {
	m, _ := newMarket(t, func(int) int { return 0 })
	if _, err := m.Quote("kraken", ETHUSD); !errors.Is(err, ErrUnknownSource) {
		t.Errorf("source: %v", err)
	}
	if _, err := m.Quote(OKX, ETHUSD); !errors.Is(err, ErrUnknownPair) {
		t.Errorf("pair: %v", err)
	}
	if _, err := m.Quote(Coinbase, ETHUSDT); !errors.Is(err, ErrUnknownPair) {
		t.Errorf("pair: %v", err)
	}
	if err := m.SetEnabled("kraken", false); !errors.Is(err, ErrUnknownSource) {
		t.Errorf("set: %v", err)
	}
}

func TestDisableAndEnable(t *testing.T) {
	m, _ := newMarket(t, func(int) int { return 0 })
	if err := m.SetEnabled(OKX, false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Quote(OKX, ETHUSDT); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled okx: %v", err)
	}
	if _, err := m.Quote(Coinbase, ETHUSD); err != nil {
		t.Fatalf("coinbase must stay up: %v", err)
	}
	if m.State().Enabled[OKX] || !m.State().Enabled[Coinbase] {
		t.Fatalf("state %+v", m.State())
	}
	// A disabled source with an unknown pair is still an unknown pair.
	if _, err := m.Quote(OKX, ETHUSD); !errors.Is(err, ErrUnknownPair) {
		t.Fatalf("pair of disabled source: %v", err)
	}
	if err := m.SetEnabled(OKX, true); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Quote(OKX, ETHUSDT); err != nil {
		t.Fatal(err)
	}
}

func TestDivergence(t *testing.T) {
	m, now := newMarket(t, func(int) int { return 0 })
	if err := m.SetDivergence(MaxDivergenceBps + 1); !errors.Is(err, ErrDivergenceRange) {
		t.Fatalf("high: %v", err)
	}
	if err := m.SetDivergence(-MaxDivergenceBps - 1); !errors.Is(err, ErrDivergenceRange) {
		t.Fatalf("low: %v", err)
	}
	if err := m.SetDivergence(150); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	m.Tick()
	cb, _ := m.Quote(Coinbase, ETHUSD)
	okx, _ := m.Quote(OKX, ETHUSDT)
	// Intn=0 is the largest step down, the clamp keeps okx at the lower bound of its band: +1.5 % - 0.1 %.
	if got := (okx.Price - cb.Price) * 10000 / cb.Price; got < 130 {
		t.Fatalf("divergence %d bps, want at least 130", got)
	}
	if !okx.QuotedAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("quotedAt %v", okx.QuotedAt)
	}
	if m.State().DivergenceBps != 150 {
		t.Fatalf("state %+v", m.State())
	}
	if err := m.SetDivergence(-150); err != nil {
		t.Fatal(err)
	}
	m.Tick()
	okx, _ = m.Quote(OKX, ETHUSDT)
	if okx.Price >= cb.Price {
		t.Fatalf("negative divergence: %d vs %d", okx.Price, cb.Price)
	}
}

func TestReset(t *testing.T) {
	m, _ := newMarket(t, func(n int) int { return n / 2 })
	_ = m.SetEnabled(Coinbase, false)
	_ = m.SetEnabled(OKX, false)
	_ = m.SetDivergence(300)
	m.Reset()
	st := m.State()
	if !st.Enabled[Coinbase] || !st.Enabled[OKX] || st.DivergenceBps != 0 {
		t.Fatalf("state after reset %+v", st)
	}
	m.Tick()
	cb, _ := m.Quote(Coinbase, ETHUSD)
	okx, _ := m.Quote(OKX, ETHUSDT)
	if cb.Price != okx.Price {
		t.Fatalf("prices after reset: %d vs %d", cb.Price, okx.Price)
	}
}

func TestWalkMovesAndStaysInBand(t *testing.T) {
	up := true
	m, _ := newMarket(t, func(n int) int {
		up = !up
		if up {
			return n - 1
		}
		return 0
	})
	moved := false
	for range 200 {
		m.Tick()
		q, _ := m.Quote(Coinbase, ETHUSD)
		if q.Price != 3700*Scale {
			moved = true
		}
		if q.Price < 3700*Scale*9990/10000 || q.Price > 3700*Scale*10010/10000 {
			t.Fatalf("price %d left the band", q.Price)
		}
	}
	if !moved {
		t.Fatal("price never moved")
	}
}

func TestZeroStepKeepsPrice(t *testing.T) {
	m, err := New(Config{ETHUSD: 3700 * Scale, USDTUSD: Scale, BandBps: 10})
	if err != nil {
		t.Fatal(err)
	}
	m.Tick()
	if q, _ := m.Quote(Coinbase, ETHUSD); q.Price != 3700*Scale {
		t.Fatalf("price %d", q.Price)
	}
}

// Without commands the two sources never diverge by more than 1 %, whatever the walk does.
func TestNormalModeNeverDiverges(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m, err := New(Config{ETHUSD: 3700 * Scale, USDTUSD: Scale, StepBps: 5, BandBps: 10,
			Intn: func(n int) int { return rapid.IntRange(0, n-1).Draw(t, "r") }})
		if err != nil {
			t.Fatal(err)
		}
		for range 50 {
			m.Tick()
			cb, _ := m.Quote(Coinbase, ETHUSD)
			usdt, _ := m.Quote(Coinbase, USDTUSD)
			okx, _ := m.Quote(OKX, ETHUSDT)
			okxUSD := okx.Price * usdt.Price / Scale
			diff := cb.Price - okxUSD
			if diff < 0 {
				diff = -diff
			}
			if diff*100 > cb.Price {
				t.Fatalf("diverged: coinbase %d okx %d", cb.Price, okxUSD)
			}
		}
	})
}

func TestRunTicksUntilCancelled(t *testing.T) {
	calls := make(chan struct{}, 100)
	m, _ := newMarket(t, func(int) int { calls <- struct{}{}; return 0 })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx, time.Millisecond); close(done) }()
	select {
	case <-calls:
	case <-time.After(5 * time.Second):
		t.Fatal("no tick")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestPriceFormatting(t *testing.T) {
	if got := FormatPrice(3694250000); got != "3694.250000" {
		t.Fatal(got)
	}
	if got := FormatPrice(1000001); got != "1.000001" {
		t.Fatal(got)
	}
	for in, want := range map[string]int64{"3700": 3700 * Scale, "3694.25": 3694250000, "0.999999": 999999, "1.": Scale} {
		got, err := ParsePrice(in)
		if err != nil || got != want {
			t.Errorf("ParsePrice(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", ".5", "-1", "+1", "1.1234567", "abc", "1.x", "0", "0.0", "1 0", "9999999999"} {
		if _, err := ParsePrice(in); err == nil {
			t.Errorf("ParsePrice(%q): no error", in)
		}
	}
}
