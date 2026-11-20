package rate

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"pgregory.net/rapid"
)

var t0 = time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)

func TestParsePrice(t *testing.T) {
	for in, want := range map[string]string{
		"3694.250000": "3694.25",
		"1.000000":    "1",
		"0.000001":    "0.000001",
		"0.5":         "0.5",
		"42":          "42",
		"10":          "10",
	} {
		p, err := ParsePrice(in)
		if err != nil || p.String() != want {
			t.Errorf("ParsePrice(%q) = %q, %v; want %q", in, p.String(), err, want)
		}
	}
	for _, bad := range []string{"", "0", "0.000000", "-1", "1e3", "01.5", "1.", ".5", "1.2.3", " 1", "1,5", "NaN"} {
		if _, err := ParsePrice(bad); !errors.Is(err, ErrInvalidPrice) {
			t.Errorf("ParsePrice(%q) error = %v", bad, err)
		}
	}
	if !(Price{}).IsZero() || (Price{}).String() != "" {
		t.Error("zero price")
	}
}

func TestMustPricePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("no panic")
		}
	}()
	MustPrice("x")
}

func TestArithmetic(t *testing.T) {
	if got := MustPrice("2500.800000").Mul(MustPrice("0.999800")).String(); got != "2500.29984" {
		t.Errorf("mul = %s", got)
	}
	if got := Mean(MustPrice("2501.35"), MustPrice("2500.8")).String(); got != "2501.075" {
		t.Errorf("mean = %s", got)
	}
	if got := Mean(MustPrice("0.000001"), MustPrice("0.000002")).String(); got != "0.0000015" {
		t.Errorf("mean of small = %s", got)
	}
	if MustPrice("1.5").Cmp(MustPrice("1.50")) != 0 || MustPrice("1.5").Cmp(MustPrice("1.6")) != -1 || MustPrice("2").Cmp(MustPrice("1.99")) != 1 {
		t.Error("cmp")
	}
}

func TestDivergence(t *testing.T) {
	cases := []struct {
		a, b   string
		bps    int64
		within bool
	}{
		{"100", "101", 100, true},         // exactly 1 % of the lower one
		{"101", "100", 100, true},         // symmetric
		{"100", "101.000001", 100, false}, // just above 1 %
		{"3700", "3700", 0, true},
		{"3700", "3755.5", 150, false},
		{"3700", "3736.99", 99, true},
	}
	for _, c := range cases {
		a, b := MustPrice(c.a), MustPrice(c.b)
		if got := DivergenceBps(a, b); got != c.bps {
			t.Errorf("DivergenceBps(%s, %s) = %d, want %d", c.a, c.b, got, c.bps)
		}
		if got := WithinBps(a, b, 100); got != c.within {
			t.Errorf("WithinBps(%s, %s, 100) = %v", c.a, c.b, got)
		}
	}
	huge := Price{unscaled: new(big.Int).Lsh(big.NewInt(1), 80)}
	if DivergenceBps(huge, MustPrice("0.000001")) != 1<<63-1 {
		t.Error("overflow is not capped")
	}
}

func genPrice(t *rapid.T, label string) Price {
	n := rapid.Int64Range(1, 1_000_000_000_000).Draw(t, label)
	scale := rapid.IntRange(0, 12).Draw(t, label+"-scale")
	return Price{unscaled: big.NewInt(n), scale: scale}.normalize()
}

// TestPriceProperties: the mean lies between the prices, the divergence is symmetric and consistent with
// WithinBps, and a rendered price parses back to the same value.
func TestPriceProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a, b := genPrice(t, "a"), genPrice(t, "b")
		m := Mean(a, b)
		lo, hi := a, b
		if lo.Cmp(hi) > 0 {
			lo, hi = hi, lo
		}
		if m.Cmp(lo) < 0 || m.Cmp(hi) > 0 {
			t.Fatalf("mean %s outside [%s, %s]", m, lo, hi)
		}
		if DivergenceBps(a, b) != DivergenceBps(b, a) || WithinBps(a, b, 100) != WithinBps(b, a, 100) {
			t.Fatal("divergence is not symmetric")
		}
		bps := rapid.Int64Range(0, 10000).Draw(t, "bps")
		if WithinBps(a, b, bps) && DivergenceBps(a, b) > bps {
			t.Fatalf("within %d but divergence %d", bps, DivergenceBps(a, b))
		}
		if !WithinBps(a, b, bps) && DivergenceBps(a, b) < bps {
			t.Fatalf("not within %d but divergence %d", bps, DivergenceBps(a, b))
		}
		back, err := ParsePrice(a.String())
		if err != nil || back.Cmp(a) != 0 || back.String() != a.String() {
			t.Fatalf("round trip %s -> %s, %v", a, back, err)
		}
	})
}

func q(src Source, pair Pair, price string, at time.Time) Quote {
	return Quote{Source: src, Pair: pair, Price: MustPrice(price), QuotedAt: at}
}

func TestAggregate(t *testing.T) {
	cb := q(Coinbase, ETHUSD, "3700", t0)
	okx := q(OKX, ETHUSD, "3710", t0.Add(-2*time.Second))
	far := q(OKX, ETHUSD, "3760", t0)

	if _, out, _ := Aggregate(ETHUSD, nil, 100); out != NoQuote {
		t.Errorf("no quotes: %v", out)
	}

	r, out, d := Aggregate(ETHUSD, []Quote{cb}, 100)
	if out != Aggregated || r.Price.String() != "3700" || len(r.Sources) != 1 || r.Sources[0] != Coinbase || !r.QuotedAt.Equal(t0) || d != 0 {
		t.Errorf("one source: %+v %v %d", r, out, d)
	}

	r, out, d = Aggregate(ETHUSD, []Quote{cb, okx}, 100)
	if out != Aggregated || r.Price.String() != "3705" || len(r.Sources) != 2 || d != 27 {
		t.Errorf("mean: %+v %v %d", r, out, d)
	}
	if !r.QuotedAt.Equal(okx.QuotedAt) {
		t.Errorf("mean takes the older quote time: %v", r.QuotedAt)
	}

	if _, out, d = Aggregate(ETHUSD, []Quote{cb, far}, 100); out != Diverged || d != 162 {
		t.Errorf("divergence: %v %d", out, d)
	}
	// The limit is configuration.
	if _, out, _ = Aggregate(ETHUSD, []Quote{cb, far}, 200); out != Aggregated {
		t.Errorf("divergence under a wider limit: %v", out)
	}
}

func TestConvert(t *testing.T) {
	eth := q(OKX, ETHUSDT, "3700.000000", t0)
	usdt := q(Coinbase, USDTUSD, "0.999000", t0.Add(-time.Hour))
	got, ok := Convert(eth, usdt, t0, time.Hour)
	if !ok || got.Price.String() != "3696.3" || got.Pair != ETHUSD || got.Source != OKX || !got.QuotedAt.Equal(t0) {
		t.Errorf("convert: %+v %v", got, ok)
	}
	if _, ok := Convert(eth, usdt, t0.Add(time.Nanosecond), time.Hour); ok {
		t.Error("USDT/USD older than 1 h is used")
	}
	if _, ok := Convert(eth, Quote{}, t0, time.Hour); ok {
		t.Error("missing USDT/USD is used")
	}
}

func TestEvaluate(t *testing.T) {
	r := Rate{Pair: ETHUSD, Price: MustPrice("3700"), QuotedAt: t0, Sources: []Source{Coinbase}}
	s := Snapshot{Rate: r, UpdatedAt: t0}
	const maxAge = 60 * time.Second

	if _, _, err := Evaluate(Snapshot{}, false, t0, maxAge); !errors.Is(err, ErrUnavailable) {
		t.Errorf("missing: %v", err)
	}
	if _, _, err := Evaluate(Snapshot{UpdatedAt: t0}, true, t0, maxAge); !errors.Is(err, ErrUnavailable) {
		t.Errorf("empty: %v", err)
	}
	if _, _, err := Evaluate(Snapshot{Diverged: true, UpdatedAt: t0}, true, t0, maxAge); !errors.Is(err, ErrDiverged) {
		t.Errorf("diverged: %v", err)
	}
	if got, age, err := Evaluate(s, true, t0.Add(3500*time.Millisecond), maxAge); err != nil || age != 3 || got.Price.String() != "3700" {
		t.Errorf("fresh: %d %v", age, err)
	}
	if _, age, err := Evaluate(s, true, t0.Add(maxAge), maxAge); err != nil || age != 60 {
		t.Errorf("exactly 60 s is still served: %d %v", age, err)
	}
	if _, _, err := Evaluate(s, true, t0.Add(maxAge+time.Millisecond), maxAge); !errors.Is(err, ErrStale) {
		t.Errorf("older than 60 s: %v", err)
	}
	if _, age, err := Evaluate(s, true, t0.Add(-5*time.Second), maxAge); err != nil || age != 0 {
		t.Errorf("quote from the future: %d %v", age, err)
	}
}

func TestCacheAndNewer(t *testing.T) {
	c := NewCache()
	if _, ok := c.Get(ETHUSD); ok {
		t.Fatal("empty cache")
	}
	old := Snapshot{Rate: Rate{Price: MustPrice("1")}, UpdatedAt: t0}
	newer := Snapshot{Diverged: true, UpdatedAt: t0.Add(time.Second)}
	if !c.Put(ETHUSD, old) || !c.Put(ETHUSD, newer) {
		t.Fatal("put")
	}
	if c.Put(ETHUSD, old) || c.Put(ETHUSD, newer) {
		t.Error("an older or equal snapshot replaced the newer one")
	}
	if s, ok := c.Get(ETHUSD); !ok || !s.Diverged {
		t.Errorf("get: %+v", s)
	}

	if s, ok := Newer(old, false, newer, false); ok || s.Diverged {
		t.Error("neither present")
	}
	if s, ok := Newer(old, true, newer, false); !ok || s.Diverged {
		t.Error("only a")
	}
	if s, ok := Newer(old, false, newer, true); !ok || !s.Diverged {
		t.Error("only b")
	}
	if s, _ := Newer(old, true, newer, true); !s.Diverged {
		t.Error("b is newer")
	}
	if s, _ := Newer(newer, true, old, true); !s.Diverged {
		t.Error("a is newer")
	}
	if !IsServed(ETHUSD) || !IsServed(USDTUSD) || IsServed(ETHUSDT) || len(Served) != 2 {
		t.Error("served pairs")
	}
}
