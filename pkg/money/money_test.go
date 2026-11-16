package money

import (
	"errors"
	"math/big"
	"testing"

	"pgregory.net/rapid"
)

type fataler interface {
	Helper()
	Fatal(args ...any)
}

func mustRate(t fataler, s string) Rate {
	t.Helper()
	r, err := ParseRate(s)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func bi(n int64) *big.Int { return big.NewInt(n) }

func TestParseRate(t *testing.T) {
	for _, s := range []string{"1", "3694.25", "0.5", "0.000001", "10"} {
		if _, err := ParseRate(s); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
	for _, s := range []string{"", "0", "0.0", "-1", "+1", "1e3", "01", "1.", ".5", "1,5", "NaN", " 1", "1.2.3"} {
		if _, err := ParseRate(s); !errors.Is(err, ErrInvalidRate) {
			t.Errorf("%q: want ErrInvalidRate, got %v", s, err)
		}
	}
}

func TestNewRate(t *testing.T) {
	if _, err := NewRate(bi(1), bi(2)); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]*big.Int{{nil, bi(1)}, {bi(1), nil}, {bi(0), bi(1)}, {bi(1), bi(0)}, {bi(-1), bi(1)}} {
		if _, err := NewRate(c[0], c[1]); !errors.Is(err, ErrInvalidRate) {
			t.Errorf("%v: got %v", c, err)
		}
	}
}

func TestUSDToAsset(t *testing.T) {
	tests := []struct {
		name     string
		cents    int64
		rate     string
		decimals int
		want     string
	}{
		{"zero", 0, "2500", 18, "0"},
		{"one cent eth", 1, "2500", 18, "4000000000000"},
		{"exact", 250000, "2500", 18, "1000000000000000000"},
		{"rounds up", 1, "3", 0, "1"},
		{"rounds up eth", 100, "3", 18, "333333333333333334"},
		{"usdt 6 decimals", 12345, "1", 6, "123450000"},
		{"fractional rate", 100, "3694.25", 18, "270690938620830"},
		{"max int64 cents", 9223372036854775807, "1", 18, "92233720368547758070000000000000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := USDToAsset(tc.cents, mustRate(t, tc.rate), tc.decimals)
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestUSDToAssetErrors(t *testing.T) {
	r := mustRate(t, "1")
	if _, err := USDToAsset(-1, r, 18); !errors.Is(err, ErrInvalidAmount) {
		t.Error(err)
	}
	if _, err := USDToAsset(1, Rate{}, 18); !errors.Is(err, ErrInvalidRate) {
		t.Error(err)
	}
	if _, err := USDToAsset(1, r, -1); !errors.Is(err, ErrInvalidDecimals) {
		t.Error(err)
	}
	if _, err := USDToAsset(1, r, MaxAssetDecimals+1); !errors.Is(err, ErrInvalidDecimals) {
		t.Error(err)
	}
}

func TestFee(t *testing.T) {
	tests := []struct {
		amount int64
		bps    int
		want   int64
	}{
		{0, 50, 0},
		{1, 50, 0},
		{199, 50, 0},
		{200, 50, 1},
		{399, 50, 1},
		{1000000, 50, 5000},
		{1000, 0, 0},
		{1000, 10000, 1000},
	}
	for _, tc := range tests {
		got, err := Fee(bi(tc.amount), tc.bps)
		if err != nil {
			t.Fatal(err)
		}
		if got.Int64() != tc.want {
			t.Errorf("Fee(%d,%d)=%s want %d", tc.amount, tc.bps, got, tc.want)
		}
	}
	if _, err := Fee(nil, 50); !errors.Is(err, ErrInvalidAmount) {
		t.Error(err)
	}
	if _, err := Fee(bi(-1), 50); !errors.Is(err, ErrInvalidAmount) {
		t.Error(err)
	}
	if _, err := Fee(bi(1), -1); !errors.Is(err, ErrInvalidBps) {
		t.Error(err)
	}
	if _, err := Fee(bi(1), BpsDenominator+1); !errors.Is(err, ErrInvalidBps) {
		t.Error(err)
	}
}

func TestMinAcceptedAndCheck(t *testing.T) {
	tests := []struct {
		name     string
		amount   int64
		bps      int
		received int64
		min      int64
		paid     bool
		over     bool
	}{
		{"zero amount zero received", 0, 100, 0, 0, true, false},
		{"one cent exact", 1, 0, 1, 1, true, false},
		{"one cent tol 5%", 1, 500, 1, 1, true, false},
		{"one cent tol 5% zero received", 1, 500, 0, 1, false, false},
		{"tol 0 exact", 10000, 0, 10000, 10000, true, false},
		{"tol 0 one short", 10000, 0, 9999, 10000, false, false},
		{"tol 5% edge", 10000, 500, 9500, 9500, true, false},
		{"tol 5% below edge", 10000, 500, 9499, 9500, false, false},
		{"tol 1% fractional bound", 150, 100, 149, 149, true, false},
		{"tol 1% fractional bound short", 150, 100, 148, 149, false, false},
		{"overpaid", 10000, 100, 10001, 9900, true, true},
		{"exact not overpaid", 10000, 100, 10000, 9900, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			min, err := MinAccepted(bi(tc.amount), tc.bps)
			if err != nil {
				t.Fatal(err)
			}
			if min.Int64() != tc.min {
				t.Errorf("min=%s want %d", min, tc.min)
			}
			res, err := Check(bi(tc.amount), bi(tc.received), tc.bps)
			if err != nil {
				t.Fatal(err)
			}
			if res.Paid != tc.paid || res.Overpaid != tc.over {
				t.Errorf("got %+v", res)
			}
		})
	}
}

func TestCheckErrors(t *testing.T) {
	if _, err := Check(bi(1), nil, 0); !errors.Is(err, ErrInvalidAmount) {
		t.Error(err)
	}
	if _, err := Check(bi(1), bi(-1), 0); !errors.Is(err, ErrInvalidAmount) {
		t.Error(err)
	}
	if _, err := Check(nil, bi(1), 0); !errors.Is(err, ErrInvalidAmount) {
		t.Error(err)
	}
	if _, err := Check(bi(1), bi(1), MaxBps+1); !errors.Is(err, ErrInvalidBps) {
		t.Error(err)
	}
	if _, err := MinAccepted(bi(-1), 0); !errors.Is(err, ErrInvalidAmount) {
		t.Error(err)
	}
	if _, err := MinAccepted(bi(1), -1); !errors.Is(err, ErrInvalidBps) {
		t.Error(err)
	}
}

func rateGen() *rapid.Generator[Rate] {
	return rapid.Custom(func(t *rapid.T) Rate {
		n := rapid.Int64Range(1, 1_000_000_000_000).Draw(t, "num")
		d := rapid.SampledFrom([]int64{1, 10, 100, 10000, 1000000}).Draw(t, "den")
		r, err := NewRate(bi(n), bi(d))
		if err != nil {
			t.Fatal(err)
		}
		return r
	})
}

// Rounding is upward: units*rate >= cents, and one unit less is not enough.
func TestPropUSDToAssetRoundsUp(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cents := rapid.Int64Range(0, 1<<62).Draw(t, "cents")
		dec := rapid.IntRange(0, 24).Draw(t, "dec")
		r := rateGen().Draw(t, "rate")
		u, err := USDToAsset(cents, r, dec)
		if err != nil {
			t.Fatal(err)
		}
		// value(units) in cents*10^dec*den scale: units*100*num vs cents*10^dec*den
		scale := new(big.Int).Exp(big10, big.NewInt(int64(dec)), nil)
		target := new(big.Int).Mul(bi(cents), scale)
		target.Mul(target, r.den)
		have := new(big.Int).Mul(u, big100)
		have.Mul(have, r.num)
		if have.Cmp(target) < 0 {
			t.Fatalf("rounded down: %s", u)
		}
		if u.Sign() > 0 {
			less := new(big.Int).Sub(u, bi(1))
			less.Mul(less, big100)
			less.Mul(less, r.num)
			if less.Cmp(target) >= 0 {
				t.Fatalf("not minimal: %s", u)
			}
		}
	})
}

// A larger USD amount never gives fewer units; a higher rate never gives more.
func TestPropUSDToAssetMonotonic(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := rapid.Int64Range(0, 1<<62).Draw(t, "a")
		b := rapid.Int64Range(a, 1<<62).Draw(t, "b")
		dec := rapid.IntRange(0, 24).Draw(t, "dec")
		r := rateGen().Draw(t, "rate")
		ua, _ := USDToAsset(a, r, dec)
		ub, _ := USDToAsset(b, r, dec)
		if ua.Cmp(ub) > 0 {
			t.Fatalf("not monotonic in amount: %s > %s", ua, ub)
		}
		r2, _ := NewRate(new(big.Int).Add(r.num, bi(1)), r.den)
		u2, _ := USDToAsset(b, r2, dec)
		if u2.Cmp(ub) > 0 {
			t.Fatalf("not monotonic in rate: %s > %s", u2, ub)
		}
	})
}

// No overflow: the result is exact for any int64 cents and up to 36 decimals,
// and fits the NUMERIC(78,0) of the storage for realistic rates.
func TestPropNoOverflow(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cents := rapid.Int64Range(0, 1<<63-1).Draw(t, "cents")
		dec := rapid.IntRange(0, 18).Draw(t, "dec")
		r := mustRate(t, "0.01")
		u, err := USDToAsset(cents, r, dec)
		if err != nil {
			t.Fatal(err)
		}
		// at rate 0.01 USD: units = cents * 10^dec exactly
		want := new(big.Int).Mul(bi(cents), new(big.Int).Exp(big10, big.NewInt(int64(dec)), nil))
		if u.Cmp(want) != 0 {
			t.Fatalf("got %s want %s", u, want)
		}
		if len(u.String()) > 78 {
			t.Fatalf("exceeds NUMERIC(78,0): %s", u)
		}
	})
}

func amountGen() *rapid.Generator[*big.Int] {
	return rapid.Custom(func(t *rapid.T) *big.Int {
		hi := rapid.Uint64().Draw(t, "hi")
		lo := rapid.Uint64().Draw(t, "lo")
		v := new(big.Int).SetUint64(hi)
		v.Lsh(v, 64).Or(v, new(big.Int).SetUint64(lo))
		return v
	})
}

// Fee rounds down, never exceeds the exact value, is monotonic and <= amount.
func TestPropFeeRoundsDown(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := amountGen().Draw(t, "amount")
		bps := rapid.IntRange(0, BpsDenominator).Draw(t, "bps")
		f, err := Fee(a, bps)
		if err != nil {
			t.Fatal(err)
		}
		exact := new(big.Int).Mul(a, bi(int64(bps)))
		have := new(big.Int).Mul(f, bigBps)
		if have.Cmp(exact) > 0 {
			t.Fatalf("rounded up: %s", f)
		}
		next := new(big.Int).Mul(new(big.Int).Add(f, bi(1)), bigBps)
		if next.Cmp(exact) <= 0 {
			t.Fatalf("not maximal: %s", f)
		}
		if f.Cmp(a) > 0 {
			t.Fatalf("fee above amount")
		}
		more := new(big.Int).Add(a, bi(1))
		f2, _ := Fee(more, bps)
		if f2.Cmp(f) < 0 {
			t.Fatalf("not monotonic")
		}
	})
}

// Check: received >= amount - tol exactly, monotonic in received and tolerance.
func TestPropCheck(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := amountGen().Draw(t, "amount")
		recv := amountGen().Draw(t, "received")
		bps := rapid.IntRange(0, MaxBps).Draw(t, "bps")
		res, err := Check(a, recv, bps)
		if err != nil {
			t.Fatal(err)
		}
		// exact: recv*10000 >= amount*(10000-bps)
		l := new(big.Int).Mul(recv, bigBps)
		r := new(big.Int).Mul(a, bi(int64(BpsDenominator-bps)))
		if res.Paid != (l.Cmp(r) >= 0) {
			t.Fatalf("paid=%v for amount=%s recv=%s bps=%d", res.Paid, a, recv, bps)
		}
		if res.Overpaid != (recv.Cmp(a) > 0) {
			t.Fatal("overpaid mismatch")
		}
		if res.Overpaid && !res.Paid {
			t.Fatal("overpaid but not paid")
		}
		more, _ := Check(a, new(big.Int).Add(recv, bi(1)), bps)
		if res.Paid && !more.Paid {
			t.Fatal("not monotonic in received")
		}
		if bps < MaxBps {
			wider, _ := Check(a, recv, bps+1)
			if res.Paid && !wider.Paid {
				t.Fatal("not monotonic in tolerance")
			}
		}
	})
}
