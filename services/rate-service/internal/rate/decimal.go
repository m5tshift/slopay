package rate

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// ErrInvalidPrice is returned for a malformed or non-positive price.
var ErrInvalidPrice = errors.New("rate: invalid price")

var big10 = big.NewInt(10)

// Price is an exact positive decimal: unscaled * 10^-scale. Prices are never floats (AGENTS.md).
type Price struct {
	unscaled *big.Int
	scale    int
}

// ParsePrice parses a decimal string of the contracts, such as "3694.250000": digits, an optional point
// and fraction, no sign, no exponent, no leading zeros. The price must be positive.
func ParsePrice(s string) (Price, error) {
	intPart, frac, hasDot := strings.Cut(s, ".")
	if !isDigits(intPart) || (hasDot && !isDigits(frac)) || (len(intPart) > 1 && intPart[0] == '0') {
		return Price{}, fmt.Errorf("%w: %q", ErrInvalidPrice, s)
	}
	n, ok := new(big.Int).SetString(intPart+frac, 10)
	if !ok || n.Sign() <= 0 {
		return Price{}, fmt.Errorf("%w: %q", ErrInvalidPrice, s)
	}
	return Price{unscaled: n, scale: len(frac)}.normalize(), nil
}

// MustPrice is ParsePrice for constants in tests; it panics on a malformed price.
func MustPrice(s string) Price {
	p, err := ParsePrice(s)
	if err != nil {
		panic(err)
	}
	return p
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// IsZero reports whether p is the zero value, which is not a price.
func (p Price) IsZero() bool { return p.unscaled == nil }

// normalize strips trailing zeros of the fraction, so equal prices have equal representations.
func (p Price) normalize() Price {
	u := new(big.Int).Set(p.unscaled)
	s := p.scale
	q, m := new(big.Int), new(big.Int)
	for s > 0 {
		q.QuoRem(u, big10, m)
		if m.Sign() != 0 {
			break
		}
		u.Set(q)
		s--
	}
	return Price{unscaled: u, scale: s}
}

// rescale returns the unscaled value of p at a scale not below its own.
func (p Price) rescale(scale int) *big.Int {
	return new(big.Int).Mul(p.unscaled, new(big.Int).Exp(big10, big.NewInt(int64(scale-p.scale)), nil))
}

// align returns the unscaled values of a and b at their common scale.
func align(a, b Price) (x, y *big.Int, scale int) {
	scale = max(a.scale, b.scale)
	return a.rescale(scale), b.rescale(scale), scale
}

// Mul returns the exact product p * q.
func (p Price) Mul(q Price) Price {
	return Price{unscaled: new(big.Int).Mul(p.unscaled, q.unscaled), scale: p.scale + q.scale}.normalize()
}

// Mean returns the exact arithmetic mean of p and q. (x+y)/2 of decimals is a decimal with one more digit:
// (x+y)*5 at scale+1.
func Mean(p, q Price) Price {
	x, y, scale := align(p, q)
	sum := new(big.Int).Add(x, y)
	return Price{unscaled: sum.Mul(sum, big.NewInt(5)), scale: scale + 1}.normalize()
}

// Cmp compares p and q: -1, 0 or +1.
func (p Price) Cmp(q Price) int {
	x, y, _ := align(p, q)
	return x.Cmp(y)
}

// DivergenceBps returns the divergence of p and q in basis points of the lower one, rounded down:
// floor(|p - q| * 10000 / min(p, q)).
func DivergenceBps(p, q Price) int64 {
	x, y, _ := align(p, q)
	low := x
	if y.Cmp(x) < 0 {
		low = y
	}
	d := new(big.Int).Sub(x, y)
	d.Abs(d).Mul(d, big.NewInt(10000)).Quo(d, low)
	if !d.IsInt64() {
		return 1<<63 - 1
	}
	return d.Int64()
}

// WithinBps reports whether p and q diverge by no more than bps basis points of the lower one, exactly:
// |p - q| * 10000 <= bps * min(p, q).
func WithinBps(p, q Price, bps int64) bool {
	x, y, _ := align(p, q)
	low := x
	if y.Cmp(x) < 0 {
		low = y
	}
	d := new(big.Int).Sub(x, y)
	d.Abs(d).Mul(d, big.NewInt(10000))
	return d.Cmp(new(big.Int).Mul(low, big.NewInt(bps))) <= 0
}

// String renders p as a decimal string of the contracts without trailing zeros: "3694.25", "1".
func (p Price) String() string {
	if p.IsZero() {
		return ""
	}
	digits := p.unscaled.String()
	if p.scale == 0 {
		return digits
	}
	if len(digits) <= p.scale {
		digits = strings.Repeat("0", p.scale-len(digits)+1) + digits
	}
	cut := len(digits) - p.scale
	return digits[:cut] + "." + digits[cut:]
}
