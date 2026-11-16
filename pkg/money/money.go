// Package money implements SLOPay amount arithmetic (SRS 4.1.3, 4.1.4).
//
// USD is int64 cents, asset amounts are *big.Int in minimal units, rates are
// exact rationals. There is no floating point anywhere.
package money

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// BpsDenominator is the number of basis points in one whole.
const BpsDenominator = 10000

// MaxBps is the largest merchant tolerance, 5 % (SRS 4.1.4).
const MaxBps = 500

// MaxAssetDecimals bounds decimals so that 10^decimals stays small.
const MaxAssetDecimals = 36

var (
	// ErrInvalidRate is returned for a malformed or non-positive rate.
	ErrInvalidRate = errors.New("money: invalid rate")
	// ErrInvalidAmount is returned for a negative amount.
	ErrInvalidAmount = errors.New("money: invalid amount")
	// ErrInvalidDecimals is returned for decimals outside 0..MaxAssetDecimals.
	ErrInvalidDecimals = errors.New("money: invalid decimals")
	// ErrInvalidBps is returned for basis points outside the allowed range.
	ErrInvalidBps = errors.New("money: invalid basis points")
)

var (
	big10  = big.NewInt(10)
	big100 = big.NewInt(100)
	bigBps = big.NewInt(BpsDenominator)
)

// Rate is the USD price of one whole unit of an asset, an exact rational
// number num/den with num > 0 and den > 0.
type Rate struct {
	num, den *big.Int
}

// ParseRate parses a decimal string such as "3694.25" (the format of the rate
// contract: no sign, no exponent, no leading zeros). The rate must be positive.
func ParseRate(s string) (Rate, error) {
	intPart, frac, hasDot := strings.Cut(s, ".")
	if !isDigits(intPart) || (hasDot && !isDigits(frac)) {
		return Rate{}, fmt.Errorf("%w: %q", ErrInvalidRate, s)
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return Rate{}, fmt.Errorf("%w: %q", ErrInvalidRate, s)
	}
	n, ok := new(big.Int).SetString(intPart+frac, 10)
	if !ok || n.Sign() == 0 {
		return Rate{}, fmt.Errorf("%w: %q", ErrInvalidRate, s)
	}
	den := new(big.Int).Exp(big10, big.NewInt(int64(len(frac))), nil)
	return Rate{num: n, den: den}, nil
}

// NewRate builds the rate num/den; both must be positive.
func NewRate(num, den *big.Int) (Rate, error) {
	if num == nil || den == nil || num.Sign() <= 0 || den.Sign() <= 0 {
		return Rate{}, ErrInvalidRate
	}
	return Rate{num: new(big.Int).Set(num), den: new(big.Int).Set(den)}, nil
}

// IsZero reports whether r is the zero value, which is not a usable rate.
func (r Rate) IsZero() bool { return r.num == nil || r.den == nil }

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

// USDToAsset converts USD cents to minimal asset units at rate with the given
// decimals, rounding up (SRS 4.1.3):
//
//	units = ceil(cents * 10^decimals * den / (100 * num))
func USDToAsset(cents int64, rate Rate, decimals int) (*big.Int, error) {
	if cents < 0 {
		return nil, ErrInvalidAmount
	}
	if rate.IsZero() || rate.num.Sign() <= 0 || rate.den.Sign() <= 0 {
		return nil, ErrInvalidRate
	}
	if decimals < 0 || decimals > MaxAssetDecimals {
		return nil, ErrInvalidDecimals
	}
	n := big.NewInt(cents)
	n.Mul(n, new(big.Int).Exp(big10, big.NewInt(int64(decimals)), nil))
	n.Mul(n, rate.den)
	d := new(big.Int).Mul(big100, rate.num)
	return ceilDiv(n, d), nil
}

// Fee returns the platform fee for amount: floor(amount * feeBps / 10000)
// (SRS 4.1.3). feeBps is configuration, 50 means 0.5 %.
func Fee(amount *big.Int, feeBps int) (*big.Int, error) {
	if amount == nil || amount.Sign() < 0 {
		return nil, ErrInvalidAmount
	}
	if feeBps < 0 || feeBps > BpsDenominator {
		return nil, ErrInvalidBps
	}
	f := new(big.Int).Mul(amount, big.NewInt(int64(feeBps)))
	return f.Quo(f, bigBps), nil
}

// MinAccepted returns the smallest received amount that still pays an invoice
// of the given amount with toleranceBps: ceil(amount * (10000 - bps) / 10000).
// Received >= amount - amount*bps/10000 holds exactly for received >= result.
func MinAccepted(amount *big.Int, toleranceBps int) (*big.Int, error) {
	if amount == nil || amount.Sign() < 0 {
		return nil, ErrInvalidAmount
	}
	if toleranceBps < 0 || toleranceBps > MaxBps {
		return nil, ErrInvalidBps
	}
	n := new(big.Int).Mul(amount, big.NewInt(int64(BpsDenominator-toleranceBps)))
	return ceilDiv(n, bigBps), nil
}

// Result is the outcome of comparing a received amount with an invoice.
type Result struct {
	// Paid: received is not less than the invoice amount minus the tolerance.
	Paid bool
	// Overpaid: received is greater than the invoice amount.
	Overpaid bool
}

// Check compares received with the invoice amount under toleranceBps
// (SRS 4.1.4).
func Check(amount, received *big.Int, toleranceBps int) (Result, error) {
	if received == nil || received.Sign() < 0 {
		return Result{}, ErrInvalidAmount
	}
	min, err := MinAccepted(amount, toleranceBps)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Paid:     received.Cmp(min) >= 0,
		Overpaid: received.Cmp(amount) > 0,
	}, nil
}

// ceilDiv returns ceil(n/d) for n >= 0, d > 0.
func ceilDiv(n, d *big.Int) *big.Int {
	q, m := new(big.Int).QuoRem(n, d, new(big.Int))
	if m.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return q
}
