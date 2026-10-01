// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

// Exact sums for SUM, AVG and the variances and standard deviations.
//
// Integers, decimals and doubles are all fractions whose denominator is a
// power of 2 times a power of 5, so exactSum keeps the sum of its finite
// values exactly, as an integer times 2^exp / 5^scale, and counts the NaNs
// and infinities. Results are computed from the exact sum (and the exact
// sum of squares) and rounded once: to the nearest double, ties to even,
// or to the decimal the SQL type asks for. So a result doesn't depend on
// the order in which the rows arrive, which on a cluster depends on which
// shard answers first, and a window frame removes a value by subtracting
// it, exactly. Adding a value costs an integer addition (and a shift or a
// multiplication when it has a smaller unit than every value before it).

import (
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"strings"
)

// exactSum is the exact sum of non-NULL numbers (and, with squares, of
// their squares). The finite values sum to sum × 2^exp / 5^scale, and their
// squares to sq × 2^(2 exp) / 5^(2 scale), where scale is the most decimal
// places of a decimal value and exp ≤ -scale is at most the exponent of the
// lowest bit of every double. A value with s decimal places, d / 10^s, is
// d × 5^(scale-s) × 2^(-s-exp) such units, a double m × 2^e is m × 5^scale ×
// 2^(e-exp), and an integer i is i × 5^scale × 2^-exp.
type exactSum struct {
	squares bool
	// n counts the values, nan the NaNs, inf and ninf the infinities.
	n, nan, inf, ninf int64
	sum, sq           big.Int
	exp               int
	scale             int32
	t, t2             big.Int // the value being added, and its square
	a, b, c, d        big.Int // scratch for results
}

func (s *exactSum) reset() {
	s.n, s.nan, s.inf, s.ninf = 0, 0, 0, 0
	s.sum.SetInt64(0)
	s.sq.SetInt64(0)
	s.exp, s.scale = 0, 0
}

// add adds a non-NULL value.
func (s *exactSum) add(v Value) { s.update(v, false) }

// remove removes a value added before.
func (s *exactSum) remove(v Value) { s.update(v, true) }

func (s *exactSum) update(v Value, remove bool) {
	d := int64(1)
	if remove {
		d = -1
	}
	if s.n == s.nan+s.inf+s.ninf {
		// No finite value (so the sums are 0): start at the coarsest unit.
		s.exp, s.scale = 0, 0
	}
	s.n += d
	t := &s.t
	switch k := v.T.Kind; {
	case k.isInteger() || k == KindBool:
		if v.I == 0 {
			return
		}
		t.SetInt64(v.I)
		if s.scale > 0 {
			t.Mul(t, pow5(s.scale))
		}
		if s.exp < 0 {
			t.Lsh(t, uint(-s.exp))
		}
	case k == KindDecimal:
		if v.D.Sign() == 0 {
			return
		}
		t.Set(v.D)
		sc := v.T.Scale
		if sc < 0 {
			t.Mul(t, pow10(-sc))
			sc = 0
		}
		if sc > s.scale {
			s.raiseScale(sc)
		}
		if s.scale > sc {
			t.Mul(t, pow5(s.scale-sc))
		}
		if sh := -int(sc) - s.exp; sh > 0 {
			t.Lsh(t, uint(sh))
		}
	default:
		// Doubles; other kinds count as 0, as they always have.
		f, _ := v.asFloat()
		switch {
		case f == 0:
			return
		case f != f:
			s.nan += d
			return
		case f > math.MaxFloat64:
			s.inf += d
			return
		case f < -math.MaxFloat64:
			s.ninf += d
			return
		}
		m, e := splitFloat(f)
		if e < s.exp {
			s.lowerExp(e)
		}
		sh := uint(e - s.exp)
		if s.scale == 0 && sh <= 10 { // m < 2^53
			t.SetUint64(m << sh)
		} else {
			t.SetUint64(m)
			if s.scale > 0 {
				t.Mul(t, pow5(s.scale))
			}
			t.Lsh(t, sh)
		}
		if f < 0 {
			t.Neg(t)
		}
	}
	if remove {
		s.sum.Sub(&s.sum, t)
	} else {
		s.sum.Add(&s.sum, t)
	}
	if s.squares {
		s.t2.Mul(t, t)
		if remove {
			s.sq.Sub(&s.sq, &s.t2)
		} else {
			s.sq.Add(&s.sq, &s.t2)
		}
	}
}

// raiseScale brings the sums to sc > scale decimal places.
func (s *exactSum) raiseScale(sc int32) {
	k := sc - s.scale
	s.sum.Mul(&s.sum, pow5(k))
	if s.squares {
		s.sq.Mul(&s.sq, pow5(2*k))
	}
	s.scale = sc
	if s.exp > -int(sc) {
		s.lowerExp(-int(sc))
	}
}

// lowerExp brings the sums to the unit 2^e < 2^exp.
func (s *exactSum) lowerExp(e int) {
	k := uint(s.exp - e)
	s.sum.Lsh(&s.sum, k)
	if s.squares {
		s.sq.Lsh(&s.sq, 2*k)
	}
	s.exp = e
}

// special returns the sum of the NaNs and infinities, as IEEE arithmetic
// gives it in any order (NaN for a NaN or for both infinities), and false
// if there are none.
func (s *exactSum) special() (float64, bool) {
	switch {
	case s.nan > 0 || (s.inf > 0 && s.ninf > 0):
		return math.NaN(), true
	case s.inf > 0:
		return math.Inf(1), true
	case s.ninf > 0:
		return math.Inf(-1), true
	}
	return 0, false
}

// float returns the sum rounded to the nearest double: an infinity if it
// is beyond the largest one.
func (s *exactSum) float() float64 {
	if f, ok := s.special(); ok {
		return f
	}
	if s.scale == 0 {
		return nearestFloat(&s.sum, s.exp, false, &s.a)
	}
	return quoNearest(&s.sum, s.exp, pow5(s.scale), &s.a, &s.b, &s.c)
}

// mean returns the sum divided by the number of values, rounded to the
// nearest double.
func (s *exactSum) mean() float64 {
	if f, ok := s.special(); ok {
		return f
	}
	den := s.d.SetInt64(s.n)
	if s.scale > 0 {
		den.Mul(den, pow5(s.scale))
	}
	return quoNearest(&s.sum, s.exp, den, &s.a, &s.b, &s.c)
}

// decimal returns the sum with sc decimal places (a new integer), rounded
// half away from zero if it has more. Integers and decimals sum to a
// decimal with scale places.
func (s *exactSum) decimal(sc int32) *big.Int {
	// sum × 2^exp / 5^scale × 10^sc = sum × 5^(sc-scale) × 2^(exp+sc)
	out := new(big.Int).Set(&s.sum)
	k5, k2 := sc-s.scale, s.exp+int(sc)
	if k5 > 0 {
		out.Mul(out, pow5(k5))
	}
	if k2 > 0 {
		out.Lsh(out, uint(k2))
	}
	if k5 >= 0 && k2 >= 0 {
		return out
	}
	den := s.d.SetInt64(1)
	if k5 < 0 {
		den.Set(pow5(-k5))
	}
	if k2 < 0 {
		den.Lsh(den, uint(-k2))
	}
	r := &s.a
	out.QuoRem(out, den, r)
	if r.Abs(r).Lsh(r, 1).Cmp(den) >= 0 {
		if s.sum.Sign() < 0 {
			out.Sub(out, big.NewInt(1))
		} else {
			out.Add(out, big.NewInt(1))
		}
	}
	return out
}

// sumValue returns the sum as a value of type t: BIGINT (an error if it
// doesn't fit), a decimal (with t's scale, or more places if the values
// have them) or a double.
func (s *exactSum) sumValue(t ColType) (Value, error) {
	switch t.Kind {
	case KindInt64:
		if s.exp == 0 && s.scale == 0 {
			if !s.sum.IsInt64() {
				return Value{}, fmt.Errorf("integer overflow in SUM")
			}
			return intValue(typeInt64, s.sum.Int64()), nil
		}
		d := s.decimal(0)
		if !d.IsInt64() {
			return Value{}, fmt.Errorf("integer overflow in SUM")
		}
		return intValue(typeInt64, d.Int64()), nil
	case KindDecimal:
		sc := max(t.Scale, s.scale)
		return decimalValue(s.decimal(sc), t.Precision, sc), nil
	}
	return floatValue(typeFloat64, s.float()), nil
}

// variance returns the sample variance of the values (the population
// variance with pop), or its square root with root, rounded to the nearest
// double; false means NULL: no values, or one for a sample. As in
// PostgreSQL, a NaN or an infinity makes it NaN.
func (s *exactSum) variance(pop, root bool) (float64, bool) {
	n := s.n
	if n == 0 || (n == 1 && !pop) {
		return 0, false
	}
	if s.nan+s.inf+s.ninf > 0 {
		return math.NaN(), true
	}
	// (nΣx² - (Σx)²) / (n(n - 1)), or / n² for the population, which is
	// (n sq - sum²) × 2^(2 exp) / 5^(2 scale) / (n(n - 1)).
	num := s.d.SetInt64(n)
	num.Mul(num, &s.sq)
	num.Sub(num, s.a.Mul(&s.sum, &s.sum))
	den := new(big.Int).SetInt64(n)
	if pop {
		den.Mul(den, s.a.SetInt64(n))
	} else {
		den.Mul(den, s.a.SetInt64(n-1))
	}
	if s.scale > 0 {
		den.Mul(den, pow5(2*s.scale))
	}
	if !root {
		return quoNearest(num, 2*s.exp, den, &s.a, &s.b, &s.c), true
	}
	if num.Sign() == 0 {
		return 0, true
	}
	var x, y big.Float
	x.SetPrec(256).SetInt(num)
	x.SetMantExp(&x, 2*s.exp)
	x.Quo(&x, y.SetPrec(256).SetInt(den))
	v, _ := x.Sqrt(&x).Float64()
	return v, true
}

// varianceValue returns the variance or standard deviation called name.
func (s *exactSum) varianceValue(name string) Value {
	v, ok := s.variance(name == "VAR_POP" || name == "STDDEV_POP", strings.HasPrefix(name, "STDDEV"))
	if !ok {
		return nullValue(typeFloat64)
	}
	return floatValue(typeFloat64, v)
}

// splitFloat returns a finite, non-zero double's magnitude as m × 2^e, m
// odd.
func splitFloat(f float64) (m uint64, e int) {
	b := math.Float64bits(f)
	exp := int(b >> 52 & 0x7ff)
	m = b & (1<<52 - 1)
	if exp == 0 {
		exp = 1 // subnormal
	} else {
		m |= 1 << 52
	}
	tz := bits.TrailingZeros64(m)
	return m >> tz, exp - 1075 + tz
}

// nearestFloat returns m × 2^exp rounded to the nearest double, ties to
// even. With inexact, the value is a little larger in magnitude, by less
// than 2^exp (what a truncated division drops), and m has at least 55
// bits. tmp is scratch.
func nearestFloat(m *big.Int, exp int, inexact bool, tmp *big.Int) float64 {
	n := m.BitLen()
	if n == 0 {
		return 0
	}
	if exp+n-1 < -1022 {
		// The leading bit is below the smallest normal double, so the result
		// may be subnormal, with fewer bits: big.Float rounds it. Between m
		// and m + 1 (inexact), m + 1/2 rounds the same, since every rounding
		// boundary there is a multiple of 2^-1075, so of 2^exp ≤ 2^-1076.
		v := m
		if inexact {
			v = tmp.Abs(m)
			v.Lsh(v, 1)
			v.Add(v, big.NewInt(1))
			if m.Sign() < 0 {
				v.Neg(v)
			}
			exp--
		}
		var x big.Float // SetInt gives it the precision to hold v exactly
		f, _ := x.SetInt(v).SetMantExp(&x, exp).Float64()
		return f
	}
	var u uint64
	shift := 0
	if n <= 64 {
		u = m.Uint64() // the magnitude
	} else {
		shift = n - 64
		inexact = inexact || m.TrailingZeroBits() < uint(shift)
		u = tmp.Rsh(tmp.Abs(m), uint(shift)).Uint64()
	}
	if inexact {
		// u has at least 55 bits, so bit 0 is below the rounding bit: setting
		// it only breaks a tie upwards, as the dropped part does.
		u |= 1
	}
	// float64(u) rounds to 53 bits, ties to even; scaling a normal double by
	// a power of two is exact (or overflows to an infinity).
	f := math.Ldexp(float64(u), exp+shift)
	if m.Sign() < 0 {
		f = -f
	}
	return f
}

// quoNearest returns num × 2^exp / den (den > 0) rounded to the nearest
// double. q, r and tmp are scratch.
func quoNearest(num *big.Int, exp int, den, q, r, tmp *big.Int) float64 {
	nb, db := num.BitLen(), den.BitLen()
	if nb == 0 {
		return 0
	}
	if nb <= 53 && db <= 53 && exp+nb-db-1 >= -1022 {
		// Both are exact doubles, so one IEEE division rounds the quotient
		// once, and it is a normal double.
		return math.Ldexp(float64(num.Int64())/float64(den.Int64()), exp)
	}
	// Shift num so that the truncated quotient has at least 55 bits.
	k := max(55+db-nb, 0)
	q.Lsh(num, uint(k))
	q.QuoRem(q, den, r)
	return nearestFloat(q, exp-k, r.Sign() != 0, tmp)
}

// pow5s caches 5^k for the scales decimals have.
var pow5s = func() []*big.Int {
	p := make([]*big.Int, 160)
	p[0] = big.NewInt(1)
	for k := 1; k < len(p); k++ {
		p[k] = new(big.Int).Mul(p[k-1], big.NewInt(5))
	}
	return p
}()

// pow5 returns 5^k (k ≥ 0), which must not be modified.
func pow5(k int32) *big.Int {
	if int(k) < len(pow5s) {
		return pow5s[k]
	}
	return new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(k)), nil)
}
