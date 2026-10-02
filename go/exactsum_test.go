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

// Unit tests for the exact sums of exactsum.go. They need no Redis server.
// The expected values are computed independently with math/big: sums as
// big.Rat (or big.Float with enough bits to be exact), rounded once by
// big.Rat.Float64 / big.Float.Float64.

import (
	"math"
	"math/big"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

// ratOf returns the exact value of a finite double, integer or decimal.
func ratOf(v Value) *big.Rat {
	switch {
	case v.T.Kind == KindDecimal:
		r := new(big.Rat).SetInt(v.D)
		if v.T.Scale >= 0 {
			return r.Quo(r, new(big.Rat).SetInt(pow10(v.T.Scale)))
		}
		return r.Mul(r, new(big.Rat).SetInt(pow10(-v.T.Scale)))
	case v.T.Kind.isInteger():
		return new(big.Rat).SetInt64(v.I)
	}
	return new(big.Rat).SetFloat64(v.F)
}

// refSum returns the exact sum of finite values.
func refSum(vals []Value) *big.Rat {
	s := new(big.Rat)
	for _, v := range vals {
		s.Add(s, ratOf(v))
	}
	return s
}

func ratFloat(r *big.Rat) float64 {
	f, _ := r.Float64()
	return f
}

// refVariance returns the variance of finite values, or its square root.
func refVariance(vals []Value, pop, root bool) float64 {
	n := int64(len(vals))
	s1, s2 := new(big.Rat), new(big.Rat)
	for _, v := range vals {
		r := ratOf(v)
		s1.Add(s1, r)
		s2.Add(s2, new(big.Rat).Mul(r, r))
	}
	num := new(big.Rat).Mul(big.NewRat(n, 1), s2)
	num.Sub(num, new(big.Rat).Mul(s1, s1))
	den := big.NewRat(n*(n-1), 1)
	if pop {
		den = big.NewRat(n*n, 1)
	}
	v := num.Quo(num, den)
	if !root {
		return ratFloat(v)
	}
	f := new(big.Float).SetPrec(2000).SetRat(v)
	out, _ := f.Sqrt(f).Float64()
	return out
}

func floats(fs ...float64) []Value {
	out := make([]Value, len(fs))
	for i, f := range fs {
		out[i] = floatValue(typeFloat64, f)
	}
	return out
}

func sumOf(vals []Value) *exactSum {
	s := &exactSum{squares: true}
	for _, v := range vals {
		s.add(v)
	}
	return s
}

func same(a, b float64) bool {
	return math.Float64bits(a) == math.Float64bits(b) || (math.IsNaN(a) && math.IsNaN(b))
}

func fmtF(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

// checkSum checks the sum, mean and variances of finite values against the
// reference, in this order and shuffled.
func checkSum(t *testing.T, name string, vals []Value, rng *rand.Rand) {
	t.Helper()
	want := ratFloat(refSum(vals))
	wantMean := ratFloat(new(big.Rat).Quo(refSum(vals), big.NewRat(int64(len(vals)), 1)))
	var wantVar [4]float64
	if len(vals) > 1 {
		for i := range wantVar {
			wantVar[i] = refVariance(vals, i&1 != 0, i&2 != 0)
		}
	}
	order := slices.Clone(vals)
	for round := range 4 {
		s := sumOf(order)
		if got := s.float(); !same(got, want) {
			t.Fatalf("%s (order %d): sum = %s, want %s", name, round, fmtF(got), fmtF(want))
		}
		if got := s.mean(); !same(got, wantMean) {
			t.Fatalf("%s (order %d): mean = %s, want %s", name, round, fmtF(got), fmtF(wantMean))
		}
		if len(vals) > 1 {
			for i := range wantVar {
				got, ok := s.variance(i&1 != 0, i&2 != 0)
				if !ok || !same(got, wantVar[i]) {
					t.Fatalf("%s (order %d): variance(pop=%v, root=%v) = %s, %v; want %s",
						name, round, i&1 != 0, i&2 != 0, fmtF(got), ok, fmtF(wantVar[i]))
				}
			}
		}
		rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	}
}

// TestExactSumIssueData is issue #83's data: 50,000 values whose sum, in
// the order they were inserted, a naive float sum gets right, but other
// orders (as rows come back from a cluster's shards) don't.
func TestExactSumIssueData(t *testing.T) {
	var vals []Value
	var fs []float64
	for x := 1; x <= 150000; x++ {
		if x%3 == 1 {
			f := float64((x*7919)%2000)/100.0 + 0.05
			vals = append(vals, floatValue(typeFloat64, f))
			fs = append(fs, f)
		}
	}
	rng := rand.New(rand.NewPCG(1, 2))
	if want := ratFloat(refSum(vals)); want != 502250 {
		t.Fatalf("the exact sum rounds to %s, want 502250", fmtF(want))
	}
	// Naive sums in other orders differ, which is the bug.
	naive := map[float64]bool{}
	for range 20 {
		rng.Shuffle(len(fs), func(i, j int) { fs[i], fs[j] = fs[j], fs[i] })
		s := 0.0
		for _, f := range fs {
			s += f
		}
		naive[s] = true
	}
	if len(naive) < 2 {
		t.Fatalf("naive sums in 20 orders all gave %v; the data should show the problem", naive)
	}
	checkSum(t, "issue data", vals, rng)
}

func TestExactSumRandom(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for c := range 300 {
		n := 1 + rng.IntN(60)
		vals := make([]Value, n)
		lo, hi := -1074, 1023
		if c%3 == 0 { // a narrow range, as real data has
			lo = -20 + rng.IntN(20)
			hi = lo + rng.IntN(30)
		}
		for i := range vals {
			f := math.Ldexp(rng.Float64(), lo+rng.IntN(hi-lo+1))
			if rng.IntN(2) == 0 {
				f = -f
			}
			if c%5 == 0 && i > 0 && rng.IntN(3) == 0 {
				f = -vals[rng.IntN(i)].F // cancellation
			}
			if math.IsInf(f, 0) {
				f = math.MaxFloat64
			}
			vals[i] = floatValue(typeFloat64, f)
		}
		checkSum(t, "random case "+strconv.Itoa(c), vals, rng)
	}
}

func TestExactSumRounding(t *testing.T) {
	p53 := math.Exp2(53)
	tiny := math.SmallestNonzeroFloat64
	for _, c := range []struct {
		name string
		vals []float64
		want float64
	}{
		{"tie to even, down", []float64{p53, 1}, p53},
		{"tie to even, up", []float64{p53, 3}, p53 + 4},
		{"a tie broken by a tiny value", []float64{p53, 1, 0x1p-60}, p53 + 2},
		{"a tie broken downwards", []float64{p53, 1, -0x1p-60}, p53},
		{"negative tie", []float64{-p53, -1}, -p53},
		{"0.1 ten times", []float64{0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1}, 1},
		{"cancellation", []float64{1e100, 1, -1e100}, 1},
		{"intermediate overflow", []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64}, math.MaxFloat64},
		{"overflow", []float64{math.MaxFloat64, math.MaxFloat64}, math.Inf(1)},
		{"negative overflow", []float64{-math.MaxFloat64, -math.MaxFloat64 / 2}, math.Inf(-1)},
		{"just below overflow", []float64{math.MaxFloat64, 0x1p969}, math.MaxFloat64},
		{"rounds to overflow", []float64{math.MaxFloat64, 0x1p970}, math.Inf(1)},
		{"subnormals", []float64{tiny, tiny, tiny}, 3 * tiny},
		{"normal from subnormals", []float64{0x1p-1023, 0x1p-1023}, 0x1p-1022},
		{"zeros", []float64{0, math.Copysign(0, -1)}, 0},
		{"exact zero", []float64{0.1, 0.2, -0.1, -0.2}, 0},
	} {
		s := sumOf(floats(c.vals...))
		if got := s.float(); !same(got, c.want) {
			t.Errorf("%s: sum = %s, want %s", c.name, fmtF(got), fmtF(c.want))
		}
	}
	// Half of the smallest subnormal can't be added, but a mean can be it:
	// the mean of the smallest subnormal and 0 is a tie, to even (0), and
	// of 3 × it and 0 is 1.5 × it, to even (2 × it).
	for _, c := range []struct {
		vals []float64
		want float64
	}{
		{[]float64{tiny, 0}, 0},
		{[]float64{3 * tiny, 0}, 2 * tiny},
		{[]float64{3 * tiny, 0, 0}, tiny},
		{[]float64{-3 * tiny, 0}, -2 * tiny},
		{[]float64{math.MaxFloat64, math.MaxFloat64}, math.MaxFloat64},
		{[]float64{1, 2}, 1.5},
		{[]float64{1, 1, 2}, 4.0 / 3},
	} {
		s := sumOf(floats(c.vals...))
		if got := s.mean(); !same(got, c.want) {
			t.Errorf("mean of %v = %s, want %s", c.vals, fmtF(got), fmtF(c.want))
		}
	}
}

// TestNearestFloat rounds values near the subnormal range, where rounding to
// 53 bits first and then to the subnormal's fewer bits would round twice,
// and quotients with a remainder there.
func TestNearestFloat(t *testing.T) {
	var tmp, q, r, c big.Int
	checked := 0
	for k := int64(1); k <= 40; k++ {
		for j := uint(52); j <= 60; j++ {
			for _, delta := range []int64{-1, 0, 1} {
				m := new(big.Int).Lsh(big.NewInt(k), j)
				m.Add(m, big.NewInt(delta))
				for top := -1080; top <= -1018; top++ {
					exp := top - m.BitLen() + 1
					for _, neg := range []bool{false, true} {
						mm := new(big.Int).Set(m)
						if neg {
							mm.Neg(mm)
						}
						x := new(big.Rat).SetInt(mm)
						scale := new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).Lsh(big.NewInt(1), uint(-exp)))
						x.Mul(x, scale)
						if got, want := nearestFloat(mm, exp, false, &tmp), ratFloat(x); !same(got, want) {
							t.Fatalf("nearestFloat(%v, %d) = %s, want %s", mm, exp, fmtF(got), fmtF(want))
						}
						// A little more in magnitude: (m + 1/3) × 2^exp.
						y := new(big.Rat).SetFrac(big.NewInt(1), big.NewInt(3))
						if neg {
							y.Neg(y)
						}
						y.Mul(y, scale)
						y.Add(y, x)
						if m.BitLen() >= 55 {
							if got, want := nearestFloat(mm, exp, true, &tmp), ratFloat(y); !same(got, want) {
								t.Fatalf("nearestFloat(%v, %d, inexact) = %s, want %s", mm, exp, fmtF(got), fmtF(want))
							}
						}
						// The same as a quotient with a remainder: (3m + 1) / 3.
						num := new(big.Int).Mul(mm, big.NewInt(3))
						if neg {
							num.Sub(num, big.NewInt(1))
						} else {
							num.Add(num, big.NewInt(1))
						}
						if got, want := quoNearest(num, exp, big.NewInt(3), &q, &r, &c), ratFloat(y); !same(got, want) {
							t.Fatalf("quoNearest(%v, %d, 3) = %s, want %s", num, exp, fmtF(got), fmtF(want))
						}
						checked++
					}
				}
			}
		}
	}
	// Small quotients (one IEEE division unless subnormal) around the
	// subnormal range.
	for num := int64(-300); num <= 300; num++ {
		for _, den := range []int64{1, 3, 7, 10, 1000003, 1<<53 - 1} {
			for exp := -1130; exp <= -1015; exp++ {
				x := new(big.Rat).SetFrac(big.NewInt(num), new(big.Int).Mul(big.NewInt(den), new(big.Int).Lsh(big.NewInt(1), uint(-exp))))
				if got, want := quoNearest(big.NewInt(num), exp, big.NewInt(den), &q, &r, &c), ratFloat(x); !same(got, want) {
					t.Fatalf("quoNearest(%d, %d, %d) = %s, want %s", num, exp, den, fmtF(got), fmtF(want))
				}
			}
		}
	}
	// A value just below a subnormal tie: rounding to 53 bits first would
	// give 1.5 × the smallest subnormal, then 2 × it (to even).
	m := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(3), 53), big.NewInt(1))
	if got := nearestFloat(m, -1074-54, false, &tmp); got != math.SmallestNonzeroFloat64 {
		t.Errorf("(1.5 - 2^-54) × 2^-1074 rounds to %s, want the smallest subnormal", fmtF(got))
	}
	// 2.5 × the smallest subnormal and a little more rounds up, to 3 × it.
	m = new(big.Int).Lsh(big.NewInt(5), 54)
	if got := nearestFloat(m, -1074-55, true, &tmp); got != 3*math.SmallestNonzeroFloat64 {
		t.Errorf("2.5 × 2^-1074 and a little rounds to %s, want 3 × the smallest subnormal", fmtF(got))
	}
	if checked < 1000 {
		t.Fatalf("checked only %d values", checked)
	}
}

func TestExactSumSpecial(t *testing.T) {
	inf, nan := math.Inf(1), math.NaN()
	for _, c := range []struct {
		vals []float64
		want float64
	}{
		{[]float64{1, inf}, inf},
		{[]float64{1, -inf, 2}, -inf},
		{[]float64{inf, -inf}, nan},
		{[]float64{1, nan}, nan},
		{[]float64{inf, nan}, nan},
		{[]float64{inf, inf}, inf},
	} {
		s := sumOf(floats(c.vals...))
		if got := s.float(); !same(got, c.want) {
			t.Errorf("sum of %v = %s, want %s", c.vals, fmtF(got), fmtF(c.want))
		}
		if got := s.mean(); !same(got, c.want) {
			t.Errorf("mean of %v = %s, want %s", c.vals, fmtF(got), fmtF(c.want))
		}
		if got, ok := s.variance(false, false); !ok || !math.IsNaN(got) {
			t.Errorf("variance of %v = %s, %v; want NaN", c.vals, fmtF(got), ok)
		}
	}
	// Removing the infinity gives the finite sum back.
	s := sumOf(floats(1, inf, 2, -inf))
	s.remove(floatValue(typeFloat64, 1))
	s.remove(floatValue(typeFloat64, inf))
	s.remove(floatValue(typeFloat64, 2))
	s.remove(floatValue(typeFloat64, -inf))
	s.add(floatValue(typeFloat64, 0.5))
	if got := s.float(); got != 0.5 {
		t.Errorf("after removing the infinities, sum = %s, want 0.5", fmtF(got))
	}
	// NULL results.
	if _, ok := sumOf(floats(1)).variance(false, false); ok {
		t.Errorf("the sample variance of one value should be NULL")
	}
	if v, ok := sumOf(floats(inf)).variance(true, false); !ok || !math.IsNaN(v) {
		t.Errorf("the population variance of an infinity = %v, %v; want NaN", v, ok)
	}
	if v, ok := sumOf(floats(5)).variance(true, true); !ok || v != 0 {
		t.Errorf("the population standard deviation of one value = %v, %v; want 0", v, ok)
	}
}

// TestExactSumSliding removes values from a sliding window and compares
// with a fresh sum of the values left, as window frames do.
func TestExactSumSliding(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	vals := make([]Value, 400)
	for i := range vals {
		switch rng.IntN(10) {
		case 0:
			vals[i] = floatValue(typeFloat64, math.Ldexp(rng.Float64(), -1074+rng.IntN(2097)))
		case 1:
			vals[i] = floatValue(typeFloat64, math.Inf(1-2*rng.IntN(2)))
		default:
			vals[i] = floatValue(typeFloat64, math.Round(rng.NormFloat64()*1e6)/100+0.05)
		}
	}
	for _, width := range []int{1, 2, 7, 50} {
		s := &exactSum{squares: true}
		for i, v := range vals {
			s.add(v)
			if i >= width {
				s.remove(vals[i-width])
			}
			fresh := sumOf(vals[max(0, i-width+1) : i+1])
			if !same(s.float(), fresh.float()) || !same(s.mean(), fresh.mean()) {
				t.Fatalf("width %d, row %d: sliding sum %s / mean %s, fresh %s / %s",
					width, i, fmtF(s.float()), fmtF(s.mean()), fmtF(fresh.float()), fmtF(fresh.mean()))
			}
			for _, pop := range []bool{false, true} {
				a, aok := s.variance(pop, true)
				b, bok := fresh.variance(pop, true)
				if aok != bok || !same(a, b) {
					t.Fatalf("width %d, row %d: sliding stddev %s, fresh %s", width, i, fmtF(a), fmtF(b))
				}
			}
		}
	}
}

// TestExactSumKinds sums integers and decimals, also mixed with doubles.
func TestExactSumKinds(t *testing.T) {
	dec := func(s string) Value {
		d, scale, err := parseDecimal(s)
		if err != nil {
			t.Fatal(err)
		}
		return decimalValue(d, 38, scale)
	}
	ints := func(is ...int64) []Value {
		out := make([]Value, len(is))
		for i, v := range is {
			out[i] = intValue(typeInt64, v)
		}
		return out
	}
	// Integers: exact, also past 2^53 and through an intermediate overflow.
	s := sumOf(ints(math.MaxInt64, 1, -1, 7))
	if _, err := s.sumValue(typeInt64); err == nil || err.Error() != "integer overflow in SUM" {
		t.Errorf("SUM past the largest BIGINT: %v", err)
	}
	s.remove(intValue(typeInt64, 7))
	if v, err := s.sumValue(typeInt64); err != nil || v.I != math.MaxInt64 {
		t.Errorf("SUM through an overflow = %v, %v; want %d", v.I, err, int64(math.MaxInt64))
	}
	big1 := int64(1<<53 + 1)
	if got, want := sumOf(ints(big1, big1, 1)).mean(), ratFloat(big.NewRat(2*big1+1, 3)); got != want {
		t.Errorf("AVG of large integers = %s, want %s", fmtF(got), fmtF(want))
	}
	// Decimals: exact at their scale, more places kept.
	vals := []Value{dec("0.10"), dec("0.20"), dec("1.005"), intValue(typeInt64, 2)}
	v, err := sumOf(vals).sumValue(decimalType(38, 2))
	if err != nil || formatDecimal(v.D, v.T.Scale) != "3.305" {
		t.Errorf("SUM of decimals = %s (scale %d), %v; want 3.305", formatDecimal(v.D, v.T.Scale), v.T.Scale, err)
	}
	if got, want := sumOf(vals).mean(), ratFloat(big.NewRat(3305, 4000)); got != want {
		t.Errorf("AVG of decimals = %s, want %s", fmtF(got), fmtF(want))
	}
	// A third is no double: the mean of decimals is rounded once.
	third := []Value{dec("1"), dec("0"), dec("0")}
	if got := sumOf(third).mean(); got != 1.0/3 {
		t.Errorf("AVG(1, 0, 0) = %s", fmtF(got))
	}
	// Mixed kinds, exactly.
	rng := rand.New(rand.NewPCG(7, 8))
	for c := range 100 {
		var mixed []Value
		for range 1 + rng.IntN(20) {
			switch rng.IntN(3) {
			case 0:
				mixed = append(mixed, intValue(typeInt64, rng.Int64N(1<<62)-1<<61))
			case 1:
				d := big.NewInt(rng.Int64N(1 << 40))
				mixed = append(mixed, decimalValue(d.Sub(d, big.NewInt(1<<39)), 38, int32(rng.IntN(12))))
			default:
				mixed = append(mixed, floatValue(typeFloat64, math.Ldexp(rng.NormFloat64(), rng.IntN(80)-40)))
			}
		}
		checkSum(t, "mixed case "+strconv.Itoa(c), mixed, rng)
	}
	// Rounding a decimal sum to fewer places: half away from zero.
	for _, c := range []struct {
		vals []Value
		want string
	}{
		{[]Value{dec("1.25")}, "1.3"},
		{[]Value{dec("-1.25")}, "-1.3"},
		{[]Value{dec("1.24"), floatValue(typeFloat64, 0.5)}, "1.7"},
		{[]Value{dec("-0.05")}, "-0.1"},
	} {
		if got := formatDecimal(sumOf(c.vals).decimal(1), 1); got != c.want {
			t.Errorf("decimal(1) of %v = %s, want %s", c.vals, got, c.want)
		}
	}
}

// BenchmarkExactSum compares adding a double exactly with a plain float
// addition.
func BenchmarkExactSum(b *testing.B) {
	vals := make([]Value, 200000)
	for i := range vals {
		vals[i] = floatValue(typeFloat64, float64((i*7919)%2000)/100.0+0.05)
	}
	b.Run("float64", func(b *testing.B) {
		for b.Loop() {
			s := 0.0
			for _, v := range vals {
				s += v.F
			}
			_ = s
		}
	})
	b.Run("exactSum", func(b *testing.B) {
		for b.Loop() {
			var s exactSum
			for _, v := range vals {
				s.add(v)
			}
			_ = s.float()
		}
	})
	b.Run("exactSum with squares", func(b *testing.B) {
		for b.Loop() {
			s := exactSum{squares: true}
			for _, v := range vals {
				s.add(v)
			}
			_, _ = s.variance(false, true)
		}
	})
	b.Run("sliding mean", func(b *testing.B) {
		for b.Loop() {
			var s exactSum
			for i, v := range vals {
				s.add(v)
				if i >= 10 {
					s.remove(vals[i-10])
				}
				_ = s.mean()
			}
		}
	})
}
