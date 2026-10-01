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

import (
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// equalityValues covers every kind compareValues handles, with values that
// are equal across types (1 = 1.0 = 1.00 = TRUE, '1 day' = '24 hours', the
// same instant in different units), values that only compare equal as
// doubles, and values joinKey can't key (NaN).
func equalityValues() []Value {
	dec := func(unscaled string, scale int32) Value {
		d, _ := new(big.Int).SetString(unscaled, 10)
		return decimalValue(d, 38, scale)
	}
	ts := func(u arrow.TimeUnit, i int64) Value { return Value{T: timestampType(u, ""), I: i} }
	tm := func(u arrow.TimeUnit, i int64) Value { return Value{T: timeType(u), I: i} }
	iv := func(months, days int32, ns int64) Value {
		return Value{T: typeInterval, Months: months, Days: days, I: ns}
	}
	return []Value{
		intValue(ColType{Kind: KindInt16}, 0), intValue(typeInt32, 1), intValue(typeInt64, -1),
		intValue(typeInt64, 2), intValue(typeInt64, 1<<53+1), intValue(typeInt64, 1<<60),
		intValue(typeInt64, 1_000_000_000_000_000), intValue(typeInt64, 999_999_999_999_999),
		intValue(typeInt64, math.MaxInt64),
		boolValue(true), boolValue(false),
		dec("100", 2), dec("15", 1), dec("10000000000000001", 17), dec("-50", 2), dec("0", 3),
		dec("2", 0), dec("1000000000000000", 0), dec("9007199254740993", 0), dec("1", 1),
		floatValue(typeFloat64, 0), floatValue(typeFloat64, math.Copysign(0, -1)),
		floatValue(typeFloat64, 1), floatValue(typeFloat64, 1.5), floatValue(typeFloat64, 0.1),
		floatValue(typeFloat64, 2), floatValue(typeFloat64, 1e15), floatValue(typeFloat64, 1<<53),
		floatValue(typeFloat64, 1<<60), floatValue(typeFloat64, math.Inf(1)),
		floatValue(typeFloat64, math.Inf(-1)), floatValue(typeFloat64, math.NaN()),
		floatValue(ColType{Kind: KindFloat32}, float64(float32(0.1))), floatValue(typeFloat64, -0.5),
		stringValue("1"), stringValue("1.0"), stringValue("abc"), stringValue(""),
		stringValue("a\x00b"), stringValue("NaN"), stringValue("2025-01-02"),
		binaryValue("abc"), binaryValue("1"),
		intValue(typeDate, 0), intValue(typeDate, 20090), intValue(typeDate, -1),
		ts(arrow.Second, 1_736_000_000), ts(arrow.Millisecond, 1_736_000_000_000),
		ts(arrow.Microsecond, 1_736_000_000_000_001), ts(arrow.Nanosecond, 1_736_000_000_000_000_000),
		ts(arrow.Second, 9_000_000_000_000), ts(arrow.Microsecond, 1_735_776_000_000_000),
		tm(arrow.Second, 3600), tm(arrow.Millisecond, 3_600_000), tm(arrow.Nanosecond, 3_600_000_000_001),
		iv(0, 1, 0), iv(0, 0, 24*3600*1e9), iv(1, 0, 0), iv(0, 30, 0), iv(0, 0, 1),
	}
}

func showBool(v Value) string {
	if v.Null {
		return "NULL"
	}
	if v.I != 0 {
		return "true"
	}
	return "false"
}

// The hash set must give exactly the answers of comparing one by one, for
// every probe against sets of each class, of mixed classes, and with NULLs.
func TestInSetMatchesLinearScan(t *testing.T) {
	values := equalityValues()
	var sets [][]Value
	byClass := map[eqClass][]Value{}
	for _, v := range values {
		c, _ := eqKey(v)
		byClass[c] = append(byClass[c], v)
	}
	for _, vs := range byClass {
		sets = append(sets, vs)
	}
	sets = append(sets, values, append(byClass[clsNum], byClass[clsFloat]...),
		append(byClass[clsDate], byClass[clsTimestamp]...), append(byClass[clsStr], byClass[clsNum]...))
	rnd := rand.New(rand.NewSource(17))
	for i := 0; i < 40; i++ {
		var vs []Value
		for _, v := range values {
			if rnd.Intn(4) == 0 {
				vs = append(vs, v)
			}
		}
		if i%2 == 0 {
			vs = append(vs, nullValue(typeInt64))
		}
		sets = append(sets, vs)
	}
	for n, vs := range sets {
		// Repeat the values so that the set is large enough to be hashed.
		var rows [][]Value
		for len(rows) <= inSetLinear {
			for _, v := range vs {
				rows = append(rows, []Value{v})
			}
			if len(vs) == 0 {
				break
			}
		}
		hashed := &inSet{rows: rows}
		linear := &inSet{rows: rows, noIndex: true}
		for _, x := range append(values, nullValue(typeInt64)) {
			for _, not := range []bool{false, true} {
				got, want := showBool(hashed.eval(x, not)), showBool(linear.eval(x, not))
				if got != want {
					t.Errorf("set %d: %s (%s) NOT=%v: hashed %s, linear %s", n, x.Text(), x.T.Kind, not, got, want)
				}
			}
		}
	}
}

// A semi-join compares keys of the same class by their eqKey; that must agree
// with compareValues, which never fails for such values.
func TestSemiKeyMatchesCompareValues(t *testing.T) {
	values := equalityValues()
	for _, a := range values {
		for _, b := range values {
			if !sameEqClass(a.T, b.T) {
				continue
			}
			c, ok := compareValues(a, b)
			if !ok {
				t.Errorf("%s (%s) and %s (%s) fail to compare", a.Text(), a.T.Kind, b.Text(), b.T.Kind)
				continue
			}
			ka, okA := semiKey(a, a.T)
			kb, okB := semiKey(b, a.T)
			if !okA || !okB {
				if !(a.T.Kind.isFloat() && math.IsNaN(a.F)) && !(b.T.Kind.isFloat() && math.IsNaN(b.F)) {
					t.Errorf("no key for %s (%s) or %s (%s)", a.Text(), a.T.Kind, b.Text(), b.T.Kind)
				}
				continue
			}
			if (c == 0) != (ka == kb) {
				t.Errorf("%s (%s) vs %s (%s): compare %d, keys %q %q", a.Text(), a.T.Kind, b.Text(), b.T.Kind, c, ka, kb)
			}
		}
	}
}
