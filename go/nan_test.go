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

// Unit tests (no Redis) for NaNs and infinities of REAL and DOUBLE PRECISION
// values (nan.go, issue #110): how they are stored, shown, compared, looked
// up in the index and hashed.

import (
	"context"
	"math"
	"slices"
	"strings"
	"testing"
)

// TestNaNStored checks the HASH values of floats and what they read back as,
// also for the values earlier versions wrote and RediSearch replies with.
func TestNaNStored(t *testing.T) {
	inf, nan := math.Inf(1), math.NaN()
	for _, typ := range []ColType{typeFloat64, {Kind: KindFloat32}} {
		for _, c := range []struct {
			f      float64
			stored string
		}{
			{nan, "infinity"}, {inf, "+Inf"}, {-inf, "-Inf"}, {1.5, "1.5"}, {0, "0"}, {-0.375, "-0.375"},
		} {
			got := encodeStored(floatValue(typ, c.f))
			if got != c.stored {
				t.Errorf("%s %v stored as %q, want %q", typ.SQLName(), c.f, got, c.stored)
			}
			back, err := decodeStored(got, typ)
			if err != nil || back.T.Kind != typ.Kind || !(back.F == c.f || math.IsNaN(back.F) && math.IsNaN(c.f)) {
				t.Errorf("%s %q read back as %v (%v)", typ.SQLName(), got, back.F, err)
			}
		}
		// Earlier versions wrote "NaN"; RediSearch replies (reducers) say inf
		// and nan, and -nan on amd64. Only nanStored is a NaN that the index
		// holds as +inf.
		for s, want := range map[string]float64{"NaN": nan, "inf": inf, "-inf": -inf, "nan": nan, "+Inf": inf, "-nan": nan, "+nan": nan, "-NaN": nan} {
			v, err := decodeStored(s, typ)
			if err != nil || !(v.F == want || math.IsNaN(v.F) && math.IsNaN(want)) {
				t.Errorf("%s %q read as %v (%v), want %v", typ.SQLName(), s, v.F, err, want)
			}
		}
	}
	// nanStored is only a float's NaN.
	if v, err := decodeStored(nanStored, typeString); err != nil || v.S != nanStored {
		t.Errorf("VARCHAR %q read as %v (%v)", nanStored, v, err)
	}
	// What the driver writes for +Infinity, and RediSearch's replies, aren't
	// nanStored.
	for _, s := range []string{encodeStored(floatValue(typeFloat64, inf)), "inf", "+inf"} {
		if s == nanStored {
			t.Errorf("%q is nanStored", s)
		}
	}
}

// TestNaNText checks NaNs and infinities as text, as Postgres's float8out
// and float4out write them, and comparisons and conversions of them.
func TestNaNText(t *testing.T) {
	inf := math.Inf(1)
	for _, typ := range []ColType{typeFloat64, {Kind: KindFloat32}} {
		for f, want := range map[float64]string{math.NaN(): "NaN", inf: "Infinity", -inf: "-Infinity", 1.5: "1.5", 1e20: "1e+20"} {
			if got := floatValue(typ, f).Text(); got != want {
				t.Errorf("%s %v as text is %q, want %q", typ.SQLName(), f, got, want)
			}
		}
	}
	const d, r, b, s = "DOUBLE PRECISION", "REAL", "BOOLEAN", "VARCHAR"
	runScalarCases(t, []scalarCase{
		{"CAST('NaN' AS DOUBLE PRECISION)", "NaN", d},
		{"CAST('nan' AS REAL)", "NaN", r},
		{"CAST('Infinity' AS DOUBLE PRECISION) - CAST('Infinity' AS DOUBLE PRECISION)", "NaN", d},
		{"CAST(CAST('inf' AS DOUBLE PRECISION) AS VARCHAR)", "Infinity", s},
		{"CAST(CAST('-Infinity' AS REAL) AS VARCHAR)", "-Infinity", s},
		{"CAST(CAST('NaN' AS REAL) AS VARCHAR)", "NaN", s},
		{"CAST(CAST(CAST('Infinity' AS DOUBLE PRECISION) AS VARCHAR) AS DOUBLE PRECISION)", "Infinity", d},
		{"CAST('NaN' AS DOUBLE PRECISION) || ''", "NaN", ""},
		{"CAST('-inf' AS DOUBLE PRECISION) || ''", "-Infinity", ""},
		// NaN is above every number and equal to itself.
		{"CAST('NaN' AS DOUBLE PRECISION) = CAST('NaN' AS DOUBLE PRECISION)", "true", b},
		{"CAST('NaN' AS DOUBLE PRECISION) > CAST('Infinity' AS DOUBLE PRECISION)", "true", b},
		{"CAST('NaN' AS DOUBLE PRECISION) <> 5", "true", b},
		{"CAST('NaN' AS REAL) = CAST('NaN' AS DOUBLE PRECISION)", "true", b},
		{"CAST('NaN' AS DOUBLE PRECISION) IN (1, CAST('NaN' AS DOUBLE PRECISION))", "true", b},
		{"GREATEST(CAST('Infinity' AS DOUBLE PRECISION), CAST('NaN' AS DOUBLE PRECISION), 1)", "NaN", d},
		{"LEAST(CAST('-Infinity' AS DOUBLE PRECISION), CAST('NaN' AS DOUBLE PRECISION))", "-Infinity", d},
	})
	// NUMERIC holds neither, from text or from a float.
	for _, expr := range []string{
		"CAST('NaN' AS NUMERIC)", "CAST(' Infinity ' AS NUMERIC(10,2))", "CAST('-inf' AS NUMERIC)",
		"CAST(CAST('NaN' AS DOUBLE PRECISION) AS NUMERIC)", "CAST(CAST('Infinity' AS REAL) AS NUMERIC(10,2))",
	} {
		if _, _, err := evalTestExpr(t, expr); err == nil || !strings.Contains(err.Error(), "NUMERIC values can't be NaN or infinite") {
			t.Errorf("%s: error %v", expr, err)
		}
	}
	if _, _, err := evalTestExpr(t, "CAST('-Infinity' AS NUMERIC(10,2))"); err == nil ||
		err.Error() != "cannot convert -Infinity to NUMERIC(10,2): NUMERIC values can't be NaN or infinite" {
		t.Errorf("error %v", err)
	}
	// Other text isn't affected: the n of 'banana' is no NaN.
	if _, _, err := evalTestExpr(t, "CAST('banana' AS NUMERIC)"); err == nil || !strings.Contains(err.Error(), `invalid decimal "banana"`) {
		t.Errorf("error %v", err)
	}
}

// nanTestMeta is a table with an indexed DOUBLE x, a REAL r and a NOINDEX
// DOUBLE n; x and r may hold NaN when nans is set.
func nanTestMeta(nans bool) *tableMeta {
	return &tableMeta{Schema: defaultSchema, Name: "t", Columns: []columnMeta{
		{Name: "id", Type: typeInt32, Indexed: true},
		{Name: "x", Type: typeFloat64, Indexed: true, NaNs: nans},
		{Name: "r", Type: ColType{Kind: KindFloat32}, Indexed: true, NaNs: nans},
		{Name: "n", Type: typeFloat64},
		{Name: "s", Type: typeString, Indexed: true, TagsChecked: true},
	}}
}

// TestNaNPushdown checks the index queries of comparisons with NaN and the
// infinities: their bounds are +inf and -inf, and a NaN or +Infinity bound
// is inclusive and re-checked, whether the column holds NaNs or not.
func TestNaNPushdown(t *testing.T) {
	for _, c := range []struct {
		where, query string
		residual     bool
	}{
		{"x = 5", "@x:[5 5]", false},
		{"x > 5", "@x:[(5 +inf]", false},
		{"x < 5", "@x:[-inf (5]", false},
		{"x = 'NaN'", "@x:[+inf +inf]", true},
		{"x = CAST('NaN' AS DOUBLE PRECISION)", "@x:[+inf +inf]", true},
		{"x > 'NaN'", "@x:[+inf +inf]", true},
		{"x < 'NaN'", "@x:[-inf +inf]", true},
		{"x <= 'NaN'", "@x:[-inf +inf]", true},
		{"x = 'Infinity'", "@x:[+inf +inf]", true},
		{"x > 'Infinity'", "@x:[+inf +inf]", true},
		{"x >= 'Infinity'", "@x:[+inf +inf]", true},
		{"x < 'Infinity'", "@x:[-inf +inf]", true},
		{"x = '-Infinity'", "@x:[-inf -inf]", false},
		{"x > '-Infinity'", "@x:[(-inf +inf]", false},
		{"x <= '-inf'", "@x:[-inf -inf]", false},
		{"x < '-Infinity'", "@x:[-inf (-inf]", false},
		{"r = 'NaN'", "@r:[+inf +inf]", true},
		{"r > 1e300", "@r:[+inf +inf]", true}, // +Infinity as a REAL
		{"r < '-Infinity'", "@r:[-inf (-inf]", false},
		{"x IN (1.5, 'NaN', 'Infinity')", "(@x:[1.5 1.5] | @x:[+inf +inf] | @x:[+inf +inf])", true},
		{"x = 'NaN' OR x = '-Infinity'", "(@x:[+inf +inf] | @x:[-inf -inf])", true},
		{"x BETWEEN 2 AND 'NaN'", "@x:[2 +inf] @x:[-inf +inf]", true},
		{"n = 'NaN'", "*", true},
		{"x <> 'NaN'", "*", true},
	} {
		for _, nans := range []bool{false, true} {
			parsed, err := ParseScript("SELECT 1 FROM t WHERE " + c.where)
			if err != nil {
				t.Fatalf("%s: %v", c.where, err)
			}
			e := &executor{}
			wp, err := e.planWhere(context.Background(), parsed[0].Stmt.(*SelectStmt).Where, nanTestMeta(nans), nil)
			if err != nil {
				t.Fatalf("%s: %v", c.where, err)
			}
			if wp.query != c.query || (wp.residual != nil) != c.residual {
				t.Errorf("%s (NaNs %v): query %q residual %v, want %q %v", c.where, nans, wp.query, wp.residual != nil, c.query, c.residual)
			}
		}
	}
	for v, want := range map[float64]string{math.NaN(): "+inf", math.Inf(1): "+inf", math.Inf(-1): "-inf", -1.5: "-1.5"} {
		if got := numericBound(floatValue(typeFloat64, v)); got != want {
			t.Errorf("numericBound(%v) = %q, want %q", v, got, want)
		}
	}
	if got := numericBound(intValue(typeInt64, 7)); got != "7" {
		t.Errorf("numericBound(7) = %q", got)
	}
}

// TestNaNColumnNeeds checks which writes set a column's NaNs, alongside the
// tag levels, and what NaNs turns off.
func TestNaNColumnNeeds(t *testing.T) {
	meta := nanTestMeta(false)
	nan := math.NaN()
	row := func(x, r, n float64, s string) []Value {
		return []Value{intValue(typeInt32, 1), floatValue(typeFloat64, x), floatValue(ColType{Kind: KindFloat32}, r),
			floatValue(typeFloat64, n), stringValue(s)}
	}
	null := []Value{nullValue(typeInt32), nullValue(typeFloat64), nullValue(ColType{Kind: KindFloat32}), nullValue(typeFloat64), nullValue(typeString)}
	for _, c := range []struct {
		rows   [][]Value
		nans   string
		levels string
	}{
		{[][]Value{row(1, 2, 3, "a")}, "", ""},
		{[][]Value{row(math.Inf(1), math.Inf(-1), nan, "a")}, "", ""}, // n isn't indexed
		{[][]Value{row(1, 2, 3, "a"), row(nan, 2, 3, "b ")}, "x", "s=normalized"},
		{[][]Value{row(nan, nan, 3, "a"), null}, "r x", ""},
	} {
		need := rowNeeds(meta, c.rows)
		if got := needsText(need.nans); got != c.nans {
			t.Errorf("rowNeeds NaNs %q, want %q", got, c.nans)
		}
		if got := levelsText(need.levels); got != c.levels {
			t.Errorf("rowNeeds levels %q, want %q", got, c.levels)
		}
	}
	if need := rowNeeds(nanTestMeta(true), [][]Value{row(nan, nan, nan, "a")}); len(need.nans) != 0 {
		t.Errorf("rowNeeds of columns with NaNs: %v", need.nans)
	}
	key := meta.prefix() + "1"
	changes := []rowChange{
		newRowChange(meta, key, []int{1, 3}, []Value{floatValue(typeFloat64, 1), floatValue(typeFloat64, nan)}),
		newRowChange(meta, key, []int{2, 4}, []Value{floatValue(ColType{Kind: KindFloat32}, nan), stringValue("\x00")}),
		newRowChange(meta, key, []int{1}, []Value{nullValue(typeFloat64)}),
	}
	need := changeNeeds(meta, changes)
	if got, got2 := needsText(need.nans), levelsText(need.levels); got != "r" || got2 != "s=truncated" {
		t.Errorf("changeNeeds NaNs %q levels %q, want r, s=truncated", got, got2)
	}
	if need := changeNeeds(nanTestMeta(true), changes[:2]); len(need.nans) != 0 {
		t.Errorf("changeNeeds of columns with NaNs: %v", need.nans)
	}

	// NaNs turns off index sorts, groups and reducers other than COUNT.
	for _, nans := range []bool{false, true} {
		x, _ := nanTestMeta(nans).column("x")
		if x.sortsExactly() == nans || x.pushable(PushdownAll) == nans {
			t.Errorf("NaNs %v: sortsExactly %v, pushable %v", nans, x.sortsExactly(), x.pushable(PushdownAll))
		}
		if x.pushable(PushdownExact) {
			t.Errorf("NaNs %v: a double is pushable in the exact mode", nans)
		}
	}
}

func needsText(m map[string]bool) string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return strings.Join(out, " ")
}

func levelsText(m map[string]string) string {
	var out []string
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	slices.Sort(out)
	return strings.Join(out, " ")
}

// TestNaNJoinKeys checks that NaNs of either float type have one hash key,
// apart from +Infinity's, so that hash joins, IN sets and DISTINCT see NaN =
// NaN.
func TestNaNJoinKeys(t *testing.T) {
	k64, ok64 := joinKey(floatValue(typeFloat64, math.NaN()))
	k32, ok32 := joinKey(floatValue(ColType{Kind: KindFloat32}, float64(float32(math.NaN()))))
	kinf, _ := joinKey(floatValue(typeFloat64, math.Inf(1)))
	if !ok64 || !ok32 || k64 != k32 || k64 == kinf {
		t.Errorf("NaN keys %q %v, %q %v; +Infinity %q", k64, ok64, k32, ok32, kinf)
	}
	nan := []Value{floatValue(typeFloat64, math.NaN())}
	if rowKey(nan) != rowKey([]Value{floatValue(typeFloat64, math.NaN())}) || rowKey(nan) == rowKey([]Value{nullValue(typeFloat64)}) {
		t.Errorf("row keys of NaN rows")
	}
	if c, k := eqKey(nan[0]); c != clsFloat || k != k64 {
		t.Errorf("eqKey(NaN) = %v, %q", c, k)
	}
	if k, ok := semiKey(nan[0], typeFloat64); !ok || k != k64 {
		t.Errorf("semiKey(NaN) = %q, %v", k, ok)
	}
}
