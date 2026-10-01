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

// Integration tests for exact SUM, AVG, variances and standard deviations
// of doubles (exactsum.go), issue #83, and for the trigonometric functions
// in dbt_utils' haversine_distance, issue #89. They run against the Redis
// server at REDIS_URI, like sql_integration_test.go, standalone or a
// cluster. The expected values are computed here from the values the table
// holds (read back), exactly with math/big, and rounded once.

import (
	"fmt"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// floatColumn runs a query of one DOUBLE column and returns its values.
func (h *sqlHarness) floatColumn(sql string) []float64 {
	h.t.Helper()
	rows, _ := h.query(sql)
	out := make([]float64, len(rows))
	for i, r := range rows {
		f, err := strconv.ParseFloat(r, 64)
		if err != nil {
			h.t.Fatalf("%s: row %d is %q", sql, i, r)
		}
		out[i] = f
	}
	return out
}

// xsStats returns SUM, AVG, STDDEV_SAMP, STDDEV_POP, VAR_SAMP and VAR_POP
// of doubles, computed exactly and rounded once, as the driver renders them.
// Σx and Σx² are added as big.Floats with enough bits to be exact, then
// combined as fractions.
func xsStats(fs []float64) []string {
	if len(fs) == 0 {
		return []string{"NULL", "NULL", "NULL", "NULL", "NULL", "NULL"}
	}
	const prec = 8192 // exact for the sums of these doubles and their squares
	s1, s2, x, sq := new(big.Float).SetPrec(prec), new(big.Float).SetPrec(prec), new(big.Float), new(big.Float).SetPrec(prec)
	for _, f := range fs {
		x.SetFloat64(f)
		s1.Add(s1, x)
		s2.Add(s2, sq.Mul(x, x))
	}
	if s1.Acc() != big.Exact || s2.Acc() != big.Exact {
		panic("inexact reference")
	}
	n := int64(len(fs))
	sum, _ := s1.Rat(nil)
	mean := new(big.Rat).Quo(sum, big.NewRat(n, 1))
	out := []string{fmtF(ratFloat(sum)), fmtF(ratFloat(mean)), "NULL", "0", "NULL", "0"}
	if n > 1 {
		r2, _ := s2.Rat(nil)
		num := new(big.Rat).Mul(big.NewRat(n, 1), r2)
		num.Sub(num, new(big.Rat).Mul(sum, sum))
		root := func(r *big.Rat) float64 {
			f := new(big.Float).SetPrec(2000).SetRat(r)
			v, _ := f.Sqrt(f).Float64()
			return v
		}
		samp := new(big.Rat).Quo(num, big.NewRat(n*(n-1), 1))
		pop := new(big.Rat).Quo(num, big.NewRat(n*n, 1))
		out[2], out[3] = fmtF(root(samp)), fmtF(root(pop))
		out[4], out[5] = fmtF(ratFloat(samp)), fmtF(ratFloat(pop))
	}
	return out
}

// naiveSum adds doubles in order, as the driver did before.
func naiveSum(fs []float64) float64 {
	s := 0.0
	for _, f := range fs {
		s += f
	}
	return s
}

// pushdownModes are the default pushdown mode and each mode, by name.
var pushdownModes = []string{"default", PushdownExact, PushdownNone, PushdownAll}

// pushdownHarnesses returns a connection for each of pushdownModes.
func pushdownHarnesses(t *testing.T) map[string]*sqlHarness {
	hs := map[string]*sqlHarness{"default": newSQLHarness(t)}
	for _, m := range pushdownModes[1:] {
		hs[m] = newGSPushdownHarness(t, m)
	}
	return hs
}

// forModes runs check in a subtest for each pushdown mode, with its
// connection.
func forModes(t *testing.T, hs map[string]*sqlHarness, check func(mode string, h *sqlHarness)) {
	t.Helper()
	for _, mode := range pushdownModes {
		t.Run(mode, func(t *testing.T) {
			h := *hs[mode]
			h.t = t
			check(mode, &h)
		})
	}
}

// TestSQLExactSumsIssue is issue #83's query, over two tables holding its
// 150,000 rows in different orders. In the old driver, the sum of the
// 50,000 values of group 1 depended on the order the rows came in: the
// naive sums of these values in the orders below differ.
func TestSQLExactSumsIssue(t *testing.T) {
	hs := pushdownHarnesses(t)
	h := hs["default"]
	orders := map[string]string{"it_xs_asc": "x", "it_xs_mixed": "(x * 7919) % 150001 DESC"}
	for name, order := range orders {
		h.exec("DROP TABLE IF EXISTS " + name)
		h.exec("CREATE TABLE " + name + " (id INTEGER, g INTEGER, x DOUBLE PRECISION)")
		h.exec("INSERT INTO " + name + ` SELECT x, x % 3, ((x * 7919) % 2000) / 100.0 + 0.05
			FROM generate_series(1, 150000) AS s(x) ORDER BY ` + order)
		t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS " + name) })
	}
	vals := h.floatColumn("SELECT x FROM it_xs_asc WHERE g = 1 ORDER BY id")
	if len(vals) != 50000 {
		t.Fatalf("group 1 has %d rows", len(vals))
	}
	// The exact sum: the decimals (k + 5) / 100 sum to 502250, and the
	// doubles nearest them to a value that rounds to 502250 too.
	want := xsStats(vals)
	if want[0] != "502250" || want[1] != "10.045" {
		t.Fatalf("exact sum and mean %s, %s; want 502250, 10.045", want[0], want[1])
	}
	byID := naiveSum(vals)
	desc := slices.Clone(vals)
	slices.Reverse(desc)
	byX := slices.Clone(vals)
	slices.Sort(byX)
	naive := map[float64]bool{byID: true, naiveSum(desc): true, naiveSum(byX): true}
	if len(naive) < 2 {
		t.Fatalf("the naive sums in three orders are all %v: the data doesn't show the issue", byID)
	}
	t.Logf("naive sums by id, by id descending and by x: %s, %s, %s", fmtF(byID), fmtF(naiveSum(desc)), fmtF(naiveSum(byX)))
	others := map[int]float64{}
	for g := range 3 {
		if g != 1 {
			others[g], _ = strconv.ParseFloat(xsStats(h.floatColumn(fmt.Sprintf("SELECT x FROM it_xs_asc WHERE g = %d", g)))[0], 64)
		}
	}
	forModes(t, hs, func(_ string, mh *sqlHarness) {
		for table := range orders {
			for range 3 { // a cluster's shards answer in a different order each time
				mh.expectRows("SELECT sum(x) FROM "+table+" WHERE g = 1", want[0])
				mh.expectRows("SELECT sum(x), avg(x), stddev(x), stddev_pop(x), var_samp(x), var_pop(x) FROM "+table+" WHERE g = 1",
					strings.Join(want, "|"))
			}
			mh.expectRows("SELECT g, sum(x) FROM "+table+" GROUP BY g ORDER BY g",
				"0|"+fmtF(others[0]), "1|"+want[0], "2|"+fmtF(others[2]))
			// round() of the sum, as in the issue's real-data case.
			mh.expectRows("SELECT round(sum(x), 1), round(avg(x), 2) FROM "+table+" WHERE g = 1", "502250|10.05")
		}
	})
}

// setupExactSums creates tables of doubles whose naive sums are wrong:
//
//   - it_xs_tenths: 5,000 rows of 0.1 (g 0) and 5,000 of 0.7 (g 1). Their
//     naive sums are wrong in every order; the exact ones round to 500 and
//     3500.
//   - it_xs_cancel: ±1e16 with small values between them, inserted in an
//     order where adding left to right loses the small ones.
//   - it_xs_spread: values near 10⁹, for variances.
//   - it_xs_frac: 1/3 and 1/7, whose sum has 17 significant digits.
func (h *sqlHarness) setupExactSums() {
	h.t.Helper()
	for _, name := range []string{"it_xs_tenths", "it_xs_cancel", "it_xs_spread", "it_xs_frac"} {
		h.exec("DROP TABLE IF EXISTS " + name)
		h.exec("CREATE TABLE " + name + " (id INTEGER, g INTEGER, x DOUBLE PRECISION)")
		h.t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS " + name) })
	}
	h.exec(`INSERT INTO it_xs_tenths SELECT i, i % 2, CASE WHEN i % 2 = 0 THEN 0.1e0 ELSE 0.7e0 END
		FROM generate_series(1, 10000) AS s(i)`)
	h.exec(`INSERT INTO it_xs_cancel VALUES (1, 0, 1e16), (2, 0, 1), (3, 0, -1e16), (4, 0, 3), (5, 0, 1e16),
		(6, 0, 0.5), (7, 0, -1e16), (8, 1, 0.1), (9, 1, 1e20), (10, 1, 0.2), (11, 1, -1e20)`)
	h.exec(`INSERT INTO it_xs_spread SELECT i, i % 4, 1000000000 + ((i * 37) % 1000) / 10.0 + i % 7 * 0.003e0
		FROM generate_series(1, 20000) AS s(i)`)
	h.exec(`INSERT INTO it_xs_frac VALUES (1, 0, 1 / 3e0), (2, 0, 1 / 7e0)`)
}

// TestSQLExactSums checks SUM, AVG, the variances and standard deviations
// against the exact values, in every pushdown mode, as GROUP BY and as
// window functions.
func TestSQLExactSums(t *testing.T) {
	hs := pushdownHarnesses(t)
	h := hs["default"]
	h.setupExactSums()
	tenths := h.floatColumn("SELECT x FROM it_xs_tenths WHERE g = 0")
	if got := naiveSum(tenths); got == 500 {
		t.Fatalf("the naive sum of 5,000 × 0.1 is 500")
	}
	for _, table := range []string{"it_xs_tenths", "it_xs_cancel", "it_xs_spread", "it_xs_frac"} {
		all := xsStats(h.floatColumn("SELECT x FROM " + table))
		var groups []string
		for g := range 4 {
			fs := h.floatColumn(fmt.Sprintf("SELECT x FROM %s WHERE g = %d", table, g))
			if len(fs) > 1 {
				groups = append(groups, fmt.Sprintf("%d|%s", g, strings.Join(xsStats(fs), "|")))
			}
		}
		t.Run(table, func(t *testing.T) {
			forModes(t, hs, func(mode string, mh *sqlHarness) {
				stats := "SELECT sum(x), avg(x), stddev_samp(x), stddev_pop(x), var_samp(x), var_pop(x) FROM " + table
				mh.expectRows(stats, strings.Join(all, "|"))
				mh.expectRows("SELECT g, sum(x), avg(x), stddev_samp(x), stddev_pop(x), var_samp(x), var_pop(x) FROM "+
					table+" GROUP BY g HAVING count(*) > 1 ORDER BY g", groups...)
				mh.expectRows("SELECT sum(x), sum(x) FILTER (WHERE g >= 0), avg(DISTINCT x) IS NOT NULL FROM "+table,
					all[0]+"|"+all[0]+"|true")
				// SUM and AVG alone: `all` has RediSearch compute them (see below).
				if mode != PushdownAll {
					mh.expectRows("SELECT sum(x), avg(x) FROM "+table, all[0]+"|"+all[1])
				}
			})
		})
	}
	// Known values, to see the references mean what they say.
	h.expectRows("SELECT g, sum(x), avg(x), stddev(x) FROM it_xs_tenths GROUP BY g ORDER BY g", "0|500|0.1|0", "1|3500|0.7|0")
	h.expectRows("SELECT g, sum(x) FROM it_xs_cancel GROUP BY g ORDER BY g", "0|4.5", "1|0.30000000000000004")
	h.expectRows("SELECT sum(x), avg(x) FROM it_xs_frac", "0.47619047619047616|0.23809523809523808")
	h.expectRows("SELECT sum(x) FROM it_xs_tenths", "4000")
	// Integers and decimals: a running total that overflows, and integers
	// mixed into a decimal sum (they used to be left out: 2.5).
	h.expectRows(`SELECT sum(CASE WHEN x = 1 THEN 9223372036854775807 WHEN x = 2 THEN 1 ELSE -1 END),
			sum(CASE WHEN x > 1 THEN 1 ELSE 2.5 END), avg(CASE WHEN x > 1 THEN 1 ELSE 0.5e0 END)
		FROM generate_series(1, 3) AS g(x)`, "9223372036854775807|4.5|0.8333333333333334")
	h.expectError("SELECT sum(9223372036854775807) FROM generate_series(1, 2) AS g(x)", "integer overflow in SUM")

	// `all`: RediSearch sums the doubles itself, in its order, and returns
	// the sum with 12 significant digits. That is the exact sum when it has
	// at most 12 significant digits and RediSearch's rounding errors stay
	// below the 12th: here for it_xs_tenths, but not for it_xs_frac
	// (0.47619047619, not 0.47619047619047616) or for the cancellations,
	// which lose the small values (or not) depending on the order.
	ah := hs[PushdownAll]
	ah.expectRows("SELECT g, sum(x), avg(x) FROM it_xs_tenths GROUP BY g ORDER BY g", "0|500|0.1", "1|3500|0.7")
	ah.expectRows("SELECT sum(x), avg(x) FROM it_xs_frac", "0.47619047619|0.238095238095")
	ah.expectRows("SELECT sum(x), stddev(x) IS NOT NULL FROM it_xs_frac", "0.47619047619047616|true") // STDDEV runs in the driver
	got, _ := ah.query("SELECT sum(x) FROM it_xs_cancel WHERE g = 0")
	if len(got) != 1 {
		t.Fatalf("all: sum of it_xs_cancel: %q", got)
	}
	t.Logf("aggregate_pushdown all: SUM over it_xs_cancel group 0 is %s (exact: 4.5)", got[0])
}

// TestSQLExactWindowSums checks window aggregates over frames that slide
// (values leave them exactly), running frames, peers and EXCLUDE, against
// sums computed exactly here for each row.
func TestSQLExactWindowSums(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_xs_win")
	h.exec("CREATE TABLE it_xs_win (id INTEGER, g INTEGER, x DOUBLE PRECISION)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_xs_win") })
	// Every fifth value is ±1e15, so naive sums of frames around it lose the
	// small values' last digits.
	h.exec(`INSERT INTO it_xs_win SELECT i, i % 4,
			CASE WHEN i % 5 = 0 THEN (1 - 2 * (i / 5 % 2)) * 1e15 ELSE ((i * 7919) % 2000) / 100.0 + 0.05 END
		FROM generate_series(1, 2000) AS s(i)`)
	xs := h.floatColumn("SELECT x FROM it_xs_win ORDER BY id")
	gs := make([]int, len(xs))
	for i := range gs {
		gs[i] = (i + 1) % 4
	}
	// frameStats returns the exact SUM, AVG and STDDEV_SAMP of rows [lo, hi)
	// (positions by id) for which keep is true.
	frame := func(lo, hi int, keep func(j int) bool) (string, string, string) {
		var fs []float64
		for j := max(lo, 0); j < min(hi, len(xs)); j++ {
			if keep == nil || keep(j) {
				fs = append(fs, xs[j])
			}
		}
		s := xsStats(fs)
		return s[0], s[1], s[2]
	}
	check := func(sql string, row func(i int) string) {
		t.Helper()
		want := make([]string, len(xs))
		for i := range xs {
			want[i] = strconv.Itoa(i+1) + "|" + row(i)
		}
		h.expectRows(sql, want...)
	}
	// Sliding ROWS frames, of SUM, AVG and STDDEV.
	check(`SELECT id, sum(x) OVER w, avg(x) OVER w, stddev(x) OVER w FROM it_xs_win
		WINDOW w AS (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING) ORDER BY id`,
		func(i int) string {
			s, a, d := frame(i-3, i+3, nil)
			return s + "|" + a + "|" + d
		})
	// Running sums per partition, and the partition's total (its rows in any
	// order).
	part := map[int][]string{} // the stats of each partition
	upTo := map[int]string{}   // the sum of the rows with g up to a value
	for g := range 4 {
		var in, below []float64
		for j := range xs {
			if gs[j] == g {
				in = append(in, xs[j])
			}
			if gs[j] <= g {
				below = append(below, xs[j])
			}
		}
		part[g], upTo[g] = xsStats(in), xsStats(below)[0]
	}
	check(`SELECT id, sum(x) OVER (PARTITION BY g ORDER BY id), var_pop(x) OVER (PARTITION BY g),
			avg(x) OVER (PARTITION BY g) FROM it_xs_win ORDER BY id`,
		func(i int) string {
			s, _, _ := frame(0, i+1, func(j int) bool { return gs[j] == gs[i] })
			return s + "|" + part[gs[i]][5] + "|" + part[gs[i]][1]
		})
	// RANGE peers: every row with g up to the current row's.
	check(`SELECT id, sum(x) OVER (ORDER BY g) FROM it_xs_win ORDER BY id`,
		func(i int) string { return upTo[gs[i]] })
	// EXCLUDE: the frame has a hole.
	check(`SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE CURRENT ROW)
		FROM it_xs_win ORDER BY id`,
		func(i int) string {
			s, _, _ := frame(i-2, i+3, func(j int) bool { return j != i })
			return s
		})
	// A long sliding frame: values leave it by subtraction, exactly.
	check(`SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN 299 PRECEDING AND CURRENT ROW) FROM it_xs_win ORDER BY id`,
		func(i int) string {
			s, _, _ := frame(i-299, i+1, nil)
			return s
		})
	// The naive sums of these frames would be wrong: a check of the data.
	wrong := 0
	for i := range xs {
		lo := max(i-3, 0)
		hi := min(i+3, len(xs))
		s, _, _ := frame(lo, hi, nil)
		if fmtF(naiveSum(xs[lo:hi])) != s {
			wrong++
		}
	}
	if wrong < 100 {
		t.Errorf("only %d of the sliding frames have a wrong naive sum", wrong)
	}
}

// TestSQLPushdownSumNulls is issue #101: on a cluster, a shard whose rows of
// a group all lack the value sent NaN as its partial REDUCE SUM, so pushed
// down SUM and AVG of a nullable column were NaN for the group (DOUBLE with
// aggregate_pushdown all, and AVG of an INTEGER in the default mode too).
func TestSQLPushdownSumNulls(t *testing.T) {
	hs := pushdownHarnesses(t)
	h := hs["default"]
	h.exec("DROP TABLE IF EXISTS it_xs_nulls")
	h.exec("CREATE TABLE it_xs_nulls (g INTEGER, x DOUBLE PRECISION, i INTEGER)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_xs_nulls") })
	// The issue's rows: each of groups 0-19 has 2 values and 13 NULLs, so on
	// a cluster some shards hold only NULLs of a group. Group 20 is all NULLs;
	// group 21's +Infinity and -Infinity add up to a real NaN.
	h.exec(`INSERT INTO it_xs_nulls SELECT s % 20, CASE WHEN s < 40 THEN 1.0 + (s % 2) ELSE NULL END,
			CASE WHEN s < 40 THEN 1 + s % 2 ELSE NULL END
		FROM generate_series(0, 299) AS q(s)`)
	h.exec(`INSERT INTO it_xs_nulls VALUES (20, NULL, NULL), (20, NULL, NULL), (20, NULL, NULL),
		(21, 'Infinity', 1), (21, '-Infinity', NULL), (21, NULL, 2)`)
	var want, wantAvg []string
	for g := range 20 {
		v := 1 + g%2
		want = append(want, fmt.Sprintf("%d|%d|%d|2|%d|%d", g, 2*v, v, 2*v, v))
		wantAvg = append(wantAvg, fmt.Sprintf("%d|%d", g, v))
	}
	want = append(want, "20|NULL|NULL|0|NULL|NULL", "21|NaN|NaN|2|3|1.5")
	wantAvg = append(wantAvg, "20|NULL", "21|1.5")
	byGroup := "SELECT g, sum(x), avg(x), count(x), sum(i), avg(i) FROM it_xs_nulls GROUP BY g ORDER BY g"
	avgInt := "SELECT g, avg(i) FROM it_xs_nulls GROUP BY g ORDER BY g"
	if _, _, indexAgg := h.planOf(avgInt); !indexAgg {
		t.Fatalf("%s should run in the index", avgInt)
	}
	forModes(t, hs, func(_ string, mh *sqlHarness) {
		for range 3 {
			mh.expectRows(byGroup, want...)
			mh.expectRows(avgInt, wantAvg...)
		}
		mh.expectRows("SELECT sum(x), avg(x), count(x), sum(i), avg(i) FROM it_xs_nulls WHERE g < 20", "60|1.5|40|60|1.5")
		mh.expectRows("SELECT sum(x), avg(x), count(x), avg(i) FROM it_xs_nulls WHERE g = 20", "NULL|NULL|0|NULL")
		mh.expectRows("SELECT g, sum(x), avg(x), min(x), max(i) FROM it_xs_nulls WHERE g >= 19 GROUP BY g ORDER BY g",
			"19|4|2|2|2", "20|NULL|NULL|NULL|NULL", "21|NaN|NaN|-Inf|2")
	})
}

// TestSQLHaversineDistance is dbt_utils' haversine_distance, as its macros
// write it (issue #89): the default one, which uses radians(), and the
// BigQuery one, which uses acos(-1). Paris to Amsterdam is 267 miles, 430
// km (dbt_utils' own test data). The expected doubles are computed with the
// same operations, in Go.
func TestSQLHaversineDistance(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_xs_geo")
	h.exec("CREATE TABLE it_xs_geo (id INTEGER, lat_1 DOUBLE PRECISION, lon_1 DOUBLE PRECISION, lat_2 DOUBLE PRECISION, lon_2 DOUBLE PRECISION)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_xs_geo") })
	h.exec("INSERT INTO it_xs_geo VALUES (1, 48.864716, 2.349014, 52.379189, 4.899431), (2, 0, 0, 0, 0), (3, NULL, 1, 2, 3)")
	lat1, lon1, lat2, lon2 := 48.864716, 2.349014, 52.379189, 4.899431
	rad := func(d float64) float64 { return d * radiansPerDegree }
	// The products are rounded before they are added, as the driver
	// evaluates each operator on its own.
	q1 := math.Pow(math.Sin(rad((lat2-lat1)/2)), 2)
	q2 := math.Pow(math.Sin(rad((lon2-lon1)/2)), 2)
	miles := 7922 * math.Asin(math.Sqrt(q1+float64(float64(math.Cos(rad(lat1))*math.Cos(rad(lat2)))*q2)))
	km := float64(miles * 1.60934)
	// BigQuery's form: radians as acos(-1) * degrees / 180.
	brad := func(d float64) float64 { return float64(math.Acos(-1)*d) / 180 }
	b1 := math.Pow(math.Sin((brad(lat2)-brad(lat1))/2), 2)
	b2 := math.Pow(math.Sin((brad(lon2)-brad(lon1))/2), 2)
	bmiles := 7922 * math.Asin(math.Sqrt(b1+float64(float64(math.Cos(brad(lat1))*math.Cos(brad(lat2)))*b2)))
	bkm := float64(bmiles * 1.60934)
	if math.Round(miles) != 267 || math.Round(km) != 430 || math.Round(bkm) != 430 {
		t.Fatalf("expected distances %v mi, %v km, %v km", miles, km, bkm)
	}

	// default__haversine_distance(lat_1, lon_1, lat_2, lon_2, unit)
	def := func(rate string) string {
		return `2 * 3961 * asin(sqrt(power((sin(radians((lat_2 - lat_1) / 2))), 2) +
    cos(radians(lat_1)) * cos(radians(lat_2)) *
    power((sin(radians((lon_2 - lon_1) / 2))), 2))) * ` + rate
	}
	h.expectRows("SELECT id, "+def("1")+" AS mi, "+def("1.60934")+" AS km FROM it_xs_geo ORDER BY id",
		"1|"+fmtF(miles)+"|"+fmtF(km), "2|0|0", "3|NULL|NULL")
	h.expectRows("SELECT round(cast("+def("1")+" AS numeric), 0), round(cast("+def("1.60934")+" AS numeric), 0) FROM it_xs_geo WHERE id = 1",
		"267|430")
	// bigquery__haversine_distance, with dbt_utils.degrees_to_radians.
	r := func(c string) string { return "acos(-1) * " + c + " / 180" }
	bq := `2 * 3961 * asin(sqrt(power(sin((` + r("lat_2") + ` - ` + r("lat_1") + `) / 2), 2) +
    cos(` + r("lat_1") + `) * cos(` + r("lat_2") + `) *
    power(sin((` + r("lon_2") + ` - ` + r("lon_1") + `) / 2), 2))) * 1.60934`
	h.expectRows("SELECT "+bq+" FROM it_xs_geo WHERE id = 1", fmtF(bkm))
	// As a model would use it: an aggregate over the trips.
	h.expectRows("SELECT count(*), round(cast(sum("+def("1.60934")+") AS numeric), 3) FROM it_xs_geo",
		"3|"+strconv.FormatFloat(km, 'f', 3, 64))
}
