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

// Integration tests for the aggregate functions of aggfuncs.go, FILTER,
// IGNORE NULLS, frame EXCLUDE and RANGE offsets on TIME keys. Expected
// statistics were computed with exact rational arithmetic and correctly
// rounded to doubles; percentiles follow PostgreSQL's formulas.

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// setupAggregates creates the aggregate data set:
//
//	id g k    v    n      f    b
//	1  a x    1    1.50   0.5  true
//	2  a y    2    2.25   1.25 false
//	3  a NULL 4    NULL   2    true
//	4  a x    NULL 3.00   NULL NULL
//	5  b z    10   10.00  4    false
//	6  b z    10   -1.00  8    false
//	7  c NULL NULL NULL   NULL NULL
func (h *sqlHarness) setupAggregates() {
	h.t.Helper()
	h.exec("DROP TABLE IF EXISTS it_agg")
	h.exec(`CREATE TABLE it_agg (id INTEGER NOT NULL, g VARCHAR, k VARCHAR, v INTEGER, n NUMERIC(6,2),
		f DOUBLE PRECISION, b BOOLEAN)`)
	h.exec(`INSERT INTO it_agg VALUES
		(1, 'a', 'x', 1, 1.50, 0.5, true),
		(2, 'a', 'y', 2, 2.25, 1.25, false),
		(3, 'a', NULL, 4, NULL, 2.0, true),
		(4, 'a', 'x', NULL, 3.00, NULL, NULL),
		(5, 'b', 'z', 10, 10.00, 4.0, false),
		(6, 'b', 'z', 10, -1.00, 8.0, false),
		(7, 'c', NULL, NULL, NULL, NULL, NULL)`)
	h.t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_agg") })
}

// expectRowsNear checks a one-row result of doubles, each within 1e-12 of
// the value wanted (relative).
func (h *sqlHarness) expectRowsNear(sql string, want ...float64) {
	h.t.Helper()
	rows, _ := h.query(sql)
	if len(rows) != 1 {
		h.t.Fatalf("%s\n got: %q, want one row", sql, rows)
	}
	got := strings.Split(rows[0], "|")
	if len(got) != len(want) {
		h.t.Fatalf("%s\n got: %q, want %v", sql, rows[0], want)
	}
	for i, s := range got {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || math.Abs(f-want[i]) > 1e-12*math.Abs(want[i]) {
			h.t.Errorf("%s\n got: %q, want %v", sql, rows[0], want)
			return
		}
	}
}

// expectTypeIDs checks the Arrow type IDs of a result's columns.
func expectTypeIDs(t *testing.T, schema *arrow.Schema, want ...arrow.Type) {
	t.Helper()
	for i, w := range want {
		if got := schema.Field(i).Type.ID(); got != w {
			t.Errorf("column %d (%s) type = %s, want %s", i, schema.Field(i).Name, got, w)
		}
	}
}

func TestSQLAggStringAgg(t *testing.T) {
	h := newSQLHarness(t)
	h.setupAggregates()

	// NULLs are skipped; ORDER BY inside the call, or WITHIN GROUP.
	schema := h.expectRows(`SELECT STRING_AGG(k, ',' ORDER BY id), STRING_AGG(k, ',' ORDER BY id DESC),
			LISTAGG(k, ';') WITHIN GROUP (ORDER BY id), LISTAGG(k) WITHIN GROUP (ORDER BY k DESC, id),
			STRING_AGG(k, ',') WITHIN GROUP (ORDER BY id DESC)
		FROM it_agg`,
		"x,y,x,z,z|z,z,x,y,x|x;y;x;z;z|zzyxx|z,z,x,y,x")
	expectTypeIDs(t, schema, arrow.STRING, arrow.STRING, arrow.STRING, arrow.STRING, arrow.STRING)

	// Each value but the first is preceded by its own row's separator, as in
	// PostgreSQL; a NULL separator adds nothing. Values of any type are
	// rendered as text.
	h.expectRows(`SELECT STRING_AGG(k, g ORDER BY id), STRING_AGG(k, NULL ORDER BY id),
			STRING_AGG(v, '-' ORDER BY v DESC), STRING_AGG(n, ';' ORDER BY n), STRING_AGG(b, ' ' ORDER BY id)
		FROM it_agg`,
		"xayaxbzbz|xyxzz|10-10-4-2-1|-1.00;1.50;2.25;3.00;10.00|true false true false false")

	// DISTINCT sorts the values (by the ORDER BY keys, then the arguments),
	// as PostgreSQL does; numbers sort as numbers.
	h.expectRows(`SELECT STRING_AGG(DISTINCT k, ','), LISTAGG(DISTINCT k, ',') WITHIN GROUP (ORDER BY k DESC),
			STRING_AGG(DISTINCT v, '+')
		FROM it_agg`,
		"x,y,z|z,y,x|1+2+4+10")

	// Per group; a group with only NULLs, and no rows at all, give NULL.
	h.expectRows(`SELECT g, STRING_AGG(k, ',' ORDER BY id), LISTAGG(k, '') WITHIN GROUP (ORDER BY id DESC)
		FROM it_agg GROUP BY g ORDER BY g`,
		"a|x,y,x|xyx", "b|z,z|zz", "c|NULL|NULL")
	h.expectRows(`SELECT STRING_AGG(k, ','), LISTAGG(k) FROM it_agg WHERE id > 100`, "NULL|NULL")

	// As window functions: a running list and a sliding frame.
	h.expectRows(`SELECT id, STRING_AGG(k, ',') OVER (PARTITION BY g ORDER BY id),
			STRING_AGG(k, '-') OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
			LISTAGG(k) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)
		FROM it_agg ORDER BY id`,
		"1|x|x-y|xy", "2|x,y|x-y|yx", "3|x,y|y-x|xz", "4|x,y,x|x-z|xzz",
		"5|z|x-z-z|zz", "6|z,z|z-z|z", "7|NULL|z|NULL")

	// Errors.
	h.expectError(`SELECT STRING_AGG(k) FROM it_agg`, "STRING_AGG expects 2 arguments")
	h.expectError(`SELECT LISTAGG(k, ',', ';') FROM it_agg`, "LISTAGG expects 1 or 2 arguments")
	h.expectError(`SELECT STRING_AGG(DISTINCT k, ',' ORDER BY id) FROM it_agg`,
		"in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list")
	h.expectError(`SELECT STRING_AGG(k, ',' ORDER BY id) WITHIN GROUP (ORDER BY id) FROM it_agg`,
		"cannot use multiple ORDER BY clauses with WITHIN GROUP")
	h.expectError(`SELECT STRING_AGG(k, ',' ORDER BY id) OVER () FROM it_agg`,
		"aggregate ORDER BY is not implemented for window functions")
	h.expectError(`SELECT UPPER(k ORDER BY id) FROM it_agg`, "ORDER BY specified, but UPPER is not an aggregate function")
}

func TestSQLAggBoolAnyValueCountDistinct(t *testing.T) {
	h := newSQLHarness(t)
	h.setupAggregates()

	// BOOL_OR / BOOL_AND / EVERY skip NULLs, and are NULL with no values.
	schema := h.expectRows(`SELECT BOOL_OR(b), BOOL_AND(b), EVERY(b), BOOL_OR(v > 4), BOOL_AND(v > 1), BOOL_AND(v > 0)
		FROM it_agg`,
		"true|false|false|true|false|true")
	expectTypeIDs(t, schema, arrow.BOOL, arrow.BOOL, arrow.BOOL, arrow.BOOL, arrow.BOOL, arrow.BOOL)
	h.expectRows(`SELECT g, BOOL_OR(v > 4), BOOL_AND(v > 0), BOOL_OR(k = 'y') FROM it_agg GROUP BY g ORDER BY g`,
		"a|false|true|true", "b|true|true|false", "c|NULL|NULL|NULL")

	// Over a boolean column they run in the index, as MAX / MIN of 0 and 1.
	q := `SELECT g, BOOL_OR(b), BOOL_AND(b), EVERY(b) FROM it_agg GROUP BY g ORDER BY g`
	if _, _, indexAgg := h.planOf(q); !indexAgg {
		t.Errorf("%s: not aggregated in the index", q)
	}
	h.expectRows(q, "a|true|false|false", "b|false|false|false", "c|NULL|NULL|NULL")
	q = `SELECT BOOL_OR(v > 4) FROM it_agg`
	if _, _, indexAgg := h.planOf(q); indexAgg {
		t.Errorf("%s: aggregated in the index", q)
	}

	// ANY_VALUE returns a non-NULL value when there is one.
	schema = h.expectRows(`SELECT ANY_VALUE(k), ANY_VALUE(v), ANY_VALUE(n) FROM it_agg WHERE id IN (3, 4)`, "x|4|3.00")
	expectTypeIDs(t, schema, arrow.STRING, arrow.INT32, arrow.DECIMAL128)
	h.expectRows(`SELECT g, ANY_VALUE(k) FROM it_agg WHERE g <> 'a' GROUP BY g ORDER BY g`, "b|z", "c|NULL")

	// COUNT(DISTINCT a, b) counts the distinct rows where no argument is NULL.
	h.expectRows(`SELECT COUNT(DISTINCT k, v), COUNT(DISTINCT g, k), COUNT(DISTINCT k), COUNT(DISTINCT v) FROM it_agg`,
		"3|3|3|4")
	h.expectRows(`SELECT g, COUNT(DISTINCT k, v) FROM it_agg GROUP BY g ORDER BY g`, "a|2", "b|1", "c|0")

	// As window functions over sliding frames (b by id: t f t NULL f f NULL).
	h.expectRows(`SELECT id, BOOL_OR(b) OVER w, BOOL_AND(b) OVER w, EVERY(v > 1) OVER w
		FROM it_agg WINDOW w AS (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY id`,
		"1|true|true|false", "2|true|false|false", "3|true|false|true", "4|true|true|true",
		"5|false|false|true", "6|false|false|true", "7|false|false|true")
	h.expectRows(`SELECT id, ANY_VALUE(k) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM it_agg ORDER BY id`,
		"1|x", "2|y", "3|x", "4|x", "5|z", "6|z", "7|NULL")

	// Errors.
	h.expectError(`SELECT BOOL_OR(v) FROM it_agg`, "function bool_or(integer) does not exist")
	h.expectError(`SELECT EVERY(k) FROM it_agg GROUP BY g`, "function every(varchar) does not exist")
	h.expectError(`SELECT COUNT(k, v) FROM it_agg`, "COUNT of more than one argument requires DISTINCT")
	h.expectError(`SELECT COUNT(DISTINCT k, v) OVER () FROM it_agg`, "DISTINCT is not supported")
	h.expectError(`SELECT ANY_VALUE(k, v) FROM it_agg`, "ANY_VALUE expects 1 argument")
	h.expectError(`SELECT SUM(SUM(v)) FROM it_agg`, "aggregate function calls cannot be nested")
}

func TestSQLAggStatistics(t *testing.T) {
	h := newSQLHarness(t)
	h.setupAggregates()
	h.exec("DROP TABLE IF EXISTS it_agg_big")
	h.exec("CREATE TABLE it_agg_big (x BIGINT, d DOUBLE PRECISION)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_agg_big") })

	// v = 1 2 4 10 10: mean 5.4, Σ(x - mean)² = 75.2, so the sample variance
	// is 18.8 and the population variance 15.04.
	schema := h.expectRows(`SELECT VAR_SAMP(v), VARIANCE(v), VAR_POP(v), STDDEV_SAMP(v), STDDEV(v), STDDEV_POP(v) FROM it_agg`,
		"18.8|18.8|15.04|4.33589667773576|4.33589667773576|3.878143885933063")
	expectTypeIDs(t, schema, arrow.FLOAT64, arrow.FLOAT64, arrow.FLOAT64, arrow.FLOAT64, arrow.FLOAT64, arrow.FLOAT64)
	// Decimals and doubles are summed exactly too.
	h.expectRows(`SELECT VAR_SAMP(n), VAR_POP(n), STDDEV_SAMP(n), STDDEV_POP(n) FROM it_agg`,
		"16.925|13.54|4.1140004861448425|3.6796738985948196")
	// Welford's last bits depend on the order the rows come in, which on a
	// cluster depends on the shards their keys hash to (and so on the
	// table's key prefix, new each time the table is created).
	h.expectRowsNear(`SELECT VAR_SAMP(f), VAR_POP(f), STDDEV_SAMP(f), STDDEV_POP(f) FROM it_agg`,
		9.05, 7.24, 3.0083217912982647, 2.6907248094147422)

	// Per group: the sample statistics of one value are NULL, the population
	// ones 0; no values give NULL.
	h.expectRows(`SELECT g, VAR_SAMP(v), VAR_POP(v), STDDEV(v), STDDEV_POP(v), VAR_SAMP(n), STDDEV_SAMP(f)
		FROM it_agg GROUP BY g ORDER BY g`,
		"a|2.3333333333333335|1.5555555555555556|1.5275252316519468|1.247219128924647|0.5625|0.75",
		"b|0|0|0|0|60.5|2.8284271247461903",
		"c|NULL|NULL|NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT VAR_SAMP(v), VAR_POP(v), STDDEV_SAMP(v), STDDEV_POP(f) FROM it_agg WHERE id = 1`, "NULL|0|NULL|0")
	h.expectRows(`SELECT VAR_SAMP(v), STDDEV_POP(v) FROM it_agg WHERE id > 100`, "NULL|NULL")
	h.expectRows(`SELECT VAR_SAMP(DISTINCT v), VAR_POP(DISTINCT v) FROM it_agg`, "16.25|12.1875")

	// Numerical stability: values far from zero relative to their spread.
	// The integers are exact even beyond 2^53, where doubles can't hold them.
	h.exec(`INSERT INTO it_agg_big VALUES (1000000000000001, 1000000000000001), (1000000000000002, 1000000000000002),
		(1000000000000003, 1000000000000003)`)
	h.expectRows(`SELECT VAR_SAMP(x), STDDEV_POP(x), VAR_SAMP(d), VAR_POP(d) FROM it_agg_big`,
		"1|0.816496580927726|1|0.6666666666666666")
	h.exec("DELETE FROM it_agg_big")
	h.exec(`INSERT INTO it_agg_big (x) VALUES (9223372036854775805), (9223372036854775806), (9223372036854775807)`)
	h.expectRows(`SELECT VAR_SAMP(x), STDDEV_SAMP(x), VAR_POP(x) FROM it_agg_big`, "1|1|0.6666666666666666")

	// As window functions; values leave the sliding frames exactly.
	h.expectRows(`SELECT id, VAR_SAMP(v) OVER w, STDDEV_POP(v) OVER w, VAR_SAMP(f) OVER w, STDDEV_POP(f) OVER w,
			VAR_POP(n) OVER (PARTITION BY g)
		FROM it_agg WINDOW w AS (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY id`,
		"1|0.5|0.5|0.28125|0.375|0.375",
		"2|2.3333333333333335|1.247219128924647|0.5625|0.6123724356957945|0.375",
		"3|2|1|0.28125|0.375|0.375",
		"4|18|3|2|1|0.375",
		"5|0|0|8|2|30.25",
		"6|0|0|8|2|30.25",
		"7|NULL|0|NULL|0|NULL")

	// Errors.
	h.expectError(`SELECT STDDEV(k) FROM it_agg`, "function stddev(varchar) does not exist")
	h.expectError(`SELECT VAR_POP(b) FROM it_agg`, "function var_pop(boolean) does not exist")
	h.expectError(`SELECT VARIANCE(*) FROM it_agg`, "VARIANCE does not accept *")
}

func TestSQLAggOrderedSet(t *testing.T) {
	h := newSQLHarness(t)
	h.setupAggregates()

	// v sorted: 1 2 4 10 10. PERCENTILE_CONT interpolates at position
	// fraction × (n - 1); PERCENTILE_DISC takes the first value whose
	// position reaches fraction × n. DESC reverses the order.
	schema := h.expectRows(`SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v), PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY v),
			PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY v DESC), PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY v),
			PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY v), PERCENTILE_DISC(0.3) WITHIN GROUP (ORDER BY v),
			PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY v DESC), PERCENTILE_DISC(0) WITHIN GROUP (ORDER BY v), MEDIAN(v)
		FROM it_agg`,
		"4|2.4|8.8|1|10|2|1|1|4")
	expectTypeIDs(t, schema, arrow.FLOAT64, arrow.FLOAT64, arrow.FLOAT64, arrow.FLOAT64, arrow.FLOAT64,
		arrow.INT32, arrow.INT32, arrow.INT32, arrow.FLOAT64)

	// Decimals, doubles and strings; PERCENTILE_DISC and MODE keep the type.
	schema = h.expectRows(`SELECT MEDIAN(n), PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY f),
			PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY k), PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY n),
			MODE() WITHIN GROUP (ORDER BY k), MODE() WITHIN GROUP (ORDER BY k DESC), MODE() WITHIN GROUP (ORDER BY v)
		FROM it_agg`,
		"2.25|1.4|y|2.25|x|z|10")
	expectTypeIDs(t, schema, arrow.FLOAT64, arrow.FLOAT64, arrow.STRING, arrow.DECIMAL128, arrow.STRING, arrow.STRING, arrow.INT32)

	// Per group; an even count interpolates between the middle values.
	h.expectRows(`SELECT g, MEDIAN(v), MEDIAN(n), PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f), MODE() WITHIN GROUP (ORDER BY k)
		FROM it_agg GROUP BY g ORDER BY g`,
		"a|2|2.25|1.625|x", "b|10|4.5|7|z", "c|NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v), MODE() WITHIN GROUP (ORDER BY v), MEDIAN(v)
		FROM it_agg WHERE id > 100`, "NULL|NULL|NULL")
	h.expectRows(`SELECT PERCENTILE_CONT(NULL) WITHIN GROUP (ORDER BY v) FROM it_agg`, "NULL")

	// Intervals interpolate as intervals; over a CTE (aggregated in memory).
	h.expectRows(`WITH x(i) AS (SELECT INTERVAL '1 day' UNION ALL SELECT INTERVAL '3 days 12 hours' UNION ALL SELECT INTERVAL '2 hours')
		SELECT CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY i) AS VARCHAR),
			CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY i) AS VARCHAR),
			CAST(PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY i DESC) AS VARCHAR)
		FROM x`,
		"1 day|2 days 06:00:00|1 day")

	// Errors, with PostgreSQL's messages. The fraction is checked even when
	// there are no rows, as in PostgreSQL.
	h.expectError(`SELECT PERCENTILE_CONT(1.5) WITHIN GROUP (ORDER BY v) FROM it_agg`, "percentile value 1.5 is not between 0 and 1")
	h.expectError(`SELECT PERCENTILE_DISC(-0.5) WITHIN GROUP (ORDER BY v) FROM it_agg WHERE id > 100`,
		"percentile value -0.5 is not between 0 and 1")
	h.expectError(`SELECT PERCENTILE_CONT(0.5) FROM it_agg`, "WITHIN GROUP is required for ordered-set aggregate PERCENTILE_CONT")
	h.expectError(`SELECT MODE() FROM it_agg`, "WITHIN GROUP is required for ordered-set aggregate MODE")
	h.expectError(`SELECT SUM(v) WITHIN GROUP (ORDER BY v) FROM it_agg`, "SUM is not an ordered-set aggregate, so it cannot have WITHIN GROUP")
	h.expectError(`SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) OVER () FROM it_agg`,
		"OVER is not supported for ordered-set aggregate PERCENTILE_CONT")
	h.expectError(`SELECT MEDIAN(v) OVER () FROM it_agg`, "OVER is not supported for ordered-set aggregate MEDIAN")
	h.expectError(`SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY k) FROM it_agg`, "function percentile_cont(varchar) does not exist")
	h.expectError(`SELECT MEDIAN(d) FROM (SELECT DATE '2024-01-01' AS d) s`, "function median(date) does not exist")
	h.expectError(`SELECT PERCENTILE_DISC('x') WITHIN GROUP (ORDER BY v) FROM it_agg`, "the fraction of PERCENTILE_DISC must be a number")
	h.expectError(`SELECT MODE(v) WITHIN GROUP (ORDER BY v) FROM it_agg`, "MODE expects no arguments")
	h.expectError(`SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v, id) FROM it_agg`, "expects one ORDER BY expression")
	h.expectError(`SELECT PERCENTILE_CONT(DISTINCT 0.5) WITHIN GROUP (ORDER BY v) FROM it_agg`, "cannot use DISTINCT with WITHIN GROUP")
	h.expectError(`SELECT MEDIAN(DISTINCT v) FROM it_agg`, "DISTINCT is not supported for MEDIAN")
}

func TestSQLAggFilter(t *testing.T) {
	h := newSQLHarness(t)
	h.setupAggregates()
	h.exec("DROP VIEW IF EXISTS it_agg_v")
	h.exec("DROP TABLE IF EXISTS it_agg_ctas")
	t.Cleanup(func() {
		h.exec("DROP VIEW IF EXISTS it_agg_v")
		h.exec("DROP TABLE IF EXISTS it_agg_ctas")
	})

	// Only the rows where the condition is true (not NULL) are aggregated.
	schema := h.expectRows(`SELECT COUNT(*) FILTER (WHERE v > 2), COUNT(*) FILTER (WHERE b), SUM(v) FILTER (WHERE g = 'a'),
			SUM(v) FILTER (WHERE NOT b), AVG(n) FILTER (WHERE v < 5), MAX(k) FILTER (WHERE id < 5), COUNT(*)
		FROM it_agg`,
		"3|2|7|22|1.875|y|7")
	expectTypeIDs(t, schema, arrow.INT64, arrow.INT64, arrow.INT64, arrow.INT64, arrow.FLOAT64, arrow.STRING, arrow.INT64)
	h.expectRows(`SELECT COUNT(*) FILTER (WHERE false), SUM(v) FILTER (WHERE false), STRING_AGG(k, ',') FILTER (WHERE false)
		FROM it_agg`, "0|NULL|NULL")

	// FILTER works with every aggregate.
	h.expectRows(`SELECT STRING_AGG(k, ',' ORDER BY id) FILTER (WHERE id > 1),
			PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE g = 'a'), STDDEV_POP(v) FILTER (WHERE g = 'b'),
			BOOL_AND(b) FILTER (WHERE v < 3), COUNT(DISTINCT k) FILTER (WHERE id <> 2), MEDIAN(f) FILTER (WHERE b),
			ANY_VALUE(k) FILTER (WHERE id = 6)
		FROM it_agg`,
		"y,x,z,z|2|0|false|2|1.25|z")

	// The pivot idiom, per group.
	h.expectRows(`SELECT g, COUNT(*) FILTER (WHERE b) AS yes, COUNT(*) FILTER (WHERE NOT b) AS no,
			SUM(v) FILTER (WHERE b), COUNT(v) FILTER (WHERE k IS NOT NULL)
		FROM it_agg GROUP BY g ORDER BY g`,
		"a|2|1|5|2", "b|0|2|NULL|2", "c|0|0|NULL|0")
	// The arguments of rejected rows are not evaluated (100 / 0 for v = 1).
	h.expectRows(`SELECT SUM(100 / (v - 1)) FILTER (WHERE v > 1) FROM it_agg`, "155")
	h.expectError(`SELECT SUM(100 / (v - 1)) FROM it_agg`, "division by zero")
	// FILTER in HAVING and ORDER BY.
	h.expectRows(`SELECT g FROM it_agg GROUP BY g HAVING COUNT(*) FILTER (WHERE v IS NOT NULL) > 1
		ORDER BY SUM(v) FILTER (WHERE v > 1) DESC`, "b", "a")

	// A FILTER is evaluated by the driver, never pushed down.
	for _, q := range []string{
		`SELECT COUNT(*) FILTER (WHERE v > 2) FROM it_agg`,
		`SELECT g, COUNT(*), SUM(v) FILTER (WHERE v > 2) FROM it_agg GROUP BY g`,
	} {
		if _, _, indexAgg := h.planOf(q); indexAgg {
			t.Errorf("%s: aggregated in the index", q)
		}
	}
	// SUM(id), not SUM(v): on a cluster a shard whose rows of a group all
	// lack v reports its SUM as nan, and the driver then aggregates itself.
	// Which shard has which rows depends on the table's key prefix.
	if _, _, indexAgg := h.planOf(`SELECT g, COUNT(*), SUM(id) FROM it_agg GROUP BY g`); !indexAgg {
		t.Errorf("COUNT / SUM without FILTER: not aggregated in the index")
	}

	// As window functions; a filtered COUNT(*) counts only the rows kept.
	h.expectRows(`SELECT id, COUNT(*) FILTER (WHERE v > 2) OVER (), SUM(v) FILTER (WHERE b) OVER (ORDER BY id),
			COUNT(*) FILTER (WHERE b) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
			STRING_AGG(k, ',') FILTER (WHERE id <> 2) OVER (PARTITION BY g ORDER BY id),
			SUM(100 / (v - 1)) FILTER (WHERE v > 1) OVER ()
		FROM it_agg ORDER BY id`,
		"1|3|1|1|x|155", "2|3|1|1|x|155", "3|3|5|1|x|155", "4|3|5|1|x,x|155",
		"5|3|5|0|z|155", "6|3|5|0|z,z|155", "7|3|5|0|NULL|155")
	// Over a grouped query (one row per group: a 4, b 2, c 1).
	h.expectRows(`SELECT g, COUNT(*), SUM(COUNT(*)) FILTER (WHERE g <> 'b') OVER (ORDER BY g) FROM it_agg GROUP BY g ORDER BY g`,
		"a|4|4", "b|2|4", "c|1|5")

	// In a view and a CTAS: the SQL text round-trips, and the result types
	// are the aggregates' types.
	h.exec(`CREATE VIEW it_agg_v AS SELECT g, COUNT(*) FILTER (WHERE b) AS nb, STRING_AGG(k, ',' ORDER BY id) AS ks,
		PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) AS p FROM it_agg GROUP BY g`)
	h.expectRows(`SELECT * FROM it_agg_v ORDER BY g`, "a|2|x,y,x|2", "b|0|z,z|10", "c|0|NULL|NULL")
	h.exec(`CREATE TABLE it_agg_ctas AS SELECT g, STDDEV(v) AS sd, BOOL_OR(b) AS bo, STRING_AGG(k, ',' ORDER BY id) AS ks,
		MEDIAN(v) AS med, MODE() WITHIN GROUP (ORDER BY n) AS mo FROM it_agg GROUP BY g`)
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_agg_ctas'
		ORDER BY ordinal_position`,
		"g|VARCHAR", "sd|DOUBLE PRECISION", "bo|BOOLEAN", "ks|VARCHAR", "med|DOUBLE PRECISION", "mo|NUMERIC(6,2)")
	h.expectRows(`SELECT g, sd, bo, ks, med, mo FROM it_agg_ctas ORDER BY g`,
		"a|1.5275252316519468|true|x,y,x|2|1.50", "b|0|false|z,z|10|-1.00", "c|NULL|NULL|NULL|NULL|NULL")

	// FILTER is still a valid column alias.
	schema = h.expectRows(`SELECT COUNT(*) filter FROM it_agg`, "7")
	if name := schema.Field(0).Name; name != "filter" {
		t.Errorf("column name = %q, want filter", name)
	}

	// Errors.
	h.expectError(`SELECT COUNT(*) FILTER (WHERE v) FROM it_agg`, "argument of FILTER must be type boolean, not type INTEGER")
	h.expectError(`SELECT COUNT(*) FILTER (WHERE COUNT(*) > 1) FROM it_agg`, "aggregate functions are not allowed in FILTER")
	h.expectError(`SELECT COUNT(*) FILTER (WHERE ROW_NUMBER() OVER () > 1) FROM it_agg`, "window functions are not allowed in FILTER")
	h.expectError(`SELECT UPPER(k) FILTER (WHERE true) FROM it_agg`, "FILTER specified, but UPPER is not an aggregate function")
	h.expectError(`SELECT ROW_NUMBER() FILTER (WHERE true) OVER () FROM it_agg`,
		"FILTER is not implemented for non-aggregate window functions")
	h.expectError(`SELECT COUNT(*) FILTER (v > 1) FROM it_agg`, `expected WHERE`)
}

func TestSQLAggGroupingSets(t *testing.T) {
	h := newSQLHarness(t)
	h.setupAggregates()

	// With ROLLUP every set (here g, then the total) computes the
	// aggregates; a window FILTER may use aggregates and GROUPING() of the
	// combined groups (a 4 rows, b 2, c 1, total 7).
	h.expectRows(`SELECT g, COUNT(*) FILTER (WHERE b), STRING_AGG(k, ',' ORDER BY id), STDDEV_POP(v),
			SUM(COUNT(*)) FILTER (WHERE COUNT(*) > 1) OVER (ORDER BY g),
			SUM(COUNT(*)) FILTER (WHERE GROUPING(g) = 0) OVER (ORDER BY g)
		FROM it_agg GROUP BY ROLLUP (g) ORDER BY g`,
		"a|2|x,y,x|1.247219128924647|4|4",
		"b|0|z,z|0|6|6",
		"c|0|NULL|NULL|6|7",
		"NULL|2|x,y,x,z,z|3.878143885933063|13|7")
	// By g, by b (true: v 1 4; false: 2 10 10; NULL: id 4, no v), and in
	// all; the b = NULL group comes before the total, its set being first.
	h.expectRows(`SELECT g, b, BOOL_OR(v > 4), MEDIAN(v) FROM it_agg WHERE g <> 'c' GROUP BY GROUPING SETS ((g), (b), ())
		ORDER BY g, b`,
		"a|NULL|false|2", "b|NULL|true|10", "NULL|false|true|10", "NULL|true|false|2.5", "NULL|NULL|NULL|NULL", "NULL|NULL|true|4")
}

func TestSQLWindowIgnoreNulls(t *testing.T) {
	h := newSQLHarness(t)
	h.setupAggregates()

	// v by id: 1 2 4 NULL 10 10 NULL. IGNORE NULLS skips the rows whose value
	// is NULL: LAST_VALUE … IGNORE NULLS over the default frame is the usual
	// forward fill.
	h.expectRows(`SELECT id, LAST_VALUE(v) IGNORE NULLS OVER w, LAST_VALUE(v) OVER w, LAST_VALUE(v) RESPECT NULLS OVER w,
			LAG(v) IGNORE NULLS OVER w, LAG(v) RESPECT NULLS OVER w, LEAD(v) IGNORE NULLS OVER w,
			LAG(v, 2, 0) IGNORE NULLS OVER w, LEAD(v, -1) IGNORE NULLS OVER w, LAG(v, 0) IGNORE NULLS OVER w
		FROM it_agg WINDOW w AS (ORDER BY id) ORDER BY id`,
		"1|1|1|1|NULL|NULL|2|0|NULL|1",
		"2|2|2|2|1|1|4|0|1|2",
		"3|4|4|4|2|2|10|1|2|4",
		"4|4|NULL|NULL|4|4|10|2|4|NULL",
		"5|10|10|10|4|NULL|10|2|4|10",
		"6|10|10|10|10|10|NULL|4|10|10",
		"7|10|NULL|NULL|10|10|NULL|10|10|NULL")

	// FIRST_VALUE / NTH_VALUE, with IGNORE NULLS inside the parentheses
	// (BigQuery, DuckDB) or after them (SQL standard).
	h.expectRows(`SELECT id, FIRST_VALUE(v IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
			NTH_VALUE(v, 2) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
			NTH_VALUE(v, 4) IGNORE NULLS OVER (ORDER BY id), FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY g ORDER BY id DESC)
		FROM it_agg ORDER BY id`,
		"1|1|2|NULL|x", "2|2|2|NULL|x", "3|4|2|NULL|x", "4|10|2|NULL|x",
		"5|10|2|10|z", "6|10|2|10|z", "7|NULL|2|10|NULL")

	// Partitioned; forward fill of a string column.
	h.expectRows(`SELECT id, LAG(k) IGNORE NULLS OVER (PARTITION BY g ORDER BY id),
			COALESCE(k, LAST_VALUE(k) IGNORE NULLS OVER (PARTITION BY g ORDER BY id))
		FROM it_agg ORDER BY id`,
		"1|NULL|x", "2|x|y", "3|y|y", "4|y|x", "5|NULL|z", "6|z|z", "7|NULL|NULL")

	// Errors.
	h.expectError(`SELECT RANK() IGNORE NULLS OVER (ORDER BY id) FROM it_agg`, "function RANK does not allow RESPECT/IGNORE NULLS")
	h.expectError(`SELECT SUM(v) IGNORE NULLS OVER () FROM it_agg`, "aggregate functions do not accept RESPECT/IGNORE NULLS")
	h.expectError(`SELECT MAX(v) RESPECT NULLS FROM it_agg`, "aggregate functions do not accept RESPECT/IGNORE NULLS")
	h.expectError(`SELECT UPPER(k) IGNORE NULLS FROM it_agg`, "function UPPER does not allow RESPECT/IGNORE NULLS")
	h.expectError(`SELECT LAG(v) IGNORE NULLS FROM it_agg`, "requires an OVER clause")
	h.expectError(`SELECT LAG(v IGNORE NULLS) RESPECT NULLS OVER () FROM it_agg`, "specified more than once")
}

func TestSQLWindowFrameExclude(t *testing.T) {
	h := newSQLHarness(t)
	h.setupAggregates()

	// v by id: 1 2 4 NULL 10 10 NULL.
	h.expectRows(`SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW),
			MAX(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW),
			COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW),
			FIRST_VALUE(v) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING EXCLUDE CURRENT ROW),
			SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE NO OTHERS)
		FROM it_agg ORDER BY id`,
		"1|2|2|1|2|3", "2|5|4|2|4|7", "3|2|2|2|10|6", "4|14|10|2|10|14",
		"5|10|10|2|10|20", "6|10|10|2|NULL|20", "7|10|10|1|NULL|10")

	// Peers on g: {1, 2, 3, 4} {5, 6} {7}. The CTE keeps id order, so peers
	// stay in id order. EXCLUDE GROUP removes the current row's peers and
	// the row itself, EXCLUDE TIES only its peers.
	all := "w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
	h.expectRows(strings.ReplaceAll(`WITH s AS (SELECT * FROM it_agg ORDER BY id)
		SELECT id, COUNT(*) OVER (ALL EXCLUDE GROUP), COUNT(*) OVER (ALL EXCLUDE TIES), COUNT(*) OVER (ALL EXCLUDE CURRENT ROW),
			STRING_AGG(CAST(id AS VARCHAR), ',') OVER (ALL EXCLUDE TIES),
			STRING_AGG(CAST(id AS VARCHAR), ',') OVER (ALL EXCLUDE GROUP),
			FIRST_VALUE(id) OVER (ALL EXCLUDE GROUP), LAST_VALUE(id) OVER (ALL EXCLUDE GROUP),
			NTH_VALUE(id, 2) OVER (ALL EXCLUDE TIES), NTH_VALUE(id, 5) OVER (ALL EXCLUDE TIES)
		FROM s WINDOW w AS (ORDER BY g) ORDER BY id`, "ALL", all),
		"1|3|4|6|1,5,6,7|5,6,7|5|7|5|NULL",
		"2|3|4|6|2,5,6,7|5,6,7|5|7|5|NULL",
		"3|3|4|6|3,5,6,7|5,6,7|5|7|5|NULL",
		"4|3|4|6|4,5,6,7|5,6,7|5|7|5|NULL",
		"5|5|6|6|1,2,3,4,5,7|1,2,3,4,7|1|7|2|5",
		"6|5|6|6|1,2,3,4,6,7|1,2,3,4,7|1|7|2|6",
		"7|6|7|6|1,2,3,4,5,6,7|1,2,3,4,5,6|1|6|2|5")

	// With the default-like RANGE frame (up to the current row's last peer).
	h.expectRows(`SELECT id, SUM(v) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP),
			SUM(v) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES),
			AVG(f) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP),
			VAR_POP(v) OVER (ORDER BY g GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING EXCLUDE GROUP)
		FROM it_agg ORDER BY id`,
		"1|NULL|1|6|0", "2|NULL|2|6|0", "3|NULL|4|6|0", "4|NULL|NULL|6|0",
		"5|7|17|1.25|NULL", "6|7|17|1.25|NULL", "7|27|27|3.15|NULL")
}

func TestSQLWindowRangeTime(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_agg_t")
	h.exec("CREATE TABLE it_agg_t (id INTEGER, t TIME, v INTEGER)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_agg_t") })
	// Sorted by t: 00:15 (5), 08:00 (1), 08:30 (2, 7), 09:15 (3), 23:30 (4), NULL (6).
	h.exec(`INSERT INTO it_agg_t VALUES (1, TIME '08:00', 1), (2, TIME '08:30', 2), (3, TIME '09:15', 4),
		(4, TIME '23:30', 8), (5, TIME '00:15', 16), (6, NULL, 32), (7, TIME '08:30', 64)`)

	// The frames don't wrap around midnight (unlike time ± interval): 23:30
	// and 00:15 never see each other. As in PostgreSQL, an offset's days are
	// ignored, so '1 day 45 minutes' is 45 minutes. NULL keys are peers.
	h.expectRows(`SELECT id, SUM(v) OVER (ORDER BY t RANGE BETWEEN INTERVAL '45 minutes' PRECEDING AND CURRENT ROW),
			COUNT(*) OVER (ORDER BY t RANGE BETWEEN CURRENT ROW AND INTERVAL '1 hour' FOLLOWING),
			SUM(v) OVER (ORDER BY t RANGE BETWEEN INTERVAL '1 day 45 minutes' PRECEDING AND CURRENT ROW),
			SUM(v) OVER (ORDER BY t DESC RANGE BETWEEN INTERVAL '45 minutes' PRECEDING AND CURRENT ROW),
			COUNT(*) OVER (ORDER BY t RANGE BETWEEN INTERVAL '30 minutes' PRECEDING AND INTERVAL '30 minutes' FOLLOWING),
			SUM(v) OVER (ORDER BY t RANGE BETWEEN CURRENT ROW AND INTERVAL '100000 hours' FOLLOWING)
		FROM it_agg_t ORDER BY id`,
		"1|1|3|1|67|3|79",
		"2|67|3|67|70|3|78",
		"3|70|1|70|4|1|12",
		"4|8|1|8|8|1|8",
		"5|16|1|16|16|1|95",
		"6|32|1|32|32|1|32",
		"7|67|3|67|70|3|78")

	// Errors.
	h.expectError(`SELECT SUM(v) OVER (ORDER BY t RANGE BETWEEN INTERVAL '-30 minutes' PRECEDING AND CURRENT ROW) FROM it_agg_t`,
		"frame starting offset must not be negative")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY t RANGE BETWEEN CURRENT ROW AND INTERVAL '1 day -30 minutes' FOLLOWING) FROM it_agg_t`,
		"frame ending offset must not be negative")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY t RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM it_agg_t`,
		"not supported for ORDER BY type TIME(6) and offset type BIGINT")
}
