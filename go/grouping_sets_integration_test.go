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

// Integration tests for GROUPING SETS, ROLLUP, CUBE and GROUPING(). The
// expected rows are PostgreSQL 16's for the same data and queries.

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// setupGroupingSets creates the sales data set. region and product have
// real NULLs, so subtotal rows (NULL because the column is not grouped) and
// groups of NULLs look alike except for GROUPING().
func (h *sqlHarness) setupGroupingSets() {
	h.t.Helper()
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_gs_v")
		h.exec("DROP TABLE IF EXISTS it_gs_out")
		h.exec("DROP TABLE IF EXISTS it_gs_sales")
		h.exec("DROP TABLE IF EXISTS it_gs_regions")
	}
	drop()
	h.t.Cleanup(drop)
	h.exec("CREATE TABLE it_gs_sales (id INTEGER, region VARCHAR, product VARCHAR, yr INTEGER, qty INTEGER, amount NUMERIC(10,2), note VARCHAR NOINDEX)")
	h.exec(`INSERT INTO it_gs_sales VALUES
		(1, 'east', 'apple', 2023, 3, 1.50, 'a'),
		(2, 'east', 'apple', 2024, 5, 2.50, 'b'),
		(3, 'east', 'pear', 2024, 2, 4.00, 'a'),
		(4, 'west', 'apple', 2023, 1, 0.75, NULL),
		(5, 'west', NULL, 2024, 4, 3.25, 'b'),
		(6, NULL, 'pear', 2023, 6, 5.00, 'a'),
		(7, NULL, NULL, 2024, 7, NULL, NULL)`)
	h.exec("CREATE TABLE it_gs_regions (code VARCHAR, name VARCHAR)")
	h.exec("INSERT INTO it_gs_regions VALUES ('east', 'East'), ('west', 'West')")
}

// newGSPushdownHarness connects with adbc.redis.aggregate_pushdown set.
func newGSPushdownHarness(t *testing.T, mode string) *sqlHarness {
	t.Helper()
	ctx := context.Background()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: os.Getenv("REDIS_URI"), OptionStringAggregatePushdown: mode})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return &sqlHarness{t: t, ctx: ctx, conn: conn}
}

// gsSetPushdown plans a query with grouping sets and reports, for each of its
// grouping sets, whether it runs in the index (FT.AGGREGATE GROUPBY) in the
// default (exact) pushdown mode. It also checks that the driver predicts
// it: the sets it expects in the driver share one read of the rows.
func (h *sqlHarness) gsSetPushdown(sql string) []bool {
	h.t.Helper()
	parsed, err := ParseScript(sql)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	e := &executor{store: &store{client: h.rawClient()}, schema: "public", pushdown: PushdownExact, now: time.Now().UTC()}
	e.cache = newExecCache()
	plan, err := e.planSelect(h.ctx, parsed[0].Stmt.(*SelectStmt), nil)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	if plan.grouping == nil {
		h.t.Fatalf("%s: not planned as grouping sets", sql)
	}
	var out []bool
	for _, set := range plan.grouping.sets {
		sub := plan.grouping.setPlan(set, nil)
		wp, err := e.planWhere(h.ctx, sub.sel.Where, sub.meta, nil)
		if err != nil {
			h.t.Fatalf("%s: %v", sql, err)
		}
		var aggs []*Func
		for _, it := range sub.items {
			collectAggregates(it.expr, &aggs)
		}
		_, ok, err := e.indexAggregate(h.ctx, sub, wp, aggs)
		if err != nil {
			h.t.Fatalf("%s: %v", sql, err)
		}
		if predicted := plan.grouping.indexable(e, wp, set); predicted != ok {
			h.t.Errorf("%s: set %v predicted in the index %v, is %v", sql, set, predicted, ok)
		}
		out = append(out, ok)
	}
	return out
}

func TestSQLGroupingSets(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupingSets()

	// ROLLUP (a, b) is (a, b), (a), (). The (west, NULL) and (NULL, NULL)
	// rows with GROUPING 0 are groups of real NULLs; GROUPING tells them
	// from the subtotals.
	h.expectRows(`SELECT region, product, SUM(qty), COUNT(*), GROUPING(region, product) FROM it_gs_sales
		GROUP BY ROLLUP (region, product) ORDER BY GROUPING(region, product), region, product`,
		"east|apple|8|2|0", "east|pear|2|1|0", "west|apple|1|1|0", "west|NULL|4|1|0", "NULL|pear|6|1|0", "NULL|NULL|7|1|0",
		"east|NULL|10|3|1", "west|NULL|5|2|1", "NULL|NULL|13|2|1",
		"NULL|NULL|28|7|3")
	// CUBE is every subset.
	h.expectRows(`SELECT region, yr, SUM(qty), COUNT(amount), GROUPING(region, yr) FROM it_gs_sales
		GROUP BY CUBE (region, yr) ORDER BY 5, 1, 2`,
		"east|2023|3|1|0", "east|2024|7|2|0", "west|2023|1|1|0", "west|2024|4|1|0", "NULL|2023|6|1|0", "NULL|2024|7|0|0",
		"east|NULL|10|3|1", "west|NULL|5|2|1", "NULL|NULL|13|1|1",
		"NULL|2023|10|3|2", "NULL|2024|18|3|2",
		"NULL|NULL|28|6|3")
	h.expectRows(`SELECT region, product, COUNT(*), GROUPING(region), GROUPING(product) FROM it_gs_sales
		GROUP BY GROUPING SETS ((region, product), (region), (product), ()) ORDER BY 4, 5, 1, 2`,
		"east|apple|2|0|0", "east|pear|1|0|0", "west|apple|1|0|0", "west|NULL|1|0|0", "NULL|pear|1|0|0", "NULL|NULL|1|0|0",
		"east|NULL|3|0|1", "west|NULL|2|0|1", "NULL|NULL|2|0|1",
		"NULL|apple|3|1|0", "NULL|pear|2|1|0", "NULL|NULL|2|1|0",
		"NULL|NULL|7|1|1")
	// Plain items are in every set (a cross product).
	h.expectRows(`SELECT region, yr, product, SUM(qty), GROUPING(yr, product) FROM it_gs_sales
		GROUP BY region, ROLLUP (yr, product) ORDER BY 5, 1, 2, 3`,
		"east|2023|apple|3|0", "east|2024|apple|5|0", "east|2024|pear|2|0", "west|2023|apple|1|0", "west|2024|NULL|4|0",
		"NULL|2023|pear|6|0", "NULL|2024|NULL|7|0",
		"east|2023|NULL|3|1", "east|2024|NULL|7|1", "west|2023|NULL|1|1", "west|2024|NULL|4|1", "NULL|2023|NULL|6|1",
		"NULL|2024|NULL|7|1",
		"east|NULL|NULL|10|3", "west|NULL|NULL|5|3", "NULL|NULL|NULL|13|3")
	h.expectRows(`SELECT region, yr, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (region), CUBE (yr) ORDER BY GROUPING(region, yr), 1, 2`,
		"east|2023|1", "east|2024|2", "west|2023|1", "west|2024|1", "NULL|2023|1", "NULL|2024|1",
		"east|NULL|3", "west|NULL|2", "NULL|NULL|2",
		"NULL|2023|3", "NULL|2024|4",
		"NULL|NULL|7")
	h.expectRows(`SELECT region, product, COUNT(*) FROM it_gs_sales GROUP BY region, GROUPING SETS ((), (product))
		ORDER BY GROUPING(product), 1, 2`,
		"east|apple|2", "east|pear|1", "west|apple|1", "west|NULL|1", "NULL|pear|1", "NULL|NULL|1",
		"east|NULL|3", "west|NULL|2", "NULL|NULL|2")
	// Nested in GROUPING SETS, ROLLUP and CUBE add their sets: () twice here.
	h.expectRows(`SELECT region, yr, product, SUM(qty), GROUPING(region, yr, product) FROM it_gs_sales
		GROUP BY GROUPING SETS (ROLLUP (region), CUBE (yr), (product, yr)) ORDER BY 5, 1, 2, 3`,
		"east|NULL|NULL|10|3", "west|NULL|NULL|5|3", "NULL|NULL|NULL|13|3",
		"NULL|2023|apple|4|4", "NULL|2023|pear|6|4", "NULL|2024|apple|5|4", "NULL|2024|pear|2|4", "NULL|2024|NULL|11|4",
		"NULL|2023|NULL|10|5", "NULL|2024|NULL|18|5",
		"NULL|NULL|NULL|28|7", "NULL|NULL|NULL|28|7")
	// Plain items of GROUPING SETS are one set each; a parenthesized list
	// is one item of ROLLUP or CUBE.
	h.expectRows(`SELECT region, product, COUNT(*) FROM it_gs_sales GROUP BY GROUPING SETS (region, product)
		ORDER BY GROUPING(region, product), 1, 2`,
		"east|NULL|3", "west|NULL|2", "NULL|NULL|2", "NULL|apple|3", "NULL|pear|2", "NULL|NULL|2")
	h.expectRows(`SELECT region, product, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP ((region, product))
		ORDER BY GROUPING(region, product), 1, 2`,
		"east|apple|2", "east|pear|1", "west|apple|1", "west|NULL|1", "NULL|pear|1", "NULL|NULL|1", "NULL|NULL|7")
	h.expectRows(`SELECT region, yr, product, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (region, (yr, product))
		ORDER BY GROUPING(region, yr, product), 1, 2, 3`,
		"east|2023|apple|1", "east|2024|apple|1", "east|2024|pear|1", "west|2023|apple|1", "west|2024|NULL|1",
		"NULL|2023|pear|1", "NULL|2024|NULL|1",
		"east|NULL|NULL|3", "west|NULL|NULL|2", "NULL|NULL|NULL|2", "NULL|NULL|NULL|7")
	h.expectRows(`SELECT region, product, yr, GROUPING(region, product, yr), GROUPING(yr, region) FROM it_gs_sales
		WHERE id <= 2 GROUP BY CUBE (region, product, yr) ORDER BY 4, 1, 2, 3`,
		"east|apple|2023|0|0", "east|apple|2024|0|0", "east|apple|NULL|1|2", "east|NULL|2023|2|0", "east|NULL|2024|2|0",
		"east|NULL|NULL|3|2", "NULL|apple|2023|4|1", "NULL|apple|2024|4|1", "NULL|apple|NULL|5|3", "NULL|NULL|2023|6|1",
		"NULL|NULL|2024|6|1", "NULL|NULL|NULL|7|3")

	// Repeated sets are kept, as in Postgres (GROUP BY DISTINCT would drop
	// them).
	h.expectRows(`SELECT region, COUNT(*) FROM it_gs_sales GROUP BY GROUPING SETS ((region), (region), ())
		ORDER BY GROUPING(region), region`,
		"east|3", "east|3", "west|2", "west|2", "NULL|2", "NULL|2", "NULL|7")
	h.expectRows(`SELECT region, product, COUNT(*), GROUPING(region, product) FROM it_gs_sales
		GROUP BY ROLLUP (region, product), ROLLUP (region) ORDER BY 4, 1, 2, 3`,
		"east|apple|2|0", "east|apple|2|0", "east|pear|1|0", "east|pear|1|0", "west|apple|1|0", "west|apple|1|0",
		"west|NULL|1|0", "west|NULL|1|0", "NULL|pear|1|0", "NULL|pear|1|0", "NULL|NULL|1|0", "NULL|NULL|1|0",
		"east|NULL|3|1", "east|NULL|3|1", "east|NULL|3|1", "west|NULL|2|1", "west|NULL|2|1", "west|NULL|2|1",
		"NULL|NULL|2|1", "NULL|NULL|2|1", "NULL|NULL|2|1", "NULL|NULL|7|3")

	// () alone is one group, even without rows; (a, b) alone is GROUP BY a, b.
	h.expectRows("SELECT COUNT(*), SUM(qty) FROM it_gs_sales GROUP BY ()", "7|28")
	h.expectRows("SELECT COUNT(*), SUM(qty) FROM it_gs_sales WHERE id > 100 GROUP BY ()", "0|NULL")
	h.expectRows("SELECT region, yr, SUM(qty) FROM it_gs_sales GROUP BY (region, yr) ORDER BY 1, 2",
		"east|2023|3", "east|2024|7", "west|2023|1", "west|2024|4", "NULL|2023|6", "NULL|2024|7")
	// Without rows only the empty set has a group.
	h.expectRows("SELECT region, COUNT(*), SUM(qty) FROM it_gs_sales WHERE id > 100 GROUP BY ROLLUP (region)", "NULL|0|NULL")
	h.expectRows("SELECT region, COUNT(*), SUM(qty) FROM it_gs_sales WHERE id > 100 GROUP BY CUBE (region, product)", "NULL|0|NULL")
	h.expectRows("SELECT region, COUNT(*) FROM it_gs_sales WHERE id > 100 GROUP BY GROUPING SETS ((region))")
	// WHERE filters the rows of every set.
	h.expectRows(`SELECT region, product, COUNT(*) FROM it_gs_sales WHERE region = 'east' GROUP BY CUBE (region, product)
		ORDER BY GROUPING(region, product), 1, 2`,
		"east|apple|2", "east|pear|1", "east|NULL|3", "NULL|apple|2", "NULL|pear|1", "NULL|NULL|3")

	// Grouping items are expressions (matched as a whole), output positions
	// or aliases, as in a plain GROUP BY.
	h.expectRows(`SELECT UPPER(region), yr % 2, SUM(qty), GROUPING(UPPER(region), yr % 2) FROM it_gs_sales
		GROUP BY ROLLUP (UPPER(region), yr % 2) ORDER BY 4, 1, 2`,
		"EAST|0|7|0", "EAST|1|3|0", "WEST|0|4|0", "WEST|1|1|0", "NULL|0|7|0", "NULL|1|6|0",
		"EAST|NULL|10|1", "WEST|NULL|5|1", "NULL|NULL|13|1", "NULL|NULL|28|3")
	for _, by := range []string{"1, 2", "r, y"} {
		h.expectRows(`SELECT region AS r, yr AS y, SUM(qty) AS q FROM it_gs_sales GROUP BY ROLLUP (`+by+`)
			ORDER BY GROUPING(region, yr), 1, 2`,
			"east|2023|3", "east|2024|7", "west|2023|1", "west|2024|4", "NULL|2023|6", "NULL|2024|7",
			"east|NULL|10", "west|NULL|5", "NULL|NULL|13", "NULL|NULL|28")
	}
	h.expectRows(`SELECT CASE WHEN qty > 3 THEN 'big' ELSE 'small' END AS size, COUNT(*) FROM it_gs_sales
		GROUP BY ROLLUP (CASE WHEN qty > 3 THEN 'big' ELSE 'small' END) ORDER BY 1`,
		"big|4", "small|3", "NULL|7")
	h.expectRows(`SELECT qty > 3 AS big, COUNT(*), GROUPING(qty > 3) FROM it_gs_sales GROUP BY ROLLUP (qty > 3) ORDER BY 3, 1`,
		"false|3|0", "true|4|0", "NULL|7|1")
	h.expectRows(`SELECT COALESCE(region, '-') AS r, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (COALESCE(region, '-'))
		ORDER BY GROUPING(COALESCE(region, '-')), 1`,
		"-|2", "east|3", "west|2", "NULL|7")
	h.expectRows(`SELECT region || '!' AS r, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (region) ORDER BY GROUPING(region), region`,
		"east!|3", "west!|2", "NULL|2", "NULL|7")
	// A constant grouped by position is NULL where it isn't grouped; other
	// constants are just constants.
	h.expectRows(`SELECT 'x' AS k, region, COUNT(*) + 1 FROM it_gs_sales GROUP BY ROLLUP (1, region)
		ORDER BY GROUPING(region), k, region`,
		"x|east|4", "x|west|3", "x|NULL|3", "x|NULL|8", "NULL|NULL|8")

	// Aggregates over other columns, including NOINDEX ones, and DISTINCT.
	h.expectRows(`SELECT region, SUM(amount), MIN(product), MAX(yr), COUNT(DISTINCT product), MAX(note) FROM it_gs_sales
		GROUP BY ROLLUP (region) ORDER BY GROUPING(region), region`,
		"east|8.00|apple|2024|2|b", "west|4.00|apple|2024|1|b", "NULL|5.00|pear|2024|1|a", "NULL|17.00|apple|2024|2|b")
	h.expectRows(`SELECT region, AVG(qty), MIN(qty), MAX(qty), COUNT(qty) FROM it_gs_sales GROUP BY ROLLUP (region)
		ORDER BY GROUPING(region), region`,
		"east|3.3333333333333335|2|5|3", "west|2.5|1|4|2", "NULL|6.5|6|7|2", "NULL|4|1|7|7")
	h.expectRows(`SELECT region, note, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (region, note)
		ORDER BY GROUPING(region, note), region, note`,
		"east|a|2", "east|b|1", "west|b|1", "west|NULL|1", "NULL|a|1", "NULL|NULL|1",
		"east|NULL|3", "west|NULL|2", "NULL|NULL|2", "NULL|NULL|7")
	// Without aggregates.
	h.expectRows(`SELECT region, product FROM it_gs_sales GROUP BY ROLLUP (region, product) ORDER BY GROUPING(region, product), 1, 2`,
		"east|apple", "east|pear", "west|apple", "west|NULL", "NULL|pear", "NULL|NULL",
		"east|NULL", "west|NULL", "NULL|NULL", "NULL|NULL")
}

func TestSQLGroupingFunction(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupingSets()

	// Labelling subtotals, which a real NULL can't be mistaken for.
	h.expectRows(`SELECT CASE WHEN GROUPING(region) = 1 THEN 'all' ELSE COALESCE(region, '(none)') END AS r, SUM(qty)
		FROM it_gs_sales GROUP BY ROLLUP (region) ORDER BY 1`,
		"(none)|13", "all|28", "east|10", "west|5")
	h.expectRows(`SELECT region, SUM(qty) AS q FROM it_gs_sales GROUP BY ROLLUP (region) HAVING GROUPING(region) = 0 AND region IS NULL`,
		"NULL|13")
	// In a plain GROUP BY GROUPING() is 0.
	h.expectRows("SELECT region, GROUPING(region), COUNT(*) FROM it_gs_sales GROUP BY region ORDER BY 1",
		"east|0|3", "west|0|2", "NULL|0|2")
	// A bit per argument, the last one lowest; up to 31 arguments.
	h.expectRows(`SELECT GROUPING(id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id,
			id, id, id, id, id, id, id) FROM it_gs_sales WHERE id = 1 GROUP BY ROLLUP (id) ORDER BY 1`,
		"0", "2147483647")
	// It is an INTEGER.
	schema := h.expectRows(`SELECT GROUPING(region) AS g, GROUPING(region) + 1 AS g1 FROM it_gs_sales GROUP BY ROLLUP (region)
		ORDER BY 1, 2 LIMIT 1`, "0|1")
	if dt := schema.Field(0).Type; dt.ID() != arrow.INT32 {
		t.Errorf("GROUPING() type = %s, want int32", dt)
	}
	if dt := schema.Field(1).Type; dt.ID() != arrow.INT64 {
		t.Errorf("GROUPING() + 1 type = %s, want int64", dt)
	}

	// Postgres's errors.
	h.expectError("SELECT GROUPING(product) FROM it_gs_sales GROUP BY ROLLUP (region)",
		"arguments to GROUPING must be grouping expressions of the associated query level")
	h.expectError("SELECT GROUPING(region) FROM it_gs_sales",
		"arguments to GROUPING must be grouping expressions of the associated query level")
	h.expectError("SELECT GROUPING(region), COUNT(*) FROM it_gs_sales",
		"arguments to GROUPING must be grouping expressions of the associated query level")
	h.expectError("SELECT DISTINCT GROUPING(region) FROM it_gs_sales",
		"arguments to GROUPING must be grouping expressions of the associated query level")
	h.expectError("SELECT COUNT(*) FROM it_gs_sales WHERE GROUPING(region) = 0 GROUP BY ROLLUP (region)",
		"grouping operations are not allowed in WHERE")
	h.expectError("SELECT COUNT(*) FROM it_gs_sales GROUP BY GROUPING(region)",
		"grouping operations are not allowed in GROUP BY")
	h.expectError("SELECT COUNT(*) FROM it_gs_sales s JOIN it_gs_regions r ON GROUPING(r.code) = 0 GROUP BY ROLLUP (s.region)",
		"grouping operations are not allowed in JOIN conditions")
	h.expectError("SELECT SUM(GROUPING(region)) FROM it_gs_sales GROUP BY ROLLUP (region)",
		"aggregate function calls cannot be nested")
	h.expectError(`SELECT GROUPING(id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id,
		id, id, id, id, id, id, id, id) FROM it_gs_sales GROUP BY id`, "GROUPING must have fewer than 32 arguments")
	h.expectError("SELECT GROUPING() FROM it_gs_sales GROUP BY ROLLUP (region)", "GROUPING expects one or more grouping expressions")
	h.expectError("SELECT GROUPING(region) OVER () FROM it_gs_sales GROUP BY ROLLUP (region)", "GROUPING is not a window function")
}

// HAVING, ORDER BY, LIMIT, DISTINCT, window functions and QUALIFY apply to
// the combined groups of all sets.
func TestSQLGroupingSetsCombined(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupingSets()

	h.expectRows(`SELECT region, SUM(qty) FROM it_gs_sales GROUP BY ROLLUP (region) HAVING SUM(qty) > 8 OR GROUPING(region) = 1
		ORDER BY GROUPING(region), region`,
		"east|10", "NULL|13", "NULL|28")
	h.expectRows(`SELECT region, product, COUNT(*), GROUPING(region, product) FROM it_gs_sales GROUP BY ROLLUP (region, product)
		HAVING COUNT(*) > 1 ORDER BY 4, 1, 2`,
		"east|apple|2|0", "east|NULL|3|1", "west|NULL|2|1", "NULL|NULL|2|1", "NULL|NULL|7|3")
	h.expectRows(`SELECT yr, product, SUM(qty) FROM it_gs_sales GROUP BY GROUPING SETS ((yr, product), (yr))
		HAVING GROUPING(product) = 1 OR SUM(qty) > 4 ORDER BY 1, GROUPING(product), 2`,
		"2023|pear|6", "2023|NULL|10", "2024|apple|5", "2024|NULL|11", "2024|NULL|18")
	h.expectRows(`SELECT region FROM it_gs_sales GROUP BY ROLLUP (region) HAVING region = 'east' OR region IS NULL
		ORDER BY GROUPING(region), region`, "east", "NULL", "NULL")
	// HAVING may use output aliases, like a plain GROUP BY.
	h.expectRows(`SELECT region, SUM(qty) AS q FROM it_gs_sales GROUP BY ROLLUP (region) HAVING q < 10 ORDER BY q`, "west|5")

	h.expectRows(`SELECT region, SUM(qty) FROM it_gs_sales GROUP BY ROLLUP (region) ORDER BY GROUPING(region) DESC, SUM(qty) DESC LIMIT 3`,
		"NULL|28", "NULL|13", "east|10")
	h.expectRows(`SELECT region, SUM(qty) FROM it_gs_sales GROUP BY ROLLUP (region)
		ORDER BY GROUPING(region) DESC, SUM(qty) DESC LIMIT 2 OFFSET 1`, "NULL|13", "east|10")
	h.expectRows(`SELECT region, SUM(qty) FROM it_gs_sales GROUP BY ROLLUP (region) ORDER BY region NULLS FIRST, GROUPING(region)`,
		"NULL|13", "NULL|28", "east|10", "west|5")
	h.expectRows(`SELECT region, SUM(qty) FROM it_gs_sales GROUP BY ROLLUP (region) ORDER BY 1, GROUPING(region) DESC`,
		"east|10", "west|5", "NULL|28", "NULL|13")

	h.expectRows(`SELECT DISTINCT GROUPING(region, product) FROM it_gs_sales GROUP BY CUBE (region, product) ORDER BY 1`,
		"0", "1", "2", "3")
	h.expectRows(`SELECT DISTINCT region FROM it_gs_sales GROUP BY ROLLUP (region, product) ORDER BY region LIMIT 2`, "east", "west")
	h.expectRows(`SELECT DISTINCT ON (GROUPING(region)) GROUPING(region), region, SUM(qty) FROM it_gs_sales GROUP BY ROLLUP (region)
		ORDER BY GROUPING(region), SUM(qty) DESC`, "0|NULL|13", "1|NULL|28")

	// Window functions see one row per group of every set.
	h.expectRows(`SELECT region, SUM(qty), RANK() OVER (PARTITION BY GROUPING(region) ORDER BY SUM(qty) DESC) FROM it_gs_sales
		GROUP BY ROLLUP (region) ORDER BY GROUPING(region), region`,
		"east|10|2", "west|5|3", "NULL|13|1", "NULL|28|1")
	h.expectRows(`SELECT region, product, SUM(qty), SUM(SUM(qty)) OVER (PARTITION BY region, GROUPING(product)) FROM it_gs_sales
		GROUP BY ROLLUP (region, product) ORDER BY GROUPING(region, product), region, product`,
		"east|apple|8|10", "east|pear|2|10", "west|apple|1|5", "west|NULL|4|5", "NULL|pear|6|13", "NULL|NULL|7|13",
		"east|NULL|10|10", "west|NULL|5|5", "NULL|NULL|13|41", "NULL|NULL|28|41")
	h.expectRows(`SELECT region, SUM(qty), SUM(SUM(qty)) OVER (ORDER BY GROUPING(region), region ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		FROM it_gs_sales GROUP BY ROLLUP (region) ORDER BY GROUPING(region), region`,
		"east|10|10", "west|5|15", "NULL|13|28", "NULL|28|56")
	h.expectRows(`SELECT region, SUM(qty), RANK() OVER w, LAG(region) OVER w, COUNT(*) OVER () FROM it_gs_sales GROUP BY ROLLUP (region)
		WINDOW w AS (PARTITION BY GROUPING(region) ORDER BY SUM(qty) DESC) ORDER BY GROUPING(region), region`,
		"east|10|2|NULL|4", "west|5|3|east|4", "NULL|13|1|NULL|4", "NULL|28|1|NULL|4")
	h.expectRows(`SELECT region, product, SUM(qty) AS q FROM it_gs_sales GROUP BY ROLLUP (region, product)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY GROUPING(region, product) ORDER BY SUM(qty) DESC) = 1 ORDER BY q`,
		"east|apple|8", "NULL|NULL|13", "NULL|NULL|28")
	h.expectRows(`SELECT region, SUM(qty) * 100 / (SELECT SUM(qty) FROM it_gs_sales) AS pct FROM it_gs_sales GROUP BY ROLLUP (region)
		ORDER BY GROUPING(region), region`, "east|35", "west|17", "NULL|46", "NULL|100")
}

// Each grouping set runs as its own grouped query, in the index when it can.
func TestSQLGroupingSetsPushdown(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupingSets()

	for _, c := range []struct {
		sql  string
		want []bool
	}{
		// COUNT and integer SUM over indexed columns: every set.
		{"SELECT region, product, COUNT(*), SUM(qty) FROM it_gs_sales GROUP BY ROLLUP (region, product)", []bool{true, true, true}},
		{"SELECT region, yr, GROUPING(region, yr), MIN(qty) FROM it_gs_sales GROUP BY CUBE (region, yr) HAVING COUNT(*) > 1 ORDER BY 3",
			[]bool{true, true, true, true}},
		{"SELECT region, COUNT(*) FROM it_gs_sales WHERE yr = 2024 GROUP BY GROUPING SETS ((region), ())", []bool{true, true}},
		// A NOINDEX column or an expression in the set: the driver groups
		// that set; the others still run in the index.
		{"SELECT region, note, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (region, note)", []bool{false, true, true}},
		{"SELECT UPPER(region), COUNT(*) FROM it_gs_sales GROUP BY GROUPING SETS ((UPPER(region)), ())", []bool{false, true}},
		// Aggregates the index can't compute exactly (decimal SUM in exact
		// mode, DISTINCT), a filter it can't answer, a correlated subquery
		// or a join: every set in the driver.
		{"SELECT region, SUM(amount) FROM it_gs_sales GROUP BY ROLLUP (region)", []bool{false, false}},
		{"SELECT region, COUNT(DISTINCT product) FROM it_gs_sales GROUP BY ROLLUP (region)", []bool{false, false}},
		{"SELECT region, COUNT(*) FROM it_gs_sales WHERE note = 'a' GROUP BY ROLLUP (region)", []bool{false, false}},
		{"SELECT yr, COUNT((SELECT r.name FROM it_gs_regions r WHERE r.code = s.region)) FROM it_gs_sales s GROUP BY ROLLUP (yr)",
			[]bool{false, false}},
		{"SELECT r.name, COUNT(*) FROM it_gs_sales s JOIN it_gs_regions r ON r.code = s.region GROUP BY CUBE (r.name)",
			[]bool{false, false}},
	} {
		if got := h.gsSetPushdown(c.sql); !slices.Equal(got, c.want) {
			t.Errorf("%s\n pushed down %v, want %v", c.sql, got, c.want)
		}
	}

	// The same results in every aggregate_pushdown mode: in the index, in
	// the driver, and in both. Several sets in the driver share one read of
	// the rows.
	queries := []struct {
		sql  string
		want []string
	}{
		{`SELECT region, product, SUM(qty), COUNT(*), GROUPING(region, product) FROM it_gs_sales
			GROUP BY ROLLUP (region, product) ORDER BY 5, 1, 2`,
			[]string{"east|apple|8|2|0", "east|pear|2|1|0", "west|apple|1|1|0", "west|NULL|4|1|0", "NULL|pear|6|1|0",
				"NULL|NULL|7|1|0", "east|NULL|10|3|1", "west|NULL|5|2|1", "NULL|NULL|13|2|1", "NULL|NULL|28|7|3"}},
		{`SELECT region, yr, SUM(amount), MIN(qty), MAX(qty), COUNT(amount) FROM it_gs_sales GROUP BY CUBE (region, yr)
			ORDER BY GROUPING(region, yr), 1, 2`,
			[]string{"east|2023|1.50|3|3|1", "east|2024|6.50|2|5|2", "west|2023|0.75|1|1|1", "west|2024|3.25|4|4|1",
				"NULL|2023|5.00|6|6|1", "NULL|2024|NULL|7|7|0", "east|NULL|8.00|2|5|3", "west|NULL|4.00|1|4|2",
				"NULL|NULL|5.00|6|7|1", "NULL|2023|7.25|1|6|3", "NULL|2024|9.75|2|7|3", "NULL|NULL|17.00|1|7|6"}},
		{`SELECT region, note, COUNT(*), SUM(qty) FROM it_gs_sales GROUP BY ROLLUP (region, note)
			ORDER BY GROUPING(region, note), region, note`,
			[]string{"east|a|2|5", "east|b|1|5", "west|b|1|4", "west|NULL|1|1", "NULL|a|1|6", "NULL|NULL|1|7",
				"east|NULL|3|10", "west|NULL|2|5", "NULL|NULL|2|13", "NULL|NULL|7|28"}},
		{`SELECT region, SUM(qty), SUM(amount) FROM it_gs_sales WHERE note = 'a' GROUP BY ROLLUP (region)
			ORDER BY GROUPING(region), region`,
			[]string{"east|5|5.50", "NULL|6|5.00", "NULL|11|10.50"}},
		{`SELECT yr, COUNT((SELECT r.name FROM it_gs_regions r WHERE r.code = s.region)) FROM it_gs_sales s
			GROUP BY ROLLUP (yr) ORDER BY GROUPING(yr), yr`,
			[]string{"2023|2", "2024|3", "NULL|5"}},
		{`SELECT r.name, s.yr, SUM(s.amount), COUNT(*) FROM it_gs_sales s JOIN it_gs_regions r ON r.code = s.region
			WHERE s.qty > 1 GROUP BY CUBE (r.name, s.yr) ORDER BY GROUPING(r.name, s.yr), 1, 2`,
			[]string{"East|2023|1.50|1", "East|2024|6.50|2", "West|2024|3.25|1", "East|NULL|8.00|3", "West|NULL|3.25|1",
				"NULL|2023|1.50|1", "NULL|2024|9.75|3", "NULL|NULL|11.25|4"}},
	}
	for _, mode := range []string{PushdownExact, PushdownAll, PushdownNone} {
		m := newGSPushdownHarness(t, mode)
		for _, q := range queries {
			m.expectRows(q.sql, q.want...)
		}
	}
}

// Grouping sets work wherever a query does.
func TestSQLGroupingSetsQueries(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupingSets()

	// Joins, CTEs and derived tables.
	h.expectRows(`SELECT r.name, s.product, SUM(s.qty), GROUPING(r.name, s.product) FROM it_gs_sales s
		JOIN it_gs_regions r ON r.code = s.region GROUP BY ROLLUP (r.name, s.product) ORDER BY 4, 1, 2`,
		"East|apple|8|0", "East|pear|2|0", "West|apple|1|0", "West|NULL|4|0", "East|NULL|10|1", "West|NULL|5|1", "NULL|NULL|15|3")
	h.expectRows(`SELECT s.region, r.name, COUNT(*), GROUPING(s.region, r.name) FROM it_gs_sales s
		LEFT JOIN it_gs_regions r ON r.code = s.region GROUP BY CUBE (s.region, r.name) ORDER BY 4, 1, 2`,
		"east|East|3|0", "west|West|2|0", "NULL|NULL|2|0", "east|NULL|3|1", "west|NULL|2|1", "NULL|NULL|2|1",
		"NULL|East|3|2", "NULL|West|2|2", "NULL|NULL|2|2", "NULL|NULL|7|3")
	h.expectRows(`WITH x AS (SELECT region, qty FROM it_gs_sales) SELECT region, SUM(qty) FROM x GROUP BY CUBE (region)
		ORDER BY GROUPING(region), region`, "east|10", "west|5", "NULL|13", "NULL|28")
	h.expectRows("SELECT COUNT(*) FROM (SELECT region FROM it_gs_sales GROUP BY ROLLUP (region)) x", "4")

	// Subqueries: IN, and correlated ones run per outer row.
	h.expectRows(`SELECT id FROM it_gs_sales WHERE qty IN (SELECT SUM(qty) FROM it_gs_sales GROUP BY ROLLUP (region)) ORDER BY id`, "2")
	h.expectRows(`SELECT r.code, EXISTS (SELECT 1 FROM it_gs_sales s WHERE s.region = r.code GROUP BY ROLLUP (s.product)
		HAVING COUNT(*) > 2) FROM it_gs_regions r ORDER BY 1`, "east|true", "west|false")
	h.expectRows(`SELECT r.code, (SELECT SUM(qty) FROM it_gs_sales s WHERE s.region = r.code GROUP BY ROLLUP (product)
		ORDER BY GROUPING(product) DESC LIMIT 1) FROM it_gs_regions r ORDER BY 1`, "east|10", "west|5")
	h.expectRows(`SELECT r.code, (SELECT COUNT(*) FROM it_gs_sales s WHERE s.region = r.code AND s.id > 100 GROUP BY ())
		FROM it_gs_regions r ORDER BY 1`, "east|0", "west|0")
	h.expectRows("SELECT EXISTS (SELECT 1 FROM it_gs_sales WHERE id > 100 GROUP BY ())", "true")
	// A correlated subquery reads a grouping column: NULL in the subtotal.
	h.expectRows(`SELECT region, (SELECT COUNT(*) FROM it_gs_sales s2 WHERE s2.region = s.region) AS n, SUM(qty)
		FROM it_gs_sales s GROUP BY ROLLUP (region) ORDER BY GROUPING(region), region`,
		"east|3|10", "west|2|5", "NULL|0|13", "NULL|0|28")
	h.expectRows(`SELECT region, COUNT(*) FROM it_gs_sales s GROUP BY ROLLUP (region)
		HAVING EXISTS (SELECT 1 FROM it_gs_regions r WHERE r.code = s.region) ORDER BY region`, "east|3", "west|2")

	// Set operations, views, CTAS and INSERT … SELECT.
	h.expectRows(`SELECT region, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (region) UNION ALL SELECT 'x', 0 ORDER BY 1, 2`,
		"east|3", "west|2", "x|0", "NULL|2", "NULL|7")
	h.exec(`CREATE VIEW it_gs_v AS SELECT region, SUM(qty) AS q, GROUPING(region) AS g FROM it_gs_sales GROUP BY ROLLUP (region)`)
	h.expectRows("SELECT region, q FROM it_gs_v WHERE g = 1", "NULL|28")
	h.expectRows("SELECT region, q FROM it_gs_v WHERE region IS NULL ORDER BY g", "NULL|13", "NULL|28")
	h.exec(`CREATE TABLE it_gs_out AS SELECT region, SUM(qty) AS q, GROUPING(region) AS g FROM it_gs_sales GROUP BY CUBE (region)`)
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_gs_out' ORDER BY ordinal_position`,
		"region|VARCHAR", "q|BIGINT", "g|INTEGER")
	h.exec(`INSERT INTO it_gs_out SELECT product, COUNT(*), GROUPING(product) FROM it_gs_sales GROUP BY ROLLUP (product)`)
	h.expectRows("SELECT region, q, g FROM it_gs_out ORDER BY g, region, q",
		"apple|3|0", "east|10|0", "pear|2|0", "west|5|0", "NULL|2|0", "NULL|13|0", "NULL|7|1", "NULL|28|1")

	// A statement is planned again for each execution.
	st := h.gsPrepare(`SELECT region AS r, yr, SUM(qty), GROUPING(region, yr) FROM it_gs_sales GROUP BY ROLLUP (1, yr)
		HAVING GROUPING(r, yr) > 0 ORDER BY 4, 1`)
	for range 2 {
		if got, want := h.gsRows(st), []string{"east|NULL|10|1", "west|NULL|5|1", "NULL|NULL|13|1", "NULL|NULL|28|3"}; !slices.Equal(got, want) {
			t.Errorf("executed again: got %q, want %q", got, want)
		}
	}
	// Bound parameters. The parser reads the ? in the parenthesized item
	// twice (as a list, then as an expression) and must number it once.
	st = h.gsPrepare(`SELECT COUNT(*), SUM(qty) FROM it_gs_sales WHERE qty > ?
		GROUP BY ROLLUP ((qty > ?)) HAVING COUNT(*) > ? ORDER BY 1`)
	for _, c := range []struct {
		params []int64
		want   []string
	}{
		{[]int64{2, 4, 1}, []string{"2|7", "3|18", "5|25"}},
		{[]int64{4, 5, 0}, []string{"1|5", "2|13", "3|18"}},
	} {
		ib := array.NewInt64Builder(memory.DefaultAllocator)
		var cols []arrow.Array
		var fields []arrow.Field
		for i, v := range c.params {
			ib.Append(v)
			cols = append(cols, ib.NewArray())
			fields = append(fields, arrow.Field{Name: string(rune('a' + i)), Type: arrow.PrimitiveTypes.Int64})
		}
		ib.Release()
		if err := st.Bind(h.ctx, array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 1)); err != nil {
			t.Fatal(err)
		}
		if got := h.gsRows(st); !slices.Equal(got, c.want) {
			t.Errorf("bound parameters %v: got %q, want %q", c.params, got, c.want)
		}
	}
}

// gsPrepare creates a statement for a query, closed at the end of the test.
func (h *sqlHarness) gsPrepare(sql string) adbc.StatementWithContext {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = st.Close(h.ctx) })
	if err := st.SetSqlQuery(h.ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	return st
}

// gsRows executes a statement and renders its rows like query.
func (h *sqlHarness) gsRows(st adbc.StatementWithContext) []string {
	h.t.Helper()
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer rdr.Release()
	var rows []string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			cells := make([]string, rec.NumCols())
			for c := range cells {
				cells[c] = "NULL"
				if col := rec.Column(c); !col.IsNull(r) {
					cells[c] = col.ValueStr(r)
				}
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
	}
	return rows
}

func TestSQLGroupingSetsErrors(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupingSets()

	// Outside aggregates, only grouping expressions (Postgres's rule).
	for _, sql := range []string{
		"SELECT region, product, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (region)",
		"SELECT region FROM it_gs_sales GROUP BY ROLLUP (region) ORDER BY product",
		"SELECT region FROM it_gs_sales GROUP BY ROLLUP (region) HAVING product = 'x'",
		"SELECT region, SUM(COUNT(*)) OVER (PARTITION BY product) FROM it_gs_sales GROUP BY ROLLUP (region)",
		"SELECT region, product, COUNT(*) FROM it_gs_sales GROUP BY GROUPING SETS ((UPPER(product)), (region))",
	} {
		h.expectError(sql, `column "it_gs_sales.product" must appear in the GROUP BY clause or be used in an aggregate function`)
	}
	h.expectError("SELECT * FROM it_gs_sales GROUP BY ROLLUP (region)",
		`column "it_gs_sales.id" must appear in the GROUP BY clause or be used in an aggregate function`)
	h.expectError("SELECT region, s.qty FROM it_gs_sales s GROUP BY CUBE (region)",
		`column "s.qty" must appear in the GROUP BY clause or be used in an aggregate function`)
	h.expectError("SELECT s.region, r.name FROM it_gs_sales s JOIN it_gs_regions r ON r.code = s.region GROUP BY ROLLUP (s.region)",
		`column "r.name" must appear in the GROUP BY clause or be used in an aggregate function`)
	h.expectError("SELECT region, (SELECT s.product) FROM it_gs_sales s GROUP BY ROLLUP (region)",
		`subquery uses ungrouped column "s.product" from outer query`)

	// Limits, and items that can't be grouped.
	h.expectError(`SELECT COUNT(*) FROM it_gs_sales GROUP BY CUBE (id, region, product, yr, qty, amount, note,
		id + 1, id + 2, id + 3, id + 4, id + 5, id + 6)`, "CUBE is limited to 12 elements")
	h.expectError(`SELECT COUNT(*) FROM it_gs_sales GROUP BY CUBE (id, region, product, yr, qty, amount, note),
		CUBE (id + 1, id + 2, id + 3, id + 4, id + 5, id + 6)`, "too many grouping sets present (maximum 4096)")
	h.expectError("SELECT region, COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (3)", "GROUP BY position 3 is out of range")
	h.expectError("SELECT COUNT(*) FROM it_gs_sales GROUP BY ROLLUP (COUNT(*))", "aggregates are not allowed in GROUP BY")
	h.expectError("SELECT COUNT(*) FROM it_gs_sales GROUP BY CUBE (ROW_NUMBER() OVER ())", "window functions are not allowed in GROUP BY")
	h.expectError("SELECT COUNT(*) GROUP BY ()", "aggregates require a FROM clause")

	// Syntax.
	for _, c := range []struct{ sql, err string }{
		{"SELECT COUNT(*) FROM it_gs_sales GROUP BY ROLLUP ()", `syntax error: unexpected ")"`},
		{"SELECT COUNT(*) FROM it_gs_sales GROUP BY CUBE (region, ())", "syntax error: CUBE items must be expressions or lists of them, not ()"},
		{"SELECT COUNT(*) FROM it_gs_sales GROUP BY GROUPING SETS (region", `syntax error: expected ")"`},
		{"SELECT COUNT(*) FROM it_gs_sales GROUP BY GROUPING SETS ()", `syntax error: unexpected ")"`},
	} {
		if _, err := ParseScript(c.sql); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: error %v, want %q", c.sql, err, c.err)
		}
	}
}
