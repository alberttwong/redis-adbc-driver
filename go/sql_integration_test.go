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

// Integration tests for SQL features beyond the ADBC validation suite. They
// run against the Redis server at REDIS_URI and are skipped when it is unset:
//
//	REDIS_URI=redis://localhost:6379/0 go test -run TestSQL ./...

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

type sqlHarness struct {
	t    *testing.T
	ctx  context.Context
	conn adbc.ConnectionWithContext
}

func newSQLHarness(t *testing.T) *sqlHarness {
	t.Helper()
	uri := os.Getenv("REDIS_URI")
	if uri == "" {
		t.Skip("set REDIS_URI to run SQL integration tests")
	}
	ctx := context.Background()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{adbc.OptionKeyURI: uri})
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

// exec runs a statement and returns rows affected.
func (h *sqlHarness) exec(sql string) int64 {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	n, err := st.ExecuteUpdate(h.ctx)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// query runs a SELECT and renders each row as "v1|v2|..." (NULL for nulls),
// plus the result schema.
func (h *sqlHarness) query(sql string) ([]string, *arrow.Schema) {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	defer rdr.Release()
	var rows []string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			cells := make([]string, rec.NumCols())
			for c := 0; c < int(rec.NumCols()); c++ {
				col := rec.Column(c)
				switch dec := col.(type) {
				case *array.Decimal128:
					if !col.IsNull(r) {
						// Render at the declared scale (ValueStr drops trailing zeros).
						scale := dec.DataType().(*arrow.Decimal128Type).Scale
						cells[c] = formatDecimal(dec.Value(r).BigInt(), scale)
						continue
					}
				}
				if col.IsNull(r) {
					cells[c] = "NULL"
				} else {
					cells[c] = col.ValueStr(r)
				}
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
	}
	return rows, rdr.Schema()
}

func (h *sqlHarness) expectRows(sql string, want ...string) *arrow.Schema {
	h.t.Helper()
	got, schema := h.query(sql)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		h.t.Errorf("%s\n got: %q\nwant: %q", sql, got, want)
	}
	return schema
}

func (h *sqlHarness) expectError(sql, substr string) {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err == nil {
		rdr.Release()
		h.t.Fatalf("%s: expected an error containing %q", sql, substr)
	}
	if !strings.Contains(err.Error(), substr) {
		h.t.Fatalf("%s: error %q does not contain %q", sql, err, substr)
	}
}

// setupOrders creates a small orders/customers data set.
func (h *sqlHarness) setupOrders() {
	h.t.Helper()
	h.exec("DROP TABLE IF EXISTS it_orders")
	h.exec("DROP TABLE IF EXISTS it_customers")
	h.exec("CREATE TABLE it_customers (id INTEGER NOT NULL, name VARCHAR, country VARCHAR)")
	h.exec(`INSERT INTO it_customers VALUES
		(1, 'Ada', 'GBR'), (2, 'Bo', 'USA'), (3, 'Cy', 'USA'), (4, 'Di', NULL)`)
	h.exec("CREATE TABLE it_orders (id INTEGER NOT NULL, customer_id INTEGER, amount NUMERIC(10,2), qty INTEGER, status VARCHAR)")
	h.exec(`INSERT INTO it_orders VALUES
		(1, 1, 10.50, 1, 'shipped'),
		(2, 1, 99.99, 3, 'pending'),
		(3, 2, 5.00, 2, 'shipped'),
		(4, 2, 250.00, 10, 'shipped'),
		(5, 3, NULL, 1, 'returned'),
		(6, 9, 42.00, 4, 'shipped')`)
	h.t.Cleanup(func() {
		h.exec("DROP TABLE IF EXISTS it_orders")
		h.exec("DROP TABLE IF EXISTS it_customers")
	})
}

func TestSQLCase(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()

	// Searched CASE, with and without ELSE.
	h.expectRows(`SELECT id, CASE WHEN qty >= 10 THEN 'bulk' WHEN qty > 1 THEN 'multi' ELSE 'single' END AS size
		FROM it_orders ORDER BY id`,
		"1|single", "2|multi", "3|multi", "4|bulk", "5|single", "6|multi")
	h.expectRows(`SELECT id, CASE WHEN amount > 100 THEN 'big' END FROM it_orders ORDER BY id`,
		"1|NULL", "2|NULL", "3|NULL", "4|big", "5|NULL", "6|NULL")

	// Simple CASE; NULL operand matches no WHEN.
	h.expectRows(`SELECT id, CASE status WHEN 'shipped' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END AS code
		FROM it_orders ORDER BY id`,
		"1|1", "2|2", "3|1", "4|1", "5|0", "6|1")
	h.expectRows(`SELECT name, CASE country WHEN 'USA' THEN 'domestic' ELSE 'foreign' END FROM it_customers ORDER BY id`,
		"Ada|foreign", "Bo|domestic", "Cy|domestic", "Di|foreign")

	// Result type: mixed integer and decimal branches widen to decimal.
	schema := h.expectRows(`SELECT CASE WHEN id = 1 THEN 1 ELSE amount END AS v FROM it_orders WHERE id <= 2 ORDER BY id`,
		"1.00", "99.99")
	if dt := schema.Field(0).Type; dt.ID() != arrow.DECIMAL128 {
		t.Errorf("CASE result type = %s, want decimal", dt)
	}

	// CASE in WHERE, ORDER BY, and inside aggregates.
	h.expectRows(`SELECT id FROM it_orders WHERE CASE WHEN status = 'shipped' THEN qty ELSE 0 END > 1 ORDER BY id`,
		"3", "4", "6")
	h.expectRows(`SELECT id FROM it_orders ORDER BY CASE WHEN status = 'pending' THEN 0 ELSE 1 END, id DESC LIMIT 3`,
		"2", "6", "5")
	h.expectRows(`SELECT SUM(CASE WHEN status = 'shipped' THEN qty ELSE 0 END) AS shipped_qty,
			COUNT(CASE WHEN amount IS NULL THEN 1 END) AS missing
		FROM it_orders`,
		"17|1")

	// GROUP BY a CASE expression through its alias and ordinal.
	h.expectRows(`SELECT CASE WHEN qty >= 3 THEN 'large' ELSE 'small' END AS bucket, COUNT(*) AS n
		FROM it_orders GROUP BY bucket ORDER BY bucket`,
		"large|3", "small|3")
	h.expectRows(`SELECT CASE WHEN qty >= 3 THEN 'large' ELSE 'small' END, SUM(qty)
		FROM it_orders GROUP BY 1 ORDER BY 2 DESC`,
		"large|17", "small|4")

	// CASE without FROM.
	h.expectRows(`SELECT CASE WHEN 1 > 2 THEN 'no' ELSE 'yes' END`, "yes")
	h.expectError(`SELECT CASE END`, "WHEN")
}

func TestSQLHaving(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()

	// Index-side GROUPBY/REDUCE (integer SUM), filtered by HAVING.
	h.expectRows(`SELECT customer_id, COUNT(*) AS orders, SUM(qty) AS units
		FROM it_orders GROUP BY customer_id HAVING COUNT(*) > 1 ORDER BY customer_id`,
		"1|2|4", "2|2|12")
	// HAVING on an output alias, and on an aggregate not in the SELECT list.
	h.expectRows(`SELECT customer_id, SUM(qty) AS units FROM it_orders
		GROUP BY customer_id HAVING units >= 4 ORDER BY units DESC, customer_id`,
		"2|12", "1|4", "9|4")
	h.expectRows(`SELECT customer_id FROM it_orders GROUP BY customer_id HAVING MAX(qty) < 3 ORDER BY customer_id`,
		"3")
	// Driver-side aggregation (decimal SUM) with HAVING.
	h.expectRows(`SELECT status, SUM(amount) AS total FROM it_orders
		GROUP BY status HAVING SUM(amount) > 50 ORDER BY status`,
		"pending|99.99", "shipped|307.50")
	// HAVING combined with WHERE and a NULL-producing aggregate.
	h.expectRows(`SELECT status, AVG(amount) AS avg_amount FROM it_orders WHERE qty < 10
		GROUP BY status HAVING AVG(amount) IS NULL OR AVG(amount) < 30 ORDER BY status`,
		"returned|NULL", "shipped|19.166666666666668")
	// HAVING without GROUP BY: the whole table is one group.
	h.expectRows(`SELECT COUNT(*) FROM it_orders HAVING COUNT(*) > 5`, "6")
	h.expectRows(`SELECT COUNT(*) FROM it_orders HAVING COUNT(*) > 100`)
	// HAVING with CASE over aggregates.
	h.expectRows(`SELECT customer_id, CASE WHEN SUM(qty) > 5 THEN 'big' ELSE 'small' END AS tier
		FROM it_orders GROUP BY customer_id HAVING COUNT(*) >= 1 ORDER BY customer_id`,
		"1|small", "2|big", "3|small", "9|small")
}

func TestSQLCreateTableAs(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	h.exec("DROP TABLE IF EXISTS it_ctas_summary")
	h.exec("DROP TABLE IF EXISTS it_ctas_copy")
	t.Cleanup(func() {
		h.exec("DROP TABLE IF EXISTS it_ctas_summary")
		h.exec("DROP TABLE IF EXISTS it_ctas_copy")
	})

	// Filtered copy: column types carry over, rows affected = rows inserted.
	if n := h.exec(`CREATE TABLE it_ctas_copy AS SELECT id, amount, status FROM it_orders WHERE status = 'shipped'`); n != 4 {
		t.Errorf("CTAS rows affected = %d, want 4", n)
	}
	schema := h.expectRows(`SELECT * FROM it_ctas_copy ORDER BY id`,
		"1|10.50|shipped", "3|5.00|shipped", "4|250.00|shipped", "6|42.00|shipped")
	if got := schema.String(); !strings.Contains(got, "id: type=int32") || !strings.Contains(got, "amount: type=decimal(10, 2)") {
		t.Errorf("CTAS schema = %s", got)
	}
	// The new table is indexed like any other: pushdown filters work.
	h.expectRows(`SELECT id FROM it_ctas_copy WHERE amount > 20 ORDER BY amount DESC`, "4", "6")

	// Aggregate + CASE result, parenthesized query.
	h.exec(`CREATE TABLE it_ctas_summary AS (
		SELECT customer_id, COUNT(*) AS orders, SUM(amount) AS total,
			CASE WHEN SUM(qty) > 5 THEN 'big' ELSE 'small' END AS tier
		FROM it_orders GROUP BY customer_id)`)
	h.expectRows(`SELECT customer_id, orders, total, tier FROM it_ctas_summary ORDER BY customer_id`,
		"1|2|110.49|small", "2|2|255.00|big", "3|1|NULL|small", "9|1|42.00|small")
	h.expectRows(`SELECT tier, COUNT(*) FROM it_ctas_summary GROUP BY tier ORDER BY tier`, "big|1", "small|3")

	// IF NOT EXISTS leaves an existing table alone; without it, an error.
	h.exec(`CREATE TABLE IF NOT EXISTS it_ctas_copy AS SELECT id FROM it_orders`)
	h.expectRows(`SELECT COUNT(*) FROM it_ctas_copy`, "4")
	h.expectError(`CREATE TABLE it_ctas_copy AS SELECT id FROM it_orders`, "already exists")
}

func TestSQLSubqueries(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()

	// Uncorrelated scalar subqueries (run once; usable as a pushed-down constant).
	h.expectRows(`SELECT id FROM it_orders WHERE amount > (SELECT AVG(amount) FROM it_orders) ORDER BY id`, "2", "4")
	h.expectRows(`SELECT (SELECT MAX(qty) FROM it_orders) AS m`, "10")
	h.expectRows(`SELECT (SELECT id FROM it_orders WHERE id = 99)`, "NULL")
	h.expectError(`SELECT (SELECT id FROM it_orders)`, "returned 6 rows")
	h.expectError(`SELECT id FROM it_orders WHERE id IN (SELECT id, qty FROM it_orders)`, "subquery has too many columns")

	// IN / NOT IN with subqueries, including SQL NULL semantics.
	h.expectRows(`SELECT name FROM it_customers WHERE id IN (SELECT customer_id FROM it_orders WHERE status = 'shipped') ORDER BY name`,
		"Ada", "Bo")
	h.expectRows(`SELECT name FROM it_customers WHERE id NOT IN (SELECT customer_id FROM it_orders) ORDER BY name`, "Di")
	// The subquery returns {USA, NULL}: NOT IN is never true.
	h.expectRows(`SELECT name FROM it_customers WHERE country NOT IN (SELECT country FROM it_customers WHERE id > 2)`)
	h.expectRows(`SELECT id FROM it_orders WHERE customer_id IN (SELECT id FROM it_customers WHERE id > 100)`)

	// IN lists on indexed columns (index union queries).
	h.expectRows(`SELECT id FROM it_orders WHERE status IN ('pending', 'returned') ORDER BY id`, "2", "5")
	h.expectRows(`SELECT id FROM it_orders WHERE customer_id IN (1, 3) ORDER BY id`, "1", "2", "5")
	h.expectRows(`SELECT id FROM it_orders WHERE customer_id = 2 OR customer_id = 9 ORDER BY id`, "3", "4", "6")

	// Correlated EXISTS / NOT EXISTS.
	h.expectRows(`SELECT name FROM it_customers c
		WHERE EXISTS (SELECT 1 FROM it_orders o WHERE o.customer_id = c.id AND o.status = 'pending')`, "Ada")
	h.expectRows(`SELECT name FROM it_customers c
		WHERE NOT EXISTS (SELECT 1 FROM it_orders o WHERE o.customer_id = c.id) ORDER BY name`, "Di")

	// Correlated scalar subqueries in the SELECT list; the outer query must
	// fetch c.id even though it doesn't select it.
	h.expectRows(`SELECT c.name,
			(SELECT COUNT(*) FROM it_orders o WHERE o.customer_id = c.id) AS n,
			(SELECT SUM(o.amount) FROM it_orders o WHERE o.customer_id = c.id) AS total
		FROM it_customers c ORDER BY c.id`,
		"Ada|2|110.49", "Bo|2|255.00", "Cy|1|NULL", "Di|0|NULL")
	// Same table inside and out, told apart by aliases.
	h.expectRows(`SELECT id FROM it_orders o1
		WHERE amount > (SELECT AVG(amount) FROM it_orders o2 WHERE o2.customer_id = o1.customer_id) ORDER BY id`,
		"2", "4")
	// Two levels of nesting, with the innermost referring to the outermost.
	h.expectRows(`SELECT name FROM it_customers c
		WHERE EXISTS (SELECT 1 FROM it_orders o WHERE o.customer_id = c.id
			AND o.qty > (SELECT MIN(o2.qty) FROM it_orders o2 WHERE o2.customer_id = c.id))
		ORDER BY name`,
		"Ada", "Bo")
	h.expectError(`SELECT id FROM it_orders WHERE nope.id = 1`, "does not exist")
}

func TestSQLDerivedTablesAndCTEs(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()

	// Derived tables.
	// An IN list on a joined (alias-qualified) column.
	h.expectRows(`SELECT o.id FROM it_orders o JOIN it_customers c ON c.id = o.customer_id
		WHERE c.country IN ('GBR', 'XXX') ORDER BY o.id`, "1", "2")
	h.expectRows(`SELECT tier, COUNT(*) FROM (
			SELECT customer_id, CASE WHEN SUM(qty) > 5 THEN 'big' ELSE 'small' END AS tier
			FROM it_orders GROUP BY customer_id) AS t
		GROUP BY tier ORDER BY tier`,
		"big|1", "small|3")
	h.expectRows(`SELECT t.n FROM (SELECT COUNT(*) AS n FROM it_orders) t`, "6")
	// A SELECT alias isn't visible in the same query's WHERE (standard SQL).
	h.expectError(`SELECT id, qty * 2 AS dbl FROM it_orders WHERE dbl > 7`, "does not exist")
	h.expectRows(`SELECT id, dbl FROM (SELECT id, qty * 2 AS dbl FROM it_orders) q WHERE dbl > 7 ORDER BY dbl DESC`, "4|20", "6|8")
	h.expectError(`SELECT * FROM (SELECT id, id FROM it_orders) x`, "more than once")

	// CTEs: filtering, chaining, column lists, reuse, use inside subqueries.
	h.expectRows(`WITH totals AS (SELECT customer_id, SUM(amount) AS total FROM it_orders GROUP BY customer_id)
		SELECT customer_id, total FROM totals WHERE total > 100 ORDER BY total DESC`,
		"2|255.00", "1|110.49")
	h.expectRows(`WITH shipped AS (SELECT * FROM it_orders WHERE status = 'shipped'),
			big AS (SELECT id FROM shipped WHERE qty >= 4)
		SELECT id FROM big ORDER BY id`,
		"4", "6")
	h.expectRows(`WITH x(a, b) AS (SELECT id, qty FROM it_orders WHERE id <= 2) SELECT a + b FROM x ORDER BY 1`, "2", "5")
	h.expectRows(`WITH vip AS (SELECT customer_id FROM it_orders GROUP BY customer_id HAVING SUM(qty) > 5)
		SELECT name FROM it_customers WHERE id IN (SELECT customer_id FROM vip)`, "Bo")
	h.expectRows(`WITH s AS (SELECT qty FROM it_orders) SELECT (SELECT MAX(qty) FROM s) - (SELECT MIN(qty) FROM s)`, "9")
	h.expectRows(`WITH c AS (SELECT id, name FROM it_customers)
		SELECT c.name, (SELECT COUNT(*) FROM it_orders o WHERE o.customer_id = c.id) FROM c ORDER BY c.id`,
		"Ada|2", "Bo|2", "Cy|1", "Di|0")
	h.expectRows(`WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r`, "1")
	h.expectError(`WITH r AS (SELECT * FROM r) SELECT * FROM r`, "recursive")
	h.expectError(`WITH x(a) AS (SELECT id, qty FROM it_orders) SELECT * FROM x`, "column names")

	// CTAS from a CTE.
	h.exec("DROP TABLE IF EXISTS it_ctas_cte")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_ctas_cte") })
	if n := h.exec(`CREATE TABLE it_ctas_cte AS WITH t AS (SELECT customer_id, COUNT(*) AS n FROM it_orders GROUP BY customer_id)
		SELECT * FROM t WHERE n > 1`); n != 2 {
		t.Errorf("CTAS from CTE inserted %d rows, want 2", n)
	}
	h.expectRows(`SELECT customer_id, n FROM it_ctas_cte ORDER BY customer_id`, "1|2", "2|2")
}

func TestSQLSubqueryDML(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	h.exec("DROP TABLE IF EXISTS it_archive")
	h.exec("CREATE TABLE it_archive (id INTEGER, amount NUMERIC(10,2))")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_archive") })

	// INSERT … SELECT, with and without a column list.
	if n := h.exec(`INSERT INTO it_archive SELECT id, amount FROM it_orders WHERE status = 'shipped'`); n != 4 {
		t.Errorf("INSERT SELECT inserted %d rows, want 4", n)
	}
	h.exec(`INSERT INTO it_archive (id) SELECT customer_id FROM it_orders WHERE id = 5`)
	h.expectRows(`SELECT id, amount FROM it_archive ORDER BY id, amount`,
		"1|10.50", "3|5.00", "3|NULL", "4|250.00", "6|42.00") // NULLs sort last

	// UPDATE / DELETE filtered by subqueries, and a correlated SET value.
	if n := h.exec(`UPDATE it_orders SET status = 'vip' WHERE customer_id IN (SELECT id FROM it_customers WHERE country = 'GBR')`); n != 2 {
		t.Errorf("UPDATE matched %d rows, want 2", n)
	}
	if n := h.exec(`DELETE FROM it_orders WHERE NOT EXISTS (SELECT 1 FROM it_customers c WHERE c.id = it_orders.customer_id)`); n != 1 {
		t.Errorf("DELETE matched %d rows, want 1", n)
	}
	h.exec(`UPDATE it_customers SET country = (SELECT MAX(o.status) FROM it_orders o WHERE o.customer_id = it_customers.id) WHERE id = 3`)
	h.expectRows(`SELECT id, status FROM it_orders ORDER BY id`, "1|vip", "2|vip", "3|shipped", "4|shipped", "5|returned")
	h.expectRows(`SELECT country FROM it_customers WHERE id = 3`, "returned")
}

// subqueryWork is the subquery work done by one statement (subqueryStats).
type subqueryWork struct{ runs, semiJoins, inSets int64 }

// expectWork runs a query, checks its rows, and returns the subquery work it
// did.
func (h *sqlHarness) expectWork(sql string, want ...string) subqueryWork {
	h.t.Helper()
	r, s, i := subqueryStats.runs.Load(), subqueryStats.semiJoins.Load(), subqueryStats.inSets.Load()
	h.expectRows(sql, want...)
	return subqueryWork{subqueryStats.runs.Load() - r, subqueryStats.semiJoins.Load() - s, subqueryStats.inSets.Load() - i}
}

// IN (SELECT …) probes a hash set of the subquery's values; the answers must
// be those of comparing one by one, including NULLs and cross-type equality.
func TestSQLInSubqueryHashSet(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP TABLE IF EXISTS it_hs_keys")
		h.exec("DROP TABLE IF EXISTS it_hs_probe")
		h.exec("DROP TABLE IF EXISTS it_hs_many")
	}
	drop()
	t.Cleanup(drop)
	// NOINDEX: the predicates are evaluated by the driver, not by an index
	// union. Twelve values (k = 10, 20, …, 120; f alternates 10.5, 20, 30.5,
	// …) plus a row of NULLs.
	h.exec(`CREATE TABLE it_hs_keys (i INTEGER, k BIGINT NOINDEX, d NUMERIC(10,2) NOINDEX,
		f DOUBLE NOINDEX, s VARCHAR NOINDEX)`)
	var vals []string
	for i := 1; i <= 12; i++ {
		vals = append(vals, fmt.Sprintf("(%d, %d, %d.00, %g, 'k%d')", i, 10*i, 10*i, float64(10*i)+float64(i%2)*0.5, i))
	}
	h.exec("INSERT INTO it_hs_keys VALUES " + strings.Join(vals, ", ") + ", (13, NULL, NULL, NULL, NULL)")
	h.exec(`CREATE TABLE it_hs_probe (id INTEGER, n BIGINT NOINDEX, d NUMERIC(10,2) NOINDEX,
		f DOUBLE NOINDEX, s VARCHAR NOINDEX)`)
	h.exec(`INSERT INTO it_hs_probe VALUES (1, 10, 10.00, 10.0, 'k1'), (2, 20, 30.50, 30.5, 'k2'),
		(3, 25, 25.00, 25.0, 'zz'), (4, NULL, NULL, NULL, NULL), (5, 120, 120.00, 120.0, 'k12')`)

	// Each subquery runs once and builds one hash set, whatever the number of
	// outer rows.
	w := h.expectWork(`SELECT id, n IN (SELECT k FROM it_hs_keys WHERE k IS NOT NULL),
			n NOT IN (SELECT k FROM it_hs_keys WHERE k IS NOT NULL)
		FROM it_hs_probe ORDER BY id`,
		"1|true|false", "2|true|false", "3|false|true", "4|NULL|NULL", "5|true|false")
	if w.runs != 2 || w.inSets != 2 {
		t.Errorf("IN subqueries: %+v, want 2 runs and 2 hash sets", w)
	}
	// A NULL in the subquery: no match is NULL, so NOT IN is never true.
	h.expectRows(`SELECT id, n IN (SELECT k FROM it_hs_keys), n NOT IN (SELECT k FROM it_hs_keys)
		FROM it_hs_probe ORDER BY id`,
		"1|true|false", "2|true|false", "3|NULL|NULL", "4|NULL|NULL", "5|true|false")
	h.expectRows(`SELECT id FROM it_hs_probe WHERE n NOT IN (SELECT k FROM it_hs_keys)`)
	h.expectRows(`SELECT id FROM it_hs_probe WHERE n NOT IN (SELECT k FROM it_hs_keys WHERE k IS NOT NULL)`, "3")
	h.expectRows(`SELECT id FROM it_hs_probe WHERE n IN (SELECT k FROM it_hs_keys) ORDER BY id`, "1", "2", "5")
	// Cross-type equality: integers, decimals (10 = 10.00) and doubles
	// (20 = 20.0, 30.50 = 30.5, but 10 <> 10.5).
	h.expectRows(`SELECT id,
			n IN (SELECT d FROM it_hs_keys WHERE i <= 12), d IN (SELECT k FROM it_hs_keys WHERE i <= 12),
			n IN (SELECT f FROM it_hs_keys WHERE i <= 12), f IN (SELECT k FROM it_hs_keys WHERE i <= 12),
			d IN (SELECT f FROM it_hs_keys WHERE i <= 12), f IN (SELECT d FROM it_hs_keys WHERE i <= 12)
		FROM it_hs_probe ORDER BY id`,
		"1|true|true|false|true|false|true", "2|true|false|true|false|true|false",
		"3|false|false|false|false|false|false", "4|NULL|NULL|NULL|NULL|NULL|NULL", "5|true|true|true|true|true|true")
	// Strings, and a string compared with numbers (converted, as before).
	h.expectRows(`SELECT id, s IN (SELECT s FROM it_hs_keys WHERE i <= 12),
			CAST(n AS VARCHAR) IN (SELECT k FROM it_hs_keys WHERE i <= 12)
		FROM it_hs_probe ORDER BY id`,
		"1|true|true", "2|true|true", "3|false|false", "4|NULL|NULL", "5|true|true")

	// More than maxUnionTerms values on an indexed column: no index union,
	// every row is checked against the set.
	h.exec(`CREATE TABLE it_hs_many AS SELECT a.k * 1000000 + b.k * 1000 + c.k AS v, c.k AS c
		FROM it_hs_keys a, it_hs_keys b, it_hs_keys c WHERE a.i <= 12 AND b.i <= 12 AND c.i <= 12`)
	w = h.expectWork(`SELECT COUNT(*) FROM it_hs_many WHERE v IN (SELECT v FROM it_hs_many WHERE c <= 100)`, "1440")
	if w.runs != 1 || w.inSets != 1 {
		t.Errorf("large IN subquery: %+v, want 1 run and 1 hash set", w)
	}
	h.expectRows(`SELECT COUNT(*) FROM it_hs_many WHERE v NOT IN (SELECT v FROM it_hs_many WHERE c <= 100)`, "288")
	h.expectRows(`SELECT COUNT(*) FROM it_hs_many WHERE v + 0.0 IN (SELECT v FROM it_hs_many WHERE c <= 100)`, "1440")
}

// Correlated EXISTS / IN with only `inner = outer` correlation run once as a
// semi-join; the results must be those of running them per outer row.
func TestSQLSemiJoins(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_sj_v")
		for _, name := range []string{"it_sj_o", "it_sj_i", "it_sj_big", "it_sj_mo", "it_sj_tmp", "it_sj_tmp2"} {
			h.exec("DROP TABLE IF EXISTS " + name)
		}
	}
	drop()
	t.Cleanup(drop)
	h.exec("CREATE TABLE it_sj_o (id INTEGER, k BIGINT, g VARCHAR, x INTEGER)")
	h.exec(`INSERT INTO it_sj_o VALUES (1, 1, 'a', 10), (2, 2, 'a', 20), (3, 2, 'b', 30),
		(4, 3, 'a', NULL), (5, NULL, 'a', 50), (6, 4, NULL, 60), (7, 5, 'c', 70)`)
	h.exec("CREATE TABLE it_sj_i (k BIGINT, g VARCHAR, v INTEGER, kd NUMERIC(10,2), z INTEGER)")
	h.exec(`INSERT INTO it_sj_i VALUES (1, 'a', 10, 1.00, 1), (2, 'b', 20, 2.00, 1), (2, 'b', NULL, 2.00, 1),
		(3, 'x', 31, 3.00, 1), (NULL, 'a', 50, NULL, 1), (4, NULL, 60, 4.00, 1), (9, 'c', 70, 9.00, 0)`)

	semi := func(w subqueryWork, what string, semiJoins int64) {
		t.Helper()
		if w.semiJoins != semiJoins || w.runs != 0 {
			t.Errorf("%s: %+v, want %d semi-join(s) and no per-row runs", what, w, semiJoins)
		}
	}
	perRow := func(w subqueryWork, what string) {
		t.Helper()
		if w.semiJoins != 0 || w.runs < 2 {
			t.Errorf("%s: %+v, want per-row runs and no semi-join", what, w)
		}
	}

	// EXISTS / NOT EXISTS: a NULL key never matches. Each runs the inner
	// query once, not once per outer row.
	semi(h.expectWork(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k) ORDER BY id`,
		"1", "2", "3", "4", "6"), "EXISTS", 1)
	semi(h.expectWork(`SELECT id FROM it_sj_o o WHERE NOT EXISTS (SELECT * FROM it_sj_i i WHERE o.k = i.k) ORDER BY id`,
		"5", "7"), "NOT EXISTS", 1)
	semi(h.expectWork(`SELECT id, EXISTS (SELECT i.v FROM it_sj_i i WHERE i.k = o.k LIMIT 1) FROM it_sj_o o ORDER BY id`,
		"1|true", "2|true", "3|true", "4|true", "5|false", "6|true", "7|false"), "EXISTS in SELECT", 1)
	// Composite keys (NULL = NULL is not a match), extra inner conditions, a
	// decimal key against an integer one, and a CTE.
	semi(h.expectWork(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k AND i.g = o.g) ORDER BY id`,
		"1", "3"), "composite EXISTS", 1)
	h.expectRows(`SELECT id FROM it_sj_o o WHERE NOT EXISTS (SELECT 1 FROM it_sj_i i WHERE i.g = o.g AND o.k = i.k) ORDER BY id`,
		"2", "4", "5", "6", "7")
	h.expectRows(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k AND i.v > 15) ORDER BY id`,
		"2", "3", "4", "6")
	semi(h.expectWork(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.kd = o.k) ORDER BY id`,
		"1", "2", "3", "4", "6"), "decimal = integer", 1)
	h.expectRows(`WITH ks AS (SELECT k FROM it_sj_i WHERE v >= 20)
		SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM ks WHERE ks.k = o.k) ORDER BY id`,
		"2", "3", "4", "6")
	// Temporary tables, inside the subquery and as the outer table.
	h.exec("CREATE TEMP TABLE it_sj_tmp (k BIGINT, g VARCHAR)")
	h.exec("INSERT INTO it_sj_tmp VALUES (1, 'a'), (2, 'b'), (NULL, 'a')")
	semi(h.expectWork(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_tmp t WHERE t.k = o.k AND t.g = o.g) ORDER BY id`,
		"1", "3"), "temp table EXISTS", 1)
	semi(h.expectWork(`SELECT id FROM it_sj_o o WHERE o.k IN (SELECT t.k FROM it_sj_tmp t WHERE t.g = o.g) ORDER BY id`,
		"1", "3"), "temp table IN", 1)
	h.exec("CREATE TEMP TABLE it_sj_tmp2 (k BIGINT)")
	h.exec("INSERT INTO it_sj_tmp2 VALUES (1), (5), (9), (NULL)")
	if n := h.exec(`DELETE FROM it_sj_tmp2 WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = it_sj_tmp2.k)`); n != 2 {
		t.Errorf("DELETE from a temp table … WHERE EXISTS matched %d rows, want 2", n)
	}
	h.expectRows(`SELECT k FROM it_sj_tmp2 ORDER BY k`, "5", "NULL")
	// The outer query is a join, or a view.
	h.expectRows(`SELECT o.id FROM it_sj_o o JOIN it_sj_o p ON p.id = o.id
		WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k AND i.g = p.g) ORDER BY o.id`, "1", "3")
	h.exec(`CREATE VIEW it_sj_v AS SELECT id, k FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k)`)
	semi(h.expectWork(`SELECT id FROM it_sj_v WHERE k >= 2 ORDER BY id`, "2", "3", "4", "6"), "view", 1)
	// A semi-join inside a semi-join (correlated to the inner query only).
	semi(h.expectWork(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i
			WHERE i.k = o.k AND EXISTS (SELECT 1 FROM it_sj_i j WHERE j.k = i.k AND j.v IS NULL)) ORDER BY id`,
		"2", "3"), "nested semi-joins", 2)
	// No inner rows: EXISTS is false for every row and NOT EXISTS true.
	semi(h.expectWork(`SELECT COUNT(*) FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k AND i.v < 0)`,
		"0"), "empty EXISTS", 1)
	h.expectRows(`SELECT COUNT(*) FROM it_sj_o o WHERE NOT EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k AND i.v < 0)`, "7")

	// Correlated [NOT] IN: SQL's three-valued logic over each outer row's
	// values (k = 2 gives {20, NULL}; k = NULL or 5 gives no rows).
	semi(h.expectWork(`SELECT id, x IN (SELECT v FROM it_sj_i i WHERE i.k = o.k),
			x NOT IN (SELECT v FROM it_sj_i i WHERE i.k = o.k)
		FROM it_sj_o o ORDER BY id`,
		"1|true|false", "2|true|false", "3|NULL|NULL", "4|NULL|NULL", "5|false|true", "6|true|false", "7|false|true"),
		"correlated IN", 2)
	h.expectRows(`SELECT id FROM it_sj_o o WHERE x IN (SELECT v FROM it_sj_i i WHERE i.k = o.k) ORDER BY id`, "1", "2", "6")
	h.expectRows(`SELECT id FROM it_sj_o o WHERE x NOT IN (SELECT v FROM it_sj_i i WHERE i.k = o.k) ORDER BY id`, "5", "7")
	h.expectRows(`SELECT id FROM it_sj_o o WHERE g IN (SELECT i.g FROM it_sj_i i WHERE i.kd = o.k) ORDER BY id`, "1", "3")

	// Shapes that keep running per outer row, with the same results as ever:
	// non-equality or OR correlation, an aggregate, LIMIT in IN, an outer
	// column in the select list or in a nested subquery, keys of different
	// types, a join in the subquery, RANDOM().
	perRow(h.expectWork(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k > o.k) ORDER BY id`,
		"1", "2", "3", "4", "6", "7"), "non-equality")
	perRow(h.expectWork(`SELECT COUNT(*) FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k OR i.v = o.x)`,
		"7"), "OR")
	perRow(h.expectWork(`SELECT COUNT(*) FROM it_sj_o o WHERE EXISTS (SELECT COUNT(*) FROM it_sj_i i WHERE i.k = o.k)`,
		"7"), "aggregate")
	perRow(h.expectWork(`SELECT id, x IN (SELECT v FROM it_sj_i i WHERE i.k = o.k ORDER BY v LIMIT 1) FROM it_sj_o o ORDER BY id`,
		"1|true", "2|true", "3|false", "4|NULL", "5|false", "6|true", "7|false"), "LIMIT")
	perRow(h.expectWork(`SELECT id, x IN (SELECT v + o.id - o.id FROM it_sj_i i WHERE i.k = o.k) FROM it_sj_o o ORDER BY id`,
		"1|true", "2|true", "3|NULL", "4|NULL", "5|false", "6|true", "7|false"), "outer column in the select list")
	perRow(h.expectWork(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i
			WHERE i.k = o.k AND EXISTS (SELECT 1 FROM it_sj_i j WHERE j.v = o.x)) ORDER BY id`,
		"1", "2", "6"), "nested correlation")
	perRow(h.expectWork(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE CAST(i.k AS DOUBLE) = o.k) ORDER BY id`,
		"1", "2", "3", "4", "6"), "double = integer")
	perRow(h.expectWork(`SELECT id FROM it_sj_o o
			WHERE EXISTS (SELECT 1 FROM it_sj_i i JOIN it_sj_i j ON j.k = i.k WHERE j.v IS NULL AND i.k = o.k) ORDER BY id`,
		"2", "3"), "join in the subquery")
	perRow(h.expectWork(`SELECT id FROM it_sj_o o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k AND RANDOM() >= 0) ORDER BY id`,
		"1", "2", "3", "4", "6"), "volatile function")
	// The inner query fails on a row (10 / 0) that no outer row matches:
	// the semi-join gives up and the per-row path runs, as before.
	w := h.expectWork(`SELECT id FROM it_sj_o o
		WHERE o.k IS NOT NULL AND EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = o.k AND 10 / i.z > 1) ORDER BY id`,
		"1", "2", "3", "4", "6")
	if w.semiJoins != 1 || w.runs < 2 {
		t.Errorf("failing semi-join: %+v, want 1 semi-join attempt, then per-row runs", w)
	}

	// A large inner side (2,000 rows) is read only once running per outer
	// row has cost about as much (2000 / rowsPerRun = 20 runs); a few outer
	// rows never pay for it.
	var big []string
	for k := 1; k <= 2000; k++ {
		big = append(big, fmt.Sprintf("(%d)", k))
	}
	h.exec("CREATE TABLE it_sj_big (k BIGINT)")
	h.exec("INSERT INTO it_sj_big VALUES " + strings.Join(big, ", "))
	h.exec("CREATE TABLE it_sj_mo (id INTEGER, k BIGINT)")
	var mo []string
	for id := 1; id <= 100; id++ {
		mo = append(mo, fmt.Sprintf("(%d, %d)", id, id*25))
	}
	h.exec("INSERT INTO it_sj_mo VALUES " + strings.Join(mo, ", "))
	if w := h.expectWork(`SELECT COUNT(*) FROM it_sj_mo o WHERE EXISTS (SELECT 1 FROM it_sj_big b WHERE b.k = o.k)`,
		"80"); w.runs != 2000/rowsPerRun || w.semiJoins != 1 {
		t.Errorf("large inner side: %+v, want %d per-row runs, then 1 semi-join", w, 2000/rowsPerRun)
	}
	h.expectRows(`SELECT COUNT(*) FROM it_sj_mo o WHERE NOT EXISTS (SELECT 1 FROM it_sj_big b WHERE b.k = o.k)`, "20")
	if w := h.expectWork(`SELECT COUNT(*) FROM it_sj_mo o WHERE o.id <= 5 AND EXISTS (SELECT 1 FROM it_sj_big b WHERE b.k = o.k)`,
		"5"); w.runs != 5 || w.semiJoins != 0 {
		t.Errorf("large inner side, 5 outer rows: %+v, want 5 per-row runs", w)
	}

	// UPDATE and DELETE.
	if n := h.exec(`UPDATE it_sj_o SET x = 0 WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = it_sj_o.k AND i.g = it_sj_o.g)`); n != 2 {
		t.Errorf("UPDATE … WHERE EXISTS matched %d rows, want 2", n)
	}
	if n := h.exec(`UPDATE it_sj_o SET g = 'in' WHERE x IN (SELECT v FROM it_sj_i i WHERE i.k = it_sj_o.k)`); n != 2 {
		t.Errorf("UPDATE … WHERE IN matched %d rows, want 2", n)
	}
	h.exec(`UPDATE it_sj_o SET x = CASE WHEN EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = it_sj_o.k) THEN x ELSE -1 END`)
	h.expectRows(`SELECT id, g, x FROM it_sj_o ORDER BY id`,
		"1|a|0", "2|in|20", "3|b|0", "4|a|NULL", "5|a|-1", "6|in|60", "7|c|-1")
	if n := h.exec(`DELETE FROM it_sj_o WHERE EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = it_sj_o.k AND i.v > 1000)`); n != 0 {
		t.Errorf("DELETE … WHERE EXISTS (no rows) matched %d rows, want 0", n)
	}
	if n := h.exec(`DELETE FROM it_sj_o WHERE NOT EXISTS (SELECT 1 FROM it_sj_i i WHERE i.k = it_sj_o.k)`); n != 2 {
		t.Errorf("DELETE … WHERE NOT EXISTS matched %d rows, want 2", n)
	}
	h.expectRows(`SELECT id, g, x FROM it_sj_o ORDER BY id`, "1|a|0", "2|in|20", "3|b|0", "4|a|NULL", "6|in|60")
}

func TestSQLJoins(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	h.exec("DROP TABLE IF EXISTS it_regions")
	h.exec("CREATE TABLE it_regions (country VARCHAR, region VARCHAR)")
	h.exec("INSERT INTO it_regions VALUES ('GBR', 'Europe'), ('USA', 'Americas')")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_regions") })

	// INNER JOIN, explicit and as a comma join with the condition in WHERE.
	h.expectRows(`SELECT o.id, c.name FROM it_orders o JOIN it_customers c ON o.customer_id = c.id ORDER BY o.id`,
		"1|Ada", "2|Ada", "3|Bo", "4|Bo", "5|Cy")
	h.expectRows(`SELECT o.id, c.name FROM it_orders o, it_customers c
		WHERE o.customer_id = c.id AND c.country = 'USA' ORDER BY o.id`,
		"3|Bo", "4|Bo", "5|Cy")
	h.expectRows(`SELECT c.name, r.region FROM it_customers c JOIN it_regions r USING (country) ORDER BY c.name`,
		"Ada|Europe", "Bo|Americas", "Cy|Americas")

	// LEFT JOIN: unmatched rows, the anti-join idiom, and ON vs WHERE filters.
	h.expectRows(`SELECT c.name, o.id FROM it_customers c LEFT JOIN it_orders o ON o.customer_id = c.id ORDER BY c.id, o.id`,
		"Ada|1", "Ada|2", "Bo|3", "Bo|4", "Cy|5", "Di|NULL")
	h.expectRows(`SELECT c.name FROM it_customers c LEFT JOIN it_orders o ON o.customer_id = c.id WHERE o.id IS NULL`,
		"Di")
	h.expectRows(`SELECT c.name, o.id FROM it_customers c
		LEFT JOIN it_orders o ON o.customer_id = c.id AND o.status = 'pending' ORDER BY c.id`,
		"Ada|2", "Bo|NULL", "Cy|NULL", "Di|NULL")
	h.expectRows(`SELECT c.name, o.id FROM it_customers c
		LEFT JOIN it_orders o ON o.customer_id = c.id WHERE o.status = 'pending'`,
		"Ada|2")

	// RIGHT and FULL JOIN.
	h.expectRows(`SELECT c.name, o.id FROM it_customers c RIGHT JOIN it_orders o ON o.customer_id = c.id ORDER BY o.id`,
		"Ada|1", "Ada|2", "Bo|3", "Bo|4", "Cy|5", "NULL|6")
	h.expectRows(`SELECT c.name, o.id FROM it_customers c FULL JOIN it_orders o ON o.customer_id = c.id ORDER BY o.id, c.name`,
		"Ada|1", "Ada|2", "Bo|3", "Bo|4", "Cy|5", "NULL|6", "Di|NULL")
	h.expectRows(`SELECT COUNT(*) FROM it_customers CROSS JOIN it_regions`, "8")

	// Three tables, grouped; HAVING over a join; a self join.
	h.expectRows(`SELECT c.name, r.region, SUM(o.qty) FROM it_orders o
		JOIN it_customers c ON o.customer_id = c.id
		JOIN it_regions r ON r.country = c.country
		GROUP BY c.name, r.region ORDER BY c.name`,
		"Ada|Europe|4", "Bo|Americas|12", "Cy|Americas|1")
	h.expectRows(`SELECT c.country, COUNT(*) AS n FROM it_orders o JOIN it_customers c ON o.customer_id = c.id
		GROUP BY c.country HAVING COUNT(*) > 1 ORDER BY c.country`,
		"GBR|2", "USA|3")
	h.expectRows(`SELECT a.id, b.id FROM it_orders a JOIN it_orders b ON a.customer_id = b.customer_id AND a.id < b.id ORDER BY a.id`,
		"1|2", "3|4")

	// Joining a CTE, SELECT * naming, and a correlated subquery over a join.
	h.expectRows(`WITH t AS (SELECT customer_id, SUM(qty) AS units FROM it_orders GROUP BY customer_id)
		SELECT c.name, t.units FROM t JOIN it_customers c ON c.id = t.customer_id ORDER BY t.units DESC`,
		"Bo|12", "Ada|4", "Cy|1")
	schema := h.expectRows(`SELECT * FROM it_customers c JOIN it_regions r USING (country) WHERE c.id = 1`,
		"1|Ada|GBR|GBR|Europe")
	var names []string
	for _, f := range schema.Fields() {
		names = append(names, f.Name)
	}
	if got := strings.Join(names, ","); got != "id,name,country,country,region" {
		t.Errorf("SELECT * column names = %s", got)
	}
	h.expectRows(`SELECT c.name FROM it_customers c JOIN it_regions r ON r.country = c.country
		WHERE EXISTS (SELECT 1 FROM it_orders o WHERE o.customer_id = c.id AND o.qty > 5)`,
		"Bo")

	// Errors.
	h.expectError(`SELECT id FROM it_orders o JOIN it_customers c ON o.customer_id = c.id`, "ambiguous")
	h.expectError(`SELECT 1 FROM it_orders JOIN it_orders ON 1 = 1`, "more than once")
	h.expectError(`SELECT 1 FROM it_orders o JOIN it_customers c`, "requires ON or USING")
	h.expectError(`SELECT 1 FROM it_orders o JOIN (SELECT id FROM it_customers) ON 1 = 1`, "must have an alias")
}

func TestSQLViews(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	views := []string{"v_big", "v_shipped", "v_totals", "v_cust", "secondary.v_o2", "v_tmp"}
	drop := func() {
		for _, v := range views {
			h.exec("DROP VIEW IF EXISTS " + v)
		}
		h.exec("DROP TABLE IF EXISTS it_view_base")
	}
	drop()
	t.Cleanup(drop)

	// A simple (lazy) view; outer filters combine with the view's own WHERE.
	h.exec(`CREATE VIEW v_shipped AS SELECT id, customer_id, amount, qty FROM it_orders WHERE status = 'shipped'`)
	h.expectRows(`SELECT id FROM v_shipped WHERE qty >= 4 ORDER BY id`, "4", "6")
	h.expectRows(`SELECT COUNT(*) FROM v_shipped`, "4")
	// Column list renames the output; expressions are fine too.
	h.exec(`CREATE VIEW v_totals (oid, total) AS SELECT id, amount * qty FROM it_orders`)
	h.expectRows(`SELECT oid, total FROM v_totals WHERE total > 250 ORDER BY oid`, "2|299.97", "4|2500.00")
	// A grouped (computed) view.
	h.exec(`CREATE VIEW v_cust AS SELECT customer_id, COUNT(*) AS n, SUM(qty) AS units FROM it_orders GROUP BY customer_id`)
	h.expectRows(`SELECT customer_id FROM v_cust WHERE units > 3 ORDER BY customer_id`, "1", "2", "9")

	// Views in joins, in subqueries, and views of views.
	h.expectRows(`SELECT c.name, v.units FROM it_customers c JOIN v_cust v ON v.customer_id = c.id ORDER BY c.id`,
		"Ada|4", "Bo|12", "Cy|1")
	h.expectRows(`SELECT c.name, s.id FROM v_shipped s JOIN it_customers c ON c.id = s.customer_id ORDER BY s.id`,
		"Ada|1", "Bo|3", "Bo|4")
	h.expectRows(`SELECT name FROM it_customers WHERE id IN (SELECT customer_id FROM v_shipped) ORDER BY name`, "Ada", "Bo")
	h.exec(`CREATE VIEW v_big AS SELECT id FROM v_shipped WHERE amount > 20`)
	h.expectRows(`SELECT id FROM v_big ORDER BY id`, "4", "6")

	// OR REPLACE, IF NOT EXISTS, and name clashes.
	h.exec(`CREATE OR REPLACE VIEW v_big AS SELECT id FROM v_shipped WHERE amount > 100`)
	h.expectRows(`SELECT id FROM v_big`, "4")
	h.exec(`CREATE VIEW IF NOT EXISTS v_big AS SELECT 1 AS id`)
	h.expectRows(`SELECT id FROM v_big`, "4")
	h.expectError(`CREATE VIEW v_big AS SELECT 1 AS id`, "already exists")
	h.expectError(`CREATE TABLE v_big (a INT)`, "already exists as a view")
	h.expectError(`CREATE VIEW it_orders AS SELECT 1 AS a`, "already exists as a table")
	h.expectError(`CREATE VIEW v_dup AS SELECT id, id FROM it_orders`, "more than once")
	h.expectError(`CREATE VIEW v_bad AS SELECT nope FROM it_orders`, "does not exist")

	// Views live in a schema; unqualified names inside resolve there.
	h.exec("CREATE SCHEMA IF NOT EXISTS secondary")
	h.expectError(`CREATE VIEW secondary.v_o AS SELECT id FROM it_orders`, "does not exist")
	h.exec(`CREATE VIEW secondary.v_o2 AS SELECT id FROM public.it_orders`)
	h.expectRows(`SELECT COUNT(*) FROM secondary.v_o2`, "6")

	// Schema reporting.
	schema, err := h.conn.GetTableSchema(h.ctx, nil, nil, "v_totals")
	if err != nil {
		t.Fatal(err)
	}
	if got := schema.String(); !strings.Contains(got, "oid: type=int32") || !strings.Contains(got, "total: type=decimal") {
		t.Errorf("view schema = %s", got)
	}

	// A view whose base table is dropped reports why it broke.
	h.exec(`CREATE TABLE it_view_base (a INT)`)
	h.exec(`CREATE VIEW v_tmp AS SELECT a FROM it_view_base`)
	h.exec(`DROP TABLE it_view_base`)
	h.expectError(`SELECT * FROM v_tmp`, "no longer valid")

	h.exec(`DROP VIEW v_tmp`)
	h.expectError(`SELECT * FROM v_tmp`, "does not exist")
	h.exec(`DROP VIEW IF EXISTS v_tmp`)
}

// adminURI is the URI tests use to inspect and set up the database
// themselves: REDIS_ADMIN_URI if set, else REDIS_URI. Setting it lets the
// driver run as an ACL user that has only the commands it needs (see
// acl_integration_test.go), while the tests' own commands (KEYS, ACL, …)
// use another user.
func adminURI() string {
	if uri := os.Getenv("REDIS_ADMIN_URI"); uri != "" {
		return uri
	}
	return os.Getenv("REDIS_URI")
}

// rawClient connects to adminURI directly (cluster-aware) so tests can
// inspect row HASHes.
func (h *sqlHarness) rawClient() goredis.UniversalClient {
	h.t.Helper()
	opts, err := goredis.ParseURL(adminURI())
	if err != nil {
		h.t.Fatal(err)
	}
	opts.Protocol = 2 // the driver's reply parsing expects RESP2
	var c goredis.UniversalClient = goredis.NewClient(opts)
	if info, err := c.Info(h.ctx, "cluster").Result(); err == nil && strings.Contains(info, "cluster_enabled:1") {
		_ = c.Close()
		c = goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{opts.Addr}, Password: opts.Password,
			Username: opts.Username, TLSConfig: opts.TLSConfig, Protocol: 2,
			ReadTimeout: opts.ReadTimeout, WriteTimeout: opts.WriteTimeout})
	}
	h.t.Cleanup(func() { _ = c.Close() })
	return c
}

// rowFields returns the field names of every row HASH under a table prefix.
func (h *sqlHarness) rowFields(c goredis.UniversalClient, schema, table string) map[string]int {
	h.t.Helper()
	st := &store{client: c}
	meta, err := st.getTable(h.ctx, schema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	rows, err := st.aggregate(h.ctx, &aggRequest{index: meta.index(), query: "*", load: []string{"__key"}})
	if err != nil {
		h.t.Fatal(err)
	}
	counts := map[string]int{}
	for _, r := range rows {
		fields, err := c.HKeys(h.ctx, r["__key"]).Result()
		if err != nil {
			h.t.Fatal(err)
		}
		for _, f := range fields {
			counts[f]++
		}
	}
	return counts
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSQLAlterTable(t *testing.T) {
	h := newSQLHarness(t)
	for _, n := range []string{"it_alter", "it_alter2", "it_alter3"} {
		h.exec("DROP TABLE IF EXISTS " + n)
	}
	t.Cleanup(func() {
		for _, n := range []string{"it_alter", "it_alter2", "it_alter3"} {
			h.exec("DROP TABLE IF EXISTS " + n)
		}
	})
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_alter (id INTEGER NOT NULL, name VARCHAR, qty INTEGER)`)
	h.exec(`INSERT INTO it_alter VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', NULL)`)

	// RENAME COLUMN: SQL name changes, data and index attribute stay.
	h.exec(`ALTER TABLE it_alter RENAME COLUMN name TO label`)
	h.expectRows(`SELECT id, label FROM it_alter WHERE label = 'b'`, "2|b")
	h.expectError(`SELECT name FROM it_alter`, "does not exist")
	if f := h.rowFields(raw, "public", "it_alter"); f["name"] != 3 || f["label"] != 0 {
		t.Errorf("row fields after RENAME COLUMN = %v, want the original 'name' field", f)
	}
	h.expectError(`ALTER TABLE it_alter RENAME COLUMN label TO qty`, "already exists")
	h.expectError(`ALTER TABLE it_alter RENAME COLUMN nope TO x`, "does not exist")

	// ADD COLUMN: NULL for existing rows, indexed for new data.
	h.exec(`ALTER TABLE it_alter ADD COLUMN note VARCHAR`)
	h.exec(`INSERT INTO it_alter (id, label, qty, note) VALUES (4, 'd', 40, 'x')`)
	h.expectRows(`SELECT id, note FROM it_alter ORDER BY id`, "1|NULL", "2|NULL", "3|NULL", "4|x")
	h.expectRows(`SELECT id FROM it_alter WHERE note = 'x'`, "4")
	h.exec(`ALTER TABLE it_alter ADD COLUMN IF NOT EXISTS note VARCHAR`)
	h.expectError(`ALTER TABLE it_alter ADD COLUMN note VARCHAR`, "already exists")
	h.expectError(`ALTER TABLE it_alter ADD COLUMN must INTEGER NOT NULL`, "NOT NULL")
	h.expectError(`ALTER TABLE it_alter ADD COLUMN d INTEGER DEFAULT RANDOM()`, "volatile DEFAULT")

	// DROP COLUMN: gone at once; its field is removed from rows in the
	// background; re-adding the name never shows the old values.
	h.exec(`ALTER TABLE it_alter DROP COLUMN qty`)
	h.expectRows(`SELECT * FROM it_alter WHERE id = 1`, "1|a|NULL")
	h.expectError(`SELECT qty FROM it_alter`, "does not exist")
	h.exec(`ALTER TABLE it_alter ADD COLUMN qty INTEGER`)
	h.expectRows(`SELECT id, qty FROM it_alter ORDER BY id`, "1|NULL", "2|NULL", "3|NULL", "4|NULL")
	waitFor(t, "dropped column cleanup", func() bool { return h.rowFields(raw, "public", "it_alter")["qty"] == 0 })
	h.exec(`UPDATE it_alter SET qty = 7 WHERE id = 1`)
	h.expectRows(`SELECT id FROM it_alter WHERE qty = 7`, "1")
	h.exec(`ALTER TABLE it_alter DROP COLUMN IF EXISTS nope`)
	h.expectError(`ALTER TABLE it_alter DROP COLUMN nope`, "does not exist")

	// RENAME TO: rows and index follow without being rewritten. A new table
	// with the old name must not see (or, when dropped, delete) them.
	h.exec(`ALTER TABLE it_alter RENAME TO it_alter2`)
	h.expectRows(`SELECT COUNT(*) FROM it_alter2`, "4")
	h.expectRows(`SELECT id FROM it_alter2 WHERE label = 'c'`, "3")
	h.expectError(`SELECT * FROM it_alter`, "does not exist")
	h.exec(`CREATE TABLE it_alter (x INTEGER)`)
	h.exec(`INSERT INTO it_alter VALUES (100)`)
	h.expectRows(`SELECT COUNT(*) FROM it_alter`, "1")
	h.exec(`INSERT INTO it_alter2 (id, label) VALUES (5, 'e')`)
	h.expectRows(`SELECT COUNT(*) FROM it_alter2`, "5")
	h.exec(`DROP TABLE it_alter`)
	h.expectRows(`SELECT COUNT(*) FROM it_alter2`, "5")
	h.exec(`CREATE TABLE it_alter3 (a INTEGER)`)
	h.expectError(`ALTER TABLE it_alter2 RENAME TO it_alter3`, "already exists")
	h.expectError(`ALTER TABLE it_alter2 RENAME TO secondary.x`, "another schema")
	h.exec(`ALTER TABLE it_alter3 ADD COLUMN b INTEGER`)
	h.exec(`ALTER TABLE it_alter3 DROP COLUMN a`)
	h.expectError(`ALTER TABLE it_alter3 DROP COLUMN b`, "only column")
	h.exec(`ALTER TABLE IF EXISTS it_missing RENAME TO it_missing2`)
	h.expectError(`ALTER TABLE it_missing RENAME TO it_missing2`, "does not exist")
}

func TestSQLAlterCleanupResumes(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_resume")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_resume") })
	h.exec(`CREATE TABLE it_resume (id INTEGER, v INTEGER)`)
	h.exec(`INSERT INTO it_resume VALUES (1, 1), (2, 2), (3, 3)`)
	raw := h.rawClient()

	// Leave a cleanup pending, as if the process had exited mid-way: the
	// rows still hold a dropped column's field.
	st := &store{client: raw}
	meta, err := st.getTable(h.ctx, "public", "it_resume")
	if err != nil {
		t.Fatal(err)
	}
	pipe := raw.Pipeline()
	for id := 1; id <= 3; id++ {
		pipe.HSet(h.ctx, fmt.Sprintf("%s%d", meta.prefix(), id), "ghost", "boo")
	}
	if _, err := pipe.Exec(h.ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.updateTable(h.ctx, "public", "it_resume", func(m *tableMeta) error {
		m.RetiredFields = append(m.RetiredFields, "ghost")
		m.PendingCleanup = append(m.PendingCleanup, "ghost")
		return nil
	}, func(p goredis.Pipeliner) { p.SAdd(h.ctx, cleanupKey, cleanupMember("public", "it_resume")) }); err != nil {
		t.Fatal(err)
	}
	if h.rowFields(raw, "public", "it_resume")["ghost"] != 3 {
		t.Fatal("setup: ghost field missing")
	}

	// A new connection resumes and finishes it.
	h2 := newSQLHarness(t)
	waitFor(t, "resumed cleanup", func() bool { return h2.rowFields(raw, "public", "it_resume")["ghost"] == 0 })
	waitFor(t, "cleanup marker removal", func() bool {
		m, _ := raw.SIsMember(h.ctx, cleanupKey, cleanupMember("public", "it_resume")).Result()
		return !m
	})
	h2.expectRows(`SELECT id, v FROM it_resume ORDER BY id`, "1|1", "2|2", "3|3")
}

func TestSQLInformationSchema(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	h.exec("DROP VIEW IF EXISTS it_is_view")
	h.exec(`CREATE VIEW it_is_view AS SELECT id, amount FROM it_orders WHERE qty > 1`)
	t.Cleanup(func() { h.exec("DROP VIEW IF EXISTS it_is_view") })

	h.expectRows(`SELECT schema_name FROM information_schema.schemata
		WHERE schema_name IN ('public', 'information_schema') ORDER BY schema_name`,
		"information_schema", "public")
	h.expectRows(`SELECT table_name, table_type FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name LIKE 'it_%' ORDER BY table_name`,
		"it_customers|BASE TABLE", "it_is_view|VIEW", "it_orders|BASE TABLE")
	h.expectRows(`SELECT column_name, ordinal_position, data_type, is_nullable, numeric_precision, numeric_scale, is_indexed
		FROM information_schema.columns WHERE table_name = 'it_orders' ORDER BY ordinal_position`,
		"id|1|INTEGER|NO|32|0|YES",
		"customer_id|2|INTEGER|YES|32|0|YES",
		"amount|3|NUMERIC(10,2)|YES|10|2|YES",
		"qty|4|INTEGER|YES|32|0|YES",
		"status|5|VARCHAR|YES|NULL|NULL|YES")
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_is_view' ORDER BY ordinal_position`,
		"id|INTEGER", "amount|NUMERIC(10,2)")
	h.expectRows(`SELECT view_definition FROM information_schema.views WHERE table_name = 'it_is_view'`,
		"SELECT id, amount FROM it_orders WHERE qty > 1")

	// Any SQL works: joins and grouping across information_schema tables.
	h.expectRows(`SELECT t.table_type, COUNT(*) FROM information_schema.tables t
		JOIN information_schema.columns c ON c.table_schema = t.table_schema AND c.table_name = t.table_name
		WHERE t.table_name IN ('it_orders', 'it_is_view') GROUP BY t.table_type ORDER BY t.table_type`,
		"BASE TABLE|5", "VIEW|2")

	// It reflects ALTER TABLE.
	h.exec(`ALTER TABLE it_customers RENAME COLUMN name TO full_name`)
	h.expectRows(`SELECT column_name FROM information_schema.columns WHERE table_name = 'it_customers' ORDER BY ordinal_position`,
		"id", "full_name", "country")

	// Read-only.
	h.expectError(`CREATE TABLE information_schema.x (a INT)`, "read-only")
	h.expectError(`CREATE VIEW information_schema.v AS SELECT 1 AS a`, "read-only")
	h.expectError(`SELECT * FROM information_schema.nope`, "does not exist")
}

func TestSQLLike(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	h.exec("DROP TABLE IF EXISTS it_like")
	h.exec("CREATE TABLE it_like (id INTEGER, s VARCHAR)")
	h.exec(`INSERT INTO it_like VALUES (1, 'apple'), (2, 'Apricot'), (3, 'banana'), (4, '50% off'),
		(5, 'a_b'), (6, 'axb'), (7, NULL), (8, 'ap'), (9, 'こんにちは')`)
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_like") })

	h.expectRows(`SELECT id FROM it_like WHERE s LIKE 'ap%' ORDER BY id`, "1", "8") // prefix: index query
	h.expectRows(`SELECT id FROM it_like WHERE s LIKE '%an%' ORDER BY id`, "3")
	h.expectRows(`SELECT id FROM it_like WHERE s LIKE 'a_b' ORDER BY id`, "5", "6")        // _ is one character
	h.expectRows(`SELECT id FROM it_like WHERE s LIKE 'a\_b' ESCAPE '\' ORDER BY id`, "5") // escaped _
	h.expectRows(`SELECT id FROM it_like WHERE s LIKE '%\%%' ESCAPE '\'`, "4")
	h.expectRows(`SELECT id FROM it_like WHERE s ILIKE 'ap%' ORDER BY id`, "1", "2", "8")
	// No lowercase 'a' (LIKE is case-sensitive); NULL is neither LIKE nor NOT LIKE.
	h.expectRows(`SELECT id FROM it_like WHERE s NOT LIKE '%a%' ORDER BY id`, "2", "4", "9")
	h.expectRows(`SELECT id FROM it_like WHERE s LIKE 'こん%'`, "9")
	h.expectRows(`SELECT id FROM it_like WHERE s LIKE '%'  ORDER BY id`, "1", "2", "3", "4", "5", "6", "8", "9")
	h.expectRows(`SELECT 'abc' LIKE 'a%c', 'abc' LIKE 'b%', NULL LIKE 'a'`, "true|false|NULL")
}

func TestSQLSetOperations(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	h.exec("DROP VIEW IF EXISTS it_union_view")
	h.exec("DROP TABLE IF EXISTS it_union_ctas")
	t.Cleanup(func() {
		h.exec("DROP VIEW IF EXISTS it_union_view")
		h.exec("DROP TABLE IF EXISTS it_union_ctas")
	})

	// customer ids: customers {1,2,3,4}; orders {1,1,2,2,3,9}
	h.expectRows(`SELECT id FROM it_customers UNION SELECT customer_id FROM it_orders ORDER BY 1`,
		"1", "2", "3", "4", "9")
	h.expectRows(`SELECT id FROM it_customers UNION ALL SELECT customer_id FROM it_orders ORDER BY id`,
		"1", "1", "1", "2", "2", "2", "3", "3", "4", "9")
	h.expectRows(`SELECT id FROM it_customers INTERSECT SELECT customer_id FROM it_orders ORDER BY id`, "1", "2", "3")
	h.expectRows(`SELECT customer_id FROM it_orders INTERSECT ALL SELECT customer_id FROM it_orders WHERE status = 'shipped' ORDER BY 1`,
		"1", "2", "2", "9")
	h.expectRows(`SELECT id FROM it_customers EXCEPT SELECT customer_id FROM it_orders`, "4")
	h.expectRows(`SELECT customer_id FROM it_orders EXCEPT ALL SELECT id FROM it_customers ORDER BY 1`, "1", "2", "9")

	// NULLs are equal for duplicate elimination; types are widened.
	h.expectRows(`SELECT country FROM it_customers UNION SELECT country FROM it_customers ORDER BY country`,
		"GBR", "USA", "NULL")
	h.expectRows(`SELECT country FROM it_customers UNION SELECT country FROM it_customers ORDER BY country NULLS FIRST`,
		"NULL", "GBR", "USA")
	schema := h.expectRows(`SELECT qty FROM it_orders WHERE id = 1 UNION SELECT amount FROM it_orders WHERE id = 2 ORDER BY 1`,
		"1.00", "99.99")
	if dt := schema.Field(0).Type; dt.ID() != arrow.DECIMAL128 {
		t.Errorf("UNION of INTEGER and NUMERIC = %s, want decimal", dt)
	}
	h.expectRows(`SELECT 1 AS n UNION SELECT 1.0 UNION SELECT 2`, "1.0", "2.0")
	h.expectRows(`SELECT NULL AS x UNION ALL SELECT 'a' ORDER BY x`, "a", "NULL")

	// Precedence (INTERSECT first), parentheses, chaining, per-branch LIMIT.
	h.expectRows(`SELECT 1 AS n UNION SELECT 2 INTERSECT SELECT 3`, "1")
	h.expectRows(`(SELECT 1 AS n UNION SELECT 2) INTERSECT SELECT 2`, "2")
	h.expectRows(`SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 EXCEPT SELECT 2 ORDER BY n DESC`, "3", "1")
	h.expectRows(`(SELECT id FROM it_orders ORDER BY amount DESC LIMIT 2) UNION ALL (SELECT id FROM it_orders ORDER BY id LIMIT 1) ORDER BY 1`,
		"1", "2", "4")
	h.expectRows(`SELECT id, status FROM it_orders WHERE qty > 3 UNION SELECT id, 'small' FROM it_orders WHERE qty = 1
		ORDER BY status, id LIMIT 3 OFFSET 1`,
		"6|shipped", "1|small", "5|small")

	// In subqueries, CTEs, views, IN, correlated, CTAS.
	h.expectRows(`SELECT name FROM it_customers WHERE id IN (SELECT customer_id FROM it_orders WHERE qty >= 10 UNION SELECT 3) ORDER BY name`,
		"Bo", "Cy")
	h.expectRows(`WITH ids AS (SELECT id FROM it_customers EXCEPT SELECT customer_id FROM it_orders) SELECT COUNT(*) FROM ids`, "1")
	h.expectRows(`SELECT COUNT(*) FROM (SELECT customer_id FROM it_orders UNION SELECT id FROM it_customers) AS u`, "5")
	h.exec(`CREATE VIEW it_union_view AS SELECT id AS k FROM it_customers UNION SELECT customer_id FROM it_orders`)
	h.expectRows(`SELECT k FROM it_union_view WHERE k > 3 ORDER BY k`, "4", "9")
	h.expectRows(`SELECT c.name FROM it_customers c WHERE EXISTS (
			SELECT 1 FROM it_orders o WHERE o.customer_id = c.id AND o.status = 'pending'
			UNION ALL SELECT 1 FROM it_orders o WHERE o.customer_id = c.id AND o.qty >= 10) ORDER BY c.name`,
		"Ada", "Bo")
	h.exec(`CREATE TABLE it_union_ctas AS SELECT id, name FROM it_customers WHERE id <= 2 UNION ALL SELECT 99, 'extra'`)
	h.expectRows(`SELECT id, name FROM it_union_ctas ORDER BY id`, "1|Ada", "2|Bo", "99|extra")

	// NULLS FIRST / LAST on ordinary queries now apply too.
	h.expectRows(`SELECT id FROM it_orders ORDER BY amount NULLS FIRST, id LIMIT 2`, "5", "3")
	h.expectRows(`SELECT id FROM it_orders ORDER BY amount DESC NULLS LAST LIMIT 1`, "4")

	// Errors.
	h.expectError(`SELECT id, name FROM it_customers UNION SELECT id FROM it_orders`, "same number of columns")
	h.expectError(`SELECT id FROM it_customers UNION SELECT name FROM it_customers`, "cannot be matched")
	h.expectError(`SELECT id FROM it_customers UNION SELECT customer_id FROM it_orders ORDER BY id + 1`, "output column names or positions")
	h.expectError(`SELECT id FROM it_customers ORDER BY id UNION SELECT 1`, "UNION")
}

func TestSQLDateTimeFunctions(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_dt")
	h.exec(`CREATE TABLE it_dt (id INTEGER, d DATE, ts TIMESTAMP(6), tz TIMESTAMP(3) WITH TIME ZONE, t TIME(6))`)
	h.exec(`INSERT INTO it_dt VALUES
		(1, DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:30.123456', TIMESTAMP WITH TIME ZONE '2024-02-29 23:59:59.999+00', TIME '13:45:30.5'),
		(2, DATE '1999-12-31', TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP WITH TIME ZONE '1999-12-31 22:00:00-05', TIME '00:00:00'),
		(3, NULL, NULL, NULL, NULL)`)
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_dt") })

	// EXTRACT / DATE_PART.
	h.expectRows(`SELECT EXTRACT(YEAR FROM ts), EXTRACT(QUARTER FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(WEEK FROM ts),
			EXTRACT(DAY FROM ts), EXTRACT(DOW FROM ts), EXTRACT(ISODOW FROM ts), EXTRACT(DOY FROM ts)
		FROM it_dt WHERE id = 1`, "2024|1|2|9|29|4|4|60")
	h.expectRows(`SELECT EXTRACT(HOUR FROM ts), EXTRACT(MINUTE FROM ts), EXTRACT(SECOND FROM ts),
			EXTRACT(MILLISECONDS FROM ts), EXTRACT(MICROSECONDS FROM ts), EXTRACT(EPOCH FROM ts)
		FROM it_dt WHERE id = 1`, "13|45|30.123456|30123.456|30123456|1.709214330123456e+09")
	h.expectRows(`SELECT DATE_PART('decade', d), DATE_PART('century', d), DATE_PART('millennium', d), DATE_PART('isoyear', d)
		FROM it_dt WHERE id = 1`, "202|21|3|2024")
	h.expectRows(`SELECT EXTRACT(HOUR FROM t), EXTRACT(SECOND FROM t), EXTRACT(HOUR FROM tz) FROM it_dt ORDER BY id`,
		"13|30.5|23", "0|0|3", "NULL|NULL|NULL") // row 2: 22:00-05 is 03:00 UTC
	h.expectError(`SELECT EXTRACT(YEAR FROM t) FROM it_dt`, "not valid for TIME")
	h.expectError(`SELECT DATE_PART('fortnight', ts) FROM it_dt`, "unknown date/time field")
	h.expectRows(`SELECT YEAR(d), MONTH(d), DAY(d), HOUR(ts), MINUTE(ts), SECOND(ts), DAYOFYEAR(d), QUARTER(d), WEEK(d)
		FROM it_dt WHERE id = 1`, "2024|2|29|13|45|30|60|1|9")

	// DATE_TRUNC: dates stay dates, timestamps keep their type.
	h.expectRows(`SELECT CAST(DATE_TRUNC('month', ts) AS VARCHAR), CAST(DATE_TRUNC('quarter', ts) AS VARCHAR),
			CAST(DATE_TRUNC('hour', ts) AS VARCHAR), DATE_TRUNC('week', d), DATE_TRUNC('year', d),
			CAST(DATE_TRUNC('day', tz) AS VARCHAR)
		FROM it_dt WHERE id = 1`,
		"2024-02-01 00:00:00|2024-01-01 00:00:00|2024-02-29 13:00:00|2024-02-26|2024-01-01|2024-02-29 00:00:00+00")
	h.expectRows(`SELECT DATE_TRUNC('year', ts) AS y, COUNT(*) FROM it_dt WHERE ts IS NOT NULL GROUP BY y ORDER BY y`,
		"2000-01-01T00:00:00|1", "2024-01-01T00:00:00|1")

	// DATE_DIFF counts unit boundaries crossed.
	h.expectRows(`SELECT DATE_DIFF('day', DATE '2024-02-01', DATE '2024-03-01'),
			DATE_DIFF('month', DATE '2024-01-31', DATE '2024-02-01'),
			DATE_DIFF('year', DATE '2023-12-31', DATE '2024-01-01'),
			DATE_DIFF('hour', TIMESTAMP '2024-01-01 01:59:00', TIMESTAMP '2024-01-01 02:01:00'),
			DATE_DIFF('week', DATE '2024-02-25', DATE '2024-02-26'),
			DATEDIFF('day', DATE '2024-03-01', DATE '2024-02-01')`,
		"29|1|1|1|1|-29")

	// Construction and conversion.
	h.expectRows(`SELECT MAKE_DATE(2024, 2, 29), CAST(MAKE_TIMESTAMP(2024, 1, 2, 3, 4, 5.25) AS VARCHAR),
			CAST(MAKE_TIME(13, 45, 30.5) AS VARCHAR), LAST_DAY(DATE '2024-02-10'), LAST_DAY(DATE '2023-02-10')`,
		"2024-02-29|2024-01-02 03:04:05.25|13:45:30.5|2024-02-29|2023-02-28")
	h.expectError(`SELECT MAKE_DATE(2023, 2, 29)`, "out of range")
	h.expectRows(`SELECT CAST(TO_TIMESTAMP(0) AS VARCHAR), CAST(TO_TIMESTAMP(1700000000.5) AS VARCHAR)`,
		"1970-01-01 00:00:00+00|2023-11-14 22:13:20.5+00")
	h.expectRows(`SELECT CAST(TO_TIMESTAMP('2024-03-15 14:30', 'YYYY-MM-DD HH24:MI') AS VARCHAR),
			TO_DATE('15 Mar 2024', 'DD Mon YYYY'), TO_DATE('March 5, 2024', 'Month DD, YYYY'),
			CAST(TO_TIMESTAMP('03/15/2024 2:30 PM', 'MM/DD/YYYY HH12:MI AM') AS VARCHAR)`,
		"2024-03-15 14:30:00+00|2024-03-15|2024-03-05|2024-03-15 14:30:00+00")
	h.expectError(`SELECT TO_DATE('2024-02-30', 'YYYY-MM-DD')`, "out of range")
	h.expectError(`SELECT TO_DATE('2024/02', 'YYYY-MM-DD')`, "does not match format")
	h.expectError(`SELECT TO_DATE('2024-xx-01', 'YYYY-MM-DD')`, "expected digits")
	h.expectRows(`SELECT EPOCH(ts), EPOCH_MS(ts) FROM it_dt WHERE id = 1`, "1.709214330123456e+09|1709214330123")

	// TO_CHAR.
	h.expectRows(`SELECT TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS.MS'), TO_CHAR(ts, 'Mon DD YYYY HH12:MI AM'),
			TO_CHAR(d, 'FMDay, FMMonth FMDD, YYYY'), TO_CHAR(d, '"Q"Q IYYY-"W"IW'), TO_CHAR(d, 'Day|DY|dy')
		FROM it_dt WHERE id = 1`,
		"2024-02-29 13:45:30.123|Feb 29 2024 01:45 PM|Thursday, February 29, 2024|Q1 2024-W09|Thursday |THU|thu")

	// Current time: fixed within a statement, typed, and close to now.
	h.expectRows(`SELECT CURRENT_DATE = CAST(CURRENT_TIMESTAMP AS DATE), CURRENT_TIMESTAMP = NOW(),
			LOCALTIMESTAMP = CAST(NOW() AS TIMESTAMP)`, "true|true|true")
	got, schema := h.query(`SELECT EPOCH(CURRENT_TIMESTAMP), CURRENT_DATE, CURRENT_TIME, LOCALTIMESTAMP`)
	var epoch float64
	fmt.Sscan(strings.Split(got[0], "|")[0], &epoch)
	if d := time.Since(time.Unix(int64(epoch), 0)); d < -time.Minute || d > time.Minute {
		t.Errorf("CURRENT_TIMESTAMP is %v away from now", d)
	}
	if s := schema.String(); !strings.Contains(s, "date32") || !strings.Contains(s, "time64[us]") || !strings.Contains(s, "timestamp[us]") {
		t.Errorf("current time types = %s", s)
	}

	// Constants built from functions still push down to the index.
	h.expectRows(`SELECT id FROM it_dt WHERE ts >= DATE_TRUNC('year', TIMESTAMP '2024-06-01 00:00:00')`, "1")
	h.expectRows(`SELECT id FROM it_dt WHERE d < CURRENT_DATE ORDER BY id`, "1", "2")
	h.expectRows(`SELECT DATE_TRUNC('month', d), YEAR(ts), TO_CHAR(ts, 'YYYY') FROM it_dt WHERE id = 3`, "NULL|NULL|NULL")
}

func TestSQLIntervals(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_iv")
	h.exec("DROP TABLE IF EXISTS it_iv_ctas")
	h.exec(`CREATE TABLE it_iv (id INTEGER, ts TIMESTAMP(6), d DATE)`)
	h.exec(`INSERT INTO it_iv VALUES (1, TIMESTAMP '2024-02-28 12:00:00', DATE '2024-02-28'),
		(2, TIMESTAMP '2024-03-01 00:00:00', DATE '2024-03-01'), (3, TIMESTAMP '2024-03-10 08:30:00', DATE '2024-03-10'),
		(4, NULL, NULL)`)
	t.Cleanup(func() {
		h.exec("DROP TABLE IF EXISTS it_iv")
		h.exec("DROP TABLE IF EXISTS it_iv_ctas")
	})
	str := func(e string) string { return "CAST(" + e + " AS VARCHAR)" }

	// Literals in every accepted form.
	h.expectRows(`SELECT `+strings.Join([]string{
		str(`INTERVAL '1 year 2 months 3 days 04:05:06.5'`), str(`INTERVAL '1.5 hours'`), str(`INTERVAL '1.5 days'`),
		str(`INTERVAL '1.5 months'`), str(`INTERVAL '-2 weeks'`), str(`INTERVAL '2 days ago'`),
		str(`INTERVAL '3 04:05:06'`), str(`INTERVAL '1-2' YEAR TO MONTH`), str(`INTERVAL 'P1Y2M3DT4H5M6S'`),
		str(`INTERVAL '2' HOUR`), str(`INTERVAL 90 MINUTE`), str(`INTERVAL '0' DAY`)}, ", "),
		"1 year 2 mons 3 days 04:05:06.5|01:30:00|1 day 12:00:00|1 mon 15 days|-14 days|-2 days|"+
			"3 days 04:05:06|1 year 2 mons|1 year 2 mons 3 days 04:05:06|02:00:00|01:30:00|00:00:00")
	h.expectRows(`SELECT `+str(`CAST('7 days' AS INTERVAL)`)+`, `+str(`CAST('1 hour' AS INTERVAL DAY TO SECOND)`), "7 days|01:00:00")

	// Timestamp / date arithmetic, with calendar-correct months.
	h.expectRows(`SELECT `+strings.Join([]string{
		str(`TIMESTAMP '2024-01-31 10:00:00' + INTERVAL '1 month'`),
		str(`TIMESTAMP '2023-01-31 10:00:00' + INTERVAL '1 month'`),
		str(`INTERVAL '1 day 2 hours' + TIMESTAMP '2024-02-28 23:00:00'`),
		str(`TIMESTAMP '2024-03-01 00:00:00' - INTERVAL '1 year'`),
		str(`DATE '2024-02-29' + INTERVAL '1 year'`),
		str(`DATE '2024-02-28' + 2`), str(`DATE '2024-03-01' - 1`)}, ", "),
		"2024-02-29 10:00:00|2023-02-28 10:00:00|2024-03-01 01:00:00|2023-03-01 00:00:00|"+
			"2025-02-28 00:00:00|2024-03-01|2024-02-29")
	h.expectRows(`SELECT DATE '2024-03-01' - DATE '2024-02-01', `+
		str(`TIMESTAMP '2024-03-01 00:00:00' - TIMESTAMP '2024-02-28 12:00:00'`)+`, `+
		str(`TIMESTAMP '2024-02-28 12:00:00' - TIMESTAMP '2024-03-01 00:00:00'`)+`, `+
		str(`TIME '23:30:00' + INTERVAL '45 minutes'`)+`, `+str(`TIME '10:00:00' - TIME '08:30:00'`),
		"29|1 day 12:00:00|-1 days -12:00:00|00:15:00|01:30:00")

	// Interval arithmetic and comparison.
	h.expectRows(`SELECT `+str(`INTERVAL '1 day' * 2.5`)+`, `+str(`INTERVAL '1 hour' / 4`)+`, `+str(`-INTERVAL '1 day'`)+`, `+
		str(`INTERVAL '1 day' + INTERVAL '2 hours'`)+`, `+str(`3 * INTERVAL '20 minutes'`)+`,
		INTERVAL '1 day' = INTERVAL '24 hours', INTERVAL '1 month' > INTERVAL '29 days'`,
		"2 days 12:00:00|00:15:00|-1 days|1 day 02:00:00|01:00:00|true|true")

	// AGE (the Postgres documentation example) and EXTRACT from intervals.
	h.expectRows(`SELECT `+str(`AGE(TIMESTAMP '2001-04-10 00:00:00', TIMESTAMP '1957-06-13 00:00:00')`)+`, `+
		str(`AGE(TIMESTAMP '1957-06-13 00:00:00', TIMESTAMP '2001-04-10 00:00:00')`)+`, `+
		str(`AGE(TIMESTAMP '2024-03-01 10:00:00', TIMESTAMP '2024-02-28 12:30:00')`),
		"43 years 9 mons 27 days|-43 years -9 mons -27 days|1 day 21:30:00")
	h.expectRows(`SELECT EXTRACT(EPOCH FROM INTERVAL '1 day 2 hours'), EXTRACT(HOUR FROM INTERVAL '26 hours'),
			EXTRACT(DAY FROM INTERVAL '1 month 3 days'), EXTRACT(YEAR FROM INTERVAL '14 months'),
			EXTRACT(MONTH FROM INTERVAL '14 months'), EXTRACT(SECOND FROM INTERVAL '1 minute 30.5 seconds')`,
		"93600|26|3|1|2|30.5")

	// On table data: filters push down, NULLs propagate, ORDER BY intervals.
	h.expectRows(`SELECT id FROM it_iv WHERE ts >= TIMESTAMP '2024-03-01 12:00:00' - INTERVAL '1 day' ORDER BY id`, "2", "3")
	h.expectRows(`SELECT id FROM it_iv WHERE d + 1 = DATE '2024-02-29'`, "1")
	h.expectRows(`SELECT id, `+str(`ts - TIMESTAMP '2024-02-28 00:00:00'`)+` FROM it_iv ORDER BY ts - TIMESTAMP '2024-01-01 00:00:00' DESC`,
		"3|11 days 08:30:00", "2|2 days", "1|12:00:00", "4|NULL")
	h.expectRows(`SELECT id FROM it_iv WHERE ts - INTERVAL '1 hour' < TIMESTAMP '2024-02-28 12:00:00'`, "1")

	// Interval columns: CTAS stores them; types are reported.
	h.exec(`CREATE TABLE it_iv_ctas AS SELECT id, ts - TIMESTAMP '2024-02-28 00:00:00' AS since FROM it_iv`)
	schema := h.expectRows(`SELECT id, `+str(`since`)+`, since > INTERVAL '1 day' FROM it_iv_ctas ORDER BY since`,
		"1|12:00:00|false", "2|2 days|true", "3|11 days 08:30:00|true", "4|NULL|NULL")
	_ = schema
	_, schema = h.query(`SELECT since FROM it_iv_ctas`)
	if dt := schema.Field(0).Type; dt.ID() != arrow.INTERVAL_MONTH_DAY_NANO {
		t.Errorf("interval column type = %s", dt)
	}
	h.expectRows(`SELECT data_type FROM information_schema.columns WHERE table_name = 'it_iv_ctas' AND column_name = 'since'`, "INTERVAL")
	_, schema = h.query(`SELECT CURRENT_TIMESTAMP - INTERVAL '7 days', DATE '2024-01-01' + INTERVAL '1 day'`)
	if s := schema.String(); !strings.Contains(s, "timestamp[us, tz=UTC]") || !strings.Contains(s, "timestamp[us]") {
		t.Errorf("arithmetic result types = %s", s)
	}

	// Errors.
	h.expectError(`SELECT TIMESTAMP '2024-01-01 00:00:00' + 1`, "not supported")
	h.expectError(`SELECT INTERVAL 'abc'`, "invalid interval")
	h.expectError(`SELECT INTERVAL 'x' HOUR`, "needs a number")
	h.expectError(`SELECT INTERVAL '1 day' / 0`, "division by zero")
}

func TestSQLIntervalBind(t *testing.T) {
	h := newSQLHarness(t)
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, `SELECT CAST(TIMESTAMP '2024-01-31 00:00:00' + ? AS VARCHAR), CAST(? AS VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	mem := memory.DefaultAllocator
	ib := array.NewMonthDayNanoIntervalBuilder(mem)
	ib.Append(arrow.MonthDayNanoInterval{Months: 1, Days: 1, Nanoseconds: int64(time.Hour)})
	db := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Millisecond})
	db.Append(arrow.Duration(90_000))
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "iv", Type: arrow.FixedWidthTypes.MonthDayNanoInterval},
		{Name: "dur", Type: &arrow.DurationType{Unit: arrow.Millisecond}},
	}, nil), []arrow.Array{ib.NewArray(), db.NewArray()}, 1)
	if err := st.Bind(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	rdr.Next()
	r := rdr.RecordBatch()
	got := r.Column(0).ValueStr(0) + "|" + r.Column(1).ValueStr(0)
	// Jan 31 + 1 month = Feb 29 (leap year), + 1 day = Mar 1, + 1 hour.
	if want := "2024-03-01 01:00:00|00:01:30"; got != want {
		t.Errorf("bound intervals: got %q, want %q", got, want)
	}
}

// Rows found through the index are read by FT.AGGREGATE LOAD, which returns
// SORTABLE numbers rounded to 12 significant digits. Every type must still
// read back exactly: through an index filter, sort + limit, a residual
// filter, and a projection of only the columns LOAD @field returns exactly.
func TestSQLScanReadsExactValues(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_exact")
	h.exec(`CREATE TABLE it_exact (id INTEGER, big BIGINT, d DOUBLE, amt DECIMAL(38, 10), ts TIMESTAMP,
		t TIME, s VARCHAR, ok BOOLEAN, day DATE, note VARCHAR NOINDEX)`)
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_exact") })
	h.exec(`INSERT INTO it_exact VALUES
		(1, 9007199254740993, 0.30000000000000004, 1234567890123456789012345678.0123456789,
		 TIMESTAMP '2999-12-31 23:59:59.999999', TIME '23:59:59.999999', 'a|b "q" {x}', TRUE, DATE '2024-02-29', 'n1'),
		(2, 9223372036854775807, -1.2345678901234568e-300, -0.0000000001,
		 TIMESTAMP '1970-01-01 00:00:00.000001', TIME '00:00:00', '', FALSE, DATE '1900-01-01', NULL),
		(2147483647, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)

	const cols = `id, big, d, amt, CAST(ts AS VARCHAR), CAST(t AS VARCHAR), s, ok, day, note`
	row1 := `1|9007199254740993|0.30000000000000004|1234567890123456789012345678.0123456789|` +
		`2999-12-31 23:59:59.999999|23:59:59.999999|a|b "q" {x}|true|2024-02-29|n1`
	row2 := `2|9223372036854775807|-1.2345678901234568e-300|-0.0000000001|` +
		`1970-01-01 00:00:00.000001|00:00:00|` + `|false|1900-01-01|NULL`
	row3 := `2147483647|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL`
	h.expectRows(`SELECT `+cols+` FROM it_exact WHERE id >= 1 ORDER BY id`, row1, row2, row3)
	h.expectRows(`SELECT `+cols+` FROM it_exact WHERE big > 0 ORDER BY big LIMIT 2`, row1, row2)
	h.expectRows(`SELECT `+cols+` FROM it_exact WHERE note LIKE 'n%' OR note IS NULL ORDER BY id`, row1, row2, row3)
	h.expectRows(`SELECT id, CAST(t AS VARCHAR), s, ok, day, note FROM it_exact WHERE id >= 1 ORDER BY id`,
		`1|23:59:59.999999|a|b "q" {x}|true|2024-02-29|n1`,
		`2|00:00:00||false|1900-01-01|NULL`,
		`2147483647|NULL|NULL|NULL|NULL|NULL`)

	// UPDATE and DELETE find their rows the same way.
	if n := h.exec(`UPDATE it_exact SET note = 'u' WHERE big = 9223372036854775807`); n != 1 {
		t.Errorf("UPDATE affected %d rows, want 1", n)
	}
	if n := h.exec(`DELETE FROM it_exact WHERE id = 2147483647`); n != 1 {
		t.Errorf("DELETE affected %d rows, want 1", n)
	}
	h.expectRows(`SELECT id, big, note FROM it_exact ORDER BY id`, `1|9007199254740993|n1`, `2|9223372036854775807|u`)

	// Index sorts compare numbers as numbers on a cluster too, where the
	// coordinator merges the shards' sorted rows.
	h.exec("DROP TABLE IF EXISTS it_sortnum")
	h.exec("CREATE TABLE it_sortnum (n BIGINT, label VARCHAR)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_sortnum") })
	var values, cubes []string
	for i := 1; i <= 12; i++ {
		values = append(values, fmt.Sprintf("(%d, 'r%d')", i*i*i, i))
		cubes = append(cubes, fmt.Sprint(i*i*i))
	}
	h.exec("INSERT INTO it_sortnum VALUES " + strings.Join(values, ", "))
	h.expectRows(`SELECT n FROM it_sortnum`, cubes...) // insertion (__rowid) order
	h.expectRows(`SELECT n, label FROM it_sortnum ORDER BY n DESC LIMIT 4`, "1728|r12", "1331|r11", "1000|r10", "729|r9")
}

// Scans read rows a cursor page at a time; check results that span several
// pages, including sorting and offsets across page boundaries.
func TestSQLScanAcrossCursorPages(t *testing.T) {
	h := newSQLHarness(t)
	const n = 2*cursorCount + 500
	h.exec("DROP TABLE IF EXISTS it_pages")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_pages") })

	mem := memory.DefaultAllocator
	ib := array.NewInt64Builder(mem)
	defer ib.Release()
	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	for i := int64(1); i <= n; i++ {
		ib.Append(i)
		sb.Append(fmt.Sprintf("row %d", i))
	}
	ids, labels := ib.NewArray(), sb.NewArray()
	defer ids.Release()
	defer labels.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String},
	}, nil), []arrow.Array{ids, labels}, n)
	defer rec.Release()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetOption(h.ctx, adbc.OptionKeyIngestTargetTable, "it_pages"); err != nil {
		t.Fatal(err)
	}
	if err := st.Bind(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	if got, err := st.ExecuteUpdate(h.ctx); err != nil || got != n {
		t.Fatalf("ingest: n=%d err=%v", got, err)
	}

	rows, _ := h.query(`SELECT id, label FROM it_pages`)
	if len(rows) != n {
		t.Fatalf("full scan returned %d rows, want %d", len(rows), n)
	}
	for i, r := range rows {
		if want := fmt.Sprintf("%d|row %d", i+1, i+1); r != want {
			t.Fatalf("row %d = %q, want %q", i, r, want)
		}
	}
	h.expectRows(fmt.Sprintf(`SELECT id FROM it_pages ORDER BY id DESC LIMIT 3 OFFSET %d`, cursorCount-1),
		fmt.Sprint(n-cursorCount+1), fmt.Sprint(n-cursorCount), fmt.Sprint(n-cursorCount-1))
	// Driver-side filter and aggregate over every page.
	h.expectRows(`SELECT COUNT(*), SUM(id) FROM it_pages WHERE label LIKE '%5'`,
		fmt.Sprintf("%d|%d", n/10, (5+n-5)*(n/10)/2))
}

// tablePrefix returns a table's row key prefix.
func (h *sqlHarness) tablePrefix(c goredis.UniversalClient, schema, table string) string {
	h.t.Helper()
	meta, err := (&store{client: c}).getTable(h.ctx, schema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	return meta.prefix()
}

// prefixKeyCount counts the keys under a row key prefix (on every master of
// a cluster), independently of any index.
func (h *sqlHarness) prefixKeyCount(c goredis.UniversalClient, prefix string) int {
	h.t.Helper()
	var mu sync.Mutex
	total := 0
	count := func(ctx context.Context, n *goredis.Client) error {
		keys, err := n.Keys(ctx, prefix+"*").Result()
		mu.Lock()
		total += len(keys)
		mu.Unlock()
		return err
	}
	var err error
	if cc, ok := c.(*goredis.ClusterClient); ok {
		err = cc.ForEachMaster(h.ctx, count)
	} else {
		err = count(h.ctx, c.(*goredis.Client))
	}
	if err != nil {
		h.t.Fatal(err)
	}
	return total
}

func TestSQLInsertParenthesizedQuery(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() { h.exec("DROP TABLE IF EXISTS it_ins_paren") }
	drop()
	t.Cleanup(drop)
	h.exec("CREATE TABLE it_ins_paren (id BIGINT, s VARCHAR)")

	// With and without a column list; the `(` after the table name is a query,
	// not a column list, when SELECT / WITH / `(` follows it.
	if n := h.exec("INSERT INTO it_ins_paren (id, s) (SELECT 1, 'a')"); n != 1 {
		t.Errorf("rows affected = %d, want 1", n)
	}
	h.exec("INSERT INTO it_ins_paren (SELECT 2, 'b')")
	h.exec("INSERT INTO it_ins_paren ((SELECT 3, 'c') UNION ALL (SELECT 4, 'd'))")
	h.exec("INSERT INTO it_ins_paren (WITH x AS (SELECT 5 AS id, 'e' AS s) SELECT id, s FROM x)")
	// dbt's incremental insert: a parenthesized SELECT from another relation.
	if n := h.exec("INSERT INTO it_ins_paren (id, s) (SELECT id + 10, s FROM it_ins_paren WHERE id <= 2)"); n != 2 {
		t.Errorf("rows affected = %d, want 2", n)
	}
	// Column lists and VALUES are unchanged.
	h.exec("INSERT INTO it_ins_paren (s, id) VALUES ('f', 6)")
	h.expectRows("SELECT id, s FROM it_ins_paren ORDER BY id",
		"1|a", "2|b", "3|c", "4|d", "5|e", "6|f", "11|a", "12|b")

	h.expectError("INSERT INTO it_ins_paren (SELECT 1)", "2 target columns but the query returns 1")
	h.expectError("INSERT INTO it_ins_paren (id, s) (SELECT 1, 'x'", "syntax error")
}

func TestSQLTruncate(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_trunc_v")
		h.exec("DROP TABLE IF EXISTS it_trunc")
		h.exec("DROP TABLE IF EXISTS it_trunc2")
	}
	drop()
	t.Cleanup(drop)
	raw := h.rawClient()

	h.exec("CREATE TABLE it_trunc (id BIGINT NOT NULL, name VARCHAR, note VARCHAR NOINDEX)")
	h.exec("INSERT INTO it_trunc VALUES (1, 'a', 'x'), (2, 'b', NULL), (3, 'c', 'z')")
	// The index is rebuilt from metadata, so columns added or renamed by
	// ALTER TABLE must still be indexed afterwards.
	h.exec("ALTER TABLE it_trunc ADD COLUMN qty INTEGER")
	h.exec("ALTER TABLE it_trunc RENAME COLUMN name TO label")
	h.exec("INSERT INTO it_trunc (id, label, qty) VALUES (4, 'd', 7)")
	h.exec("CREATE TABLE it_trunc2 (k VARCHAR)")
	h.exec("INSERT INTO it_trunc2 VALUES ('p'), ('q')")
	h.exec("CREATE VIEW it_trunc_v AS SELECT id FROM it_trunc")

	h.exec("TRUNCATE TABLE it_trunc")
	h.expectRows("SELECT COUNT(*) FROM it_trunc", "0")
	if n := h.prefixKeyCount(raw, h.tablePrefix(raw, "public", "it_trunc")); n != 0 {
		t.Errorf("%d row keys left after TRUNCATE", n)
	}
	h.expectRows("SELECT COUNT(*) FROM it_trunc2", "2")
	h.expectRows(`SELECT column_name, data_type, is_indexed FROM information_schema.columns
		WHERE table_name = 'it_trunc' ORDER BY ordinal_position`,
		"id|BIGINT|YES", "label|VARCHAR|YES", "note|VARCHAR|NO", "qty|INTEGER|YES")

	// The table still works, filters still use the index, and row ids
	// continue (CONTINUE IDENTITY is the default).
	h.exec("INSERT INTO it_trunc (id, label, note, qty) VALUES (5, 'e', 'n', 1), (6, 'f', NULL, 9)")
	h.expectRows("SELECT id, label, note, qty FROM it_trunc WHERE qty > 5 AND label = 'f'", "6|f|NULL|9")
	h.expectRows("SELECT __rowid, id FROM it_trunc ORDER BY id", "5|5", "6|6")
	h.expectRows("SELECT id FROM it_trunc_v ORDER BY id", "5", "6")

	// RESTART IDENTITY, several tables at once, and no TABLE keyword.
	h.exec("TRUNCATE it_trunc, it_trunc2 RESTART IDENTITY CASCADE")
	h.expectRows("SELECT COUNT(*) FROM it_trunc2", "0")
	h.exec("INSERT INTO it_trunc (id) VALUES (7)")
	h.expectRows("SELECT __rowid, id FROM it_trunc", "1|7")

	// A bad name anywhere in the list leaves every table untouched.
	h.expectError("TRUNCATE it_trunc, it_trunc_missing", "does not exist")
	h.expectRows("SELECT COUNT(*) FROM it_trunc", "1")
	h.expectError("TRUNCATE it_trunc_v", "it is a view")
	h.expectError("TRUNCATE information_schema.tables", "read-only")
}

func TestSQLDropSchemaCascade(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP SCHEMA IF EXISTS it_cascade CASCADE")
		h.exec("DROP SCHEMA IF EXISTS it_cascade_v CASCADE")
	}
	drop()
	t.Cleanup(drop)
	raw := h.rawClient()

	h.exec("CREATE SCHEMA it_cascade")
	h.exec("CREATE TABLE it_cascade.t1 (id BIGINT, s VARCHAR)")
	h.exec("INSERT INTO it_cascade.t1 VALUES (1, 'a'), (2, 'b')")
	h.exec("CREATE TABLE it_cascade.t2 AS SELECT id FROM it_cascade.t1")
	h.exec("CREATE VIEW it_cascade.v1 AS SELECT id FROM t1 WHERE id > 1")

	// RESTRICT (the default) refuses a schema with tables or views.
	h.expectError("DROP SCHEMA it_cascade", "is not empty (2 tables, 1 views)")
	h.expectError("DROP SCHEMA it_cascade RESTRICT", "is not empty")
	h.expectRows("SELECT id FROM it_cascade.v1", "2")

	// A schema holding only a view is not empty either (it used to be
	// dropped, leaving the view behind).
	h.exec("CREATE SCHEMA it_cascade_v")
	h.exec("CREATE VIEW it_cascade_v.only AS SELECT 1 AS a")
	h.expectError("DROP SCHEMA it_cascade_v", "is not empty (0 tables, 1 views)")
	h.exec("DROP SCHEMA it_cascade_v CASCADE")
	h.expectRows("SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'it_cascade_v'", "0")
	h.expectError("SELECT * FROM it_cascade_v.only", "does not exist")

	// CASCADE drops the views, the tables (index and rows), then the schema.
	prefix := h.tablePrefix(raw, "it_cascade", "t1")
	if n := h.prefixKeyCount(raw, prefix); n != 2 {
		t.Fatalf("it_cascade.t1 has %d row keys, want 2", n)
	}
	h.exec("DROP SCHEMA it_cascade CASCADE")
	h.expectRows("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'it_cascade'", "0")
	h.expectRows("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'it_cascade'", "0")
	if n := h.prefixKeyCount(raw, prefix); n != 0 {
		t.Errorf("%d row keys left after DROP SCHEMA … CASCADE", n)
	}
	h.expectError("SELECT * FROM it_cascade.t1", "does not exist")
	h.expectError("DROP SCHEMA it_cascade CASCADE", "does not exist")
	h.exec("DROP SCHEMA IF EXISTS it_cascade CASCADE")

	// The names are free again.
	h.exec("CREATE SCHEMA it_cascade")
	h.exec("CREATE VIEW it_cascade.v1 AS SELECT 3 AS id")
	h.expectRows("SELECT id FROM it_cascade.v1", "3")
}

func TestSQLDropBehaviorKeywords(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_dropkw_v")
		h.exec("DROP TABLE IF EXISTS it_dropkw")
	}
	drop()
	t.Cleanup(drop)

	// dbt's default macros end DROP TABLE / DROP VIEW / DROP COLUMN with
	// CASCADE; the keywords are accepted (dependencies aren't tracked).
	h.exec("CREATE TABLE it_dropkw (a BIGINT, b VARCHAR)")
	h.exec("CREATE VIEW it_dropkw_v AS SELECT a FROM it_dropkw")
	h.exec("ALTER TABLE it_dropkw DROP COLUMN b CASCADE")
	h.expectRows("SELECT column_name FROM information_schema.columns WHERE table_name = 'it_dropkw'", "a")
	h.exec("DROP VIEW IF EXISTS it_dropkw_v RESTRICT")
	h.exec("DROP TABLE IF EXISTS it_dropkw CASCADE")
	h.expectRows("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'it_dropkw%'", "0")
	h.expectError("DROP TABLE it_dropkw CASCADE extra", "syntax error")
}

func TestSQLAlterViewRename(t *testing.T) {
	h := newSQLHarness(t)
	views := []string{"it_av_v", "it_av_v2", "it_av_v3", "it_av_dep", "it_av_m", "it_av_m__dbt_tmp", "it_av_m__dbt_backup", "it_av_other"}
	drop := func() {
		for _, v := range views {
			h.exec("DROP VIEW IF EXISTS " + v)
		}
		h.exec("DROP TABLE IF EXISTS it_av_base")
		h.exec("DROP TABLE IF EXISTS it_av_table")
	}
	drop()
	t.Cleanup(drop)

	h.exec("CREATE TABLE it_av_base (id BIGINT, s VARCHAR)")
	h.exec("INSERT INTO it_av_base VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	h.exec("CREATE TABLE it_av_table (x BIGINT)")
	h.exec("CREATE VIEW it_av_v AS SELECT id, s FROM it_av_base WHERE id > 1")
	h.exec("CREATE VIEW it_av_dep AS SELECT id FROM it_av_v")
	h.exec("CREATE VIEW it_av_other AS SELECT 1 AS one")

	// ALTER VIEW … RENAME TO, and ALTER TABLE … RENAME TO on a view.
	h.exec("ALTER VIEW it_av_v RENAME TO it_av_v2")
	h.expectRows("SELECT id, s FROM it_av_v2 WHERE id < 3", "2|b")
	h.expectError("SELECT * FROM it_av_v", "does not exist")
	h.exec("ALTER TABLE it_av_v2 RENAME TO it_av_v3")
	h.expectRows("SELECT COUNT(*) FROM it_av_v3", "2")
	h.expectRows(`SELECT table_name, table_type FROM information_schema.tables
		WHERE table_name LIKE 'it_av_v%' ORDER BY table_name`, "it_av_v3|VIEW")
	h.expectRows("SELECT table_name FROM information_schema.views WHERE table_name LIKE 'it_av_v%'", "it_av_v3")
	h.exec("ALTER VIEW it_av_v3 RENAME TO it_av_v3") // same name: no-op
	// A view that reads the old name breaks, as it does when a table is renamed.
	h.expectError("SELECT * FROM it_av_dep", "does not exist")

	// dbt's view materialization: build __dbt_tmp, move the old view to
	// __dbt_backup, move __dbt_tmp into place, drop the backup.
	h.exec("CREATE VIEW it_av_m AS SELECT id FROM it_av_base WHERE id = 1")
	h.exec("CREATE VIEW it_av_m__dbt_tmp AS SELECT id FROM it_av_base WHERE id >= 2")
	h.exec("ALTER TABLE it_av_m RENAME TO it_av_m__dbt_backup")
	h.exec("ALTER TABLE it_av_m__dbt_tmp RENAME TO it_av_m")
	h.exec("DROP VIEW IF EXISTS it_av_m__dbt_backup")
	h.expectRows("SELECT id FROM it_av_m ORDER BY id", "2", "3")

	// Tables and views share one namespace.
	h.expectError("ALTER VIEW it_av_m RENAME TO it_av_other", `view "public"."it_av_other" already exists`)
	h.expectError("ALTER VIEW it_av_m RENAME TO it_av_table", "already exists as a table")
	h.expectError("ALTER TABLE it_av_table RENAME TO it_av_other", "already exists as a view")
	h.expectError("ALTER VIEW it_av_m RENAME TO secondary.it_av_m", "cannot move a view to another schema")

	// Missing objects, wrong object kinds, and unsupported ALTER VIEW actions.
	h.exec("ALTER VIEW IF EXISTS it_av_missing RENAME TO it_av_x")
	h.exec("ALTER TABLE IF EXISTS it_av_missing RENAME TO it_av_x")
	h.expectError("ALTER VIEW it_av_missing RENAME TO it_av_x", `view "public"."it_av_missing" does not exist`)
	h.expectError("ALTER VIEW it_av_table RENAME TO it_av_x", "is a table, not a view")
	h.expectError("ALTER TABLE it_av_m ADD COLUMN z INT", "is a view; ALTER TABLE on a view supports only RENAME TO")
	h.expectError("ALTER TABLE it_av_m RENAME COLUMN id TO k", "is a view")
	h.expectError("ALTER VIEW it_av_m RENAME COLUMN id TO k", "ALTER VIEW supports only RENAME TO")
	h.expectError("ALTER VIEW it_av_m ADD COLUMN z INT", "ALTER VIEW supports only RENAME TO")
	h.expectError("ALTER VIEW it_av_m SET SCHEMA x", "unsupported ALTER VIEW action")
	h.expectError("ALTER SEQUENCE s RENAME TO t", "expected ALTER TABLE or ALTER VIEW")
	h.expectRows("SELECT id FROM it_av_m ORDER BY id", "2", "3")
}

// planOf plans a SELECT the way the driver runs it and reports the index
// query, whether a residual predicate is left for the driver to evaluate on
// fetched rows, and whether the aggregates run inside FT.AGGREGATE.
func (h *sqlHarness) planOf(sql string) (query string, residual, indexAgg bool) {
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
	wp, err := e.planWhere(h.ctx, plan.sel.Where, plan.meta, nil)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	if plan.aggregate {
		var aggs []*Func
		for _, it := range plan.items {
			collectAggregates(it.expr, &aggs)
		}
		if _, indexAgg, err = e.indexAggregate(h.ctx, plan, wp, aggs); err != nil {
			h.t.Fatalf("%s: %v", sql, err)
		}
	}
	return wp.query, wp.residual != nil, indexAgg
}

func TestSQLScalarFunctions(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP VIEW IF EXISTS it_fn_rand")
	h.exec("DROP TABLE IF EXISTS it_fn")
	h.exec("DROP TABLE IF EXISTS it_fn_ctas")
	h.exec(`CREATE TABLE it_fn (id INTEGER, amt NUMERIC(10,3), x DOUBLE, n BIGINT, s VARCHAR, code VARCHAR, d DATE, ts TIMESTAMP(6))`)
	h.exec(`INSERT INTO it_fn VALUES
		(1, 2.345, 2.5, 1250, '  Hello World  ', 'ab-12-x', DATE '2024-02-29', TIMESTAMP '2024-03-01 10:00:00'),
		(2, -2.345, -2.675, -1249, 'héllo wörld', 'cd-345-y', DATE '2024-01-01', TIMESTAMP '2023-12-31 23:00:00'),
		(3, 9.995, 0.5, 7, '', 'ef--z', DATE '2023-06-15', NULL),
		(4, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)
	t.Cleanup(func() {
		h.exec("DROP VIEW IF EXISTS it_fn_rand")
		h.exec("DROP TABLE IF EXISTS it_fn")
		h.exec("DROP TABLE IF EXISTS it_fn_ctas")
	})

	// Math on NUMERIC: exact, half away from zero; NULL in, NULL out.
	h.expectRows(`SELECT id, ROUND(amt, 2), ROUND(amt), TRUNC(amt, 1), FLOOR(amt), CEIL(amt), SIGN(amt) FROM it_fn ORDER BY id`,
		"1|2.35|2|2.3|2|3|1", "2|-2.35|-2|-2.3|-3|-2|-1", "3|10.00|10|9.9|9|10|1", "4|NULL|NULL|NULL|NULL|NULL|NULL")
	// On DOUBLE, also half away from zero (Postgres would round half to
	// even), and on the decimal value as written (-2.675 is -2.67499… in binary).
	h.expectRows(`SELECT id, ROUND(x), ROUND(x, 2), TRUNC(x), FLOOR(x), CEILING(x) FROM it_fn ORDER BY id`,
		"1|3|2.5|2|2|3", "2|-3|-2.68|-2|-3|-2", "3|1|0.5|0|0|1", "4|NULL|NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT id, ROUND(n, -2), TRUNC(n, -2), MOD(n, 7), SIGN(n), ABS(n) FROM it_fn ORDER BY id`,
		"1|1300|1200|4|1|1250", "2|-1200|-1200|-3|-1|1249", "3|0|0|0|1|7", "4|NULL|NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT id, POWER(id, 2), SQRT(id * id), LOG(10, POWER(10, id)), LOG10(POWER(10, id)), EXP(0 * id), LN(1) FROM it_fn ORDER BY id`,
		"1|1|1|1|1|1|0", "2|4|2|2|2|1|0", "3|9|3|3|3|1|0", "4|16|4|4|4|1|0")
	got, _ := h.query(`SELECT RANDOM(), RANDOM() FROM it_fn`)
	if len(got) != 4 || got[0] == got[1] {
		t.Errorf("RANDOM() per row = %q, want 4 rows of distinct values", got)
	}

	// Strings: positions count characters.
	h.expectRows(`SELECT id, TRIM(s), LENGTH(TRIM(s)), UPPER(LEFT(TRIM(s), 3)), RIGHT(TRIM(s), 3),
			SUBSTRING(TRIM(s) FROM 2 FOR 4), POSITION('o' IN s), REVERSE(TRIM(s)), INITCAP(s)
		FROM it_fn ORDER BY id`,
		"1|Hello World|11|HEL|rld|ello|7|dlroW olleH|  Hello World  ",
		"2|héllo wörld|11|HÉL|rld|éllo|5|dlröw olléh|Héllo Wörld",
		"3||0||||0||",
		"4|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT id, SPLIT_PART(code, '-', 2), SPLIT_PART(code, '-', -1), REPLACE(code, '-', '/'),
			LPAD(SPLIT_PART(code, '-', 2), 5, '0'), RPAD(code, 4, '.'), REGEXP_REPLACE(code, '[0-9]+', '#'),
			REGEXP_REPLACE(code, '[a-z]', '_', 'g'), STARTS_WITH(code, 'cd')
		FROM it_fn ORDER BY id`,
		"1|12|x|ab/12/x|00012|ab-1|ab-#-x|__-12-_|false",
		"2|345|y|cd/345/y|00345|cd-3|cd-#-y|__-345-_|true",
		"3||z|ef//z|00000|ef--|ef--z|__--_|false",
		"4|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT TRIM(BOTH ' Hd' FROM s), TRIM(LEADING FROM s), TRIM(TRAILING 'd ' FROM s) FROM it_fn WHERE id = 1`,
		"ello Worl|Hello World  |  Hello Worl")
	// dbt's cross-database macros: hash, split_part, right, replace, position.
	h.expectRows(`SELECT MD5(CAST(id AS VARCHAR)), MD5(code) FROM it_fn WHERE id = 1`,
		"c4ca4238a0b923820dcc509a6f75849b|e4aa4b61e84ad0745c42ee87571c749b")

	// Conditional functions: GREATEST / LEAST ignore NULLs and widen.
	schema := h.expectRows(`SELECT id, NULLIF(code, 'ef--z'), GREATEST(amt, x, n), LEAST(amt, x), COALESCE(amt, x, 0),
			IIF(n > 0, 'pos', 'neg')
		FROM it_fn ORDER BY id`,
		"1|ab-12-x|1250|2.345|2.345|pos", "2|cd-345-y|-2.345|-2.675|-2.345|neg",
		"3|NULL|9.995|0.5|9.995|pos", "4|NULL|NULL|NULL|0|neg")
	for _, i := range []int{2, 3, 4} {
		if dt := schema.Field(i).Type; dt.ID() != arrow.FLOAT64 {
			t.Errorf("column %d type = %s, want double", i, dt)
		}
	}
	h.expectRows(`SELECT id, CAST(GREATEST(d, ts) AS VARCHAR), LEAST(d, DATE '2024-01-15'), LEAST(d, '2024-01-15') FROM it_fn ORDER BY id`,
		"1|2024-03-01 10:00:00|2024-01-15|2024-01-15", "2|2024-01-01 00:00:00|2024-01-01|2024-01-01",
		"3|2023-06-15 00:00:00|2023-06-15|2023-06-15", "4|NULL|2024-01-15|2024-01-15")
	// IIF only evaluates the branch it returns (row 3 would divide by zero);
	// a NULL condition picks the second branch.
	h.expectRows(`SELECT id, IIF(n = 7, -1, 10000 / (n - 7)) FROM it_fn ORDER BY id`, "1|8", "2|-7", "3|-1", "4|NULL")

	// Result types: CTAS columns and information_schema.
	h.exec(`CREATE TABLE it_fn_ctas AS SELECT id, ROUND(amt, 2) AS r2, ROUND(x) AS rx, FLOOR(id) AS fl, SUBSTRING(s, 1, 3) AS sub,
		POSITION('o' IN s) AS pos, STARTS_WITH(code, 'ab') AS sw, GREATEST(id, n) AS g, COALESCE(amt, 0) AS c,
		MOD(n, 7) AS m, SQRT(id) AS sq, NULLIF(id, 1) AS ni, IIF(id > 2, amt, 0) AS ii FROM it_fn`)
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_fn_ctas' ORDER BY ordinal_position`,
		"id|INTEGER", "r2|NUMERIC(10,2)", "rx|DOUBLE PRECISION", "fl|INTEGER", "sub|VARCHAR", "pos|BIGINT", "sw|BOOLEAN",
		"g|BIGINT", "c|NUMERIC(10,3)", "m|BIGINT", "sq|DOUBLE PRECISION", "ni|INTEGER", "ii|NUMERIC(38,3)")
	h.expectRows(`SELECT id, r2, rx, sub, pos, sw, g, c, m, ni, ii FROM it_fn_ctas ORDER BY id`,
		"1|2.35|3|  H|7|true|1250|2.345|4|NULL|0.000",
		"2|-2.35|-3|hél|5|false|2|-2.345|-3|2|0.000",
		"3|10.00|1||0|false|7|9.995|0|3|9.995",
		"4|NULL|NULL|NULL|NULL|NULL|4|0.000|NULL|4|NULL")
	h.expectRows(`SELECT id FROM it_fn_ctas WHERE r2 > 0 ORDER BY id`, "1", "3")

	// Pushdown: functions of constants are computed once and stay index
	// queries; functions over columns are residuals evaluated by the driver.
	for _, c := range []struct {
		sql, query string
		residual   bool
		rows       []string
	}{
		{`SELECT id FROM it_fn WHERE n > ROUND(1000.4) ORDER BY id`, "@n:[(1000 +inf]", false, []string{"1"}},
		{`SELECT id FROM it_fn WHERE code = LOWER('AB-12-X') ORDER BY id`, `@code:{ab\-12\-x}`, false, []string{"1"}},
		{`SELECT id FROM it_fn WHERE amt BETWEEN LEAST(-3, 0) AND ABS(-3) ORDER BY id`, "@amt:[-3.000 +inf] @amt:[-inf 3.000]", false, []string{"1", "2"}},
		{`SELECT id FROM it_fn WHERE ROUND(amt) = 2 ORDER BY id`, "*", true, []string{"1"}},
		{`SELECT id FROM it_fn WHERE STARTS_WITH(code, 'cd') ORDER BY id`, "*", true, []string{"2"}},
		{`SELECT id FROM it_fn WHERE id < 3 AND SPLIT_PART(code, '-', 3) = 'y' ORDER BY id`, "@id:[-inf (3]", true, []string{"2"}},
		{`SELECT id FROM it_fn WHERE n < RANDOM() ORDER BY id`, "*", true, []string{"2"}},
	} {
		if q, residual, _ := h.planOf(c.sql); q != c.query || residual != c.residual {
			t.Errorf("%s: index query %q, residual %v; want %q, residual %v", c.sql, q, residual, c.query, c.residual)
		}
		h.expectRows(c.sql, c.rows...)
	}
	// Functions applied to aggregates or GROUP BY columns keep the
	// aggregation in the index; grouping by a function runs in the driver.
	for _, c := range []struct {
		sql      string
		indexAgg bool
		rows     []string
	}{
		{`SELECT ROUND(AVG(id), 1), MAX(id) * 2, SIGN(MIN(id) - 2), GREATEST(COUNT(*), 10) FROM it_fn`, true, []string{"2.5|8|-1|10"}},
		{`SELECT LPAD(CAST(id AS VARCHAR), 3, '0'), COUNT(*) FROM it_fn GROUP BY id ORDER BY id`, true, []string{"001|1", "002|1", "003|1", "004|1"}},
		{`SELECT SPLIT_PART(code, '-', 1) AS prefix, COUNT(*) FROM it_fn GROUP BY prefix ORDER BY prefix`, false, []string{"ab|1", "cd|1", "ef|1", "NULL|1"}},
		{`SELECT SUM(ROUND(amt)), MAX(LENGTH(s)) FROM it_fn`, false, []string{"10|15"}},
	} {
		if _, _, indexAgg := h.planOf(c.sql); indexAgg != c.indexAgg {
			t.Errorf("%s: aggregated in the index = %v, want %v", c.sql, indexAgg, c.indexAgg)
		}
		h.expectRows(c.sql, c.rows...)
	}
	// RANDOM() in a view is not rewritten into the base table's filter
	// (where it would be computed a second time).
	h.exec(`CREATE VIEW it_fn_rand AS SELECT id, RANDOM() AS r FROM it_fn`)
	h.expectRows(`SELECT COUNT(*) FROM it_fn_rand WHERE r = r AND r >= 0 AND r < 1`, "4")
	h.expectRows(`SELECT COUNT(*) FROM (SELECT id FROM it_fn ORDER BY RANDOM() LIMIT 2) AS sample`, "2")

	// In INSERT and UPDATE; the updated column is still indexed.
	h.exec(`INSERT INTO it_fn (id, amt, s, code) VALUES (5, ROUND(7.777, 1), INITCAP(REPEAT('ab ', 2)), LPAD('9', 3, '0'))`)
	h.exec(`UPDATE it_fn SET s = TRIM(s), code = UPPER(SPLIT_PART(code, '-', 1)) WHERE id IN (1, 5)`)
	h.expectRows(`SELECT id, amt, s, code FROM it_fn WHERE id IN (1, 5) ORDER BY id`, "1|2.345|Hello World|AB", "5|7.800|Ab Ab|009")
	h.expectRows(`SELECT id FROM it_fn WHERE code = 'AB'`, "1")
	h.exec(`DELETE FROM it_fn WHERE id = 5`)
	h.exec(`UPDATE it_fn SET s = '  Hello World  ', code = 'ab-12-x' WHERE id = 1`)

	// Errors on table data, and argument counts checked when planning.
	h.expectError(`SELECT SQRT(n) FROM it_fn`, "SQRT: cannot take square root of a negative number")
	h.expectError(`SELECT LN(n - 7) FROM it_fn WHERE id = 3`, "LN: cannot take logarithm of zero")
	h.expectError(`SELECT MOD(id, id - id) FROM it_fn`, "MOD: division by zero")
	h.expectError(`SELECT SUBSTRING(s, 1, -1) FROM it_fn`, "negative substring length not allowed")
	h.expectError(`SELECT SPLIT_PART(code, '-', 0) FROM it_fn`, "field position must not be zero")
	h.expectError(`SELECT ROUND(s) FROM it_fn WHERE id = 1`, "cannot convert")
	h.expectError(`SELECT LEFT(s) FROM it_fn WHERE id > 100`, "LEFT expects 2 arguments")
	h.expectError(`SELECT GREATEST(d, id) FROM it_fn`, "GREATEST types DATE and INTEGER cannot be matched")
}

// Bound parameters inside the special syntaxes are numbered in order.
func TestSQLScalarFunctionBind(t *testing.T) {
	h := newSQLHarness(t)
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, `SELECT ROUND(?, 2), SUBSTRING(? FROM 2 FOR ?), POSITION(? IN 'hello'), TRIM(BOTH ? FROM 'xxhixx')`); err != nil {
		t.Fatal(err)
	}
	mem := memory.DefaultAllocator
	fb := array.NewFloat64Builder(mem)
	fb.Append(2.675)
	sb := array.NewStringBuilder(mem)
	sb.Append("abcdef")
	ib := array.NewInt64Builder(mem)
	ib.Append(3)
	pb := array.NewStringBuilder(mem)
	pb.Append("ll")
	cb := array.NewStringBuilder(mem)
	cb.Append("x")
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "f", Type: arrow.PrimitiveTypes.Float64}, {Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64}, {Name: "p", Type: arrow.BinaryTypes.String},
		{Name: "c", Type: arrow.BinaryTypes.String},
	}, nil), []arrow.Array{fb.NewArray(), sb.NewArray(), ib.NewArray(), pb.NewArray(), cb.NewArray()}, 1)
	if err := st.Bind(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	rdr.Next()
	r := rdr.RecordBatch()
	var cells []string
	for i := 0; i < int(r.NumCols()); i++ {
		cells = append(cells, r.Column(i).ValueStr(0))
	}
	if got, want := strings.Join(cells, "|"), "2.68|bcd|3|hi"; got != want {
		t.Errorf("bound parameters: got %q, want %q", got, want)
	}
}

// expectAffected runs a statement and checks the number of rows it affected.
func (h *sqlHarness) expectAffected(sql string, want int64) {
	h.t.Helper()
	if n := h.exec(sql); n != want {
		h.t.Errorf("%s\n affected %d rows, want %d", sql, n, want)
	}
}

func TestSQLMerge(t *testing.T) {
	h := newSQLHarness(t)
	tables := []string{"it_m_tgt", "it_m_src", "it_m_log", "it_m_n"}
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_m_v")
		for _, n := range tables {
			h.exec("DROP TABLE IF EXISTS " + n)
		}
	}
	drop()
	t.Cleanup(drop)
	h.exec("CREATE TABLE it_m_tgt (id INTEGER NOT NULL, name VARCHAR, qty INTEGER, note VARCHAR)")
	h.exec("CREATE TABLE it_m_src (id INTEGER, name VARCHAR, qty INTEGER, op VARCHAR)")
	h.exec(`INSERT INTO it_m_src VALUES
		(2, 'B', 21, 'upd'), (3, 'C', 99, 'del'), (4, 'D', 1, 'upd'), (5, 'e', 50, 'ins'), (6, 'f', 60, 'skip')`)
	reset := func() {
		h.exec("DELETE FROM it_m_tgt")
		h.exec("INSERT INTO it_m_tgt VALUES (1, 'a', 10, NULL), (2, 'b', 20, NULL), (3, 'c', 30, NULL), (4, 'd', 40, NULL)")
	}
	const all = "SELECT id, name, qty, note FROM it_m_tgt ORDER BY id"
	original := []string{"1|a|10|NULL", "2|b|20|NULL", "3|c|30|NULL", "4|d|40|NULL"}

	// Every clause type. Within each kind the first clause whose condition
	// holds applies: row 3 satisfies the DELETE and the UPDATE conditions and
	// is deleted; row 4 satisfies neither and falls through to DO NOTHING.
	reset()
	h.expectAffected(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN MATCHED AND s.op = 'del' THEN DELETE
		WHEN MATCHED AND s.qty > t.qty THEN UPDATE SET name = s.name, qty = s.qty, note = 'updated'
		WHEN MATCHED THEN DO NOTHING
		WHEN NOT MATCHED AND s.op = 'skip' THEN DO NOTHING
		WHEN NOT MATCHED THEN INSERT (id, name, qty) VALUES (s.id, s.name, s.qty)`, 3)
	h.expectRows(all, "1|a|10|NULL", "2|B|21|updated", "4|d|40|NULL", "5|e|50|NULL")
	// The index follows: TAG and range lookups see the new values.
	h.expectRows(`SELECT id FROM it_m_tgt WHERE name = 'B'`, "2")
	h.expectRows(`SELECT id FROM it_m_tgt WHERE name = 'b'`)
	h.expectRows(`SELECT id FROM it_m_tgt WHERE qty >= 21 ORDER BY qty`, "2", "4", "5")
	h.expectRows(`SELECT COUNT(*) FROM it_m_tgt WHERE id = 3`, "0")
	// Running it again: rows 2 and 5 now match and nothing is newer.
	h.expectAffected(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN MATCHED AND s.qty > t.qty THEN UPDATE SET qty = s.qty
		WHEN NOT MATCHED AND s.op <> 'skip' THEN INSERT VALUES (s.id, s.name, s.qty, s.op)`, 1)
	h.expectRows(all, "1|a|10|NULL", "2|B|21|updated", "3|C|99|del", "4|d|40|NULL", "5|e|50|NULL")

	// The issue's example: a one-row upsert from a subquery, inserting first
	// and updating the second time.
	reset()
	upsert := `MERGE INTO it_m_tgt AS d USING (SELECT 7 AS id) AS s ON d.id = s.id
		WHEN MATCHED THEN UPDATE SET qty = d.qty + 1
		WHEN NOT MATCHED THEN INSERT (id, qty) VALUES (s.id, 0)`
	h.expectAffected(upsert, 1)
	h.expectAffected(upsert, 1)
	h.expectRows(`SELECT id, qty FROM it_m_tgt WHERE id = 7`, "7|1")

	// WHEN NOT MATCHED BY SOURCE (and the optional BY TARGET), with a
	// subquery source.
	reset()
	h.expectAffected(`MERGE INTO it_m_tgt t USING (SELECT id, qty FROM it_m_src WHERE op = 'upd') s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET qty = s.qty
		WHEN NOT MATCHED BY TARGET THEN INSERT (id, qty) VALUES (s.id, s.qty)
		WHEN NOT MATCHED BY SOURCE AND t.qty >= 30 THEN DELETE
		WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'stale'`, 4)
	h.expectRows(all, "1|a|10|stale", "2|b|21|NULL", "4|d|1|NULL")

	// dbt's insert_overwrite shape: ON FALSE matches nothing, so every source
	// row is inserted and every target row is "not matched by source".
	reset()
	h.expectAffected(`MERGE INTO it_m_tgt AS DBT_INTERNAL_DEST
		USING (SELECT id, name FROM it_m_src WHERE op = 'ins') AS DBT_INTERNAL_SOURCE
		ON FALSE
		WHEN NOT MATCHED BY SOURCE AND DBT_INTERNAL_DEST.qty < 25 THEN DELETE
		WHEN NOT MATCHED THEN INSERT (id, name) VALUES (id, name)`, 3)
	h.expectRows(all, "3|c|30|NULL", "4|d|40|NULL", "5|e|NULL|NULL")
	h.expectAffected(`MERGE INTO it_m_tgt t USING it_m_src s ON 1 = 0
		WHEN MATCHED THEN DELETE WHEN NOT MATCHED AND s.id > 5 THEN INSERT (id) VALUES (s.id)`, 1)
	h.expectRows(`SELECT id FROM it_m_tgt ORDER BY id`, "3", "4", "5", "6")

	// Sources: a CTE (unqualified names in WHEN NOT MATCHED refer to the
	// source, the only relation it can see), a view, and a table without an
	// alias.
	reset()
	h.expectAffected(`WITH s AS (SELECT id + 10 AS id, name FROM it_m_src WHERE op = 'ins')
		MERGE INTO it_m_tgt t USING s ON t.id = s.id
		WHEN NOT MATCHED THEN INSERT (id, name) VALUES (id, name)`, 1)
	h.exec(`CREATE VIEW it_m_v AS SELECT id, name FROM it_m_src WHERE op IN ('skip', 'upd')`)
	h.expectAffected(`MERGE INTO it_m_tgt USING it_m_v v ON it_m_tgt.id = v.id
		WHEN MATCHED THEN UPDATE SET name = v.name
		WHEN NOT MATCHED THEN INSERT (id, name) VALUES (v.id, v.name)`, 3)
	h.expectAffected(`MERGE INTO it_m_tgt USING it_m_src ON it_m_tgt.id = it_m_src.id
		WHEN MATCHED AND it_m_src.op = 'del' THEN UPDATE SET it_m_tgt.note = it_m_src.op`, 1)
	h.expectRows(all, "1|a|10|NULL", "2|B|20|NULL", "3|c|30|del", "4|D|40|NULL", "6|f|NULL|NULL", "15|e|NULL|NULL")

	// A target row may be changed only once. Nothing is written when the
	// check fails, not even the rows processed before it.
	reset()
	h.expectError(`MERGE INTO it_m_tgt t USING (SELECT 2 AS id, 1 AS v UNION ALL SELECT 2, 2 UNION ALL SELECT 99, 0) s
		ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET qty = s.v
		WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)`, "MERGE command cannot affect row a second time")
	h.expectError(`MERGE INTO it_m_tgt t USING (SELECT 1 AS id UNION ALL SELECT 1) s ON t.id = s.id
		WHEN MATCHED THEN DELETE`, "cannot affect row a second time")
	h.expectRows(all, original...)
	// Matching twice is fine as long as only one match changes the row.
	h.expectAffected(`MERGE INTO it_m_tgt t USING (SELECT 2 AS id, 1 AS v UNION ALL SELECT 2, 2) s ON t.id = s.id
		WHEN MATCHED AND s.v = 2 THEN UPDATE SET qty = s.v
		WHEN MATCHED THEN DO NOTHING`, 1)
	h.expectRows(`SELECT qty FROM it_m_tgt WHERE id = 2`, "2")

	// Everything is checked before anything is written: a NOT NULL violation
	// or a bad value in a later row leaves the earlier rows untouched.
	reset()
	h.expectError(`MERGE INTO it_m_tgt t USING (SELECT 1 AS id, 5 AS q UNION ALL SELECT NULL, 6) s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET qty = s.q
		WHEN NOT MATCHED THEN INSERT (id, qty) VALUES (s.id, s.q)`, `NULL value in column "id" violates not-null constraint`)
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN MATCHED AND s.op = 'del' THEN UPDATE SET qty = 'many'
		WHEN MATCHED THEN UPDATE SET qty = s.qty`, `column "qty"`)
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET id = NULL`, "not-null constraint")
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN NOT MATCHED THEN INSERT DEFAULT VALUES`, "not-null constraint")
	h.expectRows(all, original...)

	// Casts follow the column types; INSERT DEFAULT VALUES inserts NULLs.
	h.exec("CREATE TABLE it_m_log (k INTEGER, v VARCHAR, amount NUMERIC(6,2))")
	h.expectAffected(`MERGE INTO it_m_log l USING (SELECT '7' AS k, 3 AS v, '1.005' AS a) s ON FALSE
		WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.v, s.a)`, 1)
	h.expectAffected(`MERGE INTO it_m_log l USING (SELECT 1 AS x) s ON FALSE WHEN NOT MATCHED THEN INSERT DEFAULT VALUES`, 1)
	h.expectRows(`SELECT k + 1, v || '!', amount FROM it_m_log ORDER BY k`, "8|3!|1.01", "NULL|NULL|NULL")

	// NULL keys never match: a NULL source key is inserted, a NULL target
	// key is "not matched by source".
	h.exec("CREATE TABLE it_m_n (k INTEGER, v VARCHAR)")
	h.exec("INSERT INTO it_m_n VALUES (1, 'one'), (NULL, 'null-t')")
	h.expectAffected(`MERGE INTO it_m_n t USING (SELECT 1 AS k, 'x' AS v UNION ALL SELECT NULL, 'null-s') s ON t.k = s.k
		WHEN MATCHED THEN UPDATE SET v = s.v
		WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)
		WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = t.v || '!'`, 3)
	h.expectRows(`SELECT k, v FROM it_m_n ORDER BY v`, "NULL|null-s", "NULL|null-t!", "1|x")

	// Errors.
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN NOT MATCHED THEN INSERT (id) VALUES (t.id)`, "WHEN NOT MATCHED clauses can only refer to the source")
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN NOT MATCHED BY SOURCE THEN UPDATE SET qty = s.qty`, "WHEN NOT MATCHED BY SOURCE clauses can only refer to the target")
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN MATCHED THEN INSERT (id) VALUES (s.id)`, "INSERT is only allowed in WHEN NOT MATCHED")
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN NOT MATCHED THEN DELETE`, "DELETE is not allowed in WHEN NOT MATCHED")
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id`, "at least one WHEN clause")
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id)`, "2 target columns but 1 values")
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET nope = 1`, `column "nope" does not exist`)
	h.expectError(`MERGE INTO it_m_tgt t USING it_m_src s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET s.qty = 1`, "is not in the table being updated")
	h.expectError(`MERGE INTO it_m_tgt s USING it_m_src s ON s.id = s.id WHEN MATCHED THEN DELETE`, "specified more than once")
	h.expectError(`MERGE INTO it_m_v t USING it_m_src s ON t.id = s.id WHEN MATCHED THEN DELETE`, "does not exist")
	h.expectError(`MERGE INTO it_m_tgt t USING (SELECT 1 AS id) ON TRUE WHEN MATCHED THEN DELETE`, "must have an alias")
	h.expectRows(all, original...)
}

func TestSQLUpdateFromDeleteUsing(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	h.exec("DROP TABLE IF EXISTS it_regions")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_regions") })
	h.exec("CREATE TABLE it_regions (country VARCHAR, region VARCHAR)")
	h.exec("INSERT INTO it_regions VALUES ('GBR', 'Europe'), ('USA', 'Americas'), (NULL, 'Nowhere')")
	const customers = "SELECT id, name, country FROM it_customers ORDER BY id"

	// SET reads the FROM item; a WHERE predicate on the target alone runs in
	// its index scan. Order 6's customer doesn't exist, so it isn't updated.
	h.expectAffected(`UPDATE it_orders AS o SET status = c.country FROM it_customers c
		WHERE c.id = o.customer_id AND o.qty > 1`, 3)
	h.expectRows(`SELECT id, status FROM it_orders ORDER BY id`,
		"1|shipped", "2|GBR", "3|USA", "4|USA", "5|returned", "6|shipped")
	h.expectRows(`SELECT id FROM it_orders WHERE status = 'USA' ORDER BY id`, "3", "4")

	// SET reading the target's own columns, from a derived table; the
	// target named by its table name.
	h.expectAffected(`UPDATE it_orders SET qty = it_orders.qty + d.extra FROM (SELECT 1 AS cid, 100 AS extra) d
		WHERE it_orders.customer_id = d.cid`, 2)
	h.expectRows(`SELECT id, qty FROM it_orders WHERE customer_id = 1 ORDER BY id`, "1|101", "2|103")

	// A FROM list with a join of its own, and a comma item.
	h.setupOrders()
	h.expectAffected(`UPDATE it_customers AS c SET country = r.region || '/' || x.tag
		FROM it_orders o JOIN it_regions r ON r.country = 'USA', (SELECT 'z' AS tag) x
		WHERE o.customer_id = c.id AND o.status = 'returned'`, 1)
	h.expectRows(customers, "1|Ada|GBR", "2|Bo|USA", "3|Cy|Americas/z", "4|Di|NULL")

	// A target row matching several FROM rows is updated once, if they all
	// give the same values; if they don't, it's an error and nothing changes.
	h.setupOrders()
	h.expectAffected(`UPDATE it_customers c SET name = 'has orders' FROM it_orders o WHERE o.customer_id = c.id`, 3)
	h.expectRows(customers, "1|has orders|GBR", "2|has orders|USA", "3|has orders|USA", "4|Di|NULL")
	h.setupOrders()
	h.expectError(`UPDATE it_customers c SET name = o.status FROM it_orders o WHERE o.customer_id = c.id`,
		`a row of "it_customers" matches more than one FROM row, and they set column "name" to different values`)
	h.expectRows(customers, "1|Ada|GBR", "2|Bo|USA", "3|Cy|USA", "4|Di|NULL")

	// NULL keys never match (Di's country is NULL, as is a region's).
	h.expectAffected(`UPDATE it_customers c SET name = r.region FROM it_regions r WHERE r.country = c.country`, 3)
	h.expectRows(customers, "1|Europe|GBR", "2|Americas|USA", "3|Americas|USA", "4|Di|NULL")

	// A CTE as the FROM item, and a correlated subquery in SET reading it.
	h.setupOrders()
	h.expectAffected(`WITH t AS (SELECT customer_id, SUM(qty) AS units FROM it_orders GROUP BY customer_id)
		UPDATE it_customers SET name = name || ':' || CAST(t.units AS VARCHAR) FROM t WHERE t.customer_id = it_customers.id`, 3)
	h.expectAffected(`UPDATE it_customers c
		SET country = (SELECT MAX(o.status) FROM it_orders o WHERE o.customer_id = r.cid)
		FROM (SELECT 3 AS cid) r WHERE c.id = r.cid`, 1)
	h.expectRows(customers, "1|Ada:4|GBR", "2|Bo:12|USA", "3|Cy:1|returned", "4|Di|NULL")

	// Aliases also work without FROM / USING.
	h.setupOrders()
	h.expectAffected(`UPDATE it_orders AS o SET qty = o.qty * 2 WHERE o.id = 1`, 1)
	h.expectAffected(`DELETE FROM it_orders o WHERE o.qty > 5`, 1)
	h.expectRows(`SELECT id, qty FROM it_orders ORDER BY id`, "1|2", "2|3", "3|2", "5|1", "6|4")

	// DELETE … USING, with a target-only filter, a join in the USING list,
	// and a target row matching several USING rows (deleted once).
	h.setupOrders()
	h.expectAffected(`DELETE FROM it_orders o USING it_customers c WHERE o.customer_id = c.id AND c.country = 'USA'`, 3)
	h.expectRows(`SELECT id FROM it_orders ORDER BY id`, "1", "2", "6")
	h.expectAffected(`DELETE FROM it_customers AS c USING it_orders o JOIN it_regions r ON r.region = 'Europe'
		WHERE o.customer_id = c.id AND c.country = r.country`, 1)
	h.expectAffected(`WITH gone AS (SELECT id FROM it_customers WHERE country IS NULL)
		DELETE FROM it_customers USING gone WHERE gone.id = it_customers.id`, 1)
	h.expectRows(customers, "2|Bo|USA", "3|Cy|USA")
	h.setupOrders()
	h.expectAffected(`DELETE FROM it_customers c USING it_orders o WHERE o.customer_id = c.id`, 3)
	h.expectRows(customers, "4|Di|NULL")

	// Errors.
	h.setupOrders()
	h.expectError(`UPDATE it_orders SET qty = 1 FROM it_orders`, "specified more than once")
	h.expectError(`UPDATE it_orders o SET c.qty = 1 FROM it_customers c`, "is not in the table being updated")
	h.expectError(`UPDATE it_orders o SET qty = id FROM it_customers c WHERE c.id = o.customer_id`, "ambiguous")
	h.expectError(`UPDATE it_orders o SET qty = 1 FROM it_customers c WHERE COUNT(*) > 1`, "aggregates are not allowed")
	h.expectError(`DELETE FROM it_orders o USING it_customers c WHERE c.nope = o.id`, "does not exist")
	h.expectRows(`SELECT COUNT(*) FROM it_orders`, "6")
}

// The statements dbt generates for the merge incremental strategy and for
// snapshots.
func TestSQLMergeDbt(t *testing.T) {
	h := newSQLHarness(t)
	tables := []string{"it_dbt_inc", "it_dbt_inc__dbt_tmp", "it_snap", "it_snap__dbt_tmp"}
	drop := func() {
		for _, n := range tables {
			h.exec("DROP TABLE IF EXISTS " + n)
		}
	}
	drop()
	t.Cleanup(drop)

	// Incremental model, merge strategy (default__get_merge_sql). dbt-core
	// writes the INSERT values unqualified; adapters often qualify them.
	h.exec(`CREATE TABLE it_dbt_inc (id INTEGER, name VARCHAR, updated_at TIMESTAMP)`)
	h.exec(`INSERT INTO it_dbt_inc VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01')`)
	h.exec(`CREATE TABLE it_dbt_inc__dbt_tmp AS
		SELECT 2 AS id, 'b2' AS name, TIMESTAMP '2024-01-02' AS updated_at
		UNION ALL SELECT 3, 'c', TIMESTAMP '2024-01-02'`)
	h.expectAffected(`merge into "redis"."public"."it_dbt_inc" as DBT_INTERNAL_DEST
        using "redis"."public"."it_dbt_inc__dbt_tmp" as DBT_INTERNAL_SOURCE
        on (DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id)

    when matched then update set
        "id" = DBT_INTERNAL_SOURCE."id","name" = DBT_INTERNAL_SOURCE."name","updated_at" = DBT_INTERNAL_SOURCE."updated_at"

    when not matched then insert
        ("id", "name", "updated_at")
    values
        (DBT_INTERNAL_SOURCE."id", DBT_INTERNAL_SOURCE."name", DBT_INTERNAL_SOURCE."updated_at")
`, 2)
	h.exec(`DELETE FROM it_dbt_inc__dbt_tmp`)
	h.exec(`INSERT INTO it_dbt_inc__dbt_tmp VALUES (3, 'c3', '2024-01-03'), (4, 'd', '2024-01-03')`)
	h.expectAffected(`merge into "redis"."public"."it_dbt_inc" as DBT_INTERNAL_DEST
        using "redis"."public"."it_dbt_inc__dbt_tmp" as DBT_INTERNAL_SOURCE
        on (DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id)
    when matched then update set
        "id" = DBT_INTERNAL_SOURCE."id","name" = DBT_INTERNAL_SOURCE."name","updated_at" = DBT_INTERNAL_SOURCE."updated_at"
    when not matched then insert
        ("id", "name", "updated_at")
    values
        ("id", "name", "updated_at")
`, 2)
	h.expectRows(`SELECT id, name, CAST(updated_at AS DATE) FROM it_dbt_inc ORDER BY id`,
		"1|a|2024-01-01", "2|b2|2024-01-02", "3|c3|2024-01-03", "4|d|2024-01-03")

	// Snapshot. The staging table holds dbt's change rows: a changed record
	// (an 'update' row closing the current version h1 and an 'insert' row for
	// the new one), a hard delete (h3), a new record (h4), and an 'update' for
	// an already closed version (h0), which must be left alone.
	snapCols := `id INTEGER, name VARCHAR, dbt_scd_id VARCHAR, dbt_updated_at TIMESTAMP,
		dbt_valid_from TIMESTAMP, dbt_valid_to TIMESTAMP`
	resetSnapshot := func() {
		h.exec(`DROP TABLE IF EXISTS it_snap`)
		h.exec(`CREATE TABLE it_snap (` + snapCols + `)`)
		h.exec(`INSERT INTO it_snap VALUES
			(1, 'a0', 'h0', '2024-01-01', '2024-01-01', '2024-01-05'),
			(1, 'a', 'h1', '2024-01-05', '2024-01-05', NULL),
			(2, 'b', 'h2', '2024-01-05', '2024-01-05', NULL),
			(3, 'c', 'h3', '2024-01-05', '2024-01-05', NULL)`)
	}
	h.exec(`CREATE TABLE it_snap__dbt_tmp (dbt_change_type VARCHAR, ` + snapCols + `)`)
	h.exec(`INSERT INTO it_snap__dbt_tmp VALUES
		('insert', 1, 'a2', 'h1b', '2024-01-09', '2024-01-09', NULL),
		('update', 1, 'a2', 'h1', '2024-01-09', '2024-01-05', '2024-01-09'),
		('delete', 3, 'c', 'h3', '2024-01-05', '2024-01-05', '2024-01-09'),
		('insert', 4, 'd', 'h4', '2024-01-09', '2024-01-09', NULL),
		('update', 1, 'x', 'h0', '2024-01-09', '2024-01-01', '2024-01-09')`)
	const snapshot = `SELECT dbt_scd_id, id, name, CAST(dbt_valid_to AS DATE) FROM it_snap ORDER BY dbt_scd_id`
	want := []string{"h0|1|a0|2024-01-05", "h1|1|a|2024-01-09", "h1b|1|a2|NULL", "h2|2|b|NULL", "h3|3|c|2024-01-09", "h4|4|d|NULL"}

	// dbt-postgres's snapshot_merge_sql: UPDATE … FROM with `::text` casts
	// and the target referenced by its full name, then INSERT … SELECT.
	resetSnapshot()
	update := `update "redis"."public"."it_snap"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "it_snap__dbt_tmp" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "redis"."public"."it_snap".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update', 'delete')
        and "redis"."public"."it_snap".dbt_valid_to is null;`
	insert := `insert into "redis"."public"."it_snap" ("id", "name", "dbt_scd_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to")
    select DBT_INTERNAL_SOURCE."id",DBT_INTERNAL_SOURCE."name",DBT_INTERNAL_SOURCE."dbt_scd_id",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to"
    from "it_snap__dbt_tmp" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert';`
	h.expectAffected(update, 2)
	h.expectAffected(insert, 2)
	h.expectRows(snapshot, want...)
	// The same two statements as one script, as dbt sends them.
	resetSnapshot()
	h.expectAffected(update+"\n\n"+insert, 2)
	h.expectRows(snapshot, want...)

	// dbt-core's default snapshot_merge_sql: the same change as one MERGE.
	resetSnapshot()
	h.expectAffected(`merge into "redis"."public"."it_snap" as DBT_INTERNAL_DEST
    using "it_snap__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("id", "name", "dbt_scd_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to")
        values ("id", "name", "dbt_scd_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to")
    ;`, 4)
	h.expectRows(snapshot, want...)
}

// `x::type` is shorthand for CAST(x AS type) and binds tighter than any
// operator.
func TestSQLCastShorthand(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	h.expectRows(`SELECT '42'::integer + 1, 7::text || 'x', '2024-02-29'::date + 1, '12.345'::numeric(5,2),
			- '5'::int, '1'::int::text || '!', (2 + 3)::text, 3::double precision / 2,
			'2024-01-01 10:00'::timestamp with time zone`,
		"43|7x|2024-03-01|12.35|-5|1!|5|1.5|2024-01-01T10:00:00Z")
	h.expectRows(`SELECT id FROM it_orders WHERE id::text = '3'`, "3")
	h.expectRows(`SELECT id FROM it_orders WHERE status::text = 'pending' AND qty = '3'::int`, "2")
	h.expectRows(`SELECT o.id FROM it_orders o JOIN it_customers c ON c.name::text = 'Bo' AND o.customer_id::text = c.id::text
		ORDER BY o.id`, "3", "4")
	h.expectError(`SELECT 'x'::integer`, "invalid")
	h.expectError(`SELECT 1::nosuchtype`, "unsupported SQL type NOSUCHTYPE")
}

func TestSQLConcatNulls(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP TABLE IF EXISTS it_concat_out")
		h.exec("DROP TABLE IF EXISTS it_concat")
	}
	drop()
	t.Cleanup(drop)
	h.exec("CREATE TABLE it_concat (id INTEGER, first VARCHAR, last VARCHAR)")
	h.exec("INSERT INTO it_concat VALUES (1, 'Ada', 'Lovelace'), (2, 'Bo', NULL), (3, NULL, NULL)")

	// dbt's default concat macro emits CONCAT(…); a NULL part is skipped.
	h.expectRows("SELECT id, CONCAT(first, ' ', last), CONCAT_WS(' ', first, last), first || ' ' || last FROM it_concat ORDER BY id",
		"1|Ada Lovelace|Ada Lovelace|Ada Lovelace", "2|Bo |Bo|NULL", "3| ||NULL")
	h.expectRows("SELECT id FROM it_concat WHERE CONCAT(first, last) = 'Bo'", "2")
	h.exec("CREATE TABLE it_concat_out AS SELECT id, CONCAT_WS('-', id, first, last) AS label FROM it_concat")
	h.expectRows("SELECT label FROM it_concat_out ORDER BY id", "1-Ada-Lovelace", "2-Bo", "3")
	h.expectRows("SELECT data_type FROM information_schema.columns WHERE table_name = 'it_concat_out' AND column_name = 'label'", "VARCHAR")
	h.expectError("SELECT CONCAT_WS() FROM it_concat", "CONCAT_WS expects at least 1 argument")
}

// setupWindows creates the window-function data set. By g, ordered by k
// (NULLs last):
//
//	a: id1 k10 v5 | id2 k20 v3, id3 k20 vNULL (peers) | id4 k40 v8
//	b: id6 k10 v2 | id7 k30 v2 | id5 kNULL v7
//	c: id8 k5 v1
func (h *sqlHarness) setupWindows() {
	h.t.Helper()
	h.exec("DROP TABLE IF EXISTS it_win")
	h.exec(`CREATE TABLE it_win (id INTEGER NOT NULL, g VARCHAR, k INTEGER, v INTEGER, amt NUMERIC(8,2),
		f DOUBLE PRECISION, d DATE)`)
	h.exec(`INSERT INTO it_win VALUES
		(1, 'a', 10, 5, 1.50, 0.5, DATE '2024-01-01'),
		(2, 'a', 20, 3, 2.25, 1.25, DATE '2024-01-02'),
		(3, 'a', 20, NULL, NULL, 2.0, DATE '2024-01-04'),
		(4, 'a', 40, 8, 10.00, NULL, DATE '2024-01-08'),
		(5, 'b', NULL, 7, 3.00, 4.0, DATE '2024-01-03'),
		(6, 'b', 10, 2, 0.75, 0.25, DATE '2024-01-05'),
		(7, 'b', 30, 2, 5.00, 8.0, NULL),
		(8, 'c', 5, 1, 1.00, 1.0, DATE '2024-01-06')`)
	h.t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_win") })
}

func TestSQLWindowRanking(t *testing.T) {
	h := newSQLHarness(t)
	h.setupWindows()

	// Peers (k = 20 in a) share RANK / DENSE_RANK / PERCENT_RANK / CUME_DIST;
	// ROW_NUMBER breaks the tie with id. NULL k sorts last.
	schema := h.expectRows(`SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY k, id) AS rn,
			RANK() OVER w, DENSE_RANK() OVER w, PERCENT_RANK() OVER w, CUME_DIST() OVER w
		FROM it_win WINDOW w AS (PARTITION BY g ORDER BY k) ORDER BY id`,
		"1|1|1|1|0|0.25",
		"2|2|2|2|0.3333333333333333|0.75",
		"3|3|2|2|0.3333333333333333|0.75",
		"4|4|4|3|1|1",
		"5|3|3|3|1|1",
		"6|1|1|1|0|0.3333333333333333",
		"7|2|2|2|0.5|0.6666666666666666",
		"8|1|1|1|0|1")
	for i, want := range []arrow.Type{arrow.INT32, arrow.INT64, arrow.INT64, arrow.INT64, arrow.FLOAT64, arrow.FLOAT64} {
		if got := schema.Field(i).Type.ID(); got != want {
			t.Errorf("column %d type = %s, want %s", i, got, want)
		}
	}

	// NULL ordering: last by default in both directions (as in this driver's
	// ORDER BY; PostgreSQL puts them first for DESC), or as requested.
	h.expectRows(`SELECT id, RANK() OVER (ORDER BY k DESC), RANK() OVER (ORDER BY k NULLS FIRST),
			RANK() OVER (ORDER BY k DESC NULLS FIRST), RANK() OVER (ORDER BY k NULLS LAST)
		FROM it_win ORDER BY id`,
		"1|5|3|6|2", "2|3|5|4|4", "3|3|5|4|4", "4|1|8|2|7",
		"5|8|1|1|8", "6|5|3|6|2", "7|2|7|3|6", "8|7|2|8|1")

	// NTILE: the first buckets get the extra rows; more buckets than rows.
	h.expectRows(`SELECT id, NTILE(3) OVER (ORDER BY id), NTILE(2) OVER (PARTITION BY g ORDER BY id),
			NTILE(10) OVER (PARTITION BY g ORDER BY id), NTILE(NULL) OVER ()
		FROM it_win ORDER BY id`,
		"1|1|1|1|NULL", "2|1|1|2|NULL", "3|1|2|3|NULL", "4|2|2|4|NULL",
		"5|2|1|1|NULL", "6|2|1|2|NULL", "7|3|2|3|NULL", "8|3|1|1|NULL")

	// Without ORDER BY every row of a partition is a peer.
	h.expectRows(`SELECT id, ROW_NUMBER() OVER (), RANK() OVER (), DENSE_RANK() OVER (PARTITION BY g),
			PERCENT_RANK() OVER (PARTITION BY g), CUME_DIST() OVER (PARTITION BY g)
		FROM it_win WHERE id <= 5 ORDER BY id`,
		"1|1|1|1|0|1", "2|2|1|1|0|1", "3|3|1|1|0|1", "4|4|1|1|0|1", "5|5|1|1|0|1")

	// Ranking by an expression and over strings.
	h.expectRows(`SELECT g, id, DENSE_RANK() OVER (ORDER BY g DESC) FROM it_win WHERE id IN (1, 5, 6, 8) ORDER BY id`,
		"a|1|3", "b|5|2", "b|6|2", "c|8|1")
	h.expectRows(`SELECT id, RANK() OVER (PARTITION BY v IS NULL ORDER BY COALESCE(v, 0) * -1) FROM it_win WHERE g = 'a' ORDER BY id`,
		"1|2", "2|3", "3|1", "4|1")
}

func TestSQLWindowOffsetFunctions(t *testing.T) {
	h := newSQLHarness(t)
	h.setupWindows()

	// LAG / LEAD stay inside the partition. The default is used only when
	// there is no such row (not when its value is NULL); a negative offset
	// looks the other way; a NULL offset gives NULL.
	h.expectRows(`SELECT id, LAG(v) OVER w, LEAD(v) OVER w, LAG(v, 2) OVER w, LEAD(v, 1, 0) OVER w,
			LAG(v, -1) OVER w, LAG(v, NULL) OVER w
		FROM it_win WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id`,
		"1|NULL|3|NULL|3|3|NULL",
		"2|5|NULL|NULL|NULL|NULL|NULL",
		"3|3|8|5|8|8|NULL",
		"4|NULL|NULL|3|0|NULL|NULL",
		"5|NULL|2|NULL|2|2|NULL",
		"6|7|2|NULL|2|2|NULL",
		"7|2|NULL|7|0|NULL|NULL",
		"8|NULL|NULL|NULL|0|NULL|NULL")
	schema := h.expectRows(`SELECT LAG(g, 1, 'none') OVER (ORDER BY id), LEAD(amt, 1, 0) OVER (ORDER BY id),
			LAG(d) OVER (ORDER BY id) FROM it_win WHERE id IN (1, 2)`,
		"none|2.25|NULL", "a|0.00|2024-01-01")
	if s := schema.String(); !strings.Contains(s, "utf8") || !strings.Contains(s, "decimal(38, 2)") || !strings.Contains(s, "date32") {
		t.Errorf("LAG/LEAD types = %s", s)
	}

	// FIRST_VALUE / LAST_VALUE / NTH_VALUE use the frame: by default it ends
	// at the current row, so LAST_VALUE is the current row's value.
	h.expectRows(`SELECT id, FIRST_VALUE(v) OVER w, LAST_VALUE(v) OVER w, NTH_VALUE(v, 2) OVER w,
			LAST_VALUE(v) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
			NTH_VALUE(v, 3) OVER (w ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
		FROM it_win WINDOW w AS (PARTITION BY g ORDER BY k, id) ORDER BY id`,
		"1|5|5|NULL|8|NULL",
		"2|5|3|3|8|NULL",
		"3|5|NULL|3|8|8",
		"4|5|8|3|8|NULL",
		"5|2|7|2|7|NULL",
		"6|2|2|NULL|7|NULL",
		"7|2|2|2|7|7",
		"8|1|1|NULL|1|NULL")
	// An empty frame gives NULL.
	h.expectRows(`SELECT id, FIRST_VALUE(id) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
			LAST_VALUE(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING)
		FROM it_win WHERE g = 'a' ORDER BY id`,
		"1|NULL|4", "2|1|4", "3|1|4", "4|2|NULL")
}

func TestSQLWindowAggregates(t *testing.T) {
	h := newSQLHarness(t)
	h.setupWindows()

	// Default frames. With ORDER BY: RANGE UNBOUNDED PRECEDING to CURRENT ROW,
	// which includes the current row's peers (ids 2 and 3 both see 3 rows);
	// a NULL key is a peer group of its own. Without ORDER BY: the partition.
	h.expectRows(`SELECT id, COUNT(*) OVER w, COUNT(v) OVER w, SUM(v) OVER w, AVG(v) OVER w, MIN(v) OVER w,
			MAX(v) OVER w, SUM(v) OVER (PARTITION BY g)
		FROM it_win WINDOW w AS (PARTITION BY g ORDER BY k) ORDER BY id`,
		"1|1|1|5|5|5|5|16",
		"2|3|2|8|4|3|5|16",
		"3|3|2|8|4|3|5|16",
		"4|4|3|16|5.333333333333333|3|8|16",
		"5|3|3|11|3.6666666666666665|2|7|11",
		"6|1|1|2|2|2|2|11",
		"7|2|2|4|2|2|2|11",
		"8|1|1|1|1|1|1|1")

	// ROWS frames with every kind of bound (v by id: 5 3 NULL 8 7 2 2 1).
	h.expectRows(`SELECT id,
			SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
			SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
			SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
			SUM(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
			SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING),
			SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		FROM it_win ORDER BY id`,
		"1|5|8|28|NULL|11|28",
		"2|8|8|23|5|15|28",
		"3|8|11|20|8|17|28",
		"4|16|15|20|3|11|28",
		"5|23|17|12|8|5|28",
		"6|25|11|5|15|3|28",
		"7|27|5|3|9|1|28",
		"8|28|3|1|4|NULL|28")
	h.expectRows(`SELECT id,
			COUNT(*) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING),
			SUM(v) OVER (ORDER BY id ROWS 2 PRECEDING),
			MIN(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
			MAX(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
			AVG(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING),
			COUNT(v) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING)
		FROM it_win ORDER BY id`,
		"1|0|5|3|5|4|1",
		"2|1|8|3|5|3|2",
		"3|2|8|3|5|8|2",
		"4|2|11|7|8|7.5|3",
		"5|2|15|2|8|4.5|4",
		"6|2|17|2|8|2|5",
		"7|2|11|1|7|1.5|6",
		"8|2|5|1|2|1|7")

	// RANGE frames with numeric offsets, ascending and descending. A NULL key
	// is its own frame for offset bounds, and other rows' frames don't reach
	// the NULLs (wherever NULLS FIRST / LAST puts them).
	h.expectRows(`SELECT id,
			SUM(v) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING),
			COUNT(*) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
			SUM(v) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND 15 PRECEDING),
			SUM(v) OVER (PARTITION BY g ORDER BY k DESC RANGE BETWEEN 10 PRECEDING AND CURRENT ROW),
			SUM(v) OVER (PARTITION BY g ORDER BY k NULLS FIRST RANGE BETWEEN 20 PRECEDING AND CURRENT ROW)
		FROM it_win ORDER BY id`,
		"1|8|4|NULL|8|5",
		"2|8|3|NULL|3|8",
		"3|8|3|NULL|3|8",
		"4|8|1|8|8|11",
		"5|7|1|11|7|7",
		"6|2|3|NULL|2|2",
		"7|2|2|2|2|4",
		"8|1|1|NULL|1|1")
	// RANGE over doubles (f sorted: 0.25 0.5 1.0 1.25 2.0 4.0 8.0 NULL).
	h.expectRows(`SELECT id, COUNT(*) OVER (ORDER BY f RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING) FROM it_win ORDER BY id`,
		"1|3", "2|2", "3|1", "4|1", "5|1", "6|2", "7|1", "8|3")
	// RANGE over dates with interval offsets.
	h.expectRows(`SELECT id, COUNT(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '2 days' PRECEDING AND CURRENT ROW),
			SUM(v) OVER (ORDER BY d RANGE BETWEEN CURRENT ROW AND INTERVAL '1 day' FOLLOWING)
		FROM it_win ORDER BY id`,
		"1|1|8", "2|2|10", "3|3|2", "4|2|8", "5|3|7", "6|3|3", "7|1|2", "8|3|1")

	// GROUPS frames count peer groups: {8} {1,6} {2,3} {7} {4} {5}.
	h.expectRows(`SELECT id,
			SUM(v) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
			COUNT(*) OVER (ORDER BY k GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING),
			SUM(v) OVER (ORDER BY k GROUPS 1 PRECEDING),
			SUM(v) OVER (ORDER BY k GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING)
		FROM it_win ORDER BY id`,
		"1|11|5|8|1", "2|12|4|10|8", "3|12|4|10|8", "4|17|2|10|5",
		"5|15|1|15|10", "6|11|5|8|1", "7|13|3|5|10", "8|8|5|1|NULL")

	// Result types follow the aggregates': decimal SUM stays decimal, AVG and
	// floating-point SUM are doubles, MIN / MAX keep the argument's type.
	schema := h.expectRows(`SELECT id, SUM(amt) OVER (ORDER BY id),
			AVG(amt) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
			SUM(f) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), MIN(d) OVER (), MAX(g) OVER (),
			SUM(v) OVER (), COUNT(*) OVER ()
		FROM it_win ORDER BY id`,
		"1|1.50|1.5|1.75|2024-01-01|c|28|8",
		"2|3.75|1.875|3.75|2024-01-01|c|28|8",
		"3|3.75|2.25|3.25|2024-01-01|c|28|8",
		"4|13.75|10|6|2024-01-01|c|28|8",
		"5|16.75|6.5|4.25|2024-01-01|c|28|8",
		"6|17.50|1.875|12.25|2024-01-01|c|28|8",
		"7|22.50|2.875|9.25|2024-01-01|c|28|8",
		"8|23.50|3|9|2024-01-01|c|28|8")
	if s := schema.String(); !strings.Contains(s, "OVER (ORDER BY id): type=decimal(38, 2)") ||
		!strings.Contains(s, "CURRENT ROW): type=float64") || !strings.Contains(s, "MIN(d) OVER (): type=date32") ||
		!strings.Contains(s, "MAX(g) OVER (): type=utf8") || !strings.Contains(s, "SUM(v) OVER (): type=int64") ||
		!strings.Contains(s, "COUNT(*) OVER (): type=int64") {
		t.Errorf("window aggregate types = %s", s)
	}

	// Window calls inside expressions and scalar functions, and scalar
	// functions in window arguments and keys.
	h.expectRows(`SELECT id, ROUND(AVG(v) OVER (PARTITION BY g), 2), COALESCE(LAG(v) OVER (ORDER BY id), 0),
			ROUND(100.0 * v / SUM(v) OVER (), 1), ROW_NUMBER() OVER (PARTITION BY UPPER(g) ORDER BY ABS(k - 20), id),
			CASE WHEN ROW_NUMBER() OVER (PARTITION BY g ORDER BY id DESC) = 1 THEN 'latest' ELSE 'old' END,
			SUM(v) OVER (PARTITION BY g) - v
		FROM it_win ORDER BY id`,
		"1|5.33|0|17.9|3|old|11",
		"2|5.33|5|10.7|1|old|13",
		"3|5.33|3|NULL|2|old|NULL",
		"4|5.33|0|28.6|4|latest|8",
		"5|3.67|8|25|3|old|4",
		"6|3.67|7|7.1|1|old|9",
		"7|3.67|2|7.1|2|latest|9",
		"8|1|2|3.6|1|latest|0")

	// Empty input, all-NULL partitions, and empty frames.
	h.expectRows(`SELECT id, ROW_NUMBER() OVER (), SUM(v) OVER () FROM it_win WHERE id > 100`)
	h.expectRows(`SELECT id, SUM(amt) OVER (PARTITION BY amt IS NULL), COUNT(amt) OVER (PARTITION BY amt IS NULL),
			MAX(amt) OVER (PARTITION BY amt IS NULL)
		FROM it_win WHERE id <= 4 ORDER BY id`,
		"1|13.75|3|10.00", "2|13.75|3|10.00", "3|NULL|0|NULL", "4|13.75|3|10.00")
	h.expectRows(`SELECT COUNT(*), SUM(COUNT(*)) OVER () FROM it_win WHERE id > 100`, "0|0")
}

func TestSQLWindowGrouped(t *testing.T) {
	h := newSQLHarness(t)
	h.setupWindows()

	// Windows over GROUP BY see one row per group. COUNT and integer SUM are
	// reduced by the index; the windows run on the groups.
	h.expectRows(`SELECT g, COUNT(*) AS n, SUM(COUNT(*)) OVER (ORDER BY g) AS running,
			RANK() OVER (ORDER BY SUM(v) DESC) AS r, SUM(v) AS total, LAG(g) OVER (ORDER BY g)
		FROM it_win GROUP BY g ORDER BY g`,
		"a|4|4|1|16|NULL", "b|3|7|2|11|a", "c|1|8|3|1|b")
	// Decimal SUM (aggregated by the driver).
	schema := h.expectRows(`SELECT g, SUM(amt), SUM(SUM(amt)) OVER (),
			MAX(SUM(amt)) OVER (ORDER BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
		FROM it_win GROUP BY g ORDER BY g`,
		"a|13.75|23.50|13.75", "b|8.75|23.50|13.75", "c|1.00|23.50|8.75")
	if dt := schema.Field(2).Type; dt.ID() != arrow.DECIMAL128 {
		t.Errorf("SUM(SUM(amt)) OVER () type = %s, want decimal", dt)
	}
	// After HAVING; in ORDER BY; QUALIFY over aggregates.
	h.expectRows(`SELECT g, COUNT(*), ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) FROM it_win
		GROUP BY g HAVING COUNT(*) > 1 ORDER BY g`, "a|4|1", "b|3|2")
	h.expectRows(`SELECT g FROM it_win GROUP BY g ORDER BY RANK() OVER (ORDER BY COUNT(*)), g`, "c", "b", "a")
	h.expectRows(`SELECT g, SUM(v) AS total FROM it_win GROUP BY g
		QUALIFY RANK() OVER (ORDER BY SUM(v) DESC) <= 2 ORDER BY total`, "b|11", "a|16")
	// Grouped by an expression, partitioned by a grouped column.
	h.expectRows(`SELECT g, v IS NULL AS missing, COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY g)
		FROM it_win WHERE g IN ('a', 'b') GROUP BY g, v IS NULL ORDER BY g, missing`,
		"a|false|3|4", "a|true|1|4", "b|false|3|3")
}

func TestSQLWindowQueries(t *testing.T) {
	h := newSQLHarness(t)
	h.setupWindows()
	h.setupOrders()
	h.exec("DROP TABLE IF EXISTS it_events")
	h.exec("DROP TABLE IF EXISTS it_events_dedup")
	h.exec("DROP TABLE IF EXISTS it_win_ins")
	h.exec("DROP VIEW IF EXISTS it_events_ranked")
	h.exec("DROP VIEW IF EXISTS it_events_latest")
	t.Cleanup(func() {
		h.exec("DROP VIEW IF EXISTS it_events_ranked")
		h.exec("DROP VIEW IF EXISTS it_events_latest")
		h.exec("DROP TABLE IF EXISTS it_events")
		h.exec("DROP TABLE IF EXISTS it_events_dedup")
		h.exec("DROP TABLE IF EXISTS it_win_ins")
	})
	h.exec(`CREATE TABLE it_events (k VARCHAR, ts TIMESTAMP, payload VARCHAR)`)
	h.exec(`INSERT INTO it_events VALUES
		('a', TIMESTAMP '2024-01-01 10:00:00', 'a1'),
		('a', TIMESTAMP '2024-01-03 09:00:00', 'a3'),
		('a', TIMESTAMP '2024-01-02 12:00:00', 'a2'),
		('b', TIMESTAMP '2024-01-05 00:00:00', 'b1'),
		('c', TIMESTAMP '2024-01-01 00:00:00', 'c1'),
		('c', TIMESTAMP '2023-12-31 00:00:00', 'c0')`)

	// The dbt de-duplication idiom: the latest row per key. As a derived
	// table without an alias, with one, as a CTE, and with QUALIFY.
	h.expectRows(`select * from (select *, row_number() over (partition by k order by ts desc) as rn from it_events)
		where rn = 1 order by k`,
		"a|2024-01-03T09:00:00|a3|1", "b|2024-01-05T00:00:00|b1|1", "c|2024-01-01T00:00:00|c1|1")
	h.expectRows(`select deduped.k, deduped.payload from (
			select k, payload, row_number() over (partition by k order by ts desc) as rn from it_events
		) as deduped where deduped.rn = 1 order by deduped.k`,
		"a|a3", "b|b1", "c|c1")
	h.expectRows(`with ranked as (select k, payload, row_number() over (partition by k order by ts desc) as rn from it_events)
		select k, payload from ranked where rn = 1 order by k`,
		"a|a3", "b|b1", "c|c1")
	h.expectRows(`select k, payload from it_events qualify row_number() over (partition by k order by ts desc) = 1 order by k`,
		"a|a3", "b|b1", "c|c1")
	h.expectRows(`select k, payload, row_number() over (partition by k order by ts desc) as rn from it_events
		qualify rn > 1 order by k, rn`,
		"a|a2|2", "a|a1|3", "c|c0|2")
	// dbt table materialization (CTAS) of the de-duplicated rows.
	if n := h.exec(`create table it_events_dedup as select * from (
			select *, row_number() over (partition by k order by ts desc) as rn from it_events) x where rn = 1`); n != 3 {
		t.Errorf("CTAS inserted %d rows, want 3", n)
	}
	h.expectRows(`SELECT k, payload, rn FROM it_events_dedup WHERE rn = 1 ORDER BY k`, "a|a3|1", "b|b1|1", "c|c1|1")
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_events_dedup'
		ORDER BY ordinal_position`,
		"k|VARCHAR", "ts|TIMESTAMP(6)", "payload|VARCHAR", "rn|BIGINT")
	// Snapshot-style validity ranges with LEAD.
	h.expectRows(`SELECT payload, CAST(LEAD(ts) OVER (PARTITION BY k ORDER BY ts) AS VARCHAR) AS valid_to
		FROM it_events WHERE k = 'a' ORDER BY ts`,
		"a1|2024-01-02 12:00:00", "a2|2024-01-03 09:00:00", "a3|NULL")

	// A view with a window function is computed before the outer filter:
	// pushing `payload <> 'a3'` below the window would renumber the rows.
	h.exec(`CREATE VIEW it_events_ranked AS
		SELECT k, payload, row_number() over (partition by k order by ts desc) as rn FROM it_events`)
	h.expectRows(`SELECT k, payload, rn FROM it_events_ranked WHERE payload <> 'a3' AND k = 'a' ORDER BY rn`,
		"a|a2|2", "a|a1|3")
	h.exec(`CREATE VIEW it_events_latest AS SELECT k, payload FROM it_events_ranked WHERE rn = 1`)
	h.expectRows(`SELECT payload FROM it_events_latest ORDER BY k`, "a3", "b1", "c1")
	h.expectRows(`SELECT data_type FROM information_schema.columns WHERE table_name = 'it_events_ranked' AND column_name = 'rn'`,
		"BIGINT")

	// LIMIT / OFFSET apply after the windows, also when the ORDER BY could be
	// answered by the index.
	h.expectRows(`SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS rn FROM it_win ORDER BY id LIMIT 3`,
		"1|8", "2|7", "3|6")
	h.expectRows(`SELECT id, SUM(v) OVER () AS total, COUNT(*) OVER () FROM it_win ORDER BY id LIMIT 2 OFFSET 1`,
		"2|28|8", "3|28|8")
	h.expectRows(`SELECT id FROM it_win QUALIFY ROW_NUMBER() OVER (ORDER BY id) > 5 LIMIT 2`, "6", "7")
	h.expectRows(`SELECT id, LAG(id) OVER (ORDER BY id) FROM it_win LIMIT 2 OFFSET 3`, "4|3", "5|4")
	// WHERE runs first (in the index here).
	h.expectRows(`SELECT id, ROW_NUMBER() OVER (ORDER BY id), COUNT(*) OVER () FROM it_win WHERE g = 'b' ORDER BY id`,
		"5|1|3", "6|2|3", "7|3|3")

	// ORDER BY a window function, its alias, or its position.
	h.expectRows(`SELECT id, SUM(v) OVER (ORDER BY id) AS run FROM it_win ORDER BY run DESC, id LIMIT 3`,
		"8|28", "7|27", "6|25")
	h.expectRows(`SELECT id FROM it_win ORDER BY ROW_NUMBER() OVER (ORDER BY v DESC NULLS FIRST, id) LIMIT 3`, "3", "4", "5")
	h.expectRows(`SELECT id, RANK() OVER (ORDER BY k) FROM it_win ORDER BY 2 DESC, 1 LIMIT 3`, "5|8", "4|7", "7|6")

	// Named windows: OVER w, a window built on another, and OVER (w …).
	h.expectRows(`SELECT id, ROW_NUMBER() OVER w2, COUNT(*) OVER w,
			SUM(v) OVER (w2 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), FIRST_VALUE(id) OVER (w ORDER BY v DESC)
		FROM it_win WINDOW w AS (PARTITION BY g), w2 AS (w ORDER BY id) ORDER BY id`,
		"1|1|4|5|4", "2|2|4|8|4", "3|3|4|3|4", "4|4|4|8|4",
		"5|1|3|7|5", "6|2|3|9|5", "7|3|3|4|5", "8|1|1|1|8")
	h.expectRows(`SELECT id, SUM(v) OVER w FROM it_win WHERE g = 'a' WINDOW w AS (ORDER BY id ROWS 1 PRECEDING) ORDER BY id`,
		"1|5", "2|8", "3|3", "4|8")
	h.expectRows(`SELECT id FROM it_win QUALIFY ROW_NUMBER() OVER w = 1 WINDOW w AS (PARTITION BY g ORDER BY id DESC) ORDER BY id`,
		"4", "7", "8")

	// Over joins, CTEs, derived tables and subqueries.
	h.expectRows(`SELECT o.id, c.name, ROW_NUMBER() OVER (PARTITION BY c.country ORDER BY o.amount DESC) AS rn
		FROM it_orders o JOIN it_customers c ON o.customer_id = c.id ORDER BY o.id`,
		"1|Ada|2", "2|Ada|1", "3|Bo|2", "4|Bo|1", "5|Cy|3")
	h.expectRows(`WITH t AS (SELECT g, v, SUM(v) OVER (PARTITION BY g) AS gs FROM it_win)
		SELECT g, MAX(gs) FROM t GROUP BY g ORDER BY g`, "a|16", "b|11", "c|1")
	h.expectRows(`SELECT (SELECT MAX(rn) FROM (SELECT ROW_NUMBER() OVER () AS rn FROM it_win) x)`, "8")
	h.expectRows(`SELECT id FROM it_win WHERE id IN (SELECT id FROM it_win QUALIFY RANK() OVER (PARTITION BY g ORDER BY v DESC) = 1)
		ORDER BY id`, "4", "5", "8")
	// A correlated subquery's LIMIT also comes after its windows.
	h.expectRows(`SELECT c.name, (SELECT SUM(o.qty) OVER () FROM it_orders o WHERE o.customer_id = c.id LIMIT 1)
		FROM it_customers c ORDER BY c.id`, "Ada|4", "Bo|12", "Cy|1", "Di|NULL")
	// A temporary table.
	h.exec(`CREATE TEMP TABLE it_win_tmp AS SELECT id, g, v FROM it_win WHERE g <> 'c'`)
	h.expectRows(`SELECT id, DENSE_RANK() OVER (PARTITION BY g ORDER BY v DESC) FROM it_win_tmp ORDER BY id`,
		"1|2", "2|3", "3|4", "4|1", "5|1", "6|2", "7|2")
	h.exec(`DROP TABLE it_win_tmp`)
	// A temporary view with a window is computed, not expanded in place: the
	// outer filter must not renumber the rows.
	h.exec(`CREATE TEMP VIEW it_win_tv AS SELECT id, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn FROM it_win`)
	h.expectRows(`SELECT id, rn FROM it_win_tv WHERE id > 2 AND g = 'a' ORDER BY id`, "3|3", "4|4")
	h.exec(`DROP VIEW it_win_tv`)
	// Set operation branches, and SELECT without FROM.
	h.expectRows(`SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM it_win WHERE g = 'c'
		UNION ALL SELECT id, RANK() OVER (ORDER BY v) FROM it_win WHERE g = 'b' ORDER BY 1`,
		"5|3", "6|1", "7|1", "8|1")
	h.expectRows(`SELECT ROW_NUMBER() OVER (), COUNT(*) OVER (), SUM(5) OVER ()`, "1|1|5")
	h.expectRows(`SELECT 1 AS x QUALIFY ROW_NUMBER() OVER () = 2`)

	// CTAS keeps the window result types; INSERT … SELECT.
	h.exec("DROP TABLE IF EXISTS it_win_ctas")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_win_ctas") })
	h.exec(`CREATE TABLE it_win_ctas AS SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn,
			PERCENT_RANK() OVER (ORDER BY id) AS pr, SUM(amt) OVER (PARTITION BY g) AS gsum,
			LAG(v) OVER (ORDER BY id) AS prev_v, AVG(v) OVER () AS av
		FROM it_win`)
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_win_ctas'
		ORDER BY ordinal_position`,
		"id|INTEGER", "rn|BIGINT", "pr|DOUBLE PRECISION", "gsum|NUMERIC(38,2)", "prev_v|INTEGER", "av|DOUBLE PRECISION")
	h.expectRows(`SELECT id, gsum, prev_v FROM it_win_ctas WHERE rn = 1 ORDER BY id`, "1|13.75|NULL", "5|8.75|8", "8|1.00|2")
	h.exec(`CREATE TABLE it_win_ins (id INTEGER, rnk BIGINT)`)
	if n := h.exec(`INSERT INTO it_win_ins SELECT id, RANK() OVER (ORDER BY k DESC) FROM it_win WHERE g = 'a'`); n != 4 {
		t.Errorf("INSERT … SELECT inserted %d rows, want 4", n)
	}
	h.expectRows(`SELECT id, rnk FROM it_win_ins ORDER BY id`, "1|4", "2|2", "3|2", "4|1")

	// A dbt incremental merge whose source keeps only the latest row per key.
	h.exec(`INSERT INTO it_events VALUES ('a', TIMESTAMP '2024-01-04 00:00:00', 'a4'), ('d', TIMESTAMP '2024-01-01 00:00:00', 'd1')`)
	h.expectAffected(`MERGE INTO it_events_dedup t USING (
			SELECT k, ts, payload FROM it_events QUALIFY row_number() over (partition by k order by ts desc) = 1
		) s ON t.k = s.k
		WHEN MATCHED THEN UPDATE SET ts = s.ts, payload = s.payload
		WHEN NOT MATCHED THEN INSERT (k, ts, payload, rn) VALUES (s.k, s.ts, s.payload, 1)`, 4)
	h.expectRows(`SELECT k, payload, rn FROM it_events_dedup ORDER BY k`, "a|a4|1", "b|b1|1", "c|c1|1", "d|d1|1")
}

func TestSQLWindowErrors(t *testing.T) {
	h := newSQLHarness(t)
	h.setupWindows()

	// Where window functions may not appear.
	h.expectError(`SELECT id FROM it_win WHERE ROW_NUMBER() OVER () = 1`, "not allowed in WHERE")
	h.expectError(`SELECT COUNT(*) FROM it_win GROUP BY ROW_NUMBER() OVER ()`, "not allowed in GROUP BY")
	h.expectError(`SELECT ROW_NUMBER() OVER () AS rn, COUNT(*) FROM it_win GROUP BY rn`, "not allowed in GROUP BY")
	h.expectError(`SELECT g FROM it_win GROUP BY g HAVING RANK() OVER (ORDER BY g) = 1`, "not allowed in HAVING")
	h.expectError(`SELECT 1 FROM it_win a JOIN it_win b ON ROW_NUMBER() OVER () = b.id`, "not allowed in JOIN")
	h.expectError(`SELECT SUM(ROW_NUMBER() OVER ()) FROM it_win`, "cannot contain window function calls")
	h.expectError(`SELECT ROW_NUMBER() OVER (ORDER BY ROW_NUMBER() OVER ()) FROM it_win`, "cannot be nested")
	h.expectError(`SELECT LAG(LEAD(v) OVER ()) OVER () FROM it_win`, "cannot be nested")
	h.expectError(`UPDATE it_win SET v = ROW_NUMBER() OVER ()`, "only allowed in a SELECT list")
	h.expectError(`DELETE FROM it_win WHERE ROW_NUMBER() OVER () = 1`, "only allowed in a SELECT list")
	h.expectError(`INSERT INTO it_win (id) VALUES (ROW_NUMBER() OVER ())`, "only allowed in a SELECT list")
	h.expectError(`UPDATE it_win t SET v = ROW_NUMBER() OVER () FROM it_win s WHERE s.id = t.id`, "only allowed in a SELECT list")
	h.expectError(`DELETE FROM it_win t USING it_win s WHERE s.id = t.id AND RANK() OVER () = 1`, "only allowed in a SELECT list")
	h.expectError(`MERGE INTO it_win t USING it_win s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = RANK() OVER ()`,
		"only allowed in a SELECT list")
	h.expectError(`MERGE INTO it_win t USING it_win s ON t.id = s.id WHEN MATCHED AND LAG(s.v) OVER () > 0 THEN DELETE`,
		"only allowed in a SELECT list")
	h.expectError(`MERGE INTO it_win t USING it_win s ON t.id = s.id AND ROW_NUMBER() OVER () = 1 WHEN MATCHED THEN DELETE`,
		"not allowed in JOIN conditions")
	h.expectRows(`SELECT COUNT(*), SUM(v) FROM it_win`, "8|28") // nothing was changed

	// Functions and arguments.
	h.expectError(`SELECT ROW_NUMBER() FROM it_win`, "requires an OVER clause")
	h.expectError(`SELECT id FROM it_win ORDER BY RANK()`, "requires an OVER clause")
	h.expectError(`SELECT LOWER(g) OVER () FROM it_win`, "not a window function")
	h.expectError(`SELECT COUNT(DISTINCT g) OVER () FROM it_win`, "DISTINCT is not supported")
	h.expectError(`SELECT ROW_NUMBER(id) OVER () FROM it_win`, "expects no arguments")
	h.expectError(`SELECT NTILE() OVER () FROM it_win`, "NTILE expects 1 argument")
	h.expectError(`SELECT NTILE(0) OVER () FROM it_win`, "greater than zero")
	h.expectError(`SELECT NTH_VALUE(v, 0) OVER () FROM it_win`, "greater than zero")
	h.expectError(`SELECT LAG(v, 'x') OVER () FROM it_win`, "must be an integer")
	h.expectError(`SELECT SUM(*) OVER () FROM it_win`, "SUM does not accept *")
	h.expectError(`SELECT id, ROW_NUMBER() OVER (ORDER BY rn) AS rn FROM it_win`, "does not exist")

	// Frames.
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM it_win`,
		"frame start cannot be UNBOUNDED FOLLOWING")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM it_win`,
		"frame end cannot be UNBOUNDED PRECEDING")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM it_win`,
		"cannot have preceding rows")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS 1 FOLLOWING) FROM it_win`, "cannot have preceding rows")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM it_win`,
		"frame starting offset must not be negative")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND NULL FOLLOWING) FROM it_win`,
		"frame ending offset must not be null")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN v PRECEDING AND CURRENT ROW) FROM it_win`, "must be constants")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM it_win`, "must be integers")
	h.expectError(`SELECT SUM(v) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM it_win`, "exactly one ORDER BY column")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY g RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM it_win`,
		"not supported for ORDER BY type VARCHAR")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM it_win`,
		"not supported for ORDER BY type DATE")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) FROM it_win`,
		"not supported for ORDER BY type INTEGER")
	h.expectError(`SELECT SUM(v) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM it_win`, "GROUPS mode requires an ORDER BY")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE OTHERS) FROM it_win`,
		"expected CURRENT ROW, GROUP, TIES or NO OTHERS after EXCLUDE")
	h.expectError(`SELECT SUM(v) OVER (ORDER BY id ROWS 1) FROM it_win`, "expected PRECEDING or FOLLOWING")

	// Named windows.
	h.expectError(`SELECT SUM(v) OVER w FROM it_win`, `window "w" does not exist`)
	h.expectError(`SELECT SUM(v) OVER (w) FROM it_win WINDOW w2 AS ()`, `window "w" does not exist`)
	h.expectError(`SELECT SUM(v) OVER (w ORDER BY v) FROM it_win WINDOW w AS (ORDER BY id)`, "cannot override ORDER BY")
	h.expectError(`SELECT SUM(v) OVER (w PARTITION BY id) FROM it_win WINDOW w AS (PARTITION BY g)`, "cannot override PARTITION BY")
	h.expectError(`SELECT SUM(v) OVER (w) FROM it_win WINDOW w AS (ORDER BY id ROWS 1 PRECEDING)`, "cannot copy window")
	h.expectError(`SELECT 1 FROM it_win WINDOW w AS (), w AS ()`, "already defined")
	h.expectError(`SELECT 1 FROM it_win UNION SELECT 2 ORDER BY ROW_NUMBER() OVER ()`, "output column names or positions")
}

// TestSQLWindowAcrossPages runs windows over rows read in several cursor
// pages, partitioned and with LIMIT.
func TestSQLWindowAcrossPages(t *testing.T) {
	h := newSQLHarness(t)
	const n = 2*cursorCount + 500
	h.exec("DROP TABLE IF EXISTS it_win_pages")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_win_pages") })
	h.exec("CREATE TABLE it_win_pages (id BIGINT, g INTEGER)")
	var values []string
	for i := 1; i <= n; i++ {
		values = append(values, fmt.Sprintf("(%d, %d)", i, i%3))
	}
	h.exec("INSERT INTO it_win_pages VALUES " + strings.Join(values, ", "))

	var inGroup [3]int
	for i := 1; i <= n; i++ {
		inGroup[i%3]++
	}
	h.expectRows(`SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC), SUM(id) OVER (ORDER BY id ROWS 2 PRECEDING),
			COUNT(*) OVER (PARTITION BY g) FROM it_win_pages ORDER BY id LIMIT 3`,
		fmt.Sprintf("1|%d|1|%d", n, inGroup[1]), fmt.Sprintf("2|%d|3|%d", n-1, inGroup[2]), fmt.Sprintf("3|%d|6|%d", n-2, inGroup[0]))
	h.expectRows(`SELECT MAX(s), COUNT(*) FROM (SELECT SUM(id) OVER (ORDER BY id) AS s FROM it_win_pages) x`,
		fmt.Sprintf("%d|%d", n*(n+1)/2, n))
	h.expectRows(`SELECT id FROM it_win_pages QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY id DESC) = 1 ORDER BY id`,
		fmt.Sprint(n-2), fmt.Sprint(n-1), fmt.Sprint(n))
}

func TestSQLSelectDistinct(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_dist_v")
		h.exec("DROP TABLE IF EXISTS it_dist_out")
		h.exec("DROP TABLE IF EXISTS it_dist")
	}
	drop()
	t.Cleanup(drop)
	h.exec("CREATE TABLE it_dist (id INTEGER, g VARCHAR, v INTEGER, s VARCHAR NOINDEX)")
	h.exec(`INSERT INTO it_dist VALUES (1, 'a', 1, 'x'), (2, 'a', 1, 'x'), (3, 'a', 2, NULL),
		(4, 'b', NULL, NULL), (5, 'b', NULL, NULL), (6, NULL, 3, 'y')`)

	// Duplicates go; NULLs are equal to each other (and sort last).
	h.expectRows("SELECT DISTINCT g FROM it_dist ORDER BY g", "a", "b", "NULL")
	h.expectRows("SELECT DISTINCT g, v FROM it_dist ORDER BY g, v", "a|1", "a|2", "b|NULL", "NULL|3")
	h.expectRows("SELECT DISTINCT v, s FROM it_dist ORDER BY v", "1|x", "2|NULL", "3|y", "NULL|NULL")
	h.expectRows("SELECT DISTINCT v * 10 AS x FROM it_dist ORDER BY x", "10", "20", "30", "NULL")
	h.expectRows("SELECT DISTINCT g FROM it_dist ORDER BY g LIMIT 1 OFFSET 1", "b")
	h.expectRows("SELECT COUNT(*) FROM (SELECT DISTINCT g, v FROM it_dist) d", "4")
	h.expectRows("SELECT ALL g FROM it_dist WHERE g = 'b'", "b", "b")

	// Over indexed columns DISTINCT is computed in the index (GROUPBY);
	// NOINDEX columns, stars and expressions are de-duplicated by the driver.
	if _, _, indexAgg := h.planOf("SELECT DISTINCT g, v FROM it_dist"); !indexAgg {
		t.Error("SELECT DISTINCT g, v should run as an index GROUPBY")
	}
	if _, _, indexAgg := h.planOf("SELECT DISTINCT g, s FROM it_dist"); indexAgg {
		t.Error("SELECT DISTINCT over a NOINDEX column can't run in the index")
	}

	// After GROUP BY and window functions, and with a star.
	h.expectRows("SELECT DISTINCT COUNT(*) FROM it_dist GROUP BY g ORDER BY 1", "1", "2", "3")
	h.expectRows("SELECT DISTINCT g, COUNT(*) OVER (PARTITION BY g) AS n FROM it_dist ORDER BY g", "a|3", "b|2", "NULL|1")
	h.expectRows("SELECT DISTINCT * FROM (SELECT g, v FROM it_dist) d ORDER BY g, v", "a|1", "a|2", "b|NULL", "NULL|3")

	// In views (never expanded in place), subqueries, CTAS and set operations.
	h.exec("CREATE VIEW it_dist_v AS SELECT DISTINCT g FROM it_dist")
	h.expectRows("SELECT g FROM it_dist_v WHERE g IS NOT NULL ORDER BY g", "a", "b")
	h.expectRows("SELECT COUNT(*) FROM it_dist_v", "3")
	h.expectRows("SELECT id FROM it_dist WHERE v IN (SELECT DISTINCT v FROM it_dist WHERE g = 'a') ORDER BY id", "1", "2", "3")
	h.exec("CREATE TABLE it_dist_out AS SELECT DISTINCT g, s FROM it_dist")
	h.expectRows("SELECT COUNT(*) FROM it_dist_out", "4")
	h.expectRows("SELECT DISTINCT g FROM it_dist WHERE g = 'a' UNION ALL SELECT DISTINCT g FROM it_dist WHERE g = 'a'", "a", "a")

	// ORDER BY must use the select list (Postgres's rule).
	h.expectError("SELECT DISTINCT g FROM it_dist ORDER BY id", "ORDER BY expressions must appear in select list")
	h.expectError("SELECT DISTINCT g, s FROM it_dist ORDER BY v", "ORDER BY expressions must appear in select list")

	// DISTINCT ON keeps the first row of each key in ORDER BY order.
	h.expectRows("SELECT DISTINCT ON (g) g, id, v FROM it_dist ORDER BY g, id DESC", "a|3|2", "b|5|NULL", "NULL|6|3")
	h.expectRows("SELECT DISTINCT ON (1) g AS grp, id FROM it_dist ORDER BY 1, id", "a|1", "b|4", "NULL|6")
	h.expectRows("SELECT DISTINCT ON (grp) g AS grp, id FROM it_dist ORDER BY grp, id", "a|1", "b|4", "NULL|6")
	h.expectRows("SELECT DISTINCT ON (g) id FROM it_dist ORDER BY g, v DESC NULLS LAST, id", "3", "4", "6")
	h.expectRows("SELECT DISTINCT ON (g, v) g, v, id FROM it_dist ORDER BY v, g, id", "a|1|1", "a|2|3", "NULL|3|6", "b|NULL|4")
	h.expectRows("SELECT DISTINCT ON (g) g, id FROM it_dist ORDER BY g, id LIMIT 2", "a|1", "b|4")
	h.expectRows("SELECT COUNT(*) FROM (SELECT DISTINCT ON (g) g, id FROM it_dist) x", "3")
	h.expectError("SELECT DISTINCT ON (g) g, id FROM it_dist ORDER BY id", "must match initial ORDER BY expressions")
	h.expectError("SELECT DISTINCT ON (3) g, id FROM it_dist", "DISTINCT ON position 3 is out of range")
	h.expectError("SELECT DISTINCT ON (ROW_NUMBER() OVER ()) g FROM it_dist", "window functions are not allowed in DISTINCT ON")
}

func TestSQLQualifiedStar(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_qs_v")
		h.exec("DROP TABLE IF EXISTS it_qs_a")
		h.exec("DROP TABLE IF EXISTS it_qs_b")
	}
	drop()
	t.Cleanup(drop)
	h.exec("CREATE TABLE it_qs_a (id INTEGER, s VARCHAR)")
	h.exec("INSERT INTO it_qs_a VALUES (1, 'x'), (2, 'y')")
	h.exec("CREATE TABLE it_qs_b (id INTEGER, n INTEGER)")
	h.exec("INSERT INTO it_qs_b VALUES (1, 10), (3, 30)")

	// One FROM item: by alias, by table name, schema- and catalog-qualified.
	schema := h.expectRows("SELECT a.* FROM it_qs_a a WHERE a.id = 2", "2|y")
	if got := schema.String(); !strings.Contains(got, "id: type=int32") || !strings.Contains(got, "s: type=utf8") {
		t.Errorf("a.* schema = %s", got)
	}
	h.expectRows("SELECT it_qs_a.* FROM it_qs_a ORDER BY id", "1|x", "2|y")
	h.expectRows("SELECT public.it_qs_a.* FROM it_qs_a ORDER BY id", "1|x", "2|y")
	h.expectRows("SELECT redis.public.it_qs_a.* FROM public.it_qs_a ORDER BY id", "1|x", "2|y")
	h.expectRows(`SELECT "a".* FROM it_qs_a "a" ORDER BY id`, "1|x", "2|y")

	// Joins: each star picks its own item's columns, under their own names.
	h.expectRows("SELECT a.*, b.n FROM it_qs_a a JOIN it_qs_b b ON a.id = b.id", "1|x|10")
	h.expectRows("SELECT b.*, a.s FROM it_qs_a a LEFT JOIN it_qs_b b ON a.id = b.id ORDER BY a.id", "1|10|x", "NULL|NULL|y")
	schema = h.expectRows("SELECT a.*, b.* FROM it_qs_a a JOIN it_qs_b b ON a.id = b.id", "1|x|1|10")
	if got := schema.String(); strings.Count(got, "id: type=int32") != 2 || !strings.Contains(got, "n: type=int32") {
		t.Errorf("a.*, b.* schema = %s", got)
	}
	h.expectRows("SELECT DISTINCT a.* FROM it_qs_a a JOIN (SELECT 1 AS id UNION ALL SELECT 1) d ON d.id = a.id", "1|x")

	// Derived tables and CTEs; dbt's snapshot staging selects source_data.*.
	h.expectRows("SELECT d.* FROM (SELECT id * 2 AS dbl FROM it_qs_a) d ORDER BY dbl", "2", "4")
	h.expectRows("WITH c AS (SELECT id FROM it_qs_a) SELECT c.* FROM c ORDER BY id", "1", "2")
	h.expectRows("SELECT source_data.*, 'k' AS dbt_scd_id FROM (SELECT * FROM it_qs_a) source_data ORDER BY id", "1|x|k", "2|y|k")
	h.exec("CREATE VIEW it_qs_v AS SELECT a.*, b.n FROM it_qs_a a JOIN it_qs_b b ON a.id = b.id")
	h.expectRows("SELECT * FROM it_qs_v", "1|x|10")

	h.expectError("SELECT x.* FROM it_qs_a a", `missing FROM-clause entry for table "x"`)
	h.expectError("SELECT public.a.* FROM it_qs_a a", `missing FROM-clause entry for table "public.a"`)
	h.expectError("SELECT other.it_qs_a.* FROM it_qs_a", `missing FROM-clause entry for table "other.it_qs_a"`)
	h.expectError("SELECT a.*", "requires a FROM clause")
}

// A correlated subquery with DISTINCT ON keeps one row per key after the
// correlation filter, so it must not run as a semi-join (which drops the
// subquery's DISTINCT ON and would test every row).
func TestSQLDistinctOnInCorrelatedSubquery(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP TABLE IF EXISTS it_dson_o")
		h.exec("DROP TABLE IF EXISTS it_dson_i")
	}
	drop()
	t.Cleanup(drop)
	h.exec("CREATE TABLE it_dson_i (k INTEGER, g VARCHAR, ts INTEGER, v INTEGER)")
	h.exec("INSERT INTO it_dson_i VALUES (1, 'a', 1, 10), (1, 'a', 2, 20), (1, 'b', 1, 30), (2, 'a', 1, 20)")
	h.exec("CREATE TABLE it_dson_o (id INTEGER, k INTEGER, x INTEGER)")
	h.exec("INSERT INTO it_dson_o VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 2, 20), (5, 3, 10)")

	// For k = 1 the first row per g (by ts) has v 10 (a) and 30 (b), not 20.
	h.expectRows(`SELECT o.id FROM it_dson_o o
		WHERE o.x IN (SELECT DISTINCT ON (g) v FROM it_dson_i i WHERE i.k = o.k ORDER BY g, ts) ORDER BY o.id`, "1", "3", "4")
	h.expectRows(`SELECT o.id FROM it_dson_o o
		WHERE o.x NOT IN (SELECT DISTINCT ON (g) v FROM it_dson_i i WHERE i.k = o.k ORDER BY g, ts) ORDER BY o.id`, "2", "5")
	h.expectRows(`SELECT o.id FROM it_dson_o o
		WHERE EXISTS (SELECT DISTINCT ON (g) v FROM it_dson_i i WHERE i.k = o.k ORDER BY g, ts) ORDER BY o.id`, "1", "2", "3", "4")
	// Plain DISTINCT doesn't change which values exist.
	h.expectRows(`SELECT o.id FROM it_dson_o o
		WHERE o.x IN (SELECT DISTINCT v FROM it_dson_i i WHERE i.k = o.k) ORDER BY o.id`, "1", "2", "3", "4")
}

// Comparisons with constants that don't fit the column type (1.5 for an
// integer, 1.249 for NUMERIC(6,2), a timestamp with a time of day for a
// DATE) must give the same answer whether the column is indexed (pushed
// down) or NOINDEX (evaluated by the driver).
func TestSQLInexactConstantPushdown(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() { h.exec("DROP TABLE IF EXISTS it_inexact") }
	drop()
	t.Cleanup(drop)
	types := []struct{ col, typ string }{
		{"i", "INTEGER"}, {"b", "BIGINT"}, {"sm", "SMALLINT"}, {"f", "DOUBLE PRECISION"},
		{"n", "NUMERIC(6,2)"}, {"d", "DATE"}, {"ts", "TIMESTAMP"}, {"s", "VARCHAR"},
	}
	var defs []string
	for _, c := range types {
		defs = append(defs, c.col+" "+c.typ, c.col+"_x "+c.typ+" NOINDEX")
	}
	h.exec("CREATE TABLE it_inexact (" + strings.Join(defs, ", ") + ")")
	for _, r := range [][]string{
		{"1", "1", "1", "1.0", "1.00", "DATE '2024-01-01'", "TIMESTAMP '2024-01-01 00:00:00'", "'1'"},
		{"2", "2", "2", "1.5", "1.25", "DATE '2024-01-02'", "TIMESTAMP '2024-01-01 12:00:00'", "'01'"},
		{"3", "3", "3", "2.0", "2.50", "DATE '2024-01-03'", "TIMESTAMP '2024-01-02 00:00:00'", "'a'"},
		{"NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"},
	} {
		var vals []string
		for _, v := range r {
			vals = append(vals, v, v)
		}
		h.exec("INSERT INTO it_inexact VALUES (" + strings.Join(vals, ", ") + ")")
	}
	consts := map[string][]string{
		"i":  {"1.5", "1.4", "2.5", "2", "2.0", "-0.5", "CAST(1.5 AS REAL)"},
		"b":  {"1.5", "2.5", "2.0", "CAST(1.5 AS DOUBLE PRECISION)"},
		"sm": {"2.5", "1.5", "3"},
		"f":  {"1.5", "1", "2", "1.25"},
		"n":  {"1.25", "1.249", "1.251", "1.255", "2.5", "1", "CAST(1.25 AS DOUBLE PRECISION)"},
		"d":  {"DATE '2024-01-02'", "TIMESTAMP '2024-01-02 00:00:00'", "TIMESTAMP '2024-01-02 12:00:00'", "TIMESTAMP '2024-01-01 23:59:59'"},
		"ts": {"TIMESTAMP '2024-01-01 12:00:00'", "DATE '2024-01-02'", "TIMESTAMP '2024-01-01 12:00:00.000001'"},
		"s":  {"'1'", "'01'", "'a'", "1"},
	}
	count := func(sql string) string {
		st, err := h.conn.NewStatement(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close(h.ctx)
		if err := st.SetSqlQuery(h.ctx, sql); err != nil {
			t.Fatal(err)
		}
		rdr, _, err := st.ExecuteQuery(h.ctx)
		if err != nil {
			return "error"
		}
		defer rdr.Release()
		rdr.Next()
		return rdr.RecordBatch().Column(0).ValueStr(0)
	}
	for _, c := range types {
		for _, k := range consts[c.col] {
			for _, op := range []string{"=", "<>", "<", "<=", ">", ">="} {
				pushed := count(fmt.Sprintf("SELECT COUNT(*) FROM it_inexact WHERE %s %s %s", c.col, op, k))
				exact := count(fmt.Sprintf("SELECT COUNT(*) FROM it_inexact WHERE %s_x %s %s", c.col, op, k))
				if pushed != exact {
					t.Errorf("%s %s %s: indexed column gives %s, NOINDEX column gives %s", c.col, op, k, pushed, exact)
				}
			}
		}
	}

	// Exact constants are still answered by the index alone.
	for _, q := range []string{
		"SELECT i FROM it_inexact WHERE i = 2", "SELECT i FROM it_inexact WHERE i > 2.0",
		"SELECT n FROM it_inexact WHERE n = 1.25", "SELECT s FROM it_inexact WHERE s = 'a'",
		"SELECT d FROM it_inexact WHERE d >= TIMESTAMP '2024-01-02 00:00:00'",
	} {
		if query, residual, _ := h.planOf(q); query == "" || residual {
			t.Errorf("%s: query %q, residual %v; want an index query only", q, query, residual)
		}
	}
	// Inexact ones use the index too, with the predicate re-checked.
	for _, q := range []string{"SELECT i FROM it_inexact WHERE i > 1.5", "SELECT n FROM it_inexact WHERE n = 1.249"} {
		if query, residual, _ := h.planOf(q); query == "" || !residual {
			t.Errorf("%s: query %q, residual %v; want an index query plus a residual", q, query, residual)
		}
	}
}

// Literal IN lists longer than maxUnionTerms are evaluated by the driver;
// they now probe a hash set (built once per statement) instead of
// comparing every value for every row.
func TestSQLLongInList(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() { h.exec("DROP TABLE IF EXISTS it_inlist") }
	drop()
	t.Cleanup(drop)
	h.exec("CREATE TABLE it_inlist (id BIGINT, u BIGINT NOINDEX, s VARCHAR, f DOUBLE PRECISION)")
	var rows []string
	for i := 1; i <= 3000; i++ {
		if i%100 == 0 {
			rows = append(rows, fmt.Sprintf("(%d, NULL, NULL, NULL)", i))
			continue
		}
		rows = append(rows, fmt.Sprintf("(%d, %d, 's%d', %d.5)", i, i, i, i))
	}
	for i := 0; i < len(rows); i += 500 {
		h.exec("INSERT INTO it_inlist VALUES " + strings.Join(rows[i:i+500], ", "))
	}
	// Every third id from 3 to 4500: 1,000 of them exist (3..3000), and
	// the ones that are multiples of 100 have NULL u, s and f.
	var ids, strs, floats []string
	for i := 3; i <= 4500; i += 3 {
		ids = append(ids, fmt.Sprint(i))
		strs = append(strs, fmt.Sprintf("'s%d'", i))
		floats = append(floats, fmt.Sprintf("%d.5", i))
	}
	list := strings.Join(ids, ", ")
	before := subqueryStats.inLists.Load()
	h.expectRows("SELECT COUNT(*) FROM it_inlist WHERE id IN ("+list+")", "1000")
	h.expectRows("SELECT COUNT(*) FROM it_inlist WHERE u IN ("+list+")", "990")
	h.expectRows("SELECT COUNT(*) FROM it_inlist WHERE s IN ("+strings.Join(strs, ", ")+")", "990")
	h.expectRows("SELECT COUNT(*) FROM it_inlist WHERE f IN ("+strings.Join(floats, ", ")+")", "990")
	h.expectRows("SELECT COUNT(*) FROM it_inlist WHERE u NOT IN ("+list+")", "1980")
	// A NULL in the list makes NOT IN never true.
	h.expectRows("SELECT COUNT(*) FROM it_inlist WHERE u NOT IN ("+list+", NULL)", "0")
	h.expectRows("SELECT COUNT(*) FROM it_inlist WHERE u IN ("+list+", NULL)", "990")
	// Integers compare with the decimals and doubles of the list exactly.
	h.expectRows("SELECT COUNT(*) FROM it_inlist WHERE u IN ("+list+", 1.0, CAST(2 AS DOUBLE PRECISION))", "992")
	if subqueryStats.inLists.Load() == before {
		t.Error("no IN list used its hash set")
	}
	// A string column against numbers can fail to compare: it is still
	// evaluated value by value, with the same error as before.
	h.expectError("SELECT COUNT(*) FROM it_inlist WHERE s IN ("+list+")", "cannot compare")
}
