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
	h.expectError(`SELECT id FROM it_orders WHERE id IN (SELECT id, qty FROM it_orders)`, "exactly one column")

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
	h.expectError(`WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r`, "not supported")
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

// rawClient connects to REDIS_URI directly (cluster-aware) so tests can
// inspect row HASHes.
func (h *sqlHarness) rawClient() goredis.UniversalClient {
	h.t.Helper()
	opts, err := goredis.ParseURL(os.Getenv("REDIS_URI"))
	if err != nil {
		h.t.Fatal(err)
	}
	opts.Protocol = 2 // the driver's reply parsing expects RESP2
	var c goredis.UniversalClient = goredis.NewClient(opts)
	if info, err := c.Info(h.ctx, "cluster").Result(); err == nil && strings.Contains(info, "cluster_enabled:1") {
		_ = c.Close()
		c = goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{opts.Addr}, Password: opts.Password,
			Username: opts.Username, TLSConfig: opts.TLSConfig, Protocol: 2})
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
	h.expectError(`ALTER TABLE it_alter ADD COLUMN d INTEGER DEFAULT 5`, "defaults")

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
