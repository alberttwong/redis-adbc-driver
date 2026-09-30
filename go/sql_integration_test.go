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
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

