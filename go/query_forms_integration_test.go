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

// Integration tests for WITH RECURSIVE, LATERAL, ANY / ALL comparisons,
// NATURAL JOIN and GENERATE_SERIES. They use the harness of
// sql_integration_test.go and are skipped when REDIS_URI is unset.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// setupQueryForms creates the tables of these tests:
//
//	it_qf_emp    an org chart: two trees, 1 (Ann) and 7 (Gus)
//	it_qf_edges  a graph with the cycle 1 → 2 → 3 → 1, and 3 → 4, 5 → 6
//	it_qf_g      (id, k, v) with a NULL k and a NULL v
//	it_qf_g2     (k, id, w), sharing k and id with it_qf_g
func (h *sqlHarness) setupQueryForms() {
	h.t.Helper()
	tables := []string{"it_qf_emp", "it_qf_edges", "it_qf_g", "it_qf_g2"}
	drop := func() {
		for _, t := range tables {
			h.exec("DROP TABLE IF EXISTS " + t)
		}
	}
	drop()
	h.t.Cleanup(drop)
	h.exec("CREATE TABLE it_qf_emp (id INTEGER NOT NULL, name VARCHAR, manager_id INTEGER)")
	h.exec(`INSERT INTO it_qf_emp VALUES (1, 'Ann', NULL), (2, 'Bob', 1), (3, 'Cid', 1), (4, 'Dee', 2),
		(5, 'Eve', 4), (6, 'Fay', 3), (7, 'Gus', NULL), (8, 'Hal', 7)`)
	h.exec("CREATE TABLE it_qf_edges (src INTEGER, dst INTEGER)")
	h.exec("INSERT INTO it_qf_edges VALUES (1, 2), (2, 3), (3, 1), (3, 4), (5, 6)")
	h.exec("CREATE TABLE it_qf_g (id INTEGER NOT NULL, k VARCHAR, v INTEGER)")
	h.exec("INSERT INTO it_qf_g VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', NULL), (5, NULL, 50)")
	h.exec("CREATE TABLE it_qf_g2 (k VARCHAR, id INTEGER, w INTEGER)")
	h.exec("INSERT INTO it_qf_g2 VALUES ('a', 1, 100), ('b', 3, 300), ('c', 9, 900)")
}

// columnNames returns the field names of a result schema, comma-separated.
func columnNames(s *arrow.Schema) string {
	var names []string
	for _, f := range s.Fields() {
		names = append(names, f.Name)
	}
	return strings.Join(names, ",")
}

// queryInts runs a query with one row of BIGINT parameters and renders its
// rows like query.
func (h *sqlHarness) queryInts(sql string, params ...int64) []string {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	fields := make([]arrow.Field, len(params))
	cols := make([]arrow.Array, len(params))
	for i, p := range params {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.Append(p)
		cols[i] = b.NewArray()
		fields[i] = arrow.Field{Name: fmt.Sprintf("p%d", i+1), Type: arrow.PrimitiveTypes.Int64}
	}
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 1)
	defer rec.Release()
	if err := st.Bind(h.ctx, rec); err != nil {
		h.t.Fatal(err)
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	defer rdr.Release()
	var rows []string
	for rdr.Next() {
		r := rdr.RecordBatch()
		for i := 0; i < int(r.NumRows()); i++ {
			cells := make([]string, r.NumCols())
			for c := range cells {
				if r.Column(c).IsNull(i) {
					cells[c] = "NULL"
				} else {
					cells[c] = r.Column(c).ValueStr(i)
				}
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
	}
	return rows
}

// In a correlated subquery, a condition on outer columns only is a constant,
// not an index filter on the inner table's column of the same name.
func TestSQLSubqueryOuterOnlyCondition(t *testing.T) {
	h := newSQLHarness(t)
	h.setupQueryForms()

	h.expectRows(`SELECT id, (SELECT COUNT(*) FROM it_qf_g2 x WHERE g.id = 3), (SELECT COUNT(*) FROM it_qf_g2 x WHERE g.id > 2)
		FROM it_qf_g g ORDER BY id`,
		"1|0|0", "2|0|0", "3|3|3", "4|0|3", "5|0|3")
	h.expectRows(`SELECT id FROM it_qf_g g WHERE NOT EXISTS (SELECT 1 FROM it_qf_g2 x WHERE g.id < 3 AND x.w > 100) ORDER BY id`,
		"3", "4", "5")
}

func TestSQLRecursiveCTE(t *testing.T) {
	h := newSQLHarness(t)
	h.setupQueryForms()

	h.expectRows(`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r WHERE x < 3) SELECT * FROM r`,
		"1", "2", "3")

	// A hierarchy: Ann's subtree, with each row's depth and path. The
	// recursive term joins the table with the working table.
	schema := h.expectRows(`WITH RECURSIVE sub AS (
			SELECT id, name, 0 AS depth, name AS path FROM it_qf_emp WHERE id = 1
			UNION ALL
			SELECT e.id, e.name, s.depth + 1, s.path || '/' || e.name
			FROM it_qf_emp e JOIN sub s ON e.manager_id = s.id)
		SELECT id, depth, path FROM sub ORDER BY path`,
		"1|0|Ann", "2|1|Ann/Bob", "4|2|Ann/Bob/Dee", "5|3|Ann/Bob/Dee/Eve", "3|1|Ann/Cid", "6|2|Ann/Cid/Fay")
	if got := columnNames(schema); got != "id,depth,path" {
		t.Errorf("column names = %s", got)
	}
	// Walking up, with a column list and the working table on the left.
	h.expectRows(`WITH RECURSIVE up(emp, boss) AS (
			SELECT id, manager_id FROM it_qf_emp WHERE id = 5
			UNION ALL
			SELECT e.id, e.manager_id FROM up JOIN it_qf_emp e ON e.id = up.boss)
		SELECT emp, boss FROM up ORDER BY emp DESC`,
		"5|4", "4|2", "2|1", "1|NULL")
	// The recursive CTE in a join, in a subquery and as UPDATE's filter.
	h.expectRows(`WITH RECURSIVE sub(id) AS (
			SELECT 2 UNION ALL SELECT e.id FROM it_qf_emp e JOIN sub ON e.manager_id = sub.id)
		SELECT e.name, (SELECT COUNT(*) FROM sub) FROM it_qf_emp e JOIN sub ON sub.id = e.id ORDER BY e.id`,
		"Bob|3", "Dee|3", "Eve|3")
	h.expectAffected(`WITH RECURSIVE sub(id) AS (
			SELECT 7 UNION ALL SELECT e.id FROM it_qf_emp e JOIN sub ON e.manager_id = sub.id)
		UPDATE it_qf_emp SET name = name || '*' WHERE id IN (SELECT id FROM sub)`, 2)
	h.expectRows(`SELECT name FROM it_qf_emp WHERE id >= 6 ORDER BY id`, "Fay", "Gus*", "Hal*")

	// UNION drops rows produced before (NULLs equal), so a cycle ends;
	// UNION ALL keeps them, so the walk needs its own stop condition.
	h.expectRows(`WITH RECURSIVE reach(n) AS (
			SELECT 1 UNION SELECT e.dst FROM it_qf_edges e JOIN reach r ON e.src = r.n)
		SELECT n FROM reach ORDER BY n`,
		"1", "2", "3", "4")
	h.expectRows(`WITH RECURSIVE walk(n, d) AS (
			SELECT 1, 0 UNION ALL SELECT e.dst, w.d + 1 FROM it_qf_edges e JOIN walk w ON e.src = w.n WHERE w.d < 4)
		SELECT n, COUNT(*) FROM walk GROUP BY n ORDER BY n`,
		"1|2", "2|2", "3|1", "4|1")
	h.expectRows(`WITH RECURSIVE r(x) AS (SELECT 1 UNION SELECT 1 UNION SELECT x FROM r) SELECT * FROM r`, "1")
	h.expectRows(`WITH RECURSIVE r(x, y) AS (SELECT 1, CAST(NULL AS INTEGER) UNION SELECT x, y FROM r) SELECT * FROM r`,
		"1|NULL")
	h.expectRows(`WITH RECURSIVE r(x) AS (SELECT 1 UNION SELECT x % 3 + 1 FROM r) SELECT x FROM r ORDER BY x`,
		"1", "2", "3")

	// Column names and types come from the non-recursive term, widened to
	// hold the recursive term's values within one category.
	schema = h.expectRows(`WITH RECURSIVE r(a, b) AS (SELECT 1, 'x' UNION ALL SELECT a + 1, b || 'y' FROM r WHERE a < 3)
		SELECT * FROM r`,
		"1|x", "2|xy", "3|xyy")
	if got := columnNames(schema); got != "a,b" {
		t.Errorf("column names = %s", got)
	}
	if schema.Field(0).Type.ID() != arrow.INT64 || schema.Field(1).Type.ID() != arrow.STRING {
		t.Errorf("column types = %s", schema)
	}
	h.expectRows(`WITH RECURSIVE r(x) AS (SELECT 1.5 UNION ALL SELECT x + 1 FROM r WHERE x < 3) SELECT * FROM r`,
		"1.5", "2.5", "3.5")
	h.expectRows(`WITH RECURSIVE r(d) AS (SELECT DATE '2024-02-27' UNION ALL SELECT d + 1 FROM r WHERE d < DATE '2024-03-01')
		SELECT * FROM r`,
		"2024-02-27", "2024-02-28", "2024-02-29", "2024-03-01")
	h.expectError(`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x * 1.5 FROM r WHERE x < 3) SELECT * FROM r`,
		`recursive query "r" column 1 has type BIGINT in non-recursive term but type NUMERIC(38,1) overall`)
	h.expectError(`WITH RECURSIVE r(d) AS (SELECT DATE '2024-02-27' UNION ALL SELECT d + INTERVAL '1 day' FROM r WHERE d < DATE '2024-03-01')
		SELECT * FROM r`,
		`recursive query "r" column 1 has type DATE in non-recursive term but type TIMESTAMP(6) overall`)
	h.expectError(`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT 'a' FROM r) SELECT * FROM r`,
		`has type BIGINT in non-recursive term but type VARCHAR overall`)
	h.expectError(`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x, x FROM r) SELECT * FROM r`,
		"each UNION query must have the same number of columns")
	h.expectError(`WITH RECURSIVE r(a, b) AS (SELECT 1 UNION ALL SELECT a + 1 FROM r WHERE a < 3) SELECT * FROM r`,
		`"r" has 1 columns but 2 column names were given`)

	// The working table may be read through a derived table, a LATERAL
	// subquery, a WITH query and a LEFT JOIN's preserved side.
	h.expectRows(`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT s.y FROM (SELECT x + 1 AS y FROM r) s WHERE s.y <= 3)
		SELECT * FROM r`,
		"1", "2", "3")
	h.expectRows(`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT l.y FROM r, LATERAL (SELECT r.x + 1 AS y) l WHERE l.y <= 3)
		SELECT * FROM r`,
		"1", "2", "3")
	h.expectRows(`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL
			SELECT n.x + 1 FROM (WITH w AS (SELECT x FROM r) SELECT x FROM w) n WHERE n.x < 3)
		SELECT * FROM r`,
		"1", "2", "3")
	h.expectRows(`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL
			SELECT r.x + 1 FROM r LEFT JOIN it_qf_emp e ON e.id = r.x WHERE r.x < 3)
		SELECT * FROM r`,
		"1", "2", "3")
	// Other CTEs of the list, the body's own WITH, and a recursive CTE
	// inside a correlated subquery.
	h.expectRows(`WITH RECURSIVE t(n) AS (SELECT 2), r(x) AS (SELECT n FROM t UNION ALL SELECT x + 1 FROM r WHERE x < 3)
		SELECT * FROM r`,
		"2", "3")
	h.expectRows(`WITH RECURSIVE r(x) AS (WITH w AS (SELECT 1 AS one) SELECT one FROM w UNION ALL SELECT x + 1 FROM r WHERE x < 3)
		SELECT * FROM r`,
		"1", "2", "3")
	h.expectRows(`SELECT id, (WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r WHERE x < 4)
			SELECT COUNT(*) FROM r WHERE x <= g.id)
		FROM it_qf_g g ORDER BY id`,
		"1|1", "2|2", "3|3", "4|4", "5|4")
	// Bound parameters.
	if got, want := strings.Join(h.queryInts(`WITH RECURSIVE r(x) AS (SELECT ? UNION ALL SELECT x + ? FROM r WHERE x < ?)
		SELECT * FROM r`, 1, 10, 25), ","), "1,11,21,31"; got != want {
		t.Errorf("bound parameters: got %s, want %s", got, want)
	}

	// Postgres's rules for the recursive reference.
	for _, c := range []struct{ sql, err string }{
		{`WITH RECURSIVE r AS (SELECT * FROM r) SELECT * FROM r`,
			`recursive query "r" does not have the form non-recursive-term UNION [ALL] recursive-term`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 INTERSECT SELECT x FROM r) SELECT * FROM r`,
			`does not have the form non-recursive-term UNION [ALL] recursive-term`},
		{`WITH RECURSIVE r(x) AS (SELECT x FROM r UNION ALL SELECT 1) SELECT * FROM r`,
			`recursive reference to query "r" must not appear within its non-recursive term`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT a.x + 1 FROM r a JOIN r b ON a.x = b.x WHERE a.x < 3) SELECT * FROM r`,
			`recursive reference to query "r" must not appear more than once`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT 2 WHERE EXISTS (SELECT 1 FROM r)) SELECT * FROM r`,
			`recursive reference to query "r" must not appear within a subquery`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT id FROM it_qf_emp WHERE id IN (SELECT x + 1 FROM r)) SELECT * FROM r`,
			`recursive reference to query "r" must not appear within a subquery`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT e.id FROM it_qf_emp e LEFT JOIN r ON e.manager_id = r.x) SELECT * FROM r`,
			`recursive reference to query "r" must not appear within an outer join`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT e.id FROM r FULL JOIN it_qf_emp e ON e.manager_id = r.x) SELECT * FROM r`,
			`recursive reference to query "r" must not appear within an outer join`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL (SELECT v FROM it_qf_g EXCEPT SELECT x + 1 FROM r)) SELECT * FROM r`,
			`recursive reference to query "r" must not appear within EXCEPT`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL (SELECT x + 1 FROM r INTERSECT ALL SELECT v FROM it_qf_g)) SELECT * FROM r`,
			`recursive reference to query "r" must not appear within INTERSECT`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT MAX(x) + 1 FROM r) SELECT * FROM r`,
			`aggregate functions are not allowed in a recursive query's recursive term`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r WHERE x < 3 ORDER BY 1) SELECT * FROM r`,
			`ORDER BY in a recursive query is not implemented`},
		{`WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r WHERE x < 3 LIMIT 2) SELECT * FROM r`,
			`LIMIT in a recursive query is not implemented`},
		{`WITH RECURSIVE r(x) AS (WITH w AS (SELECT x FROM r) SELECT 1 UNION ALL SELECT x + 1 FROM r WHERE x < 3) SELECT * FROM r`,
			`recursive reference to query "r" must not appear within a subquery`},
		{`WITH RECURSIVE a(x) AS (SELECT 1 UNION ALL SELECT x FROM b), b(x) AS (SELECT x FROM a) SELECT * FROM a`,
			`mutual recursion between WITH items is not implemented`},
		{`WITH RECURSIVE a(x) AS (SELECT x FROM b), b(x) AS (SELECT x FROM a) SELECT * FROM a`,
			`mutual recursion between WITH items is not implemented`},
		// Without RECURSIVE, a CTE can't refer to itself.
		{`WITH r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r WHERE x < 3) SELECT * FROM r`,
			`"r" refers to itself; recursive references need WITH RECURSIVE`},
	} {
		h.expectError(c.sql, c.err)
	}
}

func TestSQLRecursiveSearchCycle(t *testing.T) {
	h := newSQLHarness(t)
	h.setupQueryForms()

	// Ann's subtree, depth first and breadth first.
	const sub = `WITH RECURSIVE t(id, name) AS (
			SELECT id, name FROM it_qf_emp WHERE id = 1
			UNION ALL
			SELECT e.id, e.name FROM it_qf_emp e JOIN t ON e.manager_id = t.id)`
	schema := h.expectRows(sub+` SEARCH DEPTH FIRST BY id SET ord SELECT * FROM t ORDER BY ord`,
		"1|Ann|1", "2|Bob|2", "4|Dee|3", "5|Eve|4", "3|Cid|5", "6|Fay|6")
	if got := columnNames(schema); got != "id,name,ord" || schema.Field(2).Type.ID() != arrow.INT64 {
		t.Errorf("SEARCH columns = %s", schema)
	}
	h.expectRows(sub+` SEARCH BREADTH FIRST BY id SET ord SELECT name FROM t ORDER BY ord`,
		"Ann", "Bob", "Cid", "Dee", "Fay", "Eve")

	// CYCLE marks the row that closes a cycle and doesn't recurse into it.
	schema = h.expectRows(`WITH RECURSIVE w(n, d) AS (
			SELECT 1, 0 UNION ALL SELECT e.dst, w.d + 1 FROM it_qf_edges e JOIN w ON e.src = w.n)
		CYCLE n SET is_cycle USING path
		SELECT * FROM w ORDER BY d, n`,
		"1|0|false|{(1)}", "2|1|false|{(1),(2)}", "3|2|false|{(1),(2),(3)}",
		"1|3|true|{(1),(2),(3),(1)}", "4|3|false|{(1),(2),(3),(4)}")
	if got := columnNames(schema); got != "n,d,is_cycle,path" {
		t.Errorf("CYCLE columns = %s", got)
	}
	h.expectRows(`WITH RECURSIVE w(s, t) AS (
			SELECT src, dst FROM it_qf_edges WHERE src = 1
			UNION ALL SELECT e.src, e.dst FROM it_qf_edges e JOIN w ON e.src = w.t)
		CYCLE s, t SET c TO 'Y' DEFAULT 'N' USING p
		SELECT s, t, c, p FROM w WHERE c = 'Y'`,
		`1|2|Y|{"(1,2)","(2,3)","(3,1)","(1,2)"}`)
	// Both, and with UNION (the added columns are part of the rows).
	h.expectRows(`WITH RECURSIVE w(n) AS (
			SELECT 1 UNION SELECT e.dst FROM it_qf_edges e JOIN w ON e.src = w.n)
		SEARCH DEPTH FIRST BY n SET ord CYCLE n SET c USING p
		SELECT n, ord, c FROM w ORDER BY ord`,
		"1|1|false", "2|2|false", "3|3|false", "1|4|true", "4|5|false")
	h.expectRows(`WITH RECURSIVE w(n, s) AS (SELECT 1, 'a b' UNION ALL SELECT n + 1, s || '"' FROM w WHERE n < 2)
		CYCLE n, s SET c USING p SELECT p FROM w ORDER BY n`,
		`{"(1,\"a b\")"}`, `{"(1,\"a b\")","(2,\"a b\"\"\")"}`)

	const w = `WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM w WHERE n < 3)`
	for _, c := range []struct{ sql, err string }{
		{`WITH w(n) AS (SELECT 1) CYCLE n SET c USING p SELECT * FROM w`, "WITH query is not recursive"},
		{w + ` SEARCH DEPTH FIRST BY m SET o SELECT * FROM w`, `search column "m" not in WITH query column list`},
		{w + ` SEARCH BREADTH FIRST BY n, n SET o SELECT * FROM w`, `search column "n" specified more than once`},
		{w + ` SEARCH DEPTH FIRST BY n SET n SELECT * FROM w`, `search sequence column name "n" already used in WITH query column list`},
		{w + ` CYCLE m SET c USING p SELECT * FROM w`, `cycle column "m" not in WITH query column list`},
		{w + ` CYCLE n SET n USING p SELECT * FROM w`, `cycle mark column name "n" already used in WITH query column list`},
		{w + ` CYCLE n SET c USING n SELECT * FROM w`, `cycle path column name "n" already used in WITH query column list`},
		{w + ` CYCLE n SET c USING c SELECT * FROM w`, "cycle mark column name and cycle path column name are the same"},
		{w + ` SEARCH DEPTH FIRST BY n SET c CYCLE n SET c USING p SELECT * FROM w`,
			"search sequence column name and cycle mark column name are the same"},
		{w + ` CYCLE n SET c TO 1 DEFAULT 'x' USING p SELECT * FROM w`, "CYCLE types BIGINT and VARCHAR cannot be matched"},
		{`WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT s.m FROM (SELECT n + 1 AS m FROM w) s WHERE s.m < 3)
			CYCLE n SET c USING p SELECT * FROM w`,
			`with a SEARCH or CYCLE clause, the recursive reference to WITH query "w" must be at the top level of its right-hand SELECT`},
		{`WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT n + 1 FROM w WHERE n < 3)
			CYCLE n SET c USING p SELECT * FROM w`,
			"with a SEARCH or CYCLE clause, the left side of the UNION must be a SELECT"},
		{`WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL (SELECT n + 1 FROM w WHERE n < 3 LIMIT 5))
			SEARCH DEPTH FIRST BY n SET o SELECT * FROM w`,
			"SEARCH and CYCLE are not supported with GROUP BY, HAVING, window functions, LIMIT or OFFSET in the recursive term"},
	} {
		h.expectError(c.sql, c.err)
	}
}

// A recursive CTE without a working stop condition fails once it has run
// maxRecursion iterations or produced maxRecursiveRows rows.
func TestSQLRecursiveCTELimits(t *testing.T) {
	h := newSQLHarness(t)
	h.setupQueryForms()

	h.expectError(`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT COUNT(*) FROM r`,
		`recursive query "r" did not finish after 10000 iterations; check the stop condition of its recursive term`)
	h.expectRows(`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10001) SELECT COUNT(*), MAX(n) FROM r`,
		"10001|10001")
	h.expectError(`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10002) SELECT COUNT(*) FROM r`,
		"did not finish after 10000 iterations")

	defer func(iterations, rows int) { maxRecursion, maxRecursiveRows = iterations, rows }(maxRecursion, maxRecursiveRows)
	maxRecursion, maxRecursiveRows = 50, 1000
	// The cycle 1 → 2 → 3 → 1 never ends with UNION ALL.
	h.expectError(`WITH RECURSIVE walk(n) AS (
			SELECT 1 UNION ALL SELECT e.dst FROM it_qf_edges e JOIN walk w ON e.src = w.n)
		SELECT COUNT(*) FROM walk`,
		`recursive query "walk" did not finish after 50 iterations`)
	// Rows double with each iteration.
	h.expectError(`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r, generate_series(1, 2) WHERE n < 40)
		SELECT COUNT(*) FROM r`,
		`recursive query "r" returned more than 1000 rows; check the stop condition of its recursive term`)
	h.expectRows(`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r, generate_series(1, 2) WHERE n < 9)
		SELECT COUNT(*) FROM r`,
		"511")
}

func TestSQLLateral(t *testing.T) {
	h := newSQLHarness(t)
	h.setupQueryForms()

	h.expectRows(`SELECT g.id, l.x FROM it_qf_g g, LATERAL (SELECT g.id * 2 AS x) l ORDER BY g.id`,
		"1|2", "2|4", "3|6", "4|8", "5|10")
	// Top-N per group.
	h.expectRows(`SELECT k.k, t.id, t.v FROM (SELECT DISTINCT k FROM it_qf_g WHERE k IS NOT NULL) k
		CROSS JOIN LATERAL (SELECT id, v FROM it_qf_g g WHERE g.k = k.k ORDER BY v DESC NULLS LAST LIMIT 1) t
		ORDER BY k.k`,
		"a|2|20", "b|3|30")
	// Run once per distinct value of the outer columns it reads.
	w := h.expectWork(`SELECT g.id, t.n FROM it_qf_g g
		CROSS JOIN LATERAL (SELECT COUNT(*) AS n FROM it_qf_g x WHERE x.k = g.k) t ORDER BY g.id`,
		"1|2", "2|2", "3|2", "4|2", "5|0")
	if w.runs != 3 {
		t.Errorf("LATERAL subquery ran %d times, want 3 (once per distinct k)", w.runs)
	}
	// LEFT JOIN LATERAL keeps the outer rows; ON filters the LATERAL rows.
	h.expectRows(`SELECT g.id, t.id FROM it_qf_g g
		LEFT JOIN LATERAL (SELECT x.id FROM it_qf_g x WHERE x.k = g.k AND x.id > g.id) t ON TRUE ORDER BY g.id`,
		"1|2", "2|NULL", "3|4", "4|NULL", "5|NULL")
	h.expectRows(`SELECT g.id, t.v FROM it_qf_g g JOIN LATERAL (SELECT v FROM it_qf_g x WHERE x.k = g.k) t ON t.v > g.v
		ORDER BY g.id`,
		"1|20")
	h.expectRows(`SELECT g.id, t.v FROM it_qf_g g LEFT JOIN LATERAL (SELECT v FROM it_qf_g x WHERE x.k = g.k) t ON t.v > g.v
		ORDER BY g.id`,
		"1|20", "2|NULL", "3|NULL", "4|NULL", "5|NULL")
	// Column aliases, and a WHERE condition on the LATERAL item.
	h.expectRows(`SELECT g.id, l.a, l.b FROM it_qf_g g, LATERAL (SELECT g.v, g.v * 2) AS l(a, b) WHERE l.b > 50 ORDER BY g.id`,
		"3|30|60", "5|50|100")
	// A LATERAL item that also reads the enclosing query.
	h.expectRows(`SELECT id FROM it_qf_g g
		WHERE EXISTS (SELECT 1 FROM it_qf_g2 x, LATERAL (SELECT x.w + g.id AS s) l WHERE l.s = 103)`,
		"3")
	// One that reads no earlier item is joined like a derived table, with
	// any join kind.
	h.expectRows(`SELECT g.id, t.w FROM it_qf_g g RIGHT JOIN LATERAL (SELECT id, w FROM it_qf_g2) t ON t.id = g.id ORDER BY t.w`,
		"1|100", "3|300", "NULL|900")
	h.expectRows(`SELECT * FROM LATERAL (SELECT 1 AS a) x`, "1")

	// Table functions read earlier items with or without LATERAL.
	h.expectRows(`SELECT g.id, s.n FROM it_qf_g g, generate_series(1, g.id) AS s(n) WHERE g.id <= 3 ORDER BY 1, 2`,
		"1|1", "2|1", "2|2", "3|1", "3|2", "3|3")
	h.expectRows(`SELECT g.id, s.n FROM it_qf_g g CROSS JOIN LATERAL generate_series(g.id, 3) s(n) ORDER BY 1, 2`,
		"1|1", "1|2", "1|3", "2|2", "2|3", "3|3")
	h.expectRows(`SELECT g.id, s.n FROM it_qf_g g LEFT JOIN LATERAL generate_series(g.id, 2) s(n) ON TRUE ORDER BY 1, 2`,
		"1|1", "1|2", "2|2", "3|NULL", "4|NULL", "5|NULL")
	h.expectRows(`SELECT id, (SELECT SUM(n) FROM generate_series(1, g.id) s(n)) FROM it_qf_g g ORDER BY id`,
		"1|1", "2|3", "3|6", "4|10", "5|15")

	h.expectError(`SELECT * FROM it_qf_g g RIGHT JOIN LATERAL (SELECT g.id) t ON TRUE`,
		`invalid reference to FROM-clause entry for table "g": the combining JOIN type must be INNER or LEFT for a LATERAL reference`)
	h.expectError(`SELECT * FROM it_qf_g g FULL JOIN generate_series(1, g.id) s ON TRUE`,
		"the combining JOIN type must be INNER or LEFT for a LATERAL reference")
	h.expectError(`SELECT * FROM LATERAL (SELECT y.id) t, it_qf_g y`, `column "y.id" does not exist`)
	h.expectError(`SELECT * FROM it_qf_g g, (SELECT g.id) t`, `column "g.id" does not exist`)
	h.expectError(`SELECT * FROM it_qf_g g, LATERAL (SELECT 1)`, "must have an alias")
	h.expectError(`SELECT * FROM it_qf_g, LATERAL it_qf_g2`, "LATERAL must be followed by a subquery or a function call")
}

func TestSQLQuantifiedComparisons(t *testing.T) {
	h := newSQLHarness(t)
	h.setupQueryForms()

	// v is 10, 20, 30, NULL, 50. s2 is {20, 30, NULL}, s1 {20, 30}, s0 empty.
	const s2 = "(SELECT v FROM it_qf_g WHERE id BETWEEN 2 AND 4)"
	const s1 = "(SELECT v FROM it_qf_g WHERE id IN (2, 3))"
	const s0 = "(SELECT v FROM it_qf_g WHERE id > 100)"
	want := []string{
		"1|NULL|NULL|false|true|false|true",
		"2|NULL|false|false|true|false|true",
		"3|true|false|true|false|false|true",
		"4|NULL|NULL|NULL|NULL|false|true",
		"5|true|false|true|false|false|true",
	}
	h.expectRows(`SELECT id, v > ANY `+s2+`, v < ALL `+s2+`, v >= ALL `+s1+`, v < SOME `+s1+`,
			v > ANY `+s0+`, v > ALL `+s0+`
		FROM it_qf_g ORDER BY id`, want...)
	// The same subqueries correlated (always the same rows): compared value
	// by value instead of with the smallest / largest value. (The outer
	// g.id = g.id must not be taken for a filter on the inner id column.)
	corr := func(s string) string { return strings.Replace(s, "WHERE", "WHERE g.id = g.id AND", 1) }
	h.expectRows(`SELECT id, v > ANY `+corr(s2)+`, v < ALL `+corr(s2)+`, v >= ALL `+corr(s1)+`, v < SOME `+corr(s1)+`,
			v > ANY `+corr(s0)+`, v > ALL `+corr(s0)+`
		FROM it_qf_g g ORDER BY id`, want...)
	// = ANY is IN, <> ALL is NOT IN; = ALL and <> ANY compare value by value.
	h.expectRows(`SELECT id, v = ANY `+s2+`, v IN `+s2+`, v <> ALL `+s2+`, v NOT IN `+s2+`, v = SOME `+s1+`,
			v = ANY `+s0+`, v <> ALL `+s0+`
		FROM it_qf_g ORDER BY id`,
		"1|NULL|NULL|NULL|NULL|false|false|true",
		"2|true|true|false|false|true|false|true",
		"3|true|true|false|false|true|false|true",
		"4|NULL|NULL|NULL|NULL|NULL|false|true",
		"5|NULL|NULL|NULL|NULL|false|false|true")
	h.expectRows(`SELECT id, v = ALL (SELECT v FROM it_qf_g WHERE id = 2), v <> ANY `+s1+`, v <> ANY `+s2+`
		FROM it_qf_g ORDER BY id`,
		"1|false|true|true", "2|true|true|true", "3|false|true|true", "4|NULL|NULL|NULL", "5|false|true|true")

	// In WHERE, correlated, and against more values than are compared one by
	// one.
	h.expectRows(`SELECT id FROM it_qf_g WHERE v > ALL (SELECT v FROM it_qf_g WHERE id < 3) ORDER BY id`, "3", "5")
	h.expectRows(`SELECT id FROM it_qf_g a WHERE v >= ALL (SELECT v FROM it_qf_g b WHERE b.id <= a.id AND b.v IS NOT NULL)
		ORDER BY id`,
		"1", "2", "3", "5")
	h.expectRows(`SELECT id FROM it_qf_g WHERE v > ALL (SELECT n FROM generate_series(1, 25) n) ORDER BY id`, "3", "5")
	h.expectRows(`SELECT id FROM it_qf_g WHERE v < ANY (SELECT n FROM generate_series(1, 25) n) ORDER BY id`, "1", "2")
	h.expectRows(`SELECT id FROM it_qf_g WHERE NOT (v <= ALL (SELECT n * 10 FROM generate_series(1, 4) n)) ORDER BY id`,
		"2", "3", "5")

	// = ANY and <> ALL take IN's fast paths: an index union, a hash set, a
	// semi-join.
	w := h.expectWork(`SELECT id FROM it_qf_g WHERE id = ANY (SELECT id FROM it_qf_g2) ORDER BY id`, "1", "3")
	if w.runs != 1 {
		t.Errorf("= ANY: %+v, want the subquery run once", w)
	}
	w = h.expectWork(`SELECT COUNT(*) FROM it_qf_g WHERE v <> ALL (SELECT n FROM generate_series(1, 40) n)`, "1")
	if w.runs != 1 || w.inSets != 1 {
		t.Errorf("<> ALL: %+v, want 1 run and 1 hash set", w)
	}
	w = h.expectWork(`SELECT id FROM it_qf_g g WHERE g.id = ANY (SELECT x.id FROM it_qf_g2 x WHERE x.k = g.k) ORDER BY id`,
		"1", "3")
	if w.semiJoins != 1 || w.runs != 0 {
		t.Errorf("correlated = ANY: %+v, want a semi-join", w)
	}
	w = h.expectWork(`SELECT id FROM it_qf_g g WHERE g.id <> ALL (SELECT x.id FROM it_qf_g2 x WHERE x.k = g.k) ORDER BY id`,
		"2", "4", "5")
	if w.semiJoins != 1 || w.runs != 0 {
		t.Errorf("correlated <> ALL: %+v, want an anti-join", w)
	}

	h.expectError(`SELECT id FROM it_qf_g WHERE id = ANY (1, 2)`, "ANY (…) needs a subquery")
	h.expectError(`SELECT id FROM it_qf_g WHERE v > ALL (SELECT id, v FROM it_qf_g)`, "subquery must return exactly one column")
	h.expectError(`SELECT id FROM it_qf_g WHERE v > ANY (SELECT k FROM it_qf_g WHERE k IS NOT NULL)`, "cannot compare")
}

func TestSQLNaturalJoin(t *testing.T) {
	h := newSQLHarness(t)
	h.setupQueryForms()

	// it_qf_g (id, k, v) and it_qf_g2 (k, id, w) have id and k in common:
	// they come first, in the left table's order, once.
	schema := h.expectRows(`SELECT * FROM it_qf_g NATURAL JOIN it_qf_g2 ORDER BY id`,
		"1|a|10|100", "3|b|30|300")
	if got := columnNames(schema); got != "id,k,v,w" {
		t.Errorf("NATURAL JOIN columns = %s, want id,k,v,w", got)
	}
	h.expectRows(`SELECT * FROM it_qf_g NATURAL INNER JOIN it_qf_g2 ORDER BY id`, "1|a|10|100", "3|b|30|300")
	h.expectRows(`SELECT * FROM it_qf_g NATURAL LEFT JOIN it_qf_g2 ORDER BY id`,
		"1|a|10|100", "2|a|20|NULL", "3|b|30|300", "4|b|NULL|NULL", "5|NULL|50|NULL")
	h.expectRows(`SELECT * FROM it_qf_g NATURAL RIGHT OUTER JOIN it_qf_g2 ORDER BY id`,
		"1|a|10|100", "3|b|30|300", "9|c|NULL|900")
	schema = h.expectRows(`SELECT * FROM it_qf_g NATURAL FULL JOIN it_qf_g2 ORDER BY id`,
		"1|a|10|100", "2|a|20|NULL", "3|b|30|300", "4|b|NULL|NULL", "5|NULL|50|NULL", "9|c|NULL|900")
	if got := columnNames(schema); got != "id,k,v,w" {
		t.Errorf("NATURAL FULL JOIN columns = %s, want id,k,v,w", got)
	}
	// Unqualified, the merged column (COALESCE of both for FULL); qualified,
	// each side's own.
	h.expectRows(`SELECT id, it_qf_g.id, it_qf_g2.id, k FROM it_qf_g NATURAL FULL JOIN it_qf_g2 ORDER BY id`,
		"1|1|1|a", "2|2|NULL|a", "3|3|3|b", "4|4|NULL|b", "5|5|NULL|NULL", "9|NULL|9|c")
	h.expectRows(`SELECT id, w FROM it_qf_g NATURAL FULL JOIN it_qf_g2 WHERE id > 3 ORDER BY id`,
		"4|NULL", "5|NULL", "9|900")
	h.expectRows(`SELECT k, COUNT(*) FROM it_qf_g NATURAL FULL JOIN it_qf_g2 GROUP BY k ORDER BY k`,
		"a|2", "b|2", "c|1", "NULL|1")
	h.expectRows(`SELECT a.v, b.w FROM it_qf_g a NATURAL JOIN it_qf_g2 b ORDER BY id`, "10|100", "30|300")
	// A chain: the left side is everything joined so far, with its merged
	// columns.
	schema = h.expectRows(`SELECT * FROM it_qf_g NATURAL JOIN it_qf_g2 NATURAL JOIN (SELECT 3 AS id, 'x' AS note) t`,
		"3|b|30|300|x")
	if got := columnNames(schema); got != "id,k,v,w,note" {
		t.Errorf("chained NATURAL JOIN columns = %s", got)
	}
	schema = h.expectRows(`SELECT * FROM (SELECT 1 AS id, 'p' AS a) x NATURAL FULL JOIN (SELECT 2 AS id, 'q' AS b) y
		NATURAL FULL JOIN (SELECT 2 AS id, 'r' AS c) z ORDER BY id`,
		"1|p|NULL|NULL", "2|NULL|q|r")
	if got := columnNames(schema); got != "id,a,b,c" {
		t.Errorf("chained NATURAL FULL JOIN columns = %s", got)
	}
	// A later ON condition on a FULL join's merged column.
	h.expectRows(`SELECT x.id, y.id, z.c FROM (SELECT 1 AS id) x NATURAL FULL JOIN (SELECT 2 AS id) y
		JOIN (SELECT 2 AS zid, 'r' AS c) z ON z.zid = id`,
		"NULL|2|r")
	// No column in common: a cross join.
	h.expectRows(`SELECT COUNT(*) FROM it_qf_g NATURAL JOIN (SELECT 1 AS zz UNION ALL SELECT 2) z`, "10")
	h.expectRows(`SELECT COUNT(*) FROM it_qf_g NATURAL LEFT JOIN (SELECT 1 AS zz WHERE FALSE) z`, "5")

	h.expectError(`SELECT * FROM it_qf_g a JOIN it_qf_g b ON a.id = b.id NATURAL JOIN it_qf_g2`,
		`common column name "id" appears more than once in left table`)
	h.expectError(`SELECT * FROM it_qf_g NATURAL JOIN (SELECT CAST(id AS VARCHAR) AS id FROM it_qf_g2) z`,
		"JOIN/USING types INTEGER and VARCHAR cannot be matched")
	h.expectError(`SELECT * FROM it_qf_g NATURAL JOIN it_qf_g2 ON TRUE`, "NATURAL JOIN cannot have an ON or USING clause")
	h.expectError(`SELECT * FROM it_qf_g NATURAL CROSS JOIN it_qf_g2`, "expected [INNER | LEFT | RIGHT | FULL] JOIN after NATURAL")
}

func TestSQLGenerateSeries(t *testing.T) {
	h := newSQLHarness(t)
	h.setupQueryForms()
	h.exec("DROP TABLE IF EXISTS it_qf_ev")
	h.exec("CREATE TABLE it_qf_ev (id INTEGER, day DATE, n INTEGER)")
	h.exec("INSERT INTO it_qf_ev VALUES (1, '2024-01-01', 1), (2, '2024-01-01', 2), (3, '2024-01-03', 5)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_qf_ev") })

	schema := h.expectRows(`SELECT * FROM generate_series(1, 3)`, "1", "2", "3")
	if got := columnNames(schema); got != "generate_series" || schema.Field(0).Type.ID() != arrow.INT64 {
		t.Errorf("generate_series column = %s", schema)
	}
	h.expectRows(`SELECT * FROM generate_series(10, 1, -3)`, "10", "7", "4", "1")
	h.expectRows(`SELECT * FROM generate_series(1, 3, -1)`)
	h.expectRows(`SELECT * FROM generate_series(1, NULL)`)
	h.expectRows(`SELECT * FROM generate_series(9223372036854775806, 9223372036854775807)`,
		"9223372036854775806", "9223372036854775807")
	h.expectError(`SELECT * FROM generate_series(1, 10, 0)`, "step size cannot equal zero")
	// The column is named by a column alias, else by the table alias.
	if got := columnNames(h.expectRows(`SELECT * FROM generate_series(1, 2) g`, "1", "2")); got != "g" {
		t.Errorf("column of generate_series(…) g = %s, want g", got)
	}
	if got := columnNames(h.expectRows(`SELECT g.n FROM generate_series(1, 2) AS g(n)`, "1", "2")); got != "n" {
		t.Errorf("column of generate_series(…) g(n) = %s, want n", got)
	}
	h.expectRows(`SELECT SUM(n), COUNT(*) FROM generate_series(1, 100) n`, "5050|100")
	h.expectError(`SELECT * FROM generate_series(1, 3) g(a, b)`, `table "g" has 1 columns available but 2 columns specified`)

	// Numerics, and doubles (which Postgres has no overload for).
	schema = h.expectRows(`SELECT * FROM generate_series(0.5, 2, 0.5)`, "0.5", "1.0", "1.5", "2.0")
	if dt, ok := schema.Field(0).Type.(*arrow.Decimal128Type); !ok || dt.Scale != 1 {
		t.Errorf("numeric series type = %s", schema.Field(0).Type)
	}
	h.expectRows(`SELECT * FROM generate_series(1, 2, 0.25e0)`, "1", "1.25", "1.5", "1.75", "2")
	h.expectError(`SELECT * FROM generate_series(1, 2, 0.0)`, "step size cannot equal zero")

	// Timestamps with an interval step; dates (and untyped strings) give
	// timestamps with time zone, as in Postgres. The step is added to the
	// previous value, so months clamp.
	schema = h.expectRows(`SELECT * FROM generate_series(DATE '2024-02-27', DATE '2024-03-01', INTERVAL '1 day')`,
		"2024-02-27T00:00:00Z", "2024-02-28T00:00:00Z", "2024-02-29T00:00:00Z", "2024-03-01T00:00:00Z")
	if ts, ok := schema.Field(0).Type.(*arrow.TimestampType); !ok || ts.TimeZone != "UTC" {
		t.Errorf("date series type = %s, want a timestamp with time zone", schema.Field(0).Type)
	}
	h.expectRows(`SELECT * FROM generate_series(TIMESTAMP '2024-01-31', TIMESTAMP '2024-05-01', INTERVAL '1 month')`,
		"2024-01-31T00:00:00", "2024-02-29T00:00:00", "2024-03-29T00:00:00", "2024-04-29T00:00:00")
	h.expectRows(`SELECT * FROM generate_series(TIMESTAMP '2024-01-01 12:00', TIMESTAMP '2024-01-01 10:30', INTERVAL '-45 minutes')`,
		"2024-01-01T12:00:00", "2024-01-01T11:15:00", "2024-01-01T10:30:00")
	h.expectRows(`SELECT CAST(d AS DATE) FROM generate_series('2024-01-01', '2024-01-02', INTERVAL '12 hours') d`,
		"2024-01-01", "2024-01-01", "2024-01-02")
	h.expectError(`SELECT * FROM generate_series(TIMESTAMP '2024-01-01', TIMESTAMP '2024-01-02', INTERVAL '0 days')`,
		"step size cannot equal zero")

	// A date spine joined with a table, and a series joined by an index
	// lookup, with bound parameters.
	h.expectRows(`SELECT CAST(d AS DATE) AS day, COUNT(e.n), COALESCE(SUM(e.n), 0)
		FROM generate_series(DATE '2024-01-01', DATE '2024-01-04', INTERVAL '1 day') d
		LEFT JOIN it_qf_ev e ON e.day = CAST(d AS DATE)
		GROUP BY 1 ORDER BY 1`,
		"2024-01-01|2|3", "2024-01-02|0|0", "2024-01-03|1|5", "2024-01-04|0|0")
	if got, want := strings.Join(h.queryInts(`SELECT s.n, g.v FROM generate_series(?, ?) s(n) JOIN it_qf_g g ON g.id = s.n ORDER BY s.n`, 2, 4), ","),
		"2|20,3|30,4|NULL"; got != want {
		t.Errorf("bound parameters: got %s, want %s", got, want)
	}
	h.expectRows(`SELECT id FROM it_qf_g WHERE id IN (SELECT * FROM generate_series(2, 3)) ORDER BY id`, "2", "3")
	h.expectAffected(`DELETE FROM it_qf_ev USING generate_series(1, 2) s(n) WHERE it_qf_ev.id = s.n`, 2)
	h.expectRows(`SELECT id FROM it_qf_ev`, "3")

	// The result schema without bound parameters.
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, `SELECT * FROM generate_series(?, ?) AS s(n)`); err != nil {
		t.Fatal(err)
	}
	ss, ok := st.(adbc.StatementExecuteSchemaWithContext)
	if !ok {
		t.Fatal("the statement doesn't support ExecuteSchema")
	}
	if schema, err := ss.ExecuteSchema(h.ctx); err != nil || columnNames(schema) != "n" || schema.Field(0).Type.ID() != arrow.INT64 {
		t.Errorf("ExecuteSchema = %v, %v", schema, err)
	}

	// At most maxSeriesRows rows, checked before any is made.
	h.expectError(`SELECT * FROM generate_series(1, 1e12)`, "generate_series would return more than 1000000 rows")
	h.expectError(`SELECT COUNT(*) FROM generate_series(-9223372036854775807, 9223372036854775807)`,
		"generate_series would return more than 1000000 rows")
	defer func(n int) { maxSeriesRows = n }(maxSeriesRows)
	maxSeriesRows = 10
	h.expectRows(`SELECT COUNT(*) FROM generate_series(1, 10)`, "10")
	h.expectError(`SELECT COUNT(*) FROM generate_series(1, 11)`, "generate_series would return more than 10 rows")
	h.expectError(`SELECT COUNT(*) FROM generate_series(0.1, 1.1, 0.1)`, "more than 10 rows")
	h.expectError(`SELECT COUNT(*) FROM generate_series(TIMESTAMP '2024-01-01', TIMESTAMP '2024-01-02', INTERVAL '1 hour')`,
		"more than 10 rows")
	maxSeriesRows = 1_000_000

	h.expectError(`SELECT generate_series(1, 3)`, "generate_series is only supported in FROM")
	h.expectError(`SELECT * FROM unnest(1)`, "table function unnest is not supported")
	h.expectError(`SELECT * FROM generate_series(1)`, "function generate_series(BIGINT) does not exist")
	h.expectError(`SELECT * FROM generate_series(1, 10, INTERVAL '1 day')`,
		"function generate_series(BIGINT, BIGINT, INTERVAL) does not exist")
	h.expectError(`SELECT * FROM generate_series(DATE '2024-01-01', DATE '2024-01-02')`,
		"function generate_series(DATE, DATE) does not exist")
	h.expectError(`SELECT * FROM generate_series(1, COUNT(*))`, "aggregate functions are not allowed in functions in FROM")
}
