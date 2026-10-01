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

// Integration tests for RETURNING and for UPDATE … SET (a, b) = (…). They
// run against the Redis server at REDIS_URI, like sql_integration_test.go.

import (
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// setupReturning creates the target table it_ret_t (rows 1 to 3, with row
// ids 1 to 3) and the source table it_ret_s.
func (h *sqlHarness) setupReturning() {
	h.t.Helper()
	drop := func() {
		h.exec("DROP TABLE IF EXISTS it_ret_t")
		h.exec("DROP TABLE IF EXISTS it_ret_s")
	}
	drop()
	h.t.Cleanup(drop)
	h.exec("CREATE TABLE it_ret_t (id INTEGER NOT NULL, k VARCHAR, v INTEGER, amount NUMERIC(6,2))")
	h.exec("INSERT INTO it_ret_t VALUES (1, 'a', 10, 1.50), (2, 'b', 20, NULL), (3, 'c', NULL, 3.00)")
	h.exec("CREATE TABLE it_ret_s (id INTEGER, k VARCHAR, op VARCHAR)")
	h.exec("INSERT INTO it_ret_s VALUES (1, 'A', 'upd'), (3, 'C', 'del'), (4, 'D', 'ins'), (5, 'E', 'skip')")
}

const allRet = "SELECT id, k, v, amount FROM it_ret_t ORDER BY id"

var originalRet = []string{"1|a|10|1.50", "2|b|20|NULL", "3|c|NULL|3.00"}

// fieldNames renders a schema's field names as "a|b|…".
func fieldNames(schema *arrow.Schema) string {
	names := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		names[i] = f.Name
	}
	return strings.Join(names, "|")
}

// expectReturning runs a statement with ExecuteQuery and checks the names of
// its result columns (cols, as "a|b|…") and its rows, in any order: as in
// Postgres, RETURNING returns the changed rows in no particular order.
func (h *sqlHarness) expectReturning(sql, cols string, want ...string) {
	h.t.Helper()
	got, schema := h.query(sql)
	if names := fieldNames(schema); names != cols {
		h.t.Errorf("%s\n columns: %q, want %q", sql, names, cols)
	}
	slices.Sort(got)
	want = slices.Clone(want)
	slices.Sort(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		h.t.Errorf("%s\n got: %q\nwant: %q", sql, got, want)
	}
}

func TestSQLReturningInsert(t *testing.T) {
	h := newSQLHarness(t)
	h.setupReturning()

	// The new rows, in VALUES order, with the row ids they were given; *
	// lists the columns, not __rowid.
	schema := h.expectRows(`INSERT INTO it_ret_t (id, k) VALUES (4, 'd'), (5, 'e') RETURNING __rowid, *`,
		"4|4|d|NULL|NULL", "5|5|e|NULL|NULL")
	var types []string
	for _, f := range schema.Fields() {
		types = append(types, f.Name+" "+f.Type.String())
	}
	if got, want := strings.Join(types, ", "), "__rowid int64, id int32, k utf8, v int32, amount decimal(6, 2)"; got != want {
		t.Errorf("INSERT … RETURNING schema: %s, want %s", got, want)
	}
	h.expectRows(`SELECT __rowid, id FROM it_ret_t WHERE id >= 4 ORDER BY id`, "4|4", "5|5")

	// The values as stored (cast to the column types), expressions, aliases
	// with and without AS, and the table name as a qualifier.
	schema = h.expectRows(`INSERT INTO it_ret_t (id, amount, k) VALUES ('6', 2.005, 6)
		RETURNING id, amount, k, id * 2 AS double, UPPER(k || 'x') up, it_ret_t.v, k || '!'`,
		"6|2.01|6|12|6X|NULL|6!")
	if got, want := fieldNames(schema), "id|amount|k|double|up|v|k || '!'"; got != want {
		t.Errorf("RETURNING column names: %s, want %s", got, want)
	}

	// INSERT … SELECT returns the rows in the query's order; also
	// parenthesized, and with a qualified star.
	h.expectRows(`INSERT INTO it_ret_t (id, k) SELECT id + 10, LOWER(k) FROM it_ret_s WHERE op <> 'skip' ORDER BY id DESC
		RETURNING it_ret_t.id, it_ret_t.*`,
		"14|14|d|NULL|NULL", "13|13|c|NULL|NULL", "11|11|a|NULL|NULL")
	h.expectRows(`INSERT INTO it_ret_t (SELECT 20, 'x', NULL, NULL) RETURNING id, k`, "20|x")

	// No rows: the result still has the RETURNING columns.
	schema = h.expectRows(`INSERT INTO it_ret_t SELECT * FROM it_ret_t WHERE id < 0 RETURNING id, k AS key`)
	if got := fieldNames(schema); got != "id|key" {
		t.Errorf("empty INSERT … RETURNING columns: %s, want id|key", got)
	}

	// Subqueries see the table as it was before the statement (there are 10
	// rows now), and may be correlated with the new row.
	h.expectRows(`INSERT INTO it_ret_t (id, k) VALUES (30, 'D'), (31, 'Z')
		RETURNING id, (SELECT COUNT(*) FROM it_ret_t) AS before, (SELECT op FROM it_ret_s s WHERE s.k = it_ret_t.k) AS op`,
		"30|10|ins", "31|10|NULL")

	// ExecuteUpdate runs the statement and returns the row count.
	h.expectAffected(`INSERT INTO it_ret_t (id) VALUES (40), (41) RETURNING id`, 2)
	h.expectRows(`SELECT id FROM it_ret_t WHERE id >= 40 ORDER BY id`, "40", "41")

	// RETURNING is evaluated before anything is written: an error in it, as
	// in any other check, leaves the table untouched.
	h.expectError(`INSERT INTO it_ret_t (id) VALUES (50), (0) RETURNING 100 / id`, "division by zero")
	h.expectError(`INSERT INTO it_ret_t (id, k) SELECT 51, 'x' UNION ALL SELECT 0, 'y' RETURNING 100 / id`, "division by zero")
	h.expectError(`INSERT INTO it_ret_t (k) VALUES ('x') RETURNING k`, "not-null constraint")
	h.expectRows(`SELECT COUNT(*) FROM it_ret_t WHERE id >= 50 OR id = 0 OR k = 'x' OR k = 'y'`, "1") // (20, 'x')

	// The row ids returned are those the rows are stored under, also after
	// ids were used up by the failed statements.
	got, _ := h.query(`INSERT INTO it_ret_t (id) VALUES (60) RETURNING __rowid`)
	h.expectRows(`SELECT __rowid FROM it_ret_t WHERE id = 60`, got...)

	// Errors.
	h.expectError(`INSERT INTO it_ret_t (id) VALUES (70) RETURNING COUNT(*)`, "aggregate functions are not allowed in RETURNING")
	h.expectError(`INSERT INTO it_ret_t (id) VALUES (70) RETURNING ROW_NUMBER() OVER ()`, "window functions are not allowed in RETURNING")
	h.expectError(`INSERT INTO it_ret_t (id) VALUES (70) RETURNING nope`, `column "nope" does not exist`)
	h.expectError(`INSERT INTO it_ret_t (id) VALUES (70) RETURNING s.id`, `column "s.id" does not exist`)
	h.expectError(`INSERT INTO it_ret_t (id) VALUES (70) RETURNING s.*`, `missing FROM-clause entry for table "s"`)
	h.expectError(`INSERT INTO it_ret_t (id) VALUES (70) RETURNING merge_action()`,
		"MERGE_ACTION() can only be used in the RETURNING list of a MERGE command")
	h.expectError(`INSERT INTO it_ret_t (id) VALUES (70) RETURNING`, "syntax error")
	h.expectError(`SELECT 1 RETURNING 1`, `unexpected "RETURNING"`)
	h.expectError(`CREATE TABLE it_ret_x (id INTEGER) RETURNING id`, `unexpected "RETURNING"`)
	h.expectRows(`SELECT COUNT(*) FROM it_ret_t WHERE id = 70`, "0")
}

func TestSQLReturningUpdate(t *testing.T) {
	h := newSQLHarness(t)
	h.setupReturning()

	// The rows after the change.
	h.expectReturning(`UPDATE it_ret_t SET v = COALESCE(v, 0) + 1, k = UPPER(k) WHERE id <= 2 RETURNING *`,
		"id|k|v|amount", "1|A|11|1.50", "2|B|21|NULL")
	// An alias, __rowid, expressions over the new values, and a star.
	h.expectReturning(`UPDATE it_ret_t AS t SET amount = t.amount * 2 WHERE t.id = 1
		RETURNING t.id, __rowid, t.amount AS doubled, t.*`,
		"id|__rowid|doubled|id|k|v|amount", "1|1|3.00|1|A|11|3.00")
	h.expectReturning(`UPDATE it_ret_t SET k = k || '!' WHERE __rowid = 2 RETURNING __rowid, k`, "__rowid|k", "2|B!")
	// Every matched row is returned, also when its values stay the same.
	h.expectReturning(`UPDATE it_ret_t SET k = k WHERE id = 1 RETURNING k`, "k", "A")

	// Subqueries see the rows before the change; a correlated one sees the
	// new row.
	h.expectReturning(`UPDATE it_ret_t SET v = 100 WHERE id = 2 RETURNING v, (SELECT v FROM it_ret_t WHERE id = 2) AS old`,
		"v|old", "100|21")
	h.expectReturning(`UPDATE it_ret_t t SET id = 4 WHERE id = 3 RETURNING id, (SELECT s.op FROM it_ret_s s WHERE s.id = t.id) AS op`,
		"id|op", "4|ins")
	h.exec(`UPDATE it_ret_t SET id = 3 WHERE id = 4`)

	// No rows.
	h.expectReturning(`UPDATE it_ret_t SET v = 0 WHERE id < 0 RETURNING id, v AS new_v`, "id|new_v")

	// An error in RETURNING leaves the table untouched.
	const now = "1|A|11|3.00"
	h.expectError(`UPDATE it_ret_t SET v = 0 RETURNING 1 / v`, "division by zero")
	h.expectRows(allRet, now, "2|B!|100|NULL", "3|c|NULL|3.00")

	// UPDATE … FROM: * lists the target's columns, then the FROM items'.
	h.expectReturning(`UPDATE it_ret_t t SET k = s.k || '!' FROM it_ret_s s WHERE s.id = t.id RETURNING *`,
		"id|k|v|amount|id|k|op", "1|A!|11|3.00|1|A|upd", "3|C!|NULL|3.00|3|C|del")
	h.expectReturning(`UPDATE it_ret_t t SET v = 0 FROM it_ret_s s WHERE s.id = t.id AND s.op = 'upd' RETURNING s.*, t.id, v`,
		"id|k|op|id|v", "1|A|upd|1|0")
	// A target row matching several FROM rows is updated, and returned, once.
	h.expectReturning(`UPDATE it_ret_t t SET v = 5 FROM (SELECT 1 AS id UNION ALL SELECT 1) d WHERE d.id = t.id RETURNING t.id, d.id, v`,
		"id|id|v", "1|1|5")
	// A CTE, read by a subquery in RETURNING too.
	h.expectReturning(`WITH c AS (SELECT 3 AS id, 'cte' AS tag)
		UPDATE it_ret_t t SET k = c.tag FROM c WHERE c.id = t.id RETURNING t.id, k, (SELECT COUNT(*) FROM c) AS n`,
		"id|k|n", "3|cte|1")
	h.expectError(`UPDATE it_ret_t t SET v = 7 FROM it_ret_s s WHERE s.id = t.id RETURNING t.id / (s.id - 1)`, "division by zero")
	h.expectError(`UPDATE it_ret_t t SET v = 1 FROM it_ret_s s WHERE s.id = t.id RETURNING k`, "ambiguous")
	h.expectError(`UPDATE it_ret_t t SET v = 1 FROM it_ret_s s WHERE s.id = t.id RETURNING __rowid`, `column "__rowid" does not exist`)
	h.expectError(`UPDATE it_ret_t AS t SET v = 1 RETURNING it_ret_s.*`, "missing FROM-clause entry")
	h.expectError(`UPDATE it_ret_t SET v = 1 RETURNING SUM(v)`, "aggregate functions are not allowed in RETURNING")
	h.expectRows(allRet, "1|A!|5|3.00", "2|B!|100|NULL", "3|cte|NULL|3.00")
}

func TestSQLReturningDelete(t *testing.T) {
	h := newSQLHarness(t)
	h.setupReturning()

	// The deleted rows' values.
	h.expectReturning(`DELETE FROM it_ret_t WHERE id = 2 RETURNING *`, "id|k|v|amount", "2|b|20|NULL")
	h.expectReturning(`DELETE FROM it_ret_t WHERE id < 0 RETURNING id`, "id")
	// Subqueries see the rows before the delete.
	h.expectReturning(`DELETE FROM it_ret_t AS x WHERE x.id = 3 RETURNING __rowid, x.k, (SELECT COUNT(*) FROM it_ret_t) AS before`,
		"__rowid|k|before", "3|c|2")
	h.expectRows(allRet, "1|a|10|1.50")

	// An error in RETURNING deletes nothing.
	h.setupReturning()
	h.expectError(`DELETE FROM it_ret_t RETURNING 1 / (v - 10)`, "division by zero")
	h.expectRows(allRet, originalRet...)
	h.expectReturning(`DELETE FROM it_ret_t WHERE id >= 2 RETURNING id, amount`, "id|amount", "2|NULL", "3|3.00")

	// DELETE … USING: * lists the target's columns, then the USING items';
	// a target row matching several USING rows is deleted, and returned,
	// once.
	h.setupReturning()
	h.expectReturning(`DELETE FROM it_ret_t t USING it_ret_s s WHERE s.id = t.id AND s.op = 'del' RETURNING s.op, t.*`,
		"op|id|k|v|amount", "del|3|c|NULL|3.00")
	h.expectReturning(`DELETE FROM it_ret_t t USING (SELECT 2 AS id UNION ALL SELECT 2) d WHERE d.id = t.id RETURNING t.id, d.id`,
		"id|id", "2|2")
	h.expectReturning(`WITH g AS (SELECT 1 AS id) DELETE FROM it_ret_t USING it_ret_s s, g
		WHERE s.id = it_ret_t.id AND g.id = s.id RETURNING *`,
		"id|k|v|amount|id|k|op|id", "1|a|10|1.50|1|A|upd|1")
	h.expectRows(allRet)
}

func TestSQLReturningMerge(t *testing.T) {
	h := newSQLHarness(t)
	h.setupReturning()

	// Every action, with merge_action() and the source row's columns (NULL
	// for a target row not matched by the source). DO NOTHING rows aren't
	// returned.
	h.expectReturning(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id
		WHEN MATCHED AND s.op = 'del' THEN DELETE
		WHEN MATCHED THEN UPDATE SET k = s.k, v = t.v + 1
		WHEN NOT MATCHED AND s.op = 'skip' THEN DO NOTHING
		WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id, s.k)
		WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = -1
		RETURNING merge_action() AS action, s.id, s.op, t.*`,
		"action|id|op|id|k|v|amount",
		"UPDATE|1|upd|1|A|11|1.50", "DELETE|3|del|3|c|NULL|3.00", "INSERT|4|ins|4|D|NULL|NULL", "UPDATE|NULL|NULL|2|b|-1|NULL")
	h.expectRows(allRet, "1|A|11|1.50", "2|b|-1|NULL", "4|D|NULL|NULL")

	// * lists the source's columns, then the target's.
	h.setupReturning()
	h.expectReturning(`MERGE INTO it_ret_t t USING (SELECT 2 AS id, 'two' AS label) s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET k = s.label RETURNING *, merge_action()`,
		"id|label|id|k|v|amount|merge_action()", "2|two|2|two|20|NULL|UPDATE")
	// An upsert: inserted the first time, updated the second.
	upsert := `MERGE INTO it_ret_t AS d USING (SELECT 7 AS id) AS s ON d.id = s.id
		WHEN MATCHED THEN UPDATE SET v = d.v + 1
		WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, 0)
		RETURNING merge_action(), CASE merge_action() WHEN 'INSERT' THEN 'new' ELSE 'old' END AS age, d.id, d.v`
	h.expectReturning(upsert, "merge_action()|age|id|v", "INSERT|new|7|0")
	h.expectReturning(upsert, "merge_action()|age|id|v", "UPDATE|old|7|1")
	// A CTE source, and no rows.
	h.expectReturning(`WITH src AS (SELECT 9 AS id) MERGE INTO it_ret_t t USING src ON t.id = src.id
		WHEN NOT MATCHED THEN INSERT (id, k) VALUES (src.id, 'nine') RETURNING merge_action() AS a, t.id, t.k`,
		"a|id|k", "INSERT|9|nine")
	h.expectReturning(`MERGE INTO it_ret_t t USING it_ret_s s ON FALSE WHEN MATCHED THEN DELETE RETURNING merge_action() AS a, t.id`, "a|id")
	h.expectRows(allRet, "1|a|10|1.50", "2|two|20|NULL", "3|c|NULL|3.00", "7|NULL|1|NULL", "9|nine|NULL|NULL")

	// An error in RETURNING leaves the table untouched, including the rows
	// processed before it.
	h.setupReturning()
	h.expectError(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id
		WHEN MATCHED THEN DELETE
		WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)
		RETURNING 1 / (t.id - 4)`, "division by zero")
	h.expectRows(allRet, originalRet...)

	// Errors.
	h.expectError(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id WHEN MATCHED THEN DELETE
		RETURNING (SELECT merge_action())`, "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command")
	h.expectError(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id
		WHEN MATCHED AND merge_action() = 'UPDATE' THEN DELETE`, "MERGE_ACTION() can only be used in the RETURNING list")
	h.expectError(`SELECT merge_action()`, "MERGE_ACTION() can only be used in the RETURNING list")
	h.expectError(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id WHEN MATCHED THEN DELETE RETURNING merge_action(1)`,
		"MERGE_ACTION expects no arguments")
	h.expectError(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id WHEN MATCHED THEN DELETE RETURNING id`, "ambiguous")
	h.expectError(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id WHEN MATCHED THEN DELETE RETURNING MAX(t.v)`,
		"aggregate functions are not allowed in RETURNING")
	h.expectRows(allRet, originalRet...)
}

func TestSQLUpdateRowAssignment(t *testing.T) {
	h := newSQLHarness(t)
	h.setupReturning()

	// Row constructors: (…) and ROW(…), which also allows a single column;
	// mixed with plain assignments; with qualified columns.
	h.expectAffected(`UPDATE it_ret_t SET (k, v) = ('q', 1) WHERE id = 1`, 1)
	h.expectAffected(`UPDATE it_ret_t SET (k, v) = ROW('r', v + 1) WHERE id = 2`, 1)
	h.expectAffected(`UPDATE it_ret_t SET (v) = ROW(30), amount = 9 WHERE id = 3`, 1)
	h.expectRows(allRet, "1|q|1|1.50", "2|r|21|NULL", "3|c|30|9.00")
	h.expectAffected(`UPDATE it_ret_t AS t SET amount = 0, (t.k, v) = (UPPER(t.k), t.v * 2) WHERE t.id = 1`, 1)
	// Every value is computed from the row before the change, so this swaps
	// v and amount (cast to each column's type).
	h.expectAffected(`UPDATE it_ret_t SET (v, amount) = (amount, v) WHERE id = 3`, 1)
	h.expectAffected(`UPDATE it_ret_t SET (v, amount) = ('5', '1.005') WHERE id = 2`, 1)
	h.expectRows(allRet, "1|Q|2|0.00", "2|r|5|1.01", "3|c|9|30.00")

	// A subquery: uncorrelated, it runs once for all rows.
	w := h.expectWork(`UPDATE it_ret_t SET (k, v) = (SELECT k, id * 100 FROM it_ret_s WHERE op = 'ins') WHERE id >= 2 RETURNING k, v`,
		"D|400", "D|400")
	if w.runs != 1 {
		t.Errorf("uncorrelated SET (…) = (SELECT …) ran %d times, want 1", w.runs)
	}
	h.expectRows(allRet, "1|Q|2|0.00", "2|D|400|1.01", "3|D|400|30.00")
	// Correlated: a row without a match gets NULLs; the subquery runs once
	// per row, not once per column.
	w = h.expectWork(`UPDATE it_ret_t t SET (k, v) = (SELECT s.k, s.id * 10 FROM it_ret_s s WHERE s.id = t.id) RETURNING 1`, "1", "1", "1")
	if w.runs != 3 {
		t.Errorf("correlated SET (…) = (SELECT …) ran %d times for 3 rows, want 3", w.runs)
	}
	h.expectRows(allRet, "1|A|10|0.00", "2|NULL|NULL|1.01", "3|C|30|30.00")
	h.expectAffected(`UPDATE it_ret_t SET (k) = (SELECT 'one') WHERE id = 1`, 1)
	h.expectRows(`SELECT k FROM it_ret_t WHERE id = 1`, "one")

	// More than one row is an error, as are NULLs from no row in a NOT NULL
	// column; nothing is written, even for rows before the failing one.
	h.expectError(`UPDATE it_ret_t SET (k, v) = (SELECT k, id FROM it_ret_s WHERE id < 4)`,
		"more than one row returned by a subquery used as an expression")
	h.expectError(`UPDATE it_ret_t t SET (k, v) = (SELECT s.k, s.id FROM it_ret_s s WHERE s.id = t.id OR (t.id = 3 AND s.id = 4))`,
		"more than one row returned by a subquery used as an expression")
	h.expectError(`UPDATE it_ret_t t SET (id, k) = (SELECT s.id, s.k FROM it_ret_s s WHERE s.id = t.id)`, "not-null constraint")
	h.expectError(`UPDATE it_ret_t SET (v) = ROW('x')`, `column "v"`)
	const now = "1|one|10|0.00"
	h.expectRows(allRet, now, "2|NULL|NULL|1.01", "3|C|30|30.00")

	// The number of columns must match.
	for _, q := range []string{
		`UPDATE it_ret_t SET (k, v) = ('x', 1, 2)`,
		`UPDATE it_ret_t SET (k, v) = ROW('x')`,
		`UPDATE it_ret_t SET (k) = ROW()`,
		`UPDATE it_ret_t SET (k, v) = (SELECT 'x')`,
		`UPDATE it_ret_t SET (k, v) = (SELECT 'x', 1, 2)`,
	} {
		h.expectError(q, "number of columns does not match number of values")
	}
	// The source must be a row constructor or a subquery.
	for _, q := range []string{
		`UPDATE it_ret_t SET (k, v) = 1`,
		`UPDATE it_ret_t SET (k) = ('x')`,
		`UPDATE it_ret_t SET (k, v) = k`,
	} {
		h.expectError(q, "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression")
	}
	h.expectError(`UPDATE it_ret_t SET (nope, v) = (1, 2)`, `column "nope" does not exist`)
	h.expectError(`UPDATE it_ret_t t SET (s.k, v) = (1, 2) FROM it_ret_s s`, "is not in the table being updated")
	h.expectError(`UPDATE it_ret_t SET (k, v) = (1, 2`, "syntax error")
	h.expectRows(allRet, now, "2|NULL|NULL|1.01", "3|C|30|30.00")

	// UPDATE … FROM: values and a subquery reading the FROM items.
	h.setupReturning()
	h.expectReturning(`UPDATE it_ret_t t SET (k, v) = (s.op, s.id + 1000) FROM it_ret_s s WHERE s.id = t.id RETURNING t.*`,
		"id|k|v|amount", "1|upd|1001|1.50", "3|del|1003|3.00")
	h.expectReturning(`UPDATE it_ret_t t SET (k, v) = (SELECT UPPER(s.op), COUNT(*) FROM it_ret_s s2 WHERE s2.id <= s.id)
		FROM it_ret_s s WHERE s.id = t.id RETURNING t.id, t.k, t.v`,
		"id|k|v", "1|UPD|1", "3|DEL|2")

	// MERGE's UPDATE SET, in WHEN MATCHED and WHEN NOT MATCHED BY SOURCE.
	h.setupReturning()
	h.expectReturning(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET (k, v) = (s.op, t.v + 1)
		WHEN NOT MATCHED AND s.op = 'ins' THEN INSERT (id, k) VALUES (s.id, s.k)
		WHEN NOT MATCHED BY SOURCE THEN UPDATE SET (k, v) = (SELECT 'gone', COUNT(*) FROM it_ret_s)
		RETURNING merge_action() AS a, t.id, t.k, t.v`,
		"a|id|k|v", "UPDATE|1|upd|11", "UPDATE|3|del|NULL", "INSERT|4|D|NULL", "UPDATE|2|gone|4")
	h.expectError(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET (k, v) = ROW(s.k)`,
		"number of columns does not match number of values")
	h.expectError(`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET (k, v) = (SELECT k, id FROM it_ret_s)`,
		"more than one row returned by a subquery used as an expression")
	h.expectRows(allRet, "1|upd|11|1.50", "2|gone|4|NULL", "3|del|NULL|3.00", "4|D|NULL|NULL")
}

// stmtWith prepares a statement with sql and, if rec is not nil, binds it.
func (h *sqlHarness) stmtWith(sql string, rec arrow.RecordBatch) adbc.StatementWithContext {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { st.Close(h.ctx) })
	if err := st.SetSqlQuery(h.ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	if rec != nil {
		if err := st.Bind(h.ctx, rec); err != nil {
			h.t.Fatal(err)
		}
	}
	return st
}

// executeSchema returns the result schema of a statement without running it.
func (h *sqlHarness) executeSchema(sql string) (*arrow.Schema, error) {
	h.t.Helper()
	return h.stmtWith(sql, nil).(adbc.StatementExecuteSchemaWithContext).ExecuteSchema(h.ctx)
}

// queryWith runs a statement with ExecuteQuery and returns its column names,
// its rows ("v1|v2|…", sorted) and the row count it reported.
func (h *sqlHarness) queryWith(sql string, rec arrow.RecordBatch) (string, []string, int64) {
	h.t.Helper()
	rdr, n, err := h.stmtWith(sql, rec).ExecuteQuery(h.ctx)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	defer rdr.Release()
	var rows []string
	for rdr.Next() {
		b := rdr.RecordBatch()
		for r := 0; r < int(b.NumRows()); r++ {
			cells := make([]string, b.NumCols())
			for c := range cells {
				if cells[c] = "NULL"; !b.Column(c).IsNull(r) {
					cells[c] = b.Column(c).ValueStr(r)
				}
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
	}
	slices.Sort(rows)
	return fieldNames(rdr.Schema()), rows, n
}

// paramBatch builds a batch of parameter rows from int64 ids and strings.
func paramBatch(ids []int64, ks []string) arrow.RecordBatch {
	mem := memory.DefaultAllocator
	ib := array.NewInt64Builder(mem)
	ib.AppendValues(ids, nil)
	sb := array.NewStringBuilder(mem)
	sb.AppendValues(ks, nil)
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "k", Type: arrow.BinaryTypes.String},
	}, nil), []arrow.Array{ib.NewArray(), sb.NewArray()}, int64(len(ids)))
}

func TestSQLReturningStatement(t *testing.T) {
	h := newSQLHarness(t)
	h.setupReturning()
	check := func(what string, gotCols string, got []string, n int64, wantCols string, wantN int64, want ...string) {
		t.Helper()
		if gotCols != wantCols || strings.Join(got, "\n") != strings.Join(want, "\n") || n != wantN {
			t.Errorf("%s: got %s %q (%d rows), want %s %q (%d rows)", what, gotCols, got, n, wantCols, want, wantN)
		}
	}

	// ExecuteQuery returns the rows and their count.
	cols, rows, n := h.queryWith(`INSERT INTO it_ret_t (id, k) VALUES (4, 'd'), (5, 'e') RETURNING id, __rowid`, nil)
	check("INSERT … RETURNING", cols, rows, n, "id|__rowid", 2, "4|4", "5|5")
	// Without RETURNING, as before: no columns, and the number of rows
	// changed.
	cols, rows, n = h.queryWith(`UPDATE it_ret_t SET v = 0 WHERE id >= 4`, nil)
	check("UPDATE without RETURNING", cols, rows, n, "", 2)
	// ExecuteUpdate returns the number of rows changed.
	if n, err := h.stmtWith(`DELETE FROM it_ret_t WHERE id >= 4 RETURNING *`, nil).ExecuteUpdate(h.ctx); err != nil || n != 2 {
		t.Errorf("ExecuteUpdate of DELETE … RETURNING = %d, %v; want 2", n, err)
	}
	h.expectRows(allRet, originalRet...)

	// ExecuteSchema plans RETURNING without running the statement.
	for _, c := range []struct{ sql, want string }{
		{`INSERT INTO it_ret_t (id) VALUES (9) RETURNING __rowid, *`, "__rowid: int64, id: int32, k: utf8, v: int32, amount: decimal(6, 2)"},
		{`INSERT INTO it_ret_t SELECT * FROM it_ret_t RETURNING id + 1 AS next`, "next: int64"},
		{`UPDATE it_ret_t SET v = 1 RETURNING v, k`, "v: int32, k: utf8"},
		{`UPDATE it_ret_t t SET (k, v) = (s.k, 1) FROM it_ret_s s WHERE s.id = t.id RETURNING *`,
			"id: int32, k: utf8, v: int32, amount: decimal(6, 2), id: int32, k: utf8, op: utf8"},
		{`DELETE FROM it_ret_t RETURNING amount`, "amount: decimal(6, 2)"},
		{`WITH g AS (SELECT 1 AS gid) DELETE FROM it_ret_t USING g WHERE g.gid = it_ret_t.id RETURNING g.*, k`, "gid: int64, k: utf8"},
		{`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id WHEN MATCHED THEN DELETE RETURNING merge_action() AS a, s.op, t.v`,
			"a: utf8, op: utf8, v: int32"},
		{`DELETE FROM it_ret_t`, ""},
		{`MERGE INTO it_ret_t t USING it_ret_s s ON t.id = s.id WHEN MATCHED THEN DELETE`, ""},
	} {
		schema, err := h.executeSchema(c.sql)
		if err != nil {
			t.Errorf("ExecuteSchema(%s): %v", c.sql, err)
			continue
		}
		var fields []string
		for _, f := range schema.Fields() {
			fields = append(fields, f.Name+": "+f.Type.String())
		}
		if got := strings.Join(fields, ", "); got != c.want {
			t.Errorf("ExecuteSchema(%s) = %s, want %s", c.sql, got, c.want)
		}
	}
	if _, err := h.executeSchema(`UPDATE it_ret_t SET v = 1 RETURNING nope`); err == nil ||
		!strings.Contains(err.Error(), `column "nope" does not exist`) {
		t.Errorf("ExecuteSchema with a bad RETURNING list: %v", err)
	}
	h.expectRows(allRet, originalRet...)
}

func TestSQLReturningBind(t *testing.T) {
	h := newSQLHarness(t)
	h.setupReturning()

	// One execution per parameter row; their rows are returned together.
	cols, rows, n := h.queryWith(`INSERT INTO it_ret_t (id, k) VALUES ($1, $2) RETURNING __rowid, id + 1, k, $2 AS p`,
		paramBatch([]int64{4, 5, 6}, []string{"d", "e", "f"}))
	if want := []string{"4|5|d|d", "5|6|e|e", "6|7|f|f"}; cols != "__rowid|id + 1|k|p" ||
		strings.Join(rows, "\n") != strings.Join(want, "\n") || n != 3 {
		t.Errorf("INSERT … RETURNING with parameters: %s %q (%d rows), want %q", cols, rows, n, want)
	}
	// Bound parameters on every statement type; ExecuteUpdate adds up the
	// counts.
	cols, rows, _ = h.queryWith(`UPDATE it_ret_t SET (k, v) = ($2, LENGTH($2)) WHERE id = $1 RETURNING id, k, v`,
		paramBatch([]int64{4, 6}, []string{"dd", "ffff"}))
	if want := []string{"4|dd|2", "6|ffff|4"}; cols != "id|k|v" || strings.Join(rows, "\n") != strings.Join(want, "\n") {
		t.Errorf("UPDATE … RETURNING with parameters: %s %q, want %q", cols, rows, want)
	}
	cols, rows, _ = h.queryWith(`MERGE INTO it_ret_t t USING (SELECT ? AS id, ? AS k) s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET k = s.k WHEN NOT MATCHED THEN INSERT (id, k) VALUES (s.id, s.k)
		RETURNING merge_action(), t.id, t.k`,
		paramBatch([]int64{1, 9}, []string{"upd", "new"}))
	if want := []string{"INSERT|9|new", "UPDATE|1|upd"}; cols != "merge_action()|id|k" || strings.Join(rows, "\n") != strings.Join(want, "\n") {
		t.Errorf("MERGE … RETURNING with parameters: %s %q, want %q", cols, rows, want)
	}
	if n, err := h.stmtWith(`DELETE FROM it_ret_t WHERE id = ? OR k = ? RETURNING k`,
		paramBatch([]int64{4, 9}, []string{"e", "ffff"})).ExecuteUpdate(h.ctx); err != nil || n != 4 {
		t.Errorf("ExecuteUpdate of DELETE … RETURNING with parameters = %d, %v; want 4", n, err)
	}
	h.expectRows(allRet, "1|upd|10|1.50", "2|b|20|NULL", "3|c|NULL|3.00")

	// No parameter rows: nothing runs, and the result has the columns.
	cols, rows, n = h.queryWith(`INSERT INTO it_ret_t (id, k) VALUES (?, ?) RETURNING id, k AS key`, paramBatch(nil, nil))
	if cols != "id|key" || len(rows) != 0 || n != 0 {
		t.Errorf("INSERT … RETURNING without parameter rows: %s %q (%d rows), want id|key and no rows", cols, rows, n)
	}
	cols, rows, _ = h.queryWith(`UPDATE it_ret_t t SET v = 0 FROM it_ret_s s WHERE s.id = t.id AND t.id = ? AND s.k <> ? RETURNING *`,
		paramBatch(nil, nil))
	if cols != "id|k|v|amount|id|k|op" || len(rows) != 0 {
		t.Errorf("UPDATE … FROM … RETURNING without parameter rows: %s %q", cols, rows)
	}
	h.expectRows(allRet, "1|upd|10|1.50", "2|b|20|NULL", "3|c|NULL|3.00")
}

func TestSQLReturningTemp(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP TABLE IF EXISTS pg_temp.it_ret_tmp")
		h.exec("DROP TABLE IF EXISTS public.it_ret_tmp")
	}
	drop()
	t.Cleanup(drop)
	h.exec("CREATE TABLE it_ret_tmp (id INTEGER, k VARCHAR)")
	h.exec("INSERT INTO it_ret_tmp VALUES (1, 'perm'), (2, 'perm2')")

	// A temporary table that shadows the permanent one, with its own row
	// ids.
	h.exec("CREATE TEMP TABLE it_ret_tmp (id INTEGER, k VARCHAR)")
	h.expectRows(`INSERT INTO it_ret_tmp VALUES (1, 'temp') RETURNING __rowid, *`, "1|1|temp")
	h.expectRows(`INSERT INTO it_ret_tmp SELECT id, k || '+' FROM public.it_ret_tmp WHERE id = 2 RETURNING __rowid, *`, "2|2|perm2+")
	h.expectReturning(`UPDATE it_ret_tmp SET (k) = ROW(k || '!') RETURNING *`, "id|k", "1|temp!", "2|perm2+!")
	h.expectReturning(`MERGE INTO it_ret_tmp t USING public.it_ret_tmp p ON t.id = p.id
		WHEN MATCHED AND p.id = 1 THEN UPDATE SET (k) = (SELECT t.k || p.k)
		WHEN MATCHED THEN DELETE
		RETURNING merge_action(), p.k, t.k`,
		"merge_action()|k|k", "UPDATE|perm|temp!perm", "DELETE|perm2|perm2+!")
	h.expectReturning(`DELETE FROM pg_temp.it_ret_tmp t USING public.it_ret_tmp p WHERE p.id = t.id RETURNING t.*`,
		"id|k", "1|temp!perm")
	h.expectRows(`SELECT COUNT(*) FROM pg_temp.it_ret_tmp`, "0")
	h.expectRows(`SELECT id, k FROM public.it_ret_tmp ORDER BY id`, "1|perm", "2|perm2")
	// RETURNING on the permanent table, by schema.
	h.expectReturning(`UPDATE public.it_ret_tmp SET k = 'p' WHERE id = 1 RETURNING it_ret_tmp.k`, "k", "p")
}
