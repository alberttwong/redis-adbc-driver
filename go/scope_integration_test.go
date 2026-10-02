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

// Name resolution across query levels (#102). As in Postgres, a FROM item
// with an alias is visible only by its alias, so in a subquery that reads
// a table under an alias, the table's own name means an enclosing query's
// item: in `SELECT … FROM t WHERE EXISTS (SELECT 1 FROM t x WHERE x.v >
// t.v)`, t.v is the outer row's.

import (
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// setupScope creates it_sc.t (the issue's table) and it_sc.e (keyed
// events: v grows within each k).
func (h *sqlHarness) setupScope() {
	h.t.Helper()
	h.exec(`DROP SCHEMA IF EXISTS it_sc CASCADE`)
	h.exec(`CREATE SCHEMA it_sc`)
	h.t.Cleanup(func() { h.exec(`DROP SCHEMA IF EXISTS it_sc CASCADE`) })
	h.exec(`CREATE TABLE it_sc.t (id INTEGER, v INTEGER)`)
	h.exec(`INSERT INTO it_sc.t VALUES (1, 10), (2, 20)`)
	h.exec(`CREATE TABLE it_sc.e (id INTEGER, k INTEGER, v INTEGER)`)
	h.exec(`INSERT INTO it_sc.e VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 2, 5), (5, 2, 15)`)
}

// expectScopeError runs a query (or, with exec, a statement) and checks that
// it fails with an error containing substr. Unlike expectError, it goes on
// after a mismatch.
func (h *sqlHarness) expectScopeError(sql, substr string, exec bool) {
	h.t.Helper()
	var err error
	if exec {
		err = h.execErr(sql)
	} else {
		var st adbc.StatementWithContext
		if st, err = h.conn.NewStatement(h.ctx); err != nil {
			h.t.Fatal(err)
		}
		defer st.Close(h.ctx)
		if err := st.SetSqlQuery(h.ctx, sql); err != nil {
			h.t.Fatal(err)
		}
		var rdr array.RecordReader
		if rdr, _, err = st.ExecuteQuery(h.ctx); err == nil {
			rdr.Release()
		}
	}
	if err == nil {
		h.t.Errorf("%s: expected an error containing %q", sql, substr)
	} else if !strings.Contains(err.Error(), substr) {
		h.t.Errorf("%s: error %q does not contain %q", sql, err, substr)
	}
}

// The issue's queries.
func TestSQLAliasScopeIssue(t *testing.T) {
	h := newSQLHarness(t)
	h.setupScope()
	h.expectRows(`select id from it_sc.t where exists (select 1 from it_sc.t x where x.v > t.v)`, "1")
	h.expectRows(`select id, (select count(*) from it_sc.t x where x.v < t.v) from it_sc.t order by id`, "1|0", "2|1")
	h.expectRows(`select id from it_sc.t o where exists (select 1 from it_sc.t x where x.v > o.v)`, "1")
}

// The rule at every level and in every kind of subquery.
func TestSQLAliasScopeForms(t *testing.T) {
	h := newSQLHarness(t)
	h.setupScope()

	// Nested two levels deep: t.v is the outermost row's, x.v the middle
	// one's.
	h.expectRows(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x
			WHERE EXISTS (SELECT 1 FROM it_sc.t y WHERE y.v > x.v AND y.v > t.v)) ORDER BY id`, "1")
	h.expectRows(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x
			WHERE x.id = t.id AND EXISTS (SELECT 1 FROM it_sc.t y WHERE y.v > t.v)) ORDER BY id`, "1")
	// The same table three times: the previous event of each key.
	h.expectRows(`SELECT id, (SELECT x.id FROM it_sc.e x WHERE x.k = e.k AND x.v < e.v
			AND NOT EXISTS (SELECT 1 FROM it_sc.e y WHERE y.k = e.k AND y.v > x.v AND y.v < e.v)) AS prev
		FROM it_sc.e ORDER BY id`,
		"1|NULL", "2|1", "3|2", "4|NULL", "5|4")
	// The outer query is a join: e.k is its item e.
	h.expectRows(`SELECT t.id, e.id FROM it_sc.t JOIN it_sc.e ON e.v = t.v
		WHERE EXISTS (SELECT 1 FROM it_sc.e x WHERE x.k = e.k AND x.v > e.v) ORDER BY 1`, "1|1", "2|2")
	// A join in the subquery.
	h.expectRows(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x JOIN it_sc.e y ON y.v = x.v
		WHERE x.v > t.v) ORDER BY id`, "1")

	// A CTE named like the table, outside and inside the subquery.
	h.expectRows(`WITH t AS (SELECT id, v - 5 AS v FROM it_sc.t)
		SELECT id FROM t WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE x.v > t.v) ORDER BY id`, "1", "2")
	h.expectRows(`WITH t AS (SELECT id, v + 5 AS v FROM it_sc.t)
		SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM t x WHERE x.v < t.v) ORDER BY id`, "2")

	// LATERAL.
	h.expectRows(`SELECT t.id, l.n FROM it_sc.t, LATERAL (SELECT COUNT(*) AS n FROM it_sc.t x WHERE x.v < t.v) l
		ORDER BY t.id`, "1|0", "2|1")

	// Scalar subqueries in WHERE, HAVING and ORDER BY (and in the SELECT list,
	// above).
	h.expectRows(`SELECT id FROM it_sc.t WHERE (SELECT COUNT(*) FROM it_sc.t x WHERE x.v < t.v) = 1`, "2")
	h.expectRows(`SELECT v, COUNT(*) FROM it_sc.t GROUP BY v
		HAVING (SELECT COUNT(*) FROM it_sc.t x WHERE x.v < t.v) = 1`, "20|1")
	h.expectRows(`SELECT id FROM it_sc.t ORDER BY (SELECT COUNT(*) FROM it_sc.t x WHERE x.v > t.v), id`, "2", "1")

	// IN, NOT IN, ALL and ANY.
	h.expectRows(`SELECT id FROM it_sc.t WHERE v - 10 IN (SELECT x.v FROM it_sc.t x WHERE x.v < t.v) ORDER BY id`, "2")
	h.expectRows(`SELECT id FROM it_sc.t WHERE v - 10 NOT IN (SELECT x.v FROM it_sc.t x WHERE x.v < t.v) ORDER BY id`, "1")
	h.expectRows(`SELECT id FROM it_sc.t WHERE v > ALL (SELECT x.v FROM it_sc.t x WHERE x.id <> t.id) ORDER BY id`, "2")
	h.expectRows(`SELECT id FROM it_sc.t WHERE v < ANY (SELECT x.v FROM it_sc.t x WHERE x.id <> t.id) ORDER BY id`, "1")

	// A derived table, a view and set operations.
	h.expectRows(`SELECT id FROM (SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE x.v > t.v)) d`, "1")
	h.exec(`CREATE VIEW it_sc.later AS SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE x.v > t.v)`)
	h.expectRows(`SELECT id FROM it_sc.later`, "1")
	h.expectRows(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE x.v > t.v)
		UNION ALL SELECT id + 10 FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE x.v < t.v) ORDER BY 1`, "1", "12")
	h.expectRows(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT x.id FROM it_sc.t x WHERE x.v > t.v
		UNION SELECT y.id FROM it_sc.t y WHERE y.v < t.v - 5) ORDER BY id`, "1", "2")

	// What doesn't change: an item without an alias is visible by its name
	// (the innermost one), also schema-qualified, and an unqualified column
	// is the innermost level's.
	h.expectRows(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t WHERE t.v > 15) ORDER BY id`, "1", "2")
	h.expectRows(`SELECT id FROM it_sc.t o WHERE EXISTS (SELECT 1 FROM it_sc.t WHERE t.v > o.v) ORDER BY id`, "1")
	h.expectRows(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE x.v > it_sc.t.v) ORDER BY id`, "1")
	h.expectRows(`SELECT it_sc.t.id FROM it_sc.t WHERE it_sc.t.v > 15`, "2")
	h.expectRows(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE v > 15) ORDER BY id`, "1", "2")
}

// A subquery correlated through the outer table's name still runs as a
// semi-join when its correlation is `inner = outer`, and otherwise per
// distinct outer value; an uncorrelated one still runs once.
func TestSQLAliasScopeSubqueryPaths(t *testing.T) {
	h := newSQLHarness(t)
	h.setupScope()
	semi := func(w subqueryWork, what string) {
		t.Helper()
		if w.semiJoins != 1 || w.runs != 0 {
			t.Errorf("%s: %+v, want 1 semi-join and no per-row runs", what, w)
		}
	}
	semi(h.expectWork(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE x.id - 1 = t.id)`, "1"), "EXISTS")
	semi(h.expectWork(`SELECT id FROM it_sc.t WHERE NOT EXISTS (SELECT 1 FROM it_sc.t x WHERE x.id - 1 = t.id)`, "2"), "NOT EXISTS")
	semi(h.expectWork(`SELECT id FROM it_sc.t WHERE id + 1 IN (SELECT x.id FROM it_sc.t x WHERE x.v - 10 = t.v)`, "1"), "IN")
	semi(h.expectWork(`SELECT id FROM it_sc.e WHERE k IN (SELECT x.k FROM it_sc.e x WHERE x.id - 1 = e.id) ORDER BY id`,
		"1", "2", "4"), "IN on the same column")
	semi(h.expectWork(`SELECT id FROM it_sc.e WHERE (k, v) IN (SELECT x.k, x.v - 10 FROM it_sc.e x WHERE x.id - 1 = e.id)
		ORDER BY id`, "1", "2", "4"), "row IN")

	// x.v > e.v keeps it per row: once per distinct (k, v) of the outer rows.
	w := h.expectWork(`SELECT id FROM it_sc.e WHERE EXISTS (SELECT 1 FROM it_sc.e x WHERE x.k = e.k AND x.v > e.v) ORDER BY id`,
		"1", "2", "4")
	if w.semiJoins != 0 || w.runs != 5 {
		t.Errorf("per-row EXISTS: %+v, want 5 runs and no semi-join", w)
	}
	w = h.expectWork(`SELECT id, (SELECT COUNT(*) FROM it_sc.t x WHERE x.v < t.v) FROM it_sc.t ORDER BY id`, "1|0", "2|1")
	if w.semiJoins != 0 || w.runs != 2 {
		t.Errorf("per-row scalar subquery: %+v, want 2 runs", w)
	}
	// Uncorrelated (x.v is the subquery's own column): one run for all the
	// rows.
	w = h.expectWork(`SELECT id, v IN (SELECT x.v FROM it_sc.t x WHERE x.v > 15) FROM it_sc.t ORDER BY id`, "1|false", "2|true")
	if w.runs != 1 || w.semiJoins != 0 {
		t.Errorf("uncorrelated IN: %+v, want 1 run", w)
	}
}

// UPDATE, DELETE, UPDATE … FROM, DELETE … USING and MERGE, whose target's
// name is visible in their subqueries.
func TestSQLAliasScopeDML(t *testing.T) {
	h := newSQLHarness(t)
	h.setupScope()
	h.exec(`CREATE TABLE it_sc.w (id INTEGER, v INTEGER, n INTEGER)`)
	reset := func() {
		h.exec(`DELETE FROM it_sc.w`)
		h.exec(`INSERT INTO it_sc.w VALUES (1, 10, NULL), (2, 20, NULL), (3, 30, NULL)`)
	}

	reset()
	h.expectAffected(`UPDATE it_sc.w SET n = 1 WHERE EXISTS (SELECT 1 FROM it_sc.w x WHERE x.v > w.v)`, 2)
	h.expectRows(`SELECT id, n FROM it_sc.w ORDER BY id`, "1|1", "2|1", "3|NULL")
	h.expectAffected(`UPDATE it_sc.w SET n = (SELECT COUNT(*) FROM it_sc.w x WHERE x.v < w.v)`, 3)
	h.expectRows(`SELECT id, n FROM it_sc.w ORDER BY id`, "1|0", "2|1", "3|2")
	h.expectRows(`UPDATE it_sc.w SET n = 9 WHERE id = 3 RETURNING id, (SELECT COUNT(*) FROM it_sc.w x WHERE x.v < w.v)`, "3|2")
	h.expectAffected(`UPDATE it_sc.w o SET n = 7 WHERE EXISTS (SELECT 1 FROM it_sc.w x WHERE x.v > o.v)`, 2)
	h.expectRows(`SELECT id, n FROM it_sc.w ORDER BY id`, "1|7", "2|7", "3|9")
	h.expectAffected(`DELETE FROM it_sc.w WHERE EXISTS (SELECT 1 FROM it_sc.w x WHERE x.v < w.v)`, 2)
	h.expectRows(`SELECT id FROM it_sc.w ORDER BY id`, "1")

	// A semi-join.
	reset()
	h.expectAffected(`DELETE FROM it_sc.w WHERE EXISTS (SELECT 1 FROM it_sc.w x WHERE x.id - 1 = w.id)`, 2)
	h.expectRows(`SELECT id FROM it_sc.w ORDER BY id`, "3")

	// UPDATE … FROM and DELETE … USING.
	reset()
	h.expectAffected(`UPDATE it_sc.w SET n = s.v FROM it_sc.t s
		WHERE s.id = w.id AND EXISTS (SELECT 1 FROM it_sc.w x WHERE x.v < w.v)`, 1)
	h.expectRows(`SELECT id, n FROM it_sc.w ORDER BY id`, "1|NULL", "2|20", "3|NULL")
	h.expectAffected(`DELETE FROM it_sc.w USING it_sc.t s
		WHERE s.id = w.id AND EXISTS (SELECT 1 FROM it_sc.w x WHERE x.v < w.v)`, 1)
	h.expectRows(`SELECT id FROM it_sc.w ORDER BY id`, "1", "3")

	// MERGE.
	reset()
	h.expectAffected(`MERGE INTO it_sc.w USING it_sc.t s ON s.id = w.id
		WHEN MATCHED AND EXISTS (SELECT 1 FROM it_sc.w x WHERE x.v < w.v) THEN UPDATE SET n = s.v + 100
		WHEN MATCHED THEN UPDATE SET n = -1`, 2)
	h.expectRows(`SELECT id, n FROM it_sc.w ORDER BY id`, "1|-1", "2|120", "3|NULL")
}

// Postgres's errors.
func TestSQLAliasScopeErrors(t *testing.T) {
	h := newSQLHarness(t)
	h.setupScope()
	h.exec(`CREATE TABLE it_sc.w (id INTEGER, v INTEGER, n INTEGER)`)
	h.exec(`INSERT INTO it_sc.w VALUES (1, 10, NULL)`)

	// The table's name where only its alias is visible.
	const hintX = `invalid reference to FROM-clause entry for table "t"; perhaps you meant to reference the table alias "x"`
	h.expectScopeError(`SELECT t.id FROM it_sc.t x`, hintX, false)
	h.expectScopeError(`SELECT t.* FROM it_sc.t x`, hintX, false)
	h.expectScopeError(`SELECT id FROM it_sc.t o WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE x.v > t.v)`, hintX, false)
	h.expectScopeError(`SELECT x.id FROM it_sc.t x JOIN it_sc.e y ON y.id = t.id`, hintX, false)
	h.expectScopeError(`UPDATE it_sc.w o SET n = 1 WHERE w.id = 1`,
		`invalid reference to FROM-clause entry for table "w"; perhaps you meant to reference the table alias "o"`, true)
	h.expectScopeError(`DELETE FROM it_sc.w o WHERE EXISTS (SELECT 1 FROM it_sc.w x WHERE x.v > w.v)`,
		`invalid reference to FROM-clause entry for table "w"; perhaps you meant to reference the table alias "x"`, true)
	h.expectRows(`SELECT id, n FROM it_sc.w`, "1|NULL")

	// A name no level has.
	h.expectScopeError(`SELECT z.id FROM it_sc.t`, `missing FROM-clause entry for table "z"`, false)
	h.expectScopeError(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x WHERE x.v > z.v)`,
		`missing FROM-clause entry for table "z"`, false)
	h.expectScopeError(`UPDATE it_sc.w SET n = z.v`, `missing FROM-clause entry for table "z"`, true)

	// The innermost item of a name hides the outer ones, also for a column
	// only an outer one has.
	h.expectScopeError(`SELECT id FROM it_sc.e WHERE EXISTS (SELECT 1 FROM it_sc.t e WHERE e.k = 1)`,
		`column "e.k" does not exist in table "t"`, false)

	// Ambiguous names, at the innermost level that has them.
	h.expectScopeError(`SELECT id FROM it_sc.t, it_sc.e`, `column reference "id" is ambiguous`, false)
	h.expectScopeError(`SELECT id FROM it_sc.t WHERE EXISTS (SELECT 1 FROM it_sc.t x, it_sc.e y WHERE id = 1)`,
		`column reference "id" is ambiguous`, false)
	h.expectScopeError(`SELECT 1 FROM it_sc.t, it_sc.e t`, `table name "t" specified more than once`, false)
}
