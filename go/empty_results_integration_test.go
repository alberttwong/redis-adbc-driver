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

// Queries that can't return rows (empty.go): their results, their schemas
// (those of the full queries), the forms dbt writes, their errors, and that
// they read no rows.

import (
	"context"
	"maps"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	goredis "github.com/redis/go-redis/v9"
)

// setupEmpty creates it_empty_t (4 rows, one of NULLs) and it_empty_u (4
// rows, three matching it_empty_t on t_id), and the views it_empty_v
// (expanded in place) and it_empty_vagg (computed).
func (h *sqlHarness) setupEmpty() {
	h.t.Helper()
	views := []string{"it_empty_v", "it_empty_vagg"}
	dropViews := func() {
		for _, v := range views {
			h.exec("DROP VIEW IF EXISTS " + v)
		}
	}
	dropViews()
	h.dropTables("it_empty_t", "it_empty_u")
	h.t.Cleanup(dropViews) // runs before the tables are dropped
	h.exec(`CREATE TABLE it_empty_t (id INTEGER NOT NULL, g VARCHAR, x INTEGER, amount NUMERIC(10,2),
		d DATE, ts TIMESTAMP, f DOUBLE, flag BOOLEAN)`)
	h.exec(`INSERT INTO it_empty_t VALUES
		(1, 'a', 10, 1.50, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00', 0.5, true),
		(2, 'a', 20, 2.25, DATE '2024-02-01', TIMESTAMP '2024-02-01 11:00:00', 1.5, false),
		(3, 'b', NULL, NULL, NULL, NULL, NULL, NULL),
		(4, 'c', 40, 4.00, DATE '2024-04-01', TIMESTAMP '2024-04-01 12:30:00', 4.25, true)`)
	h.exec("CREATE TABLE it_empty_u (id INTEGER, t_id INTEGER, name VARCHAR)")
	h.exec("INSERT INTO it_empty_u VALUES (1, 1, 'one'), (2, 1, 'uno'), (3, 4, 'four'), (4, 9, 'nine')")
	h.exec("CREATE VIEW it_empty_v AS SELECT id, g, x * 2 AS x2 FROM it_empty_t WHERE id > 1")
	h.exec("CREATE VIEW it_empty_vagg AS SELECT g, COUNT(*) AS n, SUM(amount) AS s FROM it_empty_t GROUP BY g")
}

// emptyModel is a dbt-like model: two GROUP BYs in CTEs, one over a join,
// left-joined together. Its rows are "a|2|3.75|2", "b|1|NULL|0" and
// "c|1|4.00|1".
const emptyModel = `with totals as (
    select g as k, count(*) as n, sum(amount) as total
    from it_empty_t
    group by g
), matches as (
    select t.g as k, count(u.id) as m
    from it_empty_t t
    left join it_empty_u u on u.t_id = t.id
    group by t.g
)
select totals.k, totals.n, totals.total, coalesce(matches.m, 0) as m
from totals
left join matches on matches.k = totals.k
order by totals.k`

// dbtEmpty is dbt's get_empty_subquery_sql (model contracts, snapshots,
// unit tests) indented by two spaces; TestSQLEmptyDbt also writes it with
// dbt-postgres's own indentation.
func dbtEmpty(sql string) string {
	return "select * from (\n  " + sql + "\n) as __dbt_sbq\nwhere false\nlimit 0"
}

// schemaText renders a schema: each field's name, Arrow type, nullability
// and metadata.
func schemaText(s *arrow.Schema) string {
	parts := make([]string, s.NumFields())
	for i, f := range s.Fields() {
		parts[i] = f.Name + " " + f.Type.String()
		if !f.Nullable {
			parts[i] += " NOT NULL"
		}
		if f.HasMetadata() {
			parts[i] += " " + f.Metadata.String()
		}
	}
	return strings.Join(parts, ", ")
}

// expectNoRows runs a query and checks that it returns no rows, with the
// schema want (schemaText).
func (h *sqlHarness) expectNoRows(sql, want string) {
	h.t.Helper()
	if got := schemaText(h.expectRows(sql)); got != want {
		h.t.Errorf("%s\n schema: %s\n   want: %s", sql, got, want)
	}
}

func (h *sqlHarness) expectSchemaError(sql, substr string) {
	h.t.Helper()
	if _, err := h.executeSchema(sql); err == nil || !strings.Contains(err.Error(), substr) {
		h.t.Errorf("ExecuteSchema(%s): error %v, want one containing %q", sql, err, substr)
	}
}

const emptyTSchema = "id int32, g utf8, x int32, amount decimal(10, 2), d date32, ts timestamp[us], f float64, flag bool"

func TestSQLEmptyWhere(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()

	// Conditions that are FALSE or NULL whatever the rows hold.
	for _, cond := range []string{
		"false", "1 = 0", "null", "x > 0 and false", "false and x > 0", "not true", "false or 1 = 0",
		"not (x > 0 or true)", "cast(null as boolean)", "null and x > 0", "x > 0 and (false or null)",
		"not (not false)", "'a' = 'b'", "0", "1 > 2 or x = 1 and false", "date '2024-01-02' < date '2024-01-01'",
		// A part that fails to compute is left to the rows: none is read.
		"x / 0 = 1 and false", "1 / 0 = 1 and false",
	} {
		h.expectNoRows("SELECT * FROM it_empty_t WHERE "+cond, emptyTSchema)
	}
	// Not constant: these still read the rows.
	h.expectRows("SELECT id FROM it_empty_t WHERE x > 0 OR false ORDER BY id", "1", "2", "4")
	h.expectRows("SELECT id FROM it_empty_t WHERE NOT (x > 0 AND false) ORDER BY id", "1", "2", "3", "4")
	h.expectRows("SELECT id FROM it_empty_t WHERE 1 = 1 AND id < 3 ORDER BY id", "1", "2")
	h.expectRows("SELECT id FROM it_empty_t WHERE true ORDER BY id", "1", "2", "3", "4")

	// The rest of the query runs over no rows: aggregates without GROUP BY
	// return one row (COUNT 0, the others NULL), with GROUP BY none.
	h.expectRows(`SELECT COUNT(*), COUNT(x), SUM(x), SUM(amount), AVG(f), MIN(g), MAX(d), BOOL_AND(flag),
			STRING_AGG(g, ','), COUNT(*) + 1 FROM it_empty_t WHERE false`,
		"0|0|NULL|NULL|NULL|NULL|NULL|NULL|NULL|1")
	h.expectRows("SELECT COUNT(*) FROM it_empty_t WHERE id = 1 AND false", "0")
	h.expectRows("SELECT g, COUNT(*) FROM it_empty_t WHERE false GROUP BY g")
	h.expectRows("SELECT COUNT(*) AS n FROM it_empty_t WHERE false HAVING COUNT(*) = 0", "0")
	h.expectRows("SELECT COUNT(*) FROM (SELECT * FROM it_empty_t WHERE false) s", "0")
	h.expectRows("SELECT COUNT(*), SUM(x) FROM (SELECT * FROM it_empty_t WHERE false LIMIT 0) s", "0|NULL")
	// The empty grouping set still has its row.
	h.expectRows("SELECT g, COUNT(*) FROM it_empty_t WHERE false GROUP BY ROLLUP (g)", "NULL|0")
	h.expectRows("SELECT g, x, COUNT(*), GROUPING(g) FROM it_empty_t WHERE false GROUP BY GROUPING SETS ((g), (x), ())",
		"NULL|NULL|0|1")
	// Window functions: none over no rows, one over the aggregate's row.
	h.expectRows("SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM it_empty_t WHERE false")
	h.expectRows("SELECT COUNT(*), ROW_NUMBER() OVER () FROM it_empty_t WHERE false", "0|1")
	h.expectRows("SELECT DISTINCT g FROM it_empty_t WHERE false")
	h.expectRows("SELECT DISTINCT ON (g) g, id FROM it_empty_t WHERE false ORDER BY g, id")

	// Indexed lookups, row ids and IN lists are not run either.
	h.expectRows("SELECT * FROM it_empty_t WHERE __rowid = 1 AND false")
	h.expectRows("SELECT * FROM it_empty_t WHERE g = 'a' AND id IN (1, 2) AND g LIKE 'a%' AND false")
	h.expectRows("SELECT g, COUNT(*) FROM it_empty_t WHERE g = 'a' AND false GROUP BY g")

	// Views, CTEs and derived tables, at any level.
	h.expectNoRows("SELECT * FROM it_empty_v WHERE false", "id int32, g utf8, x2 int64")
	h.expectNoRows("SELECT * FROM it_empty_vagg WHERE false", "g utf8, n int64, s decimal(38, 2)")
	h.expectRows("SELECT COUNT(*) FROM it_empty_vagg WHERE false", "0")
	h.expectRows("SELECT COUNT(*) FROM it_empty_v WHERE x2 > 0 AND false", "0")
	h.expectRows("WITH c AS (SELECT * FROM it_empty_t WHERE false) SELECT COUNT(*) FROM c", "0")
	h.expectNoRows("WITH c AS (SELECT * FROM it_empty_t) SELECT * FROM c WHERE false", emptyTSchema)
	h.expectRows("SELECT * FROM (SELECT * FROM (SELECT id FROM it_empty_t WHERE false) a) b")
	h.expectRows("SELECT * FROM (SELECT id FROM it_empty_t ORDER BY id LIMIT 2) s WHERE false")
	// A CTE read both where it doesn't matter and where it does.
	h.expectRows(`WITH c AS (SELECT * FROM it_empty_t)
		SELECT COUNT(*) FROM c WHERE false UNION ALL SELECT COUNT(*) FROM c ORDER BY 1`, "0", "4")
	h.expectRows(`WITH c AS (SELECT * FROM it_empty_t)
		SELECT COUNT(*) FROM c UNION ALL SELECT COUNT(*) FROM c WHERE false ORDER BY 1`, "0", "4")

	// Recursive CTEs: an empty non-recursive term, or a query that reads none
	// of the result.
	h.expectRows(`WITH RECURSIVE r(n) AS (SELECT 1 FROM it_empty_t WHERE false UNION ALL SELECT n + 1 FROM r WHERE n < 3)
		SELECT * FROM r`)
	h.expectNoRows(`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r WHERE false`,
		"n int64")
	h.expectNoRows(`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3)
			SEARCH DEPTH FIRST BY n SET ord SELECT * FROM r LIMIT 0`, "n int64, ord int64")

	// Other FROM items.
	h.expectNoRows("SELECT * FROM generate_series(1, 3) AS s(n) WHERE false", "n int64")
	h.expectRows("SELECT COUNT(*) FROM information_schema.tables WHERE false", "0")
	h.expectRows("SELECT * FROM it_empty_t, LATERAL (SELECT * FROM it_empty_u WHERE it_empty_u.t_id = it_empty_t.id AND false) s")

	// Without FROM.
	h.expectNoRows("SELECT 1 WHERE false", "1 int64")
	h.expectNoRows("SELECT (SELECT COUNT(*) FROM it_empty_t) WHERE 1 = 0", "(SELECT COUNT(*) FROM it_empty_t) int64")
	h.expectRows("SELECT (SELECT COUNT(*) FROM it_empty_t WHERE false)", "0")

	// Values that would fail to compute on rows that aren't read raise no
	// error, as when no row matched.
	h.expectRows("SELECT 1 / 0 FROM it_empty_t WHERE false")
	h.expectRows("SELECT x / 0 FROM it_empty_t WHERE false")
	h.expectRows("SELECT CAST(g AS INTEGER) FROM it_empty_t WHERE false")
	h.expectError("SELECT COUNT(*), 1 / 0 FROM it_empty_t WHERE false", "division by zero")
}

func TestSQLEmptyJoins(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()
	empty := "(SELECT * FROM it_empty_u WHERE false)"

	// The other side of an outer join keeps its rows, with NULLs.
	h.expectRows("SELECT t.id, s.name FROM it_empty_t t LEFT JOIN "+empty+" s ON s.t_id = t.id ORDER BY t.id",
		"1|NULL", "2|NULL", "3|NULL", "4|NULL")
	h.expectRows("SELECT t.id, u.name FROM it_empty_t t LEFT JOIN it_empty_u u ON false ORDER BY t.id",
		"1|NULL", "2|NULL", "3|NULL", "4|NULL")
	h.expectRows("SELECT t.id, u.name FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id AND 1 = 0 ORDER BY t.id",
		"1|NULL", "2|NULL", "3|NULL", "4|NULL")
	h.expectRows("SELECT s.id, t.id FROM "+empty+" s RIGHT JOIN it_empty_t t ON s.t_id = t.id ORDER BY t.id",
		"NULL|1", "NULL|2", "NULL|3", "NULL|4")
	h.expectRows("SELECT t.id, u.id FROM it_empty_t t RIGHT JOIN it_empty_u u ON false ORDER BY u.id",
		"NULL|1", "NULL|2", "NULL|3", "NULL|4")
	h.expectRows("SELECT t.id, s.id FROM it_empty_t t FULL JOIN "+empty+" s ON s.t_id = t.id ORDER BY t.id",
		"1|NULL", "2|NULL", "3|NULL", "4|NULL")
	h.expectRows("SELECT t.id, s.id FROM "+empty+" s FULL JOIN it_empty_t t ON s.t_id = t.id ORDER BY t.id",
		"1|NULL", "2|NULL", "3|NULL", "4|NULL")
	h.expectRows("SELECT t.id, u.id FROM it_empty_t t FULL JOIN it_empty_u u ON false ORDER BY t.id, u.id",
		"1|NULL", "2|NULL", "3|NULL", "4|NULL", "NULL|1", "NULL|2", "NULL|3", "NULL|4")
	// NATURAL FULL JOIN merges the columns with the empty side's NULLs.
	h.expectRows("SELECT id, g FROM it_empty_t NATURAL FULL JOIN (SELECT id, g FROM it_empty_t WHERE false) s ORDER BY id",
		"1|a", "2|a", "3|b", "4|c")
	h.expectRows("SELECT id, g, s.x FROM it_empty_t NATURAL LEFT JOIN (SELECT id, x FROM it_empty_t WHERE false) s ORDER BY id",
		"1|a|NULL", "2|a|NULL", "3|b|NULL", "4|c|NULL")
	h.expectRows("SELECT t.id, s.id FROM it_empty_t t LEFT JOIN LATERAL (SELECT * FROM it_empty_u WHERE it_empty_u.t_id = t.id) s ON false ORDER BY t.id",
		"1|NULL", "2|NULL", "3|NULL", "4|NULL")

	// Inner joins are empty.
	h.expectRows("SELECT t.id, u.id FROM it_empty_t t JOIN it_empty_u u ON false")
	h.expectRows("SELECT COUNT(*) FROM it_empty_t t JOIN it_empty_u u ON u.t_id = t.id AND false", "0")
	h.expectRows("SELECT COUNT(*) FROM it_empty_t t CROSS JOIN "+empty+" s", "0")
	h.expectRows("SELECT COUNT(*) FROM it_empty_t t, "+empty+" s WHERE s.t_id = t.id", "0")
	h.expectRows("SELECT COUNT(*) FROM it_empty_t t JOIN (SELECT * FROM it_empty_u LIMIT 0) s ON s.t_id = t.id", "0")
	h.expectRows("SELECT COUNT(*) FROM it_empty_v v JOIN it_empty_vagg a ON false", "0")
	h.expectRows("SELECT COUNT(*) FROM generate_series(1, 3) AS s(n) JOIN it_empty_t ON false", "0")

	// Several items: what the empty one decides, in written order.
	h.expectRows("SELECT t.id, u.id, w.id FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id JOIN it_empty_u w ON false")
	h.expectRows(`SELECT t.id, u.id, w.id FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id
		RIGHT JOIN it_empty_u w ON false ORDER BY w.id`,
		"NULL|NULL|1", "NULL|NULL|2", "NULL|NULL|3", "NULL|NULL|4")
	h.expectRows(`SELECT t.id, u.id, w.id FROM it_empty_t t JOIN it_empty_u u ON false
		RIGHT JOIN it_empty_u w ON w.t_id = t.id ORDER BY w.id`,
		"NULL|NULL|1", "NULL|NULL|2", "NULL|NULL|3", "NULL|NULL|4")
	h.expectRows(`SELECT t.id, u.id, w.id FROM it_empty_t t JOIN it_empty_u u ON false
		FULL JOIN it_empty_u w ON w.t_id = t.id ORDER BY w.id`,
		"NULL|NULL|1", "NULL|NULL|2", "NULL|NULL|3", "NULL|NULL|4")
	h.expectRows(`SELECT t.id, u.id, w.id FROM it_empty_t t JOIN it_empty_u u ON false
		LEFT JOIN it_empty_u w ON w.t_id = t.id`)
	h.expectRows(`SELECT t.id, u.id, w.name FROM it_empty_t t RIGHT JOIN it_empty_u u ON false
		JOIN it_empty_u w ON w.id = u.id ORDER BY u.id`,
		"NULL|1|one", "NULL|2|uno", "NULL|3|four", "NULL|4|nine")
	h.expectRows(`SELECT t.id, s.id, u.name FROM it_empty_t t LEFT JOIN `+empty+` s ON s.t_id = t.id
		JOIN it_empty_u u ON u.t_id = t.id ORDER BY t.id, u.name`,
		"1|NULL|one", "1|NULL|uno", "4|NULL|four")
	h.expectRows(`SELECT t.id, w.n FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id
		RIGHT JOIN generate_series(1, 2) AS w(n) ON false ORDER BY w.n`, "NULL|1", "NULL|2")
	// An item whose filter matches nothing (IN over an empty subquery).
	h.expectRows(`SELECT t.id, u.name FROM it_empty_t t LEFT JOIN it_empty_u u
		ON u.t_id = t.id AND u.name IN (SELECT name FROM it_empty_u WHERE false) ORDER BY t.id`,
		"1|NULL", "2|NULL", "3|NULL", "4|NULL")
	h.expectRows(`SELECT COUNT(*) FROM it_empty_t t JOIN it_empty_u u
		ON u.t_id = t.id AND u.name IN (SELECT name FROM it_empty_u WHERE false)`, "0")
	// An index lookup join whose keys are all NULL.
	h.expectRows(`SELECT s.id, u.id FROM (SELECT id, x FROM it_empty_t WHERE id = 3) s
		LEFT JOIN it_empty_u u ON u.id = s.x`, "3|NULL")

	// A WHERE that is never true reads no item.
	h.expectRows("SELECT COUNT(*) FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id WHERE false", "0")
	h.expectRows("SELECT t.g, COUNT(u.id) FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id WHERE 1 = 0 GROUP BY t.g")
}

func TestSQLEmptySetOpsAndSubqueries(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()

	// UNION ALL with an empty branch returns the other branch.
	h.expectRows("SELECT id FROM it_empty_t UNION ALL SELECT id FROM it_empty_u WHERE false ORDER BY 1", "1", "2", "3", "4")
	h.expectRows("SELECT id FROM it_empty_t WHERE false UNION ALL SELECT t_id FROM it_empty_u ORDER BY 1", "1", "1", "4", "9")
	h.expectRows("SELECT g FROM it_empty_t UNION SELECT name FROM it_empty_u WHERE false ORDER BY 1", "a", "b", "c")
	h.expectRows("SELECT id FROM it_empty_t INTERSECT SELECT id FROM it_empty_u WHERE false")
	h.expectRows("SELECT g FROM it_empty_t EXCEPT SELECT g FROM it_empty_t WHERE false ORDER BY 1", "a", "b", "c")
	h.expectRows("SELECT g FROM it_empty_t WHERE false EXCEPT SELECT g FROM it_empty_t")
	h.expectRows("SELECT id FROM it_empty_t UNION ALL SELECT id FROM it_empty_u LIMIT 0")
	h.expectRows("SELECT COUNT(*) FROM (SELECT id FROM it_empty_t WHERE false UNION ALL SELECT id FROM it_empty_u WHERE 1 = 0) s", "0")
	h.expectRows("(SELECT id FROM it_empty_t LIMIT 0) UNION ALL (SELECT id FROM it_empty_u ORDER BY id LIMIT 1)", "1")

	// EXISTS over no rows is false, IN false, NOT IN true, a scalar
	// subquery NULL.
	h.expectRows(`SELECT EXISTS (SELECT 1 FROM it_empty_t WHERE false), NOT EXISTS (SELECT 1 FROM it_empty_t WHERE false),
			1 IN (SELECT id FROM it_empty_t WHERE false), 1 NOT IN (SELECT id FROM it_empty_t WHERE false),
			NULL IN (SELECT id FROM it_empty_t WHERE false), (SELECT MAX(id) FROM it_empty_t WHERE false),
			(SELECT id FROM it_empty_t WHERE false), 1 = ANY (SELECT id FROM it_empty_t WHERE false),
			1 > ALL (SELECT id FROM it_empty_t WHERE false), EXISTS (SELECT 1 FROM it_empty_t LIMIT 0)`,
		"false|true|false|true|false|NULL|NULL|false|true|false")
	// Correlated, as semi-joins and per row.
	h.expectRows("SELECT id FROM it_empty_t WHERE EXISTS (SELECT 1 FROM it_empty_u WHERE it_empty_u.t_id = it_empty_t.id AND false)")
	h.expectRows("SELECT id FROM it_empty_t WHERE NOT EXISTS (SELECT 1 FROM it_empty_u WHERE it_empty_u.t_id = it_empty_t.id AND false) ORDER BY id",
		"1", "2", "3", "4")
	h.expectRows("SELECT id FROM it_empty_t WHERE id IN (SELECT t_id FROM it_empty_u WHERE false)")
	h.expectRows("SELECT id FROM it_empty_t WHERE id NOT IN (SELECT t_id FROM it_empty_u WHERE false) ORDER BY id",
		"1", "2", "3", "4")
	h.expectRows("SELECT id FROM it_empty_t WHERE x IN (SELECT t_id FROM it_empty_u WHERE 1 = 0) OR id = 3", "3")
	h.expectRows(`SELECT id, EXISTS (SELECT 1 FROM it_empty_u WHERE it_empty_u.t_id = it_empty_t.id AND false),
			(SELECT MAX(u.name) FROM it_empty_u u WHERE u.t_id = it_empty_t.id AND false),
			(SELECT COUNT(*) FROM it_empty_u u WHERE u.t_id = it_empty_t.id AND false)
		FROM it_empty_t ORDER BY id`,
		"1|false|NULL|0", "2|false|NULL|0", "3|false|NULL|0", "4|false|NULL|0")
	h.expectRows("SELECT id, (SELECT name FROM it_empty_u WHERE false) FROM it_empty_t ORDER BY id",
		"1|NULL", "2|NULL", "3|NULL", "4|NULL")
	// The subqueries of a WHERE that is never true are not run.
	h.expectRows("SELECT id FROM it_empty_t WHERE id IN (SELECT t_id FROM (SELECT * FROM it_empty_u) s) AND false")
	h.expectRows(`SELECT id FROM it_empty_t WHERE false AND (SELECT COUNT(*) FROM it_empty_u) > 0`)
}

func TestSQLEmptyLimit(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()

	// LIMIT 0 and FETCH FIRST 0 ROWS at any level.
	for _, q := range []string{
		"SELECT * FROM it_empty_t LIMIT 0",
		"SELECT * FROM it_empty_t ORDER BY f DESC LIMIT 0",
		"SELECT * FROM it_empty_t LIMIT 0 OFFSET 2",
		"SELECT * FROM it_empty_t FETCH FIRST 0 ROWS ONLY",
		"SELECT * FROM it_empty_t OFFSET 1 ROWS FETCH NEXT 0 ROWS ONLY",
		"SELECT * FROM (SELECT * FROM it_empty_t) s LIMIT 0",
		"SELECT * FROM (SELECT * FROM it_empty_t LIMIT 0) s",
		"SELECT * FROM (SELECT * FROM it_empty_t FETCH FIRST 0 ROWS ONLY) s",
		"SELECT s.* FROM it_empty_u u JOIN (SELECT * FROM it_empty_t LIMIT 0) s ON s.id = u.t_id",
		"WITH c AS (SELECT * FROM it_empty_t) SELECT * FROM c LIMIT 0",
		"WITH c AS (SELECT * FROM it_empty_t LIMIT 0) SELECT * FROM c",
	} {
		h.expectNoRows(q, emptyTSchema)
	}
	h.expectRows("SELECT COUNT(*) FROM it_empty_t LIMIT 0")
	h.expectRows("SELECT g, COUNT(*) FROM it_empty_t GROUP BY g LIMIT 0")
	h.expectRows("SELECT DISTINCT g FROM it_empty_t LIMIT 0")
	h.expectRows("SELECT id, ROW_NUMBER() OVER () FROM it_empty_t LIMIT 0")
	h.expectRows("SELECT g, COUNT(*) FROM it_empty_t GROUP BY ROLLUP (g) LIMIT 0")
	h.expectRows("SELECT 1 LIMIT 0")
	h.expectRows("SELECT (SELECT COUNT(*) FROM it_empty_t) LIMIT 0")
	h.expectRows("SELECT COUNT(*) FROM (SELECT * FROM it_empty_t LIMIT 0) s", "0")
	h.expectRows("SELECT 1 / 0 FROM it_empty_t LIMIT 0")
	h.expectRows("SELECT * FROM it_empty_t ORDER BY 1 / x LIMIT 0")

	// HAVING and QUALIFY that are never true.
	h.expectRows("SELECT g, COUNT(*) FROM it_empty_t GROUP BY g HAVING false")
	h.expectRows("SELECT COUNT(*) FROM it_empty_t HAVING COUNT(*) > 0 AND 1 = 0")
	h.expectRows("SELECT g, COUNT(*) FROM it_empty_t GROUP BY ROLLUP (g) HAVING false")
	h.expectRows("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM it_empty_t QUALIFY false")
	h.expectRows("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM it_empty_t QUALIFY rn = 1 AND NULL")
	h.expectRows("SELECT g, COUNT(*) AS n FROM it_empty_t GROUP BY g HAVING COUNT(*) > 1 OR false", "a|2")

	// FETCH FIRST / NEXT, with the count left out meaning 1.
	h.expectRows("SELECT id FROM it_empty_t ORDER BY id FETCH FIRST 2 ROWS ONLY", "1", "2")
	h.expectRows("SELECT id FROM it_empty_t ORDER BY id FETCH FIRST ROW ONLY", "1")
	h.expectRows("SELECT id FROM it_empty_t ORDER BY id OFFSET 1 ROW FETCH NEXT 2 ROWS ONLY", "2", "3")
	h.expectRows("SELECT id FROM it_empty_t ORDER BY id DESC FETCH NEXT 1 ROW ONLY", "4")
	h.expectRows("SELECT x AS fetch FROM it_empty_t ORDER BY id FETCH FIRST 1 ROW ONLY", "10")
	h.expectError("SELECT id FROM it_empty_t FETCH FIRST 1 ROWS WITH TIES", "FETCH … WITH TIES is not supported")
	h.expectError("SELECT id FROM it_empty_t FETCH 1 ROWS ONLY", "expected FIRST or NEXT after FETCH")
	h.expectError("SELECT id FROM it_empty_t FETCH FIRST 1", "expected ROW or ROWS")
	h.expectError("SELECT id FROM it_empty_t FETCH FIRST 1 ROWS", "expected ONLY")
	h.expectError("SELECT id FROM it_empty_t LIMIT 1 FETCH FIRST 1 ROWS ONLY", "multiple LIMIT clauses not allowed")
	h.expectError("SELECT id FROM it_empty_t FETCH FIRST 1 ROWS ONLY LIMIT 1", "multiple LIMIT clauses not allowed")
	h.expectError("SELECT id FROM it_empty_t LIMIT 1 LIMIT 2", "multiple LIMIT clauses not allowed")
}

func TestSQLEmptyWrites(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()
	h.dropTables("it_empty_w", "it_empty_ctas", "it_empty_ctas_full")
	h.exec("CREATE TABLE it_empty_w (id INTEGER, name VARCHAR)")
	h.exec("INSERT INTO it_empty_w VALUES (1, 'one'), (2, 'two')")
	rows := []string{"1|one", "2|two"}

	h.expectAffected("INSERT INTO it_empty_w SELECT id, g FROM it_empty_t WHERE false", 0)
	h.expectAffected("INSERT INTO it_empty_w SELECT id, g FROM it_empty_t LIMIT 0", 0)
	h.expectAffected("INSERT INTO it_empty_w SELECT * FROM (SELECT id, g FROM it_empty_t) s WHERE 1 = 0", 0)
	h.expectAffected("UPDATE it_empty_w SET name = 'x' WHERE false", 0)
	h.expectAffected("UPDATE it_empty_w SET name = (SELECT MAX(g) FROM (SELECT g FROM it_empty_t) s) WHERE id > 0 AND false", 0)
	h.expectAffected("DELETE FROM it_empty_w WHERE false", 0)
	h.expectAffected("DELETE FROM it_empty_w WHERE id IN (SELECT id FROM it_empty_t WHERE false)", 0)
	h.expectAffected("UPDATE it_empty_w w SET name = t.g FROM it_empty_t t WHERE t.id = w.id AND false", 0)
	h.expectAffected("UPDATE it_empty_w w SET name = t.g FROM (SELECT * FROM it_empty_t WHERE false) t WHERE t.id = w.id", 0)
	h.expectAffected("DELETE FROM it_empty_w w USING it_empty_t t WHERE t.id = w.id AND false", 0)
	h.expectAffected(`MERGE INTO it_empty_w w USING (SELECT id, g FROM it_empty_t WHERE false) s ON s.id = w.id
		WHEN MATCHED THEN UPDATE SET name = s.g WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.g)`, 0)
	h.expectAffected(`MERGE INTO it_empty_w w USING it_empty_t s ON false
		WHEN MATCHED THEN UPDATE SET name = s.g`, 0)
	h.expectReturning("UPDATE it_empty_w SET name = 'x' WHERE false RETURNING id, name", "id|name")
	h.expectReturning("DELETE FROM it_empty_w WHERE 1 = 0 RETURNING *", "id|name")
	h.expectRows("SELECT * FROM it_empty_w ORDER BY id", rows...)

	// An empty source still deletes the target's rows WHEN NOT MATCHED BY
	// SOURCE.
	h.expectAffected(`MERGE INTO it_empty_w w USING (SELECT id FROM it_empty_t WHERE false) s ON s.id = w.id
		WHEN NOT MATCHED BY SOURCE AND w.id = 2 THEN DELETE`, 1)
	h.expectRows("SELECT * FROM it_empty_w ORDER BY id", "1|one")

	// CREATE TABLE … AS over no rows has the full query's columns.
	full := "SELECT id, g, amount * 2 AS twice, CAST(x AS BIGINT) AS big, ts, NULL AS nothing FROM it_empty_t"
	h.expectAffected("CREATE TABLE it_empty_ctas_full AS "+full, 4)
	h.expectAffected("CREATE TABLE it_empty_ctas AS "+full+" WHERE false LIMIT 0", 0)
	h.expectRows("SELECT COUNT(*) FROM it_empty_ctas", "0")
	want := "id int32, g utf8, twice decimal(38, 2), big int64, ts timestamp[us], nothing utf8"
	for _, table := range []string{"it_empty_ctas_full", "it_empty_ctas"} {
		schema, err := h.conn.GetTableSchema(h.ctx, nil, nil, table)
		if err != nil {
			t.Fatal(err)
		}
		if got := schemaText(schema); got != want {
			t.Errorf("GetTableSchema(%s) = %s, want %s", table, got, want)
		}
	}
	h.exec("CREATE TEMPORARY TABLE it_empty_ctas AS SELECT * FROM (" + full + ") s WHERE false LIMIT 0")
	h.expectNoRows("SELECT * FROM it_empty_ctas", want)
	h.exec("DROP TABLE it_empty_ctas") // the temporary one
}

// Rows that are written still pass NOT NULL and CHECK when part of the
// statement's input can't return rows.
func TestSQLEmptyKeepsChecks(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()
	h.dropTables("it_empty_chk")
	h.exec("CREATE TABLE it_empty_chk (id INTEGER NOT NULL, v INTEGER CHECK (v > 0))")
	h.exec("INSERT INTO it_empty_chk VALUES (1, 1)")

	// Nothing is written, so nothing is checked.
	h.expectAffected("INSERT INTO it_empty_chk SELECT NULL, -1 FROM it_empty_t WHERE false", 0)
	h.expectAffected("INSERT INTO it_empty_chk SELECT NULL, -1 FROM it_empty_t LIMIT 0", 0)
	h.expectAffected("UPDATE it_empty_chk SET id = NULL, v = -1 WHERE false", 0)
	h.expectAffected(`MERGE INTO it_empty_chk c USING (SELECT id FROM it_empty_t WHERE false) s ON s.id = c.id
		WHEN MATCHED THEN UPDATE SET v = -1 WHEN NOT MATCHED THEN INSERT VALUES (NULL, -1)`, 0)

	// Rows written next to an input that has none are checked.
	const check = `new row for relation "it_empty_chk" violates check constraint "it_empty_chk_v_check"`
	const notNull = `NULL value in column "id" violates not-null constraint`
	h.expectError(`INSERT INTO it_empty_chk SELECT 2, -1 FROM it_empty_t WHERE id = 1
		UNION ALL SELECT 3, 1 FROM it_empty_t WHERE false`, check)
	h.expectError("INSERT INTO it_empty_chk SELECT t.id + 1, -1 FROM it_empty_t t LEFT JOIN it_empty_u u ON false WHERE t.id = 1", check)
	h.expectError(`INSERT INTO it_empty_chk SELECT s.id, 1 FROM it_empty_t t
		LEFT JOIN (SELECT * FROM it_empty_u WHERE false) s ON s.t_id = t.id WHERE t.id = 1`, notNull)
	h.expectError("MERGE INTO it_empty_chk c USING (SELECT 5 AS id) s ON false WHEN NOT MATCHED THEN INSERT VALUES (s.id, -1)", check)
	h.expectError(`MERGE INTO it_empty_chk c USING (SELECT 1 AS id) s ON s.id = c.id AND 1 = 0
		WHEN NOT MATCHED THEN INSERT VALUES (NULL, 1)`, notNull)
	h.expectError("UPDATE it_empty_chk SET v = -1 WHERE id = 1 OR false", check)
	h.expectError("UPDATE it_empty_chk c SET v = -1 FROM it_empty_t t LEFT JOIN it_empty_u u ON false WHERE t.id = c.id", check)
	h.expectRows("SELECT * FROM it_empty_chk ORDER BY id", "1|1")
}

// emptyShapes are queries of many shapes whose empty forms must have their
// exact schemas.
var emptyShapes = []string{
	"SELECT * FROM it_empty_t",
	emptyModel,
	`SELECT t.id, t.g, u.name, COUNT(*) OVER (PARTITION BY t.g) AS per_g FROM it_empty_t t
		LEFT JOIN it_empty_u u ON u.t_id = t.id`,
	`SELECT t.g, COUNT(u.id) AS n, SUM(t.amount) AS total, AVG(t.f) AS avg_f FROM it_empty_t t
		JOIN it_empty_u u ON u.t_id = t.id GROUP BY t.g HAVING COUNT(*) > 0`,
	`SELECT id, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn, SUM(x) OVER (ORDER BY id) AS running,
		LAG(amount) OVER (ORDER BY id) AS prev, AVG(f) OVER () AS avg_f, RANK() OVER (ORDER BY g) AS rk,
		FIRST_VALUE(ts) OVER (ORDER BY id) AS first_ts, NTILE(2) OVER (ORDER BY id) AS half FROM it_empty_t`,
	"SELECT id, g FROM it_empty_t UNION ALL SELECT id, name FROM it_empty_u",
	"SELECT x FROM it_empty_t UNION SELECT t_id FROM it_empty_u",
	"SELECT amount FROM it_empty_t UNION ALL SELECT x FROM it_empty_t",
	"SELECT id FROM it_empty_t INTERSECT SELECT t_id FROM it_empty_u",
	"SELECT g FROM it_empty_t EXCEPT SELECT name FROM it_empty_u",
	`SELECT CASE WHEN x > 10 THEN 'big' WHEN x IS NULL THEN NULL ELSE 'small' END AS size,
		CASE g WHEN 'a' THEN 1 ELSE amount END AS mixed, CASE WHEN flag THEN f END AS maybe FROM it_empty_t`,
	`SELECT CAST(id AS BIGINT) AS b, CAST(x AS VARCHAR) AS s, CAST(amount AS DOUBLE) AS dd,
		CAST('2024-01-01' AS DATE) AS dt, CAST(f AS NUMERIC(12,3)) AS n, x::text AS t2, TRY_CAST(g AS INTEGER) AS gi,
		CAST(ts AS DATE) AS day FROM it_empty_t`,
	`SELECT ROUND(amount, 1) AS r1, ROUND(f, 2) AS r2, ROUND(amount) AS r0, amount * 2 AS twice, amount / 3 AS third,
		amount + x AS plus, x * 1.5 AS scaled, ABS(x - 30) AS dist, x % 3 AS m FROM it_empty_t`,
	`SELECT UPPER(g) AS u, LOWER(g) AS l, LENGTH(g) AS n, SUBSTRING(g FROM 1 FOR 1) AS s1,
		g || '-' || CAST(id AS VARCHAR) AS label, TRIM(g) AS tr, REPLACE(g, 'a', 'b') AS rp, CONCAT(g, x) AS c,
		g LIKE 'a%' AS starts FROM it_empty_t`,
	`SELECT DATE_TRUNC('month', ts) AS m, EXTRACT(YEAR FROM d) AS y, d + 1 AS next_day,
		ts + INTERVAL '1 day' AS later, DATE_PART('dow', ts) AS dow, ts - TIMESTAMP '2024-01-01 00:00:00' AS since,
		CURRENT_DATE AS today FROM it_empty_t`,
	"SELECT COALESCE(x, 0) AS cx, COALESCE(amount, x) AS ca, COALESCE(f, amount, 0) AS cf, COALESCE(g, 'none') AS cg, NULLIF(g, 'a') AS ng FROM it_empty_t",
	`SELECT g, COUNT(*) AS n, COUNT(x) AS nx, SUM(x) AS sx, AVG(x) AS ax, MIN(amount) AS mn, MAX(ts) AS mx,
		SUM(amount) AS sa, AVG(amount) AS aa, STRING_AGG(CAST(id AS VARCHAR), ',') AS ids, BOOL_OR(flag) AS any_flag,
		STDDEV(f) AS sd, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f) AS med FROM it_empty_t GROUP BY g`,
	"SELECT COUNT(*) AS n, SUM(amount) AS total, MAX(g) AS last_g FROM it_empty_t",
	"SELECT g, x, COUNT(*) AS n, GROUPING(g, x) AS gr FROM it_empty_t GROUP BY ROLLUP (g, x)",
	"SELECT DISTINCT g, flag FROM it_empty_t",
	"SELECT * FROM it_empty_v",
	"SELECT * FROM it_empty_vagg",
	"SELECT v.id, v.x2, a.n, a.s FROM it_empty_v v JOIN it_empty_vagg a ON a.g = v.g",
	"SELECT NULL AS nothing, id, TRUE AS yes FROM it_empty_t",
	`SELECT id, (SELECT MAX(name) FROM it_empty_u u WHERE u.t_id = t.id) AS name,
		EXISTS (SELECT 1 FROM it_empty_u u WHERE u.t_id = t.id) AS has, x IN (SELECT t_id FROM it_empty_u) AS xin FROM it_empty_t t`,
	"SELECT g.n, s.id FROM generate_series(1, 2) AS g(n), LATERAL (SELECT id FROM it_empty_t t WHERE t.id = g.n) s",
	"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n, n * 1.5 AS m FROM r",
	"SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_name = 'it_empty_t'",
	"SELECT * FROM it_empty_tmp",
	"SELECT * FROM it_empty_tmpv",
	"SELECT 1 AS one, 'x' AS s, 1.5 AS d, DATE '2024-01-01' AS dt",
}

func TestSQLEmptySchema(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()
	h.dropTables("it_empty_full", "it_empty_probe")
	h.exec("CREATE TEMPORARY TABLE it_empty_tmp AS SELECT id, g, amount * x AS ax FROM it_empty_t")
	h.exec("CREATE TEMPORARY VIEW it_empty_tmpv AS SELECT g, SUM(ax) AS s FROM it_empty_tmp GROUP BY g")

	for _, q := range emptyShapes {
		full, fs := h.query(q)
		want := schemaText(fs)
		if len(full) == 0 {
			t.Errorf("%s: the full query returned no rows", q)
		}
		for _, form := range []string{
			dbtEmpty(q),
			"select * from (\n        " + q + "\n    ) as __dbt_sbq\n    where false\n    limit 0\n",
			"SELECT * FROM (" + q + ") AS s WHERE false",
			"SELECT * FROM (" + q + ") AS s WHERE 1 = 0",
			"SELECT * FROM (" + q + ") AS s LIMIT 0",
			"SELECT * FROM (" + q + ") AS s FETCH FIRST 0 ROWS ONLY",
		} {
			h.expectNoRows(form, want)
		}
		for _, sql := range []string{q, dbtEmpty(q)} {
			schema, err := h.executeSchema(sql)
			if err != nil {
				t.Errorf("ExecuteSchema(%s): %v", sql, err)
			} else if got := schemaText(schema); got != want {
				t.Errorf("ExecuteSchema(%s)\n schema: %s\n   want: %s", sql, got, want)
			}
		}
		// A table created from the empty form has the full query's columns.
		h.exec("CREATE TABLE it_empty_full AS " + q)
		h.exec("CREATE TEMPORARY TABLE it_empty_probe AS (" + dbtEmpty(q) + ")")
		fullTable, err := h.conn.GetTableSchema(h.ctx, nil, nil, "it_empty_full")
		if err != nil {
			t.Fatal(err)
		}
		probe, err := h.conn.GetTableSchema(h.ctx, nil, nil, "it_empty_probe")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := schemaText(probe), schemaText(fullTable); got != want {
			t.Errorf("CREATE TABLE AS (%s)\n columns: %s\n    want: %s", q, got, want)
		}
		h.expectRows("SELECT COUNT(*) FROM it_empty_probe", "0")
		h.exec("DROP TABLE it_empty_full")
		h.exec("DROP TABLE it_empty_probe")
	}
	h.exec("DROP VIEW it_empty_tmpv")
	h.exec("DROP TABLE it_empty_tmp")
}

// The forms dbt writes, as it writes them.
func TestSQLEmptyDbt(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()
	modelSchema := "k utf8, n int64, total decimal(38, 2), m int64"
	h.expectColumns(emptyModel, "k|n|total|m", "a|2|3.75|2", "b|1|NULL|0", "c|1|4.00|1")

	// get_columns_in_query (contracts, snapshots), indented both ways.
	h.expectNoRows(dbtEmpty(emptyModel), modelSchema)
	h.expectNoRows("select * from (\n        "+emptyModel+"\n    ) as __dbt_sbq\n    where false\n    limit 0\n", modelSchema)
	h.expectNoRows("select * from (\n  select * from \"redis\".\"public\".\"it_empty_t\"\n) as __dbt_sbq\nwhere false\nlimit 0", emptyTSchema)

	// dbt run --empty: every ref is limited, with or without AS, quoted or
	// not; the model's other parts still run (COUNT gives 0).
	h.expectNoRows(`select _dbt_limit_subq_it_empty_t.id, g from (select * from it_empty_t where false limit 0) as _dbt_limit_subq_it_empty_t`,
		"id int32, g utf8")
	h.expectRows(`select id from (select * from it_empty_t where false limit 0) as _dbt_limit_subq_it_empty_t`)
	h.expectRows(`select count(*) from (select * from "redis"."public"."it_empty_t" where false limit 0) _dbt_limit_subq_it_empty_t`, "0")
	limited := strings.NewReplacer(
		"from it_empty_t t", "from (select * from public.it_empty_t where false limit 0) t",
		"join it_empty_u u", "join (select * from it_empty_u where false limit 0) u",
		"from it_empty_t\n", "from (select * from \"redis\".\"public\".\"it_empty_t\" where false limit 0) _dbt_limit_subq_it_empty_t\n",
	).Replace(emptyModel)
	if strings.Count(limited, "limit 0") != 3 {
		t.Fatalf("not every ref is limited:\n%s", limited)
	}
	h.expectNoRows(limited, modelSchema)
	h.expectNoRows(dbtEmpty(limited), modelSchema)

	// Unit tests: a temporary table from the empty subquery, dbt-postgres's
	// create_table_as.
	h.exec("create temporary table \"it_empty_ut__dbt_tmp\"\n    \n    \n    as (\n    " +
		"select * from (\n        " + emptyModel + "\n    ) as __dbt_sbq\n    where false\n    limit 0\n  );")
	h.expectNoRows(`select * from "it_empty_ut__dbt_tmp"`, modelSchema)
	h.exec(`drop table if exists "it_empty_ut__dbt_tmp" cascade`)
	// A fixture written as rows.
	fixture := "select cast(1 as integer) as id, cast('a' as text) as g\nunion all\nselect cast(2 as integer) as id, cast(null as text) as g"
	h.exec("create temporary table it_empty_fixture as (\n" + dbtEmpty(fixture) + "\n)")
	h.expectNoRows("select * from it_empty_fixture", "id int32, g utf8")
	h.exec("drop table it_empty_fixture")

	// ExecuteSchema (adbc_execute_schema) of each.
	for _, sql := range []string{emptyModel, dbtEmpty(emptyModel), limited} {
		schema, err := h.executeSchema(sql)
		if err != nil || schemaText(schema) != modelSchema {
			t.Errorf("ExecuteSchema(%s) = %v, %v", sql, schema, err)
		}
	}
}

func TestSQLEmptyErrors(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()
	for _, c := range []struct{ sql, err string }{
		{"SELECT nope FROM it_empty_t WHERE false", `column "nope" does not exist`},
		{"SELECT * FROM it_empty_t WHERE nope = 1 AND false", `column "nope" does not exist`},
		{"SELECT * FROM it_empty_nope WHERE false LIMIT 0", "does not exist"},
		{dbtEmpty("SELECT nope FROM it_empty_t"), `column "nope" does not exist`},
		{dbtEmpty("SELECT * FROM it_empty_nope"), "does not exist"},
		{"SELECT * FROM (SELECT * FROM it_empty_nope) s WHERE false", "does not exist"},
		{"WITH c AS (SELECT nope FROM it_empty_t) SELECT * FROM c LIMIT 0", `column "nope" does not exist`},
		{"SELECT * FROM it_empty_t t JOIN it_empty_nope n ON false", "does not exist"},
		{"SELECT * FROM it_empty_t t LEFT JOIN (SELECT nope FROM it_empty_u) s ON false", `column "nope" does not exist`},
		{"SELECT * FROM it_empty_t t JOIN it_empty_u u ON u.nope = t.id AND false", `column "u.nope" does not exist`},
		{"SELECT * FROM it_empty_t WHERE id IN (SELECT nope FROM it_empty_u) AND false", `column "nope" does not exist`},
		{"SELECT id FROM it_empty_t WHERE false UNION SELECT id, name FROM it_empty_u", "must have the same number of columns"},
		{"SELECT id FROM it_empty_t UNION SELECT d FROM it_empty_t LIMIT 0", "cannot be matched"},
		{"SELECT g, COUNT(*) FROM it_empty_t WHERE false GROUP BY 3", "GROUP BY position 3 is out of range"},
		{"SELECT COUNT(*) FROM it_empty_t WHERE COUNT(*) > 0 AND false", "aggregates are not allowed in WHERE"},
		{"SELECT REPLACE(g, 'a') FROM it_empty_t WHERE false", "REPLACE expects 3 argument(s)"},
		{"SELECT g + 1 FROM it_empty_t LIMIT 0", "operator"},
		{"SELECT * FROM it_empty_v WHERE nope AND false", `column "nope" does not exist`},
		{"SELECT COUNT(*) LIMIT 0", "aggregates require a FROM clause"},
		{"SELECT COUNT(*) WHERE false", "aggregates require a FROM clause"},
	} {
		h.expectError(c.sql, c.err)
		if !strings.Contains(c.err, "aggregates require") {
			h.expectSchemaError(c.sql, c.err)
		}
	}
	for _, c := range []struct{ sql, err string }{
		{"UPDATE it_empty_u SET nope = 1 WHERE false", `column "nope" does not exist`},
		{"UPDATE it_empty_u SET name = nope WHERE false", `column "nope" does not exist`},
		{"DELETE FROM it_empty_u WHERE nope = 1 AND false", `column "nope" does not exist`},
		{"INSERT INTO it_empty_u SELECT nope FROM it_empty_t WHERE false", `column "nope" does not exist`},
		{"INSERT INTO it_empty_u SELECT id FROM it_empty_t WHERE false", "INSERT has 3 target columns but the query returns 1"},
		{"UPDATE it_empty_u u SET name = t.nope FROM it_empty_t t WHERE false", `column "t.nope" does not exist`},
		{"CREATE TABLE it_empty_bad AS SELECT nope FROM it_empty_t WHERE false LIMIT 0", `column "nope" does not exist`},
	} {
		h.expectError(c.sql, c.err)
	}
	h.expectRows("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'it_empty_bad'", "0")
}

// readCommands are the commands that read a table's rows: index queries,
// their cursors, and row fetches.
var readCommands = []string{"ft.aggregate", "ft.search", "ft.cursor", "ft.profile", "hmget", "scan"}

// rowReads resets the command statistics of every node, runs fn, and
// returns the calls of readCommands since then (with a cluster's
// shard-internal _FT.* commands).
func (h *sqlHarness) rowReads(c goredis.UniversalClient, fn func()) map[string]int64 {
	h.t.Helper()
	each := func(do func(ctx context.Context, n *goredis.Client) error) {
		var err error
		if cc, ok := c.(*goredis.ClusterClient); ok {
			err = cc.ForEachShard(h.ctx, do)
		} else {
			err = do(h.ctx, c.(*goredis.Client))
		}
		if err != nil {
			h.t.Fatal(err)
		}
	}
	each(func(ctx context.Context, n *goredis.Client) error { return n.ConfigResetStat(ctx).Err() })
	fn()
	var mu sync.Mutex
	got := map[string]int64{}
	each(func(ctx context.Context, n *goredis.Client) error {
		info, err := n.Info(ctx, "commandstats").Result()
		if err != nil {
			return err
		}
		for _, line := range strings.Split(info, "\n") {
			name, stats, ok := strings.Cut(strings.TrimSpace(line), ":")
			if !ok || !strings.HasPrefix(name, "cmdstat_") {
				continue
			}
			name = strings.ToLower(strings.TrimLeft(strings.TrimPrefix(name, "cmdstat_"), "_"))
			calls, _, _ := strings.Cut(strings.TrimPrefix(stats, "calls="), ",")
			n, _ := strconv.ParseInt(calls, 10, 64)
			for _, r := range readCommands {
				if name == r && n > 0 {
					mu.Lock()
					got[name] += n
					mu.Unlock()
				}
			}
		}
		return nil
	})
	return got
}

// expectNoReads checks that fn issues none of readCommands. The server's
// statistics also count other connections' background work (tag checks,
// cleanups), so it tries three times; a statement that reads rows does so
// every time.
func (h *sqlHarness) expectNoReads(c goredis.UniversalClient, what string, fn func()) {
	h.t.Helper()
	var got map[string]int64
	for try := 0; try < 3; try++ {
		if got = h.rowReads(c, fn); len(got) == 0 {
			return
		}
	}
	h.t.Errorf("%s read rows: %v", what, got)
}

func TestSQLEmptyReadsNothing(t *testing.T) {
	h := newSQLHarness(t)
	h.setupEmpty()
	h.dropTables("it_empty_w", "it_empty_rv_t")
	h.exec("CREATE TABLE it_empty_w (id INTEGER, name VARCHAR)")
	h.exec("INSERT INTO it_empty_w VALUES (1, 'one')")
	h.exec("DROP VIEW IF EXISTS it_empty_rv")
	t.Cleanup(func() { h.exec("DROP VIEW IF EXISTS it_empty_rv") })
	c := h.rawClient()

	// The check sees a query that reads.
	if got := h.rowReads(c, func() { h.query("SELECT * FROM it_empty_t") }); got["ft.aggregate"] == 0 {
		t.Fatalf("a full scan read %v, want FT.AGGREGATE", got)
	}
	if got := h.rowReads(c, func() { h.query("SELECT * FROM it_empty_t WHERE __rowid = 1") }); got["hmget"] == 0 {
		t.Fatalf("a row id lookup read %v, want HMGET", got)
	}

	empty := "(SELECT * FROM it_empty_u WHERE false)"
	queries := []string{
		dbtEmpty(emptyModel),
		dbtEmpty("SELECT * FROM it_empty_t"),
		"SELECT * FROM (" + emptyModel + ") s WHERE false",
		"SELECT * FROM (" + emptyModel + ") s LIMIT 0",
		emptyModel + " LIMIT 0",
		"SELECT * FROM it_empty_t WHERE false",
		"SELECT * FROM it_empty_t WHERE false LIMIT 0",
		"SELECT * FROM it_empty_t WHERE 1 = 0",
		"SELECT * FROM it_empty_t WHERE NULL",
		"SELECT * FROM it_empty_t WHERE x > 1 AND false",
		"SELECT * FROM it_empty_t WHERE NOT true",
		"SELECT * FROM it_empty_t WHERE __rowid = 1 AND false",
		"SELECT * FROM it_empty_t LIMIT 0",
		"SELECT * FROM it_empty_t ORDER BY f LIMIT 0",
		"SELECT * FROM it_empty_t FETCH FIRST 0 ROWS ONLY",
		"SELECT COUNT(*), SUM(x) FROM it_empty_t WHERE false",
		"SELECT g, COUNT(*) FROM it_empty_t WHERE false GROUP BY g",
		"SELECT g, COUNT(*) FROM it_empty_t WHERE false GROUP BY ROLLUP (g)",
		"SELECT g, COUNT(*) FROM it_empty_t GROUP BY g HAVING false",
		"SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM it_empty_t WHERE false",
		"SELECT DISTINCT g FROM it_empty_t WHERE false",
		"SELECT id FROM (SELECT * FROM it_empty_t WHERE false LIMIT 0) AS _dbt_limit_subq_it_empty_t",
		`SELECT t.g, COUNT(u.id) FROM (SELECT * FROM it_empty_t WHERE false LIMIT 0) t
			LEFT JOIN (SELECT * FROM it_empty_u WHERE false LIMIT 0) u ON u.t_id = t.id GROUP BY t.g`,
		"SELECT * FROM it_empty_t t JOIN it_empty_u u ON false",
		"SELECT * FROM it_empty_t t JOIN " + empty + " s ON s.t_id = t.id",
		"SELECT * FROM " + empty + " s LEFT JOIN it_empty_t t ON s.t_id = t.id",
		"SELECT * FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id JOIN it_empty_u w ON false",
		"SELECT * FROM it_empty_t t JOIN (SELECT * FROM it_empty_u LIMIT 0) s ON s.t_id = t.id",
		"SELECT COUNT(*) FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id WHERE false",
		"SELECT id FROM it_empty_t WHERE false UNION ALL SELECT id FROM it_empty_u WHERE 1 = 0",
		"SELECT id FROM it_empty_t UNION SELECT id FROM it_empty_u LIMIT 0",
		"SELECT EXISTS (SELECT 1 FROM it_empty_t WHERE false), 1 IN (SELECT id FROM it_empty_t WHERE false)",
		"SELECT id FROM it_empty_t WHERE id IN (SELECT t_id FROM it_empty_u WHERE false)",
		"SELECT id FROM it_empty_t WHERE EXISTS (SELECT 1 FROM it_empty_u WHERE it_empty_u.t_id = it_empty_t.id AND false)",
		"SELECT id FROM it_empty_t WHERE id IN (SELECT t_id FROM (SELECT * FROM it_empty_u) s) AND false",
		"WITH c AS (SELECT * FROM it_empty_t) SELECT * FROM c WHERE false",
		"WITH c AS (SELECT * FROM it_empty_t WHERE false) SELECT COUNT(*) FROM c",
		"SELECT * FROM it_empty_v WHERE false",
		"SELECT * FROM it_empty_vagg WHERE false",
		"SELECT * FROM it_empty_v v JOIN it_empty_vagg a ON false",
		"WITH RECURSIVE r(n) AS (SELECT id FROM it_empty_t WHERE false UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
		"SELECT (SELECT COUNT(*) FROM it_empty_t) LIMIT 0",
		`SELECT t.id, w.n FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id
			RIGHT JOIN generate_series(1, 2) AS w(n) ON false`,
	}
	for _, q := range queries {
		h.expectNoReads(c, q, func() { h.query(q) })
		h.expectNoReads(c, "ExecuteSchema("+q+")", func() {
			if _, err := h.executeSchema(q); err != nil {
				t.Fatal(err)
			}
		})
	}
	// Joins that need some of their items read only those: the same commands
	// as a join that reads it_empty_t alone.
	onlyT := func() { h.query("SELECT * FROM it_empty_t t CROSS JOIN generate_series(1, 1) AS g(n)") }
	for _, q := range []string{
		"SELECT * FROM it_empty_t t LEFT JOIN it_empty_u u ON false",
		"SELECT * FROM it_empty_t t LEFT JOIN (SELECT * FROM it_empty_u) s ON false",
		"SELECT * FROM it_empty_t t LEFT JOIN it_empty_vagg a ON false",
		"SELECT * FROM it_empty_t t LEFT JOIN it_empty_u u ON u.t_id = t.id AND u.name IN (SELECT name FROM it_empty_u WHERE false)",
		"SELECT * FROM it_empty_u u RIGHT JOIN it_empty_t t ON false",
		"SELECT * FROM it_empty_u u LEFT JOIN it_empty_u w ON w.id = u.id RIGHT JOIN it_empty_t t ON false",
		"SELECT * FROM it_empty_u u JOIN it_empty_u w ON false RIGHT JOIN it_empty_t t ON t.id = u.t_id",
		"SELECT * FROM it_empty_t t FULL JOIN (SELECT * FROM it_empty_u WHERE false) s ON s.t_id = t.id",
	} {
		var got, want map[string]int64
		for try := 0; try < 3; try++ {
			if want, got = h.rowReads(c, onlyT), h.rowReads(c, func() { h.query(q) }); maps.Equal(got, want) {
				break
			}
		}
		if !maps.Equal(got, want) {
			t.Errorf("%s read %v, want %v (it_empty_t alone)", q, got, want)
		}
	}

	// ExecuteSchema reads nothing, whatever the query.
	for _, q := range []string{emptyModel, "SELECT * FROM it_empty_t", "SELECT * FROM (SELECT g, COUNT(*) FROM it_empty_t GROUP BY g) s",
		"WITH c AS (SELECT * FROM it_empty_t) SELECT * FROM c, LATERAL (SELECT * FROM it_empty_u WHERE t_id = c.id) l",
		"SELECT * FROM it_empty_vagg", "SELECT * FROM information_schema.columns"} {
		h.expectNoReads(c, "ExecuteSchema("+q+")", func() {
			if _, err := h.executeSchema(q); err != nil {
				t.Fatal(err)
			}
		})
	}
	// Writes.
	for _, sql := range []string{
		"INSERT INTO it_empty_w SELECT id, g FROM it_empty_t WHERE false",
		"INSERT INTO it_empty_w " + dbtEmpty("SELECT id, g FROM it_empty_t"),
		"UPDATE it_empty_w SET name = 'x' WHERE false",
		"UPDATE it_empty_w SET name = (SELECT MAX(g) FROM (SELECT g FROM it_empty_t) s) WHERE false",
		"DELETE FROM it_empty_w WHERE 1 = 0",
		"UPDATE it_empty_w w SET name = t.g FROM (SELECT * FROM it_empty_t) t WHERE t.id = w.id AND false",
		"DELETE FROM it_empty_w w USING it_empty_t t WHERE t.id = w.id AND false",
		`MERGE INTO it_empty_w w USING (SELECT id, g FROM it_empty_t WHERE false) s ON s.id = w.id
			WHEN MATCHED THEN UPDATE SET name = s.g WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.g)`,
		"CREATE TABLE it_empty_rv_t AS (" + dbtEmpty(emptyModel) + ")",
		"CREATE TEMPORARY TABLE it_empty_rv_t AS (" + dbtEmpty(emptyModel) + ")",
		"CREATE VIEW it_empty_rv AS " + emptyModel,
	} {
		h.expectNoReads(c, sql, func() {
			h.exec(sql)
			switch {
			case strings.HasPrefix(sql, "CREATE TABLE"), strings.HasPrefix(sql, "CREATE TEMPORARY"):
				h.exec("DROP TABLE it_empty_rv_t")
			case strings.HasPrefix(sql, "CREATE VIEW"):
				h.exec("DROP VIEW it_empty_rv")
			}
		})
	}
	h.expectRows("SELECT * FROM it_empty_w", "1|one")
}
