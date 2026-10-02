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

// Subqueries of grouped queries (#112). As in Postgres, where a query with
// GROUP BY, aggregates or HAVING computes a value per group, a subquery may
// read only the query's grouped columns: in `SELECT v FROM g GROUP BY v
// HAVING EXISTS (SELECT 1 FROM g x WHERE x.id > g.id)`, g.id would be one
// row of each group. The results and errors expected are Postgres 16's.

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// setupGroupedSubqueries creates it_gq.g, the issue's table.
func (h *sqlHarness) setupGroupedSubqueries() {
	h.t.Helper()
	h.exec(`DROP SCHEMA IF EXISTS it_gq CASCADE`)
	h.exec(`CREATE SCHEMA it_gq`)
	h.t.Cleanup(func() { h.exec(`DROP SCHEMA IF EXISTS it_gq CASCADE`) })
	h.exec(`CREATE TABLE it_gq.g (id INTEGER, v INTEGER)`)
	h.exec(`INSERT INTO it_gq.g VALUES (1, 10), (2, 10), (3, 20)`)
}

// expectQueryError runs a query and checks its error's status and message.
// Unlike expectError, it goes on after a mismatch.
func (h *sqlHarness) expectQueryError(sql string, code adbc.Status, msg string) {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	var rdr array.RecordReader
	if rdr, _, err = st.ExecuteQuery(h.ctx); err == nil {
		rdr.Release()
		h.t.Errorf("%s: expected the error %q", sql, msg)
		return
	}
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != code || ae.Msg != "[redis] "+msg {
		h.t.Errorf("%s:\n got: %v\nwant: %q (%v)", sql, err, msg, code)
	}
}

// expectUngrouped checks that a query fails with Postgres's error for a
// subquery that reads the ungrouped column col.
func (h *sqlHarness) expectUngrouped(sql, col string) {
	h.t.Helper()
	h.expectQueryError(sql, adbc.StatusInvalidArgument, fmt.Sprintf("subquery uses ungrouped column %q from outer query", col))
}

// The issue's queries.
func TestSQLGroupedSubqueryIssue(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupedSubqueries()
	h.expectUngrouped(`select v from it_gq.g group by v having exists (select 1 from it_gq.g x where x.id > g.id)`, "g.id")
	h.expectUngrouped(`select v, (select max(x.id) from it_gq.g x where x.id <> g.id) from it_gq.g group by v`, "g.id")
	// A grouped column may be read.
	h.expectRows(`select v, (select max(x.id) from it_gq.g x where x.v = g.v) from it_gq.g group by v order by v`,
		"10|2", "20|3")
	// A view is checked when it is created.
	err := h.execErr(`CREATE VIEW it_gq.bad AS
		select v from it_gq.g group by v having exists (select 1 from it_gq.g x where x.id > g.id)`)
	var ae adbc.Error
	if want := `[redis] subquery uses ungrouped column "g.id" from outer query`; !errors.As(err, &ae) || ae.Msg != want {
		t.Errorf("CREATE VIEW: error %v, want %q", err, want)
	}
}

// Every clause that computes a value per group, with an ungrouped and a
// grouped column.
func TestSQLGroupedSubqueryClauses(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupedSubqueries()

	// The SELECT list.
	h.expectUngrouped(`select v, case when v > 15 then (select g.id) end from it_gq.g group by v`, "g.id")
	h.expectUngrouped(`select distinct v, (select g.id) from it_gq.g group by v`, "g.id")
	h.expectRows(`select v, exists (select 1 from it_gq.g x where x.v = g.v and x.id > 2) from it_gq.g group by v order by v`,
		"10|false", "20|true")

	// HAVING, with every kind of subquery.
	h.expectUngrouped(`select v from it_gq.g group by v having v in (select x.v from it_gq.g x where x.id = g.id)`, "g.id")
	h.expectUngrouped(`select v from it_gq.g group by v having v = any (select x.v from it_gq.g x where x.id = g.id)`, "g.id")
	h.expectUngrouped(`select v from it_gq.g group by v having (select g.id) in (select 1)`, "g.id")
	h.expectUngrouped(`select v from it_gq.g group by v
		having exists (select x.id from it_gq.g x where x.id > g.id union select 0)`, "g.id")
	h.expectRows(`select v, (select count(*) from it_gq.g x where x.v = g.v) from it_gq.g group by v
		having (select count(*) from it_gq.g x where x.v = g.v) > 1`, "10|2")
	h.expectRows(`select v, sum(id) from it_gq.g group by v
		having sum(id) > (select min(x.id) from it_gq.g x where x.v = g.v) order by v`, "10|3")

	// ORDER BY, also by an alias.
	h.expectUngrouped(`select v from it_gq.g group by v order by (select g.id)`, "g.id")
	h.expectRows(`select v from it_gq.g group by v order by (select -g.v)`, "20", "10")
	h.expectRows(`select v, (select max(x.id) from it_gq.g x where x.v = g.v) as m from it_gq.g group by v order by m desc`,
		"20|3", "10|2")
	h.expectRows(`select v, count(*) from it_gq.g group by v order by (select count(*) from it_gq.g x where x.v = g.v), v`,
		"20|1", "10|2")

	// DISTINCT ON.
	h.expectUngrouped(`select distinct on ((select g.id)) v from it_gq.g group by v`, "g.id")
	h.expectRows(`select * from (select distinct on ((select g.v / 20)) v from it_gq.g group by v) d order by v`, "10", "20")

	// Window functions: arguments, FILTER, PARTITION BY, ORDER BY, and
	// windows of the WINDOW clause, also unused (as in Postgres).
	h.expectUngrouped(`select v, first_value((select g.id)) over (order by v) from it_gq.g group by v`, "g.id")
	h.expectUngrouped(`select v, count(*) filter (where (select g.id) > 1) over () from it_gq.g group by v`, "g.id")
	h.expectUngrouped(`select v, count(*) over (partition by (select g.id)) from it_gq.g group by v`, "g.id")
	h.expectUngrouped(`select v, row_number() over (order by (select g.id)) from it_gq.g group by v`, "g.id")
	h.expectUngrouped(`select v, count(*) over w from it_gq.g group by v window w as (order by (select g.id))`, "g.id")
	h.expectUngrouped(`select v from it_gq.g group by v window w as (partition by (select g.id))`, "g.id")
	h.expectRows(`select v, sum((select g.v)) over (order by v) from it_gq.g group by v order by v`, "10|10", "20|30")
	h.expectRows(`select v, count(*) over (partition by (select g.v / 20)) from it_gq.g group by v order by v`, "10|1", "20|1")
	h.expectRows(`select v, count(*) over w from it_gq.g group by v window w as (order by (select g.v)) order by v`,
		"10|1", "20|2")

	// QUALIFY (not in Postgres; it filters the groups like HAVING).
	h.expectUngrouped(`select v, max(id) from it_gq.g group by v qualify (select g.id) > 0`, "g.id")
	h.expectUngrouped(`select v, max(id) from it_gq.g group by v qualify row_number() over (order by (select g.id)) = 1`, "g.id")
	h.expectRows(`select v, max(id) from it_gq.g group by v qualify row_number() over (order by (select g.v)) = 1`, "10|2")
}

// Nested subqueries, grouped subqueries, joins, other FROM items, queries
// grouped without GROUP BY, and what counts as grouped.
func TestSQLGroupedSubqueryLevels(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupedSubqueries()

	// A reference two levels down counts.
	h.expectUngrouped(`select v from it_gq.g group by v
		having exists (select 1 from it_gq.g x where exists (select 1 from it_gq.g y where y.id > g.id))`, "g.id")
	h.expectUngrouped(`select v, (select (select g.id)) from it_gq.g group by v`, "g.id")
	h.expectUngrouped(`select v, (select x.v from it_gq.g x where x.id = (select g.id)) from it_gq.g group by v`, "g.id")
	h.expectRows(`select v, (select count(*) from it_gq.g x
			where exists (select 1 from it_gq.g y where y.v = g.v and y.id = x.id)) from it_gq.g group by v order by v`,
		"10|2", "20|1")

	// The grouped query is a subquery: the rule is about its own columns
	// (x.id), not those of the query around it (g.id, a constant there).
	h.expectUngrouped(`select id, (select max(x.v) from it_gq.g x group by x.v
		having exists (select 1 where x.id > 2) order by 1 limit 1) from it_gq.g order by id`, "x.id")
	h.expectRows(`select id, (select max(x.v) from it_gq.g x group by x.v
		having exists (select 1 where g.id > 2) order by 1 limit 1) from it_gq.g order by id`,
		"1|NULL", "2|NULL", "3|10")

	// Joins, aliases, CTEs and derived tables.
	h.expectUngrouped(`select g.v from it_gq.g join it_gq.g h on h.id = g.id group by g.v
		having exists (select 1 where h.id > 1)`, "h.id")
	h.expectUngrouped(`select g.v, h.v from it_gq.g, it_gq.g h where h.id = g.id group by g.v, h.v
		having exists (select 1 from it_gq.g x where x.v = h.v and x.id <> g.id)`, "g.id")
	h.expectRows(`select g.v, h.v from it_gq.g, it_gq.g h where h.id = g.id group by g.v, h.v
		having exists (select 1 from it_gq.g x where x.v = h.v and x.id > 1) order by 1`, "10|10", "20|20")
	h.expectRows(`select g.v, (select max(x.id) from it_gq.g x where x.v = g.v) from it_gq.g join it_gq.g h on h.id = g.id
		group by g.v order by 1`, "10|2", "20|3")
	h.expectUngrouped(`select o.v from it_gq.g o group by o.v having exists (select 1 from it_gq.g x where x.id > o.id)`, "o.id")
	h.expectUngrouped(`with c as (select * from it_gq.g) select v from c group by v having exists (select 1 where c.id > 2)`, "c.id")
	h.expectUngrouped(`select v, (select g.id) from (select * from it_gq.g) g group by v`, "g.id")

	// Aggregates or HAVING without GROUP BY: one group, no grouped columns.
	h.expectUngrouped(`select count(*), (select g.id) from it_gq.g`, "g.id")
	h.expectUngrouped(`select 1 from it_gq.g having exists (select 1 where g.id > 0)`, "g.id")
	h.expectUngrouped(`select count(*) from it_gq.g order by (select g.id)`, "g.id")

	// Columns are named as in Postgres: those of a table function, of an
	// item with column aliases, and those a NATURAL JOIN merges (by the
	// left side's column, also for FULL, whose merged column is a
	// COALESCE).
	h.expectUngrouped(`select count(*), (select s.g) from generate_series(1, 3) as s(g)`, "s.g")
	h.expectUngrouped(`select count(*), (select t.a) from it_gq.g as t(a, b)`, "t.a")
	h.expectUngrouped(`select count(*), (select id) from it_gq.g a natural join it_gq.g b`, "a.id")
	h.expectUngrouped(`select count(*), (select id) from it_gq.g a natural full join it_gq.g b`, "a.id")
	h.expectUngrouped(`select count(*), (select x) from (select id as x from it_gq.g) d`, "d.x")
	// The same names with grouping sets, which were "s.s.g", "t.t.a" and a
	// NUL-prefixed name before.
	h.expectUngrouped(`select count(*), (select s.g) from generate_series(1, 3) as s(g) group by ()`, "s.g")
	h.expectUngrouped(`select count(*), (select id) from it_gq.g a natural full join it_gq.g b group by ()`, "a.id")
	h.expectUngrouped(`select v, (select g.id) from it_gq.g group by rollup (v)`, "g.id")
	for _, c := range []struct{ sql, col string }{
		{`select count(*), s.g from generate_series(1, 3) as s(g) group by ()`, "s.g"},
		{`select count(*), t.a from it_gq.g as t(a, b) group by ()`, "t.a"},
		{`select count(*), id from it_gq.g a natural full join it_gq.g b group by ()`, "a.id"},
	} {
		h.expectQueryError(c.sql, adbc.StatusInvalidArgument,
			fmt.Sprintf("column %q must appear in the GROUP BY clause or be used in an aggregate function", c.col))
	}

	// Only a GROUP BY column by itself is grouped. Postgres also can't
	// match a larger GROUP BY expression inside a subquery, and it allows
	// another column of a table grouped by its primary key, which the
	// driver doesn't keep.
	h.expectUngrouped(`select (select g.id + 1) from it_gq.g group by g.id + 1`, "g.id")
	h.expectUngrouped(`select g.id + 1, (select 1 where g.id + 1 > 0) from it_gq.g group by g.id + 1`, "g.id")
	h.expectUngrouped(`select id, (select g.v) from it_gq.g group by id`, "g.v")
	h.expectUngrouped(`select v, (select g.id) from it_gq.g group by v, g.v`, "g.id")
	h.expectRows(`select (select g.id) from it_gq.g group by g.id order by 1`, "1", "2", "3")
	h.expectRows(`select v, (select g.id) from it_gq.g group by v, id order by v, 2`, "10|1", "10|2", "20|3")
	// A subquery that is a GROUP BY item, by position or alias, is that
	// item's value.
	h.expectRows(`select (select g.id) from it_gq.g group by 1 order by 1`, "1", "2", "3")
	h.expectRows(`select (select g.id) as s from it_gq.g group by s order by 1`, "1", "2", "3")
}

// Where the rule doesn't apply.
func TestSQLGroupedSubqueryAllowed(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupedSubqueries()

	// Aggregate arguments, FILTER and ORDER BY see every row.
	h.expectRows(`select v, max((select g.id)) from it_gq.g group by v order by v`, "10|2", "20|3")
	h.expectRows(`select v, count(*) filter (where (select g.id) > 1) from it_gq.g group by v order by v`, "10|1", "20|1")
	h.expectRows(`select v, string_agg(id::text, ',' order by (select -g.id)) from it_gq.g group by v order by v`,
		"10|2,1", "20|3")
	// So do WHERE and LATERAL items, before grouping.
	h.expectRows(`select v, count(*) from it_gq.g where exists (select 1 from it_gq.g x where x.id > g.id)
		group by v order by v`, "10|2")
	h.expectRows(`select v, l.i from it_gq.g, lateral (select g.id as i) l group by v, l.i order by 1, 2`,
		"10|1", "10|2", "20|3")
	// A query that isn't grouped.
	h.expectRows(`select (select g.id) from it_gq.g order by 1`, "1", "2", "3")
	h.expectRows(`select id, (select count(*) from it_gq.g x where x.v = g.v) from it_gq.g order by id`, "1|2", "2|2", "3|1")
}

// An aggregate of the outer query's columns inside a subquery is the outer
// query's, as in Postgres: it is computed over the outer query's groups
// (#120). Up to #117 these were computed over the subquery's rows, then
// rejected as not supported.
func TestSQLGroupedSubqueryOuterAggregates(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupedSubqueries()
	h.expectRows(`select v, (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g group by v order by v`,
		"10|2", "20|3")
	h.expectRows(`select v, (select max(g.id) filter (where g.v > 0) from it_gq.g x where x.id = 1) from it_gq.g group by v order by v`,
		"10|2", "20|3")
	h.expectRows(`select v, (select sum(g.id)) from it_gq.g group by v order by v`, "10|3", "20|3")
	// In the subquery, max(g.id) is a constant, so ordering by it alone
	// leaves the row LIMIT picks to the scan order, which a cluster's
	// shards don't keep.
	h.expectRows(`select v, (select x.id from it_gq.g x order by abs(x.id - max(g.id)) limit 1) from it_gq.g group by v order by v`,
		"10|2", "20|3")
	h.expectRows(`select count(*), (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g`, "3|3")
	// An aggregate that also reads the subquery's own columns is the
	// subquery's.
	h.expectUngrouped(`select v, (select sum(g.id + x.id) from it_gq.g x) from it_gq.g group by v`, "g.id")
	h.expectUngrouped(`select v, (select max(x.id) filter (where g.id > 0) from it_gq.g x) from it_gq.g group by v`, "g.id")
	// So is its column outside the aggregate.
	h.expectUngrouped(`select v, (select max(g.id) + g.id from it_gq.g x where x.id = 1) from it_gq.g group by v`, "g.id")
	h.expectUngrouped(`select v, (select max(g.id) from it_gq.g x where x.id > g.id) from it_gq.g group by v`, "g.id")
}

// The queries of #120, and its acceptance cases.
func TestSQLOuterAggregateIssue(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupedSubqueries()
	// Over each group: the subquery ran once and read one row's value.
	h.expectRows(`select v, (select sum(g.v) from it_gq.g x where x.id = 1) from it_gq.g group by v order by v`,
		"10|20", "20|20")
	// Without GROUP BY, the query is an aggregate one: one row.
	h.expectRows(`select (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g`, "3")
	h.expectRows(`select v, (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g group by v order by v`,
		"10|2", "20|3")
	h.expectRows(`select count(*), (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g`, "3|3")
	h.expectRows(`select v from it_gq.g group by v having (select count(g.id) from it_gq.g x where x.id = 1) > 1 order by v`,
		"10")
	h.expectRows(`select (select sum(g.v)) from it_gq.g`, "40")
	// An aggregate that reads the subquery's columns too is the subquery's.
	h.expectRows(`select (select sum(g.v + x.id) from it_gq.g x where x.id = 1) from it_gq.g`, "11", "11", "21")
	// In the subquery, the outer aggregate is a constant, also in WHERE.
	h.expectRows(`select (select x.id from it_gq.g x where x.id = max(g.id)) from it_gq.g`, "3")
	// Where it may be is decided at its own level.
	h.expectQueryError(`select id from it_gq.g where (select max(g.id)) > 1`, adbc.StatusInvalidArgument,
		"aggregate functions are not allowed in WHERE")
}

// Outer-level aggregates wherever a query computes values per group, and
// in the forms an aggregate takes.
func TestSQLOuterAggregateForms(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupedSubqueries()
	h.exec(`CREATE TABLE it_gq.e (id INTEGER, v INTEGER)`)

	// Two levels down.
	h.expectRows(`select (select (select max(g.id) from it_gq.g y where y.id = 1) from it_gq.g x where x.id = 1) from it_gq.g`, "3")
	h.expectRows(`select v, (select (select max(g.id) from it_gq.g y where y.id = 1) from it_gq.g x where x.id = 1)
		from it_gq.g group by v order by v`, "10|2", "20|3")
	// HAVING, an IN subquery and EXISTS, ORDER BY by an alias.
	h.expectRows(`select v from it_gq.g group by v
		having exists (select 1 from it_gq.g x where x.id = max(g.id) and x.v = 20) order by v`, "20")
	h.expectRows(`select v, max(g.id) in (select x.id from it_gq.g x where x.id > min(g.id)) from it_gq.g group by v order by v`,
		"10|true", "20|false")
	h.expectRows(`select v, (select sum(g.v) + 1 from it_gq.g x where x.id = 1) from it_gq.g group by v
		having (select sum(g.v)) > 15 order by v`, "10|21", "20|21")
	h.expectRows(`select v, (select max(g.id) from it_gq.g x where x.id = 1) as m from it_gq.g group by v order by m desc`,
		"20|3", "10|2")
	// Grouping sets.
	h.expectRows(`select v, (select sum(g.id) from it_gq.g x where x.id = 1) from it_gq.g group by rollup (v) order by v`,
		"10|3", "20|3", "NULL|6")
	h.expectRows(`select v, (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g group by rollup (v) order by v`,
		"10|2", "20|3", "NULL|3")
	// Window functions, DISTINCT and DISTINCT ON.
	h.expectRows(`select v, rank() over (order by (select max(g.id))) from it_gq.g group by v order by v`, "10|1", "20|2")
	h.expectRows(`select v, sum((select max(g.id))) over () from it_gq.g group by v order by v`, "10|5", "20|5")
	h.expectRows(`select v, count(*) over (partition by (select max(g.id))) from it_gq.g group by v order by v`, "10|1", "20|1")
	h.expectRows(`select distinct (select max(g.id) / 3 from it_gq.g x where x.id = 1) from it_gq.g group by v order by 1`, "0", "1")
	h.expectRows(`select distinct on (1) (select max(g.id) from it_gq.g x where x.id = 1) m, v from it_gq.g group by v order by 1, v`,
		"2|10", "3|20")
	// DISTINCT, ORDER BY and FILTER in the aggregate.
	h.expectRows(`select v, (select count(distinct g.id) from it_gq.g x where x.id = 1) from it_gq.g group by v order by v`,
		"10|2", "20|1")
	h.expectRows(`select v, (select string_agg(g.id::text, ',' order by g.id desc)) from it_gq.g group by v order by v`,
		"10|2,1", "20|3")
	h.expectRows(`select v, (select count(*) filter (where g.id > 1)) from it_gq.g group by v order by v`, "10|1", "20|1")
	h.expectRows(`select (select percentile_disc(0.5) within group (order by g.v) from it_gq.g x where x.id = 1) from it_gq.g`, "10")
	// With the subquery's own aggregates, and the query's.
	h.expectRows(`select v, (select max(g.id) + max(x.id) from it_gq.g x) from it_gq.g group by v order by v`, "10|5", "20|6")
	h.expectRows(`select max(id), (select max(g.id) + 1) from it_gq.g`, "3|4")
	// No rows, an empty table, HAVING alone.
	h.expectRows(`select (select max(g.id)) from it_gq.g where false`, "NULL")
	h.expectRows(`select (select count(e.id)) from it_gq.e`, "0")
	h.expectRows(`select 1 from it_gq.g having (select max(g.id)) > 2`, "1")
	h.expectRows(`select 1 from it_gq.g having (select max(g.id)) > 3`)
	// Views, CTEs and set operations.
	h.exec(`CREATE VIEW it_gq.sums AS select v, (select sum(g.v) from it_gq.g x where x.id = 1) s from it_gq.g group by v`)
	h.expectRows(`select * from it_gq.sums order by v`, "10|20", "20|20")
	h.expectRows(`with c as (select * from it_gq.g) select (select max(c.id)) from c`, "3")
	h.expectRows(`select v, (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g group by v
		union all select 0, (select min(g.id)) from it_gq.g order by 1`, "0|1", "10|2", "20|3")
	// Not supported: a subquery in its arguments, whose columns are found
	// only as it is planned. Postgres gives 10|2, 20|3.
	h.expectQueryError(`select v, (select max((select g.id)) from it_gq.g x where x.id = 1) from it_gq.g group by v`,
		adbc.StatusNotImplemented, "outer-level aggregate with a subquery in its arguments is not supported")
	// A subquery in the arguments of the query's own aggregate is fine.
	h.expectRows(`select v, max((select g.id)) from it_gq.g group by v order by v`, "10|2", "20|3")
}

// Postgres's rule for where an aggregate may be applies at the aggregate's
// own level, also when a subquery holds it. The errors are Postgres's.
func TestSQLOuterAggregatePlacement(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupedSubqueries()
	h.exec(`CREATE TABLE it_gq.h (id INTEGER, v INTEGER)`)
	notAllowed := func(sql, clause string) {
		h.t.Helper()
		h.expectQueryError(sql, adbc.StatusInvalidArgument, "aggregate functions are not allowed in "+clause)
	}
	for _, c := range []struct{ sql, clause string }{
		{`select id from it_gq.g where (select max(g.id)) > 1`, "WHERE"},
		{`select id from it_gq.g where max(id) > 1`, "WHERE"},
		{`select 1 from it_gq.g where sum(count(*)) > 1`, "WHERE"},
		{`select g.id from it_gq.g join it_gq.g h on h.id = (select max(g.id))`, "JOIN conditions"},
		{`select 1 from it_gq.g join it_gq.g h on h.id = max(g.id)`, "JOIN conditions"},
		{`select v from it_gq.g group by (select max(g.id))`, "GROUP BY"},
		{`select (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g group by 1`, "GROUP BY"},
		{`select 1 from it_gq.g group by max(id)`, "GROUP BY"},
		{`select count(*) filter (where (select max(g.id)) > 1) from it_gq.g`, "FILTER"},
		{`select count(*) filter (where count(*) > 1) from it_gq.g`, "FILTER"},
		{`select * from it_gq.g, lateral (select max(g.id)) l`, "FROM clause of their own query level"},
		{`select * from it_gq.g, generate_series(1, (select max(g.id))) s`, "functions in FROM"},
		{`update it_gq.g set v = (select max(g.id))`, "UPDATE"},
		{`update it_gq.g set v = max(id)`, "UPDATE"},
		{`update it_gq.h t set v = (select max(s.id)) from it_gq.g s where s.id = t.id`, "UPDATE"},
		{`update it_gq.g set v = 1 where (select max(g.id)) > 1`, "WHERE"},
		{`delete from it_gq.g where (select max(g.id)) > 1`, "WHERE"},
		{`delete from it_gq.h t using it_gq.g s where max(s.id) > 1`, "WHERE"},
		{`insert into it_gq.h values (count(*), 1)`, "VALUES"},
		{`update it_gq.g set v = v returning (select max(g.id))`, "RETURNING"},
		{`merge into it_gq.h t using it_gq.g s on t.id = s.id when matched and (select max(s.id)) > 1 then delete`,
			"MERGE WHEN conditions"},
		{`merge into it_gq.h t using it_gq.g s on t.id = s.id when matched and count(*) > 1 then delete`,
			"MERGE WHEN conditions"},
		{`merge into it_gq.h t using it_gq.g s on t.id = max(s.id) when matched then delete`, "JOIN conditions"},
		{`merge into it_gq.h t using it_gq.g s on t.id = s.id when matched then update set v = max(s.id)`, "UPDATE"},
		{`merge into it_gq.h t using it_gq.g s on t.id = s.id when not matched then insert values (max(s.id), 1)`, "VALUES"},
		{`with recursive t(n) as (select 1 union all select (select max(t.n) + 1) from t where n < 3) select * from t`,
			"a recursive query's recursive term"},
	} {
		notAllowed(c.sql, c.clause)
	}
	// Nothing was changed.
	h.expectRows(`select * from it_gq.g order by id`, "1|10", "2|10", "3|20")
	h.expectRows(`select count(*) from it_gq.h`, "0")

	const nested = "aggregate function calls cannot be nested"
	h.expectQueryError(`select max((select max(g.id))) from it_gq.g`, adbc.StatusInvalidArgument, nested)
	h.expectQueryError(`select (select sum(count(g.id)) from it_gq.g x where x.id = 1) from it_gq.g`, adbc.StatusInvalidArgument, nested)
	h.expectQueryError(`select sum(count(*)) from it_gq.g`, adbc.StatusInvalidArgument, nested)
	h.expectQueryError(`select (select percentile_cont(x.id / 10.0) within group (order by g.v) from it_gq.g x where x.id = 1) from it_gq.g`,
		adbc.StatusInvalidArgument, "outer-level aggregate cannot contain a lower-level variable in its direct arguments")
}

// Outer-level aggregates in every aggregate pushdown mode, in the index
// when it can compute them, and in a statement that runs again.
func TestSQLOuterAggregatePushdown(t *testing.T) {
	h := newSQLHarness(t)
	h.setupGroupedSubqueries()
	queries := []struct {
		sql  string
		want []string
	}{
		{`select v, (select sum(g.v) from it_gq.g x where x.id = 1) from it_gq.g group by v order by v`, []string{"10|20", "20|20"}},
		{`select v, (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g group by v order by v`, []string{"10|2", "20|3"}},
		{`select (select max(g.id) from it_gq.g x where x.id = 1) from it_gq.g`, []string{"3"}},
		{`select count(*), (select count(g.v) from it_gq.g x where x.id = 1) from it_gq.g where v > 10`, []string{"1|1"}},
		{`select v, (select sum(g.id) from it_gq.g x where x.id = 1) from it_gq.g group by rollup (v) order by v`,
			[]string{"10|3", "20|3", "NULL|6"}},
	}
	for _, mode := range []string{PushdownExact, PushdownAll, PushdownNone} {
		m := newGSPushdownHarness(t, mode)
		for _, q := range queries {
			m.expectRows(q.sql, q.want...)
		}
	}
	// The outer query computes SUM(v) with its GROUP BY in the index.
	if !h.indexAggIn(PushdownExact, `select v, (select sum(g.v) from it_gq.g x where x.id = 1) from it_gq.g group by v`) {
		t.Error("the outer-level SUM(g.v) doesn't run in the index")
	}

	// A statement keeps its parsed query, planned again when it runs again.
	st := h.gsPrepare(`select v, (select sum(g.v) from it_gq.g x where x.id = 1) from it_gq.g group by v order by v`)
	for range 2 {
		if got := h.gsRows(st); !slices.Equal(got, []string{"10|20", "20|20"}) {
			t.Errorf("run again: got %q", got)
		}
	}
	st = h.gsPrepare(`select v, (select sum(g.v) + ? from it_gq.g x where x.id = 1) from it_gq.g group by v order by v`)
	for _, c := range []struct {
		param int64
		want  []string
	}{{1, []string{"10|21", "20|21"}}, {5, []string{"10|25", "20|25"}}} {
		ib := array.NewInt64Builder(memory.DefaultAllocator)
		ib.Append(c.param)
		col := ib.NewArray()
		ib.Release()
		rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "p", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{col}, 1)
		if err := st.Bind(h.ctx, rec); err != nil {
			t.Fatal(err)
		}
		if got := h.gsRows(st); !slices.Equal(got, c.want) {
			t.Errorf("parameter %d: got %q, want %q", c.param, got, c.want)
		}
	}
}
