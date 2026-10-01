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

// Function calls are checked when a statement is planned (funcs.go): unknown
// functions and wrong argument counts fail whether or not a row is read, in
// every place a call can be, and write nothing. Errors that depend on values
// are still raised only on rows.

import (
	"fmt"
	"strings"
	"testing"
)

// setupFuncs creates it_fx_t (2 rows) and it_fx_e (no rows), with the same
// columns.
func (h *sqlHarness) setupFuncs() {
	h.t.Helper()
	h.dropViews("it_fx_v", "it_fx_tv", "it_fx_ok")
	h.dropTables("it_fx_t", "it_fx_e", "it_fx_c", "it_fx_d", "it_fx_ctas")
	h.t.Cleanup(func() { h.dropViews("it_fx_v", "it_fx_tv", "it_fx_ok") })
	for _, name := range []string{"it_fx_t", "it_fx_e"} {
		h.exec("CREATE TABLE " + name + " (id INTEGER NOT NULL, s VARCHAR, d DATE, n NUMERIC(10,2), b BOOLEAN, ts TIMESTAMP)")
	}
	h.exec(`INSERT INTO it_fx_t VALUES (1, 'a', DATE '2024-01-01', 1.50, true, TIMESTAMP '2024-01-01 10:00:00'),
		(2, 'x', NULL, 0, false, NULL)`)
}

// expectErrorText runs a statement with ExecuteQuery and checks that it
// fails with exactly the message want (without the driver's prefix).
func (h *sqlHarness) expectErrorText(sql, want string) {
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
		h.t.Errorf("%s: no error, want %q", sql, want)
		return
	}
	if got := errText(err); got != want {
		h.t.Errorf("%s\n error: %q\n  want: %q", sql, got, want)
	}
}

// expectSchemaErrorText is expectErrorText for ExecuteSchema.
func (h *sqlHarness) expectSchemaErrorText(sql, want string) {
	h.t.Helper()
	_, err := h.executeSchema(sql)
	if err == nil {
		h.t.Errorf("ExecuteSchema(%s): no error, want %q", sql, want)
		return
	}
	if got := errText(err); got != want {
		h.t.Errorf("ExecuteSchema(%s)\n error: %q\n  want: %q", sql, got, want)
	}
}

const noSuchInt = "function nosuchfunc(integer) does not exist"

// expectUnchanged checks that the statements of a test wrote nothing: the
// tables' rows, columns and constraints are as setupFuncs made them, and
// the objects the statements tried to create don't exist.
func (h *sqlHarness) expectUnchanged() {
	h.t.Helper()
	h.expectRows("SELECT id, s, d, n, b, ts FROM it_fx_t ORDER BY id",
		"1|a|2024-01-01|1.50|true|2024-01-01T10:00:00", "2|x|NULL|0.00|false|NULL")
	h.expectRows("SELECT COUNT(*) FROM it_fx_e", "0")
	h.expectRows(`SELECT table_name, column_name, column_default FROM information_schema.columns
		WHERE table_name IN ('it_fx_t', 'it_fx_e') ORDER BY table_name, ordinal_position`,
		"it_fx_e|id|NULL", "it_fx_e|s|NULL", "it_fx_e|d|NULL", "it_fx_e|n|NULL", "it_fx_e|b|NULL", "it_fx_e|ts|NULL",
		"it_fx_t|id|NULL", "it_fx_t|s|NULL", "it_fx_t|d|NULL", "it_fx_t|n|NULL", "it_fx_t|b|NULL", "it_fx_t|ts|NULL")
	raw := h.rawClient()
	for _, table := range []string{"it_fx_t", "it_fx_e"} {
		h.expectChecks(raw, table)
	}
}

// The statements of issue #78: each fails while it is planned (or at
// CREATE), with the same message whether the table has rows or not.
func TestSQLFunctionsIssueTable(t *testing.T) {
	h := newSQLHarness(t)
	h.setupFuncs()
	for _, table := range []string{"it_fx_t", "it_fx_e"} {
		for _, c := range []struct{ sql, err string }{
			{"select nosuchfunc(id) from %s", noSuchInt},
			{"select nosuchfunc(id) from %s where false", noSuchInt},
			{"select upper(s, s, s) from %s", "UPPER expects 1 argument"},
			{"select round(id, 1, 2, 3) from %s", "ROUND expects 1 or 2 arguments"},
			// dbt's contract check and --empty refs, which read no rows.
			{dbtEmpty("select nosuchfunc(id) as x from %s"), noSuchInt},
			{"select * from (select nosuchfunc(id) as x from %s where false limit 0) as _dbt_limit_subq", noSuchInt},
			{dbtEmpty("select upper(s, s, s) as x from %s"), "UPPER expects 1 argument"},
		} {
			sql := fmt.Sprintf(c.sql, table)
			h.expectErrorText(sql, c.err)
			h.expectSchemaErrorText(sql, c.err)
		}
		h.expectErrorText(fmt.Sprintf("create view it_fx_v as select nosuchfunc(id) as x from %s", table), noSuchInt)
		h.expectErrorText("select * from it_fx_v", `table "public"."it_fx_v" does not exist`)
		h.expectErrorText(fmt.Sprintf("alter table %s add constraint k check (nosuchfunc(id) > 0)", table), noSuchInt)
	}
	h.expectErrorText("create table it_fx_c (id integer check (nosuchfunc(id) > 0))", noSuchInt)
	h.expectErrorText("insert into it_fx_c values (1)", `table "public"."it_fx_c" does not exist`)
	h.expectUnchanged()
	h.exec("INSERT INTO it_fx_e VALUES (9, 'z', NULL, NULL, NULL, NULL)") // no constraint was added
	h.expectAffected("DELETE FROM it_fx_e", 1)
}

// wantArgCount is the error for a call of name with n arguments that takes
// min to max (max -1: no maximum), written out independently of funcs.go.
func wantArgCount(name string, min, max, n int) string {
	if name == "COUNT" && n == 0 {
		return "COUNT(*) must be used to call a parameterless aggregate function"
	}
	args := func(k int) string {
		if k == 1 {
			return "1 argument"
		}
		return fmt.Sprint(k, " arguments")
	}
	var want string
	switch {
	case max < 0:
		want = "at least " + args(min)
	case min == 0 && max == 0:
		want = "no arguments"
	case min == max:
		want = args(min)
	case max == min+1:
		want = fmt.Sprintf("%d or %d arguments", min, max)
	default:
		want = fmt.Sprintf("%d to %d arguments", min, max)
	}
	return name + " expects " + want
}

// argCountSyntax are the calls whose argument counts the parser checks,
// with its errors; a missing count is valid syntax for another form.
var argCountSyntax = map[string]map[int]string{
	"DATE_ADD":    {2: "syntax error: DATE_ADD expects (part, n, x) or (x, INTERVAL n part)", 4: "syntax error: DATE_ADD expects (part, n, x) or (x, INTERVAL n part)"},
	"DATE_SUB":    {2: "syntax error: DATE_SUB expects (x, INTERVAL n part)", 4: "syntax error: DATE_SUB expects (x, INTERVAL n part)"},
	"JSON_VALUE":  {1: `syntax error: expected "," near ")"`, 5: `syntax error: expected ON EMPTY or ON ERROR near ","`},
	"JSON_QUERY":  {1: `syntax error: expected "," near ")"`, 5: `syntax error: expected ON EMPTY or ON ERROR near ","`},
	"JSON_EXISTS": {1: `syntax error: expected "," near ")"`, 3: `syntax error: expected ON EMPTY or ON ERROR near ","`},
	"JSON_OBJECT": {3: "JSON_OBJECT expects 1 or 2 arguments"}, // JSON_OBJECT() is the SQL/JSON constructor
}

// Every function of the registry, by each of its names, rejects one
// argument too few and one too many on an empty table, with or without
// OVER where it takes one. The calls are generated from the registry, so a
// new function is covered as soon as it is registered.
func TestSQLFunctionsArgCounts(t *testing.T) {
	h := newSQLHarness(t)
	h.setupFuncs()
	checked := 0
	for i := range funcDefs {
		d := &funcDefs[i]
		for _, name := range append([]string{d.name}, d.aliases...) {
			if d.internal {
				continue // built by the parser, which gives it its operands
			}
			counts := []int{}
			if d.min > 0 {
				counts = append(counts, d.min-1)
			}
			if d.max != many {
				counts = append(counts, d.max+1)
			}
			if special, ok := argCountSyntax[name]; ok {
				counts = counts[:0]
				for n := range special {
					counts = append(counts, n)
				}
			}
			for _, n := range counts {
				args := strings.TrimSuffix(strings.Repeat("1, ", n), ", ")
				call := name + "(" + args + ")"
				if d.orderedSet {
					call += " WITHIN GROUP (ORDER BY id)"
				}
				var sqls []string
				switch {
				case d.kind == tableKind:
					sqls = []string{"SELECT * FROM " + call}
				case d.kind == windowKind:
					sqls = []string{"SELECT " + call + " OVER (ORDER BY id) FROM it_fx_e"}
				case d.impl == implGrouping:
					sqls = []string{"SELECT " + call + " FROM it_fx_e GROUP BY id"}
				default:
					sqls = []string{"SELECT " + call + " FROM it_fx_e"}
					if d.over == overOK {
						sqls = append(sqls, "SELECT "+call+" OVER () FROM it_fx_e")
					}
				}
				want := wantArgCount(name, d.min, d.max, n)
				if special, ok := argCountSyntax[name]; ok {
					want = special[n]
				}
				for _, sql := range sqls {
					h.expectErrorText(sql, want)
					checked++
				}
			}
		}
	}
	if checked < 250 {
		t.Errorf("checked %d calls; the registry should give more", checked)
	}
	// Some by hand, to see the generated ones mean what they say.
	h.expectErrorText("SELECT UPPER() FROM it_fx_e", "UPPER expects 1 argument")
	h.expectErrorText("SELECT REGEXP_INSTR(s) FROM it_fx_e", "REGEXP_INSTR expects 2 to 7 arguments")
	h.expectErrorText("SELECT CONCAT_WS() FROM it_fx_e", "CONCAT_WS expects at least 1 argument")
	h.expectErrorText("SELECT RANDOM(1) FROM it_fx_e", "RANDOM expects no arguments")
	h.expectErrorText("SELECT NOW(1, 2) FROM it_fx_e", "NOW expects 0 or 1 arguments")
	h.expectErrorText("SELECT STRING_AGG(s) OVER () FROM it_fx_e", "STRING_AGG expects 2 arguments")
	h.expectErrorText("SELECT NTILE() OVER () FROM it_fx_e", "NTILE expects 1 argument")
	h.expectErrorText("SELECT MODE(id) WITHIN GROUP (ORDER BY id) FROM it_fx_e", "MODE expects no arguments")
	h.expectErrorText("SELECT * FROM generate_series(1)", "GENERATE_SERIES expects 2 or 3 arguments")
	// * and DISTINCT.
	h.expectErrorText("SELECT UPPER(DISTINCT s) FROM it_fx_e", "UPPER does not accept * or DISTINCT")
	h.expectErrorText("SELECT LOWER(*) FROM it_fx_e", "LOWER does not accept * or DISTINCT")
	h.expectErrorText("SELECT SUM(*) FROM it_fx_e", "SUM does not accept *")
	h.expectErrorText("SELECT ROW_NUMBER(*) OVER () FROM it_fx_e", "ROW_NUMBER does not accept *")
	h.expectRows("SELECT COUNT(*), COUNT(DISTINCT id, s) FROM it_fx_t", "2|2")
	// Valid counts at both ends still work, NULL arguments included.
	h.expectRows("SELECT ROUND(n), ROUND(n, 1), LPAD(s, 2), LPAD(s, 3, '-'), COALESCE(), CONCAT(), NOW(3) IS NOT NULL FROM it_fx_t ORDER BY id",
		"2|1.5| a|--a|(null)||true", "0|0.0| x|--x|(null)||true")
	h.expectRows("SELECT UPPER(NULL), IFNULL(NULL, NULL, 3), GREATEST(NULL, id) FROM it_fx_t ORDER BY id", "NULL|3|1", "NULL|3|2")
}

// An unknown function is an error wherever a call can be, also when no row
// is read, and the statement writes nothing.
func TestSQLFunctionsEveryPosition(t *testing.T) {
	h := newSQLHarness(t)
	h.setupFuncs()
	h.exec("CREATE VIEW it_fx_ok AS SELECT id FROM it_fx_t")
	for _, c := range []struct{ sql, err string }{
		// Queries (also run in dbt's form, which reads no rows).
		{"SELECT id FROM it_fx_t WHERE nosuchfunc(id) = 1", noSuchInt},
		{"SELECT id FROM it_fx_t GROUP BY id, nosuchfunc(id)", noSuchInt},
		{"SELECT id FROM it_fx_t GROUP BY ROLLUP (id, nosuchfunc(s))", "function nosuchfunc(varchar) does not exist"},
		{"SELECT COUNT(*) FROM it_fx_t HAVING nosuchfunc(COUNT(*)) > 0", "function nosuchfunc(bigint) does not exist"},
		{"SELECT id FROM it_fx_t QUALIFY nosuchfunc(id) = 1", noSuchInt},
		{"SELECT id FROM it_fx_t ORDER BY nosuchfunc(id)", noSuchInt},
		{"SELECT DISTINCT ON (nosuchfunc(id)) id FROM it_fx_t", noSuchInt},
		{"SELECT 1 FROM it_fx_t a JOIN it_fx_e b ON nosuchfunc(a.id) = b.id", noSuchInt},
		{"SELECT 1 FROM it_fx_t a LEFT JOIN it_fx_e b ON b.id = a.id AND nosuchfunc(b.d) AND false", "function nosuchfunc(date) does not exist"},
		{"SELECT SUM(id) OVER (PARTITION BY nosuchfunc(id)) FROM it_fx_t", noSuchInt},
		{"SELECT SUM(id) OVER (ORDER BY nosuchfunc(id)) FROM it_fx_t", noSuchInt},
		{"SELECT id FROM it_fx_t WINDOW w AS (ORDER BY nosuchfunc(id))", noSuchInt},
		{"SELECT nosuchfunc(id) OVER () FROM it_fx_t", noSuchInt},
		{"SELECT nosuchfunc(id) FILTER (WHERE true) FROM it_fx_t", noSuchInt},
		{"SELECT COUNT(*) FILTER (WHERE nosuchfunc(b)) FROM it_fx_t", "function nosuchfunc(boolean) does not exist"},
		{"SELECT nosuchfunc(0.5) WITHIN GROUP (ORDER BY id) FROM it_fx_t", "function nosuchfunc(numeric) does not exist"},
		{"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY nosuchfunc(id)) FROM it_fx_t", noSuchInt},
		{"SELECT STRING_AGG(s, ',' ORDER BY nosuchfunc(id)) FROM it_fx_t", noSuchInt},
		{"SELECT SUM(nosuchfunc(id)) FROM it_fx_t", noSuchInt},
		{"SELECT LAG(nosuchfunc(id)) OVER (ORDER BY id) FROM it_fx_t", noSuchInt},
		{"SELECT CASE WHEN nosuchfunc(id) THEN 1 END FROM it_fx_t", noSuchInt},
		{"SELECT CASE WHEN false THEN nosuchfunc(id) END FROM it_fx_t", noSuchInt},
		{"SELECT COALESCE(s, nosuchfunc(s)) FROM it_fx_t", "function nosuchfunc(varchar) does not exist"},
		{"SELECT CAST(nosuchfunc(id) AS VARCHAR) FROM it_fx_t", noSuchInt},
		{"SELECT TRY_CAST(nosuchfunc(ts) AS DATE) FROM it_fx_t", "function nosuchfunc(timestamp) does not exist"},
		{"SELECT id FROM it_fx_t WHERE id IN (1, nosuchfunc(id))", noSuchInt},
		{"SELECT id FROM it_fx_t WHERE id BETWEEN 1 AND nosuchfunc(n)", "function nosuchfunc(numeric) does not exist"},
		{`SELECT JSON_VALUE(s, '$.a' DEFAULT nosuchfunc(id) ON EMPTY) FROM it_fx_t`, noSuchInt},
		{"SELECT (SELECT nosuchfunc(id) FROM it_fx_e) FROM it_fx_t", noSuchInt},
		{"SELECT id FROM it_fx_t WHERE EXISTS (SELECT 1 FROM it_fx_e e WHERE nosuchfunc(e.id) = it_fx_t.id)", noSuchInt},
		{"SELECT id FROM it_fx_t WHERE id IN (SELECT nosuchfunc(id) FROM it_fx_e)", noSuchInt},
		{"SELECT id FROM it_fx_t WHERE id > ALL (SELECT nosuchfunc(id) FROM it_fx_e)", noSuchInt},
		{"SELECT * FROM (SELECT nosuchfunc(id) AS x FROM it_fx_e) s", noSuchInt},
		{"WITH c AS (SELECT nosuchfunc(id) AS x FROM it_fx_t) SELECT * FROM c WHERE false", noSuchInt},
		{"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT nosuchfunc(n) FROM r WHERE n < 3) SELECT * FROM r", "function nosuchfunc(bigint) does not exist"},
		{"SELECT * FROM it_fx_t t, LATERAL (SELECT nosuchfunc(t.id) AS x) l", noSuchInt},
		{"SELECT * FROM it_fx_t t, generate_series(1, nosuchfunc(t.id)) g", noSuchInt},
		{"SELECT id FROM it_fx_t UNION ALL SELECT nosuchfunc(id) FROM it_fx_e", noSuchInt},
		{"SELECT id FROM it_fx_ok WHERE nosuchfunc(id) = 1", noSuchInt},
		{"SELECT nosuchfunc(id) FROM it_fx_t LIMIT 0", noSuchInt},
		{"SELECT nosuchfunc(id) FROM it_fx_t FETCH FIRST 0 ROWS ONLY", noSuchInt},
		{"SELECT COUNT(*) FROM it_fx_t HAVING false AND nosuchfunc(1) = 1", "function nosuchfunc(bigint) does not exist"},
		{"SELECT nosuchfunc(1)", "function nosuchfunc(bigint) does not exist"},
		{"SELECT nosuchfunc(s, d, n, b, ts, 1, 1.5, 'x', NULL, 2.5e0, INTERVAL '1 day', CAST(1 AS SMALLINT)) FROM it_fx_t",
			"function nosuchfunc(varchar, date, numeric, boolean, timestamp, bigint, numeric, varchar, unknown, double precision, interval, smallint) does not exist"},
		{"SELECT nosuchfunc() FROM it_fx_t", "function nosuchfunc() does not exist"},
		{"SELECT nosuchfunc(*) FROM it_fx_t", "function nosuchfunc(*) does not exist"},
		{"SELECT nosuchfunc(DISTINCT id) FROM it_fx_t", noSuchInt},
		// Writes.
		{"INSERT INTO it_fx_t SELECT nosuchfunc(id), s, d, n, b, ts FROM it_fx_t", noSuchInt},
		{"INSERT INTO it_fx_t SELECT nosuchfunc(id), s, d, n, b, ts FROM it_fx_e", noSuchInt},
		{"INSERT INTO it_fx_t " + dbtEmpty("SELECT nosuchfunc(id), s, d, n, b, ts FROM it_fx_t"), noSuchInt},
		{"INSERT INTO it_fx_t (id) VALUES (3), (nosuchfunc(4))", "function nosuchfunc(bigint) does not exist"},
		{"INSERT INTO it_fx_t (id) VALUES (3) RETURNING nosuchfunc(id)", noSuchInt},
		{"UPDATE it_fx_t SET s = nosuchfunc(s)", "function nosuchfunc(varchar) does not exist"},
		{"UPDATE it_fx_e SET s = nosuchfunc(s)", "function nosuchfunc(varchar) does not exist"},
		{"UPDATE it_fx_t SET s = 'q' WHERE nosuchfunc(id) = 1", noSuchInt},
		{"UPDATE it_fx_t SET s = upper(s, s) WHERE false", "UPPER expects 1 argument"},
		{"UPDATE it_fx_t SET s = 'q' RETURNING nosuchfunc(s)", "function nosuchfunc(varchar) does not exist"},
		{"UPDATE it_fx_t t SET s = e.s FROM it_fx_e e WHERE nosuchfunc(e.id) = t.id", noSuchInt},
		{"UPDATE it_fx_t t SET s = nosuchfunc(e.s) FROM it_fx_e e WHERE e.id = t.id", "function nosuchfunc(varchar) does not exist"},
		{"DELETE FROM it_fx_t WHERE nosuchfunc(id) = 1", noSuchInt},
		{"DELETE FROM it_fx_t WHERE id = 1 RETURNING nosuchfunc(id)", noSuchInt},
		{"DELETE FROM it_fx_t t USING it_fx_e e WHERE nosuchfunc(e.id) = t.id", noSuchInt},
		{`MERGE INTO it_fx_t t USING it_fx_e e ON e.id = t.id
			WHEN MATCHED THEN UPDATE SET s = nosuchfunc(e.s)`, "function nosuchfunc(varchar) does not exist"},
		{`MERGE INTO it_fx_t t USING it_fx_t e ON e.id = t.id
			WHEN MATCHED AND nosuchfunc(e.b) THEN DELETE`, "function nosuchfunc(boolean) does not exist"},
		{`MERGE INTO it_fx_e t USING it_fx_t e ON e.id = t.id
			WHEN NOT MATCHED THEN INSERT (id, s) VALUES (e.id, nosuchfunc(e.s))`, "function nosuchfunc(varchar) does not exist"},
		{`MERGE INTO it_fx_e t USING (SELECT * FROM it_fx_t WHERE false) e ON nosuchfunc(e.id) = t.id
			WHEN NOT MATCHED THEN INSERT (id) VALUES (e.id)`, noSuchInt},
		{`MERGE INTO it_fx_e t USING it_fx_t e ON e.id = t.id
			WHEN NOT MATCHED THEN INSERT (id) VALUES (e.id) RETURNING nosuchfunc(merge_action())`, "function nosuchfunc(varchar) does not exist"},
		// Objects that keep expressions.
		{"CREATE TABLE it_fx_ctas AS SELECT nosuchfunc(id) AS x FROM it_fx_e", noSuchInt},
		{"CREATE TABLE it_fx_ctas AS " + dbtEmpty("SELECT nosuchfunc(id) AS x FROM it_fx_t"), noSuchInt},
		{"CREATE VIEW it_fx_v AS SELECT nosuchfunc(id) AS x FROM it_fx_e", noSuchInt},
		{"CREATE VIEW it_fx_v AS SELECT id FROM it_fx_t WHERE nosuchfunc(id) AND false", noSuchInt},
		{"CREATE OR REPLACE VIEW it_fx_ok AS SELECT nosuchfunc(id) AS id FROM it_fx_t", noSuchInt},
		{"CREATE TEMP VIEW it_fx_tv AS SELECT upper(s, s) AS x FROM it_fx_e", "UPPER expects 1 argument"},
		{"CREATE TABLE it_fx_c (id INTEGER CHECK (nosuchfunc(id) > 0))", noSuchInt},
		{"CREATE TABLE it_fx_c (id INTEGER, CONSTRAINT k CHECK (upper(id, id) = 'a'))", "UPPER expects 1 argument"},
		{"CREATE TEMP TABLE it_fx_c (id INTEGER CHECK (nosuchfunc(id) > 0))", noSuchInt},
		{"CREATE TABLE it_fx_c (id INTEGER CHECK (GROUPING(id) = 0))", "grouping operations are not allowed in check constraints"},
		{"CREATE TABLE it_fx_d (id INTEGER DEFAULT GROUPING(1))", "grouping operations are not allowed in DEFAULT expressions"},
		{"CREATE TABLE it_fx_d (id INTEGER DEFAULT nosuchfunc(1))", "function nosuchfunc(bigint) does not exist"},
		{"CREATE TABLE it_fx_d (id INTEGER DEFAULT nosuchfunc(NULL))", "function nosuchfunc(unknown) does not exist"},
		{"CREATE TABLE it_fx_d (s VARCHAR DEFAULT upper(NULL, NULL))", "UPPER expects 1 argument"},
		{"CREATE TABLE it_fx_d (id INTEGER DEFAULT CASE WHEN true THEN 1 ELSE nosuchfunc(2) END)", "function nosuchfunc(bigint) does not exist"},
		{"ALTER TABLE it_fx_t ADD COLUMN c INTEGER DEFAULT nosuchfunc(NULL)", "function nosuchfunc(unknown) does not exist"},
		{"ALTER TABLE it_fx_t ADD COLUMN c INTEGER CHECK (nosuchfunc(c) > 0)", noSuchInt},
		{"ALTER TABLE it_fx_e ADD COLUMN c INTEGER CHECK (nosuchfunc(c) > 0)", noSuchInt},
		{"ALTER TABLE it_fx_t ADD CONSTRAINT k CHECK (nosuchfunc(id) > 0)", noSuchInt},
		{"ALTER TABLE it_fx_e ADD CHECK (nosuchfunc(id) > 0)", noSuchInt},
		{"ALTER TABLE it_fx_t ALTER COLUMN s SET DEFAULT nosuchfunc(NULL)", "function nosuchfunc(unknown) does not exist"},
	} {
		h.expectErrorText(c.sql, c.err)
		if strings.HasPrefix(c.sql, "SELECT") || strings.HasPrefix(c.sql, "WITH") {
			h.expectErrorText(dbtEmpty(c.sql), c.err)
			h.expectSchemaErrorText(c.sql, c.err)
		}
	}
	h.expectUnchanged()
	h.expectRows("SELECT * FROM it_fx_ok ORDER BY id", "1", "2")
	for _, name := range []string{"it_fx_v", "it_fx_tv", "it_fx_c", "it_fx_d", "it_fx_ctas"} {
		h.expectErrorText("SELECT * FROM "+name, fmt.Sprintf(`table "public".%q does not exist`, name))
	}
}

// Postgres's order: a statement's FROM items are resolved first, then its
// expressions left to right, each call after its arguments. So an unknown
// column in a call's arguments is reported before the call's own error,
// and an error in an inner call before the outer one's.
func TestSQLFunctionsErrorOrder(t *testing.T) {
	h := newSQLHarness(t)
	h.setupFuncs()
	for _, c := range []struct{ sql, err string }{
		{"SELECT nosuchfunc(id) FROM it_fx_nosuchtable", `table "public"."it_fx_nosuchtable" does not exist`},
		{"SELECT nosuchfunc(nosuchcol) FROM it_fx_t", `column "nosuchcol" does not exist in table "it_fx_t"`},
		{"SELECT upper(nosuchcol, 1) FROM it_fx_t", `column "nosuchcol" does not exist in table "it_fx_t"`},
		{"SELECT nosuchfunc(id) OVER (ORDER BY nosuchcol) FROM it_fx_t", `column "nosuchcol" does not exist in table "it_fx_t"`},
		{"SELECT nosuchfunc(1), nosuchcol FROM it_fx_t", "function nosuchfunc(bigint) does not exist"},
		{"SELECT nosuchcol, nosuchfunc(1) FROM it_fx_t", `column "nosuchcol" does not exist in table "it_fx_t"`},
		{"SELECT nosuchfunc(1) + nosuchcol FROM it_fx_t", "function nosuchfunc(bigint) does not exist"},
		{"SELECT upper(nosuchfunc(1), 2) FROM it_fx_t", "function nosuchfunc(bigint) does not exist"},
		{"SELECT nosuchfunc(upper(1, 2)) FROM it_fx_t", "UPPER expects 1 argument"},
		{"SELECT otherfunc(nosuchfunc(id)) FROM it_fx_t", noSuchInt},
		// The function, before what is wrong with its use.
		{"SELECT nosuchfunc(s) FILTER (WHERE true) FROM it_fx_t", "function nosuchfunc(varchar) does not exist"},
		{"SELECT upper(s) FILTER (WHERE true) FROM it_fx_t", "FILTER specified, but UPPER is not an aggregate function"},
		{"SELECT nosuchfunc(s) IGNORE NULLS FROM it_fx_t", "function nosuchfunc(varchar) does not exist"},
		{"SELECT id FROM it_fx_t WHERE nosuchfunc(COUNT(*)) > 0", "aggregates are not allowed in WHERE"},
		{"SELECT 1 FROM it_fx_t WHERE nosuchfunc(id) = 1 GROUP BY nosuchcol", `column "nosuchcol" does not exist in table "it_fx_t"`},
	} {
		h.expectErrorText(c.sql, c.err)
	}
}

// Errors that depend on values are raised only for rows that are read, as
// before: none for an empty table or a WHERE that is never true.
func TestSQLFunctionsValueErrors(t *testing.T) {
	h := newSQLHarness(t)
	h.setupFuncs()
	for _, c := range []struct{ expr, err string }{
		{"1 / (id - id)", "division by zero"},
		{"LOG(id - id)", "LOG: cannot take logarithm of zero"},
		{"CAST(s AS INTEGER)", `invalid decimal "a"`},
		{"SQRT(-id)", "SQRT: cannot take square root of a negative number"},
		{"MAKE_DATE(2024, 13, id)", "date field value out of range: 2024-13-01"},
		{"ROUND(n, s)", `ROUND: the number of decimal places must be an integer: invalid decimal "a"`},
	} {
		h.expectErrorText("SELECT "+c.expr+" FROM it_fx_t", c.err)
		h.expectRows("SELECT " + c.expr + " FROM it_fx_e")
		h.expectRows("SELECT " + c.expr + " FROM it_fx_t WHERE false")
		h.expectNoRows(dbtEmpty("SELECT "+c.expr+" AS v FROM it_fx_t"), schemaText(h.expectRows("SELECT "+c.expr+" AS v FROM it_fx_e")))
		if _, err := h.executeSchema("SELECT " + c.expr + " FROM it_fx_t"); err != nil {
			t.Errorf("ExecuteSchema(%s): %v", c.expr, err)
		}
	}
	// A CHECK whose value can't be computed for a row fails that row only.
	h.exec("CREATE TABLE it_fx_c (id INTEGER CHECK (LOG(id) >= 0))")
	h.exec("INSERT INTO it_fx_c VALUES (1), (NULL)")
	h.expectErrorText("INSERT INTO it_fx_c VALUES (2), (0)", "LOG: cannot take logarithm of zero")
	h.expectRows("SELECT id FROM it_fx_c ORDER BY id", "1", "NULL")
}

// Function names are case-insensitive, quoted and schema-qualified names
// are still not calls, aliases keep working, and aggregates keep working as
// window functions and with FILTER and WITHIN GROUP.
func TestSQLFunctionsNamesAndForms(t *testing.T) {
	h := newSQLHarness(t)
	h.setupFuncs()
	h.expectRows("SELECT UpPeR(s), lower('A'), Lcase('B'), UCASE(s), LEN(s), CEILING(n), POW(2, 2), IFNULL(d, DATE '2000-01-01'), NVL(s, '-') FROM it_fx_t ORDER BY id",
		"A|a|b|A|1|2|4|2024-01-01|a", "X|a|b|X|1|0|4|2000-01-01|x")
	h.expectRows(`SELECT DAYOFMONTH(d), DATE_DIFF('day', d, DATE '2024-01-03'), TIMESTAMPDIFF(day, d, DATE '2024-01-03'),
		TIMESTAMPADD(day, 1, d), EXTRACT(month FROM d), TRIM(LEADING 'a' FROM s), POSITION('x' IN s), SUBSTRING(s FROM 1 FOR 1) FROM it_fx_t ORDER BY id`,
		"1|2|2|2024-01-02|1||0|a", "NULL|NULL|NULL|NULL|NULL|x|1|x")
	h.expectErrorText("SELECT NoSuchFunc(id) FROM it_fx_t", noSuchInt)
	h.expectErrorText(`SELECT "upper"(s) FROM it_fx_t`, `syntax error: unexpected "("`)
	h.expectErrorText(`SELECT pg_catalog.upper(s) FROM it_fx_t`, `syntax error: unexpected "("`)
	h.expectRows(`SELECT * FROM "generate_series"(1, 2)`, "1", "2")
	h.expectErrorText(`SELECT * FROM nosuchfunc(1)`, "table function nosuchfunc is not supported (only GENERATE_SERIES is)")
	// The internal functions the parser builds for operators and special
	// forms can't be called by name.
	h.expectErrorText("SELECT __is_json(s) FROM it_fx_t", "function __is_json(varchar) does not exist")
	h.expectErrorText("SELECT __INTERVAL(1, 'day') FROM it_fx_e", "function __interval(bigint, varchar) does not exist")
	h.expectErrorText("CREATE TABLE it_fx_d (s VARCHAR DEFAULT __json('1'))", "function __json(varchar) does not exist")
	h.expectRows(`SELECT s IS JSON, '{}'::json, INTERVAL 1 DAY = INTERVAL '24 hours', LIKE(s, 'a%'), s ILIKE 'A' FROM it_fx_t ORDER BY id`,
		"false|{}|true|true|true", "false|{}|true|false|false")

	for _, table := range []string{"it_fx_t", "it_fx_e"} {
		want := []string{"1|1|a|x-a|1.5|x|1.5|true|false"}
		if table == "it_fx_e" {
			want = []string{"0|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL"}
		}
		h.expectRows(`SELECT COUNT(*) FILTER (WHERE id > 1), SUM(id) FILTER (WHERE b), STRING_AGG(s, ',' ORDER BY id) FILTER (WHERE id = 1),
			LISTAGG(s, '-') WITHIN GROUP (ORDER BY id DESC), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY id),
			MODE() WITHIN GROUP (ORDER BY s DESC), MEDIAN(id), BOOL_OR(b), EVERY(b) FROM `+table, want...)
		want = []string{"1|1|1|a|NULL|2|1", "2|3|1|a,x|1|1|2"}
		if table == "it_fx_e" {
			want = nil
		}
		h.expectRows(`SELECT id, SUM(id) OVER (ORDER BY id), COUNT(*) FILTER (WHERE b) OVER (), STRING_AGG(s, ',') OVER (ORDER BY id),
			LAG(id) OVER (ORDER BY id), ROW_NUMBER() OVER w, ANY_VALUE(id) OVER (ORDER BY id ROWS CURRENT ROW)
			FROM `+table+` WINDOW w AS (ORDER BY id DESC) ORDER BY id`, want...)
	}
	h.expectErrorText("SELECT SUM(id, id) OVER () FROM it_fx_e", "SUM expects 1 argument")
	h.expectErrorText("SELECT COUNT(id, s) OVER () FROM it_fx_e", "COUNT expects 1 argument or * with OVER")
	h.expectErrorText("SELECT COUNT(DISTINCT id, s) OVER () FROM it_fx_e", "DISTINCT is not supported in window functions")
	h.expectErrorText("SELECT COUNT(id, s) FROM it_fx_e", "COUNT of more than one argument requires DISTINCT")
	h.expectErrorText("SELECT upper(s) OVER () FROM it_fx_e", "UPPER is not a window function or an aggregate (supported with OVER: "+
		"ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, "+
		"COUNT, SUM, AVG, MIN, MAX, STRING_AGG, LISTAGG, BOOL_OR, BOOL_AND, EVERY, ANY_VALUE, "+
		"STDDEV_SAMP, STDDEV, STDDEV_POP, VAR_SAMP, VARIANCE, VAR_POP)")
	h.expectErrorText("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY id) OVER () FROM it_fx_e",
		"OVER is not supported for ordered-set aggregate PERCENTILE_CONT")
	h.expectErrorText("SELECT ROW_NUMBER() FROM it_fx_e", "window function ROW_NUMBER requires an OVER clause")
	h.expectErrorText("SELECT generate_series(1, 2) FROM it_fx_e", "generate_series is only supported in FROM (SELECT … FROM generate_series(…) AS g)")
	h.expectErrorText("SELECT BOOL_OR(id) FROM it_fx_e", "function bool_or(integer) does not exist")
	h.expectErrorText("SELECT STDDEV(s) OVER () FROM it_fx_e", "function stddev(varchar) does not exist")
	h.expectErrorText("SELECT * FROM generate_series(DATE '2024-01-01', DATE '2024-01-02') g", "function generate_series(date, date) does not exist")
}

// Objects stored before calls were checked: a view, a CHECK and a DEFAULT
// that call an unknown function. Reading the view, and writing rows the
// CHECK or the DEFAULT applies to, now fail whether or not there are rows;
// replacing or dropping the object fixes it.
func TestSQLFunctionsStoredBefore(t *testing.T) {
	h := newSQLHarness(t)
	h.setupFuncs()
	views := []string{"it_fx_oldv", "it_fx_olde"}
	h.dropViews(views...)
	h.dropTables("it_fx_oldc")
	t.Cleanup(func() { h.dropViews(views...) })
	raw := h.rawClient()
	st := &store{client: raw}
	for i, table := range []string{"it_fx_t", "it_fx_e"} {
		if err := st.putView(h.ctx, &viewMeta{Schema: defaultSchema, Name: views[i], SQL: "SELECT nosuchfunc(id) AS x FROM " + table,
			Columns: []columnMeta{{Name: "x", Type: typeString, Nullable: true}}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range views {
		msg := fmt.Sprintf(`view %q is no longer valid: function nosuchfunc(integer) does not exist`, v)
		h.expectErrorText("SELECT * FROM "+v, msg)
		h.expectErrorText("SELECT * FROM "+v+" WHERE false", msg)
		h.expectErrorText(dbtEmpty("SELECT * FROM "+v), msg)
		h.expectSchemaErrorText("SELECT * FROM "+v, msg)
	}
	// The metadata still lists it, and it can be replaced or dropped.
	h.expectRows("SELECT table_name, view_definition FROM information_schema.views WHERE table_name LIKE 'it_fx_old%' ORDER BY 1",
		"it_fx_olde|SELECT nosuchfunc(id) AS x FROM it_fx_e", "it_fx_oldv|SELECT nosuchfunc(id) AS x FROM it_fx_t")
	h.expectRows("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_fx_oldv'", "x|VARCHAR")
	h.exec("CREATE OR REPLACE VIEW it_fx_oldv AS SELECT upper(s) AS x FROM it_fx_t")
	h.expectRows("SELECT x FROM it_fx_oldv ORDER BY x", "A", "X")
	h.exec("DROP VIEW it_fx_olde")

	// A CHECK, and a DEFAULT, stored with an unknown function.
	h.exec("CREATE TABLE it_fx_oldc (id INTEGER, s VARCHAR)")
	h.exec("INSERT INTO it_fx_oldc VALUES (1, 'a')")
	if err := st.updateTable(h.ctx, defaultSchema, "it_fx_oldc", func(m *tableMeta) error {
		m.Checks = append(m.Checks, checkMeta{Name: "it_fx_oldc_check", Expr: "nosuchfunc(id) > 0"})
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	msg := `check constraint "it_fx_oldc_check" of relation "it_fx_oldc" can't be evaluated ` +
		`(function nosuchfunc(integer) does not exist); drop it with ALTER TABLE … DROP CONSTRAINT`
	h.expectErrorText("INSERT INTO it_fx_oldc VALUES (2, 'b')", msg)
	h.expectErrorText("INSERT INTO it_fx_oldc SELECT id, s FROM it_fx_e", msg)
	h.expectErrorText("UPDATE it_fx_oldc SET s = 'c' WHERE false", msg)
	h.exec("ALTER TABLE it_fx_oldc DROP CONSTRAINT it_fx_oldc_check")
	if err := st.updateTable(h.ctx, defaultSchema, "it_fx_oldc", func(m *tableMeta) error {
		m.Columns[1].Default = "nosuchfunc(1)"
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	h.expectErrorText("INSERT INTO it_fx_oldc (id) VALUES (2)", "function nosuchfunc(bigint) does not exist")
	h.exec("INSERT INTO it_fx_oldc VALUES (2, 'b')") // the default isn't needed
	h.exec("ALTER TABLE it_fx_oldc ALTER COLUMN s DROP DEFAULT")
	h.exec("INSERT INTO it_fx_oldc (id) VALUES (3)")
	h.expectRows("SELECT id, s FROM it_fx_oldc ORDER BY id", "1|a", "2|b", "3|NULL")
}

// information_schema.routines lists the registry's functions by each of
// their names, without the internal ones.
func TestSQLFunctionsRoutines(t *testing.T) {
	h := newSQLHarness(t)
	h.expectRows(`SELECT routine_catalog, routine_schema, routine_name, routine_type, function_kind, min_arguments, max_arguments, alias_of
		FROM information_schema.routines
		WHERE routine_name IN ('upper', 'ucase', 'round', 'count', 'row_number', 'lag', 'generate_series', 'coalesce', 'every')
		ORDER BY routine_name`,
		"redis|pg_catalog|coalesce|FUNCTION|SCALAR|0|NULL|NULL",
		"redis|pg_catalog|count|FUNCTION|AGGREGATE|1|NULL|NULL",
		"redis|pg_catalog|every|FUNCTION|AGGREGATE|1|1|bool_and",
		"redis|pg_catalog|generate_series|FUNCTION|TABLE|2|3|NULL",
		"redis|pg_catalog|lag|FUNCTION|WINDOW|1|3|NULL",
		"redis|pg_catalog|round|FUNCTION|SCALAR|1|2|NULL",
		"redis|pg_catalog|row_number|FUNCTION|WINDOW|0|0|NULL",
		"redis|pg_catalog|ucase|FUNCTION|SCALAR|1|1|upper",
		"redis|pg_catalog|upper|FUNCTION|SCALAR|1|1|NULL")
	// Every name of the registry's functions but the internal ones.
	names, kinds := 0, map[funcKind]int{}
	for i := range funcDefs {
		if d := &funcDefs[i]; !d.internal {
			names += 1 + len(d.aliases)
			kinds[d.kind] += 1 + len(d.aliases)
		}
	}
	h.expectRows("SELECT COUNT(*), COUNT(DISTINCT routine_name) FROM information_schema.routines", fmt.Sprintf("%d|%d", names, names))
	h.expectRows(`SELECT COUNT(*) FROM information_schema.routines
		WHERE STARTS_WITH(routine_name, '__') OR routine_name IN ('~', '~*', 'similar to', '->', '#>>')`, "0")
	h.expectRows("SELECT function_kind, COUNT(*) FROM information_schema.routines GROUP BY function_kind ORDER BY 1",
		fmt.Sprint("AGGREGATE|", kinds[aggregateKind]), fmt.Sprint("SCALAR|", kinds[scalarKind]),
		fmt.Sprint("TABLE|", kinds[tableKind]), fmt.Sprint("WINDOW|", kinds[windowKind]))
	if names < 150 || kinds[tableKind] != 1 {
		t.Errorf("%d names, kinds %v", names, kinds)
	}
	h.expectNoRows(dbtEmpty("SELECT routine_name, max_arguments FROM information_schema.routines"), "routine_name utf8, max_arguments int32")
}
