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

// Integration tests for ALTER TABLE with several actions (issue #91).

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// indexAttrs lists the attributes of a table's search index, in order.
func (h *sqlHarness) indexAttrs(raw goredis.UniversalClient, schema, table string) string {
	h.t.Helper()
	st := &store{client: raw}
	meta, err := st.getTable(h.ctx, schema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	reply, err := st.searchDo(h.ctx, meta.index(), "FT.INFO", meta.index()).Result()
	if err != nil {
		h.t.Fatal(err)
	}
	var names []string
	info := reply.([]any)
	for i := 0; i+1 < len(info); i += 2 {
		if info[i] != "attributes" {
			continue
		}
		for _, a := range info[i+1].([]any) {
			parts := a.([]any)
			for j := 0; j+1 < len(parts); j += 2 {
				if parts[j] == "identifier" {
					names = append(names, parts[j+1].(string))
				}
			}
		}
	}
	return strings.Join(names, " ")
}

// rawMeta returns a table's metadata as stored.
func (h *sqlHarness) rawMeta(raw goredis.UniversalClient, schema, table string) string {
	h.t.Helper()
	s, err := raw.Get(h.ctx, metaKey(schema, table)).Result()
	if err != nil {
		h.t.Fatal(err)
	}
	return s
}

// dbt-core's alter_relation_add_remove_columns, as on_schema_change and
// snapshots run it: every added and removed column in one ALTER TABLE.
func TestSQLAlterMultiDbt(t *testing.T) {
	h := newSQLHarness(t)
	h.exec(`CREATE SCHEMA IF NOT EXISTS it_am_s`)
	h.dropTables("it_am_s.osc")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_am_s.osc (id integer, g varchar)`)
	h.exec(`INSERT INTO it_am_s.osc VALUES (1, 'a'), (2, 'b')`)

	// Two added columns (append_new_columns), dbt's exact SQL.
	h.exec(`alter table it_am_s.osc add column "extra" character varying(256), add column "extra2" BIGINT`)
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns
		WHERE table_schema = 'it_am_s' AND table_name = 'osc' ORDER BY ordinal_position`,
		"id|INTEGER", "g|VARCHAR", "extra|VARCHAR(256)", "extra2|BIGINT")
	h.expectRows(`SELECT id, g, extra, extra2 FROM it_am_s.osc ORDER BY id`, "1|a|NULL|NULL", "2|b|NULL|NULL")
	if got := h.indexAttrs(raw, "it_am_s", "osc"); got != "__rowid id g extra extra2" {
		t.Errorf("index attributes: %q", got)
	}
	h.exec(`INSERT INTO it_am_s.osc VALUES (3, 'c', 'x', 30)`)
	h.expectErrorText(`INSERT INTO it_am_s.osc VALUES (4, 'd', REPEAT('x', 257), 40)`, `value too long for type character varying(256)`)
	h.expectRows(`SELECT id FROM it_am_s.osc WHERE extra = 'x' AND extra2 = 30`, "3")

	// One added and one removed (sync_all_columns).
	h.exec(`alter table it_am_s.osc add column "extra3" integer, drop column "g"`)
	h.expectColumns(`SELECT * FROM it_am_s.osc ORDER BY id`, "id|extra|extra2|extra3",
		"1|NULL|NULL|NULL", "2|NULL|NULL|NULL", "3|x|30|NULL")
	waitFor(t, "dropped column cleanup", func() bool { return h.rowFields(raw, "it_am_s", "osc")["g"] == 0 })
	h.exec(`UPDATE it_am_s.osc SET extra3 = id * 10`)
	h.expectRows(`SELECT id FROM it_am_s.osc WHERE extra3 >= 20 ORDER BY id`, "2", "3")

	// Two removed.
	h.exec(`alter table it_am_s.osc drop column "extra", drop column "extra2"`)
	h.expectColumns(`SELECT * FROM it_am_s.osc ORDER BY id`, "id|extra3", "1|10", "2|20", "3|30")
	waitFor(t, "dropped columns cleanup", func() bool {
		f := h.rowFields(raw, "it_am_s", "osc")
		return f["extra"] == 0 && f["extra2"] == 0
	})
	meta, err := (&store{client: raw}).getTable(h.ctx, "it_am_s", "osc")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(meta.RetiredFields, []string{"g", "extra", "extra2"}) {
		t.Errorf("retired fields: %q", meta.RetiredFields)
	}
}

// Actions apply in the order written, each seeing what the ones before it
// did, with constraints and defaults.
func TestSQLAlterMultiMixed(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_am_mix", "it_am_one")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_am_mix (id integer, v integer)`)
	h.exec(`INSERT INTO it_am_mix VALUES (1, 10), (2, NULL)`)

	// A column, then a CHECK on it, which the rows pass with its default.
	h.exec(`ALTER TABLE it_am_mix ADD COLUMN w integer DEFAULT 5, ADD CONSTRAINT w_pos CHECK (w > 0)`)
	h.expectRows(`SELECT id, v, w FROM it_am_mix ORDER BY id`, "1|10|5", "2|NULL|5")
	h.expectChecks(raw, "it_am_mix", "w_pos: w > 0")

	// Two unnamed CHECKs, named as Postgres names them; a column with its
	// own CHECK; a new default; and a constraint dropped.
	h.exec(`ALTER TABLE it_am_mix ADD CHECK (v > 0), ADD CHECK (v < 100),
		ADD COLUMN x integer CHECK (x IS NULL OR x > id), ALTER COLUMN v SET DEFAULT 7, DROP CONSTRAINT w_pos`)
	h.expectChecks(raw, "it_am_mix", "it_am_mix_v_check: v > 0", "it_am_mix_v_check1: v < 100", "it_am_mix_check: x IS NULL OR x > id")
	h.exec(`INSERT INTO it_am_mix (id) VALUES (3)`)
	h.expectRows(`SELECT id, v, w, x FROM it_am_mix ORDER BY id`, "1|10|5|NULL", "2|NULL|5|NULL", "3|7|5|NULL")
	h.exec(`INSERT INTO it_am_mix (id, w) VALUES (4, 0)`) // w_pos is gone
	h.expectErrorText(`INSERT INTO it_am_mix (id, x) VALUES (5, 1)`,
		`new row for relation "it_am_mix" violates check constraint "it_am_mix_check"`)
	h.exec(`DELETE FROM it_am_mix WHERE id = 4`)

	// A default set on a column added before it: existing rows read NULL,
	// new rows the default. A default dropped from a column added before
	// it: existing rows keep it as their missing value.
	h.exec(`ALTER TABLE it_am_mix ADD COLUMN y integer, ALTER COLUMN y SET DEFAULT 3,
		ADD COLUMN z integer DEFAULT 9, ALTER COLUMN z DROP DEFAULT`)
	h.exec(`INSERT INTO it_am_mix (id) VALUES (6)`)
	h.expectRows(`SELECT id, y, z FROM it_am_mix ORDER BY id`, "1|NULL|9", "2|NULL|9", "3|NULL|9", "6|3|NULL")
	if got := h.columnDefs("it_am_mix"); strings.Join(got, " ") != "id= v=7 w=5 x= y=3 z=" {
		t.Errorf("defaults: %q", got)
	}

	// Dropping a column and adding one with its name: the new column is
	// NULL for every row, and the old one's CHECK goes with it.
	h.exec(`UPDATE it_am_mix SET x = id + 1`)
	h.exec(`ALTER TABLE it_am_mix DROP COLUMN x, ADD COLUMN x varchar`)
	h.expectRows(`SELECT id, x FROM it_am_mix ORDER BY id`, "1|NULL", "2|NULL", "3|NULL", "6|NULL")
	h.expectChecks(raw, "it_am_mix", "it_am_mix_v_check: v > 0", "it_am_mix_v_check1: v < 100")
	if f := h.tableColumn(raw, "it_am_mix", "x").field(); f != "x_2" {
		t.Errorf("field of the new x: %q", f)
	}
	h.exec(`UPDATE it_am_mix SET x = 'new' WHERE id = 1`)
	h.expectRows(`SELECT id FROM it_am_mix WHERE x = 'new'`, "1")

	// A CHAR(n) default, padded for the existing rows, with a CHECK on it.
	h.exec(`ALTER TABLE it_am_mix ADD COLUMN code char(4) DEFAULT 'ab', ADD CONSTRAINT code_len CHECK (LENGTH(code) = 2)`)
	h.expectRows(`SELECT id, code, LENGTH(code) FROM it_am_mix WHERE code = 'ab' ORDER BY id`,
		"1|ab  |2", "2|ab  |2", "3|ab  |2", "6|ab  |2")
	h.exec(`ALTER TABLE it_am_mix DROP COLUMN code`)

	// IF [NOT] EXISTS skip their action, and PRIMARY KEY / UNIQUE are
	// accepted and ignored, in a list as alone.
	h.exec(`ALTER TABLE it_am_mix ADD COLUMN IF NOT EXISTS w integer, DROP COLUMN IF EXISTS nope,
		ADD PRIMARY KEY (id), ADD UNIQUE (v), DROP CONSTRAINT IF EXISTS nope, ADD COLUMN q boolean`)
	h.expectColumns(`SELECT * FROM it_am_mix WHERE id = 1`, "id|v|w|y|z|x|q", "1|10|5|NULL|9|new|NULL")

	// The table's only column dropped and another added: the check for an
	// empty table is on the result.
	h.exec(`CREATE TABLE it_am_one (a integer)`)
	h.exec(`INSERT INTO it_am_one VALUES (1)`)
	h.exec(`ALTER TABLE it_am_one DROP COLUMN a, ADD COLUMN b text`)
	h.expectColumns(`SELECT * FROM it_am_one`, "b", "NULL")
	// A column added and dropped again in one statement leaves nothing:
	// no index attribute, no retired field.
	h.exec(`ALTER TABLE it_am_one ADD COLUMN c integer, DROP COLUMN c`)
	h.expectColumns(`SELECT * FROM it_am_one`, "b", "NULL")
	if got := h.indexAttrs(raw, defaultSchema, "it_am_one"); got != "__rowid a b" {
		t.Errorf("index attributes: %q", got)
	}
	meta, err := (&store{client: raw}).getTable(h.ctx, defaultSchema, "it_am_one")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(meta.RetiredFields, []string{"a"}) {
		t.Errorf("retired fields: %q", meta.RetiredFields)
	}
	h.expectErrorText(`ALTER TABLE it_am_one DROP COLUMN b`, `cannot drop the only column of table "it_am_one"`)
}

// An action that fails leaves the table as it was: metadata, index, rows.
func TestSQLAlterMultiAtomic(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_am_atom")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_am_atom (id integer, v integer, s varchar)`)
	h.exec(`INSERT INTO it_am_atom VALUES (1, 10, 'a'), (2, -5, 'b')`)
	h.exec(`ALTER TABLE it_am_atom ADD CONSTRAINT id_pos CHECK (id > 0)`)
	meta, attrs := h.rawMeta(raw, defaultSchema, "it_am_atom"), h.indexAttrs(raw, defaultSchema, "it_am_atom")

	for _, c := range []struct{ sql, want string }{
		// A CHECK the rows fail, after two added columns.
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer, ADD COLUMN n2 varchar, ADD CONSTRAINT v_pos CHECK (v > 0)`,
			`check constraint "v_pos" of relation "it_am_atom" is violated by some row`},
		// A CHECK on an added column's default and a stored column.
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer DEFAULT 0, ADD CHECK (n1 + v > 0)`,
			`check constraint "it_am_atom_check" of relation "it_am_atom" is violated by some row`},
		// A CHECK on the added column alone, which every row fails.
		{`ALTER TABLE it_am_atom DROP COLUMN s, ADD COLUMN n1 integer DEFAULT -1 CHECK (n1 >= 0)`,
			`check constraint "it_am_atom_n1_check" of relation "it_am_atom" is violated by some row`},
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer, DROP COLUMN nope`,
			`column "nope" does not exist in table "it_am_atom"`},
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer, ADD COLUMN N1 varchar`,
			`column "N1" already exists in table "it_am_atom"`},
		{`ALTER TABLE it_am_atom DROP COLUMN s, ADD COLUMN n1 integer DEFAULT 'abc'`,
			`column "n1": invalid decimal "abc"`},
		// String lengths: a DEFAULT too long for the added column, and a
		// new DEFAULT too long for a column added before it.
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer, ADD COLUMN n2 varchar(3) DEFAULT 'toolong'`,
			`value too long for type character varying(3)`},
		{`ALTER TABLE it_am_atom ADD COLUMN n2 char(3) DEFAULT 'abc', ALTER COLUMN n2 SET DEFAULT 'abcd'`,
			`value too long for type character(3)`},
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer, ALTER COLUMN nope SET DEFAULT 1`,
			`column "nope" does not exist in table "it_am_atom"`},
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer, DROP CONSTRAINT nope`,
			`constraint "nope" of relation "it_am_atom" does not exist`},
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer, ADD CONSTRAINT c CHECK (n1 > 0), ADD CONSTRAINT C CHECK (v > 0)`,
			`constraint "C" for relation "it_am_atom" already exists`},
		{`ALTER TABLE it_am_atom DROP COLUMN id, DROP COLUMN v, DROP COLUMN s`,
			`cannot drop all the columns of table "it_am_atom"`},
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer, ADD COLUMN n2 integer NOT NULL`,
			`cannot add a NOT NULL column without a non-NULL DEFAULT: existing rows would have no value`},
		{`ALTER TABLE it_am_atom ADD COLUMN n1 integer, ADD COLUMN n2 float DEFAULT random()`,
			`ADD COLUMN with a volatile DEFAULT is not supported: existing rows would each need their own value`},
		// Written order: a column dropped before a CHECK on it, and a CHECK
		// on a column added after it.
		{`ALTER TABLE it_am_atom DROP COLUMN v, ADD CONSTRAINT c CHECK (v > 0)`,
			`column "v" does not exist in table "it_am_atom"`},
		{`ALTER TABLE it_am_atom ADD CONSTRAINT c CHECK (n1 > 0), ADD COLUMN n1 integer`,
			`column "n1" does not exist in table "it_am_atom"`},
		// A DROP COLUMN that would drop a CHECK, then a failure.
		{`ALTER TABLE it_am_atom DROP COLUMN id, ADD COLUMN n1 integer, ADD COLUMN n1 integer`,
			`column "n1" already exists in table "it_am_atom"`},
	} {
		h.expectErrorText(c.sql, c.want)
		if got := h.rawMeta(raw, defaultSchema, "it_am_atom"); got != meta {
			t.Errorf("%s: metadata changed\n got: %s\nwant: %s", c.sql, got, meta)
		}
		if got := h.indexAttrs(raw, defaultSchema, "it_am_atom"); got != attrs {
			t.Errorf("%s: index attributes %q, want %q", c.sql, got, attrs)
		}
		h.expectColumns(`SELECT * FROM it_am_atom ORDER BY id`, "id|v|s", "1|10|a", "2|-5|b")
	}
	if f := h.rowFields(raw, defaultSchema, "it_am_atom"); f["id"] != 2 || f["v"] != 2 || f["s"] != 2 || len(f) != 4 {
		t.Errorf("row fields: %v", f)
	}
	h.expectChecks(raw, "it_am_atom", "id_pos: id > 0")
}

// RENAME takes no list, as in Postgres; a view takes only RENAME TO.
func TestSQLAlterMultiErrors(t *testing.T) {
	h := newSQLHarness(t)
	h.dropViews("it_am_errv")
	h.dropTables("it_am_err")
	h.exec(`CREATE TABLE it_am_err (id integer, v integer)`)
	h.exec(`CREATE VIEW it_am_errv AS SELECT id FROM it_am_err`)
	for _, c := range []struct{ sql, want string }{
		{`ALTER TABLE it_am_err RENAME TO it_am_err2, ADD COLUMN x integer`, `syntax error at or near ","`},
		{`ALTER TABLE it_am_err RENAME COLUMN v TO w, ADD COLUMN x integer`, `syntax error at or near ","`},
		{`ALTER TABLE it_am_err RENAME v TO w, RENAME id TO k`, `syntax error at or near ","`},
		{`ALTER TABLE it_am_err ADD COLUMN x integer, RENAME COLUMN v TO w`, `syntax error at or near "RENAME"`},
		{`alter table it_am_err add column x integer, rename to it_am_err2`, `syntax error at or near "rename"`},
		{`ALTER TABLE it_am_err ADD COLUMN x integer,`, `syntax error at end of input`},
		{`ALTER TABLE it_am_err ADD COLUMN x integer, VALIDATE CONSTRAINT c`,
			`unsupported ALTER TABLE action near "VALIDATE" (supported: RENAME TO, RENAME COLUMN, ADD COLUMN, DROP COLUMN, ALTER COLUMN, ADD CONSTRAINT, DROP CONSTRAINT)`},
		{`ALTER TABLE it_am_err ADD COLUMN x integer, ALTER COLUMN v TYPE bigint`,
			`unsupported ALTER COLUMN action near "TYPE" (supported: SET DEFAULT, DROP DEFAULT)`},
		{`ALTER TABLE it_am_err ADD COLUMN x integer ADD COLUMN y integer`, `syntax error: unexpected "ADD"`},
		{`ALTER TABLE it_am_errv ADD COLUMN x integer, ADD COLUMN y integer`,
			`"public"."it_am_errv" is a view; ALTER TABLE on a view supports only RENAME TO`},
		{`ALTER TABLE it_am_errv DROP COLUMN id`,
			`"public"."it_am_errv" is a view; ALTER TABLE on a view supports only RENAME TO`},
		{`ALTER VIEW it_am_errv RENAME TO it_am_errv2, ADD COLUMN x integer`, `syntax error at or near ","`},
		{`ALTER VIEW it_am_errv ADD COLUMN x integer, ADD COLUMN y integer`, `ALTER VIEW supports only RENAME TO`},
		{`ALTER VIEW it_am_errv RENAME COLUMN id TO k`, `ALTER VIEW supports only RENAME TO`},
	} {
		h.expectErrorText(c.sql, c.want)
	}
	h.expectColumns(`SELECT * FROM it_am_err`, "id|v")
	h.expectColumns(`SELECT * FROM it_am_errv`, "id")
	// RENAME alone still works, for the table and the view.
	h.exec(`ALTER TABLE it_am_err RENAME COLUMN v TO w`)
	h.exec(`ALTER TABLE it_am_errv RENAME TO it_am_errv2`)
	h.exec(`ALTER VIEW it_am_errv2 RENAME TO it_am_errv`)
	h.expectColumns(`SELECT * FROM it_am_err`, "id|w")
}

// An index attribute that no column uses, as an ADD COLUMN whose metadata
// change failed after its FT.ALTER leaves, doesn't make adding a column
// with that name fail.
func TestSQLAlterLeftoverAttribute(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_am_left")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_am_left (id integer)`)
	st := &store{client: raw}
	meta, err := st.getTable(h.ctx, defaultSchema, "it_am_left")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.searchDo(h.ctx, meta.index(), "FT.ALTER", meta.index(), "SCHEMA", "ADD",
		"extra", "NUMERIC", "SORTABLE", "more", "TAG").Err(); err != nil {
		t.Fatal(err)
	}
	// One action (main failed with "Duplicate field in schema - extra"),
	// and several.
	h.exec(`ALTER TABLE it_am_left ADD COLUMN extra integer`)
	h.exec(`ALTER TABLE it_am_left ADD COLUMN other varchar, ADD COLUMN more integer`)
	for col, want := range map[string]string{"extra": "extra_2", "other": "other", "more": "more_2"} {
		if f := h.tableColumn(raw, "it_am_left", col).field(); f != want {
			t.Errorf("field of %s: %q, want %q", col, f, want)
		}
	}
	if got := h.indexAttrs(raw, defaultSchema, "it_am_left"); got != "__rowid id extra more extra_2 other more_2" {
		t.Errorf("index attributes: %q", got)
	}
	h.exec(`INSERT INTO it_am_left VALUES (1, 5, 'o', 7), (2, 6, 'p', 8)`)
	h.expectRows(`SELECT id FROM it_am_left WHERE extra = 5 AND other = 'o' AND more = 7`, "1")
	h.expectRows(`SELECT id, extra FROM it_am_left ORDER BY extra DESC`, "2|6", "1|5")
}

// Several connections changing a table's columns at once: every change is
// applied whole.
func TestSQLAlterMultiConcurrent(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_am_conc")
	h.exec(`CREATE TABLE it_am_conc (id integer)`)
	h.exec(`INSERT INTO it_am_conc VALUES (1), (2)`)
	const conns, each = 3, 6
	var wg sync.WaitGroup
	errs := make(chan error, conns*each)
	for c := 0; c < conns; c++ {
		hc := newSQLHarness(t)
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				sql := fmt.Sprintf(`ALTER TABLE it_am_conc ADD COLUMN a%d_%d integer DEFAULT %d,
					ADD COLUMN b%d_%d varchar, ADD CONSTRAINT c%d_%d CHECK (a%d_%d >= 0)`, c, i, i, c, i, c, i, c, i)
				st, err := hc.conn.NewStatement(hc.ctx)
				if err != nil {
					errs <- err
					return
				}
				if err := st.SetSqlQuery(hc.ctx, sql); err == nil {
					_, err = st.ExecuteUpdate(hc.ctx)
					if err != nil {
						errs <- fmt.Errorf("%s: %w", sql, err)
					}
				}
				_ = st.Close(hc.ctx)
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var want []string
	for c := 0; c < conns; c++ {
		for i := 0; i < each; i++ {
			want = append(want, fmt.Sprintf("a%d_%d|%d", c, i, i), fmt.Sprintf("b%d_%d|NULL", c, i))
		}
	}
	slices.Sort(want)
	var got []string
	raw := h.rawClient()
	meta, err := (&store{client: raw}).getTable(h.ctx, defaultSchema, "it_am_conc")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range meta.Columns[1:] {
		rows, _ := h.query(fmt.Sprintf(`SELECT %s FROM it_am_conc WHERE id = 1`, c.Name))
		got = append(got, c.Name+"|"+rows[0])
	}
	slices.Sort(got)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("columns\n got: %q\nwant: %q", got, want)
	}
	if len(meta.Checks) != conns*each {
		t.Errorf("%d checks, want %d", len(meta.Checks), conns*each)
	}
	attrs := strings.Fields(h.indexAttrs(raw, defaultSchema, "it_am_conc"))
	if len(attrs) != 2+2*conns*each {
		t.Errorf("index attributes: %q", attrs)
	}
}
