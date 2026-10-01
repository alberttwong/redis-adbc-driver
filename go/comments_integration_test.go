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

// Integration tests for COMMENT ON (comment.go).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func (h *sqlHarness) dropViews(names ...string) {
	h.t.Helper()
	drop := func() {
		for _, n := range names {
			h.exec("DROP VIEW IF EXISTS " + n)
		}
	}
	drop()
	h.t.Cleanup(drop)
}

// remarks lists GetObjects' remarks of the columns of a table or view as
// "column=remarks", NULL for none.
func (h *sqlHarness) remarks(schema, table string) []string {
	h.t.Helper()
	rdr, err := h.conn.GetObjects(h.ctx, adbc.ObjectDepthColumns, nil, &schema, &table, nil, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	defer rdr.Release()
	var out []string
	for rdr.Next() {
		var buf bytes.Buffer
		if err := array.RecordToJSON(rdr.RecordBatch(), &buf); err != nil {
			h.t.Fatal(err)
		}
		dec := json.NewDecoder(&buf)
		for dec.More() {
			var cat struct {
				Schemas []struct {
					Tables []struct {
						Name    string `json:"table_name"`
						Columns []struct {
							Name    string  `json:"column_name"`
							Remarks *string `json:"remarks"`
						} `json:"table_columns"`
					} `json:"db_schema_tables"`
				} `json:"catalog_db_schemas"`
			}
			if err := dec.Decode(&cat); err != nil {
				h.t.Fatal(err)
			}
			for _, s := range cat.Schemas {
				for _, t := range s.Tables {
					if t.Name != table {
						continue
					}
					for _, c := range t.Columns {
						r := "NULL"
						if c.Remarks != nil {
							r = *c.Remarks
						}
						out = append(out, c.Name+"="+r)
					}
				}
			}
		}
	}
	return out
}

// fieldRemarks lists the ARROW:FLIGHT:SQL:REMARKS metadata of the fields
// GetTableSchema returns as "column=remarks", NULL for a field without
// metadata.
func (h *sqlHarness) fieldRemarks(schema, table string) []string {
	h.t.Helper()
	s, err := h.conn.GetTableSchema(h.ctx, nil, &schema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	var out []string
	for _, f := range s.Fields() {
		r := "NULL"
		if f.HasMetadata() {
			i := f.Metadata.FindKey(remarksKey)
			if i < 0 || f.Metadata.Len() != 1 {
				h.t.Errorf("%s.%s: field %q has metadata %v", schema, table, f.Name, f.Metadata)
				continue
			}
			r = f.Metadata.Values()[i]
		}
		out = append(out, f.Name+"="+r)
	}
	return out
}

// expectComments checks a table's or view's comments wherever they are
// shown. want is its own comment followed by "column=comment" for each
// column, NULL for none: information_schema shows all of them, GetObjects and
// GetTableSchema the columns'.
func (h *sqlHarness) expectComments(schema, table string, want ...string) {
	h.t.Helper()
	tbl, _ := h.query(fmt.Sprintf(`SELECT COALESCE(comment, 'NULL') FROM information_schema.tables
		WHERE table_schema = '%s' AND table_name = '%s'`, schema, table))
	cols, _ := h.query(fmt.Sprintf(`SELECT column_name || '=' || COALESCE(comment, 'NULL') FROM information_schema.columns
		WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position`, schema, table))
	if got := append(tbl, cols...); strings.Join(got, "\n") != strings.Join(want, "\n") {
		h.t.Errorf("%s.%s: information_schema comments\n got: %q\nwant: %q", schema, table, got, want)
	}
	if got := h.remarks(schema, table); strings.Join(got, "\n") != strings.Join(want[1:], "\n") {
		h.t.Errorf("%s.%s: GetObjects remarks\n got: %q\nwant: %q", schema, table, got, want[1:])
	}
	if got := h.fieldRemarks(schema, table); strings.Join(got, "\n") != strings.Join(want[1:], "\n") {
		h.t.Errorf("%s.%s: GetTableSchema remarks\n got: %q\nwant: %q", schema, table, got, want[1:])
	}
}

// COMMENT ON TABLE and COLUMN, with every form of name; NULL and an empty
// string remove a comment.
func TestSQLCommentOnTable(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_cmt_t", "it_cmt_other")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_cmt_t (id INTEGER NOT NULL, "Fare Amount" NUMERIC(10,2), note VARCHAR)`)
	h.exec(`CREATE TABLE it_cmt_other (id INTEGER)`)
	h.expectComments("public", "it_cmt_t", "NULL", "id=NULL", "Fare Amount=NULL", "note=NULL")

	h.exec(`COMMENT ON TABLE it_cmt_t IS 'Daily revenue per pickup date'`)
	h.exec(`COMMENT ON COLUMN it_cmt_t.id IS 'Trip id'`)
	h.exec(`COMMENT ON COLUMN public.it_cmt_t."Fare Amount" IS 'It''s in USD'`)
	h.exec("COMMENT ON COLUMN redis.public.it_cmt_t.NOTE IS 'Free text\nover two lines, ünïcode'")
	h.expectComments("public", "it_cmt_t", "Daily revenue per pickup date",
		"id=Trip id", "Fare Amount=It's in USD", "note=Free text\nover two lines, ünïcode")
	h.expectComments("public", "it_cmt_other", "NULL", "id=NULL")
	h.expectRows(`SELECT table_name, table_type, comment FROM information_schema.tables
		WHERE table_name IN ('it_cmt_t', 'it_cmt_other') ORDER BY table_name`,
		"it_cmt_other|BASE TABLE|NULL", "it_cmt_t|BASE TABLE|Daily revenue per pickup date")
	h.expectRows(`SELECT column_name FROM information_schema.columns WHERE comment LIKE 'Trip%' AND table_name = 'it_cmt_t'`, "id")

	// A new comment replaces the old one; NULL and '' remove it, and so its
	// field in the metadata.
	h.exec(`COMMENT ON TABLE redis.public.it_cmt_t IS 'Revenue'`)
	h.exec(`COMMENT ON COLUMN it_cmt_t.id IS NULL`)
	h.exec(`COMMENT ON COLUMN it_cmt_t."Fare Amount" IS ''`)
	h.expectComments("public", "it_cmt_t", "Revenue", "id=NULL", "Fare Amount=NULL", "note=Free text\nover two lines, ünïcode")
	h.exec(`COMMENT ON TABLE public.it_cmt_t IS NULL`)
	h.exec(`COMMENT ON COLUMN it_cmt_t.note IS ''`)
	h.expectComments("public", "it_cmt_t", "NULL", "id=NULL", "Fare Amount=NULL", "note=NULL")
	if stored, err := raw.Get(h.ctx, metaKey("public", "it_cmt_t")).Result(); err != nil || strings.Contains(stored, `"comment"`) {
		t.Errorf("metadata after removing every comment: %s (%v)", stored, err)
	}
	// Setting a comment that is already absent is fine.
	h.exec(`COMMENT ON TABLE it_cmt_t IS NULL`)

	// The table works as before.
	h.exec(`COMMENT ON COLUMN it_cmt_t.note IS 'Note'`)
	h.exec(`INSERT INTO it_cmt_t VALUES (1, 2.50, 'x'), (2, NULL, 'y')`)
	h.expectRows(`SELECT * FROM it_cmt_t WHERE note = 'x'`, "1|2.50|x")
	h.expectRows(`SELECT COUNT(*) FROM it_cmt_t`, "2")
}

// COMMENT ON VIEW and a view's columns; CREATE OR REPLACE VIEW keeps them,
// as in Postgres, and DROP VIEW drops them.
func TestSQLCommentOnView(t *testing.T) {
	h := newSQLHarness(t)
	h.dropViews("it_cmt_v", "it_cmt_v2")
	h.dropTables("it_cmt_vbase")
	h.exec(`CREATE TABLE it_cmt_vbase (id INTEGER, amount NUMERIC(10,2), label VARCHAR)`)
	h.exec(`INSERT INTO it_cmt_vbase VALUES (1, 1.50, 'a'), (2, 2.50, 'b')`)
	h.exec(`COMMENT ON TABLE it_cmt_vbase IS 'Base'`)
	h.exec(`COMMENT ON COLUMN it_cmt_vbase.id IS 'Base id'`)

	// A view takes no comments from the table it reads.
	h.exec(`CREATE VIEW it_cmt_v AS SELECT id, amount, label FROM it_cmt_vbase`)
	h.expectComments("public", "it_cmt_v", "NULL", "id=NULL", "amount=NULL", "label=NULL")
	h.exec(`COMMENT ON VIEW it_cmt_v IS 'Amounts'`)
	h.exec(`COMMENT ON COLUMN it_cmt_v.id IS 'Id'`)
	h.exec(`COMMENT ON COLUMN public.it_cmt_v.amount IS 'Amount'`)
	h.expectComments("public", "it_cmt_v", "Amounts", "id=Id", "amount=Amount", "label=NULL")
	h.expectComments("public", "it_cmt_vbase", "Base", "id=Base id", "amount=NULL", "label=NULL")
	h.expectRows(`SELECT table_name, table_type, comment FROM information_schema.tables
		WHERE table_name IN ('it_cmt_v', 'it_cmt_vbase') ORDER BY table_name`,
		"it_cmt_v|VIEW|Amounts", "it_cmt_vbase|BASE TABLE|Base")
	h.expectRows(`SELECT id, amount FROM it_cmt_v WHERE amount > 2`, "2|2.50")

	// CREATE OR REPLACE VIEW keeps the view's comment and its columns'; a
	// column added at the end has none.
	h.exec(`CREATE OR REPLACE VIEW it_cmt_v AS SELECT id, amount, label, label AS extra FROM it_cmt_vbase WHERE id > 1`)
	h.expectComments("public", "it_cmt_v", "Amounts", "id=Id", "amount=Amount", "label=NULL", "extra=NULL")
	h.expectRows(`SELECT id, extra FROM it_cmt_v`, "2|b")
	// Postgres can't rename a column that way; here the comments follow
	// the column names.
	h.exec(`CREATE OR REPLACE VIEW it_cmt_v (key, amount) AS SELECT id, amount FROM it_cmt_vbase`)
	h.expectComments("public", "it_cmt_v", "Amounts", "key=NULL", "amount=Amount")

	// RENAME TO keeps them; a dropped view's go with it.
	h.exec(`ALTER VIEW it_cmt_v RENAME TO it_cmt_v2`)
	h.expectComments("public", "it_cmt_v2", "Amounts", "key=NULL", "amount=Amount")
	h.exec(`ALTER TABLE it_cmt_v2 RENAME TO it_cmt_v`)
	h.expectComments("public", "it_cmt_v", "Amounts", "key=NULL", "amount=Amount")
	h.exec(`DROP VIEW it_cmt_v`)
	h.exec(`CREATE VIEW it_cmt_v AS SELECT id, amount FROM it_cmt_vbase`)
	h.expectComments("public", "it_cmt_v", "NULL", "id=NULL", "amount=NULL")
	h.exec(`CREATE OR REPLACE VIEW it_cmt_v AS SELECT id, amount FROM it_cmt_vbase`)
	h.expectComments("public", "it_cmt_v", "NULL", "id=NULL", "amount=NULL")
}

// Comments survive RENAME TO (also when it moves the rows) and RENAME
// COLUMN, and go away with DROP COLUMN and DROP TABLE; TRUNCATE keeps them.
func TestSQLCommentRename(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_cmt_r", "it_cmt_r2", "it_cmt_r3")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_cmt_r (id INTEGER, label VARCHAR, qty INTEGER)`)
	h.exec(`INSERT INTO it_cmt_r VALUES (1, 'a', 10), (2, 'b', 20)`)
	h.exec(`COMMENT ON TABLE it_cmt_r IS 'Orders'`)
	h.exec(`COMMENT ON COLUMN it_cmt_r.label IS 'Label'`)
	h.exec(`COMMENT ON COLUMN it_cmt_r.qty IS 'Quantity'`)

	h.exec(`ALTER TABLE it_cmt_r RENAME COLUMN label TO name`)
	h.expectComments("public", "it_cmt_r", "Orders", "id=NULL", "name=Label", "qty=Quantity")
	h.exec(`COMMENT ON COLUMN it_cmt_r.name IS 'Name'`)
	h.expectError(`COMMENT ON COLUMN it_cmt_r.label IS 'x'`, `column "label" of relation "it_cmt_r" does not exist`)
	h.exec(`ALTER TABLE it_cmt_r RENAME TO it_cmt_r2`)
	h.expectComments("public", "it_cmt_r2", "Orders", "id=NULL", "name=Name", "qty=Quantity")
	if prefix, _ := h.tableNames(raw, "public", "it_cmt_r2"); prefix != "public:it_cmt_r:" {
		t.Errorf("a plain rename moved the rows to %q", prefix)
	}
	// A new table with the old name has none.
	h.exec(`CREATE TABLE it_cmt_r (id INTEGER)`)
	h.expectComments("public", "it_cmt_r", "NULL", "id=NULL")
	h.exec(`DROP TABLE it_cmt_r`)

	// A dropped column's comment goes with it, also when a column of that
	// name is added again.
	h.exec(`ALTER TABLE it_cmt_r2 DROP COLUMN qty`)
	h.expectComments("public", "it_cmt_r2", "Orders", "id=NULL", "name=Name")
	h.exec(`ALTER TABLE it_cmt_r2 ADD COLUMN qty INTEGER`)
	h.expectComments("public", "it_cmt_r2", "Orders", "id=NULL", "name=Name", "qty=NULL")
	h.exec(`TRUNCATE it_cmt_r2`)
	h.expectComments("public", "it_cmt_r2", "Orders", "id=NULL", "name=Name", "qty=NULL")

	// With adbc.redis.rename_rekey, the rows move to the new name's keys and
	// the comments go with the metadata.
	h.exec(`INSERT INTO it_cmt_r2 VALUES (3, 'c', 30)`)
	h.exec(`COMMENT ON COLUMN it_cmt_r2.qty IS 'Quantity again'`)
	if err := h.setConnOption(OptionStringRenameRekey, "true"); err != nil {
		t.Fatal(err)
	}
	h.exec(`ALTER TABLE it_cmt_r2 RENAME TO it_cmt_r3`)
	if prefix, _ := h.tableNames(raw, "public", "it_cmt_r3"); prefix != "public:it_cmt_r3:" {
		t.Errorf("a re-keying rename left the rows under %q", prefix)
	}
	h.expectComments("public", "it_cmt_r3", "Orders", "id=NULL", "name=Name", "qty=Quantity again")
	h.expectRows(`SELECT id, name, qty FROM it_cmt_r3 WHERE name = 'c'`, "3|c|30")

	// DROP TABLE drops them.
	h.exec(`DROP TABLE it_cmt_r3`)
	h.exec(`CREATE TABLE it_cmt_r3 (id INTEGER, name VARCHAR, qty INTEGER)`)
	h.expectComments("public", "it_cmt_r3", "NULL", "id=NULL", "name=NULL", "qty=NULL")
}

// COMMENT ON isn't refused while a re-keying rename moves the rows, and the
// comment is kept by the switch, or by a rollback.
func TestSQLCommentDuringRekey(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_cmt_mv", "it_cmt_mv2")
	st := &store{client: h.rawClient()}
	h.exec(`CREATE TABLE it_cmt_mv (id INTEGER, v VARCHAR)`)
	h.exec(`INSERT INTO it_cmt_mv VALUES (1, 'a')`)

	job, _, err := st.beginRekey(h.ctx, "public", "it_cmt_mv", "it_cmt_mv2")
	if err != nil {
		t.Fatal(err)
	}
	h.exec(`COMMENT ON TABLE it_cmt_mv IS 'During a move rolled back'`)
	h.expectError(`INSERT INTO it_cmt_mv VALUES (2, 'b')`, "is being renamed")
	err = st.rollbackRekey(h.ctx, job)
	job.lease.release()
	if err != nil {
		t.Fatal(err)
	}
	h.expectComments("public", "it_cmt_mv", "During a move rolled back", "id=NULL", "v=NULL")

	job, start, err := st.beginRekey(h.ctx, "public", "it_cmt_mv", "it_cmt_mv2")
	if err != nil {
		t.Fatal(err)
	}
	defer job.lease.release()
	h.exec(`COMMENT ON COLUMN it_cmt_mv.v IS 'During the copy'`)
	if err := st.moveRows(h.ctx, job, start, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.exec(`COMMENT ON TABLE it_cmt_mv IS 'Before the switch'`)
	if _, err := st.switchRekey(h.ctx, job, start); err != nil {
		t.Fatal(err)
	}
	if err := st.finishRekey(h.ctx, job); err != nil {
		t.Fatal(err)
	}
	h.expectComments("public", "it_cmt_mv2", "Before the switch", "id=NULL", "v=During the copy")
	h.expectRows(`SELECT id, v FROM it_cmt_mv2 WHERE v = 'a'`, "1|a")
	h.expectError(`COMMENT ON TABLE it_cmt_mv IS 'x'`, `relation "it_cmt_mv" does not exist`)
}

// Names resolve as in other statements: an unqualified name means a
// temporary object first. Temporary objects' comments go with them.
func TestSQLCommentTemp(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("public.it_cmt_shadow")
	h.exec(`CREATE TABLE it_cmt_shadow (id INTEGER)`)
	h.exec(`CREATE TEMP TABLE it_cmt_shadow (id INTEGER, v VARCHAR)`)
	h.exec(`CREATE TEMP VIEW it_cmt_tv AS SELECT v FROM it_cmt_shadow`)

	h.exec(`COMMENT ON TABLE it_cmt_shadow IS 'temp'`)
	h.exec(`COMMENT ON COLUMN it_cmt_shadow.id IS 'temp id'`)
	h.exec(`COMMENT ON COLUMN pg_temp.it_cmt_shadow.v IS 'temp v'`)
	h.exec(`COMMENT ON TABLE public.it_cmt_shadow IS 'permanent'`)
	h.exec(`COMMENT ON COLUMN redis.public.it_cmt_shadow.id IS 'permanent id'`)
	h.exec(`COMMENT ON VIEW it_cmt_tv IS 'temp view'`)
	h.exec(`COMMENT ON COLUMN pg_temp.it_cmt_tv.v IS 'view v'`)
	h.expectComments("pg_temp", "it_cmt_shadow", "temp", "id=temp id", "v=temp v")
	h.expectComments("pg_temp", "it_cmt_tv", "temp view", "v=view v")
	h.expectComments("public", "it_cmt_shadow", "permanent", "id=permanent id")
	h.expectRows(`SELECT table_schema, table_name, table_type, comment FROM information_schema.tables
		WHERE table_name IN ('it_cmt_shadow', 'it_cmt_tv') ORDER BY table_schema, table_name`,
		"pg_temp|it_cmt_shadow|LOCAL TEMPORARY|temp", "pg_temp|it_cmt_tv|VIEW|temp view",
		"public|it_cmt_shadow|BASE TABLE|permanent")

	// Another connection doesn't see them; its unqualified name is the
	// permanent table.
	other := newSQLHarness(t)
	other.expectError(`COMMENT ON VIEW it_cmt_tv IS 'x'`, `relation "it_cmt_tv" does not exist`)
	other.expectError(`COMMENT ON TABLE pg_temp.it_cmt_shadow IS 'x'`, `relation "pg_temp.it_cmt_shadow" does not exist`)
	other.exec(`COMMENT ON TABLE it_cmt_shadow IS 'from another connection'`)
	h.expectComments("public", "it_cmt_shadow", "from another connection", "id=permanent id")
	h.expectComments("pg_temp", "it_cmt_shadow", "temp", "id=temp id", "v=temp v")

	// Dropped with the temporary objects.
	h.exec(`DROP VIEW it_cmt_tv`)
	h.exec(`DROP TABLE it_cmt_shadow`)
	h.exec(`CREATE TEMP TABLE it_cmt_shadow (id INTEGER, v VARCHAR)`)
	h.expectComments("pg_temp", "it_cmt_shadow", "NULL", "id=NULL", "v=NULL")
	h.expectComments("public", "it_cmt_shadow", "from another connection", "id=permanent id")
}

// Errors, with Postgres's wording; nothing is changed.
func TestSQLCommentErrors(t *testing.T) {
	h := newSQLHarness(t)
	h.dropViews("it_cmt_ev")
	h.dropTables("it_cmt_et")
	h.exec(`CREATE TABLE it_cmt_et (id INTEGER)`)
	h.exec(`CREATE VIEW it_cmt_ev AS SELECT id FROM it_cmt_et`)
	for sql, msg := range map[string]string{
		`COMMENT ON TABLE it_cmt_ev IS 'x'`:                 `"it_cmt_ev" is not a table`,
		`COMMENT ON VIEW it_cmt_et IS 'x'`:                  `"it_cmt_et" is not a view`,
		`COMMENT ON VIEW public.it_cmt_et IS NULL`:          `"it_cmt_et" is not a view`,
		`COMMENT ON TABLE it_cmt_nosuch IS 'x'`:             `relation "it_cmt_nosuch" does not exist`,
		`COMMENT ON VIEW public.it_cmt_nosuch IS 'x'`:       `relation "public.it_cmt_nosuch" does not exist`,
		`COMMENT ON TABLE redis.public.it_cmt_nosuch IS ''`: `relation "redis.public.it_cmt_nosuch" does not exist`,
		`COMMENT ON COLUMN it_cmt_nosuch.id IS 'x'`:         `relation "it_cmt_nosuch" does not exist`,
		`COMMENT ON TABLE pg_temp.it_cmt_nosuch IS 'x'`:     `relation "pg_temp.it_cmt_nosuch" does not exist`,
		`COMMENT ON TABLE it_cmt_nosuch_s.it_cmt_et IS 'x'`: `schema "it_cmt_nosuch_s" does not exist`,
		`COMMENT ON COLUMN it_cmt_et.nosuch IS 'x'`:         `column "nosuch" of relation "it_cmt_et" does not exist`,
		`COMMENT ON COLUMN public.it_cmt_ev.nosuch IS 'x'`:  `column "nosuch" of relation "it_cmt_ev" does not exist`,
		`COMMENT ON COLUMN it_cmt_et.__rowid IS 'x'`:        `column "__rowid" of relation "it_cmt_et" does not exist`,
		`COMMENT ON COLUMN it_cmt_et IS 'x'`:                "column name must be qualified",
		`COMMENT ON TABLE a.b.c.d IS 'x'`:                   "improper qualified name (too many dotted names): a.b.c.d",
		`COMMENT ON COLUMN a.b.c.d.e IS 'x'`:                "improper qualified name (too many dotted names): a.b.c.d.e",
		`COMMENT ON TABLE other.public.it_cmt_et IS 'x'`:    "cross-database references are not implemented: other.public.it_cmt_et",
		`COMMENT ON COLUMN other.public.it_cmt_et.id IS ''`: "cross-database references are not implemented: other.public.it_cmt_et.id",
		`COMMENT ON TABLE information_schema.tables IS 'x'`: "information_schema is read-only",
		`COMMENT ON SCHEMA public IS 'x'`:                   `unsupported COMMENT ON object "SCHEMA" (supported: TABLE, VIEW, COLUMN)`,
		`COMMENT ON TABLE it_cmt_et IS 42`:                  `expected a string or NULL near "42"`,
		`COMMENT ON TABLE it_cmt_et IS ?`:                   "expected a string or NULL",
		`COMMENT ON TABLE it_cmt_et IS UPPER('x')`:          `expected a string or NULL near "UPPER"`,
		`COMMENT ON TABLE it_cmt_et 'x'`:                    "expected IS",
		`COMMENT TABLE it_cmt_et IS 'x'`:                    `expected ON near "TABLE"`,
		`COMMENT ON TABLE it_cmt_et IS 'x' 'y'`:             `unexpected "y"`,
	} {
		h.expectError(sql, msg)
	}
	h.expectComments("public", "it_cmt_et", "NULL", "id=NULL")
	h.expectComments("public", "it_cmt_ev", "NULL", "id=NULL")
}

// The statements dbt-postgres's persist_docs runs (alter_relation_comment
// and alter_column_comment): quoted three-part names, strings quoted with
// $dbt_comment_literal_block$, and one statement per column in a script.
func TestSQLCommentDbt(t *testing.T) {
	h := newSQLHarness(t)
	h.dropViews("it_cmt_dbt_v", "it_cmt_dollar_v")
	h.dropTables("it_cmt_dbt", "it_cmt_dollar")
	h.exec(`CREATE TABLE it_cmt_dbt (id INTEGER, "Fare Amount" NUMERIC(10,2), pickup_date DATE)`)
	h.exec(`CREATE VIEW it_cmt_dbt_v AS SELECT id, pickup_date FROM it_cmt_dbt`)

	h.exec(`
  comment on table "redis"."public"."it_cmt_dbt" is $dbt_comment_literal_block$Daily revenue per pickup date$dbt_comment_literal_block$;
`)
	h.exec(`
  comment on view "redis"."public"."it_cmt_dbt_v" is $dbt_comment_literal_block$It's a view: 'single', "double", $$, $5 and \n$dbt_comment_literal_block$;
`)
	h.exec(`

    comment on column "redis"."public"."it_cmt_dbt".id is $dbt_comment_literal_block$Trip id$dbt_comment_literal_block$;

    comment on column "redis"."public"."it_cmt_dbt"."Fare Amount" is $dbt_comment_literal_block$Fare, in USD
-- not a SQL comment; /* nor this */$dbt_comment_literal_block$;

`)
	// The Redis dbt adapter's relations render as schema.identifier.
	h.exec(`comment on column public.it_cmt_dbt_v.pickup_date is $dbt_comment_literal_block$Pickup date$dbt_comment_literal_block$;`)
	h.expectComments("public", "it_cmt_dbt", "Daily revenue per pickup date",
		"id=Trip id", "Fare Amount=Fare, in USD\n-- not a SQL comment; /* nor this */", "pickup_date=NULL")
	h.expectComments("public", "it_cmt_dbt_v", `It's a view: 'single', "double", $$, $5 and \n`,
		"id=NULL", "pickup_date=Pickup date")
	// An empty description removes the comment.
	h.exec(`comment on table "redis"."public"."it_cmt_dbt" is $dbt_comment_literal_block$$dbt_comment_literal_block$;`)
	h.expectComments("public", "it_cmt_dbt", "NULL",
		"id=Trip id", "Fare Amount=Fare, in USD\n-- not a SQL comment; /* nor this */", "pickup_date=NULL")

	// Dollar quoting works wherever a string does, also in stored SQL (a
	// default, a view) that is parsed again later.
	h.expectRows(`SELECT $$it's$$, $a$x$$y$a$, $_1$$_1$ = '', $Ü$ü$Ü$`, "it's|x$$y|true|ü")
	h.exec(`CREATE TABLE it_cmt_dollar (id INTEGER, s VARCHAR DEFAULT $$it's$$)`)
	h.exec(`INSERT INTO it_cmt_dollar (id) VALUES (1)`)
	h.expectRows(`SELECT id, s FROM it_cmt_dollar`, "1|it's")
	h.exec(`CREATE VIEW it_cmt_dollar_v AS SELECT id, $q$a'b$q$ AS s FROM it_cmt_dollar WHERE s = $$it's$$`)
	h.expectRows(`SELECT id, s FROM it_cmt_dollar_v`, "1|a'b")
	h.expectError(`SELECT $$abc`, "unterminated dollar-quoted string")
	h.expectError(`SELECT $a$abc$b$`, "unterminated dollar-quoted string")
	h.expectError(`SELECT $ 1`, `unexpected character '$'`)
}

// CREATE TABLE and ADD COLUMN take MySQL / Snowflake COMMENT clauses; CREATE
// TABLE … AS copies no comments.
func TestSQLCreateTableComment(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_cmt_ct", "it_cmt_ct2", "it_cmt_ctas", "it_cmt_bad")
	h.exec(`CREATE TABLE it_cmt_ct (id INTEGER NOT NULL COMMENT 'Key', s VARCHAR DEFAULT 'a' COMMENT 'Has a default' NOINDEX,
		n INTEGER, PRIMARY KEY (id)) COMMENT = 'Created with comments'`)
	h.expectComments("public", "it_cmt_ct", "Created with comments", "id=Key", "s=Has a default", "n=NULL")
	h.expectRows(`SELECT column_name, column_default, is_nullable, is_indexed FROM information_schema.columns
		WHERE table_name = 'it_cmt_ct' ORDER BY ordinal_position`, "id|NULL|NO|YES", "s|'a'|YES|NO", "n|NULL|YES|YES")
	h.exec(`ALTER TABLE it_cmt_ct ADD COLUMN extra VARCHAR COMMENT 'Added later'`)
	h.expectComments("public", "it_cmt_ct", "Created with comments", "id=Key", "s=Has a default", "n=NULL", "extra=Added later")
	h.exec(`INSERT INTO it_cmt_ct (id, extra) VALUES (1, 'e')`)
	h.expectRows(`SELECT id, s, n, extra FROM it_cmt_ct WHERE extra = 'e'`, "1|a|NULL|e")

	// Without '='; a column may still be named comment.
	h.exec(`CREATE TABLE it_cmt_ct2 (id INTEGER, comment VARCHAR COMMENT 'A column named comment') COMMENT 'No equals sign'`)
	h.expectComments("public", "it_cmt_ct2", "No equals sign", "id=NULL", "comment=A column named comment")
	h.exec(`INSERT INTO it_cmt_ct2 VALUES (1, 'c')`)
	h.expectRows(`SELECT comment FROM it_cmt_ct2 WHERE comment = 'c'`, "c")

	h.exec(`CREATE TABLE it_cmt_ctas AS SELECT * FROM it_cmt_ct`)
	h.expectComments("public", "it_cmt_ctas", "NULL", "id=NULL", "s=NULL", "n=NULL", "extra=NULL")

	h.expectError(`CREATE TABLE it_cmt_bad (id INTEGER COMMENT 42)`, `expected a string after COMMENT near "42"`)
	h.expectError(`CREATE TABLE it_cmt_bad (id INTEGER) COMMENT NULL`, `expected a string after COMMENT near "NULL"`)
	h.expectError(`CREATE TABLE it_cmt_bad (id INTEGER) COMMENT = 'x' 'y'`, `unexpected "y"`)
}

// Comments are written with WATCH/MULTI: concurrent comments on one table
// from several connections are all kept, and so is a concurrent ALTER.
func TestSQLCommentConcurrent(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_cmt_conc")
	const n = 8
	cols, want := make([]string, n), make([]string, n)
	for i := range cols {
		cols[i] = fmt.Sprintf("c%d INTEGER", i)
		want[i] = fmt.Sprintf("c%d=comment %d", i, i)
	}
	h.exec(`CREATE TABLE it_cmt_conc (` + strings.Join(cols, ", ") + `)`)
	// One connection per column comments it and then the table; one more
	// adds a column.
	conns := make([]*sqlHarness, n+1)
	for i := range conns {
		conns[i] = newSQLHarness(t)
	}
	var wg sync.WaitGroup
	errs := make([]error, n+1)
	for i, c := range conns {
		sqls := []string{
			fmt.Sprintf(`COMMENT ON COLUMN it_cmt_conc.c%d IS 'comment %d'`, i, i),
			fmt.Sprintf(`COMMENT ON TABLE it_cmt_conc IS 'set by %d'`, i),
		}
		if i == n {
			sqls = []string{`ALTER TABLE it_cmt_conc ADD COLUMN added INTEGER`}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, sql := range sqls {
				if _, err := tryExec(h.ctx, c.conn, sql); err != nil {
					errs[i] = err
					return
				}
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("statement %d: %v", i, err)
		}
	}
	rows, _ := h.query(`SELECT comment FROM information_schema.tables WHERE table_name = 'it_cmt_conc'`)
	if len(rows) != 1 || !strings.HasPrefix(rows[0], "set by ") {
		t.Fatalf("table comment = %q", rows)
	}
	h.expectComments("public", "it_cmt_conc", append([]string{rows[0]}, append(want, "added=NULL")...)...)
}
