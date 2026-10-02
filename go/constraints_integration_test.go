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

// Integration tests for column and table constraints, and CHECK
// enforcement (check.go).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// storedChecks lists a table's CHECK constraints as its metadata stores
// them, "name: expr".
func (h *sqlHarness) storedChecks(raw goredis.UniversalClient, schema, table string) []string {
	h.t.Helper()
	meta, err := (&store{client: raw}).getTable(h.ctx, schema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	var out []string
	for _, c := range meta.Checks {
		out = append(out, c.Name+": "+c.Expr)
	}
	return out
}

// expectChecks checks a table's stored CHECK constraints ("name: expr").
func (h *sqlHarness) expectChecks(raw goredis.UniversalClient, table string, want ...string) {
	h.t.Helper()
	got := h.storedChecks(raw, defaultSchema, table)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		h.t.Errorf("checks of %s\n got: %q\nwant: %q", table, got, want)
	}
}

// tableConstraints lists GetObjects' table_constraints of a table as
// "name TYPE (columns)".
func (h *sqlHarness) tableConstraints(schema, table string) []string {
	h.t.Helper()
	rdr, err := h.conn.GetObjects(h.ctx, adbc.ObjectDepthAll, nil, &schema, &table, nil, nil)
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
						Name        string `json:"table_name"`
						Constraints []struct {
							Name    *string  `json:"constraint_name"`
							Type    string   `json:"constraint_type"`
							Columns []string `json:"constraint_column_names"`
						} `json:"table_constraints"`
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
					out = append(out, "table")
					for _, c := range t.Constraints {
						name := "NULL"
						if c.Name != nil {
							name = *c.Name
						}
						out = append(out, fmt.Sprintf("%s %s (%s)", name, c.Type, strings.Join(c.Columns, ", ")))
					}
				}
			}
		}
	}
	return out
}

// Every constraint form parses, in CREATE TABLE and ADD COLUMN, with the
// same meaning at both levels. PRIMARY KEY, UNIQUE and FOREIGN KEY are not
// enforced.
func TestSQLConstraintForms(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_con_parent", "it_con_forms", "it_con_tbl", "it_con_multi", "it_con_bad")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_con_parent (id INTEGER NOT NULL PRIMARY KEY)`)

	// The forms of issue #75, column level.
	h.exec(`CREATE TABLE it_con_forms (
		id integer not null primary key,
		code varchar unique,
		amount integer check (amount >= 0),
		qty integer constraint qty_nonneg check (qty >= 0),
		parent_id integer references it_con_parent (id),
		other_id integer references it_con_parent
	)`)
	h.expectChecks(raw, "it_con_forms", "it_con_forms_amount_check: amount >= 0", "qty_nonneg: qty >= 0")
	h.exec(`INSERT INTO it_con_forms VALUES (1, 'a', 0, 0, 1, 1)`)
	h.expectError(`INSERT INTO it_con_forms VALUES (2, 'b', -1, 0, 1, 1)`,
		`new row for relation "it_con_forms" violates check constraint "it_con_forms_amount_check"`)
	h.expectError(`INSERT INTO it_con_forms VALUES (2, 'b', 0, -1, 1, 1)`,
		`new row for relation "it_con_forms" violates check constraint "qty_nonneg"`)
	// Not enforced: a duplicate primary key and unique value, and parents
	// that don't exist.
	h.exec(`INSERT INTO it_con_forms VALUES (1, 'a', 5, 5, 99, 98)`)
	h.expectRows(`SELECT id, code, amount, qty, parent_id, other_id FROM it_con_forms ORDER BY amount`,
		"1|a|0|0|1|1", "1|a|5|5|99|98")

	// The table-level forms of the issue, named or not, all on one table.
	h.exec(`CREATE TABLE it_con_tbl (
		a integer, b integer, parent_id integer,
		check (a >= 0),
		constraint b_nonneg check (b >= 0),
		unique (a, b),
		primary key (a),
		constraint u_b unique (b),
		constraint pk_a primary key (a),
		foreign key (parent_id) references it_con_parent (id),
		constraint fk_p foreign key (parent_id) references it_con_parent,
		foreign key (parent_id) references it_con_parent,
		foreign key (parent_id) references it_con_parent (id) on delete cascade on update set null
	)`)
	h.expectChecks(raw, "it_con_tbl", "it_con_tbl_a_check: a >= 0", "b_nonneg: b >= 0")
	h.exec(`INSERT INTO it_con_tbl VALUES (1, 1, 7), (1, 1, 8)`)
	h.expectError(`INSERT INTO it_con_tbl VALUES (-1, 1, 7)`, `violates check constraint "it_con_tbl_a_check"`)
	h.expectError(`INSERT INTO it_con_tbl VALUES (1, -1, 7)`, `violates check constraint "b_nonneg"`)

	// Several constraints on one column, in any order, each of them may be
	// named, and REFERENCES takes Postgres's MATCH and ON DELETE / ON UPDATE.
	h.exec(`CREATE TABLE it_con_multi (
		a integer constraint a_nn not null constraint a_pos check (a > 0) constraint a_pk primary key,
		b integer check (b > 0) default 5 check (b < 100) constraint b_ref references it_con_parent (id) match full on update cascade on delete set null not null,
		c varchar constraint c_d default 'x' constraint c_u unique constraint c_null null comment 'C' noindex,
		d integer references public.it_con_parent (id) on delete set default (d) on update no action,
		e integer references it_con_parent match simple on update restrict check (e <> 0) no inherit,
		f integer constraint f_ref references it_con_parent (id) match partial on delete set null (f, e) unique primary key
	)`)
	h.expectChecks(raw, "it_con_multi",
		"a_pos: a > 0", "it_con_multi_b_check: b > 0", "it_con_multi_b_check1: b < 100", "it_con_multi_e_check: e <> 0")
	h.exec(`INSERT INTO it_con_multi (a) VALUES (1)`)
	h.expectRows(`SELECT a, b, c, d, e, f FROM it_con_multi`, "1|5|x|NULL|NULL|NULL")
	h.expectError(`INSERT INTO it_con_multi (a) VALUES (NULL)`, `NULL value in column "a" violates not-null constraint`)
	h.expectError(`INSERT INTO it_con_multi (a, b) VALUES (1, 100)`, `violates check constraint "it_con_multi_b_check1"`)
	h.expectError(`INSERT INTO it_con_multi (a, e) VALUES (1, 0)`, `violates check constraint "it_con_multi_e_check"`)
	h.expectRows(`SELECT column_name, is_nullable, column_default, is_indexed, comment FROM information_schema.columns
		WHERE table_name = 'it_con_multi' ORDER BY ordinal_position`,
		"a|NO|NULL|YES|NULL", "b|NO|5|YES|NULL", "c|YES|'x'|NO|C", "d|YES|NULL|YES|NULL", "e|YES|NULL|YES|NULL", "f|YES|NULL|YES|NULL")

	// ADD COLUMN takes the same column constraints.
	h.exec(`ALTER TABLE it_con_forms ADD COLUMN ref_id integer constraint ref_fk references it_con_parent (id) on delete cascade check (ref_id > 0) unique`)
	h.exec(`ALTER TABLE it_con_forms ADD qty2 integer not null default 1 constraint qty2_pos check (qty2 > 0) primary key`)
	h.expectChecks(raw, "it_con_forms", "it_con_forms_amount_check: amount >= 0", "qty_nonneg: qty >= 0",
		"it_con_forms_ref_id_check: ref_id > 0", "qty2_pos: qty2 > 0")
	h.expectError(`INSERT INTO it_con_forms (id, ref_id) VALUES (3, 0)`, `violates check constraint "it_con_forms_ref_id_check"`)
	h.expectError(`INSERT INTO it_con_forms (id, qty2) VALUES (3, 0)`, `violates check constraint "qty2_pos"`)
	h.exec(`INSERT INTO it_con_forms (id, ref_id) VALUES (3, 1)`)
	h.expectRows(`SELECT id, ref_id, qty2 FROM it_con_forms WHERE id = 3`, "3|1|1")

	// Syntax errors.
	for sql, msg := range map[string]string{
		`CREATE TABLE it_con_bad (a integer constraint n noindex)`:                               `syntax error: expected a constraint after CONSTRAINT n near "noindex"`,
		`CREATE TABLE it_con_bad (a integer constraint n)`:                                       `syntax error: expected a constraint after CONSTRAINT n near ")"`,
		`CREATE TABLE it_con_bad (a integer constraint)`:                                         `syntax error: expected identifier near ")"`,
		`CREATE TABLE it_con_bad (a integer check a > 0)`:                                        `syntax error: expected "(" near "a"`,
		`CREATE TABLE it_con_bad (a integer check (a > 0)`:                                       `syntax error: expected ")" near ""`,
		`CREATE TABLE it_con_bad (a integer check ())`:                                           `syntax error: unexpected ")"`,
		`CREATE TABLE it_con_bad (a integer references)`:                                         `syntax error: expected identifier near ")"`,
		`CREATE TABLE it_con_bad (a integer references p (x, y))`:                                `number of referencing and referenced columns for foreign key disagree`,
		`CREATE TABLE it_con_bad (a integer references p match bogus)`:                           `syntax error: expected FULL, PARTIAL or SIMPLE after MATCH near "bogus"`,
		`CREATE TABLE it_con_bad (a integer references p on delete bogus)`:                       `syntax error: expected NO ACTION, RESTRICT, CASCADE, SET NULL or SET DEFAULT near "bogus"`,
		`CREATE TABLE it_con_bad (a integer references p on delete cascade on delete no action)`: `syntax error: ON DELETE specified more than once`,
		`CREATE TABLE it_con_bad (a integer references p on insert cascade)`:                     `syntax error: expected ")" near "on"`,
		`CREATE TABLE it_con_bad (a integer, check (a > 0) deferrable)`:                          `syntax error: expected ")" near "deferrable"`,
		`CREATE TABLE it_con_bad (a integer, constraint c)`:                                      `syntax error: expected a constraint after CONSTRAINT c near ")"`,
		`CREATE TABLE it_con_bad (a integer, constraint c d integer)`:                            `syntax error: expected a constraint after CONSTRAINT c near "d"`,
		`CREATE TABLE it_con_bad (a integer, primary key (a)`:                                    `syntax error: unexpected end of input`,
	} {
		h.expectError(sql, msg)
	}
	if ok, err := (&store{client: raw}).tableExists(h.ctx, defaultSchema, "it_con_bad"); err != nil || ok {
		t.Errorf("it_con_bad exists (%v)", err)
	}
}

// CHECK on INSERT … VALUES, DEFAULT VALUES and INSERT … SELECT: only FALSE
// rejects a row, defaults are filled in first, NOT NULL is checked before,
// and a statement with a failing row writes nothing.
func TestSQLCheckInsert(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_chk_ins", "it_chk_src")
	h.exec(`CREATE TABLE it_chk_ins (
		id integer not null,
		amount numeric(10,2) default 1 check (amount >= 0),
		lo integer, hi integer,
		check (lo <= hi),
		label varchar check (label in ('a', 'b') or label like 'x%')
	)`)
	h.exec(`INSERT INTO it_chk_ins VALUES (1, 0, 1, 2, 'a')`)
	// NULL passes: amount, lo / hi, label.
	h.exec(`INSERT INTO it_chk_ins VALUES (2, NULL, NULL, 5, NULL), (3, 2.5, 7, NULL, 'xyz')`)
	h.exec(`INSERT INTO it_chk_ins (id) VALUES (4)`)
	h.expectRows(`SELECT id, amount, lo, hi, label FROM it_chk_ins ORDER BY id`,
		"1|0.00|1|2|a", "2|NULL|NULL|5|NULL", "3|2.50|7|NULL|xyz", "4|1.00|NULL|NULL|NULL")

	// A failing row anywhere in the statement writes nothing.
	for sql, name := range map[string]string{
		`INSERT INTO it_chk_ins VALUES (5, 1, 1, 1, 'a'), (6, -0.01, 1, 1, 'a'), (7, 1, 1, 1, 'a')`: "it_chk_ins_amount_check",
		`INSERT INTO it_chk_ins VALUES (5, 1, 1, 1, 'a'), (6, 1, 3, 2, 'a')`:                        "it_chk_ins_check",
		`INSERT INTO it_chk_ins VALUES (5, 1, 1, 1, 'c')`:                                           "it_chk_ins_label_check",
		`INSERT INTO it_chk_ins (id, amount) VALUES (5, 1), (6, -1)`:                                "it_chk_ins_amount_check",
		`INSERT INTO it_chk_ins SELECT id + 10, amount - 1, lo, hi, label FROM it_chk_ins`:          "it_chk_ins_amount_check",
		`INSERT INTO it_chk_ins (id, lo, hi) (SELECT id + 10, hi, lo FROM it_chk_ins WHERE id = 1)`: "it_chk_ins_check",
	} {
		h.expectError(sql, fmt.Sprintf(`new row for relation "it_chk_ins" violates check constraint %q`, name))
	}
	h.expectRows(`SELECT COUNT(*) FROM it_chk_ins`, "4")

	// The default goes through the check too.
	h.exec(`ALTER TABLE it_chk_ins ALTER COLUMN amount SET DEFAULT -1`)
	h.expectError(`INSERT INTO it_chk_ins (id) VALUES (5)`, `violates check constraint "it_chk_ins_amount_check"`)
	h.exec(`ALTER TABLE it_chk_ins ALTER COLUMN amount DROP DEFAULT`)
	h.exec(`INSERT INTO it_chk_ins (id) VALUES (5)`)
	// NOT NULL comes first, also for INSERT … SELECT.
	h.expectError(`INSERT INTO it_chk_ins VALUES (NULL, -1, 1, 1, 'a')`, `NULL value in column "id" violates not-null constraint`)
	h.expectError(`INSERT INTO it_chk_ins SELECT CASE WHEN id = 1 THEN NULL ELSE id END, -1, 1, 1, 'a' FROM it_chk_ins WHERE id IN (1, 2)`,
		`NULL value in column "id" violates not-null constraint`)

	// INSERT … SELECT that passes, and DEFAULT VALUES.
	h.exec(`CREATE TABLE it_chk_src (n integer)`)
	h.exec(`INSERT INTO it_chk_src VALUES (10), (11)`)
	if n := h.exec(`INSERT INTO it_chk_ins (id, amount, lo, hi) SELECT n, n / 2, n, n FROM it_chk_src`); n != 2 {
		t.Errorf("INSERT … SELECT inserted %d rows", n)
	}
	h.exec(`CREATE TABLE IF NOT EXISTS it_chk_src (n integer)`)
	h.dropTables("it_chk_dv")
	h.exec(`CREATE TABLE it_chk_dv (a integer default 0 check (a > 0), b integer check (b is null))`)
	h.expectError(`INSERT INTO it_chk_dv DEFAULT VALUES`, `new row for relation "it_chk_dv" violates check constraint "it_chk_dv_a_check"`)
	h.exec(`ALTER TABLE it_chk_dv ALTER COLUMN a SET DEFAULT 1`)
	h.exec(`INSERT INTO it_chk_dv DEFAULT VALUES`)
	h.expectError(`INSERT INTO it_chk_dv VALUES (1, 1)`, `violates check constraint "it_chk_dv_b_check"`)
	h.expectRows(`SELECT id, amount, lo, hi FROM it_chk_ins WHERE id >= 5 ORDER BY id`, "5|NULL|NULL|NULL", "10|5.00|10|10", "11|5.00|11|11")
	h.expectRows(`SELECT a, b FROM it_chk_dv`, "1|NULL")

	// A row that fails several constraints reports the first by name, as
	// in Postgres.
	h.dropTables("it_chk_order")
	h.exec(`CREATE TABLE it_chk_order (x integer constraint z_pos check (x > 0) constraint a_neg check (x < -5), check (x <> 0))`)
	h.expectError(`INSERT INTO it_chk_order VALUES (0)`, `violates check constraint "a_neg"`)
	h.expectError(`INSERT INTO it_chk_order VALUES (-1)`, `violates check constraint "a_neg"`)
	h.expectError(`INSERT INTO it_chk_order VALUES (-6)`, `violates check constraint "z_pos"`)
	h.exec(`ALTER TABLE it_chk_order DROP CONSTRAINT a_neg`)
	h.expectError(`INSERT INTO it_chk_order VALUES (0)`, `violates check constraint "it_chk_order_x_check"`)

	// RETURNING doesn't change that a failing statement writes nothing.
	h.expectError(`INSERT INTO it_chk_ins (id, amount) VALUES (20, 1), (21, -1) RETURNING id`, `violates check constraint "it_chk_ins_amount_check"`)
	h.expectRows(`SELECT COUNT(*) FROM it_chk_ins WHERE id >= 20`, "0")
	h.expectRows(`INSERT INTO it_chk_ins (id, amount) VALUES (20, 1) RETURNING id, amount`, "20|1.00")
}

// CHECK on UPDATE, UPDATE … FROM and both branches of MERGE. The rows are
// checked whole (a constraint on columns the statement doesn't set too),
// and a statement with a failing row writes nothing.
func TestSQLCheckUpdateMerge(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_chk_upd", "it_chk_new")
	h.exec(`CREATE TABLE it_chk_upd (id integer, lo integer check (lo >= 0), hi integer, note varchar, check (lo <= hi))`)
	h.exec(`INSERT INTO it_chk_upd VALUES (1, 0, 10, 'a'), (2, 5, 5, 'b'), (3, NULL, 1, 'c'), (4, 2, NULL, 'd')`)
	all := []string{"1|0|10|a", "2|5|5|b", "3|NULL|1|c", "4|2|NULL|d"}
	expectAll := func() {
		t.Helper()
		h.expectRows(`SELECT id, lo, hi, note FROM it_chk_upd ORDER BY id`, all...)
	}

	for sql, name := range map[string]string{
		// hi isn't set, but lo <= hi reads it.
		`UPDATE it_chk_upd SET lo = 6 WHERE id = 2`:                  "it_chk_upd_check",
		`UPDATE it_chk_upd SET lo = lo - 1`:                          "it_chk_upd_lo_check",
		`UPDATE it_chk_upd SET hi = hi - 3, note = 'z'`:              "it_chk_upd_check",
		`UPDATE it_chk_upd u SET hi = u.lo - 1 WHERE u.id IN (2, 4)`: "it_chk_upd_check",
		`UPDATE it_chk_upd SET lo = n.v FROM (SELECT 1 AS id, 3 AS v UNION ALL SELECT 2, -1) AS n WHERE it_chk_upd.id = n.id`: "it_chk_upd_lo_check",
		`UPDATE it_chk_upd AS u SET hi = n.v FROM (SELECT 1 AS id, -5 AS v) AS n WHERE u.id = n.id`:                           "it_chk_upd_check",
	} {
		h.expectError(sql, fmt.Sprintf(`new row for relation "it_chk_upd" violates check constraint %q`, name))
		expectAll()
	}
	// NULL passes, and so does a change the constraints don't read.
	if n := h.exec(`UPDATE it_chk_upd SET lo = NULL WHERE id = 2`); n != 1 {
		t.Errorf("UPDATE changed %d rows", n)
	}
	h.exec(`UPDATE it_chk_upd SET note = upper(note)`)
	h.exec(`UPDATE it_chk_upd SET lo = n.v FROM (SELECT 1 AS id, 3 AS v UNION ALL SELECT 4, 2) AS n WHERE it_chk_upd.id = n.id`)
	all = []string{"1|3|10|A", "2|NULL|5|B", "3|NULL|1|C", "4|2|NULL|D"}
	expectAll()

	// MERGE: a failing UPDATE or INSERT leaves the table as it was, the
	// other branch's changes included.
	h.exec(`CREATE TABLE it_chk_new (id integer, lo integer, hi integer)`)
	h.exec(`INSERT INTO it_chk_new VALUES (1, 4, 4), (5, 1, 2)`)
	merge := `MERGE INTO it_chk_upd t USING it_chk_new s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET lo = s.lo
		WHEN NOT MATCHED THEN INSERT (id, lo, hi, note) VALUES (s.id, s.lo, s.hi, 'new')`
	h.exec(`UPDATE it_chk_new SET hi = 0 WHERE id = 5`)
	h.expectError(merge, `new row for relation "it_chk_upd" violates check constraint "it_chk_upd_check"`)
	expectAll()
	h.exec(`UPDATE it_chk_new SET hi = 2, lo = 11 WHERE id = 5`)
	h.expectError(merge, `new row for relation "it_chk_upd" violates check constraint "it_chk_upd_check"`)
	expectAll()
	h.exec(`UPDATE it_chk_new SET lo = 1 WHERE id = 5`)
	h.exec(`UPDATE it_chk_new SET lo = 11 WHERE id = 1`)
	h.expectError(merge, `new row for relation "it_chk_upd" violates check constraint "it_chk_upd_check"`)
	expectAll()
	h.exec(`UPDATE it_chk_new SET lo = -1, hi = NULL WHERE id = 1`)
	h.expectError(merge, `new row for relation "it_chk_upd" violates check constraint "it_chk_upd_lo_check"`)
	expectAll()
	// WHEN NOT MATCHED BY SOURCE … UPDATE reads only the target.
	h.expectError(`MERGE INTO it_chk_upd t USING it_chk_new s ON t.id = s.id
		WHEN NOT MATCHED BY SOURCE THEN UPDATE SET hi = -1`, `violates check constraint "it_chk_upd_check"`)
	expectAll()

	h.exec(`UPDATE it_chk_new SET lo = 4 WHERE id = 1`)
	if n := h.exec(merge); n != 2 {
		t.Errorf("MERGE changed %d rows", n)
	}
	all = []string{"1|4|10|A", "2|NULL|5|B", "3|NULL|1|C", "4|2|NULL|D", "5|1|2|new"}
	expectAll()
}

// Bulk ingest into a table with CHECK constraints: a batch with a failing
// row writes nothing.
func TestSQLCheckIngest(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_chk_ing")
	h.exec(`CREATE TABLE it_chk_ing (id bigint, amount bigint check (amount >= 0), label varchar default 'd' check (label <> 'bad'))`)
	ingest := func(ids []int64, amounts []int64, valid []bool) error {
		t.Helper()
		mem := memory.DefaultAllocator
		ib, ab := array.NewInt64Builder(mem), array.NewInt64Builder(mem)
		defer ib.Release()
		defer ab.Release()
		ib.AppendValues(ids, nil)
		ab.AppendValues(amounts, valid)
		idCol, amountCol := ib.NewArray(), ab.NewArray()
		defer idCol.Release()
		defer amountCol.Release()
		rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}, nil), []arrow.Array{idCol, amountCol}, int64(len(ids)))
		defer rec.Release()
		st, err := h.conn.NewStatement(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close(h.ctx)
		for k, v := range map[string]string{
			adbc.OptionKeyIngestTargetTable: "it_chk_ing",
			adbc.OptionKeyIngestMode:        adbc.OptionValueIngestModeAppend,
		} {
			if err := st.SetOption(h.ctx, k, v); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.Bind(h.ctx, rec); err != nil {
			t.Fatal(err)
		}
		_, err = st.ExecuteUpdate(h.ctx)
		return err
	}
	if err := ingest([]int64{1, 2, 3}, []int64{0, 5, 0}, []bool{true, true, false}); err != nil {
		t.Fatal(err)
	}
	err := ingest([]int64{4, 5, 6}, []int64{1, -1, 2}, nil)
	if err == nil || !strings.Contains(err.Error(), `new row for relation "it_chk_ing" violates check constraint "it_chk_ing_amount_check"`) {
		t.Errorf("ingest of a failing row: %v", err)
	}
	h.expectRows(`SELECT id, amount, label FROM it_chk_ing ORDER BY id`, "1|0|d", "2|5|d", "3|NULL|d")
	// The default of a column the data lacks is checked too.
	h.exec(`ALTER TABLE it_chk_ing ALTER COLUMN label SET DEFAULT 'bad'`)
	err = ingest([]int64{7}, []int64{1}, nil)
	if err == nil || !strings.Contains(err.Error(), `violates check constraint "it_chk_ing_label_check"`) {
		t.Errorf("ingest with a failing default: %v", err)
	}
	h.expectRows(`SELECT COUNT(*) FROM it_chk_ing`, "3")
}

// Default names, explicit names, and the errors CREATE TABLE (and ADD
// COLUMN, ADD CONSTRAINT) give for a constraint Postgres would refuse.
// A failing statement creates or changes nothing.
func TestSQLCheckDefinition(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_chk_names", "it_chk_bad", "it_chk_def")
	raw := h.rawClient()
	// As in Postgres: <table>_<column>_check when the expression reads one
	// column, wherever it is written, <table>_check otherwise; a number is
	// added to a name already taken, and explicit names are as written.
	h.exec(`CREATE TABLE it_chk_names (
		a integer check (a > 0) check (a < 100) check (a <> 50),
		b integer check (b > a),
		c integer constraint "My Check" check (c > 0) check (1 = 1),
		check (b > 0), check (a + b > 0), check (true), constraint it_chk_names_c_check check (c < 10),
		check (c <> 5),
		CONSTRAINT Upper_Name CHECK (it_chk_names.c <> 6 AND public.it_chk_names.a <> 7)
	)`)
	h.expectChecks(raw, "it_chk_names",
		"it_chk_names_a_check: a > 0",
		"it_chk_names_a_check1: a < 100",
		"it_chk_names_a_check2: a <> 50",
		"it_chk_names_check: b > a",
		"My Check: c > 0",
		"it_chk_names_check1: 1 = 1",
		"it_chk_names_b_check: b > 0",
		"it_chk_names_check2: a + b > 0",
		"it_chk_names_check3: true",
		"it_chk_names_c_check: c < 10",
		"it_chk_names_c_check1: c <> 5",
		"Upper_Name: c <> 6 AND a <> 7")
	h.expectError(`INSERT INTO it_chk_names (c) VALUES (0)`, `violates check constraint "My Check"`)
	h.expectError(`INSERT INTO it_chk_names (c) VALUES (6)`, `violates check constraint "Upper_Name"`)
	h.expectError(`INSERT INTO it_chk_names (a) VALUES (7)`, `violates check constraint "Upper_Name"`)
	// Later constraints continue the numbering.
	h.exec(`ALTER TABLE it_chk_names ADD COLUMN d integer check (d > 0) check (a > 1)`)
	h.exec(`ALTER TABLE it_chk_names ADD CHECK (d < 9)`)
	h.exec(`ALTER TABLE it_chk_names ADD CONSTRAINT d_even CHECK (d % 2 = 0)`)
	h.exec(`ALTER TABLE it_chk_names ADD CHECK (c > d)`)
	h.expectChecks(raw, "it_chk_names",
		"it_chk_names_a_check: a > 0",
		"it_chk_names_a_check1: a < 100",
		"it_chk_names_a_check2: a <> 50",
		"it_chk_names_check: b > a",
		"My Check: c > 0",
		"it_chk_names_check1: 1 = 1",
		"it_chk_names_b_check: b > 0",
		"it_chk_names_check2: a + b > 0",
		"it_chk_names_check3: true",
		"it_chk_names_c_check: c < 10",
		"it_chk_names_c_check1: c <> 5",
		"Upper_Name: c <> 6 AND a <> 7",
		"it_chk_names_d_check: d > 0",
		"it_chk_names_a_check3: a > 1",
		"it_chk_names_d_check1: d < 9",
		"d_even: d % 2 = 0",
		"it_chk_names_check4: c > d")
	h.exec(`INSERT INTO it_chk_names (a, b, c, d) VALUES (2, 3, 9, 4)`)
	h.expectError(`INSERT INTO it_chk_names (a, b, c, d) VALUES (2, 3, 9, 3)`, `violates check constraint "d_even"`)
	h.expectError(`INSERT INTO it_chk_names (a, b, c, d) VALUES (2, 3, 4, 4)`, `violates check constraint "it_chk_names_check4"`)

	h.exec(`CREATE TABLE it_chk_def (a integer, b varchar)`)
	before := h.storedChecks(raw, defaultSchema, "it_chk_names")
	for sql, msg := range map[string]string{
		`CREATE TABLE it_chk_bad (a integer check (nosuch > 0))`:                                                    `column "nosuch" does not exist in table "it_chk_bad"`,
		`CREATE TABLE it_chk_bad (a integer, check (a > (SELECT 1)))`:                                               "cannot use subquery in check constraint",
		`CREATE TABLE it_chk_bad (a integer check (EXISTS (SELECT 1 FROM it_chk_def)))`:                             "cannot use subquery in check constraint",
		`CREATE TABLE it_chk_bad (a integer check (a IN (SELECT a FROM it_chk_def)))`:                               "cannot use subquery in check constraint",
		`CREATE TABLE it_chk_bad (a integer check (sum(a) > 0))`:                                                    "aggregate functions are not allowed in check constraints",
		`CREATE TABLE it_chk_bad (a integer check (count(*) > 0))`:                                                  "aggregate functions are not allowed in check constraints",
		`CREATE TABLE it_chk_bad (a integer check (row_number() over () > 0))`:                                      "window functions are not allowed in check constraints",
		`CREATE TABLE it_chk_bad (a integer check (it_chk_def.a > 0))`:                                              `missing FROM-clause entry for table "it_chk_def"`,
		`CREATE TABLE it_chk_bad (a integer, check (other.a > 0))`:                                                  `missing FROM-clause entry for table "other"`,
		`CREATE TABLE it_chk_bad (a integer check (__rowid > 0))`:                                                   `system column "__rowid" reference in check constraint is invalid`,
		`CREATE TABLE it_chk_bad (a integer check (a))`:                                                             "argument of CHECK must be type boolean, not type integer",
		`CREATE TABLE it_chk_bad (a integer check (a + 1))`:                                                         "argument of CHECK must be type boolean, not type bigint",
		`CREATE TABLE it_chk_bad (a integer, b varchar check (b))`:                                                  "argument of CHECK must be type boolean, not type varchar",
		`CREATE TABLE it_chk_bad (a integer check (merge_action() = 'x'))`:                                          "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command",
		`CREATE TABLE it_chk_bad (a integer constraint c check (a > 0) constraint c check (a < 9))`:                 `check constraint "c" already exists`,
		`CREATE TABLE it_chk_bad (a integer constraint c check (a > 0), check (a < 9), constraint C check (a < 8))`: `check constraint "C" already exists`,
		`ALTER TABLE it_chk_names ADD COLUMN e integer constraint d_even check (e > 0)`:                             `constraint "d_even" for relation "it_chk_names" already exists`,
		`ALTER TABLE it_chk_names ADD CONSTRAINT "My Check" CHECK (a > 0)`:                                          `constraint "My Check" for relation "it_chk_names" already exists`,
		`ALTER TABLE it_chk_names ADD COLUMN e integer check (nosuch > 0)`:                                          `column "nosuch" does not exist in table "it_chk_names"`,
		`ALTER TABLE it_chk_names ADD COLUMN e integer check (e > (SELECT 1))`:                                      "cannot use subquery in check constraint",
		`ALTER TABLE it_chk_names ADD CONSTRAINT x CHECK (avg(a) > 0)`:                                              "aggregate functions are not allowed in check constraints",
		`ALTER TABLE it_chk_names ADD CHECK (it_chk_def.a > 0)`:                                                     `missing FROM-clause entry for table "it_chk_def"`,
		`ALTER TABLE it_chk_names ADD CHECK (a)`:                                                                    "argument of CHECK must be type boolean, not type integer",
	} {
		h.expectError(sql, msg)
	}
	// A parameter, bound or not.
	for _, sql := range []string{`CREATE TABLE it_chk_bad (a integer check (a > ?))`, `ALTER TABLE it_chk_names ADD CHECK (a > $1)`} {
		mem := memory.DefaultAllocator
		b := array.NewInt64Builder(mem)
		b.Append(1)
		col := b.NewArray()
		rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "p", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{col}, 1)
		_, err := h.stmtWith(sql, rec).ExecuteUpdate(h.ctx)
		if err == nil || !strings.Contains(err.Error(), "cannot use parameter in check constraint") {
			t.Errorf("%s: %v", sql, err)
		}
		rec.Release()
		col.Release()
		b.Release()
		h.expectError(sql, "query has 1 parameter(s) but none are bound")
	}
	if ok, err := (&store{client: raw}).tableExists(h.ctx, defaultSchema, "it_chk_bad"); err != nil || ok {
		t.Errorf("it_chk_bad exists (%v)", err)
	}
	h.expectChecks(raw, "it_chk_names", before...)
	h.expectColumns(`SELECT * FROM it_chk_names`, "a|b|c|d", "2|3|9|4")
	// A qualified reference to the table itself is fine, and is stored
	// without the qualifier.
	h.exec(`CREATE TABLE it_chk_bad (a integer check (it_chk_bad.a > 0))`)
	h.expectChecks(raw, "it_chk_bad", "it_chk_bad_a_check: a > 0")
}

// ALTER TABLE … ADD COLUMN … CHECK and ADD CONSTRAINT … CHECK check the
// existing rows, which read the new column's default; DROP CONSTRAINT
// removes a CHECK.
func TestSQLCheckAlter(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_chk_alt", "it_chk_empty")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_chk_alt (id integer, v integer)`)
	h.exec(`INSERT INTO it_chk_alt VALUES (1, 10), (2, NULL), (3, -4)`)

	for sql, msg := range map[string]string{
		`ALTER TABLE it_chk_alt ADD CHECK (v >= 0)`:                                                           `check constraint "it_chk_alt_v_check" of relation "it_chk_alt" is violated by some row`,
		`ALTER TABLE it_chk_alt ADD CONSTRAINT pos CHECK (v > id)`:                                            `check constraint "pos" of relation "it_chk_alt" is violated by some row`,
		`ALTER TABLE it_chk_alt ADD COLUMN w integer DEFAULT 5 CHECK (w < v)`:                                 `check constraint "it_chk_alt_check" of relation "it_chk_alt" is violated by some row`,
		`ALTER TABLE it_chk_alt ADD COLUMN w integer DEFAULT -1 CHECK (w >= 0)`:                               `check constraint "it_chk_alt_w_check" of relation "it_chk_alt" is violated by some row`,
		`ALTER TABLE it_chk_alt ADD COLUMN w integer CHECK (w IS NOT NULL)`:                                   `check constraint "it_chk_alt_w_check" of relation "it_chk_alt" is violated by some row`,
		`ALTER TABLE it_chk_alt ADD COLUMN w integer DEFAULT 1 CHECK (w > 0) CHECK (coalesce(v, 0) + w <> 1)`: `check constraint "it_chk_alt_check" of relation "it_chk_alt" is violated by some row`,
	} {
		h.expectError(sql, msg)
		h.expectColumns(`SELECT * FROM it_chk_alt ORDER BY id`, "id|v", "1|10", "2|NULL", "3|-4")
		h.expectChecks(raw, "it_chk_alt")
	}
	// The same constraints pass on an empty table.
	h.exec(`CREATE TABLE it_chk_empty (id integer, v integer)`)
	h.exec(`ALTER TABLE it_chk_empty ADD COLUMN w integer DEFAULT -1 CHECK (w >= 0)`)
	h.exec(`ALTER TABLE it_chk_empty ADD COLUMN x integer CHECK (x IS NOT NULL)`)
	h.exec(`ALTER TABLE it_chk_empty ADD CHECK (v >= 0)`)
	h.expectChecks(raw, "it_chk_empty", "it_chk_empty_w_check: w >= 0", "it_chk_empty_x_check: x IS NOT NULL", "it_chk_empty_v_check: v >= 0")
	h.expectError(`INSERT INTO it_chk_empty (id, x) VALUES (1, 1)`, `violates check constraint "it_chk_empty_w_check"`)

	// Constraints the rows pass: NULL passes, and the new column's default
	// counts.
	h.exec(`ALTER TABLE it_chk_alt ADD CHECK (v <> 0)`)
	h.exec(`ALTER TABLE it_chk_alt ADD COLUMN w integer DEFAULT 20 CHECK (w > v OR v IS NULL)`)
	h.exec(`ALTER TABLE it_chk_alt ADD COLUMN x integer CHECK (x > 0)`)
	h.exec(`ALTER TABLE it_chk_alt ADD CONSTRAINT id_pos CHECK (id > 0)`)
	h.expectChecks(raw, "it_chk_alt", "it_chk_alt_v_check: v <> 0", "it_chk_alt_check: w > v OR v IS NULL", "it_chk_alt_x_check: x > 0", "id_pos: id > 0")
	h.expectRows(`SELECT id, v, w, x FROM it_chk_alt ORDER BY id`, "1|10|20|NULL", "2|NULL|20|NULL", "3|-4|20|NULL")
	h.expectError(`UPDATE it_chk_alt SET v = 25 WHERE id = 1`, `violates check constraint "it_chk_alt_check"`)
	h.expectError(`INSERT INTO it_chk_alt (id, v) VALUES (0, 1)`, `violates check constraint "id_pos"`)
	// ADD of a PRIMARY KEY, UNIQUE or FOREIGN KEY is accepted and ignored.
	h.exec(`ALTER TABLE it_chk_alt ADD PRIMARY KEY (id)`)
	h.exec(`ALTER TABLE it_chk_alt ADD CONSTRAINT u UNIQUE (v, w)`)
	h.exec(`ALTER TABLE it_chk_alt ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES it_chk_empty (id) ON DELETE CASCADE`)
	h.exec(`ALTER TABLE it_chk_alt ADD UNIQUE (id);`)
	h.expectChecks(raw, "it_chk_alt", "it_chk_alt_v_check: v <> 0", "it_chk_alt_check: w > v OR v IS NULL", "it_chk_alt_x_check: x > 0", "id_pos: id > 0")

	// DROP CONSTRAINT.
	h.exec(`ALTER TABLE it_chk_alt DROP CONSTRAINT id_pos`)
	h.exec(`ALTER TABLE it_chk_alt DROP CONSTRAINT IT_CHK_ALT_CHECK CASCADE`)
	h.exec(`ALTER TABLE it_chk_alt DROP CONSTRAINT IF EXISTS id_pos`)
	h.expectError(`ALTER TABLE it_chk_alt DROP CONSTRAINT id_pos`, `constraint "id_pos" of relation "it_chk_alt" does not exist`)
	h.expectError(`ALTER TABLE it_chk_alt DROP CONSTRAINT u`, `constraint "u" of relation "it_chk_alt" does not exist`)
	h.expectChecks(raw, "it_chk_alt", "it_chk_alt_v_check: v <> 0", "it_chk_alt_x_check: x > 0")
	h.exec(`INSERT INTO it_chk_alt (id, v, w) VALUES (0, 30, 1)`)
	h.expectError(`ALTER TABLE it_chk_alt DROP CONSTRAINT`, `expected identifier near ""`)
	h.expectError(`ALTER TABLE it_chk_alt ADD CONSTRAINT c`, `syntax error: expected a constraint after CONSTRAINT c near ""`)
	h.expectError(`ALTER TABLE it_chk_alt ADD CHECK v > 0`, `syntax error: expected "(" near "v"`)
	h.expectError(`ALTER TABLE it_chk_alt ALTER COLUMN v SET NOT NULL`, `unsupported ALTER COLUMN action near "SET"`)
	h.expectError(`ALTER TABLE it_chk_alt VALIDATE CONSTRAINT x`,
		`unsupported ALTER TABLE action near "VALIDATE" (supported: RENAME TO, RENAME COLUMN, ADD COLUMN, DROP COLUMN, ALTER COLUMN, ADD CONSTRAINT, DROP CONSTRAINT)`)
	h.expectError(`ALTER VIEW it_chk_alt DROP CONSTRAINT x`, "ALTER VIEW supports only RENAME TO")
}

// CHECK constraints stay with the table through RENAME TO (also when it
// moves the rows), RENAME COLUMN and TRUNCATE; DROP COLUMN drops those that
// read the column. They show in GetObjects' table_constraints.
func TestSQLCheckLifetime(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_chk_life", "it_chk_life2", "it_chk_life3")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_chk_life (id integer, amount integer check (amount >= 0), lo integer, hi integer,
		check (lo <= hi), constraint "Big" check (amount < 1000 and it_chk_life.hi < 1000), check (id > 0))`)
	h.exec(`INSERT INTO it_chk_life VALUES (1, 5, 1, 2)`)
	checks := []string{"it_chk_life_amount_check: amount >= 0", "it_chk_life_check: lo <= hi",
		"Big: amount < 1000 and hi < 1000", "it_chk_life_id_check: id > 0"}
	h.expectChecks(raw, "it_chk_life", checks...)
	if got, want := strings.Join(h.tableConstraints("public", "it_chk_life"), "\n"), strings.Join([]string{"table",
		"it_chk_life_amount_check CHECK (amount)", "it_chk_life_check CHECK (lo, hi)",
		"Big CHECK (amount, hi)", "it_chk_life_id_check CHECK (id)"}, "\n"); got != want {
		t.Errorf("GetObjects constraints\n got: %q\nwant: %q", got, want)
	}

	// RENAME TO keeps them, and their names; errors name the table as it is
	// now, as in Postgres.
	h.exec(`ALTER TABLE it_chk_life RENAME TO it_chk_life2`)
	h.expectChecks(raw, "it_chk_life2", checks...)
	h.expectError(`INSERT INTO it_chk_life2 VALUES (2, -1, 1, 2)`, `new row for relation "it_chk_life2" violates check constraint "it_chk_life_amount_check"`)
	h.expectError(`UPDATE it_chk_life2 SET hi = 1000`, `new row for relation "it_chk_life2" violates check constraint "Big"`)

	// RENAME COLUMN rewrites the expressions, quoting a name only when it
	// needs it.
	h.exec(`ALTER TABLE it_chk_life2 RENAME COLUMN amount TO "Amount Due"`)
	h.exec(`ALTER TABLE it_chk_life2 RENAME COLUMN LO TO low`)
	h.exec(`ALTER TABLE it_chk_life2 RENAME COLUMN hi TO "interval"`)
	h.exec(`ALTER TABLE it_chk_life2 RENAME COLUMN id TO Ident`)
	checks = []string{`it_chk_life_amount_check: "Amount Due" >= 0`, `it_chk_life_check: low <= "interval"`,
		`Big: "Amount Due" < 1000 and "interval" < 1000`, "it_chk_life_id_check: Ident > 0"}
	h.expectChecks(raw, "it_chk_life2", checks...)
	h.expectError(`INSERT INTO it_chk_life2 VALUES (2, -1, 1, 2)`, `violates check constraint "it_chk_life_amount_check"`)
	h.expectError(`INSERT INTO it_chk_life2 VALUES (2, 1, 3, 2)`, `violates check constraint "it_chk_life_check"`)
	h.expectError(`INSERT INTO it_chk_life2 ("interval") VALUES (1000)`, `violates check constraint "Big"`)
	h.expectError(`UPDATE it_chk_life2 SET ident = 0`, `violates check constraint "it_chk_life_id_check"`)
	h.exec(`ALTER TABLE it_chk_life2 RENAME COLUMN "interval" TO hi`)
	checks[1], checks[2] = `it_chk_life_check: low <= hi`, `Big: "Amount Due" < 1000 and hi < 1000`
	h.expectChecks(raw, "it_chk_life2", checks...)
	if got, want := strings.Join(h.tableConstraints("public", "it_chk_life2"), "\n"), strings.Join([]string{"table",
		"it_chk_life_amount_check CHECK (Amount Due)",
		"it_chk_life_check CHECK (low, hi)", "Big CHECK (Amount Due, hi)", "it_chk_life_id_check CHECK (Ident)"}, "\n"); got != want {
		t.Errorf("GetObjects constraints\n got: %q\nwant: %q", got, want)
	}

	// TRUNCATE keeps them.
	h.exec(`TRUNCATE it_chk_life2`)
	h.expectChecks(raw, "it_chk_life2", checks...)
	h.expectError(`INSERT INTO it_chk_life2 VALUES (2, -1, 1, 2)`, `violates check constraint "it_chk_life_amount_check"`)
	h.exec(`INSERT INTO it_chk_life2 VALUES (2, 1, 1, 2)`)

	// DROP COLUMN drops the constraints that read it, including ones that
	// read other columns too.
	h.exec(`ALTER TABLE it_chk_life2 DROP COLUMN hi`)
	h.expectChecks(raw, "it_chk_life2", checks[0], checks[3])
	h.exec(`INSERT INTO it_chk_life2 VALUES (3, 1, 100)`)
	h.exec(`ALTER TABLE it_chk_life2 ADD COLUMN hi integer`)
	h.exec(`INSERT INTO it_chk_life2 VALUES (4, 1, 100, 1)`)
	h.expectError(`INSERT INTO it_chk_life2 VALUES (5, -1, 100, 1)`, `violates check constraint "it_chk_life_amount_check"`)

	// A re-keying rename moves the rows and keeps the constraints.
	if err := h.setConnOption(OptionStringRenameRekey, "true"); err != nil {
		t.Fatal(err)
	}
	h.exec(`ALTER TABLE it_chk_life2 RENAME TO it_chk_life3`)
	h.namesN(raw, "public", "it_chk_life3", "it_chk_life3") // the new name's keys
	h.expectChecks(raw, "it_chk_life3", checks[0], checks[3])
	h.expectError(`INSERT INTO it_chk_life3 VALUES (5, -1, 100, 1)`, `new row for relation "it_chk_life3" violates check constraint "it_chk_life_amount_check"`)
	h.expectError(`UPDATE it_chk_life3 SET ident = -1`, `violates check constraint "it_chk_life_id_check"`)
	h.expectRows(`SELECT ident, "Amount Due", low, hi FROM it_chk_life3 ORDER BY ident`, "2|1|1|NULL", "3|1|100|NULL", "4|1|100|1")

	// The stored text keeps comments and quoting inside the expression, and
	// RENAME COLUMN finds every reference: also a column named like a date
	// part in DATE_ADD(day, INTERVAL …).
	h.dropTables("it_chk_text")
	h.exec(`CREATE TABLE it_chk_text (day date, label varchar,
		check (DATE_ADD(day, INTERVAL 1 DAY) > DATE '2020-01-01' /* after 2020 */),
		check (label /* not empty */ <> $$$$ AND "label" <> 'it''s' -- no quote
		))`)
	h.expectChecks(raw, "it_chk_text", `it_chk_text_day_check: DATE_ADD(day, INTERVAL 1 DAY) > DATE '2020-01-01'`,
		`it_chk_text_label_check: label /* not empty */ <> $$$$ AND "label" <> 'it''s'`)
	h.exec(`ALTER TABLE it_chk_text RENAME COLUMN day TO d`)
	h.exec(`ALTER TABLE it_chk_text RENAME COLUMN label TO "Label"`)
	h.expectChecks(raw, "it_chk_text", `it_chk_text_day_check: DATE_ADD(d, INTERVAL 1 DAY) > DATE '2020-01-01'`,
		`it_chk_text_label_check: Label /* not empty */ <> $$$$ AND Label <> 'it''s'`)
	h.expectError(`INSERT INTO it_chk_text VALUES (DATE '2019-12-30', 'a')`, `violates check constraint "it_chk_text_day_check"`)
	h.expectError(`INSERT INTO it_chk_text VALUES (DATE '2020-01-01', '')`, `violates check constraint "it_chk_text_label_check"`)
	h.expectError(`INSERT INTO it_chk_text VALUES (DATE '2020-01-01', 'it''s')`, `violates check constraint "it_chk_text_label_check"`)
	h.exec(`ALTER TABLE it_chk_text RENAME COLUMN d TO month`)
	h.expectChecks(raw, "it_chk_text", `it_chk_text_day_check: DATE_ADD(month, INTERVAL 1 DAY) > DATE '2020-01-01'`,
		`it_chk_text_label_check: Label /* not empty */ <> $$$$ AND Label <> 'it''s'`)
	h.expectError(`INSERT INTO it_chk_text VALUES (DATE '2019-12-30', 'a')`, `violates check constraint "it_chk_text_day_check"`)
	h.exec(`INSERT INTO it_chk_text VALUES (DATE '2020-01-01', 'a')`)

	// DROP TABLE drops them; the metadata of a table without any has no
	// checks field.
	h.exec(`DROP TABLE it_chk_life3`)
	h.exec(`CREATE TABLE it_chk_life3 (amount integer)`)
	h.exec(`INSERT INTO it_chk_life3 VALUES (-1)`)
	if raw, err := raw.Get(h.ctx, metaKey("public", "it_chk_life3")).Result(); err != nil || strings.Contains(raw, "checks") {
		t.Errorf("metadata %s (%v)", raw, err)
	}
	if got := h.tableConstraints("public", "it_chk_life3"); strings.Join(got, ",") != "table" {
		t.Errorf("GetObjects constraints %q", got)
	}
}

// Metadata written before CHECK support reads as a table without any, and
// a stored CHECK that no longer binds (say a client older than CHECK
// support renamed its column, which keeps the text) stops writes with an
// error that says how to drop it.
func TestSQLCheckMetadata(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_chk_meta")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_chk_meta (a integer check (a > 0), b integer)`)
	key := metaKey("public", "it_chk_meta")
	js, err := raw.Get(h.ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(js), &m); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(m["checks"]); got != "[map[expr:a > 0 name:it_chk_meta_a_check]]" {
		t.Errorf("stored checks %s", got)
	}
	// A rename that kept the text, as an older client's would.
	cols := m["columns"].([]any)
	cols[0].(map[string]any)["name"] = "x"
	cols[0].(map[string]any)["field"] = "a"
	out, _ := json.Marshal(m)
	if err := raw.Set(h.ctx, key, out, 0).Err(); err != nil {
		t.Fatal(err)
	}
	msg := `check constraint "it_chk_meta_a_check" of relation "it_chk_meta" can't be evaluated (column "a" does not exist in table "it_chk_meta"); drop it with ALTER TABLE … DROP CONSTRAINT`
	h.expectError(`INSERT INTO it_chk_meta VALUES (1, 1)`, msg)
	h.expectError(`UPDATE it_chk_meta SET b = 2`, msg)
	h.exec(`ALTER TABLE it_chk_meta DROP CONSTRAINT it_chk_meta_a_check`)
	h.exec(`INSERT INTO it_chk_meta VALUES (-1, 1)`)
	h.expectRows(`SELECT x, b FROM it_chk_meta`, "-1|1")

	// Without the checks field (an older table), nothing is checked.
	delete(m, "checks")
	out, _ = json.Marshal(m)
	if err := raw.Set(h.ctx, key, out, 0).Err(); err != nil {
		t.Fatal(err)
	}
	h.exec(`INSERT INTO it_chk_meta VALUES (-2, 1)`)
	h.expectRows(`SELECT COUNT(*) FROM it_chk_meta`, "2")
}

// Temporary tables check their constraints like permanent ones, also when
// one shadows a permanent table of the same name.
func TestSQLCheckTemp(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_chk_tmp")
	h.exec(`CREATE TABLE it_chk_tmp (a integer check (a > 100))`)
	h.exec(`CREATE TEMP TABLE it_chk_tmp (a integer check (a > 0), b integer, check (b <> a))`)
	t.Cleanup(func() { h.exec(`DROP TABLE IF EXISTS pg_temp.it_chk_tmp`) })
	h.exec(`INSERT INTO it_chk_tmp VALUES (1, 2)`)
	h.expectError(`INSERT INTO it_chk_tmp VALUES (0, 2)`, `new row for relation "it_chk_tmp" violates check constraint "it_chk_tmp_a_check"`)
	h.expectError(`INSERT INTO pg_temp.it_chk_tmp VALUES (2, 2)`, `violates check constraint "it_chk_tmp_check"`)
	h.expectError(`INSERT INTO public.it_chk_tmp VALUES (50)`, `violates check constraint "it_chk_tmp_a_check"`)
	h.exec(`INSERT INTO public.it_chk_tmp VALUES (150)`)
	h.expectError(`UPDATE it_chk_tmp SET b = 1`, `violates check constraint "it_chk_tmp_check"`)
	h.exec(`ALTER TABLE it_chk_tmp ADD COLUMN c integer DEFAULT 3 CHECK (c > b)`)
	h.expectError(`ALTER TABLE it_chk_tmp ADD COLUMN d integer DEFAULT 1 CHECK (d > b)`, `check constraint "it_chk_tmp_check2" of relation "it_chk_tmp" is violated by some row`)
	h.exec(`ALTER TABLE it_chk_tmp RENAME COLUMN b TO bb`)
	h.expectError(`MERGE INTO it_chk_tmp t USING (SELECT 1 AS a) s ON t.a = s.a WHEN MATCHED THEN UPDATE SET c = 0`,
		`violates check constraint "it_chk_tmp_check1"`)
	h.expectRows(`SELECT a, bb, c FROM it_chk_tmp`, "1|2|3")
	if got := strings.Join(h.tableConstraints("pg_temp", "it_chk_tmp"), "\n"); got != strings.Join([]string{"table",
		"it_chk_tmp_a_check CHECK (a)", "it_chk_tmp_check CHECK (a, bb)", "it_chk_tmp_check1 CHECK (bb, c)"}, "\n") {
		t.Errorf("GetObjects constraints of the temporary table %q", got)
	}
	h.exec(`ALTER TABLE it_chk_tmp DROP COLUMN bb`)
	h.exec(`INSERT INTO it_chk_tmp VALUES (5, 0)`)
	h.expectError(`INSERT INTO it_chk_tmp VALUES (0, 0)`, `violates check constraint "it_chk_tmp_a_check"`)
	h.exec(`TRUNCATE it_chk_tmp`)
	h.exec(`ALTER TABLE it_chk_tmp RENAME TO it_chk_tmp2`)
	h.expectError(`INSERT INTO it_chk_tmp2 VALUES (0, 0)`, `new row for relation "it_chk_tmp2" violates check constraint "it_chk_tmp_a_check"`)
	h.exec(`ALTER TABLE it_chk_tmp2 RENAME TO it_chk_tmp`)

	// Another connection sees only the permanent table.
	h2 := newSQLHarness(t)
	h2.expectError(`INSERT INTO it_chk_tmp VALUES (50)`, `violates check constraint "it_chk_tmp_a_check"`)
	h2.exec(`INSERT INTO it_chk_tmp VALUES (101)`)
	h.expectRows(`SELECT a FROM public.it_chk_tmp ORDER BY a`, "101", "150")
}

// The DDL dbt renders for a model with an enforced contract (columns with
// their constraints, then the model-level ones) and the insert that
// follows, in one script, as the redis_adbc adapter runs them.
func TestSQLCheckDbt(t *testing.T) {
	h := newSQLHarness(t)
	h.exec(`DROP SCHEMA IF EXISTS it_chk_s CASCADE`)
	h.exec(`CREATE SCHEMA it_chk_s`)
	t.Cleanup(func() { h.exec(`DROP SCHEMA IF EXISTS it_chk_s CASCADE`) })
	h.exec(`CREATE TABLE it_chk_s.zones (location_id integer, zone varchar)`)
	h.exec(`INSERT INTO it_chk_s.zones VALUES (1, 'Newark'), (2, 'Queens')`)
	ddl := func(rows string) string {
		return `
  create table it_chk_s.t
    (
    id integer not null primary key,
    amount numeric check (amount >= 0),
    zone_id integer references it_chk_s.zones (location_id),
    check (id > 0),
    constraint fk_z foreign key (zone_id) references it_chk_s.zones (location_id)
    )
  ;
  insert into it_chk_s.t (id, amount, zone_id)
  (
    select id, amount, zone_id
    from (
        ` + rows + `
    ) as model_subq
  );
`
	}
	h.expectError(ddl(`select 1 as id, 10.5 as amount, 1 as zone_id
        union all select 2 as id, -1 as amount, 2 as zone_id`),
		`new row for relation "t" violates check constraint "t_amount_check"`)
	// The table was created, and the insert wrote nothing.
	h.expectRows(`SELECT COUNT(*) FROM it_chk_s.t`, "0")
	if got := h.storedChecks(h.rawClient(), "it_chk_s", "t"); strings.Join(got, "\n") != "t_amount_check: amount >= 0\nt_id_check: id > 0" {
		t.Errorf("checks %q", got)
	}
	h.exec(`DROP TABLE it_chk_s.t`)
	h.expectError(ddl(`select 0 as id, 1 as amount, 1 as zone_id`), `new row for relation "t" violates check constraint "t_id_check"`)
	h.exec(`DROP TABLE it_chk_s.t`)
	h.expectError(ddl(`select cast(null as integer) as id, 1 as amount, 1 as zone_id`), `NULL value in column "id" violates not-null constraint`)
	h.exec(`DROP TABLE it_chk_s.t`)
	// A NULL amount passes, and the foreign key isn't enforced (zone 9).
	h.exec(ddl(`select 1 as id, 10.5 as amount, 1 as zone_id
        union all select 2 as id, null as amount, 9 as zone_id
        union all select 2 as id, 0 as amount, 2 as zone_id`))
	h.expectRows(`SELECT id, amount, zone_id FROM it_chk_s.t ORDER BY id, zone_id`, "1|10.500000000|1", "2|0.000000000|2", "2|NULL|9")
	if got := strings.Join(h.tableConstraints("it_chk_s", "t"), "\n"); got != "table\nt_amount_check CHECK (amount)\nt_id_check CHECK (id)" {
		t.Errorf("GetObjects constraints %q", got)
	}
}
