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

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

func (h *sqlHarness) dropTables(names ...string) {
	h.t.Helper()
	drop := func() {
		for _, n := range names {
			h.exec("DROP TABLE IF EXISTS " + n)
		}
	}
	drop()
	h.t.Cleanup(drop)
}

// columnDefs lists GetObjects' xdbc_column_def of a table's columns as
// "column=default".
func (h *sqlHarness) columnDefs(table string) []string {
	h.t.Helper()
	rdr, err := h.conn.GetObjects(h.ctx, adbc.ObjectDepthColumns, nil, nil, &table, nil, nil)
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
						Columns []struct {
							Name string  `json:"column_name"`
							Def  *string `json:"xdbc_column_def"`
						} `json:"table_columns"`
					} `json:"db_schema_tables"`
				} `json:"catalog_db_schemas"`
			}
			if err := dec.Decode(&cat); err != nil {
				h.t.Fatal(err)
			}
			for _, s := range cat.Schemas {
				for _, t := range s.Tables {
					for _, c := range t.Columns {
						def := ""
						if c.Def != nil {
							def = *c.Def
						}
						out = append(out, c.Name+"="+def)
					}
				}
			}
		}
	}
	return out
}

// tableColumn returns a column's metadata as stored.
func (h *sqlHarness) tableColumn(c goredis.UniversalClient, table, column string) columnMeta {
	h.t.Helper()
	meta, err := (&store{client: c}).getTable(h.ctx, defaultSchema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	col, ok := meta.column(column)
	if !ok {
		h.t.Fatalf("column %q of %q not found", column, table)
	}
	return col
}

// pushedWhere returns the index query and whether a residual is left for
// the WHERE clause of a single-table SELECT.
func (h *sqlHarness) pushedWhere(c goredis.UniversalClient, sql string) (string, bool) {
	h.t.Helper()
	parsed, err := ParseScript(sql)
	if err != nil {
		h.t.Fatal(err)
	}
	e := &executor{store: &store{client: c}, schema: defaultSchema}
	e.cache = newExecCache()
	plan, err := e.planSelect(h.ctx, parsed[0].Stmt.(*SelectStmt), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	wp, err := e.planWhere(h.ctx, plan.sel.Where, plan.meta, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	return wp.query, wp.residual != nil
}

// CREATE TABLE defaults apply to every form of INSERT; NOT NULL + DEFAULT
// works, and information_schema / GetObjects show the default's text.
func TestSQLDefaultsInsert(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_def_d", "it_def_ts", "it_def_rand", "it_def_types", "it_def_null", "it_def_ctas")

	h.exec(`CREATE TABLE it_def_d (a INTEGER, b INTEGER DEFAULT 5, c TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		s VARCHAR NOT NULL DEFAULT 'x')`)
	h.exec(`INSERT INTO it_def_d (a) VALUES (1)`)
	h.exec(`INSERT INTO it_def_d VALUES (2, DEFAULT, DEFAULT, 'y')`)
	h.exec(`INSERT INTO it_def_d DEFAULT VALUES`)
	// An explicit NULL stays NULL; DEFAULT for a column without one is NULL.
	h.exec(`INSERT INTO it_def_d (a, b) VALUES (4, NULL)`)
	h.exec(`INSERT INTO it_def_d VALUES (DEFAULT, 6, NULL, DEFAULT)`)
	h.expectError(`INSERT INTO it_def_d (a, s) VALUES (5, NULL)`, `NULL value in column "s" violates not-null constraint`)
	// INSERT … SELECT with a column list.
	if n := h.exec(`INSERT INTO it_def_d (a, b) SELECT a + 100, b * 2 FROM it_def_d WHERE a = 1`); n != 1 {
		t.Errorf("INSERT … SELECT affected %d rows, want 1", n)
	}
	h.expectRows(`SELECT a, b, c IS NOT NULL, s FROM it_def_d ORDER BY __rowid`,
		"1|5|true|x", "2|5|true|y", "NULL|5|true|x", "4|NULL|true|x", "NULL|6|false|x", "101|10|true|x")
	h.expectRows(`SELECT a FROM it_def_d WHERE c > CURRENT_TIMESTAMP - INTERVAL '10 minutes' AND c <= CURRENT_TIMESTAMP
		ORDER BY __rowid`, "1", "2", "NULL", "4", "101")

	h.expectRows(`SELECT column_name, column_default, is_nullable FROM information_schema.columns
		WHERE table_name = 'it_def_d' ORDER BY ordinal_position`,
		"a|NULL|YES", "b|5|YES", "c|CURRENT_TIMESTAMP|YES", "s|'x'|NO")
	if got, want := strings.Join(h.columnDefs("it_def_d"), " "), "a= b=5 c=CURRENT_TIMESTAMP s='x'"; got != want {
		t.Errorf("GetObjects xdbc_column_def = %q, want %q", got, want)
	}

	// The current time is the statement's, the same for every row and as
	// CURRENT_TIMESTAMP in the statement itself.
	h.exec(`CREATE TABLE it_def_ts (k INTEGER, t1 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		t2 TIMESTAMP WITH TIME ZONE DEFAULT NOW(), d DATE DEFAULT CURRENT_DATE, given TIMESTAMP)`)
	h.exec(`INSERT INTO it_def_ts (k, given) VALUES (1, CURRENT_TIMESTAMP), (2, CURRENT_TIMESTAMP)`)
	h.exec(`INSERT INTO it_def_ts (k, given) SELECT k + 2, CURRENT_TIMESTAMP FROM it_def_ts`)
	h.expectRows(`SELECT k, t1 = given, t2 = given, d = CAST(given AS DATE) FROM it_def_ts ORDER BY k`,
		"1|true|true|true", "2|true|true|true", "3|true|true|true", "4|true|true|true")
	h.expectRows(`SELECT COUNT(DISTINCT t1) FROM it_def_ts WHERE k <= 2`, "1")

	// A volatile default is computed for every row.
	h.exec(`CREATE TABLE it_def_rand (k INTEGER, r DOUBLE PRECISION DEFAULT RANDOM())`)
	h.exec(`INSERT INTO it_def_rand (k) VALUES (1), (2), (3), (4), (5), (6), (7), (8)`)
	h.expectRows(`SELECT COUNT(DISTINCT r) > 1, MIN(r) >= 0, MAX(r) < 1 FROM it_def_rand`, "true|true|true")

	// Defaults are converted to the column types.
	h.exec(`CREATE TABLE it_def_types (n NUMERIC(6,2) DEFAULT 1.005, f DOUBLE PRECISION DEFAULT 2,
		b BOOLEAN DEFAULT 'yes', dt DATE DEFAULT '2024-02-29', s VARCHAR DEFAULT 42, i SMALLINT DEFAULT -7,
		e VARCHAR DEFAULT '')`)
	h.exec(`INSERT INTO it_def_types DEFAULT VALUES`)
	h.expectRows(`SELECT n, f, b, dt, s, i, e = '' FROM it_def_types`, "1.01|2|true|2024-02-29|42|-7|true")

	// DEFAULT NULL is the same as no default, as in Postgres.
	h.exec(`CREATE TABLE it_def_null (a INTEGER DEFAULT NULL, s VARCHAR NOT NULL DEFAULT NULL,
		v VARCHAR DEFAULT CAST(NULL AS VARCHAR))`)
	h.expectRows(`SELECT column_name, column_default FROM information_schema.columns
		WHERE table_name = 'it_def_null' ORDER BY ordinal_position`, "a|NULL", "s|NULL", "v|NULL")
	h.expectError(`INSERT INTO it_def_null (a) VALUES (1)`, `NULL value in column "s" violates not-null constraint`)

	// CREATE TABLE … AS copies values, not defaults.
	h.exec(`CREATE TABLE it_def_ctas AS SELECT a, b, s FROM it_def_d`)
	h.expectRows(`SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'it_def_ctas' AND column_default IS NOT NULL`, "0")
	h.exec(`INSERT INTO it_def_ctas (a) VALUES (9)`)
	h.expectRows(`SELECT a, b, s FROM it_def_ctas WHERE a IN (1, 9) ORDER BY a`, "1|5|x", "9|NULL|NULL")
}

// Defaults are checked when they are defined, with Postgres's errors.
func TestSQLDefaultsValidation(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_def_bad", "it_def_ok")

	for def, msg := range map[string]string{
		"INTEGER DEFAULT a":                       "cannot use column reference in DEFAULT expression",
		"INTEGER DEFAULT a + 1":                   "cannot use column reference in DEFAULT expression",
		"INTEGER DEFAULT (SELECT 1)":              "cannot use subquery in DEFAULT expression",
		"BOOLEAN DEFAULT EXISTS (SELECT 1)":       "cannot use subquery in DEFAULT expression",
		"BIGINT DEFAULT COUNT(*)":                 "aggregate functions are not allowed in DEFAULT expressions",
		"BIGINT DEFAULT ROW_NUMBER() OVER ()":     "window functions are not allowed in DEFAULT expressions",
		"INTEGER DEFAULT 'abc'":                   `column "b"`,
		"DATE DEFAULT 'tomorrow-ish'":             `column "b"`,
		"SMALLINT DEFAULT 100000":                 "out of range",
		"INTEGER DEFAULT 1 / 0":                   "division by zero",
		"INTEGER DEFAULT CURRENT_TIMESTAMP":       `column "b"`,
		"INTEGER DEFAULT no_such_function()":      "NO_SUCH_FUNCTION",
		"VARCHAR NOT NULL DEFAULT UPPER(1, 2, 3)": "UPPER",
		"INTEGER DEFAULT 1 NOT NULL DEFAULT 2":    `multiple default values specified for column "b"`,
	} {
		h.expectError("CREATE TABLE it_def_bad (a INTEGER, b "+def+")", msg)
	}
	h.expectRows(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'it_def_bad'`, "0")

	h.exec(`CREATE TABLE it_def_ok (a INTEGER)`)
	h.exec(`INSERT INTO it_def_ok VALUES (1)`)
	h.expectError(`ALTER TABLE it_def_ok ADD COLUMN c INTEGER DEFAULT a`, "cannot use column reference in DEFAULT expression")
	h.expectError(`ALTER TABLE it_def_ok ADD COLUMN c INTEGER DEFAULT 'abc'`, `column "c"`)
	h.expectError(`ALTER TABLE it_def_ok ADD COLUMN c INTEGER NOT NULL`, "NOT NULL")
	h.expectError(`ALTER TABLE it_def_ok ADD COLUMN c INTEGER NOT NULL DEFAULT NULL`, "NOT NULL")
	h.expectError(`ALTER TABLE it_def_ok ADD COLUMN c DOUBLE PRECISION DEFAULT RANDOM()`, "volatile DEFAULT")
	h.expectError(`ALTER TABLE it_def_ok ALTER COLUMN nope SET DEFAULT 1`, `column "nope" does not exist`)
	h.expectError(`ALTER TABLE it_def_ok ALTER COLUMN a SET DEFAULT 'many'`, `column "a"`)
	h.expectError(`ALTER TABLE it_def_ok ALTER COLUMN a SET DEFAULT a`, "cannot use column reference in DEFAULT expression")
	h.expectError(`ALTER TABLE it_def_ok ALTER COLUMN a TYPE BIGINT`, "unsupported ALTER COLUMN action")
	h.expectRows(`SELECT column_name, column_default FROM information_schema.columns WHERE table_name = 'it_def_ok'`, "a|NULL")
	h.expectRows(`SELECT * FROM it_def_ok`, "1")
}

// MERGE's INSERT uses the defaults too: for omitted columns, DEFAULT in
// VALUES and INSERT DEFAULT VALUES.
func TestSQLDefaultsMerge(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_def_m", "it_def_m2")

	h.exec(`CREATE TABLE it_def_m (id INTEGER NOT NULL, qty INTEGER DEFAULT 1, note VARCHAR NOT NULL DEFAULT 'new')`)
	h.exec(`INSERT INTO it_def_m (id) VALUES (1)`)
	h.expectAffected(`MERGE INTO it_def_m t USING (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET qty = t.qty + 10
		WHEN NOT MATCHED AND s.id = 2 THEN INSERT (id) VALUES (s.id)
		WHEN NOT MATCHED THEN INSERT VALUES (s.id, DEFAULT, 'merged')`, 3)
	h.expectRows(`SELECT id, qty, note FROM it_def_m ORDER BY id`, "1|11|new", "2|1|new", "3|1|merged")

	h.exec(`CREATE TABLE it_def_m2 (k INTEGER DEFAULT 7, v VARCHAR DEFAULT 'v', at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`)
	h.expectAffected(`MERGE INTO it_def_m2 t USING (SELECT 1 AS x UNION ALL SELECT 2) s ON FALSE
		WHEN NOT MATCHED THEN INSERT DEFAULT VALUES`, 2)
	h.expectRows(`SELECT k, v, COUNT(DISTINCT at) FROM it_def_m2 GROUP BY k, v`, "7|v|1")
}

// Bulk ingest fills the columns the Arrow data doesn't have with their
// defaults; a NULL in the data stays NULL.
func TestSQLDefaultsIngest(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_def_ing")

	h.exec(`CREATE TABLE it_def_ing (id BIGINT NOT NULL, label VARCHAR DEFAULT 'none',
		qty INTEGER NOT NULL DEFAULT 0, at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`)
	ids := array.NewInt64Builder(memory.DefaultAllocator)
	defer ids.Release()
	ids.AppendValues([]int64{1, 2, 3}, nil)
	labels := array.NewStringBuilder(memory.DefaultAllocator)
	defer labels.Release()
	labels.AppendValues([]string{"a", "", "c"}, []bool{true, false, true})
	idCol, labelCol := ids.NewArray(), labels.NewArray()
	defer idCol.Release()
	defer labelCol.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil), []arrow.Array{idCol, labelCol}, 3)
	defer rec.Release()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	for k, v := range map[string]string{adbc.OptionKeyIngestTargetTable: "it_def_ing", adbc.OptionKeyIngestMode: adbc.OptionValueIngestModeAppend} {
		if err := st.SetOption(h.ctx, k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Bind(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ExecuteUpdate(h.ctx); err != nil || n != 3 {
		t.Fatalf("ingest: n=%d err=%v", n, err)
	}
	if err := h.ingest("it_def_ing", adbc.OptionValueIngestModeAppend, false, 4, 5); err != nil {
		t.Fatal(err)
	}
	h.expectRows(`SELECT id, label, qty, at IS NOT NULL FROM it_def_ing ORDER BY id`,
		"1|a|0|true", "2|NULL|0|true", "3|c|0|true", "4|none|0|true", "5|none|0|true")
	h.expectRows(`SELECT COUNT(DISTINCT at) FROM it_def_ing WHERE id <= 3`, "1")
}

// ADD COLUMN … DEFAULT: the rows that existed read the default without being
// rewritten; rows inserted later read what they were given, NULL included.
// Every read path agrees: scans, index filters, sorts, aggregates, joins,
// subqueries, views, and the rows UPDATE / DELETE / MERGE match.
func TestSQLAddColumnDefault(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP VIEW IF EXISTS it_def_v")
	t.Cleanup(func() { h.exec("DROP VIEW IF EXISTS it_def_v") })
	h.dropTables("it_def_add", "it_def_keys", "it_def_copy")
	raw := h.rawClient()

	h.exec(`CREATE TABLE it_def_add (id INTEGER NOT NULL, label VARCHAR)`)
	h.exec(`INSERT INTO it_def_add VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')`)
	h.exec(`ALTER TABLE it_def_add ADD COLUMN qty INTEGER DEFAULT 7`)
	h.exec(`ALTER TABLE it_def_add ADD COLUMN tag VARCHAR NOT NULL DEFAULT 'old'`)
	h.exec(`ALTER TABLE it_def_add ADD COLUMN blank VARCHAR DEFAULT ''`)
	if f := h.rowFields(raw, "public", "it_def_add"); f["qty"] != 0 || f["tag"] != 0 || f["blank"] != 0 {
		t.Errorf("row fields after ADD COLUMN … DEFAULT = %v, want no qty, tag or blank fields", f)
	}
	if c := h.tableColumn(raw, "it_def_add", "qty"); c.Missing != "7" || c.MissingThrough != 4 || c.Default != "7" {
		t.Errorf("qty metadata = %+v, want missing value 7 through row 4", c)
	}
	h.exec(`INSERT INTO it_def_add (id, label) VALUES (5, 'e')`)
	h.exec(`INSERT INTO it_def_add (id, label, qty, tag, blank) VALUES (6, 'f', NULL, 'new', NULL), (7, 'g', 13, 'new', 'x')`)
	h.expectError(`INSERT INTO it_def_add (id, tag) VALUES (99, NULL)`, `NULL value in column "tag" violates not-null constraint`)
	all := []string{"1|a|7|old|", "2|b|7|old|", "3|c|7|old|", "4|d|7|old|", "5|e|7|old|", "6|f|NULL|new|NULL", "7|g|13|new|x"}
	h.expectRows(`SELECT * FROM it_def_add ORDER BY id`, all...)
	h.expectRows(`SELECT * FROM it_def_add WHERE __rowid = 2`, all[1])

	// Index filters: a predicate the missing value satisfies also reads the
	// old rows (and is re-checked); one it doesn't is answered by the index.
	for where, want := range map[string][]string{
		"qty = 7":                               {"1", "2", "3", "4", "5"},
		"qty IS NULL":                           {"6"},
		"qty > 8":                               {"7"},
		"qty <= 7":                              {"1", "2", "3", "4", "5"},
		"qty BETWEEN 7 AND 8":                   {"1", "2", "3", "4", "5"},
		"qty <> 7":                              {"7"},
		"qty IN (7, 13)":                        {"1", "2", "3", "4", "5", "7"},
		"qty IN (1, 13)":                        {"7"},
		"qty = 7 OR qty = 1":                    {"1", "2", "3", "4", "5"},
		"qty = 7 AND label = 'b'":               {"2"},
		"tag = 'old'":                           {"1", "2", "3", "4", "5"},
		"tag = 'new'":                           {"6", "7"},
		"tag LIKE 'ol%'":                        {"1", "2", "3", "4", "5"},
		"blank = ''":                            {"1", "2", "3", "4", "5"},
		"blank = 'x'":                           {"7"},
		"qty * 2 = 14":                          {"1", "2", "3", "4", "5"},
		"COALESCE(qty, 0) = 0":                  {"6"},
		"qty = 7 AND tag = 'old'":               {"1", "2", "3", "4", "5"},
		"qty IN (SELECT 13 UNION ALL SELECT 7)": {"1", "2", "3", "4", "5", "7"},
	} {
		h.expectRows(`SELECT id FROM it_def_add WHERE `+where+` ORDER BY id`, want...)
	}
	for sql, want := range map[string]string{
		`SELECT id FROM it_def_add WHERE qty = 7`:     "(@qty:[7 7] | @__rowid:[-inf 4]) residual",
		`SELECT id FROM it_def_add WHERE qty > 8`:     "@qty:[(8 +inf]",
		`SELECT id FROM it_def_add WHERE tag = 'old'`: "(@tag:{old} | @__rowid:[-inf 4]) residual",
		`SELECT id FROM it_def_add WHERE tag = 'new'`: "@tag:{new}",
	} {
		q, residual := h.pushedWhere(raw, sql)
		if residual {
			q += " residual"
		}
		if q != want {
			t.Errorf("%s: pushed down %q, want %q", sql, q, want)
		}
	}

	// Sorts and aggregates are computed by the driver for such columns.
	h.expectRows(`SELECT id, qty FROM it_def_add ORDER BY qty DESC, id`,
		"7|13", "1|7", "2|7", "3|7", "4|7", "5|7", "6|NULL")
	h.expectRows(`SELECT id FROM it_def_add ORDER BY tag, id DESC LIMIT 2`, "7", "6")
	h.expectRows(`SELECT COUNT(*), COUNT(qty), SUM(qty), MIN(qty), MAX(qty), AVG(qty) FROM it_def_add`, "7|6|48|7|13|8")
	h.expectRows(`SELECT qty, COUNT(*) FROM it_def_add GROUP BY qty ORDER BY qty`, "7|5", "13|1", "NULL|1")
	h.expectRows(`SELECT tag, COUNT(*), SUM(qty) FROM it_def_add GROUP BY tag ORDER BY tag`, "new|2|13", "old|5|35")
	h.expectRows(`SELECT DISTINCT tag FROM it_def_add ORDER BY tag`, "new", "old")
	h.expectRows(`SELECT COUNT(*) FROM it_def_add WHERE qty = 7`, "5")

	// Joins (an index lookup join on qty), subqueries and views.
	h.exec(`CREATE TABLE it_def_keys (k INTEGER, name VARCHAR)`)
	h.exec(`INSERT INTO it_def_keys VALUES (7, 'seven'), (13, 'thirteen')`)
	h.expectRows(`SELECT t.id, k.name FROM it_def_keys k JOIN it_def_add t ON t.qty = k.k ORDER BY t.id`,
		"1|seven", "2|seven", "3|seven", "4|seven", "5|seven", "7|thirteen")
	h.expectRows(`SELECT t.id, k.name FROM it_def_add t LEFT JOIN it_def_keys k ON k.k = t.qty ORDER BY t.id`,
		"1|seven", "2|seven", "3|seven", "4|seven", "5|seven", "6|NULL", "7|thirteen")
	h.expectRows(`SELECT id FROM it_def_add WHERE qty IN (SELECT k FROM it_def_keys WHERE name = 'seven') ORDER BY id`,
		"1", "2", "3", "4", "5")
	h.expectRows(`SELECT id FROM it_def_add t WHERE EXISTS (SELECT 1 FROM it_def_keys k WHERE k.k = t.qty AND k.name = 'thirteen')`, "7")
	h.expectRows(`SELECT id FROM it_def_add t WHERE EXISTS (SELECT 1 FROM it_def_keys k WHERE k.k = t.qty AND k.name = 'seven')
		ORDER BY id`, "1", "2", "3", "4", "5")
	h.expectRows(`SELECT k.name FROM it_def_keys k WHERE EXISTS (SELECT 1 FROM it_def_add t WHERE t.qty = k.k AND t.id = 2)`, "seven")
	h.exec(`CREATE VIEW it_def_v AS SELECT id, qty AS q, tag FROM it_def_add`)
	h.expectRows(`SELECT id FROM it_def_v WHERE q = 7 AND tag = 'old' ORDER BY id`, "1", "2", "3", "4", "5")
	h.exec(`CREATE TABLE it_def_copy AS SELECT id, qty FROM it_def_add`)
	h.expectRows(`SELECT id, qty FROM it_def_copy WHERE qty = 7 ORDER BY id`, "1|7", "2|7", "3|7", "4|7", "5|7")

	// UPDATE / DELETE / MERGE match the old rows by the value they read;
	// setting an old row's column to NULL marks it, setting a value unmarks it.
	h.expectAffected(`UPDATE it_def_add SET qty = NULL WHERE id = 2`, 1)
	h.expectRows(`SELECT id, qty FROM it_def_add WHERE id <= 3 ORDER BY id`, "1|7", "2|NULL", "3|7")
	if f := h.rowFields(raw, "public", "it_def_add"); f[nullMarker("qty")] != 1 || f["qty"] != 2 {
		t.Errorf("row fields after UPDATE SET qty = NULL = %v, want one %s marker", f, nullMarker("qty"))
	}
	h.expectRows(`SELECT id FROM it_def_add WHERE qty = 7 ORDER BY id`, "1", "3", "4", "5")
	h.expectRows(`SELECT id FROM it_def_add WHERE qty IS NULL ORDER BY id`, "2", "6")
	h.expectAffected(`UPDATE it_def_add SET qty = qty + 1 WHERE id = 1`, 1)
	h.expectAffected(`UPDATE it_def_add SET qty = 8 WHERE id = 2`, 1)
	if f := h.rowFields(raw, "public", "it_def_add"); f[nullMarker("qty")] != 0 {
		t.Errorf("row fields after UPDATE SET qty = 8 = %v, want no %s marker", f, nullMarker("qty"))
	}
	h.expectAffected(`UPDATE it_def_add SET label = 'seven' WHERE qty = 7`, 3)
	h.expectAffected(`MERGE INTO it_def_add t USING (SELECT 3 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET qty = NULL`, 1)
	h.expectAffected(`UPDATE it_def_add t SET label = k.name FROM it_def_keys k WHERE t.qty = k.k AND t.id >= 5`, 2)
	h.expectRows(`SELECT id, label, qty FROM it_def_add ORDER BY id`,
		"1|a|8", "2|b|8", "3|seven|NULL", "4|seven|7", "5|seven|7", "6|f|NULL", "7|thirteen|13")
	h.expectAffected(`DELETE FROM it_def_add WHERE qty = 8 AND tag = 'old'`, 2)
	h.expectAffected(`DELETE FROM it_def_add t USING it_def_keys k WHERE t.qty = k.k AND k.name = 'thirteen'`, 1)
	h.expectRows(`SELECT id, qty FROM it_def_add ORDER BY id`, "3|NULL", "4|7", "5|7", "6|NULL")

	// The missing value follows RENAME COLUMN; DROP COLUMN removes the field
	// and the NULL markers.
	h.exec(`ALTER TABLE it_def_add RENAME COLUMN qty TO amount`)
	h.expectRows(`SELECT id FROM it_def_add WHERE amount = 7 ORDER BY id`, "4", "5")
	h.exec(`ALTER TABLE it_def_add DROP COLUMN amount`)
	waitFor(t, "dropped column cleanup", func() bool {
		f := h.rowFields(raw, "public", "it_def_add")
		return f["qty"] == 0 && f[nullMarker("qty")] == 0
	})
	h.exec(`ALTER TABLE it_def_add ADD COLUMN qty INTEGER`)
	h.expectRows(`SELECT id, qty, tag FROM it_def_add ORDER BY id`, "3|NULL|old", "4|NULL|old", "5|NULL|old", "6|NULL|new")

	// TRUNCATE forgets the missing values: after RESTART IDENTITY new rows
	// reuse the old row ids, and an absent field is NULL for them.
	h.exec(`ALTER TABLE it_def_add ADD COLUMN extra INTEGER DEFAULT 5`)
	h.expectRows(`SELECT id, extra FROM it_def_add WHERE extra = 5 ORDER BY id`, "3|5", "4|5", "5|5", "6|5")
	h.exec(`TRUNCATE it_def_add RESTART IDENTITY`)
	if c := h.tableColumn(raw, "it_def_add", "extra"); c.MissingThrough != 0 || c.Default != "5" {
		t.Errorf("extra metadata after TRUNCATE = %+v, want the default but no missing value", c)
	}
	h.exec(`INSERT INTO it_def_add (id, extra) VALUES (1, NULL)`)
	h.exec(`INSERT INTO it_def_add (id) VALUES (2)`)
	h.expectRows(`SELECT __rowid, id, tag, extra FROM it_def_add ORDER BY id`, "1|1|old|NULL", "2|2|old|5")
	h.expectRows(`SELECT id FROM it_def_add WHERE extra = 5`, "2")
}

// ALTER COLUMN … SET / DROP DEFAULT change what later inserts get; existing
// rows, and the missing value of a column added with a default, stay.
func TestSQLAlterColumnDefault(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_def_alt")
	h.exec(`CREATE TABLE it_def_alt (id INTEGER, qty INTEGER)`)
	h.exec(`INSERT INTO it_def_alt VALUES (1, NULL)`)
	h.exec(`ALTER TABLE it_def_alt ALTER COLUMN qty SET DEFAULT 40 + 2`)
	h.exec(`INSERT INTO it_def_alt (id) VALUES (2)`)
	h.expectRows(`SELECT column_default FROM information_schema.columns WHERE table_name = 'it_def_alt' AND column_name = 'qty'`, "40 + 2")
	h.exec(`ALTER TABLE it_def_alt ALTER qty DROP DEFAULT`)
	h.exec(`ALTER TABLE it_def_alt ALTER COLUMN qty DROP DEFAULT`)
	h.exec(`INSERT INTO it_def_alt (id) VALUES (3)`)
	h.expectRows(`SELECT id, qty FROM it_def_alt ORDER BY id`, "1|NULL", "2|42", "3|NULL")
	h.expectRows(`SELECT column_default FROM information_schema.columns WHERE table_name = 'it_def_alt' AND column_name = 'qty'`, "NULL")

	h.exec(`ALTER TABLE it_def_alt ADD COLUMN flag BOOLEAN DEFAULT TRUE`)
	h.exec(`ALTER TABLE it_def_alt ALTER COLUMN flag SET DEFAULT FALSE`)
	h.exec(`INSERT INTO it_def_alt (id) VALUES (4)`)
	h.exec(`ALTER TABLE it_def_alt ALTER COLUMN flag DROP DEFAULT`)
	h.exec(`INSERT INTO it_def_alt (id) VALUES (5)`)
	h.expectRows(`SELECT id, flag FROM it_def_alt ORDER BY id`, "1|true", "2|true", "3|true", "4|false", "5|NULL")
	h.expectRows(`SELECT id FROM it_def_alt WHERE flag = TRUE ORDER BY id`, "1", "2", "3")
	h.expectRows(`SELECT id FROM it_def_alt WHERE flag = FALSE ORDER BY id`, "4")
	h.expectRows(`SELECT id FROM it_def_alt WHERE flag IS NULL ORDER BY id`, "5")

	// Numeric and timestamp missing values; the existing rows all read the
	// ALTER statement's time.
	h.exec(`ALTER TABLE it_def_alt ADD COLUMN price NUMERIC(8,2) DEFAULT 9.99`)
	h.exec(`ALTER TABLE it_def_alt ADD COLUMN at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`)
	h.exec(`INSERT INTO it_def_alt (id, price, at) VALUES (6, 1.50, NULL)`)
	h.expectRows(`SELECT id, price FROM it_def_alt WHERE price = 9.99 ORDER BY id`, "1|9.99", "2|9.99", "3|9.99", "4|9.99", "5|9.99")
	h.expectRows(`SELECT id FROM it_def_alt WHERE price < 2 ORDER BY id`, "6")
	h.expectRows(`SELECT COUNT(*), COUNT(at), COUNT(DISTINCT at) FROM it_def_alt WHERE at <= CURRENT_TIMESTAMP`, "5|5|1")
	h.expectRows(`SELECT id FROM it_def_alt WHERE at IS NULL`, "6")

	// A table that never had rows (since RESTART IDENTITY) has none to read
	// the missing value.
	h.exec(`TRUNCATE it_def_alt RESTART IDENTITY`)
	h.exec(`ALTER TABLE it_def_alt ADD COLUMN later INTEGER DEFAULT 3`)
	if c := h.tableColumn(h.rawClient(), "it_def_alt", "later"); c.MissingThrough != 0 {
		t.Errorf("later metadata = %+v, want no missing value", c)
	}
	h.exec(`INSERT INTO it_def_alt (id) VALUES (6)`)
	h.exec(`INSERT INTO it_def_alt (id, later) VALUES (7, NULL)`)
	h.expectRows(`SELECT id, later FROM it_def_alt ORDER BY id`, "6|3", "7|NULL")
}

// A statement that read the table's metadata before ADD COLUMN … DEFAULT
// (here, rows written with the old metadata) still gives its rows the
// column's missing value: they were not inserted with an explicit NULL.
func TestSQLAddColumnDefaultConcurrentInsert(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_def_race")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_def_race (id INTEGER)`)
	h.exec(`INSERT INTO it_def_race VALUES (1)`)
	st := &store{client: raw}
	old, err := st.getTable(h.ctx, defaultSchema, "it_def_race")
	if err != nil {
		t.Fatal(err)
	}
	h.exec(`ALTER TABLE it_def_race ADD COLUMN extra INTEGER DEFAULT 11`)
	h.exec(`ALTER TABLE it_def_race ADD COLUMN plain VARCHAR`)
	id, err := Coerce(intValue(typeInt64, 2), old.Columns[0].Type)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.insertRows(h.ctx, old, [][]Value{{id}}); err != nil {
		t.Fatal(err)
	}
	h.exec(`INSERT INTO it_def_race (id, extra) VALUES (3, NULL)`)
	h.expectRows(`SELECT id, extra, plain FROM it_def_race ORDER BY id`, "1|11|NULL", "2|11|NULL", "3|NULL|NULL")
	if f := h.rowFields(raw, "public", "it_def_race"); f["extra"] != 1 || f["plain"] != 0 {
		t.Errorf("row fields = %v, want extra written for the row inserted with the old metadata only", f)
	}
}

// Temporary tables have defaults like permanent ones.
func TestSQLDefaultsTemp(t *testing.T) {
	h := newSQLHarness(t)
	h.exec(`DROP TABLE IF EXISTS pg_temp.it_def_tmp`)
	h.exec(`CREATE TEMP TABLE it_def_tmp (a INTEGER, b VARCHAR DEFAULT 'tmp')`)
	h.exec(`INSERT INTO it_def_tmp (a) VALUES (1)`)
	h.exec(`ALTER TABLE it_def_tmp ADD COLUMN c INTEGER DEFAULT 3`)
	h.exec(`INSERT INTO it_def_tmp (a, c) VALUES (2, NULL)`)
	h.expectRows(`SELECT a, b, c FROM it_def_tmp ORDER BY a`, "1|tmp|3", "2|tmp|NULL")
	h.expectRows(`SELECT a FROM it_def_tmp WHERE c = 3`, "1")
	h.expectRows(`SELECT column_name, column_default FROM information_schema.columns
		WHERE table_schema = 'pg_temp' AND table_name = 'it_def_tmp' ORDER BY ordinal_position`, "a|NULL", "b|'tmp'", "c|3")
	h.exec(`DROP TABLE it_def_tmp`)
}
