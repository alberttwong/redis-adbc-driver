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

// Integration tests for NaNs and infinities in REAL and DOUBLE PRECISION
// columns, issue #110 (see nan.go). They run against the Redis server at
// REDIS_URI, standalone or a cluster, like sql_integration_test.go. The
// expected rows are Postgres 16's. Each query runs on an indexed column and
// on a NOINDEX copy, which the driver always evaluates itself, of both
// types, and in every aggregate pushdown mode.

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// nanColumns are it_nan_t's columns with the same values: DOUBLE PRECISION
// x (indexed) and n (NOINDEX), REAL r (indexed) and rn (NOINDEX).
var nanColumns = []string{"x", "n", "r", "rn"}

// nanAgree runs sql with {c} replaced by each of nanColumns, and {t} by
// it_nan_t, and checks that each gives want.
func (h *sqlHarness) nanAgree(sql string, want ...string) {
	h.t.Helper()
	sql = strings.ReplaceAll(sql, "{t}", "it_nan_t")
	for _, c := range nanColumns {
		h.expectRows(strings.ReplaceAll(sql, "{c}", c), want...)
	}
}

// setupNaNTable creates it_nan_t holding, by id, 1.5, NaN, Infinity,
// -Infinity, NULL, 5 and NaN in each of nanColumns. +Infinity is written
// first and the NaNs last, so an index sort that tied them would put
// +Infinity first.
func (h *sqlHarness) setupNaNTable() {
	h.t.Helper()
	h.dropTables("it_nan_t")
	h.exec("CREATE TABLE it_nan_t (id INTEGER, x DOUBLE PRECISION, n DOUBLE PRECISION NOINDEX, r REAL, rn REAL NOINDEX)")
	var rows []string
	for _, v := range []string{"3, 'Infinity'", "1, 1.5", "4, '-Infinity'", "6, 5", "5, NULL", "2, 'NaN'", "7, 'NaN'"} {
		id, val, _ := strings.Cut(v, ", ")
		rows = append(rows, "("+id+strings.Repeat(", "+val, 4)+")")
	}
	h.exec("INSERT INTO it_nan_t VALUES " + strings.Join(rows, ", "))
}

// storedField returns the HASH field of the row of schema.table whose id is
// id, read directly.
func (h *sqlHarness) storedField(c goredis.UniversalClient, schema, table, id, field string) string {
	h.t.Helper()
	got, _ := h.query("SELECT __rowid FROM " + schema + "." + table + " WHERE id = " + id)
	if len(got) != 1 {
		h.t.Fatalf("row %s of %s.%s: %q", id, schema, table, got)
	}
	v, err := c.HGet(h.ctx, h.tablePrefix(c, schema, table)+got[0], field).Result()
	if err != nil {
		h.t.Fatalf("row %s of %s.%s, field %s: %v", id, schema, table, field, err)
	}
	return v
}

// expectNoOrphans checks that every HASH under a table's key prefix is a
// row that COUNT(*), which reads the index, counts.
func (h *sqlHarness) expectNoOrphans(c goredis.UniversalClient, schema, table string) {
	h.t.Helper()
	got, _ := h.query("SELECT COUNT(*) FROM " + schema + "." + table)
	keys := h.prefixKeyCount(c, h.tablePrefix(c, schema, table))
	if len(got) != 1 || got[0] != strconv.Itoa(keys) {
		h.t.Errorf("%s.%s: COUNT(*) is %q, and %d HASHes under its prefix", schema, table, got, keys)
	}
}

// expectNaNs checks the NaNs flag of each indexed column of a table in the
// default schema: set on those in nans, clear on the others.
func (h *sqlHarness) expectNaNs(c goredis.UniversalClient, table string, nans ...string) {
	h.t.Helper()
	meta, err := (&store{client: c}).getTable(h.ctx, defaultSchema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	for _, col := range meta.Columns {
		want := false
		for _, n := range nans {
			want = want || strings.EqualFold(n, col.Name)
		}
		if col.NaNs != want {
			h.t.Errorf("%s.%s: NaNs %v, want %v", table, col.Name, col.NaNs, want)
		}
	}
}

// indexAggIn reports whether a SELECT's aggregates run in the index in
// aggregate pushdown mode mode (planOf does it for the exact mode).
func (h *sqlHarness) indexAggIn(mode, sql string) bool {
	h.t.Helper()
	parsed, err := ParseScript(sql)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	e := &executor{store: &store{client: h.rawClient()}, schema: defaultSchema, pushdown: mode, now: time.Now().UTC()}
	e.cache = newExecCache()
	plan, err := e.planSelect(h.ctx, parsed[0].Stmt.(*SelectStmt), nil)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	wp, err := e.planWhere(h.ctx, plan.sel.Where, plan.meta, nil)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	var aggs []*Func
	for _, it := range plan.items {
		collectAggregates(it.expr, &aggs)
	}
	_, ok, err := e.indexAggregate(h.ctx, plan, wp, aggs)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	return ok
}

// TestSQLNaNIssue110 is the issue's script: the row with a NaN was written
// but RediSearch didn't index it, so COUNT(*) was 2 and only WHERE __rowid =
// N found it. Its HASH now holds "infinity", which the index reads as +inf.
func TestSQLNaNIssue110(t *testing.T) {
	h := newSQLHarness(t)
	raw := h.rawClient()
	h.exec("CREATE SCHEMA IF NOT EXISTS it_nan_s")
	h.dropTables("it_nan_s.f")
	for _, typ := range []string{"double precision", "real", "float8 NOINDEX", "float4 NOINDEX"} {
		h.exec("drop table if exists it_nan_s.f")
		h.exec("create table it_nan_s.f (id integer, x " + typ + ")")
		h.exec("insert into it_nan_s.f values (1, 1.5), (2, cast('NaN' as double precision)), (3, 2.5)")
		h.expectRows("select count(*) from it_nan_s.f", "3")
		h.expectRows("select id, x from it_nan_s.f order by id", "1|1.5", "2|NaN", "3|2.5")
		h.expectRows("select * from it_nan_s.f where id = 2", "2|NaN")
		h.expectRows("select id from it_nan_s.f where x = 'NaN'", "2")
		h.expectRows("select id from it_nan_s.f where x > 2 order by id", "2", "3")
		h.expectNoOrphans(raw, "it_nan_s", "f")
		if got := h.storedField(raw, "it_nan_s", "f", "2", "x"); got != "infinity" {
			t.Errorf("%s: NaN stored as %q", typ, got)
		}
		prefix := h.tablePrefix(raw, "it_nan_s", "f")
		h.exec("drop table it_nan_s.f")
		if n := h.prefixKeyCount(raw, prefix); n != 0 {
			t.Errorf("%s: %d HASHes left after DROP TABLE", typ, n)
		}
	}
}

// TestSQLNaNWritePaths writes NaNs in every way a statement can and checks
// that the rows are found, that the indexed float columns that got one
// have NaNs set (and keep it), and that no HASH is left out of the index.
func TestSQLNaNWritePaths(t *testing.T) {
	h := newSQLHarness(t)
	raw := h.rawClient()
	tables := []string{"it_nan_ins", "it_nan_sel", "it_nan_upd", "it_nan_mrg", "it_nan_ctas", "it_nan_def",
		"it_nan_add", "it_nan_ing", "it_nan_ing2"}
	h.dropTables(tables...)
	create := func(table string) {
		h.exec("CREATE TABLE " + table + " (id INTEGER, x DOUBLE PRECISION, n DOUBLE PRECISION NOINDEX, r REAL, rn REAL NOINDEX)")
	}
	// check is what every table must give once rows 1 and 2 hold 1 and NaN.
	check := func(table string) {
		t.Helper()
		h.expectRows("SELECT id, x, n, r, rn FROM "+table+" ORDER BY id", "1|1|1|1|1", "2|NaN|NaN|NaN|NaN")
		for _, c := range nanColumns {
			h.expectRows("SELECT id FROM "+table+" WHERE "+c+" = 'NaN'", "2")
			h.expectRows("SELECT id FROM "+table+" WHERE "+c+" > 'Infinity'", "2")
			h.expectRows("SELECT id FROM "+table+" WHERE "+c+" < 'Infinity'", "1")
			h.expectRows("SELECT id FROM "+table+" ORDER BY "+c+" DESC LIMIT 1", "2")
			h.expectRows("SELECT COUNT(*), COUNT("+c+"), MAX("+c+"), MIN("+c+") FROM "+table, "2|2|NaN|1")
		}
		h.expectNaNs(raw, table, "x", "r")
		h.expectNoOrphans(raw, defaultSchema, table)
	}
	nan := "CAST('Infinity' AS DOUBLE PRECISION) - CAST('Infinity' AS DOUBLE PRECISION)"

	// INSERT … VALUES: no NaN, then a NaN.
	create("it_nan_ins")
	h.exec("INSERT INTO it_nan_ins VALUES (1, 1, 1, 1, 1)")
	h.expectNaNs(raw, "it_nan_ins")
	h.exec("INSERT INTO it_nan_ins VALUES (2, 'NaN', 'nan', CAST('NaN' AS REAL), 'NaN')")
	check("it_nan_ins")

	// INSERT … SELECT of a computed NaN.
	create("it_nan_sel")
	h.exec("INSERT INTO it_nan_sel VALUES (1, 1, 1, 1, 1)")
	h.exec("INSERT INTO it_nan_sel SELECT 2, v, v, v, v FROM (SELECT " + nan + " AS v) AS s")
	check("it_nan_sel")

	// UPDATE to NaN and back. NaNs stays set.
	create("it_nan_upd")
	h.exec("INSERT INTO it_nan_upd VALUES (1, 1, 1, 1, 1), (2, 2, 2, 2, 2)")
	h.expectAffected("UPDATE it_nan_upd SET x = 'NaN', n = 'NaN', r = 'NaN', rn = 'NaN' WHERE id = 2", 1)
	check("it_nan_upd")
	h.expectAffected("UPDATE it_nan_upd SET x = 3, n = 3, r = 3, rn = 3 WHERE x = 'NaN'", 1)
	h.expectRows("SELECT id, x, n, r, rn FROM it_nan_upd ORDER BY id", "1|1|1|1|1", "2|3|3|3|3")
	h.expectRows("SELECT COUNT(*) FROM it_nan_upd WHERE x = 'NaN'", "0")
	h.expectNaNs(raw, "it_nan_upd", "x", "r")
	h.expectAffected("UPDATE it_nan_upd SET x = "+nan+", r = "+nan+", n = "+nan+", rn = "+nan+" WHERE x > 2", 1)
	check("it_nan_upd")

	// MERGE: an update to NaN and an insert of one.
	create("it_nan_mrg")
	h.exec("INSERT INTO it_nan_mrg VALUES (1, 0, 0, 0, 0)")
	h.exec(`MERGE INTO it_nan_mrg t USING (SELECT 1 AS id, 1e0 AS v UNION ALL SELECT 2, ` + nan + `) AS s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET x = s.v, n = s.v, r = s.v, rn = s.v
		WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v, s.v, s.v, s.v)`)
	check("it_nan_mrg")
	h.exec(`MERGE INTO it_nan_mrg t USING (SELECT 1 AS id) AS s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET x = 'NaN', n = 'NaN', r = 'NaN', rn = 'NaN'`)
	h.expectRows("SELECT id FROM it_nan_mrg WHERE x = 'NaN' ORDER BY id", "1", "2")
	h.exec(`MERGE INTO it_nan_mrg t USING (SELECT 1 AS id) AS s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET x = 1, n = 1, r = 1, rn = 1`)

	// CREATE TABLE … AS indexes the copy, NaNs and all.
	h.exec("CREATE TABLE it_nan_ctas AS SELECT id, x, n, r, rn FROM it_nan_mrg")
	h.expectRows("SELECT column_name, is_indexed FROM information_schema.columns WHERE table_name = 'it_nan_ctas' ORDER BY ordinal_position",
		"id|YES", "x|YES", "n|YES", "r|YES", "rn|YES")
	h.expectRows("SELECT id, x, n, r, rn FROM it_nan_ctas ORDER BY id", "1|1|1|1|1", "2|NaN|NaN|NaN|NaN")
	h.expectNaNs(raw, "it_nan_ctas", "x", "n", "r", "rn")
	h.expectRows("SELECT id FROM it_nan_ctas WHERE n = 'NaN'", "2")
	h.expectNoOrphans(raw, defaultSchema, "it_nan_ctas")

	// Defaults.
	h.exec(`CREATE TABLE it_nan_def (id INTEGER, x DOUBLE PRECISION DEFAULT 'NaN', n DOUBLE PRECISION DEFAULT 'NaN' NOINDEX,
		r REAL DEFAULT 'NaN', rn REAL DEFAULT 'NaN' NOINDEX)`)
	h.exec("INSERT INTO it_nan_def VALUES (1, 1, 1, 1, 1)")
	h.expectNaNs(raw, "it_nan_def")
	h.exec("INSERT INTO it_nan_def (id) VALUES (2)")
	check("it_nan_def")

	// ADD COLUMN … DEFAULT 'NaN': the existing rows read it as their
	// missing value, and rows written later have it in the index.
	h.exec("CREATE TABLE it_nan_add (id INTEGER)")
	h.exec("INSERT INTO it_nan_add VALUES (2)")
	h.exec("ALTER TABLE it_nan_add ADD COLUMN x DOUBLE PRECISION DEFAULT 'NaN', ADD COLUMN n DOUBLE PRECISION DEFAULT 'NaN' NOINDEX, " +
		"ADD COLUMN r REAL DEFAULT 'NaN', ADD COLUMN rn REAL DEFAULT 'NaN' NOINDEX")
	h.expectNaNs(raw, "it_nan_add", "x", "r")
	h.exec("INSERT INTO it_nan_add VALUES (1, 1, 1, 1, 1), (3, 'NaN', 'NaN', 'NaN', 'NaN')")
	h.expectRows("SELECT id, x, n, r, rn FROM it_nan_add ORDER BY id", "1|1|1|1|1", "2|NaN|NaN|NaN|NaN", "3|NaN|NaN|NaN|NaN")
	for _, c := range nanColumns {
		h.expectRows("SELECT id FROM it_nan_add WHERE "+c+" = 'NaN' ORDER BY id", "2", "3")
		h.expectRows("SELECT id FROM it_nan_add WHERE "+c+" < 'NaN' ORDER BY id", "1")
		h.expectRows("SELECT "+c+", COUNT(*) FROM it_nan_add GROUP BY "+c+" ORDER BY "+c, "1|1", "NaN|2")
	}
	h.expectNoOrphans(raw, defaultSchema, "it_nan_add")

	// Bulk ingest of float64 and float32 NaNs, into a table and creating one.
	create("it_nan_ing")
	h.nanIngest("it_nan_ing", adbc.OptionValueIngestModeAppend)
	check("it_nan_ing")
	h.nanIngest("it_nan_ing2", adbc.OptionValueIngestModeCreate)
	h.expectRows("SELECT data_type FROM information_schema.columns WHERE table_name = 'it_nan_ing2' ORDER BY ordinal_position",
		"BIGINT", "DOUBLE PRECISION", "DOUBLE PRECISION", "REAL", "REAL")
	h.expectRows("SELECT id, x, n, r, rn FROM it_nan_ing2 ORDER BY id", "1|1|1|1|1", "2|NaN|NaN|NaN|NaN")
	h.expectNaNs(raw, "it_nan_ing2", "x", "n", "r", "rn")
	h.expectRows("SELECT id FROM it_nan_ing2 WHERE rn > 'Infinity'", "2")
	h.expectNoOrphans(raw, defaultSchema, "it_nan_ing2")
}

// nanIngest bulk-ingests rows 1 (1 everywhere) and 2 (NaN everywhere) as
// (id BIGINT, x, n DOUBLE, r, rn FLOAT).
func (h *sqlHarness) nanIngest(table, mode string) {
	h.t.Helper()
	mem := memory.DefaultAllocator
	ib := array.NewInt64Builder(mem)
	defer ib.Release()
	ib.AppendValues([]int64{1, 2}, nil)
	cols := []arrow.Array{ib.NewArray()}
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}
	for _, name := range []string{"x", "n"} {
		b := array.NewFloat64Builder(mem)
		b.AppendValues([]float64{1, math.NaN()}, nil)
		cols = append(cols, b.NewArray())
		b.Release()
		fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: true})
	}
	for _, name := range []string{"r", "rn"} {
		b := array.NewFloat32Builder(mem)
		b.AppendValues([]float32{1, float32(math.NaN())}, nil)
		cols = append(cols, b.NewArray())
		b.Release()
		fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float32, Nullable: true})
	}
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 2)
	defer rec.Release()
	for _, c := range cols {
		c.Release()
	}
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	for k, v := range map[string]string{adbc.OptionKeyIngestTargetTable: table, adbc.OptionKeyIngestMode: mode} {
		if err := st.SetOption(h.ctx, k, v); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := st.Bind(h.ctx, rec); err != nil {
		h.t.Fatal(err)
	}
	if n, err := st.ExecuteUpdate(h.ctx); err != nil || n != 2 {
		h.t.Fatalf("ingest into %s: n=%d err=%v", table, n, err)
	}
}

// nanFilters are WHERE clauses on it_nan_t's {c} with the ids Postgres 16
// returns, in order.
var nanFilters = []struct{ where, ids string }{
	{"{c} = 5", "6"},
	{"{c} <> 5", "1 2 3 4 7"},
	{"{c} < 5", "1 4"},
	{"{c} <= 5", "1 4 6"},
	{"{c} > 5", "2 3 7"},
	{"{c} >= 5", "2 3 6 7"},
	{"{c} = 'Infinity'", "3"},
	{"{c} <> 'Infinity'", "1 2 4 6 7"},
	{"{c} < 'Infinity'", "1 4 6"},
	{"{c} <= 'Infinity'", "1 3 4 6"},
	{"{c} > 'Infinity'", "2 7"},
	{"{c} >= 'Infinity'", "2 3 7"},
	{"{c} = 'NaN'", "2 7"},
	{"{c} <> 'NaN'", "1 3 4 6"},
	{"{c} < 'NaN'", "1 3 4 6"},
	{"{c} <= 'NaN'", "1 2 3 4 6 7"},
	{"{c} > 'NaN'", ""},
	{"{c} >= 'NaN'", "2 7"},
	{"{c} = '-Infinity'", "4"},
	{"{c} <> '-Infinity'", "1 2 3 6 7"},
	{"{c} < '-Infinity'", ""},
	{"{c} <= '-Infinity'", "4"},
	{"{c} > '-Infinity'", "1 2 3 6 7"},
	{"{c} >= '-Infinity'", "1 2 3 4 6 7"},
	{"'NaN' = {c}", "2 7"},
	{"'Infinity' > {c}", "1 4 6"},
	{"{c} = CAST('NaN' AS DOUBLE PRECISION)", "2 7"},
	{"{c} > CAST('Infinity' AS DOUBLE PRECISION)", "2 7"},
	{"{c} = CAST('inf' AS REAL)", "3"},
	{"{c} IN ('NaN', 1.5)", "1 2 7"},
	{"{c} IN ('NaN')", "2 7"},
	{"{c} IN ('Infinity', '-Infinity')", "3 4"},
	{"{c} NOT IN ('NaN', 5)", "1 3 4"},
	// 16 or more constants: a hash set of them.
	{"{c} IN ('NaN', 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)", "2 7"},
	{"{c} NOT IN ('NaN', 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)", "1 3 4 6"},
	{"{c} = 'NaN' OR {c} = 1.5", "1 2 7"},
	{"{c} BETWEEN 2 AND 'NaN'", "2 3 6 7"},
	{"{c} BETWEEN 'Infinity' AND 'NaN'", "2 3 7"},
	{"{c} NOT BETWEEN 1 AND 'Infinity'", "2 4 7"},
	{"{c} IS NULL", "5"},
	{"{c} IS NOT NULL", "1 2 3 4 6 7"},
	{"{c} IS DISTINCT FROM 'NaN'", "1 3 4 5 6"},
	{"{c} IS NOT DISTINCT FROM 'NaN'", "2 7"},
	{"{c} > 5 AND {c} < 'NaN'", "3"},
	{"{c} >= 'Infinity' AND id > 2", "3 7"},
	{"NOT ({c} < 'NaN')", "2 7"},
	{"{c} + 1 > 'Infinity'", "2 7"},
	{"{c} * 0 = 0", "1 6"},
	{"{c} - {c} <> 0", "2 3 4 7"},
}

// TestSQLNaNFilters checks comparisons with numbers, the infinities and NaN,
// IN lists, BETWEEN and IS NULL, pushed down and not, in every mode, and
// that UPDATE and DELETE find the same rows.
func TestSQLNaNFilters(t *testing.T) {
	hs := pushdownHarnesses(t)
	h := hs["default"]
	raw := h.rawClient()
	h.setupNaNTable()
	h.expectNaNs(raw, "it_nan_t", "x", "r")
	h.expectNoOrphans(raw, defaultSchema, "it_nan_t")
	forModes(t, hs, func(_ string, mh *sqlHarness) {
		for _, f := range nanFilters {
			var want []string
			if f.ids != "" {
				want = strings.Fields(f.ids)
			}
			mh.nanAgree("SELECT id FROM {t} WHERE "+f.where+" ORDER BY id", want...)
			mh.nanAgree("SELECT COUNT(*) FROM {t} WHERE "+f.where, strconv.Itoa(len(want)))
		}
	})
	// The index queries (see TestNaNPushdown): NaN and +Infinity bounds are
	// +inf, and re-checked.
	for _, c := range []struct {
		sql, query string
		residual   bool
	}{
		{"SELECT id FROM it_nan_t WHERE x > 5", "@x:[(5 +inf]", false},
		{"SELECT id FROM it_nan_t WHERE x = 'NaN'", "@x:[+inf +inf]", true},
		{"SELECT id FROM it_nan_t WHERE r >= 'Infinity'", "@r:[+inf +inf]", true},
		{"SELECT id FROM it_nan_t WHERE x > '-Infinity'", "@x:[(-inf +inf]", false},
		{"SELECT id FROM it_nan_t WHERE x IN ('NaN', 1.5)", "(@x:[+inf +inf] | @x:[1.5 1.5])", true},
	} {
		if q, r := h.pushedWhere(raw, c.sql); q != c.query || r != c.residual {
			t.Errorf("%s: query %q residual %v, want %q %v", c.sql, q, r, c.query, c.residual)
		}
	}

	// UPDATE and DELETE find their rows the same way, on a copy.
	h.dropTables("it_nan_dml")
	for _, c := range nanColumns {
		h.exec("DROP TABLE IF EXISTS it_nan_dml")
		h.exec("CREATE TABLE it_nan_dml (id INTEGER, x DOUBLE PRECISION, n DOUBLE PRECISION NOINDEX, r REAL, rn REAL NOINDEX)")
		h.exec("INSERT INTO it_nan_dml SELECT * FROM it_nan_t")
		h.expectAffected("UPDATE it_nan_dml SET id = id + 10 WHERE "+c+" = 'NaN'", 2)
		h.expectAffected("UPDATE it_nan_dml SET id = id + 20 WHERE "+c+" >= 'Infinity'", 3)
		h.expectAffected("DELETE FROM it_nan_dml WHERE "+c+" > 'Infinity'", 2)
		h.expectAffected("DELETE FROM it_nan_dml WHERE "+c+" > 5", 1)
		h.expectRows("SELECT id FROM it_nan_dml ORDER BY id", "1", "4", "5", "6")
		h.expectNoOrphans(raw, defaultSchema, "it_nan_dml")
	}
}

// TestSQLNaNOrder checks that NaN sorts after +Infinity (and NULLs last
// unless NULLS FIRST), with and without LIMIT, in window functions too.
func TestSQLNaNOrder(t *testing.T) {
	hs := pushdownHarnesses(t)
	h := hs["default"]
	h.setupNaNTable()
	forModes(t, hs, func(_ string, mh *sqlHarness) {
		mh.nanAgree("SELECT id FROM {t} ORDER BY {c}, id", "4", "1", "6", "3", "2", "7", "5")
		mh.nanAgree("SELECT id FROM {t} ORDER BY {c} DESC NULLS LAST, id", "2", "7", "3", "6", "1", "4", "5")
		mh.nanAgree("SELECT id FROM {t} ORDER BY {c} DESC NULLS FIRST, id", "5", "2", "7", "3", "6", "1", "4")
		mh.nanAgree("SELECT id FROM {t} ORDER BY {c} NULLS FIRST, id", "5", "4", "1", "6", "3", "2", "7")
		mh.nanAgree("SELECT {c} FROM {t} ORDER BY {c}", "-Inf", "1.5", "5", "+Inf", "NaN", "NaN", "NULL")
		mh.nanAgree("SELECT {c} FROM {t} ORDER BY {c} DESC LIMIT 3", "NaN", "NaN", "+Inf")
		mh.nanAgree("SELECT {c} FROM {t} ORDER BY {c} DESC LIMIT 1", "NaN")
		mh.nanAgree("SELECT {c} FROM {t} ORDER BY {c} LIMIT 2 OFFSET 3", "+Inf", "NaN")
		mh.nanAgree("SELECT {c} FROM {t} WHERE {c} >= 5 ORDER BY {c} DESC LIMIT 3", "NaN", "NaN", "+Inf")
		mh.nanAgree("SELECT {c} FROM {t} WHERE {c} >= 'Infinity' ORDER BY {c} LIMIT 1", "+Inf")
		mh.nanAgree("SELECT id, RANK() OVER (ORDER BY {c}) FROM {t} ORDER BY id",
			"1|2", "2|5", "3|4", "4|1", "5|7", "6|3", "7|5")
		mh.nanAgree("SELECT id, MAX({c}) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM {t} ORDER BY id",
			"1|1.5", "2|NaN", "3|NaN", "4|+Inf", "5|-Inf", "6|5", "7|NaN")
	})
}

// TestSQLNaNAggregates checks GROUP BY, DISTINCT and the aggregates of NaNs
// and infinities in every mode, and which run in the index: a column with
// NaNs has no MIN, MAX, SUM, AVG or GROUP BY there (aggregate_pushdown
// all), COUNT still does.
func TestSQLNaNAggregates(t *testing.T) {
	hs := pushdownHarnesses(t)
	h := hs["default"]
	h.setupNaNTable()
	forModes(t, hs, func(_ string, mh *sqlHarness) {
		mh.nanAgree("SELECT {c}, COUNT(*) FROM {t} GROUP BY {c} ORDER BY {c}",
			"-Inf|1", "1.5|1", "5|1", "+Inf|1", "NaN|2", "NULL|1")
		mh.nanAgree("SELECT {c}, COUNT(*) FROM {t} GROUP BY {c} HAVING COUNT(*) > 1", "NaN|2")
		mh.nanAgree("SELECT DISTINCT {c} FROM {t} ORDER BY {c}", "-Inf", "1.5", "5", "+Inf", "NaN", "NULL")
		mh.nanAgree("SELECT COUNT(DISTINCT {c}) FROM {t}", "5")
		mh.nanAgree("SELECT COUNT({c}), MIN({c}), MAX({c}), SUM({c}), AVG({c}) FROM {t}", "6|-Inf|NaN|NaN|NaN")
		mh.nanAgree("SELECT COUNT({c}), MIN({c}), MAX({c}), SUM({c}), AVG({c}) FROM {t} WHERE {c} < 'Infinity'", "3|-Inf|5|-Inf|-Inf")
		mh.nanAgree("SELECT COUNT({c}), MIN({c}), MAX({c}), SUM({c}), AVG({c}) FROM {t} WHERE id IN (1, 3, 6)", "3|1.5|+Inf|+Inf|+Inf")
		mh.nanAgree("SELECT COUNT({c}), MIN({c}), MAX({c}), SUM({c}), AVG({c}) FROM {t} WHERE {c} > 'Infinity'", "2|NaN|NaN|NaN|NaN")
		mh.nanAgree("SELECT MAX({c}) FROM {t} WHERE {c} < 'NaN'", "+Inf")
		mh.nanAgree("SELECT {c} > 5, COUNT(*), MAX({c}) FROM {t} GROUP BY 1 ORDER BY 1", "false|3|5", "true|3|NaN", "NULL|1|NULL")
		mh.nanAgree("SELECT id % 2, MIN({c}), MAX({c}) FROM {t} GROUP BY id % 2 ORDER BY 1", "0|-Inf|NaN", "1|1.5|NaN")
	})

	// In the index: an indexed double without NaNs groups and reduces there
	// (in the all mode), and stops once it gets one. COUNT stays.
	h.dropTables("it_nan_agg")
	h.exec("CREATE TABLE it_nan_agg (g INTEGER, x DOUBLE PRECISION)")
	h.exec("INSERT INTO it_nan_agg VALUES (1, 'Infinity'), (1, 2), (2, '-Infinity')")
	pushed := func(want bool, sqls ...string) {
		t.Helper()
		for _, sql := range sqls {
			if got := h.indexAggIn(PushdownAll, sql); got != want {
				t.Errorf("%s: in the index %v, want %v", sql, got, want)
			}
		}
	}
	all := []string{"SELECT MAX(x), MIN(x), SUM(x), AVG(x) FROM it_nan_agg", "SELECT x, COUNT(*) FROM it_nan_agg GROUP BY x",
		"SELECT g, MAX(x) FROM it_nan_agg GROUP BY g"}
	counts := []string{"SELECT COUNT(x), COUNT(*) FROM it_nan_agg", "SELECT g, COUNT(x) FROM it_nan_agg GROUP BY g"}
	pushed(true, all...)
	pushed(true, counts...)
	// +inf + -inf is NaN, which RediSearch writes -nan on amd64.
	ah := hs[PushdownAll]
	ah.expectRows("SELECT MAX(x), MIN(x), SUM(x), AVG(x) FROM it_nan_agg", "+Inf|-Inf|NaN|NaN")
	h.exec("INSERT INTO it_nan_agg VALUES (1, 'NaN')")
	pushed(false, all...)
	pushed(true, counts...)
	ah.expectRows("SELECT g, COUNT(x), MAX(x), SUM(x) FROM it_nan_agg GROUP BY g ORDER BY g", "1|3|NaN|NaN", "2|1|-Inf|-Inf")
	ah.expectRows("SELECT x, COUNT(*) FROM it_nan_agg GROUP BY x ORDER BY x", "-Inf|1", "2|1", "+Inf|1", "NaN|1")
	ah.expectRows("SELECT x FROM it_nan_agg ORDER BY x DESC LIMIT 1", "NaN")
}

// TestSQLNaNJoins checks hash joins, index lookup joins, IN subqueries,
// semi-joins and set operations on NaN, which equals NaN.
func TestSQLNaNJoins(t *testing.T) {
	h := newSQLHarness(t)
	h.setupNaNTable()
	pairs := []string{"1|1", "2|2", "2|7", "3|3", "4|4", "6|6", "7|2", "7|7"}
	h.nanAgree("SELECT a.id, b.id FROM {t} a JOIN {t} b ON a.{c} = b.{c} ORDER BY 1, 2", pairs...)
	h.nanAgree("SELECT a.id, b.id FROM {t} a JOIN {t} b ON a.{c} = b.x ORDER BY 1, 2", pairs...)
	h.nanAgree("SELECT a.id, b.id FROM {t} a JOIN {t} b ON b.r = a.{c} WHERE a.id < 3 ORDER BY 1, 2", "1|1", "2|2", "2|7")
	h.nanAgree("SELECT a.id, b.id FROM {t} a LEFT JOIN {t} b ON a.{c} = b.{c} AND a.id <> b.id ORDER BY 1, 2",
		"1|NULL", "2|7", "3|NULL", "4|NULL", "5|NULL", "6|NULL", "7|2")
	h.nanAgree("SELECT id FROM {t} WHERE {c} IN (SELECT n FROM {t} WHERE id = 7) ORDER BY id", "2", "7")
	h.nanAgree("SELECT id FROM {t} WHERE {c} NOT IN (SELECT n FROM {t} WHERE id IN (2, 6)) ORDER BY id", "1", "3", "4")
	h.nanAgree("SELECT id FROM {t} WHERE {c} = ANY (SELECT rn FROM {t} WHERE id IN (2, 3)) ORDER BY id", "2", "3", "7")
	h.nanAgree("SELECT id FROM {t} a WHERE EXISTS (SELECT 1 FROM {t} b WHERE b.{c} = a.x AND b.id <> a.id) ORDER BY id", "2", "7")
	h.nanAgree("SELECT id FROM {t} a WHERE NOT EXISTS (SELECT 1 FROM {t} b WHERE b.{c} = a.n AND b.id <> a.id) ORDER BY id",
		"1", "3", "4", "5", "6")
	h.nanAgree("SELECT id FROM {t} WHERE ({c}, 1) IN (SELECT x, 1 FROM {t} WHERE id = 2) ORDER BY id", "2", "7")
	h.nanAgree("SELECT {c} FROM {t} UNION SELECT n FROM {t} ORDER BY 1", "-Inf", "1.5", "5", "+Inf", "NaN", "NULL")
	h.nanAgree("SELECT {c} FROM {t} INTERSECT SELECT x FROM {t} WHERE id > 5 ORDER BY 1", "5", "NaN")
	h.nanAgree("SELECT {c} FROM {t} EXCEPT SELECT x FROM {t} WHERE id = 2 ORDER BY 1", "-Inf", "1.5", "5", "+Inf", "NULL")
}

// TestSQLNaNText checks NaNs and infinities as text and JSON, which Postgres
// writes NaN, Infinity and -Infinity, and as Arrow values.
func TestSQLNaNText(t *testing.T) {
	h := newSQLHarness(t)
	h.setupNaNTable()
	h.nanAgree("SELECT id, CAST({c} AS VARCHAR), {c} || '' FROM {t} ORDER BY id",
		"1|1.5|1.5", "2|NaN|NaN", "3|Infinity|Infinity", "4|-Infinity|-Infinity", "5|NULL|NULL", "6|5|5", "7|NaN|NaN")
	h.nanAgree(`SELECT TO_JSON({c}) FROM {t} WHERE id IN (2, 3, 4, 6) ORDER BY id`, `"NaN"`, `"Infinity"`, `"-Infinity"`, "5")
	h.nanAgree("SELECT id FROM {t} WHERE CAST({c} AS VARCHAR) = 'Infinity'", "3")
	h.nanAgree("SELECT id FROM {t} WHERE CAST(CAST({c} AS VARCHAR) AS DOUBLE PRECISION) = 'NaN' ORDER BY id", "2", "7")
	rows, schema := h.query("SELECT x, r FROM it_nan_t WHERE id = 2")
	if len(rows) != 1 || rows[0] != "NaN|NaN" || schema.Field(0).Type.ID() != arrow.FLOAT64 || schema.Field(1).Type.ID() != arrow.FLOAT32 {
		t.Errorf("Arrow NaNs: %q, %s", rows, schema)
	}
}

// TestSQLNaNNoOrphans checks that DELETE, TRUNCATE and DROP TABLE remove
// the HASHes of rows with NaNs (the index lists them now).
func TestSQLNaNNoOrphans(t *testing.T) {
	h := newSQLHarness(t)
	raw := h.rawClient()
	h.setupNaNTable()
	h.expectNoOrphans(raw, defaultSchema, "it_nan_t")
	prefix := h.tablePrefix(raw, defaultSchema, "it_nan_t")
	h.expectAffected("DELETE FROM it_nan_t WHERE x = 'NaN'", 2)
	if n := h.prefixKeyCount(raw, prefix); n != 5 {
		t.Errorf("%d HASHes after DELETE, want 5", n)
	}
	h.exec("INSERT INTO it_nan_t VALUES (8, 'NaN', 'NaN', 'NaN', 'NaN')")
	h.exec("TRUNCATE it_nan_t")
	if n := h.prefixKeyCount(raw, prefix); n != 0 {
		t.Errorf("%d HASHes left under the old prefix after TRUNCATE", n)
	}
	h.expectNaNs(raw, "it_nan_t", "x", "r") // never cleared
	h.exec("INSERT INTO it_nan_t VALUES (9, 'NaN', 'NaN', 'NaN', 'NaN')")
	h.expectRows("SELECT id, x FROM it_nan_t", "9|NaN")
	prefix = h.tablePrefix(raw, defaultSchema, "it_nan_t")
	h.exec("DROP TABLE it_nan_t")
	if n := h.prefixKeyCount(raw, prefix); n != 0 {
		t.Errorf("%d HASHes left after DROP TABLE", n)
	}

	// A row that an earlier version wrote with "NaN" isn't in the index,
	// but its id finds it, and writing it again indexes it (see the
	// README).
	h.setupNaNTable()
	prefix = h.tablePrefix(raw, defaultSchema, "it_nan_t")
	got, _ := h.query("SELECT __rowid FROM it_nan_t WHERE id = 2")
	if err := raw.HSet(h.ctx, prefix+got[0], "x", "NaN").Err(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the old row to leave the index", func() bool {
		rows, _ := h.query("SELECT COUNT(*) FROM it_nan_t")
		return len(rows) == 1 && rows[0] == "6"
	})
	h.expectRows("SELECT id, x FROM it_nan_t WHERE __rowid = "+got[0], "2|NaN")
	h.expectAffected("UPDATE it_nan_t SET x = 'NaN' WHERE __rowid = "+got[0], 1)
	h.expectRows("SELECT id FROM it_nan_t WHERE x = 'NaN' ORDER BY id", "2", "7")
	h.expectNoOrphans(raw, defaultSchema, "it_nan_t")
}

// TestSQLNaNNumeric checks that NUMERIC still refuses NaN and the
// infinities, from text, floats and bulk ingest, and writes nothing.
func TestSQLNaNNumeric(t *testing.T) {
	h := newSQLHarness(t)
	h.setupNaNTable()
	h.dropTables("it_nan_num")
	h.exec("CREATE TABLE it_nan_num (id INTEGER, d NUMERIC(10,2))")
	const msg = "NUMERIC values can't be NaN or infinite"
	h.expectError("INSERT INTO it_nan_num VALUES (1, 'NaN')", "cannot convert NaN to NUMERIC(10,2): "+msg)
	h.expectError("INSERT INTO it_nan_num VALUES (1, 1.5), (2, CAST('Infinity' AS DOUBLE PRECISION))", "cannot convert Infinity to NUMERIC(10,2)")
	h.expectError("INSERT INTO it_nan_num SELECT id, x FROM it_nan_t", msg)
	h.expectError("SELECT CAST(x AS NUMERIC) FROM it_nan_t", msg)
	h.expectError("SELECT CAST('-inf' AS NUMERIC)", "cannot convert -Infinity to NUMERIC(38,9): "+msg)
	h.expectRows("SELECT COUNT(*) FROM it_nan_num", "0")
	h.exec("INSERT INTO it_nan_num VALUES (1, 1.5)")
	h.expectError("UPDATE it_nan_num SET d = 'NaN'", msg)
	h.expectError("UPDATE it_nan_num SET d = (SELECT r FROM it_nan_t WHERE id = 3)", msg)
	h.expectRows("SELECT id, d FROM it_nan_num", "1|1.50")
	h.expectRows("SELECT CAST(x AS NUMERIC(10,2)) FROM it_nan_t WHERE id IN (1, 6) ORDER BY id", "1.50", "5.00")
}
