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

// Integration tests for temporary tables and views (see temp.go). Like the
// other SQL integration tests, they need REDIS_URI:
//
//	REDIS_URI=redis://localhost:6379/0 go test -run TestSQLTemp ./...

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// tempOwners returns the connection ids registered as temporary schema
// owners.
func (h *sqlHarness) tempOwners(raw goredis.UniversalClient) map[string]bool {
	h.t.Helper()
	ids, err := raw.SMembers(h.ctx, tempOwnersKey).Result()
	if err != nil {
		h.t.Fatal(err)
	}
	out := map[string]bool{}
	for _, id := range ids {
		out[id] = true
	}
	return out
}

// newTempID returns the one owner id registered since before was taken.
func (h *sqlHarness) newTempID(raw goredis.UniversalClient, before map[string]bool) string {
	h.t.Helper()
	var added []string
	for id := range h.tempOwners(raw) {
		if !before[id] {
			added = append(added, id)
		}
	}
	if len(added) != 1 {
		h.t.Fatalf("expected one new temporary schema owner, got %v", added)
	}
	return added[0]
}

// existing returns which of the keys exist (one EXISTS per key: on a
// cluster, row keys are in different slots).
func (h *sqlHarness) existing(raw goredis.UniversalClient, keys ...string) []string {
	h.t.Helper()
	var out []string
	for _, k := range keys {
		n, err := raw.Exists(h.ctx, k).Result()
		if err != nil {
			h.t.Fatal(err)
		}
		if n > 0 {
			out = append(out, k)
		}
	}
	return out
}

func (h *sqlHarness) indexExists(raw goredis.UniversalClient, index string) bool {
	h.t.Helper()
	err := (&store{client: raw}).searchDo(h.ctx, index, "FT.INFO", index).Err()
	if err != nil && !isUnknownIndex(err) {
		h.t.Fatal(err)
	}
	return err == nil
}

// objects lists GetObjects results as "catalog.schema[.table TYPE]".
func (h *sqlHarness) objects(depth adbc.ObjectDepth, schema, table *string) []string {
	h.t.Helper()
	rdr, err := h.conn.GetObjects(h.ctx, depth, nil, schema, table, nil, nil)
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
				Name    string `json:"catalog_name"`
				Schemas []struct {
					Name   string `json:"db_schema_name"`
					Tables []struct {
						Name    string `json:"table_name"`
						Type    string `json:"table_type"`
						Columns []struct {
							Name string `json:"column_name"`
						} `json:"table_columns"`
					} `json:"db_schema_tables"`
				} `json:"catalog_db_schemas"`
			}
			if err := dec.Decode(&cat); err != nil {
				h.t.Fatal(err)
			}
			for _, s := range cat.Schemas {
				if depth == adbc.ObjectDepthDBSchemas {
					out = append(out, cat.Name+"."+s.Name)
					continue
				}
				for _, t := range s.Tables {
					entry := fmt.Sprintf("%s.%s.%s %s", cat.Name, s.Name, t.Name, t.Type)
					for _, c := range t.Columns {
						entry += " " + c.Name
					}
					out = append(out, entry)
				}
			}
		}
	}
	slices.Sort(out)
	return out
}

func (h *sqlHarness) setCurrentSchema(schema string) {
	h.t.Helper()
	if err := h.conn.(interface {
		SetOption(context.Context, string, string) error
	}).SetOption(h.ctx, adbc.OptionKeyCurrentDbSchema, schema); err != nil {
		h.t.Fatal(err)
	}
}

// ingest bulk-loads ids into a table and returns the error, if any.
func (h *sqlHarness) ingest(table, mode string, temporary bool, ids ...int64) error {
	h.t.Helper()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(ids, nil)
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]arrow.Array{col}, int64(len(ids)))
	defer rec.Release()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	temp := adbc.OptionValueDisabled
	if temporary {
		temp = adbc.OptionValueEnabled
	}
	for k, v := range map[string]string{adbc.OptionKeyIngestTargetTable: table, adbc.OptionKeyIngestMode: mode,
		adbc.OptionValueIngestTemporary: temp} {
		if err := st.SetOption(h.ctx, k, v); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := st.Bind(h.ctx, rec); err != nil {
		h.t.Fatal(err)
	}
	_, err = st.ExecuteUpdate(h.ctx)
	return err
}

func TestSQLTempTables(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	h.exec("DROP TABLE IF EXISTS it_tmp_target")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_tmp_target") })
	raw := h.rawClient()
	before := h.tempOwners(raw)

	h.exec(`CREATE TEMP TABLE it_tmp (id INTEGER NOT NULL, label VARCHAR, qty INTEGER)`)
	schema := tempSchemaBase + h.newTempID(raw, before)
	if n := h.exec(`INSERT INTO it_tmp VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', NULL)`); n != 3 {
		t.Errorf("INSERT affected %d rows, want 3", n)
	}
	h.expectRows(`SELECT id, label, qty FROM it_tmp ORDER BY id`, "1|a|10", "2|b|20", "3|c|NULL")
	h.expectRows(`SELECT id FROM it_tmp WHERE qty >= 15 AND label = 'b'`, "2")
	h.exec(`UPDATE it_tmp SET qty = 30 WHERE id = 3`)
	h.exec(`DELETE FROM it_tmp WHERE id = 1`)
	h.expectRows(`SELECT id, qty FROM it_tmp ORDER BY id`, "2|20", "3|30")

	// Stored like any table, under the connection's own schema, which is not
	// registered as a schema.
	meta, err := (&store{client: raw}).getTable(h.ctx, schema, "it_tmp")
	if err != nil {
		t.Fatal(err)
	}
	if meta.prefix() != schema+":it_tmp:" || meta.index() != "idx:"+schema+":it_tmp" {
		t.Errorf("prefix %q, index %q", meta.prefix(), meta.index())
	}
	if ok, err := raw.SIsMember(h.ctx, schemasKey, schema).Result(); err != nil || ok {
		t.Errorf("%s is registered in %s (err %v)", schema, schemasKey, err)
	}

	// IF NOT EXISTS and clashes.
	h.exec(`CREATE TEMPORARY TABLE IF NOT EXISTS it_tmp (x INTEGER)`)
	h.expectRows(`SELECT COUNT(*) FROM it_tmp`, "2")
	h.expectError(`CREATE TEMP TABLE it_tmp (x INTEGER)`, `table "pg_temp"."it_tmp" already exists`)

	// CTAS, including dbt's parenthesized form.
	h.exec(`CREATE TEMP TABLE it_tmp_ctas AS SELECT customer_id, SUM(amount) AS total FROM it_orders
		WHERE status = 'shipped' GROUP BY customer_id`)
	h.expectRows(`SELECT customer_id, total FROM it_tmp_ctas ORDER BY customer_id`, "1|10.50", "2|255.00", "9|42.00")
	h.exec(`create temporary table it_tmp_dbt as (select id, name from it_customers where country = 'USA')`)
	h.expectRows(`SELECT id, name FROM it_tmp_dbt ORDER BY id`, "2|Bo", "3|Cy")
	h.exec(`CREATE TEMP TABLE IF NOT EXISTS it_tmp_dbt AS SELECT 1 AS id`)
	h.expectRows(`SELECT COUNT(*) FROM it_tmp_dbt`, "2")

	// Joins, subqueries, CTEs and set operations mix temporary and permanent
	// tables freely.
	h.expectRows(`SELECT c.name, t.total FROM it_tmp_ctas t JOIN it_customers c ON c.id = t.customer_id ORDER BY c.name`,
		"Ada|10.50", "Bo|255.00")
	h.expectRows(`SELECT d.name, COUNT(o.id) FROM it_tmp_dbt d LEFT JOIN it_orders o ON o.customer_id = d.id
		GROUP BY d.name ORDER BY d.name`, "Bo|2", "Cy|1")
	h.expectRows(`WITH big AS (SELECT customer_id FROM it_tmp_ctas WHERE total > 20)
		SELECT name FROM it_customers WHERE id IN (SELECT customer_id FROM big) ORDER BY name`, "Bo")
	h.expectRows(`SELECT name FROM it_tmp_dbt UNION SELECT name FROM it_customers WHERE id = 1 ORDER BY 1`, "Ada", "Bo", "Cy")

	// INSERT … SELECT from a temporary table into a permanent one (dbt's
	// incremental models), and CTAS of a permanent table.
	h.exec(`CREATE TABLE it_tmp_target (id INTEGER, name VARCHAR)`)
	h.exec(`INSERT INTO it_tmp_target (id, name) SELECT id, name FROM it_tmp_dbt`)
	h.expectRows(`SELECT id, name FROM it_tmp_target ORDER BY id`, "2|Bo", "3|Cy")

	// pg_temp names the connection's temporary schema; CREATE TABLE in it
	// creates a temporary table. The stored schema name is private.
	h.expectRows(`SELECT COUNT(*) FROM pg_temp.it_tmp`, "2")
	h.expectRows(`SELECT COUNT(*) FROM redis.pg_temp.it_tmp`, "2")
	h.exec(`CREATE TABLE pg_temp.it_tmp_q (a INTEGER)`)
	h.expectRows(`SELECT table_schema, table_type FROM information_schema.tables WHERE table_name = 'it_tmp_q'`,
		"pg_temp|LOCAL TEMPORARY")
	h.expectError(`SELECT * FROM public.it_tmp`, `table "public"."it_tmp" does not exist`)
	h.expectError(`SELECT * FROM `+schema+`.it_tmp`, "does not exist")
	h.expectError(`CREATE TEMP TABLE public.it_tmp_x (a INTEGER)`, "cannot create a temporary table or view in schema")
	h.expectError(`CREATE TEMP TABLE secondary.it_tmp_x AS SELECT 1 AS a`, "cannot create a temporary table or view in schema")
	h.expectError(`CREATE TABLE `+schema+`.it_tmp_x (a INTEGER)`, "reserved")
	h.expectError(`CREATE SCHEMA pg_temp`, "reserved")
	h.expectError(`CREATE SCHEMA pg_temp_99`, "reserved")
	if _, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(h.ctx, map[string]string{
		adbc.OptionKeyURI: os.Getenv("REDIS_URI"), OptionStringDefaultSchema: "pg_temp"}); err == nil ||
		!strings.Contains(err.Error(), "reserved") {
		t.Errorf("default schema pg_temp: %v", err)
	}

	// ALTER TABLE works on temporary tables.
	h.exec(`ALTER TABLE it_tmp RENAME COLUMN label TO tag`)
	h.exec(`ALTER TABLE it_tmp ADD COLUMN note VARCHAR`)
	h.exec(`ALTER TABLE it_tmp RENAME TO it_tmp2`)
	h.expectRows(`SELECT id, tag, note FROM it_tmp2 ORDER BY id`, "2|b|NULL", "3|c|NULL")
	h.expectError(`SELECT * FROM it_tmp`, `table "public"."it_tmp" does not exist`)
	h.exec(`ALTER TABLE pg_temp.it_tmp2 RENAME TO pg_temp.it_tmp`)
	h.expectError(`ALTER TABLE it_tmp RENAME TO public.it_tmp`, "another schema")
	h.expectRows(`SELECT id, tag FROM it_tmp WHERE tag = 'c'`, "3|c")

	// TRUNCATE keeps the table and its index.
	h.exec(`TRUNCATE TABLE it_tmp_ctas`)
	h.expectRows(`SELECT COUNT(*) FROM it_tmp_ctas`, "0")
	h.exec(`INSERT INTO it_tmp_ctas VALUES (5, 1.25)`)
	h.expectRows(`SELECT customer_id, total FROM it_tmp_ctas WHERE customer_id = 5`, "5|1.25")

	// DROP TABLE.
	h.exec(`DROP TABLE IF EXISTS it_tmp_q CASCADE`)
	h.exec(`DROP TABLE it_tmp`)
	h.expectError(`SELECT * FROM it_tmp`, "does not exist")
	h.exec(`DROP TABLE IF EXISTS it_tmp`)
	h.exec(`DROP TABLE IF EXISTS pg_temp.it_tmp`)
	if got := h.existing(raw, metaKey(schema, "it_tmp"), metaKey(schema, "it_tmp2"), meta.prefix()+"2"); len(got) > 0 {
		t.Errorf("left after DROP TABLE: %v", got)
	}
	if h.indexExists(raw, meta.index()) {
		t.Errorf("index %s left after DROP TABLE", meta.index())
	}
}

func TestSQLTempShadowing(t *testing.T) {
	h := newSQLHarness(t)
	other := newSQLHarness(t)
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_shadow_pv")
		h.exec("DROP TABLE IF EXISTS public.it_shadow")
	}
	drop()
	t.Cleanup(drop)
	h.exec(`CREATE TABLE it_shadow (id INTEGER, src VARCHAR)`)
	h.exec(`INSERT INTO it_shadow VALUES (1, 'perm'), (2, 'perm'), (3, 'perm')`)
	h.exec(`CREATE VIEW it_shadow_pv AS SELECT id, src FROM it_shadow`)

	// The query runs before the temporary table exists, so it reads the
	// permanent one. Afterwards the unqualified name means the temporary
	// table, for reads and writes; public.it_shadow is the permanent one.
	h.exec(`CREATE TEMP TABLE it_shadow AS SELECT id, 'temp' AS src, id * 10 AS extra FROM it_shadow WHERE id <= 2`)
	h.expectRows(`SELECT id, src, extra FROM it_shadow ORDER BY id`, "1|temp|10", "2|temp|20")
	h.expectRows(`SELECT id, src FROM public.it_shadow ORDER BY id`, "1|perm", "2|perm", "3|perm")
	h.exec(`INSERT INTO it_shadow (id, src) VALUES (9, 'temp')`)
	h.exec(`UPDATE it_shadow SET src = 'TEMP' WHERE id = 1`)
	h.exec(`DELETE FROM it_shadow WHERE id = 2`)
	h.expectRows(`SELECT id, src, extra FROM it_shadow ORDER BY id`, "1|TEMP|10", "9|temp|NULL")
	h.expectRows(`SELECT COUNT(*) FROM public.it_shadow WHERE src = 'perm'`, "3")
	h.expectRows(`SELECT t.id, t.src, p.src FROM it_shadow t JOIN public.it_shadow p ON p.id = t.id`, "1|TEMP|perm")

	// GetTableSchema without a schema describes the temporary table.
	for _, c := range []struct {
		schema *string
		want   int
	}{{nil, 3}, {new("pg_temp"), 3}, {new("public"), 2}} {
		s, err := h.conn.GetTableSchema(h.ctx, nil, c.schema, "it_shadow")
		if err != nil {
			t.Fatal(err)
		}
		if s.NumFields() != c.want {
			t.Errorf("GetTableSchema(%v) = %s, want %d columns", c.schema, s, c.want)
		}
	}

	// Names inside a permanent view are never shadowed, and a permanent view
	// can't refer to a temporary table.
	h.expectRows(`SELECT id, src FROM it_shadow_pv ORDER BY id`, "1|perm", "2|perm", "3|perm")
	h.expectError(`CREATE VIEW it_shadow_bad AS SELECT id FROM it_shadow`, "refers to a temporary table or view; use CREATE TEMP VIEW")
	h.expectError(`CREATE VIEW it_shadow_bad AS SELECT id FROM public.it_shadow WHERE id IN (SELECT id FROM pg_temp.it_shadow)`,
		"refers to a temporary table or view")
	h.exec(`CREATE TEMP VIEW it_shadow_tv AS SELECT id FROM it_shadow`)
	h.expectRows(`SELECT id FROM it_shadow_tv ORDER BY id`, "1", "9")

	// Other connections only see the permanent table.
	other.expectRows(`SELECT id, src FROM it_shadow ORDER BY id`, "1|perm", "2|perm", "3|perm")
	other.expectError(`SELECT * FROM pg_temp.it_shadow`, `table "pg_temp"."it_shadow" does not exist`)
	other.expectError(`SELECT * FROM it_shadow_tv`, `table "public"."it_shadow_tv" does not exist`)

	// CREATE TABLE without TEMP means a permanent table; DROP TABLE drops
	// the temporary one first.
	h.expectError(`CREATE TABLE it_shadow (a INTEGER)`, `table "public"."it_shadow" already exists`)
	h.exec(`DROP VIEW it_shadow_tv`)
	h.exec(`DROP TABLE it_shadow`)
	h.expectRows(`SELECT id, src FROM it_shadow ORDER BY id`, "1|perm", "2|perm", "3|perm")
}

func TestSQLTempIsolation(t *testing.T) {
	a := newSQLHarness(t)
	b := newSQLHarness(t)
	raw := a.rawClient()

	// Two connections create a temporary table with the same name.
	before := a.tempOwners(raw)
	a.exec(`CREATE TEMP TABLE it_iso (id INTEGER, who VARCHAR)`)
	schemaA := tempSchemaBase + a.newTempID(raw, before)
	before = a.tempOwners(raw)
	b.exec(`CREATE TEMP TABLE it_iso (id INTEGER, who VARCHAR, n INTEGER)`)
	schemaB := tempSchemaBase + b.newTempID(raw, before)
	a.exec(`INSERT INTO it_iso VALUES (1, 'a'), (2, 'a')`)
	b.exec(`INSERT INTO it_iso VALUES (1, 'b', 7)`)
	a.expectRows(`SELECT id, who FROM it_iso ORDER BY id`, "1|a", "2|a")
	b.expectRows(`SELECT id, who, n FROM it_iso`, "1|b|7")
	b.expectRows(`SELECT COUNT(*) FROM it_iso WHERE who = 'a'`, "0")

	// Their rows and indexes don't overlap.
	st := &store{client: raw}
	ma, err := st.getTable(a.ctx, schemaA, "it_iso")
	if err != nil {
		t.Fatal(err)
	}
	mb, err := st.getTable(a.ctx, schemaB, "it_iso")
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(ma.prefix(), mb.prefix()) || strings.HasPrefix(mb.prefix(), ma.prefix()) || ma.index() == mb.index() {
		t.Errorf("prefixes %q / %q, indexes %q / %q", ma.prefix(), mb.prefix(), ma.index(), mb.index())
	}

	// A third connection sees neither.
	c := newSQLHarness(t)
	c.expectError(`SELECT * FROM it_iso`, `table "public"."it_iso" does not exist`)

	// Closing one connection leaves the other's table alone.
	if err := a.conn.Close(a.ctx); err != nil {
		t.Fatal(err)
	}
	b.exec(`INSERT INTO it_iso VALUES (2, 'b', 8)`)
	b.expectRows(`SELECT id, who, n FROM it_iso ORDER BY id`, "1|b|7", "2|b|8")
	if got := b.existing(raw, metaKey(schemaA, "it_iso"), ma.prefix()+"1"); len(got) > 0 {
		t.Errorf("left after closing the owner: %v", got)
	}
}

func TestSQLTempViews(t *testing.T) {
	h := newSQLHarness(t)
	h.setupOrders()
	other := newSQLHarness(t)

	h.exec(`CREATE TEMP TABLE it_tv (id INTEGER, customer_id INTEGER, qty INTEGER)`)
	h.exec(`INSERT INTO it_tv VALUES (1, 1, 5), (2, 2, 1), (3, 2, 7)`)

	// Views over a temporary table (lazy and grouped) and over a permanent
	// one; views of views, and joins with permanent tables.
	h.exec(`CREATE TEMP VIEW it_tv_big AS SELECT id, customer_id FROM it_tv WHERE qty >= 5`)
	h.expectRows(`SELECT id FROM it_tv_big WHERE customer_id = 2`, "3")
	h.exec(`CREATE TEMPORARY VIEW it_tv_sum AS SELECT customer_id, SUM(qty) AS units FROM it_tv GROUP BY customer_id`)
	h.expectRows(`SELECT customer_id, units FROM it_tv_sum ORDER BY customer_id`, "1|5", "2|8")
	h.exec(`CREATE TEMP VIEW it_tv_shipped AS SELECT id, customer_id FROM it_orders WHERE status = 'shipped'`)
	h.expectRows(`SELECT COUNT(*) FROM it_tv_shipped`, "4")
	h.expectRows(`SELECT c.name, s.units FROM it_tv_sum s JOIN it_customers c ON c.id = s.customer_id ORDER BY c.name`,
		"Ada|5", "Bo|8")
	h.exec(`CREATE TEMP VIEW it_tv_names AS SELECT c.name FROM it_tv_big b JOIN it_customers c ON c.id = b.customer_id`)
	h.expectRows(`SELECT name FROM it_tv_names ORDER BY name`, "Ada", "Bo")
	h.expectRows(`SELECT name FROM it_customers WHERE id IN (SELECT customer_id FROM it_tv_big) ORDER BY name`, "Ada", "Bo")

	// OR REPLACE, IF NOT EXISTS, and clashes within the temporary schema.
	h.exec(`CREATE OR REPLACE TEMP VIEW it_tv_big AS SELECT id, customer_id FROM it_tv WHERE qty >= 7`)
	h.expectRows(`SELECT id FROM it_tv_big`, "3")
	h.exec(`CREATE TEMP VIEW IF NOT EXISTS it_tv_big AS SELECT 1 AS id`)
	h.expectRows(`SELECT id FROM it_tv_big`, "3")
	h.expectError(`CREATE TEMP VIEW it_tv_big AS SELECT 1 AS id`, `view "pg_temp"."it_tv_big" already exists`)
	h.expectError(`CREATE TEMP VIEW it_tv AS SELECT 1 AS a`, `"pg_temp"."it_tv" already exists as a table`)
	h.expectError(`CREATE TEMP TABLE it_tv_sum (a INTEGER)`, `"pg_temp"."it_tv_sum" already exists as a view`)
	h.expectError(`CREATE VIEW it_tv_perm AS SELECT * FROM it_tv_sum`, "refers to a temporary table or view")

	// Unqualified names in a temporary view resolve in the schema that was
	// current when it was created.
	h.exec("CREATE SCHEMA IF NOT EXISTS secondary")
	h.setCurrentSchema("secondary")
	h.expectRows(`SELECT COUNT(*) FROM it_tv_shipped`, "4")
	h.expectError(`SELECT COUNT(*) FROM it_orders`, `table "secondary"."it_orders" does not exist`)
	h.setCurrentSchema("public")

	// Schema reporting, other connections, DROP VIEW.
	s, err := h.conn.GetTableSchema(h.ctx, nil, nil, "it_tv_sum")
	if err != nil {
		t.Fatal(err)
	}
	if s.NumFields() != 2 || s.Field(0).Name != "customer_id" || s.Field(1).Name != "units" {
		t.Errorf("GetTableSchema(it_tv_sum) = %s", s)
	}
	other.expectError(`SELECT * FROM it_tv_sum`, `table "public"."it_tv_sum" does not exist`)
	h.exec(`ALTER VIEW it_tv_names RENAME TO it_tv_names2`)
	h.expectRows(`SELECT name FROM it_tv_names2 ORDER BY name`, "Bo") // it_tv_big was replaced above
	h.expectError(`SELECT * FROM it_tv_names`, `table "public"."it_tv_names" does not exist`)
	h.exec(`ALTER TABLE pg_temp.it_tv_names2 RENAME TO pg_temp.it_tv_names`)
	h.expectError(`ALTER VIEW it_tv_names RENAME TO it_tv`, `"pg_temp"."it_tv" already exists as a table`)
	h.expectError(`ALTER VIEW it_tv_names RENAME TO public.it_tv_names`, "another schema")
	h.exec(`DROP VIEW it_tv_names`)
	h.expectError(`SELECT * FROM it_tv_names`, "does not exist")
	h.exec(`DROP VIEW IF EXISTS it_tv_names CASCADE`)
	h.exec(`DROP VIEW pg_temp.it_tv_big`)
	h.expectError(`SELECT * FROM it_tv_big`, "does not exist")
}

// DROP SCHEMA … CASCADE drops a schema's permanent objects only; temporary
// schemas can't be dropped (or even named) that way.
func TestSQLTempDropSchema(t *testing.T) {
	a := newSQLHarness(t)
	b := newSQLHarness(t)
	raw := a.rawClient()
	a.exec("DROP SCHEMA IF EXISTS it_tmp_s CASCADE")
	t.Cleanup(func() { a.exec("DROP SCHEMA IF EXISTS it_tmp_s CASCADE") })
	a.exec("CREATE SCHEMA it_tmp_s")
	a.exec("CREATE TABLE it_tmp_s.p (id INTEGER)")
	a.exec("INSERT INTO it_tmp_s.p VALUES (1), (2)")

	// b's temporary objects: a table, and a view over it_tmp_s.p created
	// while it_tmp_s was b's current schema.
	b.setCurrentSchema("it_tmp_s")
	before := b.tempOwners(raw)
	b.exec("CREATE TEMP TABLE it_tmp_keep (id INTEGER)")
	schemaB := tempSchemaBase + b.newTempID(raw, before)
	b.exec("INSERT INTO it_tmp_keep VALUES (7)")
	b.exec("CREATE TEMP VIEW it_tmp_pv AS SELECT id FROM p")
	b.expectRows("SELECT COUNT(*) FROM it_tmp_pv", "2")
	b.setCurrentSchema("public")

	a.expectError("DROP SCHEMA pg_temp CASCADE", `schema "pg_temp" does not exist`)
	a.expectError("DROP SCHEMA "+schemaB+" CASCADE", "does not exist")
	a.exec("DROP SCHEMA it_tmp_s CASCADE")
	b.expectRows("SELECT id FROM it_tmp_keep", "7")
	b.expectError("SELECT * FROM it_tmp_pv", "no longer valid")
	if got := b.existing(raw, metaKey(schemaB, "it_tmp_keep"), viewKey(schemaB, "it_tmp_pv")); len(got) != 2 {
		t.Errorf("DROP SCHEMA CASCADE touched another connection's temporary objects: only %v exist", got)
	}
}

func TestSQLTempDropOnClose(t *testing.T) {
	h := newSQLHarness(t)
	raw := h.rawClient()
	before := h.tempOwners(raw)
	h.exec(`CREATE TEMP TABLE it_close (id INTEGER, v VARCHAR)`)
	h.exec(`INSERT INTO it_close VALUES (1, 'x'), (2, 'y')`)
	h.exec(`CREATE TEMP VIEW it_close_v AS SELECT id FROM it_close`)
	id := h.newTempID(raw, before)
	schema := tempSchemaBase + id
	meta, err := (&store{client: raw}).getTable(h.ctx, schema, "it_close")
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{metaKey(schema, "it_close"), seqKey(schema, "it_close"), tablesKey(schema),
		viewKey(schema, "it_close_v"), viewsKey(schema), tempAliveKey(id), meta.prefix() + "1", meta.prefix() + "2"}
	if got := h.existing(raw, keys...); len(got) != len(keys) {
		t.Fatalf("before close, only %v exist", got)
	}
	if ttl, err := raw.PTTL(h.ctx, tempAliveKey(id)).Result(); err != nil || ttl <= 0 || ttl > tempTTL {
		t.Errorf("alive key TTL = %v (err %v), want (0, %v]", ttl, err, tempTTL)
	}

	if err := h.conn.Close(h.ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.existing(raw, keys...); len(got) > 0 {
		t.Errorf("left after close: %v", got)
	}
	if h.indexExists(raw, meta.index()) {
		t.Errorf("index %s left after close", meta.index())
	}
	if h.tempOwners(raw)[id] {
		t.Errorf("owner %s still registered after close", id)
	}
	h2 := newSQLHarness(t)
	h2.expectError(`SELECT * FROM pg_temp.it_close`, "does not exist")
}

// A connection that goes away without closing leaves its temporary objects
// behind; once its alive key expires, the next connection to open drops them.
func TestSQLTempSweep(t *testing.T) {
	saved := tempTTL
	tempTTL = 600 * time.Millisecond
	t.Cleanup(func() { tempTTL = saved })
	h := newSQLHarness(t)
	raw := h.rawClient()

	// The connection that will die: a store and executor like a
	// connection's own, with its own client.
	client := h.rawClient()
	gone := &store{client: client}
	run := func(sql string) {
		t.Helper()
		parsed, err := ParseScript(sql)
		if err != nil {
			t.Fatal(err)
		}
		ex := &executor{store: gone, schema: defaultSchema, pushdown: "exact"}
		if _, err := ex.execute(h.ctx, parsed[0], nil, nil); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	before := h.tempOwners(raw)
	run(`CREATE TEMP TABLE it_sweep (id INTEGER, v VARCHAR)`)
	run(`INSERT INTO it_sweep VALUES (1, 'x'), (2, 'y')`)
	run(`CREATE TEMP VIEW it_sweep_v AS SELECT id FROM it_sweep`)
	id := h.newTempID(raw, before)
	schema := tempSchemaBase + id
	meta, err := gone.getTable(h.ctx, schema, "it_sweep")
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{metaKey(schema, "it_sweep"), seqKey(schema, "it_sweep"), tablesKey(schema),
		viewKey(schema, "it_sweep_v"), viewsKey(schema), meta.prefix() + "1", meta.prefix() + "2"}

	// A live connection's temporary table.
	h.exec(`CREATE TEMP TABLE it_sweep_live (id INTEGER)`)
	h.exec(`INSERT INTO it_sweep_live VALUES (7)`)

	// While its heartbeat runs, the objects outlive several TTLs, and new
	// connections leave them alone.
	time.Sleep(3 * tempTTL)
	newSQLHarness(t)
	if got := h.existing(raw, keys...); len(got) != len(keys) {
		t.Fatalf("a live connection's objects were swept: only %v exist", got)
	}

	// The process dies: the heartbeat stops and Close is never called.
	gone.stopHeartbeat()
	waitFor(t, "the alive key to expire", func() bool { return len(h.existing(raw, tempAliveKey(id))) == 0 })
	if got := h.existing(raw, keys...); len(got) != len(keys) {
		t.Fatalf("objects vanished before a sweep: only %v exist", got)
	}

	// The next connection to open drops the leftovers and the owner entry.
	newSQLHarness(t)
	if got := h.existing(raw, keys...); len(got) > 0 {
		t.Errorf("left after the sweep: %v", got)
	}
	if h.indexExists(raw, meta.index()) {
		t.Errorf("index %s left after the sweep", meta.index())
	}
	if h.tempOwners(raw)[id] {
		t.Errorf("owner %s still registered after the sweep", id)
	}
	// The live connection's table is untouched.
	h.expectRows(`SELECT id FROM it_sweep_live`, "7")
}

func TestSQLTempVisibility(t *testing.T) {
	h := newSQLHarness(t)
	other := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_vis_perm")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_vis_perm") })
	h.exec(`CREATE TABLE it_vis_perm (a INTEGER)`)
	h.expectRows(`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'pg_temp'`, "0")
	h.exec(`CREATE TEMP TABLE it_vis_t (id INTEGER NOT NULL, label VARCHAR)`)
	h.exec(`CREATE TEMP VIEW it_vis_v AS SELECT label FROM it_vis_t`)

	// The owner sees its temporary objects under pg_temp.
	h.expectRows(`SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('public', 'pg_temp')
		ORDER BY schema_name`, "pg_temp", "public")
	h.expectRows(`SELECT table_schema, table_name, table_type FROM information_schema.tables
		WHERE table_name LIKE 'it_vis%' ORDER BY table_name`,
		"public|it_vis_perm|BASE TABLE", "pg_temp|it_vis_t|LOCAL TEMPORARY", "pg_temp|it_vis_v|VIEW")
	h.expectRows(`SELECT table_schema, column_name, data_type, is_nullable FROM information_schema.columns
		WHERE table_name = 'it_vis_t' ORDER BY ordinal_position`, "pg_temp|id|INTEGER|NO", "pg_temp|label|VARCHAR|YES")
	h.expectRows(`SELECT table_schema, view_definition FROM information_schema.views WHERE table_name = 'it_vis_v'`,
		"pg_temp|SELECT label FROM it_vis_t")
	pattern, tempName := "it_vis%", "pg_temp"
	if got, want := h.objects(adbc.ObjectDepthTables, nil, &pattern), []string{
		"redis.pg_temp.it_vis_t TABLE", "redis.pg_temp.it_vis_v VIEW", "redis.public.it_vis_perm TABLE",
	}; !slices.Equal(got, want) {
		t.Errorf("owner's GetObjects = %q, want %q", got, want)
	}
	if got := h.objects(adbc.ObjectDepthDBSchemas, &tempName, nil); !slices.Equal(got, []string{"redis.pg_temp"}) {
		t.Errorf("owner's GetObjects(pg_temp) = %q", got)
	}
	tableName := "it_vis_t"
	if got := h.objects(adbc.ObjectDepthAll, &tempName, &tableName); !slices.Equal(got, []string{"redis.pg_temp.it_vis_t TABLE id label"}) {
		t.Errorf("owner's GetObjects(pg_temp.it_vis_t, columns) = %q", got)
	}

	// Other connections don't see them.
	other.expectRows(`SELECT table_schema, table_name FROM information_schema.tables WHERE table_name LIKE 'it_vis%'`,
		"public|it_vis_perm")
	other.expectRows(`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name LIKE 'pg_temp%'`, "0")
	other.expectRows(`SELECT COUNT(*) FROM information_schema.columns WHERE table_name LIKE 'it_vis_%' AND table_schema <> 'public'`, "0")
	if got := other.objects(adbc.ObjectDepthTables, nil, &pattern); !slices.Equal(got, []string{"redis.public.it_vis_perm TABLE"}) {
		t.Errorf("other connection's GetObjects = %q", got)
	}
	tempPattern := "pg_temp%"
	if got := other.objects(adbc.ObjectDepthDBSchemas, &tempPattern, nil); len(got) > 0 {
		t.Errorf("other connection's GetObjects(pg_temp%%) = %q", got)
	}

	// pg_temp is listed while the connection has temporary objects.
	h.exec(`DROP VIEW it_vis_v`)
	h.exec(`DROP TABLE it_vis_t`)
	h.expectRows(`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'pg_temp'`, "0")
	if got := h.objects(adbc.ObjectDepthDBSchemas, &tempName, nil); len(got) > 0 {
		t.Errorf("GetObjects(pg_temp) after dropping everything = %q", got)
	}
}

func TestSQLTempIngest(t *testing.T) {
	h := newSQLHarness(t)
	other := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_ting")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS public.it_ting") })

	// adbc.ingest.temporary chooses the temporary table; without it, ingest
	// always targets the permanent table, even when a temporary table of the
	// same name shadows it in SQL.
	for _, step := range []struct {
		mode      string
		temporary bool
		ids       []int64
	}{
		{adbc.OptionValueIngestModeCreate, true, []int64{1, 2}},
		{adbc.OptionValueIngestModeCreate, false, []int64{3}},
		{adbc.OptionValueIngestModeAppend, true, []int64{4}},
		{adbc.OptionValueIngestModeCreateAppend, false, []int64{5}},
	} {
		if err := h.ingest("it_ting", step.mode, step.temporary, step.ids...); err != nil {
			t.Fatalf("ingest %s temporary=%v: %v", step.mode, step.temporary, err)
		}
	}
	h.expectRows(`SELECT id FROM it_ting ORDER BY id`, "1", "2", "4")
	h.expectRows(`SELECT id FROM public.it_ting ORDER BY id`, "3", "5")
	if err := h.ingest("it_ting", adbc.OptionValueIngestModeReplace, true, 6); err != nil {
		t.Fatal(err)
	}
	h.expectRows(`SELECT id FROM pg_temp.it_ting`, "6")
	h.expectRows(`SELECT table_type FROM information_schema.tables WHERE table_schema = 'pg_temp' AND table_name = 'it_ting'`,
		"LOCAL TEMPORARY")

	other.expectRows(`SELECT id FROM it_ting ORDER BY id`, "3", "5")
	if err := other.ingest("it_ting", adbc.OptionValueIngestModeAppend, true, 7); err == nil ||
		!strings.Contains(err.Error(), `table "pg_temp"."it_ting" does not exist`) {
		t.Errorf("appending to a missing temporary table: %v", err)
	}
	if err := h.ingest("it_ting", adbc.OptionValueIngestModeCreate, true, 8); err == nil ||
		!strings.Contains(err.Error(), "already exists") {
		t.Errorf("creating an existing temporary table: %v", err)
	}
}

// MERGE, UPDATE … FROM and DELETE … USING with temporary tables as targets
// and as sources (dbt's merge strategy stages new rows in a temporary table).
func TestSQLTempDML(t *testing.T) {
	h := newSQLHarness(t)
	other := newSQLHarness(t)
	drop := func() { h.exec("DROP TABLE IF EXISTS public.it_tdml") }
	drop()
	t.Cleanup(drop)

	h.exec("CREATE TABLE it_tdml (id BIGINT, v VARCHAR)")
	h.exec("INSERT INTO it_tdml VALUES (1, 'a'), (2, 'b')")

	// dbt: stage in a temporary table, then MERGE into the permanent target.
	h.exec("CREATE TEMPORARY TABLE it_tdml__dbt_tmp AS SELECT 2 AS id, 'B' AS v UNION ALL SELECT 3, 'c'")
	if n := h.exec(`MERGE INTO it_tdml AS d USING it_tdml__dbt_tmp AS s ON s.id = d.id
		WHEN MATCHED THEN UPDATE SET v = s.v
		WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)`); n != 2 {
		t.Errorf("MERGE affected %d rows, want 2", n)
	}
	h.expectRows("SELECT id, v FROM it_tdml ORDER BY id", "1|a", "2|B", "3|c")

	// A temporary table as the target, with a permanent source.
	h.exec("CREATE TEMP TABLE it_tdml_t (id BIGINT, v VARCHAR)")
	h.exec("INSERT INTO it_tdml_t VALUES (1, 'x'), (9, 'z')")
	h.exec(`MERGE INTO it_tdml_t USING it_tdml s ON s.id = it_tdml_t.id
		WHEN MATCHED THEN UPDATE SET v = s.v
		WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)`)
	h.expectRows("SELECT id, v FROM it_tdml_t ORDER BY id", "1|a", "2|B", "3|c", "9|z")
	h.exec("UPDATE it_tdml_t SET v = s.v || '!' FROM it_tdml s WHERE s.id = it_tdml_t.id AND s.id = 3")
	h.exec("DELETE FROM it_tdml_t USING it_tdml s WHERE s.id = it_tdml_t.id AND s.id <= 2")
	h.expectRows("SELECT id, v FROM it_tdml_t ORDER BY id", "3|c!", "9|z")

	// A temporary table as the USING item of a permanent target.
	h.exec("DELETE FROM it_tdml USING it_tdml_t t WHERE t.id = it_tdml.id")
	h.expectRows("SELECT id FROM it_tdml ORDER BY id", "1", "2")

	// A temporary table that shadows the permanent one; pg_temp and the
	// schema name pick each explicitly.
	h.exec("CREATE TEMP TABLE it_tdml (id BIGINT, v VARCHAR)")
	h.exec(`MERGE INTO it_tdml USING public.it_tdml p ON p.id = it_tdml.id
		WHEN NOT MATCHED THEN INSERT (id, v) VALUES (p.id, 'copied')`)
	h.expectRows("SELECT id, v FROM pg_temp.it_tdml ORDER BY id", "1|copied", "2|copied")
	h.expectRows("SELECT id, v FROM public.it_tdml ORDER BY id", "1|a", "2|B")

	// Another connection sees only the permanent table.
	other.expectRows("SELECT id, v FROM it_tdml ORDER BY id", "1|a", "2|B")
	other.expectError("MERGE INTO it_tdml_t USING it_tdml s ON s.id = it_tdml_t.id WHEN MATCHED THEN DELETE", "does not exist")
}
