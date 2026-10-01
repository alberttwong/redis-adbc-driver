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

// Function resolution (functions.go): unknown functions and wrong argument
// counts are errors when a statement is planned or an expression is
// defined, whether or not a row is evaluated.

import (
	"encoding/json"
	"strings"
	"testing"
)

// setupFunctions creates it_fn_t (one row) and the empty it_fn_e.
func (h *sqlHarness) setupFunctions() {
	h.t.Helper()
	h.dropViews("it_fn_v")
	h.dropTables("it_fn_t", "it_fn_e", "it_fn_c", "it_fn_d")
	h.t.Cleanup(func() { h.dropViews("it_fn_v") })
	h.exec("CREATE TABLE it_fn_t (id INTEGER, s VARCHAR)")
	h.exec("INSERT INTO it_fn_t VALUES (1, 'a')")
	h.exec("CREATE TABLE it_fn_e (id INTEGER, s VARCHAR)")
}

func TestSQLFunctionResolution(t *testing.T) {
	h := newSQLHarness(t)
	h.setupFunctions()

	const unknown = "unsupported function NOSUCHFUNC"
	// The same error over a table with rows, an empty one, and no rows read.
	for _, c := range []struct{ sql, err string }{
		{"SELECT nosuchfunc(id) FROM %s", unknown},
		{"SELECT nosuchfunc(id) FROM %s WHERE false", unknown},
		{"SELECT nosuchfunc(id) FROM %s LIMIT 0", unknown},
		{dbtEmpty("SELECT nosuchfunc(id) AS x FROM %s"), unknown},
		{"SELECT id FROM %s WHERE nosuchfunc(id) > 0", unknown},
		{"SELECT id FROM %s WHERE id > 0 AND nosuchfunc(id) > 0 AND false", unknown},
		{"SELECT s, COUNT(*) FROM %s GROUP BY s HAVING nosuchfunc(COUNT(*)) > 0", unknown},
		{"SELECT s FROM %s GROUP BY nosuchfunc(s)", unknown},
		{"SELECT id FROM %s ORDER BY nosuchfunc(id)", unknown},
		{"SELECT id, ROW_NUMBER() OVER () AS rn FROM %s QUALIFY nosuchfunc(rn) = 1", unknown},
		{"SELECT a.id FROM %s a JOIN it_fn_t b ON nosuchfunc(a.id) = b.id", unknown},
		{"SELECT id FROM %s WHERE id IN (SELECT nosuchfunc(id) FROM it_fn_t) AND false", unknown},
		{"SELECT CASE WHEN id > 5 THEN nosuchfunc(id) END FROM %s", unknown},
		{"SELECT COALESCE(id, nosuchfunc(id)) FROM %s", unknown},
		{"SELECT upper(s, s, s) FROM %s", "UPPER expects 1 argument(s)"},
		{"SELECT id FROM %s WHERE upper(s, s) = 'A'", "UPPER expects 1 argument(s)"},
		{"SELECT round(id, 1, 2, 3) FROM %s", "ROUND expects 1 or 2 arguments"},
		{"SELECT date_trunc('day') FROM %s", "DATE_TRUNC expects 2 argument(s)"},
		{"SELECT age(1, 2, 3) FROM %s", "AGE expects 1 or 2 arguments"},
		{"SELECT nvl(s, s, s) FROM %s", "NVL expects 2 argument(s)"},
		{"SELECT lower(DISTINCT s) FROM %s", "LOWER does not accept * or DISTINCT"},
		{"SELECT upper(s) FILTER (WHERE id > 0) FROM %s", "FILTER specified, but UPPER is not an aggregate function"},
	} {
		for _, table := range []string{"it_fn_t", "it_fn_e"} {
			sql := strings.ReplaceAll(c.sql, "%s", table)
			h.expectError(sql, c.err)
			h.expectSchemaError(sql, c.err)
		}
	}

	// Writes that read no rows.
	for _, c := range []struct{ sql, err string }{
		{"UPDATE it_fn_e SET s = nosuchfunc(s)", unknown},
		{"UPDATE it_fn_t SET s = upper(s, s) WHERE false", "UPPER expects 1 argument(s)"},
		{"DELETE FROM it_fn_e WHERE nosuchfunc(id) = 1", unknown},
		{"DELETE FROM it_fn_t WHERE id = 1 RETURNING nosuchfunc(id)", unknown},
		{"INSERT INTO it_fn_e SELECT id, nosuchfunc(s) FROM it_fn_e", unknown},
		{"INSERT INTO it_fn_e VALUES (1, CASE WHEN false THEN nosuchfunc(1) END)", unknown},
		{"CREATE TABLE it_fn_c AS SELECT nosuchfunc(id) AS x FROM it_fn_e", unknown},
	} {
		h.expectError(c.sql, c.err)
	}
	h.expectRows("SELECT id, s FROM it_fn_t", "1|a")
	h.expectRows("SELECT COUNT(*) FROM it_fn_e", "0")

	// Definitions are checked when they are made, not when a row uses them.
	for _, c := range []struct{ sql, err string }{
		{"CREATE VIEW it_fn_v AS SELECT nosuchfunc(id) AS x FROM it_fn_t", unknown},
		{"CREATE VIEW it_fn_v AS SELECT upper(s, s) AS x FROM it_fn_e WHERE false", "UPPER expects 1 argument(s)"},
		{"CREATE TABLE it_fn_c (id INTEGER CHECK (nosuchfunc(id) > 0))", unknown},
		{"CREATE TABLE it_fn_c (id INTEGER, s VARCHAR, CHECK (id > 0 OR upper(s, s) = 'A'))", "UPPER expects 1 argument(s)"},
		{"CREATE TABLE it_fn_d (id INTEGER DEFAULT CASE WHEN false THEN nosuchfunc() END)", unknown},
		{"CREATE TABLE it_fn_d (s VARCHAR DEFAULT IIF(true, 'x', lower('a', 'b')))", "LOWER expects 1 argument(s)"},
		{"ALTER TABLE it_fn_t ADD CONSTRAINT c CHECK (nosuchfunc(id) > 0)", unknown},
		{"ALTER TABLE it_fn_e ADD COLUMN c INTEGER CHECK (nosuchfunc(c) > 0)", unknown},
		{"ALTER TABLE it_fn_e ADD COLUMN c INTEGER DEFAULT CASE WHEN false THEN nosuchfunc() END", unknown},
	} {
		h.expectError(c.sql, c.err)
	}
	h.expectRows(`SELECT table_name FROM information_schema.tables
		WHERE table_name IN ('it_fn_v', 'it_fn_c', 'it_fn_d') ORDER BY table_name`)
	h.expectRows("SELECT column_name FROM information_schema.columns WHERE table_name = 'it_fn_e' ORDER BY ordinal_position", "id", "s")
	h.exec("INSERT INTO it_fn_t VALUES (2, 'b')")

	// Errors that depend on values stay with the rows that cause them.
	h.expectRows("SELECT 1 / 0 FROM it_fn_e")
	h.expectRows("SELECT CAST(s AS INTEGER) FROM it_fn_t WHERE false")
	h.expectRows("SELECT CASE WHEN id > 5 THEN 1 / 0 END FROM it_fn_t ORDER BY id", "NULL", "NULL")
	h.expectError("SELECT CAST(s AS INTEGER) FROM it_fn_t", `invalid decimal "a"`)

	// Known functions of every family still resolve.
	h.expectRows(`SELECT upper(s), lower(s), length(s), coalesce(s, 'x'), concat(s, id), abs(-id),
		round(id * 1.5), date_part('year', DATE '2024-05-01'), now() IS NOT NULL, current_date IS NOT NULL,
		json_typeof('[1]'), s LIKE 'a%', s ~ '^a', COUNT(*) OVER (), date_trunc('month', DATE '2024-05-03')
		FROM it_fn_t WHERE id = 1`,
		"A|a|1|a|a1|1|2|2024|true|true|array|true|true|1|2024-05-01")
}

// A CHECK stored before calls were resolved (by an older driver) fails every
// write with an error that says how to drop it, and can be dropped.
func TestSQLFunctionStoredCheck(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_fn_chk")
	raw := h.rawClient()
	h.exec("CREATE TABLE it_fn_chk (a INTEGER CONSTRAINT c CHECK (a > 0))")
	key := metaKey("public", "it_fn_chk")
	js, err := raw.Get(h.ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(js), &m); err != nil {
		t.Fatal(err)
	}
	m["checks"] = []any{map[string]any{"name": "c", "expr": "nosuchfunc(a) > 0"}}
	out, _ := json.Marshal(m)
	if err := raw.Set(h.ctx, key, out, 0).Err(); err != nil {
		t.Fatal(err)
	}
	msg := `check constraint "c" of relation "it_fn_chk" can't be evaluated (unsupported function NOSUCHFUNC); drop it with ALTER TABLE … DROP CONSTRAINT`
	h.expectError("INSERT INTO it_fn_chk VALUES (1)", msg)
	h.expectError("UPDATE it_fn_chk SET a = 2 WHERE false", msg)
	h.expectRows("SELECT COUNT(*) FROM it_fn_chk", "0")
	h.exec("ALTER TABLE it_fn_chk DROP CONSTRAINT c")
	h.exec("INSERT INTO it_fn_chk VALUES (-1)")
	h.expectRows("SELECT a FROM it_fn_chk", "-1")
}
