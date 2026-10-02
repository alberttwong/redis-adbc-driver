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

import "testing"

// An index that the driver didn't create but that has the index name a new
// table would take (an application's own idx:public:<table>) isn't dropped
// with its HASHes: CREATE TABLE, TRUNCATE and a re-keying RENAME take the
// next names instead.
func TestSQLForeignIndexName(t *testing.T) {
	h := newSQLHarness(t)
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`DROP TABLE IF EXISTS it_fi_t`)
	h.exec(`DROP TABLE IF EXISTS it_fi_u`)
	// The names the tables would take next (names are never reused, so
	// earlier runs have used some).
	last := func(table string) int64 {
		n, err := st.lastNames(h.ctx, raw, "public", table)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	tn, un := last("it_fi_t"), last("it_fi_u")
	// The application's indexes: one on its own prefix, one on exactly the
	// prefix a table would take but with a document, and one with a filter.
	t2, t3, t4 := namesFor("public", "it_fi_t", tn+2), namesFor("public", "it_fi_t", tn+3), namesFor("public", "it_fi_t", tn+4)
	u1, u2 := namesFor("public", "it_fi_u", un+1), namesFor("public", "it_fi_u", un+2)
	apps := []struct{ index, prefix, key string }{
		{namesFor("public", "it_fi_t", tn+1).index, "it_fi_app:", "it_fi_app:1"},
		{t3.index, t3.prefix, t3.prefix + "1"},
		{u1.index, u1.prefix, ""},
	}
	t.Cleanup(func() {
		h.exec(`DROP TABLE IF EXISTS it_fi_t`)
		h.exec(`DROP TABLE IF EXISTS it_fi_u`)
		for _, a := range apps {
			_ = st.searchDo(h.ctx, a.index, "FT.DROPINDEX", a.index).Err()
			if a.key != "" {
				_ = raw.Del(h.ctx, a.key).Err()
			}
		}
	})
	for _, a := range apps {
		args := []any{"FT.CREATE", a.index, "ON", "HASH", "PREFIX", 1, a.prefix}
		if a.key == "" {
			args = append(args, "FILTER", "@n > 0")
		}
		args = append(args, "SCHEMA", "n", "NUMERIC")
		if err := st.searchDo(h.ctx, a.index, args...).Err(); err != nil {
			t.Fatal(err)
		}
		if a.key != "" {
			if err := raw.HSet(h.ctx, a.key, "n", 1).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Each still exists, and so does its HASH.
	expectApps := func(step string) {
		t.Helper()
		for _, a := range apps {
			if err := st.searchDo(h.ctx, a.index, "FT.INFO", a.index).Err(); err != nil {
				t.Errorf("%s: the application's index %s: %v", step, a.index, err)
			}
			if a.key == "" {
				continue
			}
			if n, err := raw.Exists(h.ctx, a.key).Result(); err != nil || n != 1 {
				t.Errorf("%s: the application's HASH %s: exists %d, %v", step, a.key, n, err)
			}
		}
	}
	expectNames := func(step, table string, want tableNames) {
		t.Helper()
		rows, _ := h.query(`SELECT key_prefix || ' ' || index_name FROM information_schema.tables WHERE table_name = '` + table + `'`)
		if w := want.prefix + " " + want.index; len(rows) != 1 || rows[0] != w {
			t.Errorf("%s took %q, want %q", step, rows, w)
		}
	}

	h.exec(`CREATE TABLE it_fi_t (a INTEGER)`)
	expectApps("CREATE TABLE")
	expectNames("CREATE TABLE", "it_fi_t", t2)
	h.exec(`INSERT INTO it_fi_t VALUES (1), (2)`)
	h.expectRows(`SELECT COUNT(*) FROM it_fi_t`, "2")

	h.exec(`TRUNCATE it_fi_t`)
	expectApps("TRUNCATE")
	expectNames("TRUNCATE", "it_fi_t", t4)
	h.exec(`INSERT INTO it_fi_t VALUES (3)`)

	if err := h.setConnOption(OptionStringRenameRekey, "true"); err != nil {
		t.Fatal(err)
	}
	h.exec(`ALTER TABLE it_fi_t RENAME TO it_fi_u`)
	expectApps("RENAME")
	expectNames("RENAME", "it_fi_u", u2)
	h.expectRows(`SELECT a FROM it_fi_u`, "3")
}

// A leftover of an interrupted CREATE TABLE (an index on the names' prefix
// with no documents) is still dropped, and its names reused.
func TestSQLLeftoverIndexReused(t *testing.T) {
	h := newSQLHarness(t)
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`DROP TABLE IF EXISTS it_fi_left`)
	t.Cleanup(func() { h.exec(`DROP TABLE IF EXISTS it_fi_left`) })
	last, err := st.lastNames(h.ctx, raw, "public", "it_fi_left")
	if err != nil {
		t.Fatal(err)
	}
	nm := namesFor("public", "it_fi_left", last+1)
	meta := &tableMeta{Schema: "public", Name: "it_fi_left", Columns: []columnMeta{{Name: "a", Type: typeInt32, Indexed: true}}}
	meta.KeyPrefix, meta.IndexName = nm.prefix, nm.index
	if err := st.searchDo(h.ctx, meta.index(), indexCreateArgs(meta)...).Err(); err != nil {
		t.Fatal(err)
	}
	h.exec(`CREATE TABLE it_fi_left (a INTEGER)`)
	h.expectRows(`SELECT index_name FROM information_schema.tables WHERE table_name = 'it_fi_left'`, nm.index)
	h.exec(`INSERT INTO it_fi_left VALUES (7)`)
	h.expectRows(`SELECT a FROM it_fi_left`, "7")
}
