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

// Integration tests for RENAME TO with adbc.redis.rename_rekey (rekey.go).

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// newRekeyHarness is newSQLHarness with extra database options.
func newRekeyHarness(t *testing.T, opts map[string]string) *sqlHarness {
	t.Helper()
	uri := os.Getenv("REDIS_URI")
	if uri == "" {
		t.Skip("set REDIS_URI to run SQL integration tests")
	}
	ctx := context.Background()
	all := map[string]string{adbc.OptionKeyURI: uri}
	for k, v := range opts {
		all[k] = v
	}
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, all)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return &sqlHarness{t: t, ctx: ctx, conn: conn}
}

func (h *sqlHarness) setConnOption(key, value string) error {
	return h.conn.(adbc.GetSetOptionsWithContext).SetOption(h.ctx, key, value)
}

// tableNames returns a table's key prefix and index name.
func (h *sqlHarness) tableNames(raw goredis.UniversalClient, schema, table string) (string, string) {
	h.t.Helper()
	meta, err := (&store{client: raw}).getTable(h.ctx, schema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	return meta.prefix(), meta.index()
}

// expectReleased checks that a prefix and index name are free in the
// registry, and that neither keys nor an index use them.
func (h *sqlHarness) expectReleased(raw goredis.UniversalClient, prefix, index string) {
	h.t.Helper()
	if ok, err := raw.SIsMember(h.ctx, prefixesKey, prefix).Result(); err != nil || ok {
		h.t.Errorf("prefix %q still reserved (%v)", prefix, err)
	}
	if ok, err := raw.SIsMember(h.ctx, indexesKey, index).Result(); err != nil || ok {
		h.t.Errorf("index name %q still reserved (%v)", index, err)
	}
	if n := h.prefixKeyCount(raw, prefix); n != 0 {
		h.t.Errorf("%d keys left under %q", n, prefix)
	}
	if h.indexExists(raw, index) {
		h.t.Errorf("index %q still exists", index)
	}
}

// rekeyPending reports whether a re-key of the old prefix is recorded.
func (h *sqlHarness) rekeyPending(raw goredis.UniversalClient, oldPrefix string) bool {
	h.t.Helper()
	ok, err := raw.HExists(h.ctx, rekeyKey, oldPrefix).Result()
	if err != nil {
		h.t.Fatal(err)
	}
	return ok
}

// dbt's table materialization: build <name>__dbt_tmp, drop <name>, rename.
func TestSQLRekeyDbtPattern(t *testing.T) {
	h := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	h.dropTables("it_rk_model", "it_rk_model__dbt_tmp")
	h.exec("DROP VIEW IF EXISTS it_rk_view")
	t.Cleanup(func() { h.exec("DROP VIEW IF EXISTS it_rk_view") })
	raw := h.rawClient()

	h.exec(`CREATE TABLE it_rk_model (id INTEGER, label VARCHAR)`)
	h.exec(`INSERT INTO it_rk_model VALUES (100, 'old')`)
	// Key prefixes are never reused, so each run's names take a new ~N.
	seen := map[string]bool{}
	lastTmp, last := int64(0), h.namesN(raw, "public", "it_rk_model", "it_rk_model")
	for run := 1; run <= 2; run++ {
		h.exec(`CREATE TABLE it_rk_model__dbt_tmp (id INTEGER NOT NULL, label VARCHAR, amount NUMERIC(10,2), blob VARBINARY, note VARCHAR NOINDEX)`)
		h.exec(fmt.Sprintf(`INSERT INTO it_rk_model__dbt_tmp VALUES
			(1, 'a', 1.50, X'00ff', 'n1'), (2, 'b', NULL, NULL, NULL), (3, 'c', 3.25, X'01', 'n3'), (4, NULL, -4.00, NULL, 'run %d')`, run))
		tmpPrefix, tmpIndex := h.tableNames(raw, "public", "it_rk_model__dbt_tmp")
		if n := h.namesN(raw, "public", "it_rk_model__dbt_tmp", "it_rk_model__dbt_tmp"); n <= lastTmp || seen[tmpPrefix] {
			t.Fatalf("run %d: tmp table names %q, %q were used before", run, tmpPrefix, tmpIndex)
		} else {
			lastTmp, seen[tmpPrefix] = n, true
		}
		h.exec(`DROP TABLE it_rk_model`)
		h.exec(`ALTER TABLE it_rk_model__dbt_tmp RENAME TO it_rk_model`)

		// The rows and the index carry the new name, and nothing is left
		// under the old one, on every run.
		prefix, index := h.tableNames(raw, "public", "it_rk_model")
		if n := h.namesN(raw, "public", "it_rk_model", "it_rk_model"); n <= last || seen[prefix] {
			t.Fatalf("run %d: renamed table names %q, %q were used before", run, prefix, index)
		} else {
			last, seen[prefix] = n, true
		}
		if n := h.prefixKeyCount(raw, prefix); n != 4 {
			t.Errorf("run %d: %d keys under %q, want 4", run, n, prefix)
		}
		if !h.indexExists(raw, index) {
			t.Errorf("run %d: index %q missing", run, index)
		}
		h.expectReleased(raw, tmpPrefix, tmpIndex)
		if h.rekeyPending(raw, tmpPrefix) {
			t.Errorf("run %d: re-key job still recorded", run)
		}
		fields, err := raw.HGetAll(h.ctx, prefix+"1").Result()
		if err != nil {
			t.Fatal(err)
		}
		if fields["__rowid"] != "1" || fields["label"] != "a" || fields["blob"] != "\x00\xff" || fields["note"] != "n1" {
			t.Errorf("run %d: row 1 = %q", run, fields)
		}

		h.expectRows(`SELECT id, label, amount, note FROM it_rk_model ORDER BY id`,
			"1|a|1.50|n1", "2|b|NULL|NULL", "3|c|3.25|n3", fmt.Sprintf("4|NULL|-4.00|run %d", run))
		h.expectRows(`SELECT id FROM it_rk_model WHERE label = 'c'`, "3")
		h.expectRows(`SELECT id FROM it_rk_model WHERE amount > 0 ORDER BY amount DESC`, "3", "1")
		h.expectRows(`SELECT COUNT(*), COUNT(amount) FROM it_rk_model`, "4|3")
		h.expectRows(`SELECT id FROM it_rk_model WHERE __rowid = 2`, "2")
		h.expectError(`SELECT * FROM it_rk_model__dbt_tmp`, "does not exist")

		// Row ids continue, and writes go to the new keys.
		h.exec(`INSERT INTO it_rk_model (id, label) VALUES (5, 'e')`)
		h.exec(`UPDATE it_rk_model SET label = 'B' WHERE id = 2`)
		h.exec(`DELETE FROM it_rk_model WHERE id = 3`)
		h.expectRows(`SELECT __rowid, id, label FROM it_rk_model ORDER BY id`,
			"1|1|a", "2|2|B", "4|4|NULL", "5|5|e")
		if n := h.prefixKeyCount(raw, prefix); n != 4 {
			t.Errorf("run %d: %d keys under %q after writes, want 4", run, n, prefix)
		}
	}

	// Views are renamed in the metadata, as before.
	h.exec(`CREATE VIEW it_rk_view AS SELECT id FROM it_rk_model`)
	h.exec(`ALTER TABLE it_rk_view RENAME TO it_rk_view2`)
	h.exec(`ALTER VIEW it_rk_view2 RENAME TO it_rk_view`)
	h.expectRows(`SELECT COUNT(*) FROM it_rk_view`, "4")

	// The usual checks still apply.
	h.exec(`CREATE TABLE it_rk_model__dbt_tmp (x INTEGER)`)
	h.expectError(`ALTER TABLE it_rk_model RENAME TO it_rk_model__dbt_tmp`, "already exists")
	h.expectError(`ALTER TABLE it_rk_model RENAME TO it_rk_view`, "already exists as a view")
	h.expectError(`ALTER TABLE it_rk_model RENAME TO secondary.x`, "another schema")
	before, _ := h.tableNames(raw, "public", "it_rk_model")
	h.exec(`ALTER TABLE it_rk_model RENAME TO it_rk_model`)
	if prefix, _ := h.tableNames(raw, "public", "it_rk_model"); prefix != before {
		t.Errorf("renaming to the same name moved the rows from %q to %q", before, prefix)
	}
}

// A table larger than a cursor page is copied page by page.
func TestSQLRekeyManyPages(t *testing.T) {
	h := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	const n = 2*cursorCount + 500
	h.dropTables("it_rk_pages", "it_rk_pages2")
	raw := h.rawClient()
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	if err := h.ingest("it_rk_pages", adbc.OptionValueIngestModeCreate, false, ids...); err != nil {
		t.Fatal(err)
	}
	oldPrefix, oldIndex := h.tableNames(raw, "public", "it_rk_pages")
	h.exec(`ALTER TABLE it_rk_pages RENAME TO it_rk_pages2`)
	h.namesN(raw, "public", "it_rk_pages2", "it_rk_pages2")
	newPrefix, _ := h.tableNames(raw, "public", "it_rk_pages2")
	if got := h.prefixKeyCount(raw, newPrefix); got != n {
		t.Errorf("%d keys under the new prefix, want %d", got, n)
	}
	h.expectReleased(raw, oldPrefix, oldIndex)
	h.expectRows(`SELECT COUNT(*), SUM(id), MIN(id), MAX(id) FROM it_rk_pages2`,
		fmt.Sprintf("%d|%d|1|%d", n, n*(n+1)/2, n))
	h.expectRows(fmt.Sprintf(`SELECT id FROM it_rk_pages2 ORDER BY id LIMIT 2 OFFSET %d`, cursorCount-1),
		fmt.Sprint(cursorCount), fmt.Sprint(cursorCount+1))
	rows, _ := h.query(`SELECT __rowid, id FROM it_rk_pages2`)
	if len(rows) != n {
		t.Fatalf("full scan returned %d rows, want %d", len(rows), n)
	}
	for _, r := range rows {
		rowid, id, _ := strings.Cut(r, "|")
		if rowid != id {
			t.Fatalf("row %q: __rowid and id differ", r)
		}
	}
}

// Without the option, RENAME TO keeps the keys, as before.
func TestSQLRekeyDefaultOff(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_off", "it_rk_off2")
	raw := h.rawClient()
	if v, err := h.conn.(adbc.GetSetOptionsWithContext).GetOption(h.ctx, OptionStringRenameRekey); err != nil || v != "false" {
		t.Errorf("default %s = %q, %v", OptionStringRenameRekey, v, err)
	}
	h.exec(`CREATE TABLE it_rk_off (id INTEGER)`)
	h.exec(`INSERT INTO it_rk_off VALUES (1), (2)`)
	oldPrefix, oldIndex := h.tableNames(raw, "public", "it_rk_off")
	h.exec(`ALTER TABLE it_rk_off RENAME TO it_rk_off2`)
	prefix, index := h.tableNames(raw, "public", "it_rk_off2")
	if prefix != oldPrefix || index != oldIndex {
		t.Errorf("names after a plain rename: %q, %q, want %q, %q", prefix, index, oldPrefix, oldIndex)
	}
	if got := h.prefixKeyCount(raw, prefix); got != 2 {
		t.Errorf("%d keys under %q, want 2", got, prefix)
	}
	h.expectRows(`SELECT id FROM it_rk_off2 ORDER BY id`, "1", "2")
}

// The connection option overrides the database's.
func TestSQLRekeyConnectionOption(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_conn", "it_rk_conn2", "it_rk_conn3")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_rk_conn (id INTEGER)`)
	h.exec(`INSERT INTO it_rk_conn VALUES (1), (2), (3)`)

	if err := h.setConnOption(OptionStringRenameRekey, "yes"); err == nil || !strings.Contains(err.Error(), "want true or false") {
		t.Errorf("invalid value: %v", err)
	}
	if err := h.setConnOption(OptionStringRenameRekey, "true"); err != nil {
		t.Fatal(err)
	}
	h.exec(`ALTER TABLE it_rk_conn RENAME TO it_rk_conn2`)
	h.namesN(raw, "public", "it_rk_conn2", "it_rk_conn2")
	movedPrefix, _ := h.tableNames(raw, "public", "it_rk_conn2")

	// Another connection of a database with the option on, turned off.
	other := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	if err := other.setConnOption(OptionStringRenameRekey, "false"); err != nil {
		t.Fatal(err)
	}
	other.exec(`ALTER TABLE it_rk_conn2 RENAME TO it_rk_conn3`)
	if prefix, _ := h.tableNames(raw, "public", "it_rk_conn3"); prefix != movedPrefix {
		t.Errorf("connection option off: prefix %q, want %q", prefix, movedPrefix)
	}
	other.expectRows(`SELECT COUNT(*) FROM it_rk_conn3`, "3")

	if _, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(h.ctx,
		map[string]string{OptionStringRenameRekey: "1"}); err == nil {
		t.Error("invalid database option value accepted")
	}
}

// Temporary tables are re-keyed within the connection's temporary schema.
func TestSQLRekeyTempTable(t *testing.T) {
	h := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	raw := h.rawClient()
	before := h.tempOwners(raw)
	h.exec(`CREATE TEMP TABLE it_rk_tmp (id INTEGER, label VARCHAR)`)
	h.exec(`INSERT INTO it_rk_tmp VALUES (1, 'a'), (2, 'b')`)
	schema := tempSchemaBase + h.newTempID(raw, before)
	oldPrefix, oldIndex := h.tableNames(raw, schema, "it_rk_tmp")
	h.exec(`ALTER TABLE it_rk_tmp RENAME TO it_rk_tmp2`)
	prefix, index := h.tableNames(raw, schema, "it_rk_tmp2")
	if prefix != schema+":it_rk_tmp2:" || index != "idx:"+schema+":it_rk_tmp2" {
		t.Errorf("temporary table names %q, %q", prefix, index)
	}
	h.expectReleased(raw, oldPrefix, oldIndex)
	h.expectRows(`SELECT id, label FROM it_rk_tmp2 WHERE label = 'b'`, "2|b")
	h.expectRows(`SELECT table_schema, table_type, key_prefix FROM information_schema.tables WHERE table_name = 'it_rk_tmp2'`,
		"pg_temp|LOCAL TEMPORARY|"+prefix)
	// Moving it back takes new names: the old ones are never reused.
	h.exec(`ALTER TABLE it_rk_tmp2 RENAME TO it_rk_tmp`)
	if n := h.namesN(raw, schema, "it_rk_tmp", "it_rk_tmp"); n != 2 {
		t.Errorf("temporary table moved back to the names of N = %d, want 2", n)
	}
	h.expectRows(`SELECT id, label FROM it_rk_tmp ORDER BY id`, "1|a", "2|b")
}

// Every field is copied, including __rowid and the fields of dropped
// columns; a pending DROP COLUMN cleanup carries over to the new keys.
func TestSQLRekeyFieldsAndCleanup(t *testing.T) {
	h := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	h.dropTables("it_rk_fields", "it_rk_fields2", "it_rk_fields3")
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`CREATE TABLE it_rk_fields (id INTEGER, v INTEGER)`)
	h.exec(`INSERT INTO it_rk_fields VALUES (1, 10), (2, 20), (3, 30)`)

	// A retired field that is no longer being cleaned up stays in the rows.
	prefix, _ := h.tableNames(raw, "public", "it_rk_fields")
	pipe := raw.Pipeline()
	for id := 1; id <= 3; id++ {
		pipe.HSet(h.ctx, fmt.Sprintf("%s%d", prefix, id), "kept", "k", "ghost", "boo")
	}
	if _, err := pipe.Exec(h.ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.updateTable(h.ctx, "public", "it_rk_fields", func(m *tableMeta) error {
		m.RetiredFields = append(m.RetiredFields, "kept")
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	h.exec(`ALTER TABLE it_rk_fields RENAME TO it_rk_fields2`)
	if f := h.rowFields(raw, "public", "it_rk_fields2"); f["__rowid"] != 3 || f["id"] != 3 || f["v"] != 3 || f["kept"] != 3 || f["ghost"] != 3 {
		t.Errorf("fields after the move: %v", f)
	}

	// A dropped column's pending cleanup (left over, as in
	// TestSQLAlterCleanupResumes) continues on the new keys.
	if err := st.updateTable(h.ctx, "public", "it_rk_fields2", func(m *tableMeta) error {
		m.RetiredFields = append(m.RetiredFields, "ghost")
		m.PendingCleanup = append(m.PendingCleanup, "ghost")
		return nil
	}, func(p goredis.Pipeliner) { p.SAdd(h.ctx, cleanupKey, cleanupMember("public", "it_rk_fields2")) }); err != nil {
		t.Fatal(err)
	}
	h.exec(`ALTER TABLE it_rk_fields2 RENAME TO it_rk_fields3`)
	waitFor(t, "cleanup on the new keys", func() bool { return h.rowFields(raw, "public", "it_rk_fields3")["ghost"] == 0 })
	waitFor(t, "cleanup marker removal", func() bool {
		m, _ := raw.SIsMember(h.ctx, cleanupKey, cleanupMember("public", "it_rk_fields3")).Result()
		return !m
	})
	if f := h.rowFields(raw, "public", "it_rk_fields3"); f["kept"] != 3 || f["v"] != 3 {
		t.Errorf("fields after the cleanup: %v", f)
	}
	h.expectRows(`SELECT id, v FROM it_rk_fields3 ORDER BY id`, "1|10", "2|20", "3|30")
}

// A move interrupted before the switch is rolled back by a later
// connection, or by a statement that the move refuses; one interrupted after
// the switch is finished.
func TestSQLRekeyResumes(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_res", "it_rk_res2")
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`CREATE TABLE it_rk_res (id INTEGER, label VARCHAR)`)
	h.exec(`INSERT INTO it_rk_res VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	oldPrefix, oldIndex := h.tableNames(raw, "public", "it_rk_res")

	// The names that a move took are released when it is rolled back, but
	// never handed out again.
	used := map[string]bool{}
	newNames := func(job *rekeyJob) {
		t.Helper()
		if used[job.KeyPrefix] || used[job.IndexName] {
			t.Errorf("new names %q, %q were used by an earlier move", job.KeyPrefix, job.IndexName)
		}
		used[job.KeyPrefix], used[job.IndexName] = true, true
		if !strings.HasPrefix(job.KeyPrefix, "public:it_rk_res2") || !strings.HasPrefix(job.IndexName, "idx:public:it_rk_res2") {
			t.Errorf("new names %q, %q", job.KeyPrefix, job.IndexName)
		}
	}

	// stop copies the rows and switches if asked, then stops as if the
	// process had exited: the lease goes, the rest is left over.
	stop := func(switched bool) *rekeyJob {
		t.Helper()
		job, start, err := st.beginRekey(h.ctx, "public", "it_rk_res", "it_rk_res2")
		if err != nil {
			t.Fatal(err)
		}
		newNames(job)
		if err := st.moveRows(h.ctx, job, start, time.Now()); err != nil {
			t.Fatal(err)
		}
		if switched {
			if _, err := st.switchRekey(h.ctx, job, start); err != nil {
				t.Fatal(err)
			}
		}
		job.lease.release()
		if err := raw.Del(h.ctx, rekeyAliveKey(job.Owner)).Err(); err != nil {
			t.Fatal(err)
		}
		return job
	}
	expectRolledBack := func(h *sqlHarness, job *rekeyJob, rows ...string) {
		t.Helper()
		h.expectReleased(raw, job.KeyPrefix, job.IndexName)
		if prefix, index := h.tableNames(raw, "public", "it_rk_res"); prefix != oldPrefix || index != oldIndex {
			t.Errorf("names after the rollback: %q, %q", prefix, index)
		}
		h.expectRows(`SELECT id, label FROM it_rk_res ORDER BY id`, rows...)
	}

	// While the move runs (its lease is held), the table is intact under its
	// old name and refuses changes.
	job, start, err := st.beginRekey(h.ctx, "public", "it_rk_res", "it_rk_res2")
	if err != nil {
		t.Fatal(err)
	}
	newNames(job)
	if err := st.moveRows(h.ctx, job, start, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := h.prefixKeyCount(raw, job.KeyPrefix); got != 3 {
		t.Fatalf("setup: %d copies, want 3", got)
	}
	h.expectRows(`SELECT id, label FROM it_rk_res ORDER BY id`, "1|a", "2|b", "3|c")
	for _, sql := range []string{
		`INSERT INTO it_rk_res VALUES (4, 'd')`,
		`UPDATE it_rk_res SET label = 'x'`,
		`DELETE FROM it_rk_res`,
		`TRUNCATE it_rk_res`,
		`ALTER TABLE it_rk_res ADD COLUMN z INTEGER`,
		`ALTER TABLE it_rk_res RENAME TO it_rk_res3`,
	} {
		h.expectError(sql, "being renamed")
	}
	if err := st.rollbackRekey(h.ctx, job); err != nil {
		t.Fatal(err)
	}
	job.lease.release()
	expectRolledBack(h, job, "1|a", "2|b", "3|c")

	// Interrupted before the switch: a new connection rolls it back before
	// it is open.
	job = stop(false)
	h2 := newSQLHarness(t)
	if h2.rekeyPending(raw, oldPrefix) {
		t.Error("a new connection did not roll back the abandoned move")
	}
	expectRolledBack(h2, job, "1|a", "2|b", "3|c")

	// Or, on a connection that was already open, the first statement that
	// it would refuse, which then goes on.
	job = stop(false)
	h2.exec(`INSERT INTO it_rk_res VALUES (4, 'd')`)
	if h2.rekeyPending(raw, oldPrefix) {
		t.Error("a statement did not roll back the abandoned move")
	}
	expectRolledBack(h2, job, "1|a", "2|b", "3|c", "4|d")

	// Interrupted after the switch: the table already reads the new keys,
	// the old ones are left over, and a new connection removes them.
	stop(true)
	h2.expectRows(`SELECT id, label FROM it_rk_res2 ORDER BY id`, "1|a", "2|b", "3|c", "4|d")
	h2.expectError(`SELECT * FROM it_rk_res`, "does not exist")
	if got := h.prefixKeyCount(raw, oldPrefix); got != 4 {
		t.Fatalf("setup: %d old keys, want 4", got)
	}
	h3 := newSQLHarness(t)
	if h3.rekeyPending(raw, oldPrefix) {
		t.Error("a new connection did not finish the abandoned move")
	}
	h3.expectReleased(raw, oldPrefix, oldIndex)
	h3.expectRows(`SELECT id, label FROM it_rk_res2 WHERE label = 'd'`, "4|d")
	h3.exec(`INSERT INTO it_rk_res2 VALUES (5, 'e')`)
	h3.expectRows(`SELECT COUNT(*) FROM it_rk_res2`, "5")
}

// Statements that read the metadata before a move started find out.
func TestSQLRekeyConcurrentStatements(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_conc", "it_rk_conc2", "it_rk_conc3")
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`CREATE TABLE it_rk_conc (id INTEGER)`)
	h.exec(`INSERT INTO it_rk_conc VALUES (1), (2)`)
	load := func(table string) *tableMeta {
		t.Helper()
		meta, err := st.getTable(h.ctx, "public", table)
		if err != nil {
			t.Fatal(err)
		}
		return meta
	}
	// slow makes metadata look read a while ago.
	slow := func(m *tableMeta) *tableMeta {
		m.readAt = time.Now().Add(-time.Second)
		return m
	}
	expectErr := func(err error, want, what string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want an error containing %q", what, err, want)
		}
	}
	stale := load("it_rk_conc")
	row := [][]Value{{intValue(typeInt32, 3)}}

	// A write that read the metadata recently needs no check; one that read
	// it longer ago checks, and passes while nothing moves the table.
	fresh := *stale
	if _, err := st.insertRows(h.ctx, &fresh, row); err != nil {
		t.Fatal(err)
	}
	if _, err := st.insertRows(h.ctx, slow(stale), row); err != nil {
		t.Fatal(err)
	}
	if elapsed(stale.readAt) > time.Second/2 {
		t.Error("a passed check did not refresh readAt")
	}

	// Once a move has begun, a slow write fails, but reads pass until the
	// switch, even through metadata read during the move.
	job, start, err := st.beginRekey(h.ctx, "public", "it_rk_conc", "it_rk_conc2")
	if err != nil {
		t.Fatal(err)
	}
	defer job.lease.release()
	during := load("it_rk_conc")
	_, err = st.insertRows(h.ctx, slow(stale), row)
	expectErr(err, "may be lost", "slow write during the move")
	if err := st.checkReads(h.ctx, []*tableMeta{slow(stale), during}); err != nil {
		t.Errorf("reads before the switch: %v", err)
	}
	if err := st.moveRows(h.ctx, job, start, time.Now()); err != nil {
		t.Fatal(err)
	}
	// A view that takes the new name meanwhile makes the switch fail.
	h.exec(`CREATE VIEW it_rk_conc2 AS SELECT 1 AS x`)
	_, err = st.switchRekey(h.ctx, job, start)
	expectErr(err, "already exists as a view", "switch onto a view")
	h.exec(`DROP VIEW it_rk_conc2`)
	if _, err := st.switchRekey(h.ctx, job, start); err != nil {
		t.Fatal(err)
	}
	expectErr(st.checkReads(h.ctx, []*tableMeta{during}), "while this statement was reading", "read across the switch")
	if err := st.finishRekey(h.ctx, job); err != nil {
		t.Fatal(err)
	}
	err = (&executor{store: st}).deleteKeys(h.ctx, slow(stale), []string{stale.prefix() + "1"})
	expectErr(err, "may be lost", "slow write after the move")
	expectErr(st.checkReads(h.ctx, []*tableMeta{slow(stale)}), "while this statement was reading", "slow read after the move")
	// The failed insert landed before the copy here, so it was moved too; the
	// failed delete only removed an old key.
	h.expectRows(`SELECT id FROM it_rk_conc2 ORDER BY id`, "1", "2", "3", "3", "3")

	// A new table of the old name gets new keys (prefixes are never reused).
	// Statements with the old metadata fail, also fast ones, which find a
	// different table when they allocate row ids; its own statements pass.
	h.exec(`CREATE TABLE it_rk_conc (id INTEGER)`)
	reused := load("it_rk_conc")
	if reused.prefix() == stale.prefix() || reused.PrefixGen != 0 {
		t.Fatalf("new table: prefix %q gen %d; old: %q gen %d", reused.prefix(), reused.PrefixGen, stale.prefix(), stale.PrefixGen)
	}
	_, err = st.insertRows(h.ctx, slow(stale), row)
	expectErr(err, "may be lost", "slow write through the old metadata")
	freshStale := *stale
	freshStale.readAt = time.Now()
	_, err = st.insertRows(h.ctx, &freshStale, row)
	expectErr(err, "may be lost", "fast write through the old metadata")
	expectErr(st.checkReads(h.ctx, []*tableMeta{slow(stale)}), "while this statement was reading", "slow read through the old metadata")
	if _, err := st.insertRows(h.ctx, slow(reused), row); err != nil {
		t.Errorf("slow write to the new table: %v", err)
	}
	if err := st.checkReads(h.ctx, []*tableMeta{slow(reused)}); err != nil {
		t.Errorf("slow read of the new table: %v", err)
	}
	h.expectRows(`SELECT COUNT(*) FROM it_rk_conc`, "1")
	// A re-keying rename into a dropped table's name (dbt's swap: drop, then
	// rename into place) gets new keys too, and is caught the same way.
	h.exec(`DROP TABLE it_rk_conc`)
	other := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	other.exec(`ALTER TABLE it_rk_conc2 RENAME TO it_rk_conc`)
	if prefix, _ := h.tableNames(raw, "public", "it_rk_conc"); prefix == reused.prefix() || prefix == stale.prefix() {
		t.Fatalf("renamed table prefix %q was used before", prefix)
	}
	_, err = st.insertRows(h.ctx, slow(reused), row)
	expectErr(err, "may be lost", "slow write to a table dropped and replaced by a rename")
	// None of its rows reached the table that has the name now.
	h.expectRows(`SELECT COUNT(*) FROM it_rk_conc`, "5")
}

// A move stops once its lease can't be renewed.
func TestSQLRekeyLease(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_lease", "it_rk_lease2")
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`CREATE TABLE it_rk_lease (id INTEGER)`)
	h.exec(`INSERT INTO it_rk_lease VALUES (1)`)
	job, start, err := st.beginRekey(h.ctx, "public", "it_rk_lease", "it_rk_lease2")
	if err != nil {
		t.Fatal(err)
	}
	defer job.lease.release()
	if !job.lease.renew(st, job.Owner) || job.mayAct() != nil {
		t.Fatal("a renewed lease is not valid")
	}
	// A lease whose renewals have failed for rekeyTTL/2 is no longer valid.
	stuck := &lease{ctx: h.ctx, renewed: time.Now().Add(-rekeyTTL / 2)}
	if stuck.valid() {
		t.Error("a lease last renewed rekeyTTL/2 ago is valid")
	}
	// Once the lease key has expired (here: deleted), it can't be renewed,
	// and no destructive step runs.
	if err := raw.Del(h.ctx, rekeyAliveKey(job.Owner)).Err(); err != nil {
		t.Fatal(err)
	}
	if job.lease.renew(st, job.Owner) {
		t.Error("an expired lease was renewed")
	}
	job.lease.cancel()
	lost := func(err error) bool { return err != nil && strings.Contains(err.Error(), "lease could not be renewed") }
	if err := st.moveRows(h.ctx, job, start, time.Now()); !lost(err) {
		t.Errorf("move without the lease: %v", err)
	}
	if err := st.rollbackRekey(h.ctx, job); !lost(err) {
		t.Errorf("rollback without the lease: %v", err)
	}
	if h.indexExists(raw, job.IndexName) {
		t.Error("the new index was created without the lease")
	}
	// A new connection rolls the move back.
	h2 := newSQLHarness(t)
	waitFor(t, "rollback", func() bool { return !h2.rekeyPending(raw, job.OldPrefix) })
	h2.expectReleased(raw, job.KeyPrefix, job.IndexName)
	h2.exec(`INSERT INTO it_rk_lease VALUES (2)`)
	h2.expectRows(`SELECT id FROM it_rk_lease ORDER BY id`, "1", "2")
}

func TestSQLRekeyInformationSchema(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_is", "it_rk_is2", "it_rk_is3")
	h.exec("DROP VIEW IF EXISTS it_rk_isv")
	t.Cleanup(func() { h.exec("DROP VIEW IF EXISTS it_rk_isv") })
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_rk_is (id INTEGER)`)
	h.exec(`CREATE VIEW it_rk_isv AS SELECT id FROM it_rk_is`)
	q := `SELECT table_name, table_type, key_prefix, index_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name LIKE 'it_rk_is%' ORDER BY table_name`
	names := func(table, named string) string {
		nm := namesFor("public", named, h.namesN(raw, "public", table, named))
		return nm.prefix + "|" + nm.index
	}
	h.expectRows(q, "it_rk_is|BASE TABLE|"+names("it_rk_is", "it_rk_is"), "it_rk_isv|VIEW|NULL|NULL")

	// A plain rename keeps the names; a re-keying one changes them.
	h.exec(`ALTER TABLE it_rk_is RENAME TO it_rk_is2`)
	h.expectRows(q, "it_rk_is2|BASE TABLE|"+names("it_rk_is2", "it_rk_is"), "it_rk_isv|VIEW|NULL|NULL")
	if err := h.setConnOption(OptionStringRenameRekey, "true"); err != nil {
		t.Fatal(err)
	}
	h.exec(`ALTER TABLE it_rk_is2 RENAME TO it_rk_is3`)
	h.expectRows(q, "it_rk_is3|BASE TABLE|"+names("it_rk_is3", "it_rk_is3"), "it_rk_isv|VIEW|NULL|NULL")
}

// tryExec runs a statement and returns the rows affected and its error.
func tryExec(ctx context.Context, conn adbc.ConnectionWithContext, sql string) (int64, error) {
	st, err := conn.NewStatement(ctx)
	if err != nil {
		return 0, err
	}
	defer st.Close(ctx)
	if err := st.SetSqlQuery(ctx, sql); err != nil {
		return 0, err
	}
	return st.ExecuteUpdate(ctx)
}

// Another connection keeps writing while the table is moved: every write
// that succeeds is in the renamed table.
func TestSQLRekeyConcurrentWriters(t *testing.T) {
	h := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	h.dropTables("it_rk_cw", "it_rk_cw2")
	raw := h.rawClient()
	ids := make([]int64, 3000)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	if err := h.ingest("it_rk_cw", adbc.OptionValueIngestModeCreate, false, ids...); err != nil {
		t.Fatal(err)
	}
	oldPrefix, oldIndex := h.tableNames(raw, "public", "it_rk_cw")

	// The writer inserts rows and moves one row's id along -1, -2, ….
	w := newSQLHarness(t)
	stop, stopped := make(chan struct{}), make(chan struct{})
	var progress atomic.Int64
	var inserted, unsure []int64
	moved, movedUnsure := int64(1), false
	go func() {
		defer close(stopped)
		for i := int64(1); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, err := tryExec(h.ctx, w.conn, fmt.Sprintf(`INSERT INTO it_rk_cw VALUES (%d)`, 100000+i))
			switch {
			case err == nil:
				inserted = append(inserted, 100000+i)
			case strings.Contains(err.Error(), "may be lost"):
				unsure = append(unsure, 100000+i)
			}
			n, err := tryExec(h.ctx, w.conn, fmt.Sprintf(`UPDATE it_rk_cw SET id = %d WHERE id = %d`, -i, moved))
			switch {
			case err == nil && n == 1:
				moved = -i
			case err != nil && strings.Contains(err.Error(), "may be lost"):
				movedUnsure = true // from now on the row may be at either id
			}
			progress.Add(1)
		}
	}()
	waitFor(t, "the writer", func() bool { return progress.Load() >= 5 })
	h.exec(`ALTER TABLE it_rk_cw RENAME TO it_rk_cw2`)
	time.Sleep(50 * time.Millisecond)
	close(stop)
	<-stopped

	got := map[int64]bool{}
	rows, _ := h.query(`SELECT id FROM it_rk_cw2`)
	for _, r := range rows {
		var id int64
		if _, err := fmt.Sscan(r, &id); err != nil {
			t.Fatal(err)
		}
		got[id] = true
	}
	for _, id := range inserted {
		if !got[id] {
			t.Errorf("inserted row %d is missing after the move", id)
		}
	}
	for id := range got {
		if id > 100000 && !slices.Contains(inserted, id) && !slices.Contains(unsure, id) {
			t.Errorf("row %d is there after a failed insert", id)
		}
	}
	if !got[moved] && !movedUnsure {
		t.Errorf("the last successful update (to id %d) is missing", moved)
	}
	if len(rows) != len(got) {
		t.Errorf("%d rows, %d distinct ids", len(rows), len(got))
	}
	newPrefix, _ := h.tableNames(raw, "public", "it_rk_cw2")
	if n := h.prefixKeyCount(raw, newPrefix); n != len(rows) {
		t.Errorf("%d keys under the new prefix, %d rows", n, len(rows))
	}
	h.expectReleased(raw, oldPrefix, oldIndex)
	if len(inserted) == 0 || moved == 1 {
		t.Errorf("the writer made no progress before the rename")
	}
	t.Logf("%d inserts succeeded, %d unsure; the moved row is at %d", len(inserted), len(unsure), moved)
}
