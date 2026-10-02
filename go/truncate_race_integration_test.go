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

// A table truncated while another statement is still writing to it (#104):
// TRUNCATE moves the table to new keys, so the writer fails and deletes what
// it wrote, and the rows inserted afterwards, from row id 1 again after
// RESTART IDENTITY, are all new. The races are made with the hooks of
// stale_rows_integration_test.go (raceDrop).

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

func (h *sqlHarness) setupTruncRace() {
	h.t.Helper()
	h.dropTables("it_tr_src", "it_tr_t")
	h.exec(fmt.Sprintf(`CREATE TABLE it_tr_src AS SELECT x AS id, 'old' AS note FROM generate_series(1, %d) AS g(x)`, srRows))
}

// truncModes are the TRUNCATE variants the races run with.
var truncModes = []struct {
	name    string
	clause  string
	restart bool
}{
	{"continue", "", false},
	{"restart", " RESTART IDENTITY", true},
}

// expectTruncated checks a table that was truncated while a statement was
// writing to it (r): it is empty and has new names, and the rows inserted
// now are new. Their note is NULL (none was merged into a row the writer
// left), and their row ids run from firstID.
func (h *sqlHarness) expectTruncated(raw goredis.UniversalClient, r *dropRace, schema, table string, firstID int) {
	h.t.Helper()
	h.expectRows(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table), "0")
	if prefix := h.tablePrefix(raw, schema, strings.TrimPrefix(table, "pg_temp.")); prefix == r.prefix {
		h.t.Errorf("the truncated table kept its key prefix %q", prefix)
	}
	h.exec(fmt.Sprintf(`INSERT INTO %s (id) SELECT id FROM it_tr_src`, table))
	h.expectRows(fmt.Sprintf(`SELECT COUNT(*), COUNT(note), MIN(__rowid), MAX(__rowid) FROM %s`, table),
		fmt.Sprintf("%d|0|%d|%d", srRows, firstID, firstID+srRows-1))
	h.expectRows(fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE note = 'old'`, table), "0")
	if n := h.prefixKeyCount(raw, r.prefix); n != 0 {
		h.t.Errorf("%d keys under the old key prefix %q", n, r.prefix)
	}
}

// The repro of #104: INSERT … SELECT, truncated by another connection after
// the first of its three pipelines. The writer took row ids 1-3000 before
// the TRUNCATE.
func TestSQLTruncateDuringInsertSelect(t *testing.T) {
	for _, m := range truncModes {
		t.Run(m.name, func(t *testing.T) {
			h := newSQLHarness(t)
			h.setupTruncRace()
			raw := h.rawClient()
			other := newSQLHarness(t)
			h.exec(`CREATE TABLE it_tr_t (id BIGINT, note VARCHAR)`)
			r := h.raceDrop(raw, "it_tr_t", func(*tableMeta) { other.exec(`TRUNCATE TABLE it_tr_t` + m.clause) })
			_, err := tryExec(h.ctx, h.conn, `INSERT INTO it_tr_t SELECT id, note FROM it_tr_src`)
			h.expectRaceLost(raw, r, err)
			h.expectTruncated(raw, r, "public", "it_tr_t", map[bool]int{true: 1, false: srRows + 1}[m.restart])
		})
	}
}

// CREATE TABLE … AS, truncated while it writes its rows; the table it
// created stays.
func TestSQLTruncateDuringCreateTableAs(t *testing.T) {
	for _, m := range truncModes {
		t.Run(m.name, func(t *testing.T) {
			h := newSQLHarness(t)
			h.setupTruncRace()
			raw := h.rawClient()
			other := newSQLHarness(t)
			r := h.raceDrop(raw, "it_tr_t", func(*tableMeta) { other.exec(`TRUNCATE it_tr_t` + m.clause) })
			_, err := tryExec(h.ctx, h.conn, `CREATE TABLE it_tr_t AS SELECT id, note FROM it_tr_src`)
			h.expectRaceLost(raw, r, err)
			h.expectTruncated(raw, r, "public", "it_tr_t", map[bool]int{true: 1, false: srRows + 1}[m.restart])
		})
	}
}

// Bulk ingest, into a table it creates and into an existing one, in two
// batches: the one written when the TRUNCATE lands, and one that is not
// written.
func TestSQLTruncateDuringIngest(t *testing.T) {
	for _, mode := range []string{adbc.OptionValueIngestModeCreate, adbc.OptionValueIngestModeAppend} {
		for _, m := range truncModes {
			t.Run(mode+"/"+m.name, func(t *testing.T) {
				h := newSQLHarness(t)
				h.setupTruncRace()
				raw := h.rawClient()
				other := newSQLHarness(t)
				if mode == adbc.OptionValueIngestModeAppend {
					h.exec(`CREATE TABLE it_tr_t (id BIGINT, note VARCHAR)`)
				}
				r := h.raceDrop(raw, "it_tr_t", func(*tableMeta) { other.exec(`TRUNCATE it_tr_t` + m.clause) })
				err := h.ingestNotes("it_tr_t", mode, srRows, srRows)
				h.expectRaceLost(raw, r, err)
				h.expectTruncated(raw, r, "public", "it_tr_t", map[bool]int{true: 1, false: srRows + 1}[m.restart])
			})
		}
	}

	// A batch written in one pipeline, soon enough not to check, is caught
	// by the next batch's row id allocation, which then deletes it. That
	// allocation took ids 501-1000 of the counter that RESTART IDENTITY
	// restarted, so the new rows' ids start after them.
	for _, m := range truncModes {
		t.Run("one pipeline/"+m.name, func(t *testing.T) {
			h := newSQLHarness(t)
			h.setupTruncRace()
			raw := h.rawClient()
			other := newSQLHarness(t)
			h.exec(`CREATE TABLE it_tr_t (id BIGINT, note VARCHAR)`)
			r := &dropRace{}
			testHookRowsWritten = func(meta *tableMeta, written int) {
				if meta.Name == "it_tr_t" && r.prefix == "" {
					r.prefix = meta.prefix()
					other.exec(`TRUNCATE it_tr_t` + m.clause)
					// The batch's rows are written again after the TRUNCATE,
					// as if they had been on their way.
					for id := int64(1); id <= int64(written); id++ {
						if err := raw.HSet(h.ctx, r.prefix+strconv.FormatInt(id, 10), rowIDField, id, "id", id, "note", "old").Err(); err != nil {
							t.Fatal(err)
						}
					}
					meta.readAt = time.Now().Add(time.Hour)
				}
			}
			t.Cleanup(func() { testHookRowsWritten = nil })
			err := h.ingestNotes("it_tr_t", adbc.OptionValueIngestModeAppend, pipelineChunk/2, pipelineChunk/2)
			testHookRowsWritten = nil
			if err == nil || !strings.Contains(err.Error(), srWriteErr) {
				t.Errorf("writer: got %v, want an error containing %q", err, srWriteErr)
			}
			if n := h.prefixKeyCount(raw, r.prefix); n != 0 {
				t.Errorf("%d keys left under %q", n, r.prefix)
			}
			h.expectTruncated(raw, r, "public", "it_tr_t", map[bool]int{true: pipelineChunk/2 + 1, false: pipelineChunk + 1}[m.restart])
		})
	}
}

// A temporary table, truncated by its own connection (the only one that can
// name it) while a statement of that connection is still writing to it.
func TestSQLTruncateDuringTempCreateTableAs(t *testing.T) {
	for _, m := range truncModes {
		t.Run(m.name, func(t *testing.T) {
			h := newSQLHarness(t)
			h.setupTruncRace()
			raw := h.rawClient()
			r := h.raceDrop(raw, "it_tr_tmp", func(*tableMeta) {
				if _, err := tryExec(h.ctx, h.conn, `TRUNCATE pg_temp.it_tr_tmp`+m.clause); err != nil {
					t.Fatal(err)
				}
			})
			_, err := tryExec(h.ctx, h.conn, `CREATE TEMP TABLE it_tr_tmp AS SELECT id, note FROM it_tr_src`)
			h.expectRaceLost(raw, r, err)
			schema, _, _ := strings.Cut(r.prefix, ":")
			h.expectTruncated(raw, r, schema, "pg_temp.it_tr_tmp", map[bool]int{true: 1, false: srRows + 1}[m.restart])
			if n := h.namesN(raw, schema, "it_tr_tmp", "it_tr_tmp"); n != 2 {
				t.Errorf("the truncated temporary table has the names of N = %d, want 2", n)
			}
		})
	}
}

// A write that allocated its row ids before TRUNCATE … RESTART IDENTITY and
// sends its rows after it, soon enough not to check (as a small INSERT
// does), succeeds. Its row stays under the old key prefix, where nothing
// reads it, and the rows inserted afterwards, from row id 1 again, are new.
func TestSQLTruncateRestartLateWrite(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tr_late")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_tr_late (id BIGINT, note VARCHAR)`)
	h.exec(`INSERT INTO it_tr_late VALUES (1, 'a'), (2, 'b')`)
	st := &store{client: raw}
	stale, err := st.getTable(h.ctx, defaultSchema, "it_tr_late")
	if err != nil {
		t.Fatal(err)
	}
	rows := [][]Value{{intValue(typeInt64, 3), stringValue("old")}}
	a, err := st.allocRows(h.ctx, stale, rows)
	if err != nil {
		t.Fatal(err)
	}
	h.exec(`TRUNCATE it_tr_late RESTART IDENTITY`)
	stale.readAt = time.Now().Add(time.Hour) // within rekeyFence: no check
	if _, err := st.writeRows(h.ctx, stale, a, rows); err != nil {
		t.Fatal(err)
	}
	h.exec(`INSERT INTO it_tr_late (id) VALUES (10), (11), (12), (13)`)
	h.expectRows(`SELECT __rowid, id, note FROM it_tr_late ORDER BY __rowid`,
		"1|10|NULL", "2|11|NULL", "3|12|NULL", "4|13|NULL")
	if n := h.prefixKeyCount(raw, stale.prefix()); n != 1 {
		t.Errorf("%d keys under the old key prefix %q, want the late row's", n, stale.prefix())
	}
}

// An ADD COLUMN that commits while TRUNCATE makes the new index: the switch
// adds the column to it.
func TestSQLTruncateDuringAddColumn(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tr_alt")
	raw := h.rawClient()
	other := newSQLHarness(t)
	h.exec(`CREATE TABLE it_tr_alt (id BIGINT)`)
	h.exec(`INSERT INTO it_tr_alt VALUES (1), (2)`)
	testHookTruncating = func() {
		testHookTruncating = nil
		other.exec(`ALTER TABLE it_tr_alt ADD COLUMN z INTEGER`)
	}
	t.Cleanup(func() { testHookTruncating = nil })
	h.exec(`TRUNCATE it_tr_alt`)
	testHookTruncating = nil
	if got := h.indexAttrs(raw, "public", "it_tr_alt"); got != "__rowid id z" {
		t.Errorf("index attributes %q, want __rowid id z", got)
	}
	h.exec(`INSERT INTO it_tr_alt VALUES (3, 5), (4, 6)`)
	h.expectRows(`SELECT id FROM it_tr_alt WHERE z = 5`, "3")
}

// An ADD COLUMN that added its column to the old index commits after a
// TRUNCATE: it fails, and works when run again.
func TestSQLAddColumnDuringTruncate(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tr_alt2")
	raw := h.rawClient()
	other := newSQLHarness(t)
	h.exec(`CREATE TABLE it_tr_alt2 (id BIGINT)`)
	h.exec(`INSERT INTO it_tr_alt2 VALUES (1), (2)`)
	testHookAltered = func(*tableMeta) {
		testHookAltered = nil
		other.exec(`TRUNCATE it_tr_alt2`)
	}
	t.Cleanup(func() { testHookAltered = nil })
	h.expectError(`ALTER TABLE it_tr_alt2 ADD COLUMN z INTEGER`, `table "it_tr_alt2" changed concurrently; try again`)
	testHookAltered = nil
	h.expectRows(`SELECT column_name FROM information_schema.columns WHERE table_name = 'it_tr_alt2'`, "id")
	h.exec(`ALTER TABLE it_tr_alt2 ADD COLUMN z INTEGER`)
	if got := h.indexAttrs(raw, "public", "it_tr_alt2"); got != "__rowid id z" {
		t.Errorf("index attributes %q, want __rowid id z", got)
	}
	h.exec(`INSERT INTO it_tr_alt2 VALUES (3, 5), (4, 6)`)
	h.expectRows(`SELECT id FROM it_tr_alt2 WHERE z = 5`, "3")
}

// TRUNCATE of several tables empties all of them or none: here one is
// dropped while the new indexes are made. The other keeps its rows and
// names, and the names reserved for it are given back.
func TestSQLTruncateAllOrNone(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tr_a", "it_tr_b")
	raw := h.rawClient()
	other := newSQLHarness(t)
	h.exec(`CREATE TABLE it_tr_a (id BIGINT)`)
	h.exec(`CREATE TABLE it_tr_b (id BIGINT)`)
	h.exec(`INSERT INTO it_tr_a VALUES (1), (2)`)
	h.exec(`INSERT INTO it_tr_b VALUES (3)`)
	prefix, index := h.tableNames(raw, "public", "it_tr_a")
	testHookTruncating = func() {
		testHookTruncating = nil
		other.exec(`DROP TABLE it_tr_b`)
	}
	t.Cleanup(func() { testHookTruncating = nil })
	h.expectError(`TRUNCATE it_tr_a, it_tr_b`, `table "public"."it_tr_b" does not exist`)
	testHookTruncating = nil
	h.expectRows(`SELECT id FROM it_tr_a ORDER BY id`, "1", "2")
	if p, i := h.tableNames(raw, "public", "it_tr_a"); p != prefix || i != index {
		t.Errorf("it_tr_a has the names %q, %q, want %q, %q", p, i, prefix, index)
	}
	n, err := raw.HGet(h.ctx, namesNextKey, rowPrefix("public", "it_tr_a")).Int64()
	if err != nil {
		t.Fatal(err)
	}
	if reserved := namesFor("public", "it_tr_a", n); reserved.prefix == prefix {
		t.Errorf("no names were reserved for it_tr_a")
	} else {
		h.expectReleased(raw, reserved.prefix, reserved.index)
	}
}

// A DROP TABLE that has dropped the table's index when a TRUNCATE moves the
// table to new keys starts again: it drops the new index too, with the rows
// written since, and releases the new names.
func TestSQLDropDuringTruncate(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tr_drop")
	raw := h.rawClient()
	other := newSQLHarness(t)
	h.exec(`CREATE TABLE it_tr_drop (id BIGINT)`)
	h.exec(`INSERT INTO it_tr_drop VALUES (1), (2)`)
	prefix, index := h.tableNames(raw, "public", "it_tr_drop")
	var newPrefix, newIndex string
	testHookDropping = func() {
		testHookDropping = nil
		other.exec(`TRUNCATE it_tr_drop`)
		other.exec(`INSERT INTO it_tr_drop VALUES (3)`)
		newPrefix, newIndex = h.tableNames(raw, "public", "it_tr_drop")
	}
	t.Cleanup(func() { testHookDropping = nil })
	h.exec(`DROP TABLE it_tr_drop`)
	testHookDropping = nil
	h.expectError(`SELECT * FROM it_tr_drop`, "does not exist")
	h.expectReleased(raw, prefix, index)
	if newPrefix == prefix {
		t.Errorf("the TRUNCATE kept the key prefix %q", prefix)
	}
	h.expectReleased(raw, newPrefix, newIndex)
}
