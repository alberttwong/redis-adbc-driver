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

// A table dropped while another statement is still writing to it (#82):
// the writer fails, deletes what it wrote, and a new table of the same name
// gets new keys, so it never reads the dropped table's rows.

import (
	"encoding/json"
	"fmt"
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

// srRows is the number of rows the racing statements write: three
// pipelines.
const srRows = 3 * pipelineChunk

const srWriteErr = "was renamed with its rows moved to new keys, truncated or dropped, while this statement was writing to it; some of its changes may be lost or have gone to another table"

// namesN checks that a table's key prefix and index name are those of
// the name named with some N (namesFor), and returns N. N depends on the
// names earlier runs used, since they are never reused.
func (h *sqlHarness) namesN(raw goredis.UniversalClient, schema, table, named string) int64 {
	h.t.Helper()
	prefix, index := h.tableNames(raw, schema, table)
	base := rowPrefix(schema, named)
	n := int64(1)
	if prefix != base {
		digits, ok := strings.CutPrefix(prefix, strings.TrimSuffix(base, ":")+"~")
		digits, ok2 := strings.CutSuffix(digits, ":")
		var err error
		if n, err = strconv.ParseInt(digits, 10, 64); !ok || !ok2 || err != nil || n < 2 {
			h.t.Fatalf("table %s.%s: key prefix %q is not one of %q", schema, table, prefix, named)
		}
	}
	if want := namesFor(schema, named, n); prefix != want.prefix || index != want.index {
		h.t.Fatalf("table %s.%s: names %q, %q, want %q, %q", schema, table, prefix, index, want.prefix, want.index)
	}
	return n
}

// dropRace is a DROP TABLE that lands while a statement writes a table.
type dropRace struct {
	prefix string // the table's key prefix
	left   int    // keys under it once the statement had written every row
}

// raceDrop arranges for drop to run after the first pipeline of rows that a
// statement writes to table. The later pipelines then skip the check
// between pipelines (as they do within rekeyFence of the last one), so the
// statement writes the rest of the rows to keys that no table has, like
// one that was already sending them: the hook counts them after the last
// pipeline. Only the check after the last pipeline can find out.
func (h *sqlHarness) raceDrop(raw goredis.UniversalClient, table string, drop func(meta *tableMeta)) *dropRace {
	h.t.Helper()
	r := &dropRace{}
	testHookRowsWritten = func(meta *tableMeta, written int) {
		if meta.Name != table {
			return
		}
		if r.prefix == "" && written == pipelineChunk {
			r.prefix = meta.prefix()
			drop(meta)
			meta.readAt = time.Now().Add(time.Hour)
		}
		if r.prefix != "" && written == srRows {
			r.left = h.prefixKeyCount(raw, r.prefix)
		}
	}
	h.t.Cleanup(func() { testHookRowsWritten = nil })
	return r
}

// expectRaceLost checks the outcome of a statement that raced a DROP: it
// failed, it had written two pipelines of rows after the DROP, and none is
// left.
func (h *sqlHarness) expectRaceLost(raw goredis.UniversalClient, r *dropRace, err error) {
	h.t.Helper()
	testHookRowsWritten = nil
	if err == nil || !strings.Contains(err.Error(), srWriteErr) {
		h.t.Errorf("writer: got %v, want an error containing %q", err, srWriteErr)
	}
	if r.prefix == "" {
		h.t.Fatal("the DROP did not run")
	}
	if want := srRows - pipelineChunk; r.left != want {
		h.t.Errorf("%d keys under %q after the last write, want %d", r.left, r.prefix, want)
	}
	if n := h.prefixKeyCount(raw, r.prefix); n != 0 {
		h.t.Errorf("%d keys left under %q", n, r.prefix)
	}
}

// expectNewTable re-creates a raced table and fills it with rows whose
// note is NULL: none reads the dropped table's 'old'.
func (h *sqlHarness) expectNewTable(raw goredis.UniversalClient, r *dropRace, schema, create, table string) {
	h.t.Helper()
	h.exec(create)
	h.exec(fmt.Sprintf(`INSERT INTO %s (id) SELECT id FROM it_sr_src`, table))
	h.expectRows(fmt.Sprintf(`SELECT COUNT(*), COUNT(note) FROM %s`, table), fmt.Sprintf("%d|0", srRows))
	h.expectRows(fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE note = 'old'`, table), "0")
	if prefix, _ := h.tableNames(raw, schema, strings.TrimPrefix(table, "pg_temp.")); prefix == r.prefix {
		h.t.Errorf("the new table took the dropped table's key prefix %q", prefix)
	}
	if n := h.prefixKeyCount(raw, r.prefix); n != 0 {
		h.t.Errorf("%d keys under the dropped table's prefix %q", n, r.prefix)
	}
}

func (h *sqlHarness) setupStaleRows() {
	h.t.Helper()
	h.dropTables("it_sr_src", "it_sr_t")
	h.exec(fmt.Sprintf(`CREATE TABLE it_sr_src AS SELECT x AS id, 'old' AS note FROM generate_series(1, %d) AS g(x)`, srRows))
}

// The repro of #82: CREATE TABLE … AS, dropped by another connection.
func TestSQLDropDuringCreateTableAs(t *testing.T) {
	h := newSQLHarness(t)
	h.setupStaleRows()
	raw := h.rawClient()
	other := newSQLHarness(t)
	r := h.raceDrop(raw, "it_sr_t", func(*tableMeta) { other.exec(`DROP TABLE it_sr_t`) })
	_, err := tryExec(h.ctx, h.conn, `CREATE TABLE it_sr_t AS SELECT id, note FROM it_sr_src`)
	h.expectRaceLost(raw, r, err)
	h.expectError(`SELECT * FROM it_sr_t`, "does not exist")
	h.expectNewTable(raw, r, "public", `CREATE TABLE it_sr_t (id BIGINT, note VARCHAR)`, "it_sr_t")
}

func TestSQLDropDuringInsertSelect(t *testing.T) {
	h := newSQLHarness(t)
	h.setupStaleRows()
	raw := h.rawClient()
	other := newSQLHarness(t)
	h.exec(`CREATE TABLE it_sr_t (id BIGINT, note VARCHAR)`)
	r := h.raceDrop(raw, "it_sr_t", func(*tableMeta) { other.exec(`DROP TABLE it_sr_t`) })
	_, err := tryExec(h.ctx, h.conn, `INSERT INTO it_sr_t SELECT id, note FROM it_sr_src`)
	h.expectRaceLost(raw, r, err)
	h.expectNewTable(raw, r, "public", `CREATE TABLE it_sr_t (id BIGINT, note VARCHAR)`, "it_sr_t")
}

// ingestNotes bulk-ingests rows (id, 'old') in batches of the given sizes.
func (h *sqlHarness) ingestNotes(table, mode string, batches ...int) error {
	h.t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	var recs []arrow.RecordBatch
	next := int64(1)
	for _, n := range batches {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for i := 0; i < n; i++ {
			b.Field(0).(*array.Int64Builder).Append(next)
			b.Field(1).(*array.StringBuilder).Append("old")
			next++
		}
		recs = append(recs, b.NewRecordBatch())
		b.Release()
	}
	rdr, err := array.NewRecordReader(schema, recs)
	if err != nil {
		h.t.Fatal(err)
	}
	defer rdr.Release()
	for _, r := range recs {
		r.Release()
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
	if err := st.BindStream(h.ctx, rdr); err != nil {
		h.t.Fatal(err)
	}
	_, err = st.ExecuteUpdate(h.ctx)
	return err
}

// Bulk ingest, into a table it creates and into an existing one, in two
// batches: the one written when the DROP lands, and one that is not written.
func TestSQLDropDuringIngest(t *testing.T) {
	for _, mode := range []string{adbc.OptionValueIngestModeCreate, adbc.OptionValueIngestModeAppend} {
		t.Run(mode, func(t *testing.T) {
			h := newSQLHarness(t)
			h.setupStaleRows()
			raw := h.rawClient()
			other := newSQLHarness(t)
			if mode == adbc.OptionValueIngestModeAppend {
				h.exec(`CREATE TABLE it_sr_t (id BIGINT, note VARCHAR)`)
			}
			r := h.raceDrop(raw, "it_sr_t", func(*tableMeta) { other.exec(`DROP TABLE it_sr_t`) })
			err := h.ingestNotes("it_sr_t", mode, srRows, srRows)
			h.expectRaceLost(raw, r, err)
			h.expectNewTable(raw, r, "public", `CREATE TABLE it_sr_t (id BIGINT, note VARCHAR)`, "it_sr_t")
		})
	}

	// A batch written in one pipeline, soon enough not to check, is caught
	// by the next batch's row id allocation, which then deletes it.
	h := newSQLHarness(t)
	h.setupStaleRows()
	raw := h.rawClient()
	other := newSQLHarness(t)
	h.exec(`CREATE TABLE it_sr_t (id BIGINT, note VARCHAR)`)
	var prefix string
	testHookRowsWritten = func(meta *tableMeta, written int) {
		if meta.Name == "it_sr_t" && prefix == "" {
			prefix = meta.prefix()
			other.exec(`DROP TABLE it_sr_t`)
			// The batch's rows are written again after the DROP, as if
			// they had been on their way.
			for id := int64(1); id <= int64(written); id++ {
				if err := raw.HSet(h.ctx, prefix+strconv.FormatInt(id, 10), rowIDField, id, "note", "old").Err(); err != nil {
					t.Fatal(err)
				}
			}
			meta.readAt = time.Now().Add(time.Hour)
		}
	}
	t.Cleanup(func() { testHookRowsWritten = nil })
	err := h.ingestNotes("it_sr_t", adbc.OptionValueIngestModeAppend, pipelineChunk/2, pipelineChunk/2)
	testHookRowsWritten = nil
	if err == nil || !strings.Contains(err.Error(), srWriteErr) {
		t.Errorf("writer: got %v, want an error containing %q", err, srWriteErr)
	}
	if n := h.prefixKeyCount(raw, prefix); n != 0 {
		t.Errorf("%d keys left under %q", n, prefix)
	}
}

// A temporary table: here the DROP comes from another connection that took
// its owner for gone (sweepTemp), and the owner then creates it again.
func TestSQLDropDuringTempCreateTableAs(t *testing.T) {
	h := newSQLHarness(t)
	h.setupStaleRows()
	raw := h.rawClient()
	sweeper := &store{client: raw}
	r := h.raceDrop(raw, "it_sr_tmp", func(meta *tableMeta) {
		if err := sweeper.dropTable(h.ctx, meta.Schema, meta.Name, false); err != nil {
			t.Fatal(err)
		}
	})
	_, err := tryExec(h.ctx, h.conn, `CREATE TEMP TABLE it_sr_tmp AS SELECT id, note FROM it_sr_src`)
	h.expectRaceLost(raw, r, err)
	schema, _, _ := strings.Cut(r.prefix, ":")
	h.expectNewTable(raw, r, schema, `CREATE TEMP TABLE it_sr_tmp (id BIGINT, note VARCHAR)`, "pg_temp.it_sr_tmp")
	if n := h.namesN(raw, schema, "it_sr_tmp", "it_sr_tmp"); n < 2 {
		t.Errorf("the new temporary table has the names of N = %d", n)
	}
}

// sameKeys tells a table's current metadata from another table's of the
// same name (no Redis needed).
func TestSameKeys(t *testing.T) {
	enc := func(m tableMeta) string {
		raw, err := json.Marshal(&m)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	mine := tableMeta{Schema: "public", Name: "t", KeyPrefix: "public:t~2:", IndexName: "idx:public:t~2",
		Columns: []columnMeta{{Name: "c", Type: typeString}}}
	other := mine
	other.KeyPrefix, other.IndexName = "public:t~3:", "idx:public:t~3"
	// A comment that spells out another prefix doesn't fool it.
	sneaky := other
	sneaky.Comment = `"key_prefix":"public:t~2:"`
	legacy := tableMeta{Schema: "public", Name: "t"}
	reused := legacy
	reused.PrefixGen = 1
	for _, c := range []struct {
		meta tableMeta
		raw  string
		want bool
	}{
		{mine, enc(mine), true},
		{mine, enc(other), false},
		{mine, enc(sneaky), false},
		{mine, "", false},
		{legacy, enc(legacy), true},
		{legacy, enc(reused), false},
		{reused, enc(reused), true},
		{mine, enc(legacy), false},
	} {
		if got := sameKeys(&c.meta, c.raw); got != c.want {
			t.Errorf("sameKeys(%q, %s) = %v, want %v", c.meta.prefix(), c.raw, got, c.want)
		}
	}
}

// Key prefixes and index names are never reused: not after DROP TABLE, not
// after a rename, and not those that earlier versions released.
func TestSQLKeyPrefixesNotReused(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_sr_names", "it_sr_names2")
	raw := h.rawClient()
	seen := map[string]bool{}
	last := int64(0)
	create := func(what string) {
		t.Helper()
		h.exec(`CREATE TABLE it_sr_names (id INTEGER)`)
		n := h.namesN(raw, "public", "it_sr_names", "it_sr_names")
		prefix, index := h.tableNames(raw, "public", "it_sr_names")
		if seen[prefix] || seen[index] || n <= last {
			t.Errorf("%s: names %q, %q (N = %d after %d) were used before", what, prefix, index, n, last)
		}
		seen[prefix], seen[index], last = true, true, n
	}
	for i := 0; i < 3; i++ {
		create("after DROP TABLE")
		h.exec(`INSERT INTO it_sr_names VALUES (1)`)
		h.exec(`DROP TABLE it_sr_names`)
	}
	// Without the counter (a database used by v0.0.7, which recorded
	// releases but no counter), every prefix that was released is skipped.
	if err := raw.HDel(h.ctx, namesNextKey, rowPrefix("public", "it_sr_names")).Err(); err != nil {
		t.Fatal(err)
	}
	create("without the counter")
	// A renamed table keeps its names, and the name it leaves takes new ones.
	h.exec(`ALTER TABLE it_sr_names RENAME TO it_sr_names2`)
	if n := h.namesN(raw, "public", "it_sr_names2", "it_sr_names"); n != last {
		t.Errorf("a plain rename changed the names to N = %d, want %d", n, last)
	}
	create("after RENAME TO")
	h.expectRows(`SELECT COUNT(*) FROM it_sr_names`, "0")
	h.expectRows(`SELECT COUNT(*) FROM it_sr_names2`, "0")
}
