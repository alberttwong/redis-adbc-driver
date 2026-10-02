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
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

const streamRows = 25_000 // three cursor pages of 10,000 rows

// setupStreamTable ingests table with n rows: id 0…n-1, label "r<id>" and
// qty id % 10, all indexed.
func (h *sqlHarness) setupStreamTable(table string, n int) {
	h.t.Helper()
	h.dropTables(table)
	mem := memory.DefaultAllocator
	ib, lb, qb := array.NewInt64Builder(mem), array.NewStringBuilder(mem), array.NewInt32Builder(mem)
	defer ib.Release()
	defer lb.Release()
	defer qb.Release()
	for i := range n {
		ib.Append(int64(i))
		lb.Append(fmt.Sprintf("r%d", i))
		qb.Append(int32(i % 10))
	}
	cols := []arrow.Array{ib.NewArray(), lb.NewArray(), qb.NewArray()}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "qty", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	rec := array.NewRecordBatch(schema, cols, int64(n))
	defer rec.Release()
	for _, c := range cols {
		c.Release()
	}
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetOption(h.ctx, adbc.OptionKeyIngestTargetTable, table); err != nil {
		h.t.Fatal(err)
	}
	if err := st.Bind(h.ctx, rec); err != nil {
		h.t.Fatal(err)
	}
	if _, err := st.ExecuteUpdate(h.ctx); err != nil {
		h.t.Fatal(err)
	}
}

// openStream runs sql with the statement options, and returns its reader
// and row count.
func (h *sqlHarness) openStream(ctx context.Context, conn adbc.ConnectionWithContext, sql string, rec arrow.RecordBatch,
	opts map[string]string) (array.RecordReader, int64) {
	h.t.Helper()
	st, err := conn.NewStatement(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { st.Close(ctx) })
	for k, v := range opts {
		if err := st.SetOption(ctx, k, v); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := st.SetSqlQuery(ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	if rec != nil {
		if err := st.Bind(ctx, rec); err != nil {
			h.t.Fatal(err)
		}
	}
	rdr, n, err := st.ExecuteQuery(ctx)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	return rdr, n
}

// readInts reads a result to the end: its first column, and the size of
// each batch.
func readInts(rdr array.RecordReader) (vals []int64, batches []int, err error) {
	for rdr.Next() {
		rec := rdr.RecordBatch()
		col := rec.Column(0).(*array.Int64)
		for i := range col.Len() {
			vals = append(vals, col.Value(i))
		}
		batches = append(batches, int(rec.NumRows()))
	}
	return vals, batches, rdr.Err()
}

// cursorOf returns the id of a stream's open cursor, or 0.
func cursorOf(rdr array.RecordReader) int64 {
	if r, ok := rdr.(*streamReader); ok && r.it != nil && r.it.cur != nil {
		return r.it.cur.cursor
	}
	return 0
}

// delCursor deletes a cursor of a table's index, on the node that runs its
// searches, and reports whether it was there.
func (h *sqlHarness) delCursor(raw goredis.UniversalClient, table string, id int64) bool {
	h.t.Helper()
	st := &store{client: raw}
	meta, err := st.getTable(h.ctx, defaultSchema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	return st.searchDo(h.ctx, meta.index(), "FT.CURSOR", "DEL", meta.index(), id).Err() == nil
}

func sequence(from, n, step int64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = from + int64(i)*step
	}
	return out
}

func TestSQLStreamResults(t *testing.T) {
	h := newSQLHarness(t)
	const table = "it_stream_t"
	h.setupStreamTable(table, streamRows)
	raw := h.rawClient()
	small := map[string]string{OptionIntStreamBatchRows: "1000"}

	t.Run("streams in insertion order", func(t *testing.T) {
		rdr, n := h.openStream(h.ctx, h.conn, "SELECT id, label FROM "+table, nil, small)
		defer rdr.Release()
		if n != -1 {
			t.Errorf("row count %d, want -1 for a stream", n)
		}
		if !rdr.Next() || rdr.RecordBatch().NumRows() != 1000 {
			t.Fatalf("first batch: %v", rdr.Err())
		}
		id := cursorOf(rdr)
		if id == 0 {
			t.Fatal("after the first batch, the stream has no cursor")
		}
		ids, batches, err := readInts(rdr)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(sequence(0, 1000, 1), ids...)
		if !slices.Equal(ids, sequence(0, streamRows, 1)) {
			t.Errorf("got %d ids, not 0…%d in order", len(ids), streamRows-1)
		}
		if len(batches) != streamRows/1000-1 || slices.ContainsFunc(batches, func(b int) bool { return b != 1000 }) {
			t.Errorf("batches %v, want batches of 1000", batches)
		}
		if h.delCursor(raw, table, id) {
			t.Error("the cursor was open at the end")
		}
		if fieldNames(rdr.Schema()) != "id|label" {
			t.Errorf("schema %v", rdr.Schema())
		}
	})

	t.Run("ORDER BY in the index", func(t *testing.T) {
		rdr, n := h.openStream(h.ctx, h.conn, "SELECT id FROM "+table+" ORDER BY id DESC", nil, small)
		defer rdr.Release()
		ids, _, err := readInts(rdr)
		if err != nil || n != -1 || !slices.Equal(ids, sequence(streamRows-1, streamRows, -1)) {
			t.Errorf("count %d, %d ids, %v", n, len(ids), err)
		}
	})

	t.Run("releasing early deletes the cursor", func(t *testing.T) {
		rdr, _ := h.openStream(h.ctx, h.conn, "SELECT id FROM "+table, nil, small)
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		id := cursorOf(rdr)
		rdr.Release()
		if id == 0 || h.delCursor(raw, table, id) {
			t.Errorf("cursor %d was open after Release", id)
		}
	})

	t.Run("cancelling the context ends the stream", func(t *testing.T) {
		ctx, cancel := context.WithCancel(h.ctx)
		defer cancel()
		rdr, _ := h.openStream(ctx, h.conn, "SELECT id FROM "+table, nil, small)
		defer rdr.Release()
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		id := cursorOf(rdr)
		cancel()
		var ae adbc.Error
		if rdr.Next() || !errors.As(rdr.Err(), &ae) || ae.Code != adbc.StatusCancelled {
			t.Errorf("after cancel: err = %v, want Cancelled", rdr.Err())
		}
		if id == 0 || h.delCursor(raw, table, id) {
			t.Errorf("cursor %d was open after cancel", id)
		}
	})

	t.Run("closing the connection ends the stream", func(t *testing.T) {
		db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(h.ctx, map[string]string{adbc.OptionKeyURI: adminURI()})
		if err != nil {
			t.Fatal(err)
		}
		conn, err := db.Open(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
		rdr, _ := h.openStream(h.ctx, conn, "SELECT id FROM "+table, nil, small)
		defer rdr.Release()
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		id := cursorOf(rdr)
		if err := conn.Close(h.ctx); err != nil {
			t.Fatal(err)
		}
		if id == 0 || h.delCursor(raw, table, id) {
			t.Errorf("cursor %d was open after Close", id)
		}
		var ae adbc.Error
		if rdr.Next() || !errors.As(rdr.Err(), &ae) || ae.Code != adbc.StatusInvalidState {
			t.Errorf("after Close: err = %v, want InvalidState", rdr.Err())
		}
	})

	t.Run("LIMIT and OFFSET with a residual filter", func(t *testing.T) {
		// Fits in the first batch: the LIMIT closes the cursor at once.
		rdr, n := h.openStream(h.ctx, h.conn, "SELECT id FROM "+table+" WHERE id % 7 = 3 LIMIT 5 OFFSET 2", nil, small)
		ids, _, err := readInts(rdr)
		rdr.Release()
		if err != nil || n != 5 || !slices.Equal(ids, []int64{17, 24, 31, 38, 45}) {
			t.Errorf("small LIMIT: %v (count %d), %v", ids, n, err)
		}
		// Streamed, and stopping in the second of three cursor pages.
		rdr, n = h.openStream(h.ctx, h.conn, "SELECT id FROM "+table+" WHERE id % 2 = 0 LIMIT 8000 OFFSET 100", nil, small)
		defer rdr.Release()
		id := cursorOf(rdr)
		ids, _, err = readInts(rdr)
		if err != nil || n != -1 || !slices.Equal(ids, sequence(200, 8000, 2)) {
			t.Errorf("streamed LIMIT: %d ids from %v (count %d), %v", len(ids), ids[:min(len(ids), 3)], n, err)
		}
		if id == 0 || h.delCursor(raw, table, id) {
			t.Errorf("cursor %d was open after the LIMIT", id)
		}
	})

	t.Run("an error on a later page", func(t *testing.T) {
		rdr, n := h.openStream(h.ctx, h.conn, "SELECT 1000 / (id - 15000) FROM "+table, nil, small)
		defer rdr.Release()
		id := cursorOf(rdr)
		for rdr.Next() {
		}
		if n != -1 || rdr.Err() == nil || !strings.Contains(rdr.Err().Error(), "division by zero") {
			t.Errorf("count %d, err = %v, want division by zero", n, rdr.Err())
		}
		if id == 0 || h.delCursor(raw, table, id) {
			t.Errorf("cursor %d was open after the error", id)
		}
	})

	t.Run("a cursor that expired", func(t *testing.T) {
		rdr, _ := h.openStream(h.ctx, h.conn, "SELECT id FROM "+table, nil, small)
		defer rdr.Release()
		if !rdr.Next() {
			t.Fatal(rdr.Err())
		}
		if !h.delCursor(raw, table, cursorOf(rdr)) {
			t.Fatal("the stream's cursor wasn't open")
		}
		for rdr.Next() {
		}
		if rdr.Err() == nil || !strings.Contains(rdr.Err().Error(), OptionStringStreamResults+"=false") {
			t.Errorf("err = %v, want the expired cursor's", rdr.Err())
		}
	})

	t.Run("a result in one batch is whole", func(t *testing.T) {
		rdr, n := h.openStream(h.ctx, h.conn, "SELECT id FROM "+table+" WHERE qty = 3", nil, nil)
		defer rdr.Release()
		ids, _, err := readInts(rdr)
		if err != nil || n != streamRows/10 || !slices.Equal(ids, sequence(3, streamRows/10, 10)) {
			t.Errorf("count %d, %d ids, %v", n, len(ids), err)
		}
	})

	t.Run("bound parameters", func(t *testing.T) {
		sql := "SELECT id, label FROM " + table + " WHERE qty = $1"
		want, err := h.executeSchema(sql)
		if err != nil {
			t.Fatal(err)
		}
		b := array.NewInt32Builder(memory.DefaultAllocator)
		defer b.Release()
		b.Append(3)
		col := b.NewArray()
		defer col.Release()
		rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "p", Type: arrow.PrimitiveTypes.Int32}}, nil),
			[]arrow.Array{col}, 1)
		defer rec.Release()
		rdr, n := h.openStream(h.ctx, h.conn, sql, rec, map[string]string{OptionIntStreamBatchRows: "500"})
		defer rdr.Release()
		ids, _, err := readInts(rdr)
		if err != nil || n != -1 || !slices.Equal(ids, sequence(3, streamRows/10, 10)) {
			t.Errorf("count %d, %d ids, %v", n, len(ids), err)
		}
		if !rdr.Schema().Equal(want) {
			t.Errorf("schema %v, want ExecuteSchema's %v", rdr.Schema(), want)
		}
	})

	t.Run("materialized", func(t *testing.T) {
		h.dropTables("it_stream_k")
		h.exec("CREATE TABLE it_stream_k (k INTEGER, name VARCHAR)")
		h.exec("INSERT INTO it_stream_k VALUES (1, 'one'), (2, 'two')")
		for _, tc := range []struct {
			sql  string
			opts map[string]string
			rows int64
		}{
			{"SELECT id FROM " + table, map[string]string{OptionStringStreamResults: "false"}, streamRows},
			{"SELECT id FROM " + table + " ORDER BY id % 10, id", small, streamRows},
			{"SELECT id, label FROM " + table + " ORDER BY qty NULLS FIRST, id", small, streamRows},
			{"SELECT DISTINCT id FROM " + table, small, streamRows},
			{"SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM " + table, small, streamRows},
			{"SELECT t.id FROM " + table + " t JOIN it_stream_k k ON k.k = t.qty", small, 2 * streamRows / 10},
			{"SELECT id, (SELECT name FROM it_stream_k k WHERE k.k = t.qty) FROM " + table + " t", small, streamRows},
			{"SELECT id FROM " + table + " UNION ALL SELECT k FROM it_stream_k", small, streamRows + 2},
			{"SELECT qty, COUNT(*) FROM " + table + " GROUP BY qty", small, 10},
		} {
			rdr, n := h.openStream(h.ctx, h.conn, tc.sql, nil, tc.opts)
			var rows int64
			for rdr.Next() {
				rows += rdr.RecordBatch().NumRows()
			}
			if n != tc.rows || rows != tc.rows || rdr.Err() != nil {
				t.Errorf("%s: count %d, %d rows, %v; want %d, whole", tc.sql, n, rows, rdr.Err(), tc.rows)
			}
			rdr.Release()
		}
	})

	t.Run("options", func(t *testing.T) {
		st, err := h.conn.NewStatement(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close(h.ctx)
		for _, kv := range [][2]string{{OptionStringStreamResults, "maybe"}, {OptionIntStreamBatchRows, "0"}} {
			if err := st.SetOption(h.ctx, kv[0], kv[1]); err == nil {
				t.Errorf("%s=%s was accepted", kv[0], kv[1])
			}
		}
		opts := st.(adbc.GetSetOptionsWithContext)
		if v, _ := opts.GetOption(h.ctx, OptionStringStreamResults); v != "true" {
			t.Errorf("%s = %q, want true", OptionStringStreamResults, v)
		}
		if err := opts.SetOptionInt(h.ctx, OptionIntStreamBatchRows, 42); err != nil {
			t.Fatal(err)
		}
		if v, _ := opts.GetOptionInt(h.ctx, OptionIntStreamBatchRows); v != 42 {
			t.Errorf("%s = %d, want 42", OptionIntStreamBatchRows, v)
		}
	})
}
