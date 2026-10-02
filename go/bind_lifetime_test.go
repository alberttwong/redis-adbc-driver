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

// Regression tests for https://github.com/alberttwong/redis-adbc-driver-arrow-ipc/issues/13:
// bound string parameters must not be read after the bound Arrow data is
// released. Parameter data is built with an allocator that overwrites
// buffers when they are freed, so a value that still points into a released
// buffer reads back as 0xC0 bytes every time (instead of only when the
// memory happens to be reused, as with data imported from C).

import (
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// poisonAllocator overwrites every buffer with 0xC0 when it is freed.
type poisonAllocator struct{ memory.Allocator }

func (p poisonAllocator) Free(b []byte) {
	for i := range b {
		b[i] = 0xC0
	}
	p.Allocator.Free(b)
}

func (p poisonAllocator) Reallocate(size int, b []byte) []byte {
	out := p.Allocator.Allocate(size)
	copy(out, b)
	p.Free(b)
	return out
}

// stringBatch builds a batch of (id, s) rows whose buffers are poisoned on
// release.
func stringBatch(t *testing.T, mem memory.Allocator, valueType arrow.DataType, ids []int64, strs []string) arrow.RecordBatch {
	t.Helper()
	ib := array.NewInt64Builder(mem)
	defer ib.Release()
	ib.AppendValues(ids, nil)
	idArr := ib.NewArray()
	defer idArr.Release()

	var sArr arrow.Array
	switch valueType.ID() {
	case arrow.STRING:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.AppendValues(strs, nil)
		sArr = b.NewArray()
	case arrow.LARGE_STRING:
		b := array.NewLargeStringBuilder(mem)
		defer b.Release()
		b.AppendValues(strs, nil)
		sArr = b.NewArray()
	case arrow.STRING_VIEW:
		b := array.NewStringViewBuilder(mem)
		defer b.Release()
		for _, s := range strs {
			b.Append(s)
		}
		sArr = b.NewArray()
	case arrow.DICTIONARY:
		b := array.NewDictionaryBuilder(mem, valueType.(*arrow.DictionaryType)).(*array.BinaryDictionaryBuilder)
		defer b.Release()
		for _, s := range strs {
			if err := b.AppendString(s); err != nil {
				t.Fatal(err)
			}
		}
		sArr = b.NewArray()
	default:
		t.Fatalf("unsupported type %s", valueType)
	}
	defer sArr.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: valueType, Nullable: true},
	}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{idArr, sArr}, int64(len(ids)))
}

func TestSQLBoundStringsOutliveTheirBuffers(t *testing.T) {
	h := newSQLHarness(t)
	mem := poisonAllocator{memory.NewGoAllocator()}
	h.exec("DROP TABLE IF EXISTS it_bindstr")
	h.exec("CREATE TABLE it_bindstr (id BIGINT, s VARCHAR)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_bindstr") })

	// Multi-row INSERT with string parameters (the dbt seed pattern), for
	// every string type the driver accepts.
	types := []arrow.DataType{
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.LargeString,
		arrow.BinaryTypes.StringView,
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String},
	}
	var want []string
	for i, typ := range types {
		st, err := h.conn.NewStatement(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.SetSqlQuery(h.ctx, `INSERT INTO it_bindstr (id, s) VALUES (?, ?)`); err != nil {
			t.Fatal(err)
		}
		ids := []int64{int64(i*10 + 1), int64(i*10 + 2)}
		strs := []string{"two " + typ.String(), "three: こんにちは " + strings.Repeat("x", 40)}
		rec := stringBatch(t, mem, typ, ids, strs)
		if err := st.Bind(h.ctx, rec); err != nil {
			t.Fatal(err)
		}
		rec.Release() // the caller's reference, as the C FFI does after Bind
		if n, err := st.ExecuteUpdate(h.ctx); err != nil || n != 2 {
			t.Fatalf("%s: INSERT: n=%d err=%v", typ, n, err)
		}
		st.Close(h.ctx)
		for k := range ids {
			want = append(want, strings.Join([]string{itoa(ids[k]), strs[k]}, "|"))
		}
	}

	// A stream of several batches: each batch is released when the next one
	// is read, before the statement executes.
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetSqlQuery(h.ctx, `INSERT INTO it_bindstr (id, s) VALUES (?, ?)`); err != nil {
		t.Fatal(err)
	}
	var batches []arrow.RecordBatch
	for b := 0; b < 3; b++ {
		id := int64(100 + b)
		s := "stream batch " + itoa(id)
		batches = append(batches, stringBatch(t, mem, arrow.BinaryTypes.String, []int64{id}, []string{s}))
		want = append(want, itoa(id)+"|"+s)
	}
	rdr, err := array.NewRecordReader(batches[0].Schema(), batches)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range batches {
		b.Release()
	}
	if err := st.BindStream(h.ctx, rdr); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ExecuteUpdate(h.ctx); err != nil || n != 3 {
		t.Fatalf("stream INSERT: n=%d err=%v", n, err)
	}
	st.Close(h.ctx)

	h.expectRows(`SELECT id, s FROM it_bindstr ORDER BY id`, want...)

	// SELECT ? with string parameters returns them intact.
	st, err = h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, `SELECT ? AS a, ? AS b`); err != nil {
		t.Fatal(err)
	}
	b1 := array.NewStringBuilder(mem)
	b1.Append("x")
	a1 := b1.NewArray()
	b2 := array.NewStringBuilder(mem)
	b2.Append("yy")
	a2 := b2.NewArray()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "0", Type: arrow.BinaryTypes.String}, {Name: "1", Type: arrow.BinaryTypes.String},
	}, nil), []arrow.Array{a1, a2}, 1)
	a1.Release()
	a2.Release()
	b1.Release()
	b2.Release()
	if err := st.Bind(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	rec.Release()
	out, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	out.Next()
	r := out.RecordBatch()
	if got := r.Column(0).ValueStr(0) + "|" + r.Column(1).ValueStr(0); got != "x|yy" {
		t.Errorf("SELECT ?, ? = %q, want %q", got, "x|yy")
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
