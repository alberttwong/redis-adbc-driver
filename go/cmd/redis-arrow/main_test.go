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

package main

import (
	"bytes"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

func TestResolveFormat(t *testing.T) {
	for _, c := range []struct {
		format, out string
		file        bool
	}{
		{"auto", "-", false},
		{"auto", "x.arrow", true},
		{"auto", "x.ARROWS", false},
		{"auto", "dir/x.arrows", false},
		{"auto", "x.feather", true},
		{"file", "-", true},
		{"stream", "x.arrow", false},
	} {
		got, err := resolveFormat(c.format, c.out)
		if err != nil || got != c.file {
			t.Errorf("resolveFormat(%q, %q) = %v, %v; want %v", c.format, c.out, got, err, c.file)
		}
	}
	if _, err := resolveFormat("parquet", "x"); err == nil {
		t.Error("resolveFormat accepted parquet")
	}
}

func TestOptionsSet(t *testing.T) {
	o := options{}
	for _, s := range []string{"adbc.redis.read_timeout=30m", "a=b=c", "empty="} {
		if err := o.Set(s); err != nil {
			t.Fatalf("Set(%q): %v", s, err)
		}
	}
	if got, want := o.String(), "a=b=c,adbc.redis.read_timeout=30m,empty="; got != want {
		t.Errorf("options = %q, want %q", got, want)
	}
	for _, s := range []string{"novalue", "=v"} {
		if err := o.Set(s); err == nil {
			t.Errorf("Set(%q) succeeded", s)
		}
	}
}

func TestCommandLineErrors(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"export"}, "no query given"},
		{[]string{"export", "-sql", "SELECT 1", "SELECT 2"}, "not both"},
		{[]string{"export", "-format", "csv", "SELECT 1"}, "unknown -format"},
		{[]string{"export", "-compression", "gzip", "SELECT 1"}, "unknown -compression"},
		{[]string{"import", "x.arrow"}, "-table is required"},
		{[]string{"import", "-table", "t", "-mode", "upsert", "x.arrow"}, "unknown -mode"},
		{[]string{"import", "-table", "t", "a.arrow", "b.arrow"}, "want one input path"},
		{[]string{"import", "-table", "t", filepath.Join(t.TempDir(), "missing.arrow")}, "no such file"},
		{[]string{"export", "-bogus"}, "flag provided but not defined"},
		{[]string{"scan"}, "-prefix is required"},
		{[]string{"scan", "-prefix", "u:", "-on-error", "skip"}, "unknown -on-error"},
		{[]string{"scan", "-prefix", "u:", "-format", "csv"}, "unknown -format"},
		{[]string{"scan", "-prefix", "u:", "-type", "age"}, "want key=value"},
		{[]string{"check", "u:"}, "unexpected arguments"},
		{[]string{"merge"}, `unknown command "merge"`},
	} {
		err := run(ctx, c.args, strings.NewReader(""), io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: err = %v, want it to contain %q", c.args, err, c.want)
		}
	}
}

// testBatch has a column of each kind that the driver maps to its own SQL
// type, with NULLs and non-finite doubles.
func testBatch(t *testing.T) arrow.RecordBatch {
	t.Helper()
	ts := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	dec := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "at", Type: ts, Nullable: true},
		{Name: "price", Type: dec, Nullable: true},
		{Name: "day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "ok", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	valid := []bool{true, true, true, true, false}
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"ada", "", "café ☕", "x\ty", ""}, valid)
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.5, math.NaN(), math.Inf(1), math.Inf(-1), 0}, valid)
	at := time.Date(2026, 10, 2, 12, 34, 56, 789012000, time.UTC)
	b.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{
		arrow.Timestamp(at.UnixMicro()), 0, arrow.Timestamp(at.Add(-time.Hour).UnixMicro()), -1, 0,
	}, valid)
	b.Field(4).(*array.Decimal128Builder).AppendValues([]decimal128.Num{
		decimal128.FromI64(1999), decimal128.FromI64(-5), decimal128.FromI64(0), decimal128.FromI64(99999999), {},
	}, valid)
	b.Field(5).(*array.Date32Builder).AppendValues([]arrow.Date32{
		arrow.Date32FromTime(at), 0, -1, 20000, 0,
	}, valid)
	b.Field(6).(*array.BooleanBuilder).AppendValues([]bool{true, false, true, false, false}, valid)
	return b.NewRecordBatch()
}

func writeTestFile(t *testing.T, path string, rec arrow.RecordBatch, fileFormat bool) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Two batches, to check that every batch is read.
	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec.NewSlice(0, 2), rec.NewSlice(2, rec.NumRows())})
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	if _, err := writeIPC(f, rdr, fileFormat, nil); err != nil {
		t.Fatal(err)
	}
}

// readAll concatenates every batch that rdr returns into one batch, and
// releases rdr and the input.
func readAll(t *testing.T, rdr array.RecordReader, closeInput func()) arrow.RecordBatch {
	t.Helper()
	defer closeInput()
	defer rdr.Release()
	schema := rdr.Schema()
	cols := make([][]arrow.Array, schema.NumFields())
	var rows int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for i := range cols {
			c := rec.Column(i)
			c.Retain()
			cols[i] = append(cols[i], c)
		}
		rows += rec.NumRows()
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	out := make([]arrow.Array, len(cols))
	for i, parts := range cols {
		if len(parts) == 0 {
			out[i] = array.MakeArrayOfNull(alloc, schema.Field(i).Type, 0)
			continue
		}
		c, err := array.Concatenate(parts, alloc)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range parts {
			p.Release()
		}
		out[i] = c
	}
	rec := array.NewRecordBatch(schema, out, rows)
	for _, c := range out {
		c.Release()
	}
	return rec
}

func readPath(t *testing.T, path string) arrow.RecordBatch {
	t.Helper()
	rdr, closeInput, err := openInput(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return readAll(t, rdr, closeInput)
}

func readBytes(t *testing.T, data []byte) arrow.RecordBatch {
	t.Helper()
	rdr, closeInput, err := openInput("-", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return readAll(t, rdr, closeInput)
}

// checkEqual compares names, types and values; NaN equals NaN.
func checkEqual(t *testing.T, what string, got, want arrow.RecordBatch) {
	t.Helper()
	if got.NumCols() != want.NumCols() || got.NumRows() != want.NumRows() {
		t.Fatalf("%s: got %d columns x %d rows, want %d x %d", what, got.NumCols(), got.NumRows(), want.NumCols(), want.NumRows())
	}
	for i := range int(want.NumCols()) {
		name := want.ColumnName(i)
		if got.ColumnName(i) != name {
			t.Errorf("%s: column %d is %q, want %q", what, i, got.ColumnName(i), name)
		}
		g, w := got.Column(i), want.Column(i)
		if !arrow.TypeEqual(g.DataType(), w.DataType()) {
			t.Errorf("%s: column %s has type %s, want %s", what, name, g.DataType(), w.DataType())
			continue
		}
		if !array.ApproxEqual(g, w, array.WithNaNsEqual(true)) {
			t.Errorf("%s: column %s = %v, want %v", what, name, g, w)
		}
	}
}

func TestOpenInputFormats(t *testing.T) {
	rec := testBatch(t)
	defer rec.Release()
	dir := t.TempDir()
	for _, fileFormat := range []bool{true, false} {
		path := filepath.Join(dir, "t.arrows")
		if fileFormat {
			path = filepath.Join(dir, "t.arrow")
		}
		writeTestFile(t, path, rec, fileFormat)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := bytes.HasPrefix(data, fileMagic); got != fileFormat {
			t.Fatalf("%s starts with the file magic: %v, want %v", path, got, fileFormat)
		}

		got := readPath(t, path)
		checkEqual(t, path, got, rec)
		got.Release()

		got = readBytes(t, data)
		checkEqual(t, path+" on stdin", got, rec)
		got.Release()
	}

	bad := filepath.Join(dir, "bad.arrow")
	if err := os.WriteFile(bad, []byte("id,name\n1,ada\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openInput(bad, nil); err == nil || !strings.Contains(err.Error(), "neither an Arrow IPC file nor a stream") {
		t.Errorf("openInput of a CSV file: err = %v", err)
	}
}

// export runs the export command and returns what it wrote to stdout and
// stderr.
func export(t *testing.T, args ...string) ([]byte, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := runExport(context.Background(), args, &stdout, &stderr); err != nil {
		t.Fatalf("export %v: %v", args, err)
	}
	return stdout.Bytes(), stderr.String()
}

func importFile(t *testing.T, args ...string) string {
	t.Helper()
	var stderr bytes.Buffer
	if err := runImport(context.Background(), args, nil, &stderr); err != nil {
		t.Fatalf("import %v: %v", args, err)
	}
	return stderr.String()
}

func count(t *testing.T, uri, table string) int64 {
	t.Helper()
	out, _ := export(t, "-uri", uri, "SELECT COUNT(*) AS n FROM "+table)
	rec := readBytes(t, out)
	defer rec.Release()
	return rec.Column(0).(*array.Int64).Value(0)
}

func execSQL(t *testing.T, uri, sql string) {
	t.Helper()
	ctx := context.Background()
	conn, closeConn, err := (&connFlags{uri: uri}).open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeConn()
	st, err := conn.NewStatement(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(ctx)
	if err := st.SetSqlQuery(ctx, sql); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ExecuteUpdate(ctx); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func TestRedisArrowRoundTrip(t *testing.T) {
	uri := os.Getenv("REDIS_URI")
	if uri == "" {
		t.Skip("set REDIS_URI to run the redis-arrow integration test")
	}
	const table = "it_ipc_rt"
	execSQL(t, uri, "DROP TABLE IF EXISTS "+table)
	t.Cleanup(func() { execSQL(t, uri, "DROP TABLE IF EXISTS "+table) })

	rec := testBatch(t)
	defer rec.Release()
	dir := t.TempDir()
	in := filepath.Join(dir, "in.arrow")
	writeTestFile(t, in, rec, true)
	if got, want := importFile(t, "-uri", uri, "-table", table, in), "5 rows ingested into "+table+"\n"; got != want {
		t.Errorf("import printed %q, want %q", got, want)
	}

	query := "SELECT id, name, score, at, price, day, ok FROM " + table + " ORDER BY id"
	out := filepath.Join(dir, "out.arrow")
	if _, msg := export(t, "-uri", uri, "-o", out, query); msg != "5 rows\n" {
		t.Errorf("export printed %q, want %q", msg, "5 rows\n")
	}
	got := readPath(t, out)
	checkEqual(t, "file export", got, rec)
	got.Release()
	if info, err := os.Stat(out); err != nil || info.Mode().Perm() != 0o644 {
		t.Errorf("stat %s: %v, %v", out, info, err)
	}

	for _, compression := range []string{"none", "lz4", "zstd"} {
		stream, _ := export(t, "-uri", uri, "-compression", compression, "-sql", query)
		if bytes.HasPrefix(stream, fileMagic) {
			t.Errorf("stdout export with -compression %s wrote the file format", compression)
		}
		got := readBytes(t, stream)
		checkEqual(t, "stream export, "+compression, got, rec)
		got.Release()
	}

	// A query without rows still writes its schema.
	empty := filepath.Join(dir, "empty.arrows")
	if _, msg := export(t, "-uri", uri, "-o", empty, "SELECT id, name FROM "+table+" WHERE id < 0"); msg != "0 rows\n" {
		t.Errorf("empty export printed %q", msg)
	}
	got = readPath(t, empty)
	if fields := got.Schema().Fields(); len(fields) != 2 || fields[0].Name != "id" || fields[1].Name != "name" || got.NumRows() != 0 {
		t.Errorf("empty export: schema %s, %d rows", got.Schema(), got.NumRows())
	}
	got.Release()

	// Modes.
	importFile(t, "-uri", uri, "-table", table, "-mode", "append", in)
	if n := count(t, uri, table); n != 10 {
		t.Errorf("after append: %d rows, want 10", n)
	}
	var stderr bytes.Buffer
	if err := runImport(context.Background(), []string{"-uri", uri, "-table", table, in}, nil, &stderr); err == nil {
		t.Error("import -mode create into an existing table succeeded")
	}
	importFile(t, "-uri", uri, "-table", table, "-mode", "replace", in)
	if n := count(t, uri, table); n != 5 {
		t.Errorf("after replace: %d rows, want 5", n)
	}

	// Statements without a result set, and failing queries, write nothing.
	for _, sql := range []string{"DROP TABLE IF EXISTS it_ipc_missing", "SELECT nope FROM " + table} {
		path := filepath.Join(dir, "fail.arrow")
		err := runExport(context.Background(), []string{"-uri", uri, "-o", path, sql}, io.Discard, io.Discard)
		if err == nil {
			t.Errorf("export of %q succeeded", sql)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("export of %q left %s: %v", sql, path, err)
		}
	}
	err := runExport(context.Background(), []string{"-uri", uri, "DROP TABLE IF EXISTS it_ipc_missing"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "no result set") {
		t.Errorf("export of DDL: err = %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("temporary file %s left behind", e.Name())
		}
	}
}
