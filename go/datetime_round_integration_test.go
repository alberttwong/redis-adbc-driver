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

// Integration tests for rounding to a column's declared fractional-seconds
// precision (#103): INSERT, INSERT … SELECT, UPDATE, MERGE, CTAS and bulk
// ingest store the rounded value, which its text, its Arrow value and
// index queries agree on.

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestRoundPrecisionIssue103(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rnd", "it_rnd_src", "it_rnd_ctas", "it_rnd_ing")
	h.exec(`CREATE TABLE it_rnd (id INTEGER, ts0 TIMESTAMP(0), ts3 TIMESTAMP(3), ts6 TIMESTAMP,
		tz0 TIMESTAMPTZ(0), tz3 TIMESTAMP(3) WITH TIME ZONE, t0 TIME(0), t3 TIME(3), t6 TIME)`)
	// INSERT … VALUES with text: halves, less than halves, carries into the
	// next day, month and year (leap days too), 24:00:00, and before 2000
	// (where a half rounds down, as in Postgres) and BC.
	h.exec(`INSERT INTO it_rnd VALUES
		(1, '2024-01-10 10:00:00.5', '2024-01-10 10:00:00.1236', '2024-01-10 10:00:00.1234567',
		    '2024-01-10 23:59:59.9+00', '2024-01-10 10:00:00.0005+00', '10:00:00.5', '10:00:00.1236', '10:00:00.1234567'),
		(2, '2024-01-10 10:00:00.4', '2024-01-10 10:00:00.1234', '2024-01-10 10:00:00.1234564',
		    '2024-01-10 10:00:00.4+00', '2024-01-10 10:00:00.1234+00', '10:00:00.4', '10:00:00.1234', '10:00:00.1234564'),
		(3, '2024-12-31 23:59:59.5', '2024-02-28 23:59:59.9995', '2024-01-31 23:59:59.9999995',
		    '2024-02-29 23:59:59.5+00', '2023-12-31 23:59:59.9999-05', '23:59:59.9', '23:59:59.9995', '23:59:59.9999995'),
		(4, '1999-12-31 23:59:59.5', '1969-12-31 23:59:59.9996', '0044-03-15 12:00:00.0000005 BC',
		    '1990-06-15 10:00:00.5+00', '0001-12-31 23:59:59.9995+00 BC', '00:00:00.4', NULL, NULL),
		(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)
	stored := []string{
		"1|2024-01-10 10:00:01|2024-01-10 10:00:00.124|2024-01-10 10:00:00.123457|2024-01-11 00:00:00+00|2024-01-10 10:00:00.001+00|" +
			"10:00:01|10:00:00.124|10:00:00.123457",
		"2|2024-01-10 10:00:00|2024-01-10 10:00:00.123|2024-01-10 10:00:00.123456|2024-01-10 10:00:00+00|2024-01-10 10:00:00.123+00|" +
			"10:00:00|10:00:00.123|10:00:00.123456",
		"3|2025-01-01 00:00:00|2024-02-29 00:00:00|2024-02-01 00:00:00|2024-03-01 00:00:00+00|2024-01-01 05:00:00+00|" +
			"24:00:00|24:00:00|24:00:00",
		"4|1999-12-31 23:59:59|1970-01-01 00:00:00|0044-03-15 12:00:00 BC|1990-06-15 10:00:00+00|0001-12-31 23:59:59.999+00 BC|" +
			"00:00:00|NULL|NULL",
		"5|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL",
	}
	text := `SELECT id, CAST(ts0 AS TEXT), CAST(ts3 AS TEXT), CAST(ts6 AS TEXT), CAST(tz0 AS TEXT), CAST(tz3 AS TEXT),
		CAST(t0 AS TEXT), CAST(t3 AS TEXT), CAST(t6 AS TEXT) FROM it_rnd`
	h.expectRows(text+` ORDER BY id`, stored...)
	// The Arrow values are the rounded ones.
	h.expectRows(`SELECT id, ts0, ts3, ts6, tz0, tz3 FROM it_rnd WHERE id <= 3 ORDER BY id`,
		"1|2024-01-10T10:00:01|2024-01-10T10:00:00.124|2024-01-10T10:00:00.123457|2024-01-11T00:00:00Z|2024-01-10T10:00:00.001Z",
		"2|2024-01-10T10:00:00|2024-01-10T10:00:00.123|2024-01-10T10:00:00.123456|2024-01-10T10:00:00Z|2024-01-10T10:00:00.123Z",
		"3|2025-01-01T00:00:00|2024-02-29T00:00:00|2024-02-01T00:00:00|2024-03-01T00:00:00Z|2024-01-01T05:00:00Z")
	h.expectRows(`SELECT id, EXTRACT(epoch FROM t0), EXTRACT(epoch FROM t3), EXTRACT(hour FROM t6) FROM it_rnd WHERE id <= 3 ORDER BY id`,
		"1|36001|36000.124|10", "2|36000|36000.123|10", "3|86400|86400|24")
	// As stored in the row hash.
	raw := h.rawClient()
	if got, err := raw.HGet(h.ctx, rowKeyOf(h, "it_rnd", 1), "ts0").Result(); err != nil || got != "1704880801" {
		t.Errorf("stored ts0 of row 1: %q, %v; want 1704880801", got, err)
	}

	// Index queries compare with the stored, rounded values, and give what
	// the same comparison on the rows gives.
	for _, c := range []struct{ where, want string }{
		{`ts0 = '2024-01-10 10:00:01'`, "1"},
		{`ts0 = '2024-01-10 10:00:00.5'`, ""},
		{`ts0 = TIMESTAMP '2024-01-10 10:00:00.5'`, ""},
		{`ts0 > '2024-01-10 10:00:00.5'`, "1,3"},
		{`ts0 >= '2024-01-10 10:00:00.5' AND ts0 < '2024-01-10 10:00:01.5'`, "1"},
		{`ts0 <= '2024-01-10 10:00:00.4'`, "2,4"},
		{`ts0 < '2024-01-10 10:00:00.4'`, "2,4"},
		{`ts0 IN ('2024-01-10 10:00:01', '2025-01-01 00:00:00')`, "1,3"},
		{`ts0 IN ('2024-01-10 10:00:00.5', '2024-12-31 23:59:59.5')`, ""},
		{`ts3 = '2024-01-10 10:00:00.124'`, "1"},
		{`ts3 = '2024-01-10 10:00:00.1236'`, ""},
		{`ts3 >= '2024-02-29'`, "3"},
		{`ts6 = '2024-01-10 10:00:00.123457'`, "1"},
		{`ts6 > TIMESTAMP '2024-01-10 10:00:00.1234565'`, "1,3"},
		{`tz0 = '2024-01-11 00:00:00+00'`, "1"},
		{`tz0 > '2024-01-10 23:59:59.9+00'`, "1,3"},
		{`tz3 = '2024-01-10 10:00:00.001+00'`, "1"},
		{`tz3 = '2024-01-10 10:00:00.0005+00'`, ""},
		{`t0 = '24:00:00'`, "3"},
		{`t0 = '10:00:01'`, "1"},
		{`t0 > '10:00:00.5'`, "1,3"},
		{`t3 >= '23:59:59.9995'`, "3"},
		{`t6 = '24:00:00'`, "3"},
		{`t6 < '10:00:00.1234567'`, "2"},
		// Text with 7 digits is read in microseconds, as Postgres reads it
		// (a half to even): 10:00:00.123456, not below row 2's.
		{`t6 < '10:00:00.1234565'`, ""},
		{`ts6 = '2024-01-10 10:00:00.1234565'`, "2"},
	} {
		rows := func(where string) string {
			got, _ := h.query(`SELECT id FROM it_rnd WHERE ` + where + ` ORDER BY id`)
			return strings.Join(got, ",")
		}
		if got := rows(c.where); got != c.want {
			t.Errorf("WHERE %s: rows %q, want %q", c.where, got, c.want)
		}
		// The same comparison on an expression, which isn't pushed down.
		col := c.where[:strings.IndexByte(c.where, ' ')]
		unpushed := strings.ReplaceAll(c.where, col+" ", "COALESCE("+col+", NULL) ")
		if got := rows(unpushed); got != c.want {
			t.Errorf("WHERE %s: rows %q, want %q", unpushed, got, c.want)
		}
	}
	if q, residual := h.pushedWhere(raw, `SELECT id FROM it_rnd WHERE ts0 = '2024-01-10 10:00:01'`); q != "@ts0:[1704880801 1704880801]" || residual {
		t.Errorf("ts0 = '…:01': query %q, residual %v", q, residual)
	}
	if q, residual := h.pushedWhere(raw, `SELECT id FROM it_rnd WHERE ts0 = '2024-01-10 10:00:00.5'`); q != "@ts0:[1704880801 1704880801]" || !residual {
		t.Errorf("ts0 = '…:00.5': query %q, residual %v", q, residual)
	}
	if q, residual := h.pushedWhere(raw, `SELECT id FROM it_rnd WHERE t0 > '10:00:00.5'`); q != "@t0:[36001 +inf]" || !residual {
		t.Errorf("t0 > '10:00:00.5': query %q, residual %v", q, residual)
	}

	// INSERT … SELECT, UPDATE and MERGE from values of a finer precision.
	h.exec(`CREATE TABLE it_rnd_src (id INTEGER, ts9 TIMESTAMP(9), tz9 TIMESTAMP(9) WITH TIME ZONE, t9 TIME(9))`)
	h.exec(`INSERT INTO it_rnd_src VALUES
		(1, '2024-06-30 23:59:59.999999999', '2024-06-30 23:59:59.9999995+00', '23:59:59.999999999'),
		(2, '2024-06-30 12:00:00.000500001', '2024-06-30 12:00:00.000000499+00', '12:00:00.4999')`)
	h.exec(`INSERT INTO it_rnd (id, ts0, ts3, ts6, tz0, tz3, t0, t3, t6) SELECT id + 10, ts9, ts9, ts9, tz9, tz9, t9, t9, t9 FROM it_rnd_src`)
	h.expectRows(text+` WHERE id > 10 ORDER BY id`,
		"11|2024-07-01 00:00:00|2024-07-01 00:00:00|2024-07-01 00:00:00|2024-07-01 00:00:00+00|2024-07-01 00:00:00+00|24:00:00|24:00:00|24:00:00",
		"12|2024-06-30 12:00:00|2024-06-30 12:00:00.001|2024-06-30 12:00:00.0005|2024-06-30 12:00:00+00|2024-06-30 12:00:00+00|"+
			"12:00:00|12:00:00.5|12:00:00.4999")
	h.exec(`UPDATE it_rnd SET ts0 = TIMESTAMP '2024-05-05 05:05:05.5', t0 = '05:05:05.5', ts3 = ts6 WHERE id = 1`)
	h.expectRows(`SELECT CAST(ts0 AS TEXT), CAST(t0 AS TEXT), CAST(ts3 AS TEXT) FROM it_rnd WHERE id = 1`,
		"2024-05-05 05:05:06|05:05:06|2024-01-10 10:00:00.123")
	h.exec(`MERGE INTO it_rnd t USING it_rnd_src s ON t.id = s.id + 10
		WHEN MATCHED AND s.id = 1 THEN UPDATE SET ts0 = s.ts9 - INTERVAL '0.6 seconds', tz0 = s.tz9 - INTERVAL '1 second'
		WHEN MATCHED THEN DELETE`)
	h.exec(`MERGE INTO it_rnd t USING it_rnd_src s ON t.id = s.id + 20
		WHEN NOT MATCHED THEN INSERT (id, ts0, t3) VALUES (s.id + 20, s.ts9, s.t9)`)
	h.expectRows(`SELECT id, CAST(ts0 AS TEXT), CAST(tz0 AS TEXT), CAST(t3 AS TEXT) FROM it_rnd WHERE id > 10 ORDER BY id`,
		"11|2024-06-30 23:59:59|2024-06-30 23:59:59+00|24:00:00",
		"21|2024-07-01 00:00:00|NULL|24:00:00",
		"22|2024-06-30 12:00:00|NULL|12:00:00.5")

	// CTAS: the cast's precision is the column's.
	h.exec(`CREATE TABLE it_rnd_ctas AS SELECT id, CAST(ts9 AS TIMESTAMP(0)) AS ts0, CAST(tz9 AS TIMESTAMPTZ(3)) AS tz3,
		CAST(t9 AS TIME(0)) AS t0 FROM it_rnd_src`)
	h.expectRows(`SELECT column_name, data_type, datetime_precision FROM information_schema.columns
		WHERE table_name = 'it_rnd_ctas' AND column_name <> 'id' ORDER BY ordinal_position`,
		"ts0|TIMESTAMP(0)|0", "tz3|TIMESTAMP(3) WITH TIME ZONE|3", "t0|TIME(0)|0")
	h.expectRows(`SELECT id, CAST(ts0 AS TEXT), CAST(tz3 AS TEXT), CAST(t0 AS TEXT) FROM it_rnd_ctas ORDER BY id`,
		"1|2024-07-01 00:00:00|2024-07-01 00:00:00+00|24:00:00", "2|2024-06-30 12:00:00|2024-06-30 12:00:00+00|12:00:00")
	h.expectRows(`SELECT id FROM it_rnd_ctas WHERE ts0 = '2024-07-01' AND t0 = '24:00:00'`, "1")

	// Bulk ingest into columns of a lower precision rounds too.
	h.exec(`CREATE TABLE it_rnd_ing (id BIGINT, ts0 TIMESTAMP(0), tz3 TIMESTAMPTZ(3), t0 TIME(0))`)
	mem := memory.DefaultAllocator
	ib := array.NewInt64Builder(mem)
	tsb := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	tzb := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"})
	tb := array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Nanosecond})
	for _, b := range []array.Builder{ib, tsb, tzb, tb} {
		defer b.Release()
	}
	ns := func(s string) arrow.Timestamp {
		tm, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatal(err)
		}
		return arrow.Timestamp(tm.UnixNano())
	}
	ib.AppendValues([]int64{1, 2}, nil)
	tsb.AppendValues([]arrow.Timestamp{ns("2024-01-10T10:00:00.5Z"), ns("2024-12-31T23:59:59.5Z")}, nil)
	tzb.AppendValues([]arrow.Timestamp{ns("2024-01-10T10:00:00.1236Z"), ns("2024-01-10T10:00:00.1234Z")}, nil)
	tb.AppendValues([]arrow.Time64{arrow.Time64(10*3600e9 + 5e8), arrow.Time64(86399e9 + 9e8)}, nil)
	cols := []arrow.Array{ib.NewArray(), tsb.NewArray(), tzb.NewArray(), tb.NewArray()}
	for _, c := range cols {
		defer c.Release()
	}
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ts0", Type: cols[1].DataType()},
		{Name: "tz3", Type: cols[2].DataType()},
		{Name: "t0", Type: cols[3].DataType()},
	}, nil), cols, 2)
	defer rec.Release()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	for k, v := range map[string]string{adbc.OptionKeyIngestTargetTable: "it_rnd_ing", adbc.OptionKeyIngestMode: adbc.OptionValueIngestModeAppend} {
		if err := st.SetOption(h.ctx, k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Bind(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ExecuteUpdate(h.ctx); err != nil || n != 2 {
		t.Fatalf("ingest: %d, %v", n, err)
	}
	h.expectRows(`SELECT id, CAST(ts0 AS TEXT), CAST(tz3 AS TEXT), CAST(t0 AS TEXT) FROM it_rnd_ing ORDER BY id`,
		"1|2024-01-10 10:00:01|2024-01-10 10:00:00.124+00|10:00:01", "2|2025-01-01 00:00:00|2024-01-10 10:00:00.123+00|24:00:00")
	h.expectRows(`SELECT id FROM it_rnd_ing WHERE ts0 = '2024-01-10 10:00:01' OR tz3 = '2024-01-10 10:00:00.123+00' ORDER BY id`, "1", "2")
}

// rowKeyOf is the key of the row with the given id of a table in the
// default schema.
func rowKeyOf(h *sqlHarness, table string, id int) string {
	h.t.Helper()
	got, _ := h.query(`SELECT __rowid FROM ` + table + ` WHERE id = ` + string(rune('0'+id)))
	if len(got) != 1 {
		h.t.Fatalf("row %d of %s: %q", id, table, got)
	}
	meta, err := (&store{client: h.rawClient()}).getTable(h.ctx, defaultSchema, table)
	if err != nil {
		h.t.Fatal(err)
	}
	return meta.prefix() + got[0]
}
