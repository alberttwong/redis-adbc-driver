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

// Integration tests for fractional-second precisions other than 0, 3, 6
// and 9 (#113): columns of TIMESTAMP(2), TIME(5), TIMESTAMP(1) WITH TIME
// ZONE and so on hold values rounded to their digits, on every write path,
// and report their type; CURRENT_TIMESTAMP(p) is rounded to p. The expected
// results are Postgres 16's, except where noted.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestFractionalPrecisionIssue113(t *testing.T) {
	h := newSQLHarness(t)
	h.dropViews("it_prec_v")
	h.dropTables("it_prec", "it_prec_src", "it_prec_ctas", "it_prec_def", "it_prec_ing", "it_prec_bad")
	h.exec(`CREATE TABLE it_prec (id INTEGER, ts2 TIMESTAMP(2), t5 TIME(5), tz1 TIMESTAMP(1) WITH TIME ZONE,
		ts4 TIMESTAMP(4), ts1 TIMESTAMP(1))`)
	// The issue's acceptance: '…00.123456' in a TIMESTAMP(2) is …00.12.
	// Also halves, carries into the next day and year, 24:00:00, a half
	// before 2000 (down) and BC.
	h.exec(`INSERT INTO it_prec VALUES
		(1, '2024-01-10 10:00:00.123456', '10:00:00.123456', '2024-01-10 10:00:00.15+00', '2024-01-10 10:00:00.12345',
		    '2024-01-10 10:00:00.123456'),
		(2, '2024-01-10 10:00:00.125', '23:59:59.999995', '2024-12-31 23:59:59.96-05', '2024-01-10 10:00:00.123449',
		    '2024-01-10 10:00:00.15'),
		(3, '1999-12-31 23:59:59.995', '00:00:00.000004', '2024-01-10 10:00:00.04+00', '2024-12-31 23:59:59.99995',
		    '1999-12-31 23:59:59.95'),
		(4, '2024-01-10 23:59:59.995', '10:00:00.1', NULL, '0044-03-15 12:00:00.00005 BC', '2024-01-10 10:00:00.1'),
		(5, NULL, NULL, NULL, NULL, NULL)`)
	text := `SELECT id, CAST(ts2 AS TEXT), CAST(t5 AS TEXT), CAST(tz1 AS TEXT), CAST(ts4 AS TEXT), CAST(ts1 AS TEXT) FROM it_prec`
	h.expectRows(text+` ORDER BY id`,
		"1|2024-01-10 10:00:00.12|10:00:00.12346|2024-01-10 10:00:00.2+00|2024-01-10 10:00:00.1235|2024-01-10 10:00:00.1",
		"2|2024-01-10 10:00:00.13|24:00:00|2025-01-01 05:00:00+00|2024-01-10 10:00:00.1234|2024-01-10 10:00:00.2",
		"3|1999-12-31 23:59:59.99|00:00:00|2024-01-10 10:00:00+00|2025-01-01 00:00:00|1999-12-31 23:59:59.9",
		"4|2024-01-11 00:00:00|10:00:00.1|NULL|0044-03-15 12:00:00 BC|2024-01-10 10:00:00.1",
		"5|NULL|NULL|NULL|NULL|NULL")
	// The Arrow values are the rounded ones, in the unit with the next
	// number of digits.
	schema := h.expectRows(`SELECT id, ts2, tz1, ts4, ts1, EXTRACT(epoch FROM t5) FROM it_prec WHERE id <= 2 ORDER BY id`,
		"1|2024-01-10T10:00:00.12|2024-01-10T10:00:00.2Z|2024-01-10T10:00:00.1235|2024-01-10T10:00:00.1|36000.12346",
		"2|2024-01-10T10:00:00.13|2025-01-01T05:00:00Z|2024-01-10T10:00:00.1234|2024-01-10T10:00:00.2|86400")
	var units []string
	for _, f := range schema.Fields()[1:5] {
		units = append(units, f.Type.String())
	}
	if got := strings.Join(units, " "); got != "timestamp[ms] timestamp[ms, tz=UTC] timestamp[us] timestamp[ms]" {
		t.Errorf("Arrow types: %s", got)
	}
	if _, schema := h.query(`SELECT t5 FROM it_prec`); schema.Field(0).Type.String() != "time64[us]" {
		t.Errorf("TIME(5) is Arrow %s", schema.Field(0).Type)
	}
	// As stored in the row hash: milliseconds, a multiple of 10.
	raw := h.rawClient()
	if got, err := raw.HGet(h.ctx, rowKeyOf(h, "it_prec", 1), "ts2").Result(); err != nil || got != "1704880800120" {
		t.Errorf("stored ts2 of row 1: %q, %v; want 1704880800120", got, err)
	}
	if got, err := raw.HGet(h.ctx, rowKeyOf(h, "it_prec", 1), "t5").Result(); err != nil || got != "36000123460" {
		t.Errorf("stored t5 of row 1: %q, %v; want 36000123460", got, err)
	}

	// Index queries compare with the stored, rounded values, and give what
	// the same comparison on the rows gives.
	for _, c := range []struct{ where, want string }{
		{`ts2 = '2024-01-10 10:00:00.12'`, "1"},
		{`ts2 = '2024-01-10 10:00:00.123456'`, ""},
		{`ts2 = '2024-01-10 10:00:00.13'`, "2"},
		{`ts2 > '2024-01-10 10:00:00.125'`, "2,4"},
		{`ts2 >= '2024-01-10 10:00:00.12' AND ts2 < '2024-01-10 10:00:00.125'`, "1"},
		{`ts2 IN ('2024-01-10 10:00:00.12', '2024-01-11')`, "1,4"},
		{`ts2 = TIMESTAMP(2) '2024-01-10 10:00:00.123'`, "1"},
		{`ts2 <= '1999-12-31 23:59:59.99'`, "3"},
		{`t5 = '10:00:00.12346'`, "1"},
		{`t5 = '24:00:00'`, "2"},
		{`t5 < '10:00:00.123456'`, "3,4"},
		{`t5 >= '10:00:00.1'`, "1,2,4"},
		{`tz1 = '2024-01-10 10:00:00.2+00'`, "1"},
		{`tz1 > '2024-01-10 10:00:00.15+00'`, "1,2"},
		{`ts4 = '2024-01-10 10:00:00.1235'`, "1"},
		{`ts4 <= '2024-01-10 10:00:00.12345'`, "2,4"},
		{`ts1 = '2024-01-10 10:00:00.1'`, "1,4"},
		{`ts1 BETWEEN '2024-01-10 10:00:00.1' AND '2024-01-10 10:00:00.2'`, "1,2,4"},
	} {
		rows := func(where string) string {
			got, _ := h.query(`SELECT id FROM it_prec WHERE ` + where + ` ORDER BY id`)
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
	if q, residual := h.pushedWhere(raw, `SELECT id FROM it_prec WHERE ts2 = '2024-01-10 10:00:00.12'`); q != "@ts2:[1704880800120 1704880800120]" || residual {
		t.Errorf("ts2 = '….12': query %q, residual %v", q, residual)
	}
	// A constant with more digits is rounded to a stored value; the
	// comparison is re-checked.
	if q, residual := h.pushedWhere(raw, `SELECT id FROM it_prec WHERE ts2 = '2024-01-10 10:00:00.125'`); q != "@ts2:[1704880800130 1704880800130]" || !residual {
		t.Errorf("ts2 = '….125': query %q, residual %v", q, residual)
	}

	// Joins, IN and EXISTS, DISTINCT, GROUP BY, aggregates, UNION and CASE
	// see the rounded values; combined types have the most digits.
	h.expectRows(`SELECT a.id, b.id FROM it_prec a JOIN it_prec b ON a.ts1 = CAST(b.ts2 AS TIMESTAMP(1)) ORDER BY a.id, b.id`,
		"1|1", "1|2", "4|1", "4|2")
	h.expectRows(`SELECT id FROM it_prec WHERE ts2 IN (SELECT CAST(ts4 AS TIMESTAMP(2)) FROM it_prec)`, "1")
	h.expectRows(`SELECT id FROM it_prec a WHERE EXISTS (SELECT 1 FROM it_prec b WHERE CAST(b.ts4 AS TIMESTAMP(2)) = a.ts2)`, "1")
	h.expectRows(`SELECT COUNT(*) FROM it_prec a JOIN it_prec b ON a.ts1 = b.ts1`, "6")
	h.expectRows(`SELECT COUNT(DISTINCT ts1), CAST(MAX(ts2) AS TEXT), CAST(MIN(t5) AS TEXT) FROM it_prec`,
		"3|2024-01-11 00:00:00|00:00:00")
	h.expectRows(`SELECT CAST(ts1 AS TEXT), COUNT(*) FROM it_prec GROUP BY ts1 ORDER BY ts1 NULLS LAST`,
		"1999-12-31 23:59:59.9|1", "2024-01-10 10:00:00.1|2", "2024-01-10 10:00:00.2|1", "NULL|1")
	h.expectRows(`SELECT CAST(x AS TEXT) FROM (SELECT ts2 AS x FROM it_prec WHERE id <= 2 UNION SELECT ts4 FROM it_prec WHERE id <= 2) u ORDER BY x`,
		"2024-01-10 10:00:00.12", "2024-01-10 10:00:00.1234", "2024-01-10 10:00:00.1235", "2024-01-10 10:00:00.13")
	h.expectRows(`SELECT id, CAST(CASE WHEN id = 1 THEN ts2 ELSE ts4 END AS TEXT) FROM it_prec WHERE id <= 2 ORDER BY 2`,
		"1|2024-01-10 10:00:00.12", "2|2024-01-10 10:00:00.1234")
	// Arithmetic and functions have no declared precision, as in Postgres.
	h.expectRows(`SELECT id, CAST(ts2 + INTERVAL '0.001 second' AS TEXT), CAST(DATE_TRUNC('second', ts4) AS TEXT) FROM it_prec
		WHERE id <= 2 ORDER BY id`,
		"1|2024-01-10 10:00:00.121|2024-01-10 10:00:00", "2|2024-01-10 10:00:00.131|2024-01-10 10:00:00")

	// The types: information_schema (data_type is the driver's name, which
	// Postgres writes as `timestamp without time zone`; datetime_precision
	// is p), GetObjects (xdbc_type_name and xdbc_decimal_digits) and the
	// stored metadata.
	h.expectRows(`SELECT column_name, data_type, datetime_precision FROM information_schema.columns
		WHERE table_name = 'it_prec' AND column_name <> 'id' ORDER BY ordinal_position`,
		"ts2|TIMESTAMP(2)|2", "t5|TIME(5)|5", "tz1|TIMESTAMP(1) WITH TIME ZONE|1", "ts4|TIMESTAMP(4)|4", "ts1|TIMESTAMP(1)|1")
	if got, want := strings.Join(h.columnDigits("it_prec"), ", "),
		"id INTEGER NULL, ts2 TIMESTAMP(2) 2, t5 TIME(5) 5, tz1 TIMESTAMP(1) WITH TIME ZONE 1, ts4 TIMESTAMP(4) 4, ts1 TIMESTAMP(1) 1"; got != want {
		t.Errorf("GetObjects: %s\nwant %s", got, want)
	}
	if m := h.rawMeta(raw, defaultSchema, "it_prec"); !strings.Contains(m, `{"kind":"timestamp","unit":"ms","precision":2}`) ||
		!strings.Contains(m, `{"kind":"time","unit":"us","precision":5}`) {
		t.Errorf("stored metadata: %s", m)
	}

	// INSERT … SELECT, UPDATE and both kinds of MERGE round too.
	h.exec(`CREATE TABLE it_prec_src (id INTEGER, ts TIMESTAMP, tz TIMESTAMPTZ, t TIME)`)
	h.exec(`INSERT INTO it_prec_src VALUES
		(1, '2024-06-30 23:59:59.996', '2024-06-30 23:59:59.95+00', '23:59:59.999996'),
		(2, '2024-06-30 12:00:00.004999', '2024-06-30 12:00:00.049999+00', '12:00:00.000005')`)
	h.exec(`INSERT INTO it_prec (id, ts2, t5, tz1, ts4, ts1) SELECT id + 10, ts, t, tz, ts, ts FROM it_prec_src`)
	h.expectRows(text+` WHERE id > 10 ORDER BY id`,
		"11|2024-07-01 00:00:00|24:00:00|2024-07-01 00:00:00+00|2024-06-30 23:59:59.996|2024-07-01 00:00:00",
		"12|2024-06-30 12:00:00|12:00:00.00001|2024-06-30 12:00:00+00|2024-06-30 12:00:00.005|2024-06-30 12:00:00")
	h.exec(`UPDATE it_prec SET ts2 = '2024-05-05 05:05:05.555', t5 = '05:05:05.555555', tz1 = tz1 + INTERVAL '0.06 second',
		ts1 = ts4 WHERE id = 1`)
	h.expectRows(`SELECT id, CAST(ts2 AS TEXT), CAST(t5 AS TEXT), CAST(tz1 AS TEXT), CAST(ts1 AS TEXT) FROM it_prec WHERE id = 1`,
		"1|2024-05-05 05:05:05.56|05:05:05.55556|2024-01-10 10:00:00.3+00|2024-01-10 10:00:00.1")
	h.exec(`MERGE INTO it_prec t USING it_prec_src s ON t.id = s.id + 10
		WHEN MATCHED AND s.id = 1 THEN UPDATE SET ts2 = s.ts - INTERVAL '0.006 second', tz1 = s.tz - INTERVAL '0.1 second'
		WHEN MATCHED THEN DELETE`)
	h.exec(`MERGE INTO it_prec t USING it_prec_src s ON t.id = s.id + 20
		WHEN NOT MATCHED THEN INSERT (id, ts2, t5) VALUES (s.id + 20, s.ts, s.t)`)
	h.expectRows(`SELECT id, CAST(ts2 AS TEXT), CAST(tz1 AS TEXT), CAST(t5 AS TEXT) FROM it_prec WHERE id > 10 ORDER BY id`,
		"11|2024-06-30 23:59:59.99|2024-06-30 23:59:59.9+00|24:00:00",
		"21|2024-07-01 00:00:00|NULL|24:00:00",
		"22|2024-06-30 12:00:00|NULL|12:00:00.00001")

	// CTAS and views: a column or a cast keeps its precision. A computed
	// value has the unit's digits (Postgres: none, which is 6).
	h.exec(`CREATE TABLE it_prec_ctas AS SELECT s.id, ts2, CAST(ts AS TIMESTAMP(1)) AS c1, CAST(tz AS TIMESTAMPTZ(2)) AS c2,
		CAST(t AS TIME(4)) AS c4, ts2 + INTERVAL '1 second' AS plus FROM it_prec_src s JOIN it_prec p ON p.id = s.id`)
	h.expectRows(`SELECT column_name, data_type, datetime_precision FROM information_schema.columns
		WHERE table_name = 'it_prec_ctas' AND column_name <> 'id' ORDER BY ordinal_position`,
		"ts2|TIMESTAMP(2)|2", "c1|TIMESTAMP(1)|1", "c2|TIMESTAMP(2) WITH TIME ZONE|2", "c4|TIME(4)|4", "plus|TIMESTAMP(3)|3")
	h.expectRows(`SELECT id, CAST(ts2 AS TEXT), CAST(c1 AS TEXT), CAST(c2 AS TEXT), CAST(c4 AS TEXT), CAST(plus AS TEXT)
		FROM it_prec_ctas ORDER BY id`,
		"1|2024-05-05 05:05:05.56|2024-07-01 00:00:00|2024-06-30 23:59:59.95+00|24:00:00|2024-05-05 05:05:06.56",
		"2|2024-01-10 10:00:00.13|2024-06-30 12:00:00|2024-06-30 12:00:00.05+00|12:00:00|2024-01-10 10:00:01.13")
	h.expectRows(`SELECT id FROM it_prec_ctas WHERE c2 = '2024-06-30 12:00:00.05+00'`, "2")
	h.exec(`CREATE VIEW it_prec_v AS SELECT id, ts2, t5, CAST(ts4 AS TIMESTAMP(1)) AS v1 FROM it_prec`)
	h.expectRows(`SELECT column_name, data_type, datetime_precision FROM information_schema.columns
		WHERE table_name = 'it_prec_v' AND column_name <> 'id' ORDER BY ordinal_position`,
		"ts2|TIMESTAMP(2)|2", "t5|TIME(5)|5", "v1|TIMESTAMP(1)|1")
	h.expectRows(`SELECT CAST(v1 AS TEXT) FROM it_prec_v WHERE id = 2`, "2024-01-10 10:00:00.1")

	// Defaults are rounded, and CURRENT_TIMESTAMP(0) is whole seconds.
	h.exec(`CREATE TABLE it_prec_def (id INTEGER, d2 TIMESTAMP(2) DEFAULT '2024-01-10 10:00:00.125',
		c0 TIMESTAMPTZ(3) DEFAULT CURRENT_TIMESTAMP(0), l1 TIME(1) DEFAULT LOCALTIME(1))`)
	h.exec(`INSERT INTO it_prec_def (id) VALUES (1)`)
	h.expectRows(`SELECT CAST(d2 AS TEXT), EXTRACT(microseconds FROM c0) % 1000000, EXTRACT(microseconds FROM l1) % 100000
		FROM it_prec_def`, "2024-01-10 10:00:00.13|0|0")
	h.expectRows(`SELECT EXTRACT(microseconds FROM CURRENT_TIMESTAMP(0)) % 1000000, CURRENT_TIMESTAMP(0) = CAST(NOW() AS TIMESTAMPTZ(0)),
		CURRENT_TIMESTAMP(2) = CAST(CURRENT_TIMESTAMP AS TIMESTAMPTZ(2)), LOCALTIMESTAMP(1) = CAST(LOCALTIMESTAMP AS TIMESTAMP(1)),
		CURRENT_TIMESTAMP(6) = CURRENT_TIMESTAMP`, "0|true|true|true|true")
	if _, schema := h.query(`SELECT CURRENT_TIMESTAMP(0), LOCALTIMESTAMP(2), CURRENT_TIME(5)`); schema.Field(0).Type.String() != "timestamp[s, tz=UTC]" ||
		schema.Field(1).Type.String() != "timestamp[ms]" || schema.Field(2).Type.String() != "time64[us]" {
		t.Errorf("Arrow types of the current times: %s", schema)
	}

	// Bulk ingest rounds Arrow nanoseconds.
	h.exec(`CREATE TABLE it_prec_ing (id BIGINT, ts2 TIMESTAMP(2), tz1 TIMESTAMPTZ(1), t5 TIME(5))`)
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
	tsb.AppendValues([]arrow.Timestamp{ns("2024-01-10T10:00:00.124999999Z"), ns("2024-12-31T23:59:59.995Z")}, nil)
	tzb.AppendValues([]arrow.Timestamp{ns("2024-01-10T10:00:00.15Z"), ns("2024-01-10T10:00:00.149999999Z")}, nil)
	tb.AppendValues([]arrow.Time64{arrow.Time64(10*3600e9 + 123_455_000), arrow.Time64(86399e9 + 999_995_000)}, nil)
	cols := []arrow.Array{ib.NewArray(), tsb.NewArray(), tzb.NewArray(), tb.NewArray()}
	for _, c := range cols {
		defer c.Release()
	}
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ts2", Type: cols[1].DataType()},
		{Name: "tz1", Type: cols[2].DataType()},
		{Name: "t5", Type: cols[3].DataType()},
	}, nil), cols, 2)
	defer rec.Release()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	for k, v := range map[string]string{adbc.OptionKeyIngestTargetTable: "it_prec_ing", adbc.OptionKeyIngestMode: adbc.OptionValueIngestModeAppend} {
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
	// In one step from the nanoseconds: .124999999 is .12.
	h.expectRows(`SELECT id, CAST(ts2 AS TEXT), CAST(tz1 AS TEXT), CAST(t5 AS TEXT) FROM it_prec_ing ORDER BY id`,
		"1|2024-01-10 10:00:00.12|2024-01-10 10:00:00.2+00|10:00:00.12346",
		"2|2025-01-01 00:00:00|2024-01-10 10:00:00.1+00|24:00:00")
	h.expectRows(`SELECT id FROM it_prec_ing WHERE ts2 = '2024-01-10 10:00:00.12' OR tz1 = '2024-01-10 10:00:00.1+00' ORDER BY id`, "1", "2")

	// Errors: a negative p, as in Postgres, and a current time's p that
	// isn't an integer constant from 0 to 9.
	for sql, want := range map[string]string{
		`CREATE TABLE it_prec_bad (ts TIMESTAMP(-1))`:             "TIMESTAMP(-1) precision must not be negative",
		`SELECT CAST('10:00' AS TIME(-1))`:                        "TIME(-1) precision must not be negative",
		`SELECT CAST('2024-01-10' AS TIMESTAMPTZ(-1))`:            "TIMESTAMP(-1) WITH TIME ZONE precision must not be negative",
		`SELECT CURRENT_TIMESTAMP(10)`:                            "CURRENT_TIMESTAMP precision must be an integer constant from 0 to 9",
		`SELECT LOCALTIME(id) FROM it_prec`:                       "LOCALTIME precision must be an integer constant from 0 to 9",
		`CREATE TABLE it_prec_bad (ts TIMESTAMP DEFAULT NOW(-1))`: "NOW precision must be an integer constant from 0 to 9",
	} {
		h.expectErrorText(sql, want)
	}
}

// columnDigits lists GetObjects' columns of a table as
// "name type decimal_digits" (NULL for none).
func (h *sqlHarness) columnDigits(table string) []string {
	h.t.Helper()
	rdr, err := h.conn.GetObjects(h.ctx, adbc.ObjectDepthColumns, nil, nil, &table, nil, nil)
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
				Schemas []struct {
					Tables []struct {
						Name    string `json:"table_name"`
						Columns []struct {
							Name   string `json:"column_name"`
							Type   string `json:"xdbc_type_name"`
							Digits *int16 `json:"xdbc_decimal_digits"`
						} `json:"table_columns"`
					} `json:"db_schema_tables"`
				} `json:"catalog_db_schemas"`
			}
			if err := dec.Decode(&cat); err != nil {
				h.t.Fatal(err)
			}
			for _, s := range cat.Schemas {
				for _, t := range s.Tables {
					if t.Name != table {
						continue
					}
					for _, c := range t.Columns {
						digits := "NULL"
						if c.Digits != nil {
							digits = fmt.Sprint(*c.Digits)
						}
						out = append(out, fmt.Sprintf("%s %s %s", c.Name, c.Type, digits))
					}
				}
			}
		}
	}
	return out
}
