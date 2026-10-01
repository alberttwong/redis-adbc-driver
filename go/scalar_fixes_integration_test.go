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

// Integration tests for TRY_CAST / SAFE_CAST / CAST … DEFAULT … ON
// CONVERSION ERROR, the % operator (which is MOD), EXP underflow, and
// DATEADD / DATEDIFF with bare date parts.

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// expectTypes checks the Arrow types of a result's columns.
func expectTypes(t *testing.T, schema *arrow.Schema, want ...arrow.DataType) {
	t.Helper()
	if len(schema.Fields()) != len(want) {
		t.Fatalf("result has %d columns, want %d", len(schema.Fields()), len(want))
	}
	for i, w := range want {
		if got := schema.Field(i).Type; !arrow.TypeEqual(got, w) {
			t.Errorf("column %d (%s) type = %s, want %s", i, schema.Field(i).Name, got, w)
		}
	}
}

var (
	sfInt16  = arrow.PrimitiveTypes.Int16
	sfInt32  = arrow.PrimitiveTypes.Int32
	sfInt64  = arrow.PrimitiveTypes.Int64
	sfDouble = arrow.PrimitiveTypes.Float64
	sfBool   = arrow.FixedWidthTypes.Boolean
	sfString = arrow.BinaryTypes.String
	sfDate   = arrow.FixedWidthTypes.Date32
	sfTS     = &arrow.TimestampType{Unit: arrow.Microsecond}
	sfTSTZ   = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	sfTSms   = &arrow.TimestampType{Unit: arrow.Millisecond}
	sfTime   = &arrow.Time64Type{Unit: arrow.Microsecond}
)

func sfDecimal(p, s int32) arrow.DataType { return &arrow.Decimal128Type{Precision: p, Scale: s} }

// TRY_CAST and SAFE_CAST return NULL where CAST fails to convert the value:
// parse errors, out-of-range values and overflow. The result has the target
// type; errors in the operand itself, and casts between types that never
// convert, are still errors.
func TestScalarFixesTryCast(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_sf_cast_v")
		h.exec("DROP TABLE IF EXISTS it_sf_raw")
	}
	drop()
	t.Cleanup(drop)
	h.exec(`CREATE TABLE it_sf_raw (id INTEGER, s VARCHAR, n BIGINT, d NUMERIC(6,2))`)
	h.exec(`INSERT INTO it_sf_raw VALUES (1, '42', 100000, 1.25), (2, 'x', 7, 9999.99),
		(3, '2024-13-01', -40000, NULL), (4, NULL, NULL, -0.5), (5, ' 12 ', 12, 12.00)`)

	// The examples from the issue.
	schema := h.expectRows(`SELECT TRY_CAST('x' AS INTEGER), TRY_CAST('2024-13-01' AS DATE),
			TRY_CAST(100000 AS SMALLINT), TRY_CAST('1.5' AS NUMERIC(4,2))`,
		"NULL|NULL|NULL|1.50")
	expectTypes(t, schema, sfInt32, sfDate, sfInt16, sfDecimal(4, 2))

	// Row by row over columns; SAFE_CAST is the same function.
	schema = h.expectRows(`SELECT id, TRY_CAST(s AS INTEGER), SAFE_CAST(n AS SMALLINT), TRY_CAST(d AS NUMERIC(3,1)),
			SAFE_CAST(s AS DATE)
		FROM it_sf_raw ORDER BY id`,
		"1|42|NULL|1.3|NULL", "2|NULL|7|NULL|NULL", "3|NULL|NULL|NULL|NULL", "4|NULL|NULL|-0.5|NULL", "5|12|12|12.0|NULL")
	expectTypes(t, schema, sfInt32, sfInt32, sfInt16, sfDecimal(3, 1), sfDate)

	// Other targets: valid values convert, invalid ones become NULL.
	schema = h.expectRows(`SELECT TRY_CAST('2024-02-29' AS DATE), TRY_CAST('2023-02-29' AS DATE),
			TRY_CAST('yes' AS BOOLEAN), TRY_CAST('maybe' AS BOOLEAN), TRY_CAST('2.5' AS DOUBLE),
			TRY_CAST('1e400' AS DOUBLE), CAST(TRY_CAST('3 days' AS INTERVAL) AS VARCHAR), TRY_CAST('soon' AS INTERVAL),
			CAST(TRY_CAST('2024-01-02 03:04:05' AS TIMESTAMP) AS VARCHAR), TRY_CAST('noon' AS TIMESTAMP)`,
		"2024-02-29|NULL|true|NULL|2.5|NULL|3 days|NULL|2024-01-02 03:04:05.000000|NULL")
	expectTypes(t, schema, sfDate, sfDate, sfBool, sfBool, sfDouble, sfDouble, sfString,
		arrow.FixedWidthTypes.MonthDayNanoInterval, sfString, sfTS)

	// Overflow: integers, doubles and NaN to integers and NUMERIC, and
	// timestamps beyond the nanosecond range.
	h.expectRows(`SELECT TRY_CAST('99999999999999999999' AS BIGINT), TRY_CAST(1e300 AS BIGINT),
			TRY_CAST(CAST('NaN' AS DOUBLE) AS NUMERIC(10,2)), TRY_CAST(123.456 AS NUMERIC(4,2)),
			TRY_CAST(TIMESTAMP '2300-01-01 00:00:00' AS TIMESTAMP(9)), TRY_CAST(2147483648 AS INTEGER),
			TRY_CAST(-2147483648 AS INTEGER)`,
		"NULL|NULL|NULL|NULL|NULL|NULL|-2147483648")
	h.expectError(`SELECT CAST(TIMESTAMP '2300-01-01 00:00:00' AS TIMESTAMP(9))`, "out of range")
	h.expectError(`SELECT CAST(123.456 AS NUMERIC(4,2))`, "out of range")

	// NULL stays NULL, with the target type.
	schema = h.expectRows(`SELECT TRY_CAST(NULL AS INTEGER), SAFE_CAST(NULL AS DATE)`, "NULL|NULL")
	expectTypes(t, schema, sfInt32, sfDate)

	// In WHERE, GROUP BY and aggregates.
	h.expectRows(`SELECT id FROM it_sf_raw WHERE TRY_CAST(s AS INTEGER) IS NOT NULL ORDER BY id`, "1", "5")
	h.expectRows(`SELECT id FROM it_sf_raw WHERE TRY_CAST(s AS INTEGER) > 20`, "1")
	schema = h.expectRows(`SELECT SUM(TRY_CAST(s AS INTEGER)), COUNT(TRY_CAST(s AS INTEGER)) FROM it_sf_raw`, "54|2")
	expectTypes(t, schema, sfInt64, sfInt64)
	h.expectRows(`SELECT TRY_CAST(s AS INTEGER) AS si, COUNT(*) FROM it_sf_raw GROUP BY si ORDER BY si`,
		"12|1", "42|1", "NULL|3")

	// CAST(x AS t DEFAULT v ON CONVERSION ERROR): v (converted to t) for the
	// values that fail; NULL is still NULL. v may read the row.
	schema = h.expectRows(`SELECT id, CAST(s AS INTEGER DEFAULT -1 ON CONVERSION ERROR),
			CAST(s AS DATE DEFAULT '2000-01-01' ON CONVERSION ERROR), CAST(s AS INTEGER DEFAULT id * 10 ON CONVERSION ERROR)
		FROM it_sf_raw ORDER BY id`,
		"1|42|2000-01-01|42", "2|-1|2000-01-01|20", "3|-1|2000-01-01|30", "4|NULL|NULL|NULL", "5|12|2000-01-01|12")
	expectTypes(t, schema, sfInt32, sfInt32, sfDate, sfInt32)
	h.expectError(`SELECT CAST('x' AS INTEGER DEFAULT 'y' ON CONVERSION ERROR)`, `invalid decimal "y"`)
	h.expectError(`SELECT CAST('x' AS INTEGER DEFAULT 0)`, "expected ON CONVERSION ERROR")
	h.expectError(`SELECT TRY_CAST('1' AS INTEGER DEFAULT 0 ON CONVERSION ERROR)`, `expected ")"`)

	// Through a view, with outer filters rewritten over the base table.
	h.exec(`CREATE VIEW it_sf_cast_v AS SELECT id, TRY_CAST(s AS INTEGER) AS si,
		CAST(s AS INTEGER DEFAULT id ON CONVERSION ERROR) AS sd FROM it_sf_raw`)
	h.expectRows(`SELECT id, si FROM it_sf_cast_v WHERE si > 20`, "1|42")
	h.expectRows(`SELECT id, sd FROM it_sf_cast_v WHERE sd < 10 ORDER BY id`, "2|2", "3|3")

	// Errors in the operand, and casts between types that never convert,
	// still fail. CAST and :: still fail on bad values.
	h.expectError(`SELECT TRY_CAST(1 / 0 AS INTEGER)`, "division by zero")
	h.expectError(`SELECT TRY_CAST(n / (n - n) AS INTEGER) FROM it_sf_raw`, "division by zero")
	h.expectError(`SELECT TRY_CAST(DATE '2024-01-01' AS BOOLEAN)`, "cannot convert")
	h.expectError(`SELECT SAFE_CAST(id AS DATE) FROM it_sf_raw`, "cannot convert")
	h.expectError(`SELECT CAST('x' AS INTEGER)`, `invalid decimal "x"`)
	h.expectError(`SELECT 'x'::INTEGER`, `invalid decimal "x"`)
	h.expectError(`SELECT CAST(100000 AS SMALLINT)`, "out of range")
}

// a % b is MOD(a, b): the same result types (NUMERIC stays exact) and a
// division-by-zero error for every numeric type.
func TestScalarFixesModOperator(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() { h.exec("DROP TABLE IF EXISTS it_sf_mod") }
	drop()
	t.Cleanup(drop)
	h.exec(`CREATE TABLE it_sf_mod (id INTEGER, n NUMERIC(6,2), v DOUBLE, i INTEGER, b BIGINT, s SMALLINT)`)
	h.exec(`INSERT INTO it_sf_mod VALUES (1, 7.25, 7.5, 7, -7, 5), (2, -7.25, -7.5, -7, 7, -5),
		(3, NULL, NULL, NULL, NULL, NULL)`)

	// The values from the issue, side by side with MOD.
	schema := h.expectRows(`SELECT n % 2, MOD(n, 2), v % 2, MOD(v, 2), i % 2, MOD(i, 2), b % 3, MOD(b, 3),
			s % 3, MOD(s, 3), n % i, MOD(n, i), n % 0.5, MOD(n, 0.5), i % 2.5, MOD(i, 2.5)
		FROM it_sf_mod ORDER BY id`,
		"1.25|1.25|1.5|1.5|1|1|-1|-1|2|2|0.25|0.25|0.25|0.25|2.0|2.0",
		"-1.25|-1.25|-1.5|-1.5|-1|-1|1|1|-2|-2|-0.25|-0.25|-0.25|-0.25|-2.0|-2.0",
		"NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL")
	expectTypes(t, schema, sfDecimal(6, 2), sfDecimal(6, 2), sfDouble, sfDouble, sfInt32, sfInt32, sfInt64, sfInt64,
		sfInt16, sfInt16, sfDecimal(38, 2), sfDecimal(38, 2), sfDecimal(6, 2), sfDecimal(6, 2),
		sfDecimal(38, 1), sfDecimal(38, 1))

	// Literals, NULL and precedence (% binds like * and /).
	schema = h.expectRows(`SELECT 7 % 3, -7 % 3, 7.5 % 2, 7.5e0 % 2, 7 % 3 * 2, 2 * 7 % 3, 7 + 5 % 3, 7 % NULL,
			-9223372036854775808 % -1`,
		"1|-1|1.5|1.5|2|2|9|NULL|0")
	expectTypes(t, schema, sfInt64, sfInt64, sfDecimal(2, 1), sfDouble, sfInt64, sfInt64, sfInt64, sfInt64, sfInt64)

	// In WHERE: over a column (evaluated by the driver) and of constants
	// (computed once, then an index query).
	h.expectRows(`SELECT id FROM it_sf_mod WHERE i % 2 = 1`, "1")
	h.expectRows(`SELECT id FROM it_sf_mod WHERE n % 2 = -1.25`, "2")
	h.expectRows(`SELECT id FROM it_sf_mod WHERE i = 15 % 8`, "1")
	h.expectRows(`SELECT i % 2 AS k, COUNT(*) FROM it_sf_mod GROUP BY i % 2 ORDER BY k`, "-1|1", "1|1", "NULL|1")

	// A zero divisor is an error for every numeric type, as with MOD.
	for _, sql := range []string{
		`SELECT n % 0 FROM it_sf_mod`, `SELECT v % 0 FROM it_sf_mod`, `SELECT i % 0 FROM it_sf_mod`,
		`SELECT b % (b - b) FROM it_sf_mod`, `SELECT n % 0.00 FROM it_sf_mod`, `SELECT v % 0e0 FROM it_sf_mod`,
		`SELECT 7.25 % 0`, `SELECT 7.5e0 % 0`, `SELECT 7 % 0`,
	} {
		h.expectError(sql, "division by zero")
	}
	h.expectError(`SELECT DATE '2024-01-01' % 2`, "MOD expects a number")
}

// EXP of a double raises "underflow" where the result is too small, as
// "overflow" where it is too large (Postgres); the exp of a NUMERIC is then 0.
func TestScalarFixesExpUnderflow(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() { h.exec("DROP TABLE IF EXISTS it_sf_exp") }
	drop()
	t.Cleanup(drop)
	h.exec(`CREATE TABLE it_sf_exp (id INTEGER, n NUMERIC(10,1), v DOUBLE)`)
	h.exec(`INSERT INTO it_sf_exp VALUES (1, -1000.0, -1000), (2, 0.0, 0), (3, NULL, NULL)`)

	schema := h.expectRows(`SELECT EXP(0), EXP(-1000.0), EXP(CAST(-1000 AS NUMERIC(10,0))),
			EXP(CAST('-Infinity' AS DOUBLE)), EXP(-700)`,
		"1|0|0|0|9.85967654375977e-305")
	expectTypes(t, schema, sfDouble, sfDouble, sfDouble, sfDouble, sfDouble)
	h.expectRows(`SELECT id, EXP(n) FROM it_sf_exp ORDER BY id`, "1|0", "2|1", "3|NULL")
	h.expectRows(`SELECT id, EXP(v) FROM it_sf_exp WHERE id > 1 ORDER BY id`, "2|1", "3|NULL")

	for _, sql := range []string{
		`SELECT EXP(-1000)`, `SELECT EXP(-1000e0)`, `SELECT EXP(CAST(-1000 AS DOUBLE))`,
		`SELECT EXP(CAST(-1000 AS INTEGER))`, `SELECT EXP('-1000')`, `SELECT EXP(v) FROM it_sf_exp`,
	} {
		h.expectError(sql, "EXP: value out of range: underflow")
	}
	h.expectError(`SELECT EXP(1000)`, "EXP: value out of range: overflow")
}

// DATEADD / DATEDIFF (Snowflake, Redshift, SQL Server, and dbt's
// cross-database macros), with the part as a bare keyword or a string, and
// their aliases.
func TestScalarFixesDateAddDiff(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() { h.exec("DROP TABLE IF EXISTS it_sf_dates") }
	drop()
	t.Cleanup(drop)
	// Columns named day and d, like date parts.
	h.exec(`CREATE TABLE it_sf_dates (id INTEGER, d DATE, ts TIMESTAMP(6), tz TIMESTAMP(3), t TIME(6), day INTEGER)`)
	h.exec(`INSERT INTO it_sf_dates VALUES
		(1, DATE '2024-01-31', TIMESTAMP '2024-01-31 23:30:00.5', TIMESTAMP '2024-02-29 12:00:00.250', TIME '23:30:00', 2),
		(2, DATE '2023-12-31', TIMESTAMP '2023-12-31 00:00:00', TIMESTAMP '2023-03-31 00:00:00', TIME '00:15:00', -3),
		(3, NULL, NULL, NULL, NULL, NULL)`)

	// The examples from the issue.
	schema := h.expectRows(`SELECT DATEADD(day, 1, DATE '2024-01-01'), DATEDIFF(day, DATE '2024-01-01', DATE '2024-01-31'),
			DATE_ADD('day', 1, DATE '2024-01-01')`,
		"2024-01-02|30|2024-01-02")
	expectTypes(t, schema, sfDate, sfInt64, sfDate)

	// A date stays a date for parts of a day or longer; months clamp to the
	// end of the month.
	schema = h.expectRows(`SELECT DATEADD(year, 1, d), DATEADD(quarter, 1, d), DATEADD(month, 1, d),
			DATEADD(week, 1, d), DATEADD(day, -1, d), DATEADD(month, -10, d), DATEADD('Year', -1, DATE '2024-02-29')
		FROM it_sf_dates ORDER BY id`,
		"2025-01-31|2024-04-30|2024-02-29|2024-02-07|2024-01-30|2023-03-31|2023-02-28",
		"2024-12-31|2024-03-31|2024-01-31|2024-01-07|2023-12-30|2023-02-28|2023-02-28",
		"NULL|NULL|NULL|NULL|NULL|NULL|2023-02-28")
	expectTypes(t, schema, sfDate, sfDate, sfDate, sfDate, sfDate, sfDate, sfDate)

	// A smaller part makes a date a timestamp.
	schema = h.expectRows(`SELECT DATEADD(hour, 25, d), DATEADD(minute, -1, d), DATEADD(second, 90, d),
			DATEADD(millisecond, 1500, d), DATEADD(microsecond, 1, d)
		FROM it_sf_dates WHERE id = 1`,
		"2024-02-01T01:00:00|2024-01-30T23:59:00|2024-01-31T00:01:30|2024-01-31T00:00:01.5|2024-01-31T00:00:00.000001")
	expectTypes(t, schema, sfTS, sfTS, sfTS, sfTS, sfTS)

	// Timestamps keep their type (unit and time zone).
	schema = h.expectRows(`SELECT CAST(DATEADD(month, 1, ts) AS VARCHAR), CAST(DATEADD(hour, 1, ts) AS VARCHAR),
			CAST(DATEADD(microsecond, -500000, ts) AS VARCHAR), CAST(DATEADD(year, -1, tz) AS VARCHAR),
			DATEADD(day, 1, ts), DATEADD(quarter, 1, tz),
			DATEADD(day, 1, TIMESTAMP WITH TIME ZONE '2024-01-01 00:00:00+00')
		FROM it_sf_dates WHERE id = 1`,
		"2024-02-29 23:30:00.500000|2024-02-01 00:30:00.500000|2024-01-31 23:30:00.000000|2023-02-28 12:00:00.250|"+
			"2024-02-01T23:30:00.5|2024-05-29T12:00:00.25|2024-01-02T00:00:00Z")
	expectTypes(t, schema, sfString, sfString, sfString, sfString, sfTS, sfTSms, sfTSTZ)

	// Times wrap around midnight; date parts are not valid for them.
	schema = h.expectRows(`SELECT CAST(DATEADD(hour, 25, t) AS VARCHAR), CAST(DATEADD(minute, -30, t) AS VARCHAR), DATEADD(second, 1, t)
		FROM it_sf_dates ORDER BY id`,
		"00:30:00.000000|23:00:00.000000|23:30:01.000000", "01:15:00.000000|23:45:00.000000|00:15:01.000000", "NULL|NULL|NULL")
	expectTypes(t, schema, sfString, sfString, sfTime)
	h.expectError(`SELECT DATEADD(day, 1, t) FROM it_sf_dates`, "date part day is not valid for TIME values")

	// Text is read as a timestamp.
	schema = h.expectRows(`SELECT CAST(DATEADD(day, 1, '2024-01-01') AS VARCHAR), DATEADD(hour, 1, '2024-01-01 10:00:00')`,
		"2024-01-02 00:00:00.000000|2024-01-01T11:00:00")
	expectTypes(t, schema, sfString, sfTS)

	// Abbreviations, bare or quoted, in any case.
	h.expectRows(`SELECT DATEADD(yy, 1, d), DATEADD(YYYY, 1, d), DATEADD(qq, 1, d), DATEADD(q, 1, d), DATEADD(mm, 1, d),
			DATEADD(mon, 1, d), DATEADD(wk, 1, d), DATEADD(ww, 1, d), DATEADD(dd, 1, d), DATEADD(d, 1, d),
			DATEADD('DAYS', 1, d), DATEADD('yr', 1, d)
		FROM it_sf_dates WHERE id = 1`,
		"2025-01-31|2025-01-31|2024-04-30|2024-04-30|2024-02-29|2024-02-29|2024-02-07|2024-02-07|2024-02-01|2024-02-01|2024-02-01|2025-01-31")
	h.expectRows(`SELECT CAST(DATEADD(hh, 1, ts) AS VARCHAR), CAST(DATEADD(mi, 1, ts) AS VARCHAR), CAST(DATEADD(n, 1, ts) AS VARCHAR),
			CAST(DATEADD(m, 1, ts) AS VARCHAR), CAST(DATEADD(ss, 1, ts) AS VARCHAR), CAST(DATEADD(s, 1, ts) AS VARCHAR),
			CAST(DATEADD(ms, 1, ts) AS VARCHAR), CAST(DATEADD(us, 1, ts) AS VARCHAR), CAST(DATEADD(mcs, 1, ts) AS VARCHAR)
		FROM it_sf_dates WHERE id = 1`,
		"2024-02-01 00:30:00.500000|2024-01-31 23:31:00.500000|2024-01-31 23:31:00.500000|2024-01-31 23:31:00.500000|"+
			"2024-01-31 23:30:01.500000|2024-01-31 23:30:01.500000|2024-01-31 23:30:00.501000|2024-01-31 23:30:00.500001|"+
			"2024-01-31 23:30:00.500001")

	// Columns named like date parts are still columns everywhere else,
	// including as DATEADD's count and in DATE_ADD(x, INTERVAL …).
	h.expectRows(`SELECT day, DATEADD(day, day, d), DATEDIFF(day, d, DATEADD(day, day, d)), d + day, DATE_PART('day', d)
		FROM it_sf_dates WHERE day > 0`,
		"2|2024-02-02|2|2024-02-02|31")
	h.expectRows(`SELECT id FROM it_sf_dates WHERE DATEADD(day, day, d) < d`, "2")
	h.expectRows(`SELECT DATE_ADD(d, INTERVAL day DAY), DATE_SUB(d, INTERVAL 1 MONTH) FROM it_sf_dates ORDER BY id`,
		"2024-02-02|2023-12-31", "2023-12-28|2023-11-30", "NULL|NULL")
	h.expectRows(`WITH x AS (SELECT DATE '2024-01-31' AS day, 5 AS mm)
		SELECT DATE_ADD(day, INTERVAL 1 MONTH), DATE_SUB(day, INTERVAL mm DAY), DATEADD(mm, mm, day) FROM x`,
		"2024-02-29|2024-01-26|2024-06-30")
	h.expectRows(`SELECT "day" FROM it_sf_dates ORDER BY "day"`, "-3", "2", "NULL")

	// DATEDIFF counts boundaries crossed, exactly like DATE_DIFF.
	schema = h.expectRows(`SELECT DATEDIFF(month, DATE '2024-01-31', DATE '2024-02-01'), DATEDIFF(yy, DATE '2023-12-31', DATE '2024-01-01'),
			DATEDIFF(hh, TIMESTAMP '2024-01-01 01:59:00', TIMESTAMP '2024-01-01 02:01:00'), DATEDIFF(wk, DATE '2024-02-25', DATE '2024-02-26'),
			DATEDIFF(qq, DATE '2024-03-31', DATE '2024-04-01'), DATEDIFF(mi, TIMESTAMP '2024-01-01 00:00:59', TIMESTAMP '2024-01-01 00:01:00'),
			DATEDIFF(ss, TIMESTAMP '2024-01-01 00:00:00.9', TIMESTAMP '2024-01-01 00:00:01.1'),
			DATEDIFF(ms, TIMESTAMP '2024-01-01 00:00:00.0009', TIMESTAMP '2024-01-01 00:00:00.0011'),
			DATEDIFF(us, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:01'),
			DATEDIFF(day, DATE '2024-03-01', DATE '2024-02-01'), DATEDIFF('Day', DATE '2024-02-01', TIMESTAMP '2024-02-02 23:59:59')`,
		"1|1|1|1|1|1|1|1|1000000|-29|1")
	expectTypes(t, schema, sfInt64, sfInt64, sfInt64, sfInt64, sfInt64, sfInt64, sfInt64, sfInt64, sfInt64, sfInt64, sfInt64)
	for _, p := range [][2]string{{"year", "year"}, {"quarter", "quarter"}, {"month", "month"}, {"week", "week"},
		{"day", "day"}, {"hour", "hour"}, {"minute", "minute"}, {"second", "second"}, {"ms", "milliseconds"},
		{"us", "microseconds"}} {
		h.expectRows(`SELECT COUNT(*) FROM it_sf_dates a, it_sf_dates b
			WHERE DATEDIFF(`+p[0]+`, a.ts, b.tz) = DATE_DIFF('`+p[1]+`', a.ts, b.tz)
			AND DATEDIFF(`+p[0]+`, a.d, b.ts) = DATE_DIFF('`+p[1]+`', a.d, b.ts)`, "4")
	}

	// TIMESTAMPADD and TIMESTAMPDIFF are Snowflake's aliases; DATE_ADD also
	// takes the part first (Trino, Databricks).
	schema = h.expectRows(`SELECT TIMESTAMPADD(day, 1, d), TIMESTAMPDIFF(month, d, DATE '2024-03-01'), DATE_ADD(week, 2, d),
			DATE_ADD('hour', 1, d)
		FROM it_sf_dates WHERE id = 1`,
		"2024-02-01|2|2024-02-14|2024-01-31T01:00:00")
	expectTypes(t, schema, sfDate, sfInt64, sfDate, sfTS)

	// DATE_ADD / DATE_SUB(x, INTERVAL n part) (MySQL, BigQuery) are DATEADD
	// with n or -n: dates stay dates for date parts.
	schema = h.expectRows(`SELECT DATE_ADD(DATE '2024-01-31', INTERVAL 1 MONTH), DATE_SUB(DATE '2024-03-01', INTERVAL 1 DAY),
			DATE_ADD(DATE '2024-01-01', INTERVAL '3' DAY), DATE_ADD(DATE '2024-01-01', INTERVAL 2 HOUR),
			DATE_SUB(TIMESTAMP '2024-01-01 00:00:00', INTERVAL 1 SECOND), DATE_SUB(DATE '2024-01-01', INTERVAL -1 YEAR)`,
		"2024-02-29|2024-02-29|2024-01-04|2024-01-01T02:00:00|2023-12-31T23:59:59|2025-01-01")
	expectTypes(t, schema, sfDate, sfDate, sfDate, sfTS, sfTS, sfDate)

	// NULL in any argument gives NULL, with the result type.
	schema = h.expectRows(`SELECT DATEADD(day, NULL, d), DATEADD(day, 1, NULL), DATEDIFF(day, NULL, d), DATEADD(hour, NULL, d)
		FROM it_sf_dates WHERE id = 1`, "NULL|NULL|NULL|NULL")
	expectTypes(t, schema, sfDate, sfTS, sfInt64, sfTS)

	// What dbt_utils.date_spine generates: a window function as the count.
	h.expectRows(`SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY id) - 1, CAST('2024-02-27' AS DATE)) AS date_day
		FROM it_sf_dates ORDER BY date_day`,
		"2024-02-27", "2024-02-28", "2024-02-29")

	// Of constants, DATEADD is computed once and pushed into the index.
	h.expectRows(`SELECT id FROM it_sf_dates WHERE d = DATEADD(day, 1, DATE '2024-01-30')`, "1")
	h.expectRows(`SELECT id FROM it_sf_dates WHERE ts >= DATEADD(month, -1, TIMESTAMP '2024-02-15 00:00:00')`, "1")

	// Errors.
	h.expectError(`SELECT DATEADD('fortnight', 1, d) FROM it_sf_dates`, `DATEADD: unknown date part "fortnight"`)
	h.expectError(`SELECT DATEADD(fortnight, 1, d) FROM it_sf_dates`, "fortnight")
	h.expectError(`SELECT DATEADD(day, 'x', d) FROM it_sf_dates`, "DATEADD: the number of units must be an integer")
	h.expectError(`SELECT DATEADD(day, 1, 5)`, "DATEADD expects a date, time or timestamp")
	h.expectError(`SELECT DATEADD(day, 1)`, "DATEADD expects 3 arguments")
	h.expectError(`SELECT DATEADD(year, 10000000, DATE '2024-01-01')`, "DATEADD: date out of range")
	h.expectError(`SELECT DATEADD(day, 3000000000, DATE '2024-01-01')`, "DATEADD: interval out of range")
	h.expectError(`SELECT DATEADD(year, 300000, TIMESTAMP '2024-01-01 00:00:00')`, "out of range")
	h.expectError(`SELECT DATE_ADD(DATE '2024-01-01', INTERVAL '1 day')`, "DATE_ADD expects (part, n, x) or (x, INTERVAL n part)")
	h.expectError(`SELECT DATE_SUB('day', 1, DATE '2024-01-01')`, "DATE_SUB expects (x, INTERVAL n part)")
}
