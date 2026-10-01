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
// CONVERSION ERROR, the % operator (which is MOD) and EXP underflow.

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
