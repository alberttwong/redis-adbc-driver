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

// Integration tests for the range of date/time arithmetic: results past the
// range of their type are errors, as in Postgres, rather than wrapping.

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// A DATE is an Arrow date32: days since 1970-01-01 from -2^31
// (-5877641-06-23) to 2^31 - 1 (5881580-07-11). A DATE past that is "date
// out of range", from date ± integer, MAKE_DATE, DATEADD, the other date
// functions and CAST.
func TestDateRange(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() { h.exec("DROP TABLE IF EXISTS it_date_range") }
	drop()
	t.Cleanup(drop)

	// date ± integer: the first and last date32 days.
	schema := h.expectRows(`SELECT DATE '1970-01-01' + 2147483647, DATE '1970-01-01' - 2147483648,
			2147483647 + DATE '1970-01-01', DATE '2024-01-01' + 2147463924, DATE '2024-01-01' - 2147503371,
			DATE '2024-01-01' + CAST(1 AS SMALLINT), DATE '2024-01-01' - CAST(-1 AS INTEGER)`,
		"5881580-07-11|-5877641-06-23|5881580-07-11|5881580-07-11|-5877641-06-23|2024-01-02|2024-01-02")
	expectTypes(t, schema, sfDate, sfDate, sfDate, sfDate, sfDate, sfDate, sfDate)
	// The day counts, the difference of the two (date - date can't
	// overflow), and as text.
	h.expectRows(`SELECT (DATE '1970-01-01' + 2147483647) - DATE '1970-01-01',
			(DATE '1970-01-01' - 2147483648) - DATE '1970-01-01',
			(DATE '1970-01-01' + 2147483647) - (DATE '1970-01-01' - 2147483648),
			CAST(DATE '1970-01-01' + 2147483647 AS VARCHAR), CAST(DATE '1970-01-01' - 2147483648 AS VARCHAR)`,
		"2147483647|-2147483648|4294967295|5881580-07-11|-5877641-06-23")
	// A day past either end is out of range: the result must not wrap
	// (2147483647 days after 2024-01-01 was -5877587-06-21), nor overflow
	// int64 (9223372036854775807 days after it was 2023-12-31).
	for _, sql := range []string{
		`SELECT DATE '1970-01-01' + 2147483648`,
		`SELECT DATE '1970-01-01' - 2147483649`,
		`SELECT 2147483648 + DATE '1970-01-01'`,
		`SELECT DATE '2024-01-01' + 2147463925`,
		`SELECT DATE '2024-01-01' - 2147503372`,
		`SELECT DATE '2024-01-01' + 2147483647`,
		`SELECT DATE '2024-01-01' + 10000000000`,
		`SELECT DATE '2024-01-01' - 10000000000`,
		`SELECT DATE '2024-01-01' + 9223372036854775807`,
		`SELECT 9223372036854775807 + DATE '2024-01-01'`,
		`SELECT DATE '2024-01-01' - 9223372036854775807`,
		`SELECT DATE '2024-01-01' - (-9223372036854775807 - 1)`,
		`SELECT CAST(DATE '2024-01-01' + 2147483647 AS VARCHAR)`,
		`SELECT (DATE '1970-01-01' + 2147483647) + 1`,
	} {
		h.expectError(sql, "date out of range")
	}

	// The same for rows evaluated by the driver.
	h.exec(`CREATE TABLE it_date_range (id INTEGER, d DATE, n BIGINT)`)
	h.exec(`INSERT INTO it_date_range VALUES (1, DATE '2024-01-01', 2147463924), (2, DATE '2024-01-01', 2147463925),
		(3, DATE '2024-01-01', -2147503371), (4, DATE '2024-01-01', -2147503372), (5, NULL, 1), (6, DATE '2024-01-01', NULL)`)
	h.expectRows(`SELECT id, d + n FROM it_date_range WHERE id IN (1, 3, 5, 6) ORDER BY id`,
		"1|5881580-07-11", "3|-5877641-06-23", "5|NULL", "6|NULL")
	h.expectError(`SELECT d + n FROM it_date_range WHERE id = 2`, "date out of range")
	h.expectError(`SELECT d + n FROM it_date_range WHERE id = 4`, "date out of range")
	h.expectError(`SELECT id FROM it_date_range WHERE d + n > d`, "date out of range")

	// DATEADD (it adds an interval, so a day count is int32).
	h.expectRows(`SELECT DATEADD(day, 2147483647, DATE '1970-01-01'), DATEADD(day, -2147483648, DATE '1970-01-01'),
			DATEADD(day, 2147463924, DATE '2024-01-01')`,
		"5881580-07-11|-5877641-06-23|5881580-07-11")
	h.expectError(`SELECT DATEADD(day, 2147463925, DATE '2024-01-01')`, "DATEADD: date out of range")
	h.expectError(`SELECT DATEADD(day, -1, DATEADD(day, -2147483648, DATE '1970-01-01'))`, "DATEADD: date out of range")
	h.expectRows(`SELECT DATE_SUB(DATE '1970-01-01', INTERVAL 2147483648 DAY)`, "-5877641-06-23")
	h.expectError(`SELECT DATE_SUB(DATE '1969-12-31', INTERVAL 2147483648 DAY)`, "DATE_SUB: date out of range")

	// MAKE_DATE: the first and last days, then past them.
	schema = h.expectRows(`SELECT MAKE_DATE(5881580, 7, 11), MAKE_DATE(-5877641, 6, 23), MAKE_DATE(5000000, 1, 1)`,
		"5881580-07-11|-5877641-06-23|5000000-01-01")
	expectTypes(t, schema, sfDate, sfDate, sfDate)
	for sql, want := range map[string]string{
		// Was -1759223-12-12.
		`SELECT MAKE_DATE(9999999, 1, 1)`:             "date out of range: 9999999-01-01",
		`SELECT MAKE_DATE(5881580, 7, 12)`:            "date out of range: 5881580-07-12",
		`SELECT MAKE_DATE(5881581, 1, 1)`:             "date out of range: 5881581-01-01",
		`SELECT MAKE_DATE(-5877641, 6, 22)`:           "date out of range: -5877641-06-22",
		`SELECT MAKE_DATE(2147483647, 12, 31)`:        "date out of range: 2147483647-12-31",
		`SELECT MAKE_DATE(-2147483648, 1, 1)`:         "date out of range: -2147483648-01-01",
		`SELECT MAKE_DATE(300000000000, 1, 1)`:        "date out of range: 300000000000-01-01",
		`SELECT MAKE_DATE(9223372036854775807, 1, 1)`: "date out of range: 9223372036854775807-01-01",
		// Invalid fields are still reported as such.
		`SELECT MAKE_DATE(2023, 2, 29)`:         "date field value out of range: 2023-02-29",
		`SELECT MAKE_DATE(2024, 13, 1)`:         "date field value out of range: 2024-13-01",
		`SELECT MAKE_DATE(9999999, 13, 1)`:      "date field value out of range: 9999999-13-01",
		`SELECT MAKE_DATE(2024, 4294967297, 1)`: "date field value out of range: 2024-4294967297-01",
	} {
		h.expectError(sql, want)
	}

	// Other functions that return a DATE.
	h.expectRows(`SELECT LAST_DAY(DATE '1970-01-01' - 2147483648), DATE_TRUNC('month', DATE '1970-01-01' + 2147483647)`,
		"-5877641-06-30|5881580-07-01")
	h.expectError(`SELECT LAST_DAY(DATE '1970-01-01' + 2147483647)`, "date out of range")

	// CAST to DATE of a timestamp in seconds, which goes further than a
	// date32; TRY_CAST makes it NULL.
	const lastSecondDay = `CAST(CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP(0)) + INTERVAL '2147483647 days' AS DATE)`
	h.expectRows(`SELECT `+lastSecondDay, "5881580-07-11")
	const pastLast = `CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP(0)) + INTERVAL '2147483647 days' + INTERVAL '1 day'`
	h.expectError(`SELECT CAST(`+pastLast+` AS DATE)`, "date out of range")
	h.expectRows(`SELECT TRY_CAST(`+pastLast+` AS DATE), CAST(`+pastLast+` AS VARCHAR)`, "NULL|5881580-07-12 00:00:00")

	// A bound Arrow date64 past the range.
	dayMS := int64(86_400_000)
	if got, err := queryBoundDate64(h, 2147483647*dayMS); err != nil || got != "5881580-07-11" {
		t.Errorf("date64 of the last date32 day: got %q, %v", got, err)
	}
	if got, err := queryBoundDate64(h, -2147483648*dayMS); err != nil || got != "-5877641-06-23" {
		t.Errorf("date64 of the first date32 day: got %q, %v", got, err)
	}
	for _, ms := range []int64{2147483648 * dayMS, -2147483649 * dayMS} {
		if got, err := queryBoundDate64(h, ms); err == nil || !strings.Contains(err.Error(), "date out of range") {
			t.Errorf("date64 %d: got %q, %v; want date out of range", ms, got, err)
		}
	}
}

// queryBoundDate64 runs SELECT CAST(? AS VARCHAR) with a date64 parameter.
func queryBoundDate64(h *sqlHarness, ms int64) (string, error) {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, `SELECT CAST(? AS VARCHAR)`); err != nil {
		h.t.Fatal(err)
	}
	b := array.NewDate64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(arrow.Date64(ms))
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "d", Type: arrow.FixedWidthTypes.Date64}}, nil),
		[]arrow.Array{arr}, 1)
	defer rec.Release()
	if err := st.Bind(h.ctx, rec); err != nil {
		return "", err
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		return "", err
	}
	defer rdr.Release()
	if !rdr.Next() {
		return "", rdr.Err()
	}
	return rdr.RecordBatch().Column(0).ValueStr(0), nil
}

// timestamp ± interval, interval ± interval, interval * n and time ±
// interval: past the range of a timestamp it is "timestamp out of range",
// and an interval whose nanoseconds would pass int64 is "interval out of
// range", rather than wrapping (or saturating, depending on the platform).
func TestIntervalArithmeticRange(t *testing.T) {
	h := newSQLHarness(t)

	// In range: the largest whole hours of a nanosecond interval, either
	// way round.
	h.expectRows(`SELECT CAST(TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '2562047 hours' AS VARCHAR),
			CAST(TIMESTAMP '2024-01-01 00:00:00' - INTERVAL '-2562047 hours' AS VARCHAR),
			CAST(INTERVAL '2000000 hours' + INTERVAL '562047 hours' AS VARCHAR),
			CAST(INTERVAL '-2000000 hours' - INTERVAL '562047 hours' AS VARCHAR),
			CAST(INTERVAL '1000000 hours' * 2.5 AS VARCHAR),
			CAST(TIMESTAMP '2024-03-31 00:00:00' - INTERVAL '1 month' AS VARCHAR)`,
		"2316-04-11 23:00:00.000000|2316-04-11 23:00:00.000000|2562047:00:00|-2562047:00:00|2500000:00:00|2024-02-29 00:00:00.000000")

	for sql, want := range map[string]string{
		// Past int64 nanoseconds: was 2316-04-11 23:47:16.854775 (saturated).
		`SELECT INTERVAL '3000000 hours'`:                                   "interval out of range",
		`SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '3000000 hours'`: "interval out of range",
		`SELECT INTERVAL '2000000 hours' * 3`:                               "interval out of range",
		// Was -1124095:34:33.709551616, and 1895-10-06 added to a timestamp.
		`SELECT INTERVAL '2000000 hours' + INTERVAL '2000000 hours'`:                                     "interval out of range",
		`SELECT INTERVAL '-2000000 hours' - INTERVAL '2000000 hours'`:                                    "interval out of range",
		`SELECT TIMESTAMP '2024-01-01 00:00:00' + (INTERVAL '2000000 hours' + INTERVAL '2000000 hours')`: "interval out of range",
		// -2^63 nanoseconds can't be negated: was 1731-09-22 00:12:43.145224.
		`SELECT TIMESTAMP '2024-01-01 00:00:00' - INTERVAL '-9223372036.854775808 seconds'`: "interval out of range",
		// Past the range of a timestamp.
		`SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '300000 years'`:                    "timestamp out of range",
		`SELECT TIMESTAMP '2024-01-01 00:00:00' - INTERVAL '300000 years'`:                    "timestamp out of range",
		`SELECT DATE '2024-01-01' + INTERVAL '300000 years'`:                                  "timestamp out of range",
		`SELECT CAST(TIMESTAMP '2024-01-01 00:00:00' AS TIMESTAMP(9)) + INTERVAL '300 years'`: "timestamp out of range",
	} {
		h.expectError(sql, want)
	}

	// time ± interval wraps around midnight, also for intervals near the
	// int64 limit (10:00 + 2^63 - 808 ns, and - -2^63 ns, are 09:47:16.854775
	// to the microsecond; adding them used to overflow).
	h.expectRows(`SELECT CAST(TIME '10:00:00' + (INTERVAL '2562047 hours' + INTERVAL '47 minutes' + INTERVAL '16.854775 seconds') AS VARCHAR),
			CAST(TIME '10:00:00' - INTERVAL '-9223372036.854775808 seconds' AS VARCHAR),
			CAST(TIME '10:00:00' - INTERVAL '2562047 hours' AS VARCHAR)`,
		"09:47:16.854775|09:47:16.854775|11:00:00.000000")
}
