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

// Unit tests (no Redis) for rounding to a lower fractional-seconds
// precision (#103).

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// Casts to a lower precision round half away from zero, as Postgres's
// AdjustTimestampForTypmod and AdjustTimeForTypmod do, carrying into the
// next second, minute, day, month or year.
func TestRoundPrecision(t *testing.T) {
	runScalarCases(t, []scalarCase{
		// The issue's table.
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.5' AS TIMESTAMP(0))", "2024-01-10 10:00:01", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.1236' AS TIMESTAMP(3))", "2024-01-10 10:00:00.124", "TIMESTAMP(3)"},
		{"CAST(TIME '10:00:00.5' AS TIME(0))", "10:00:01", "TIME(0)"},
		{"CAST(TIMESTAMPTZ '2024-01-10 23:59:59.9+00' AS TIMESTAMPTZ(0))", "2024-01-11 00:00:00+00", "TIMESTAMP(0) WITH TIME ZONE"},

		// TIMESTAMP, every precision: halves round up, less than a half down.
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.4999' AS TIMESTAMP(0))", "2024-01-10 10:00:00", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.1235' AS TIMESTAMP(3))", "2024-01-10 10:00:00.124", "TIMESTAMP(3)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.1234' AS TIMESTAMP(3))", "2024-01-10 10:00:00.123", "TIMESTAMP(3)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.9995' AS TIMESTAMP(3))", "2024-01-10 10:00:01", "TIMESTAMP(3)"},
		// A TIMESTAMP(9) value (a literal with more digits than 6 is one)
		// rounds half away from zero like any other.
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.1234565' AS TIMESTAMP(6))", "2024-01-10 10:00:00.123457", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.1234564' AS TIMESTAMP)", "2024-01-10 10:00:00.123456", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.999999999' AS TIMESTAMP(0))", "2024-01-10 10:00:01", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.123456789' AS TIMESTAMP(3))", "2024-01-10 10:00:00.123", "TIMESTAMP(3)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.5' AS TIMESTAMP(9))", "2024-01-10 10:00:00.5", "TIMESTAMP(9)"},
		{"CAST(CAST('2024-01-10 10:00:00.5' AS TIMESTAMP(3)) AS TIMESTAMP(6))", "2024-01-10 10:00:00.5", "TIMESTAMP(6)"},
		// A precision between 0, 3, 6 and 9 is the next of those.
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.1236' AS TIMESTAMP(2))", "2024-01-10 10:00:00.124", "TIMESTAMP(3)"},
		// Text and dates. Text with more than 6 digits read into a type of 6
		// or fewer is first read in microseconds as Postgres reads it (a
		// half to even), then rounded.
		{"CAST('2024-01-10 10:00:00.5' AS TIMESTAMP(0))", "2024-01-10 10:00:01", "TIMESTAMP(0)"},
		{"CAST('2024-01-10 10:00:00.1234565' AS TIMESTAMP)", "2024-01-10 10:00:00.123456", "TIMESTAMP(6)"},
		{"CAST('2024-01-10 10:00:00.1234575' AS TIMESTAMP)", "2024-01-10 10:00:00.123458", "TIMESTAMP(6)"},
		{"CAST('2024-01-10 10:00:00.4999995' AS TIMESTAMP(0))", "2024-01-10 10:00:01", "TIMESTAMP(0)"},
		{"'2024-01-10 10:00:00.1234995'::timestamp(3)", "2024-01-10 10:00:00.124", "TIMESTAMP(3)"},
		{"CAST('2024-01-10 10:00:00.1234565' AS TIMESTAMP(9))", "2024-01-10 10:00:00.1234565", "TIMESTAMP(9)"},
		{"CAST('2024-01-10 23:59:59.9999995' AS TIMESTAMP)", "2024-01-11 00:00:00", "TIMESTAMP(6)"},
		{"'2024-01-10 10:00:00.0005'::timestamp(3)", "2024-01-10 10:00:00.001", "TIMESTAMP(3)"},
		{"CAST(DATE '2024-01-10' AS TIMESTAMP(0))", "2024-01-10 00:00:00", "TIMESTAMP(0)"},

		// Carries: to the next minute, hour, day, month, year; leap days.
		{"CAST(TIMESTAMP '2024-01-10 10:59:59.5' AS TIMESTAMP(0))", "2024-01-10 11:00:00", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '2024-01-10 23:59:59.9999995' AS TIMESTAMP(6))", "2024-01-11 00:00:00", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMP '2024-01-31 23:59:59.5' AS TIMESTAMP(0))", "2024-02-01 00:00:00", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '2024-12-31 23:59:59.9995' AS TIMESTAMP(3))", "2025-01-01 00:00:00", "TIMESTAMP(3)"},
		{"CAST(TIMESTAMP '2024-02-28 23:59:59.5' AS TIMESTAMP(0))", "2024-02-29 00:00:00", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '2024-02-29 23:59:59.5' AS TIMESTAMP(0))", "2024-03-01 00:00:00", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '2023-02-28 23:59:59.5' AS TIMESTAMP(0))", "2023-03-01 00:00:00", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '9999-12-31 23:59:59.5' AS TIMESTAMP(0))", "10000-01-01 00:00:00", "TIMESTAMP(0)"},

		// Before 2000-01-01, Postgres's epoch, a half rounds down (away
		// from the epoch); other values round to the nearest.
		{"CAST(TIMESTAMP '2000-01-01 00:00:00.5' AS TIMESTAMP(0))", "2000-01-01 00:00:01", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '1999-12-31 23:59:59.5' AS TIMESTAMP(0))", "1999-12-31 23:59:59", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '1990-06-15 10:00:00.5' AS TIMESTAMP(0))", "1990-06-15 10:00:00", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '1990-06-15 10:00:00.6' AS TIMESTAMP(0))", "1990-06-15 10:00:01", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '1990-06-15 10:00:00.1235' AS TIMESTAMP(3))", "1990-06-15 10:00:00.123", "TIMESTAMP(3)"},
		{"CAST(TIMESTAMP '1969-12-31 23:59:59.5' AS TIMESTAMP(0))", "1969-12-31 23:59:59", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '1969-12-31 23:59:59.7' AS TIMESTAMP(0))", "1970-01-01 00:00:00", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '1969-12-31 23:59:59.2' AS TIMESTAMP(0))", "1969-12-31 23:59:59", "TIMESTAMP(0)"},
		// More than 6 digits outside the nanoseconds' range: microseconds,
		// rounded.
		{"TIMESTAMP '0044-03-15 12:00:00.0000005 BC'", "0044-03-15 12:00:00 BC", "TIMESTAMP(6)"},
		{"TIMESTAMP '0044-03-15 12:00:00.0000006 BC'", "0044-03-15 12:00:00.000001 BC", "TIMESTAMP(6)"},
		{"TIMESTAMP '0044-03-15 12:00:00.0000015 BC'", "0044-03-15 12:00:00.000002 BC", "TIMESTAMP(6)"},
		{"TIMESTAMP '3000-01-01 00:00:00.0000005'", "3000-01-01 00:00:00", "TIMESTAMP(6)"},
		{"TIMESTAMP '3000-12-31 23:59:59.9999999'", "3001-01-01 00:00:00", "TIMESTAMP(6)"},
		{"TIMESTAMP '2024-01-10 10:00:00.1234567'", "2024-01-10 10:00:00.1234567", "TIMESTAMP(9)"},
		// BC dates.
		{"CAST(TIMESTAMP '0044-03-15 12:00:00.5 BC' AS TIMESTAMP(0))", "0044-03-15 12:00:00 BC", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '0044-03-15 12:00:00.6 BC' AS TIMESTAMP(0))", "0044-03-15 12:00:01 BC", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '0044-03-15 23:59:59.7 BC' AS TIMESTAMP(0))", "0044-03-16 00:00:00 BC", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '0001-12-31 23:59:59.5 BC' AS TIMESTAMP(0))", "0001-12-31 23:59:59 BC", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '0001-12-31 23:59:59.6 BC' AS TIMESTAMP(0))", "0001-01-01 00:00:00", "TIMESTAMP(0)"},

		// TIMESTAMP WITH TIME ZONE rounds the instant.
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00.5+00' AS TIMESTAMPTZ(0))", "2024-01-10 10:00:01+00", "TIMESTAMP(0) WITH TIME ZONE"},
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00.4+00' AS TIMESTAMPTZ(0))", "2024-01-10 10:00:00+00", "TIMESTAMP(0) WITH TIME ZONE"},
		{"CAST(TIMESTAMPTZ '2024-12-31 23:59:59.9995-05' AS TIMESTAMP(3) WITH TIME ZONE)", "2025-01-01 05:00:00+00", "TIMESTAMP(3) WITH TIME ZONE"},
		{"CAST('2024-01-10 10:00:00.1234565+00' AS TIMESTAMPTZ)", "2024-01-10 10:00:00.123456+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00.1234565+00' AS TIMESTAMPTZ)", "2024-01-10 10:00:00.123457+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"CAST(TIMESTAMP '2024-01-10 23:59:59.9' AS TIMESTAMPTZ(0))", "2024-01-11 00:00:00+00", "TIMESTAMP(0) WITH TIME ZONE"},
		{"CAST(TIMESTAMPTZ '2024-01-10 23:59:59.9+00' AS TIMESTAMP(0))", "2024-01-11 00:00:00", "TIMESTAMP(0)"},

		// TIME: up to 24:00:00, which Postgres allows.
		{"CAST(TIME '10:00:00.4' AS TIME(0))", "10:00:00", "TIME(0)"},
		{"CAST(TIME '10:00:00.1236' AS TIME(3))", "10:00:00.124", "TIME(3)"},
		{"CAST(TIME '10:00:00.1234565' AS TIME(6))", "10:00:00.123457", "TIME(6)"},
		{"CAST('10:00:00.1234565' AS TIME(6))", "10:00:00.123456", "TIME(6)"},
		{"CAST('10:00:00.4999995' AS TIME(0))", "10:00:01", "TIME(0)"},
		{"CAST(TIME '10:59:59.5' AS TIME(0))", "11:00:00", "TIME(0)"},
		{"CAST(TIME '23:59:59.9' AS TIME(0))", "24:00:00", "TIME(0)"},
		{"CAST(TIME '23:59:59.9995' AS TIME(3))", "24:00:00", "TIME(3)"},
		{"CAST(TIME '23:59:59.9999995' AS TIME)", "24:00:00", "TIME(6)"},
		{"CAST(TIME '00:00:00.4' AS TIME(0))", "00:00:00", "TIME(0)"},
		{"CAST('10:00:00.5' AS TIME(0))", "10:00:01", "TIME(0)"},
		// A timestamp's time of day.
		{"CAST(TIMESTAMP '2024-01-10 23:59:59.9' AS TIME(0))", "24:00:00", "TIME(0)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.5' AS TIME(0))", "10:00:01", "TIME(0)"},
		{"CAST(TIMESTAMP '1969-12-31 10:00:00.5' AS TIME(0))", "10:00:01", "TIME(0)"},
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00.5+00' AS TIME(0))", "10:00:01", "TIME(0)"},
		// 24:00:00's fields.
		{"EXTRACT(hour FROM CAST(TIME '23:59:59.9' AS TIME(0)))", "24", "BIGINT"},
		{"EXTRACT(minute FROM CAST(TIME '23:59:59.9' AS TIME(0)))", "0", "BIGINT"},
		{"EXTRACT(epoch FROM CAST(TIME '23:59:59.9' AS TIME(0)))", "86400", "DOUBLE PRECISION"},
		{"CAST(TIME '23:59:59.9' AS TIME(0)) > TIME '23:59:59.999999'", "true", "BOOLEAN"},

		// Text of the rounded value, and comparisons: text compared with a
		// time or timestamp keeps its precision, as in Postgres.
		{"CAST(CAST(TIMESTAMP '2024-01-10 10:00:00.5' AS TIMESTAMP(0)) AS VARCHAR)", "2024-01-10 10:00:01", "VARCHAR"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.5' AS TIMESTAMP(0)) = '2024-01-10 10:00:01'", "true", "BOOLEAN"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.5' AS TIMESTAMP(0)) = '2024-01-10 10:00:00.5'", "false", "BOOLEAN"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.5' AS TIMESTAMP(0)) > '2024-01-10 10:00:00.5'", "true", "BOOLEAN"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.4' AS TIMESTAMP(0)) = '2024-01-10 10:00:00.4'", "false", "BOOLEAN"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.5' AS TIMESTAMP(0)) = TIMESTAMP '2024-01-10 10:00:00.5'", "false", "BOOLEAN"},
		{"CAST(TIME '10:00:00.5' AS TIME(0)) = '10:00:00.5'", "false", "BOOLEAN"},
		{"CAST(TIME '10:00:00.5' AS TIME(0)) = '10:00:01'", "true", "BOOLEAN"},
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00.5+00' AS TIMESTAMPTZ(0)) = '2024-01-10 10:00:01+00'", "true", "BOOLEAN"},

		// CASE and COALESCE of different precisions keep the finer one.
		{"COALESCE(CAST(NULL AS TIME(0)), TIME '10:00:00.5')", "10:00:00.5", "TIME(6)"},
		{"CASE WHEN true THEN TIME '10:00:00.25' ELSE CAST(TIME '10:00:00' AS TIME(0)) END", "10:00:00.25", "TIME(6)"},
		{"COALESCE(CAST(NULL AS TIMESTAMP(0)), TIMESTAMP '2024-01-10 10:00:00.5')", "2024-01-10 10:00:00.5", "TIMESTAMP(6)"},
	})
}

func TestRoundUnit(t *testing.T) {
	ms, s, us, ns := arrow.Millisecond, arrow.Second, arrow.Microsecond, arrow.Nanosecond
	for _, c := range []struct {
		v        int64
		from, to arrow.TimeUnit
		epoch    int64
		want     int64
	}{
		{1500, ms, s, 0, 2}, {1499, ms, s, 0, 1}, {-1500, ms, s, 0, -2}, {-1499, ms, s, 0, -1}, {-1501, ms, s, 0, -2},
		{2500, ms, s, 1, 3}, {2500, ms, s, 3, 2}, // half away from the epoch
		{1, s, ms, 0, 1000}, {7, us, us, 0, 7}, {999_999_999, ns, s, 0, 1}, {-1, ns, us, 0, 0},
		{-9223372036854775808, ns, s, 0, -9223372037}, {9223372036854775807, ns, s, 0, 9223372037},
	} {
		got, err := roundUnit(c.v, c.from, c.to, c.epoch)
		if err != nil || got != c.want {
			t.Errorf("roundUnit(%d, %s, %s, %d) = %d, %v; want %d", c.v, unitNames[c.from], unitNames[c.to], c.epoch, got, err, c.want)
		}
	}
	if _, err := roundUnit(9223372036854775807, s, ns, 0); err == nil {
		t.Error("roundUnit to a finer unit: no overflow error")
	}
}
