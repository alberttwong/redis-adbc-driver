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
// precision (#103), and for precisions other than 0, 3, 6 and 9 (#113).

import (
	"encoding/json"
	"testing"
	"time"

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
		// Any precision from 0 to 9, also one between 0, 3, 6 and 9
		// (TestFractionalPrecision).
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.1236' AS TIMESTAMP(2))", "2024-01-10 10:00:00.12", "TIMESTAMP(2)"},
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

// A TIME(p) or TIMESTAMP(p) of a p other than 0, 3, 6 or 9 (#113) holds p
// digits, rounded as above, in the Arrow unit with the next number of
// digits, and its type is TIME(p) / TIMESTAMP(p). The results are Postgres
// 16's, except where noted.
func TestFractionalPrecision(t *testing.T) {
	runScalarCases(t, []scalarCase{
		// The issue's table.
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.123456' AS TIMESTAMP(1))", "2024-01-10 10:00:00.1", "TIMESTAMP(1)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.123456' AS TIMESTAMP(2))", "2024-01-10 10:00:00.12", "TIMESTAMP(2)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.123456' AS TIMESTAMP(4))", "2024-01-10 10:00:00.1235", "TIMESTAMP(4)"},
		{"CAST(TIME '10:00:00.123456' AS TIME(5))", "10:00:00.12346", "TIME(5)"},

		// Halves round up, less than a half down, at every p.
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.123456' AS TIMESTAMP(5))", "2024-01-10 10:00:00.12346", "TIMESTAMP(5)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.15' AS TIMESTAMP(1))", "2024-01-10 10:00:00.2", "TIMESTAMP(1)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.149999' AS TIMESTAMP(1))", "2024-01-10 10:00:00.1", "TIMESTAMP(1)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2))", "2024-01-10 10:00:00.13", "TIMESTAMP(2)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.124999' AS TIMESTAMP(2))", "2024-01-10 10:00:00.12", "TIMESTAMP(2)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.12345' AS TIMESTAMP(4))", "2024-01-10 10:00:00.1235", "TIMESTAMP(4)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.123449' AS TIMESTAMP(4))", "2024-01-10 10:00:00.1234", "TIMESTAMP(4)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.123455' AS TIMESTAMP(5))", "2024-01-10 10:00:00.12346", "TIMESTAMP(5)"},
		// In one step from the value: .1245 is .12, not .125 (the unit's 3
		// digits) and then .13.
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.1245' AS TIMESTAMP(2))", "2024-01-10 10:00:00.12", "TIMESTAMP(2)"},
		{"CAST(TIME '10:00:00.1495' AS TIME(1))", "10:00:00.1", "TIME(1)"},
		// A cast of a cast rounds twice, as in Postgres.
		{"CAST(CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)) AS TIMESTAMP(1))", "2024-01-10 10:00:00.1", "TIMESTAMP(1)"},
		{"CAST(CAST(TIMESTAMP '2024-01-10 10:00:00.15' AS TIMESTAMP(2)) AS TIMESTAMP(1))", "2024-01-10 10:00:00.2", "TIMESTAMP(1)"},
		{"CAST(CAST(TIMESTAMP '2024-01-10 10:00:00.123456' AS TIMESTAMP(2)) AS TIMESTAMP(4))", "2024-01-10 10:00:00.12", "TIMESTAMP(4)"},
		// Nanoseconds (the driver's: a literal with more than 6 digits is a
		// TIMESTAMP(9); Postgres reads it in microseconds, .125, and gives
		// .13) round in one step too, and 7 and 8 digits are kept (Postgres
		// reduces a p above 6 to 6).
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.124999999' AS TIMESTAMP(2))", "2024-01-10 10:00:00.12", "TIMESTAMP(2)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.123456789' AS TIMESTAMP(7))", "2024-01-10 10:00:00.1234568", "TIMESTAMP(7)"},
		{"CAST(TIME '10:00:00.123456789' AS TIME(8))", "10:00:00.12345679", "TIME(8)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.5' AS TIMESTAMP(12))", "2024-01-10 10:00:00.5", "TIMESTAMP(9)"},

		// Carries into the next second, hour, day, month and year.
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.95' AS TIMESTAMP(1))", "2024-01-10 10:00:01", "TIMESTAMP(1)"},
		{"CAST(TIMESTAMP '2024-01-10 10:59:59.995' AS TIMESTAMP(2))", "2024-01-10 11:00:00", "TIMESTAMP(2)"},
		{"CAST(TIMESTAMP '2024-01-10 23:59:59.99995' AS TIMESTAMP(4))", "2024-01-11 00:00:00", "TIMESTAMP(4)"},
		{"CAST(TIMESTAMP '2024-02-28 23:59:59.999995' AS TIMESTAMP(5))", "2024-02-29 00:00:00", "TIMESTAMP(5)"},
		{"CAST(TIMESTAMP '2024-12-31 23:59:59.96' AS TIMESTAMP(1))", "2025-01-01 00:00:00", "TIMESTAMP(1)"},
		// Before 2000-01-01 a half rounds down, BC too.
		{"CAST(TIMESTAMP '1999-12-31 23:59:59.95' AS TIMESTAMP(1))", "1999-12-31 23:59:59.9", "TIMESTAMP(1)"},
		{"CAST(TIMESTAMP '1999-12-31 23:59:59.96' AS TIMESTAMP(1))", "2000-01-01 00:00:00", "TIMESTAMP(1)"},
		{"CAST(TIMESTAMP '1990-06-15 10:00:00.125' AS TIMESTAMP(2))", "1990-06-15 10:00:00.12", "TIMESTAMP(2)"},
		{"CAST(TIMESTAMP '1990-06-15 10:00:00.126' AS TIMESTAMP(2))", "1990-06-15 10:00:00.13", "TIMESTAMP(2)"},
		{"CAST(TIMESTAMP '2000-01-01 00:00:00.125' AS TIMESTAMP(2))", "2000-01-01 00:00:00.13", "TIMESTAMP(2)"},
		{"CAST(TIMESTAMP '1969-12-31 23:59:59.995' AS TIMESTAMP(2))", "1969-12-31 23:59:59.99", "TIMESTAMP(2)"},
		{"CAST(TIMESTAMP '0044-03-15 12:00:00.15 BC' AS TIMESTAMP(1))", "0044-03-15 12:00:00.1 BC", "TIMESTAMP(1)"},
		{"CAST(TIMESTAMP '0044-03-15 12:00:00.16 BC' AS TIMESTAMP(1))", "0044-03-15 12:00:00.2 BC", "TIMESTAMP(1)"},

		// Text is read in microseconds (with more digits, a half to even)
		// and then rounded; dates have no fraction.
		{"CAST('2024-01-10 10:00:00.123456' AS TIMESTAMP(2))", "2024-01-10 10:00:00.12", "TIMESTAMP(2)"},
		{"CAST('2024-01-10 10:00:00.125' AS TIMESTAMP(2))", "2024-01-10 10:00:00.13", "TIMESTAMP(2)"},
		{"CAST('2024-01-10 10:00:00.1249995' AS TIMESTAMP(2))", "2024-01-10 10:00:00.13", "TIMESTAMP(2)"},
		{"'2024-01-10 10:00:00.1234565'::timestamp(5)", "2024-01-10 10:00:00.12346", "TIMESTAMP(5)"},
		{"CAST(DATE '2024-01-10' AS TIMESTAMP(2))", "2024-01-10 00:00:00", "TIMESTAMP(2)"},
		// Typed literals with a precision.
		{"TIMESTAMP(2) '2024-01-10 10:00:00.125'", "2024-01-10 10:00:00.13", "TIMESTAMP(2)"},
		{"TIMESTAMP(2) '2024-01-10 10:00:00.1249995'", "2024-01-10 10:00:00.13", "TIMESTAMP(2)"},
		{"TIMESTAMP(1) WITH TIME ZONE '2024-01-10 10:00:00.15+00'", "2024-01-10 10:00:00.2+00", "TIMESTAMP(1) WITH TIME ZONE"},
		{"TIMESTAMPTZ(1) '2024-01-10 10:00:00.15+00'", "2024-01-10 10:00:00.2+00", "TIMESTAMP(1) WITH TIME ZONE"},
		{"TIMESTAMPTZ(1) '2024-01-10 10:00:00.15'", "2024-01-10 10:00:00.2+00", "TIMESTAMP(1) WITH TIME ZONE"},
		{"TIME(1) '10:00:00.15'", "10:00:00.2", "TIME(1)"},

		// TIMESTAMP WITH TIME ZONE rounds the instant.
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00.15+00' AS TIMESTAMPTZ(1))", "2024-01-10 10:00:00.2+00", "TIMESTAMP(1) WITH TIME ZONE"},
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00.15+05:30' AS TIMESTAMPTZ(1))", "2024-01-10 04:30:00.2+00", "TIMESTAMP(1) WITH TIME ZONE"},
		{"CAST(TIMESTAMPTZ '2024-01-10 23:59:59.95-05' AS TIMESTAMP(1) WITH TIME ZONE)", "2024-01-11 05:00:00+00", "TIMESTAMP(1) WITH TIME ZONE"},
		{"CAST(TIMESTAMP '2024-01-10 23:59:59.96' AS TIMESTAMPTZ(1))", "2024-01-11 00:00:00+00", "TIMESTAMP(1) WITH TIME ZONE"},
		{"CAST(TIMESTAMPTZ '2024-01-10 23:59:59.96+00' AS TIMESTAMP(1))", "2024-01-11 00:00:00", "TIMESTAMP(1)"},
		{"CAST('2024-01-10 10:00:00.12345+00' AS TIMESTAMPTZ(4))", "2024-01-10 10:00:00.1235+00", "TIMESTAMP(4) WITH TIME ZONE"},

		// TIME, up to 24:00:00, and a timestamp's time of day.
		{"CAST(TIME '10:00:00.15' AS TIME(1))", "10:00:00.2", "TIME(1)"},
		{"CAST(TIME '10:00:00.125' AS TIME(2))", "10:00:00.13", "TIME(2)"},
		{"CAST(TIME '10:00:00.12345' AS TIME(4))", "10:00:00.1235", "TIME(4)"},
		{"CAST(TIME '23:59:59.95' AS TIME(1))", "24:00:00", "TIME(1)"},
		{"CAST(TIME '23:59:59.999995' AS TIME(5))", "24:00:00", "TIME(5)"},
		{"CAST('10:00:00.1234565' AS TIME(5))", "10:00:00.12346", "TIME(5)"},
		{"CAST(CAST(TIME '10:00:00.15' AS TIME(1)) AS TIME(4))", "10:00:00.2", "TIME(4)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIME(2))", "10:00:00.13", "TIME(2)"},
		{"CAST(TIMESTAMP '1969-12-31 10:00:00.125' AS TIME(2))", "10:00:00.13", "TIME(2)"},
		{"CAST(TIMESTAMP '2024-01-10 23:59:59.96' AS TIME(1))", "24:00:00", "TIME(1)"},
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00.15+00' AS TIME(1))", "10:00:00.2", "TIME(1)"},

		// Fields, text and comparisons see the rounded value.
		{"EXTRACT(epoch FROM CAST(TIME '10:00:00.125' AS TIME(2)))", "36000.13", "DOUBLE PRECISION"},
		{"EXTRACT(microseconds FROM CAST(TIMESTAMP '2024-01-10 10:00:00.123456' AS TIMESTAMP(2)))", "120000", "BIGINT"},
		{"EXTRACT(milliseconds FROM CAST(TIMESTAMP '2024-01-10 10:00:01.125' AS TIMESTAMP(1)))", "1100", "DOUBLE PRECISION"},
		{"TO_CHAR(CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)), 'HH24:MI:SS.US')", "10:00:00.130000", "VARCHAR"},
		{"CAST(CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)) AS VARCHAR)", "2024-01-10 10:00:00.13", "VARCHAR"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)) = '2024-01-10 10:00:00.13'", "true", "BOOLEAN"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)) = '2024-01-10 10:00:00.125'", "false", "BOOLEAN"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)) > TIMESTAMP '2024-01-10 10:00:00.125'", "true", "BOOLEAN"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)) = CAST(TIMESTAMP '2024-01-10 10:00:00.13' AS TIMESTAMP(3))", "true", "BOOLEAN"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.12345' AS TIMESTAMP(4)) = TIMESTAMP '2024-01-10 10:00:00.1235'", "true", "BOOLEAN"},
		{"CAST(TIME '10:00:00.12346' AS TIME(5)) < TIME '10:00:00.123461'", "true", "BOOLEAN"},
		{"CAST(TIME '10:00:00.15' AS TIME(1)) = '10:00:00.2'", "true", "BOOLEAN"},

		// Combined types have the most digits, so that nothing is rounded
		// (Postgres's type has no precision, which is 6, when they
		// differ). A text literal and arithmetic give the unit's digits:
		// Postgres's types have no precision there either.
		{"COALESCE(CAST(NULL AS TIMESTAMP(2)), CAST(TIMESTAMP '2024-01-10 10:00:00.123456' AS TIMESTAMP(4)))", "2024-01-10 10:00:00.1235", "TIMESTAMP(4)"},
		{"CASE WHEN true THEN CAST(TIME '10:00:00.125' AS TIME(2)) ELSE CAST(TIME '10:00:00' AS TIME(1)) END", "10:00:00.13", "TIME(2)"},
		{"GREATEST(CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)), CAST(TIMESTAMP '2024-01-10 10:00:00.12' AS TIMESTAMP(1)))", "2024-01-10 10:00:00.13", "TIMESTAMP(2)"},
		{"COALESCE(CAST(NULL AS TIMESTAMP(1)), CAST(TIMESTAMPTZ '2024-01-10 10:00:00.125+00' AS TIMESTAMPTZ(2)))", "2024-01-10 10:00:00.13+00", "TIMESTAMP(2) WITH TIME ZONE"},
		{"COALESCE(CAST(NULL AS TIMESTAMP(2)), '2024-01-10 10:00:00.123')", "2024-01-10 10:00:00.123", "TIMESTAMP(3)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)) + INTERVAL '0.001 second'", "2024-01-10 10:00:00.131", "TIMESTAMP(3)"},
		{"CAST(TIME '10:00:00.15' AS TIME(1)) + INTERVAL '0.01 second'", "10:00:00.21", "TIME(3)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)) - INTERVAL '1 day'", "2024-01-09 10:00:00.13", "TIMESTAMP(3)"},
		{"DATE_TRUNC('second', CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)))", "2024-01-10 10:00:00", "TIMESTAMP(3)"},
		{"DATEADD(millisecond, 1, CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)))", "2024-01-10 10:00:00.131", "TIMESTAMP(3)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00.125' AS TIMESTAMP(2)) AT TIME ZONE 'UTC'", "2024-01-10 10:00:00.13+00", "TIMESTAMP(3) WITH TIME ZONE"},
	})
}

// CURRENT_TIMESTAMP(p), LOCALTIMESTAMP(p), CURRENT_TIME(p) and LOCALTIME(p)
// (and NOW(p)) are the current time, read in microseconds as without p,
// rounded to p digits as Postgres rounds it (CURRENT_TIMESTAMP(0) is whole
// seconds), of type …(p). p is an integer constant from 0 to 9.
func TestCurrentTimePrecision(t *testing.T) {
	now := time.Date(2024, 1, 10, 5, 0, 0, 654_321_987, time.UTC)
	runZoneCasesAt(t, "UTC", now, []scalarCase{
		{"CURRENT_TIMESTAMP", "2024-01-10 05:00:00.654321+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"CURRENT_TIMESTAMP(0)", "2024-01-10 05:00:01+00", "TIMESTAMP(0) WITH TIME ZONE"},
		{"CURRENT_TIMESTAMP(1)", "2024-01-10 05:00:00.7+00", "TIMESTAMP(1) WITH TIME ZONE"},
		{"CURRENT_TIMESTAMP(2)", "2024-01-10 05:00:00.65+00", "TIMESTAMP(2) WITH TIME ZONE"},
		{"CURRENT_TIMESTAMP(5)", "2024-01-10 05:00:00.65432+00", "TIMESTAMP(5) WITH TIME ZONE"},
		{"CURRENT_TIMESTAMP(6)", "2024-01-10 05:00:00.654321+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"CURRENT_TIMESTAMP(9)", "2024-01-10 05:00:00.654321+00", "TIMESTAMP(9) WITH TIME ZONE"},
		{"NOW(3)", "2024-01-10 05:00:00.654+00", "TIMESTAMP(3) WITH TIME ZONE"},
		{"TRANSACTION_TIMESTAMP(0)", "2024-01-10 05:00:01+00", "TIMESTAMP(0) WITH TIME ZONE"},
		{"LOCALTIMESTAMP(3)", "2024-01-10 05:00:00.654", "TIMESTAMP(3)"},
		{"CURRENT_TIME(0)", "05:00:01", "TIME(0)"},
		{"LOCALTIME(4)", "05:00:00.6543", "TIME(4)"},
		{"CURRENT_TIMESTAMP(2) = CAST(CURRENT_TIMESTAMP AS TIMESTAMPTZ(2))", "true", "BOOLEAN"},
		{"CURRENT_TIMESTAMP(6) = NOW()", "true", "BOOLEAN"},
		{"EXTRACT(microseconds FROM CURRENT_TIMESTAMP(0))", "1000000", "BIGINT"},
		{"CAST(CURRENT_TIMESTAMP(1) AS TEXT)", "2024-01-10 05:00:00.7+00", "VARCHAR"},
		// p is an integer constant from 0 to 9.
		{"CURRENT_TIMESTAMP(10)", "error: CURRENT_TIMESTAMP precision must be an integer constant from 0 to 9", ""},
		{"CURRENT_TIMESTAMP(-1)", "error: CURRENT_TIMESTAMP precision must be an integer constant from 0 to 9", ""},
		{"NOW(1.5)", "error: NOW precision must be an integer constant from 0 to 9", ""},
		{"LOCALTIME('1')", "error: LOCALTIME precision must be an integer constant from 0 to 9", ""},
		{"CURRENT_TIME(NULL)", "error: CURRENT_TIME precision must be an integer constant from 0 to 9", ""},
		{"LOCALTIMESTAMP(1 + 1)", "error: LOCALTIMESTAMP precision must be an integer constant from 0 to 9", ""},
	})
	// The local times, in the session time zone.
	runZoneCasesAt(t, "America/Los_Angeles", now, []scalarCase{
		{"CURRENT_TIMESTAMP(2)", "2024-01-09 21:00:00.65-08", "TIMESTAMP(2) WITH TIME ZONE"},
		{"LOCALTIMESTAMP(3)", "2024-01-09 21:00:00.654", "TIMESTAMP(3)"},
		{"LOCALTIME(1)", "21:00:00.7", "TIME(1)"},
	})
	// A carry into the next day, month and year; a time of day of
	// 24:00:00.
	carry := time.Date(2024, 12, 31, 23, 59, 59, 950_000_000, time.UTC)
	runZoneCasesAt(t, "UTC", carry, []scalarCase{
		{"CURRENT_TIMESTAMP(1)", "2025-01-01 00:00:00+00", "TIMESTAMP(1) WITH TIME ZONE"},
		{"LOCALTIMESTAMP(0)", "2025-01-01 00:00:00", "TIMESTAMP(0)"},
		{"LOCALTIME(1)", "24:00:00", "TIME(1)"},
		{"CURRENT_TIMESTAMP(2)", "2024-12-31 23:59:59.95+00", "TIMESTAMP(2) WITH TIME ZONE"},
	})
}

// A TIME(p) or TIMESTAMP(p) keeps p in the table metadata only when it is
// below its unit's digits, so metadata written before p was kept reads back
// as the type it was (a TIMESTAMP(2) column was then a TIMESTAMP(3)), and
// TIMESTAMP(3) is the plain millisecond type.
func TestColTypeFracDigitsJSON(t *testing.T) {
	parse := func(sql string) ColType {
		t.Helper()
		ct, err := colTypeFromSQL(sqlTypeSpecOf(t, sql))
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return ct
	}
	for _, c := range []struct {
		sql, json string
		digits    int
	}{
		{"TIMESTAMP(2)", `{"kind":"timestamp","unit":"ms","precision":2}`, 2},
		{"TIMESTAMP(3)", `{"kind":"timestamp","unit":"ms"}`, 3},
		{"TIMESTAMP(0)", `{"kind":"timestamp","unit":"s"}`, 0},
		{"TIMESTAMP", `{"kind":"timestamp","unit":"us"}`, 6},
		{"TIMESTAMP(4) WITH TIME ZONE", `{"kind":"timestamp","unit":"us","tz":"UTC","precision":4}`, 4},
		{"TIMESTAMPTZ(1)", `{"kind":"timestamp","unit":"ms","tz":"UTC","precision":1}`, 1},
		{"TIME(5)", `{"kind":"time","unit":"us","precision":5}`, 5},
		{"TIME(8)", `{"kind":"time","unit":"ns","precision":8}`, 8},
		{"TIME(9)", `{"kind":"time","unit":"ns"}`, 9},
		{"TIME(12)", `{"kind":"time","unit":"ns"}`, 9},
	} {
		ct := parse(c.sql)
		b, err := json.Marshal(ct)
		if err != nil || string(b) != c.json {
			t.Errorf("%s is stored as %s, %v; want %s", c.sql, b, err, c.json)
		}
		var back ColType
		if err := json.Unmarshal(b, &back); err != nil || back != ct || back.fracDigits() != c.digits {
			t.Errorf("%s: round trip %s gives %+v (%d digits), %v", c.sql, b, back, back.fracDigits(), err)
		}
	}
	if parse("TIMESTAMP(3)") != timestampType(arrow.Millisecond, "") || parse("TIME(6)") != typeTimeUS {
		t.Error("TIMESTAMP(3) or TIME(6) is not the plain unit's type")
	}
	// Metadata written before p was kept: the unit's digits.
	for js, want := range map[string]string{
		`{"kind":"timestamp","unit":"ms"}`:                           "TIMESTAMP(3)",
		`{"kind":"timestamp","unit":"us","tz":"UTC"}`:                "TIMESTAMP(6) WITH TIME ZONE",
		`{"kind":"time","unit":"s"}`:                                 "TIME(0)",
		`{"kind":"time","unit":"ns"}`:                                "TIME(9)",
		`{"kind":"timestamp","unit":"ms","precision":3}`:             "TIMESTAMP(3)", // not below the unit's: none
		`{"kind":"decimal","precision":10,"scale":2}`:                "NUMERIC(10,2)",
		`{"kind":"timestamp","unit":"us","tz":"UTC","precision":-1}`: "TIMESTAMP(6) WITH TIME ZONE",
	} {
		var ct ColType
		if err := json.Unmarshal([]byte(js), &ct); err != nil || ct.SQLName() != want {
			t.Errorf("%s reads as %s, %v; want %s", js, ct.SQLName(), err, want)
		}
		if ct.Kind == KindTimestamp && ct.Precision != 0 {
			t.Errorf("%s reads with precision %d", js, ct.Precision)
		}
	}
	// A negative p is an error, as in Postgres; so is a second modifier.
	for sql, want := range map[string]string{
		"TIMESTAMP(-1)":                "TIMESTAMP(-1) precision must not be negative",
		"TIMESTAMP(-1) WITH TIME ZONE": "TIMESTAMP(-1) WITH TIME ZONE precision must not be negative",
		"TIMESTAMPTZ(-2)":              "TIMESTAMP(-2) WITH TIME ZONE precision must not be negative",
		"TIME(-1)":                     "TIME(-1) precision must not be negative",
		"TIMESTAMP(3, 1)":              "invalid type modifier for type timestamp",
	} {
		if _, err := colTypeFromSQL(sqlTypeSpecOf(t, sql)); err == nil || err.Error() != want {
			t.Errorf("%s: error %v, want %q", sql, err, want)
		}
	}
}

// sqlTypeSpecOf parses a type name, as CAST's target.
func sqlTypeSpecOf(t *testing.T, sql string) sqlTypeSpec {
	t.Helper()
	toks, err := lex(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	p := &parser{src: sql, toks: toks}
	spec, err := p.parseTypeSpec()
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return spec
}

func TestRoundUnit(t *testing.T) {
	ms, s, us, ns := arrow.Millisecond, arrow.Second, arrow.Microsecond, arrow.Nanosecond
	for _, c := range []struct {
		v        int64
		from, to arrow.TimeUnit
		digits   int // -1: the digits of `to`
		epoch    int64
		want     int64
	}{
		{1500, ms, s, -1, 0, 2}, {1499, ms, s, -1, 0, 1}, {-1500, ms, s, -1, 0, -2}, {-1499, ms, s, -1, 0, -1}, {-1501, ms, s, -1, 0, -2},
		{2500, ms, s, -1, 1, 3}, {2500, ms, s, -1, 3, 2}, // half away from the epoch
		{1, s, ms, -1, 0, 1000}, {7, us, us, -1, 0, 7}, {999_999_999, ns, s, -1, 0, 1}, {-1, ns, us, -1, 0, 0},
		{-9223372036854775808, ns, s, -1, 0, -9223372037}, {9223372036854775807, ns, s, -1, 0, 9223372037},
		// Fewer digits than the unit's (#113), in one step from v: .1245 is
		// .12, not .125 and then .13.
		{123456, us, ms, 2, 0, 120}, {124500, us, ms, 2, 0, 120}, {125000, us, ms, 2, 0, 130}, {124999, us, ms, 2, 0, 120},
		{-125000, us, ms, 2, 0, -130}, {-124999, us, ms, 2, 0, -120}, {125000, us, ms, 2, 1, 120},
		{123456, us, us, 4, 0, 123500}, {123456, us, us, 5, 0, 123460}, {150, ms, ms, 1, 0, 200}, {1_150, ms, ms, 1, 2, 1_100},
		{123_456_789, ns, ms, 2, 0, 120}, {124_999_999, ns, ms, 2, 0, 120}, {123_456_789, ns, ns, 7, 0, 123_456_800},
		{999_950, us, us, 4, 0, 1_000_000}, {12, s, us, 4, 0, 12_000_000}, {1_995, ms, us, 2, 0, 2_000_000},
		{123_456, us, ms, 6, 0, 123}, // more digits than `to` has: `to`'s
	} {
		digits := c.digits
		if digits < 0 {
			digits = precisionForUnit(c.to)
		}
		got, err := roundUnit(c.v, c.from, c.to, digits, c.epoch)
		if err != nil || got != c.want {
			t.Errorf("roundUnit(%d, %s, %s, %d, %d) = %d, %v; want %d", c.v, unitNames[c.from], unitNames[c.to], digits, c.epoch, got, err, c.want)
		}
	}
	if _, err := roundUnit(9223372036854775807, s, ns, 9, 0); err == nil {
		t.Error("roundUnit to a finer unit: no overflow error")
	}
	if _, err := roundUnit(9223372036854775807, ns, ns, 3, 0); err == nil {
		t.Error("roundUnit up past the int64 range: no overflow error")
	}
}
