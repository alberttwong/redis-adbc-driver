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
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Dates, times and timestamps as text: as Postgres writes them, whatever
// the declared precision.
func TestDateTimeText(t *testing.T) {
	runScalarCases(t, []scalarCase{
		// Seconds always, and no fraction when it is zero.
		{"CAST(CAST('2024-01-10 10:00:00' AS TIMESTAMP(0)) AS VARCHAR)", "2024-01-10 10:00:00", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00' AS TIMESTAMP(3)) AS VARCHAR)", "2024-01-10 10:00:00", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00' AS TIMESTAMP) AS VARCHAR)", "2024-01-10 10:00:00", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00' AS TIMESTAMP(9)) AS VARCHAR)", "2024-01-10 10:00:00", "VARCHAR"},
		{"CAST(TIMESTAMP '2024-01-10 10:00' AS VARCHAR)", "2024-01-10 10:00:00", "VARCHAR"},
		// The fraction without trailing zeros.
		{"CAST(CAST('2024-01-10 10:00:00.5' AS TIMESTAMP(3)) AS VARCHAR)", "2024-01-10 10:00:00.5", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.5' AS TIMESTAMP) AS VARCHAR)", "2024-01-10 10:00:00.5", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.5' AS TIMESTAMP(9)) AS VARCHAR)", "2024-01-10 10:00:00.5", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.050' AS TIMESTAMP(3)) AS VARCHAR)", "2024-01-10 10:00:00.05", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.050' AS TIMESTAMP) AS VARCHAR)", "2024-01-10 10:00:00.05", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.123456' AS TIMESTAMP) AS VARCHAR)", "2024-01-10 10:00:00.123456", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.123456' AS TIMESTAMP(3)) AS VARCHAR)", "2024-01-10 10:00:00.123", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.000001' AS TIMESTAMP) AS VARCHAR)", "2024-01-10 10:00:00.000001", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.123456789' AS TIMESTAMP(9)) AS VARCHAR)", "2024-01-10 10:00:00.123456789", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.100000000' AS TIMESTAMP(9)) AS VARCHAR)", "2024-01-10 10:00:00.1", "VARCHAR"},
		// With time zone: the offset, +00 in the session time zone UTC.
		{"CAST(CAST('2024-01-10 10:00:00+00' AS TIMESTAMPTZ) AS VARCHAR)", "2024-01-10 10:00:00+00", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00+00' AS TIMESTAMP(3) WITH TIME ZONE) AS VARCHAR)", "2024-01-10 10:00:00+00", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00.5+00' AS TIMESTAMPTZ(0)) AS VARCHAR)", "2024-01-10 10:00:01+00", "VARCHAR"}, // rounded (#103)
		{"CAST(CAST('2024-01-10 10:00:00.25-08' AS TIMESTAMPTZ) AS VARCHAR)", "2024-01-10 18:00:00.25+00", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00-08:30' AS TIMESTAMPTZ) AS VARCHAR)", "2024-01-10 18:30:00+00", "VARCHAR"},
		{"CAST(CAST('2024-01-10 10:00:00+05:30' AS TIMESTAMPTZ) AS VARCHAR)", "2024-01-10 04:30:00+00", "VARCHAR"},
		// Times.
		{"CAST(TIME '10:00:00' AS VARCHAR)", "10:00:00", "VARCHAR"},
		{"CAST(CAST('10:00:00' AS TIME(0)) AS VARCHAR)", "10:00:00", "VARCHAR"},
		{"CAST(CAST('10:00:00.5' AS TIME(3)) AS VARCHAR)", "10:00:00.5", "VARCHAR"},
		{"CAST(CAST('10:00:00.050' AS TIME) AS VARCHAR)", "10:00:00.05", "VARCHAR"},
		{"CAST(TIME '23:59:59.999999' AS VARCHAR)", "23:59:59.999999", "VARCHAR"},
		{"CAST(TIME '00:00:00' AS VARCHAR)", "00:00:00", "VARCHAR"},
		// Dates, BC dates (astronomical year 0 is 1 BC) and years past 9999.
		{"CAST(DATE '2024-01-10' AS VARCHAR)", "2024-01-10", "VARCHAR"},
		{"CAST(DATE '0001-01-01' AS VARCHAR)", "0001-01-01", "VARCHAR"},
		{"CAST(DATE '0001-01-01' - 1 AS VARCHAR)", "0001-12-31 BC", "VARCHAR"},
		{"CAST(DATE '0044-03-15 BC' AS VARCHAR)", "0044-03-15 BC", "VARCHAR"},
		{"CAST(CAST('0044-03-15 bc' AS DATE) AS VARCHAR)", "0044-03-15 BC", "VARCHAR"},
		{"CAST(CAST('2024-01-10 AD' AS DATE) AS VARCHAR)", "2024-01-10", "VARCHAR"},
		{"DATE '0044-03-15 BC' - DATE '0001-01-01'", "-15998", "BIGINT"},
		{"CAST(TIMESTAMP '0001-01-01 00:00:00' - INTERVAL '0.5 seconds' AS VARCHAR)", "0001-12-31 23:59:59.5 BC", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '0001-01-01 00:00:00+00' - INTERVAL '1 day' AS VARCHAR)", "0001-12-31 00:00:00+00 BC", "VARCHAR"},
		{"CAST(CAST('0001-12-31 00:00:00+00 BC' AS TIMESTAMPTZ) AS VARCHAR)", "0001-12-31 00:00:00+00 BC", "VARCHAR"},
		{"CAST(DATE '9999-12-31' + 1 AS VARCHAR)", "10000-01-01", "VARCHAR"},
		{"CAST(CAST('10000-01-01' AS DATE) AS VARCHAR)", "10000-01-01", "VARCHAR"},
		{"CAST(TIMESTAMP '9999-12-31 23:59:59.5' + INTERVAL '1 second' AS VARCHAR)", "10000-01-01 00:00:00.5", "VARCHAR"},
		// The text reads back as the same value.
		{"CAST(CAST(DATE '0044-03-15 BC' AS VARCHAR) AS DATE) = DATE '0044-03-15 BC'", "true", "BOOLEAN"},
		{"CAST(CAST(TIMESTAMPTZ '0001-01-01 00:00:00+00' - INTERVAL '1 day' AS VARCHAR) AS TIMESTAMPTZ) = TIMESTAMPTZ '0001-01-01 00:00:00+00' - INTERVAL '1 day'", "true", "BOOLEAN"},
		{"CAST(CAST(TIMESTAMP '9999-12-31 23:59:59.5' + INTERVAL '1 second' AS VARCHAR) AS TIMESTAMP) = TIMESTAMP '9999-12-31 23:59:59.5' + INTERVAL '1 second'", "true", "BOOLEAN"},

		// Everything that makes text of them.
		{"'x' || CAST('2024-01-10 10:00:00' AS TIMESTAMP)", "x2024-01-10 10:00:00", "VARCHAR"},
		{"CAST('2024-01-10 10:00:00.5' AS TIMESTAMP(3)) || '|' || CAST('2024-01-10 10:00:00+00' AS TIMESTAMPTZ) || '|' || TIME '10:00:00' || '|' || DATE '2024-01-10'",
			"2024-01-10 10:00:00.5|2024-01-10 10:00:00+00|10:00:00|2024-01-10", "VARCHAR"},
		{"CONCAT('a', CAST('2024-01-10 10:00:00' AS TIMESTAMP(3)), '|', CAST('2024-01-10 10:00:00' AS TIMESTAMP(6)), CAST(NULL AS TIMESTAMP))",
			"a2024-01-10 10:00:00|2024-01-10 10:00:00", "VARCHAR"},
		{"CONCAT_WS(',', TIME '10:00:00.25', CAST(NULL AS TIME), TIMESTAMPTZ '2024-01-10 10:00:00-01')", "10:00:00.25,2024-01-10 11:00:00+00", "VARCHAR"},
		{"UPPER(CAST(DATE '0044-03-15 BC' AS VARCHAR))", "0044-03-15 BC", "VARCHAR"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00' AS VARCHAR) LIKE '%10:00:00'", "true", "BOOLEAN"},
		{"MD5(CAST(TIMESTAMP '2024-01-10 10:00:00' AS VARCHAR))", "b4f75bf4a0f2ba251eb0ce41710d6971", "VARCHAR"},
		{"MD5(CAST(CAST('2024-01-10 10:00:00' AS TIMESTAMP(3)) AS VARCHAR)) = MD5(CAST(CAST('2024-01-10 10:00:00' AS TIMESTAMP(6)) AS VARCHAR))", "true", "BOOLEAN"},
		{"MD5(CAST(CAST('2024-01-10 10:00:00.5' AS TIMESTAMP(3)) AS VARCHAR)) = MD5(CAST(CAST('2024-01-10 10:00:00.5' AS TIMESTAMP(9)) AS VARCHAR))", "true", "BOOLEAN"},
		// NULLs.
		{"CAST(CAST(NULL AS TIMESTAMP) AS VARCHAR)", "NULL", "VARCHAR"},
		{"'x' || CAST(NULL AS TIMESTAMPTZ)", "NULL", "VARCHAR"},
		// JSON: ISO 8601, with a T, offsets as +00:00 and the era last.
		{"TO_JSON(CAST('2024-01-10 10:00:00' AS TIMESTAMP(3)))", `"2024-01-10T10:00:00"`, ""},
		{"TO_JSON(CAST('2024-01-10 10:00:00.5' AS TIMESTAMP))", `"2024-01-10T10:00:00.5"`, ""},
		{"TO_JSON(TIMESTAMPTZ '2024-01-10 10:00:00-01')", `"2024-01-10T11:00:00+00:00"`, ""},
		{"TO_JSON(TIMESTAMP '0001-01-01 00:00:00' - INTERVAL '1 day')", `"0001-12-31T00:00:00 BC"`, ""},
		{"TO_JSON(TIMESTAMPTZ '0001-01-01 00:00:00+00' - INTERVAL '1 day')", `"0001-12-31T00:00:00+00:00 BC"`, ""},
		{"TO_JSON(DATE '0044-03-15 BC')", `"0044-03-15 BC"`, ""},
		{"TO_JSON(TIME '10:00:00.5')", `"10:00:00.5"`, ""},
	})
	expectExprErrors(t, map[string]string{
		// There is no year 0, as in Postgres.
		"CAST('0000-01-01' AS DATE)":           `invalid date "0000-01-01"`,
		"CAST('0000-01-01 BC' AS DATE)":        `invalid timestamp "0000-01-01 BC"`,
		"CAST('2024-01-10x' AS DATE)":          `invalid timestamp "2024-01-10x"`,
		"CAST('12345678-01-01' AS DATE)":       `invalid timestamp "12345678-01-01"`,
		"CAST('2024-1-10 10:00' AS TIMESTAMP)": `invalid timestamp "2024-1-10 10:00"`,
	})
}

func TestFormatOffsetAndTimestamp(t *testing.T) {
	for _, c := range []struct {
		secs int
		xsd  bool
		want string
	}{
		{0, false, "+00"}, {19800, false, "+05:30"}, {-28800, false, "-08"}, {-(5*3600 + 30*60 + 15), false, "-05:30:15"},
		{-34200, false, "-09:30"}, {0, true, "+00:00"}, {3600, true, "+01:00"}, {-28800, true, "-08:00"},
	} {
		if got := formatOffset(c.secs, c.xsd); got != c.want {
			t.Errorf("formatOffset(%d, %v) = %q, want %q", c.secs, c.xsd, got, c.want)
		}
	}
	us := time.Date(2024, 1, 10, 10, 0, 0, 500_000_000, time.UTC).UnixMicro()
	for _, c := range []struct {
		off  int
		xsd  bool
		want string
	}{
		{0, false, "2024-01-10 10:00:00.5+00"},
		{19800, false, "2024-01-10 15:30:00.5+05:30"},
		{-28800, false, "2024-01-10 02:00:00.5-08"},
		{-28800, true, "2024-01-10T02:00:00.5-08:00"},
	} {
		if got := formatTimestamp(us, arrow.Microsecond, true, c.off, c.xsd); got != c.want {
			t.Errorf("formatTimestamp(%d, %v) = %q, want %q", c.off, c.xsd, got, c.want)
		}
	}
}

// TO_CHAR, pattern by pattern, as in Postgres.
func TestToChar(t *testing.T) {
	type tc struct{ value, format, want string }
	const (
		nov29 = "DATE '2020-11-29'" // a Sunday
		ts    = "TIMESTAMP '2024-01-10 13:05:09.123456'"
	)
	cases := []tc{
		// The issue's patterns.
		{nov29, "WW", "48"}, {nov29, "W", "5"}, {nov29, "J", "2459183"}, {nov29, "CC", "21"},
		{nov29, "RM", "XI  "}, {nov29, "FMRM", "XI"}, {nov29, "rm", "xi  "}, {nov29, "YYYY-MM-DD", "2020-11-29"}, {nov29, "IW", "48"},
		// Days and weeks.
		{nov29, "DDD", "334"}, {nov29, "IDDD", "336"}, {nov29, "D", "1"}, {nov29, "ID", "7"}, {nov29, "Q", "4"},
		{nov29, "DD", "29"}, {nov29, "Dy dy DY", "Sun sun SUN"},
		{nov29, "Day|DAY|day", "Sunday   |SUNDAY   |sunday   "}, {nov29, "FMDay|TMDay", "Sunday|Sunday"},
		{nov29, "Month|MONTH|month", "November |NOVEMBER |november "}, {nov29, "FMMonth|TMMonth|tmmonth", "November|November|november"},
		{nov29, "Mon MON mon", "Nov NOV nov"}, {nov29, "MM", "11"},
		// Years.
		{nov29, "Y,YYY YYY YY Y", "2,020 020 20 0"}, {nov29, "IYYY IYY IY I", "2020 020 20 0"},
		{nov29, "AD ad A.D. a.d. BC bc B.C. b.c.", "AD ad A.D. a.d. AD ad A.D. a.d."},
		// Week 53, and the ISO year differing from the calendar year.
		{"DATE '2020-12-31'", "WW IW IYYY DDD IDDD W", "53 53 2020 366 368 5"},
		{"DATE '2021-01-01'", "WW IW IYYY IDDD DDD J", "01 53 2020 369 001 2459216"},
		{"DATE '2021-01-03'", "IW IDDD ID", "53 371 7"},
		{"DATE '2021-01-04'", "WW IW IYYY IDDD", "01 01 2021 001"},
		// A leap day.
		{"DATE '2024-02-29'", "DDD WW W J RM Q IW IDDD", "060 09 5 2460370 II   1 09 060"},
		{"DATE '2023-03-01'", "DDD", "060"},
		// Centuries start in years ending in 01.
		{"DATE '2000-01-01'", "CC IYYY IW ID D J", "20 1999 52 6 7 2451545"},
		{"DATE '2001-01-01'", "CC", "21"},
		{"DATE '10000-01-01'", "CC YYYY", "100 10000"},
		// BC: years as written with the era, and the 1st century BC is -01.
		{"DATE '0044-03-15 BC'", "YYYY-MM-DD BC B.C. bc", "0044-03-15 BC B.C. bc"},
		{"DATE '0044-03-15 BC'", "CC FMCC Y,YYY YY J", "-01 -1 0,044 44 1705428"},
		{"DATE '0001-12-31 BC'", "YYYY CC AD", "0001 -01 BC"},
		{"DATE '0001-01-01'", "YYYY CC AD J", "0001 01 AD 1721426"},
		// Times of day.
		{ts, "HH:MI:SS HH12 HH24 FMHH12 FMMI FMSS", "01:05:09 01 13 1 5 9"},
		{ts, "SSSS SSSSS", "47109 47109"},
		{ts, "MS US FF1 FF2 FF3 FF4 FF5 FF6", "123 123456 1 12 123 1234 12345 123456"},
		{ts, "AM am A.M. a.m. PM pm P.M. p.m.", "PM pm P.M. p.m. PM pm P.M. p.m."},
		{"TIMESTAMP '2024-01-10 00:00:05.005'", "HH12 AM a.m. MS FMMS US FMUS FMFF2", "12 AM a.m. 005 005 005000 005000 00"},
		{"CAST('2024-01-10 10:00:00.123456789' AS TIMESTAMP(9))", "US FF6", "123456 123456"},
		{"TIME '13:05:09.5'", "HH24:MI:SS.MS", "13:05:09.500"},
		// Time zones: UTC for a timestamp with time zone and a date, nothing
		// for a timestamp.
		{ts, "[TZ][tz]", "[][]"},
		{"TIMESTAMPTZ '2024-01-10 13:05:09-05'", "HH24:MI TZ tz", "18:05 UTC utc"},
		{nov29, "TZ TZH:TZM OF FMOF", "UTC +00:00 +00 +0"},
		{ts, "TZH TZM OF", "+00 00 +00"},
		// Ordinals.
		{"DATE '2020-11-01'", "DDth DDTH FMDDth", "01st 01ST 1st"},
		{"DATE '2020-11-02'", "DDth", "02nd"}, {"DATE '2020-11-03'", "DDth", "03rd"}, {"DATE '2020-11-04'", "DDth", "04th"},
		{"DATE '2020-11-11'", "DDth", "11th"}, {"DATE '2020-11-12'", "DDth", "12th"}, {"DATE '2020-11-13'", "DDth", "13th"},
		{"DATE '2020-11-21'", "DDth", "21st"}, {"DATE '2020-11-22'", "DDTH", "22ND"}, {"DATE '2020-11-23'", "DDth", "23rd"},
		{nov29, "WWth Qth CCth Jth YYYYth Y,YYYth", "48th 4th 21st 2459183rd 2020th 2,020th"},
		{ts, "HH24th MIth SSTH MSth", "13th 05th 09TH 123rd"},
		// TH on a name, and SP, are accepted and ignored, as in Postgres.
		{nov29, "MonthTH|DDSP|DDthSP", "November |29|29thSP"},
		// FX only matters to TO_DATE / TO_TIMESTAMP.
		{nov29, "FXYYYY", "2020"},
		// Patterns are case-sensitive: lower-case numbers are the same
		// patterns, and other mixed cases are other patterns or literals.
		{nov29, "yyyy-mm-dd hh24:mi:ss iw cc j w ww", "2020-11-29 00:00:00 48 21 2459183 5 48"},
		{nov29, "Mm|Dd|yYYY", "Mm|11|0020"},
		// One prefix, and only before a pattern.
		{"DATE '2020-11-05'", "FM-DD", "-05"},
		{nov29, "FMTMMonth", "T11onth"},
		{nov29, "FM", ""},
		// Literals: quoted text (with \ escapes) and other characters.
		{nov29, `"Week "WW", day "D`, "Week 48, day 1"},
		{nov29, `"WW is" WW`, "WW is 48"},
		{nov29, `"a\"b"`, `a"b`},
		{nov29, `\"WW\"`, `"48"`},
		{nov29, `\WW`, `\48`},
		{nov29, `YYYY"`, "2020"},
		{nov29, "YYYY/MM/DD!@#%^&*()[]{}<>?+=~ é", "2020/11/29!@#%^&*()[]{}<>?+=~ é"},
	}
	var sc []scalarCase
	for _, c := range cases {
		sc = append(sc, scalarCase{"TO_CHAR(" + c.value + ", '" + c.format + "')", c.want, "VARCHAR"})
	}
	sc = append(sc,
		scalarCase{"TO_CHAR(DATE '2020-11-29', '')", "NULL", "VARCHAR"},
		scalarCase{"TO_CHAR(CAST(NULL AS DATE), 'YYYY')", "NULL", "VARCHAR"},
		// TO_DATE and TO_TIMESTAMP read the same templates.
		scalarCase{"TO_DATE('2024-03-15', 'yyyy-mm-dd')", "2024-03-15", "DATE"},
		scalarCase{"CAST(TO_TIMESTAMP('2024-03-15 14:30', 'FXYYYY-MM-DD HH24:MI') AS VARCHAR)", "2024-03-15 14:30:00+00", "VARCHAR"},
		scalarCase{`TO_DATE('2024 "x" 03', 'YYYY "\"x\"" MM')`, "2024-03-01", "DATE"},
	)
	runScalarCases(t, sc)
	// Every pattern is implemented, for each type TO_CHAR takes.
	for _, k := range dchKeywords {
		for _, v := range []string{"DATE '2020-11-29'", "TIMESTAMP '2020-11-29 10:00:00'", "TIMESTAMPTZ '2020-11-29 10:00:00+00'", "TIME '10:00:00'"} {
			if _, _, err := evalTestExpr(t, "TO_CHAR("+v+", '"+k.name+"')"); err != nil {
				t.Errorf("TO_CHAR(%s, '%s'): %v", v, k.name, err)
			}
		}
	}
	expectExprErrors(t, map[string]string{
		"TO_DATE('48 2020', 'WW YYYY')": `format pattern "WW" cannot be parsed`,
		"TO_DATE('2020', 'Y,YYY')":      `format pattern "Y,YYY" cannot be parsed`,
	})
}
