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

// Unit tests (no Redis) for the session time zone (#105): the TimeZone
// values SET takes, and constant expressions evaluated in a session time
// zone.

import (
	"testing"
	"time"
)

// zoneTestNow is the current time of the expressions below: 2024-01-10
// 05:00 UTC, which is 2024-01-09 21:00 in Los Angeles and 2024-01-10 10:30
// in Kolkata.
var zoneTestNow = time.Date(2024, 1, 10, 5, 0, 0, 0, time.UTC)

// evalInZone types and evaluates a constant expression as evalTestExpr
// does, in a session whose TimeZone is zone, and renders the result as text
// in that zone.
func evalInZone(t *testing.T, zone, expr string) (string, string, error) {
	t.Helper()
	return evalInZoneAt(t, zone, zoneTestNow, expr)
}

// evalInZoneAt is evalInZone with the current time now.
func evalInZoneAt(t *testing.T, zone string, now time.Time, expr string) (string, string, error) {
	t.Helper()
	tz, err := timeZoneParam(zone)
	if err != nil {
		t.Fatalf("zone %q: %v", zone, err)
	}
	sess := newSession(defaultSchema)
	sess.vals["timezone"] = tz
	e := &executor{sess: sess, cache: newExecCache(), now: now}
	x := parseTestExpr(t, expr)
	typ, err := inferType(x, nil, nil)
	if err != nil {
		return "", "", err
	}
	v, err := (&evalEnv{exec: e}).eval(x)
	if err != nil {
		return "", "", err
	}
	out, err := coerceIn(v, typ, tz.zone)
	if err != nil {
		t.Fatalf("%s: value %s (%s) does not fit the inferred type %s: %v", expr, v.textIn(tz.zone), v.T.SQLName(), typ.SQLName(), err)
	}
	if out.Null {
		return "NULL", typ.SQLName(), nil
	}
	return out.textIn(tz.zone), typ.SQLName(), nil
}

func runZoneCases(t *testing.T, zone string, cases []scalarCase) {
	t.Helper()
	runZoneCasesAt(t, zone, zoneTestNow, cases)
}

// runZoneCasesAt is runZoneCases with the current time now.
func runZoneCasesAt(t *testing.T, zone string, now time.Time, cases []scalarCase) {
	t.Helper()
	for _, c := range cases {
		got, typ, err := evalInZoneAt(t, zone, now, c.expr)
		if err != nil {
			if c.want != "error: "+errText(err) {
				t.Errorf("%s: %s: %v", zone, c.expr, err)
			}
			continue
		}
		if got != c.want || (c.typ != "" && typ != c.typ) {
			t.Errorf("%s: %s = %q (%s), want %q (%s)", zone, c.expr, got, typ, c.want, c.typ)
		}
	}
}

// The values of TimeZone, with the text SHOW gives and the offset they
// have on 2024-01-10 and 2024-07-10.
func TestSessionZoneValues(t *testing.T) {
	jan, jul := time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC).Unix(), time.Date(2024, 7, 10, 12, 0, 0, 0, time.UTC).Unix()
	for _, c := range []struct {
		value, shown string
		janOff       int // seconds east
		julOff       int
	}{
		// IANA names, case-insensitive, shown as the tz database spells them.
		{"America/Los_Angeles", "America/Los_Angeles", -8 * 3600, -7 * 3600},
		{"america/los_angeles", "America/Los_Angeles", -8 * 3600, -7 * 3600},
		{"US/Pacific", "US/Pacific", -8 * 3600, -7 * 3600},
		{"Asia/Kolkata", "Asia/Kolkata", 19800, 19800},
		{"ASIA/KOLKATA", "Asia/Kolkata", 19800, 19800},
		{"Australia/Sydney", "Australia/Sydney", 11 * 3600, 10 * 3600},
		{"America/St_Johns", "America/St_Johns", -(3*3600 + 1800), -(2*3600 + 1800)},
		{"UTC", "UTC", 0, 0},
		{"utc", "UTC", 0, 0},
		{"Etc/UTC", "Etc/UTC", 0, 0},
		{"GMT", "GMT", 0, 0},
		{"Etc/GMT+5", "Etc/GMT+5", -5 * 3600, -5 * 3600},
		{"EST5EDT", "EST5EDT", -5 * 3600, -4 * 3600},
		{"est5edt", "EST5EDT", -5 * 3600, -4 * 3600},
		// CET is the zone name, with summer time (not the abbreviation).
		{"CET", "CET", 3600, 7200},
		// POSIX-style offsets: west of Greenwich, shown in upper case.
		{"UTC+5", "UTC+5", -5 * 3600, -5 * 3600},
		{"utc+5", "UTC+5", -5 * 3600, -5 * 3600},
		{"+05:30", "+05:30", -19800, -19800},
		{"<+0530>-05:30", "<+0530>-05:30", 19800, 19800},
		// Numbers: hours east of Greenwich, shown as Postgres names them.
		{"-8", "<-08>+08", -8 * 3600, -8 * 3600},
		{"+3", "<+03>-03", 3 * 3600, 3 * 3600},
		{"5.5", "<+05:30>-05:30", 19800, 19800},
		{"0", "<+00>-00", 0, 0},
		// INTERVAL: east of Greenwich.
		{"INTERVAL '+05:30' HOUR TO MINUTE", "<+05:30>-05:30", 19800, 19800},
		{"INTERVAL '-08:00'", "<-08>+08", -8 * 3600, -8 * 3600},
		{"interval '-3 hours -30 minutes'", "<-03:30>+03:30", -(3*3600 + 1800), -(3*3600 + 1800)},
		{"INTERVAL '5' HOUR", "<+05>-05", 5 * 3600, 5 * 3600},
		// Abbreviations, which Postgres refuses here: fixed offsets.
		{"PST", "PST", -8 * 3600, -8 * 3600},
		{"pdt", "PDT", -7 * 3600, -7 * 3600},
		{"Z", "Z", 0, 0},
	} {
		v, err := timeZoneParam(c.value)
		if err != nil {
			t.Errorf("%q: %v", c.value, err)
			continue
		}
		if v.text != c.shown || v.zone.offsetAt(jan) != c.janOff || v.zone.offsetAt(jul) != c.julOff {
			t.Errorf("%q: shown %q, offsets %d and %d; want %q, %d and %d",
				c.value, v.text, v.zone.offsetAt(jan), v.zone.offsetAt(jul), c.shown, c.janOff, c.julOff)
		}
	}
	for value, want := range map[string]string{
		"Mars/Base":                "invalid value for parameter \"TimeZone\": \"Mars/Base\"",
		"":                         "invalid value for parameter \"TimeZone\": \"\"",
		"Local":                    "invalid value for parameter \"TimeZone\": \"Local\"",
		" UTC":                     "invalid value for parameter \"TimeZone\": \" UTC\"",
		"IST":                      "invalid value for parameter \"TimeZone\": \"IST\"",
		"200":                      "invalid value for parameter \"TimeZone\": \"200\"",
		"UTC+200":                  "invalid value for parameter \"TimeZone\": \"UTC+200\"",
		"CET-1CEST,M3.5.0,M10.5.0": "invalid value for parameter \"TimeZone\": \"CET-1CEST,M3.5.0,M10.5.0\"",
		"INTERVAL '1 day'":         "invalid value for parameter \"TimeZone\": \"INTERVAL '1 day'\" (cannot specify days in time zone interval)",
		"INTERVAL '1 month'":       "invalid value for parameter \"TimeZone\": \"INTERVAL '1 mon'\" (cannot specify months in time zone interval)",
		"INTERVAL '200 hours'":     "invalid value for parameter \"TimeZone\": \"INTERVAL '200:00:00'\"",
		"INTERVAL 'soon'":          "invalid value for parameter \"TimeZone\": \"INTERVAL 'soon'\"",
	} {
		if _, err := timeZoneParam(value); err == nil || errText(err) != want {
			t.Errorf("%q: error %v, want %q", value, err, want)
		}
	}
}

// What the session time zone changes, in Los Angeles (around its DST
// transitions on 2024-03-10 at 10:00 UTC and 2024-11-03 at 09:00 UTC).
func TestSessionZoneLosAngeles(t *testing.T) {
	runZoneCases(t, "America/Los_Angeles", []scalarCase{
		// A timestamp with time zone as text: its local time and offset.
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00+00' AS VARCHAR)", "2024-01-10 02:00:00-08", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-07-10 10:00:00.5+00' AS TEXT)", "2024-07-10 03:00:00.5-07", "VARCHAR"},
		{"TIMESTAMPTZ '2024-03-10 09:59:59+00' || '|'", "2024-03-10 01:59:59-08|", "VARCHAR"},
		{"TIMESTAMPTZ '2024-03-10 10:00:00+00' || '|'", "2024-03-10 03:00:00-07|", "VARCHAR"},
		{"CONCAT(TIMESTAMPTZ '2024-11-03 08:30:00+00', ' ', TIMESTAMPTZ '2024-11-03 09:30:00+00')",
			"2024-11-03 01:30:00-07 2024-11-03 01:30:00-08", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '1850-01-01 12:00:00+00' AS TEXT)", "1850-01-01 04:07:02-07:52:58", "VARCHAR"},
		{"UPPER(CAST(TIMESTAMPTZ '2024-01-10 10:00:00+00' AS TEXT))", "2024-01-10 02:00:00-08", "VARCHAR"},
		{"LENGTH(CAST(TIMESTAMPTZ '2024-01-10 10:00:00+00' AS TEXT))", "22", "BIGINT"},
		{"MD5(CAST(TIMESTAMPTZ '2024-01-10 10:00:00+00' AS TEXT)) = MD5('2024-01-10 02:00:00-08')", "true", "BOOLEAN"},
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00+00' AS VARCHAR(10))", "2024-01-10", "VARCHAR(10)"},
		{"CAST(TIMESTAMP '2024-01-10 10:00:00' AS TEXT)", "2024-01-10 10:00:00", "VARCHAR"},
		// Functions that read a value as text.
		{"LOWER(TIMESTAMPTZ '2024-01-10 10:00:00+00')", "2024-01-10 02:00:00-08", "VARCHAR"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' LIKE '%02:00:00-08'", "true", "BOOLEAN"},
		{"STARTS_WITH(TIMESTAMPTZ '2024-01-10 10:00:00+00', '2024-01-10 02')", "true", "BOOLEAN"},
		{"REGEXP_LIKE(TIMESTAMPTZ '2024-01-10 10:00:00+00', '-08$')", "true", "BOOLEAN"},
		{"REGEXP_REPLACE(TIMESTAMPTZ '2024-01-10 10:00:00+00', '-08$', ' PST')", "2024-01-10 02:00:00 PST", "VARCHAR"},
		{"SUBSTR(TIMESTAMPTZ '2024-01-10 10:00:00+00', 12)", "02:00:00-08", "VARCHAR"},

		// Text without an offset is a local time (DST gaps read with the
		// offset before the transition, overlaps with the one after it).
		{"TIMESTAMPTZ '2024-01-10 02:00:00' AT TIME ZONE 'UTC'", "2024-01-10 10:00:00", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMPTZ '2024-03-10 02:30:00' AS VARCHAR)", "2024-03-10 03:30:00-07", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-11-03 01:30:00' AS VARCHAR)", "2024-11-03 01:30:00-08", "VARCHAR"},
		{"TIMESTAMP WITH TIME ZONE '2024-07-01 12:00:00' AT TIME ZONE 'UTC'", "2024-07-01 19:00:00", "TIMESTAMP(6)"},
		{"CAST('2024-01-10 02:00' AS TIMESTAMPTZ) AT TIME ZONE 'UTC'", "2024-01-10 10:00:00", "TIMESTAMP(6)"},
		{"'2024-01-10 02:00'::timestamptz AT TIME ZONE 'UTC'", "2024-01-10 10:00:00", "TIMESTAMP(6)"},
		{"CAST(CAST('2024-01-10' AS TIMESTAMPTZ) AS TEXT)", "2024-01-10 00:00:00-08", "VARCHAR"},
		// An offset or a zone in the text is kept.
		{"CAST(CAST('2024-01-10 02:00:00+00' AS TIMESTAMPTZ) AS TEXT)", "2024-01-09 18:00:00-08", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-01-10 02:00:00 Asia/Kolkata' AS TEXT)", "2024-01-09 12:30:00-08", "VARCHAR"},

		// TIMESTAMP WITH TIME ZONE <-> TIMESTAMP: local times.
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00+00' AS TIMESTAMP)", "2024-01-10 02:00:00", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMPTZ '2024-11-03 08:30:00+00' AS TIMESTAMP)", "2024-11-03 01:30:00", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMPTZ '2024-11-03 09:30:00+00' AS TIMESTAMP(0))", "2024-11-03 01:30:00", "TIMESTAMP(0)"},
		{"CAST(TIMESTAMP '2024-01-10 02:00:00' AS TIMESTAMPTZ) AT TIME ZONE 'UTC'", "2024-01-10 10:00:00", "TIMESTAMP(6)"},
		{"CAST(CAST(TIMESTAMP '2024-03-10 02:30:00' AS TIMESTAMPTZ) AS TEXT)", "2024-03-10 03:30:00-07", "VARCHAR"},
		{"CAST(TIMESTAMP '2024-11-03 01:30:00' AS TIMESTAMPTZ) AT TIME ZONE 'UTC'", "2024-11-03 09:30:00", "TIMESTAMP(6)"},
		{"CAST(CAST(TIMESTAMP '2024-07-04 12:00:00.25' AS TIMESTAMPTZ) AS TIMESTAMP)", "2024-07-04 12:00:00.25", "TIMESTAMP(6)"},
		// DATE <-> TIMESTAMP WITH TIME ZONE: local midnight, the local date.
		{"CAST(CAST(DATE '2024-03-10' AS TIMESTAMPTZ) AS TEXT)", "2024-03-10 00:00:00-08", "VARCHAR"},
		{"CAST(DATE '2024-03-11' AS TIMESTAMPTZ) AT TIME ZONE 'UTC'", "2024-03-11 07:00:00", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMPTZ '2024-01-10 05:00:00+00' AS DATE)", "2024-01-09", "DATE"},
		{"CAST(TIMESTAMPTZ '2024-01-10 08:00:00+00' AS DATE)", "2024-01-10", "DATE"},
		{"CAST(TIMESTAMPTZ '2024-01-10 05:00:00+00' AS TIME)", "21:00:00", "TIME(6)"},

		// AT LOCAL and TIMEZONE(x); a date or text in AT TIME ZONE is read
		// as a local time.
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' AT LOCAL", "2024-01-10 02:00:00", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMP '2024-07-10 03:00:00' AT LOCAL AS TEXT)", "2024-07-10 03:00:00-07", "VARCHAR"},
		{"TIMESTAMP '2024-07-10 03:00:00' AT LOCAL AT TIME ZONE 'UTC'", "2024-07-10 10:00:00", "TIMESTAMP(6)"},
		{"TIMEZONE(TIMESTAMPTZ '2024-07-10 10:00:00+00')", "2024-07-10 03:00:00", "TIMESTAMP(6)"},
		{"'2024-01-10 02:00:00' AT TIME ZONE 'UTC'", "2024-01-10 10:00:00", "TIMESTAMP(6)"},
		{"DATE '2024-01-10' AT TIME ZONE 'UTC'", "2024-01-10 08:00:00", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMP '2024-01-10 02:00:00' AT TIME ZONE 'UTC' AS TEXT)", "2024-01-09 18:00:00-08", "VARCHAR"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' AT TIME ZONE 'Asia/Kolkata'", "2024-01-10 15:30:00", "TIMESTAMP(6)"},

		// DATE_TRUNC: days and longer units start at local midnight; shorter
		// ones keep the offset (01:30 PST truncates to 01:00 PST).
		{"CAST(DATE_TRUNC('day', TIMESTAMPTZ '2024-01-10 05:00:00+00') AS TEXT)", "2024-01-09 00:00:00-08", "VARCHAR"},
		{"CAST(DATE_TRUNC('day', TIMESTAMPTZ '2024-03-10 12:00:00+00') AS TEXT)", "2024-03-10 00:00:00-08", "VARCHAR"},
		{"CAST(DATE_TRUNC('hour', TIMESTAMPTZ '2024-11-03 08:30:00+00') AS TEXT)", "2024-11-03 01:00:00-07", "VARCHAR"},
		{"CAST(DATE_TRUNC('hour', TIMESTAMPTZ '2024-11-03 09:30:00+00') AS TEXT)", "2024-11-03 01:00:00-08", "VARCHAR"},
		{"CAST(DATE_TRUNC('month', TIMESTAMPTZ '2024-03-20 12:00:00+00') AS TEXT)", "2024-03-01 00:00:00-08", "VARCHAR"},
		{"CAST(DATE_TRUNC('week', TIMESTAMPTZ '2024-03-12 06:00:00+00') AS TEXT)", "2024-03-11 00:00:00-07", "VARCHAR"},
		{"CAST(DATE_TRUNC('year', TIMESTAMPTZ '2025-01-01 05:00:00+00') AS TEXT)", "2024-01-01 00:00:00-08", "VARCHAR"},
		{"CAST(DATE_TRUNC('minute', TIMESTAMPTZ '2024-01-10 05:00:30.5+00') AS TEXT)", "2024-01-09 21:00:00-08", "VARCHAR"},
		{"DATE_TRUNC('day', TIMESTAMP '2024-01-10 05:00:00')", "2024-01-10 00:00:00", "TIMESTAMP(6)"},

		// EXTRACT / DATE_PART and the field functions: local fields; epoch
		// is the instant's; timezone* the offset.
		{"EXTRACT(hour FROM TIMESTAMPTZ '2024-01-10 05:00:00+00')", "21", "BIGINT"},
		{"EXTRACT(day FROM TIMESTAMPTZ '2024-01-10 05:00:00+00')", "9", "BIGINT"},
		{"EXTRACT(dow FROM TIMESTAMPTZ '2024-01-10 05:00:00+00')", "2", "BIGINT"},
		{"DATE_PART('year', TIMESTAMPTZ '2025-01-01 05:00:00+00')", "2024", "BIGINT"},
		{"CAST(EXTRACT(epoch FROM TIMESTAMPTZ '2024-01-10 05:00:00+00') AS BIGINT)", "1704862800", "BIGINT"},
		{"EXTRACT(timezone FROM TIMESTAMPTZ '2024-01-10 05:00:00+00')", "-28800", "BIGINT"},
		{"EXTRACT(timezone_hour FROM TIMESTAMPTZ '2024-07-10 05:00:00+00')", "-7", "BIGINT"},
		{"EXTRACT(timezone_minute FROM TIMESTAMPTZ '2024-07-10 05:00:00+00')", "0", "BIGINT"},
		{"HOUR(TIMESTAMPTZ '2024-03-10 10:30:00+00')", "3", "BIGINT"},
		{"EXTRACT(hour FROM TIMESTAMP '2024-01-10 05:00:00')", "5", "BIGINT"},
		{"EXTRACT(timezone FROM TIMESTAMP '2024-01-10 05:00:00')", `error: unit "timezone" not supported for type timestamp without time zone`, ""},

		// DATE_BIN: the stride is elapsed time; the origin is read as a
		// local time.
		{"CAST(DATE_BIN(INTERVAL '1 day', TIMESTAMPTZ '2024-01-10 05:00:00+00', TIMESTAMPTZ '2024-01-01 00:00:00') AS TEXT)",
			"2024-01-09 00:00:00-08", "VARCHAR"},
		{"CAST(DATE_BIN(INTERVAL '1 day', TIMESTAMPTZ '2024-03-12 12:00:00+00', TIMESTAMPTZ '2024-03-01 00:00:00') AS TEXT)",
			"2024-03-12 01:00:00-07", "VARCHAR"},
		{"CAST(DATE_BIN(INTERVAL '15 minutes', TIMESTAMPTZ '2024-01-10 05:07:00+00', TIMESTAMPTZ '2024-01-01 00:00:00+00') AS TEXT)",
			"2024-01-09 21:00:00-08", "VARCHAR"},
		{"CAST(DATE_BIN(INTERVAL '1 day', TIMESTAMPTZ '2024-01-10 05:00:00+00', '2024-01-01') AS TEXT)", "2024-01-09 00:00:00-08", "VARCHAR"},
		{"DATE_BIN('1 hour', TIMESTAMP '2024-01-10 05:30:00', TIMESTAMP '2024-01-01 00:00:00')", "2024-01-10 05:00:00", "TIMESTAMP(6)"},
		{"DATE_BIN(INTERVAL '1 hour', TIMESTAMP '2024-01-10 05:30:00', TIMESTAMP '2024-01-10 07:15:00')", "2024-01-10 05:15:00", "TIMESTAMP(6)"},
		// Postgres's choice of form: TIMESTAMP if an argument is one and
		// none has a time zone.
		{"DATE_BIN(INTERVAL '1 day', TIMESTAMP '2024-01-10 05:30:00', DATE '2024-01-01')", "2024-01-10 00:00:00", "TIMESTAMP(6)"},
		{"DATE_BIN(INTERVAL '1 day', DATE '2024-01-10', TIMESTAMP '2024-01-01 00:00:00')", "2024-01-10 00:00:00", "TIMESTAMP(6)"},
		{"DATE_BIN(INTERVAL '1 day', DATE '2024-01-10', DATE '2024-01-01')", "2024-01-10 00:00:00-08", "TIMESTAMP(6) WITH TIME ZONE"},
		{"DATE_BIN(INTERVAL '1 day', TIMESTAMP '2024-01-10 05:30:00', TIMESTAMPTZ '2024-01-01 00:00:00+00')",
			"2024-01-09 16:00:00-08", "TIMESTAMP(6) WITH TIME ZONE"},
		{"DATE_BIN(INTERVAL '1 month', TIMESTAMP '2024-01-10 05:30:00', TIMESTAMP '2024-01-01 00:00:00')",
			"error: timestamps cannot be binned into intervals containing months or years", ""},
		{"DATE_BIN(INTERVAL '0 days', TIMESTAMP '2024-01-10 05:30:00', TIMESTAMP '2024-01-01 00:00:00')",
			"error: stride must be greater than zero", ""},

		// The current date and time (2024-01-10 05:00 UTC).
		{"CURRENT_DATE", "2024-01-09", "DATE"},
		{"LOCALTIMESTAMP", "2024-01-09 21:00:00", "TIMESTAMP(6)"},
		{"CURRENT_TIME", "21:00:00", "TIME(6)"},
		{"LOCALTIME", "21:00:00", "TIME(6)"},
		{"CAST(CURRENT_TIMESTAMP AS TEXT)", "2024-01-09 21:00:00-08", "VARCHAR"},
		{"NOW() AT TIME ZONE 'UTC'", "2024-01-10 05:00:00", "TIMESTAMP(6)"},
		{"CAST(CAST(CURRENT_DATE AS TIMESTAMPTZ) AS TEXT)", "2024-01-09 00:00:00-08", "VARCHAR"},
		{"AGE(TIMESTAMPTZ '2024-01-01 00:00:00-08')", "8 days", "INTERVAL"},

		// MAKE_TIMESTAMPTZ and TO_TIMESTAMP(text, format): local times.
		{"CAST(MAKE_TIMESTAMPTZ(2024, 1, 10, 2, 0, 0) AS TEXT)", "2024-01-10 02:00:00-08", "VARCHAR"},
		{"CAST(MAKE_TIMESTAMPTZ(2024, 3, 10, 2, 30, 0) AS TEXT)", "2024-03-10 03:30:00-07", "VARCHAR"},
		{"MAKE_TIMESTAMPTZ(2024, 7, 1, 12, 0, 0.5) AT TIME ZONE 'UTC'", "2024-07-01 19:00:00.5", "TIMESTAMP(6)"},
		{"CAST(TO_TIMESTAMP('2024-01-10 02:00', 'YYYY-MM-DD HH24:MI') AS TEXT)", "2024-01-10 02:00:00-08", "VARCHAR"},
		{"CAST(TO_TIMESTAMP(0) AS TEXT)", "1969-12-31 16:00:00-08", "VARCHAR"},

		// TO_CHAR: TZ is the zone's abbreviation, OF / TZH / TZM its offset.
		{"TO_CHAR(TIMESTAMPTZ '2024-01-10 10:00:00+00', 'YYYY-MM-DD HH24:MI:SS TZ tz OF TZH:TZM')",
			"2024-01-10 02:00:00 PST pst -08 -08:00", "VARCHAR"},
		{"TO_CHAR(TIMESTAMPTZ '2024-07-10 10:00:00+00', 'HH12:MI AM TZ OF')", "03:00 AM PDT -07", "VARCHAR"},
		{"TO_CHAR(DATE '2024-07-10', 'YYYY-MM-DD HH24 TZ OF')", "2024-07-10 00 PDT -07", "VARCHAR"},
		{"TO_CHAR(TIMESTAMP '2024-07-10 10:00:00', 'HH24 TZ|OF')", "10 |+00", "VARCHAR"},
		{"TO_CHAR(TIMESTAMPTZ '1850-01-01 12:00:00+00', 'HH24:MI:SS TZ OF')", "04:07:02 LMT -07:52", "VARCHAR"},

		// JSON.
		{"TO_JSON(TIMESTAMPTZ '2024-01-10 10:00:00.5+00')", `"2024-01-10T02:00:00.5-08:00"`, ""},
		{"JSON_BUILD_OBJECT('t', TIMESTAMPTZ '2024-07-10 10:00:00+00')", `{"t" : "2024-07-10T03:00:00-07:00"}`, ""},
		{"JSON_BUILD_ARRAY(TIMESTAMPTZ '2024-07-10 10:00:00+00', TIMESTAMP '2024-07-10 10:00:00')",
			`["2024-07-10T03:00:00-07:00", "2024-07-10T10:00:00"]`, ""},

		// Comparisons: a TIMESTAMP, a DATE or text is a local time.
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' = TIMESTAMP '2024-01-10 02:00:00'", "true", "BOOLEAN"},
		{"TIMESTAMPTZ '2024-01-10 08:00:00+00' = DATE '2024-01-10'", "true", "BOOLEAN"},
		{"DATE '2024-01-10' < TIMESTAMPTZ '2024-01-10 07:59:59+00'", "false", "BOOLEAN"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' = '2024-01-10 02:00'", "true", "BOOLEAN"},
		{"'2024-01-10 02:00' = TIMESTAMPTZ '2024-01-10 10:00:00+00'", "true", "BOOLEAN"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' > TIMESTAMP '2024-01-10 02:30:00'", "false", "BOOLEAN"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' IS DISTINCT FROM TIMESTAMP '2024-01-10 02:00:00'", "false", "BOOLEAN"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' IN (TIMESTAMP '2024-01-10 02:00:00', TIMESTAMP '2024-01-10 03:00:00')", "true", "BOOLEAN"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' IN ('2024-01-10 01:00', '2024-01-10 02:00', '2024-01-10 03:00', '2024-01-10 04:00')",
			"true", "BOOLEAN"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' BETWEEN '2024-01-10 01:00' AND '2024-01-10 02:00'", "true", "BOOLEAN"},
		{"CAST(GREATEST(TIMESTAMPTZ '2024-01-10 09:00:00+00', TIMESTAMP '2024-01-10 02:00:00') AS TEXT)", "2024-01-10 02:00:00-08", "VARCHAR"},
		{"COALESCE(NULL, TIMESTAMP '2024-01-10 02:00:00', TIMESTAMPTZ '2024-01-01 00:00:00+00') AT TIME ZONE 'UTC'",
			"2024-01-10 10:00:00", "TIMESTAMP(6)"},
		{"CASE WHEN true THEN TIMESTAMP '2024-01-10 02:00:00' ELSE TIMESTAMPTZ '2024-01-01 00:00:00+00' END",
			"2024-01-10 02:00:00-08", "TIMESTAMP(6) WITH TIME ZONE"},
		// A CASE or IIF branch is converted to the result's type, also where
		// the value is used inside the query.
		{"CAST(CASE WHEN true THEN TIMESTAMP '2024-01-10 01:00:00' ELSE TIMESTAMPTZ '2024-01-10 05:00:00+00' END AS TEXT)",
			"2024-01-10 01:00:00-08", "VARCHAR"},
		{"CASE WHEN true THEN TIMESTAMP '2024-01-10 01:00:00' ELSE TIMESTAMPTZ '2024-01-10 05:00:00+00' END > TIMESTAMPTZ '2024-01-10 08:30:00+00'",
			"true", "BOOLEAN"},
		{"CAST(IIF(true, DATE '2024-01-10', TIMESTAMPTZ '2024-01-10 05:00:00+00') AS TEXT)", "2024-01-10 00:00:00-08", "VARCHAR"},

		// Arithmetic: months and days on the local time (a day is 23 hours
		// across the spring-forward transition), the time part elapsed.
		{"CAST(TIMESTAMPTZ '2024-03-09 12:00:00-08' + INTERVAL '1 day' AS TEXT)", "2024-03-10 12:00:00-07", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-03-09 12:00:00-08' + INTERVAL '24 hours' AS TEXT)", "2024-03-10 13:00:00-07", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-02-10 12:00:00-08' + INTERVAL '1 month' AS TEXT)", "2024-03-10 12:00:00-07", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-03-10 12:00:00-07' - INTERVAL '1 day' AS TEXT)", "2024-03-09 12:00:00-08", "VARCHAR"},
		{"CAST(INTERVAL '1 day' + TIMESTAMPTZ '2024-11-02 12:00:00-07' AS TEXT)", "2024-11-03 12:00:00-08", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-03-10 00:30:00-08' + INTERVAL '1 day 2 hours' AS TEXT)", "2024-03-11 02:30:00-07", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-03-09 02:30:00-08' + INTERVAL '1 day' AS TEXT)", "2024-03-10 03:30:00-07", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-03-10 12:00:00-07' - TIMESTAMPTZ '2024-03-09 12:00:00-08' AS TEXT)", "23:00:00", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-03-10 12:00:00-07' - TIMESTAMP '2024-03-09 12:00:00' AS TEXT)", "23:00:00", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-03-11 00:00:00-07' - DATE '2024-03-10' AS TEXT)", "23:00:00", "VARCHAR"},
		{"CAST(TIMESTAMP '2024-03-09 12:00:00' + INTERVAL '1 day' AS TEXT)", "2024-03-10 12:00:00", "VARCHAR"},
		{"CAST(DATEADD(day, 1, TIMESTAMPTZ '2024-03-09 12:00:00-08') AS TEXT)", "2024-03-10 12:00:00-07", "VARCHAR"},
		{"DATEDIFF(day, TIMESTAMPTZ '2024-01-10 06:00:00+00', TIMESTAMPTZ '2024-01-10 09:00:00+00')", "1", "BIGINT"},
		{"AGE(TIMESTAMPTZ '2024-03-11 00:00:00-07', TIMESTAMPTZ '2024-03-10 00:00:00-08')", "1 day", "INTERVAL"},
		{"LAST_DAY(TIMESTAMPTZ '2024-03-01 05:00:00+00')", "2024-02-29", "DATE"},
	})
}

// The same in Kolkata (UTC+05:30, no DST), whose half-hour offset shows in
// the fields too.
func TestSessionZoneKolkata(t *testing.T) {
	runZoneCases(t, "Asia/Kolkata", []scalarCase{
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00+00' AS TEXT)", "2024-01-10 15:30:00+05:30", "VARCHAR"},
		{"CAST(TIMESTAMPTZ '2024-07-10 10:00:00.25+00' AS TEXT)", "2024-07-10 15:30:00.25+05:30", "VARCHAR"},
		{"TIMESTAMPTZ '2024-01-10 15:30:00' AT TIME ZONE 'UTC'", "2024-01-10 10:00:00", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMPTZ '2024-01-10 20:00:00+00' AS DATE)", "2024-01-11", "DATE"},
		{"CAST(TIMESTAMPTZ '2024-01-10 20:00:00+00' AS TIMESTAMP)", "2024-01-11 01:30:00", "TIMESTAMP(6)"},
		{"CAST(CAST(DATE '2024-01-10' AS TIMESTAMPTZ) AS TEXT)", "2024-01-10 00:00:00+05:30", "VARCHAR"},
		{"CAST(TIMESTAMP '2024-01-10 15:30:00' AS TIMESTAMPTZ) AT TIME ZONE 'UTC'", "2024-01-10 10:00:00", "TIMESTAMP(6)"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' AT LOCAL", "2024-01-10 15:30:00", "TIMESTAMP(6)"},
		{"CAST(DATE_TRUNC('hour', TIMESTAMPTZ '2024-01-10 05:00:00+00') AS TEXT)", "2024-01-10 10:00:00+05:30", "VARCHAR"},
		{"CAST(DATE_TRUNC('day', TIMESTAMPTZ '2024-01-10 20:00:00+00') AS TEXT)", "2024-01-11 00:00:00+05:30", "VARCHAR"},
		{"EXTRACT(minute FROM TIMESTAMPTZ '2024-01-10 05:00:00+00')", "30", "BIGINT"},
		{"EXTRACT(timezone FROM TIMESTAMPTZ '2024-01-10 05:00:00+00')", "19800", "BIGINT"},
		{"EXTRACT(timezone_hour FROM TIMESTAMPTZ '2024-01-10 05:00:00+00')", "5", "BIGINT"},
		{"EXTRACT(timezone_minute FROM TIMESTAMPTZ '2024-01-10 05:00:00+00')", "30", "BIGINT"},
		{"TO_CHAR(TIMESTAMPTZ '2024-01-10 10:00:00+00', 'HH24:MI TZ OF TZH TZM')", "15:30 IST +05:30 +05 30", "VARCHAR"},
		{"CURRENT_DATE", "2024-01-10", "DATE"},
		{"LOCALTIMESTAMP", "2024-01-10 10:30:00", "TIMESTAMP(6)"},
		{"CURRENT_TIME", "10:30:00", "TIME(6)"},
		{"AGE(TIMESTAMPTZ '2024-01-01 00:00:00-08')", "8 days 10:30:00", "INTERVAL"},
		{"CAST(DATE_BIN(INTERVAL '1 day', TIMESTAMPTZ '2024-01-10 20:00:00+00', TIMESTAMPTZ '2024-01-01 00:00:00') AS TEXT)",
			"2024-01-11 00:00:00+05:30", "VARCHAR"},
		{"TO_JSON(TIMESTAMPTZ '2024-01-10 10:00:00.5+00')", `"2024-01-10T15:30:00.5+05:30"`, ""},
		{"CAST(CAST(TIMESTAMPTZ '2024-01-10 23:59:59.9+00' AS TIMESTAMPTZ(0)) AS TEXT)", "2024-01-11 05:30:00+05:30", "VARCHAR"},
		{"MAKE_TIMESTAMPTZ(2024, 1, 10, 2, 0, 0) AT TIME ZONE 'UTC'", "2024-01-09 20:30:00", "TIMESTAMP(6)"},
		{"CAST(TIMESTAMPTZ '2024-01-10 10:00:00+00' + INTERVAL '1 day' AS TEXT)", "2024-01-11 15:30:00+05:30", "VARCHAR"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00+00' = TIMESTAMP '2024-01-10 15:30:00'", "true", "BOOLEAN"},
		{"TIMESTAMPTZ '2024-01-09 18:30:00+00' = DATE '2024-01-10'", "true", "BOOLEAN"},
		{"DATEDIFF(day, TIMESTAMPTZ '2024-01-10 17:00:00+00', TIMESTAMPTZ '2024-01-10 19:00:00+00')", "1", "BIGINT"},
	})
}

// Other kinds of zone: POSIX-style, numeric and INTERVAL offsets,
// abbreviations, half-hour and southern-hemisphere zones.
func TestSessionZoneKinds(t *testing.T) {
	const jan, jul = "TIMESTAMPTZ '2024-01-10 10:00:00+00'", "TIMESTAMPTZ '2024-07-10 10:00:00+00'"
	for zone, want := range map[string][2]string{
		"UTC":                              {"2024-01-10 10:00:00+00 UTC", "2024-07-10 10:00:00+00 UTC"},
		"UTC+5":                            {"2024-01-10 05:00:00-05 UTC", "2024-07-10 05:00:00-05 UTC"},
		"-8":                               {"2024-01-10 02:00:00-08 -08", "2024-07-10 02:00:00-08 -08"},
		"INTERVAL '+05:30' HOUR TO MINUTE": {"2024-01-10 15:30:00+05:30 +05:30", "2024-07-10 15:30:00+05:30 +05:30"},
		"PST":                              {"2024-01-10 02:00:00-08 PST", "2024-07-10 02:00:00-08 PST"},
		"CET":                              {"2024-01-10 11:00:00+01 CET", "2024-07-10 12:00:00+02 CEST"},
		"Australia/Sydney":                 {"2024-01-10 21:00:00+11 AEDT", "2024-07-10 20:00:00+10 AEST"},
		"America/St_Johns":                 {"2024-01-10 06:30:00-03:30 NST", "2024-07-10 07:30:00-02:30 NDT"},
	} {
		runZoneCases(t, zone, []scalarCase{
			{"CAST(" + jan + " AS TEXT) || ' ' || TO_CHAR(" + jan + ", 'TZ')", want[0], "VARCHAR"},
			{"CAST(" + jul + " AS TEXT) || ' ' || TO_CHAR(" + jul + ", 'TZ')", want[1], "VARCHAR"},
		})
	}
	// A date's local midnight in a DST gap (Chile springs forward at
	// midnight) is 01:00, as Postgres converts it.
	runZoneCases(t, "America/Santiago", []scalarCase{
		{"TO_CHAR(DATE '2024-09-08', 'HH24 TZ OF')", "01 -03 -03", "VARCHAR"},
		{"TO_CHAR(DATE '2024-09-07', 'HH24 TZ OF')", "00 -04 -04", "VARCHAR"},
		{"CAST(CAST(DATE '2024-09-08' AS TIMESTAMPTZ) AS TEXT)", "2024-09-08 01:00:00-03", "VARCHAR"},
	})
	runZoneCases(t, "UTC", []scalarCase{
		{"CAST(CASE WHEN true THEN DATE '2024-01-10' ELSE TIMESTAMP '2024-01-10 05:00:00' END AS TEXT)", "2024-01-10 00:00:00", "VARCHAR"},
	})
	runZoneCases(t, "America/St_Johns", []scalarCase{
		{"TO_CHAR(TIMESTAMPTZ '2024-01-10 10:00:00+00', 'OF|TZH|TZM')", "-03:30|-03|30", "VARCHAR"},
		{"EXTRACT(timezone_minute FROM TIMESTAMPTZ '2024-01-10 10:00:00+00')", "-30", "BIGINT"},
	})
}
