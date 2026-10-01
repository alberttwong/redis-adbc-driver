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
	"archive/zip"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// expectExprErrors checks the exact errors of constant expressions.
func expectExprErrors(t *testing.T, cases map[string]string) {
	t.Helper()
	for expr, want := range cases {
		got, _, err := evalTestExpr(t, expr)
		if err == nil || errText(err) != want {
			t.Errorf("%s: got %q, error %v; want error %q", expr, got, err, want)
		}
	}
}

// AT TIME ZONE, TIMEZONE() and CONVERT_TIMEZONE(), evaluated without Redis.
func TestAtTimeZone(t *testing.T) {
	runScalarCases(t, []scalarCase{
		// timestamp AT TIME ZONE z: a local time in z, giving the instant.
		{"TIMESTAMP '2021-06-07 14:35:20' AT TIME ZONE 'UTC'", "2021-06-07 14:35:20+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"TIMESTAMP '2021-06-07 14:35:20' AT TIME ZONE 'America/Los_Angeles'", "2021-06-07 21:35:20+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"TIMEZONE('Asia/Tokyo', TIMESTAMP '2021-06-07 14:35:20')", "2021-06-07 05:35:20+00", "TIMESTAMP(6) WITH TIME ZONE"},
		// timestamptz AT TIME ZONE z: the local time in z.
		{"TIMESTAMPTZ '2021-06-07 14:35:20+00' AT TIME ZONE 'America/Los_Angeles'", "2021-06-07 07:35:20", "TIMESTAMP(6)"},
		{"TIMEZONE('America/Los_Angeles', CAST('2021-06-07 14:35:20+00' AS TIMESTAMPTZ))", "2021-06-07 07:35:20", "TIMESTAMP(6)"},
		// The unit is kept.
		{"CAST('2024-01-10 10:00:00.5' AS TIMESTAMP(3)) AT TIME ZONE 'Asia/Kolkata'", "2024-01-10 04:30:00.5+00", "TIMESTAMP(3) WITH TIME ZONE"},
		{"CAST('2024-01-10 10:00:00.123456789' AS TIMESTAMP(9) WITH TIME ZONE) AT TIME ZONE 'Asia/Kolkata'", "2024-01-10 15:30:00.123456789", "TIMESTAMP(9)"},
		{"CAST('2024-01-10 10:00:00' AS TIMESTAMP(0)) AT TIME ZONE 'Asia/Kolkata'", "2024-01-10 04:30:00+00", "TIMESTAMP(0) WITH TIME ZONE"},
		// A date and text are read as a timestamp with time zone.
		{"DATE '2024-01-10' AT TIME ZONE 'Asia/Tokyo'", "2024-01-10 09:00:00", "TIMESTAMP(6)"},
		{"'2024-01-10 10:00:00' AT TIME ZONE 'Asia/Tokyo'", "2024-01-10 19:00:00", "TIMESTAMP(6)"},
		{"'2024-01-10 10:00:00-05' AT TIME ZONE 'UTC'", "2024-01-10 15:00:00", "TIMESTAMP(6)"},
		// AT LOCAL and TIMEZONE(x) use the session time zone, UTC.
		{"TIMESTAMP '2024-01-10 10:00:00' AT LOCAL", "2024-01-10 10:00:00+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"TIMESTAMPTZ '2024-01-10 10:00:00-05' AT LOCAL", "2024-01-10 15:00:00", "TIMESTAMP(6)"},
		{"TIMEZONE(TIMESTAMPTZ '2024-01-10 10:00:00-05')", "2024-01-10 15:00:00", "TIMESTAMP(6)"},
		// Round trips.
		{"(TIMESTAMP '2024-06-01 12:00:00' AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata'", "2024-06-01 12:00:00", "TIMESTAMP(6)"},
		{"(TIMESTAMPTZ '2024-11-03 09:30:00+00' AT TIME ZONE 'America/Los_Angeles') AT TIME ZONE 'America/Los_Angeles'", "2024-11-03 09:30:00+00", "TIMESTAMP(6) WITH TIME ZONE"},

		// Precedence, as in Postgres: tighter than + and *, looser than ::
		// and unary minus, left to right.
		{"TIMESTAMP '2024-01-10 10:00:00' AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo'", "2024-01-10 19:00:00", "TIMESTAMP(6)"},
		{"CAST(CAST('2021-06-07 14:35:20' AS TIMESTAMP) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles' AS TIMESTAMP)", "2021-06-07 07:35:20", "TIMESTAMP(6)"},
		{"TIMESTAMP '2024-01-10 10:00:00' AT TIME ZONE 'UTC' + INTERVAL '1 hour'", "2024-01-10 11:00:00+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"INTERVAL '1 hour' + TIMESTAMP '2024-01-10 10:00:00' AT TIME ZONE 'UTC'", "2024-01-10 11:00:00+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"'2024-01-10 10:00:00'::timestamp AT TIME ZONE 'Asia/Tokyo'", "2024-01-10 01:00:00+00", "TIMESTAMP(6) WITH TIME ZONE"},
		{"TIMESTAMP '2024-01-10 10:00:00' AT TIME ZONE 'UTC' || '!'", "2024-01-10 10:00:00+00!", "VARCHAR"},

		// CONVERT_TIMEZONE(src, tgt, x): x read in src, shown in tgt.
		{"CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST('2021-06-07 14:35:20' AS TIMESTAMP))", "2021-06-07 07:35:20", "TIMESTAMP(6)"},
		{"CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', TIMESTAMP '2024-07-01 09:00:00')", "2024-07-01 17:00:00", "TIMESTAMP(6)"},
		{"CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', TIMESTAMP '2024-06-01 12:00:00'))", "2024-06-01 12:00:00", "TIMESTAMP(6)"},
		{"CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CAST('2024-01-10 10:00:00.25' AS TIMESTAMP(3)))", "2024-01-10 15:30:00.25", "TIMESTAMP(3)"},
		// A timestamp with time zone is read as its UTC time, a date as
		// midnight.
		{"CONVERT_TIMEZONE('America/New_York', 'UTC', TIMESTAMPTZ '2024-01-10 10:00:00+00')", "2024-01-10 15:00:00", "TIMESTAMP(6)"},
		{"CONVERT_TIMEZONE('UTC', 'America/New_York', DATE '2024-01-10')", "2024-01-09 19:00:00", "TIMESTAMP(6)"},
		// CONVERT_TIMEZONE(tgt, x) reads x in UTC.
		{"CONVERT_TIMEZONE('America/New_York', TIMESTAMP '2024-01-10 10:00:00')", "2024-01-10 05:00:00", "TIMESTAMP(6)"},
		{"CONVERT_TIMEZONE('Asia/Tokyo', TIMESTAMPTZ '2024-01-10 10:00:00+00')", "2024-01-10 19:00:00", "TIMESTAMP(6)"},

		// NULLs.
		{"CAST(NULL AS TIMESTAMP) AT TIME ZONE 'UTC'", "NULL", "TIMESTAMP(6) WITH TIME ZONE"},
		{"TIMESTAMP '2024-01-10 10:00:00' AT TIME ZONE NULL", "NULL", "TIMESTAMP(6) WITH TIME ZONE"},
		{"CAST(NULL AS TIMESTAMPTZ) AT TIME ZONE 'UTC'", "NULL", "TIMESTAMP(6)"},
		{"NULL AT TIME ZONE 'UTC'", "NULL", "TIMESTAMP(6)"},
		{"CONVERT_TIMEZONE(NULL, 'UTC', TIMESTAMP '2024-01-10 10:00:00')", "NULL", "TIMESTAMP(6)"},
		{"CONVERT_TIMEZONE('UTC', CAST(NULL AS TIMESTAMP(3)))", "NULL", "TIMESTAMP(3)"},
	})
}

// Local times across DST transitions, both ways, as Postgres resolves them:
// a local time that the spring-forward transition skips is read with the
// offset before it, and one that the fall-back transition repeats with the
// offset after it.
func TestAtTimeZoneDST(t *testing.T) {
	cases := []scalarCase{
		// America/Los_Angeles, 2024-03-10: 02:00 PST becomes 03:00 PDT at
		// 10:00 UTC.
		{"TIMESTAMP '2024-03-10 01:59:59' AT TIME ZONE 'America/Los_Angeles'", "2024-03-10 09:59:59+00", ""},
		{"TIMESTAMP '2024-03-10 02:00:00' AT TIME ZONE 'America/Los_Angeles'", "2024-03-10 10:00:00+00", ""},
		{"TIMESTAMP '2024-03-10 02:30:00' AT TIME ZONE 'America/Los_Angeles'", "2024-03-10 10:30:00+00", ""},
		{"TIMESTAMP '2024-03-10 03:00:00' AT TIME ZONE 'America/Los_Angeles'", "2024-03-10 10:00:00+00", ""},
		{"TIMESTAMP '2024-03-10 03:30:00' AT TIME ZONE 'America/Los_Angeles'", "2024-03-10 10:30:00+00", ""},
		{"TIMESTAMPTZ '2024-03-10 09:59:59+00' AT TIME ZONE 'America/Los_Angeles'", "2024-03-10 01:59:59", ""},
		{"TIMESTAMPTZ '2024-03-10 10:00:00+00' AT TIME ZONE 'America/Los_Angeles'", "2024-03-10 03:00:00", ""},
		// 2024-11-03: 02:00 PDT becomes 01:00 PST at 09:00 UTC.
		{"TIMESTAMP '2024-11-03 00:59:59' AT TIME ZONE 'America/Los_Angeles'", "2024-11-03 07:59:59+00", ""},
		{"TIMESTAMP '2024-11-03 01:00:00' AT TIME ZONE 'America/Los_Angeles'", "2024-11-03 09:00:00+00", ""},
		{"TIMESTAMP '2024-11-03 01:30:00' AT TIME ZONE 'America/Los_Angeles'", "2024-11-03 09:30:00+00", ""},
		{"TIMESTAMP '2024-11-03 02:00:00' AT TIME ZONE 'America/Los_Angeles'", "2024-11-03 10:00:00+00", ""},
		{"TIMESTAMPTZ '2024-11-03 08:30:00+00' AT TIME ZONE 'America/Los_Angeles'", "2024-11-03 01:30:00", ""},
		{"TIMESTAMPTZ '2024-11-03 09:30:00+00' AT TIME ZONE 'America/Los_Angeles'", "2024-11-03 01:30:00", ""},
		// Europe/London, 2024-03-31: 01:00 GMT becomes 02:00 BST at 01:00
		// UTC.
		{"TIMESTAMP '2024-03-31 00:59:59' AT TIME ZONE 'Europe/London'", "2024-03-31 00:59:59+00", ""},
		{"TIMESTAMP '2024-03-31 01:30:00' AT TIME ZONE 'Europe/London'", "2024-03-31 01:30:00+00", ""},
		{"TIMESTAMP '2024-03-31 02:00:00' AT TIME ZONE 'Europe/London'", "2024-03-31 01:00:00+00", ""},
		{"TIMESTAMPTZ '2024-03-31 00:59:59+00' AT TIME ZONE 'Europe/London'", "2024-03-31 00:59:59", ""},
		{"TIMESTAMPTZ '2024-03-31 01:00:00+00' AT TIME ZONE 'Europe/London'", "2024-03-31 02:00:00", ""},
		// 2024-10-27: 02:00 BST becomes 01:00 GMT at 01:00 UTC.
		{"TIMESTAMP '2024-10-27 00:30:00' AT TIME ZONE 'Europe/London'", "2024-10-26 23:30:00+00", ""},
		{"TIMESTAMP '2024-10-27 01:30:00' AT TIME ZONE 'Europe/London'", "2024-10-27 01:30:00+00", ""},
		{"TIMESTAMP '2024-10-27 02:00:00' AT TIME ZONE 'Europe/London'", "2024-10-27 02:00:00+00", ""},
		{"TIMESTAMPTZ '2024-10-27 00:30:00+00' AT TIME ZONE 'Europe/London'", "2024-10-27 01:30:00", ""},
		{"TIMESTAMPTZ '2024-10-27 01:30:00+00' AT TIME ZONE 'Europe/London'", "2024-10-27 01:30:00", ""},
		// The southern hemisphere: Australia/Sydney, 2024-04-07, 03:00 AEDT
		// becomes 02:00 AEST at 16:00 UTC the day before.
		{"TIMESTAMP '2024-04-07 02:30:00' AT TIME ZONE 'Australia/Sydney'", "2024-04-06 16:30:00+00", ""},
		{"TIMESTAMPTZ '2024-04-06 15:30:00+00' AT TIME ZONE 'Australia/Sydney'", "2024-04-07 02:30:00", ""},
		// CONVERT_TIMEZONE resolves local times the same way.
		{"CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TIMESTAMP '2024-03-10 02:30:00')", "2024-03-10 10:30:00", ""},
		{"CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TIMESTAMP '2024-11-03 01:30:00')", "2024-11-03 09:30:00", ""},
		{"CONVERT_TIMEZONE('UTC', 'Europe/London', TIMESTAMP '2024-10-27 00:30:00')", "2024-10-27 01:30:00", ""},
		{"CONVERT_TIMEZONE('UTC', 'Europe/London', TIMESTAMP '2024-10-27 01:30:00')", "2024-10-27 01:30:00", ""},
		// Timestamp input with a zone name, too.
		{"CAST('2024-03-10 02:30:00 America/Los_Angeles' AS TIMESTAMPTZ)", "2024-03-10 10:30:00+00", ""},
		{"CAST('2024-11-03 01:30:00 America/Los_Angeles' AS TIMESTAMPTZ)", "2024-11-03 09:30:00+00", ""},
	}
	runScalarCases(t, cases)
}

// Zone names, abbreviations, POSIX-style offsets and intervals.
func TestTimeZoneNames(t *testing.T) {
	const ts = "TIMESTAMPTZ '2024-01-10 10:00:00+00' AT TIME ZONE "
	runScalarCases(t, []scalarCase{
		{ts + "'UTC'", "2024-01-10 10:00:00", ""},
		{ts + "'utc'", "2024-01-10 10:00:00", ""},
		{ts + "'GMT'", "2024-01-10 10:00:00", ""},
		{ts + "'Z'", "2024-01-10 10:00:00", ""},
		{ts + "'Etc/UTC'", "2024-01-10 10:00:00", ""},
		// IANA names are case-insensitive, as in Postgres.
		{ts + "'America/New_York'", "2024-01-10 05:00:00", ""},
		{ts + "'america/new_york'", "2024-01-10 05:00:00", ""},
		{ts + "'AMERICA/NEW_YORK'", "2024-01-10 05:00:00", ""},
		{ts + "'us/eastern'", "2024-01-10 05:00:00", ""},
		{ts + "'america/port-au-prince'", "2024-01-10 05:00:00", ""},
		{ts + "'antarctica/mcmurdo'", "2024-01-10 23:00:00", ""},
		{ts + "'Asia/Kathmandu'", "2024-01-10 15:45:00", ""},
		{ts + "'est5edt'", "2024-01-10 05:00:00", ""},
		// Etc/GMT+5 is UTC-5, as in the tz database.
		{ts + "'Etc/GMT+5'", "2024-01-10 05:00:00", ""},
		// Abbreviations are fixed offsets: CET is UTC+1 in July too, and
		// PST in July is still UTC-8.
		{ts + "'EST'", "2024-01-10 05:00:00", ""},
		{ts + "'pst'", "2024-01-10 02:00:00", ""},
		{ts + "'PDT'", "2024-01-10 03:00:00", ""},
		{ts + "'nst'", "2024-01-10 06:30:00", ""},
		{"TIMESTAMPTZ '2024-07-01 10:00:00+00' AT TIME ZONE 'CET'", "2024-07-01 11:00:00", ""},
		{"TIMESTAMPTZ '2024-07-01 10:00:00+00' AT TIME ZONE 'Europe/Paris'", "2024-07-01 12:00:00", ""},
		{"TIMESTAMPTZ '2024-07-01 10:00:00+00' AT TIME ZONE 'PST'", "2024-07-01 02:00:00", ""},
		// POSIX-style offsets are west of Greenwich: UTC+5 and +05:30 are
		// behind UTC.
		{ts + "'UTC+5'", "2024-01-10 05:00:00", ""},
		{ts + "'UTC-5'", "2024-01-10 15:00:00", ""},
		{ts + "'+05:30'", "2024-01-10 04:30:00", ""},
		{ts + "'-05:30'", "2024-01-10 15:30:00", ""},
		{ts + "'5'", "2024-01-10 05:00:00", ""},
		{ts + "'EST5'", "2024-01-10 05:00:00", ""},
		{ts + "'<+0530>-05:30'", "2024-01-10 15:30:00", ""},
		{ts + "'XYZ+01:02:03'", "2024-01-10 08:57:57", ""},
		{"TIMESTAMP '2024-01-10 10:00:00' AT TIME ZONE '+05:30'", "2024-01-10 15:30:00+00", ""},
		// Intervals are east of Greenwich, as in ISO 8601.
		{ts + "INTERVAL '+05:30'", "2024-01-10 15:30:00", ""},
		{ts + "INTERVAL '-08:00'", "2024-01-10 02:00:00", ""},
		{ts + "INTERVAL '-8 hours'", "2024-01-10 02:00:00", ""},
		{ts + "INTERVAL '90 minutes 30.9 seconds'", "2024-01-10 11:30:30", ""},
		{"TIMESTAMP '2024-01-10 10:00:00' AT TIME ZONE INTERVAL '+05:30'", "2024-01-10 04:30:00+00", ""},
	})
}

func TestAtTimeZoneErrors(t *testing.T) {
	const ts = "TIMESTAMP '2024-01-10 10:00:00'"
	expectExprErrors(t, map[string]string{
		ts + " AT TIME ZONE 'Mars/Phobos'":               `time zone "Mars/Phobos" not recognized`,
		ts + " AT TIME ZONE ''":                          `time zone "" not recognized`,
		ts + " AT TIME ZONE 'Local'":                     `time zone "Local" not recognized`,
		ts + " AT TIME ZONE 'FOO'":                       `time zone "FOO" not recognized`,
		ts + " AT TIME ZONE 'UTC+200'":                   `time zone "UTC+200" not recognized`,
		ts + " AT TIME ZONE '+0530'":                     `time zone "+0530" not recognized`,
		ts + " AT TIME ZONE 'XYZ1ABC'":                   `time zone "XYZ1ABC" not recognized`,
		ts + " AT TIME ZONE ' UTC'":                      `time zone " UTC" not recognized`,
		ts + " AT TIME ZONE 'Asia/' || 'Tokyo'":          `time zone "Asia/" not recognized`,
		"TIMEZONE('Nowhere', " + ts + ")":                `time zone "Nowhere" not recognized`,
		"CONVERT_TIMEZONE('Nowhere', 'UTC', " + ts + ")": `time zone "Nowhere" not recognized`,
		"CONVERT_TIMEZONE('UTC', 'Nowhere', " + ts + ")": `time zone "Nowhere" not recognized`,
		"CONVERT_TIMEZONE('Nowhere', " + ts + ")":        `time zone "Nowhere" not recognized`,
		ts + " AT TIME ZONE INTERVAL '1 day'":            `interval time zone "1 day" must not include months or days`,
		ts + " AT TIME ZONE INTERVAL '1 mon 2 hours'":    `interval time zone "1 mon 02:00:00" must not include months or days`,
		// Types without a TIMEZONE: there is no TIME WITH TIME ZONE.
		"TIME '10:00:00' AT TIME ZONE 'UTC'":                     "function timezone(varchar, time) does not exist",
		ts + " AT TIME ZONE 5":                                   "function timezone(bigint, timestamp) does not exist",
		"INTERVAL '1 hour' AT TIME ZONE 'UTC'":                   "function timezone(varchar, interval) does not exist",
		"1 AT TIME ZONE 'UTC'":                                   "function timezone(varchar, bigint) does not exist",
		"CONVERT_TIMEZONE(INTERVAL '1 hour', 'UTC', " + ts + ")": "function convert_timezone(interval, varchar, timestamp) does not exist",
		"CONVERT_TIMEZONE('UTC', 1)":                             "function convert_timezone(varchar, bigint) does not exist",
		"CONVERT_TIMEZONE('UTC', TIME '10:00:00')":               "function convert_timezone(varchar, time) does not exist",
		// Argument counts.
		"TIMEZONE()":                  "TIMEZONE expects 1 or 2 arguments",
		"TIMEZONE('UTC', 'UTC', 'x')": "TIMEZONE expects 1 or 2 arguments",
		"CONVERT_TIMEZONE('UTC')":     "CONVERT_TIMEZONE expects 2 or 3 arguments",
	})
}

// AT is a keyword only before TIME ZONE and LOCAL.
func TestAtTimeZoneParse(t *testing.T) {
	stmts, err := ParseScript("SELECT 1 at, 2 AS at FROM t at WHERE at.x = 1")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmts[0].Stmt.(*SelectStmt)
	if sel.Items[0].Alias != "at" || sel.Items[1].Alias != "at" {
		t.Errorf("aliases %q, %q", sel.Items[0].Alias, sel.Items[1].Alias)
	}
	for _, sql := range []string{"SELECT x AT TIME", "SELECT x AT TIME ZONE", "SELECT x AT ZONE 'UTC'"} {
		if _, err := ParseScript(sql); err == nil {
			t.Errorf("%s: expected a syntax error", sql)
		}
	}
	// x AT TIME ZONE z is TIMEZONE(z, x), x AT LOCAL TIMEZONE(x).
	e := parseTestExpr(t, "-x AT TIME ZONE 'a' AT LOCAL")
	outer, ok := e.(*Func)
	if !ok || outer.Name != "TIMEZONE" || len(outer.Args) != 1 {
		t.Fatalf("got %#v", e)
	}
	inner, ok := outer.Args[0].(*Func)
	if !ok || inner.Name != "TIMEZONE" || len(inner.Args) != 2 {
		t.Fatalf("got %#v", outer.Args[0])
	}
	if _, ok := inner.Args[1].(*Unary); !ok {
		t.Errorf("unary minus should bind tighter than AT TIME ZONE: %#v", inner.Args[1])
	}
}

// Every zone of the tz database resolves in lower and upper case to the
// same zone, except the names that are also abbreviations (which Postgres
// reads as fixed offsets first).
func TestTimeZoneCaseInsensitive(t *testing.T) {
	r, err := zip.OpenReader(filepath.Join(runtime.GOROOT(), "lib", "time", "zoneinfo.zip"))
	if err != nil {
		t.Skipf("no tz database to list zones from: %v", err)
	}
	defer r.Close()
	instants := []int64{0, 1_700_000_000, 1_720_000_000}
	n := 0
	for _, f := range r.File {
		name := f.Name
		if strings.HasSuffix(name, "/") {
			continue
		}
		if _, ok := zoneAbbrevs[strings.ToLower(name)]; ok {
			continue
		}
		want, err := time.LoadLocation(name)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		for _, spelling := range []string{name, strings.ToLower(name), strings.ToUpper(name)} {
			z, err := resolveZone(spelling)
			if err != nil {
				t.Errorf("%s: %v", spelling, err)
				continue
			}
			for _, sec := range instants {
				_, off := time.Unix(sec, 0).In(want).Zone()
				if got := z.offsetAt(sec); got != off {
					t.Errorf("%s at %d: offset %d, want %d", spelling, sec, got, off)
				}
			}
		}
		n++
	}
	if n < 400 {
		t.Errorf("checked %d zones", n)
	}
}
