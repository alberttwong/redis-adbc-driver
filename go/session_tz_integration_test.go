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

// Integration tests for the session time zone (#105): SET TIME ZONE and
// adbc.redis.time_zone, what the zone changes on stored rows (in Los
// Angeles, across its DST transitions, and in Kolkata), index queries with
// local times, per-connection isolation, and what it doesn't change (stored
// values, Arrow results, bulk ingest).

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// pushedWhereIn is pushedWhere in a session whose TimeZone is zone.
func (h *sqlHarness) pushedWhereIn(c goredis.UniversalClient, zone, sql string) (string, bool) {
	h.t.Helper()
	parsed, err := ParseScript(sql)
	if err != nil {
		h.t.Fatal(err)
	}
	tz, err := timeZoneParam(zone)
	if err != nil {
		h.t.Fatal(err)
	}
	sess := newSession(defaultSchema)
	sess.vals["timezone"] = tz
	e := &executor{store: &store{client: c}, sess: sess, schema: defaultSchema}
	e.cache = newExecCache()
	plan, err := e.planSelect(h.ctx, parsed[0].Stmt.(*SelectStmt), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	wp, err := e.planWhere(h.ctx, plan.sel.Where, plan.meta, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	return wp.query, wp.residual != nil
}

// setupSessionTZ creates it_stz: instants around Los Angeles's 2024 DST
// transitions (2024-03-10 10:00 UTC and 2024-11-03 09:00 UTC), with local
// times as TIMESTAMP, DATE and text.
func (h *sqlHarness) setupSessionTZ() {
	h.t.Helper()
	h.dropTables("it_stz", "it_stz_ing")
	h.exec(`CREATE TABLE it_stz (id INTEGER, tz TIMESTAMPTZ, ts TIMESTAMP, d DATE, s VARCHAR)`)
	h.exec(`INSERT INTO it_stz VALUES
		(1, '2024-01-10 05:00:00+00', '2024-01-09 21:00:00', '2024-01-10', '2024-01-09 21:00'),
		(2, '2024-03-10 09:30:00+00', '2024-03-10 02:30:00', '2024-03-10', '2024-03-10 02:30'),
		(3, '2024-03-10 10:30:00+00', '2024-03-10 03:30:00', '2024-03-11', '2024-03-10 03:30'),
		(4, '2024-11-03 08:30:00+00', '2024-11-03 01:30:00', '2024-11-03', '2024-11-03 01:30'),
		(5, '2024-11-03 09:30:00+00', '2024-11-03 01:30:00', '2024-11-04', '2024-11-03 01:30'),
		(6, '2024-07-01 18:45:30.25+00', '2024-07-01 11:45:30.25', '2024-07-01', '2024-07-01 11:45:30.25'),
		(7, NULL, NULL, NULL, NULL)`)
}

// SET [SESSION | LOCAL] TIME ZONE, SET timezone, RESET and SHOW, and that
// the zone set is the one timestamps with time zone are shown in.
func TestSessionTimeZoneSet(t *testing.T) {
	h := newSQLHarness(t)
	h.setupSessionTZ()
	row1 := `SELECT CAST(tz AS TEXT) FROM it_stz WHERE id = 1`
	h.expectColumns(`SHOW timezone`, "TimeZone", "UTC")
	h.expectRows(row1, "2024-01-10 05:00:00+00")
	for _, c := range []struct{ set, shown, text string }{
		{`SET TIME ZONE 'America/Los_Angeles'`, "America/Los_Angeles", "2024-01-09 21:00:00-08"},
		{`set time zone 'asia/kolkata'`, "Asia/Kolkata", "2024-01-10 10:30:00+05:30"},
		{`SET SESSION TIME ZONE 'Europe/London'`, "Europe/London", "2024-01-10 05:00:00+00"},
		{`SET timezone = 'US/Pacific'`, "US/Pacific", "2024-01-09 21:00:00-08"},
		{`SET TimeZone TO "Asia/Tokyo"`, "Asia/Tokyo", "2024-01-10 14:00:00+09"},
		{`SET SESSION timezone = 'UTC+5'`, "UTC+5", "2024-01-10 00:00:00-05"},
		{`SET TIME ZONE -8`, "<-08>+08", "2024-01-09 21:00:00-08"},
		{`SET TIME ZONE '5.5'`, "<+05:30>-05:30", "2024-01-10 10:30:00+05:30"},
		{`SET TIME ZONE INTERVAL '-03:30' HOUR TO MINUTE`, "<-03:30>+03:30", "2024-01-10 01:30:00-03:30"},
		{`SET TIME ZONE 'PST'`, "PST", "2024-01-09 21:00:00-08"},
		{`SET TIME ZONE LOCAL`, "UTC", "2024-01-10 05:00:00+00"},
	} {
		h.exec(c.set)
		h.expectColumns(`SHOW TIME ZONE`, "TimeZone", c.shown)
		h.expectRows(row1, c.text)
	}
	// Back to the default.
	for _, reset := range []string{`SET TIME ZONE DEFAULT`, `RESET timezone`, `RESET TIME ZONE`, `SET timezone TO DEFAULT`,
		`SET SESSION timezone = DEFAULT`, `RESET ALL`} {
		h.exec(`SET TIME ZONE 'Asia/Kolkata'`)
		h.exec(reset)
		h.expectRows(`SHOW timezone`, "UTC")
	}
	h.exec(`SET TIME ZONE 'America/Los_Angeles'`)
	if all, _ := h.query(`SHOW ALL`); !slices.Contains(all,
		"TimeZone|America/Los_Angeles|Time zone for displaying and interpreting time stamps (adbc.redis.time_zone, UTC by default).") {
		t.Errorf("SHOW ALL: %q", all)
	}
	h.exec(`RESET ALL`)

	// SET LOCAL: on its own, no effect; in a script, until its end; after
	// BEGIN, until COMMIT or ROLLBACK, after which a SET's value is back.
	h.exec(`SET LOCAL TIME ZONE 'Asia/Kolkata'`)
	h.expectRows(`SHOW timezone`, "UTC")
	h.expectRows(`SET LOCAL TIME ZONE 'Asia/Kolkata'; `+row1, "2024-01-10 10:30:00+05:30")
	h.expectRows(row1, "2024-01-10 05:00:00+00")
	h.exec(`BEGIN`)
	h.exec(`SET LOCAL timezone = 'America/Los_Angeles'`)
	h.expectRows(`SHOW timezone`, "America/Los_Angeles")
	h.expectRows(row1, "2024-01-09 21:00:00-08")
	h.exec(`COMMIT`)
	h.expectRows(row1, "2024-01-10 05:00:00+00")
	h.exec(`SET TIME ZONE 'Asia/Kolkata'`)
	h.exec(`BEGIN; SET LOCAL TIME ZONE 'America/Los_Angeles'`)
	h.expectRows(`SHOW timezone`, "America/Los_Angeles")
	h.exec(`ROLLBACK`)
	h.expectRows(`SHOW timezone`, "Asia/Kolkata")
	h.exec(`RESET timezone`)

	// A SET earlier in a script applies to the statements after it, also to
	// a literal read as a local time.
	h.expectRows(`SET TIME ZONE 'America/Los_Angeles';
		SELECT CAST(TIMESTAMPTZ '2024-01-10 02:00:00' AS TEXT), TIMESTAMPTZ '2024-01-10 02:00:00' AT TIME ZONE 'UTC'`,
		"2024-01-10 02:00:00-08|2024-01-10T10:00:00")
	h.expectRows(`SHOW timezone`, "America/Los_Angeles")
	h.exec(`RESET timezone`)

	// Errors, which change nothing.
	h.exec(`SET TIME ZONE 'Asia/Kolkata'`)
	for _, c := range []struct{ sql, want string }{
		{`SET TIME ZONE 'Mars/Base'`, `invalid value for parameter "TimeZone": "Mars/Base"`},
		{`SET timezone = 'IST'`, `invalid value for parameter "TimeZone": "IST"`},
		{`SET LOCAL TIME ZONE ''`, `invalid value for parameter "TimeZone": ""`},
		{`SET TIME ZONE 'CET-1CEST,M3.5.0,M10.5.0'`, `invalid value for parameter "TimeZone": "CET-1CEST,M3.5.0,M10.5.0"`},
		{`SET TIME ZONE INTERVAL '1 month'`, `invalid value for parameter "TimeZone": "INTERVAL '1 mon'" (cannot specify months in time zone interval)`},
		{`SET TIME ZONE 168`, `invalid value for parameter "TimeZone": "168"`},
		{`SET timezone = local`, `invalid value for parameter "TimeZone": "local"`},
	} {
		h.expectErrorText(c.sql, c.want)
	}
	h.expectRows(`SHOW timezone`, "Asia/Kolkata")
}

// What the session time zone changes, on stored rows, in Los Angeles and
// in Kolkata (UTC+05:30, without DST).
func TestSessionTimeZoneFunctions(t *testing.T) {
	h := newSQLHarness(t)
	h.setupSessionTZ()

	conversions := `SELECT id, CAST(tz AS TEXT), CAST(tz AS TIMESTAMP), CAST(tz AS DATE), CAST(tz AS TIME),
		CAST(ts AS TIMESTAMPTZ) = tz, CAST(CAST(d AS TIMESTAMPTZ) AS TEXT), CAST(s AS TIMESTAMPTZ) = tz FROM it_stz ORDER BY id`
	text := `SELECT id, tz || ' / ' || ts, CONCAT(tz, ' ', d), TO_CHAR(tz, 'YYYY-MM-DD HH24:MI TZ OF'), TO_JSON(tz)
		FROM it_stz WHERE id IN (1, 3, 5, 6) ORDER BY id`
	fields := `SELECT id, CAST(DATE_TRUNC('day', tz) AS TEXT), CAST(DATE_TRUNC('hour', tz) AS TEXT), EXTRACT(hour FROM tz),
		EXTRACT(day FROM tz), DATE_PART('timezone_hour', tz),
		CAST(DATE_BIN(INTERVAL '1 day', tz, TIMESTAMPTZ '2024-01-01 00:00') AS TEXT) FROM it_stz WHERE id <= 6 ORDER BY id`
	days := `SELECT CAST(tz AS DATE) AS day, COUNT(*) FROM it_stz WHERE tz IS NOT NULL GROUP BY CAST(tz AS DATE) ORDER BY day`

	// UTC, the default.
	h.expectRows(text,
		`1|2024-01-10 05:00:00+00 / 2024-01-09 21:00:00|2024-01-10 05:00:00+00 2024-01-10|2024-01-10 05:00 UTC +00|"2024-01-10T05:00:00+00:00"`,
		`3|2024-03-10 10:30:00+00 / 2024-03-10 03:30:00|2024-03-10 10:30:00+00 2024-03-11|2024-03-10 10:30 UTC +00|"2024-03-10T10:30:00+00:00"`,
		`5|2024-11-03 09:30:00+00 / 2024-11-03 01:30:00|2024-11-03 09:30:00+00 2024-11-04|2024-11-03 09:30 UTC +00|"2024-11-03T09:30:00+00:00"`,
		`6|2024-07-01 18:45:30.25+00 / 2024-07-01 11:45:30.25|2024-07-01 18:45:30.25+00 2024-07-01|2024-07-01 18:45 UTC +00|"2024-07-01T18:45:30.25+00:00"`)
	h.expectRows(days, "2024-01-10|1", "2024-03-10|2", "2024-07-01|1", "2024-11-03|2")

	// The current date and time are local (one current time per statement).
	now := func(zone string) string {
		return `SELECT CURRENT_DATE = CAST(NOW() AT TIME ZONE '` + zone + `' AS DATE),
			LOCALTIMESTAMP = NOW() AT LOCAL, LOCALTIMESTAMP = NOW() AT TIME ZONE '` + zone + `',
			LOCALTIME = CAST(LOCALTIMESTAMP AS TIME), CURRENT_TIME = LOCALTIME,
			CAST(CURRENT_DATE AS TIMESTAMPTZ) = DATE_TRUNC('day', NOW()), CAST(NOW() AS DATE) = CURRENT_DATE`
	}
	h.expectRows(now("UTC")+`, TO_CHAR(NOW(), 'TZ'), EXTRACT(timezone FROM NOW())`, "true|true|true|true|true|true|true|UTC|0")

	h.exec(`SET TIME ZONE 'America/Los_Angeles'`)
	h.expectRows(now("America/Los_Angeles")+`, TO_CHAR(NOW(), 'TZ') IN ('PST', 'PDT'), EXTRACT(timezone_hour FROM NOW()) IN (-8, -7)`,
		"true|true|true|true|true|true|true|true|true")
	h.expectRows(conversions,
		"1|2024-01-09 21:00:00-08|2024-01-09T21:00:00|2024-01-09|21:00:00.000000|true|2024-01-10 00:00:00-08|true",
		"2|2024-03-10 01:30:00-08|2024-03-10T01:30:00|2024-03-10|01:30:00.000000|false|2024-03-10 00:00:00-08|false",
		"3|2024-03-10 03:30:00-07|2024-03-10T03:30:00|2024-03-10|03:30:00.000000|true|2024-03-11 00:00:00-07|true",
		"4|2024-11-03 01:30:00-07|2024-11-03T01:30:00|2024-11-03|01:30:00.000000|false|2024-11-03 00:00:00-07|false",
		"5|2024-11-03 01:30:00-08|2024-11-03T01:30:00|2024-11-03|01:30:00.000000|true|2024-11-04 00:00:00-08|true",
		"6|2024-07-01 11:45:30.25-07|2024-07-01T11:45:30.25|2024-07-01|11:45:30.250000|true|2024-07-01 00:00:00-07|true",
		"7|NULL|NULL|NULL|NULL|NULL|NULL|NULL")
	h.expectRows(text,
		`1|2024-01-09 21:00:00-08 / 2024-01-09 21:00:00|2024-01-09 21:00:00-08 2024-01-10|2024-01-09 21:00 PST -08|"2024-01-09T21:00:00-08:00"`,
		`3|2024-03-10 03:30:00-07 / 2024-03-10 03:30:00|2024-03-10 03:30:00-07 2024-03-11|2024-03-10 03:30 PDT -07|"2024-03-10T03:30:00-07:00"`,
		`5|2024-11-03 01:30:00-08 / 2024-11-03 01:30:00|2024-11-03 01:30:00-08 2024-11-04|2024-11-03 01:30 PST -08|"2024-11-03T01:30:00-08:00"`,
		`6|2024-07-01 11:45:30.25-07 / 2024-07-01 11:45:30.25|2024-07-01 11:45:30.25-07 2024-07-01|2024-07-01 11:45 PDT -07|"2024-07-01T11:45:30.25-07:00"`)
	h.expectRows(fields,
		"1|2024-01-09 00:00:00-08|2024-01-09 21:00:00-08|21|9|-8|2024-01-09 00:00:00-08",
		"2|2024-03-10 00:00:00-08|2024-03-10 01:00:00-08|1|10|-8|2024-03-10 00:00:00-08",
		"3|2024-03-10 00:00:00-08|2024-03-10 03:00:00-07|3|10|-7|2024-03-10 00:00:00-08",
		"4|2024-11-03 00:00:00-07|2024-11-03 01:00:00-07|1|3|-7|2024-11-03 01:00:00-07",
		"5|2024-11-03 00:00:00-07|2024-11-03 01:00:00-08|1|3|-8|2024-11-03 01:00:00-07",
		"6|2024-07-01 00:00:00-07|2024-07-01 11:00:00-07|11|1|-7|2024-07-01 01:00:00-07")
	h.expectRows(days, "2024-01-09|1", "2024-03-10|2", "2024-07-01|1", "2024-11-03|2")
	// Aggregates and window functions that make text.
	h.expectRows(`SELECT STRING_AGG(tz, ', ' ORDER BY id), STRING_AGG(CAST(tz AS DATE), ',' ORDER BY id) FROM it_stz WHERE id <= 3`,
		"2024-01-09 21:00:00-08, 2024-03-10 01:30:00-08, 2024-03-10 03:30:00-07|2024-01-09,2024-03-10,2024-03-10")
	h.expectRows(`SELECT JSON_AGG(tz ORDER BY id) FROM it_stz WHERE id IN (1, 6)`,
		`["2024-01-09T21:00:00-08:00", "2024-07-01T11:45:30.25-07:00"]`)
	h.expectRows(`SELECT id, STRING_AGG(tz, ',') OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM it_stz
		WHERE id IN (4, 5) ORDER BY id`,
		"4|2024-11-03 01:30:00-07", "5|2024-11-03 01:30:00-07,2024-11-03 01:30:00-08")
	// Adding months and days on the local time; AT LOCAL both ways.
	h.expectRows(`SELECT id, CAST(tz + INTERVAL '1 day' AS TEXT), CAST(tz - INTERVAL '1 month' AS TEXT), tz AT LOCAL,
			CAST(ts AT LOCAL AS TEXT), CAST(tz - CAST(d AS TIMESTAMP) AS TEXT)
		FROM it_stz WHERE id IN (2, 3, 4, 5) ORDER BY id`,
		"2|2024-03-11 01:30:00-07|2024-02-10 01:30:00-08|2024-03-10T01:30:00|2024-03-10 03:30:00-07|01:30:00",
		"3|2024-03-11 03:30:00-07|2024-02-10 03:30:00-08|2024-03-10T03:30:00|2024-03-10 03:30:00-07|-20:30:00",
		"4|2024-11-04 01:30:00-08|2024-10-03 01:30:00-07|2024-11-03T01:30:00|2024-11-03 01:30:00-08|01:30:00",
		"5|2024-11-04 01:30:00-08|2024-10-03 01:30:00-07|2024-11-03T01:30:00|2024-11-03 01:30:00-08|-22:30:00")
	// GENERATE_SERIES steps in local days; RANGE frames measure a day of
	// local time.
	h.expectRows(`SELECT CAST(x AS TEXT) FROM generate_series(TIMESTAMPTZ '2024-03-09', TIMESTAMPTZ '2024-03-11', INTERVAL '1 day') AS g(x)`,
		"2024-03-09 00:00:00-08", "2024-03-10 00:00:00-08", "2024-03-11 00:00:00-07")
	h.expectRows(`SELECT CAST(x AS TEXT) FROM generate_series(DATE '2024-11-02', DATE '2024-11-04', INTERVAL '1 day') AS g(x)`,
		"2024-11-02 00:00:00-07", "2024-11-03 00:00:00-07", "2024-11-04 00:00:00-08")
	window := `SELECT CAST(t AS TEXT), COUNT(*) OVER (ORDER BY t RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW)
		FROM (SELECT TIMESTAMPTZ '2024-03-09 11:30:00-08' AS t UNION ALL SELECT TIMESTAMPTZ '2024-03-10 12:00:00-07') AS w ORDER BY t`
	h.expectRows(window, "2024-03-09 11:30:00-08|1", "2024-03-10 12:00:00-07|1")
	// A value of a CASE branch of another type is converted to the CASE's
	// type, for the result and where the query uses it: sorting, DISTINCT,
	// partitions, aggregates.
	h.expectRows(`SELECT CASE WHEN id = 1 THEN ts ELSE tz END FROM it_stz WHERE id IN (1, 6) ORDER BY id`,
		"2024-01-10T05:00:00Z", "2024-07-01T18:45:30.25Z")
	mixed := `(SELECT n, CASE WHEN n = 1 THEN TIMESTAMP '2024-01-10 01:00:00' WHEN n = 2 THEN TIMESTAMPTZ '2024-01-10 05:00:00+00'
		ELSE TIMESTAMP '2024-01-09 21:00:00' END AS x FROM (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) AS k) AS v`
	h.expectRows(`SELECT n, CAST(x AS TEXT) FROM `+mixed+` ORDER BY x, n`,
		"2|2024-01-09 21:00:00-08", "3|2024-01-09 21:00:00-08", "1|2024-01-10 01:00:00-08")
	h.expectRows(`SELECT COUNT(DISTINCT x), CAST(MAX(x) AS TEXT), CAST(MIN(x) AS TEXT) FROM `+mixed,
		"2|2024-01-10 01:00:00-08|2024-01-09 21:00:00-08")
	h.expectRows(`SELECT n, COUNT(*) OVER (PARTITION BY x) FROM `+mixed+` ORDER BY n`, "1|1", "2|2", "3|2")

	h.exec(`SET TIME ZONE 'Asia/Kolkata'`)
	h.expectRows(now("Asia/Kolkata")+`, TO_CHAR(NOW(), 'TZ OF'), EXTRACT(timezone FROM NOW())`,
		"true|true|true|true|true|true|true|IST +05:30|19800")
	h.expectRows(conversions,
		"1|2024-01-10 10:30:00+05:30|2024-01-10T10:30:00|2024-01-10|10:30:00.000000|false|2024-01-10 00:00:00+05:30|false",
		"2|2024-03-10 15:00:00+05:30|2024-03-10T15:00:00|2024-03-10|15:00:00.000000|false|2024-03-10 00:00:00+05:30|false",
		"3|2024-03-10 16:00:00+05:30|2024-03-10T16:00:00|2024-03-10|16:00:00.000000|false|2024-03-11 00:00:00+05:30|false",
		"4|2024-11-03 14:00:00+05:30|2024-11-03T14:00:00|2024-11-03|14:00:00.000000|false|2024-11-03 00:00:00+05:30|false",
		"5|2024-11-03 15:00:00+05:30|2024-11-03T15:00:00|2024-11-03|15:00:00.000000|false|2024-11-04 00:00:00+05:30|false",
		"6|2024-07-02 00:15:30.25+05:30|2024-07-02T00:15:30.25|2024-07-02|00:15:30.250000|false|2024-07-01 00:00:00+05:30|false",
		"7|NULL|NULL|NULL|NULL|NULL|NULL|NULL")
	h.expectRows(text,
		`1|2024-01-10 10:30:00+05:30 / 2024-01-09 21:00:00|2024-01-10 10:30:00+05:30 2024-01-10|2024-01-10 10:30 IST +05:30|"2024-01-10T10:30:00+05:30"`,
		`3|2024-03-10 16:00:00+05:30 / 2024-03-10 03:30:00|2024-03-10 16:00:00+05:30 2024-03-11|2024-03-10 16:00 IST +05:30|"2024-03-10T16:00:00+05:30"`,
		`5|2024-11-03 15:00:00+05:30 / 2024-11-03 01:30:00|2024-11-03 15:00:00+05:30 2024-11-04|2024-11-03 15:00 IST +05:30|"2024-11-03T15:00:00+05:30"`,
		`6|2024-07-02 00:15:30.25+05:30 / 2024-07-01 11:45:30.25|2024-07-02 00:15:30.25+05:30 2024-07-01|2024-07-02 00:15 IST +05:30|"2024-07-02T00:15:30.25+05:30"`)
	h.expectRows(fields,
		"1|2024-01-10 00:00:00+05:30|2024-01-10 10:00:00+05:30|10|10|5|2024-01-10 00:00:00+05:30",
		"2|2024-03-10 00:00:00+05:30|2024-03-10 15:00:00+05:30|15|10|5|2024-03-10 00:00:00+05:30",
		"3|2024-03-10 00:00:00+05:30|2024-03-10 16:00:00+05:30|16|10|5|2024-03-10 00:00:00+05:30",
		"4|2024-11-03 00:00:00+05:30|2024-11-03 14:00:00+05:30|14|3|5|2024-11-03 00:00:00+05:30",
		"5|2024-11-03 00:00:00+05:30|2024-11-03 15:00:00+05:30|15|3|5|2024-11-03 00:00:00+05:30",
		"6|2024-07-02 00:00:00+05:30|2024-07-02 00:00:00+05:30|0|2|5|2024-07-02 00:00:00+05:30")
	h.expectRows(days, "2024-01-10|1", "2024-03-10|2", "2024-07-02|1", "2024-11-03|2")
	h.expectRows(window, "2024-03-10 01:00:00+05:30|1", "2024-03-11 00:30:00+05:30|2")

	// Back in UTC.
	h.exec(`RESET TIME ZONE`)
	h.expectRows(window, "2024-03-09 19:30:00+00|1", "2024-03-10 19:00:00+00|2")
}

// Index queries with local times give what the same comparison on the
// rows gives; connections keep their own time zone; stored values, Arrow
// results and bulk ingest don't depend on it; adbc.redis.time_zone.
func TestSessionTimeZoneIndexAndIsolation(t *testing.T) {
	h := newSQLHarness(t)
	h.setupSessionTZ()
	raw := h.rawClient()
	rows := func(where string) string {
		got, _ := h.query(`SELECT id FROM it_stz WHERE ` + where + ` ORDER BY id`)
		return strings.Join(got, ",")
	}
	check := func(zone string, cases []struct{ where, want string }) {
		t.Helper()
		h.exec(`SET TIME ZONE '` + zone + `'`)
		for _, c := range cases {
			if got := rows(c.where); got != c.want {
				t.Errorf("%s: WHERE %s: rows %q, want %q", zone, c.where, got, c.want)
			}
			// The same comparison on an expression, which isn't pushed down.
			col := c.where[:strings.IndexByte(c.where, ' ')]
			unpushed := strings.ReplaceAll(c.where, col+" ", "COALESCE("+col+", NULL) ")
			if got := rows(unpushed); got != c.want {
				t.Errorf("%s: WHERE %s: rows %q, want %q", zone, unpushed, got, c.want)
			}
		}
	}
	check("America/Los_Angeles", []struct{ where, want string }{
		{`tz >= '2024-01-09' AND tz < '2024-01-10'`, "1"},
		{`tz = '2024-11-03 01:30'`, "5"},
		{`tz = TIMESTAMP '2024-11-03 01:30:00'`, "5"},
		{`tz >= DATE '2024-11-03' AND tz < DATE '2024-11-04'`, "4,5"},
		{`tz IN ('2024-01-09 21:00', '2024-11-03 01:30')`, "1,5"},
		{`tz BETWEEN '2024-07-01 11:45:30.25' AND '2024-07-01 12:00'`, "6"},
		{`tz > '2024-03-10 02:30'`, "4,5,6"},
		{`tz >= '2024-03-10 03:00'`, "3,4,5,6"},
		{`tz < CAST('2024-03-10' AS DATE) + 1`, "1,2,3"},
		// A TIMESTAMP or DATE column compared with a timestamp with time
		// zone: each row's value is a local time (02:30 in the gap and
		// 03:30 are both 10:30 UTC).
		{`ts = TIMESTAMPTZ '2024-03-10 10:30:00+00'`, "2,3"},
		{`ts IN (TIMESTAMPTZ '2024-03-10 10:30:00+00', TIMESTAMPTZ '2024-01-10 05:00:00+00')`, "1,2,3"},
		{`ts < TIMESTAMPTZ '2024-11-03 09:00:00+00'`, "1,2,3,6"},
		{`ts >= TIMESTAMPTZ '2024-11-03 09:30:00+00'`, "4,5"},
		{`d = TIMESTAMPTZ '2024-03-11 07:00:00+00'`, "3"},
		{`d < TIMESTAMPTZ '2024-03-11 06:59:59+00'`, "1,2"},
	})
	check("Asia/Kolkata", []struct{ where, want string }{
		{`tz >= '2024-07-02' AND tz < '2024-07-03'`, "6"},
		{`tz = '2024-01-10 10:30'`, "1"},
		{`tz = TIMESTAMP '2024-01-10 10:30:00'`, "1"},
		{`tz >= DATE '2024-03-10' AND tz < DATE '2024-03-11'`, "2,3"},
		{`ts = TIMESTAMPTZ '2024-01-09 15:30:00+00'`, "1"},
	})
	h.exec(`RESET TIME ZONE`)
	check("UTC", []struct{ where, want string }{
		{`tz >= '2024-01-09' AND tz < '2024-01-10'`, ""},
		{`tz = '2024-11-03 01:30'`, ""},
		{`ts = TIMESTAMPTZ '2024-03-10 03:30:00+00'`, "3"},
	})
	// The index queries: the instant of the local time, exact; for a
	// TIMESTAMP column, the local times around it, re-checked.
	for _, c := range []struct{ zone, sql, query string }{
		{"America/Los_Angeles", `SELECT id FROM it_stz WHERE tz >= '2024-01-09'`, "@tz:[1704787200000000 +inf]"},
		{"Asia/Kolkata", `SELECT id FROM it_stz WHERE tz < DATE '2024-01-10'`, "@tz:[-inf (1704825000000000]"},
		{"UTC", `SELECT id FROM it_stz WHERE tz = '2024-01-10 05:00'`, "@tz:[1704862800000000 1704862800000000]"},
	} {
		if q, residual := h.pushedWhereIn(raw, c.zone, c.sql); q != c.query || residual {
			t.Errorf("%s: %s: query %q, residual %v; want %q", c.zone, c.sql, q, residual, c.query)
		}
	}
	if q, residual := h.pushedWhereIn(raw, "America/Los_Angeles", `SELECT id FROM it_stz WHERE ts = TIMESTAMPTZ '2024-03-10 10:30:00+00'`); q != "@ts:[1710037800000000 1710041400000000]" || !residual {
		t.Errorf("ts = timestamptz: query %q, residual %v", q, residual)
	}

	// Writes in a session time zone store instants; another connection
	// (in UTC) reads them, and its own session stays in UTC.
	h2 := newSQLHarness(t)
	h.exec(`SET TIME ZONE 'America/Los_Angeles'`)
	h.exec(`INSERT INTO it_stz (id, tz, ts) VALUES (8, '2024-03-10 02:30', TIMESTAMPTZ '2024-03-10 10:30:00+00')`)
	h.exec(`UPDATE it_stz SET d = TIMESTAMPTZ '2024-01-10 05:00:00+00' WHERE id = 8`)
	h2.expectRows(`SHOW timezone`, "UTC")
	h2.expectRows(`SELECT CAST(tz AS TEXT), CAST(ts AS TEXT), CAST(d AS TEXT) FROM it_stz WHERE id = 8`,
		"2024-03-10 10:30:00+00|2024-03-10 03:30:00|2024-01-09")
	h.expectRows(`SELECT CAST(tz AS TEXT), CAST(ts AS TEXT), CAST(d AS TEXT) FROM it_stz WHERE id = 8`,
		"2024-03-10 03:30:00-07|2024-03-10 03:30:00|2024-01-09")
	h2.exec(`SET TIME ZONE 'Asia/Kolkata'`)
	h.expectRows(`SHOW timezone`, "America/Los_Angeles")
	h2.exec(`DELETE FROM it_stz WHERE id = 8`)

	// Arrow results are instants with time zone UTC, whatever the session
	// time zone; the stored values are the same.
	for _, zone := range []string{"UTC", "America/Los_Angeles", "Asia/Kolkata"} {
		h.exec(`SET TIME ZONE '` + zone + `'`)
		schema := h.expectRows(`SELECT tz, MIN(tz) OVER () FROM it_stz WHERE id IN (1, 6) ORDER BY id`,
			"2024-01-10T05:00:00Z|2024-01-10T05:00:00Z", "2024-07-01T18:45:30.25Z|2024-01-10T05:00:00Z")
		for _, f := range schema.Fields() {
			if f.Type.String() != "timestamp[us, tz=UTC]" {
				t.Errorf("%s: column %s: %s", zone, f.Name, f.Type)
			}
		}
		h.expectRows(`SELECT MAX(tz), COUNT(*) FROM it_stz`, "2024-11-03T09:30:00Z|7")
	}
	if got, err := raw.HGet(h.ctx, rowKeyOf(h, "it_stz", 1), "tz").Result(); err != nil || got != "1704862800000000" {
		t.Errorf("stored tz of row 1: %q, %v", got, err)
	}

	// Bulk ingest doesn't use the session time zone: Arrow timestamps with
	// a zone are instants, and ones without one (into a TIMESTAMP WITH TIME
	// ZONE column) are UTC.
	h.exec(`SET TIME ZONE 'America/Los_Angeles'`)
	h.exec(`CREATE TABLE it_stz_ing (id BIGINT, tz TIMESTAMPTZ, naive TIMESTAMPTZ)`)
	mem := memory.DefaultAllocator
	ib := array.NewInt64Builder(mem)
	zb := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/New_York"})
	nb := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond})
	for _, b := range []array.Builder{ib, zb, nb} {
		defer b.Release()
	}
	us := arrow.Timestamp(time.Date(2024, 1, 10, 5, 0, 0, 0, time.UTC).UnixMicro())
	ib.Append(1)
	zb.Append(us)
	nb.Append(us)
	cols := []arrow.Array{ib.NewArray(), zb.NewArray(), nb.NewArray()}
	for _, c := range cols {
		defer c.Release()
	}
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "tz", Type: cols[1].DataType()},
		{Name: "naive", Type: cols[2].DataType()},
	}, nil), cols, 1)
	defer rec.Release()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	for k, v := range map[string]string{adbc.OptionKeyIngestTargetTable: "it_stz_ing", adbc.OptionKeyIngestMode: adbc.OptionValueIngestModeAppend} {
		if err := st.SetOption(h.ctx, k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Bind(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ExecuteUpdate(h.ctx); err != nil || n != 1 {
		t.Fatalf("ingest: %d, %v", n, err)
	}
	h2.expectRows(`SELECT tz, naive FROM it_stz_ing`, "2024-01-10T05:00:00Z|2024-01-10T05:00:00Z")
	h.expectRows(`SELECT CAST(tz AS TEXT), CAST(naive AS TEXT) FROM it_stz_ing`, "2024-01-09 21:00:00-08|2024-01-09 21:00:00-08")

	// adbc.redis.time_zone: on the database, the default of its
	// connections (what RESET goes back to); on a connection, its zone.
	hz := newRekeyHarness(t, map[string]string{OptionStringTimeZone: "asia/kolkata"})
	hz.expectRows(`SHOW timezone`, "Asia/Kolkata")
	hz.expectRows(`SELECT CAST(tz AS TEXT) FROM it_stz WHERE id = 1`, "2024-01-10 10:30:00+05:30")
	hz.exec(`SET TIME ZONE 'UTC'`)
	hz.expectRows(`SELECT CAST(tz AS TEXT) FROM it_stz WHERE id = 1`, "2024-01-10 05:00:00+00")
	hz.exec(`RESET timezone`)
	hz.expectRows(`SHOW timezone`, "Asia/Kolkata")
	getTZ := func(h *sqlHarness) string {
		v, err := h.conn.(adbc.GetSetOptionsWithContext).GetOption(h.ctx, OptionStringTimeZone)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if got := getTZ(hz); got != "Asia/Kolkata" {
		t.Errorf("GetOption: %q", got)
	}
	if err := hz.setConnOption(OptionStringTimeZone, "America/Los_Angeles"); err != nil {
		t.Fatal(err)
	}
	hz.expectRows(`SHOW timezone`, "America/Los_Angeles")
	hz.exec(`SET TIME ZONE 'Asia/Tokyo'`)
	if got := getTZ(hz); got != "Asia/Tokyo" {
		t.Errorf("GetOption after SET: %q", got)
	}
	hz.exec(`RESET timezone`)
	hz.expectRows(`SHOW timezone`, "America/Los_Angeles")
	if err := hz.setConnOption(OptionStringTimeZone, "Mars/Base"); err == nil || !strings.Contains(err.Error(), `invalid value for parameter "TimeZone": "Mars/Base"`) {
		t.Errorf("SetOption(Mars/Base): %v", err)
	}
	hz.expectRows(`SHOW timezone`, "America/Los_Angeles")
	if _, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(h.ctx, map[string]string{
		adbc.OptionKeyURI: "redis://localhost:1", OptionStringTimeZone: "Mars/Base"}); err == nil ||
		!strings.Contains(err.Error(), `invalid value for parameter "TimeZone": "Mars/Base"`) {
		t.Errorf("database option Mars/Base: %v", err)
	}
	h2.expectRows(`SHOW timezone`, "Asia/Kolkata")
}
