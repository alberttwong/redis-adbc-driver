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

// Integration tests for dates and times as text (#84), time zone conversion
// (#88) and TO_CHAR's patterns (#90), on stored rows.

import "testing"

// #84: timestamps, times and dates become text as in Postgres, whatever
// the declared precision, in every way they become text.
func TestDateTimeTextIssue84(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_dtx", "it_dtx_txt")
	h.exec(`CREATE TABLE it_dtx (id INTEGER, ts0 TIMESTAMP(0), ts3 TIMESTAMP(3), ts6 TIMESTAMP, ts9 TIMESTAMP(9),
		tz3 TIMESTAMP(3) WITH TIME ZONE, tz6 TIMESTAMPTZ, t3 TIME(3), t6 TIME, d DATE)`)
	h.exec(`INSERT INTO it_dtx VALUES
		(1, '2024-01-10 10:00:00', '2024-01-10 10:00:00', '2024-01-10 10:00:00', '2024-01-10 10:00:00',
		    '2024-01-10 10:00:00+00', '2024-01-10 10:00:00+00', '10:00:00', '10:00:00', '2024-01-10'),
		(2, '2024-01-10 10:00:01', '2024-01-10 10:00:00.5', '2024-01-10 10:00:00.5', '2024-01-10 10:00:00.5',
		    '2024-01-10 10:00:00.5-08', '2024-01-10 10:00:00.5-08', '10:00:00.5', '10:00:00.5', '0044-03-15 BC'),
		(3, '2024-01-10 10:00:02', '2024-01-10 10:00:00.050', '2024-01-10 10:00:00.050', '2024-01-10 10:00:00.050',
		    '2024-01-10 10:00:00.05+05:30', '2024-01-10 10:00:00.05+05:30', '23:59:59.05', '23:59:59.05', '10000-01-01'),
		(4, NULL, '2024-01-10 10:00:00.123', '2024-01-10 10:00:00.123456', '2024-01-10 10:00:00.123456',
		    '2024-01-10 10:00:00.123-08:30', '2024-01-10 10:00:00.123456-08:30', '10:00:00.123', '10:00:00.123456', NULL),
		(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)

	// Each type and precision as text.
	h.expectRows(`SELECT id, CAST(ts0 AS VARCHAR), CAST(ts3 AS VARCHAR), CAST(ts6 AS TEXT), ts9::text,
			CAST(tz3 AS VARCHAR), CAST(tz6 AS VARCHAR), CAST(t3 AS VARCHAR), CAST(t6 AS VARCHAR), CAST(d AS VARCHAR)
		FROM it_dtx ORDER BY id`,
		"1|2024-01-10 10:00:00|2024-01-10 10:00:00|2024-01-10 10:00:00|2024-01-10 10:00:00|"+
			"2024-01-10 10:00:00+00|2024-01-10 10:00:00+00|10:00:00|10:00:00|2024-01-10",
		"2|2024-01-10 10:00:01|2024-01-10 10:00:00.5|2024-01-10 10:00:00.5|2024-01-10 10:00:00.5|"+
			"2024-01-10 18:00:00.5+00|2024-01-10 18:00:00.5+00|10:00:00.5|10:00:00.5|0044-03-15 BC",
		"3|2024-01-10 10:00:02|2024-01-10 10:00:00.05|2024-01-10 10:00:00.05|2024-01-10 10:00:00.05|"+
			"2024-01-10 04:30:00.05+00|2024-01-10 04:30:00.05+00|23:59:59.05|23:59:59.05|10000-01-01",
		"4|NULL|2024-01-10 10:00:00.123|2024-01-10 10:00:00.123456|2024-01-10 10:00:00.123456|"+
			"2024-01-10 18:30:00.123+00|2024-01-10 18:30:00.123456+00|10:00:00.123|10:00:00.123456|NULL",
		"5|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL")

	// ||, CONCAT and CONCAT_WS (which skip NULLs).
	h.expectRows(`SELECT id, 'x' || ts3, ts6 || '|' || tz6, CONCAT(ts3, '/', t3, '/', d), CONCAT_WS(',', ts0, ts9, t6)
		FROM it_dtx ORDER BY id`,
		"1|x2024-01-10 10:00:00|2024-01-10 10:00:00|2024-01-10 10:00:00+00|2024-01-10 10:00:00/10:00:00/2024-01-10|"+
			"2024-01-10 10:00:00,2024-01-10 10:00:00,10:00:00",
		"2|x2024-01-10 10:00:00.5|2024-01-10 10:00:00.5|2024-01-10 18:00:00.5+00|2024-01-10 10:00:00.5/10:00:00.5/0044-03-15 BC|"+
			"2024-01-10 10:00:01,2024-01-10 10:00:00.5,10:00:00.5",
		"3|x2024-01-10 10:00:00.05|2024-01-10 10:00:00.05|2024-01-10 04:30:00.05+00|2024-01-10 10:00:00.05/23:59:59.05/10000-01-01|"+
			"2024-01-10 10:00:02,2024-01-10 10:00:00.05,23:59:59.05",
		"4|x2024-01-10 10:00:00.123|2024-01-10 10:00:00.123456|2024-01-10 18:30:00.123456+00|2024-01-10 10:00:00.123/10:00:00.123/|"+
			"2024-01-10 10:00:00.123456,10:00:00.123456",
		"5|NULL|NULL|//|")

	// STRING_AGG and LISTAGG.
	h.expectRows(`SELECT STRING_AGG(ts3, ',' ORDER BY id), STRING_AGG(CAST(tz6 AS VARCHAR), ';' ORDER BY id),
			STRING_AGG(t6, ' ' ORDER BY id), LISTAGG(d, '|') WITHIN GROUP (ORDER BY id) FROM it_dtx`,
		"2024-01-10 10:00:00,2024-01-10 10:00:00.5,2024-01-10 10:00:00.05,2024-01-10 10:00:00.123|"+
			"2024-01-10 10:00:00+00;2024-01-10 18:00:00.5+00;2024-01-10 04:30:00.05+00;2024-01-10 18:30:00.123456+00|"+
			"10:00:00 10:00:00.5 23:59:59.05 10:00:00.123456|2024-01-10|0044-03-15 BC|10000-01-01")
	h.expectRows(`SELECT CAST(ts3 AS VARCHAR) = CAST(ts9 AS VARCHAR) AS same, STRING_AGG(CAST(id AS VARCHAR), ',' ORDER BY id)
		FROM it_dtx GROUP BY CAST(ts3 AS VARCHAR) = CAST(ts9 AS VARCHAR) ORDER BY same`,
		"false|4", "true|1,2,3", "NULL|5")

	// Hashes of the text agree across precisions, and with Postgres: the
	// text is 2024-01-10 10:00:00, as dbt_utils' generate_surrogate_key and
	// dbt snapshots' dbt_scd_id hash it.
	h.expectRows(`SELECT id, MD5(CAST(ts3 AS VARCHAR)) = MD5(CAST(ts6 AS VARCHAR)), MD5(CAST(ts6 AS VARCHAR)) = MD5(CAST(ts9 AS VARCHAR)),
			MD5(CAST(tz3 AS VARCHAR)) = MD5(CAST(tz6 AS VARCHAR))
		FROM it_dtx ORDER BY id`,
		"1|true|true|true", "2|true|true|true", "3|true|true|true", "4|false|true|false", "5|NULL|NULL|NULL")
	h.expectRows(`SELECT id, md5(cast(coalesce(cast(ts3 as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)),
			md5(coalesce(cast(id as varchar), '') || '|' || coalesce(cast(ts6 as varchar), ''))
		FROM it_dtx WHERE id IN (1, 2, 5) ORDER BY id`,
		"1|b4f75bf4a0f2ba251eb0ce41710d6971|a69ef24286aa4de03c1970cb3935b7b1",
		"2|3a2f2dda8f8d77bdc984e6cdfbfeba55|d71f605dd148fbd869b518bfca1d9c76",
		"5|f14cc5cdce0420f4a5a6b6d9d7b85f39|e6450fe1829d95134378cf3394e40b30")

	// The text in conditions.
	h.expectRows(`SELECT id FROM it_dtx WHERE CAST(ts6 AS VARCHAR) = '2024-01-10 10:00:00.5'`, "2")
	h.expectRows(`SELECT id FROM it_dtx WHERE CAST(ts9 AS VARCHAR) LIKE '%:00' OR CAST(d AS VARCHAR) LIKE '% BC' ORDER BY id`, "1", "2")

	// JSON has ISO 8601 text.
	h.expectRows(`SELECT TO_JSON(ts3), TO_JSON(tz6), TO_JSON(d), JSON_BUILD_OBJECT('t', t6) FROM it_dtx WHERE id = 2`,
		`"2024-01-10T10:00:00.5"|"2024-01-10T18:00:00.5+00:00"|"0044-03-15 BC"|{"t" : "10:00:00.5"}`)

	// The text reads back as the same values.
	h.exec(`CREATE TABLE it_dtx_txt AS SELECT id, CAST(ts3 AS VARCHAR) AS s3, CAST(tz6 AS VARCHAR) AS sz, CAST(d AS VARCHAR) AS sd FROM it_dtx`)
	h.expectRows(`SELECT a.id, CAST(b.s3 AS TIMESTAMP(3)) = a.ts3, CAST(b.sz AS TIMESTAMPTZ) = a.tz6, CAST(b.sd AS DATE) = a.d
		FROM it_dtx a JOIN it_dtx_txt b ON a.id = b.id ORDER BY a.id`,
		"1|true|true|true", "2|true|true|true", "3|true|true|true", "4|true|true|NULL", "5|NULL|NULL|NULL")

	// Intervals: a positive field after a negative one has a +.
	h.expectRows(`SELECT CAST(ts6 - TIMESTAMP '2024-01-11 12:00:00' AS VARCHAR), CAST(INTERVAL '-1 day' + INTERVAL '3 hours' AS VARCHAR)
		FROM it_dtx WHERE id = 1`, "-1 days -02:00:00|-1 days +03:00:00")
}

// #88: AT TIME ZONE, TIMEZONE() and CONVERT_TIMEZONE() on stored rows,
// across DST transitions, and in the forms dbt_date and dbt_expectations
// use.
func TestTimeZonesIssue88(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tzx", "it_tzx_ctas", "it_tzx_def")
	h.dropViews("it_tzx_v")
	h.exec(`CREATE TABLE it_tzx (id INTEGER, ts TIMESTAMP, tz TIMESTAMPTZ, zone VARCHAR)`)
	h.exec(`INSERT INTO it_tzx VALUES
		(1, '2024-03-10 02:30:00', '2024-03-10 10:00:00+00', 'America/Los_Angeles'),
		(2, '2024-11-03 01:30:00', '2024-11-03 09:30:00+00', 'America/Los_Angeles'),
		(3, '2024-03-31 01:30:00', '2024-03-31 01:00:00+00', 'Europe/London'),
		(4, '2024-10-27 01:30:00', '2024-10-27 00:30:00+00', 'europe/london'),
		(5, '2024-01-10 10:00:00', '2024-01-10 10:00:00+00', '+05:30'),
		(6, NULL, NULL, 'UTC'),
		(7, '2024-01-10 10:00:00', '2024-01-10 10:00:00+00', NULL)`)

	// Zones from a column: DST gaps take the offset before the transition,
	// overlaps the offset after it; '+05:30' is POSIX-style, UTC-05:30.
	h.expectRows(`SELECT id, CAST(ts AT TIME ZONE zone AS VARCHAR), CAST(tz AT TIME ZONE zone AS VARCHAR),
			CAST(CONVERT_TIMEZONE(zone, 'UTC', ts) AS VARCHAR), CAST(CONVERT_TIMEZONE('UTC', zone, ts) AS VARCHAR)
		FROM it_tzx ORDER BY id`,
		"1|2024-03-10 10:30:00+00|2024-03-10 03:00:00|2024-03-10 10:30:00|2024-03-09 18:30:00",
		"2|2024-11-03 09:30:00+00|2024-11-03 01:30:00|2024-11-03 09:30:00|2024-11-02 18:30:00",
		"3|2024-03-31 01:30:00+00|2024-03-31 02:00:00|2024-03-31 01:30:00|2024-03-31 02:30:00",
		"4|2024-10-27 01:30:00+00|2024-10-27 01:30:00|2024-10-27 01:30:00|2024-10-27 01:30:00",
		"5|2024-01-10 15:30:00+00|2024-01-10 04:30:00|2024-01-10 15:30:00|2024-01-10 04:30:00",
		"6|NULL|NULL|NULL|NULL",
		"7|NULL|NULL|NULL|NULL")
	// Round trips: exact except for the local times a transition skips
	// (rows 1 and 3) or repeats (row 4: 00:30 UTC is the first 01:30 in
	// London, and 01:30 is read as the second, as in Postgres).
	h.expectRows(`SELECT id, (ts AT TIME ZONE zone) AT TIME ZONE zone = ts, (tz AT TIME ZONE zone) AT TIME ZONE zone = tz,
			CONVERT_TIMEZONE('UTC', zone, CONVERT_TIMEZONE(zone, 'UTC', ts)) = ts
		FROM it_tzx WHERE id <= 5 ORDER BY id`,
		"1|false|true|false", "2|true|true|true", "3|false|true|false", "4|true|false|true", "5|true|true|true")

	// The result types: AT TIME ZONE switches between timestamp and
	// timestamp with time zone; CONVERT_TIMEZONE gives a timestamp.
	schema := h.expectRows(`SELECT ts AT TIME ZONE 'UTC', tz AT TIME ZONE 'UTC', CONVERT_TIMEZONE('UTC', 'UTC', ts),
			TIMEZONE('UTC', tz), CONVERT_TIMEZONE('Asia/Tokyo', tz) FROM it_tzx WHERE id = 5`,
		"2024-01-10T10:00:00Z|2024-01-10T10:00:00|2024-01-10T10:00:00|2024-01-10T10:00:00|2024-01-10T19:00:00")
	expectTypes(t, schema, sfTSTZ, sfTS, sfTS, sfTS, sfTS)
	h.expectNoRows(dbtEmpty(`SELECT ts AT TIME ZONE zone AS utc, tz AT TIME ZONE 'Mars/Phobos' AS local FROM it_tzx`),
		"utc timestamp[us, tz=UTC], local timestamp[us]")

	// dbt_date's forms: now() on Postgres, and its default convert_timezone.
	h.expectRows(`SELECT cast(cast(ts as timestamp) at time zone 'UTC' at time zone 'America/Los_Angeles' as timestamp),
			convert_timezone('UTC', 'America/Los_Angeles', cast(ts as timestamp))
		FROM it_tzx WHERE id = 5`, "2024-01-10T02:00:00|2024-01-10T02:00:00")
	h.expectRows(`SELECT convert_timezone('UTC', 'America/Los_Angeles', cast(current_timestamp as timestamp)) =
			cast(cast(current_timestamp as timestamp) at time zone 'UTC' at time zone 'America/Los_Angeles' as timestamp),
		cast(cast(now() as timestamp) at time zone 'UTC' at time zone 'America/Los_Angeles' as timestamp) < cast(now() as timestamp)`,
		"true|true")
	// dbt_date's n_days_ago(7) on Postgres, and dbt_expectations'
	// recent-data bound (dbt.dateadd over dbt_date.now()).
	h.expectRows(`SELECT cast(cast(cast(cast(now() as timestamp) at time zone 'UTC' at time zone 'America/Los_Angeles' as timestamp) as date)
				+ ((interval '1 day') * (-1 * 7)) as date)
			= cast(convert_timezone('UTC', 'America/Los_Angeles', cast(now() as timestamp)) as date) - 7`, "true")
	h.expectRows(`SELECT max(ts) >= cast(cast(cast(now() as timestamp) at time zone 'UTC' at time zone 'America/Los_Angeles' as timestamp)
			+ ((interval '1 year') * (-10)) as timestamp) FROM it_tzx`, "true")

	// In conditions, GROUP BY and constants that push down.
	h.expectRows(`SELECT id FROM it_tzx WHERE ts AT TIME ZONE 'UTC' < TIMESTAMPTZ '2024-02-01 00:00:00+00' ORDER BY id`, "5", "7")
	h.expectRows(`SELECT CAST(tz AT TIME ZONE 'America/New_York' AS DATE) AS day, COUNT(*) FROM it_tzx WHERE tz IS NOT NULL
		GROUP BY day ORDER BY day`,
		"2024-01-10|2", "2024-03-10|1", "2024-03-30|1", "2024-10-26|1", "2024-11-03|1")
	h.expectRows(`SELECT id FROM it_tzx WHERE tz = TIMESTAMP '2024-01-10 15:30:00' AT TIME ZONE 'Asia/Kolkata' ORDER BY id`, "5", "7")

	// Stored: CREATE TABLE AS, a view, a DEFAULT, INSERT and UPDATE.
	h.exec(`CREATE TABLE it_tzx_ctas AS SELECT id, ts AT TIME ZONE zone AS utc, tz AT TIME ZONE zone AS local FROM it_tzx WHERE id <= 5`)
	schema = h.expectRows(`SELECT id, utc, local FROM it_tzx_ctas WHERE id = 5`, "5|2024-01-10T15:30:00Z|2024-01-10T04:30:00")
	expectTypes(t, schema, sfInt32, sfTSTZ, sfTS)
	h.exec(`CREATE VIEW it_tzx_v AS SELECT id, tz AT TIME ZONE 'Asia/Tokyo' AS tokyo FROM it_tzx`)
	h.expectRows(`SELECT id, CAST(tokyo AS VARCHAR) FROM it_tzx_v WHERE id IN (5, 6) ORDER BY id`, "5|2024-01-10 19:00:00", "6|NULL")
	h.exec(`CREATE TABLE it_tzx_def (id INTEGER, ingested TIMESTAMP DEFAULT CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')`)
	h.exec(`INSERT INTO it_tzx_def (id) VALUES (1)`)
	h.expectRows(`SELECT ingested IS NOT NULL, ingested < LOCALTIMESTAMP FROM it_tzx_def`, "true|true")
	h.exec(`INSERT INTO it_tzx VALUES (8, TIMESTAMPTZ '2024-07-04 16:00:00+00' AT TIME ZONE 'America/Chicago',
		TIMESTAMP '2024-07-04 12:00:00' AT TIME ZONE 'America/Chicago', 'America/Chicago')`)
	h.exec(`UPDATE it_tzx SET tz = ts AT TIME ZONE zone WHERE id = 1`)
	h.expectRows(`SELECT id, CAST(ts AS VARCHAR), CAST(tz AS VARCHAR) FROM it_tzx WHERE id IN (1, 8) ORDER BY id`,
		"1|2024-03-10 02:30:00|2024-03-10 10:30:00+00", "8|2024-07-04 11:00:00|2024-07-04 17:00:00+00")

	// Errors, with Postgres's messages.
	h.expectErrorText(`SELECT ts AT TIME ZONE 'Mars/Phobos' FROM it_tzx`, `time zone "Mars/Phobos" not recognized`)
	h.expectErrorText(`SELECT CONVERT_TIMEZONE('UTC', 'Nowhere', ts) FROM it_tzx WHERE id = 5`, `time zone "Nowhere" not recognized`)
	h.expectErrorText(`SELECT tz AT TIME ZONE INTERVAL '1 day' FROM it_tzx WHERE id = 5`,
		`interval time zone "1 day" must not include months or days`)
	h.expectErrorText(`SELECT CAST('10:00' AS TIME) AT TIME ZONE zone FROM it_tzx WHERE id = 5`, "function timezone(varchar, time) does not exist")
	h.expectErrorText(`SELECT TIMEZONE() FROM it_tzx`, "TIMEZONE expects 1 or 2 arguments")
}

// #90: TO_CHAR's template patterns, on stored rows.
func TestToCharIssue90(t *testing.T) {
	h := newSQLHarness(t)
	// The issue's query, and dbt_date's postgres__week_of_year.
	h.expectRows(`select to_char(date '2020-11-29', 'WW'), to_char(date '2020-11-29', 'W'),
			to_char(date '2020-11-29', 'J'),  to_char(date '2020-11-29', 'CC'), to_char(date '2020-11-29', 'RM')`,
		"48|5|2459183|21|XI  ")
	h.expectRows(`select cast(to_char(cast('2020-11-29' as date), 'WW') as integer)`, "48")

	h.dropTables("it_tcx")
	h.exec(`CREATE TABLE it_tcx (id INTEGER, d DATE, ts TIMESTAMP(3), tz TIMESTAMPTZ)`)
	h.exec(`INSERT INTO it_tcx VALUES (1, '2020-11-29', '2024-01-10 13:05:09.123', '2024-01-10 13:05:09+00'),
		(2, '2020-12-31', NULL, NULL), (3, '2021-01-01', NULL, NULL), (4, '2024-02-29', NULL, NULL),
		(5, '0044-03-15 BC', NULL, NULL), (6, NULL, NULL, NULL)`)
	h.expectRows(`SELECT id, TO_CHAR(d, 'YYYY-MM-DD WW W J CC RM IW IYYY IDDD DDD'), TO_CHAR(d, 'FMDDth "of" FMMonth, Y,YYY BC')
		FROM it_tcx ORDER BY id`,
		"1|2020-11-29 48 5 2459183 21 XI   48 2020 336 334|29th of November, 2,020 AD",
		"2|2020-12-31 53 5 2459215 21 XII  53 2020 368 366|31st of December, 2,020 AD",
		"3|2021-01-01 01 1 2459216 21 I    53 2020 369 001|1st of January, 2,021 AD",
		"4|2024-02-29 09 5 2460370 21 II   09 2024 060 060|29th of February, 2,024 AD",
		"5|0044-03-15 11 3 1705428 -01 III  11 0044 075 074|15th of March, 0,044 BC",
		"6|NULL|NULL")
	h.expectRows(`SELECT TO_CHAR(ts, 'HH12:MI:SS.MS AM SSSS FF3 US [TZ]'), TO_CHAR(tz, 'YYYY-MM-DD HH24:MI:SS TZ OF'),
			TO_CHAR(ts, 'FMHH24"h"MI"m"SS"s"')
		FROM it_tcx WHERE id = 1`,
		"01:05:09.123 PM 47109 123 123000 []|2024-01-10 13:05:09 UTC +00|13h05m09s")
}
