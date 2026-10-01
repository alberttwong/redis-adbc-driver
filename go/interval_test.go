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

// Unit tests for rendering intervals as text and for the interval range.
// They need no Redis server.

import (
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// formatInterval renders the time part's sign once, in front of the clock,
// including for the extremes of the nanosecond field: negating MinInt64
// overflows int64, so its magnitude must not be taken in int64.
func TestFormatInterval(t *testing.T) {
	iv := func(months, days int32, ns int64) Value {
		return Value{T: typeInterval, Months: months, Days: days, I: ns}
	}
	cases := []struct {
		v    Value
		want string
	}{
		{iv(0, 0, 0), "00:00:00"},
		{iv(0, 0, 1), "00:00:00.000000001"},
		{iv(0, 0, -1), "-00:00:00.000000001"},
		{iv(0, 0, -(4*nsPerHour + 5*nsPerMinute + 6*nsPerSecond + 500_000_000)), "-04:05:06.5"},
		{iv(0, 0, math.MaxInt64), "2562047:47:16.854775807"},
		{iv(0, 0, -math.MaxInt64), "-2562047:47:16.854775807"},
		{iv(0, 0, math.MinInt64), "-2562047:47:16.854775808"},
		{iv(14, 3, math.MinInt64), "1 year 2 mons 3 days -2562047:47:16.854775808"},
		{iv(-14, -3, 0), "-1 years -2 mons -3 days"},
	}
	for _, c := range cases {
		if got := formatInterval(c.v); got != c.want {
			t.Errorf("formatInterval(%d months, %d days, %d ns) = %q, want %q", c.v.Months, c.v.Days, c.v.I, got, c.want)
		}
	}

	runScalarCases(t, []scalarCase{
		{"CAST(INTERVAL '-9223372036.854775808 seconds' AS VARCHAR)", "-2562047:47:16.854775808", "VARCHAR"},
		{"CAST(INTERVAL '-1 day -9223372036.854775808 seconds' AS VARCHAR)", "-1 days -2562047:47:16.854775808", "VARCHAR"},
	})
}

// The time part of an interval is int64 nanoseconds, about ±2562047 hours.
// Values at the ends of that range work; values beyond it are errors rather
// than saturating or wrapping around.
func TestIntervalRange(t *testing.T) {
	runScalarCases(t, []scalarCase{
		{"CAST(INTERVAL '2562047 hours' AS VARCHAR)", "2562047:00:00", "VARCHAR"},
		{"CAST(INTERVAL '-2562047 hours' AS VARCHAR)", "-2562047:00:00", "VARCHAR"},
		{"CAST(INTERVAL 'PT2562047H' AS VARCHAR)", "2562047:00:00", "VARCHAR"},
		{"CAST(INTERVAL '2562047:00:00' AS VARCHAR)", "2562047:00:00", "VARCHAR"},
		{"CAST(INTERVAL 2562047 HOUR AS VARCHAR)", "2562047:00:00", "VARCHAR"},
		// Exactly math.MinInt64 nanoseconds.
		{"CAST(INTERVAL '-9223372036.854775808 seconds' AS VARCHAR)", "-2562047:47:16.854775808", "VARCHAR"},
		{"CAST(INTERVAL '-9223372036.854775808 seconds' * 1 AS VARCHAR)", "-2562047:47:16.854775808", "VARCHAR"},
		{"CAST(INTERVAL '-9223372036.854775808 seconds' - INTERVAL '-1 second' AS VARCHAR)", "-2562047:47:15.854775808", "VARCHAR"},
		{"CAST(INTERVAL '-2562047 hours' - INTERVAL '2836.854775808 seconds' AS VARCHAR)", "-2562047:47:16.854775808", "VARCHAR"},
		{"CAST(INTERVAL '2562047 hours' + INTERVAL '-2562047 hours' AS VARCHAR)", "00:00:00", "VARCHAR"},
		{"CAST(INTERVAL '-2562047 hours' * 1 AS VARCHAR)", "-2562047:00:00", "VARCHAR"},
		// Days and months have their own fields.
		{"CAST(INTERVAL '2562047 hours' + INTERVAL '1000000 days' AS VARCHAR)", "1000000 days 2562047:00:00", "VARCHAR"},
		// time ± interval wraps around midnight, whatever the interval's size.
		{"CAST(TIME '23:00:00' + INTERVAL '2562047 hours' AS VARCHAR)", "22:00:00.000000", "VARCHAR"},
		{"CAST(TIME '01:00:00' - INTERVAL '-9223372036.854775808 seconds' AS VARCHAR)", "00:47:16.854775", "VARCHAR"},
		{"CAST(TIMESTAMP '2000-01-01 00:00:00' + INTERVAL '-9223372036.854775808 seconds' AS VARCHAR)", "1707-09-22 00:12:43.145224", "VARCHAR"},
	})
	for _, expr := range []string{
		"INTERVAL '99999999999 seconds'",
		"INTERVAL '-99999999999 seconds'",
		"INTERVAL '2562048 hours'",
		"INTERVAL '-2562048 hours'",
		"INTERVAL '2562047 hours 0.5 days'", // the fraction of a day cascades into the time
		"INTERVAL 'PT2562048H'",
		"INTERVAL '2562048:00:00'",
		"INTERVAL 2562048 HOUR",
		"INTERVAL 'Infinity' SECOND",
		"INTERVAL 'NaN' SECOND",
		// 2^63 nanoseconds: float64(math.MaxInt64) rounds up to this.
		"INTERVAL '9223372036.854775808 seconds'",
		"INTERVAL '2562047 hours' * 2",
		"INTERVAL '1 hour' * 2562048",
		"INTERVAL '2562047 hours' / 0.5",
		"INTERVAL '-9223372036.854775808 seconds' * -1",
		"INTERVAL '-9223372036.854775808 seconds' / -1",
		"INTERVAL '2562047 hours' + INTERVAL '1 hour'",
		"INTERVAL '-2562047 hours' - INTERVAL '1 hour'",
		"INTERVAL '-9223372036.854775808 seconds' + INTERVAL '-0.000000001 seconds'",
		"INTERVAL '0 seconds' - INTERVAL '-9223372036.854775808 seconds'",
		"-INTERVAL '-9223372036.854775808 seconds'",
		// As in Postgres, subtracting an interval that can't be negated.
		"TIMESTAMP '2000-01-01 00:00:00' - INTERVAL '-9223372036.854775808 seconds'",
	} {
		// An interval literal is parsed with the statement.
		_, err := ParseScript("SELECT " + expr)
		if err == nil {
			_, _, err = evalTestExpr(t, expr)
		}
		if err == nil || !strings.Contains(err.Error(), "interval out of range") {
			t.Errorf("%s: error %v, want interval out of range", expr, err)
		}
	}
}

// An Arrow duration converts to an interval's nanoseconds, and one too long
// for them is an error.
func TestIntervalFromArrowDuration(t *testing.T) {
	b := array.NewDurationBuilder(memory.NewGoAllocator(), &arrow.DurationType{Unit: arrow.Second})
	defer b.Release()
	b.AppendValues([]arrow.Duration{-9223372036, 9223372036, 9223372037, -9223372037}, nil)
	arr := b.NewArray()
	defer arr.Release()
	for i, want := range []string{"-2562047:47:16", "2562047:47:16", "", ""} {
		v, err := valueAt(arr, i)
		switch {
		case want == "" && (err == nil || err.Error() != "interval out of range"):
			t.Errorf("duration %v: %s, error %v, want interval out of range", arr.(*array.Duration).Value(i), v.Text(), err)
		case want != "" && err != nil:
			t.Errorf("duration %v: %v", arr.(*array.Duration).Value(i), err)
		case want != "" && v.Text() != want:
			t.Errorf("duration %v = %s, want %s", arr.(*array.Duration).Value(i), v.Text(), want)
		}
	}
}
