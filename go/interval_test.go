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

// Unit tests for rendering intervals as text. They need no Redis server.

import (
	"math"
	"testing"
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
