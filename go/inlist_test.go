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
	"math/rand"
	"strings"
	"testing"
)

// inListLiterals covers every kind compareValues handles, with values that
// are equal across kinds (1, 1.0, TRUE, '1', 1.0e0), NULLs, and kinds that
// can't be compared (strings with numbers, dates with times).
var inListLiterals = []string{
	"1", "2", "-3", "0", "9007199254740993", "CAST(7 AS SMALLINT)",
	"1.0", "1.50", "0.1", "2.25",
	"CAST(1.5 AS DOUBLE PRECISION)", "CAST(2 AS DOUBLE PRECISION)", "CAST(0.1 AS REAL)",
	"TRUE", "FALSE",
	"'a'", "'1'", "'01'", "''", "'1.5'", "X'61'",
	"DATE '2024-01-02'", "TIMESTAMP '2024-01-02 00:00:00'", "TIMESTAMP '2024-01-02 12:00:00'",
	"TIME '12:00:00'", "INTERVAL '1 day'", "INTERVAL '24 hours'",
	"NULL", "CAST(NULL AS INTEGER)",
}

// evalBool evaluates a boolean expression as the driver does, rendering an
// error as "error".
func evalBool(t *testing.T, sql string) string {
	t.Helper()
	v, err := (&evalEnv{exec: &executor{}}).eval(parseTestExpr(t, sql))
	switch {
	case err != nil:
		return "error"
	case v.Null:
		return "NULL"
	}
	return v.Text()
}

// TestInListMatchesOrChain checks that a long literal IN list answers
// exactly like the chain of = it is parsed as (written out by hand here, so
// it isn't recognized as a list): same results, same NULLs, same errors.
func TestInListMatchesOrChain(t *testing.T) {
	rnd := rand.New(rand.NewSource(37))
	pools := [][]string{inListLiterals}
	byKind := map[string][]string{
		"num":  {"1", "2", "-3", "0", "9007199254740993", "CAST(7 AS SMALLINT)", "1.0", "1.50", "0.1", "2.25", "TRUE", "FALSE"},
		"flt":  {"CAST(1.5 AS DOUBLE PRECISION)", "CAST(2 AS DOUBLE PRECISION)", "CAST(0.1 AS REAL)", "1", "2.25"},
		"str":  {"'a'", "'1'", "'01'", "''", "'1.5'", "X'61'"},
		"time": {"DATE '2024-01-02'", "TIMESTAMP '2024-01-02 00:00:00'", "TIMESTAMP '2024-01-02 12:00:00'"},
	}
	for _, p := range byKind {
		pools = append(pools, append(append([]string{}, p...), "NULL"), p)
	}
	before := subqueryStats.inLists.Load()
	checked := 0
	for _, pool := range pools {
		for n := 0; n < 30; n++ {
			size := inListMin + rnd.Intn(24)
			list := make([]string, size)
			for i := range list {
				list[i] = pool[rnd.Intn(len(pool))]
			}
			for _, x := range inListLiterals {
				var chain []string
				for _, v := range list {
					chain = append(chain, "("+x+") = "+v)
				}
				in := x + " IN (" + strings.Join(list, ", ") + ")"
				or := strings.Join(chain, " OR ")
				if got, want := evalBool(t, in), evalBool(t, or); got != want {
					t.Fatalf("%s\n got %s, want %s (the OR chain)", in, got, want)
				}
				if got, want := evalBool(t, "NOT "+in), evalBool(t, "NOT ("+or+")"); got != want {
					t.Fatalf("NOT %s\n got %s, want %s", in, got, want)
				}
				checked++
			}
		}
	}
	if subqueryStats.inLists.Load() == before {
		t.Fatal("no IN list used its hash set")
	}
	t.Logf("%d lists × values checked; %d answered by the hash set", checked, subqueryStats.inLists.Load()-before)
}

func TestInListRecognition(t *testing.T) {
	long := make([]string, inListMin)
	for i := range long {
		long[i] = "1"
	}
	for _, c := range []struct {
		sql  string
		want bool
	}{
		{"x IN (" + strings.Join(long, ", ") + ")", true},
		{"x IN (" + strings.Join(long[:inListMin-1], ", ") + ")", false}, // short: compared one by one
		{"x IN (" + strings.Join(long, ", ") + ", y)", false},            // not all constant
		{"RANDOM() IN (" + strings.Join(long, ", ") + ")", false},        // x must be evaluated once per value
		{"x = 1 OR x = 1 OR x = 1 OR x = 1 OR x = 1 OR x = 1 OR x = 1 OR x = 1 OR " +
			"x = 1 OR x = 1 OR x = 1 OR x = 1 OR x = 1 OR x = 1 OR x = 1 OR x = 1", false}, // separate x nodes
	} {
		b, ok := parseTestExpr(t, c.sql).(*Binary)
		got := false
		if ok {
			_, _, got = literalInList(b)
		}
		if got != c.want {
			t.Errorf("%s: recognized %v, want %v", c.sql, got, c.want)
		}
	}
}
