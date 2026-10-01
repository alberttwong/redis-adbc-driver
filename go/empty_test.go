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
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestNeverTrue(t *testing.T) {
	for _, c := range []struct {
		cond  string
		never bool
	}{
		{"false", true}, {"1 = 0", true}, {"NULL", true}, {"CAST(NULL AS BOOLEAN)", true},
		{"x > 0 AND false", true}, {"false AND x > 0", true}, {"NOT true", true}, {"NOT NULL", true},
		{"false OR 1 = 0", true}, {"NOT (x > 0 OR true)", true}, {"0", true}, {"'a' = 'b'", true},
		{"x > 0 AND (false OR NULL)", true}, {"1 / 0 = 1 AND false", true}, {"NOT (NOT false)", true},
		{"x IN (1, 2) AND 1 > 2", true},

		{"true", false}, {"1", false}, {"x > 0", false}, {"x > 0 OR false", false}, {"NOT (x > 0 AND false)", false},
		{"NULL OR x > 0", false}, {"1 = 1 AND x > 0", false}, {"1 / 0 = 1", false}, {"$1", false},
		{"$1 AND true", false}, {"x IN (SELECT 1)", false}, {"EXISTS (SELECT 1 WHERE false)", false},
		{"RANDOM() > 2", false}, {"x IS NULL", false},
	} {
		parsed, err := ParseScript("SELECT 1 FROM t WHERE " + c.cond)
		if err != nil {
			t.Fatalf("%s: %v", c.cond, err)
		}
		e := &executor{}
		if got := e.neverTrue(context.Background(), parsed[0].Stmt.(*SelectStmt).Where); got != c.never {
			t.Errorf("neverTrue(%s) = %v, want %v", c.cond, got, c.never)
		}
	}
}

// TestJoinReads checks which items of a join in written order are read.
// Each case lists the items as kind[:n][:e] (n: ON is never true, e: no
// rows), and the items read, or "none".
func TestJoinReads(t *testing.T) {
	for _, c := range []struct{ join, want string }{
		{"- INNER", "0 1"},
		{"- INNER:n", "none"},
		{"- CROSS:e", "none"},
		{"-:e INNER", "none"},
		{"- LEFT:n", "0"},
		{"- LEFT:e", "0"},
		{"-:e LEFT", "none"},
		{"- RIGHT:n", "1"},
		{"- RIGHT:e", "none"},
		{"-:e RIGHT", "1"},
		{"- FULL:n", "0 1"},
		{"- FULL:e", "0"},
		{"-:e FULL", "1"},
		{"-:e FULL:e", "none"},
		{"- LEFT INNER:n", "none"},
		{"- LEFT RIGHT:n", "2"},
		{"- INNER:n RIGHT", "2"},
		{"- INNER:n FULL", "2"},
		{"- INNER:n LEFT", "none"},
		{"- INNER:n RIGHT INNER", "2 3"},
		{"- LEFT LEFT:n INNER", "0 1 3"},
		{"- RIGHT:n LEFT:n", "1"},
		{"- FULL:e RIGHT", "0 2"},
	} {
		items := strings.Fields(c.join)
		kinds := make([]string, len(items))
		never := make([]bool, len(items))
		empty := make([]bool, len(items))
		for i, it := range items {
			parts := strings.Split(it, ":")
			if parts[0] != "-" {
				kinds[i] = parts[0]
			}
			for _, p := range parts[1:] {
				never[i] = never[i] || p == "n"
				empty[i] = empty[i] || p == "e"
			}
		}
		read, none := joinReads(kinds, never, empty)
		var got []string
		for i, r := range read {
			if r {
				got = append(got, fmt.Sprint(i))
			}
		}
		g := strings.Join(got, " ")
		if none {
			if len(got) > 0 {
				t.Errorf("%s: none, but reads %s", c.join, g)
			}
			g = "none"
		}
		if g != c.want {
			t.Errorf("%s: reads %q, want %q", c.join, g, c.want)
		}
	}
}
