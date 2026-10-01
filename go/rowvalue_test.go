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

// memQuery parses, plans and runs a query that reads no table, rendering
// its rows as "v1|v2|…" (NULL for nulls), or its error as "error: …"
// without the driver's prefix.
func memQuery(q string) string {
	stmts, err := ParseScript(q)
	if err != nil {
		return "error: " + errText(err)
	}
	rows, err := runInMemory(stmts[0].Stmt.(*SelectStmt))
	if err != nil {
		return "error: " + errText(err)
	}
	var out []string
	for _, r := range rows {
		cells := make([]string, len(r))
		for i, v := range r {
			switch {
			case v.Null:
				cells[i] = "NULL"
			case v.T.Kind == KindBool:
				cells[i] = showBool(v)
			default:
				cells[i] = v.Text()
			}
		}
		out = append(out, strings.Join(cells, "|"))
	}
	return strings.Join(out, "\n")
}

// Row comparisons, with the results Postgres's rules give (worked out by
// hand).
func TestRowValueExpressions(t *testing.T) {
	const s = "WITH s(a, b) AS (SELECT 1, 'x' UNION ALL SELECT 2, NULL UNION ALL SELECT NULL, 'z'), " +
		"s2(a, b) AS (SELECT 1, 'x' UNION ALL SELECT 2, 'y') "
	cases := []struct{ q, want string }{
		// = and <>: NULL when the result depends on a NULL item.
		{"SELECT (1, 2) = (1, 2), (1, 2) = (1, 3), (1, NULL) = (1, 2), (1, NULL) = (2, 2), (NULL, NULL) = (NULL, NULL)",
			"true|false|NULL|false|NULL"},
		{"SELECT (1, 2) <> (1, 3), (1, 2) <> (1, 2), (NULL, 2) <> (1, 2), (NULL, 2) <> (1, 3)",
			"true|false|NULL|true"},
		{"SELECT ROW(1, 'a', 2.5) = ROW(1, 'a', 2.50), (1, 'a') = (1.0, 'a'), (TRUE, 'a') = (TRUE, 'b')",
			"true|true|false"},
		// Ordering: left to right, up to the first pair that is unequal or
		// has a NULL.
		{"SELECT (1, 2) < (1, 3), (1, 2) < (1, 2), (1, 2) <= (1, 2), (1, 3) > (1, 2), (1, 2) >= (1, 3)",
			"true|false|true|true|false"},
		{"SELECT (2, NULL) > (1, 5), (1, NULL) < (1, 5), (NULL, 1) < (2, 5), (0, NULL) < (1, NULL)",
			"true|NULL|NULL|true"},
		{"SELECT (1, 2, 3) < (1, 2, 4), (1, 2, 3) >= (1, 2, 3), (1, 2, NULL) >= (1, 2, 3), (1, 3, NULL) >= (1, 2, 3), ('a', 1) < ('b', 0)",
			"true|true|NULL|true|true"},
		// IS [NOT] DISTINCT FROM: NULLs are equal.
		{"SELECT (1, NULL) IS DISTINCT FROM (1, NULL), (1, NULL) IS NOT DISTINCT FROM (1, NULL), " +
			"(1, NULL) IS DISTINCT FROM (1, 2), (1, 2) IS NOT DISTINCT FROM (1, 2)",
			"false|true|true|true"},
		{"SELECT 1 IS DISTINCT FROM NULL, NULL IS DISTINCT FROM NULL, 1 IS NOT DISTINCT FROM 1, 1 IS DISTINCT FROM 1.0, " +
			"'a' IS NOT DISTINCT FROM NULL, NULL IS NOT DISTINCT FROM NULL",
			"true|false|true|false|false|true"},
		// IS [NOT] NULL: every item NULL / no item NULL.
		{"SELECT (1, 2) IS NULL, (NULL, NULL) IS NULL, (NULL, 1) IS NULL, (NULL, 1) IS NOT NULL, (1, 1) IS NOT NULL, " +
			"ROW() IS NULL, ROW() IS NOT NULL, NOT (NULL, 1) IS NULL",
			"false|true|false|false|true|true|true|true"},
		// IN lists.
		{"SELECT (1, 'x') IN ((1, 'x'), (2, 'y')), (1, 'z') IN ((1, 'x'), (2, 'y')), (1, NULL) IN ((1, 'x')), " +
			"(1, NULL) IN ((2, 'x')), (1, NULL) NOT IN ((2, 'x')), (1, NULL) NOT IN ((1, 'x'))",
			"true|false|NULL|false|true|NULL"},
		{"SELECT (2, 'y') IN ((1, 'x'), (NULL, 'y')), (2, 'y') NOT IN ((1, 'x'), (NULL, 'z')), ROW(2, 'y') IN (ROW(2, 'y'))",
			"NULL|true|true"},
		// IN subqueries, with NULLs on either side.
		{s + "SELECT (1, 'x') IN (SELECT a, b FROM s), (2, 'x') IN (SELECT a, b FROM s), (3, 'x') IN (SELECT a, b FROM s), " +
			"(3, 'z') IN (SELECT a, b FROM s), (3, 'z') NOT IN (SELECT a, b FROM s), (3, 'x') NOT IN (SELECT a, b FROM s)",
			"true|NULL|false|NULL|NULL|true"},
		{s + "SELECT (NULL, NULL) IN (SELECT a, b FROM s), (NULL, 'q') IN (SELECT a, b FROM s), " +
			"(NULL, NULL) IN (SELECT a, b FROM s WHERE false), (NULL, NULL) NOT IN (SELECT a, b FROM s WHERE false)",
			"NULL|NULL|false|true"},
		// = ANY / = SOME are IN, <> ALL is NOT IN; other operators compare
		// with each row.
		{s + "SELECT (1, 'x') = ANY (SELECT a, b FROM s2), (1, 'x') = SOME (SELECT a, b FROM s2), " +
			"(1, 'z') <> ALL (SELECT a, b FROM s2), (1, 'x') <> ALL (SELECT a, b FROM s2), (2, 'x') = ANY (SELECT a, b FROM s)",
			"true|true|true|false|NULL"},
		{s + "SELECT (1, 'a') < ANY (SELECT a, b FROM s2), (3, 'a') > ALL (SELECT a, b FROM s2), (2, 'a') > ALL (SELECT a, b FROM s2), " +
			"(2, NULL) > ALL (SELECT a, b FROM s2), (0, 'a') > ANY (SELECT a, b FROM s2), (NULL, 'a') <= ANY (SELECT a, b FROM s2)",
			"true|true|false|NULL|false|NULL"},
		{s + "SELECT (1, 'x') = ALL (SELECT a, b FROM s2), (1, 'x') = ALL (SELECT a, b FROM s2 WHERE a = 1), (1, 'q') <> ANY (SELECT a, b FROM s2), " +
			"(1, 'x') = ANY (SELECT a, b FROM s2 WHERE false), (1, 'x') < ALL (SELECT a, b FROM s2 WHERE false)",
			"false|true|true|false|true"},
		// A row compared with a subquery's one row (NULL for no row).
		{s + "SELECT (1, 'x') = (SELECT 1, 'x'), (1, 'x') = (SELECT a, b FROM s2 WHERE a = 5), (1, 'x') < (SELECT 1, 'y'), " +
			"(SELECT a, b FROM s2 WHERE a = 2) = (2, 'y'), (1, 'x') <> (SELECT a, b FROM s2 WHERE a = 1)",
			"true|NULL|true|true|false"},
		{s + "SELECT (1, 'x') = (SELECT a, b FROM s2)", "error: more than one row returned by a subquery used as an expression"},
		{s + "SELECT (SELECT a, b FROM s2 WHERE a = 2) IN ((1, 'x'), (2, 'y')), (SELECT 1, 2) BETWEEN (0, 5) AND (1, 2), " +
			"ROW(2) = (SELECT 2), ROW(2) > (SELECT 1)",
			"true|true|true|true"},
		// A set operation as the subquery.
		{"SELECT (1, 'x') IN (SELECT 2, 'y' UNION SELECT 1, 'x'), (3, 'z') NOT IN (SELECT 2, 'y' UNION ALL SELECT NULL, 'z')",
			"true|NULL"},
		// CASE and BETWEEN.
		{"SELECT CASE WHEN (1, 2) = (1, 2) THEN 'yes' ELSE 'no' END, CASE (1, 2) WHEN (1, 3) THEN 'a' WHEN (1, 2) THEN 'b' END, " +
			"CASE (1, NULL) WHEN (1, NULL) THEN 'a' ELSE 'c' END",
			"yes|b|c"},
		{"SELECT (1, 5) BETWEEN (1, 2) AND (2, 0), (1, 1) BETWEEN (1, 2) AND (2, 0), (1, 5) NOT BETWEEN (1, 2) AND (2, 0)",
			"true|false|false"},
		// ROW(…) with one item, and extra parentheses.
		{"SELECT ROW(1) = ROW(1), ROW(1, 2) = (1, 2), ROW(1) IN (SELECT 1), ((1, 2)) = (1, 2), ROW(1) < ROW(2)",
			"true|true|true|true|true"},
		// Over rows of a relation, also in WHERE, GROUP BY / HAVING and
		// ORDER BY.
		{s + "SELECT a, b, (a, b) = (1, 'x'), (a, b) < (2, 'a') FROM s ORDER BY a",
			"1|x|true|true\n2|NULL|false|NULL\nNULL|z|false|NULL"},
		{s + "SELECT a FROM s WHERE (a, b) IN (SELECT a, b FROM s2) OR (a, b) IS NOT DISTINCT FROM (NULL, 'z') ORDER BY a",
			"1\nNULL"},
		{s + "SELECT a, COUNT(*) FROM s GROUP BY a HAVING (a, COUNT(*)) IN (SELECT a, 1 FROM s2) ORDER BY a",
			"1|1\n2|1"},
		{s + "SELECT a FROM s2 ORDER BY CASE WHEN (a, b) > (1, 'x') THEN 0 ELSE 1 END", "2\n1"},
	}
	for _, c := range cases {
		if got := memQuery(c.q); got != c.want {
			t.Errorf("%s\n got: %q\nwant: %q", c.q, got, c.want)
		}
	}
}

// The errors, with Postgres's messages where it has one.
func TestRowValueErrors(t *testing.T) {
	const generic = "error: a row constructor can only be compared (=, <>, <, <=, >, >=, IS [NOT] DISTINCT FROM, IN, ANY, ALL) or tested with IS [NOT] NULL"
	cases := []struct{ q, want string }{
		{"SELECT (1, 2) = (1, 2, 3)", "error: unequal number of entries in row expressions"},
		{"SELECT (1, 2) < ROW(1)", "error: unequal number of entries in row expressions"},
		{"SELECT (1, 2) IN ((1, 2), (1, 2, 3))", "error: unequal number of entries in row expressions"},
		{"SELECT (1, 2) IS DISTINCT FROM (1, 2, 3)", "error: unequal number of entries in row expressions"},
		{"SELECT ROW() = ROW()", "error: cannot compare rows of zero length"},
		{"SELECT (1, 2) = 1", "error: operator does not exist: record = bigint"},
		{"SELECT 'a' = (1, 2)", "error: operator does not exist: varchar = record"},
		{"SELECT (1, 2) IN (1, 2)", "error: operator does not exist: record = bigint"},
		{"SELECT 1 IN ((1, 2))", "error: operator does not exist: bigint = record"},
		{"SELECT (1, 2) IS NOT DISTINCT FROM 1", "error: operator does not exist: record = bigint"},
		{"SELECT (1, 2) + 1", "error: operator does not exist: record + bigint"},
		{"SELECT (1, 2) AND true", "error: argument of AND must be type boolean, not type record"},
		{"SELECT ((1, 2), 3) = ((1, 2), 3)", "error: nested row constructors are not supported"},
		{"SELECT ((1, 2), 3) IN (SELECT 1, 2)", "error: nested row constructors are not supported"},
		{"SELECT ((1, 2), 3) IS NULL", "error: nested row constructors are not supported"},
		{"SELECT ROW(ROW(1)) IS NOT NULL", "error: nested row constructors are not supported"},
		{"SELECT (1, 2)", generic},
		{"SELECT ROW(1)", generic},
		{"SELECT upper((1, 2))", generic},
		{"SELECT -(1, 2)", generic},
		{"SELECT CAST((1, 2) AS VARCHAR)", generic},
		{"SELECT 1 WHERE (1, 2)", generic},
		{"SELECT (1, 2) LIKE 'a'", generic},
		// The subquery's columns.
		{"SELECT (1, 2) IN (SELECT 1)", "error: subquery has too few columns"},
		{"SELECT (1, 2) IN (SELECT 1, 2, 3)", "error: subquery has too many columns"},
		{"SELECT 1 IN (SELECT 1, 2)", "error: subquery has too many columns"},
		{"SELECT 1 NOT IN (SELECT 1, 2)", "error: subquery has too many columns"},
		{"SELECT 1 = ANY (SELECT 1, 2)", "error: subquery has too many columns"},
		{"SELECT 1 > ALL (SELECT 1, 2)", "error: subquery has too many columns"},
		{"SELECT (1, 2) < ALL (SELECT 1)", "error: subquery has too few columns"},
		{"SELECT (1, 2) = (SELECT 1)", "error: subquery has too few columns"},
		{"SELECT (1, 2) = (SELECT 1, 2, 3)", "error: subquery has too many columns"},
		{"SELECT ROW() IN (SELECT 1)", "error: subquery has too many columns"},
		// Errors in the items come first, as in Postgres.
		{"SELECT (nosuch, 1) = 1", `error: column "nosuch" does not exist`},
		{"SELECT (1, nosuchfunc(1)) = (1, 2, 3)", "error: function nosuchfunc(bigint) does not exist"},
		{"SELECT (1, nosuch) IN (SELECT 1, 2)", `error: column "nosuch" does not exist`},
		{"SELECT (1, upper(1, 2))", "error: UPPER expects 1 argument"},
		// Syntax.
		{"SELECT ROW(1,)", `error: syntax error: unexpected ")"`},
		{"SELECT (1, 2) IS 3", "error: syntax error: expected NULL, DISTINCT FROM or JSON after IS"},
		{"SELECT 1 IS DISTINCT 2", `error: syntax error: expected FROM near "2"`},
	}
	for _, c := range cases {
		if got := memQuery(c.q); got != c.want {
			t.Errorf("%s\n got: %q\nwant: %q", c.q, got, c.want)
		}
	}
}

// The row hash set gives exactly the answers of comparing row by row, for
// rows of two and three items of every kind, with NULLs in any position.
func TestRowSetMatchesLinearScan(t *testing.T) {
	// CHAR values compare without their trailing spaces: 'a' as CHAR(3)
	// equals 'a' and 'a ' as VARCHAR.
	char3 := ColType{Kind: KindString, Fixed: true, Length: 3}
	values := append(equalityValues(), nullValue(typeInt64), nullValue(typeString),
		charValue("a", char3), charValue("abc", char3), stringValue("a"), stringValue("a "))
	rnd := rand.New(rand.NewSource(85))
	pick := func(pool []Value) Value { return pool[rnd.Intn(len(pool))] }
	// Pools of one class each (hashed), and all values (mixed classes).
	byClass := map[eqClass][]Value{}
	for _, v := range values {
		c, _ := eqKey(v)
		if v.Null {
			continue
		}
		byClass[c] = append(byClass[c], v)
	}
	null := nullValue(typeInt64)
	for width := 2; width <= 3; width++ {
		for n := 0; n < 300; n++ {
			// Each item draws from one class or from everything, with a
			// NULL now and then.
			pools := make([][]Value, width)
			for i := range pools {
				if rnd.Intn(3) == 0 {
					pools[i] = values
				} else {
					pools[i] = byClass[eqClass(1+rnd.Intn(int(numClasses)-1))]
				}
				if len(pools[i]) == 0 {
					pools[i] = values
				}
			}
			row := func(nullRate int) []Value {
				r := make([]Value, width)
				for i := range r {
					if nullRate > 0 && rnd.Intn(nullRate) == 0 {
						r[i] = null
					} else {
						r[i] = pick(pools[i])
					}
				}
				return r
			}
			var rows [][]Value
			size := inSetLinear + 1 + rnd.Intn(30)
			nullRate := []int{0, 3, 10}[n%3]
			for len(rows) < size {
				rows = append(rows, row(nullRate))
			}
			hashed := &rowSet{rows: rows, n: width}
			linear := &rowSet{rows: rows, n: width, noIndex: true}
			for p := 0; p < 60; p++ {
				var x []Value
				if p%4 == 0 {
					x = rows[rnd.Intn(len(rows))] // a member
				} else {
					x = row(4)
				}
				for _, not := range []bool{false, true} {
					got, want := showBool(hashed.eval(x, not)), showBool(linear.eval(x, not))
					if got != want {
						t.Fatalf("width %d set %d: %v NOT=%v: hashed %s, linear %s\nrows: %v", width, n, x, not, got, want, rows)
					}
				}
			}
		}
	}
}

// Comparing row by row agrees with the IN list the parser builds (an OR of
// ANDs of =), for values that compare without an error.
func TestRowSetMatchesRowComparisons(t *testing.T) {
	ints := []Value{intValue(typeInt64, 1), intValue(typeInt64, 2), nullValue(typeInt64)}
	strs := []Value{stringValue("a"), stringValue("b"), nullValue(typeString)}
	var all [][]Value
	for _, a := range ints {
		for _, b := range strs {
			all = append(all, []Value{a, b})
		}
	}
	lit := func(v Value) Expr { return &Literal{V: v} }
	env := &evalEnv{}
	// Every subset of the nine rows, as a set and as an IN list.
	for mask := 1; mask < 1<<len(all); mask++ {
		var rows [][]Value
		for i, r := range all {
			if mask&(1<<i) == 0 {
				continue
			}
			rows = append(rows, r)
		}
		set := &rowSet{rows: rows, n: 2, noIndex: true}
		for _, x := range all {
			var list Expr
			for _, r := range rows {
				eq := compareExpr("=", &RowExpr{Items: []Expr{lit(x[0]), lit(x[1])}}, &RowExpr{Items: []Expr{lit(r[0]), lit(r[1])}})
				if list == nil {
					list = eq
				} else {
					list = &Binary{Op: "OR", L: list, R: eq}
				}
			}
			want, err := env.eval(list)
			if err != nil {
				t.Fatal(err)
			}
			if got := set.eval(x, false); showBool(got) != showBool(want) {
				t.Fatalf("%v IN %v: set %s, OR of ANDs %s", x, rows, showBool(got), showBool(want))
			}
			// And rowCompareValues, which ANY / ALL use, agrees with the
			// rewritten comparisons for every operator.
			for _, r := range rows {
				for _, op := range []string{"=", "<>", "<", "<=", ">", ">="} {
					rewritten := compareExpr(op, &RowExpr{Items: []Expr{lit(x[0]), lit(x[1])}}, &RowExpr{Items: []Expr{lit(r[0]), lit(r[1])}})
					want, err := env.eval(rewritten)
					if err != nil {
						t.Fatal(err)
					}
					got, err := rowCompareValues(op, x, r)
					if err != nil {
						t.Fatal(err)
					}
					if showBool(got) != showBool(want) {
						t.Fatalf("%v %s %v: rowCompareValues %s, rewritten %s", x, op, r, showBool(got), showBool(want))
					}
				}
			}
		}
	}
}
