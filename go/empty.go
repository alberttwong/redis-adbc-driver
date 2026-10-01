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

// Queries that can't return rows.
//
// dbt asks for a query's columns with `select * from (<sql>) as __dbt_sbq
// where false limit 0`, and `dbt run --empty` reads every ref as `(select *
// from <rel> where false limit 0)`. Such a query reads no rows:
//
//   - A query that returns no rows (selectPlan.noRows: LIMIT 0, FETCH FIRST
//     0 ROWS, or a HAVING or QUALIFY that is never true) is planned without
//     running anything (executor.planOnly) and returns at once.
//   - A query whose WHERE is never true has no input rows. Its FROM items
//     (and the subqueries of its WHERE) are planned but not run, and its
//     WHERE matches nothing (wherePlan.none), so no FT.AGGREGATE, FT.SEARCH
//     or HMGET reads them. The rest of the query runs over no rows:
//     COUNT(*) without GROUP BY still returns one row, 0.
//   - A join item whose ON is never true, or that has no rows, isn't read
//     when that settles the result (joinReads): an inner join is empty, a
//     LEFT JOIN keeps the left rows with the item's columns NULL, and a
//     RIGHT JOIN keeps only the item's rows.
//   - ExecuteSchema and CREATE VIEW plan their query the same way.
//
// While planOnly is set, a derived table, CTE or view becomes an empty
// in-memory relation with the columns its plan gives, so a query's column
// names and types are those of the full query, and planning errors (unknown
// tables and columns, type errors, …) are still reported. A value that would
// fail to compute on a row that isn't read raises no error: `SELECT 1/0 FROM
// t WHERE false` returns no rows, as it did before when no row matched.
//
// A condition is never true when it is FALSE or NULL whatever the rows hold:
// its parts that read no column, parameter or subquery are evaluated once,
// and AND is never true if one operand is, OR if both are, NOT if its
// operand is never false. A part whose evaluation fails is left to the rows,
// as before.

import "context"

// neverTrue reports whether a condition is FALSE or NULL for every row.
func (e *executor) neverTrue(ctx context.Context, cond Expr) bool {
	return cond != nil && cannotBe(e.newEnv(ctx, nil, nil), cond, true)
}

// cannotBe reports whether cond can never evaluate to b: whatever the rows
// hold, it is the other truth value or NULL.
func cannotBe(env *evalEnv, cond Expr, b bool) bool {
	switch x := cond.(type) {
	case *Binary:
		if x.Op == "AND" || x.Op == "OR" {
			// AND is true only if both operands are, and false if either is;
			// OR the other way round. The right operand is looked at first:
			// chains (a OR b OR …, an IN list) nest on the left.
			if (x.Op == "AND") == b {
				return cannotBe(env, x.R, b) || cannotBe(env, x.L, b)
			}
			return cannotBe(env, x.R, b) && cannotBe(env, x.L, b)
		}
	case *Unary:
		if x.Op == "NOT" {
			return cannotBe(env, x.X, !b)
		}
	}
	if !foldable(cond) {
		return false
	}
	v, err := env.eval(cond)
	if err != nil {
		return false
	}
	// A value that isn't a truth value counts as NULL, as in WHERE.
	t, ok := truthy(v)
	return !ok || t != b
}

// foldable reports whether an expression has the same value for every row:
// it reads no column, parameter or subquery, and calls no aggregate, window
// or volatile function.
func foldable(x Expr) bool {
	ok := true
	walkExpr(x, func(n Expr) {
		switch f := n.(type) {
		case *Literal, *Unary, *Binary, *IsNull, *Cast, *Case:
		case *Func:
			if aggregateFuncs[f.Name] || windowOnlyFuncs[f.Name] || volatileFuncs[f.Name] ||
				f.Name == "GENERATE_SERIES" || f.Name == "MERGE_ACTION" {
				ok = false
			}
		default:
			ok = false
		}
	})
	return ok
}

// planningOnly sets planOnly if on is set (it stays set if it already is),
// and returns the function that restores it.
func (e *executor) planningOnly(on bool) func() {
	saved := e.planOnly
	e.planOnly = saved || on
	return func() { e.planOnly = saved }
}

// noMatchQuery is an index query that matches no row: __rowid is never
// negative.
const noMatchQuery = "@" + rowIDField + ":[-1 -1]"

// joinReads works out which items of a join in written order can change its
// result, given the items whose ON can never be true (never) and those known
// to have no rows (empty); kinds are the join kinds, "" for the first item.
// none reports that the join has no rows at all. Running the join reading
// only the items in read gives its result: an item not read is joined as if
// it had no rows when it is a LEFT or FULL item, and otherwise its rows and
// those joined before it don't matter.
func joinReads(kinds []string, never, empty []bool) (read []bool, none bool) {
	// from are the items the rows joined so far come from.
	var from []int
	none = empty[0]
	if !none {
		from = []int{0}
	}
	for k := 1; k < len(kinds); k++ {
		switch kinds[k] {
		case "LEFT":
			// The left rows are kept; the item adds nothing to them if it
			// can't match.
			if !none && !never[k] && !empty[k] {
				from = append(from, k)
			}
		case "RIGHT":
			switch {
			case empty[k]:
				none, from = true, nil
			case none || never[k]:
				// Only the item's rows, with NULLs for the left side.
				none, from = false, []int{k}
			default:
				from = append(from, k)
			}
		case "FULL":
			switch {
			case empty[k]:
			case none:
				none, from = false, []int{k}
			default:
				from = append(from, k)
			}
		default: // INNER, CROSS
			if none || never[k] || empty[k] {
				none, from = true, nil
			} else {
				from = append(from, k)
			}
		}
	}
	read = make([]bool, len(kinds))
	for _, k := range from {
		read[k] = true
	}
	return read, none
}
