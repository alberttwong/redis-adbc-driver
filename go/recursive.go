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

// Recursive CTEs (WITH RECURSIVE).
//
// A CTE of a WITH RECURSIVE list that refers to itself must be
// `non-recursive term UNION [ALL] recursive term`. checkRecursion applies
// Postgres's rules, with its messages: the recursive term refers to the CTE
// exactly once, and not in a subquery, on the NULL-supplying side of an outer
// join, under INTERSECT ALL or EXCEPT, or at a level with aggregates; the
// non-recursive term and the body's own WITH don't refer to it; the UNION has
// no ORDER BY, LIMIT or OFFSET; and no other WITH item it reads refers back
// to it (mutual recursion is not implemented). A CTE of the list that doesn't
// refer to itself is an ordinary CTE.
//
// It is evaluated with the standard working-table iteration:
//
//  1. Run the non-recursive term (for UNION, without duplicate rows). Its
//     rows are the first rows of the result and the working table.
//  2. Run the recursive term, its reference to the CTE reading the working
//     table. For UNION, drop the rows equal to a row of the result so far
//     (NULLs are equal). The rows left are added to the result and become
//     the next working table.
//  3. Repeat until the working table is empty.
//
// The recursive term is planned once, against a working table relation whose
// rows are replaced before each run. Derived tables, LATERAL subqueries and
// WITH queries in it that read the working table are computed while planning,
// so for them the term is planned again for each run.
//
// The columns are named by the non-recursive term (or the CTE's column list)
// and have its types, widened to hold the recursive term's values when both
// are of one category (integers, numerics, strings, timestamps, …).
// Otherwise it is Postgres's "has type … in non-recursive term but type …
// overall" error.
//
// The whole result is computed before the query reads it (Postgres stops as
// soon as the query has read what it needs, for instance with LIMIT), so the
// recursive term needs a stop condition: a CTE whose recursive term still
// returns new rows after maxRecursion iterations, or that produces more than
// maxRecursiveRows rows, fails.

import (
	"context"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// maxRecursion caps the iterations of a recursive CTE (the runs of its
// recursive term that return new rows), and maxRecursiveRows the rows of its
// result. (Variables so that tests can lower them.)
var (
	maxRecursion     = 10_000
	maxRecursiveRows = 1_000_000
)

// materializeCTE computes a CTE once: by iteration if it refers to itself
// in a WITH RECURSIVE list, otherwise by running its query.
func (e *executor) materializeCTE(ctx context.Context, def *CTE) (*tableMeta, error) {
	if def.Recursive {
		rc, err := e.checkRecursion(def)
		if err != nil {
			return nil, err
		}
		if rc != nil {
			return e.materializeRecursive(ctx, def, rc)
		}
	}
	if def.Search != nil || def.Cycle != nil {
		return nil, errorf(adbc.StatusInvalidArgument, "WITH query is not recursive")
	}
	return e.materialize(ctx, def, def.Name, def.Select, def.Columns)
}

// materializeRecursive evaluates a recursive CTE (see the file comment).
func (e *executor) materializeRecursive(ctx context.Context, def *CTE, rc *recursionCheck) (*tableMeta, error) {
	savedScopes, savedSq := e.scopes, e.pendingSq
	e.scopes, e.pendingSq = nil, nil
	defer func() { e.scopes, e.pendingSq = savedScopes, savedSq }()
	body := def.Select
	op := body.SetOp
	pop, err := e.pushCTEs(body.With)
	if err != nil {
		return nil, err
	}
	defer pop()

	nonrec, err := e.planSelect(ctx, op.Left, e.paramTypes)
	if err != nil {
		return nil, err
	}
	cols := nonrec.columns()
	wt, err := memTable(def.Name, cols, nil, def.Columns)
	if err != nil {
		return nil, err
	}
	e.cache.working[def] = wt
	defer delete(e.cache.working, def)
	rec, err := e.planSelect(ctx, op.Right, e.paramTypes)
	if err != nil {
		return nil, err
	}
	if len(rec.items) != len(cols) {
		return nil, errorf(adbc.StatusInvalidArgument, "each UNION query must have the same number of columns (%d and %d)",
			len(cols), len(rec.items))
	}
	for i := range cols {
		t, err := recursiveType(def.Name, i, cols[i].Type, rec.items[i].typ)
		if err != nil {
			return nil, err
		}
		cols[i].Type = t
		wt.Columns[i].Type = t
	}
	var tr *tracer
	if def.Search != nil || def.Cycle != nil {
		if tr, err = e.newTracer(ctx, def, rc, wt); err != nil {
			return nil, err
		}
	}

	run := func(plan *selectPlan) ([][]Value, error) {
		rows, err := e.runSelect(ctx, plan, e.params)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			for i := range row {
				if row[i], err = Coerce(row[i], cols[i].Type); err != nil {
					return nil, invalidArg(err)
				}
			}
		}
		return rows, nil
	}
	var result [][]Value
	if e.planOnly {
		// Only the columns are wanted (see empty.go).
		return e.recursiveResult(def, cols, tr, nil)
	}
	seen := map[string]bool{}
	// add appends rows to the result (for UNION, those not in it yet) and
	// returns their positions. With a tracer, traceOf gives their traces,
	// which are part of the rows compared.
	add := func(rows [][]Value, traceOf func([]Value) trace) []int {
		var added []int
		for _, r := range rows {
			var t trace
			var key string
			if tr != nil {
				t = traceOf(r)
				key = tr.key(t)
			}
			if !op.All {
				k := rowKey(r) + key
				if seen[k] {
					continue
				}
				seen[k] = true
			}
			if tr != nil {
				tr.traces = append(tr.traces, t)
			}
			added = append(added, len(result))
			result = append(result, r)
		}
		return added
	}
	tooMany := func() error {
		if len(result) <= maxRecursiveRows {
			return nil
		}
		return errorf(adbc.StatusInvalidArgument,
			"recursive query %q returned more than %d rows; check the stop condition of its recursive term", def.Name, maxRecursiveRows)
	}
	rows, err := run(nonrec)
	if err != nil {
		return nil, err
	}
	work := add(rows, func(r []Value) trace { return tr.root(r) })
	if err := tooMany(); err != nil {
		return nil, err
	}
	// Up to maxRecursion iterations may return new rows; the one after them
	// must return none.
	for runs := 1; len(work) > 0; runs++ {
		// The recursive term reads the rows of the last iteration: all at
		// once, or with a tracer one at a time (but for cycles).
		groups := [][]int{work}
		if tr != nil {
			groups = nil
			for _, i := range work {
				if !tr.traces[i].cycle {
					groups = append(groups, []int{i})
				}
			}
		}
		var next []int
		for _, g := range groups {
			wt.mem = make([]map[string]Value, len(g))
			for r, i := range g {
				m := make(map[string]Value, len(result[i]))
				for c, v := range result[i] {
					m[wt.Columns[c].Name] = v
				}
				wt.mem[r] = m
			}
			if len(rc.stale) > 0 {
				for _, s := range rc.stale {
					switch x := s.(type) {
					case *SelectStmt:
						delete(e.cache.derived, x)
					case *CTE:
						delete(e.cache.ctes, x)
					}
				}
				if rec, err = e.planSelect(ctx, op.Right, e.paramTypes); err != nil {
					return nil, err
				}
			}
			if rows, err = run(rec); err != nil {
				return nil, err
			}
			next = append(next, add(rows, func(r []Value) trace { return tr.child(tr.traces[g[0]], r) })...)
			if err := tooMany(); err != nil {
				return nil, err
			}
		}
		if len(next) > 0 && runs > maxRecursion {
			return nil, errorf(adbc.StatusInvalidArgument,
				"recursive query %q did not finish after %d iterations; check the stop condition of its recursive term", def.Name, maxRecursion)
		}
		work = next
	}
	return e.recursiveResult(def, cols, tr, result)
}

// recursiveResult is the relation of a recursive CTE with the given rows,
// with the columns of its SEARCH and CYCLE clauses if it has a tracer.
func (e *executor) recursiveResult(def *CTE, cols []resultColumn, tr *tracer, result [][]Value) (*tableMeta, error) {
	if tr != nil {
		names := slices.Clone(def.Columns)
		if names == nil {
			for _, c := range cols {
				names = append(names, c.Name)
			}
		}
		all, names := tr.columns(cols, names)
		return memTable(def.Name, all, tr.finish(result), names)
	}
	return memTable(def.Name, cols, result, def.Columns)
}

// recursiveType is the type of column i of a recursive CTE whose terms
// return types nonrec and rec.
func recursiveType(name string, i int, nonrec, rec ColType) (ColType, error) {
	t, ok := setOpType(nonrec, rec)
	category := func(t ColType) Kind {
		switch {
		case t.Kind.isInteger():
			return KindInt64
		case t.Kind.isFloat():
			return KindFloat64
		}
		return t.Kind
	}
	if !ok || category(t) != category(nonrec) {
		if !ok {
			t = rec
		}
		return ColType{}, errorf(adbc.StatusInvalidArgument,
			"recursive query %q column %d has type %s in non-recursive term but type %s overall; cast the output of the non-recursive term to the correct type",
			name, i+1, nonrec.SQLName(), t.SQLName())
	}
	return t, nil
}

// ---- checking ----

// recursionContext is where a recursive reference is.
type recursionContext int

const (
	recursionOK recursionContext = iota
	recursionNonRecursiveTerm
	recursionSubquery
	recursionOuterJoin
	recursionIntersect
	recursionExcept
)

// recursionErrors are Postgres's messages for a recursive reference in each
// context where it isn't allowed.
var recursionErrors = map[recursionContext]string{
	recursionNonRecursiveTerm: "recursive reference to query %q must not appear within its non-recursive term",
	recursionSubquery:         "recursive reference to query %q must not appear within a subquery",
	recursionOuterJoin:        "recursive reference to query %q must not appear within an outer join",
	recursionIntersect:        "recursive reference to query %q must not appear within INTERSECT",
	recursionExcept:           "recursive reference to query %q must not appear within EXCEPT",
}

// recursionCheck walks the body of a CTE of a WITH RECURSIVE list for its
// references to itself.
type recursionCheck struct {
	cte *CTE
	// probe only counts the references, wherever they are.
	probe bool
	// siblings is the CTE's WITH list. The bodies of those it reads are
	// walked too (mutual is then set): a reference there is mutual recursion.
	siblings map[string]*CTE
	visiting map[*CTE]bool
	mutual   bool
	ctx      recursionContext
	refs     int
	err      error
	// depth is the nesting of the query being walked (1 for a term itself),
	// and refDepth that of the reference.
	depth, refDepth int
	// stale are the derived tables, LATERAL subqueries and WITH queries
	// that read the CTE (computed while planning, so planned again for each
	// run of the recursive term).
	stale []any
}

// checkRecursion checks a CTE of a WITH RECURSIVE list against Postgres's
// rules (see the file comment). It returns nil if the CTE doesn't refer to
// itself.
func (e *executor) checkRecursion(def *CTE) (*recursionCheck, error) {
	probe := &recursionCheck{cte: def, probe: true}
	probe.query(def.Select, nil)
	if probe.refs == 0 {
		return nil, nil
	}
	body := def.Select
	op := body.SetOp
	if op == nil || op.Op != "UNION" {
		return nil, errorf(adbc.StatusInvalidArgument,
			"recursive query %q does not have the form non-recursive-term UNION [ALL] recursive-term", def.Name)
	}
	rc := &recursionCheck{cte: def, siblings: e.siblingCTEs(def), visiting: map[*CTE]bool{def: true}}
	hidden := shadowCTEs(nil, body.With)
	rc.ctx = recursionSubquery
	for i := range body.With {
		rc.query(body.With[i].Select, hidden)
	}
	rc.ctx = recursionNonRecursiveTerm
	rc.query(op.Left, hidden)
	rc.ctx = recursionOK
	rc.query(op.Right, hidden)
	switch {
	case rc.err != nil:
		return nil, rc.err
	case rc.refs != 1:
		return nil, errorf(adbc.StatusInternal, "missing recursive reference to query %q", def.Name)
	case len(body.OrderBy) > 0:
		return nil, errorf(adbc.StatusNotImplemented, "ORDER BY in a recursive query is not implemented")
	case body.Offset != nil:
		return nil, errorf(adbc.StatusNotImplemented, "OFFSET in a recursive query is not implemented")
	case body.Limit != nil:
		return nil, errorf(adbc.StatusNotImplemented, "LIMIT in a recursive query is not implemented")
	}
	return rc, nil
}

// siblingCTEs returns the WITH list def belongs to.
func (e *executor) siblingCTEs(def *CTE) map[string]*CTE {
	for i := len(e.ctes) - 1; i >= 0; i-- {
		if e.ctes[i][strings.ToLower(def.Name)] == def {
			return e.ctes[i]
		}
	}
	return nil
}

// shadowCTEs adds the names a WITH list defines to hidden: inside its query
// they mean its own CTEs.
func shadowCTEs(hidden map[string]bool, with []CTE) map[string]bool {
	if len(with) == 0 {
		return hidden
	}
	out := make(map[string]bool, len(hidden)+len(with))
	for k := range hidden {
		out[k] = true
	}
	for _, c := range with {
		out[strings.ToLower(c.Name)] = true
	}
	return out
}

func (rc *recursionCheck) fail(code adbc.Status, format string, args ...any) {
	if rc.err == nil {
		rc.err = errorf(code, format, args...)
	}
}

// query walks a query; hidden are the names that inner WITH lists define.
func (rc *recursionCheck) query(sel *SelectStmt, hidden map[string]bool) {
	if sel == nil || rc.err != nil {
		return
	}
	rc.depth++
	defer func() { rc.depth-- }()
	hidden = shadowCTEs(hidden, sel.With)
	for i := range sel.With {
		before := rc.refs
		rc.query(sel.With[i].Select, hidden)
		if rc.refs > before {
			rc.stale = append(rc.stale, &sel.With[i])
		}
	}
	if op := sel.SetOp; op != nil {
		saved := rc.ctx
		switch op.Op {
		case "INTERSECT":
			if op.All {
				rc.ctx = recursionIntersect
			}
			rc.query(op.Left, hidden)
			rc.query(op.Right, hidden)
		case "EXCEPT":
			if op.All {
				rc.ctx = recursionExcept
			}
			rc.query(op.Left, hidden)
			rc.ctx = recursionExcept
			rc.query(op.Right, hidden)
		default:
			rc.query(op.Left, hidden)
			rc.query(op.Right, hidden)
		}
		rc.ctx = saved
		return
	}
	items := append([]JoinClause{{Table: sel.From, Select: sel.FromSelect, Func: sel.FromFunc}}, sel.Joins...)
	direct := false
	for i, jc := range items {
		saved := rc.ctx
		if nullSupplying(items, i) {
			rc.ctx = recursionOuterJoin
		}
		if rc.fromItem(jc, hidden) {
			direct = true
		}
		if jc.Func != nil {
			for _, a := range jc.Func.Args {
				rc.expr(a, hidden)
			}
		}
		rc.ctx = saved
		rc.expr(jc.On, hidden)
	}
	outputs := []Expr{sel.Having, sel.Qualify}
	for _, it := range sel.Items {
		outputs = append(outputs, it.Expr)
	}
	for _, o := range sel.OrderBy {
		outputs = append(outputs, o.Expr)
	}
	others := append([]Expr{sel.Where}, sel.GroupBy...)
	for _, x := range append(append(others, sel.DistinctOn...), outputs...) {
		rc.expr(x, hidden)
	}
	if direct && !rc.probe {
		for _, x := range outputs {
			if isAggregate(x) {
				rc.fail(adbc.StatusInvalidArgument, "aggregate functions are not allowed in a recursive query's recursive term")
			}
		}
	}
}

// nullSupplying reports whether FROM item i is on the NULL-supplying side of
// an outer join (as joinPlan.finish works it out).
func nullSupplying(items []JoinClause, i int) bool {
	if k := items[i].Kind; k == "LEFT" || k == "FULL" {
		return true
	}
	for _, later := range items[i+1:] {
		if later.Kind == "RIGHT" || later.Kind == "FULL" {
			return true
		}
	}
	return false
}

// fromItem walks a FROM item. It reports whether the item is a reference to
// the CTE.
func (rc *recursionCheck) fromItem(jc JoinClause, hidden map[string]bool) bool {
	switch {
	case jc.Table != nil:
		t := jc.Table
		key := strings.ToLower(t.Name)
		if t.Schema != "" || t.Catalog != "" || hidden[key] {
			return false
		}
		if key == strings.ToLower(rc.cte.Name) {
			switch {
			case rc.probe:
			case rc.mutual:
				rc.fail(adbc.StatusNotImplemented, "mutual recursion between WITH items is not implemented")
			case rc.ctx != recursionOK:
				rc.fail(adbc.StatusInvalidArgument, recursionErrors[rc.ctx], rc.cte.Name)
			case rc.refs > 0:
				rc.fail(adbc.StatusInvalidArgument, "recursive reference to query %q must not appear more than once", rc.cte.Name)
			}
			rc.refs++
			rc.refDepth = rc.depth
			return true
		}
		if sib := rc.siblings[key]; sib != nil && !rc.visiting[sib] {
			rc.visiting[sib] = true
			saved := rc.mutual
			rc.mutual = true
			rc.query(sib.Select, nil)
			rc.mutual = saved
		}
	case jc.Select != nil:
		before := rc.refs
		rc.query(jc.Select, hidden)
		if rc.refs > before {
			rc.stale = append(rc.stale, jc.Select)
		}
	}
	return false
}

// expr walks the subqueries of an expression.
func (rc *recursionCheck) expr(x Expr, hidden map[string]bool) {
	walkExpr(x, func(n Expr) {
		if sq, ok := n.(*Subquery); ok {
			saved := rc.ctx
			rc.ctx = recursionSubquery
			rc.query(sq.Select, hidden)
			rc.ctx = saved
		}
	})
}
