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

// GROUPING SETS, ROLLUP, CUBE and GROUPING().
//
// The parser expands a GROUP BY clause into its grouping sets, as Postgres
// does: ROLLUP (a, b) is (a, b), (a), (); CUBE (a, b) is every subset of
// its items; GROUPING SETS lists sets, and the ROLLUP, CUBE and GROUPING SETS
// nested in it add theirs; the items of the clause combine as a cross
// product, so GROUP BY a, ROLLUP (b) is (a, b), (a). A parenthesized list is
// one item of several expressions. SelectStmt.GroupBy holds every grouping
// expression, so they are bound like a plain GROUP BY (output positions and
// aliases work).
//
// Each grouping set runs as its own grouped query (FT.AGGREGATE GROUPBY
// when its columns and the aggregates can be pushed down, otherwise the
// driver), returning its grouping expressions and every aggregate call. If
// several sets run in the driver, they share one read of the rows (filtered
// by WHERE), so a ROLLUP the index can't compute reads the table once, not
// once per level.
//
// The groups of all sets are combined (UNION ALL) into an in-memory
// relation, one row per group, with a column for each grouping expression
// (NULL when it is not in the group's set), each GROUPING() call (its
// bitmask for the set) and each aggregate call. The rest of the query runs
// over that relation, as in Postgres: HAVING, window functions, QUALIFY,
// DISTINCT, ORDER BY and LIMIT. Its expressions are rewritten to read those
// columns, matching grouping expressions as a whole (GROUP BY a + b matches
// a + b), so outside aggregate calls they may only use grouping expressions,
// as in Postgres. A grouping column keeps its name in the relation, so
// correlated subqueries can read it (NULL where it is not grouped).

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// Postgres's limits.
const (
	maxGroupingSets = 4096
	maxCubeItems    = 12
	maxGroupingArgs = 31
)

// ---- parsing ----

// parseGroupBy parses the items of a GROUP BY clause into sel.GroupBy and,
// if they use ROLLUP, CUBE, GROUPING SETS or (), sel.GroupingSets.
func (p *parser) parseGroupBy(sel *SelectStmt) error {
	sets := [][]int{{}}
	grouping := false
	for {
		item, construct, err := p.parseGroupingItem(sel)
		if err != nil {
			return err
		}
		grouping = grouping || construct
		if len(sets)*len(item) > maxGroupingSets {
			return errTooManyGroupingSets()
		}
		var cross [][]int
		for _, s := range sets {
			for _, t := range item {
				cross = append(cross, slices.Concat(s, t))
			}
		}
		sets = cross
		if !p.acceptOp(",") {
			break
		}
	}
	if grouping {
		sel.GroupingSets = sets
	}
	return nil
}

func errTooManyGroupingSets() error {
	return &sqlError{msg: fmt.Sprintf("too many grouping sets present (maximum %d)", maxGroupingSets)}
}

// parseGroupingItem parses one item of a GROUP BY or GROUPING SETS list and
// returns its grouping sets (positions in sel.GroupBy). construct is false
// for an expression or a parenthesized list of them.
func (p *parser) parseGroupingItem(sel *SelectStmt) ([][]int, bool, error) {
	add := func(list []Expr) []int {
		set := make([]int, len(list))
		for i, e := range list {
			set[i] = len(sel.GroupBy)
			sel.GroupBy = append(sel.GroupBy, e)
		}
		return set
	}
	open := p.peekAt(1).kind == tokOp && p.peekAt(1).text == "("
	switch {
	case open && (p.isKeyword("ROLLUP") || p.isKeyword("CUBE")):
		kw := strings.ToUpper(p.next().text)
		p.pos++
		var items [][]int
		for {
			list, ok, err := p.parseGroupingList()
			if err != nil {
				return nil, false, err
			}
			if ok && len(list) == 0 {
				return nil, false, syntaxErr("%s items must be expressions or lists of them, not ()", kw)
			}
			if !ok {
				e, err := p.parseExpr()
				if err != nil {
					return nil, false, err
				}
				list = []Expr{e}
			}
			items = append(items, add(list))
			if !p.acceptOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return nil, false, err
		}
		var sets [][]int
		if kw == "ROLLUP" {
			// (a, b, c), (a, b), (a), ()
			for n := len(items); n >= 0; n-- {
				sets = append(sets, slices.Concat(items[:n]...))
			}
			return sets, true, nil
		}
		if len(items) > maxCubeItems {
			return nil, false, &sqlError{msg: fmt.Sprintf("CUBE is limited to %d elements", maxCubeItems)}
		}
		// Every subset, from all the items down to none.
		for mask := 1<<len(items) - 1; mask >= 0; mask-- {
			set := []int{}
			for i, it := range items {
				if mask&(1<<(len(items)-1-i)) != 0 {
					set = append(set, it...)
				}
			}
			sets = append(sets, set)
		}
		return sets, true, nil
	case p.isKeyword("GROUPING") && p.isKeywordAt(1, "SETS"):
		p.pos += 2
		if err := p.expectOp("("); err != nil {
			return nil, false, err
		}
		var sets [][]int
		for {
			item, _, err := p.parseGroupingItem(sel)
			if err != nil {
				return nil, false, err
			}
			if sets = append(sets, item...); len(sets) > maxGroupingSets {
				return nil, false, errTooManyGroupingSets()
			}
			if !p.acceptOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return nil, false, err
		}
		return sets, true, nil
	}
	list, ok, err := p.parseGroupingList()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		e, err := p.parseExpr()
		if err != nil {
			return nil, false, err
		}
		list = []Expr{e}
	}
	// () is the empty grouping set.
	return [][]int{add(list)}, ok && len(list) == 0, nil
}

// parseGroupingList parses `()` or a parenthesized list of two or more
// expressions. ok is false, and nothing is consumed, when the parenthesis
// opens an expression instead: (a), (a + b) * 2, (SELECT …).
func (p *parser) parseGroupingList() ([]Expr, bool, error) {
	if !p.isOp("(") || p.isKeywordAt(1, "SELECT") || p.isKeywordAt(1, "WITH") {
		return nil, false, nil
	}
	if next := p.peekAt(1); next.kind == tokOp && next.text == ")" {
		p.pos += 2
		return nil, true, nil
	}
	// A comma after the first expression makes it a list. Otherwise the
	// parse is undone, including the numbering of ? parameters.
	pos, numParams, nextParam := p.pos, p.numParams, p.nextParam
	p.pos++
	first, err := p.parseExpr()
	if err != nil || !p.isOp(",") {
		p.pos, p.numParams, p.nextParam = pos, numParams, nextParam
		return nil, false, nil
	}
	list := []Expr{first}
	for p.acceptOp(",") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, false, err
		}
		list = append(list, e)
	}
	if err := p.expectOp(")"); err != nil {
		return nil, false, err
	}
	return list, true, nil
}

// ---- planning ----

// groupingPlan is what a query with grouping sets computes before the rest
// of it runs over the combined groups (see runGroupingSets).
type groupingPlan struct {
	// base is the query as planned; each grouping set runs as a copy of it
	// (setPlan).
	base  *selectPlan
	slots []groupingSlot
	// sets are the grouping sets as slot indexes, each slot once.
	sets  [][]int
	calls []groupingCall
	aggs  []groupingAgg
	// having is the HAVING predicate over the combined groups.
	having Expr
}

// groupingSlot is a grouping expression and the column of the combined
// groups that holds its value.
type groupingSlot struct {
	expr Expr
	name string
	typ  ColType
}

// groupingCall is a GROUPING(…) call: the column holding its value, and
// the slots of its arguments.
type groupingCall struct {
	name string
	args []int
}

// groupingAgg is an aggregate call and the column holding its value.
type groupingAgg struct {
	fn   *Func
	name string
	typ  ColType
}

// hiddenColumn names a column of the combined groups that is not a column
// of the query; the NUL byte keeps it apart from those.
func hiddenColumn(kind string, i int) string { return fmt.Sprintf("\x00%s%d", kind, i) }

// usesGrouping reports whether a query calls GROUPING() (outside its
// subqueries, which are queries of their own).
func usesGrouping(sel *SelectStmt) bool {
	exprs := []Expr{sel.Where, sel.Having, sel.Qualify}
	for _, it := range sel.Items {
		exprs = append(exprs, it.Expr)
	}
	for _, o := range sel.OrderBy {
		exprs = append(exprs, o.Expr)
	}
	for _, j := range sel.Joins {
		exprs = append(exprs, j.On)
	}
	exprs = append(exprs, sel.GroupBy...)
	exprs = append(exprs, sel.DistinctOn...)
	return slices.ContainsFunc(exprs, hasGrouping)
}

func hasGrouping(e Expr) bool {
	found := false
	walkExpr(e, func(x Expr) {
		if f, ok := x.(*Func); ok && f.Name == "GROUPING" {
			found = true
		}
	})
	return found
}

// slotOf returns the slot of a grouping expression, or -1. Constants only
// match themselves (a grouping set by output position), as in Postgres: the
// 1 of COUNT(*) + 1 is not GROUP BY 1.
func (gp *groupingPlan) slotOf(e Expr) int {
	for i, s := range gp.slots {
		if s.expr == e {
			return i
		}
	}
	switch e.(type) {
	case *Literal, *Param:
		return -1
	}
	for i, s := range gp.slots {
		if exprEqual(s.expr, e) {
			return i
		}
	}
	return -1
}

// groupedColumn reports whether a column of the query is a grouping
// expression by itself.
func (gp *groupingPlan) groupedColumn(name string) bool {
	for _, s := range gp.slots {
		if c, ok := s.expr.(*ColumnRef); ok && c.Outer == 0 && c.Name == name {
			return true
		}
	}
	return false
}

// planGroupingSets turns a planned query with grouping sets (or GROUPING()
// calls) into the plan that runs over the combined groups of its sets.
// types are the columns of the query's FROM relation.
func (e *executor) planGroupingSets(plan *selectPlan, types map[string]ColType) (*selectPlan, error) {
	sel := plan.sel
	if hasGrouping(sel.Where) {
		return nil, errorf(adbc.StatusInvalidArgument, "grouping operations are not allowed in WHERE")
	}
	for _, j := range sel.Joins {
		if hasGrouping(j.On) {
			return nil, errorf(adbc.StatusInvalidArgument, "grouping operations are not allowed in JOIN conditions")
		}
	}
	if slices.ContainsFunc(sel.GroupBy, hasGrouping) {
		return nil, errorf(adbc.StatusInvalidArgument, "grouping operations are not allowed in GROUP BY")
	}
	plan.aggregate = true
	gp := &groupingPlan{base: plan}
	slot := make([]int, len(sel.GroupBy))
	for i, g := range sel.GroupBy {
		if slot[i] = gp.slotOf(g); slot[i] >= 0 {
			continue
		}
		t, err := inferType(g, types, e.paramTypes)
		if err != nil {
			return nil, invalidArg(err)
		}
		name := hiddenColumn("group", len(gp.slots))
		if c, ok := g.(*ColumnRef); ok && c.Outer == 0 {
			name = c.Name
		}
		slot[i] = len(gp.slots)
		gp.slots = append(gp.slots, groupingSlot{expr: g, name: name, typ: t})
	}
	sets := sel.GroupingSets
	if sets == nil {
		// A plain GROUP BY is one grouping set.
		all := make([]int, len(sel.GroupBy))
		for i := range all {
			all[i] = i
		}
		sets = [][]int{all}
	}
	for _, s := range sets {
		set := []int{}
		for _, i := range s {
			if !slices.Contains(set, slot[i]) {
				set = append(set, slot[i])
			}
		}
		gp.sets = append(gp.sets, set)
	}

	// The plan over the combined groups.
	out := *plan
	r := &groupingRewriter{gp: gp, memo: map[Expr]Expr{}}
	if len(sel.Joins) == 0 {
		// Name columns like Postgres, "t.c" (in a join they are "t.c"
		// already).
		r.rel = sel.FromAlias
		if r.rel == "" && sel.From != nil {
			r.rel = sel.From.Name
		}
	}
	out.items = make([]planItem, len(plan.items))
	for i, it := range plan.items {
		out.items[i] = planItem{expr: r.rewrite(it.expr), name: it.name}
	}
	gp.having = r.rewrite(plan.having)
	out.order = make([]planOrder, len(plan.order))
	for i, o := range plan.order {
		o.expr = r.rewrite(o.expr)
		out.order[i] = o
	}
	out.qualify = r.rewrite(plan.qualify)
	out.distinctOn = make([]planItem, len(plan.distinctOn))
	for i, d := range plan.distinctOn {
		out.distinctOn[i] = planItem{expr: r.rewrite(d.expr), name: d.name}
	}
	if r.err != nil {
		return nil, r.err
	}

	meta := &tableMeta{Name: "grouping sets", isMem: true}
	for _, s := range gp.slots {
		meta.Columns = append(meta.Columns, columnMeta{Name: s.name, Type: s.typ, Nullable: true})
	}
	for _, c := range gp.calls {
		meta.Columns = append(meta.Columns, columnMeta{Name: c.name, Type: typeInt32})
	}
	for i := range gp.aggs {
		a := &gp.aggs[i]
		t, err := inferType(a.fn, types, e.paramTypes)
		if err != nil {
			return nil, invalidArg(err)
		}
		a.typ = t
		meta.Columns = append(meta.Columns, columnMeta{Name: a.name, Type: t, Nullable: true})
	}
	// The output types come from the rewritten expressions, where GROUPING()
	// is an INTEGER column.
	memTypes := meta.types()
	for _, items := range [][]planItem{out.items, out.distinctOn} {
		for i := range items {
			t, err := inferType(items[i].expr, memTypes, e.paramTypes)
			if err != nil {
				return nil, invalidArg(err)
			}
			items[i].typ = t
		}
	}
	out.meta, out.having, out.extraNeed, out.grouping = meta, nil, nil, gp
	out.windows = nil
	if err := e.planWindows(&out, memTypes); err != nil {
		return nil, err
	}
	return &out, nil
}

// groupingRewriter rewrites the expressions of a query with grouping sets
// to read the columns of the combined groups.
type groupingRewriter struct {
	gp  *groupingPlan
	rel string // the FROM item's name, for errors
	// memo keeps shared nodes shared (an ORDER BY alias is its item's
	// expression), so each window function is computed once.
	memo map[Expr]Expr
	err  error
}

func (r *groupingRewriter) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

// column names a column of the query in errors.
func (r *groupingRewriter) column(name string) string {
	if r.rel != "" {
		return r.rel + "." + name
	}
	return name
}

func (r *groupingRewriter) rewrite(e Expr) Expr {
	if e == nil || r.err != nil {
		return e
	}
	if out, ok := r.memo[e]; ok {
		return out
	}
	out := r.node(e)
	r.memo[e] = out
	return out
}

func (r *groupingRewriter) rewriteAll(exprs []Expr) []Expr {
	if exprs == nil {
		return nil
	}
	out := make([]Expr, len(exprs))
	for i, x := range exprs {
		out[i] = r.rewrite(x)
	}
	return out
}

func (r *groupingRewriter) node(e Expr) Expr {
	gp := r.gp
	if i := gp.slotOf(e); i >= 0 {
		return &ColumnRef{Name: gp.slots[i].name}
	}
	switch x := e.(type) {
	case *ColumnRef:
		// Outer references are constants here.
		if x.Outer == 0 {
			r.fail(errorf(adbc.StatusInvalidArgument,
				"column %q must appear in the GROUP BY clause or be used in an aggregate function", r.column(x.Name)))
		}
		return x
	case *Unary:
		return &Unary{Op: x.Op, X: r.rewrite(x.X)}
	case *Binary:
		return &Binary{Op: x.Op, L: r.rewrite(x.L), R: r.rewrite(x.R)}
	case *IsNull:
		return &IsNull{X: r.rewrite(x.X), Not: x.Not}
	case *Cast:
		return &Cast{X: r.rewrite(x.X), T: x.T}
	case *Case:
		c := &Case{Operand: r.rewrite(x.Operand), Else: r.rewrite(x.Else)}
		for _, w := range x.Whens {
			c.Whens = append(c.Whens, WhenClause{When: r.rewrite(w.When), Then: r.rewrite(w.Then)})
		}
		return c
	case *Func:
		switch {
		case x.Name == "GROUPING":
			return r.grouping(x)
		case aggregateFuncs[x.Name]:
			if slices.ContainsFunc(x.Args, hasGrouping) {
				r.fail(errorf(adbc.StatusInvalidArgument, "aggregate function calls cannot be nested"))
			}
			a := groupingAgg{fn: x, name: hiddenColumn("agg", len(gp.aggs))}
			gp.aggs = append(gp.aggs, a)
			return &ColumnRef{Name: a.name}
		}
		f := *x
		f.Args = r.rewriteAll(x.Args)
		return &f
	case *Subquery:
		for _, ref := range x.outerRefs {
			if ref.up == 0 && !gp.groupedColumn(ref.name) {
				r.fail(errorf(adbc.StatusInvalidArgument, "subquery uses ungrouped column %q from outer query", r.column(ref.name)))
			}
		}
		if x.X == nil {
			return x
		}
		sq := *x
		sq.X = r.rewrite(x.X)
		return &sq
	case *WindowFunc:
		f := *x.Func
		f.Args = r.rewriteAll(x.Func.Args)
		f.Filter = r.rewrite(x.Func.Filter)
		over := *x.Over
		over.PartitionBy = r.rewriteAll(x.Over.PartitionBy)
		over.OrderBy = make([]OrderItem, len(x.Over.OrderBy))
		for i, o := range x.Over.OrderBy {
			o.Expr = r.rewrite(o.Expr)
			over.OrderBy[i] = o
		}
		if fr := x.Over.Frame; fr != nil {
			frame := *fr
			frame.Start.Offset, frame.End.Offset = r.rewrite(fr.Start.Offset), r.rewrite(fr.End.Offset)
			over.Frame = &frame
		}
		return &WindowFunc{Func: &f, Over: &over}
	}
	return e
}

// grouping rewrites a GROUPING(…) call into the column holding its value.
func (r *groupingRewriter) grouping(f *Func) Expr {
	gp := r.gp
	d, _ := lookupFunc(f.Name)
	if err := d.argsError(f); err != nil { // checked when bound, too
		r.fail(err)
		return f
	}
	if len(f.Args) > maxGroupingArgs {
		r.fail(errorf(adbc.StatusInvalidArgument, "GROUPING must have fewer than %d arguments", maxGroupingArgs+1))
		return f
	}
	c := groupingCall{name: hiddenColumn("grouping", len(gp.calls))}
	for _, a := range f.Args {
		i := gp.slotOf(a)
		if i < 0 {
			r.fail(errorf(adbc.StatusInvalidArgument, "arguments to GROUPING must be grouping expressions of the associated query level"))
			return f
		}
		c.args = append(c.args, i)
	}
	gp.calls = append(gp.calls, c)
	return &ColumnRef{Name: c.name}
}

// ---- execution ----

// setPlan is the grouped query of one grouping set: the base query grouped
// by the set's expressions, returning them and then every aggregate call.
// With input (see groupingInput), it groups those rows instead of reading
// its FROM relation.
func (gp *groupingPlan) setPlan(set []int, input *tableMeta) *selectPlan {
	sub := *gp.base
	sel := *gp.base.sel
	if input != nil {
		sub.meta, sel.Where = input, nil
	}
	sel.GroupBy, sel.GroupingSets = nil, nil
	sel.Having, sel.Qualify, sel.OrderBy, sel.Limit, sel.Offset = nil, nil, nil, nil, nil
	sel.Distinct, sel.DistinctOn = false, nil
	sub.items = make([]planItem, 0, len(set)+len(gp.aggs))
	for _, i := range set {
		s := gp.slots[i]
		sel.GroupBy = append(sel.GroupBy, s.expr)
		sub.items = append(sub.items, planItem{expr: s.expr, name: s.name, typ: s.typ})
	}
	for _, a := range gp.aggs {
		sub.items = append(sub.items, planItem{expr: a.fn, name: a.name, typ: a.typ})
	}
	sub.sel, sub.aggregate = &sel, true
	sub.order, sub.having, sub.qualify, sub.windows = nil, nil, nil, nil
	sub.distinct, sub.distinctOn, sub.grouping = false, nil, nil
	return &sub
}

// runGroupingSets runs each grouping set as its own grouped query, combines
// their groups into an in-memory relation, and runs the rest of the query
// (HAVING, windows, QUALIFY, ORDER BY, LIMIT) over it.
func (e *executor) runGroupingSets(ctx context.Context, plan *selectPlan, params []Value) ([][]Value, error) {
	gp := plan.grouping
	// The sets the index can't compute read the same rows: if there are
	// several, the rows are read once and each of those sets groups them.
	var input *tableMeta
	driver := make([]bool, len(gp.sets))
	if meta := gp.base.meta; meta != nil && len(gp.sets) > 1 {
		wp, err := e.planWhere(ctx, gp.base.sel.Where, meta, params)
		if err != nil {
			return nil, err
		}
		n := 0
		for i, set := range gp.sets {
			if driver[i] = !gp.indexable(e, wp, set); driver[i] {
				n++
			}
		}
		if n > 1 {
			if input, err = e.groupingInput(ctx, gp, wp, params); err != nil {
				return nil, err
			}
		}
	}
	var rows []map[string]Value
	// A set listed more than once (GROUPING SETS ((a), (a))) runs once; its
	// groups are repeated.
	done := map[string][]map[string]Value{}
	for i, set := range gp.sets {
		key := fmt.Sprint(slices.Sorted(slices.Values(set)))
		groups, ok := done[key]
		if !ok {
			var in *tableMeta
			if driver[i] {
				in = input
			}
			res, err := e.runSelect(ctx, gp.setPlan(set, in), params)
			if err != nil {
				return nil, err
			}
			groups = gp.combine(set, res)
			done[key] = groups
		}
		rows = append(rows, groups...)
	}
	meta := *plan.meta
	meta.mem = rows
	over := *plan
	over.meta, over.grouping, over.aggregate = &meta, nil, false
	over.sel = &SelectStmt{Where: gp.having, Limit: plan.sel.Limit, Offset: plan.sel.Offset}
	return e.runSelect(ctx, &over, params)
}

// indexable reports whether the index can compute a grouping set, by the
// conditions of indexAggregate. That still decides: a set misjudged here is
// computed correctly, only the slower way.
func (gp *groupingPlan) indexable(e *executor, wp wherePlan, set []int) bool {
	meta := gp.base.meta
	if e.pushdown == PushdownNone || wp.residual != nil || wp.keys != nil || meta.isMem || len(gp.base.extraNeed) > 0 {
		return false
	}
	indexed := func(x Expr) (columnMeta, bool) {
		c, ok := x.(*ColumnRef)
		if !ok {
			return columnMeta{}, false
		}
		col, ok := meta.column(c.Name)
		return col, ok && col.Indexed && simpleName(col.field())
	}
	for _, i := range set {
		col, ok := indexed(gp.slots[i].expr)
		if !ok || !(col.Type.Kind == KindString || pushableKind(col.Type.Kind, e.pushdown)) {
			return false
		}
	}
	var aggs []*Func
	for _, a := range gp.aggs {
		collectAggregates(a.fn, &aggs)
	}
	for _, f := range aggs {
		if f.Star {
			if f.Name != "COUNT" {
				return false
			}
			continue
		}
		if _, ok := indexReducers[f.Name]; !ok || f.Filter != nil || f.Distinct || len(f.Args) != 1 {
			return false
		}
		col, ok := indexed(f.Args[0])
		if !ok || col.Name == rowIDField || (f.Name != "COUNT" && !pushableKind(col.Type.Kind, e.pushdown)) {
			return false
		}
	}
	return true
}

// groupingInput reads the rows of a query with grouping sets (its FROM
// relation filtered by its WHERE) into an in-memory relation, with the
// columns its grouping sets read.
func (e *executor) groupingInput(ctx context.Context, gp *groupingPlan, wp wherePlan, params []Value) (*tableMeta, error) {
	base := gp.base
	need := maps.Clone(base.extraNeed)
	if need == nil {
		need = map[string]bool{}
	}
	for _, s := range gp.slots {
		columnRefs(s.expr, need)
	}
	for _, a := range gp.aggs {
		columnRefs(a.fn, need)
	}
	_, rows, err := e.scan(ctx, scanRequest{meta: base.meta, where: wp, need: need}, params)
	if err != nil {
		return nil, err
	}
	return &tableMeta{Name: base.meta.Name, Columns: base.meta.Columns, isMem: true, mem: rows}, nil
}

// combine turns the rows of a grouping set's query into rows of the
// combined groups.
func (gp *groupingPlan) combine(set []int, res [][]Value) []map[string]Value {
	grouped := make([]bool, len(gp.slots))
	for _, i := range set {
		grouped[i] = true
	}
	// GROUPING(a, b, …) has a bit per argument, the last one lowest, set
	// when the argument is not grouped.
	masks := make([]int64, len(gp.calls))
	for k, c := range gp.calls {
		for _, a := range c.args {
			masks[k] <<= 1
			if !grouped[a] {
				masks[k] |= 1
			}
		}
	}
	out := make([]map[string]Value, len(res))
	for r, vals := range res {
		row := make(map[string]Value, len(gp.slots)+len(gp.calls)+len(gp.aggs))
		for _, s := range gp.slots {
			row[s.name] = nullValue(s.typ)
		}
		for j, i := range set {
			row[gp.slots[i].name] = vals[j]
		}
		for k, c := range gp.calls {
			row[c.name] = intValue(typeInt32, masks[k])
		}
		for j, a := range gp.aggs {
			row[a.name] = vals[len(set)+j]
		}
		out[r] = row
	}
	return out
}
