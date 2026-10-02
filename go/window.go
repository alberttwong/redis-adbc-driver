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

// Window functions.
//
// fn(args) OVER ([PARTITION BY …] [ORDER BY …] [frame]) is computed by the
// driver once the query's rows are known: after WHERE, GROUP BY and HAVING
// (over a grouped query, a window sees one row per group), and before
// QUALIFY, ORDER BY and LIMIT. RediSearch has no window functions, so a query
// with windows never pushes its LIMIT into the index; its WHERE filters and
// GROUP BY aggregates still run in the index when they can, because they are
// computed before the windows.
//
// Window functions with the same PARTITION BY and ORDER BY share one sort:
// rows are hashed into partitions, and each partition is sorted by the ORDER
// BY keys (ties keep the input order). Each function is then computed in one
// pass over each partition:
//
//   - Ranking functions and LAG / LEAD use row positions and peer groups
//     (rows with equal ORDER BY keys).
//   - Aggregates and FIRST_VALUE / LAST_VALUE / NTH_VALUE use the frame. The
//     frame's bounds only move forward as the current row advances, so an
//     aggregate adds the rows that enter the frame and removes the rows that
//     leave it: O(1) amortized per row for COUNT, SUM and AVG, and a monotonic
//     deque for MIN / MAX. Sums are exact (exactsum.go), also of doubles, so
//     subtracting the values that leave the frame costs no precision.
//
// NULLs sort last in either direction unless NULLS FIRST is given, as in
// ORDER BY.

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ---- parsing ----

// windowSpecWords are the keywords that can start a window definition (so
// they cannot be the name of the window it builds on).
var windowSpecWords = map[string]bool{"PARTITION": true, "ORDER": true, "ROWS": true, "RANGE": true, "GROUPS": true}

// parseOver parses what follows OVER: a window name or (definition).
func (p *parser) parseOver() (*WindowSpec, error) {
	if !p.acceptOp("(") {
		t := p.peek()
		if t.kind != tokIdent && t.kind != tokQuotedIdent {
			return nil, syntaxErr("expected a window name or \"(\" after OVER near %q", t.text)
		}
		p.pos++
		return &WindowSpec{Ref: t.text, bare: true}, nil
	}
	return p.parseWindowSpec()
}

// parseWindowSpec parses `[name] [PARTITION BY …] [ORDER BY …] [frame] )`
// after the opening parenthesis.
func (p *parser) parseWindowSpec() (*WindowSpec, error) {
	spec := &WindowSpec{}
	if t := p.peek(); t.kind == tokQuotedIdent || (t.kind == tokIdent && !windowSpecWords[strings.ToUpper(t.text)]) {
		p.pos++
		spec.Ref = t.text
	}
	if p.acceptKeyword("PARTITION", "BY") {
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			spec.PartitionBy = append(spec.PartitionBy, e)
			if !p.acceptOp(",") {
				break
			}
		}
	}
	if p.acceptKeyword("ORDER", "BY") {
		items, err := p.parseOrderItems()
		if err != nil {
			return nil, err
		}
		spec.OrderBy = items
	}
	if p.isKeyword("ROWS") || p.isKeyword("RANGE") || p.isKeyword("GROUPS") {
		f, err := p.parseFrame()
		if err != nil {
			return nil, err
		}
		spec.Frame = f
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return spec, nil
}

// parseFrame parses `{ROWS | RANGE | GROUPS} {start | BETWEEN start AND end}
// [EXCLUDE {CURRENT ROW | GROUP | TIES | NO OTHERS}]`; a lone start bound
// ends at CURRENT ROW.
func (p *parser) parseFrame() (*WindowFrame, error) {
	f := &WindowFrame{}
	switch strings.ToUpper(p.next().text) {
	case "ROWS":
		f.Unit = FrameRows
	case "RANGE":
		f.Unit = FrameRange
	default:
		f.Unit = FrameGroups
	}
	var err error
	if p.acceptKeyword("BETWEEN") {
		if f.Start, err = p.parseFrameBound(); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}
		if f.End, err = p.parseFrameBound(); err != nil {
			return nil, err
		}
	} else {
		if f.Start, err = p.parseFrameBound(); err != nil {
			return nil, err
		}
		f.End = FrameBound{Kind: BoundCurrentRow}
	}
	if p.acceptKeyword("EXCLUDE") {
		switch {
		case p.acceptKeyword("CURRENT", "ROW"):
			f.Exclude = ExcludeCurrentRow
		case p.acceptKeyword("GROUP"):
			f.Exclude = ExcludeGroup
		case p.acceptKeyword("TIES"):
			f.Exclude = ExcludeTies
		case !p.acceptKeyword("NO", "OTHERS"):
			return nil, syntaxErr("expected CURRENT ROW, GROUP, TIES or NO OTHERS after EXCLUDE near %q", p.peek().text)
		}
	}
	switch {
	case f.Start.Kind == BoundUnboundedFollowing:
		return nil, &sqlError{msg: "frame start cannot be UNBOUNDED FOLLOWING"}
	case f.End.Kind == BoundUnboundedPreceding:
		return nil, &sqlError{msg: "frame end cannot be UNBOUNDED PRECEDING"}
	case f.Start.Kind == BoundCurrentRow && f.End.Kind == BoundPreceding:
		return nil, &sqlError{msg: "frame starting from current row cannot have preceding rows"}
	case f.Start.Kind == BoundFollowing && (f.End.Kind == BoundPreceding || f.End.Kind == BoundCurrentRow):
		return nil, &sqlError{msg: "frame starting from following row cannot have preceding rows"}
	}
	return f, nil
}

func (p *parser) parseFrameBound() (FrameBound, error) {
	switch {
	case p.acceptKeyword("UNBOUNDED", "PRECEDING"):
		return FrameBound{Kind: BoundUnboundedPreceding}, nil
	case p.acceptKeyword("UNBOUNDED", "FOLLOWING"):
		return FrameBound{Kind: BoundUnboundedFollowing}, nil
	case p.acceptKeyword("CURRENT", "ROW"):
		return FrameBound{Kind: BoundCurrentRow}, nil
	}
	x, err := p.parseAdditive()
	if err != nil {
		return FrameBound{}, err
	}
	switch {
	case p.acceptKeyword("PRECEDING"):
		return FrameBound{Kind: BoundPreceding, Offset: x}, nil
	case p.acceptKeyword("FOLLOWING"):
		return FrameBound{Kind: BoundFollowing, Offset: x}, nil
	}
	return FrameBound{}, syntaxErr("expected PRECEDING or FOLLOWING near %q", p.peek().text)
}

// parseWindowClause parses `name AS (…), …` after WINDOW. A definition may
// build on an earlier one.
func (p *parser) parseWindowClause(sel *SelectStmt) error {
	for {
		name, err := p.parseIdent()
		if err != nil {
			return err
		}
		for _, w := range sel.Windows {
			if strings.EqualFold(w.Name, name) {
				return &sqlError{msg: fmt.Sprintf("window %q is already defined", name)}
			}
		}
		if err := p.expectKeyword("AS"); err != nil {
			return err
		}
		if err := p.expectOp("("); err != nil {
			return err
		}
		spec, err := p.parseWindowSpec()
		if err != nil {
			return err
		}
		if spec, err = resolveWindowSpec(sel.Windows, spec); err != nil {
			return err
		}
		sel.Windows = append(sel.Windows, NamedWindow{Name: name, Spec: spec})
		if !p.acceptOp(",") {
			return nil
		}
	}
}

// resolveWindowRefs replaces the references to named windows (OVER w,
// OVER (w …)) in e with the windows they denote.
func resolveWindowRefs(defs []NamedWindow, e Expr) error {
	var err error
	walkExpr(e, func(x Expr) {
		if w, ok := x.(*WindowFunc); ok && err == nil {
			var spec *WindowSpec
			if spec, err = resolveWindowSpec(defs, w.Over); err == nil {
				w.Over = spec
			}
		}
	})
	return err
}

// resolveWindowSpec applies the SQL rules for a window based on a named one:
// OVER w uses w as it is; OVER (w …) copies w's PARTITION BY and ORDER BY,
// may add an ORDER BY only if w has none, and has its own frame (w must not
// have one).
func resolveWindowSpec(defs []NamedWindow, s *WindowSpec) (*WindowSpec, error) {
	if s.Ref == "" {
		return s, nil
	}
	var base *WindowSpec
	for _, d := range defs {
		if strings.EqualFold(d.Name, s.Ref) {
			base = d.Spec
		}
	}
	switch {
	case base == nil:
		return nil, &sqlError{msg: fmt.Sprintf("window %q does not exist", s.Ref)}
	case s.bare:
		return base, nil
	case len(s.PartitionBy) > 0:
		return nil, &sqlError{msg: fmt.Sprintf("cannot override PARTITION BY clause of window %q", s.Ref)}
	case len(s.OrderBy) > 0 && len(base.OrderBy) > 0:
		return nil, &sqlError{msg: fmt.Sprintf("cannot override ORDER BY clause of window %q", s.Ref)}
	case base.Frame != nil:
		return nil, &sqlError{msg: fmt.Sprintf("cannot copy window %q because it has a frame clause; use OVER %s without parentheses", s.Ref, s.Ref)}
	}
	out := &WindowSpec{PartitionBy: base.PartitionBy, OrderBy: base.OrderBy, Frame: s.Frame}
	if len(s.OrderBy) > 0 {
		out.OrderBy = s.OrderBy
	}
	return out, nil
}

// children returns the expressions of a window call: its arguments (with
// an aggregate's ORDER BY and FILTER), its PARTITION BY and ORDER BY
// expressions and its frame offsets.
func (w *WindowFunc) children() []Expr {
	out := slices.Clone(w.Func.Args)
	for _, o := range w.Func.OrderBy {
		out = append(out, o.Expr)
	}
	if w.Func.Filter != nil {
		out = append(out, w.Func.Filter)
	}
	return append(out, windowSpecExprs(w.Over)...)
}

// windowSpecExprs returns the expressions of a window definition: its
// PARTITION BY and ORDER BY expressions and its frame offsets.
func windowSpecExprs(s *WindowSpec) []Expr {
	out := slices.Clone(s.PartitionBy)
	for _, o := range s.OrderBy {
		out = append(out, o.Expr)
	}
	if f := s.Frame; f != nil {
		for _, b := range []FrameBound{f.Start, f.End} {
			if b.Offset != nil {
				out = append(out, b.Offset)
			}
		}
	}
	return out
}

// errWindowPlacement is the error for a window function in UPDATE, DELETE,
// MERGE or VALUES.
func errWindowPlacement() error {
	return errorf(adbc.StatusInvalidArgument, "window functions are only allowed in a SELECT list, ORDER BY or QUALIFY")
}

func containsWindow(e Expr) bool {
	found := false
	walkExpr(e, func(x Expr) {
		if _, ok := x.(*WindowFunc); ok {
			found = true
		}
	})
	return found
}

// ---- planning ----

// windowCall is a window function of a query with its result type.
type windowCall struct {
	fn  *WindowFunc
	typ ColType
}

// planWindows collects and checks the window function calls of the SELECT
// list, ORDER BY and QUALIFY (the only places they may appear).
func (e *executor) planWindows(plan *selectPlan, types map[string]ColType) error {
	exprs := []Expr{plan.qualify}
	for _, it := range plan.items {
		exprs = append(exprs, it.expr)
	}
	for _, o := range plan.order {
		exprs = append(exprs, o.expr)
	}
	var err error
	seen := map[*WindowFunc]bool{}
	for _, x := range exprs {
		walkExpr(x, func(n Expr) {
			switch v := n.(type) {
			case *WindowFunc:
				if !seen[v] {
					seen[v] = true
					plan.windows = append(plan.windows, windowCall{fn: v})
				}
			case *Func:
				if aggregateFuncs[v.Name] && err == nil {
					for _, a := range v.Args {
						if containsWindow(a) {
							err = errorf(adbc.StatusInvalidArgument, "aggregate function calls cannot contain window function calls")
						}
					}
				}
			}
		})
	}
	if err != nil {
		return err
	}
	for i := range plan.windows {
		w := plan.windows[i].fn
		if err := checkWindow(w, types, e.paramTypes); err != nil {
			return err
		}
		t, err := windowType(w, types, e.paramTypes)
		if err != nil {
			return invalidArg(err)
		}
		plan.windows[i].typ = t
	}
	return nil
}

func checkWindow(w *WindowFunc, types map[string]ColType, params []ColType) error {
	f := w.Func
	for _, c := range w.children() {
		if containsWindow(c) {
			return errorf(adbc.StatusInvalidArgument, "window function calls cannot be nested")
		}
	}
	// Binding has resolved the call (resolveCall); this is for windows that
	// weren't bound.
	if err := resolveCall(f, true); err != nil {
		if err == errUnknownFunction {
			return noSuchFunction(f.Name, f.Star, inferArgTypes(f.Args, types, params))
		}
		return err
	}
	if f.Distinct {
		return errorf(adbc.StatusNotImplemented, "DISTINCT is not supported in window functions")
	}
	// The integer arguments the registry requires (NTILE's, and the offset
	// of LAG, LEAD and NTH_VALUE).
	d, _ := lookupFunc(f.Name)
	for i, at := range d.args {
		if at != argInt || i >= len(f.Args) {
			continue
		}
		t, err := inferType(f.Args[i], types, params)
		if err != nil {
			return invalidArg(err)
		}
		if !at.accepts(t) {
			return errorf(adbc.StatusInvalidArgument, "argument %d of %s must be an integer, not %s", i+1, f.Name, t.SQLName())
		}
	}
	switch {
	case f.Filter != nil && windowOnlyFuncs[f.Name]:
		return errorf(adbc.StatusInvalidArgument, "FILTER is not implemented for non-aggregate window functions")
	case len(f.OrderBy) > 0:
		return errorf(adbc.StatusNotImplemented, "aggregate ORDER BY is not implemented for window functions")
	case f.Nulls != NullsUnspecified && !nullTreatmentFuncs[f.Name]:
		if aggregateFuncs[f.Name] {
			return errorf(adbc.StatusInvalidArgument, "aggregate functions do not accept RESPECT/IGNORE NULLS")
		}
		return errorf(adbc.StatusInvalidArgument, "function %s does not allow RESPECT/IGNORE NULLS", f.Name)
	}
	return checkFrame(w, types, params)
}

var frameUnitNames = map[FrameUnit]string{FrameRows: "ROWS", FrameRange: "RANGE", FrameGroups: "GROUPS"}

func checkFrame(w *WindowFunc, types map[string]ColType, params []ColType) error {
	fr := w.Over.Frame
	if fr == nil {
		return nil
	}
	order := w.Over.OrderBy
	if fr.Unit == FrameGroups && len(order) == 0 {
		return errorf(adbc.StatusInvalidArgument, "GROUPS mode requires an ORDER BY clause")
	}
	for _, b := range []FrameBound{fr.Start, fr.End} {
		if b.Offset == nil {
			continue
		}
		if !isConstant(b.Offset) {
			return errorf(adbc.StatusInvalidArgument, "window frame offsets must be constants")
		}
		t, err := inferType(b.Offset, types, params)
		if err != nil {
			return invalidArg(err)
		}
		if fr.Unit != FrameRange {
			if !t.Kind.isInteger() && t.Kind != KindNull {
				return errorf(adbc.StatusInvalidArgument, "%s frame offsets must be integers, not %s", frameUnitNames[fr.Unit], t.SQLName())
			}
			continue
		}
		if len(order) != 1 {
			return errorf(adbc.StatusInvalidArgument, "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column")
		}
		kt, err := inferType(order[0].Expr, types, params)
		if err != nil {
			return invalidArg(err)
		}
		if !rangeOffsetOK(kt, t) {
			return errorf(adbc.StatusInvalidArgument, "RANGE with offset PRECEDING/FOLLOWING is not supported for ORDER BY type %s and offset type %s",
				kt.SQLName(), t.SQLName())
		}
	}
	return nil
}

// rangeOffsetOK reports whether RANGE … PRECEDING/FOLLOWING can offset an
// ORDER BY key of type key by an offset of type off: numbers by numbers, and
// dates, times, timestamps and intervals by intervals.
func rangeOffsetOK(key, off ColType) bool {
	if key.Kind == KindNull || off.Kind == KindNull {
		return true
	}
	switch {
	case key.Kind.isNumeric():
		return off.Kind.isNumeric()
	case key.Kind == KindDate || key.Kind == KindTime || key.Kind == KindTimestamp || key.Kind == KindInterval:
		return off.Kind == KindInterval
	}
	return false
}

// windowType is the result type of a window function: BIGINT for ranks and
// counts, DOUBLE PRECISION for PERCENT_RANK and CUME_DIST, the argument's type
// for value functions, and the usual aggregate types for aggregates.
func windowType(w *WindowFunc, cols map[string]ColType, params []ColType) (ColType, error) {
	f := w.Func
	arg := func(i int) (ColType, error) {
		if i >= len(f.Args) {
			return typeNull, nil
		}
		return inferType(f.Args[i], cols, params)
	}
	switch f.Name {
	case "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "COUNT":
		return typeInt64, nil
	case "PERCENT_RANK", "CUME_DIST":
		return typeFloat64, nil
	case "LAG", "LEAD":
		t, err := arg(0)
		if err != nil || len(f.Args) < 3 {
			return t.withoutLength(), err
		}
		d, err := arg(2)
		if err != nil {
			return ColType{}, err
		}
		return commonType(t, d).withoutLength(), nil
	case "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
		t, err := arg(0)
		return t.withoutLength(), err
	}
	if t, ok, err := aggregateType(f, cols, params, true); ok {
		return t, err
	}
	return inferType(f, cols, params)
}

// ---- execution ----

// applyWindows computes the window functions of a query over its n input
// rows (setRow(i) points env at row i) and then applies QUALIFY. It returns
// the rows that remain.
func (e *executor) applyWindows(env *evalEnv, plan *selectPlan, n int, setRow func(int)) ([]int, error) {
	env.win = make(map[*WindowFunc][]Value, len(plan.windows))
	for _, g := range groupWindows(plan.windows) {
		if err := computeWindowGroup(env, g, n, setRow); err != nil {
			return nil, invalidArg(err)
		}
	}
	keep := make([]int, 0, n)
	for i := 0; i < n; i++ {
		if plan.qualify != nil {
			setRow(i)
			v, err := env.eval(plan.qualify)
			if err != nil {
				return nil, invalidArg(err)
			}
			if b, ok := truthy(v); !ok || !b {
				continue
			}
		}
		keep = append(keep, i)
	}
	return keep, nil
}

// windowGroup is a set of window calls with the same PARTITION BY and ORDER
// BY, computed over one sort.
type windowGroup struct {
	spec  *WindowSpec
	calls []windowCall
}

func groupWindows(calls []windowCall) []*windowGroup {
	var groups []*windowGroup
	for _, c := range calls {
		var g *windowGroup
		for _, cand := range groups {
			if sameOrdering(cand.spec, c.fn.Over) {
				g = cand
				break
			}
		}
		if g == nil {
			g = &windowGroup{spec: c.fn.Over}
			groups = append(groups, g)
		}
		g.calls = append(g.calls, c)
	}
	return groups
}

func sameOrdering(a, b *WindowSpec) bool {
	if a == b {
		return true
	}
	if len(a.PartitionBy) != len(b.PartitionBy) || len(a.OrderBy) != len(b.OrderBy) {
		return false
	}
	for i := range a.PartitionBy {
		if !exprEqual(a.PartitionBy[i], b.PartitionBy[i]) {
			return false
		}
	}
	for i, o := range a.OrderBy {
		q := b.OrderBy[i]
		if o.Desc != q.Desc || (o.Nulls == NullsFirst) != (q.Nulls == NullsFirst) || !exprEqual(o.Expr, q.Expr) {
			return false
		}
	}
	return true
}

// exprEqual reports whether two bound expressions are the same (it may
// report false for equal expressions it does not compare, e.g. subqueries).
func exprEqual(a, b Expr) bool {
	if a == b {
		return true
	}
	switch x := a.(type) {
	case *Literal:
		y, ok := b.(*Literal)
		return ok && x.V.T == y.V.T && x.V.Null == y.V.Null && x.V.Text() == y.V.Text()
	case *ColumnRef:
		y, ok := b.(*ColumnRef)
		return ok && x.Name == y.Name && x.Outer == y.Outer
	case *Param:
		y, ok := b.(*Param)
		return ok && x.Index == y.Index
	case *Unary:
		y, ok := b.(*Unary)
		return ok && x.Op == y.Op && exprEqual(x.X, y.X)
	case *Binary:
		y, ok := b.(*Binary)
		return ok && x.Op == y.Op && exprEqual(x.L, y.L) && exprEqual(x.R, y.R)
	case *IsNull:
		y, ok := b.(*IsNull)
		return ok && x.Not == y.Not && exprEqual(x.X, y.X)
	case *Cast:
		y, ok := b.(*Cast)
		return ok && x.T == y.T && exprEqual(x.X, y.X) && exprEqual(x.OnError, y.OnError)
	case *Func:
		y, ok := b.(*Func)
		if !ok || x.Name != y.Name || x.Star != y.Star || x.Distinct != y.Distinct || len(x.Args) != len(y.Args) ||
			len(x.OrderBy) != len(y.OrderBy) || x.WithinGroup != y.WithinGroup || x.Nulls != y.Nulls ||
			(x.Filter == nil) != (y.Filter == nil) || (x.Filter != nil && !exprEqual(x.Filter, y.Filter)) {
			return false
		}
		for i := range x.Args {
			if !exprEqual(x.Args[i], y.Args[i]) {
				return false
			}
		}
		for i, o := range x.OrderBy {
			q := y.OrderBy[i]
			if o.Desc != q.Desc || o.Nulls != q.Nulls || !exprEqual(o.Expr, q.Expr) {
				return false
			}
		}
		return true
	case *Case:
		y, ok := b.(*Case)
		if !ok || len(x.Whens) != len(y.Whens) || !exprEqual(x.Operand, y.Operand) || !exprEqual(x.Else, y.Else) {
			return false
		}
		for i, w := range x.Whens {
			if !exprEqual(w.When, y.Whens[i].When) || !exprEqual(w.Then, y.Whens[i].Then) {
				return false
			}
		}
		return true
	}
	return false
}

// compareKeys is a three-way lessKeys.
func compareKeys(a, b []Value, order []planOrder) int {
	for i, o := range order {
		x, y := a[i], b[i]
		switch {
		case x.Null && y.Null:
			continue
		case x.Null:
			if o.nullsFirst {
				return -1
			}
			return 1
		case y.Null:
			if o.nullsFirst {
				return 1
			}
			return -1
		}
		c, _ := compareValues(x, y)
		if c == 0 {
			continue
		}
		if o.desc {
			return -c
		}
		return c
	}
	return 0
}

// windowSet is the input rows of a window group in window order:
// partitions are contiguous, each sorted by the ORDER BY keys. Positions
// index perm; rows are the caller's row numbers.
type windowSet struct {
	perm       []int // row at each position
	parts      []int // partition boundaries: partition p is [parts[p], parts[p+1])
	grp        []int // peer group of each position (numbered across partitions)
	groupStart []int // first position of each peer group; groupStart[g+1] ends group g
	okeys      []Value
	no         int // number of ORDER BY keys
	order      []planOrder
}

func (ws *windowSet) okey(row int) []Value { return ws.okeys[row*ws.no : (row+1)*ws.no] }

func computeWindowGroup(env *evalEnv, g *windowGroup, n int, setRow func(int)) error {
	np, no := len(g.spec.PartitionBy), len(g.spec.OrderBy)
	ws := &windowSet{no: no, okeys: make([]Value, n*no), order: make([]planOrder, no)}
	for i, o := range g.spec.OrderBy {
		ws.order[i] = planOrder{desc: o.Desc, nullsFirst: o.Nulls == NullsFirst}
	}
	// Evaluate the partition keys, the order keys and every call's
	// arguments for each row. A FILTER is evaluated first: the arguments of
	// the rows it rejects are left NULL, which aggregates skip, and keep
	// records them for COUNT(*).
	pkeys := make([]Value, n*np)
	args := make([][][]Value, len(g.calls))
	keep := make([][]bool, len(g.calls))
	for j, c := range g.calls {
		args[j] = make([][]Value, len(c.fn.Func.Args))
		for a := range args[j] {
			args[j][a] = make([]Value, n)
		}
		if c.fn.Func.Filter != nil {
			keep[j] = make([]bool, n)
		}
	}
	for i := 0; i < n; i++ {
		setRow(i)
		for k, x := range g.spec.PartitionBy {
			v, err := env.eval(x)
			if err != nil {
				return err
			}
			pkeys[i*np+k] = v
		}
		for k, o := range g.spec.OrderBy {
			v, err := env.eval(o.Expr)
			if err != nil {
				return err
			}
			ws.okeys[i*no+k] = v
		}
		for j, c := range g.calls {
			if keep[j] != nil {
				v, err := env.eval(c.fn.Func.Filter)
				if err != nil {
					return err
				}
				if b, ok := truthy(v); !ok || !b {
					for a := range args[j] {
						args[j][a][i] = nullValue(typeNull)
					}
					continue
				}
				keep[j][i] = true
			}
			for a, x := range c.fn.Func.Args {
				v, err := env.eval(x)
				if err != nil {
					return err
				}
				args[j][a][i] = v
			}
		}
	}

	// Partition (NULLs equal, 1 = 1.0) with a counting sort that keeps the
	// input order, then sort each partition; ties keep the input order.
	part := make([]int, n)
	nparts := 0
	if np > 0 {
		ids := map[string]int{}
		for i := 0; i < n; i++ {
			k := rowKey(pkeys[i*np : (i+1)*np])
			id, ok := ids[k]
			if !ok {
				id = len(ids)
				ids[k] = id
			}
			part[i] = id
		}
		nparts = len(ids)
	} else if n > 0 {
		nparts = 1
	}
	ws.parts = make([]int, nparts+1)
	for _, p := range part {
		ws.parts[p+1]++
	}
	for p := 0; p < nparts; p++ {
		ws.parts[p+1] += ws.parts[p]
	}
	fill := slices.Clone(ws.parts)
	ws.perm = make([]int, n)
	for i, p := range part {
		ws.perm[fill[p]] = i
		fill[p]++
	}
	ws.grp = make([]int, n)
	for p := 0; p < nparts; p++ {
		ps, pe := ws.parts[p], ws.parts[p+1]
		if no > 0 {
			slices.SortFunc(ws.perm[ps:pe], func(a, b int) int {
				if c := compareKeys(ws.okey(a), ws.okey(b), ws.order); c != 0 {
					return c
				}
				return cmp.Compare(a, b)
			})
		}
		for i := ps; i < pe; i++ {
			if i == ps || compareKeys(ws.okey(ws.perm[i-1]), ws.okey(ws.perm[i]), ws.order) != 0 {
				ws.groupStart = append(ws.groupStart, i)
			}
			ws.grp[i] = len(ws.groupStart) - 1
		}
	}
	ws.groupStart = append(ws.groupStart, n)

	for j, c := range g.calls {
		vals := make([]Value, n)
		if err := ws.compute(env, c, args[j], keep[j], vals); err != nil {
			return err
		}
		env.win[c.fn] = vals
	}
	return nil
}

// compute evaluates one window call; vals is indexed by row. keep is the
// result of the call's FILTER for each row (nil without one).
func (ws *windowSet) compute(env *evalEnv, c windowCall, args [][]Value, keep []bool, vals []Value) error {
	f := c.fn.Func
	nparts := len(ws.parts) - 1
	switch f.Name {
	case "ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST":
		for p := 0; p < nparts; p++ {
			ps, pe := ws.parts[p], ws.parts[p+1]
			m := pe - ps
			for i := ps; i < pe; i++ {
				rank := ws.groupStart[ws.grp[i]] - ps + 1
				var v Value
				switch f.Name {
				case "ROW_NUMBER":
					v = intValue(typeInt64, int64(i-ps+1))
				case "RANK":
					v = intValue(typeInt64, int64(rank))
				case "DENSE_RANK":
					v = intValue(typeInt64, int64(ws.grp[i]-ws.grp[ps]+1))
				case "PERCENT_RANK":
					r := 0.0
					if m > 1 {
						r = float64(rank-1) / float64(m-1)
					}
					v = floatValue(typeFloat64, r)
				default: // CUME_DIST: rows up to the last peer, over the partition size
					v = floatValue(typeFloat64, float64(ws.groupStart[ws.grp[i]+1]-ps)/float64(m))
				}
				vals[ws.perm[i]] = v
			}
		}
		return nil
	case "NTILE":
		return ws.ntile(args[0], vals)
	case "LAG", "LEAD":
		return ws.lagLead(c, args, vals)
	}

	lo, hi, err := ws.frame(env, c.fn)
	if err != nil {
		return err
	}
	excl := ExcludeNoOthers
	if fr := c.fn.Over.Frame; fr != nil {
		excl = fr.Exclude
	}
	switch f.Name {
	case "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
		return ws.valueAt(c, args, lo, hi, excl, vals)
	}

	var in []Value
	star := f.Star
	switch {
	case star && keep != nil:
		// COUNT(*) FILTER (…) counts the rows kept, as non-NULL inputs.
		in, star = make([]Value, len(keep)), false
		for r, k := range keep {
			in[r] = nullValue(typeBool)
			if k {
				in[r] = boolValue(true)
			}
		}
	case !star:
		in = args[0]
	}
	agg := newFrameAgg(f, star, c.typ, in)
	if len(args) > 1 {
		agg.sep = args[1] // STRING_AGG / LISTAGG
	}
	if excl != ExcludeNoOthers {
		// The frame has a hole that moves with the current row, so each
		// row's frame is aggregated afresh, as PostgreSQL does.
		for i, row := range ws.perm {
			agg.reset()
			for _, part := range ws.frameParts(i, lo[i], hi[i], excl) {
				for p := part[0]; p < part[1]; p++ {
					agg.add(ws.perm[p], p)
				}
			}
			v, err := agg.result()
			if err != nil {
				return err
			}
			vals[row] = v
		}
		return nil
	}

	// Aggregates over the frame: add rows entering it, remove rows leaving it.
	for p := 0; p < nparts; p++ {
		ps, pe := ws.parts[p], ws.parts[p+1]
		agg.reset()
		a, b := ps, ps
		for i := ps; i < pe; i++ {
			for ; b < hi[i]; b++ {
				agg.add(ws.perm[b], b)
			}
			for ; a < lo[i]; a++ {
				agg.remove(ws.perm[a], a)
			}
			v, err := agg.result()
			if err != nil {
				return err
			}
			vals[ws.perm[i]] = v
		}
	}
	return nil
}

// valueAt computes FIRST_VALUE, LAST_VALUE and NTH_VALUE: the value at the
// first, last or n-th row of the frame (without the rows EXCLUDE removes),
// counting only rows with a non-NULL value with IGNORE NULLS.
func (ws *windowSet) valueAt(c windowCall, args [][]Value, lo, hi []int, excl FrameExclusion, vals []Value) error {
	f := c.fn.Func
	// count(s, e) is the number of rows that count in positions [s, e), and
	// kth(s, e, k) the position of the k-th one.
	count := func(s, e int) int { return max(e-s, 0) }
	kth := func(s, e, k int) int { return s + k - 1 }
	if f.Nulls == IgnoreNulls {
		pre, pos := ws.nonNulls(args[0])
		count = func(s, e int) int {
			if e <= s {
				return 0
			}
			return pre[e] - pre[s]
		}
		kth = func(s, e, k int) int { return pos[pre[s]+k-1] }
	}
	for i, row := range ws.perm {
		parts := ws.frameParts(i, lo[i], hi[i], excl)
		k := 1 // FIRST_VALUE
		switch f.Name {
		case "LAST_VALUE":
			k = 0
			for _, p := range parts {
				k += count(p[0], p[1])
			}
		case "NTH_VALUE":
			nv := args[1][row]
			if nv.Null {
				k = 0
				break
			}
			nth, err := Coerce(nv, typeInt64)
			if err != nil {
				return err
			}
			if nth.I <= 0 {
				return fmt.Errorf("argument of NTH_VALUE must be greater than zero")
			}
			k = int(min(nth.I, int64(len(ws.perm)+1)))
		}
		vals[row] = nullValue(c.typ)
		for _, p := range parts {
			if k <= 0 {
				break
			}
			if n := count(p[0], p[1]); k > n {
				k -= n
				continue
			}
			vals[row] = args[0][ws.perm[kth(p[0], p[1], k)]]
			break
		}
	}
	return nil
}

// frameParts returns the frame [lo, hi) of position i without the rows its
// EXCLUDE clause removes, as up to three ranges of positions in order (an
// empty range has end <= start).
func (ws *windowSet) frameParts(i, lo, hi int, excl FrameExclusion) [3][2]int {
	if excl == ExcludeNoOthers {
		return [3][2]int{{lo, hi}}
	}
	xs, xe := i, i+1 // EXCLUDE CURRENT ROW
	if excl != ExcludeCurrentRow {
		xs, xe = ws.groupStart[ws.grp[i]], ws.groupStart[ws.grp[i]+1]
	}
	parts := [3][2]int{{lo, min(hi, xs)}, {}, {max(lo, xe), hi}}
	if excl == ExcludeTies && i >= lo && i < hi {
		parts[1] = [2]int{i, i + 1}
	}
	return parts
}

// nonNulls indexes the positions where a row's value is not NULL, for
// IGNORE NULLS: pre[p] is the number of them before position p, and pos
// lists them in order.
func (ws *windowSet) nonNulls(arg []Value) ([]int, []int) {
	pre := make([]int, len(ws.perm)+1)
	var pos []int
	for p, row := range ws.perm {
		pre[p+1] = pre[p]
		if !arg[row].Null {
			pre[p+1]++
			pos = append(pos, p)
		}
	}
	return pre, pos
}

// ntile splits each partition into n buckets as evenly as possible (the
// first buckets get one row more); n is read from the partition's first row.
func (ws *windowSet) ntile(arg []Value, vals []Value) error {
	for p := 0; p+1 < len(ws.parts); p++ {
		ps, pe := ws.parts[p], ws.parts[p+1]
		nv := arg[ws.perm[ps]]
		if nv.Null {
			for i := ps; i < pe; i++ {
				vals[ws.perm[i]] = nullValue(typeInt64)
			}
			continue
		}
		b, err := Coerce(nv, typeInt64)
		if err != nil {
			return err
		}
		if b.I <= 0 {
			return fmt.Errorf("argument of NTILE must be greater than zero")
		}
		m := int64(pe - ps)
		q, r := m/b.I, m%b.I
		for i := ps; i < pe; i++ {
			k := int64(i - ps)
			var bucket int64
			if k < r*(q+1) {
				bucket = k/(q+1) + 1
			} else {
				bucket = r + (k-r*(q+1))/q + 1
			}
			vals[ws.perm[i]] = intValue(typeInt64, bucket)
		}
	}
	return nil
}

// lagLead reads the argument offset rows before (LAG) or after (LEAD) the
// current row in its partition, or the default when there is no such row.
// With IGNORE NULLS only the rows with a non-NULL argument count.
func (ws *windowSet) lagLead(c windowCall, args [][]Value, vals []Value) error {
	lead := c.fn.Func.Name == "LEAD"
	var pre, pos []int
	if c.fn.Func.Nulls == IgnoreNulls {
		pre, pos = ws.nonNulls(args[0])
	}
	for p := 0; p+1 < len(ws.parts); p++ {
		ps, pe := ws.parts[p], ws.parts[p+1]
		for i := ps; i < pe; i++ {
			row := ws.perm[i]
			off := int64(1)
			if len(args) > 1 {
				ov := args[1][row]
				if ov.Null {
					vals[row] = nullValue(c.typ)
					continue
				}
				o, err := Coerce(ov, typeInt64)
				if err != nil {
					return err
				}
				off = o.I
			}
			// Offsets beyond the partition (in either direction) never match.
			off = max(min(off, int64(pe-ps)), -int64(pe-ps))
			if !lead {
				off = -off
			}
			t := -1 // the position read
			switch {
			case pre == nil || off == 0:
				if q := int64(i) + off; q >= int64(ps) && q < int64(pe) {
					t = int(q)
				}
			case off < 0: // the -off-th non-NULL value before the current row
				if k := pre[i] + int(off); k >= pre[ps] {
					t = pos[k]
				}
			default:
				if k := pre[i+1] + int(off) - 1; k < pre[pe] {
					t = pos[k]
				}
			}
			v := nullValue(c.typ)
			if t >= 0 {
				v = args[0][ws.perm[t]]
			} else if len(args) > 2 {
				v = args[2][row]
			}
			if !v.Null && v.T != c.typ {
				cv, err := Coerce(v, c.typ)
				if err != nil {
					return err
				}
				v = cv
			}
			vals[row] = v
		}
	}
	return nil
}

// frame returns the frame of every position as [lo[i], hi[i]) (lo <= hi).
// Without a frame clause it is the SQL default, RANGE BETWEEN UNBOUNDED
// PRECEDING AND CURRENT ROW: the partition up to the current row's last
// peer, or the whole partition without ORDER BY.
func (ws *windowSet) frame(env *evalEnv, w *WindowFunc) ([]int, []int, error) {
	fr := w.Over.Frame
	if fr == nil {
		fr = &WindowFrame{Unit: FrameRange, Start: FrameBound{Kind: BoundUnboundedPreceding}, End: FrameBound{Kind: BoundCurrentRow}}
	}
	startOff, err := ws.frameOffset(env, fr, fr.Start, "starting")
	if err != nil {
		return nil, nil, err
	}
	endOff, err := ws.frameOffset(env, fr, fr.End, "ending")
	if err != nil {
		return nil, nil, err
	}
	n := len(ws.perm)
	lo, hi := make([]int, n), make([]int, n)
	for p := 0; p+1 < len(ws.parts); p++ {
		ps, pe := ws.parts[p], ws.parts[p+1]
		for i := ps; i < pe; i++ {
			lo[i] = ws.bound(fr.Unit, fr.Start, startOff, i, ps, pe, true)
			hi[i] = max(ws.bound(fr.Unit, fr.End, endOff, i, ps, pe, false), lo[i])
		}
	}
	return lo, hi, nil
}

// frameOffset evaluates the offset of an n PRECEDING / n FOLLOWING bound.
func (ws *windowSet) frameOffset(env *evalEnv, fr *WindowFrame, b FrameBound, which string) (Value, error) {
	if b.Offset == nil {
		return Value{}, nil
	}
	v, err := env.eval(b.Offset)
	if err != nil {
		return Value{}, err
	}
	if v.Null {
		return Value{}, fmt.Errorf("frame %s offset must not be null", which)
	}
	negative := false
	if fr.Unit != FrameRange {
		if v, err = Coerce(v, typeInt64); err != nil {
			return Value{}, fmt.Errorf("%s frame offsets must be integers: %v", frameUnitNames[fr.Unit], err)
		}
		negative = v.I < 0
	} else {
		var key ColType
		for _, k := range ws.okeys {
			if !k.Null {
				key = k.T
				break
			}
		}
		if !rangeOffsetOK(key, v.T) || key.Kind == KindNull && !(v.T.Kind.isNumeric() || v.T.Kind == KindInterval) {
			return Value{}, fmt.Errorf("RANGE with offset PRECEDING/FOLLOWING is not supported for ORDER BY type %s and offset type %s",
				key.SQLName(), v.T.SQLName())
		}
		switch {
		case v.T.Kind == KindInterval && key.Kind == KindTime:
			// A time moves by the interval's time part only (see timeOffset).
			negative = v.I < 0
		case v.T.Kind == KindInterval:
			negative = intervalTotal(v).Sign() < 0
		default:
			c, _ := compareValues(v, intValue(typeInt64, 0))
			negative = c < 0
		}
	}
	if negative {
		return Value{}, fmt.Errorf("frame %s offset must not be negative", which)
	}
	return v, nil
}

// bound returns the position where a frame starts (start) or one past where
// it ends, for the row at position i of partition [ps, pe).
func (ws *windowSet) bound(unit FrameUnit, b FrameBound, off Value, i, ps, pe int, start bool) int {
	switch b.Kind {
	case BoundUnboundedPreceding:
		return ps
	case BoundUnboundedFollowing:
		return pe
	case BoundCurrentRow:
		switch {
		case unit == FrameRows && start:
			return i
		case unit == FrameRows:
			return i + 1
		case start: // RANGE / GROUPS: the current row's peers
			return ws.groupStart[ws.grp[i]]
		}
		return ws.groupStart[ws.grp[i]+1]
	}
	following := b.Kind == BoundFollowing
	switch unit {
	case FrameRows:
		n := min(off.I, int64(pe-ps+1))
		k := int64(i)
		if following {
			k += n
		} else {
			k -= n
		}
		if !start {
			k++
		}
		return int(max(min(k, int64(pe)), int64(ps)))
	case FrameGroups:
		g0, g1 := ws.grp[ps], ws.grp[pe-1]
		n := min(off.I, int64(g1-g0+2))
		t := int64(ws.grp[i])
		if following {
			t += n
		} else {
			t -= n
		}
		switch {
		case t < int64(g0):
			return ps
		case t > int64(g1):
			return pe
		case start:
			return ws.groupStart[t]
		}
		return ws.groupStart[t+1]
	}
	return ws.rangeBound(off, i, ps, pe, following, start)
}

// rangeBound finds a RANGE n PRECEDING / FOLLOWING bound by binary search
// over the partition's sorted keys. As in PostgreSQL, the NULL keys are one
// peer group: a row with a NULL key has its peers as the frame, and other
// rows' frames never reach the NULLs.
func (ws *windowSet) rangeBound(off Value, i, ps, pe int, following, start bool) int {
	cur := ws.okey(ws.perm[i])[0]
	if cur.Null {
		if start {
			return ws.groupStart[ws.grp[i]]
		}
		return ws.groupStart[ws.grp[i]+1]
	}
	s, e := ps, pe
	if ws.okey(ws.perm[ps])[0].Null {
		s = ws.groupStart[ws.grp[ps]+1]
	}
	if ws.okey(ws.perm[pe-1])[0].Null {
		e = ws.groupStart[ws.grp[pe-1]]
	}
	desc := ws.order[0].desc
	// PRECEDING moves towards smaller keys in ascending order, larger in
	// descending order.
	toward := 1 // direction of the target from cur
	if following == desc {
		toward = -1
	}
	op := "+"
	if toward < 0 {
		op = "-"
	}
	var target Value
	var err error
	if cur.T.Kind == KindTime {
		target, err = timeOffset(cur, off, toward)
	} else {
		target, err = binaryOp(op, cur, off)
	}
	beyond := 0 // the target overflowed: it is beyond every key
	if err != nil {
		beyond = toward
	}
	return s + sort.Search(e-s, func(j int) bool {
		var c int // key compared with the target
		switch beyond {
		case 1:
			c = -1
		case -1:
			c = 1
		default:
			c, _ = compareValues(ws.okey(ws.perm[s+j])[0], target)
		}
		if desc {
			c = -c
		}
		if start {
			return c >= 0 // the first row not before the target
		}
		return c > 0 // the first row after it
	})
}

// timeOffset moves a TIME key by dir times an interval for a RANGE bound.
// As in PostgreSQL, only the interval's time part counts and the result does
// not wrap around midnight (time ± interval does), so a frame never reaches
// past the start or end of the day.
func timeOffset(t, off Value, dir int) (Value, error) {
	ns := timeOfDay(t)
	d := off.I
	if dir < 0 {
		d = -d
	}
	s := ns + d
	if (d > 0 && s < ns) || (d < 0 && s > ns) {
		return Value{}, fmt.Errorf("time out of range")
	}
	return intValue(timeType(arrow.Nanosecond), s), nil
}

// ---- frame aggregates ----

// frameAgg is an aggregate over a frame that rows enter at the end and leave
// from the start (first in, first out).
type frameAgg struct {
	name string
	star bool
	typ  ColType
	in   []Value // argument of each row
	sep  []Value // STRING_AGG / LISTAGG: separator of each row
	// count of non-NULL arguments (rows, for COUNT(*)) in the frame.
	count int64
	trues int64     // BOOL_OR / BOOL_AND: true arguments
	sum   *exactSum // SUM and AVG, and (with squares) the variances
	// MIN / MAX: a monotonic deque of (position, value). ANY_VALUE,
	// STRING_AGG and LISTAGG: the frame's non-NULL arguments, in order.
	deque []dequeEntry
	head  int
}

type dequeEntry struct {
	pos, row int
	v        Value
}

func newFrameAgg(f *Func, star bool, typ ColType, in []Value) *frameAgg {
	a := &frameAgg{name: f.Name, star: star, typ: typ, in: in}
	switch {
	case statFuncs[f.Name]:
		a.sum = &exactSum{squares: true}
	case f.Name == "SUM" || f.Name == "AVG":
		a.sum = &exactSum{}
	}
	return a
}

func (a *frameAgg) reset() {
	a.count, a.trues, a.head = 0, 0, 0
	a.deque = a.deque[:0]
	if a.sum != nil {
		a.sum.reset()
	}
}

// add adds the row at position pos.
func (a *frameAgg) add(row, pos int) {
	if a.star {
		a.count++
		return
	}
	v := a.in[row]
	if v.Null {
		return
	}
	a.count++
	switch a.name {
	case "MIN", "MAX":
		for len(a.deque) > a.head {
			c, _ := compareValues(a.deque[len(a.deque)-1].v, v)
			if (a.name == "MIN" && c < 0) || (a.name == "MAX" && c > 0) {
				break
			}
			a.deque = a.deque[:len(a.deque)-1]
		}
		a.deque = append(a.deque, dequeEntry{pos: pos, v: v})
	case "BOOL_OR", "BOOL_AND", "EVERY":
		if v.I != 0 {
			a.trues++
		}
	case "ANY_VALUE", "STRING_AGG", "LISTAGG":
		a.deque = append(a.deque, dequeEntry{pos: pos, row: row, v: v})
	default:
		if a.sum != nil {
			a.sum.add(v)
		}
	}
}

// remove removes the row at position pos, the oldest one in the frame.
func (a *frameAgg) remove(row, pos int) {
	if a.star {
		a.count--
		return
	}
	v := a.in[row]
	if v.Null {
		return
	}
	a.count--
	switch a.name {
	case "MIN", "MAX", "ANY_VALUE", "STRING_AGG", "LISTAGG":
		if a.head < len(a.deque) && a.deque[a.head].pos == pos {
			a.head++
			if a.head == len(a.deque) {
				a.deque, a.head = a.deque[:0], 0
			}
		}
	case "BOOL_OR", "BOOL_AND", "EVERY":
		if v.I != 0 {
			a.trues--
		}
	default:
		if a.sum != nil {
			a.sum.remove(v)
		}
	}
}

func (a *frameAgg) result() (Value, error) {
	switch a.name {
	case "COUNT":
		return intValue(typeInt64, a.count), nil
	case "MIN", "MAX", "ANY_VALUE":
		if a.head == len(a.deque) {
			return nullValue(a.typ), nil
		}
		return a.deque[a.head].v, nil
	case "STRING_AGG", "LISTAGG":
		if a.head == len(a.deque) {
			return nullValue(typeString), nil
		}
		var b strings.Builder
		for k, e := range a.deque[a.head:] {
			if k > 0 && a.sep != nil && !a.sep[e.row].Null {
				b.WriteString(a.sep[e.row].Text())
			}
			b.WriteString(e.v.Text())
		}
		return stringValue(b.String()), nil
	case "BOOL_OR", "BOOL_AND", "EVERY":
		return boolResult(a.name != "BOOL_OR", a.count, a.trues), nil
	}
	if statFuncs[a.name] {
		return a.sum.varianceValue(a.name), nil
	}
	if a.count == 0 {
		return nullValue(a.typ), nil
	}
	if a.name == "AVG" {
		return floatValue(typeFloat64, a.sum.mean()), nil
	}
	return a.sum.sumValue(a.typ)
}
