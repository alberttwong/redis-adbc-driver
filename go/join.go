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

// Joins.
//
// RediSearch cannot join, so joins run in the driver, with as much work as
// possible pushed into each table's own index query:
//
//   - A WHERE predicate on one table is pushed into that table's scan, unless
//     the table is on the NULL-supplying side of an outer join (pushing it
//     below the join would change the result).
//   - An ON predicate on the joined-in table of an INNER or LEFT join is
//     pushed into that table's scan.
//   - WHERE predicates linking several inner-joined tables become join
//     conditions, so `FROM a, b WHERE a.x = b.y` is a hash join, not a cross
//     product.
//   - Equality conditions drive a hash join. When the joined-in table's key
//     column is indexed and the rows joined so far have at most
//     maxUnionTerms distinct keys, only the matching rows are fetched, with an
//     index union query (an index lookup join).
//
// Joined rows key their columns as "alias.column". The joined relation is an
// in-memory relation, so WHERE, GROUP BY, HAVING, ORDER BY and LIMIT then run
// through the regular driver-side paths.

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

type joinItem struct {
	alias    string
	base     *tableMeta // table, CTE or derived table
	prefix   string     // alias + "."
	kind     string     // "" for the first item; INNER, LEFT, RIGHT, FULL, CROSS
	on       Expr       // bound join condition (may be nil)
	nullable bool       // on the NULL-supplying side of an outer join

	pushed []Expr // predicates on this table alone, applied in its scan
	extra  []Expr // WHERE predicates attached as join conditions here
}

type joinPlan struct {
	items []*joinItem
}

// planJoin resolves the FROM items of a query with joins. It returns the
// joined relation (column names "alias.column") and the scope relations.
func (e *executor) planJoin(ctx context.Context, sel *SelectStmt) (*tableMeta, []relation, *joinPlan, error) {
	jp := &joinPlan{}
	add := func(kind string, table *TableName, sub *SelectStmt, alias string) error {
		meta, name, err := e.resolveFromItem(ctx, table, sub, alias)
		if err != nil {
			return err
		}
		if meta.view != nil {
			// Joins read each item fully; compute the view now.
			rows, err := e.runView(ctx, meta.view, nil, nil, e.params)
			if err != nil {
				return err
			}
			m := *meta
			m.view, m.mem = nil, rows
			meta = &m
		}
		if name == "" {
			return errorf(adbc.StatusInvalidArgument, "a subquery in FROM must have an alias")
		}
		for _, it := range jp.items {
			if strings.EqualFold(it.alias, name) {
				return errorf(adbc.StatusInvalidArgument, "table name %q specified more than once; use aliases", name)
			}
		}
		jp.items = append(jp.items, &joinItem{alias: name, base: meta, prefix: name + ".", kind: kind})
		return nil
	}
	if err := add("", sel.From, sel.FromSelect, sel.FromAlias); err != nil {
		return nil, nil, nil, err
	}
	for _, jc := range sel.Joins {
		if err := add(jc.Kind, jc.Table, jc.Select, jc.Alias); err != nil {
			return nil, nil, nil, err
		}
	}
	// Nullability: LEFT makes the new item nullable, RIGHT the items before
	// it, FULL both.
	for i, it := range jp.items {
		switch it.kind {
		case "LEFT":
			it.nullable = true
		case "RIGHT":
			for _, prev := range jp.items[:i] {
				prev.nullable = true
			}
		case "FULL":
			it.nullable = true
			for _, prev := range jp.items[:i] {
				prev.nullable = true
			}
		}
	}
	joined := &tableMeta{Name: "join", isMem: true, join: jp}
	rels := make([]relation, len(jp.items))
	for i, it := range jp.items {
		for _, c := range it.base.Columns {
			joined.Columns = append(joined.Columns, columnMeta{
				Name: it.prefix + c.Name, Field: c.Name, Type: c.Type, Nullable: true,
			})
		}
		rels[i] = relation{name: it.alias, meta: it.base, prefix: it.prefix}
	}
	return joined, rels, jp, nil
}

// bindJoin binds the ON / USING conditions in the query's scope (called with
// the join scope pushed).
func (e *executor) bindJoin(ctx context.Context, sel *SelectStmt, jp *joinPlan) error {
	for i, jc := range sel.Joins {
		it := jp.items[i+1]
		cond := jc.On
		if len(jc.Using) > 0 {
			var parts []Expr
			for _, col := range jc.Using {
				left := ""
				for _, prev := range jp.items[:i+1] {
					if _, ok := prev.base.column(col); ok {
						if left != "" {
							return errorf(adbc.StatusInvalidArgument, "USING column %q is ambiguous", col)
						}
						left = prev.alias
					}
				}
				if left == "" {
					return errorf(adbc.StatusInvalidArgument, "USING column %q does not exist in the left side of the join", col)
				}
				parts = append(parts, &Binary{Op: "=",
					L: &ColumnRef{Qualifier: left, Name: col},
					R: &ColumnRef{Qualifier: it.alias, Name: col}})
			}
			cond = andAll(parts)
		}
		if cond == nil {
			it.on = nil
			continue
		}
		if err := e.bind(ctx, cond); err != nil {
			return err
		}
		if isAggregate(cond) {
			return errorf(adbc.StatusInvalidArgument, "aggregates are not allowed in JOIN conditions")
		}
		it.on = cond
	}
	return nil
}

// itemsOf returns the indexes of the join items an expression reads, and
// whether it can be moved (it has no correlated subquery, whose references
// would be invisible here, and no volatile function, which would be
// evaluated again on the joined rows).
func (jp *joinPlan) itemsOf(expr Expr) ([]int, bool) {
	movable := true
	refs := map[string]bool{}
	walkExpr(expr, func(x Expr) {
		switch v := x.(type) {
		case *ColumnRef:
			if v.Outer == 0 {
				refs[v.Name] = true
			}
		case *Subquery:
			if v.correlated {
				movable = false
			}
		case *Func:
			if volatileFuncs[v.Name] {
				movable = false
			}
		}
	})
	var out []int
	for i, it := range jp.items {
		for name := range refs {
			if strings.HasPrefix(name, it.prefix) {
				out = append(out, i)
				break
			}
		}
	}
	return out, movable
}

// planPushdown distributes WHERE and ON predicates (see the file comment).
// WHERE is still evaluated in full on the joined rows, so a pushed predicate
// only has to be exact, not complete.
func (jp *joinPlan) planPushdown(where Expr) {
	for _, it := range jp.items {
		it.pushed, it.extra = nil, nil
	}
	for i, it := range jp.items {
		if it.on == nil || !(it.kind == "INNER" || it.kind == "LEFT") {
			continue
		}
		for _, c := range conjuncts(it.on, nil) {
			if idx, ok := jp.itemsOf(c); ok && len(idx) == 1 && idx[0] == i {
				it.pushed = append(it.pushed, c)
			}
		}
	}
	if where == nil {
		return
	}
	for _, c := range conjuncts(where, nil) {
		idx, ok := jp.itemsOf(c)
		if !ok || len(idx) == 0 {
			continue
		}
		nullable := false
		for _, i := range idx {
			nullable = nullable || jp.items[i].nullable
		}
		if nullable {
			continue
		}
		if len(idx) == 1 {
			jp.items[idx[0]].pushed = append(jp.items[idx[0]].pushed, c)
		} else {
			last := idx[len(idx)-1]
			jp.items[last].extra = append(jp.items[last].extra, c)
		}
	}
}

// view returns the relation's columns renamed alias.column (reading the
// original HASH fields), for scanning it as part of a join.
func (it *joinItem) view() *tableMeta {
	v := *it.base
	v.Columns = make([]columnMeta, len(it.base.Columns))
	for i, c := range it.base.Columns {
		c.Field = c.field()
		c.Name = it.prefix + c.Name
		v.Columns[i] = c
	}
	if it.base.isMem {
		v.mem = make([]map[string]Value, len(it.base.mem))
		for r, row := range it.base.mem {
			m := make(map[string]Value, len(row))
			for k, val := range row {
				m[it.prefix+k] = val
			}
			v.mem[r] = m
		}
	}
	return &v
}

// joinKey normalizes a value for hash-join equality: values that compare
// equal (e.g. 1, 1.0 and 1.00) get the same key. NULLs never match.
func joinKey(v Value) (string, bool) {
	if v.Null {
		return "", false
	}
	switch k := v.T.Kind; {
	case k.isInteger() || k == KindBool:
		return "n" + strconv.FormatInt(v.I, 10), true
	case k == KindDecimal:
		s := formatDecimal(v.D, v.T.Scale)
		if strings.Contains(s, ".") {
			s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
		}
		if s == "-0" {
			s = "0"
		}
		return "n" + s, true
	case k.isFloat():
		if v.F == math.Trunc(v.F) && math.Abs(v.F) < 1e15 {
			return "n" + strconv.FormatInt(int64(v.F), 10), true
		}
		if math.IsNaN(v.F) {
			return "", false
		}
		// Shortest decimal form, matching how NUMERIC values render.
		return "n" + strconv.FormatFloat(v.F, 'f', -1, 64), true
	case k == KindString || k == KindBinary:
		return "s" + v.S, true
	case k == KindInterval:
		// Equal lengths are equal intervals ('1 day' = '24 hours').
		return "i" + intervalTotal(v).String(), true
	case k == KindTimestamp || k == KindTime:
		// Seconds plus nanoseconds, so different units compare equal.
		per := unitsPerSecond[v.T.Unit]
		sec := floorDiv(v.I, per)
		frac := (v.I - sec*per) * (1_000_000_000 / per)
		return k.String()[:2] + strconv.FormatInt(sec, 10) + "." + strconv.FormatInt(frac, 10), true
	}
	return v.T.Kind.String() + v.Text(), true
}

type equiPair struct {
	left, right Expr
}

// joinStep is one step of a join: scan item and combine it with the rows
// joined so far using conds.
type joinStep struct {
	item  int
	kind  string
	conds []Expr
}

// joinOrder returns the steps of the join. Outer joins keep the written
// order. Inner joins (INNER, CROSS, comma) start from the table whose pushed
// filters match the fewest rows (counted by its index), then repeatedly add
// the smallest table connected to the ones already joined; each condition is
// applied at the first step where all its tables are available.
func (e *executor) joinOrder(ctx context.Context, jp *joinPlan, wps []wherePlan) ([]joinStep, error) {
	inner := true
	for _, it := range jp.items {
		if it.kind != "" && it.kind != "INNER" && it.kind != "CROSS" {
			inner = false
		}
	}
	condsOf := func(it *joinItem) []Expr {
		var c []Expr
		if it.on != nil {
			c = conjuncts(it.on, nil)
		}
		return append(c, it.extra...)
	}
	if !inner {
		steps := make([]joinStep, len(jp.items))
		for i, it := range jp.items {
			steps[i] = joinStep{item: i, kind: it.kind, conds: condsOf(it)}
		}
		return steps, nil
	}
	counts := make([]int64, len(jp.items))
	for i, it := range jp.items {
		switch {
		case it.base.isMem:
			counts[i] = int64(len(it.base.mem))
		case wps[i].keys != nil:
			counts[i] = int64(len(wps[i].keys))
		default:
			n, err := e.store.countMatches(ctx, it.base.index(), wps[i].query)
			if err != nil {
				return nil, err
			}
			counts[i] = n
		}
	}
	type poolCond struct {
		expr  Expr
		items []int
		used  bool
	}
	var pool []*poolCond
	for _, it := range jp.items {
		for _, c := range condsOf(it) {
			idx, _ := jp.itemsOf(c)
			pool = append(pool, &poolCond{expr: c, items: idx})
		}
	}
	chosen := map[int]bool{}
	var steps []joinStep
	for len(steps) < len(jp.items) {
		best, bestConnected := -1, false
		for i := range jp.items {
			if chosen[i] {
				continue
			}
			connected := false
			for _, pc := range pool {
				if slices.Contains(pc.items, i) && len(pc.items) > 1 {
					others := true
					for _, x := range pc.items {
						if x != i && !chosen[x] {
							others = false
						}
					}
					connected = connected || others
				}
			}
			better := best < 0 ||
				(connected && !bestConnected) ||
				(connected == bestConnected && counts[i] < counts[best])
			if len(steps) == 0 {
				better = best < 0 || counts[i] < counts[best]
			}
			if better {
				best, bestConnected = i, connected
			}
		}
		chosen[best] = true
		st := joinStep{item: best, kind: "INNER"}
		if len(steps) == 0 {
			st.kind = ""
		}
		for _, pc := range pool {
			if pc.used {
				continue
			}
			ready := true
			for _, x := range pc.items {
				if !chosen[x] {
					ready = false
				}
			}
			// Conditions without column references wait for the first real join.
			if ready && (len(pc.items) > 0 || len(steps) > 0) {
				st.conds = append(st.conds, pc.expr)
				pc.used = true
			}
		}
		steps = append(steps, st)
	}
	return steps, nil
}

// runJoin executes the join, returning rows keyed alias.column. need lists
// the (prefixed) columns the rest of the query reads.
func (e *executor) runJoin(ctx context.Context, jp *joinPlan, need map[string]bool, params []Value) ([]map[string]Value, error) {
	// Every column read by join conditions must be fetched too.
	allNeed := map[string]bool{}
	for k := range need {
		allNeed[k] = true
	}
	for _, it := range jp.items {
		if it.on != nil {
			columnRefs(it.on, allNeed)
		}
		for _, x := range it.extra {
			columnRefs(x, allNeed)
		}
	}
	types := map[string]ColType{}
	for _, it := range jp.items {
		for _, c := range it.base.Columns {
			types[it.prefix+c.Name] = c.Type
		}
	}
	env := e.newEnv(ctx, types, params)

	// Each item's own filters, pushed into its scan.
	views := make([]*tableMeta, len(jp.items))
	wps := make([]wherePlan, len(jp.items))
	for i, it := range jp.items {
		views[i] = it.view()
		wp, err := e.planWhere(ctx, andAll(it.pushed), views[i], params)
		if err != nil {
			return nil, err
		}
		wps[i] = wp
	}
	steps, err := e.joinOrder(ctx, jp, wps)
	if err != nil {
		return nil, err
	}

	var left []map[string]Value
	avail := map[int]bool{}
	for k, st := range steps {
		i, it, view, wp := st.item, jp.items[st.item], views[st.item], wps[st.item]

		// Split equality conditions (joined so far ⋈ this item) from the rest.
		var pairs []equiPair
		var filters []Expr
		for _, c := range st.conds {
			if b, ok := c.(*Binary); ok && b.Op == "=" && k > 0 {
				li, lok := jp.itemsOf(b.L)
				ri, rok := jp.itemsOf(b.R)
				if lok && rok && len(li) > 0 && len(ri) > 0 {
					switch {
					case within(li, avail) && onlyItem(ri, i):
						pairs = append(pairs, equiPair{left: b.L, right: b.R})
						continue
					case within(ri, avail) && onlyItem(li, i):
						pairs = append(pairs, equiPair{left: b.R, right: b.L})
						continue
					}
				}
			}
			filters = append(filters, c)
		}
		avail[i] = true

		itemNeed := map[string]bool{}
		for c := range allNeed {
			if strings.HasPrefix(c, it.prefix) {
				itemNeed[c] = true
			}
		}
		innerLike := st.kind == "INNER" || st.kind == "LEFT" || st.kind == "CROSS"
		if k > 0 && len(left) == 0 && innerLike {
			// Nothing on the left to join or extend; a later RIGHT/FULL
			// join may still add rows.
			left = nil
			continue
		}
		if k > 0 && innerLike && wp.keys == nil && !view.isMem {
			// Index lookup join: fetch only rows whose key matches the left.
			for _, p := range pairs {
				ref, ok := p.right.(*ColumnRef)
				if !ok {
					continue
				}
				cm, ok := view.column(ref.Name)
				if !ok || !cm.Indexed || !simpleName(cm.field()) {
					continue
				}
				seen := map[string]bool{}
				var values []Value
				for _, row := range left {
					env.row = row
					v, err := env.eval(p.left)
					if err != nil {
						return nil, invalidArg(err)
					}
					if key, ok := joinKey(v); ok && !seen[key] {
						seen[key] = true
						values = append(values, v)
					}
				}
				if len(values) > maxUnionTerms {
					continue
				}
				if q, ok := unionQuery(cm, values); ok {
					if wp.query == "*" {
						wp.query = q
					} else {
						wp.query += " " + q
					}
				}
				break
			}
		}
		_, right, err := e.scan(ctx, scanRequest{meta: view, where: wp, need: itemNeed}, params)
		if err != nil {
			return nil, err
		}
		if k == 0 {
			// Conditions on the first table alone that could not be pushed.
			left = right
			if len(st.conds) > 0 {
				if left, err = e.joinRows(env, "INNER", left, []map[string]Value{{}}, nil, st.conds); err != nil {
					return nil, err
				}
			}
			continue
		}
		left, err = e.joinRows(env, st.kind, left, right, pairs, filters)
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func within(idx []int, set map[int]bool) bool {
	for _, x := range idx {
		if !set[x] {
			return false
		}
	}
	return true
}

func onlyItem(idx []int, i int) bool { return len(idx) == 1 && idx[0] == i }

func mergeRows(a, b map[string]Value) map[string]Value {
	m := make(map[string]Value, len(a)+len(b))
	for k, v := range a {
		m[k] = v
	}
	for k, v := range b {
		m[k] = v
	}
	return m
}

// joinRows combines left and right rows with a hash join on pairs (or a
// nested loop when there are none), keeping pairs that also pass filters.
func (e *executor) joinRows(env *evalEnv, kind string, left, right []map[string]Value, pairs []equiPair, filters []Expr) ([]map[string]Value, error) {
	keyOf := func(row map[string]Value, side func(equiPair) Expr) (string, bool, error) {
		env.row = row
		var b strings.Builder
		for _, p := range pairs {
			v, err := env.eval(side(p))
			if err != nil {
				return "", false, invalidArg(err)
			}
			k, ok := joinKey(v)
			if !ok {
				return "", false, nil
			}
			b.WriteString(k)
			b.WriteByte(0)
		}
		return b.String(), true, nil
	}
	leftSide := func(p equiPair) Expr { return p.left }
	rightSide := func(p equiPair) Expr { return p.right }

	// Bucket the right rows by key.
	buckets := map[string][]int{}
	if len(pairs) > 0 {
		for j, r := range right {
			k, ok, err := keyOf(r, rightSide)
			if err != nil {
				return nil, err
			}
			if ok {
				buckets[k] = append(buckets[k], j)
			}
		}
	}
	matchedRight := make([]bool, len(right))
	var out []map[string]Value
	passes := func(row map[string]Value) (bool, error) {
		env.row = row
		for _, f := range filters {
			v, err := env.eval(f)
			if err != nil {
				return false, invalidArg(err)
			}
			if b, ok := truthy(v); !ok || !b {
				return false, nil
			}
		}
		return true, nil
	}
	for _, l := range left {
		var candidates []int
		if len(pairs) > 0 {
			k, ok, err := keyOf(l, leftSide)
			if err != nil {
				return nil, err
			}
			if ok {
				candidates = buckets[k]
			}
		} else {
			candidates = make([]int, len(right))
			for j := range right {
				candidates[j] = j
			}
		}
		matched := false
		for _, j := range candidates {
			row := mergeRows(l, right[j])
			ok, err := passes(row)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, row)
				matched = true
				matchedRight[j] = true
			}
		}
		if !matched && (kind == "LEFT" || kind == "FULL") {
			out = append(out, l)
		}
	}
	if kind == "RIGHT" || kind == "FULL" {
		for j, r := range right {
			if !matchedRight[j] {
				out = append(out, r)
			}
		}
	}
	return out, nil
}
