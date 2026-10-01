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

// Query execution for the hybrid index-row layout:
//
//   - Full-row lookup: WHERE __rowid = N bypasses the index and reads the row
//     HASH directly.
//   - Filter + fetch: FT.AGGREGATE evaluates the pushed-down WHERE clause,
//     ORDER BY and LIMIT entirely from the index (SORTABLE fields) and returns
//     only the matching keys (LOAD @__key); the rows are then read from their
//     HASHes with pipelined HMGET.
//   - Aggregation: GROUPBY/REDUCE run inside FT.AGGREGATE over the SORTABLE
//     fields without opening the HASHes, when the result is exact (see
//     OptionStringAggregatePushdown); otherwise the driver aggregates rows it
//     fetched from the HASHes.

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-adbc/go/adbc"
)

// exactDoubleLimit bounds integers whose double representation is exact.
const exactDoubleLimit = 1 << 53

// wherePlan is the part of a WHERE clause answered by the index plus the
// residual predicate evaluated by the driver on fetched rows.
type wherePlan struct {
	query    string
	residual Expr
	// keys is set for a direct lookup (WHERE __rowid = N): the rows are read
	// straight from their HASHes without consulting the index.
	keys []string
}

func conjuncts(e Expr, out []Expr) []Expr {
	if b, ok := e.(*Binary); ok && b.Op == "AND" {
		out = conjuncts(b.L, out)
		return conjuncts(b.R, out)
	}
	return append(out, e)
}

func isConstant(e Expr) bool {
	constant := true
	walkExpr(e, func(x Expr) {
		switch f := x.(type) {
		case *ColumnRef:
			// Outer references are fixed while this query runs.
			if f.Outer == 0 {
				constant = false
			}
		case *Func:
			// RANDOM() differs per row, so it is not computed once.
			if aggregateFuncs[f.Name] || volatileFuncs[f.Name] {
				constant = false
			}
		case *Subquery:
			if f.correlated {
				constant = false
			}
		case *WindowFunc:
			constant = false
		}
	})
	return constant
}

var flipOp = map[string]string{"=": "=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}

// escapeTag escapes a tag for a DIALECT 2 TAG query (see tagQueryable for
// the tags it can write). ASCII punctuation and whitespace are escaped; the
// bytes of other characters are written as they are, since an escaped one
// is no longer part of the tag.
func escapeTag(s string) string {
	if s == "" {
		return `""`
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' || c >= utf8.RuneSelf || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			b.WriteByte(c)
		} else {
			b.WriteByte('\\')
			b.WriteByte(c)
		}
	}
	return b.String()
}

// simpleName reports whether an attribute name can be used in query strings
// and expressions without escaping.
func simpleName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r != '_' && !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

// comparison splits `col op const` (in either order) into its parts. The
// column is one of this query's: an outer reference is itself a constant.
func comparison(c Expr) (*ColumnRef, string, Expr, bool) {
	b, ok := c.(*Binary)
	if !ok || flipOp[b.Op] == "" {
		return nil, "", nil, false
	}
	if col, ok := b.L.(*ColumnRef); ok && col.Outer == 0 && isConstant(b.R) {
		return col, b.Op, b.R, true
	}
	if col, ok := b.R.(*ColumnRef); ok && col.Outer == 0 && isConstant(b.L) {
		return col, flipOp[b.Op], b.L, true
	}
	return nil, "", nil, false
}

// planWhere pushes down every predicate the index can answer: ranges on
// NUMERIC columns and equality on TAG (string) columns. Predicates the index
// may answer inexactly (double rounding of large integers, strings the TAG
// index doesn't hold exactly) are pushed down inclusively and also kept in
// the residual, as are predicates widened to the rows that read a column's
// missing value (widenMissing).
func (e *executor) planWhere(ctx context.Context, where Expr, meta *tableMeta, params []Value) (wherePlan, error) {
	wp := wherePlan{query: "*"}
	if where == nil {
		return wp, nil
	}
	if meta.isMem {
		// In-memory relations have no index: filter every row.
		wp.residual = where
		return wp, nil
	}
	env := e.newEnv(ctx, nil, params)
	parts := conjuncts(where, nil)
	var residual []Expr
	addResidual := func(e Expr) { residual = append(residual, e) }

	// Full-row lookup by row id.
	for i, c := range parts {
		col, op, other, ok := comparison(c)
		if !ok || op != "=" || col.Name != rowIDField {
			continue
		}
		v, err := env.eval(other)
		if err != nil {
			return wp, invalidArg(err)
		}
		if v, err = Coerce(v, typeInt64); err != nil || v.Null {
			continue
		}
		wp.keys = []string{meta.prefix() + strconv.FormatInt(v.I, 10)}
		for j, rest := range parts {
			if j != i {
				addResidual(rest)
			}
		}
		wp.residual = andAll(residual)
		return wp, nil
	}

	var terms []string
	for _, c := range parts {
		// col IN (SELECT …) / col IN (a, b, …) / col = a OR col = b:
		// an index union query. The predicate is re-checked on fetched rows
		// (it also carries SQL's NULL semantics).
		if term, ok, err := e.unionTerm(ctx, c, meta, env); err != nil {
			return wp, err
		} else if ok {
			terms = append(terms, term)
			addResidual(c)
			continue
		}
		if term, ok := likePrefixTerm(c, meta, env); ok {
			// A prefix whose tags the index doesn't all expand is checked
			// on the rows alone (see tags.go).
			if e.prefixComplete(ctx, meta, term) {
				terms = append(terms, term)
			}
			addResidual(c) // re-checked exactly on fetched rows
			continue
		}
		colRef, op, other, ok := comparison(c)
		if !ok {
			addResidual(c)
			continue
		}
		col, ok := meta.column(colRef.Name)
		if !ok || !col.Indexed || !simpleName(col.field()) {
			addResidual(c)
			continue
		}
		cv, err := env.eval(other)
		if err != nil {
			return wp, invalidArg(err)
		}
		if cv.Null {
			addResidual(c)
			continue
		}
		// push adds the index term for c; recheck also keeps c in the
		// residual, as does widening the term to rows that read the
		// column's missing value.
		push := func(term string, recheck bool) {
			term, widened := widenMissing(term, col, func(m Value) bool {
				r, err := binaryOp(op, m, cv)
				b, ok := truthy(r)
				return err != nil || (ok && b)
			})
			if recheck || widened {
				addResidual(c)
			}
			terms = append(terms, term)
		}
		ct := col.Type
		field := "@" + col.field()
		switch {
		case ct.Kind.indexedAsNumeric():
			v, err := Coerce(cv, ct)
			if err != nil || (ct.Kind == KindBool && op != "=") {
				addResidual(c)
				continue
			}
			bound := encodeStored(v)
			exact := true
			switch ct.Kind {
			case KindInt64, KindTime, KindTimestamp:
				exact = v.I > -exactDoubleLimit && v.I < exactDoubleLimit
			case KindDecimal:
				exact = ct.Precision <= 15
			}
			// A constant that doesn't fit the column type (1.5 for an
			// integer, 1.249 for NUMERIC(6,2), a timestamp with a time of
			// day for a DATE) was rounded to a neighbouring value. No stored
			// value lies strictly between the two, so inclusive bounds at
			// the rounded value still cover every match, and the residual
			// makes the comparison exact.
			if cmp, ok := compareValues(v, cv); !ok || cmp != 0 {
				exact = false
			}
			lo, hi := "-inf", "+inf"
			switch op {
			case "=":
				lo, hi = bound, bound
			case "<":
				hi = "(" + bound
			case "<=":
				hi = bound
			case ">":
				lo = "(" + bound
			case ">=":
				lo = bound
			}
			if !exact {
				lo, hi = strings.TrimPrefix(lo, "("), strings.TrimPrefix(hi, "(")
			}
			push(fmt.Sprintf("%s:[%s %s]", field, lo, hi), !exact)
		case ct.Kind == KindString && op == "=":
			v, err := Coerce(cv, ct)
			if err != nil {
				addResidual(c)
				continue
			}
			// Only push string constants: `s = 1` is an error, which the
			// residual reports, not a match for the row with '1'.
			if cv.T.Kind != KindString {
				addResidual(c)
				continue
			}
			// The tag of 'ab ' is 'ab' (see tags.go).
			lit, exact, ok := tagLookup(col, v.S)
			if !ok {
				addResidual(c)
				continue
			}
			push(fmt.Sprintf("%s:{%s}", field, lit), !exact)
		default:
			addResidual(c)
		}
	}
	if len(terms) > 0 {
		wp.query = strings.Join(terms, " ")
	}
	wp.residual = andAll(residual)
	return wp, nil
}

func andAll(exprs []Expr) Expr {
	var out Expr
	for _, e := range exprs {
		if out == nil {
			out = e
		} else {
			out = &Binary{Op: "AND", L: out, R: e}
		}
	}
	return out
}

// decodeRow converts a fetched HASH into values keyed by column name.
func decodeRow(meta *tableMeta, row aggRow, only map[string]bool) (map[string]Value, error) {
	out := make(map[string]Value, len(only)+1)
	if raw, ok := row[rowIDField]; ok {
		v, err := decodeStored(raw, typeInt64)
		if err != nil {
			return nil, err
		}
		out[rowIDField] = v
	}
	for _, c := range meta.Columns {
		if only != nil && !only[c.Name] {
			continue
		}
		raw, ok := row[c.field()]
		if !ok {
			v, err := c.missingValue(row)
			if err != nil {
				return nil, errorf(adbc.StatusInternal, "column %q: %v", c.Name, err)
			}
			out[c.Name] = v
			continue
		}
		v, err := decodeStored(raw, c.Type)
		if err != nil {
			return nil, errorf(adbc.StatusInternal, "column %q: %v", c.Name, err)
		}
		out[c.Name] = v
	}
	return out, nil
}

// scanRequest describes an index lookup followed by a row fetch.
type scanRequest struct {
	meta   *tableMeta
	where  wherePlan
	sortBy []sortKey
	limit  *[2]int64
	need   map[string]bool // columns to read from the row HASHes
}

// scan finds the matching rows through the index (or directly by key), reads
// the needed columns, and applies the residual predicate. Rows found through
// the index are read by the FT.AGGREGATE pipeline itself, a cursor page at a
// time, rather than with one HMGET per row.
func (e *executor) scan(ctx context.Context, req scanRequest, params []Value) ([]string, []map[string]Value, error) {
	meta := req.meta
	if meta.isMem {
		return e.scanMem(ctx, req, params)
	}
	need := map[string]bool{}
	for k := range req.need {
		need[k] = true
	}
	if req.where.residual != nil {
		columnRefs(req.where.residual, need)
	}
	var fields []string
	exact := true
	for _, c := range meta.Columns {
		if need[c.Name] {
			fields = append(fields, c.field())
			exact = exact && loadsExactly(c)
			if c.MissingThrough > 0 {
				fields = append(fields, nullMarker(c.field()))
			}
		}
	}
	keys := req.where.keys
	var fetched []aggRow
	if keys != nil {
		var err error
		if fetched, err = e.store.fetchRows(ctx, keys, fields); err != nil {
			return nil, nil, err
		}
	} else {
		ar := &aggRequest{
			index:  meta.index(),
			query:  req.where.query,
			sortBy: req.sortBy,
			limit:  req.limit,
		}
		if exact {
			ar.load = append([]string{"__key", rowIDField}, fields...)
			ar.width = len(ar.load)
		} else {
			ar.load, ar.loadAll = []string{"__key"}, true
			ar.width = len(meta.Columns) + 2
		}
		var err error
		if fetched, err = e.store.aggregate(ctx, ar); err != nil {
			return nil, nil, err
		}
		keys = make([]string, len(fetched))
		for i, r := range fetched {
			if _, ok := r[rowIDField]; !ok {
				fetched[i] = nil // deleted after the index matched it
				continue
			}
			keys[i] = r["__key"]
		}
	}
	env := e.newEnv(ctx, meta.types(), params)
	outKeys := make([]string, 0, len(keys))
	rows := make([]map[string]Value, 0, len(keys))
	for i, raw := range fetched {
		if raw == nil {
			continue
		}
		vals, err := decodeRow(meta, raw, need)
		if err != nil {
			return nil, nil, err
		}
		if req.where.residual != nil {
			env.row = vals
			ok, err := env.eval(req.where.residual)
			if err != nil {
				return nil, nil, invalidArg(err)
			}
			if b, valid := truthy(ok); !valid || !b {
				continue
			}
		}
		outKeys = append(outKeys, keys[i])
		rows = append(rows, vals)
	}
	return outKeys, rows, nil
}

// loadsExactly reports whether LOAD @field returns a column's stored value.
// LOAD reads SORTABLE attributes from the index's sorting vector, where
// numbers are doubles printed with 12 significant digits: strings (stored
// UNF) and integers below 2^53 survive, other numbers must be read from the
// HASH with LOAD *, as must strings cut at a NUL byte (see tags.go).
// Unindexed fields are always read from the HASH.
func loadsExactly(c columnMeta) bool {
	if !simpleName(c.field()) {
		return false
	}
	if !c.Indexed {
		return true
	}
	switch c.Type.Kind {
	case KindString:
		return c.sortsExactly()
	case KindBool, KindInt16, KindInt32, KindDate, KindTime:
		return true
	}
	return false
}

// scanMem filters an in-memory relation.
func (e *executor) scanMem(ctx context.Context, req scanRequest, params []Value) ([]string, []map[string]Value, error) {
	source := req.meta.mem
	if req.meta.view != nil {
		need := maps.Clone(req.need)
		if need == nil {
			need = map[string]bool{}
		}
		if req.where.residual != nil {
			// The residual is re-checked on the view's rows below.
			columnRefs(req.where.residual, need)
		}
		rows, err := e.runView(ctx, req.meta.view, req.where.residual, need, params)
		if err != nil {
			return nil, nil, err
		}
		source = rows
	}
	if req.meta.join != nil {
		need := maps.Clone(req.need)
		if need == nil {
			need = map[string]bool{}
		}
		if req.where.residual != nil {
			columnRefs(req.where.residual, need)
		}
		joined, err := e.runJoin(ctx, req.meta.join, need, params)
		if err != nil {
			return nil, nil, err
		}
		source = joined
	}
	if req.where.residual == nil {
		return nil, source, nil
	}
	env := e.newEnv(ctx, req.meta.types(), params)
	var rows []map[string]Value
	for _, r := range source {
		env.row = r
		ok, err := env.eval(req.where.residual)
		if err != nil {
			return nil, nil, invalidArg(err)
		}
		if b, valid := truthy(ok); valid && b {
			rows = append(rows, r)
		}
	}
	return nil, rows, nil
}

// ---- SELECT ----

func (e *executor) runSelect(ctx context.Context, plan *selectPlan, params []Value) ([][]Value, error) {
	if plan.distinct {
		return e.runDistinct(ctx, plan, params)
	}
	if plan.setop != nil {
		return e.runSetOp(ctx, plan, params)
	}
	if plan.grouping != nil {
		return e.runGroupingSets(ctx, plan, params)
	}
	if plan.meta == nil {
		return e.selectWithoutTable(ctx, plan, params)
	}
	if plan.aggregate {
		return e.selectAggregate(ctx, plan, params)
	}
	meta := plan.meta
	wp, err := e.planWhere(ctx, plan.sel.Where, meta, params)
	if err != nil {
		return nil, err
	}
	req := scanRequest{meta: meta, where: wp, need: maps.Clone(plan.extraNeed)}
	if req.need == nil {
		req.need = map[string]bool{}
	}
	for _, it := range plan.items {
		columnRefs(it.expr, req.need)
	}
	if plan.qualify != nil {
		columnRefs(plan.qualify, req.need)
	}

	// ORDER BY on indexed columns sorts inside the index; anything else is
	// sorted by the driver after fetching.
	indexSort := wp.keys == nil && !meta.isMem
	for _, o := range plan.order {
		c, ok := o.expr.(*ColumnRef)
		if !ok || o.nullsFirst {
			// RediSearch always sorts missing values last.
			indexSort = false
			break
		}
		col, ok := meta.column(c.Name)
		// Rows that read a missing value have no index entry to sort on,
		// and strings cut at a NUL byte sort on what is left.
		if !ok || !col.Indexed || col.MissingThrough > 0 || !col.sortsExactly() {
			indexSort = false
			break
		}
		req.sortBy = append(req.sortBy, sortKey{field: col.field(), desc: o.desc})
	}
	if !indexSort {
		req.sortBy = nil
		for _, o := range plan.order {
			columnRefs(o.expr, req.need)
		}
	}
	if len(plan.order) == 0 && wp.keys == nil && !meta.isMem {
		// Implicit insertion order.
		req.sortBy = []sortKey{{field: rowIDField}}
		indexSort = true
	}
	sel := plan.sel
	// With window functions, LIMIT applies once they are computed (and
	// QUALIFY has filtered), so it never runs in the index.
	pushLimit := !plan.windowed() && wp.residual == nil && indexSort && wp.keys == nil && sel.Limit != nil
	if pushLimit {
		off := int64(0)
		if sel.Offset != nil {
			off = *sel.Offset
		}
		req.limit = &[2]int64{off, *sel.Limit}
	}

	_, rows, err := e.scan(ctx, req, params)
	if err != nil {
		return nil, err
	}
	env := e.newEnv(ctx, meta.types(), params)
	// idx lists the rows to return, in order.
	idx := make([]int, len(rows))
	for i := range idx {
		idx[i] = i
	}
	setRow := func(i int) { env.row, env.winRow = rows[i], i }
	if plan.windowed() {
		if idx, err = e.applyWindows(env, plan, len(rows), setRow); err != nil {
			return nil, err
		}
	}
	if !indexSort && len(plan.order) > 0 {
		keys := make([][]Value, len(rows))
		for _, i := range idx {
			setRow(i)
			for _, o := range plan.order {
				k, err := env.eval(o.expr)
				if err != nil {
					return nil, invalidArg(err)
				}
				keys[i] = append(keys[i], k)
			}
		}
		sort.SliceStable(idx, func(a, b int) bool { return lessKeys(keys[idx[a]], keys[idx[b]], plan.order) })
	}
	if !pushLimit {
		idx = applyLimit(idx, sel.Offset, sel.Limit)
	}
	out := make([][]Value, 0, len(idx))
	for _, i := range idx {
		setRow(i)
		row := make([]Value, len(plan.items))
		for i, it := range plan.items {
			v, err := env.eval(it.expr)
			if err != nil {
				return nil, invalidArg(err)
			}
			row[i] = v
		}
		out = append(out, row)
	}
	return out, nil
}

// runDistinct runs a SELECT DISTINCT without its LIMIT / OFFSET (so they
// never run in the index), removes duplicate rows (NULLs are equal, as in
// set operations), then applies them. For DISTINCT ON the keys are
// computed as hidden trailing columns and the first row of each key (in
// ORDER BY order) is kept.
func (e *executor) runDistinct(ctx context.Context, plan *selectPlan, params []Value) ([][]Value, error) {
	inner := *plan
	sel := *plan.sel
	sel.Limit, sel.Offset = nil, nil
	inner.sel, inner.distinct, inner.distinctOn = &sel, false, nil
	n := len(plan.items)
	if len(plan.distinctOn) > 0 {
		inner.items = append(slices.Clip(plan.items), plan.distinctOn...)
	}
	rows, err := e.runSelect(ctx, &inner, params)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := make([][]Value, 0, len(rows))
	for _, r := range rows {
		key := rowKey(r[n:])
		if len(plan.distinctOn) == 0 {
			key = rowKey(r)
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, r[:n])
	}
	return applyLimit(out, plan.sel.Offset, plan.sel.Limit), nil
}

// ---- aggregates ----

func (e *executor) selectAggregate(ctx context.Context, plan *selectPlan, params []Value) ([][]Value, error) {
	meta := plan.meta
	wp, err := e.planWhere(ctx, plan.sel.Where, meta, params)
	if err != nil {
		return nil, err
	}
	var aggs []*Func
	for _, it := range plan.items {
		collectAggregates(it.expr, &aggs)
	}
	for _, o := range plan.order {
		collectAggregates(o.expr, &aggs)
	}
	if plan.having != nil {
		collectAggregates(plan.having, &aggs)
	}
	if plan.qualify != nil {
		collectAggregates(plan.qualify, &aggs)
	}
	types := meta.types()
	env := e.newEnv(ctx, types, params)

	groups, ok, err := e.indexAggregate(ctx, plan, wp, aggs)
	if err != nil {
		return nil, err
	}
	if !ok {
		groups, err = e.driverAggregate(ctx, plan, wp, aggs, params)
		if err != nil {
			return nil, err
		}
	}

	kept := groups
	if plan.having != nil {
		kept = make([]aggGroup, 0, len(groups))
		for _, g := range groups {
			env.row, env.aggs = g.rep, g.results
			keep, err := env.eval(plan.having)
			if err != nil {
				return nil, invalidArg(err)
			}
			if b, ok := truthy(keep); ok && b {
				kept = append(kept, g)
			}
		}
	}
	// Window functions see one row per group, after HAVING.
	idx := make([]int, len(kept))
	for i := range idx {
		idx[i] = i
	}
	setRow := func(i int) { env.row, env.aggs, env.winRow = kept[i].rep, kept[i].results, i }
	if plan.windowed() {
		if idx, err = e.applyWindows(env, plan, len(kept), setRow); err != nil {
			return nil, err
		}
	}

	type outRow struct {
		vals []Value
		keys []Value
	}
	out := make([]outRow, 0, len(idx))
	for _, i := range idx {
		setRow(i)
		row := outRow{}
		for _, it := range plan.items {
			v, err := env.eval(it.expr)
			if err != nil {
				return nil, invalidArg(err)
			}
			row.vals = append(row.vals, v)
		}
		for _, o := range plan.order {
			v, err := env.eval(o.expr)
			if err != nil {
				return nil, invalidArg(err)
			}
			row.keys = append(row.keys, v)
		}
		out = append(out, row)
	}
	sort.SliceStable(out, func(i, j int) bool { return lessKeys(out[i].keys, out[j].keys, plan.order) })
	out = applyLimit(out, plan.sel.Offset, plan.sel.Limit)
	rows := make([][]Value, len(out))
	for i, r := range out {
		rows[i] = r.vals
	}
	return rows, nil
}

// aggGroup is one output group: representative column values (the GROUP BY
// columns) and the reduced value of every aggregate call.
type aggGroup struct {
	rep     map[string]Value
	results map[*Func]Value
}

// pushableKind reports whether RediSearch returns exact results for
// aggregates over a column of this kind (values it formats with at most 12
// significant digits).
func pushableKind(k Kind, mode string) bool {
	switch k {
	case KindInt16, KindInt32, KindBool, KindDate:
		return true
	case KindString:
		return false
	}
	return mode == PushdownAll && k.indexedAsNumeric()
}

// indexReducers maps the aggregates the index can compute to their
// FT.AGGREGATE reducers. COUNT(x) sums exists(@x) flags instead, and AVG
// divides a SUM by that count. The reducers for other aggregates are
// approximate (STDDEV, QUANTILE) or don't skip missing values (FIRST_VALUE),
// so those run in the driver.
var indexReducers = map[string]string{
	"COUNT": "COUNT", "SUM": "SUM", "AVG": "SUM", "MIN": "MIN", "MAX": "MAX",
	"BOOL_OR": "MAX", "BOOL_AND": "MIN", "EVERY": "MIN",
}

// indexAggregate computes GROUP BY / aggregates with FT.AGGREGATE over the
// SORTABLE index fields. ok is false when the query cannot be answered
// exactly by the index; the caller then aggregates in the driver.
func (e *executor) indexAggregate(ctx context.Context, plan *selectPlan, wp wherePlan, aggs []*Func) ([]aggGroup, bool, error) {
	meta := plan.meta
	if e.pushdown == PushdownNone || wp.residual != nil || wp.keys != nil || meta.isMem || len(plan.extraNeed) > 0 {
		return nil, false, nil
	}
	groupCols := make([]columnMeta, 0, len(plan.sel.GroupBy))
	groupNames := map[string]bool{}
	for _, g := range plan.sel.GroupBy {
		c, ok := g.(*ColumnRef)
		if !ok {
			return nil, false, nil
		}
		col, ok := meta.column(c.Name)
		// The index doesn't see the missing values of rows without a field,
		// and groups strings cut at a NUL byte by what is left.
		if !ok || !col.Indexed || !simpleName(col.field()) || col.MissingThrough > 0 || !col.sortsExactly() {
			return nil, false, nil
		}
		if !(col.Type.Kind == KindString || pushableKind(col.Type.Kind, e.pushdown)) {
			return nil, false, nil
		}
		groupCols = append(groupCols, col)
		groupNames[col.Name] = true
	}
	// Outside aggregate calls, only GROUP BY columns may be referenced.
	outside := map[string]bool{}
	collectOutside := func(e Expr) {
		walkOutsideAggregates(e, func(x Expr) {
			if c, ok := x.(*ColumnRef); ok && c.Outer == 0 {
				outside[c.Name] = true
			}
		})
	}
	for _, it := range plan.items {
		collectOutside(it.expr)
	}
	for _, o := range plan.order {
		collectOutside(o.expr)
	}
	if plan.having != nil {
		collectOutside(plan.having)
	}
	if plan.qualify != nil {
		collectOutside(plan.qualify)
	}
	for name := range outside {
		if !groupNames[name] {
			return nil, false, nil
		}
	}

	// Build APPLY / GROUPBY / REDUCE steps.
	var steps []any
	nonNull := map[string]string{} // column -> alias of exists() flag
	argCol := make([]columnMeta, len(aggs))
	for i, f := range aggs {
		if f.Filter != nil {
			// FILTER is evaluated by the driver.
			return nil, false, nil
		}
		if f.Star {
			if f.Name != "COUNT" {
				return nil, false, nil
			}
			continue
		}
		if _, ok := indexReducers[f.Name]; !ok || f.Distinct || len(f.Args) != 1 {
			return nil, false, nil
		}
		c, ok := f.Args[0].(*ColumnRef)
		if !ok {
			return nil, false, nil
		}
		col, ok := meta.column(c.Name)
		if !ok || !col.Indexed || !simpleName(col.field()) || col.Name == rowIDField || col.MissingThrough > 0 {
			return nil, false, nil
		}
		if f.Name != "COUNT" && !pushableKind(col.Type.Kind, e.pushdown) {
			return nil, false, nil
		}
		if (f.Name == "BOOL_OR" || f.Name == "BOOL_AND" || f.Name == "EVERY") && col.Type.Kind != KindBool {
			return nil, false, nil
		}
		argCol[i] = col
		if _, ok := nonNull[col.Name]; !ok {
			alias := fmt.Sprintf("__nn%d", len(nonNull))
			nonNull[col.Name] = alias
			steps = append(steps, "APPLY", fmt.Sprintf("exists(@%s)", col.field()), "AS", alias)
		}
	}
	steps = append(steps, "GROUPBY", len(groupCols))
	for _, c := range groupCols {
		steps = append(steps, "@"+c.field())
	}
	steps = append(steps, "REDUCE", "COUNT", 0, "AS", "__count")
	for col, alias := range nonNull {
		steps = append(steps, "REDUCE", "SUM", 1, "@"+alias, "AS", alias+"_n")
		_ = col
	}
	for i, f := range aggs {
		if f.Star || f.Name == "COUNT" {
			continue
		}
		steps = append(steps, "REDUCE", indexReducers[f.Name], 1, "@"+argCol[i].field(), "AS", fmt.Sprintf("__a%d", i))
	}
	raw, err := e.store.aggregate(ctx, &aggRequest{index: meta.index(), query: wp.query, groupBy: steps})
	if err != nil {
		return nil, false, err
	}
	if len(raw) == 0 && len(groupCols) == 0 {
		raw = []aggRow{{"__count": "0"}}
	}

	groups := make([]aggGroup, 0, len(raw))
	for _, r := range raw {
		g := aggGroup{rep: map[string]Value{}, results: map[*Func]Value{}}
		for _, c := range groupCols {
			s, ok := r[c.field()]
			if !ok {
				g.rep[c.Name] = nullValue(c.Type)
				continue
			}
			v, err := decodeStored(s, c.Type)
			if err != nil {
				return nil, false, nil
			}
			g.rep[c.Name] = v
		}
		count, err := strconv.ParseInt(r["__count"], 10, 64)
		if err != nil {
			return nil, false, nil
		}
		for i, f := range aggs {
			if f.Star {
				g.results[f] = intValue(typeInt64, count)
				continue
			}
			nn, err := strconv.ParseFloat(r[nonNull[argCol[i].Name]+"_n"], 64)
			if err != nil {
				return nil, false, nil
			}
			if f.Name == "COUNT" {
				g.results[f] = intValue(typeInt64, int64(nn))
				continue
			}
			t, err := inferType(f, meta.types(), nil)
			if err != nil {
				return nil, false, invalidArg(err)
			}
			if nn == 0 {
				g.results[f] = nullValue(t)
				continue
			}
			s := r[fmt.Sprintf("__a%d", i)]
			switch f.Name {
			case "SUM":
				if t.Kind == KindInt64 {
					n, err := strconv.ParseInt(s, 10, 64)
					if err != nil {
						// Rounded by RediSearch: compute exactly in the driver.
						return nil, false, nil
					}
					g.results[f] = intValue(typeInt64, n)
				} else {
					v, err := decodeStored(s, t)
					if err != nil {
						return nil, false, nil
					}
					g.results[f] = v
				}
			case "AVG":
				sum, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return nil, false, nil
				}
				g.results[f] = floatValue(typeFloat64, sum/nn)
			default: // MIN, MAX, and BOOL_OR / BOOL_AND as MAX / MIN of 0 and 1
				v, err := decodeStored(s, argCol[i].Type)
				if err != nil {
					return nil, false, nil
				}
				g.results[f] = v
			}
		}
		groups = append(groups, g)
	}
	return groups, true, nil
}

// driverAggregate fetches the needed columns from the row HASHes and reduces
// them in the driver.
func (e *executor) driverAggregate(ctx context.Context, plan *selectPlan, wp wherePlan, aggs []*Func, params []Value) ([]aggGroup, error) {
	meta := plan.meta
	req := scanRequest{meta: meta, where: wp, need: maps.Clone(plan.extraNeed)}
	if req.need == nil {
		req.need = map[string]bool{}
	}
	for _, it := range plan.items {
		columnRefs(it.expr, req.need)
	}
	for _, g := range plan.sel.GroupBy {
		columnRefs(g, req.need)
	}
	for _, o := range plan.order {
		columnRefs(o.expr, req.need)
	}
	if plan.having != nil {
		columnRefs(plan.having, req.need)
	}
	if plan.qualify != nil {
		columnRefs(plan.qualify, req.need)
	}
	_, rows, err := e.scan(ctx, req, params)
	if err != nil {
		return nil, err
	}
	types := meta.types()
	env := e.newEnv(ctx, types, params)
	aggTypes := make([]ColType, len(aggs))
	for i, f := range aggs {
		if aggTypes[i], err = inferType(f, types, nil); err != nil {
			return nil, invalidArg(err)
		}
	}
	type group struct {
		rep  map[string]Value
		accs []*accumulator
	}
	var order []string
	groups := map[string]*group{}
	newGroup := func(rep map[string]Value) *group {
		g := &group{rep: rep}
		for _, f := range aggs {
			g.accs = append(g.accs, newAccumulator(f))
		}
		return g
	}
	if len(plan.sel.GroupBy) == 0 {
		groups[""] = newGroup(map[string]Value{})
		order = append(order, "")
	}
	for _, vals := range rows {
		env.row = vals
		var key strings.Builder
		for _, gexpr := range plan.sel.GroupBy {
			v, err := env.eval(gexpr)
			if err != nil {
				return nil, invalidArg(err)
			}
			if v.Null {
				key.WriteString("N\x00")
			} else {
				key.WriteString("V" + v.Text() + "\x00")
			}
		}
		g, ok := groups[key.String()]
		if !ok {
			g = newGroup(vals)
			groups[key.String()] = g
			order = append(order, key.String())
		}
		for _, acc := range g.accs {
			if err := acc.addRow(env); err != nil {
				return nil, invalidArg(err)
			}
		}
	}
	out := make([]aggGroup, 0, len(order))
	for _, k := range order {
		g := groups[k]
		env.row = g.rep
		results := map[*Func]Value{}
		for i, f := range aggs {
			v, err := g.accs[i].result(env, aggTypes[i])
			if err != nil {
				return nil, invalidArg(err)
			}
			results[f] = v
		}
		out = append(out, aggGroup{rep: g.rep, results: results})
	}
	return out, nil
}

// ---- UPDATE / DELETE ----

// matchRows returns the keys and requested values of the rows matching a
// WHERE clause; name is the table's name or alias in the statement.
func (e *executor) matchRows(ctx context.Context, meta *tableMeta, name string, where Expr, params []Value, need map[string]bool) ([]string, []map[string]Value, error) {
	need = maps.Clone(need)
	if need == nil {
		need = map[string]bool{}
	}
	if where != nil {
		needs, err := e.bindIn(ctx, where, meta, name)
		if err != nil {
			return nil, nil, err
		}
		for k := range needs {
			need[k] = true
		}
	}
	wp, err := e.planWhere(ctx, where, meta, params)
	if err != nil {
		return nil, nil, err
	}
	return e.scan(ctx, scanRequest{meta: meta, where: wp, need: need}, params)
}

// maxUnionTerms caps the number of values pushed down as an index union.
const maxUnionTerms = 1000

// unionTerm turns a membership predicate on an indexed column into a
// RediSearch union query: numeric `(@c:[v v] | @c:[w w])` or TAG `@c:{a | b}`.
// A semi-join (correlated EXISTS / IN) becomes one on its outer column.
func (e *executor) unionTerm(ctx context.Context, c Expr, meta *tableMeta, env *evalEnv) (string, bool, error) {
	var col *ColumnRef
	var values []Value
	switch x := c.(type) {
	case *Subquery:
		if x.semi != nil && !x.Not {
			q, ok := e.semiJoinTerm(ctx, x, meta)
			return q, ok, nil
		}
		if x.Kind != SubqueryIn || x.Not || x.correlated {
			return "", false, nil
		}
		ref, ok := x.X.(*ColumnRef)
		if !ok || ref.Outer != 0 {
			return "", false, nil
		}
		rows, err := e.subqueryRows(ctx, x, env)
		if err != nil {
			return "", false, err
		}
		if len(rows) > maxUnionTerms {
			return "", false, nil
		}
		col = ref
		for _, r := range rows {
			values = append(values, r[0])
		}
	case *Binary:
		if x.Op != "OR" {
			return "", false, nil
		}
		var leaves []Expr
		var flatten func(Expr)
		flatten = func(n Expr) {
			if b, ok := n.(*Binary); ok && b.Op == "OR" {
				flatten(b.L)
				flatten(b.R)
				return
			}
			leaves = append(leaves, n)
		}
		flatten(x)
		if len(leaves) > maxUnionTerms {
			return "", false, nil
		}
		for _, leaf := range leaves {
			ref, op, other, ok := comparison(leaf)
			if !ok || op != "=" || ref.Outer != 0 || (col != nil && ref.Name != col.Name) {
				return "", false, nil
			}
			v, err := env.eval(other)
			if err != nil {
				return "", false, invalidArg(err)
			}
			col = ref
			values = append(values, v)
		}
	default:
		return "", false, nil
	}
	cm, ok := meta.column(col.Name)
	if !ok || !cm.Indexed || !simpleName(cm.field()) {
		return "", false, nil
	}
	q, ok := unionQuery(cm, values)
	return q, ok, nil
}

// unionQuery builds an index query matching rows whose column equals any of
// values: numeric `(@c:[v v] | @c:[w w])` or TAG `@c:{a | b}`.
func unionQuery(cm columnMeta, values []Value) (string, bool) {
	var parts []string
	for _, v := range values {
		if v.Null {
			continue
		}
		cv, err := Coerce(v, cm.Type)
		if err != nil {
			continue // cannot equal any stored value
		}
		if cm.Type.Kind == KindString {
			// The tag the value is found by (see tags.go).
			lit, _, ok := tagLookup(cm, cv.S)
			if !ok {
				return "", false
			}
			parts = append(parts, lit)
		} else if cm.Type.Kind.indexedAsNumeric() {
			b := encodeStored(cv)
			parts = append(parts, fmt.Sprintf("@%s:[%s %s]", cm.field(), b, b))
		} else {
			return "", false
		}
	}
	if len(parts) == 0 {
		// Nothing can match; __rowid is never negative.
		return "@" + rowIDField + ":[-1 -1]", true
	}
	q := "(" + strings.Join(parts, " | ") + ")"
	if cm.Type.Kind == KindString {
		q = fmt.Sprintf("@%s:{%s}", cm.field(), strings.Join(parts, " | "))
	}
	// Callers re-check the predicate on the rows, so the query may be wider.
	q, _ = widenMissing(q, cm, func(m Value) bool {
		for _, v := range values {
			r, err := binaryOp("=", m, v)
			if b, _ := truthy(r); err != nil || b {
				return true
			}
		}
		return false
	})
	return q, true
}

// widenMissing widens an index term on a column that has a missing value
// (see "Missing values" in defaults.go) to every row that may read it, the
// rows with __rowid <= MissingThrough, which the index can't tell apart from
// rows with a NULL, if the value satisfies the term's predicate (holds).
// widened reports whether it did; the predicate must then be re-checked on
// the rows.
func widenMissing(term string, col columnMeta, holds func(Value) bool) (string, bool) {
	if col.MissingThrough == 0 {
		return term, false
	}
	if m, err := decodeStored(col.Missing, col.Type); err == nil && !holds(m) {
		return term, false
	}
	return fmt.Sprintf("(%s | @%s:[-inf %d])", term, rowIDField, col.MissingThrough), true
}

// likePrefixTerm turns `col LIKE 'abc%'` on an indexed string column into a
// TAG prefix query `@col:{abc*}` (RediSearch needs at least two characters),
// on the prefix's tag (see tags.go). The caller re-checks the predicate.
func likePrefixTerm(c Expr, meta *tableMeta, env *evalEnv) (string, bool) {
	f, ok := c.(*Func)
	if !ok || f.Name != "LIKE" || len(f.Args) != 2 {
		return "", false
	}
	ref, ok := f.Args[0].(*ColumnRef)
	if !ok || ref.Outer != 0 || !isConstant(f.Args[1]) {
		return "", false
	}
	col, ok := meta.column(ref.Name)
	if !ok || !col.Indexed || col.Type.Kind != KindString || !simpleName(col.field()) {
		return "", false
	}
	pv, err := env.eval(f.Args[1])
	if err != nil || pv.Null {
		return "", false
	}
	pat := pv.Text()
	if !strings.HasSuffix(pat, "%") {
		return "", false
	}
	prefix := strings.TrimSuffix(pat, "%")
	if strings.ContainsAny(prefix, "%_") || strings.Contains(prefix, tagSeparator) {
		return "", false
	}
	lit, ok := prefixLookup(prefix)
	if !ok {
		return "", false
	}
	term, _ := widenMissing(fmt.Sprintf("@%s:{%s*}", col.field(), lit), col, func(m Value) bool {
		return strings.HasPrefix(m.S, prefix)
	})
	return term, true
}
