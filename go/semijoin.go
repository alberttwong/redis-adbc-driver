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

// Hash sets for IN subqueries, and semi-joins for correlated subqueries.
//
//   - `x IN (SELECT …)` probes a hash set of the subquery's values instead of
//     comparing x with every value. The set gives exactly the answer of
//     comparing one by one with compareValues, including SQL's NULL rules and
//     cross-type equality (1 = 1.0 = 1.00); values and type pairs the hash
//     can't answer exactly are compared one by one.
//   - A correlated EXISTS / IN subquery whose only outer references are
//     top-level `inner = outer` conjuncts of its WHERE can run once as a
//     semi-join: without those conjuncts, projecting their inner sides (and,
//     for IN, its value), grouped by those keys in a hash table that each
//     outer row probes. NOT EXISTS and NOT IN are the negation and [NOT] IN's
//     own logic over the matching group. A small inner side is read up front
//     (and can then filter the outer table in its index); a large one only
//     once running per outer row has cost about as much, so that a few outer
//     rows don't pay for reading all of it. Anything else (other correlation,
//     aggregates, LIMIT, keys of different types, …) runs per outer row,
//     memoised.

import (
	"context"
	"strconv"
	"sync/atomic"
)

// subqueryStats counts subquery work; tests read it to tell which path a
// query took.
var subqueryStats struct {
	runs      atomic.Int64 // subquery bodies run by subqueryRows
	semiJoins atomic.Int64 // semi-join bodies run
	inSets    atomic.Int64 // IN hash sets built
	inLists   atomic.Int64 // rows answered by a literal IN list's hash set
}

// ---- equality keys ----

// eqClass groups the kinds that compareValues compares in one way.
type eqClass uint8

const (
	clsNone      eqClass = iota // no key: NaN, or a kind not listed here
	clsNum                      // integers and decimals, compared exactly
	clsBool                     // compared with integers and decimals as 0/1
	clsFloat                    // floats other than NaN
	clsStr                      // strings and binary, compared bytewise
	clsInterval                 // compared by total length
	clsDate                     // compared by day
	clsTime                     // compared after converting units
	clsTimestamp                // compared after converting units
	numClasses
)

func classOf(t ColType) eqClass {
	switch k := t.Kind; {
	case k.isInteger() || k == KindDecimal:
		return clsNum
	case k == KindBool:
		return clsBool
	case k.isFloat():
		return clsFloat
	case k == KindString || k == KindBinary:
		return clsStr
	case k == KindInterval:
		return clsInterval
	case k == KindDate:
		return clsDate
	case k == KindTime:
		return clsTime
	case k == KindTimestamp:
		return clsTimestamp
	}
	return clsNone
}

// eqKey returns a non-NULL value's class and its key within the class: two
// values of a class compare equal exactly when their keys are equal
// (joinKey's normalization; integers, booleans and decimals share keys).
func eqKey(v Value) (eqClass, string) {
	c := classOf(v.T)
	if c == clsDate {
		return c, strconv.FormatInt(v.I, 10)
	}
	if c == clsNone {
		return c, ""
	}
	k, ok := joinKey(v)
	if !ok {
		return clsNone, "" // NaN
	}
	return c, k
}

// floatKey is the key of a number compared as a double (an integer or decimal
// compared with a float).
func floatKey(v Value) string {
	f, _ := v.asFloat()
	k, _ := joinKey(floatValue(typeFloat64, f))
	return k
}

// classTag is the prefix that keeps the keys of different classes apart in
// one map. Integers, decimals and booleans compare exactly with each other,
// so they share a tag.
var classTag = [numClasses]string{clsNum: "n", clsBool: "n", clsFloat: "f", clsStr: "s",
	clsInterval: "i", clsDate: "d", clsTime: "t", clsTimestamp: "p"}

func exactNum(c eqClass) bool { return c == clsNum || c == clsBool }

// ---- IN hash sets ----

// inSetLinear is the size up to which an IN set compares values one by one
// rather than building a hash index.
const inSetLinear = 8

// inSet answers `x IN (values)` for the values in column col of rows, with
// the result of comparing x with each value by compareValues.
type inSet struct {
	rows [][]Value
	col  int
	// noIndex makes the set compare one by one (a result used only once).
	noIndex bool

	built      bool
	sawNull    bool
	classes    uint16              // eqClass bits of the hashed values
	keys       map[string]struct{} // classTag + eqKey of each hashed value
	odd        []Value             // values without a key, compared one by one
	numAsFloat map[string]struct{} // floatKey of the clsNum values, on demand
	char       bool                // a value is CHAR: compare one by one
}

// eval applies [NOT] IN's three-valued logic: an empty set gives FALSE (TRUE
// for NOT IN) even for a NULL x; otherwise a NULL x, or no match when the set
// contains a NULL, gives NULL.
func (s *inSet) eval(x Value, not bool) Value {
	if len(s.rows) == 0 {
		return boolValue(not)
	}
	if x.Null {
		return nullValue(typeBool)
	}
	found, sawNull := s.contains(x)
	switch {
	case found:
		return boolValue(!not)
	case sawNull:
		return nullValue(typeBool)
	}
	return boolValue(not)
}

// linear compares x with every value.
func (s *inSet) linear(x Value) (found, sawNull bool) {
	for _, row := range s.rows {
		v := row[s.col]
		if v.Null {
			sawNull = true
			continue
		}
		if c, ok := compareValues(x, v); ok && c == 0 {
			return true, sawNull
		}
	}
	return false, sawNull
}

func (s *inSet) build() {
	s.built = true
	s.keys = make(map[string]struct{}, len(s.rows))
	for _, row := range s.rows {
		v := row[s.col]
		if v.Null {
			s.sawNull = true
			continue
		}
		s.char = s.char || v.T.isChar()
		c, k := eqKey(v)
		if c == clsNone {
			s.odd = append(s.odd, v)
			continue
		}
		s.classes |= 1 << c
		s.keys[classTag[c]+k] = struct{}{}
	}
	subqueryStats.inSets.Add(1)
}

// contains reports whether x (not NULL) equals a value of the set, and
// whether the set contains a NULL.
func (s *inSet) contains(x Value) (found, sawNull bool) {
	if s.noIndex || len(s.rows) <= inSetLinear {
		return s.linear(x)
	}
	if !s.built {
		s.build()
	}
	cx, kx := eqKey(x)
	if cx == clsNone || s.char || x.T.isChar() {
		// CHAR values compare without trailing spaces (lengths.go).
		return s.linear(x)
	}
	slow := false
	for c := clsNum; c < numClasses; c++ {
		if s.classes&(1<<c) == 0 {
			continue
		}
		var key string
		switch {
		case exactNum(cx) && exactNum(c), cx == c:
			key = classTag[c] + kx
		case cx == clsNum && c == clsFloat:
			// An integer or decimal compared with a float: as doubles.
			key = classTag[c] + floatKey(x)
		case cx == clsFloat && c == clsNum:
			if s.numAsFloat == nil {
				s.numAsFloat = map[string]struct{}{}
				for _, row := range s.rows {
					if v := row[s.col]; !v.Null && classOf(v.T) == clsNum {
						s.numAsFloat[floatKey(v)] = struct{}{}
					}
				}
			}
			if _, ok := s.numAsFloat[kx]; ok {
				return true, s.sawNull
			}
			continue
		default:
			// Strings compared with other types are converted first, and
			// so are dates compared with timestamps: compare one by one.
			slow = true
			continue
		}
		if _, ok := s.keys[key]; ok {
			return true, s.sawNull
		}
	}
	if slow {
		return s.linear(x)
	}
	for _, v := range s.odd {
		if c, ok := compareValues(x, v); ok && c == 0 {
			return true, s.sawNull
		}
	}
	return false, s.sawNull
}

// cachedInSet returns the hash set of an uncorrelated IN subquery's rows,
// built once per statement.
func (e *executor) cachedInSet(sq *Subquery, rows [][]Value) *inSet {
	if s, ok := e.cache.inSets[sq]; ok {
		return s
	}
	s := &inSet{rows: rows}
	e.cache.inSets[sq] = s
	return s
}

// ---- semi-joins ----

// semiJoin is the decorrelated form of a correlated EXISTS / IN subquery.
type semiJoin struct {
	// plan is the subquery without its correlation conjuncts. Its first
	// len(outer) items are their inner sides; for IN, the subquery's value
	// follows.
	plan *selectPlan
	// outer are the outer columns the keys are compared with, and types their
	// types. Keys and outer values must be of the same class (and time unit),
	// where comparing them never fails and equal keys mean equal values.
	outer []outerRef
	types []ColType
}

// sameEqClass reports whether values of types a and b compare by their eqKey
// (and never fail to compare).
func sameEqClass(a, b ColType) bool {
	if a.isChar() || b.isChar() {
		return false // compared without trailing spaces (lengths.go)
	}
	ca, cb := classOf(a), classOf(b)
	if exactNum(ca) && exactNum(cb) {
		return true
	}
	if ca == clsNone || ca != cb {
		return false
	}
	return (ca != clsTime && ca != clsTimestamp) || a.Unit == b.Unit
}

// semiKey returns the key of a non-NULL key value expected to be of type t;
// ok is false when the value is of another class or has no key (NaN).
func semiKey(v Value, t ColType) (string, bool) {
	if !sameEqClass(v.T, t) {
		return "", false
	}
	c, k := eqKey(v)
	return k, c != clsNone
}

// planSemiJoin plans sq (a planned, correlated subquery) as a semi-join when
// that gives the same result: every outer reference in its body is the outer
// side of a top-level `inner = outer` conjunct of its WHERE, the body has no
// aggregates, GROUP BY, HAVING, window functions, OFFSET or (for IN) LIMIT,
// and the inner and outer sides of each conjunct are of the same class.
// EXISTS also requires a plain select list (columns, literals, *), which it
// doesn't need to evaluate. The body must read one table or in-memory
// relation (a CTE, a derived table or a computed view), whose size
// semiJoinState can tell, and call no volatile function.
func (e *executor) planSemiJoin(ctx context.Context, sq *Subquery) {
	sel, plan := sq.Select, sq.plan
	if !sq.correlated || (sq.Kind != SubqueryExists && sq.Kind != SubqueryIn) || sel.SetOp != nil || plan.meta == nil ||
		plan.aggregate || len(sel.GroupBy) > 0 || sel.Having != nil || sel.Offset != nil || sel.Where == nil ||
		plan.windowed() || len(sel.Windows) > 0 {
		return
	}
	if len(sel.Joins) > 0 || plan.meta.view != nil || plan.meta.join != nil {
		return
	}
	// DISTINCT ON keeps one row per key after the correlation filter; the
	// semi-join body (which drops it) would see every row.
	if len(sel.DistinctOn) > 0 {
		return
	}
	// RANDOM() would be drawn once per inner row for all outer rows.
	if hasVolatile(sel.Where) || (sq.Kind == SubqueryIn && hasVolatile(sel.Items[0].Expr)) {
		return
	}
	if sel.Limit != nil && (sq.Kind == SubqueryIn || *sel.Limit < 1) {
		return
	}
	if sq.Kind == SubqueryExists {
		for _, it := range sel.Items {
			switch x := it.Expr.(type) {
			case *Literal, *Param:
			case *ColumnRef:
				if x.Outer > 0 {
					return
				}
			default:
				if !it.Star {
					return
				}
			}
		}
	}
	type pair struct {
		inner Expr
		outer *ColumnRef
	}
	var pairs []pair
	var rest []Expr
	for _, c := range conjuncts(sel.Where, nil) {
		if b, ok := c.(*Binary); ok && b.Op == "=" {
			if r, ok := b.R.(*ColumnRef); ok && r.Outer == 1 {
				pairs = append(pairs, pair{inner: b.L, outer: r})
				continue
			}
			if l, ok := b.L.(*ColumnRef); ok && l.Outer == 1 {
				pairs = append(pairs, pair{inner: b.R, outer: l})
				continue
			}
		}
		rest = append(rest, c)
	}
	// Each pair holds one outer reference; any other (in the inner sides, the
	// other conjuncts, the select list, ORDER BY, JOIN … ON or a nested
	// subquery) keeps the per-row path.
	if len(pairs) == 0 || sq.outerUses != len(pairs) {
		return
	}
	dsel := &SelectStmt{With: sel.With, Joins: sel.Joins, From: sel.From, FromSelect: sel.FromSelect,
		FromAlias: sel.FromAlias, Where: andAll(rest)}
	for _, p := range pairs {
		dsel.Items = append(dsel.Items, SelectItem{Expr: p.inner})
	}
	if sq.Kind == SubqueryIn {
		dsel.Items = append(dsel.Items, sel.Items...)
	}
	// Plan it with no enclosing scopes: it reads nothing from the outer row.
	savedScopes, savedSq := e.scopes, e.pendingSq
	e.scopes, e.pendingSq = nil, nil
	dplan, err := e.planSelect(ctx, dsel, e.paramTypes)
	e.scopes, e.pendingSq = savedScopes, savedSq
	if err != nil || len(dplan.items) != len(dsel.Items) {
		return
	}
	sj := &semiJoin{plan: dplan}
	for i, p := range pairs {
		if !sameEqClass(dplan.items[i].typ, p.outer.OuterType) {
			return
		}
		sj.outer = append(sj.outer, outerRef{name: p.outer.Name})
		sj.types = append(sj.types, p.outer.OuterType)
	}
	sq.semi = sj
}

// semiJoinEagerRows is the size of inner side up to which a semi-join is
// built before the outer rows are read, so that its keys can also filter them
// in the index (see semiJoinTerm).
const semiJoinEagerRows = maxUnionTerms

// rowsPerRun is roughly how many rows a scan reads in the time one per-row
// run of a subquery takes: an index query round trip (~0.4–0.6 ms locally)
// against ~5 µs per row read in cursor pages.
const rowsPerRun = 100

// semiResult is a semi-join's state for one statement. Building it reads the
// whole inner side, which costs more than running the subquery for a few
// outer rows; so a large inner side is built only once the per-row runs have
// cost about as much, and until then the subquery runs per outer row.
type semiResult struct {
	eager bool // build before reading the outer rows (small or in memory)
	after int  // otherwise, build after this many per-row runs
	built bool
	// failed means the body failed to run or returned a key it can't hash;
	// the subquery then keeps running per outer row.
	failed bool
	rows   int // rows with no NULL key
	// groups maps the key tuple of each row to the rows with that key (for
	// IN, as the set of values; for EXISTS, nil).
	groups map[string]*inSet
	// distinct holds the distinct values of each key, up to maxUnionTerms+1.
	distinct [][]Value
}

func appendKey(b []byte, k string) []byte {
	b = strconv.AppendInt(b, int64(len(k)), 10)
	b = append(b, ':')
	return append(b, k...)
}

// semiJoinState returns a semi-join's state, sizing its inner side (with an
// index count) the first time.
func (e *executor) semiJoinState(ctx context.Context, sq *Subquery) *semiResult {
	e.ensureCache()
	if r, ok := e.cache.semi[sq]; ok {
		return r
	}
	r := &semiResult{}
	e.cache.semi[sq] = r
	plan := sq.semi.plan
	if plan.meta.isMem {
		// Each per-row run filters the whole relation anyway.
		r.eager = true
		return r
	}
	child := e.isolatedChild()
	wp, err := child.planWhere(ctx, plan.sel.Where, plan.meta, e.params)
	if err != nil {
		r.failed = true
		return r
	}
	n := int64(len(wp.keys))
	if wp.keys == nil && !wp.none {
		if n, err = e.store.countMatches(ctx, plan.meta.index(), wp.query); err != nil {
			r.failed = true
			return r
		}
	}
	r.eager = n <= semiJoinEagerRows
	r.after = int(n / rowsPerRun)
	return r
}

// isolatedChild returns an executor for a query that reads nothing from the
// enclosing queries.
func (e *executor) isolatedChild() *executor {
	child := *e
	child.scopes, child.pendingSq, child.outer = nil, nil, nil
	return &child
}

// buildSemiJoin runs a semi-join's body and groups its rows by key.
func (e *executor) buildSemiJoin(ctx context.Context, sq *Subquery, r *semiResult) {
	sj := sq.semi
	subqueryStats.semiJoins.Add(1)
	rows, err := e.isolatedChild().runSelect(ctx, sj.plan, e.params)
	if err != nil {
		// The body may fail on rows the per-row path never reads (its
		// conjuncts are no longer narrowed by the outer values); that path
		// reports any error the original query has.
		r.failed = true
		return
	}
	n := len(sj.outer)
	r.groups = map[string]*inSet{}
	r.distinct = make([][]Value, n)
	seen := make([]map[string]bool, n)
	for i := range seen {
		seen[i] = map[string]bool{}
	}
	keys := make([]string, n)
	var buf []byte
next:
	for _, row := range rows {
		for i := 0; i < n; i++ {
			if row[i].Null {
				continue next // never equal to anything
			}
		}
		buf = buf[:0]
		for i := 0; i < n; i++ {
			k, ok := semiKey(row[i], sj.types[i])
			if !ok {
				r.failed, r.groups, r.distinct = true, nil, nil
				return
			}
			keys[i] = k
			buf = appendKey(buf, k)
		}
		r.rows++
		for i, k := range keys {
			if len(r.distinct[i]) <= maxUnionTerms && !seen[i][k] {
				seen[i][k] = true
				r.distinct[i] = append(r.distinct[i], row[i])
			}
		}
		g, ok := r.groups[string(buf)]
		if sq.Kind == SubqueryIn {
			if !ok {
				g = &inSet{col: n}
				r.groups[string(buf)] = g
			}
			g.rows = append(g.rows, row)
		} else if !ok {
			r.groups[string(buf)] = nil
		}
	}
	r.built = true
}

// evalSemiJoin evaluates a decorrelated subquery for env's row. ok is false
// when it must be evaluated per row instead: running per row is still
// cheaper, the body failed, or an outer value is not of its key's class.
func (e *executor) evalSemiJoin(env *evalEnv, sq *Subquery) (Value, bool, error) {
	r := e.semiJoinState(env.ctx, sq)
	if !r.built && !r.failed && (r.eager || len(e.cache.memo[sq]) >= r.after) {
		e.buildSemiJoin(env.ctx, sq, r)
	}
	if !r.built {
		return Value{}, false, nil
	}
	sj := sq.semi
	var buf []byte
	null := false
	for i, ref := range sj.outer {
		v, err := env.lookupUp(ref.name, ref.up)
		if err != nil {
			return Value{}, false, err
		}
		if v.Null {
			null = true // `inner = NULL` is never true
			continue
		}
		k, ok := semiKey(v, sj.types[i])
		if !ok {
			return Value{}, false, nil
		}
		buf = appendKey(buf, k)
	}
	var g *inSet
	found := false
	if !null {
		g, found = r.groups[string(buf)]
	}
	if sq.Kind == SubqueryExists {
		return boolValue(found), true, nil
	}
	x, err := env.eval(sq.X)
	if err != nil {
		return Value{}, true, err
	}
	if !found {
		return boolValue(sq.Not), true, nil // no rows
	}
	return g.eval(x, sq.Not), true, nil
}

// semiJoinTerm turns a decorrelated EXISTS / IN conjunct of a WHERE clause
// into an index query on the outer table, when its inner side is small enough
// to build first: one that matches nothing when the subquery has no rows, or
// a union of the inner keys when there are at most maxUnionTerms of them and
// the outer column is indexed. The conjunct itself is still checked on the
// rows.
func (e *executor) semiJoinTerm(ctx context.Context, sq *Subquery, meta *tableMeta) (string, bool) {
	r := e.semiJoinState(ctx, sq)
	if !r.built && !r.failed && r.eager {
		e.buildSemiJoin(ctx, sq, r)
	}
	if !r.built {
		return "", false
	}
	if r.rows == 0 {
		return noMatchQuery, true
	}
	for i, ref := range sq.semi.outer {
		cm, ok := meta.column(ref.name)
		if !ok || !cm.Indexed || !simpleName(cm.field()) || len(r.distinct[i]) > maxUnionTerms {
			continue
		}
		if q, ok := unionQuery(cm, r.distinct[i]); ok {
			return q, true
		}
	}
	return "", false
}
