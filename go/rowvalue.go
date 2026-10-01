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

// Row constructors: (a, b, …) and ROW(…), with Postgres's semantics.
//
// The parser rewrites a comparison of two rows into comparisons of their
// items, so that the index, the planner and the evaluator see ordinary
// expressions:
//
//   - (a, b) = (x, y) is a = x AND b = y, and <> is a <> x OR b <> y. SQL's
//     three-valued logic then gives Postgres's rule: the result is NULL when
//     it depends on a NULL item.
//   - IS [NOT] DISTINCT FROM is the same with the items' IS [NOT] DISTINCT
//     FROM, which treat NULLs as equal.
//   - <, <=, > and >= compare left to right and stop at the first pair that
//     is unequal or has a NULL: (a, b) < (x, y) is a < x OR (a = x AND b <
//     y), which is NULL when the first such pair has a NULL.
//   - (a, b) IS NULL is a IS NULL AND b IS NULL; IS NOT NULL is a IS NOT
//     NULL AND b IS NOT NULL (so a row can be neither).
//   - An IN list is the OR of its rows' comparisons, BETWEEN the AND of two
//     comparisons, and a simple CASE on a row a searched CASE.
//   - (a, b) op (SELECT x, y …) compares with the subquery's one row: its
//     columns are RowColumns of one subquery, which runs once. No row gives
//     NULLs, and more than one is an error.
//
// A row compared with a subquery's rows, (a, b) [NOT] IN (SELECT x, y …) and
// (a, b) op ANY | ALL (SELECT …), keeps the row as the subquery's operand.
// IN probes a hash set of the subquery's rows (rowSet), built once per
// statement, or the groups of a semi-join when the subquery is correlated;
// ANY / ALL compare with each row (rowCompareValues).
//
// Anything else is an error, which binding reports in Postgres's order (the
// operands are bound first): rows of different lengths, a row compared with
// a single value, and a row anywhere but in a comparison or IS NULL.

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// ---- parsing ----

// compareExpr builds `l op r` for a comparison operator (=, <>, <, <=, >, >=,
// IS [NOT] DISTINCT FROM), rewriting a comparison of rows into comparisons
// of their items. A comparison that can't be rewritten (rows of different
// lengths, a row and a single value) is left as it is, for binding to
// report.
func compareExpr(op string, l, r Expr) Expr {
	lr, lok := l.(*RowExpr)
	rr, rok := r.(*RowExpr)
	switch {
	case lok && !rok:
		rr, rok = rowOfSubquery(r, len(lr.Items))
	case rok && !lok:
		lr, lok = rowOfSubquery(l, len(rr.Items))
	}
	if !lok || !rok || len(lr.Items) != len(rr.Items) || len(lr.Items) == 0 {
		return &Binary{Op: op, L: l, R: r}
	}
	ls, rs := lr.Items, rr.Items
	n := len(ls)
	// The items are compared with plain comparisons: a nested row isn't
	// rewritten (Postgres compares nested rows as composite values, with
	// other NULL rules), so binding reports it.
	cmp := func(op string, i int) Expr { return &Binary{Op: op, L: ls[i], R: rs[i]} }
	var e Expr
	switch op {
	case "=", "IS NOT DISTINCT FROM", "<>", "IS DISTINCT FROM":
		join := "AND"
		if op == "<>" || op == "IS DISTINCT FROM" {
			join = "OR"
		}
		for i := range ls {
			if e == nil {
				e = cmp(op, i)
			} else {
				e = &Binary{Op: join, L: e, R: cmp(op, i)}
			}
		}
	default:
		// (a, b, c) < (x, y, z): a < x OR (a = x AND (b < y OR (b = y AND c < z))).
		strict := op[:1]
		e = cmp(op, n-1)
		for i := n - 2; i >= 0; i-- {
			e = &Binary{Op: "OR", L: cmp(strict, i), R: &Binary{Op: "AND", L: cmp("=", i), R: e}}
		}
	}
	return e
}

// rowOfSubquery returns the columns of a scalar subquery compared with a
// row of n items, as a row of RowColumns. It may be compared more than once
// (`(SELECT a, b) IN ((1, 2), (3, 4))`), with rows of the same length.
func rowOfSubquery(e Expr, n int) (*RowExpr, bool) {
	sq, ok := e.(*Subquery)
	if !ok || sq.Kind != SubqueryScalar || n == 0 || (sq.Width != 0 && !(sq.rowCompare && sq.Width == n)) {
		return nil, false
	}
	sq.Width, sq.rowCompare = n, true
	row := &RowExpr{Items: make([]Expr, n)}
	for i := range row.Items {
		row.Items[i] = &RowColumn{Sub: sq, Index: i}
	}
	return row, true
}

// isNullExpr builds `x IS [NOT] NULL`. For a row, IS NULL is true when every
// item is NULL and IS NOT NULL when none is.
func isNullExpr(x Expr, not bool) Expr {
	r, ok := x.(*RowExpr)
	if !ok {
		return &IsNull{X: x, Not: not}
	}
	if len(r.Items) == 0 {
		return &Literal{V: boolValue(true)}
	}
	var e Expr
	for _, it := range r.Items {
		// A nested row stays the operand of its IS NULL, for binding to
		// report.
		var t Expr = &IsNull{X: it, Not: not}
		if e == nil {
			e = t
		} else {
			e = &Binary{Op: "AND", L: e, R: t}
		}
	}
	return e
}

// rowOperand is the operand of an IN / ANY / ALL subquery: ROW(x) is x.
func rowOperand(x Expr) Expr {
	if r, ok := x.(*RowExpr); ok && len(r.Items) == 1 {
		return r.Items[0]
	}
	return x
}

// rowCaseToSearched turns a simple CASE whose operand or WHEN values are
// rows into a searched CASE of row comparisons.
func rowCaseToSearched(c *Case) {
	if c.Operand == nil {
		return
	}
	_, row := c.Operand.(*RowExpr)
	for _, w := range c.Whens {
		if _, ok := w.When.(*RowExpr); ok {
			row = true
		}
	}
	if !row {
		return
	}
	for i, w := range c.Whens {
		c.Whens[i].When = compareExpr("=", c.Operand, w.When)
	}
	c.Operand = nil
}

// subqueryOperands returns the operand of an IN / ANY / ALL subquery: the
// items of a row, or the one expression (none for other subqueries).
func subqueryOperands(sq *Subquery) []Expr {
	if sq.X == nil {
		return nil
	}
	return rowItems(sq.X)
}

// rowItems returns a row's items, or x itself if it isn't a row.
func rowItems(x Expr) []Expr {
	if r, ok := x.(*RowExpr); ok {
		return r.Items
	}
	return []Expr{x}
}

// ---- errors ----

func errRowValue() error {
	return errorf(adbc.StatusInvalidArgument,
		"a row constructor can only be compared (=, <>, <, <=, >, >=, IS [NOT] DISTINCT FROM, IN, ANY, ALL) or tested with IS [NOT] NULL")
}

func errNestedRow() error {
	return errorf(adbc.StatusInvalidArgument, "nested row constructors are not supported")
}

func isRowComparison(op string) bool {
	return isComparison(op) || isDistinctOp(op)
}

// bindRowItems binds the items of a row (of nested rows too), or x.
func (e *executor) bindRowItems(ctx context.Context, x Expr) error {
	r, ok := x.(*RowExpr)
	if !ok {
		return e.bind(ctx, x)
	}
	for _, it := range r.Items {
		if err := e.bindRowItems(ctx, it); err != nil {
			return err
		}
	}
	return nil
}

// rowOperatorError is the error for an operator with a row operand that the
// parser didn't rewrite (b.L or b.R is a *RowExpr). The operands are bound
// first, so an error in them is reported before it, as in Postgres.
func (e *executor) rowOperatorError(ctx context.Context, b *Binary) error {
	for _, side := range []Expr{b.L, b.R} {
		if err := e.bindRowItems(ctx, side); err != nil {
			return err
		}
	}
	if b.Op == "AND" || b.Op == "OR" {
		return errorf(adbc.StatusInvalidArgument, "argument of %s must be type boolean, not type record", b.Op)
	}
	lr, lok := b.L.(*RowExpr)
	rr, rok := b.R.(*RowExpr)
	if lok && rok && isRowComparison(b.Op) {
		switch {
		case len(lr.Items) != len(rr.Items):
			return errorf(adbc.StatusInvalidArgument, "unequal number of entries in row expressions")
		case len(lr.Items) == 0:
			return errorf(adbc.StatusInvalidArgument, "cannot compare rows of zero length")
		}
		return errNestedRow() // the items of a rewritten comparison
	}
	name := func(x Expr) string {
		if _, ok := x.(*RowExpr); ok {
			return "record"
		}
		t, err := inferType(x, e.scopeTypes(), e.paramTypes)
		if err != nil {
			t = typeNull
		}
		return sigTypeName(t)
	}
	op := b.Op
	if isDistinctOp(op) {
		op = "=" // the operator IS DISTINCT FROM uses
	}
	return errorf(adbc.StatusInvalidArgument, "operator does not exist: %s %s %s", name(b.L), op, name(b.R))
}

// bindRowOperand binds the row operand of an IN / ANY / ALL subquery; a
// nested row is an error.
func (e *executor) bindRowOperand(ctx context.Context, r *RowExpr) error {
	for _, it := range r.Items {
		if _, nested := it.(*RowExpr); nested {
			if err := e.bindRowItems(ctx, it); err != nil {
				return err
			}
			return errNestedRow()
		}
		if err := e.bind(ctx, it); err != nil {
			return err
		}
	}
	return nil
}

// subqueryWidthError is the error for a subquery of got columns compared
// with an operand of want items, nil if they match.
func subqueryWidthError(got, want int) error {
	switch {
	case got > want:
		return errorf(adbc.StatusInvalidArgument, "subquery has too many columns")
	case got < want:
		return errorf(adbc.StatusInvalidArgument, "subquery has too few columns")
	}
	return nil
}

// ---- evaluation ----

func isDistinctOp(op string) bool { return op == "IS DISTINCT FROM" || op == "IS NOT DISTINCT FROM" }

// distinctFrom evaluates a IS [NOT] DISTINCT FROM b: NULLs are equal to each
// other and distinct from any value. The result is never NULL.
func distinctFrom(op string, a, b Value) (Value, error) {
	distinct := a.Null != b.Null
	if !a.Null && !b.Null {
		c, ok := compareValues(a, b)
		if !ok {
			return Value{}, fmt.Errorf("cannot compare %s with %s", a.T.Kind, b.T.Kind)
		}
		distinct = c != 0
	}
	return boolValue(distinct == (op == "IS DISTINCT FROM")), nil
}

// evalItems evaluates a row's items.
func (env *evalEnv) evalItems(items []Expr) ([]Value, error) {
	out := make([]Value, len(items))
	for i, it := range items {
		v, err := env.eval(it)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// rowCompareValues compares two rows of values of the same length with
// op, as the rewritten comparisons do: = and <> item by item (NULL if the
// result depends on a NULL item), the ordering operators left to right up
// to the first pair that is unequal or has a NULL.
func rowCompareValues(op string, l, r []Value) (Value, error) {
	switch op {
	case "=", "<>":
		sawNull := false
		for i := range l {
			v, err := binaryOp(op, l[i], r[i])
			if err != nil {
				return Value{}, err
			}
			b, ok := truthy(v)
			switch {
			case !ok:
				sawNull = true
			case b != (op == "="):
				// A false = decides =, a true <> decides <>.
				return boolValue(b), nil
			}
		}
		if sawNull {
			return nullValue(typeBool), nil
		}
		return boolValue(op == "="), nil
	}
	for i := range l {
		if l[i].Null || r[i].Null {
			return nullValue(typeBool), nil
		}
		eq, err := binaryOp("=", l[i], r[i])
		if err != nil {
			return Value{}, err
		}
		if b, _ := truthy(eq); !b {
			return binaryOp(op, l[i], r[i])
		}
	}
	return boolValue(op == "<=" || op == ">="), nil
}

// ---- IN hash sets of rows ----

// rowSet answers `(x1, …, xn) IN (rows)` for the values in columns col …
// col+n-1 of rows, with Postgres's rules: two rows are equal if all their
// items are equal and not NULL, unequal if a pair of items is unequal (and
// not NULL), and otherwise their comparison is NULL. IN is true if a row is
// equal, NULL if none is but a comparison is NULL, and false otherwise (also
// for no rows). Values are compared as inSet compares them (compareValues,
// and a pair that can't be compared is unequal).
//
// Rows whose values all have a key (itemKey) are hashed: the rows without a
// NULL by their keys, and every row also in a group of the rows with the
// same NULL positions (mask). Equality is one lookup; whether a comparison
// is NULL is one lookup per mask, in an index of the group's rows by the
// items where neither side is NULL, built when first needed. A probe whose
// types don't hash like the set's (a string compared with numbers, a float
// with integers) is compared with every row.
type rowSet struct {
	rows   [][]Value
	col, n int
	// noIndex makes the set compare one by one (a result used only once).
	noIndex bool

	built   bool
	classes []uint16 // per item, the eqClass bits of the hashed values
	full    map[string]struct{}
	groups  map[uint64][][]Value
	odd     [][]Value // rows with a value without a key
	// proj indexes the rows of group mask by their keys on the items in
	// on (a subset of mask).
	proj map[[2]uint64]map[string]struct{}
}

// maxRowSetItems is the most items a hashed row has (one mask bit each).
const maxRowSetItems = 64

func (s *rowSet) eval(x []Value, not bool) Value {
	if len(s.rows) == 0 {
		return boolValue(not)
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

// compareRow compares x with row as rowSet does: 1 equal, 0 NULL, -1
// unequal.
func (s *rowSet) compareRow(x, row []Value) int {
	res := 1
	for i, a := range x {
		b := row[s.col+i]
		if a.Null || b.Null {
			res = 0
			continue
		}
		if c, ok := compareValues(a, b); !ok || c != 0 {
			return -1
		}
	}
	return res
}

func (s *rowSet) linear(x []Value) (found, sawNull bool) {
	for _, row := range s.rows {
		switch s.compareRow(x, row) {
		case 1:
			return true, sawNull
		case 0:
			sawNull = true
		}
	}
	return false, sawNull
}

// itemKey is the key of a non-NULL value in a row key; ok is false if it
// has none. A CHAR value has none: it compares without its trailing spaces
// (lengths.go), also with a VARCHAR value that has them.
func itemKey(v Value) (eqClass, string, bool) {
	if v.T.isChar() {
		return clsNone, "", false
	}
	c, k := eqKey(v)
	if c == clsNone {
		return c, "", false
	}
	return c, classTag[c] + k, true
}

func (s *rowSet) build() {
	s.built = true
	s.classes = make([]uint16, s.n)
	s.full = make(map[string]struct{}, len(s.rows))
	s.groups = map[uint64][][]Value{}
	s.proj = map[[2]uint64]map[string]struct{}{}
	all := uint64(1)<<s.n - 1
	var buf []byte
	classes := make([]eqClass, s.n)
next:
	for _, row := range s.rows {
		var mask uint64
		buf = buf[:0]
		for i := 0; i < s.n; i++ {
			v := row[s.col+i]
			if v.Null {
				continue
			}
			c, k, ok := itemKey(v)
			if !ok {
				s.odd = append(s.odd, row)
				continue next
			}
			classes[i] = c
			mask |= 1 << i
			buf = appendKey(buf, k)
		}
		for i := 0; i < s.n; i++ {
			if mask&(1<<i) != 0 {
				s.classes[i] |= 1 << classes[i]
			}
		}
		if mask == all {
			s.full[string(buf)] = struct{}{}
		}
		s.groups[mask] = append(s.groups[mask], row)
	}
	subqueryStats.inSets.Add(1)
}

// hashable reports whether a value of class c compares with the values of
// classes (a set of eqClass bits) exactly by their keys.
func hashable(c eqClass, classes uint16) bool {
	for d := clsNum; d < numClasses; d++ {
		if classes&(1<<d) != 0 && !(c == d || (exactNum(c) && exactNum(d))) {
			return false
		}
	}
	return true
}

// key returns the key of x's items in on (a mask of non-NULL items).
func (s *rowSet) key(keys []string, on uint64) string {
	var buf []byte
	for i := 0; i < s.n; i++ {
		if on&(1<<i) != 0 {
			buf = appendKey(buf, keys[i])
		}
	}
	return string(buf)
}

// projection returns the index of group mask's rows by their keys on the
// items in on.
func (s *rowSet) projection(mask, on uint64) map[string]struct{} {
	k := [2]uint64{mask, on}
	if p, ok := s.proj[k]; ok {
		return p
	}
	p := map[string]struct{}{}
	keys := make([]string, s.n)
	for _, row := range s.groups[mask] {
		for i := 0; i < s.n; i++ {
			if on&(1<<i) != 0 {
				_, keys[i], _ = itemKey(row[s.col+i])
			}
		}
		p[s.key(keys, on)] = struct{}{}
	}
	s.proj[k] = p
	return p
}

// contains reports whether a row of the set equals x, and whether one
// compares as NULL with it.
func (s *rowSet) contains(x []Value) (found, sawNull bool) {
	if s.noIndex || len(s.rows) <= inSetLinear || s.n > maxRowSetItems {
		return s.linear(x)
	}
	if !s.built {
		s.build()
	}
	keys := make([]string, s.n)
	var mask uint64
	for i, v := range x {
		if v.Null {
			continue
		}
		c, k, ok := itemKey(v)
		if !ok || !hashable(c, s.classes[i]) {
			return s.linear(x)
		}
		keys[i] = k
		mask |= 1 << i
	}
	all := uint64(1)<<s.n - 1
	if mask == all {
		if _, ok := s.full[s.key(keys, all)]; ok {
			return true, false
		}
	}
	for _, row := range s.odd {
		switch s.compareRow(x, row) {
		case 1:
			return true, false
		case 0:
			sawNull = true
		}
	}
	if sawNull {
		return false, true
	}
	// A row compares as NULL with x if it agrees with x on the items where
	// neither is NULL, and one of them has a NULL.
	for g := range s.groups {
		if g == all && mask == all {
			continue // equal or unequal
		}
		on := g & mask
		if _, ok := s.projection(g, on)[s.key(keys, on)]; ok {
			return false, true
		}
	}
	return false, false
}

// cachedRowSet returns the hash set of an uncorrelated row IN subquery's
// rows, built once per statement.
func (e *executor) cachedRowSet(sq *Subquery, rows [][]Value, n int) *rowSet {
	if s, ok := e.cache.rowSets[sq]; ok {
		return s
	}
	s := &rowSet{rows: rows, n: n}
	e.cache.rowSets[sq] = s
	return s
}

// evalRowIn evaluates (x1, …) [NOT] IN (SELECT …) over the subquery's rows.
func (env *evalEnv) evalRowIn(sq *Subquery, r *RowExpr, rows [][]Value) (Value, error) {
	x, err := env.evalItems(r.Items)
	if err != nil {
		return Value{}, err
	}
	set := &rowSet{rows: rows, n: len(x), noIndex: true}
	if !sq.correlated {
		// The same rows for every outer row: probe a hash set.
		set = env.exec.cachedRowSet(sq, rows, len(x))
	}
	return set.eval(x, sq.Not), nil
}

// evalRowQuantified evaluates (x1, …) op ANY / ALL (SELECT …): ANY is true if
// a row comparison is, ALL false if one is; otherwise NULL if one is NULL.
func (env *evalEnv) evalRowQuantified(sq *Subquery, r *RowExpr, rows [][]Value) (Value, error) {
	all := sq.Kind == SubqueryAll
	if len(rows) == 0 {
		return boolValue(all), nil
	}
	x, err := env.evalItems(r.Items)
	if err != nil {
		return Value{}, err
	}
	sawNull := false
	for _, row := range rows {
		v, err := rowCompareValues(sq.Op, x, row[:len(x)])
		if err != nil {
			return Value{}, err
		}
		b, ok := truthy(v)
		switch {
		case !ok:
			sawNull = true
		case b != all:
			return boolValue(b), nil
		}
	}
	if sawNull {
		return nullValue(typeBool), nil
	}
	return boolValue(all), nil
}

// ---- index pushdown ----

// rowInTerm turns an uncorrelated `(c1, …) IN (SELECT …)` conjunct into an
// index query: one that matches nothing when no row of the subquery can be
// equal to a row (it has none, or each has a NULL), and otherwise a union of
// the values of each indexed column among the items that has at most
// maxUnionTerms distinct values in the subquery's rows without a NULL, as
// an index lookup join does. The conjunct itself is still checked on the
// rows.
func (e *executor) rowInTerm(ctx context.Context, sq *Subquery, r *RowExpr, meta *tableMeta, env *evalEnv) (string, bool, error) {
	rows, err := e.subqueryRows(ctx, sq, env)
	if err != nil {
		return "", false, err
	}
	var complete [][]Value
	for _, row := range rows {
		ok := true
		for i := range r.Items {
			if row[i].Null {
				ok = false // never equal to a row
				break
			}
		}
		if ok {
			complete = append(complete, row)
		}
	}
	if len(complete) == 0 {
		return noMatchQuery, true, nil
	}
	var terms []string
	for i, it := range r.Items {
		ref, ok := it.(*ColumnRef)
		if !ok || ref.Outer != 0 {
			continue
		}
		cm, ok := meta.column(ref.Name)
		if !ok || !cm.Indexed || !simpleName(cm.field()) {
			continue
		}
		var values []Value
		seen := map[string]bool{}
		for _, row := range complete {
			v := row[i]
			if k, ok := joinKey(v); ok {
				if seen[k] {
					continue
				}
				seen[k] = true
			}
			if values = append(values, v); len(values) > maxUnionTerms {
				break
			}
		}
		if len(values) > maxUnionTerms {
			continue
		}
		if q, ok := unionQuery(cm, values); ok {
			terms = append(terms, q)
		}
	}
	if len(terms) == 0 {
		return "", false, nil
	}
	return strings.Join(terms, " "), true, nil
}
