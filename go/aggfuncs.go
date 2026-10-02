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

// Aggregate functions beyond COUNT / SUM / AVG / MIN / MAX, and the
// modifiers of aggregate calls:
//
//   - STRING_AGG(x, sep [ORDER BY …]) and LISTAGG(x [, sep]) [WITHIN GROUP
//     (ORDER BY …)] concatenate the non-NULL values.
//   - BOOL_OR, BOOL_AND / EVERY and ANY_VALUE.
//   - VAR_SAMP / VARIANCE, VAR_POP, STDDEV_SAMP / STDDEV, STDDEV_POP. The
//     values and their squares are summed exactly (exactsum.go), whatever
//     their type, so the result is the correctly rounded double.
//   - The ordered-set aggregates PERCENTILE_CONT / PERCENTILE_DISC(fraction)
//     and MODE() WITHIN GROUP (ORDER BY x), and MEDIAN(x), which is
//     PERCENTILE_CONT(0.5). The direct argument (the fraction) is evaluated
//     once per group.
//   - agg(…) FILTER (WHERE cond) aggregates only the rows where cond is true.
//
// None of them runs in RediSearch except BOOL_OR / BOOL_AND over a boolean
// column (MAX / MIN of the stored 0 and 1): its STDDEV and QUANTILE reducers
// are approximate, and FIRST_VALUE doesn't skip missing values. A FILTER is
// always evaluated by the driver. As window functions (window.go), all but
// the ordered-set aggregates and MEDIAN work over frames.

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// statFuncs are the variance and standard deviation aggregates.
var statFuncs = map[string]bool{
	"STDDEV": true, "STDDEV_SAMP": true, "STDDEV_POP": true, "VARIANCE": true, "VAR_SAMP": true, "VAR_POP": true,
}

// checkCall checks the modifiers of a function call without OVER: only
// aggregates take ORDER BY, WITHIN GROUP and FILTER, and only window
// functions take IGNORE NULLS / RESPECT NULLS.
func checkCall(f *Func) error {
	if aggregateFuncs[f.Name] {
		return checkAggregate(f, false)
	}
	fail := func(what string) error {
		return errorf(adbc.StatusInvalidArgument, "%s specified, but %s is not an aggregate function", what, f.Name)
	}
	switch {
	case f.Filter != nil:
		return fail("FILTER")
	case f.WithinGroup:
		return fail("WITHIN GROUP")
	case len(f.OrderBy) > 0:
		return fail("ORDER BY")
	case f.Nulls != NullsUnspecified:
		return errorf(adbc.StatusInvalidArgument, "function %s does not allow RESPECT/IGNORE NULLS", f.Name)
	}
	return nil
}

// checkAggregate checks the shape of an aggregate call: its arguments and
// modifiers. A window call (over) may aggregate aggregates, as in
// SUM(COUNT(*)) OVER (…).
func checkAggregate(f *Func, over bool) error {
	fail := func(format string, args ...any) error {
		return errorf(adbc.StatusInvalidArgument, format, args...)
	}
	if d, ok := lookupFunc(f.Name); ok {
		if err := d.argsError(f); err != nil {
			return err
		}
	}
	if f.Nulls != NullsUnspecified {
		return fail("aggregate functions do not accept RESPECT/IGNORE NULLS")
	}
	switch {
	case orderedSetFuncs[f.Name]:
		switch {
		case !f.WithinGroup:
			return fail("WITHIN GROUP is required for ordered-set aggregate %s", f.Name)
		case f.Distinct:
			return fail("cannot use DISTINCT with WITHIN GROUP")
		case len(f.OrderBy) != 1:
			return fail("%s expects one ORDER BY expression in WITHIN GROUP", f.Name)
		}
	case f.WithinGroup && f.Name != "STRING_AGG" && f.Name != "LISTAGG":
		// LISTAGG … WITHIN GROUP (ORDER BY …), as in Snowflake and Oracle, and
		// STRING_AGG … WITHIN GROUP, as in SQL Server, order the values.
		return fail("%s is not an ordered-set aggregate, so it cannot have WITHIN GROUP", f.Name)
	case f.Name == "COUNT" && len(f.Args) > 1 && !f.Distinct:
		return fail("COUNT of more than one argument requires DISTINCT")
	case jsonAggregates[f.Name]:
		// Checked like the other JSON functions (json.go).
		if err := checkJSONArgs(f); err != nil {
			return fail("%v", err)
		}
	case f.Name == "MEDIAN" && f.Distinct:
		return fail("DISTINCT is not supported for MEDIAN")
	}
	if !over {
		for _, x := range f.Args {
			if isAggregate(x) {
				return fail("aggregate function calls cannot be nested")
			}
		}
	}
	for _, o := range f.OrderBy {
		if !over && isAggregate(o.Expr) {
			return fail("aggregate function calls cannot be nested")
		}
		if containsWindow(o.Expr) {
			return fail("aggregate function calls cannot contain window function calls")
		}
	}
	if f.Filter != nil {
		if !over && isAggregate(f.Filter) {
			return fail("aggregate functions are not allowed in FILTER")
		}
		if containsWindow(f.Filter) {
			return fail("window functions are not allowed in FILTER")
		}
	}
	return nil
}

// aggregateType checks an aggregate call (over: one with OVER) and returns
// its result type; ok is false if f is not an aggregate.
func aggregateType(f *Func, cols map[string]ColType, params []ColType, over bool) (ColType, bool, error) {
	if !aggregateFuncs[f.Name] {
		return ColType{}, false, nil
	}
	if err := checkAggregate(f, over); err != nil {
		return ColType{}, true, err
	}
	typesOf := func(exprs []Expr) ([]ColType, error) {
		out := make([]ColType, len(exprs))
		for i, x := range exprs {
			t, err := inferType(x, cols, params)
			if err != nil {
				return nil, err
			}
			out[i] = t
		}
		return out, nil
	}
	args, err := typesOf(f.Args)
	if err != nil {
		return ColType{}, true, err
	}
	keyExprs := make([]Expr, len(f.OrderBy))
	for i, o := range f.OrderBy {
		keyExprs[i] = o.Expr
	}
	keys, err := typesOf(keyExprs)
	if err != nil {
		return ColType{}, true, err
	}
	if f.Filter != nil {
		ft, err := inferType(f.Filter, cols, params)
		if err != nil {
			return ColType{}, true, err
		}
		if ft.Kind != KindBool && ft.Kind != KindNull {
			return ColType{}, true, errorf(adbc.StatusInvalidArgument, "argument of FILTER must be type boolean, not type %s", ft.SQLName())
		}
	}
	if f.Distinct {
		for _, k := range keyExprs {
			found := false
			for _, a := range f.Args {
				found = found || exprEqual(k, a)
			}
			if !found {
				return ColType{}, true, errorf(adbc.StatusInvalidArgument, "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list")
			}
		}
	}
	// The argument types the registry requires (funcs.go).
	noSuch := func(t ColType) (ColType, bool, error) {
		return ColType{}, true, noSuchFunction(f.Name, false, []ColType{t})
	}
	d, _ := lookupFunc(f.Name)
	for i, at := range d.args {
		if i >= len(args) || at.accepts(args[i]) {
			continue
		}
		if d.orderedSet {
			return ColType{}, true, errorf(adbc.StatusInvalidArgument, "the fraction of %s must be a number, not %s", f.Name, args[i].SQLName())
		}
		return noSuch(args[i])
	}
	if d.orderedSet && len(keys) == 1 && !d.within.accepts(keys[0]) {
		return noSuch(keys[0])
	}
	// PERCENTILE_CONT interpolates numbers (as doubles) and intervals.
	contType := func(t ColType) ColType {
		if t.Kind == KindInterval {
			return typeInterval
		}
		return typeFloat64
	}
	switch name := f.Name; {
	case name == "STRING_AGG" || name == "LISTAGG":
		return typeString, true, nil
	case name == "BOOL_OR" || name == "BOOL_AND" || name == "EVERY":
		return typeBool, true, nil
	case name == "ANY_VALUE":
		return args[0].withoutLength(), true, nil
	case statFuncs[name]:
		return typeFloat64, true, nil
	case name == "PERCENTILE_CONT":
		return contType(keys[0]), true, nil
	case name == "PERCENTILE_DISC" || name == "MODE":
		return keys[0].withoutLength(), true, nil
	case name == "MEDIAN":
		return contType(args[0]), true, nil
	}
	return inferFuncType(f, args), true, nil
}

// usesOrder reports whether an aggregate's result depends on its ORDER BY.
func usesOrder(f *Func) bool {
	return f.Name == "STRING_AGG" || f.Name == "LISTAGG" || orderedSetFuncs[f.Name] || jsonAggregates[f.Name]
}

// aggInput evaluates the input of an aggregate call for the row in env into
// in: its arguments, then the ORDER BY keys it uses. An ordered-set
// aggregate's input is its WITHIN GROUP value alone: its direct arguments
// are evaluated once per group.
func aggInput(env *evalEnv, f *Func, in []Value) ([]Value, error) {
	exprs := f.Args
	if orderedSetFuncs[f.Name] {
		exprs = nil
	}
	for _, x := range exprs {
		v, err := env.eval(x)
		if err != nil {
			return nil, err
		}
		in = append(in, v)
	}
	if usesOrder(f) {
		for _, o := range f.OrderBy {
			v, err := env.eval(o.Expr)
			if err != nil {
				return nil, err
			}
			in = append(in, v)
		}
	}
	return in, nil
}

// aggState is the state of one of this file's aggregates for one group.
type aggState interface {
	// add adds a row's input; in is reused for the next row.
	add(in []Value) error
	// result is computed with env at the group's representative row.
	result(env *evalEnv, t ColType) (Value, error)
}

func newAggState(f *Func) aggState {
	switch {
	case f.Name == "STRING_AGG" || f.Name == "LISTAGG":
		return &stringAgg{fn: f}
	case f.Name == "BOOL_OR" || f.Name == "BOOL_AND" || f.Name == "EVERY":
		return &boolAgg{and: f.Name != "BOOL_OR"}
	case f.Name == "ANY_VALUE":
		return &anyValueAgg{}
	case statFuncs[f.Name]:
		return &statAgg{name: f.Name, acc: &exactSum{squares: true}}
	case orderedSetFuncs[f.Name] || f.Name == "MEDIAN":
		return &orderedSetAgg{fn: f}
	case jsonAggregates[f.Name]:
		return &jsonAgg{fn: f}
	}
	return nil
}

// addRow adds the row in env to the aggregate, unless FILTER rejects it.
func (a *accumulator) addRow(env *evalEnv) error {
	f := a.fn
	if f.Filter != nil {
		v, err := env.eval(f.Filter)
		if err != nil {
			return err
		}
		if b, ok := truthy(v); !ok || !b {
			return nil
		}
	}
	if a.ext == nil && !f.Distinct {
		// COUNT, SUM, AVG, MIN or MAX of one value.
		var v Value
		if !f.Star {
			var err error
			if v, err = env.eval(f.Args[0]); err != nil {
				return err
			}
		}
		a.add(v)
		return nil
	}
	in, err := aggInput(env, f, a.in[:0])
	if err != nil {
		return err
	}
	a.in = in
	if f.Distinct {
		// Only the arguments count (the ORDER BY keys are among them).
		var key strings.Builder
		for _, v := range in[:len(f.Args)] {
			if v.Null {
				if f.Name == "COUNT" {
					return nil
				}
				key.WriteString("N\x00")
				continue
			}
			t := v.Text()
			key.WriteString(v.T.Kind.String())
			key.WriteString(strconv.Itoa(len(t)))
			key.WriteByte(':')
			key.WriteString(t)
		}
		if a.distinct == nil {
			a.distinct = map[string]bool{}
		}
		if a.distinct[key.String()] {
			return nil
		}
		a.distinct[key.String()] = true
	}
	if a.ext != nil {
		return a.ext.add(in)
	}
	// COUNT(DISTINCT a, b) counts the rows where none is NULL.
	v := in[0]
	for _, x := range in[1:] {
		v.Null = v.Null || x.Null
	}
	a.add(v)
	return nil
}

// stringAgg is STRING_AGG / LISTAGG: the non-NULL values in ORDER BY order
// (input order without one), each but the first preceded by its own row's
// separator, as in PostgreSQL.
type stringAgg struct {
	fn    *Func
	items [][]Value // value, [separator,] ORDER BY keys
}

func (s *stringAgg) add(in []Value) error {
	if !in[0].Null {
		s.items = append(s.items, slices.Clone(in))
	}
	return nil
}

func (s *stringAgg) result(env *evalEnv, t ColType) (Value, error) {
	if len(s.items) == 0 {
		return nullValue(typeString), nil
	}
	nargs := len(s.fn.Args)
	order := make([]planOrder, 0, len(s.fn.OrderBy)+nargs)
	for _, o := range s.fn.OrderBy {
		order = append(order, planOrder{desc: o.Desc, nullsFirst: o.Nulls == NullsFirst})
	}
	if s.fn.Distinct {
		// As in PostgreSQL, DISTINCT sorts the values: by the ORDER BY keys,
		// then by the arguments.
		for range nargs {
			order = append(order, planOrder{})
		}
		for i, item := range s.items {
			s.items[i] = append(slices.Clip(item), item[:nargs]...)
		}
	}
	if len(order) > 0 {
		sort.SliceStable(s.items, func(i, j int) bool { return lessKeys(s.items[i][nargs:], s.items[j][nargs:], order) })
	}
	var b strings.Builder
	z := env.zone()
	for i, item := range s.items {
		if i > 0 && nargs > 1 && !item[1].Null {
			b.WriteString(item[1].textIn(z))
		}
		b.WriteString(item[0].textIn(z))
	}
	return stringValue(b.String()), nil
}

// boolAgg is BOOL_OR, or BOOL_AND / EVERY (and).
type boolAgg struct {
	and      bool
	n, trues int64
}

func (b *boolAgg) add(in []Value) error {
	if !in[0].Null {
		b.n++
		if in[0].I != 0 {
			b.trues++
		}
	}
	return nil
}

func (b *boolAgg) result(*evalEnv, ColType) (Value, error) {
	return boolResult(b.and, b.n, b.trues), nil
}

func boolResult(and bool, n, trues int64) Value {
	switch {
	case n == 0:
		return nullValue(typeBool)
	case and:
		return boolValue(trues == n)
	}
	return boolValue(trues > 0)
}

// anyValueAgg is ANY_VALUE: the first non-NULL value, as in PostgreSQL.
type anyValueAgg struct {
	v   Value
	has bool
}

func (a *anyValueAgg) add(in []Value) error {
	if !a.has && !in[0].Null {
		a.v, a.has = in[0], true
	}
	return nil
}

func (a *anyValueAgg) result(_ *evalEnv, t ColType) (Value, error) {
	if !a.has {
		return nullValue(t), nil
	}
	return a.v, nil
}

type statAgg struct {
	name string
	acc  *exactSum
}

func (s *statAgg) add(in []Value) error {
	if !in[0].Null {
		s.acc.add(in[0])
	}
	return nil
}

func (s *statAgg) result(*evalEnv, ColType) (Value, error) { return s.acc.varianceValue(s.name), nil }

// orderedSetAgg is PERCENTILE_CONT, PERCENTILE_DISC, MODE or MEDIAN: it
// keeps the non-NULL values and sorts them once the group is complete.
type orderedSetAgg struct {
	fn   *Func
	vals []Value
}

func (o *orderedSetAgg) add(in []Value) error {
	if !in[0].Null {
		o.vals = append(o.vals, in[0])
	}
	return nil
}

func (o *orderedSetAgg) result(env *evalEnv, t ColType) (Value, error) {
	f := o.fn
	desc := len(f.OrderBy) > 0 && f.OrderBy[0].Desc
	frac := 0.5 // MEDIAN
	if f.Name == "PERCENTILE_CONT" || f.Name == "PERCENTILE_DISC" {
		// The fraction is checked even for an empty group, as in PostgreSQL.
		v, err := env.eval(f.Args[0])
		if err != nil {
			return Value{}, err
		}
		if v.Null {
			return nullValue(t), nil
		}
		if frac, _ = v.asFloat(); frac < 0 || frac > 1 || math.IsNaN(frac) {
			return Value{}, fmt.Errorf("percentile value %s is not between 0 and 1", strconv.FormatFloat(frac, 'g', -1, 64))
		}
	}
	n := len(o.vals)
	if n == 0 {
		return nullValue(t), nil
	}
	if t.Kind == KindFloat64 {
		// PERCENTILE_CONT and MEDIAN of numbers work on doubles.
		for i, v := range o.vals {
			fv, _ := v.asFloat()
			o.vals[i] = floatValue(typeFloat64, fv)
		}
	}
	sort.SliceStable(o.vals, func(i, j int) bool {
		c, _ := compareValues(o.vals[i], o.vals[j])
		if desc {
			return c > 0
		}
		return c < 0
	})
	switch f.Name {
	case "MODE":
		// The most frequent value; the first in sort order among ties.
		best, bestN, run := 0, 0, 0
		for i := range o.vals {
			if i > 0 {
				if c, _ := compareValues(o.vals[i], o.vals[i-1]); c != 0 {
					run = 0
				}
			}
			run++
			if run > bestN {
				best, bestN = i, run
			}
		}
		return o.vals[best], nil
	case "PERCENTILE_DISC":
		// The first value whose position in the sort is at least frac.
		k := int(math.Ceil(frac * float64(n)))
		return o.vals[max(k, 1)-1], nil
	}
	// PERCENTILE_CONT / MEDIAN interpolate between the values around
	// position frac × (n - 1), with PostgreSQL's formula. The conversion
	// keeps the product from being fused with pos - first.
	pos := float64(frac * float64(n-1))
	first, second := math.Floor(pos), math.Ceil(pos)
	lo, hi := o.vals[int(first)], o.vals[int(second)]
	if first == second {
		return lo, nil
	}
	return lerp(lo, hi, pos-first)
}

// lerp returns lo + (hi - lo) × p for doubles or intervals.
func lerp(lo, hi Value, p float64) (Value, error) {
	if lo.T.Kind == KindFloat64 {
		// The conversion keeps the product from being fused into an FMA.
		return floatValue(typeFloat64, lo.F+float64((hi.F-lo.F)*p)), nil
	}
	d, err := binaryOp("-", hi, lo, utcZone)
	if err != nil {
		return Value{}, err
	}
	if d, err = scaleInterval(d, p); err != nil {
		return Value{}, err
	}
	return binaryOp("+", lo, d, utcZone)
}
