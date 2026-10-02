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

// Literal IN lists.
//
// `x IN (v1, …, vN)` is parsed as `x = v1 OR … OR x = vN`, all sharing one x
// node. Pushdown turns that into an index union when it can (up to
// maxUnionTerms values on an indexed column). When the driver evaluates it,
// each row used to compare x with every value. A long list of constants now
// probes a hash set instead (the inSet of IN subqueries), built once per
// statement.
//
// The hash set only answers rows whose x can't fail to compare with any of
// the values: `=` raises an error for types it can't compare, and the OR
// chain stops at the first match, so a row whose type might hit an error is
// evaluated by the chain itself, exactly as before.

// inListMin is the list length from which a hash set pays off.
const inListMin = 16

type inList struct {
	x   Expr
	set *inSet
	// types are the distinct types of the non-NULL values; safe caches, by
	// x's type, whether x compares with all of them without an error.
	types []ColType
	safe  map[ColType]bool
}

// literalInList recognizes the OR chain of a literal IN list with at least
// inListMin constant values, returning x and the values (in list order).
func literalInList(or *Binary) (Expr, []Expr, bool) {
	var rev []Expr
	var node Expr = or
	for {
		b, ok := node.(*Binary)
		if !ok || b.Op != "OR" {
			rev = append(rev, node)
			break
		}
		rev = append(rev, b.R)
		node = b.L
	}
	if len(rev) < inListMin {
		return nil, nil, false
	}
	var x Expr
	values := make([]Expr, len(rev))
	for i, leaf := range rev {
		eq, ok := leaf.(*Binary)
		if !ok || eq.Op != "=" || !isConstant(eq.R) {
			return nil, nil, false
		}
		if x == nil {
			x = eq.L
		} else if eq.L != x {
			return nil, nil, false // not one IN list
		}
		values[len(rev)-1-i] = eq.R
	}
	if hasVolatile(x) || hasSubquery(x) {
		return nil, nil, false // x is evaluated once here, once per value before
	}
	return x, values, true
}

// evalInList evaluates an IN list's OR chain with its hash set; ok is false
// when the chain must be evaluated as written.
func (env *evalEnv) evalInList(or *Binary) (Value, bool, error) {
	e := env.exec
	if e == nil {
		return Value{}, false, nil
	}
	e.ensureCache()
	l, seen := e.cache.inLists[or]
	if !seen {
		l = env.buildInList(or)
		e.cache.inLists[or] = l
	}
	if l == nil {
		return Value{}, false, nil
	}
	x, err := env.eval(l.x)
	if err != nil {
		return Value{}, true, err
	}
	if !x.Null && !l.safeFor(x.T) {
		return Value{}, false, nil
	}
	subqueryStats.inLists.Add(1)
	return l.set.eval(x, false), true, nil
}

// buildInList evaluates the list's values once. It returns nil (evaluate
// the chain) if or isn't such a list or a value fails to evaluate.
func (env *evalEnv) buildInList(or *Binary) *inList {
	x, exprs, ok := literalInList(or)
	if !ok {
		// Don't look at the chain's inner OR nodes again either.
		for node, _ := or.L.(*Binary); node != nil && node.Op == "OR"; node, _ = node.L.(*Binary) {
			env.exec.cache.inLists[node] = nil
		}
		return nil
	}
	l := &inList{x: x, safe: map[ColType]bool{}}
	rows := make([][]Value, len(exprs))
	seen := map[ColType]bool{}
	for i, v := range exprs {
		val, err := env.eval(v)
		if err != nil {
			return nil
		}
		rows[i] = []Value{val}
		if !val.Null && !seen[val.T] {
			seen[val.T] = true
			l.types = append(l.types, val.T)
		}
	}
	l.set = &inSet{rows: rows, zone: env.zone()}
	// The chain's inner OR nodes are only reached through this one.
	for node, _ := or.L.(*Binary); node != nil && node.Op == "OR"; node, _ = node.L.(*Binary) {
		env.exec.cache.inLists[node] = nil
	}
	return l
}

func (l *inList) safeFor(t ColType) bool {
	if ok, seen := l.safe[t]; seen {
		return ok
	}
	ok := true
	for _, vt := range l.types {
		if !compareNeverFails(t, vt) {
			ok = false
			break
		}
	}
	l.safe[t] = ok
	return ok
}

// compareNeverFails reports whether compareValues succeeds for every pair of
// non-NULL values of types a and b (string-to-number conversions, mixed
// time units and date/timestamp mixes can fail, so they are excluded).
func compareNeverFails(a, b ColType) bool {
	ak, bk := a.Kind, b.Kind
	intOrBool := func(k Kind) bool { return k.isInteger() || k == KindBool }
	switch {
	case ak == KindInterval && bk == KindInterval:
		return true
	case (ak == KindString || ak == KindBinary) && (bk == KindString || bk == KindBinary):
		return true
	case intOrBool(ak) && intOrBool(bk):
		return true
	case ak == KindDecimal && (bk.isNumeric() || bk == KindBool), bk == KindDecimal && (ak.isNumeric() || ak == KindBool):
		return true
	case ak.isNumeric() && bk.isNumeric():
		return true
	case ak == KindDate && bk == KindDate:
		return true
	case (ak == KindTime || ak == KindTimestamp) && ak == bk && a.Unit == b.Unit && a.TZ == b.TZ:
		return true
	}
	return false
}
