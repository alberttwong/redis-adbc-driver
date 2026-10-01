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

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strings"
	"unicode/utf8"
)

// evalEnv supplies column values (keyed by canonical column name) and bound
// parameters to expression evaluation.
type evalEnv struct {
	ctx   context.Context
	exec  *executor
	outer *evalEnv // enclosing query's row, for correlated references

	row    map[string]Value
	types  map[string]ColType
	params []Value
	// aggs holds the reduced value of each aggregate call when evaluating
	// over a group (SELECT items, HAVING and ORDER BY of grouped queries).
	aggs map[*Func]Value
	// win holds the value of each window function call for every row of
	// the window input; winRow is the current row's position in it.
	win    map[*WindowFunc][]Value
	winRow int
	// action is merge_action() while MERGE … RETURNING evaluates a row.
	action string
}

func isAggregate(e Expr) bool {
	found := false
	walkExpr(e, func(x Expr) {
		if f, ok := x.(*Func); ok && aggregateFuncs[f.Name] {
			found = true
		}
	})
	return found
}

func walkExpr(e Expr, fn func(Expr)) {
	if e == nil {
		return
	}
	fn(e)
	switch x := e.(type) {
	case *Unary:
		walkExpr(x.X, fn)
	case *Binary:
		walkExpr(x.L, fn)
		walkExpr(x.R, fn)
	case *IsNull:
		walkExpr(x.X, fn)
	case *Cast:
		walkExpr(x.X, fn)
		walkExpr(x.OnError, fn)
	case *Func:
		for _, a := range x.Args {
			walkExpr(a, fn)
		}
		for _, o := range x.OrderBy {
			walkExpr(o.Expr, fn)
		}
		walkExpr(x.Filter, fn)
	case *Case:
		walkExpr(x.Operand, fn)
		for _, w := range x.Whens {
			walkExpr(w.When, fn)
			walkExpr(w.Then, fn)
		}
		walkExpr(x.Else, fn)
	case *Subquery:
		// The body is a separate scope; only the IN operand belongs here.
		walkExpr(x.X, fn)
	case *RowColumn:
		walkExpr(x.Sub, fn)
	case *WindowFunc:
		// The call itself is not visited as a *Func: SUM(x) OVER (…) is not
		// an aggregate, but aggregates in its arguments (SUM(COUNT(*))) are.
		for _, c := range x.children() {
			walkExpr(c, fn)
		}
	}
}

// walkOutsideAggregates visits the nodes of e that are not inside an
// aggregate call.
func walkOutsideAggregates(e Expr, fn func(Expr)) {
	if e == nil {
		return
	}
	if f, ok := e.(*Func); ok && aggregateFuncs[f.Name] {
		return
	}
	fn(e)
	switch x := e.(type) {
	case *Unary:
		walkOutsideAggregates(x.X, fn)
	case *Binary:
		walkOutsideAggregates(x.L, fn)
		walkOutsideAggregates(x.R, fn)
	case *IsNull:
		walkOutsideAggregates(x.X, fn)
	case *Cast:
		walkOutsideAggregates(x.X, fn)
		walkOutsideAggregates(x.OnError, fn)
	case *Func:
		for _, a := range x.Args {
			walkOutsideAggregates(a, fn)
		}
	case *Case:
		walkOutsideAggregates(x.Operand, fn)
		for _, w := range x.Whens {
			walkOutsideAggregates(w.When, fn)
			walkOutsideAggregates(w.Then, fn)
		}
		walkOutsideAggregates(x.Else, fn)
	case *Subquery:
		walkOutsideAggregates(x.X, fn)
	case *WindowFunc:
		for _, c := range x.children() {
			walkOutsideAggregates(c, fn)
		}
	}
}

// columnRefs returns the distinct column names referenced by an expression.
func columnRefs(e Expr, out map[string]bool) {
	walkExpr(e, func(x Expr) {
		if c, ok := x.(*ColumnRef); ok && c.Outer == 0 {
			out[c.Name] = true
		}
	})
}

func truthy(v Value) (bool, bool) {
	if v.Null {
		return false, false
	}
	switch v.T.Kind {
	case KindBool, KindInt16, KindInt32, KindInt64:
		return v.I != 0, true
	}
	return false, false
}

func (env *evalEnv) eval(e Expr) (Value, error) {
	switch x := e.(type) {
	case *Literal:
		return x.V, nil
	case *Param:
		if x.Index >= len(env.params) {
			return Value{}, fmt.Errorf("parameter %d is not bound", x.Index+1)
		}
		return env.params[x.Index], nil
	case *ColumnRef:
		if x.Outer > 0 {
			return env.lookupUp(x.Name, x.Outer)
		}
		if v, ok := env.row[x.Name]; ok {
			return v, nil
		}
		if t, ok := env.types[x.Name]; ok {
			return nullValue(t), nil
		}
		return Value{}, fmt.Errorf("unknown column %q", x.Name)
	case *Cast:
		v, err := env.eval(x.X)
		if err != nil {
			return Value{}, err
		}
		c, err := Coerce(v, x.T)
		if err != nil && x.OnError != nil && castable(v.T, x.T) {
			// TRY_CAST and DEFAULT … ON CONVERSION ERROR: a value that
			// cannot be converted gives the fallback, but types that never
			// convert (DATE to BOOLEAN) are still an error.
			d, err := env.eval(x.OnError)
			if err != nil {
				return Value{}, err
			}
			return Coerce(d, x.T)
		}
		return c, err
	case *IsNull:
		v, err := env.eval(x.X)
		if err != nil {
			return Value{}, err
		}
		return boolValue(v.Null != x.Not), nil
	case *Unary:
		v, err := env.eval(x.X)
		if err != nil {
			return Value{}, err
		}
		if x.Op == "NOT" {
			b, ok := truthy(v)
			if !ok {
				return nullValue(typeBool), nil
			}
			return boolValue(!b), nil
		}
		return negate(v)
	case *Binary:
		switch x.Op {
		case "AND", "OR":
			if x.Op == "OR" {
				if v, ok, err := env.evalInList(x); ok {
					return v, err
				}
			}
			l, err := env.eval(x.L)
			if err != nil {
				return Value{}, err
			}
			lb, lok := truthy(l)
			if x.Op == "AND" && lok && !lb {
				return boolValue(false), nil
			}
			if x.Op == "OR" && lok && lb {
				return boolValue(true), nil
			}
			r, err := env.eval(x.R)
			if err != nil {
				return Value{}, err
			}
			rb, rok := truthy(r)
			if x.Op == "AND" {
				if rok && !rb {
					return boolValue(false), nil
				}
				if lok && rok {
					return boolValue(true), nil
				}
				return nullValue(typeBool), nil
			}
			if rok && rb {
				return boolValue(true), nil
			}
			if lok && rok {
				return boolValue(false), nil
			}
			return nullValue(typeBool), nil
		}
		l, err := env.eval(x.L)
		if err != nil {
			return Value{}, err
		}
		r, err := env.eval(x.R)
		if err != nil {
			return Value{}, err
		}
		return binaryOp(x.Op, l, r)
	case *Func:
		return env.evalFunc(x)
	case *Case:
		return env.evalCase(x)
	case *Subquery:
		return env.evalSubquery(x)
	case *RowColumn:
		return env.evalRowColumn(x)
	case *WindowFunc:
		vals, ok := env.win[x]
		if !ok {
			return Value{}, fmt.Errorf("window function %s is not allowed here", x.Func.Name)
		}
		return vals[env.winRow], nil
	}
	return Value{}, fmt.Errorf("unsupported expression %T", e)
}

func (env *evalEnv) evalCase(c *Case) (Value, error) {
	var operand Value
	if c.Operand != nil {
		v, err := env.eval(c.Operand)
		if err != nil {
			return Value{}, err
		}
		operand = v
	}
	for _, w := range c.Whens {
		v, err := env.eval(w.When)
		if err != nil {
			return Value{}, err
		}
		if c.Operand != nil {
			if v, err = binaryOp("=", operand, v); err != nil {
				return Value{}, err
			}
		}
		if b, ok := truthy(v); ok && b {
			return env.eval(w.Then)
		}
	}
	if c.Else != nil {
		return env.eval(c.Else)
	}
	return nullValue(typeNull), nil
}

// concatValues joins the text of the non-NULL values with sep.
func concatValues(sep string, args []Value) Value {
	var b strings.Builder
	first := true
	for _, a := range args {
		if a.Null {
			continue
		}
		if !first {
			b.WriteString(sep)
		}
		b.WriteString(a.Text())
		first = false
	}
	return stringValue(b.String())
}

// evalFunc evaluates a call. The registry (funcs.go) says which code
// evaluates it; binding has checked its name and arguments.
func (env *evalEnv) evalFunc(f *Func) (Value, error) {
	d, ok := callee(f)
	if !ok {
		return Value{}, fmt.Errorf("unsupported function %s", f.Name)
	}
	switch d.kind {
	case aggregateKind:
		if v, ok := env.aggs[f]; ok {
			return v, nil
		}
		return Value{}, fmt.Errorf("aggregate %s is not allowed here", f.Name)
	case windowKind:
		return Value{}, fmt.Errorf("window function %s requires an OVER clause", f.Name)
	case tableKind:
		return Value{}, fmt.Errorf("%s is only supported in FROM", strings.ToLower(f.Name))
	}
	if err := checkArity(f); err != nil {
		return Value{}, err
	}
	switch d.impl {
	case implMergeAction:
		return env.mergeAction()
	case implIIF:
		return env.evalIIF(f)
	case implJSON:
		return env.evalJSONFunc(f)
	case implGrouping:
		return Value{}, fmt.Errorf("GROUPING is only allowed with GROUP BY")
	}
	args := make([]Value, len(f.Args))
	for i, a := range f.Args {
		v, err := env.eval(a)
		if err != nil {
			return Value{}, err
		}
		args[i] = v
	}
	switch d.impl {
	case implCoalesce:
		// The result has the arguments' common type.
		t := unifiedType(f, argTypes(args))
		for _, a := range args {
			if !a.Null {
				return Coerce(a, t)
			}
		}
		return nullValue(t), nil
	case implConditional:
		return evalConditional(f, args)
	case implConcat:
		// NULL arguments are skipped, as in Postgres (|| propagates NULL).
		if f.Name == "CONCAT" {
			return concatValues("", args), nil
		}
		if args[0].Null {
			return nullValue(typeString), nil
		}
		return concatValues(args[0].Text(), args[1:]), nil
	}
	for _, a := range args {
		if a.Null {
			return nullValue(inferFuncType(f, argTypes(args))), nil
		}
	}
	switch d.impl {
	case implDateTime:
		if v, ok, err := env.evalDateTimeFunc(f, args); ok {
			return v, err
		}
		return Value{}, fmt.Errorf("unsupported function %s", f.Name)
	case implScalar:
		return scalarFunc(f, args)
	}
	switch f.Name {
	case "FROM_HEX", "UNHEX", "DECODE_HEX":
		b, err := hex.DecodeString(args[0].S)
		if err != nil {
			return Value{}, fmt.Errorf("invalid hex string %q", args[0].S)
		}
		return binaryValue(string(b)), nil
	case "TO_HEX", "HEX":
		return stringValue(hex.EncodeToString([]byte(args[0].S))), nil
	case "LOWER", "LCASE":
		return stringValue(strings.ToLower(args[0].Text())), nil
	case "UPPER", "UCASE":
		return stringValue(strings.ToUpper(args[0].Text())), nil
	case "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "LEN":
		if args[0].T.Kind == KindBinary {
			return intValue(typeInt64, int64(len(args[0].S))), nil
		}
		return intValue(typeInt64, int64(utf8.RuneCountInString(args[0].Text()))), nil
	case "LIKE", "ILIKE":
		esc := ""
		if len(args) == 3 {
			esc = args[2].Text()
			if utf8.RuneCountInString(esc) > 1 {
				return Value{}, fmt.Errorf("ESCAPE must be a single character")
			}
		}
		s, pat := args[0].Text(), args[1].Text()
		if f.Name == "ILIKE" {
			s, pat, esc = strings.ToLower(s), strings.ToLower(pat), strings.ToLower(esc)
		}
		return boolValue(likeMatch(s, pat, esc)), nil
	case "ABS":
		v := args[0]
		switch {
		case v.T.Kind.isInteger():
			if v.I < 0 {
				return negate(v)
			}
			return v, nil
		case v.T.Kind.isFloat():
			return floatValue(v.T, math.Abs(v.F)), nil
		case v.T.Kind == KindDecimal:
			return decimalValue(new(big.Int).Abs(v.D), v.T.Precision, v.T.Scale), nil
		}
		return Value{}, fmt.Errorf("ABS of %s", v.T.Kind)
	}
	return Value{}, fmt.Errorf("unsupported function %s", f.Name)
}

func argTypes(args []Value) []ColType {
	out := make([]ColType, len(args))
	for i, a := range args {
		out[i] = a.T
	}
	return out
}

func negate(v Value) (Value, error) {
	if v.Null {
		return v, nil
	}
	switch {
	case v.T.Kind.isInteger():
		if v.I == math.MinInt64 {
			return Value{}, fmt.Errorf("integer overflow")
		}
		return intValue(v.T, -v.I), nil
	case v.T.Kind.isFloat():
		return floatValue(v.T, -v.F), nil
	case v.T.Kind == KindDecimal:
		return decimalValue(new(big.Int).Neg(v.D), v.T.Precision, v.T.Scale), nil
	case v.T.Kind == KindInterval:
		return negateInterval(v)
	}
	return Value{}, fmt.Errorf("cannot negate %s", v.T.Kind)
}

func isComparison(op string) bool {
	switch op {
	case "=", "<>", "<", "<=", ">", ">=":
		return true
	}
	return false
}

func binaryOp(op string, l, r Value) (Value, error) {
	if isComparison(op) {
		if l.Null || r.Null {
			return nullValue(typeBool), nil
		}
		c, ok := compareValues(l, r)
		if !ok {
			return Value{}, fmt.Errorf("cannot compare %s with %s", l.T.Kind, r.T.Kind)
		}
		switch op {
		case "=":
			return boolValue(c == 0), nil
		case "<>":
			return boolValue(c != 0), nil
		case "<":
			return boolValue(c < 0), nil
		case "<=":
			return boolValue(c <= 0), nil
		case ">":
			return boolValue(c > 0), nil
		default:
			return boolValue(c >= 0), nil
		}
	}
	if op == "||" {
		if l.Null || r.Null {
			return nullValue(typeString), nil
		}
		return stringValue(l.Text() + r.Text()), nil
	}
	if rt, ok, err := temporalType(op, l.T, r.T); ok {
		if err != nil {
			return Value{}, err
		}
		if l.Null || r.Null {
			return nullValue(rt), nil
		}
		return temporalOp(op, l, r, rt)
	}
	rt, err := arithmeticType(op, l.T, r.T)
	if err != nil {
		return Value{}, err
	}
	if l.Null || r.Null {
		return nullValue(rt), nil
	}
	switch rt.Kind {
	case KindInt64:
		a, b := l.I, r.I
		switch op {
		case "+":
			s := a + b
			if (s > a) != (b > 0) {
				return Value{}, fmt.Errorf("integer overflow")
			}
			return intValue(rt, s), nil
		case "-":
			s := a - b
			if (s < a) != (b > 0) {
				return Value{}, fmt.Errorf("integer overflow")
			}
			return intValue(rt, s), nil
		case "*":
			if a != 0 && b != 0 {
				p := a * b
				if p/b != a || (a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) {
					return Value{}, fmt.Errorf("integer overflow")
				}
				return intValue(rt, p), nil
			}
			return intValue(rt, 0), nil
		case "/":
			if b == 0 {
				return Value{}, fmt.Errorf("division by zero")
			}
			return intValue(rt, a/b), nil
		}
	case KindFloat64:
		a, _ := l.asFloat()
		b, _ := r.asFloat()
		switch op {
		case "+":
			return floatValue(rt, a+b), nil
		case "-":
			return floatValue(rt, a-b), nil
		case "*":
			return floatValue(rt, a*b), nil
		case "/":
			if b == 0 {
				return Value{}, fmt.Errorf("division by zero")
			}
			return floatValue(rt, a/b), nil
		}
	case KindDecimal:
		ad, as, _ := asDecimal(l)
		bd, bs, _ := asDecimal(r)
		switch op {
		case "+", "-":
			s := max(as, bs)
			x, y := rescaleDecimal(ad, as, s), rescaleDecimal(bd, bs, s)
			if op == "+" {
				return decimalValue(x.Add(x, y), rt.Precision, s), nil
			}
			return decimalValue(x.Sub(x, y), rt.Precision, s), nil
		case "*":
			return decimalValue(new(big.Int).Mul(ad, bd), rt.Precision, as+bs), nil
		}
	}
	return Value{}, fmt.Errorf("unsupported operator %s for %s and %s", op, l.T.Kind, r.T.Kind)
}

// arithmeticType returns the result type of an arithmetic operator.
func arithmeticType(op string, a, b ColType) (ColType, error) {
	if rt, ok, err := temporalType(op, a, b); ok {
		return rt, err
	}
	if a.Kind == KindNull {
		a = b
	}
	if b.Kind == KindNull {
		b = a
	}
	if a.Kind == KindNull {
		return typeInt64, nil
	}
	num := func(k Kind) bool { return k.isNumeric() || k == KindBool }
	if !num(a.Kind) || !num(b.Kind) {
		return ColType{}, fmt.Errorf("operator %s is not supported for %s and %s", op, a.Kind, b.Kind)
	}
	switch {
	case a.Kind.isFloat() || b.Kind.isFloat():
		return typeFloat64, nil
	case a.Kind == KindDecimal || b.Kind == KindDecimal:
		if op == "/" {
			return typeFloat64, nil
		}
		as, bs := int32(0), int32(0)
		if a.Kind == KindDecimal {
			as = a.Scale
		}
		if b.Kind == KindDecimal {
			bs = b.Scale
		}
		if op == "*" {
			return decimalType(38, as+bs), nil
		}
		return decimalType(38, max(as, bs)), nil
	}
	return typeInt64, nil
}

func inferFuncType(f *Func, args []ColType) ColType {
	if t, ok := dateTimeFuncType(f, args); ok {
		return t
	}
	if t, ok := scalarFuncType(f, args); ok {
		return t
	}
	if t, ok := jsonFuncType(f); ok {
		return t
	}
	switch f.Name {
	case "COUNT", "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "LEN":
		return typeInt64
	case "LIKE", "ILIKE":
		return typeBool
	case "FROM_HEX", "UNHEX", "DECODE_HEX":
		return typeBinary
	case "TO_HEX", "HEX", "LOWER", "LCASE", "UPPER", "UCASE", "CONCAT", "CONCAT_WS":
		return typeString
	case "MERGE_ACTION":
		return typeString
	case "AVG":
		return typeFloat64
	case "SUM":
		if len(args) == 1 {
			switch {
			case args[0].Kind.isInteger() || args[0].Kind == KindBool:
				return typeInt64
			case args[0].Kind == KindDecimal:
				return decimalType(38, args[0].Scale)
			}
		}
		return typeFloat64
	case "MIN", "MAX", "ABS":
		if len(args) == 1 {
			return args[0]
		}
	case "COALESCE", "IFNULL", "NVL":
		return unifiedType(f, args)
	}
	return typeNull
}

// inferType determines the static type of an expression.
func inferType(e Expr, cols map[string]ColType, params []ColType) (ColType, error) {
	switch x := e.(type) {
	case *Literal:
		return x.V.T, nil
	case *Param:
		if x.Index < len(params) {
			return params[x.Index], nil
		}
		return typeNull, nil
	case *ColumnRef:
		if x.Outer > 0 {
			return x.OuterType, nil
		}
		if t, ok := cols[x.Name]; ok {
			return t, nil
		}
		return ColType{}, fmt.Errorf("unknown column %q", x.Name)
	case *Cast:
		return x.T, nil
	case *IsNull:
		return typeBool, nil
	case *Unary:
		if x.Op == "NOT" {
			return typeBool, nil
		}
		return inferType(x.X, cols, params)
	case *Binary:
		switch {
		case x.Op == "AND" || x.Op == "OR" || isComparison(x.Op):
			return typeBool, nil
		case x.Op == "||":
			return typeString, nil
		}
		l, err := inferType(x.L, cols, params)
		if err != nil {
			return ColType{}, err
		}
		r, err := inferType(x.R, cols, params)
		if err != nil {
			return ColType{}, err
		}
		return arithmeticType(x.Op, l, r)
	case *Func:
		if _, ok := callee(x); !ok {
			return ColType{}, noSuchFunction(x.Name, x.Star, inferArgTypes(x.Args, cols, params))
		}
		if err := checkArity(x); err != nil {
			return ColType{}, err
		}
		if t, ok, err := aggregateType(x, cols, params, false); ok {
			return t, err
		}
		args := make([]ColType, len(x.Args))
		for i, a := range x.Args {
			t, err := inferType(a, cols, params)
			if err != nil {
				return ColType{}, err
			}
			args[i] = t
		}
		return inferFuncType(x, args), nil
	case *Subquery:
		if x.Kind == SubqueryScalar && x.plan != nil {
			return x.plan.items[0].typ, nil
		}
		return typeBool, nil
	case *WindowFunc:
		return windowType(x, cols, params)
	case *Case:
		results := make([]Expr, 0, len(x.Whens)+1)
		for _, w := range x.Whens {
			results = append(results, w.Then)
		}
		if x.Else != nil {
			results = append(results, x.Else)
		}
		out := typeNull
		for _, r := range results {
			t, err := inferType(r, cols, params)
			if err != nil {
				return ColType{}, err
			}
			out = commonType(out, t)
		}
		return out, nil
	}
	return ColType{}, fmt.Errorf("unsupported expression %T", e)
}

// commonType is the type that can hold values of both a and b (used for
// the branches of CASE and the arguments of COALESCE, GREATEST and LEAST).
func commonType(a, b ColType) ColType {
	switch {
	case a.Kind == KindNull:
		return b
	case b.Kind == KindNull || a == b:
		return a
	case (a.Kind.isNumeric() || a.Kind == KindBool) && (b.Kind.isNumeric() || b.Kind == KindBool):
		if a.Kind.isInteger() && b.Kind.isInteger() {
			if a.Kind > b.Kind {
				return a
			}
			return b
		}
		t, err := arithmeticType("+", a, b)
		if err != nil {
			return typeString
		}
		if t.Kind == KindDecimal {
			// Keep enough scale for both sides.
			t.Scale = max(a.Scale, b.Scale)
		}
		return t
	case a.Kind == KindTimestamp && b.Kind == KindTimestamp:
		if unitsPerSecond[b.Unit] > unitsPerSecond[a.Unit] {
			return b
		}
		return a
	case a.Kind == KindDate && b.Kind == KindTimestamp:
		return b
	case a.Kind == KindTimestamp && b.Kind == KindDate:
		return a
	case a.Kind == b.Kind:
		return a
	}
	return typeString
}

// likeMatch implements SQL LIKE: '%' matches any run of characters, '_' one
// character, and esc (if set) makes the next character literal.
func likeMatch(s, pat, esc string) bool {
	sr, pr := []rune(s), []rune(pat)
	var escR rune = -1
	if esc != "" {
		escR = []rune(esc)[0]
	}
	// Iterative matching with backtracking on the last '%'.
	si, pi, star, mark := 0, 0, -1, 0
	for si < len(sr) {
		if pi < len(pr) {
			c, literal := pr[pi], false
			if c == escR && pi+1 < len(pr) {
				c, literal = pr[pi+1], true
			}
			switch {
			case !literal && c == '%':
				star, mark = pi, si
				pi++
				continue
			case (!literal && c == '_') || c == sr[si]:
				si++
				if literal {
					pi += 2
				} else {
					pi++
				}
				continue
			}
		}
		if star < 0 {
			return false
		}
		mark++
		si, pi = mark, star+1
	}
	for pi < len(pr) && pr[pi] == '%' && rune('%') != escR {
		pi++
	}
	return pi == len(pr)
}
