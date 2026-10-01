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

// Math, string and conditional scalar functions. Semantics follow Postgres:
//
//   - A NULL argument gives NULL, except for COALESCE, NULLIF, GREATEST,
//     LEAST and IIF.
//   - ROUND rounds half away from zero, also for DOUBLE PRECISION (where
//     Postgres rounds half to even), and NUMERIC rounding is exact.
//   - String positions are 1-based and count characters, not bytes.
//   - COALESCE, GREATEST and LEAST widen their arguments to a common type.
//
// RANDOM() is volatile: it is never treated as a constant, so it is not
// pushed into index queries.

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// scalarArity gives the minimum and maximum number of arguments of each
// scalar function (-1: no maximum).
var scalarArity = map[string][2]int{
	"ROUND": {1, 2}, "TRUNC": {1, 2}, "FLOOR": {1, 1}, "CEIL": {1, 1}, "CEILING": {1, 1},
	"MOD": {2, 2}, "POWER": {2, 2}, "POW": {2, 2}, "SQRT": {1, 1}, "LN": {1, 1},
	"LOG": {1, 2}, "LOG10": {1, 1}, "EXP": {1, 1}, "SIGN": {1, 1}, "RANDOM": {0, 0},

	"SUBSTRING": {2, 3}, "SUBSTR": {2, 3}, "LEFT": {2, 2}, "RIGHT": {2, 2}, "REPLACE": {3, 3},
	"TRIM": {1, 2}, "BTRIM": {1, 2}, "LTRIM": {1, 2}, "RTRIM": {1, 2},
	"POSITION": {2, 2}, "STRPOS": {2, 2}, "SPLIT_PART": {3, 3}, "LPAD": {2, 3}, "RPAD": {2, 3},
	"REVERSE": {1, 1}, "REPEAT": {2, 2}, "INITCAP": {1, 1}, "MD5": {1, 1},
	"REGEXP_REPLACE": {3, 4}, "STARTS_WITH": {2, 2},

	"NULLIF": {2, 2}, "GREATEST": {1, -1}, "LEAST": {1, -1}, "IIF": {3, 3},
}

// volatileFuncs return a different value on every call, so expressions
// using them are never constants.
var volatileFuncs = map[string]bool{"RANDOM": true}

// hasVolatile reports whether an expression calls a volatile function.
func hasVolatile(e Expr) bool {
	found := false
	walkExpr(e, func(x Expr) {
		if f, ok := x.(*Func); ok && volatileFuncs[f.Name] {
			found = true
		}
	})
	return found
}

// checkArity validates the argument count of a scalar function.
func checkArity(f *Func) error {
	a, ok := scalarArity[f.Name]
	if !ok {
		return nil
	}
	n := len(f.Args)
	switch {
	case f.Star || f.Distinct:
		return fmt.Errorf("%s does not accept * or DISTINCT", f.Name)
	case a[1] < 0 && n < a[0]:
		return fmt.Errorf("%s expects at least %d argument(s)", f.Name, a[0])
	case a[0] == a[1] && n != a[0]:
		return fmt.Errorf("%s expects %d argument(s)", f.Name, a[0])
	case n < a[0] || (a[1] >= 0 && n > a[1]):
		return fmt.Errorf("%s expects %d or %d arguments", f.Name, a[0], a[1])
	}
	return nil
}

// maxStringLen bounds the results of LPAD, RPAD (characters) and REPEAT
// (bytes).
const maxStringLen = 64 << 20

// ---- types ----

// scalarFuncType returns the result type of a scalar function; ok is false
// if f is not one.
func scalarFuncType(f *Func, args []ColType) (ColType, bool) {
	arg := func(i int) ColType {
		if i < len(args) {
			return args[i]
		}
		return typeNull
	}
	switch f.Name {
	case "ROUND", "TRUNC", "FLOOR", "CEIL", "CEILING":
		return roundType(f, arg(0)), true
	case "SIGN":
		t := numericArgType(arg(0))
		if t.Kind == KindDecimal {
			return decimalType(1, 0), true
		}
		return t, true
	case "MOD":
		return modType(f, args), true
	case "POWER", "POW", "SQRT", "LN", "LOG", "LOG10", "EXP", "RANDOM":
		return typeFloat64, true
	case "SUBSTRING", "SUBSTR":
		if arg(0).Kind == KindBinary {
			return typeBinary, true
		}
		return typeString, true
	case "LEFT", "RIGHT", "REPLACE", "TRIM", "BTRIM", "LTRIM", "RTRIM", "SPLIT_PART", "LPAD", "RPAD",
		"REVERSE", "REPEAT", "INITCAP", "MD5", "REGEXP_REPLACE":
		return typeString, true
	case "POSITION", "STRPOS":
		return typeInt64, true
	case "STARTS_WITH":
		return typeBool, true
	case "NULLIF":
		if arg(0).Kind == KindNull {
			return arg(1), true
		}
		return arg(0), true
	case "GREATEST", "LEAST":
		return unifiedType(f, args), true
	case "IIF":
		return commonType(arg(1), arg(2)), true
	}
	return ColType{}, false
}

// numericArgType is the type a math function works in for an argument of
// type t: text is read as DOUBLE PRECISION, and REAL is widened.
func numericArgType(t ColType) ColType {
	if t.Kind == KindString || t.Kind == KindFloat32 {
		return typeFloat64
	}
	return t
}

// capPrecision limits a computed decimal precision, staying within 38 digits
// (Decimal128) unless the input was already wider.
func capPrecision(p, orig int32) int32 {
	return max(min(p, max(orig, 38)), 1)
}

// constIntArg returns the value of an integer literal, such as the digits
// argument of ROUND(x, 2).
func constIntArg(e Expr) (int64, bool) {
	neg := false
	if u, ok := e.(*Unary); ok && u.Op == "-" {
		neg, e = true, u.X
	}
	l, ok := e.(*Literal)
	if !ok || l.V.Null || !l.V.T.Kind.isInteger() {
		return 0, false
	}
	if neg {
		return -l.V.I, true
	}
	return l.V.I, true
}

// roundType is the result type of ROUND / TRUNC / FLOOR / CEIL. Integers and
// doubles keep their type. A NUMERIC(p, s) gets the scale of the result:
// ROUND(x, 2) has scale 2 (or s if smaller), ROUND(x) and FLOOR(x) scale 0,
// with room for a carry (ROUND(9.5) = 10). If the digits argument is not a
// literal, the input scale is kept.
func roundType(f *Func, t ColType) ColType {
	t = numericArgType(t)
	if t.Kind != KindDecimal {
		return t
	}
	carry := int32(1)
	if f.Name == "TRUNC" {
		carry = 0
	}
	n, isConst := int64(0), true
	if len(f.Args) > 1 {
		n, isConst = constIntArg(f.Args[1])
	}
	if !isConst {
		return decimalType(capPrecision(t.Precision+carry, t.Precision), t.Scale)
	}
	if n >= int64(t.Scale) {
		return t
	}
	s := int32(max(n, 0))
	ip := max(t.Precision-t.Scale, 0) // integer digits
	return decimalType(capPrecision(ip+s+carry, t.Precision), s)
}

// modType is the result type of MOD: the common type of its arguments.
func modType(f *Func, args []ColType) ColType {
	t := numericArgType(unifiedType(f, args))
	if t.Kind == KindBool {
		return typeInt64
	}
	return t
}

// unifiedType is the common type of the arguments of COALESCE, GREATEST,
// LEAST and MOD. As with untyped literals in Postgres, a string literal, or
// a numeric literal that the other arguments' type holds exactly, takes the
// type of the other arguments: COALESCE(int_col, 0) is an INTEGER and
// GREATEST(d, '2024-01-01') is a DATE.
func unifiedType(f *Func, args []ColType) ColType {
	literal := func(i int) (Value, bool) {
		if i < len(f.Args) {
			if l, ok := f.Args[i].(*Literal); ok && !l.V.Null {
				return l.V, true
			}
		}
		return Value{}, false
	}
	out := typeNull
	for i, t := range args {
		if _, ok := literal(i); !ok {
			out = commonType(out, t)
		}
	}
	for i, t := range args {
		v, ok := literal(i)
		if !ok {
			continue
		}
		if out.Kind != KindNull {
			if v.T.Kind == KindString {
				continue
			}
			if v.T.Kind.isNumeric() && out.Kind.isNumeric() {
				if c, err := Coerce(v, out); err == nil {
					if cmp, ok := compareValues(c, v); ok && cmp == 0 {
						continue
					}
				}
			}
		}
		out = commonType(out, t)
	}
	return out
}

// ---- evaluation ----

// evalConditional evaluates the functions that accept NULL arguments:
// NULLIF, GREATEST and LEAST (COALESCE and IIF are handled by the caller).
func evalConditional(f *Func, args []Value) (Value, error) {
	t, _ := scalarFuncType(f, argTypes(args))
	if f.Name == "NULLIF" {
		a, b := args[0], args[1]
		if a.Null {
			return nullValue(t), nil
		}
		if b.Null {
			return a, nil
		}
		c, ok := compareValues(a, b)
		if !ok {
			return Value{}, fmt.Errorf("NULLIF cannot compare %s with %s", a.T.SQLName(), b.T.SQLName())
		}
		if c == 0 {
			return nullValue(t), nil
		}
		return a, nil
	}
	// GREATEST / LEAST ignore NULLs. Values of unrelated types (whose common
	// type falls back to text) are not compared.
	if t.Kind == KindString {
		var names []string
		mixed := false
		for _, a := range args {
			if a.Null {
				continue
			}
			if a.T.Kind != KindString && a.T.Kind != KindBinary {
				mixed = true
			}
			if n := a.T.SQLName(); !slices.Contains(names, n) {
				names = append(names, n)
			}
		}
		if mixed && len(names) > 1 {
			return Value{}, fmt.Errorf("%s types %s cannot be matched", f.Name, strings.Join(names, " and "))
		}
	}
	var best Value
	has := false
	for _, a := range args {
		if a.Null {
			continue
		}
		v, err := Coerce(a, t)
		if err != nil {
			return Value{}, fmt.Errorf("%s: %v", f.Name, err)
		}
		if !has {
			best, has = v, true
			continue
		}
		c, ok := compareValues(v, best)
		if !ok {
			return Value{}, fmt.Errorf("%s cannot compare %s with %s", f.Name, v.T.SQLName(), best.T.SQLName())
		}
		if (f.Name == "GREATEST" && c > 0) || (f.Name == "LEAST" && c < 0) {
			best = v
		}
	}
	if !has {
		return nullValue(t), nil
	}
	return best, nil
}

// evalIIF evaluates IIF(cond, a, b) like CASE WHEN cond THEN a ELSE b END:
// only the chosen branch is evaluated.
func (env *evalEnv) evalIIF(f *Func) (Value, error) {
	c, err := env.eval(f.Args[0])
	if err != nil {
		return Value{}, err
	}
	if b, ok := truthy(c); ok && b {
		return env.eval(f.Args[1])
	}
	return env.eval(f.Args[2])
}

// evalScalarFunc evaluates a math or string function; ok is false if f is
// not one. Arguments are non-NULL (NULLs are handled by the caller).
func evalScalarFunc(f *Func, args []Value) (Value, bool, error) {
	if _, ok := scalarArity[f.Name]; !ok {
		return Value{}, false, nil
	}
	v, err := scalarFunc(f, args)
	return v, true, err
}

func scalarFunc(f *Func, args []Value) (Value, error) {
	text := func(i int) string { return args[i].Text() }
	switch f.Name {
	// ---- math ----
	case "ROUND", "TRUNC", "FLOOR", "CEIL", "CEILING":
		return roundFunc(f, args)
	case "SIGN":
		x, err := numericArg(f.Name, args[0])
		if err != nil {
			return Value{}, err
		}
		switch {
		case x.T.Kind.isInteger():
			return intValue(x.T, int64(cmpInt(x.I, 0))), nil
		case x.T.Kind == KindDecimal:
			return decimalValue(big.NewInt(int64(x.D.Sign())), 1, 0), nil
		}
		if math.IsNaN(x.F) {
			return floatValue(typeFloat64, x.F), nil
		}
		return floatValue(typeFloat64, float64(cmpFloat(x.F, 0))), nil
	case "MOD":
		return modFunc(f, args)
	case "POWER", "POW", "SQRT", "LN", "LOG", "LOG10", "EXP":
		return floatFunc(f, args)
	case "RANDOM":
		return floatValue(typeFloat64, rand.Float64()), nil

	// ---- strings ----
	case "SUBSTRING", "SUBSTR":
		start, err := intArg(f.Name, "start position", args[1])
		if err != nil {
			return Value{}, err
		}
		count, hasCount := int64(0), len(args) == 3
		if hasCount {
			if count, err = intArg(f.Name, "length", args[2]); err != nil {
				return Value{}, err
			}
			if count < 0 {
				return Value{}, fmt.Errorf("%s: negative substring length not allowed", f.Name)
			}
		}
		if args[0].T.Kind == KindBinary {
			b := args[0].S
			from, to := substringRange(len(b), start, count, hasCount)
			return binaryValue(b[from:to]), nil
		}
		r := []rune(text(0))
		from, to := substringRange(len(r), start, count, hasCount)
		return stringValue(string(r[from:to])), nil
	case "LEFT", "RIGHT":
		n, err := intArg(f.Name, "length", args[1])
		if err != nil {
			return Value{}, err
		}
		r := []rune(text(0))
		size := int64(len(r))
		// Characters to keep: n, or all but -n when n is negative.
		keep := min(n, size)
		if n < 0 {
			keep = max(size+n, 0)
		}
		if f.Name == "LEFT" {
			return stringValue(string(r[:keep])), nil
		}
		return stringValue(string(r[size-keep:])), nil
	case "REPLACE":
		s, from := text(0), text(1)
		if from == "" {
			return stringValue(s), nil
		}
		return stringValue(strings.ReplaceAll(s, from, text(2))), nil
	case "TRIM", "BTRIM", "LTRIM", "RTRIM":
		chars := " "
		if len(args) == 2 {
			chars = text(1)
		}
		switch f.Name {
		case "LTRIM":
			return stringValue(strings.TrimLeft(text(0), chars)), nil
		case "RTRIM":
			return stringValue(strings.TrimRight(text(0), chars)), nil
		}
		return stringValue(strings.Trim(text(0), chars)), nil
	case "POSITION", "STRPOS":
		s, sub := text(0), text(1)
		if f.Name == "POSITION" { // POSITION(sub IN s)
			s, sub = sub, s
		}
		i := strings.Index(s, sub)
		if i < 0 {
			return intValue(typeInt64, 0), nil
		}
		return intValue(typeInt64, int64(utf8.RuneCountInString(s[:i]))+1), nil
	case "SPLIT_PART":
		n, err := intArg(f.Name, "field position", args[2])
		if err != nil {
			return Value{}, err
		}
		if n == 0 {
			return Value{}, fmt.Errorf("SPLIT_PART: field position must not be zero")
		}
		s, delim := text(0), text(1)
		fields := []string{s}
		if delim != "" {
			fields = strings.Split(s, delim)
		}
		size := int64(len(fields))
		switch {
		case n > 0 && n <= size:
			return stringValue(fields[n-1]), nil
		case n < 0 && n >= -size: // counted from the end
			return stringValue(fields[size+n]), nil
		}
		return stringValue(""), nil
	case "LPAD", "RPAD":
		n, err := intArg(f.Name, "length", args[1])
		if err != nil {
			return Value{}, err
		}
		fill := " "
		if len(args) == 3 {
			fill = text(2)
		}
		return pad(f.Name, text(0), n, fill)
	case "REVERSE":
		r := []rune(text(0))
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return stringValue(string(r)), nil
	case "REPEAT":
		n, err := intArg(f.Name, "count", args[1])
		if err != nil {
			return Value{}, err
		}
		s := text(0)
		if n <= 0 || s == "" {
			return stringValue(""), nil
		}
		if n > int64(maxStringLen/len(s)) {
			return Value{}, fmt.Errorf("REPEAT: requested length too large")
		}
		return stringValue(strings.Repeat(s, int(n))), nil
	case "INITCAP":
		return stringValue(initcap(text(0))), nil
	case "MD5":
		// Of the UTF-8 text, or of the bytes of a binary value.
		sum := md5.Sum([]byte(text(0)))
		return stringValue(hex.EncodeToString(sum[:])), nil
	case "REGEXP_REPLACE":
		flags := ""
		if len(args) == 4 {
			flags = text(3)
		}
		s, err := regexpReplace(text(0), text(1), text(2), flags)
		if err != nil {
			return Value{}, err
		}
		return stringValue(s), nil
	case "STARTS_WITH":
		return boolValue(strings.HasPrefix(text(0), text(1))), nil
	}
	return Value{}, fmt.Errorf("unsupported function %s", f.Name)
}

// numericArg checks that a math function's argument is a number; text is
// read as DOUBLE PRECISION.
func numericArg(name string, v Value) (Value, error) {
	switch {
	case v.T.Kind.isInteger(), v.T.Kind.isFloat(), v.T.Kind == KindDecimal:
		return v, nil
	case v.T.Kind == KindString:
		c, err := Coerce(v, typeFloat64)
		if err != nil {
			return Value{}, fmt.Errorf("%s: %v", name, err)
		}
		return c, nil
	}
	return Value{}, fmt.Errorf("%s expects a number, got %s", name, v.T.SQLName())
}

// intArg reads an integer argument (a position, length or count).
func intArg(name, what string, v Value) (int64, error) {
	if v.T.Kind == KindBool {
		return 0, fmt.Errorf("%s: the %s must be an integer, got BOOLEAN", name, what)
	}
	c, err := Coerce(v, typeInt64)
	if err != nil {
		return 0, fmt.Errorf("%s: the %s must be an integer: %v", name, what, err)
	}
	return c.I, nil
}

// ---- rounding ----

type roundMode int

const (
	roundHalfAway roundMode = iota
	roundTrunc
	roundFloor
	roundCeil
)

func roundModeOf(name string) roundMode {
	switch name {
	case "TRUNC":
		return roundTrunc
	case "FLOOR":
		return roundFloor
	case "CEIL", "CEILING":
		return roundCeil
	}
	return roundHalfAway
}

// roundUnscaled rounds the decimal v (unscaled, at the given scale) to n
// decimal places (n < 0 rounds to tens, hundreds, …). It returns the result
// unscaled at scale max(n, 0), or v unchanged if it has no more than n
// decimal places.
func roundUnscaled(v *big.Int, scale int32, n int64, mode roundMode) (*big.Int, int32) {
	if n >= int64(scale) {
		return new(big.Int).Set(v), scale
	}
	n = max(n, -400) // beyond that every value rounds to zero
	div := pow10(scale - int32(n))
	q, r := new(big.Int).QuoRem(v, div, new(big.Int))
	if r.Sign() != 0 {
		switch mode {
		case roundHalfAway:
			twice := new(big.Int).Abs(r)
			if twice.Lsh(twice, 1).Cmp(div) >= 0 {
				q.Add(q, big.NewInt(int64(v.Sign())))
			}
		case roundFloor:
			if v.Sign() < 0 {
				q.Sub(q, big.NewInt(1))
			}
		case roundCeil:
			if v.Sign() > 0 {
				q.Add(q, big.NewInt(1))
			}
		}
	}
	if n < 0 {
		return q.Mul(q, pow10(int32(-n))), 0
	}
	return q, int32(n)
}

// roundFloat rounds a double through its shortest decimal representation,
// so ROUND(2.675, 2) is 2.68 even though the nearest double is slightly
// below 2.675. bits is 32 for REAL values.
func roundFloat(x float64, bits int, n int64, mode roundMode) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) || x == 0 {
		return x
	}
	d, s, err := parseDecimal(strconv.FormatFloat(x, 'g', -1, bits))
	if err != nil {
		return x
	}
	r, rs := roundUnscaled(d, s, n, mode)
	f, err := strconv.ParseFloat(formatDecimal(r, rs), 64)
	if err != nil {
		return x
	}
	return f
}

func roundFunc(f *Func, args []Value) (Value, error) {
	x, err := numericArg(f.Name, args[0])
	if err != nil {
		return Value{}, err
	}
	mode := roundModeOf(f.Name)
	n := int64(0)
	if len(args) == 2 {
		if n, err = intArg(f.Name, "number of decimal places", args[1]); err != nil {
			return Value{}, err
		}
	}
	switch {
	case x.T.Kind.isInteger():
		if n >= 0 {
			return x, nil
		}
		r, _ := roundUnscaled(big.NewInt(x.I), 0, n, mode)
		if !r.IsInt64() {
			return Value{}, fmt.Errorf("%s: integer out of range", f.Name)
		}
		return Coerce(intValue(typeInt64, r.Int64()), x.T)
	case x.T.Kind.isFloat():
		switch mode {
		case roundFloor:
			return floatValue(typeFloat64, math.Floor(x.F)), nil
		case roundCeil:
			return floatValue(typeFloat64, math.Ceil(x.F)), nil
		}
		bits := 64
		if x.T.Kind == KindFloat32 {
			bits = 32
		}
		return floatValue(typeFloat64, roundFloat(x.F, bits, n, mode)), nil
	}
	rt := roundType(f, x.T)
	r, rs := roundUnscaled(x.D, x.T.Scale, n, mode)
	return decimalValue(rescaleDecimal(r, rs, rt.Scale), rt.Precision, rt.Scale), nil
}

func modFunc(f *Func, args []Value) (Value, error) {
	t := modType(f, argTypes(args))
	var ops [2]Value
	for i := range ops {
		v, err := numericArg(f.Name, args[i])
		if err == nil {
			v, err = Coerce(v, t)
		}
		if err != nil {
			return Value{}, err
		}
		ops[i] = v
	}
	a, b := ops[0], ops[1]
	switch {
	case t.Kind.isInteger():
		if b.I == 0 {
			return Value{}, fmt.Errorf("MOD: division by zero")
		}
		if b.I == -1 {
			return intValue(t, 0), nil
		}
		return intValue(t, a.I%b.I), nil
	case t.Kind == KindDecimal:
		if b.D.Sign() == 0 {
			return Value{}, fmt.Errorf("MOD: division by zero")
		}
		// Rem truncates toward zero: the result has the sign of a.
		return decimalValue(new(big.Int).Rem(a.D, b.D), t.Precision, t.Scale), nil
	}
	if b.F == 0 {
		return Value{}, fmt.Errorf("MOD: division by zero")
	}
	return floatValue(typeFloat64, math.Mod(a.F, b.F)), nil
}

// floatFunc evaluates the DOUBLE PRECISION functions, with Postgres's
// domain errors.
func floatFunc(f *Func, args []Value) (Value, error) {
	xs := make([]float64, len(args))
	for i, a := range args {
		v, err := numericArg(f.Name, a)
		if err != nil {
			return Value{}, err
		}
		fv, _ := v.asFloat()
		xs[i] = fv
	}
	fail := func(msg string) (Value, error) { return Value{}, fmt.Errorf("%s: %s", f.Name, msg) }
	logArg := func(v float64) error {
		switch {
		case v == 0:
			return fmt.Errorf("%s: cannot take logarithm of zero", f.Name)
		case v < 0:
			return fmt.Errorf("%s: cannot take logarithm of a negative number", f.Name)
		}
		return nil
	}
	x := xs[0]
	var r float64
	switch f.Name {
	case "SQRT":
		if x < 0 {
			return fail("cannot take square root of a negative number")
		}
		r = math.Sqrt(x)
	case "LN":
		if err := logArg(x); err != nil {
			return Value{}, err
		}
		r = math.Log(x)
	case "LOG10":
		if err := logArg(x); err != nil {
			return Value{}, err
		}
		r = math.Log10(x)
	case "LOG":
		if len(xs) == 1 { // base 10, as in Postgres
			if err := logArg(x); err != nil {
				return Value{}, err
			}
			r = math.Log10(x)
			break
		}
		base, v := xs[0], xs[1]
		if err := logArg(base); err != nil {
			return Value{}, err
		}
		if err := logArg(v); err != nil {
			return Value{}, err
		}
		switch base {
		case 1:
			return fail("division by zero")
		case 10:
			r = math.Log10(v)
		default:
			r = math.Log2(v) / math.Log2(base)
		}
	case "EXP":
		r = math.Exp(x)
		// As in Postgres, a double that underflows to zero is an error, but
		// the exp of a NUMERIC is then 0.
		if r == 0 && !math.IsInf(x, -1) && args[0].T.Kind != KindDecimal {
			return fail("value out of range: underflow")
		}
	case "POWER", "POW":
		y := xs[1]
		switch {
		case x == 0 && y < 0:
			return fail("zero raised to a negative power is undefined")
		case x < 0 && !math.IsInf(y, 0) && !math.IsNaN(y) && y != math.Trunc(y):
			return fail("a negative number raised to a non-integer power yields a complex result")
		}
		r = math.Pow(x, y)
	}
	if math.IsInf(r, 0) && !math.IsInf(x, 0) && (len(xs) < 2 || !math.IsInf(xs[1], 0)) {
		return fail("value out of range: overflow")
	}
	return floatValue(typeFloat64, r), nil
}

// ---- strings ----

// substringRange returns the 0-based [from, to) range of SUBSTRING(s, start
// [, count]) over n characters (or bytes). As in Postgres, start may be
// before the first character: SUBSTRING('abc', 0, 2) is 'a'.
func substringRange(n int, start, count int64, hasCount bool) (int, int) {
	end := int64(n) + 1
	if hasCount && (start <= 0 || count <= math.MaxInt64-start) { // start+count does not overflow
		end = min(start+count, end)
	}
	from := max(start, 1)
	if end <= from {
		return 0, 0
	}
	if from > int64(n) {
		return n, n
	}
	return int(from - 1), int(end - 1)
}

// pad implements LPAD / RPAD: s is padded with fill (repeated) to n
// characters, or truncated to n characters if longer.
func pad(name, s string, n int64, fill string) (Value, error) {
	if n <= 0 {
		return stringValue(""), nil
	}
	if n > maxStringLen {
		return Value{}, fmt.Errorf("%s: requested length too large", name)
	}
	r := []rune(s)
	if int64(len(r)) >= n {
		return stringValue(string(r[:n])), nil
	}
	if fill == "" {
		return stringValue(s), nil
	}
	fr := []rune(fill)
	need := int(n) - len(r)
	padding := make([]rune, need)
	for i := range padding {
		padding[i] = fr[i%len(fr)]
	}
	if name == "LPAD" {
		return stringValue(string(padding) + s), nil
	}
	return stringValue(s + string(padding)), nil
}

// initcap upper-cases the first letter of each word and lower-cases the
// rest; words are runs of letters and digits.
func initcap(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inWord := false
	for _, r := range s {
		alnum := unicode.IsLetter(r) || unicode.IsDigit(r) || (inWord && unicode.IsMark(r))
		switch {
		case alnum && !inWord:
			r = unicode.ToUpper(r)
		case alnum:
			r = unicode.ToLower(r)
		}
		inWord = alnum
		b.WriteRune(r)
	}
	return b.String()
}

// regexpCache keeps compiled patterns across rows and statements.
var regexpCache struct {
	sync.Mutex
	m map[string]*regexp.Regexp
}

func compileRegexp(expr string) (*regexp.Regexp, error) {
	regexpCache.Lock()
	re, ok := regexpCache.m[expr]
	regexpCache.Unlock()
	if ok {
		return re, nil
	}
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, err
	}
	regexpCache.Lock()
	if regexpCache.m == nil || len(regexpCache.m) >= 256 {
		regexpCache.m = map[string]*regexp.Regexp{}
	}
	regexpCache.m[expr] = re
	regexpCache.Unlock()
	return re, nil
}

// regexpReplace implements REGEXP_REPLACE(s, pattern, replacement [, flags])
// with Postgres's flags and replacement syntax: only the first match is
// replaced unless the flags contain 'g', and \1 … \9 and \& in the
// replacement insert the groups and the whole match. Patterns use RE2
// syntax (no backreferences or lookaround).
func regexpReplace(s, pattern, repl, flags string) (string, error) {
	global, caseInsensitive, literal := false, false, false
	dotNL, multiLine := true, false // Postgres: '.' matches newlines; ^ and $ anchor the string
	for _, c := range flags {
		switch c {
		case 'g':
			global = true
		case 'i':
			caseInsensitive = true
		case 'c':
			caseInsensitive = false
		case 'n', 'm':
			dotNL, multiLine = false, true
		case 's':
			dotNL, multiLine = true, false
		case 'p':
			dotNL, multiLine = false, false
		case 'w':
			dotNL, multiLine = true, true
		case 'q':
			literal = true
		case 't':
		case 'x', 'b', 'e':
			return "", fmt.Errorf("REGEXP_REPLACE: regular expression option %q is not supported", string(c))
		default:
			return "", fmt.Errorf("REGEXP_REPLACE: invalid regular expression option %q", string(c))
		}
	}
	if literal {
		pattern = regexp.QuoteMeta(pattern)
	}
	var prefix string
	if caseInsensitive {
		prefix += "i"
	}
	if dotNL {
		prefix += "s"
	}
	if multiLine {
		prefix += "m"
	}
	if prefix != "" {
		pattern = "(?" + prefix + ")" + pattern
	}
	re, err := compileRegexp(pattern)
	if err != nil {
		return "", fmt.Errorf("REGEXP_REPLACE: invalid regular expression: %v", err)
	}
	var matches [][]int
	if global {
		matches = re.FindAllStringSubmatchIndex(s, -1)
	} else if m := re.FindStringSubmatchIndex(s); m != nil {
		matches = [][]int{m}
	}
	if len(matches) == 0 {
		return s, nil
	}
	var b strings.Builder
	last := 0
	for _, m := range matches {
		b.WriteString(s[last:m[0]])
		expandReplacement(&b, repl, s, m)
		last = m[1]
	}
	b.WriteString(s[last:])
	return b.String(), nil
}

// expandReplacement writes a REGEXP_REPLACE replacement for one match:
// \1 … \9 are groups (empty if absent), \& is the whole match, \\ is a
// backslash, and any other character is copied.
func expandReplacement(b *strings.Builder, repl, src string, m []int) {
	for i := 0; i < len(repl); i++ {
		c := repl[i]
		if c == '\\' && i+1 < len(repl) {
			next := repl[i+1]
			switch {
			case next >= '1' && next <= '9':
				g := int(next - '0')
				if 2*g+1 < len(m) && m[2*g] >= 0 {
					b.WriteString(src[m[2*g]:m[2*g+1]])
				}
				i++
				continue
			case next == '&':
				b.WriteString(src[m[0]:m[1]])
				i++
				continue
			case next == '\\':
				b.WriteByte('\\')
				i++
				continue
			}
		}
		b.WriteByte(c)
	}
}
