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

// The driver's functions.
//
// funcDefs lists every function a call can name: scalar functions,
// aggregates, window functions and the table function GENERATE_SERIES, with
// the argument counts they accept and, where the planner checks them, the
// argument types. Everything else about functions is derived from it: the
// sets of aggregates, window functions and so on that the planner uses,
// which code evaluates a scalar function (evalFunc dispatches on funcImpl),
// and information_schema.routines (routineRows). funcs_test.go checks that
// every name the evaluator handles is registered and every registered name
// evaluates.
//
// Names:
//
//   - The parser stores a call's name in upper case, so function names are
//     case-insensitive. A quoted name followed by "(" is not a call (a
//     syntax error, except for a table function in FROM), and
//     schema-qualified calls such as pg_catalog.lower(x) are not supported.
//   - Aliases are other names of the same function: LCASE is LOWER, NOW is
//     CURRENT_TIMESTAMP. A call keeps the name it was written with, which
//     its errors use.
//   - Special forms are calls of the functions they mean: EXTRACT(f FROM x)
//     is DATE_PART('f', x), SUBSTRING(s FROM a FOR b) is SUBSTRING(s, a, b),
//     TRIM([BOTH | LEADING | TRAILING] [c] FROM s) and TRIM(s [, c]) are
//     BTRIM / LTRIM / RTRIM(s [, c]), POSITION(a IN b) is POSITION(a, b),
//     DATE_ADD(x, INTERVAL n part) is DATE_ADD('part', n, x), x % y is
//     MOD(x, y) and x LIKE p is LIKE(x, p). Operators and the other forms
//     are calls of internal functions, named after the operator or starting
//     with "__", which a call can't name: x ~ p, x SIMILAR TO p, the JSON
//     operators ->, ->>, #> and #>>, INTERVAL n unit (__INTERVAL), x::json
//     (__JSON), x IS JSON (__IS_JSON) and the SQL/JSON constructors
//     JSON_OBJECT(k VALUE v) and JSON_ARRAY(…). CAST, TRY_CAST, SAFE_CAST
//     and CASE are expressions, not calls, and COALESCE, NULLIF, GREATEST
//     and LEAST are ordinary calls. OVERLAY is not supported.
//
// Checks (resolveCall), when a statement is planned:
//
//   - Every call is resolved while it is bound, before any row is read, in
//     every place an expression can be: SELECT lists, WHERE, GROUP BY,
//     HAVING, QUALIFY, ORDER BY, ON, window definitions (also unused ones),
//     subqueries, CTEs, derived tables, view bodies (CREATE VIEW plans its
//     query), INSERT … VALUES and … SELECT, UPDATE and DELETE, MERGE's
//     clauses and RETURNING, and CHECK and DEFAULT expressions when they are
//     defined. A query that reads no rows (empty.go) is still planned, so it
//     gets the same errors. A CTE that no part of the statement reads isn't
//     planned, so its calls (like its columns) aren't checked.
//   - A name that isn't a function gives Postgres's `function
//     nosuchfunc(integer) does not exist`, with the argument types the
//     planner infers (unknown for NULL and for a parameter without a type).
//     A function used where it can't be (a window function without OVER, an
//     aggregate in WHERE, …) keeps its own error.
//   - A wrong argument count gives `UPPER expects 1 argument` (… no
//     arguments, … 1 or 2 arguments, … 2 to 4 arguments, … at least 1
//     argument). `*` is accepted only by COUNT, DISTINCT only by aggregates.
//   - Argument types are checked where they decide whether a call exists
//     (argType): BOOL_OR of an integer is `function bool_or(integer) does
//     not exist`. Other type errors, and errors that depend on values
//     (division by zero, a cast or a logarithm that fails on a value), are
//     raised when a row is evaluated, as in Postgres.
//   - Order: a statement's FROM items are resolved before its expressions,
//     and these left to right. A call is visited before its arguments, but
//     when it is wrong its arguments are bound first, so an unknown column
//     or function in them is reported first, as in Postgres.

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

type funcKind uint8

const (
	scalarKind funcKind = iota
	aggregateKind
	windowKind // a window function that is not an aggregate (ROW_NUMBER, LAG, …)
	tableKind  // a function in FROM
)

// funcImpl is the code that evaluates a scalar function (see evalFunc).
type funcImpl uint8

const (
	implBuiltin     funcImpl = iota // evalFunc's own cases (eval.go)
	implScalar                      // scalarFunc: math, strings, regular expressions (scalar.go, regex.go)
	implDateTime                    // evalDateTimeFunc (datetime.go)
	implJSON                        // evalJSONFunc (json.go, jsonpath.go); JSON aggregates (aggfuncs.go)
	implCoalesce                    // COALESCE: NULL arguments don't make it NULL
	implConditional                 // evalConditional: NULLIF, GREATEST, LEAST
	implConcat                      // CONCAT, CONCAT_WS: NULL arguments are skipped
	implIIF                         // evalIIF: only the chosen branch is evaluated
	implMergeAction                 // mergeAction (returning.go)
	implGrouping                    // replaced by the grouping planner (grouping.go)
)

// overUse says whether a function can be called with OVER.
type overUse uint8

const (
	noOver             overUse = iota
	overOK                     // a window function, or an aggregate computed over frames
	overNotImplemented         // an aggregate that window.go doesn't compute
)

// argType is a type the planner requires of an argument. A NULL (or a
// parameter whose type isn't known) is accepted.
type argType uint8

const (
	argAny argType = iota
	argBool
	argNumber
	argInt
	argNumberOrInterval
)

func (a argType) accepts(t ColType) bool {
	switch {
	case t.Kind == KindNull:
		return true
	case a == argBool:
		return t.Kind == KindBool
	case a == argNumber:
		return t.Kind.isNumeric()
	case a == argInt:
		return t.Kind.isInteger()
	case a == argNumberOrInterval:
		return t.Kind.isNumeric() || t.Kind == KindInterval
	}
	return true
}

// many is the maximum argument count of a variadic function.
const many = -1

type funcDef struct {
	name    string   // upper case, as calls are parsed
	aliases []string // other names of the same function
	kind    funcKind
	impl    funcImpl // scalar functions; implJSON also marks the JSON aggregates
	// min and max bound the argument count (max many: no maximum).
	min, max int
	// args are the types the planner requires of the first arguments (the
	// direct arguments of an ordered-set aggregate); within is the type of
	// an ordered-set aggregate's WITHIN GROUP value.
	args   []argType
	within argType
	over   overUse
	// star: f(*) is accepted (COUNT). orderedSet: WITHIN GROUP (ORDER BY x)
	// is required. nulls: IGNORE NULLS / RESPECT NULLS are accepted.
	// volatile: a different value for every call (never a constant).
	// internal: the parser builds the call for an operator or a special
	// form; a call written with its name (__JSON(x)) is an unknown
	// function, and it isn't listed in information_schema.routines.
	star, orderedSet, nulls, volatile, internal bool
}

// funcDefs are the driver's functions (see the top of this file).
var funcDefs = []funcDef{
	// ---- math (scalar.go) ----
	{name: "ROUND", min: 1, max: 2, impl: implScalar},
	{name: "TRUNC", min: 1, max: 2, impl: implScalar},
	{name: "FLOOR", min: 1, max: 1, impl: implScalar},
	{name: "CEIL", aliases: []string{"CEILING"}, min: 1, max: 1, impl: implScalar},
	{name: "MOD", min: 2, max: 2, impl: implScalar}, // also x % y
	{name: "POWER", aliases: []string{"POW"}, min: 2, max: 2, impl: implScalar},
	{name: "SQRT", min: 1, max: 1, impl: implScalar},
	{name: "LN", min: 1, max: 1, impl: implScalar},
	{name: "LOG", min: 1, max: 2, impl: implScalar},
	{name: "LOG10", min: 1, max: 1, impl: implScalar},
	{name: "EXP", min: 1, max: 1, impl: implScalar},
	{name: "SIGN", min: 1, max: 1, impl: implScalar},
	{name: "RANDOM", min: 0, max: 0, impl: implScalar, volatile: true},
	{name: "ABS", min: 1, max: 1},

	// ---- strings (scalar.go, eval.go) ----
	// SUBSTRING also has the forms SUBSTRING(s FROM a [FOR b]) and
	// SUBSTRING(s SIMILAR p ESCAPE e), and pattern matches for text
	// arguments; SUBSTR doesn't.
	{name: "SUBSTRING", min: 2, max: 3, impl: implScalar},
	{name: "SUBSTR", min: 2, max: 3, impl: implScalar},
	{name: "LEFT", min: 2, max: 2, impl: implScalar},
	{name: "RIGHT", min: 2, max: 2, impl: implScalar},
	{name: "REPLACE", min: 3, max: 3, impl: implScalar},
	{name: "BTRIM", min: 1, max: 2, impl: implScalar},    // also TRIM([BOTH] [c] FROM s) and TRIM(s [, c])
	{name: "LTRIM", min: 1, max: 2, impl: implScalar},    // also TRIM(LEADING …)
	{name: "RTRIM", min: 1, max: 2, impl: implScalar},    // also TRIM(TRAILING …)
	{name: "POSITION", min: 2, max: 2, impl: implScalar}, // POSITION(sub IN s)
	{name: "STRPOS", min: 2, max: 2, impl: implScalar},
	{name: "SPLIT_PART", min: 3, max: 3, impl: implScalar},
	{name: "LPAD", min: 2, max: 3, impl: implScalar},
	{name: "RPAD", min: 2, max: 3, impl: implScalar},
	{name: "REVERSE", min: 1, max: 1, impl: implScalar},
	{name: "REPEAT", min: 2, max: 2, impl: implScalar},
	{name: "INITCAP", min: 1, max: 1, impl: implScalar},
	{name: "MD5", min: 1, max: 1, impl: implScalar},
	{name: "STARTS_WITH", min: 2, max: 2, impl: implScalar},
	{name: "REGEXP_REPLACE", min: 3, max: 6, impl: implScalar},
	{name: "LOWER", aliases: []string{"LCASE"}, min: 1, max: 1},
	{name: "UPPER", aliases: []string{"UCASE"}, min: 1, max: 1},
	{name: "LENGTH", aliases: []string{"CHAR_LENGTH", "CHARACTER_LENGTH", "LEN"}, min: 1, max: 1},
	{name: "FROM_HEX", aliases: []string{"UNHEX", "DECODE_HEX"}, min: 1, max: 1},
	{name: "TO_HEX", aliases: []string{"HEX"}, min: 1, max: 1},
	{name: "CONCAT", min: 0, max: many, impl: implConcat},
	{name: "CONCAT_WS", min: 1, max: many, impl: implConcat},
	{name: "LIKE", min: 2, max: 3}, // x LIKE p [ESCAPE e], also LIKE(x, p [, e])
	{name: "ILIKE", min: 2, max: 3},

	// ---- regular expressions (regex.go) ----
	{name: "~", min: 2, max: 2, impl: implScalar, internal: true},  // x ~ p (and NOT for !~)
	{name: "~*", min: 2, max: 2, impl: implScalar, internal: true}, // x ~* p
	{name: "SIMILAR TO", min: 2, max: 3, impl: implScalar, internal: true},
	{name: "REGEXP_LIKE", min: 2, max: 3, impl: implScalar},
	{name: "REGEXP_COUNT", min: 2, max: 4, impl: implScalar},
	{name: "REGEXP_INSTR", min: 2, max: 7, impl: implScalar},
	{name: "REGEXP_SUBSTR", min: 2, max: 6, impl: implScalar},
	{name: "REGEXP_MATCH", min: 2, max: 3, impl: implScalar},

	// ---- conditional (eval.go, scalar.go) ----
	{name: "COALESCE", aliases: []string{"IFNULL", "NVL"}, min: 0, max: many, impl: implCoalesce},
	{name: "NULLIF", min: 2, max: 2, impl: implConditional},
	{name: "GREATEST", min: 1, max: many, impl: implConditional},
	{name: "LEAST", min: 1, max: many, impl: implConditional},
	{name: "IIF", min: 3, max: 3, impl: implIIF},

	// ---- date and time (datetime.go) ----
	// The current date and time take an optional precision, which is
	// ignored.
	{name: "CURRENT_DATE", min: 0, max: 1, impl: implDateTime},
	{name: "CURRENT_TIMESTAMP", aliases: []string{"NOW", "TRANSACTION_TIMESTAMP", "STATEMENT_TIMESTAMP"}, min: 0, max: 1, impl: implDateTime},
	{name: "LOCALTIMESTAMP", min: 0, max: 1, impl: implDateTime},
	{name: "CURRENT_TIME", min: 0, max: 1, impl: implDateTime},
	{name: "LOCALTIME", min: 0, max: 1, impl: implDateTime},
	{name: "DATE_PART", min: 2, max: 2, impl: implDateTime}, // also EXTRACT(field FROM x)
	{name: "DATE_TRUNC", min: 2, max: 2, impl: implDateTime},
	{name: "YEAR", min: 1, max: 1, impl: implDateTime},
	{name: "QUARTER", min: 1, max: 1, impl: implDateTime},
	{name: "MONTH", min: 1, max: 1, impl: implDateTime},
	{name: "WEEK", min: 1, max: 1, impl: implDateTime},
	{name: "DAY", aliases: []string{"DAYOFMONTH"}, min: 1, max: 1, impl: implDateTime},
	{name: "DAYOFYEAR", min: 1, max: 1, impl: implDateTime},
	{name: "HOUR", min: 1, max: 1, impl: implDateTime},
	{name: "MINUTE", min: 1, max: 1, impl: implDateTime},
	{name: "SECOND", min: 1, max: 1, impl: implDateTime},
	{name: "EPOCH", min: 1, max: 1, impl: implDateTime},
	{name: "EPOCH_MS", min: 1, max: 1, impl: implDateTime},
	{name: "LAST_DAY", min: 1, max: 1, impl: implDateTime},
	{name: "AGE", min: 1, max: 2, impl: implDateTime},
	// DATE_ADD(x, INTERVAL n part) is DATE_ADD('part', n, x), and DATE_SUB
	// (written only that way) DATE_SUB('part', n, x).
	{name: "DATEADD", aliases: []string{"DATE_ADD", "TIMESTAMPADD"}, min: 3, max: 3, impl: implDateTime},
	{name: "DATE_SUB", min: 3, max: 3, impl: implDateTime},
	{name: "DATEDIFF", aliases: []string{"DATE_DIFF", "TIMESTAMPDIFF"}, min: 3, max: 3, impl: implDateTime},
	{name: "MAKE_DATE", min: 3, max: 3, impl: implDateTime},
	{name: "MAKE_TIME", min: 3, max: 3, impl: implDateTime},
	{name: "MAKE_TIMESTAMP", min: 6, max: 6, impl: implDateTime},
	{name: "MAKE_TIMESTAMPTZ", min: 6, max: 6, impl: implDateTime},
	{name: "TO_TIMESTAMP", min: 1, max: 2, impl: implDateTime}, // (epoch seconds) or (text, format)
	{name: "TO_DATE", min: 2, max: 2, impl: implDateTime},
	{name: "TO_CHAR", min: 2, max: 2, impl: implDateTime},
	{name: "__INTERVAL", min: 2, max: 2, impl: implDateTime, internal: true}, // INTERVAL 'n' unit, INTERVAL n unit

	// ---- JSON (json.go, jsonpath.go) ----
	{name: "->", min: 2, max: 2, impl: implJSON, internal: true},
	{name: "->>", min: 2, max: 2, impl: implJSON, internal: true},
	{name: "#>", min: 2, max: 2, impl: implJSON, internal: true},
	{name: "#>>", min: 2, max: 2, impl: implJSON, internal: true},
	{name: "JSON_EXTRACT_PATH", min: 2, max: many, impl: implJSON},
	{name: "JSON_EXTRACT_PATH_TEXT", min: 2, max: many, impl: implJSON},
	{name: "JSONB_EXTRACT_PATH", min: 2, max: many, impl: implJSON},
	{name: "JSONB_EXTRACT_PATH_TEXT", min: 2, max: many, impl: implJSON},
	{name: "JSON_TYPEOF", min: 1, max: 1, impl: implJSON},
	{name: "JSONB_TYPEOF", min: 1, max: 1, impl: implJSON},
	{name: "JSON_ARRAY_LENGTH", min: 1, max: 1, impl: implJSON},
	{name: "JSONB_ARRAY_LENGTH", min: 1, max: 1, impl: implJSON},
	{name: "JSON_BUILD_OBJECT", min: 0, max: many, impl: implJSON},
	{name: "JSONB_BUILD_OBJECT", min: 0, max: many, impl: implJSON},
	{name: "JSON_BUILD_ARRAY", min: 0, max: many, impl: implJSON},
	{name: "JSONB_BUILD_ARRAY", min: 0, max: many, impl: implJSON},
	// JSON_OBJECT(text[]) and JSON_OBJECT(keys, values), as in Postgres; the
	// SQL/JSON JSON_OBJECT(k VALUE v, …) is __JSON_OBJECT.
	{name: "JSON_OBJECT", min: 1, max: 2, impl: implJSON},
	{name: "JSONB_OBJECT", min: 1, max: 2, impl: implJSON},
	{name: "TO_JSON", min: 1, max: 1, impl: implJSON},
	{name: "TO_JSONB", min: 1, max: 1, impl: implJSON},
	// The SQL/JSON query functions: the document, the path and the DEFAULT
	// expressions of ON EMPTY and ON ERROR (JSON_EXISTS has none). The
	// parser checks their syntax.
	{name: "JSON_VALUE", min: 2, max: 4, impl: implJSON},
	{name: "JSON_QUERY", min: 2, max: 4, impl: implJSON},
	{name: "JSON_EXISTS", min: 2, max: 2, impl: implJSON},
	{name: "__JSON", min: 1, max: 1, impl: implJSON, internal: true},  // x::json
	{name: "__JSONB", min: 1, max: 1, impl: implJSON, internal: true}, // x::jsonb
	{name: "__IS_JSON", min: 1, max: 1, impl: implJSON, internal: true},
	{name: "__JSON_OBJECT", min: 0, max: many, impl: implJSON, internal: true}, // JSON_OBJECT(k VALUE v, …)
	{name: "__JSON_ARRAY", min: 0, max: many, impl: implJSON, internal: true},  // JSON_ARRAY(v, …)

	// ---- grouping and MERGE (grouping.go, returning.go) ----
	{name: "GROUPING", min: 1, max: many, impl: implGrouping},
	{name: "MERGE_ACTION", min: 0, max: 0, impl: implMergeAction},

	// ---- aggregates (exec.go, aggfuncs.go) ----
	// COUNT(*), or COUNT(x); several arguments need DISTINCT.
	{name: "COUNT", kind: aggregateKind, min: 1, max: many, star: true, over: overOK},
	{name: "SUM", kind: aggregateKind, min: 1, max: 1, over: overOK},
	{name: "AVG", kind: aggregateKind, min: 1, max: 1, over: overOK},
	{name: "MIN", kind: aggregateKind, min: 1, max: 1, over: overOK},
	{name: "MAX", kind: aggregateKind, min: 1, max: 1, over: overOK},
	{name: "STRING_AGG", kind: aggregateKind, min: 2, max: 2, over: overOK},
	{name: "LISTAGG", kind: aggregateKind, min: 1, max: 2, over: overOK},
	{name: "BOOL_OR", kind: aggregateKind, min: 1, max: 1, args: []argType{argBool}, over: overOK},
	{name: "BOOL_AND", aliases: []string{"EVERY"}, kind: aggregateKind, min: 1, max: 1, args: []argType{argBool}, over: overOK},
	{name: "ANY_VALUE", kind: aggregateKind, min: 1, max: 1, over: overOK},
	{name: "STDDEV_SAMP", aliases: []string{"STDDEV"}, kind: aggregateKind, min: 1, max: 1, args: []argType{argNumber}, over: overOK},
	{name: "STDDEV_POP", kind: aggregateKind, min: 1, max: 1, args: []argType{argNumber}, over: overOK},
	{name: "VAR_SAMP", aliases: []string{"VARIANCE"}, kind: aggregateKind, min: 1, max: 1, args: []argType{argNumber}, over: overOK},
	{name: "VAR_POP", kind: aggregateKind, min: 1, max: 1, args: []argType{argNumber}, over: overOK},
	// The ordered-set aggregates: f(fraction) WITHIN GROUP (ORDER BY x).
	{name: "PERCENTILE_CONT", kind: aggregateKind, min: 1, max: 1, args: []argType{argNumber}, within: argNumberOrInterval,
		orderedSet: true, over: overNotImplemented},
	{name: "PERCENTILE_DISC", kind: aggregateKind, min: 1, max: 1, args: []argType{argNumber}, orderedSet: true, over: overNotImplemented},
	{name: "MODE", kind: aggregateKind, min: 0, max: 0, orderedSet: true, over: overNotImplemented},
	// MEDIAN(x) is PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x).
	{name: "MEDIAN", kind: aggregateKind, min: 1, max: 1, args: []argType{argNumberOrInterval}, over: overNotImplemented},
	{name: "JSON_AGG", kind: aggregateKind, impl: implJSON, min: 1, max: 1},
	{name: "JSONB_AGG", kind: aggregateKind, impl: implJSON, min: 1, max: 1},
	{name: "JSON_OBJECT_AGG", kind: aggregateKind, impl: implJSON, min: 2, max: 2},
	{name: "JSONB_OBJECT_AGG", kind: aggregateKind, impl: implJSON, min: 2, max: 2},

	// ---- window functions (window.go) ----
	{name: "ROW_NUMBER", kind: windowKind, min: 0, max: 0, over: overOK},
	{name: "RANK", kind: windowKind, min: 0, max: 0, over: overOK},
	{name: "DENSE_RANK", kind: windowKind, min: 0, max: 0, over: overOK},
	{name: "PERCENT_RANK", kind: windowKind, min: 0, max: 0, over: overOK},
	{name: "CUME_DIST", kind: windowKind, min: 0, max: 0, over: overOK},
	{name: "NTILE", kind: windowKind, min: 1, max: 1, args: []argType{argInt}, over: overOK},
	{name: "LAG", kind: windowKind, min: 1, max: 3, args: []argType{argAny, argInt}, over: overOK, nulls: true},
	{name: "LEAD", kind: windowKind, min: 1, max: 3, args: []argType{argAny, argInt}, over: overOK, nulls: true},
	{name: "FIRST_VALUE", kind: windowKind, min: 1, max: 1, over: overOK, nulls: true},
	{name: "LAST_VALUE", kind: windowKind, min: 1, max: 1, over: overOK, nulls: true},
	{name: "NTH_VALUE", kind: windowKind, min: 2, max: 2, args: []argType{argAny, argInt}, over: overOK, nulls: true},

	// ---- table functions (series.go) ----
	// The overloads (integers, numerics, timestamps with an interval step)
	// are resolved by seriesType.
	{name: "GENERATE_SERIES", kind: tableKind, min: 2, max: 3},
}

// funcsByName finds a function by any of its names.
var funcsByName = func() map[string]*funcDef {
	m := map[string]*funcDef{}
	for i := range funcDefs {
		d := &funcDefs[i]
		for _, n := range append([]string{d.name}, d.aliases...) {
			if _, dup := m[n]; dup {
				panic("function " + n + " is defined twice")
			}
			m[n] = d
		}
	}
	return m
}()

// lookupFunc returns the function a name names.
func lookupFunc(name string) (*funcDef, bool) {
	d, ok := funcsByName[name]
	return d, ok
}

// callee returns the function a call calls: ok is false for an unknown
// name, and for an internal function's name written as name(…).
func callee(f *Func) (*funcDef, bool) {
	d, ok := funcsByName[f.Name]
	if !ok || (d.internal && f.named) {
		return nil, false
	}
	return d, true
}

// funcNames returns the names (and aliases) of the functions for which keep
// is true.
func funcNames(keep func(d *funcDef) bool) map[string]bool {
	out := map[string]bool{}
	for i := range funcDefs {
		if d := &funcDefs[i]; keep(d) {
			out[d.name] = true
			for _, a := range d.aliases {
				out[a] = true
			}
		}
	}
	return out
}

var (
	// aggregateFuncs are the aggregates, also when called with OVER.
	aggregateFuncs = funcNames(func(d *funcDef) bool { return d.kind == aggregateKind })
	// windowOnlyFuncs exist only as window functions (aggregates can be both).
	windowOnlyFuncs = funcNames(func(d *funcDef) bool { return d.kind == windowKind })
	// orderedSetFuncs are the aggregates called with WITHIN GROUP (ORDER BY
	// x), x being the aggregated value.
	orderedSetFuncs = funcNames(func(d *funcDef) bool { return d.orderedSet })
	// nullTreatmentFuncs take IGNORE NULLS / RESPECT NULLS.
	nullTreatmentFuncs = funcNames(func(d *funcDef) bool { return d.nulls })
	// volatileFuncs return a different value on every call, so expressions
	// using them are never constants.
	volatileFuncs = funcNames(func(d *funcDef) bool { return d.volatile })
	// jsonFuncs are the JSON scalar functions and operators, evaluated by
	// evalJSONFunc.
	jsonFuncs = funcNames(func(d *funcDef) bool { return d.kind == scalarKind && d.impl == implJSON })
	// jsonAggregates are the JSON aggregate functions. They are always
	// computed in the driver (driverAggregate), and may have an ORDER BY.
	jsonAggregates = funcNames(func(d *funcDef) bool { return d.kind == aggregateKind && d.impl == implJSON })
)

// overNames lists the functions that can be called with OVER: the window
// functions, then the aggregates.
func overNames() string {
	var names []string
	for _, k := range []funcKind{windowKind, aggregateKind} {
		for i := range funcDefs {
			if d := &funcDefs[i]; d.kind == k && d.over == overOK {
				names = append(names, d.name)
				names = append(names, d.aliases...)
			}
		}
	}
	return strings.Join(names, ", ")
}

// argCount describes the argument counts a function accepts.
func (d *funcDef) argCount() string {
	plural := func(n int) string {
		if n == 1 {
			return "1 argument"
		}
		return fmt.Sprintf("%d arguments", n)
	}
	switch {
	case d.max == many:
		return "at least " + plural(d.min)
	case d.min == d.max && d.min == 0:
		return "no arguments"
	case d.min == d.max:
		return plural(d.min)
	case d.max == d.min+1:
		return fmt.Sprintf("%d or %d arguments", d.min, d.max)
	}
	return fmt.Sprintf("%d to %d arguments", d.min, d.max)
}

// argsError checks a call's * and DISTINCT and its argument count.
func (d *funcDef) argsError(f *Func) error {
	switch {
	case f.Star && !d.star && (d.kind == aggregateKind || d.kind == windowKind):
		return errorf(adbc.StatusInvalidArgument, "%s does not accept *", f.Name)
	case (f.Star || f.Distinct) && (d.kind == scalarKind || d.kind == tableKind):
		return errorf(adbc.StatusInvalidArgument, "%s does not accept * or DISTINCT", f.Name)
	case f.Star:
		return nil
	}
	n := len(f.Args)
	if n >= d.min && (d.max == many || n <= d.max) {
		return nil
	}
	if d.star && n == 0 {
		return errorf(adbc.StatusInvalidArgument, "%s(*) must be used to call a parameterless aggregate function", f.Name)
	}
	return errorf(adbc.StatusInvalidArgument, "%s expects %s", f.Name, d.argCount())
}

// errUnknownFunction is resolveCall's error for a name that isn't a
// function; noSuchFunction gives the message.
var errUnknownFunction = errors.New("unknown function")

// resolveCall checks a call against the registry: the function exists, can
// be called where it is (over: with OVER), and gets arguments it accepts.
// For a call without OVER it also checks the modifiers (checkCall). It
// returns errUnknownFunction for a name that isn't a function. It doesn't
// look at the arguments' types, which aren't known before they are bound.
func resolveCall(f *Func, over bool) error {
	d, ok := callee(f)
	if !ok {
		return errUnknownFunction
	}
	if over {
		switch d.over {
		case noOver:
			return errorf(adbc.StatusInvalidArgument, "%s is not a window function or an aggregate (supported with OVER: %s)", f.Name, overNames())
		case overNotImplemented:
			return errorf(adbc.StatusNotImplemented, "OVER is not supported for ordered-set aggregate %s", f.Name)
		}
		if err := d.argsError(f); err != nil {
			return err
		}
		if d.star && len(f.Args) > 1 && !f.Distinct {
			// Several arguments need DISTINCT, which OVER doesn't support.
			return errorf(adbc.StatusInvalidArgument, "%s expects 1 argument or * with OVER", f.Name)
		}
		return nil
	}
	switch d.kind {
	case windowKind:
		return errorf(adbc.StatusInvalidArgument, "window function %s requires an OVER clause", f.Name)
	case tableKind:
		name := strings.ToLower(f.Name)
		return errorf(adbc.StatusNotImplemented, "%s is only supported in FROM (SELECT … FROM %s(…) AS g)", name, name)
	}
	if err := d.argsError(f); err != nil {
		return err
	}
	return checkCall(f)
}

// sigTypeName names an argument type in a function signature, as Postgres
// does: in lower case, without a precision or scale, and "unknown" for NULL
// or a parameter whose type isn't known.
func sigTypeName(t ColType) string {
	switch t.Kind {
	case KindNull:
		return "unknown"
	case KindDecimal:
		return "numeric"
	case KindTime:
		return "time"
	case KindTimestamp:
		if t.TZ != "" {
			return "timestamp with time zone"
		}
		return "timestamp"
	}
	return strings.ToLower(t.SQLName())
}

// noSuchFunction is Postgres's error for a call that matches no function:
// `function f(integer, varchar) does not exist`.
func noSuchFunction(name string, star bool, types []ColType) error {
	names := make([]string, 0, len(types)+1)
	if star {
		names = append(names, "*")
	}
	for _, t := range types {
		names = append(names, sigTypeName(t))
	}
	return errorf(adbc.StatusInvalidArgument, "function %s(%s) does not exist", strings.ToLower(name), strings.Join(names, ", "))
}

// inferArgTypes infers the types of a call's arguments for an error
// message; an argument whose type can't be inferred is NULL (unknown).
func inferArgTypes(args []Expr, cols map[string]ColType, params []ColType) []ColType {
	out := make([]ColType, len(args))
	for i, a := range args {
		if t, err := inferType(a, cols, params); err == nil {
			out[i] = t
		} else {
			out[i] = typeNull
		}
	}
	return out
}

// callArgs are the expressions of a call that are bound with it: its
// arguments, an aggregate's ORDER BY and FILTER, and with OVER (w) the
// window's expressions.
func callArgs(f *Func, w *WindowFunc) []Expr {
	if w != nil {
		return w.children()
	}
	out := append([]Expr{}, f.Args...)
	for _, o := range f.OrderBy {
		out = append(out, o.Expr)
	}
	if f.Filter != nil {
		out = append(out, f.Filter)
	}
	return out
}

// checkCalls resolves the calls of an expression that isn't bound (a column
// DEFAULT, which reads no columns), as bindCall does.
func checkCalls(x Expr) error {
	var err error
	walkExpr(x, func(n Expr) {
		if err != nil {
			return
		}
		var f *Func
		var w *WindowFunc
		switch v := n.(type) {
		case *Func:
			f = v
		case *WindowFunc:
			f, w = v.Func, v
		default:
			return
		}
		cerr := resolveCall(f, w != nil)
		if cerr == nil {
			return
		}
		// As when binding, an error in the arguments is reported first.
		for _, a := range callArgs(f, w) {
			if err = checkCalls(a); err != nil {
				return
			}
		}
		if cerr == errUnknownFunction {
			cerr = noSuchFunction(f.Name, f.Star, inferArgTypes(f.Args, nil, nil))
		}
		err = cerr
	})
	return err
}

// routineRows are the rows of information_schema.routines: each function by
// each of its names (alias_of names the function an alias is), except the
// internal ones. function_kind is SCALAR, AGGREGATE, WINDOW (a window
// function that isn't an aggregate) or TABLE, and max_arguments is NULL
// for no maximum. As in Postgres, the built-in functions are in pg_catalog,
// though a call can't name the schema.
func routineRows() [][]Value {
	kinds := map[funcKind]string{scalarKind: "SCALAR", aggregateKind: "AGGREGATE", windowKind: "WINDOW", tableKind: "TABLE"}
	var rows [][]Value
	for i := range funcDefs {
		d := &funcDefs[i]
		if d.internal {
			continue
		}
		maxArgs := nullValue(typeInt32)
		if d.max != many {
			maxArgs = intValue(typeInt32, int64(d.max))
		}
		for j, n := range append([]string{d.name}, d.aliases...) {
			aliasOf := nullValue(typeString)
			if j > 0 {
				aliasOf = stringValue(strings.ToLower(d.name))
			}
			rows = append(rows, []Value{stringValue(catalogName), stringValue("pg_catalog"), stringValue(strings.ToLower(n)),
				stringValue("FUNCTION"), stringValue(kinds[d.kind]), intValue(typeInt32, int64(d.min)), maxArgs, aliasOf})
		}
	}
	return rows
}
