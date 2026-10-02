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

// Unit tests for the function registry (funcs.go): it and the evaluator
// agree, in both directions. They need no Redis server: the sample calls
// run over generate_series, in memory.

import (
	"context"
	"go/ast"
	goparser "go/parser"
	gotoken "go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// funcSamples is a call of every function the registry has, by each of its
// names. A scalar call runs as SELECT call; an aggregate over
// generate_series(1, 3) AS g(x), and also with OVER (ORDER BY x) if it is a
// window function too; a window function with OVER (ORDER BY x); a table
// function in FROM. The internal functions are written as the syntax the
// parser builds them for. A sample that starts with SELECT is the whole
// query.
var funcSamples = map[string]string{
	// math
	"ROUND": "ROUND(2.5)", "TRUNC": "TRUNC(2.55, 1)", "FLOOR": "FLOOR(2.5)", "CEIL": "CEIL(2.5)", "CEILING": "CEILING(2.5)",
	"MOD": "MOD(5, 3)", "POWER": "POWER(2, 3)", "POW": "POW(2, 3)", "SQRT": "SQRT(4)", "LN": "LN(1)", "LOG": "LOG(2, 8)",
	"LOG10": "LOG10(100)", "EXP": "EXP(1)", "SIGN": "SIGN(-2)", "RANDOM": "RANDOM()", "ABS": "ABS(-1)",
	"CBRT": "CBRT(27)", "PI": "PI()", "SIN": "SIN(1)", "COS": "COS(1)", "TAN": "TAN(1)", "COT": "COT(1)",
	"ASIN": "ASIN(0.5)", "ACOS": "ACOS(0.5)", "ATAN": "ATAN(1)", "ATAN2": "ATAN2(1, 2)", "SIND": "SIND(30)",
	"COSD": "COSD(60)", "TAND": "TAND(45)", "COTD": "COTD(45)", "ASIND": "ASIND(0.5)", "ACOSD": "ACOSD(0.5)",
	"ATAND": "ATAND(1)", "ATAN2D": "ATAN2D(1, 1)", "RADIANS": "RADIANS(180)", "DEGREES": "DEGREES(1)",
	// strings
	"SUBSTRING": "SUBSTRING('abc' FROM 2 FOR 1)", "SUBSTR": "SUBSTR('abc', 2)", "LEFT": "LEFT('abc', 1)", "RIGHT": "RIGHT('abc', 1)",
	"REPLACE": "REPLACE('abc', 'b', 'x')", "BTRIM": "TRIM(' a ')", "LTRIM": "TRIM(LEADING FROM ' a')",
	"RTRIM": "RTRIM('a ')", "POSITION": "POSITION('b' IN 'abc')", "STRPOS": "STRPOS('abc', 'b')",
	"SPLIT_PART": "SPLIT_PART('a,b', ',', 2)", "LPAD": "LPAD('a', 3)", "RPAD": "RPAD('a', 3, '-')", "REVERSE": "REVERSE('abc')",
	"REPEAT": "REPEAT('a', 2)", "INITCAP": "INITCAP('ab cd')", "MD5": "MD5('a')", "STARTS_WITH": "STARTS_WITH('abc', 'a')",
	"REGEXP_REPLACE": "REGEXP_REPLACE('abc', 'b', 'x')", "LOWER": "LOWER('A')", "LCASE": "LCASE('A')", "UPPER": "UPPER('a')",
	"UCASE": "UCASE('a')", "LENGTH": "LENGTH('abc')", "CHAR_LENGTH": "CHAR_LENGTH('abc')",
	"CHARACTER_LENGTH": "CHARACTER_LENGTH('abc')", "LEN": "LEN('abc')", "FROM_HEX": "FROM_HEX('6162')",
	"UNHEX": "UNHEX('6162')", "DECODE_HEX": "DECODE_HEX('6162')", "TO_HEX": "TO_HEX('ab')", "HEX": "HEX('ab')",
	"CONCAT": "CONCAT('a', NULL, 'b')", "CONCAT_WS": "CONCAT_WS('-', 'a', 'b')", "LIKE": "'abc' LIKE 'a%'",
	"ILIKE": "'ABC' NOT ILIKE 'b%'",
	// regular expressions
	"~": "'abc' ~ 'b'", "~*": "'abc' !~* 'B'", "SIMILAR TO": "'abc' SIMILAR TO 'a%'", "REGEXP_LIKE": "REGEXP_LIKE('abc', 'b')",
	"REGEXP_COUNT": "REGEXP_COUNT('abab', 'b')", "REGEXP_INSTR": "REGEXP_INSTR('abc', 'b')",
	"REGEXP_SUBSTR": "REGEXP_SUBSTR('abc', 'b')", "REGEXP_MATCH": "REGEXP_MATCH('abc', '(b)')",
	// conditional
	"COALESCE": "COALESCE(NULL, 1)", "IFNULL": "IFNULL(NULL, 1)", "NVL": "NVL(NULL, 1)", "NULLIF": "NULLIF(1, 2)",
	"GREATEST": "GREATEST(1, 2)", "LEAST": "LEAST(1, 2)", "IIF": "IIF(TRUE, 1, 2)",
	// date and time
	"CURRENT_DATE": "CURRENT_DATE", "CURRENT_TIMESTAMP": "CURRENT_TIMESTAMP(3)", "NOW": "NOW()",
	"TRANSACTION_TIMESTAMP": "TRANSACTION_TIMESTAMP()", "STATEMENT_TIMESTAMP": "STATEMENT_TIMESTAMP()",
	"LOCALTIMESTAMP": "LOCALTIMESTAMP", "CURRENT_TIME": "CURRENT_TIME", "LOCALTIME": "LOCALTIME",
	"DATE_PART": "EXTRACT(year FROM DATE '2024-01-02')", "DATE_TRUNC": "DATE_TRUNC('month', DATE '2024-01-02')",
	"YEAR": "YEAR(DATE '2024-01-02')", "QUARTER": "QUARTER(DATE '2024-01-02')", "MONTH": "MONTH(DATE '2024-01-02')",
	"WEEK": "WEEK(DATE '2024-01-02')", "DAY": "DAY(DATE '2024-01-02')", "DAYOFMONTH": "DAYOFMONTH(DATE '2024-01-02')",
	"DAYOFYEAR": "DAYOFYEAR(DATE '2024-01-02')", "HOUR": "HOUR(TIMESTAMP '2024-01-02 03:04:05')",
	"MINUTE": "MINUTE(TIMESTAMP '2024-01-02 03:04:05')", "SECOND": "SECOND(TIMESTAMP '2024-01-02 03:04:05')",
	"EPOCH": "EPOCH(TIMESTAMP '2024-01-02 03:04:05')", "EPOCH_MS": "EPOCH_MS(TIMESTAMP '2024-01-02 03:04:05')",
	"LAST_DAY": "LAST_DAY(DATE '2024-02-02')", "AGE": "AGE(DATE '2024-01-02', DATE '2023-01-01')",
	"DATEADD": "DATEADD(day, 1, DATE '2024-01-02')", "DATE_ADD": "DATE_ADD(DATE '2024-01-02', INTERVAL 1 DAY)",
	"TIMESTAMPADD": "TIMESTAMPADD(day, 1, DATE '2024-01-02')", "DATE_SUB": "DATE_SUB(DATE '2024-01-02', INTERVAL 1 DAY)",
	"DATEDIFF":      "DATEDIFF(day, DATE '2024-01-01', DATE '2024-01-03')",
	"DATE_DIFF":     "DATE_DIFF('day', DATE '2024-01-01', DATE '2024-01-03')",
	"TIMESTAMPDIFF": "TIMESTAMPDIFF(day, DATE '2024-01-01', DATE '2024-01-03')", "MAKE_DATE": "MAKE_DATE(2024, 1, 2)",
	"MAKE_TIME": "MAKE_TIME(1, 2, 3)", "MAKE_TIMESTAMP": "MAKE_TIMESTAMP(2024, 1, 2, 3, 4, 5)",
	"MAKE_TIMESTAMPTZ": "MAKE_TIMESTAMPTZ(2024, 1, 2, 3, 4, 5.5)", "TO_TIMESTAMP": "TO_TIMESTAMP(0)",
	"TO_DATE": "TO_DATE('2024-01-02', 'YYYY-MM-DD')", "TO_CHAR": "TO_CHAR(DATE '2024-01-02', 'YYYY')",
	"TIMEZONE":         "TIMESTAMP '2024-01-02 03:04:05' AT TIME ZONE 'America/New_York'",
	"CONVERT_TIMEZONE": "CONVERT_TIMEZONE('UTC', 'America/New_York', TIMESTAMP '2024-01-02 03:04:05')",
	"DATE_BIN":         "DATE_BIN(INTERVAL '15 minutes', TIMESTAMP '2024-01-02 03:04:05', TIMESTAMP '2024-01-01')",
	"__INTERVAL":       "INTERVAL 7 DAY",
	// JSON
	"->": `'{"a": 1}' -> 'a'`, "->>": `'{"a": 1}' ->> 'a'`, "#>": `'{"a": [1]}' #> '{a,0}'`, "#>>": `'{"a": [1]}' #>> '{a,0}'`,
	"JSON_EXTRACT_PATH": `JSON_EXTRACT_PATH('{"a": 1}', 'a')`, "JSON_EXTRACT_PATH_TEXT": `JSON_EXTRACT_PATH_TEXT('{"a": 1}', 'a')`,
	"JSONB_EXTRACT_PATH": `JSONB_EXTRACT_PATH('{"a": 1}', 'a')`, "JSONB_EXTRACT_PATH_TEXT": `JSONB_EXTRACT_PATH_TEXT('{"a": 1}', 'a')`,
	"JSON_TYPEOF": "JSON_TYPEOF('1')", "JSONB_TYPEOF": "JSONB_TYPEOF('1')", "JSON_ARRAY_LENGTH": "JSON_ARRAY_LENGTH('[1]')",
	"JSONB_ARRAY_LENGTH": "JSONB_ARRAY_LENGTH('[1]')", "JSON_BUILD_OBJECT": "JSON_BUILD_OBJECT('a', 1)",
	"JSONB_BUILD_OBJECT": "JSONB_BUILD_OBJECT('a', 1)", "JSON_BUILD_ARRAY": "JSON_BUILD_ARRAY(1, 2)",
	"JSONB_BUILD_ARRAY": "JSONB_BUILD_ARRAY(1, 2)", "JSON_OBJECT": "JSON_OBJECT('{a,1}')", "JSONB_OBJECT": "JSONB_OBJECT('{a,1}')",
	"TO_JSON": "TO_JSON(1)", "TO_JSONB": "TO_JSONB(1)",
	"JSON_VALUE": `JSON_VALUE('{"a": 1}', '$.b' DEFAULT 0 ON EMPTY DEFAULT 1 ON ERROR)`,
	"JSON_QUERY": `JSON_QUERY('{"a": [1]}', '$.a')`, "JSON_EXISTS": `JSON_EXISTS('{"a": 1}', '$.a')`,
	"__JSON": "'1'::json", "__JSONB": "CAST('1' AS JSONB)", "__IS_JSON": "'1' IS JSON",
	"__JSON_OBJECT": "JSON_OBJECT('a' VALUE 1)", "__JSON_ARRAY": "JSON_ARRAY(1, 2)",
	// grouping and MERGE (merge_action() is evaluated as RETURNING does)
	"GROUPING":     "SELECT x, GROUPING(x) FROM generate_series(1, 2) AS g(x) GROUP BY ROLLUP (x)",
	"MERGE_ACTION": "MERGE_ACTION()",
	// aggregates
	"COUNT": "COUNT(*)", "SUM": "SUM(x)", "AVG": "AVG(x)", "MIN": "MIN(x)", "MAX": "MAX(x)",
	"STRING_AGG": "STRING_AGG(CAST(x AS VARCHAR), ',')", "LISTAGG": "LISTAGG(CAST(x AS VARCHAR))",
	"BOOL_OR": "BOOL_OR(x > 1)", "BOOL_AND": "BOOL_AND(x > 1)", "EVERY": "EVERY(x > 1)", "ANY_VALUE": "ANY_VALUE(x)",
	"STDDEV_SAMP": "STDDEV_SAMP(x)", "STDDEV": "STDDEV(x)", "STDDEV_POP": "STDDEV_POP(x)", "VAR_SAMP": "VAR_SAMP(x)",
	"VARIANCE": "VARIANCE(x)", "VAR_POP": "VAR_POP(x)",
	"PERCENTILE_CONT": "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)",
	"PERCENTILE_DISC": "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY x)", "MODE": "MODE() WITHIN GROUP (ORDER BY x)",
	"MEDIAN": "MEDIAN(x)", "JSON_AGG": "JSON_AGG(x ORDER BY x DESC)", "JSONB_AGG": "JSONB_AGG(x)",
	"JSON_OBJECT_AGG": "JSON_OBJECT_AGG(CAST(x AS VARCHAR), x)", "JSONB_OBJECT_AGG": "JSONB_OBJECT_AGG(CAST(x AS VARCHAR), x)",
	// window functions
	"ROW_NUMBER": "ROW_NUMBER()", "RANK": "RANK()", "DENSE_RANK": "DENSE_RANK()", "PERCENT_RANK": "PERCENT_RANK()",
	"CUME_DIST": "CUME_DIST()", "NTILE": "NTILE(2)", "LAG": "LAG(x)", "LEAD": "LEAD(x, 1, 0)", "FIRST_VALUE": "FIRST_VALUE(x)",
	"LAST_VALUE": "LAST_VALUE(x) IGNORE NULLS", "NTH_VALUE": "NTH_VALUE(x, 2)",
	// table functions
	"GENERATE_SERIES": "generate_series(1, 3)",
}

// sampleQueries returns the queries that call function name (of d) with
// its sample.
func sampleQueries(d *funcDef, sample string) []string {
	const rows = " FROM generate_series(1, 3) AS g(x)"
	if strings.HasPrefix(sample, "SELECT ") {
		return []string{sample}
	}
	switch d.kind {
	case aggregateKind:
		out := []string{"SELECT " + sample + rows}
		if d.over == overOK {
			out = append(out, "SELECT "+sample+" OVER (ORDER BY x)"+rows)
		}
		return out
	case windowKind:
		return []string{"SELECT " + sample + " OVER (ORDER BY x)" + rows}
	case tableKind:
		return []string{"SELECT * FROM " + sample}
	}
	return []string{"SELECT " + sample}
}

// callsIn reports whether a query calls the function name: in an
// expression of its SELECT list, with or without OVER, or in FROM.
func callsIn(sel *SelectStmt, name string) bool {
	if sel.FromFunc != nil && sel.FromFunc.Name == name {
		return true
	}
	found := false
	for _, it := range sel.Items {
		walkExpr(it.Expr, func(x Expr) {
			switch v := x.(type) {
			case *Func:
				found = found || v.Name == name
			case *WindowFunc:
				found = found || v.Func.Name == name
			}
		})
	}
	return found
}

// runInMemory plans and runs a query that reads no table.
func runInMemory(sel *SelectStmt) ([][]Value, error) {
	ctx := context.Background()
	e := &executor{cache: newExecCache()}
	plan, err := e.planSelect(ctx, sel, nil)
	if err != nil {
		return nil, err
	}
	return e.runSelect(ctx, plan, nil)
}

// Every registered name evaluates: its sample call is resolved, planned and
// computed without an error, and gives a row.
func TestFuncRegistryEvaluates(t *testing.T) {
	for name, d := range funcsByName {
		sample, ok := funcSamples[name]
		if !ok {
			t.Errorf("%s has no sample call in funcSamples", name)
			continue
		}
		for _, q := range sampleQueries(d, sample) {
			stmts, err := ParseScript(q)
			if err != nil {
				t.Errorf("%s: %s: %v", name, q, err)
				continue
			}
			sel := stmts[0].Stmt.(*SelectStmt)
			if !callsIn(sel, name) {
				t.Errorf("%s: %s doesn't call %s", name, q, name)
				continue
			}
			if d.impl == implMergeAction {
				// Only MERGE … RETURNING has a merge action.
				v, err := (&evalEnv{action: "INSERT"}).eval(sel.Items[0].Expr)
				if err != nil || v.Text() != "INSERT" {
					t.Errorf("%s: %s = %v, %v", name, q, v, err)
				}
				continue
			}
			rows, err := runInMemory(sel)
			if err != nil || len(rows) == 0 {
				t.Errorf("%s: %s: rows %v, error %v", name, q, rows, err)
			}
		}
	}
	for name := range funcSamples {
		if _, ok := lookupFunc(name); !ok {
			t.Errorf("funcSamples has %s, which isn't registered", name)
		}
	}
}

// Every name the evaluator and the parser handle is registered: the
// strings compared with a call's name (switch cases and ==), the names the
// parser gives the calls it builds, and the keys of the function sets that
// aren't derived from the registry.
func TestFuncRegistryCoversEvaluator(t *testing.T) {
	fset := gotoken.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	type use struct {
		name string
		pos  gotoken.Pos
	}
	var uses []use
	lit := func(e ast.Expr) (string, bool) {
		if b, ok := e.(*ast.BasicLit); ok && b.Kind == gotoken.STRING {
			if s, err := strconv.Unquote(b.Value); err == nil && s != "" {
				return s, true
			}
		}
		return "", false
	}
	add := func(e ast.Expr) {
		if s, ok := lit(e); ok {
			uses = append(uses, use{s, e.Pos()})
		}
	}
	// callName reports whether e is a call's name: f.Name, x.Name or v.Name
	// (the names of *Func variables), a.fn.Name or w.Func.Name.
	callName := func(e ast.Expr) bool {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Name" {
			return false
		}
		switch x := sel.X.(type) {
		case *ast.Ident:
			return x.Name == "f" || x.Name == "x" || x.Name == "v"
		case *ast.SelectorExpr:
			return x.Sel.Name == "fn" || x.Sel.Name == "Func"
		}
		return false
	}
	files := 0
	for _, ent := range entries {
		n := ent.Name()
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") || n == "funcs.go" {
			continue
		}
		file, err := goparser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files++
		ast.Inspect(file, func(node ast.Node) bool {
			switch x := node.(type) {
			case *ast.SwitchStmt:
				// switch f.Name { case "A", "B": … }, and
				// switch name := f.Name; { case name == "A": … }.
				var bound string
				if as, ok := x.Init.(*ast.AssignStmt); ok && len(as.Rhs) == 1 && callName(as.Rhs[0]) {
					bound = as.Lhs[0].(*ast.Ident).Name
				}
				for _, s := range x.Body.List {
					for _, c := range s.(*ast.CaseClause).List {
						if x.Tag != nil && callName(x.Tag) {
							add(c)
						}
						if bound != "" {
							ast.Inspect(c, func(n ast.Node) bool {
								if b, ok := n.(*ast.BinaryExpr); ok {
									if id, ok := b.X.(*ast.Ident); ok && id.Name == bound {
										add(b.Y)
									}
								}
								return true
							})
						}
					}
				}
			case *ast.BinaryExpr:
				if (x.Op == gotoken.EQL || x.Op == gotoken.NEQ) && callName(x.X) {
					add(x.Y)
				}
			case *ast.CompositeLit:
				// &Func{Name: "…"}, as the parser builds calls.
				if id, ok := x.Type.(*ast.Ident); ok && id.Name == "Func" {
					for _, el := range x.Elts {
						if kv, ok := el.(*ast.KeyValueExpr); ok && kv.Key.(*ast.Ident).Name == "Name" {
							add(kv.Value)
						}
					}
				}
			case *ast.CallExpr:
				// p.finishCall("SUBSTRING", s)
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "finishCall" && len(x.Args) > 0 {
					add(x.Args[0])
				}
			case *ast.ValueSpec:
				// var statFuncs = map[string]bool{"STDDEV": true, …}
				if len(x.Names) == 1 && strings.HasSuffix(x.Names[0].Name, "Funcs") && len(x.Values) == 1 {
					if cl, ok := x.Values[0].(*ast.CompositeLit); ok {
						for _, el := range cl.Elts {
							add(el.(*ast.KeyValueExpr).Key)
						}
					}
				}
			}
			return true
		})
	}
	if files < 20 || len(uses) < 100 {
		t.Fatalf("read %d files and found %d names: the scan is broken", files, len(uses))
	}
	for _, u := range uses {
		if _, ok := lookupFunc(u.name); !ok {
			t.Errorf("%s: %q is used as a function name but isn't registered in funcs.go", fset.Position(u.pos), u.name)
		}
	}
}

// The registry is consistent with itself.
func TestFuncRegistryShape(t *testing.T) {
	for i := range funcDefs {
		d := &funcDefs[i]
		for _, n := range append([]string{d.name}, d.aliases...) {
			if n != strings.ToUpper(n) {
				t.Errorf("%s: names are upper case, as calls are parsed", n)
			}
		}
		switch {
		case d.min < 0 || (d.max != many && d.max < d.min):
			t.Errorf("%s: argument counts %d to %d", d.name, d.min, d.max)
		case d.max != many && len(d.args) > d.max:
			t.Errorf("%s: types for %d arguments, at most %d", d.name, len(d.args), d.max)
		case d.kind == windowKind && d.over != overOK:
			t.Errorf("%s: a window function must take OVER", d.name)
		case d.kind != aggregateKind && d.kind != windowKind && d.over != noOver:
			t.Errorf("%s: only aggregates and window functions take OVER", d.name)
		case d.orderedSet && (d.kind != aggregateKind || d.over == overOK):
			t.Errorf("%s: an ordered-set aggregate isn't computed over frames", d.name)
		case d.star && d.kind != aggregateKind:
			t.Errorf("%s: only an aggregate takes *", d.name)
		case d.nulls && d.kind != windowKind:
			t.Errorf("%s: only window functions take IGNORE NULLS", d.name)
		case d.within != argAny && !d.orderedSet:
			t.Errorf("%s: only an ordered-set aggregate has a WITHIN GROUP type", d.name)
		case d.kind != scalarKind && d.impl != implBuiltin && !(d.kind == aggregateKind && d.impl == implJSON):
			t.Errorf("%s: only scalar functions (and the JSON aggregates) have an implementation family", d.name)
		case d.internal && d.kind != scalarKind:
			t.Errorf("%s: only scalar functions are internal", d.name)
		}
	}
	for _, c := range []struct {
		d    funcDef
		want string
	}{
		{funcDef{min: 0, max: 0}, "no arguments"},
		{funcDef{min: 1, max: 1}, "1 argument"},
		{funcDef{min: 3, max: 3}, "3 arguments"},
		{funcDef{min: 0, max: 1}, "0 or 1 arguments"},
		{funcDef{min: 2, max: 7}, "2 to 7 arguments"},
		{funcDef{min: 1, max: many}, "at least 1 argument"},
		{funcDef{min: 2, max: many}, "at least 2 arguments"},
	} {
		if got := c.d.argCount(); got != c.want {
			t.Errorf("argCount(%d, %d) = %q, want %q", c.d.min, c.d.max, got, c.want)
		}
	}
	if got := overNames(); !strings.HasPrefix(got, "ROW_NUMBER, RANK,") || !strings.Contains(got, ", EVERY,") ||
		strings.Contains(got, "MEDIAN") || strings.Contains(got, "JSON_AGG") {
		t.Errorf("overNames() = %s", got)
	}
}

// Expressions that aren't bound (a DEFAULT, and the expressions the unit
// tests evaluate) get the same errors as bound ones.
func TestFuncUnboundErrors(t *testing.T) {
	for _, c := range []struct{ expr, err string }{
		{"NOSUCHFUNC(1, 'a', NULL, 1.5, DATE '2024-01-01')", "function nosuchfunc(bigint, varchar, unknown, numeric, date) does not exist"},
		{"NoSuchFunc()", "function nosuchfunc() does not exist"},
		{"UPPER(1, 2)", "UPPER expects 1 argument"},
		{"UPPER(NULL, NULL)", "UPPER expects 1 argument"},
		{"UPPER(DISTINCT 'a')", "UPPER does not accept * or DISTINCT"},
		{"COALESCE(NOSUCHFUNC(NULL), 1)", "function nosuchfunc(unknown) does not exist"},
		{"CASE WHEN TRUE THEN 1 ELSE UPPER(NOSUCHFUNC(2), 3) END", "function nosuchfunc(bigint) does not exist"},
		{"CAST(NOW(1, 2) AS VARCHAR)", "NOW expects 0 or 1 arguments"},
		{"ROW_NUMBER()", "window function ROW_NUMBER requires an OVER clause"},
		{"generate_series(1, 2)", "generate_series is only supported in FROM"},
		{"LOWER('a') FILTER (WHERE TRUE)", "FILTER specified, but LOWER is not an aggregate function"},
		// The parser's internal functions can't be called by name.
		{"__JSON('1')", "function __json(varchar) does not exist"},
		{"__IS_JSON('1')", "function __is_json(varchar) does not exist"},
	} {
		err := checkCalls(parseTestExpr(t, c.expr))
		if err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("checkCalls(%s) = %v, want an error containing %q", c.expr, err, c.err)
		}
	}
	// Type inference and evaluation report them too.
	if _, _, err := evalTestExpr(t, "NOSUCHFUNC(1)"); err == nil || !strings.Contains(err.Error(), "function nosuchfunc(bigint) does not exist") {
		t.Errorf("NOSUCHFUNC(1): %v", err)
	}
	if _, _, err := evalTestExpr(t, "UPPER(NULL, NULL)"); err == nil || !strings.Contains(err.Error(), "UPPER expects 1 argument") {
		t.Errorf("UPPER(NULL, NULL): %v", err)
	}
	for _, ok := range []string{"COALESCE()", "CONCAT()", "NOW(3)", "IFNULL(NULL, 2, 3)", "COUNT(*)"} {
		if err := checkCalls(parseTestExpr(t, ok)); err != nil {
			t.Errorf("checkCalls(%s): %v", ok, err)
		}
	}
	for _, c := range []struct {
		t    ColType
		want string
	}{
		{typeNull, "unknown"}, {typeInt32, "integer"}, {decimalType(10, 2), "numeric"}, {typeString, "varchar"},
		{typeFloat64, "double precision"}, {typeTimestamp, "timestamp"}, {typeTimestampTZ, "timestamp with time zone"},
		{typeTimeUS, "time"}, {typeInterval, "interval"},
	} {
		if got := sigTypeName(c.t); got != c.want {
			t.Errorf("sigTypeName(%s) = %q, want %q", c.t.SQLName(), got, c.want)
		}
	}
}
