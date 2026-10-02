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

// Unit tests for the math, string and conditional scalar functions. They
// need no Redis server: expressions without columns are parsed, typed and
// evaluated directly, and WHERE clauses are planned against a table
// description.

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"
)

// parseTestExpr parses `SELECT expr` and returns the expression.
func parseTestExpr(t *testing.T, expr string) Expr {
	t.Helper()
	stmts, err := ParseScript("SELECT " + expr)
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	return stmts[0].Stmt.(*SelectStmt).Items[0].Expr
}

// evalTestExpr types and evaluates a constant expression and renders the
// result as the driver returns it (coerced to the inferred type).
func evalTestExpr(t *testing.T, expr string) (string, string, error) {
	t.Helper()
	e := parseTestExpr(t, expr)
	typ, err := inferType(e, nil, nil)
	if err != nil {
		return "", "", err
	}
	v, err := (&evalEnv{}).eval(e)
	if err != nil {
		return "", "", err
	}
	out, err := Coerce(v, typ)
	if err != nil {
		t.Fatalf("%s: value %s (%s) does not fit the inferred type %s: %v", expr, v.Text(), v.T.SQLName(), typ.SQLName(), err)
	}
	if out.Null {
		return "NULL", typ.SQLName(), nil
	}
	return out.Text(), typ.SQLName(), nil
}

type scalarCase struct {
	expr, want, typ string
}

func runScalarCases(t *testing.T, cases []scalarCase) {
	t.Helper()
	for _, c := range cases {
		got, typ, err := evalTestExpr(t, c.expr)
		if err != nil {
			t.Errorf("%s: %v", c.expr, err)
			continue
		}
		if got != c.want || (c.typ != "" && typ != c.typ) {
			t.Errorf("%s = %q (%s), want %q (%s)", c.expr, got, typ, c.want, c.typ)
		}
	}
}

// runScalarCasesULP is runScalarCases for DOUBLE results, which may be 1 ulp
// from the value wanted (the next double either way).
func runScalarCasesULP(t *testing.T, cases []scalarCase) {
	t.Helper()
	for _, c := range cases {
		got, typ, err := evalTestExpr(t, c.expr)
		if err != nil {
			t.Errorf("%s: %v", c.expr, err)
			continue
		}
		g, gerr := strconv.ParseFloat(got, 64)
		w, werr := strconv.ParseFloat(c.want, 64)
		if werr != nil {
			t.Fatalf("%s: want %q: %v", c.expr, c.want, werr)
		}
		if gerr != nil || (g != w && math.Nextafter(w, g) != g) || typ != c.typ {
			t.Errorf("%s = %q (%s), want %q (%s) within 1 ulp", c.expr, got, typ, c.want, c.typ)
		}
	}
}

func TestScalarMath(t *testing.T) {
	runScalarCases(t, []scalarCase{
		// ROUND on NUMERIC is exact and rounds half away from zero; the scale
		// follows the digits argument, with room for a carry.
		{"ROUND(2.5)", "3", "NUMERIC(2,0)"},
		{"ROUND(-2.5)", "-3", "NUMERIC(2,0)"},
		{"ROUND(2.4999)", "2", "NUMERIC(2,0)"},
		{"ROUND(2.345, 2)", "2.35", "NUMERIC(4,2)"},
		{"ROUND(-2.345, 2)", "-2.35", "NUMERIC(4,2)"},
		{"ROUND(9.99, 1)", "10.0", "NUMERIC(3,1)"},
		{"ROUND(1.23, 5)", "1.23", "NUMERIC(3,2)"},
		{"ROUND(1234.567, -2)", "1200", "NUMERIC(5,0)"},
		{"ROUND(1250.0, -2)", "1300", "NUMERIC(5,0)"},
		{"ROUND(0.5, -1)", "0", "NUMERIC(2,0)"},
		{"ROUND(123456789012345678901234567890.5)", "123456789012345678901234567891", "NUMERIC(31,0)"},
		{"ROUND(2.5, -1000000000)", "0", "NUMERIC(2,0)"},
		// Integers keep their type; a negative n rounds to tens, hundreds, …
		{"ROUND(5)", "5", "BIGINT"},
		{"ROUND(5, 2)", "5", "BIGINT"},
		{"ROUND(1250, -2)", "1300", "BIGINT"},
		{"ROUND(-1250, -2)", "-1300", "BIGINT"},
		{"ROUND(1249, -2)", "1200", "BIGINT"},
		{"ROUND(CAST(149 AS SMALLINT), -2)", "100", "SMALLINT"},
		// DOUBLE also rounds half away from zero (not to even), on the
		// decimal value as written: 2.675 is 2.67499999… in binary.
		{"ROUND(CAST(0.5 AS DOUBLE))", "1", "DOUBLE PRECISION"},
		{"ROUND(CAST(1.5 AS DOUBLE))", "2", "DOUBLE PRECISION"},
		{"ROUND(CAST(2.5 AS DOUBLE))", "3", "DOUBLE PRECISION"},
		{"ROUND(CAST(-2.5 AS DOUBLE))", "-3", "DOUBLE PRECISION"},
		{"ROUND(2.675e0, 2)", "2.68", "DOUBLE PRECISION"},
		{"ROUND(-2.675e0, 2)", "-2.68", "DOUBLE PRECISION"},
		{"ROUND(1.005e0, 2)", "1.01", "DOUBLE PRECISION"},
		{"ROUND(1234.5e0, -2)", "1200", "DOUBLE PRECISION"},
		{"ROUND(1.5e300, -300)", "2e+300", "DOUBLE PRECISION"},
		{"ROUND(1e-320, 2)", "0", "DOUBLE PRECISION"},
		{"ROUND(CAST(2.675 AS REAL), 2)", "2.68", "DOUBLE PRECISION"},
		{"ROUND('2.5')", "3", "DOUBLE PRECISION"},
		{"ROUND(CAST('NaN' AS DOUBLE))", "NaN", "DOUBLE PRECISION"},
		{"ROUND(2.5, NULL)", "NULL", "NUMERIC(3,1)"}, // digits not a literal: input scale
		{"ROUND(NULL)", "NULL", "NULL"},

		{"TRUNC(2.789, 1)", "2.7", "NUMERIC(2,1)"},
		{"TRUNC(-2.789)", "-2", "NUMERIC(1,0)"},
		{"TRUNC(1299, -2)", "1200", "BIGINT"},
		{"TRUNC(-1299, -2)", "-1200", "BIGINT"},
		{"TRUNC(2.789e0, 2)", "2.78", "DOUBLE PRECISION"},
		{"TRUNC(-2.789e0)", "-2", "DOUBLE PRECISION"},

		{"FLOOR(-1.5)", "-2", "NUMERIC(2,0)"},
		{"FLOOR(1.5)", "1", "NUMERIC(2,0)"},
		{"CEIL(1.2)", "2", "NUMERIC(2,0)"},
		{"CEIL(-1.2)", "-1", "NUMERIC(2,0)"},
		{"CEILING(9.1)", "10", "NUMERIC(2,0)"},
		{"FLOOR(7)", "7", "BIGINT"},
		{"CEIL(-1.5e0)", "-1", "DOUBLE PRECISION"},
		{"FLOOR(-1.5e0)", "-2", "DOUBLE PRECISION"},

		// MOD takes the sign of the dividend; NUMERIC stays exact.
		{"MOD(7, 3)", "1", "BIGINT"},
		{"MOD(-7, 3)", "-1", "BIGINT"},
		{"MOD(7, -3)", "1", "BIGINT"},
		{"MOD(-9223372036854775808, -1)", "0", "BIGINT"},
		{"MOD(7.5, 2)", "1.5", "NUMERIC(2,1)"},
		{"MOD(-7.5, 2)", "-1.5", "NUMERIC(2,1)"},
		{"MOD(10, 0.3)", "0.1", "NUMERIC(38,1)"},
		{"MOD(7.5e0, 2)", "1.5", "DOUBLE PRECISION"},
		{"MOD(CAST(7 AS INTEGER), 3)", "1", "INTEGER"},

		{"POWER(2, 10)", "1024", "DOUBLE PRECISION"},
		{"POWER(2, -1)", "0.5", "DOUBLE PRECISION"},
		{"POWER(-8, 3)", "-512", "DOUBLE PRECISION"},
		{"POW(9, 0.5)", "3", "DOUBLE PRECISION"},
		{"POWER(0, 0)", "1", "DOUBLE PRECISION"},
		{"SQRT(16)", "4", "DOUBLE PRECISION"},
		{"SQRT(2)", "1.4142135623730951", "DOUBLE PRECISION"},
		{"SQRT(0)", "0", "DOUBLE PRECISION"},
		{"LN(1)", "0", "DOUBLE PRECISION"},
		{"LN(EXP(2))", "2", "DOUBLE PRECISION"},
		{"LOG(100)", "2", "DOUBLE PRECISION"},
		{"LOG(0.001)", "-3", "DOUBLE PRECISION"},
		{"LOG10(1000)", "3", "DOUBLE PRECISION"},
		{"LOG(2, 8)", "3", "DOUBLE PRECISION"},
		{"LOG(10, 1000)", "3", "DOUBLE PRECISION"},
		{"EXP(0)", "1", "DOUBLE PRECISION"},
		{"EXP(1)", "2.718281828459045", "DOUBLE PRECISION"},
		{"EXP(-1000.0)", "0", "DOUBLE PRECISION"}, // NUMERIC: no underflow error
		{"SQRT(NULL)", "NULL", "DOUBLE PRECISION"},

		{"SIGN(-5)", "-1", "BIGINT"},
		{"SIGN(0)", "0", "BIGINT"},
		{"SIGN(2.5)", "1", "NUMERIC(1,0)"},
		{"SIGN(-0.5e0)", "-1", "DOUBLE PRECISION"},
		{"SIGN(CAST(0 AS DOUBLE))", "0", "DOUBLE PRECISION"},
	})

	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		got, typ, err := evalTestExpr(t, "RANDOM()")
		f, perr := strconv.ParseFloat(got, 64)
		if err != nil || perr != nil || typ != "DOUBLE PRECISION" || f < 0 || f >= 1 {
			t.Fatalf("RANDOM() = %s (%s), %v; want a DOUBLE in [0, 1)", got, typ, err)
		}
		seen[got] = true
	}
	if len(seen) < 90 {
		t.Errorf("RANDOM() returned only %d distinct values in 100 calls", len(seen))
	}
}

// TestScalarTrigonometry checks the trigonometric functions, PI, CBRT,
// RADIANS and DEGREES. The values are Postgres's: the examples of its
// documentation (sin(1), cot(0.5), degrees(0.5), …) and the exact results
// its degree functions guarantee at 0, 30, 45, 60 and 90 degrees. The
// radian functions are Go's math package, which can differ in the last bit
// from the C library Postgres uses (Go's tan(1) is 1.557407724654902,
// glibc's 1.5574077246549023) and between processors, as Go fuses
// multiply-adds on arm64 but not on amd64: tan(π/4) is 0.9999999999999998
// on arm64, 1 on amd64 and 0.9999999999999999 in glibc. So their inexact
// results are compared to Postgres's within 1 ulp.
func TestScalarTrigonometry(t *testing.T) {
	const d = "DOUBLE PRECISION"
	runScalarCasesULP(t, []scalarCase{
		{"SIN(1)", "0.8414709848078965", d},
		{"SIN(1.0)", "0.8414709848078965", d}, // a NUMERIC argument
		{"SIN('1')", "0.8414709848078965", d},
		{"COS(1)", "0.5403023058681398", d},
		{"TAN(1)", "1.5574077246549023", d},
		{"COT(0.5)", "1.830487721712452", d},
		{"ASIN(1)", "1.5707963267948966", d},
		{"ATAN(1)", "0.7853981633974483", d},
		{"ATAN2(1, 0)", "1.5707963267948966", d},
		{"TAN(PI() / 4)", "0.9999999999999999", d},
		{"COT(PI() / 4)", "1.0000000000000002", d},
	})
	runScalarCases(t, []scalarCase{
		{"PI()", "3.141592653589793", d},
		{"ACOS(1)", "0", d},
		{"SIN(PI() / 2)", "1", d},
		{"COS(PI())", "-1", d},
		{"SIN(0)", "0", d},
		{"COS(0)", "1", d},
		{"TAN(0)", "0", d},
		{"COT(0)", "Infinity", d}, // not an overflow, as in Postgres
		{"ACOS(-1) = PI()", "true", "BOOLEAN"},
		{"ASIN(-1) = -PI() / 2", "true", "BOOLEAN"},
		{"ATAN2(1, 1) = PI() / 4", "true", "BOOLEAN"},
		{"ATAN2(0, -1) = PI()", "true", "BOOLEAN"},
		{"ATAN(CAST('Infinity' AS DOUBLE)) = PI() / 2", "true", "BOOLEAN"},
		{"SIN(CAST('NaN' AS DOUBLE))", "NaN", d},
		{"ASIN(CAST('NaN' AS DOUBLE))", "NaN", d},
		{"ATAN2(1, CAST('NaN' AS DOUBLE))", "NaN", d},
		{"SIN(NULL)", "NULL", d},
		{"ATAN2(NULL, 1)", "NULL", d},
		{"ATAN2D(1, NULL)", "NULL", d},
		{"PI() + NULL", "NULL", d},

		// Degrees: exact at 0, 30, 45, 60 and 90, and in every quadrant.
		{"SIND(30)", "0.5", d},
		{"SIND(90)", "1", d},
		{"SIND(150)", "0.5", d},
		{"SIND(180)", "0", d},
		{"SIND(210)", "-0.5", d},
		{"SIND(270)", "-1", d},
		{"SIND(-30)", "-0.5", d},
		{"SIND(390)", "0.5", d},
		{"SIND(-720)", "0", d},
		{"COSD(0)", "1", d},
		{"COSD(60)", "0.5", d},
		{"COSD(90)", "0", d},
		{"COSD(120)", "-0.5", d},
		{"COSD(180)", "-1", d},
		{"COSD(-60)", "0.5", d},
		{"COSD(300)", "0.5", d},
		{"TAND(45)", "1", d},
		{"TAND(135)", "-1", d},
		{"TAND(-45)", "-1", d},
		{"TAND(0)", "0", d},
		{"TAND(180)", "0", d}, // not -0
		{"TAND(90)", "Infinity", d},
		{"TAND(-90)", "-Infinity", d},
		{"COTD(45)", "1", d},
		{"COTD(-45)", "-1", d},
		{"COTD(90)", "0", d},
		{"COTD(270)", "0", d},
		{"COTD(0)", "Infinity", d},
		{"ASIND(0.5)", "30", d},
		{"ASIND(1)", "90", d},
		{"ASIND(-0.5)", "-30", d},
		{"ASIND(0)", "0", d},
		{"ACOSD(0.5)", "60", d},
		{"ACOSD(1)", "0", d},
		{"ACOSD(0)", "90", d},
		{"ACOSD(-0.5)", "120", d},
		{"ACOSD(-1)", "180", d},
		{"ATAND(1)", "45", d},
		{"ATAND(-1)", "-45", d},
		{"ATAND(CAST('Infinity' AS DOUBLE))", "90", d},
		{"ATAN2D(1, 0)", "90", d},
		{"ATAN2D(1, 1)", "45", d},
		{"ATAN2D(0, -1)", "180", d},
		{"ATAN2D(-1, 0)", "-90", d},
		{"SIND(CAST('NaN' AS DOUBLE))", "NaN", d},

		{"RADIANS(45.0)", "0.7853981633974483", d},
		{"RADIANS(180)", "3.141592653589793", d},
		{"RADIANS(180) = PI()", "true", "BOOLEAN"},
		{"DEGREES(0.5)", "28.64788975654116", d},
		{"DEGREES(PI())", "180", d},
		{"DEGREES(PI() / 2)", "90", d},
		{"DEGREES(CAST('Infinity' AS DOUBLE))", "Infinity", d},
		{"CBRT(64.0)", "4", d},
		{"CBRT(27)", "3", d},
		{"CBRT(-8)", "-2", d},
		{"CBRT(2)", "1.2599210498948732", d},
		{"CBRT(0)", "0", d},
	})
	for _, c := range []struct{ expr, err string }{
		{"ASIN(2)", "ASIN: input is out of range"},
		{"ASIN(-1.0000001)", "ASIN: input is out of range"},
		{"ACOS(-1.5)", "ACOS: input is out of range"},
		{"ASIND(1.5)", "ASIND: input is out of range"},
		{"ACOSD(-2)", "ACOSD: input is out of range"},
		{"SIN(CAST('Infinity' AS DOUBLE))", "SIN: input is out of range"},
		{"COS(CAST('-Infinity' AS DOUBLE))", "COS: input is out of range"},
		{"TAN(CAST('Infinity' AS DOUBLE))", "TAN: input is out of range"},
		{"COT(CAST('Infinity' AS DOUBLE))", "COT: input is out of range"},
		{"SIND(CAST('Infinity' AS DOUBLE))", "SIND: input is out of range"},
		{"COSD(CAST('-Infinity' AS DOUBLE))", "COSD: input is out of range"},
		{"TAND(CAST('Infinity' AS DOUBLE))", "TAND: input is out of range"},
		{"COTD(CAST('Infinity' AS DOUBLE))", "COTD: input is out of range"},
		{"DEGREES(1e308)", "DEGREES: value out of range: overflow"},
		{"RADIANS(5e-324)", "RADIANS: value out of range: underflow"},
		{"SIN(TRUE)", "SIN expects a number, got BOOLEAN"},
		{"ATAN2D(1, DATE '2024-01-01')", "ATAN2D expects a number, got DATE"},
		{"COS('abc')", `COS: cannot convert "abc"`},
		{"SIN()", "SIN expects 1 argument"},
		{"ATAN2(1)", "ATAN2 expects 2 arguments"},
		{"PI(1)", "PI expects no arguments"},
	} {
		if _, _, err := evalTestExpr(t, c.expr); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: error %v, want one containing %q", c.expr, err, c.err)
		}
	}
	if !isConstant(parseTestExpr(t, "SIN(PI())")) {
		t.Errorf("SIN(PI()) should be a constant")
	}
}

func TestScalarStrings(t *testing.T) {
	runScalarCases(t, []scalarCase{
		// Positions are 1-based and count characters; a start before the
		// first character still counts toward the length, as in Postgres.
		{"SUBSTRING('hello', 2, 3)", "ell", "VARCHAR"},
		{"SUBSTRING('hello', 2)", "ello", "VARCHAR"},
		{"SUBSTRING('hello', 0, 2)", "h", "VARCHAR"},
		{"SUBSTRING('hello', -1, 3)", "h", "VARCHAR"},
		{"SUBSTRING('hello', 10)", "", "VARCHAR"},
		{"SUBSTRING('hello', 5, 10)", "o", "VARCHAR"},
		{"SUBSTRING('hello', 3, 0)", "", "VARCHAR"},
		{"SUBSTRING('hello', 2, 9223372036854775807)", "ello", "VARCHAR"},
		{"SUBSTRING('héllo wörld' FROM 2 FOR 4)", "éllo", "VARCHAR"},
		{"SUBSTRING('hello' FROM 3)", "llo", "VARCHAR"},
		{"SUBSTRING('hello' FOR 2)", "he", "VARCHAR"},
		{"SUBSTRING('hello' FROM 1 + 1 FOR LENGTH('ab'))", "el", "VARCHAR"},
		{"SUBSTR('こんにちは', 2, 2)", "んに", "VARCHAR"},
		{"SUBSTRING('', 1, 2)", "", "VARCHAR"},
		{"SUBSTRING(X'616263', 2)", "bc", "VARBINARY"},
		{"SUBSTRING(12345, 2, 2)", "23", "VARCHAR"},
		{"SUBSTRING('hello', NULL)", "NULL", "VARCHAR"},

		{"LEFT('hello', 2)", "he", "VARCHAR"},
		{"LEFT('hello', -2)", "hel", "VARCHAR"},
		{"LEFT('hello', 10)", "hello", "VARCHAR"},
		{"LEFT('hello', -10)", "", "VARCHAR"},
		{"LEFT('héllo', 2)", "hé", "VARCHAR"},
		{"RIGHT('hello', 2)", "lo", "VARCHAR"},
		{"RIGHT('hello', -2)", "llo", "VARCHAR"},
		{"RIGHT('héllo', 4)", "éllo", "VARCHAR"},
		{"RIGHT('hi', 5)", "hi", "VARCHAR"},
		{"RIGHT('hi', -5)", "", "VARCHAR"},
		{"RIGHT('hi', -9223372036854775808)", "", "VARCHAR"},

		{"REPLACE('hello world', 'o', '0')", "hell0 w0rld", "VARCHAR"},
		{"REPLACE('abc', '', 'x')", "abc", "VARCHAR"},
		{"REPLACE('aaa', 'a', '')", "", "VARCHAR"},
		{"REPLACE('ümlaut ü', 'ü', 'ue')", "uemlaut ue", "VARCHAR"},

		// TRIM removes spaces only by default, or any of the given characters.
		{"TRIM('  hi  ')", "hi", "VARCHAR"},
		{"LTRIM('  hi  ')", "hi  ", "VARCHAR"},
		{"RTRIM('  hi  ')", "  hi", "VARCHAR"},
		{"TRIM(BOTH 'xy' FROM 'xyhixyx')", "hi", "VARCHAR"},
		{"TRIM(LEADING 'x' FROM 'xxhixx')", "hixx", "VARCHAR"},
		{"TRIM(TRAILING 'x' FROM 'xxhixx')", "xxhi", "VARCHAR"},
		{"TRIM(TRAILING FROM '  hi  ')", "  hi", "VARCHAR"},
		{"TRIM(BOTH FROM '  hi  ')", "hi", "VARCHAR"},
		{"TRIM(FROM '  hi ')", "hi", "VARCHAR"},
		{"TRIM('x' FROM 'xxhixx')", "hi", "VARCHAR"},
		{"TRIM('xxhixx', 'x')", "hi", "VARCHAR"},
		{"LTRIM('zzyhi', 'yz')", "hi", "VARCHAR"},
		{"RTRIM('hi!?!', '?!')", "hi", "VARCHAR"},
		{"BTRIM('ééaéé', 'é')", "a", "VARCHAR"},
		{"TRIM('abc', '')", "abc", "VARCHAR"},
		{"TRIM('   ')", "", "VARCHAR"},
		{"TRIM(BOTH NULL FROM 'x')", "NULL", "VARCHAR"},

		{"POSITION('lo' IN 'hello')", "4", "BIGINT"},
		{"POSITION('x' IN 'hello')", "0", "BIGINT"},
		{"POSITION('' IN 'abc')", "1", "BIGINT"},
		{"POSITION('ö' IN 'wörld')", "2", "BIGINT"},
		{"POSITION('r' IN 'wö' || 'rld')", "3", "BIGINT"},
		{"POSITION('b', 'abc')", "2", "BIGINT"},
		{"STRPOS('héllo', 'llo')", "3", "BIGINT"},
		{"STRPOS('abc', 'd')", "0", "BIGINT"},

		{"SPLIT_PART('a,b,c', ',', 2)", "b", "VARCHAR"},
		{"SPLIT_PART('a,b,c', ',', 4)", "", "VARCHAR"},
		{"SPLIT_PART('a,b,c', ',', -1)", "c", "VARCHAR"},
		{"SPLIT_PART('a,b,c', ',', -3)", "a", "VARCHAR"},
		{"SPLIT_PART('a,b,c', ',', -4)", "", "VARCHAR"},
		{"SPLIT_PART('a~~b~~c', '~~', 3)", "c", "VARCHAR"},
		{"SPLIT_PART('abc', '', 1)", "abc", "VARCHAR"},
		{"SPLIT_PART('abc', '', 2)", "", "VARCHAR"},
		{"SPLIT_PART('', ',', 1)", "", "VARCHAR"},
		{"SPLIT_PART('a,,c', ',', 2)", "", "VARCHAR"},
		{"SPLIT_PART('2024-01-15', '-', 2)", "01", "VARCHAR"},

		{"LPAD('hi', 5)", "   hi", "VARCHAR"},
		{"LPAD('hi', 5, 'xy')", "xyxhi", "VARCHAR"},
		{"RPAD('hi', 5, 'xy')", "hixyx", "VARCHAR"},
		{"LPAD('hello', 2)", "he", "VARCHAR"},
		{"RPAD('hello', 2)", "he", "VARCHAR"},
		{"LPAD('hi', 5, '')", "hi", "VARCHAR"},
		{"LPAD('hi', -1)", "", "VARCHAR"},
		{"LPAD('é', 3, 'ü')", "üüé", "VARCHAR"},
		{"LPAD(CAST(7 AS INTEGER), 3, '0')", "007", "VARCHAR"},

		{"REVERSE('abc')", "cba", "VARCHAR"},
		{"REVERSE('héllo')", "olléh", "VARCHAR"},
		{"REVERSE('')", "", "VARCHAR"},
		{"REPEAT('ab', 3)", "ababab", "VARCHAR"},
		{"REPEAT('ab', 0)", "", "VARCHAR"},
		{"REPEAT('ab', -1)", "", "VARCHAR"},
		{"REPEAT('', 1000000000000)", "", "VARCHAR"},

		{"INITCAP('hello wORLD')", "Hello World", "VARCHAR"},
		{"INITCAP('o''neil mc-donald 3rd')", "O'Neil Mc-Donald 3rd", "VARCHAR"},
		{"INITCAP('élan VITAL')", "Élan Vital", "VARCHAR"},
		{"INITCAP('')", "", "VARCHAR"},

		{"MD5('abc')", "900150983cd24fb0d6963f7d28e17f72", "VARCHAR"},
		{"MD5('')", "d41d8cd98f00b204e9800998ecf8427e", "VARCHAR"},
		{"MD5('é')", "66ddcd97cfdeabb2f6fb8a999b4bc76f", "VARCHAR"}, // of the UTF-8 bytes
		{"MD5(X'616263')", "900150983cd24fb0d6963f7d28e17f72", "VARCHAR"},
		{"MD5(123)", "202cb962ac59075b964b07152d234b70", "VARCHAR"},

		// REGEXP_REPLACE replaces the first match unless the flags have 'g'.
		{"REGEXP_REPLACE('foobarbaz', 'b..', 'X')", "fooXbaz", "VARCHAR"},
		{"REGEXP_REPLACE('foobarbaz', 'b..', 'X', 'g')", "fooXX", "VARCHAR"},
		{"REGEXP_REPLACE('aaa', 'a', 'b')", "baa", "VARCHAR"},
		{"REGEXP_REPLACE('abc', 'B', 'x', 'i')", "axc", "VARCHAR"},
		{"REGEXP_REPLACE('abc', 'B', 'x')", "abc", "VARCHAR"},
		{"REGEXP_REPLACE('a.b.c', '.', '-', 'g')", "-----", "VARCHAR"},
		{"REGEXP_REPLACE('a.b.c', '.', '-', 'gq')", "a-b-c", "VARCHAR"},
		{`REGEXP_REPLACE('2024-01-15', '(\d+)-(\d+)-(\d+)', '\3/\2/\1')`, "15/01/2024", "VARCHAR"},
		{`REGEXP_REPLACE('abc', 'b', '[\&]')`, "a[b]c", "VARCHAR"},
		{`REGEXP_REPLACE('abc', 'b', '\\')`, `a\c`, "VARCHAR"},
		{`REGEXP_REPLACE('abc', '(b)', '\2\1\n')`, `ab\nc`, "VARCHAR"},
		{"REGEXP_REPLACE('abc', 'x*', '-', 'g')", "-a-b-c-", "VARCHAR"},
		{"REGEXP_REPLACE('héllo', '[éè]', 'e')", "hello", "VARCHAR"},
		{"REGEXP_REPLACE('a  b   c', ' +', ' ', 'g')", "a b c", "VARCHAR"},
		// '.' matches a newline unless the 'n' flag makes matching
		// newline-sensitive (then ^ and $ also match at line breaks).
		{"REGEXP_REPLACE('x\ny', 'x.y', 'z')", "z", "VARCHAR"},
		{"REGEXP_REPLACE('x\ny', 'x.y', 'z', 'n')", "x\ny", "VARCHAR"},
		{"REGEXP_REPLACE('ab\ncb', 'b$', 'X', 'g')", "ab\ncX", "VARCHAR"},
		{"REGEXP_REPLACE('ab\ncb', 'b$', 'X', 'gn')", "aX\ncX", "VARCHAR"},

		{"STARTS_WITH('hello', 'he')", "true", "BOOLEAN"},
		{"STARTS_WITH('hello', 'lo')", "false", "BOOLEAN"},
		{"STARTS_WITH('hello', '')", "true", "BOOLEAN"},
		{"STARTS_WITH('héllo', 'hé')", "true", "BOOLEAN"},
		{"STARTS_WITH(NULL, 'a')", "NULL", "BOOLEAN"},
	})
}

func TestScalarConditional(t *testing.T) {
	runScalarCases(t, []scalarCase{
		{"NULLIF(1, 1)", "NULL", "BIGINT"},
		{"NULLIF(1, 2)", "1", "BIGINT"},
		{"NULLIF('a', 'a')", "NULL", "VARCHAR"},
		{"NULLIF('', '')", "NULL", "VARCHAR"},
		{"NULLIF(NULL, 1)", "NULL", "BIGINT"},
		{"NULLIF(1, NULL)", "1", "BIGINT"},
		{"NULLIF(1.0, 1)", "NULL", ""},

		// GREATEST / LEAST ignore NULLs and widen to a common type.
		{"GREATEST(1, 5, 3)", "5", "BIGINT"},
		{"LEAST(1, 5, 3)", "1", "BIGINT"},
		{"GREATEST(1, NULL, 3)", "3", "BIGINT"},
		{"LEAST(NULL, 2, NULL)", "2", "BIGINT"},
		{"GREATEST(NULL, NULL)", "NULL", "NULL"},
		{"GREATEST(1, 2.5)", "2.5", "NUMERIC(38,1)"},
		{"LEAST(1, 2.5)", "1.0", "NUMERIC(38,1)"},
		{"LEAST(1.5e0, 2)", "1.5", "DOUBLE PRECISION"},
		{"GREATEST(CAST(1 AS INTEGER), 2)", "2", "INTEGER"},
		{"GREATEST('apple', 'banana', 'cherry')", "cherry", "VARCHAR"},
		{"LEAST('b', 'B', 'a')", "B", "VARCHAR"},
		{"GREATEST(DATE '2024-01-01', '2024-06-01')", "2024-06-01", "DATE"},
		{"GREATEST(DATE '2024-01-01', TIMESTAMP '2023-06-01 12:00:00')", "2024-01-01 00:00:00", "TIMESTAMP(6)"},
		{"GREATEST(5)", "5", "BIGINT"},

		// IIF is CASE WHEN: only the chosen branch is evaluated.
		{"IIF(1 < 2, 'yes', 'no')", "yes", "VARCHAR"},
		{"IIF(1 > 2, 'yes', 'no')", "no", "VARCHAR"},
		{"IIF(NULL, 'yes', 'no')", "no", "VARCHAR"},
		{"IIF(1 > 2, 1, 2.5)", "2.5", "NUMERIC(38,1)"},
		{"IIF(TRUE, 1, 1 / 0)", "1", "BIGINT"},
		{"IIF(TRUE, NULL, 1)", "NULL", "BIGINT"},

		// COALESCE widens too; literals take the other arguments' type.
		{"COALESCE(NULL, 1, 2.5)", "1.0", "NUMERIC(38,1)"},
		{"COALESCE(CAST(NULL AS INTEGER), 0)", "0", "INTEGER"},
		{"COALESCE(CAST(NULL AS INTEGER), 2.5)", "2.5", "NUMERIC(38,1)"},
		{"COALESCE(CAST(NULL AS DATE), '2024-01-01')", "2024-01-01", "DATE"},
		{"COALESCE(NULL, NULL)", "NULL", "NULL"},
	})
}

func TestScalarErrors(t *testing.T) {
	for _, c := range []struct{ expr, err string }{
		{"SQRT(-1)", "SQRT: cannot take square root of a negative number"},
		{"LN(0)", "LN: cannot take logarithm of zero"},
		{"LN(-1)", "LN: cannot take logarithm of a negative number"},
		{"LOG(0)", "cannot take logarithm of zero"},
		{"LOG10(-0.5)", "cannot take logarithm of a negative number"},
		{"LOG(-2, 8)", "cannot take logarithm of a negative number"},
		{"LOG(1, 10)", "LOG: division by zero"},
		{"MOD(5, 0)", "MOD: division by zero"},
		{"MOD(5.5, 0)", "MOD: division by zero"},
		{"MOD(5e0, 0)", "MOD: division by zero"},
		{"POWER(0, -1)", "zero raised to a negative power is undefined"},
		{"POWER(-8, 0.5)", "a negative number raised to a non-integer power yields a complex result"},
		{"POWER(10, 400)", "POWER: value out of range: overflow"},
		{"EXP(1000)", "EXP: value out of range: overflow"},
		{"EXP(-1000)", "EXP: value out of range: underflow"},
		{"ROUND(9223372036854775807, -1)", "ROUND: integer out of range"},
		{"ROUND(CAST(32760 AS SMALLINT), -2)", "out of range for SMALLINT"},
		{"ROUND('abc')", `cannot convert "abc"`},
		{"ROUND(TRUE)", "ROUND expects a number, got BOOLEAN"},
		{"ROUND(DATE '2024-01-01')", "ROUND expects a number, got DATE"},
		{"ROUND(1.5, 'x')", "ROUND: the number of decimal places must be an integer"},
		{"SUBSTRING('abc', 1, -1)", "SUBSTRING: negative substring length not allowed"},
		{"SUBSTRING('abc', TRUE)", "SUBSTRING: the start position must be an integer"},
		{"SPLIT_PART('a,b', ',', 0)", "SPLIT_PART: field position must not be zero"},
		{"LPAD('x', 100000000)", "LPAD: requested length too large"},
		{"REPEAT('ab', 100000000)", "REPEAT: requested length too large"},
		{"REGEXP_REPLACE('a', '(', 'x')", "REGEXP_REPLACE: invalid regular expression"},
		{"REGEXP_REPLACE('a', 'a', 'x', 'z')", `invalid regular expression option "z"`},
		{"REGEXP_REPLACE('a', 'a', 'x', 'x')", `option "x" is not supported`},
		{"NULLIF(1, DATE '2024-01-01')", "NULLIF cannot compare"},
		{"GREATEST(DATE '2024-01-01', CAST(1 AS INTEGER))", "GREATEST types DATE and INTEGER cannot be matched"},
		{"LEAST('a', 1)", "LEAST types VARCHAR and BIGINT cannot be matched"},
		{"ROUND(1, 2, 3)", "ROUND expects 1 or 2 arguments"},
		{"RANDOM(1)", "RANDOM expects no arguments"},
		{"GREATEST()", "GREATEST expects at least 1 argument"},
		{"IIF(TRUE, 1)", "IIF expects 3 arguments"},
		{"LEFT('abc')", "LEFT expects 2 arguments"},
		{"MD5(DISTINCT 'a')", "MD5 does not accept * or DISTINCT"},
	} {
		if _, _, err := evalTestExpr(t, c.expr); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: error %v, want one containing %q", c.expr, err, c.err)
		}
	}
	// Syntax errors in the special forms.
	for _, sql := range []string{
		"SELECT SUBSTRING('abc' FROM)",
		"SELECT SUBSTRING('abc' FROM 1 FOR 2 FOR 3)",
		"SELECT POSITION('a' IN)",
		"SELECT TRIM(BOTH 'x' FROM)",
		"SELECT TRIM(LEADING 'x' 'y')",
	} {
		if _, err := ParseScript(sql); err == nil {
			t.Errorf("%s: expected a syntax error", sql)
		}
	}
}

// Columns named like the TRIM keywords still parse as columns.
func TestScalarTrimKeywordColumns(t *testing.T) {
	e := parseTestExpr(t, "TRIM(leading)")
	f, ok := e.(*Func)
	if !ok || f.Name != "BTRIM" || len(f.Args) != 1 {
		t.Fatalf("TRIM(leading) parsed as %#v", e)
	}
	if c, ok := f.Args[0].(*ColumnRef); !ok || c.Name != "leading" {
		t.Errorf("TRIM(leading) argument = %#v, want the column", f.Args[0])
	}
	e = parseTestExpr(t, "TRIM(both || 'x', 'y')")
	if f, ok := e.(*Func); !ok || f.Name != "BTRIM" || len(f.Args) != 2 {
		t.Errorf("TRIM(both || 'x', 'y') parsed as %#v", e)
	}
}

// Functions over columns stay residual predicates evaluated by the driver;
// functions of constants are computed once and still push down. RANDOM() is
// never a constant.
func TestScalarPushdownPlan(t *testing.T) {
	meta := &tableMeta{Schema: "public", Name: "t", Columns: []columnMeta{
		{Name: "n", Type: typeInt64, Indexed: true},
		{Name: "s", Type: typeString, Indexed: true, TagsChecked: true},
		{Name: "amt", Type: decimalType(10, 2), Indexed: true},
	}}
	for _, c := range []struct {
		where, query string
		residual     bool
	}{
		{"n > ROUND(2.5)", "@n:[(3 +inf]", false},
		{"n >= GREATEST(1, 4, NULL)", "@n:[4 +inf]", false},
		{"n = MOD(17, 5)", "@n:[2 2]", false},
		{"s = LOWER(SUBSTRING('xABC' FROM 2))", "@s:{abc}", false},
		{"s = TRIM(BOTH '*' FROM '**abc**')", "@s:{abc}", false},
		// LIKE is always re-checked, and a prefix is pushed only once the
		// server has profiled it (see tags.go).
		{"s LIKE LEFT('abcdef', 2) || '%'", "*", true},
		{"amt <= NULLIF(9.5, 0)", "@amt:[-inf 9.50]", false},
		{"ROUND(amt) = 3", "*", true},
		{"UPPER(s) = 'A'", "*", true},
		{"SUBSTRING(s, 1, 2) = 'ab'", "*", true},
		{"GREATEST(n, 5) = 5", "*", true},
		{"POSITION('a' IN s) > 0", "*", true},
		{"n < RANDOM() * 10", "*", true},
		{"n = 1 AND LENGTH(s) > 2", "@n:[1 1]", true},
	} {
		stmts, err := ParseScript("SELECT n FROM t WHERE " + c.where)
		if err != nil {
			t.Fatalf("%s: %v", c.where, err)
		}
		sel := stmts[0].Stmt.(*SelectStmt)
		wp, err := (&executor{}).planWhere(context.Background(), sel.Where, meta, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.where, err)
		}
		if wp.query != c.query || (wp.residual != nil) != c.residual {
			t.Errorf("WHERE %s: index query %q, residual %v; want %q, residual %v", c.where, wp.query, wp.residual != nil, c.query, c.residual)
		}
	}
	if isConstant(parseTestExpr(t, "RANDOM()")) || !isConstant(parseTestExpr(t, "ROUND(SQRT(2), 3)")) {
		t.Error("RANDOM() must not be a constant; other functions of constants must be")
	}
}

func TestScalarConcat(t *testing.T) {
	runScalarCases(t, []scalarCase{
		// CONCAT skips NULL arguments, as in Postgres; || propagates them.
		{"CONCAT('a', 'b', 'c')", "abc", "VARCHAR"},
		{"CONCAT('a', NULL, 'b')", "ab", "VARCHAR"},
		{"CONCAT(NULL, NULL)", "", "VARCHAR"},
		{"CONCAT(NULL)", "", "VARCHAR"},
		{"CONCAT('n=', 42, ', d=', DATE '2024-01-02')", "n=42, d=2024-01-02", "VARCHAR"},
		{"CONCAT('é', NULL, 'ü')", "éü", "VARCHAR"},
		{"'a' || NULL", "NULL", "VARCHAR"},
		{"'a' || 'b'", "ab", "VARCHAR"},

		{"CONCAT_WS('-', 'a', 'b', 'c')", "a-b-c", "VARCHAR"},
		{"CONCAT_WS('-', 'a', NULL, 'c')", "a-c", "VARCHAR"},
		{"CONCAT_WS('-', NULL, 'b')", "b", "VARCHAR"},
		{"CONCAT_WS('-', NULL, NULL)", "", "VARCHAR"},
		{"CONCAT_WS(NULL, 'a', 'b')", "NULL", "VARCHAR"},
		{"CONCAT_WS(', ', 1, 2.5, TRUE)", "1, 2.5, true", "VARCHAR"},
		{"CONCAT_WS('')", "", "VARCHAR"},
	})
}
