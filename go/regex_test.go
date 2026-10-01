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

// Unit tests for regular-expression matching. They need no Redis server.
// The expected results are Postgres 16's (the examples of its manual
// included), except where a comment says otherwise.

import (
	"context"
	"strings"
	"testing"
)

// renderExpr shows the structure of a parsed expression: operators and
// functions as name(args), with string literals quoted.
func renderExpr(e Expr) string {
	switch x := e.(type) {
	case *Literal:
		if x.V.T.Kind == KindString {
			return "'" + x.V.Text() + "'"
		}
		return x.V.Text()
	case *ColumnRef:
		return x.Name
	case *Unary:
		return x.Op + "(" + renderExpr(x.X) + ")"
	case *Binary:
		return x.Op + "(" + renderExpr(x.L) + ", " + renderExpr(x.R) + ")"
	case *IsNull:
		return "ISNULL(" + renderExpr(x.X) + ")"
	case *Func:
		args := make([]string, len(x.Args))
		for i, a := range x.Args {
			args[i] = renderExpr(a)
		}
		return x.Name + "(" + strings.Join(args, ", ") + ")"
	}
	return "?"
}

// The match operators share Postgres's "any other operator" precedence with
// ||: tighter than comparisons, IS, LIKE, BETWEEN and IN, looser than
// arithmetic, and to the left.
func TestRegexOperatorPrecedence(t *testing.T) {
	for _, c := range []struct{ expr, want string }{
		{"a ~ b", "~(a, b)"},
		{"a ~* b", "~*(a, b)"},
		{"a !~ b", "NOT(~(a, b))"},
		{"a !~* b", "NOT(~*(a, b))"},
		{"a~*b", "~*(a, b)"},
		{"a!~b", "NOT(~(a, b))"},
		{"a ~ b = c", "=(~(a, b), c)"},
		{"c = a ~ b", "=(c, ~(a, b))"},
		{"a ~ b <> c ~ d", "<>(~(a, b), ~(c, d))"},
		{"a ~ b ~ c", "~(~(a, b), c)"},
		{"a + b ~ c * d", "~(+(a, b), *(c, d))"},
		{"a ~ b || c", "||(~(a, b), c)"},
		{"a || b ~ c", "~(||(a, b), c)"},
		{"a || b + c", "||(a, +(b, c))"},
		{"a - b || c * d", "||(-(a, b), *(c, d))"},
		{"POSITION(a || b IN c || d)", "POSITION(||(a, b), ||(c, d))"},
		{"a ~ b IS NULL", "ISNULL(~(a, b))"},
		{"NOT a ~ b", "NOT(~(a, b))"},
		{"a ~ b AND c !~ d", "AND(~(a, b), NOT(~(c, d)))"},
		{"a ~ b LIKE c", "LIKE(~(a, b), c)"},
		{"a LIKE b ~ c", "LIKE(a, ~(b, c))"},
		{"a ~ b IN (c)", "=(~(a, b), c)"},
		{"x BETWEEN a ~ b AND c", "AND(>=(x, ~(a, b)), <=(x, c))"},
		{"a SIMILAR TO b", "SIMILAR TO(a, b)"},
		{"a NOT SIMILAR TO b ESCAPE c", "NOT(SIMILAR TO(a, b, c))"},
		{"a SIMILAR TO b ~ c", "SIMILAR TO(a, ~(b, c))"},
		{"a SIMILAR TO b || '%' = c", "=(SIMILAR TO(a, ||(b, '%')), c)"},
		{"SUBSTRING(s FROM 'p')", "SUBSTRING(s, 'p')"},
		{"SUBSTRING(s FROM 'p' FOR '#')", "SUBSTRING(s, 'p', '#')"},
		{"SUBSTRING(s SIMILAR 'p' ESCAPE '#')", "SUBSTRING(s, 'p', '#')"},
		{"SUBSTRING(s SIMILAR 'p' || q ESCAPE '#')", "SUBSTRING(s, ||('p', q), '#')"},
		{"SUBSTRING(s FROM 2 FOR 3)", "SUBSTRING(s, 2, 3)"},
	} {
		if got := renderExpr(parseTestExpr(t, c.expr)); got != c.want {
			t.Errorf("%s parsed as %s, want %s", c.expr, got, c.want)
		}
	}
	for _, sql := range []string{
		"SELECT 'a' ~",
		"SELECT ~ 'a'",
		"SELECT 'a' !~~ 'a'",
		"SELECT 'a' SIMILAR 'a'",
		"SELECT 'a' SIMILAR TO",
		"SELECT SUBSTRING('abc' SIMILAR 'a')",
		"SELECT SUBSTRING('abc' SIMILAR 'a' ESCAPE)",
	} {
		if _, err := ParseScript(sql); err == nil {
			t.Errorf("%s: expected a syntax error", sql)
		}
	}
}

func TestRegexOperators(t *testing.T) {
	runScalarCases(t, []scalarCase{
		{"'abc' ~ 'b'", "true", "BOOLEAN"},
		{"'abc' ~ '^b'", "false", "BOOLEAN"},
		{"'abc' ~ 'B'", "false", "BOOLEAN"},
		{"'abc' ~* 'B'", "true", "BOOLEAN"},
		{"'abc' !~ 'b'", "false", "BOOLEAN"},
		{"'abc' !~ 'x'", "true", "BOOLEAN"},
		{"'abc' !~* 'B'", "false", "BOOLEAN"},
		{"'ÉTÉ' ~* 'été'", "true", "BOOLEAN"},
		{"'abc' ~ ''", "true", "BOOLEAN"},
		{"'' ~ '^$'", "true", "BOOLEAN"},
		{"'a.c' ~ 'a\\.c'", "true", "BOOLEAN"},
		{"'abc' ~ 'a\\.c'", "false", "BOOLEAN"},
		{"'2024-01-15' ~ '^\\d{4}-\\d{2}-\\d{2}$'", "true", "BOOLEAN"},
		{"'user@example.com' ~ '^[^@]+@[^@]+\\.[a-z]{2,}$'", "true", "BOOLEAN"},
		// '.' matches a newline, and ^ and $ anchor the whole string.
		{"'x\ny' ~ 'x.y'", "true", "BOOLEAN"},
		{"'a\nb' ~ '^b'", "false", "BOOLEAN"},
		{"'a\nb' ~ 'a$'", "false", "BOOLEAN"},
		{"NULL ~ 'a'", "NULL", "BOOLEAN"},
		{"'a' ~ NULL", "NULL", "BOOLEAN"},
		{"NULL !~ 'a'", "NULL", "BOOLEAN"},
		// Precedence: these would be type errors if ~ bound like =.
		{"TRUE = 'ab' ~ 'a'", "true", "BOOLEAN"},
		{"'ab' ~ 'a' = 'cd' ~ 'x'", "false", "BOOLEAN"},
		{"'a' || 'b' ~ 'ab'", "true", "BOOLEAN"},
		{"'ab' ~ 'a' || 'b'", "trueb", "VARCHAR"},
		{"'n=' || 1 + 1", "n=2", "VARCHAR"},
		{"'abc' ~ NULL IS NULL", "true", "BOOLEAN"},
		{"NOT 'abc' ~ 'x'", "true", "BOOLEAN"},
		{"CASE WHEN 'abc' ~ 'c$' THEN 'yes' ELSE 'no' END", "yes", "VARCHAR"},
	})
}

func TestRegexFunctions(t *testing.T) {
	runScalarCases(t, []scalarCase{
		{"REGEXP_LIKE('Hello', 'h')", "false", "BOOLEAN"},
		{"REGEXP_LIKE('Hello', 'h', 'i')", "true", "BOOLEAN"},
		{"REGEXP_LIKE('Hello', 'h', 'ic')", "false", "BOOLEAN"},
		{"REGEXP_LIKE('a\nb', '^b')", "false", "BOOLEAN"},
		{"REGEXP_LIKE('a\nb', '^b', 'n')", "true", "BOOLEAN"},
		{"REGEXP_LIKE('a\nb', 'a.b', 'n')", "false", "BOOLEAN"},
		{"REGEXP_LIKE('a\nb', 'a.b', 'p')", "false", "BOOLEAN"},
		{"REGEXP_LIKE('a\nb', '^b', 'p')", "false", "BOOLEAN"},
		{"REGEXP_LIKE('a\nb', '^b', 'w')", "true", "BOOLEAN"},
		{"REGEXP_LIKE('a.c', '.', 'q')", "true", "BOOLEAN"},
		{"REGEXP_LIKE('abc', '.', 'q')", "false", "BOOLEAN"},
		{"REGEXP_LIKE('abc', NULL)", "NULL", "BOOLEAN"},
		{"REGEXP_LIKE('abc', 'b', NULL)", "NULL", "BOOLEAN"},

		// REGEXP_COUNT counts non-overlapping matches from the start
		// position; empty matches count, also right after a match.
		{"REGEXP_COUNT('ABCABCAXYaxy', 'A.')", "3", "BIGINT"},
		{"REGEXP_COUNT('ABCABCAXYaxy', 'A.', 1, 'i')", "4", "BIGINT"},
		{"REGEXP_COUNT('abcabc', 'b', 2)", "2", "BIGINT"},
		{"REGEXP_COUNT('abcabc', 'b', 3)", "1", "BIGINT"},
		{"REGEXP_COUNT('aaaa', 'aa')", "2", "BIGINT"},
		{"REGEXP_COUNT('aaa', 'aa', 2)", "1", "BIGINT"},
		{"REGEXP_COUNT('abc', '')", "4", "BIGINT"},
		{"REGEXP_COUNT('', '')", "1", "BIGINT"},
		{"REGEXP_COUNT('ab', 'b*')", "3", "BIGINT"},
		{"REGEXP_COUNT('abc', 'x')", "0", "BIGINT"},
		{"REGEXP_COUNT('abc', 'c', 4)", "0", "BIGINT"},
		{"REGEXP_COUNT('abc', 'c', 100)", "0", "BIGINT"},
		{"REGEXP_COUNT('abc', '$', 4)", "1", "BIGINT"},
		{"REGEXP_COUNT('héllo wörld', '[éö]')", "2", "BIGINT"},
		{"REGEXP_COUNT('héllo wörld', 'l', 4)", "2", "BIGINT"},
		// The text before the start position is context: ^ and \A do not
		// match there, and \b (RE2's word boundary; Postgres would use \y)
		// looks at the previous character.
		{"REGEXP_COUNT('aXa', '^a', 3)", "0", "BIGINT"},
		{"REGEXP_COUNT('aXa', 'a', 3)", "1", "BIGINT"},
		{"REGEXP_COUNT('aXa', '\\Aa', 3)", "0", "BIGINT"},
		{"REGEXP_COUNT('a\nb', '^b', 3, 'n')", "1", "BIGINT"},
		{"REGEXP_COUNT('ab cd', '\\bc', 4)", "1", "BIGINT"},
		{"REGEXP_COUNT('abcd', '\\bc', 3)", "0", "BIGINT"},
		{"REGEXP_COUNT(NULL, 'a')", "NULL", "BIGINT"},
		{"REGEXP_COUNT('a', 'a', NULL)", "NULL", "BIGINT"},

		{"REGEXP_INSTR('number of your street, town zip, FR', '[^,]+', 1, 2)", "23", "BIGINT"},
		{"REGEXP_INSTR('ABCDEFGHI', '(c..)(...)', 1, 1, 0, 'i', 2)", "6", "BIGINT"},
		{"REGEXP_INSTR('abcabc', 'b')", "2", "BIGINT"},
		{"REGEXP_INSTR('abcabc', 'b', 3)", "5", "BIGINT"},
		{"REGEXP_INSTR('abcabc', 'b', 1, 2)", "5", "BIGINT"},
		{"REGEXP_INSTR('abcabc', 'b', 1, 3)", "0", "BIGINT"},
		{"REGEXP_INSTR('abcabc', 'bc', 1, 1, 1)", "4", "BIGINT"},
		{"REGEXP_INSTR('abcabc', 'bc', 1, 2, 1)", "7", "BIGINT"},
		{"REGEXP_INSTR('héllo wörld', 'ö')", "8", "BIGINT"},
		{"REGEXP_INSTR('abc', 'x')", "0", "BIGINT"},
		{"REGEXP_INSTR('abc', '(b)(c)', 1, 1, 1, '', 1)", "3", "BIGINT"},
		// A pattern without groups: subexpr 1 is the whole match, as in
		// Postgres; another group, or one that took no part, gives 0.
		{"REGEXP_INSTR('abc', 'b', 1, 1, 0, '', 1)", "2", "BIGINT"},
		{"REGEXP_INSTR('abc', 'b', 1, 1, 0, '', 2)", "0", "BIGINT"},
		{"REGEXP_INSTR('abc', '(x)?b', 1, 1, 0, '', 1)", "0", "BIGINT"},
		{"REGEXP_INSTR('abc', '', 4)", "4", "BIGINT"},
		{"REGEXP_INSTR('abc', '', 5)", "0", "BIGINT"},
		{"REGEXP_INSTR('abc', 'b', 1, 1, 0, NULL)", "NULL", "BIGINT"},

		{"REGEXP_SUBSTR('number of your street, town zip, FR', '[^,]+', 1, 2)", " town zip", "VARCHAR"},
		{"REGEXP_SUBSTR('ABCDEFGHI', '(c..)(...)', 1, 1, 'i', 2)", "FGH", "VARCHAR"},
		{"REGEXP_SUBSTR('abc123', '[0-9]+')", "123", "VARCHAR"},
		{"REGEXP_SUBSTR('abc123def456', '[0-9]+', 7)", "456", "VARCHAR"},
		{"REGEXP_SUBSTR('abc123def456', '[0-9]+', 5)", "23", "VARCHAR"},
		{"REGEXP_SUBSTR('abc123def456', '[0-9]+', 1, 2)", "456", "VARCHAR"},
		{"REGEXP_SUBSTR('abc123def456', '[0-9]+', 1, 3)", "NULL", "VARCHAR"},
		{"REGEXP_SUBSTR('abc', 'x')", "NULL", "VARCHAR"},
		{"REGEXP_SUBSTR('abc', 'c', 4)", "NULL", "VARCHAR"},
		{"REGEXP_SUBSTR('abc', '$', 4)", "", "VARCHAR"},
		{"REGEXP_SUBSTR('user@example.com', '@(.+)$', 1, 1, '', 1)", "example.com", "VARCHAR"},
		{"REGEXP_SUBSTR('abc', 'b', 1, 1, '', 1)", "b", "VARCHAR"},
		{"REGEXP_SUBSTR('abc', 'b', 1, 1, '', 2)", "NULL", "VARCHAR"},
		{"REGEXP_SUBSTR('foo', 'foo(bar)?', 1, 1, '', 1)", "NULL", "VARCHAR"},
		{"REGEXP_SUBSTR('abc', '(a)(b)?(x)?', 1, 1, '', 3)", "NULL", "VARCHAR"},
		{"REGEXP_SUBSTR('Hello World', 'o w', 1, 1, 'i')", "o W", "VARCHAR"},
		{"REGEXP_SUBSTR('héllo wörld', 'w.r')", "wör", "VARCHAR"},

		// REGEXP_MATCH returns the text form of Postgres's text[] result.
		{"REGEXP_MATCH('foobarbequebaz', 'bar.*que')", "{barbeque}", "VARCHAR"},
		{"REGEXP_MATCH('foobarbequebaz', '(bar)(beque)')", "{bar,beque}", "VARCHAR"},
		{"REGEXP_MATCH('foo', 'foo(bar)?')", "{NULL}", "VARCHAR"},
		{"REGEXP_MATCH('xy', '(x)()(y)')", `{x,"",y}`, "VARCHAR"},
		{"REGEXP_MATCH('a b,c', '(.) (.)(,)')", `{a,b,","}`, "VARCHAR"},
		{"REGEXP_MATCH('a b', '(.*)')", `{"a b"}`, "VARCHAR"},
		{"REGEXP_MATCH('null', '(.*)')", `{"null"}`, "VARCHAR"},
		{"REGEXP_MATCH('a\vb', '.*')", "{\"a\vb\"}", "VARCHAR"},
		{`REGEXP_MATCH('a"b\c{}', '.*')`, `{"a\"b\\c{}"}`, "VARCHAR"},
		{"REGEXP_MATCH('ABC', 'b', 'i')", "{B}", "VARCHAR"},
		{"REGEXP_MATCH('héllo', 'h(.)')", "{é}", "VARCHAR"},
		{"REGEXP_MATCH('abc', 'x')", "NULL", "VARCHAR"},

		// Postgres 16's start and N arguments; a text fourth argument is
		// the flags.
		{"REGEXP_REPLACE('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 0, 'i')", "X PXstgrXSQL fXnctXXn", "VARCHAR"},
		{"REGEXP_REPLACE('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 3, 'i')", "A PostgrXSQL function", "VARCHAR"},
		{"REGEXP_REPLACE('foobarbaz', 'b(..)', 'X\\1Y', 'g')", "fooXarYXazY", "VARCHAR"},
		{"REGEXP_REPLACE('abcabc', 'b', 'X', 3)", "abcaXc", "VARCHAR"},
		{"REGEXP_REPLACE('abcabc', 'b', 'X', 1, 2)", "abcaXc", "VARCHAR"},
		{"REGEXP_REPLACE('abcabc', 'b', 'X', 1, 0)", "aXcaXc", "VARCHAR"},
		{"REGEXP_REPLACE('abcabc', 'b', 'X', 3, 0)", "abcaXc", "VARCHAR"},
		{"REGEXP_REPLACE('abcabc', 'b', 'X', 1, 3)", "abcabc", "VARCHAR"},
		{"REGEXP_REPLACE('abcabc', 'b', 'X', 2, 2)", "abcaXc", "VARCHAR"},
		{"REGEXP_REPLACE('abcabc', 'B', 'X', 1, 1, 'i')", "aXcabc", "VARCHAR"},
		{"REGEXP_REPLACE('abcabc', 'b', 'X', 1, 1, 'g')", "aXcabc", "VARCHAR"}, // N wins over g
		{"REGEXP_REPLACE('abc', 'c', 'X', 10)", "abc", "VARCHAR"},
		{"REGEXP_REPLACE('abc', '$', 'X', 4)", "abcX", "VARCHAR"},
		{"REGEXP_REPLACE('aXa', '^a', 'Y', 2, 0)", "aXa", "VARCHAR"},
		{"REGEXP_REPLACE('héllo', 'l', 'L', 4)", "hélLo", "VARCHAR"},
		{"REGEXP_REPLACE('2024-01-15', '(\\d+)-(\\d+)-(\\d+)', '\\3/\\2/\\1', 1, 1)", "15/01/2024", "VARCHAR"},
		// An empty match right after a match is replaced too, as in
		// Postgres (Go's ReplaceAll would give XaX).
		{"REGEXP_REPLACE('ab', 'b*', 'X', 'g')", "XaXX", "VARCHAR"},
		{"REGEXP_REPLACE('abc', '', 'X', 'g')", "XaXbXcX", "VARCHAR"},
		{"REGEXP_REPLACE('', '', 'X', 'g')", "X", "VARCHAR"},
		{"REGEXP_REPLACE('abc', 'x*', '-', 2, 0)", "a-b-c-", "VARCHAR"},
		{"REGEXP_REPLACE('abc', 'b', 'X', 2, NULL)", "NULL", "VARCHAR"},
	})
}

func TestRegexSubstring(t *testing.T) {
	runScalarCases(t, []scalarCase{
		// SUBSTRING(s FROM pattern): the first group, or the whole match.
		{"SUBSTRING('Thomas' FROM '...$')", "mas", "VARCHAR"},
		{"SUBSTRING('foobar' FROM 'o.b')", "oob", "VARCHAR"},
		{"SUBSTRING('foobar' FROM 'o(.)b')", "o", "VARCHAR"},
		{"SUBSTRING('foobar' FROM '(o(.))b')", "oo", "VARCHAR"},
		{"SUBSTRING('k1' FROM '^.')", "k", "VARCHAR"},
		{"SUBSTRING('foobar' FROM 'x')", "NULL", "VARCHAR"},
		{"SUBSTRING('foo' FROM 'foo(bar)?')", "NULL", "VARCHAR"},
		{"SUBSTRING('abc' FROM '')", "", "VARCHAR"},
		{"SUBSTRING('héllo' FROM 'é.')", "él", "VARCHAR"},
		{"SUBSTRING('x\ny' FROM 'x.y')", "x\ny", "VARCHAR"},
		{"SUBSTRING('hello', '2')", "NULL", "VARCHAR"}, // a text argument is a pattern
		{"SUBSTRING('hello', 'l+')", "ll", "VARCHAR"},
		{"SUBSTRING('hello' FROM NULL)", "NULL", "VARCHAR"},
		// Integer arguments are still positions.
		{"SUBSTRING('hello' FROM 2)", "ello", "VARCHAR"},
		{"SUBSTRING('hello' FROM 2 FOR 3)", "ell", "VARCHAR"},
		{"SUBSTRING('hello' FROM '2' FOR 3)", "ell", "VARCHAR"},
		{"SUBSTRING('hello', 2, '3')", "ell", "VARCHAR"},
		{"SUBSTRING(X'616263', 2)", "bc", "VARBINARY"},

		// SUBSTRING(s SIMILAR pattern ESCAPE e), also written FROM … FOR …
		// or as three text arguments.
		{"SUBSTRING('Thomas' SIMILAR '%#\"o_a#\"_' ESCAPE '#')", "oma", "VARCHAR"},
		{"SUBSTRING('Thomas' FROM '%#\"o_a#\"_' FOR '#')", "oma", "VARCHAR"},
		{"SUBSTRING('foobar' SIMILAR '%#\"o_b#\"%' ESCAPE '#')", "oob", "VARCHAR"},
		{"SUBSTRING('foobar' SIMILAR '#\"o_b#\"%' ESCAPE '#')", "NULL", "VARCHAR"},
		{"SUBSTRING('foobar', '%#\"o_b#\"%', '#')", "oob", "VARCHAR"},
		// The first part matches as little as it can, then the second as
		// much as it can.
		{"SUBSTRING('abcabc' SIMILAR '%#\"b%#\"' ESCAPE '#')", "bcabc", "VARCHAR"},
		{"SUBSTRING('abcabc' SIMILAR '%#\"b%#\"c' ESCAPE '#')", "bcab", "VARCHAR"},
		{"SUBSTRING('abcabc' SIMILAR '%#\"b_#\"%' ESCAPE '#')", "bc", "VARCHAR"},
		{"SUBSTRING('aaa' SIMILAR 'a#\"a*#\"a*' ESCAPE '#')", "aa", "VARCHAR"},
		{"SUBSTRING('xa1b22c333y' SIMILAR '%#\"[0-9]+#\"%' ESCAPE '#')", "1", "VARCHAR"},
		{"SUBSTRING('user@example.com' SIMILAR '%@#\"%#\"' ESCAPE '#')", "example.com", "VARCHAR"},
		// | in a part affects only that part.
		{"SUBSTRING('ab' SIMILAR '#\"a|x#\"|b' ESCAPE '#')", "a", "VARCHAR"},
		// One separator: the part after it; none: the whole string.
		{"SUBSTRING('abc' SIMILAR 'a#\"%' ESCAPE '#')", "bc", "VARCHAR"},
		{"SUBSTRING('abc' SIMILAR 'a%' ESCAPE '#')", "abc", "VARCHAR"},
		{"SUBSTRING('abc' SIMILAR 'x%' ESCAPE '#')", "NULL", "VARCHAR"},
		{"SUBSTRING('abc' SIMILAR '#\"#\"%' ESCAPE '#')", "", "VARCHAR"},
		{"SUBSTRING('ab' SIMILAR '(a)#\"(b)#\"' ESCAPE '#')", "b", "VARCHAR"},
		{"SUBSTRING('héllo' SIMILAR 'h#\"_#\"%' ESCAPE '#')", "é", "VARCHAR"},
		{"SUBSTRING('abc' SIMILAR 'a#\"b#\"c' ESCAPE NULL)", "NULL", "VARCHAR"},
		// Separators inside parentheses: what the group between them
		// matched in the whole expression.
		{"SUBSTRING('abc' SIMILAR '(a#\"b)#\"c' ESCAPE '#')", "b", "VARCHAR"},
		{"SUBSTRING('abc123def' SIMILAR '%(#\"[0-9]+#\")%' ESCAPE '#')", "3", "VARCHAR"},
		{"SUBSTRING('aXbXc' SIMILAR '(%#\"X#\"%)' ESCAPE '#')", "X", "VARCHAR"},
	})
}

func TestRegexSimilarTo(t *testing.T) {
	runScalarCases(t, []scalarCase{
		{"'abc' SIMILAR TO 'abc'", "true", "BOOLEAN"},
		{"'abc' SIMILAR TO 'a'", "false", "BOOLEAN"},
		{"'abc' SIMILAR TO '%(b|d)%'", "true", "BOOLEAN"},
		{"'abc' SIMILAR TO '(b|c)%'", "false", "BOOLEAN"},
		{"'abc' NOT SIMILAR TO 'a%'", "false", "BOOLEAN"},
		{"'abc' SIMILAR TO 'a_c'", "true", "BOOLEAN"},
		{"'abc' SIMILAR TO 'A_C'", "false", "BOOLEAN"},
		{"'héllo' SIMILAR TO 'h_llo'", "true", "BOOLEAN"},
		{"'a\nb' SIMILAR TO 'a%'", "true", "BOOLEAN"},
		{"'a\nb' SIMILAR TO 'a_b'", "true", "BOOLEAN"},
		// The match covers the whole string, also with alternatives.
		{"'a' SIMILAR TO 'a|b'", "true", "BOOLEAN"},
		{"'ab' SIMILAR TO 'a|b'", "false", "BOOLEAN"},
		{"'xab' SIMILAR TO 'x(a|b)*'", "true", "BOOLEAN"},
		// . ^ $ and \ are literal; the other metacharacters keep their
		// meaning.
		{"'a.c' SIMILAR TO 'a.c'", "true", "BOOLEAN"},
		{"'abc' SIMILAR TO 'a.c'", "false", "BOOLEAN"},
		{"'a^c$' SIMILAR TO 'a^c$'", "true", "BOOLEAN"},
		{"'aaa' SIMILAR TO 'a{2,3}'", "true", "BOOLEAN"},
		{"'aaaa' SIMILAR TO 'a{2,3}'", "false", "BOOLEAN"},
		{"'ab' SIMILAR TO 'a+b?'", "true", "BOOLEAN"},
		{"'b' SIMILAR TO 'a*b'", "true", "BOOLEAN"},
		// Escapes: backslash by default, ESCAPE '' for none.
		{`'a%c' SIMILAR TO 'a\%c'`, "true", "BOOLEAN"},
		{`'abc' SIMILAR TO 'a\%c'`, "false", "BOOLEAN"},
		{`'a\c' SIMILAR TO 'a\\c'`, "true", "BOOLEAN"},
		{`'a\c' SIMILAR TO 'a\c' ESCAPE ''`, "true", "BOOLEAN"},
		{`'a%c' SIMILAR TO 'a\%c' ESCAPE ''`, "false", "BOOLEAN"},
		{"'a%c' SIMILAR TO 'a#%c' ESCAPE '#'", "true", "BOOLEAN"},
		{"'a_c' SIMILAR TO 'a#_c' ESCAPE '#'", "true", "BOOLEAN"},
		{"'abc' SIMILAR TO 'a#_c' ESCAPE '#'", "false", "BOOLEAN"},
		{"'a#' SIMILAR TO 'a##' ESCAPE '#'", "true", "BOOLEAN"},
		{"'a(b)' SIMILAR TO 'a#(b#)' ESCAPE '#'", "true", "BOOLEAN"},
		{"'a|b' SIMILAR TO 'a#|b' ESCAPE '#'", "true", "BOOLEAN"},
		{"'a*' SIMILAR TO 'a#*' ESCAPE '#'", "true", "BOOLEAN"},
		{"'a%' SIMILAR TO 'aé%' ESCAPE 'é'", "true", "BOOLEAN"},
		{"'ab' SIMILAR TO 'a%' ESCAPE '%'", "false", "BOOLEAN"},
		{"'a%' SIMILAR TO 'a%%' ESCAPE '%'", "true", "BOOLEAN"},
		{`'a1' SIMILAR TO 'a\d'`, "true", "BOOLEAN"},
		{`'ab' SIMILAR TO 'a\d'`, "false", "BOOLEAN"},
		{"'abc' SIMILAR TO 'a#\"b#\"c' ESCAPE '#'", "true", "BOOLEAN"},
		// Classes: % and _ are literal inside, a ] first is literal, and
		// named classes work.
		{"'abc' SIMILAR TO '[a-c]+'", "true", "BOOLEAN"},
		{"'abd' SIMILAR TO '[a-c]+'", "false", "BOOLEAN"},
		{"'%' SIMILAR TO '[%]'", "true", "BOOLEAN"},
		{"'x' SIMILAR TO '[%]'", "false", "BOOLEAN"},
		{"'x' SIMILAR TO '[^_]'", "true", "BOOLEAN"},
		{"'_' SIMILAR TO '[^_]'", "false", "BOOLEAN"},
		{"'a]' SIMILAR TO '[]a]+'", "true", "BOOLEAN"},
		{"'b' SIMILAR TO '[^]a]'", "true", "BOOLEAN"},
		{"']' SIMILAR TO '[^]a]'", "false", "BOOLEAN"},
		{"'a]' SIMILAR TO '[a#]]+' ESCAPE '#'", "true", "BOOLEAN"},
		{`'\' SIMILAR TO '[\]' ESCAPE ''`, "true", "BOOLEAN"},
		{"'[' SIMILAR TO '[[]'", "true", "BOOLEAN"},
		{"'-' SIMILAR TO '[a-]'", "true", "BOOLEAN"},
		{"']' SIMILAR TO '[]]'", "true", "BOOLEAN"},
		{"'a1' SIMILAR TO '[[:alpha:]][[:digit:]]'", "true", "BOOLEAN"},
		{"'a_' SIMILAR TO '[[:alpha:]_]+'", "true", "BOOLEAN"},
		{"'a.' SIMILAR TO '[[:alpha:]_]+'", "false", "BOOLEAN"},
		{"'%' SIMILAR TO '[[:alpha:]%]'", "true", "BOOLEAN"},
		{"'x' SIMILAR TO '[^[:digit:]]'", "true", "BOOLEAN"},
		{"'ab' SIMILAR TO '(a)(b)'", "true", "BOOLEAN"},
		{"NULL SIMILAR TO 'a'", "NULL", "BOOLEAN"},
		{"'a' SIMILAR TO NULL", "NULL", "BOOLEAN"},
		{"'a' SIMILAR TO 'a' ESCAPE NULL", "NULL", "BOOLEAN"},
	})
}

func TestRegexErrors(t *testing.T) {
	for _, c := range []struct{ expr, err string }{
		{"'a' ~ '('", "invalid regular expression: missing closing ): `(`"},
		{"'a' ~* '[a'", "invalid regular expression: missing closing ]: `[a`"},
		{`'aa' ~ '(a)\1'`, "invalid regular expression: invalid escape sequence: `\\1`"}, // no backreferences
		{"'a' ~ '(?=a)'", "invalid regular expression: invalid or unsupported Perl syntax"},
		{"REGEXP_LIKE('a', '*')", "REGEXP_LIKE: invalid regular expression: missing argument to repetition operator: `*`"},
		{"REGEXP_LIKE('a', 'a', 'g')", `REGEXP_LIKE does not support the "global" option`},
		{"REGEXP_LIKE('a', 'a', 'z')", `REGEXP_LIKE: invalid regular expression option "z"`},
		{"REGEXP_LIKE('a', 'a', 'x')", `REGEXP_LIKE: regular expression option "x" is not supported`},
		{"REGEXP_MATCH('a', 'a', 'g')", `REGEXP_MATCH does not support the "global" option`},
		{"REGEXP_COUNT('a', 'a', 1, 'g')", `REGEXP_COUNT does not support the "global" option`},
		{"REGEXP_COUNT('a', 'a', 0)", `REGEXP_COUNT: invalid value for parameter "start": 0`},
		{"REGEXP_COUNT('a', 'a', 'i')", `REGEXP_COUNT: the parameter "start" must be an integer`},
		{"REGEXP_INSTR('a', 'a', 1, 0)", `REGEXP_INSTR: invalid value for parameter "n": 0`},
		{"REGEXP_INSTR('a', 'a', 1, 1, 2)", `REGEXP_INSTR: invalid value for parameter "endoption": 2`},
		{"REGEXP_INSTR('a', 'a', 1, 1, 0, '', -1)", `REGEXP_INSTR: invalid value for parameter "subexpr": -1`},
		{"REGEXP_SUBSTR('a', 'a', -1)", `REGEXP_SUBSTR: invalid value for parameter "start": -1`},
		{"REGEXP_SUBSTR('a', 'a', 1, 1, '', -1)", `REGEXP_SUBSTR: invalid value for parameter "subexpr": -1`},
		{"REGEXP_SUBSTR('a', '(', 5)", "REGEXP_SUBSTR: invalid regular expression"}, // even past the end
		{"REGEXP_REPLACE('a', 'a', 'b', 0)", `REGEXP_REPLACE: invalid value for parameter "start": 0`},
		{"REGEXP_REPLACE('a', 'a', 'b', 1, -1)", `REGEXP_REPLACE: invalid value for parameter "n": -1`},
		{"REGEXP_REPLACE('a', 'a', 'b', 1, 1, 'z')", `REGEXP_REPLACE: invalid regular expression option "z"`},
		{"REGEXP_REPLACE('a', '(', 'b', 9)", "REGEXP_REPLACE: invalid regular expression: missing closing )"},
		{"SUBSTRING('a' FROM '(')", "SUBSTRING: invalid regular expression: missing closing )"},
		{"'a' SIMILAR TO '(a'", "invalid regular expression: missing closing )"},
		{"'a' SIMILAR TO '*a'", "invalid regular expression: missing argument to repetition operator"},
		{"'a' SIMILAR TO 'a' ESCAPE 'ab'", "invalid escape string"},
		{"'a' SIMILAR TO 'a#\"b#\"c#\"' ESCAPE '#'", "SQL regular expression may not contain more than two escape-double-quote separators"},
		{"SUBSTRING('abc' SIMILAR 'a' ESCAPE '##')", "SUBSTRING: invalid escape string"},
		{"REGEXP_LIKE('a')", "REGEXP_LIKE expects 2 or 3 arguments"},
		{"REGEXP_INSTR('a', 'b', 1, 1, 0, '', 1, 1)", "REGEXP_INSTR expects 2 to 7 arguments"},
		{"REGEXP_REPLACE('a', 'b')", "REGEXP_REPLACE expects 3 to 6 arguments"},
		{"REGEXP_SUBSTR(DISTINCT 'a', 'b')", "REGEXP_SUBSTR does not accept * or DISTINCT"},
	} {
		if _, _, err := evalTestExpr(t, c.expr); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: error %v, want one containing %q", c.expr, err, c.err)
		}
	}
}

// The SIMILAR TO translation, compared with the regular expression
// Postgres's similar_escape builds (in its syntax, the same here).
func TestRegexSimilarTranslation(t *testing.T) {
	for _, c := range []struct{ pattern, esc, want string }{
		{"abc", `\`, `(?s)^(?:abc)$`},
		{"a%b_c", `\`, `(?s)^(?:a.*b.c)$`},
		{"(a|b)*", `\`, `(?s)^(?:(?:a|b)*)$`},
		{`a.b^c$d\e`, ``, `(?s)^(?:a\.b\^c\$d\\e)$`},
		{`a\%b\_c\.`, `\`, `(?s)^(?:a\%b\_c\.)$`},
		{`a#"b#"c`, `#`, `(?s)^(?:a){1,1}?(b){1,1}(?:c)$`},
		{`a#"b`, `#`, `(?s)^(?:a){1,1}?(b)$`},
		{`[%_]`, `\`, `(?s)^(?:[%_])$`},
		{`[]a]`, `\`, `(?s)^(?:[\]a])$`},
		{`[^]a]`, `\`, `(?s)^(?:[^\]a])$`},
		{`[[:alpha:]_]`, `\`, `(?s)^(?:[[:alpha:]_])$`},
		{`[\]`, ``, `(?s)^(?:[\\])$`},
		{`[#]]`, `#`, `(?s)^(?:[\]])$`},
		{`[#"]`, `#`, `(?s)^(?:[\"])$`},
		{`é#é`, `#`, `(?s)^(?:éé)$`},
		{`a{2,3}b+c?|d*`, `\`, `(?s)^(?:a{2,3}b+c?|d*)$`},
	} {
		parts, err := similarParts("", c.pattern, c.esc)
		if err != nil {
			t.Errorf("%s ESCAPE %q: %v", c.pattern, c.esc, err)
			continue
		}
		expr := "(?s)^(?:" + parts[0]
		switch len(parts) {
		case 2:
			expr += "){1,1}?(" + parts[1]
		case 3:
			expr += "){1,1}?(" + parts[1] + "){1,1}(?:" + parts[2]
		}
		if got := expr + ")$"; got != c.want {
			t.Errorf("%s ESCAPE %q translated to %s, want %s", c.pattern, c.esc, got, c.want)
		}
	}
}

// Patterns are evaluated by the driver: a WHERE clause with them is a
// residual predicate, next to whatever else the index answers. Patterns of
// constants are computed once and still push down.
func TestRegexPushdownPlan(t *testing.T) {
	meta := &tableMeta{Schema: "public", Name: "t", Columns: []columnMeta{
		{Name: "n", Type: typeInt64, Indexed: true},
		{Name: "s", Type: typeString, Indexed: true},
	}}
	for _, c := range []struct {
		where, query string
		residual     bool
	}{
		{"s ~ '^ab'", "*", true},
		{"s ~* 'ab'", "*", true},
		{"s !~ 'ab'", "*", true},
		{"s SIMILAR TO 'ab%'", "*", true},
		{"REGEXP_LIKE(s, '^ab')", "*", true},
		{"REGEXP_COUNT(s, 'a') > 1", "*", true},
		{"n = 1 AND s ~ 'x'", "@n:[1 1]", true},
		{"s = REGEXP_REPLACE('xaby', '[xy]', '', 'g')", "@s:{ab}", false},
		{"s = SUBSTRING('k-ab-1' FROM '-(.*)-')", "@s:{ab}", false},
		{"n = REGEXP_COUNT('banana', 'a')", "@n:[3 3]", false},
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
}
