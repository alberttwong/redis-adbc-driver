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

// Regular-expression matching: the operators ~, ~*, !~ and !~*, SIMILAR TO,
// REGEXP_LIKE, REGEXP_COUNT, REGEXP_INSTR, REGEXP_SUBSTR, REGEXP_MATCH, the
// pattern forms of SUBSTRING and the searches of REGEXP_REPLACE (whose
// replacement is in scalar.go). Arguments, flags, positions and errors
// follow Postgres, with two differences:
//
//   - Patterns use RE2 syntax (Go's regexp package): no backreferences or
//     lookaround, and RE2's escapes (\b is a word boundary, not a
//     backspace).
//   - Where a pattern can match strings of different lengths at the same
//     position, RE2 takes the first alternative by Perl's rules (x|xy
//     matches the "x" of "xy"), whereas Postgres prefers the longest or the
//     shortest match depending on the pattern's quantifiers.
//
// Positions count characters. A search from a start position sees the text
// before it, as in Postgres: ^ does not match there, and \b looks at the
// previous character. Patterns are always evaluated by the driver, on the
// rows the index returns.

import (
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"
	"unicode/utf8"
)

// regexpSearch is a compiled pattern.
type regexpSearch struct {
	re *regexp.Regexp
	// prefix holds the flags, such as (?is), and body the pattern as RE2
	// syntax; from is compiled from them on first use, to search from a
	// later position (see at).
	prefix, body string
	from         *regexp.Regexp
}

// compilePattern compiles a pattern with its flags. name starts the error
// messages (none for the operators).
func compilePattern(name, pattern string, fl regexpFlags) (*regexpSearch, error) {
	body := pattern
	if fl.literal {
		body = regexp.QuoteMeta(pattern)
	}
	var prefix string
	if fl.caseInsensitive {
		prefix += "i"
	}
	if fl.dotNL {
		prefix += "s"
	}
	if fl.multiLine {
		prefix += "m"
	}
	if prefix != "" {
		prefix = "(?" + prefix + ")"
	}
	re, err := compileRegexp(prefix + body)
	if err != nil {
		// Report the error for the pattern as written, without the flags.
		if _, perr := syntax.Parse(body, syntax.Perl); perr != nil {
			err = perr
		}
		return nil, invalidRegexp(name, err, true)
	}
	return &regexpSearch{re: re, prefix: prefix, body: body}, nil
}

// invalidRegexp is Postgres's error for a pattern that does not compile,
// with RE2's reason and, if showExpr is set, the part of the pattern it is
// about.
func invalidRegexp(name string, err error, showExpr bool) error {
	reason := err.Error()
	var se *syntax.Error
	if errors.As(err, &se) {
		reason = se.Code.String()
		if showExpr && se.Expr != "" {
			reason += ": `" + se.Expr + "`"
		}
	}
	if name == "" {
		return fmt.Errorf("invalid regular expression: %s", reason)
	}
	return fmt.Errorf("%s: invalid regular expression: %s", name, reason)
}

// at returns the leftmost match that starts at byte offset pos or later, as
// submatch offsets into s (nil if there is none). The text before pos is
// context, as in Postgres: ^ and \A do not match at pos, and \b sees the
// character before it. Go cannot start a search inside a string, so a
// search from pos > 0 runs `\A(?s:.)(?s:.*?)(pattern)` from the character
// before pos: it skips that character and then finds the leftmost match,
// as an unanchored search does.
func (rs *regexpSearch) at(s string, pos int) ([]int, error) {
	if pos == 0 {
		return rs.re.FindStringSubmatchIndex(s), nil
	}
	if pos > len(s) {
		return nil, nil
	}
	if rs.from == nil {
		re, err := compileRegexp(rs.prefix + `\A(?s:.)(?s:.*?)(` + closeQuote(rs.body) + `)`)
		if err != nil {
			return nil, invalidRegexp("", err, true)
		}
		rs.from = re
	}
	_, w := utf8.DecodeLastRuneInString(s[:pos])
	base := pos - w
	m := rs.from.FindStringSubmatchIndex(s[base:])
	if m == nil {
		return nil, nil
	}
	m = m[2:] // the pattern's own match and groups
	for i := range m {
		if m[i] >= 0 {
			m[i] += base
		}
	}
	return m, nil
}

// all returns up to limit matches (-1: no limit) at or after byte offset
// pos, found as Postgres finds them: each search starts where the previous
// match ended, or one character later after an empty match.
func (rs *regexpSearch) all(s string, pos, limit int) ([][]int, error) {
	var out [][]int
	for pos <= len(s) && (limit < 0 || len(out) < limit) {
		m, err := rs.at(s, pos)
		if err != nil {
			return nil, err
		}
		if m == nil {
			break
		}
		out = append(out, m)
		pos = m[1]
		if m[0] == m[1] {
			if pos == len(s) {
				break
			}
			_, w := utf8.DecodeRuneInString(s[pos:])
			pos += w
		}
	}
	return out, nil
}

// closeQuote ends a \Q quote left open at the end of a pattern, so that the
// pattern can be followed by more syntax.
func closeQuote(body string) string {
	for i := 0; i+1 < len(body); i++ {
		if body[i] != '\\' {
			continue
		}
		if body[i+1] != 'Q' {
			i++ // an escaped character
			continue
		}
		end := strings.Index(body[i+2:], `\E`)
		if end < 0 {
			return body + `\E`
		}
		i += 2 + end + 1
	}
	return body
}

// charOffset returns the byte offset of the character at 1-based position
// pos: len(s) just past the last character, -1 beyond that.
func charOffset(s string, pos int64) int {
	off := 0
	for ; pos > 1; pos-- {
		if off == len(s) {
			return -1
		}
		_, w := utf8.DecodeRuneInString(s[off:])
		off += w
	}
	return off
}

// regexpParam reads the optional integer parameter args[i] of a REGEXP_*
// function (def if it is absent), which must be at least min.
func regexpParam(name string, args []Value, i int, param string, def, min int64) (int64, error) {
	if i >= len(args) {
		return def, nil
	}
	n, err := intArg(name, `parameter "`+param+`"`, args[i])
	if err != nil {
		return 0, err
	}
	if n < min {
		return 0, fmt.Errorf("%s: invalid value for parameter %q: %d", name, param, n)
	}
	return n, nil
}

// subexprGroup is the group a subexpr parameter selects, 0 for the whole
// match. As in Postgres, 1 also selects the whole match of a pattern
// without groups; ok is false for a group the pattern does not have.
func subexprGroup(groups int, subexpr int64) (int, bool) {
	switch {
	case subexpr == 0 || (groups == 0 && subexpr == 1):
		return 0, true
	case subexpr <= int64(groups):
		return int(subexpr), true
	}
	return 0, false
}

// regexpFunc evaluates the match operators, SIMILAR TO and the REGEXP_*
// functions other than REGEXP_REPLACE. Arguments are non-NULL.
func regexpFunc(f *Func, args []Value) (Value, error) {
	s, pattern := args[0].Text(), args[1].Text()
	switch f.Name {
	case "~", "~*":
		rs, err := compilePattern("", pattern, regexpFlags{dotNL: true, caseInsensitive: f.Name == "~*"})
		if err != nil {
			return Value{}, err
		}
		return boolValue(rs.re.MatchString(s)), nil
	case "SIMILAR TO":
		esc := `\`
		if len(args) == 3 {
			esc = args[2].Text()
		}
		re, _, err := similarRegexp("", pattern, esc)
		if err != nil {
			return Value{}, err
		}
		return boolValue(re.MatchString(s)), nil
	}

	// Postgres's parameters: REGEXP_LIKE and REGEXP_MATCH (s, pattern
	// [, flags]), REGEXP_COUNT (s, pattern [, start [, flags]]),
	// REGEXP_SUBSTR (s, pattern [, start [, n [, flags [, subexpr]]]]) and
	// REGEXP_INSTR (s, pattern [, start [, n [, endoption [, flags
	// [, subexpr]]]]]).
	start, n, endoption, subexpr := int64(1), int64(1), int64(0), int64(0)
	flagsAt := 2
	var err error
	if f.Name == "REGEXP_COUNT" || f.Name == "REGEXP_SUBSTR" || f.Name == "REGEXP_INSTR" {
		if start, err = regexpParam(f.Name, args, 2, "start", 1, 1); err != nil {
			return Value{}, err
		}
		flagsAt = 3
	}
	if f.Name == "REGEXP_SUBSTR" || f.Name == "REGEXP_INSTR" {
		if n, err = regexpParam(f.Name, args, 3, "n", 1, 1); err != nil {
			return Value{}, err
		}
		flagsAt = 4
	}
	if f.Name == "REGEXP_INSTR" {
		if endoption, err = regexpParam(f.Name, args, 4, "endoption", 0, 0); err != nil {
			return Value{}, err
		}
		if endoption > 1 {
			return Value{}, fmt.Errorf("%s: invalid value for parameter %q: %d", f.Name, "endoption", endoption)
		}
		flagsAt = 5
	}
	if f.Name == "REGEXP_SUBSTR" || f.Name == "REGEXP_INSTR" {
		if subexpr, err = regexpParam(f.Name, args, flagsAt+1, "subexpr", 0, 0); err != nil {
			return Value{}, err
		}
	}
	flags := ""
	if flagsAt < len(args) {
		flags = args[flagsAt].Text()
	}
	fl, err := parseRegexpFlags(f.Name, flags)
	if err != nil {
		return Value{}, err
	}
	if fl.global {
		return Value{}, fmt.Errorf("%s does not support the \"global\" option", f.Name)
	}
	rs, err := compilePattern(f.Name, pattern, fl)
	if err != nil {
		return Value{}, err
	}

	switch f.Name {
	case "REGEXP_LIKE":
		return boolValue(rs.re.MatchString(s)), nil
	case "REGEXP_MATCH":
		m := rs.re.FindStringSubmatchIndex(s)
		if m == nil {
			return nullValue(typeString), nil
		}
		return stringValue(matchArray(s, m)), nil
	case "REGEXP_COUNT":
		from := charOffset(s, start)
		if from < 0 {
			return intValue(typeInt64, 0), nil
		}
		matches, err := rs.all(s, from, -1)
		if err != nil {
			return Value{}, err
		}
		return intValue(typeInt64, int64(len(matches))), nil
	}

	// REGEXP_SUBSTR / REGEXP_INSTR: the n-th match from start, or a group of
	// it.
	var loc []int
	if from := charOffset(s, start); from >= 0 {
		// There are at most len(s)+1 matches.
		matches, err := rs.all(s, from, int(min(n, int64(len(s))+2)))
		if err != nil {
			return Value{}, err
		}
		if g, ok := subexprGroup(rs.re.NumSubexp(), subexpr); ok && int64(len(matches)) >= n {
			if m := matches[n-1]; m[2*g] >= 0 {
				loc = m[2*g : 2*g+2]
			}
		}
	}
	if f.Name == "REGEXP_SUBSTR" {
		if loc == nil {
			return nullValue(typeString), nil
		}
		return stringValue(s[loc[0]:loc[1]]), nil
	}
	if loc == nil {
		return intValue(typeInt64, 0), nil
	}
	return intValue(typeInt64, int64(utf8.RuneCountInString(s[:loc[endoption]]))+1), nil
}

// regexpReplaceFunc evaluates REGEXP_REPLACE(s, pattern, replacement
// [, start [, n]] [, flags]). As in Postgres, a text fourth argument is the
// flags. n is the match to replace, 0 for all of them; without n, the 'g'
// flag replaces all of them and otherwise the first is replaced.
func regexpReplaceFunc(args []Value) (Value, error) {
	const name = "REGEXP_REPLACE"
	start, n := int64(1), int64(-1)
	flags := ""
	var err error
	switch {
	case len(args) == 4 && args[3].T.Kind == KindString:
		flags = args[3].Text()
	case len(args) >= 4:
		if start, err = regexpParam(name, args, 3, "start", 1, 1); err != nil {
			return Value{}, err
		}
		if n, err = regexpParam(name, args, 4, "n", -1, 0); err != nil {
			return Value{}, err
		}
		if len(args) == 6 {
			flags = args[5].Text()
		}
	}
	fl, err := parseRegexpFlags(name, flags)
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		n = 1
		if fl.global {
			n = 0
		}
	}
	s := args[0].Text()
	out, err := regexpReplace(s, args[1].Text(), args[2].Text(), fl, charOffset(s, start), n)
	if err != nil {
		return Value{}, err
	}
	return stringValue(out), nil
}

// regexpSubstring is SUBSTRING(s FROM pattern): what the pattern's first
// group matched in the first match, or the whole match if it has no
// groups. It is NULL if there is no match, or the group took no part in it.
func regexpSubstring(s, pattern string) (Value, error) {
	rs, err := compilePattern("SUBSTRING", pattern, regexpFlags{dotNL: true})
	if err != nil {
		return Value{}, err
	}
	m := rs.re.FindStringSubmatchIndex(s)
	g := min(rs.re.NumSubexp(), 1)
	if m == nil || m[2*g] < 0 {
		return nullValue(typeString), nil
	}
	return stringValue(s[m[2*g]:m[2*g+1]]), nil
}

// matchArray renders a match as the text form of the text[] array that
// Postgres's REGEXP_MATCH returns: the groups' text, or the whole match if
// the pattern has no groups, with NULL for a group that took no part in the
// match. Elements are quoted as Postgres's array output quotes them.
func matchArray(s string, m []int) string {
	locs := m[2:]
	if len(locs) == 0 {
		locs = m[:2]
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < len(locs); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		if locs[i] < 0 {
			b.WriteString("NULL")
			continue
		}
		e := s[locs[i]:locs[i+1]]
		if e != "" && !strings.EqualFold(e, "NULL") && !strings.ContainsAny(e, "{},\"\\ \t\n\r\v\f") {
			b.WriteString(e)
			continue
		}
		b.WriteByte('"')
		for j := 0; j < len(e); j++ {
			if e[j] == '"' || e[j] == '\\' {
				b.WriteByte('\\')
			}
			b.WriteByte(e[j])
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// ---- SIMILAR TO ----

// similarParts translates a SIMILAR TO pattern to RE2 as Postgres's
// similar_escape translates it to its own regular expressions:
//
//   - % is .* and _ is . (both also match newlines).
//   - ( is a non-capturing group, and . ^ $ and \ are literal.
//   - | * + ? {m,n} ( ) and [ … ] keep their meaning. In a class, a ] just
//     after the [ or [^ is literal, and \ is literal.
//   - The escape character (esc: one character, or none if empty) makes the
//     next character literal. Escaped letters and digits are RE2 escapes,
//     such as \d, as Postgres passes them to its own regular expressions.
//
// The result is split at the escape-double-quote separators (outside
// classes) into at most three parts.
func similarParts(name, pattern, esc string) ([]string, error) {
	fail := func(msg string) error {
		if name == "" {
			return errors.New(msg)
		}
		return fmt.Errorf("%s: %s", name, msg)
	}
	if utf8.RuneCountInString(esc) > 1 {
		return nil, fail("invalid escape string: it must be empty or one character")
	}
	escR := rune(-1)
	if esc != "" {
		escR, _ = utf8.DecodeRuneInString(esc)
	}
	var parts []string
	var b strings.Builder
	afterEsc, inClass := false, false
	classPos := 0 // in a class: 1 just after [, 2 just after [^, 3 further on
	for i := 0; i < len(pattern); {
		c, w := utf8.DecodeRuneInString(pattern[i:])
		ch := pattern[i : i+w] // as written, so RE2 still rejects invalid UTF-8
		i += w
		switch {
		case afterEsc:
			afterEsc = false
			if c == '"' && !inClass {
				if len(parts) == 2 {
					return nil, fail("SQL regular expression may not contain more than two escape-double-quote separators")
				}
				parts = append(parts, b.String())
				b.Reset()
				continue
			}
			if c < utf8.RuneSelf {
				b.WriteByte('\\')
			}
			b.WriteString(ch)
			classPos = 3
		case c == escR:
			afterEsc = true
		case inClass:
			switch {
			case c == '^' && classPos == 1:
				b.WriteString(ch)
				classPos = 2
			case c == ']' && classPos == 3:
				b.WriteString(ch)
				inClass = false
			case c == '[' && strings.HasPrefix(pattern[i:], ":") && strings.Contains(pattern[i+1:], ":]"):
				// A named class such as [:alpha:], copied whole.
				end := i + 1 + strings.Index(pattern[i+1:], ":]") + 2
				b.WriteString("[" + pattern[i:end])
				i = end
				classPos = 3
			case c == ']' || c == '[' || c == '\\':
				b.WriteByte('\\')
				b.WriteString(ch)
				classPos = 3
			default:
				b.WriteString(ch)
				classPos = 3
			}
		case c == '[':
			b.WriteString(ch)
			inClass, classPos = true, 1
		case c == '%':
			b.WriteString(".*")
		case c == '_':
			b.WriteByte('.')
		case c == '(':
			b.WriteString("(?:")
		case c == '.' || c == '^' || c == '$' || c == '\\':
			b.WriteByte('\\')
			b.WriteString(ch)
		default:
			b.WriteString(ch)
		}
	}
	return append(parts, b.String()), nil
}

// similarRegexp compiles a SIMILAR TO pattern into the expression Postgres
// builds, which must match the whole string:
//
//	(?s)^(?:part1){1,1}?(part2){1,1}(?:part3)$
//
// (or ^(?:pattern)$ without separators). It also returns the parts.
func similarRegexp(name, pattern, esc string) (*regexp.Regexp, []string, error) {
	parts, err := similarParts(name, pattern, esc)
	if err != nil {
		return nil, nil, err
	}
	expr := "(?s)^(?:" + parts[0]
	switch len(parts) {
	case 2:
		expr += "){1,1}?(" + parts[1]
	case 3:
		expr += "){1,1}?(" + parts[1] + "){1,1}(?:" + parts[2]
	}
	re, err := compileRegexp(expr + ")$")
	if err != nil {
		return nil, nil, invalidRegexp(name, err, false)
	}
	return re, parts, nil
}

// similarSubstring is SUBSTRING(s SIMILAR pattern ESCAPE esc): NULL unless
// the pattern matches the whole string, and then the text matched by the
// part between the escape-double-quote separators (all of s without
// separators; with one, the part after it). As the standard and Postgres
// define it, the separators divide the pattern into independent regular
// expressions, and the first matches as little of the string as it can,
// then the second as much as it can. This is computed exactly, without
// relying on how RE2 resolves greediness.
func similarSubstring(s, pattern, esc string) (Value, error) {
	re, parts, err := similarRegexp("SUBSTRING", pattern, esc)
	if err != nil {
		return Value{}, err
	}
	if !re.MatchString(s) {
		return nullValue(typeString), nil
	}
	if len(parts) == 1 {
		return stringValue(s), nil
	}
	if len(parts) == 2 {
		parts = append(parts, "")
	}
	var whole [3]*regexp.Regexp
	for i, p := range parts {
		if whole[i], err = compileRegexp("(?s)^(?:" + p + ")$"); err != nil {
			// The separators are inside parentheses, so the parts are not
			// expressions of their own. As Postgres does, return what the
			// group between them matched in the whole expression (chosen
			// by RE2's preferences).
			m := re.FindStringSubmatchIndex(s)
			if m[2] < 0 {
				return nullValue(typeString), nil
			}
			return stringValue(s[m[2]:m[3]]), nil
		}
	}
	rest, err := compileRegexp("(?s)^(?:" + parts[1] + ")(?:" + parts[2] + ")$")
	if err != nil {
		return Value{}, invalidRegexp("SUBSTRING", err, false)
	}
	// The shortest first part after which the rest matches, then the
	// longest second part.
	for i := 0; ; {
		if whole[0].MatchString(s[:i]) && rest.MatchString(s[i:]) {
			for j := len(s); ; {
				if whole[1].MatchString(s[i:j]) && whole[2].MatchString(s[j:]) {
					return stringValue(s[i:j]), nil
				}
				if j == i {
					break
				}
				_, w := utf8.DecodeLastRuneInString(s[i:j])
				j -= w
			}
		}
		if i == len(s) {
			break
		}
		_, w := utf8.DecodeRuneInString(s[i:])
		i += w
	}
	return nullValue(typeString), nil
}
