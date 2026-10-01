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

// SQL/JSON path expressions and the query functions JSON_VALUE, JSON_QUERY
// and JSON_EXISTS, as in Postgres 17. Documents are read as jsonb: keys are
// deduplicated (the last one wins) and numbers are NUMERIC (see json.go).
//
// The path language is a subset of SQL/JSON's:
//
//	[lax | strict] $ { .key | ."key" | .* | [subscript, …] | [*] } …
//
// where a subscript is n, last, last - n, or a range a to b of those
// (0-based). Lax mode, the default, unwraps an array for a member accessor
// (.key on an array of objects reads each element), treats a non-array as an
// array of one for a subscript, and skips missing keys and out-of-range
// subscripts. Strict mode makes those errors, handled by the ON ERROR
// clause. Filters (?(…)), item methods (.size(), …), arithmetic, .** and
// PASSING variables are not supported.

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf16"
	"unicode/utf8"
)

type jsonPathStepKind uint8

const (
	pathKey        jsonPathStepKind = iota // .key, ."key"
	pathAnyKey                             // .*
	pathSubscripts                         // [subscript, …]
	pathAnyElement                         // [*]
)

type jsonPathStep struct {
	kind jsonPathStepKind
	key  string
	subs []jsonPathSubscript
}

// jsonPathSubscript is an array subscript, from or a range from TO to.
type jsonPathSubscript struct{ from, to jsonPathIndex }

// jsonPathIndex is an array index n, or with last set, last + n.
type jsonPathIndex struct {
	last bool
	n    int64
}

func (x jsonPathIndex) resolve(size int64) int64 {
	if x.last {
		return size - 1 + x.n
	}
	return x.n
}

type jsonPath struct {
	strict bool
	steps  []jsonPathStep
}

// jsonPathCache keeps compiled paths across rows and statements.
var jsonPathCache struct {
	sync.Mutex
	m map[string]*jsonPath
}

func compileJSONPath(src string) (*jsonPath, error) {
	jsonPathCache.Lock()
	p, ok := jsonPathCache.m[src]
	jsonPathCache.Unlock()
	if ok {
		return p, nil
	}
	p, err := parseJSONPath(src)
	if err != nil {
		return nil, err
	}
	jsonPathCache.Lock()
	if jsonPathCache.m == nil || len(jsonPathCache.m) >= 256 {
		jsonPathCache.m = map[string]*jsonPath{}
	}
	jsonPathCache.m[src] = p
	jsonPathCache.Unlock()
	return p, nil
}

// ---- parsing ----

// jsonPathSpecial are the characters that end an unquoted key.
const jsonPathSpecial = "?%$.[]{}()|&!=<>@#,*:-+/\\\" \t\n\r\f"

type jsonPathScanner struct {
	src string
	i   int
}

func (sc *jsonPathScanner) skipSpace() {
	for sc.i < len(sc.src) && strings.IndexByte(" \t\n\r\f", sc.src[sc.i]) >= 0 {
		sc.i++
	}
}

func (sc *jsonPathScanner) at(c byte) bool { return sc.i < len(sc.src) && sc.src[sc.i] == c }

// word reads an unquoted key or keyword.
func (sc *jsonPathScanner) word() string {
	start := sc.i
	for sc.i < len(sc.src) && strings.IndexByte(jsonPathSpecial, sc.src[sc.i]) < 0 {
		sc.i++
	}
	return sc.src[start:sc.i]
}

// syntaxError is Postgres's error for the token at the current position.
func (sc *jsonPathScanner) syntaxError() error {
	if sc.i >= len(sc.src) {
		return errors.New("syntax error at end of jsonpath input")
	}
	start := sc.i
	tok := sc.word()
	if tok == "" {
		_, size := utf8.DecodeRuneInString(sc.src[start:])
		tok = sc.src[start : start+size]
	}
	return fmt.Errorf(`syntax error at or near "%s" of jsonpath input`, tok)
}

func parseJSONPath(src string) (*jsonPath, error) {
	sc := &jsonPathScanner{src: src}
	p := &jsonPath{}
	sc.skipSpace()
	start := sc.i
	switch strings.ToLower(sc.word()) {
	case "strict":
		p.strict = true
	case "lax":
	default:
		sc.i = start
	}
	sc.skipSpace()
	if !sc.at('$') {
		return nil, sc.syntaxError()
	}
	sc.i++
	if v := sc.word(); v != "" {
		return nil, fmt.Errorf(`could not find jsonpath variable "%s"`, v)
	}
	for {
		sc.skipSpace()
		if sc.i >= len(src) {
			return p, nil
		}
		var step jsonPathStep
		var err error
		switch src[sc.i] {
		case '.':
			sc.i++
			sc.skipSpace()
			step, err = sc.member()
		case '[':
			sc.i++
			step, err = sc.subscripts()
		case '?':
			return nil, errors.New("jsonpath filter expressions are not supported")
		default:
			return nil, sc.syntaxError()
		}
		if err != nil {
			return nil, err
		}
		p.steps = append(p.steps, step)
	}
}

// member parses what follows a '.': *, "key" or key.
func (sc *jsonPathScanner) member() (jsonPathStep, error) {
	switch {
	case sc.at('*'):
		sc.i++
		if sc.at('*') {
			return jsonPathStep{}, errors.New("the jsonpath accessor .** is not supported")
		}
		return jsonPathStep{kind: pathAnyKey}, nil
	case sc.at('"'):
		key, err := sc.quoted()
		return jsonPathStep{kind: pathKey, key: key}, err
	}
	key := sc.word()
	if key == "" {
		return jsonPathStep{}, sc.syntaxError()
	}
	end := sc.i
	sc.skipSpace()
	if sc.at('(') {
		return jsonPathStep{}, fmt.Errorf("the jsonpath item method .%s() is not supported", key)
	}
	sc.i = end
	return jsonPathStep{kind: pathKey, key: key}, nil
}

var jsonPathEscapes = map[byte]byte{'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t', 'v': '\v'}

// quoted reads a "string" with JSON's escapes (and \v, \xNN, \u{N…}).
func (sc *jsonPathScanner) quoted() (string, error) {
	s := sc.src
	var b strings.Builder
	for sc.i++; sc.i < len(s); {
		c := s[sc.i]
		if c == '"' {
			sc.i++
			return b.String(), nil
		}
		sc.i++
		if c != '\\' || sc.i >= len(s) {
			b.WriteByte(c)
			continue
		}
		e := s[sc.i]
		sc.i++
		var r rune
		switch {
		case e == 'u' && sc.at('{'):
			end := strings.IndexByte(s[sc.i:], '}')
			if end < 0 {
				return "", errors.New("invalid Unicode escape sequence in jsonpath")
			}
			n, err := strconv.ParseUint(s[sc.i+1:sc.i+end], 16, 32)
			if err != nil || n > utf8.MaxRune {
				return "", errors.New("invalid Unicode escape sequence in jsonpath")
			}
			r, sc.i = rune(n), sc.i+end+1
		case e == 'u':
			n, ok := hex4(s[sc.i:])
			if !ok {
				return "", errors.New("invalid Unicode escape sequence in jsonpath")
			}
			r, sc.i = n, sc.i+4
		case e == 'x' && sc.i+2 <= len(s):
			n, err := strconv.ParseUint(s[sc.i:sc.i+2], 16, 8)
			if err != nil {
				return "", errors.New("invalid hexadecimal character sequence in jsonpath")
			}
			r, sc.i = rune(n), sc.i+2
		default:
			if x, ok := jsonPathEscapes[e]; ok {
				e = x
			}
			b.WriteByte(e)
			continue
		}
		// A surrogate pair is two \u escapes.
		if r >= 0xd800 && r < 0xdc00 && strings.HasPrefix(s[sc.i:], `\u`) {
			if lo, ok := hex4(s[sc.i+2:]); ok && lo >= 0xdc00 && lo < 0xe000 {
				r, sc.i = utf16.DecodeRune(r, lo), sc.i+6
			}
		}
		b.WriteRune(r)
	}
	return "", errors.New("syntax error at end of jsonpath input")
}

// subscripts parses what follows a '[': * ] or subscript, … ].
func (sc *jsonPathScanner) subscripts() (jsonPathStep, error) {
	sc.skipSpace()
	if sc.at('*') {
		sc.i++
		sc.skipSpace()
		if !sc.at(']') {
			return jsonPathStep{}, sc.syntaxError()
		}
		sc.i++
		return jsonPathStep{kind: pathAnyElement}, nil
	}
	st := jsonPathStep{kind: pathSubscripts}
	for {
		from, err := sc.index()
		if err != nil {
			return st, err
		}
		sub := jsonPathSubscript{from: from, to: from}
		sc.skipSpace()
		start := sc.i
		if strings.EqualFold(sc.word(), "to") {
			if sub.to, err = sc.index(); err != nil {
				return st, err
			}
			sc.skipSpace()
		} else {
			sc.i = start
		}
		st.subs = append(st.subs, sub)
		switch {
		case sc.at(','):
			sc.i++
		case sc.at(']'):
			sc.i++
			return st, nil
		default:
			return st, sc.syntaxError()
		}
	}
}

// index parses n, -n, last, last - n or last + n.
func (sc *jsonPathScanner) index() (jsonPathIndex, error) {
	sc.skipSpace()
	start := sc.i
	var x jsonPathIndex
	if strings.EqualFold(sc.word(), "last") {
		x.last = true
		sc.skipSpace()
		if !sc.at('-') && !sc.at('+') {
			return x, nil
		}
	} else {
		sc.i = start
	}
	neg := sc.at('-')
	if neg || sc.at('+') {
		sc.i++
		sc.skipSpace()
	}
	start = sc.i
	for sc.i < len(sc.src) && sc.src[sc.i] >= '0' && sc.src[sc.i] <= '9' {
		sc.i++
	}
	if start == sc.i {
		return x, sc.syntaxError()
	}
	n, err := strconv.ParseInt(sc.src[start:sc.i], 10, 32)
	if err != nil {
		return x, errors.New("jsonpath array subscript is out of integer range")
	}
	if neg {
		n = -n
	}
	x.n = n
	return x, nil
}

// ---- evaluation ----

// query parses doc as jsonb and returns the items the path selects.
func (p *jsonPath) query(doc string) ([]*jsonNode, error) {
	root, err := parseJSON(doc, true)
	if err != nil {
		return nil, err
	}
	toJSONB(root)
	items := []*jsonNode{root}
	for _, st := range p.steps {
		var next []*jsonNode
		for _, it := range items {
			if next, err = p.apply(st, it, next, true); err != nil {
				return nil, err
			}
		}
		items = next
	}
	return items, nil
}

// apply appends the items one step selects from it to out. In lax mode a
// member accessor is applied to each element of an array (unwrap is false
// for those, so nested arrays are not unwrapped).
func (p *jsonPath) apply(st jsonPathStep, it *jsonNode, out []*jsonNode, unwrap bool) ([]*jsonNode, error) {
	switch st.kind {
	case pathKey, pathAnyKey:
		if it.kind == jsonArray && !p.strict && unwrap {
			var err error
			for _, e := range it.items {
				if out, err = p.apply(st, e, out, false); err != nil {
					return nil, err
				}
			}
			return out, nil
		}
		switch {
		case it.kind != jsonObject && !p.strict:
			return out, nil
		case it.kind != jsonObject && st.kind == pathAnyKey:
			return nil, errors.New("jsonpath wildcard member accessor can only be applied to an object")
		case it.kind != jsonObject:
			return nil, errors.New("jsonpath member accessor can only be applied to an object")
		case st.kind == pathAnyKey:
			return append(out, it.items...), nil
		}
		if v := it.member(st.key); v != nil {
			return append(out, v), nil
		}
		if p.strict {
			return nil, fmt.Errorf(`JSON object does not contain key "%s"`, st.key)
		}
		return out, nil
	case pathAnyElement:
		switch {
		case it.kind == jsonArray:
			return append(out, it.items...), nil
		case p.strict:
			return nil, errors.New("jsonpath wildcard array accessor can only be applied to an array")
		}
		return append(out, it), nil
	}
	elems := it.items
	if it.kind != jsonArray {
		if p.strict {
			return nil, errors.New("jsonpath array accessor can only be applied to an array")
		}
		elems = []*jsonNode{it}
	}
	size := int64(len(elems))
	for _, sub := range st.subs {
		from, to := sub.from.resolve(size), sub.to.resolve(size)
		if p.strict && (from < 0 || from > to || to >= size) {
			return nil, errors.New("jsonpath array subscript is out of bounds")
		}
		for i := max(from, 0); i <= min(to, size-1); i++ {
			out = append(out, elems[i])
		}
	}
	return out, nil
}

// ---- JSON_VALUE, JSON_QUERY, JSON_EXISTS ----

var errNoJSONItem = errors.New("no SQL/JSON item found for specified path")

// evalJSONQuery evaluates JSON_VALUE, JSON_QUERY and JSON_EXISTS. A
// malformed document, a strict-mode path error, a result of the wrong shape
// and a failed conversion to the RETURNING type are handled by ON ERROR; an
// invalid path is always an error.
func (env *evalEnv) evalJSONQuery(f *Func) (Value, error) {
	c := f.JSON
	doc, err := env.eval(f.Args[0])
	if err != nil {
		return Value{}, err
	}
	path, err := env.eval(f.Args[1])
	if err != nil {
		return Value{}, err
	}
	switch {
	case doc.Null || path.Null:
		return nullValue(c.returning), nil
	case doc.T.Kind != KindString && doc.T.Kind != KindBinary:
		return Value{}, fmt.Errorf("%s expects JSON text, not %s", f.Name, doc.T.SQLName())
	case path.T.Kind != KindString:
		return Value{}, fmt.Errorf("%s: the path must be text, not %s", f.Name, path.T.SQLName())
	}
	jp, err := compileJSONPath(path.S)
	if err != nil {
		return Value{}, err
	}
	items, err := jp.query(doc.S)
	if f.Name == "JSON_EXISTS" {
		if err == nil {
			return boolValue(len(items) > 0), nil
		}
		switch c.onError.kind {
		case behaviorError:
			return Value{}, err
		case behaviorTrue:
			return boolValue(true), nil
		case behaviorUnknown:
			return nullValue(typeBool), nil
		}
		return boolValue(false), nil
	}
	if err != nil {
		return env.jsonOnError(f, err)
	}
	if len(items) == 0 {
		if c.onEmpty.kind == behaviorError {
			return Value{}, errNoJSONItem
		}
		v, err := env.jsonBehavior(f, c.onEmpty)
		if err != nil {
			return env.jsonOnError(f, err)
		}
		return v, nil
	}
	var v Value
	if f.Name == "JSON_VALUE" {
		v, err = jsonValueResult(c, items)
	} else {
		v, err = jsonQueryResult(c, items)
	}
	if err != nil {
		return env.jsonOnError(f, err)
	}
	return v, nil
}

// jsonOnError applies the ON ERROR behavior to err.
func (env *evalEnv) jsonOnError(f *Func, err error) (Value, error) {
	if f.JSON.onError.kind == behaviorError {
		return Value{}, err
	}
	return env.jsonBehavior(f, f.JSON.onError)
}

// jsonBehavior returns the value of an ON EMPTY / ON ERROR behavior (other
// than ERROR), of the RETURNING type.
func (env *evalEnv) jsonBehavior(f *Func, b jsonBehavior) (Value, error) {
	c := f.JSON
	switch b.kind {
	case behaviorEmptyArray:
		return jsonReturning(c, "[]")
	case behaviorEmptyObject:
		return jsonReturning(c, "{}")
	case behaviorDefault:
		x := f.Args[b.arg]
		v, err := env.eval(x)
		switch {
		case err != nil:
			return Value{}, err
		case v.Null || !c.returnJSON:
			return Coerce(v, c.returning)
		}
		// For a JSON result, text is read as JSON and other values are
		// encoded.
		text := v.S
		if v.T.Kind != KindString {
			var sb strings.Builder
			writeJSONValue(&sb, v, isJSONExpr(x))
			text = sb.String()
		}
		s, err := normalizeJSONB(text)
		if err != nil {
			return Value{}, err
		}
		return stringValue(s), nil
	}
	return nullValue(c.returning), nil
}

// jsonReturning converts JSON text (in jsonb form) to the RETURNING type.
func jsonReturning(c *jsonClauses, text string) (Value, error) {
	if c.returnJSON {
		return stringValue(text), nil
	}
	return Coerce(stringValue(text), c.returning)
}

// jsonValueResult is the result of JSON_VALUE: the one scalar item, as text
// (a string's value, a number in NUMERIC form) converted to the RETURNING
// type; NULL for a JSON null.
func jsonValueResult(c *jsonClauses, items []*jsonNode) (Value, error) {
	it := items[0]
	switch {
	case len(items) > 1 || it.kind == jsonArray || it.kind == jsonObject:
		return Value{}, errors.New("JSON path expression in JSON_VALUE must return single scalar item")
	case it.kind == jsonNull:
		return nullValue(c.returning), nil
	case c.returnJSON:
		var b strings.Builder
		if err := writeJSONB(&b, it); err != nil {
			return Value{}, err
		}
		return stringValue(b.String()), nil
	}
	var s string
	var err error
	switch it.kind {
	case jsonString:
		s, err = jsonUnescape(it.raw)
	case jsonNumber:
		s, err = jsonbNumber(it.raw)
	default:
		s = it.raw
	}
	if err != nil {
		return Value{}, err
	}
	return Coerce(stringValue(s), c.returning)
}

// jsonQueryResult is the result of JSON_QUERY: the one item as JSON, or
// the items wrapped in an array (WITH WRAPPER, or WITH CONDITIONAL WRAPPER
// unless the one item is an array or object). OMIT QUOTES returns a
// string's value instead of its JSON.
func jsonQueryResult(c *jsonClauses, items []*jsonNode) (Value, error) {
	it := items[0]
	wrap := c.wrapper == wrapperUnconditional ||
		c.wrapper == wrapperConditional && (len(items) > 1 || it.kind != jsonArray && it.kind != jsonObject)
	if !wrap && len(items) > 1 {
		return Value{}, errors.New("JSON path expression in JSON_QUERY must return single item when no wrapper is requested")
	}
	if !wrap && c.omitQuotes && it.kind == jsonString {
		s, err := jsonUnescape(it.raw)
		if err != nil {
			return Value{}, err
		}
		if c.returnJSON {
			// The string's value must itself be JSON.
			if s, err = normalizeJSONB(s); err != nil {
				return Value{}, err
			}
		}
		return jsonReturning(c, s)
	}
	var b strings.Builder
	if wrap {
		b.WriteByte('[')
	}
	for i, it := range items {
		if i > 0 {
			b.WriteString(", ")
		}
		if err := writeJSONB(&b, it); err != nil {
			return Value{}, err
		}
	}
	if wrap {
		b.WriteByte(']')
	}
	return jsonReturning(c, b.String())
}
