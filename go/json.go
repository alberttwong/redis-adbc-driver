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

// JSON stored as text. There is no JSON column type: documents live in
// VARCHAR columns and are parsed by each function or operator that reads
// them. Semantics follow Postgres:
//
//   - The operators ->, ->>, #> and #>> and the functions JSON_EXTRACT_PATH,
//     JSON_EXTRACT_PATH_TEXT, JSON_TYPEOF and JSON_ARRAY_LENGTH read their
//     argument as the json type: a value keeps the text it has in the
//     document, so numbers are never rounded, and of duplicate keys the last
//     one wins. Malformed JSON is an error ("invalid input syntax for type
//     json").
//   - The JSONB_ spellings, ::jsonb and RETURNING JSONB give jsonb's
//     normalized text: object keys sorted (shorter first) without
//     duplicates, numbers in NUMERIC form (1e2 is 100, 1.50 stays 1.50), and
//     ", " and ": " between items.
//   - JSON_VALUE, JSON_QUERY and JSON_EXISTS (jsonpath.go) read documents as
//     jsonb, as in Postgres 17; their ON ERROR clause also covers malformed
//     documents.
//   - The constructors (JSON_BUILD_OBJECT, JSON_OBJECT, JSON_AGG, …) encode
//     SQL values as Postgres's to_json does. Their results are VARCHAR. An
//     argument is embedded as JSON if it is itself JSON: the result of a JSON
//     function, of -> or #>, or of a ::json cast. Any other text becomes a
//     JSON string, so JSON read back from a table, view or subquery needs
//     ::json to be embedded.

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// ---- functions ----

// checkJSONArgs checks the rules of the JSON functions beyond their
// argument counts (which the registry has): JSON_BUILD_OBJECT's keys and
// values come in pairs, object keys can't be JSON, and a JSON aggregate
// with DISTINCT may only order by its arguments. The parser builds the
// functions starting with __: __JSON and __JSONB for casts, __IS_JSON for
// IS JSON, and __JSON_OBJECT and __JSON_ARRAY for the SQL/JSON
// constructors.
func checkJSONArgs(f *Func) error {
	n := len(f.Args)
	switch f.Name {
	case "JSON_BUILD_OBJECT", "JSONB_BUILD_OBJECT":
		if n%2 != 0 {
			return fmt.Errorf("%s: argument list must have even number of elements (alternating keys and values)", f.Name)
		}
		for i := 0; i < n; i += 2 {
			if isJSONExpr(f.Args[i]) {
				return errJSONKey
			}
		}
	case "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG":
		if n > 0 && isJSONExpr(f.Args[0]) {
			return errJSONKey
		}
	}
	if f.Distinct && jsonAggregates[f.Name] {
		for _, o := range f.OrderBy {
			if !slices.ContainsFunc(f.Args, func(a Expr) bool { return exprEqual(a, o.Expr) }) {
				return fmt.Errorf("in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list")
			}
		}
	}
	return nil
}

var (
	errJSONKey     = errors.New("key value must be scalar, not array, composite, or json")
	errJSONNullKey = errors.New("null value not allowed for object key")
)

// jsonFuncType returns the result type of a JSON function; ok is false if f
// is not one.
func jsonFuncType(f *Func) (ColType, bool) {
	switch {
	case f.JSON != nil:
		return f.JSON.returning, true
	case f.Name == "JSON_ARRAY_LENGTH" || f.Name == "JSONB_ARRAY_LENGTH":
		return typeInt64, true
	case jsonFuncs[f.Name] || jsonAggregates[f.Name]:
		return typeString, true
	}
	return ColType{}, false
}

// jsonResultFuncs return JSON (rather than text): the constructors embed
// their results as JSON.
var jsonResultFuncs = map[string]bool{
	"->": true, "#>": true, "JSON_EXTRACT_PATH": true, "JSONB_EXTRACT_PATH": true,
	"JSON_BUILD_OBJECT": true, "JSONB_BUILD_OBJECT": true, "JSON_BUILD_ARRAY": true, "JSONB_BUILD_ARRAY": true,
	"JSON_OBJECT": true, "JSONB_OBJECT": true, "TO_JSON": true, "TO_JSONB": true, "__JSON": true, "__JSONB": true,
	"JSON_AGG": true, "JSONB_AGG": true, "JSON_OBJECT_AGG": true, "JSONB_OBJECT_AGG": true,
}

// isJSONExpr reports whether an expression gives JSON: a JSON function or
// cast, or a CASE or COALESCE whose results all are.
func isJSONExpr(e Expr) bool {
	isNull := func(e Expr) bool {
		l, ok := e.(*Literal)
		return ok && l.V.Null
	}
	all := func(es []Expr) bool {
		found := false
		for _, x := range es {
			switch {
			case isJSONExpr(x):
				found = true
			case !isNull(x):
				return false
			}
		}
		return found
	}
	switch x := e.(type) {
	case *Func:
		switch {
		case x.JSON != nil:
			return x.JSON.returnJSON
		case x.Name == "COALESCE":
			return all(x.Args)
		}
		return jsonResultFuncs[x.Name]
	case *Case:
		results := []Expr{x.Else}
		for _, w := range x.Whens {
			results = append(results, w.Then)
		}
		if x.Else == nil {
			results = results[1:]
		}
		return all(results)
	}
	return false
}

// jsonClauses are the clauses of a SQL/JSON function or predicate. The
// DEFAULT expressions of ON EMPTY and ON ERROR are arguments of the call, so
// they are bound and evaluated like any other; behaviors refer to them by
// position.
type jsonClauses struct {
	// returning is the result type. returnJSON is set when the result is
	// JSON text (RETURNING JSON or JSONB, and the default of JSON_QUERY,
	// JSON_OBJECT and JSON_ARRAY), and jsonb when it is in jsonb's form.
	returning  ColType
	returnJSON bool
	jsonb      bool

	onEmpty, onError jsonBehavior
	wrapper          jsonWrapper // JSON_QUERY
	omitQuotes       bool        // JSON_QUERY … OMIT QUOTES
	absentOnNull     bool        // JSON_OBJECT, JSON_ARRAY: ABSENT ON NULL
	uniqueKeys       bool        // JSON_OBJECT, IS JSON: WITH UNIQUE KEYS
	is               string      // IS JSON: VALUE, SCALAR, ARRAY or OBJECT
}

type jsonWrapper uint8

const (
	wrapperNone jsonWrapper = iota
	wrapperConditional
	wrapperUnconditional
)

type jsonBehaviorKind uint8

const (
	behaviorNull jsonBehaviorKind = iota
	behaviorError
	behaviorDefault
	behaviorEmptyArray
	behaviorEmptyObject
	behaviorTrue
	behaviorFalse
	behaviorUnknown
)

// jsonBehavior is an ON EMPTY or ON ERROR behavior; arg is the position of a
// DEFAULT expression in the call's arguments.
type jsonBehavior struct {
	kind jsonBehaviorKind
	arg  int
}

// evalJSONFunc evaluates a JSON function or operator (implJSON in funcs.go).
func (env *evalEnv) evalJSONFunc(f *Func) (Value, error) {
	if f.Name == "JSON_VALUE" || f.Name == "JSON_QUERY" || f.Name == "JSON_EXISTS" {
		return env.evalJSONQuery(f)
	}
	args := make([]Value, len(f.Args))
	for i, a := range f.Args {
		v, err := env.eval(a)
		if err != nil {
			return Value{}, err
		}
		args[i] = v
	}
	switch f.Name {
	case "JSON_BUILD_OBJECT", "JSONB_BUILD_OBJECT", "__JSON_OBJECT":
		return buildJSONObject(f, args, env.zone())
	case "JSON_BUILD_ARRAY", "JSONB_BUILD_ARRAY", "__JSON_ARRAY":
		return buildJSONArray(f, args, env.zone())
	}
	// The other functions return NULL for a NULL argument.
	t, _ := jsonFuncType(f)
	for _, a := range args {
		if a.Null {
			return nullValue(t), nil
		}
	}
	switch f.Name {
	case "TO_JSON", "TO_JSONB":
		var b strings.Builder
		writeJSONValue(&b, args[0], isJSONExpr(f.Args[0]), env.zone())
		return jsonResult(f, b.String())
	case "JSON_OBJECT", "JSONB_OBJECT":
		return jsonObjectFromArrays(f, args)
	case "__IS_JSON":
		return isJSON(f.JSON, args[0])
	}
	doc, err := jsonDocArg(f, args)
	if err != nil {
		return Value{}, err
	}
	switch f.Name {
	case "__JSON":
		if _, err := parseJSON(doc, false); err != nil {
			return Value{}, err
		}
		return stringValue(doc), nil
	case "__JSONB":
		s, err := normalizeJSONB(doc)
		if err != nil {
			return Value{}, err
		}
		return stringValue(s), nil
	case "JSON_TYPEOF", "JSONB_TYPEOF":
		n, err := parseJSON(doc, f.Name == "JSONB_TYPEOF")
		if err != nil {
			return Value{}, err
		}
		return stringValue(jsonKindNames[n.kind]), nil
	case "JSON_ARRAY_LENGTH", "JSONB_ARRAY_LENGTH":
		n, err := parseJSON(doc, f.Name == "JSONB_ARRAY_LENGTH")
		if err != nil {
			return Value{}, err
		}
		switch n.kind {
		case jsonArray:
			return intValue(typeInt64, int64(len(n.items))), nil
		case jsonObject:
			return Value{}, errors.New("cannot get array length of a non-array")
		}
		return Value{}, errors.New("cannot get array length of a scalar")
	}
	// ->, ->>, #>, #>> and JSON_EXTRACT_PATH[_TEXT]
	steps, ok, err := jsonSteps(f, args)
	if err != nil {
		return Value{}, err
	}
	if !ok {
		return nullValue(typeString), nil
	}
	asText := f.Name == "->>" || f.Name == "#>>" || strings.HasSuffix(f.Name, "_TEXT")
	return jsonExtract(doc, steps, asText, strings.HasPrefix(f.Name, "JSONB_"))
}

// jsonDocArg returns the JSON text in the first argument of a JSON function
// or operator.
func jsonDocArg(f *Func, args []Value) (string, error) {
	v := args[0]
	if v.T.Kind == KindString {
		return v.S, nil
	}
	switch f.Name {
	case "->", "->>", "#>", "#>>":
		return "", fmt.Errorf("operator does not exist: %s %s %s", v.T.SQLName(), f.Name, args[1].T.SQLName())
	case "__JSON", "__JSONB":
		return "", fmt.Errorf("cannot cast type %s to %s", v.T.SQLName(), strings.ToLower(f.Name[2:]))
	}
	return "", fmt.Errorf("%s expects JSON text, not %s", f.Name, v.T.SQLName())
}

// jsonResult returns a constructor's JSON text, in jsonb's form for the
// JSONB_ functions and RETURNING JSONB.
func jsonResult(f *Func, text string) (Value, error) {
	if strings.HasPrefix(f.Name, "JSONB_") || (f.JSON != nil && f.JSON.jsonb) {
		s, err := normalizeJSONB(text)
		if err != nil {
			return Value{}, err
		}
		text = s
	}
	return stringValue(text), nil
}

// ---- documents ----

type jsonKind uint8

const (
	jsonNull jsonKind = iota
	jsonBool
	jsonNumber
	jsonString
	jsonArray
	jsonObject
)

// jsonKindNames are the names JSON_TYPEOF returns.
var jsonKindNames = [...]string{
	jsonNull: "null", jsonBool: "boolean", jsonNumber: "number", jsonString: "string", jsonArray: "array", jsonObject: "object",
}

// jsonNode is a value of a parsed document. raw is its text in the document,
// so a number keeps its exact spelling and a sub-document its layout.
type jsonNode struct {
	kind jsonKind
	raw  string
	// items are the elements of an array or the values of an object, whose
	// keys are in keys, in document order with duplicates kept. Keys are
	// unescaped only by a strict parse; otherwise they are the raw tokens.
	items []*jsonNode
	keys  []string
}

// member returns the value of an object's key (the last one, if the key is
// duplicated), or nil.
func (n *jsonNode) member(key string) *jsonNode {
	for i := len(n.keys) - 1; i >= 0; i-- {
		if n.keys[i] == key {
			return n.items[i]
		}
	}
	return nil
}

// element returns an array's element i (counted from the end if negative),
// or nil.
func (n *jsonNode) element(i int64) *jsonNode {
	size := int64(len(n.items))
	if i < 0 {
		i += size
	}
	if i < 0 || i >= size {
		return nil
	}
	return n.items[i]
}

// maxJSONDepth bounds the nesting of parsed documents.
const maxJSONDepth = 10000

// jsonSyntaxError is Postgres's error for malformed JSON, with its DETAIL.
func jsonSyntaxError(format string, args ...any) error {
	return fmt.Errorf("invalid input syntax for type json: "+format, args...)
}

var (
	errJSONEnded         = jsonSyntaxError("the input string ended unexpectedly")
	errJSONLowSurrogate  = jsonSyntaxError("Unicode low surrogate must follow a high surrogate")
	errJSONHighSurrogate = jsonSyntaxError("Unicode high surrogate must not follow a high surrogate")
	errJSONZero          = errors.New("unsupported Unicode escape sequence: \\u0000 cannot be converted to text")
	errJSONDepth         = jsonSyntaxError("nesting is deeper than %d levels", maxJSONDepth)
)

type jsonTokenKind uint8

const (
	jtEnd jsonTokenKind = iota
	jtString
	jtNumber
	jtTrue
	jtFalse
	jtNull
	jtPunct // { } [ ] , :
)

type jsonToken struct {
	kind       jsonTokenKind
	start, end int
}

// jsonParser is a recursive-descent parser with Postgres's error messages.
type jsonParser struct {
	src string
	pos int
	// strict rejects the \u escapes Postgres can't turn into text (code
	// point 0, unpaired surrogates) in every string, as it does when it
	// decodes a document's strings: in the processing functions and for
	// jsonb, but not when it only checks json input.
	strict bool
	depth  int
}

// parseJSON parses a document (see jsonParser.strict).
func parseJSON(s string, strict bool) (*jsonNode, error) {
	p := &jsonParser{src: s, strict: strict}
	tok, err := p.lex()
	if err != nil {
		return nil, err
	}
	n, err := p.value(tok)
	if err != nil {
		return nil, err
	}
	if tok, err = p.lex(); err != nil {
		return nil, err
	}
	if tok.kind != jtEnd {
		return nil, jsonSyntaxError(`expected end of input, but found "%s"`, p.text(tok))
	}
	return n, nil
}

func (p *jsonParser) text(t jsonToken) string { return p.src[t.start:t.end] }

func (p *jsonParser) isPunct(t jsonToken, c byte) bool {
	return t.kind == jtPunct && p.src[t.start] == c
}

// value parses the value that starts with tok.
func (p *jsonParser) value(tok jsonToken) (*jsonNode, error) {
	raw := p.text(tok)
	switch tok.kind {
	case jtEnd:
		return nil, errJSONEnded
	case jtString:
		return &jsonNode{kind: jsonString, raw: raw}, nil
	case jtNumber:
		return &jsonNode{kind: jsonNumber, raw: raw}, nil
	case jtTrue, jtFalse:
		return &jsonNode{kind: jsonBool, raw: raw}, nil
	case jtNull:
		return &jsonNode{kind: jsonNull, raw: raw}, nil
	}
	switch raw {
	case "{", "[":
		if p.depth++; p.depth > maxJSONDepth {
			return nil, errJSONDepth
		}
		var n *jsonNode
		var err error
		if raw == "{" {
			n, err = p.object(tok.start)
		} else {
			n, err = p.array(tok.start)
		}
		p.depth--
		return n, err
	}
	return nil, jsonSyntaxError(`expected JSON value, but found "%s"`, raw)
}

func (p *jsonParser) object(start int) (*jsonNode, error) {
	n := &jsonNode{kind: jsonObject}
	tok, err := p.lex()
	if err != nil {
		return nil, err
	}
	if p.isPunct(tok, '}') {
		n.raw = p.src[start:tok.end]
		return n, nil
	}
	for first := true; ; first = false {
		switch {
		case tok.kind == jtString:
		case tok.kind == jtEnd:
			return nil, errJSONEnded
		case first:
			return nil, jsonSyntaxError(`expected string or "}", but found "%s"`, p.text(tok))
		default:
			return nil, jsonSyntaxError(`expected string, but found "%s"`, p.text(tok))
		}
		key := p.text(tok)
		if p.strict {
			if key, err = jsonUnescape(key); err != nil {
				return nil, err
			}
		}
		if tok, err = p.lex(); err != nil {
			return nil, err
		}
		if !p.isPunct(tok, ':') {
			if tok.kind == jtEnd {
				return nil, errJSONEnded
			}
			return nil, jsonSyntaxError(`expected ":", but found "%s"`, p.text(tok))
		}
		if tok, err = p.lex(); err != nil {
			return nil, err
		}
		v, err := p.value(tok)
		if err != nil {
			return nil, err
		}
		n.keys = append(n.keys, key)
		n.items = append(n.items, v)
		if tok, err = p.lex(); err != nil {
			return nil, err
		}
		switch {
		case p.isPunct(tok, '}'):
			n.raw = p.src[start:tok.end]
			return n, nil
		case p.isPunct(tok, ','):
			if tok, err = p.lex(); err != nil {
				return nil, err
			}
		case tok.kind == jtEnd:
			return nil, errJSONEnded
		default:
			return nil, jsonSyntaxError(`expected "," or "}", but found "%s"`, p.text(tok))
		}
	}
}

func (p *jsonParser) array(start int) (*jsonNode, error) {
	n := &jsonNode{kind: jsonArray}
	tok, err := p.lex()
	if err != nil {
		return nil, err
	}
	if p.isPunct(tok, ']') {
		n.raw = p.src[start:tok.end]
		return n, nil
	}
	for {
		v, err := p.value(tok)
		if err != nil {
			return nil, err
		}
		n.items = append(n.items, v)
		if tok, err = p.lex(); err != nil {
			return nil, err
		}
		switch {
		case p.isPunct(tok, ']'):
			n.raw = p.src[start:tok.end]
			return n, nil
		case p.isPunct(tok, ','):
			if tok, err = p.lex(); err != nil {
				return nil, err
			}
		case tok.kind == jtEnd:
			return nil, errJSONEnded
		default:
			return nil, jsonSyntaxError(`expected "," or "]", but found "%s"`, p.text(tok))
		}
	}
}

// jsonWordChar reports whether c continues a token in Postgres's JSON lexer
// (letters, digits, '_' and the bytes of non-ASCII characters), so that an
// invalid token is reported whole.
func jsonWordChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c >= 0x80
}

func (p *jsonParser) lex() (jsonToken, error) {
	s := p.src
	for p.pos < len(s) && (s[p.pos] == ' ' || s[p.pos] == '\t' || s[p.pos] == '\n' || s[p.pos] == '\r') {
		p.pos++
	}
	start := p.pos
	if start == len(s) {
		return jsonToken{kind: jtEnd, start: start, end: start}, nil
	}
	switch c := s[start]; {
	case strings.IndexByte("{}[],:", c) >= 0:
		p.pos++
		return jsonToken{kind: jtPunct, start: start, end: p.pos}, nil
	case c == '"':
		return p.lexString()
	case c == '-' || c >= '0' && c <= '9':
		return p.lexNumber()
	case jsonWordChar(c):
		for p.pos < len(s) && jsonWordChar(s[p.pos]) {
			p.pos++
		}
		tok := jsonToken{start: start, end: p.pos}
		switch s[start:p.pos] {
		case "true":
			tok.kind = jtTrue
		case "false":
			tok.kind = jtFalse
		case "null":
			tok.kind = jtNull
		default:
			return jsonToken{}, jsonSyntaxError(`token "%s" is invalid`, s[start:p.pos])
		}
		return tok, nil
	}
	_, size := utf8.DecodeRuneInString(s[start:])
	return jsonToken{}, jsonSyntaxError(`token "%s" is invalid`, s[start:start+size])
}

// lexNumber reads -?(0|[1-9][0-9]*)(.[0-9]+)?([eE][+-]?[0-9]+)?.
func (p *jsonParser) lexNumber() (jsonToken, error) {
	s, start := p.src, p.pos
	i := start
	digits := func() int {
		n := 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			n++
		}
		return n
	}
	if s[i] == '-' {
		i++
	}
	ok := true
	if i < len(s) && s[i] == '0' {
		i++
	} else if digits() == 0 {
		ok = false
	}
	if ok && i < len(s) && s[i] == '.' {
		i++
		ok = digits() > 0
	}
	if ok && i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		ok = digits() > 0
	}
	// As in Postgres, letters and digits right after a number belong to the
	// (invalid) token: 01 and 1x are reported whole.
	for i < len(s) && jsonWordChar(s[i]) {
		i++
		ok = false
	}
	p.pos = i
	if !ok {
		return jsonToken{}, jsonSyntaxError(`token "%s" is invalid`, s[start:i])
	}
	return jsonToken{kind: jtNumber, start: start, end: i}, nil
}

func (p *jsonParser) lexString() (jsonToken, error) {
	s, start := p.src, p.pos
	hi := rune(-1) // a high surrogate waiting for its low one (strict mode)
	for i := start + 1; ; {
		if i >= len(s) {
			p.pos = i
			return jsonToken{}, jsonSyntaxError(`token "%s" is invalid`, s[start:i])
		}
		c := s[i]
		switch {
		case c == '"':
			if hi >= 0 {
				return jsonToken{}, errJSONLowSurrogate
			}
			p.pos = i + 1
			return jsonToken{kind: jtString, start: start, end: p.pos}, nil
		case c < 0x20:
			return jsonToken{}, jsonSyntaxError("character with value 0x%02x must be escaped", c)
		case c != '\\':
			if hi >= 0 {
				return jsonToken{}, errJSONLowSurrogate
			}
			i++
			continue
		}
		i++
		if i >= len(s) {
			return jsonToken{}, jsonSyntaxError(`token "%s" is invalid`, s[start:i])
		}
		switch e := s[i]; e {
		case 'u':
			r, ok := hex4(s[i+1:])
			if !ok {
				return jsonToken{}, jsonSyntaxError(`"\u" must be followed by four hexadecimal digits`)
			}
			if p.strict {
				if _, err := jsonEscapedRune(r, &hi); err != nil {
					return jsonToken{}, err
				}
			}
			i += 5
		case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			if hi >= 0 {
				return jsonToken{}, errJSONLowSurrogate
			}
			i++
		default:
			_, size := utf8.DecodeRuneInString(s[i:])
			return jsonToken{}, jsonSyntaxError(`escape sequence "\%s" is invalid`, s[i:i+size])
		}
	}
}

// hex4 reads the four hex digits of a \u escape.
func hex4(s string) (rune, bool) {
	if len(s) < 4 {
		return 0, false
	}
	n, err := strconv.ParseUint(s[:4], 16, 16)
	if err != nil {
		return 0, false
	}
	return rune(n), true
}

// jsonEscapedRune applies Postgres's rules to the code point r of a \u
// escape: a high surrogate waits in *hi for the low one that must follow,
// and code point 0 is rejected. It returns the character to emit, or -1.
func jsonEscapedRune(r rune, hi *rune) (rune, error) {
	switch {
	case r >= 0xd800 && r < 0xdc00:
		if *hi >= 0 {
			return -1, errJSONHighSurrogate
		}
		*hi = r
		return -1, nil
	case r >= 0xdc00 && r < 0xe000:
		if *hi < 0 {
			return -1, errJSONLowSurrogate
		}
		r, *hi = utf16.DecodeRune(*hi, r), -1
	case *hi >= 0:
		return -1, errJSONLowSurrogate
	case r == 0:
		return -1, errJSONZero
	}
	return r, nil
}

var jsonSimpleEscapes = map[byte]byte{'"': '"', '\\': '\\', '/': '/', 'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t'}

// jsonUnescape returns the value of a string token (with its quotes), which
// the parser has checked.
func jsonUnescape(tok string) (string, error) {
	s := tok[1 : len(tok)-1]
	if strings.IndexByte(s, '\\') < 0 {
		return s, nil
	}
	var b strings.Builder
	b.Grow(len(s))
	hi := rune(-1)
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			if hi >= 0 {
				return "", errJSONLowSurrogate
			}
			b.WriteByte(s[i])
			continue
		}
		i++
		if s[i] != 'u' {
			if hi >= 0 {
				return "", errJSONLowSurrogate
			}
			b.WriteByte(jsonSimpleEscapes[s[i]])
			continue
		}
		r, _ := hex4(s[i+1:])
		i += 4
		r, err := jsonEscapedRune(r, &hi)
		if err != nil {
			return "", err
		}
		if r >= 0 {
			b.WriteRune(r)
		}
	}
	if hi >= 0 {
		return "", errJSONLowSurrogate
	}
	return b.String(), nil
}

// ---- jsonb ----

// toJSONB turns a strictly parsed document into jsonb's form: object keys
// sorted (shorter keys first, then bytewise), keeping only the last value
// of a duplicated key. raw is no longer valid; print with writeJSONB.
func toJSONB(n *jsonNode) {
	for _, c := range n.items {
		toJSONB(c)
	}
	if n.kind != jsonObject || len(n.keys) < 2 {
		return
	}
	idx := make([]int, len(n.keys))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		ka, kb := n.keys[idx[a]], n.keys[idx[b]]
		if len(ka) != len(kb) {
			return len(ka) < len(kb)
		}
		return ka < kb
	})
	keys := make([]string, 0, len(idx))
	items := make([]*jsonNode, 0, len(idx))
	for j, i := range idx {
		// Equal keys are adjacent, in document order: keep the last.
		if j+1 < len(idx) && n.keys[idx[j+1]] == n.keys[i] {
			continue
		}
		keys = append(keys, n.keys[i])
		items = append(items, n.items[i])
	}
	n.keys, n.items = keys, items
}

// writeJSONB writes a document in jsonb form (see toJSONB) as jsonb prints
// it.
func writeJSONB(b *strings.Builder, n *jsonNode) error {
	switch n.kind {
	case jsonObject:
		b.WriteByte('{')
		for i, k := range n.keys {
			if i > 0 {
				b.WriteString(", ")
			}
			writeJSONString(b, k)
			b.WriteString(": ")
			if err := writeJSONB(b, n.items[i]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	case jsonArray:
		b.WriteByte('[')
		for i, it := range n.items {
			if i > 0 {
				b.WriteString(", ")
			}
			if err := writeJSONB(b, it); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case jsonString:
		s, err := jsonUnescape(n.raw)
		if err != nil {
			return err
		}
		writeJSONString(b, s)
	case jsonNumber:
		s, err := jsonbNumber(n.raw)
		if err != nil {
			return err
		}
		b.WriteString(s)
	default:
		b.WriteString(n.raw)
	}
	return nil
}

// normalizeJSONB parses a document and prints it as jsonb does.
func normalizeJSONB(s string) (string, error) {
	n, err := parseJSON(s, true)
	if err != nil {
		return "", err
	}
	toJSONB(n)
	var b strings.Builder
	if err := writeJSONB(&b, n); err != nil {
		return "", err
	}
	return b.String(), nil
}

// jsonbNumber renders a JSON number as jsonb stores it, a NUMERIC: exact, at
// the scale written (less the exponent), so 1e2 is 100, 1.50 is 1.50 and
// 1.5e-3 is 0.0015.
func jsonbNumber(raw string) (string, error) {
	mant, exp := raw, int64(0)
	if i := strings.IndexAny(raw, "eE"); i >= 0 {
		e, err := strconv.ParseInt(raw[i+1:], 10, 32)
		if err != nil {
			return "", errNumericOverflow
		}
		mant, exp = raw[:i], e
	}
	neg := strings.HasPrefix(mant, "-")
	intPart, frac, _ := strings.Cut(strings.TrimPrefix(mant, "-"), ".")
	digits := strings.TrimLeft(intPart+frac, "0")
	scale := int64(len(frac)) - exp // the value is digits × 10^-scale
	// Postgres's NUMERIC limits: 131072 digits before the point, 16383 after.
	if max(scale, 0) > 16383 || int64(len(digits))-scale > 131072 {
		return "", errNumericOverflow
	}
	var out string
	switch {
	case digits == "":
		out = "0"
		if scale > 0 {
			out += "." + strings.Repeat("0", int(scale))
		}
		return out, nil
	case scale <= 0:
		out = digits + strings.Repeat("0", int(-scale))
	case int64(len(digits)) > scale:
		cut := len(digits) - int(scale)
		out = digits[:cut] + "." + digits[cut:]
	default:
		out = "0." + strings.Repeat("0", int(scale)-len(digits)) + digits
	}
	if neg {
		out = "-" + out
	}
	return out, nil
}

var errNumericOverflow = errors.New("value overflows numeric format")

// ---- encoding SQL values ----

// writeJSONString writes s as a JSON string, escaped as Postgres does.
func writeJSONString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				fmt.Fprintf(b, `\u%04x`, c)
			} else {
				b.WriteByte(c)
			}
		}
	}
	b.WriteByte('"')
}

// jsonText is the text of a SQL value inside JSON, as in Postgres: ISO 8601
// times (2024-01-15T10:30:00.5+00:00, fractions without trailing zeros; a
// timestamp with time zone in the session time zone z), \x and hex digits
// for binary values, and otherwise the value's text (NaN and Infinity for
// such doubles).
func jsonText(v Value, z tzZone) string {
	switch v.T.Kind {
	case KindTimestamp:
		return formatTimestamp(v.I, v.T.Unit, v.T.TZ != "", v.offsetIn(z), true)
	case KindBinary:
		return `\x` + hex.EncodeToString([]byte(v.S))
	}
	return v.Text()
}

// writeJSONValue writes a SQL value as JSON, as Postgres's to_json does:
// numbers and booleans as themselves, NULL as null, anything else as a
// string (see jsonText). isJSON says the value is JSON already; z is the
// session time zone.
func writeJSONValue(b *strings.Builder, v Value, isJSON bool, z tzZone) {
	k := v.T.Kind
	switch {
	case v.Null:
		b.WriteString("null")
	case isJSON, k == KindBool, k.isInteger(), k == KindDecimal:
		b.WriteString(v.Text())
	case k.isFloat() && !math.IsNaN(v.F) && !math.IsInf(v.F, 0):
		b.WriteString(v.Text())
	default:
		writeJSONString(b, jsonText(v, z))
	}
}

// buildJSONObject implements JSON_BUILD_OBJECT(k, v, …), its JSONB_ spelling
// and JSON_OBJECT(k VALUE v, …).
func buildJSONObject(f *Func, args []Value, z tzZone) (Value, error) {
	c := f.JSON
	if c == nil {
		c = &jsonClauses{}
	}
	var b strings.Builder
	b.WriteByte('{')
	seen := map[string]bool{}
	n := 0
	for i := 0; i+1 < len(args); i += 2 {
		k, v := args[i], args[i+1]
		switch {
		case k.Null:
			return Value{}, errJSONNullKey
		case isJSONExpr(f.Args[i]):
			return Value{}, errJSONKey
		case v.Null && c.absentOnNull:
			continue
		}
		key := jsonText(k, z)
		if c.uniqueKeys {
			if seen[key] {
				var q strings.Builder
				writeJSONString(&q, key)
				return Value{}, fmt.Errorf("duplicate JSON object key value: %s", q.String())
			}
			seen[key] = true
		}
		if n > 0 {
			b.WriteString(", ")
		}
		writeJSONString(&b, key)
		b.WriteString(" : ")
		writeJSONValue(&b, v, isJSONExpr(f.Args[i+1]), z)
		n++
	}
	b.WriteByte('}')
	return jsonResult(f, b.String())
}

// buildJSONArray implements JSON_BUILD_ARRAY(…), its JSONB_ spelling and
// JSON_ARRAY(…).
func buildJSONArray(f *Func, args []Value, z tzZone) (Value, error) {
	var b strings.Builder
	b.WriteByte('[')
	n := 0
	for i, v := range args {
		if v.Null && f.JSON != nil && f.JSON.absentOnNull {
			continue
		}
		if n > 0 {
			b.WriteString(", ")
		}
		writeJSONValue(&b, v, isJSONExpr(f.Args[i]), z)
		n++
	}
	b.WriteByte(']')
	return jsonResult(f, b.String())
}

// jsonObjectFromArrays implements Postgres's json_object(text[]), whose array
// alternates keys and values (or has rows of two), and json_object(keys
// text[], values text[]). Values are strings.
func jsonObjectFromArrays(f *Func, args []Value) (Value, error) {
	a, err := parseTextArray(args[0].Text())
	if err != nil {
		return Value{}, err
	}
	var keys, vals textArray
	if len(args) == 1 {
		switch {
		case len(a.dims) == 1 && a.dims[0]%2 != 0:
			return Value{}, errors.New("array must have even number of elements")
		case len(a.dims) == 2 && a.dims[1] != 2:
			return Value{}, errors.New("array must have two columns")
		case len(a.dims) > 2:
			return Value{}, errors.New("wrong number of array subscripts")
		}
		for i := 0; i+1 < len(a.elems); i += 2 {
			keys.elems, keys.nulls = append(keys.elems, a.elems[i]), append(keys.nulls, a.nulls[i])
			vals.elems, vals.nulls = append(vals.elems, a.elems[i+1]), append(vals.nulls, a.nulls[i+1])
		}
	} else {
		if vals, err = parseTextArray(args[1].Text()); err != nil {
			return Value{}, err
		}
		keys = a
		if len(keys.dims) > 1 || len(vals.dims) > 1 {
			return Value{}, errors.New("wrong number of array subscripts")
		}
		if len(keys.elems) != len(vals.elems) {
			return Value{}, errors.New("mismatched array dimensions")
		}
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys.elems {
		if keys.nulls[i] {
			return Value{}, errJSONNullKey
		}
		if i > 0 {
			b.WriteString(", ")
		}
		writeJSONString(&b, k)
		b.WriteString(" : ")
		if vals.nulls[i] {
			b.WriteString("null")
		} else {
			writeJSONString(&b, vals.elems[i])
		}
	}
	b.WriteByte('}')
	return jsonResult(f, b.String())
}

// isJSON evaluates x IS JSON [VALUE | SCALAR | ARRAY | OBJECT]
// [WITH UNIQUE KEYS]: malformed text is not JSON.
func isJSON(c *jsonClauses, v Value) (Value, error) {
	if v.T.Kind != KindString && v.T.Kind != KindBinary {
		return Value{}, fmt.Errorf("cannot use type %s in IS JSON predicate", v.T.SQLName())
	}
	n, err := parseJSON(v.S, c.uniqueKeys)
	if err != nil {
		return boolValue(false), nil
	}
	ok := true
	switch c.is {
	case "OBJECT":
		ok = n.kind == jsonObject
	case "ARRAY":
		ok = n.kind == jsonArray
	case "SCALAR":
		ok = n.kind != jsonObject && n.kind != jsonArray
	}
	return boolValue(ok && !(c.uniqueKeys && hasDuplicateKeys(n))), nil
}

// hasDuplicateKeys reports whether an object of a strictly parsed document
// has a key twice.
func hasDuplicateKeys(n *jsonNode) bool {
	if n.kind == jsonObject && len(n.keys) > 1 {
		seen := make(map[string]bool, len(n.keys))
		for _, k := range n.keys {
			if seen[k] {
				return true
			}
			seen[k] = true
		}
	}
	return slices.ContainsFunc(n.items, hasDuplicateKeys)
}

// ---- -> ->> #> #>> ----

type jsonStepKind uint8

const (
	stepPath  jsonStepKind = iota // #> / JSON_EXTRACT_PATH: a key, or an array index if it is an integer
	stepKey                       // -> text: objects only
	stepIndex                     // -> integer: arrays only
)

// jsonStep is one step of ->, #> or JSON_EXTRACT_PATH.
type jsonStep struct {
	kind  jsonStepKind
	key   string
	index int64
}

// jsonSteps returns the path a JSON operator or JSON_EXTRACT_PATH follows;
// ok is false if the path has a NULL element (the result is then NULL).
// NULL arguments were handled by the caller.
func jsonSteps(f *Func, args []Value) ([]jsonStep, bool, error) {
	r := args[1]
	switch f.Name {
	case "->", "->>":
		switch {
		case r.T.Kind == KindString:
			return []jsonStep{{kind: stepKey, key: r.S}}, true, nil
		case r.T.Kind.isInteger():
			return []jsonStep{{kind: stepIndex, index: r.I}}, true, nil
		}
		return nil, false, fmt.Errorf("operator does not exist: json %s %s", f.Name, r.T.SQLName())
	case "#>", "#>>":
		if r.T.Kind != KindString {
			return nil, false, fmt.Errorf("operator does not exist: json %s %s", f.Name, r.T.SQLName())
		}
		a, err := parseTextArray(r.S)
		if err != nil {
			return nil, false, err
		}
		steps := make([]jsonStep, len(a.elems))
		for i, e := range a.elems {
			if a.nulls[i] {
				return nil, false, nil
			}
			steps[i] = jsonStep{key: e}
		}
		return steps, true, nil
	}
	steps := make([]jsonStep, len(args)-1)
	for i, a := range args[1:] {
		steps[i] = jsonStep{key: a.Text()}
	}
	return steps, true, nil
}

// jsonFollow returns the value a path leads to, or nil. As in Postgres, an
// array index may be negative (counted from the end), and a path element
// that is not an integer selects no array element.
func jsonFollow(n *jsonNode, steps []jsonStep) *jsonNode {
	for _, s := range steps {
		var next *jsonNode
		switch {
		case n.kind == jsonObject && s.kind != stepIndex:
			next = n.member(s.key)
		case n.kind == jsonArray && s.kind == stepIndex:
			next = n.element(s.index)
		case n.kind == jsonArray && s.kind == stepPath:
			if i, err := strconv.ParseInt(strings.TrimLeft(s.key, " \t\n\r\v\f"), 10, 32); err == nil {
				next = n.element(i)
			}
		}
		if next == nil {
			return nil
		}
		n = next
	}
	return n
}

// jsonExtract implements ->, ->>, #>, #>> and JSON_EXTRACT_PATH[_TEXT]: the
// value at the path, as JSON (its text in the document, or in jsonb form)
// or, asText, as text (a string's value; NULL for null).
func jsonExtract(doc string, steps []jsonStep, asText, jsonb bool) (Value, error) {
	root, err := parseJSON(doc, true)
	if err != nil {
		return Value{}, err
	}
	if jsonb {
		toJSONB(root)
	}
	n := jsonFollow(root, steps)
	switch {
	case n == nil, asText && n.kind == jsonNull:
		return nullValue(typeString), nil
	case asText && n.kind == jsonString:
		s, err := jsonUnescape(n.raw)
		if err != nil {
			return Value{}, err
		}
		return stringValue(s), nil
	case jsonb:
		var b strings.Builder
		if err := writeJSONB(&b, n); err != nil {
			return Value{}, err
		}
		return stringValue(b.String()), nil
	}
	return stringValue(n.raw), nil
}

// ---- text arrays ----

// textArray is a Postgres text array literal: its elements (row by row for
// two dimensions) and the length of each dimension (none if empty).
type textArray struct {
	elems []string
	nulls []bool
	dims  []int
}

// parseTextArray parses a Postgres text array literal such as '{a,"b c",1}':
// the path of #> and #>>, or the arrays of JSON_OBJECT. Unquoted elements
// are trimmed, NULL is a NULL element, and a backslash escapes the next
// character.
func parseTextArray(s string) (textArray, error) {
	ap := &arrayParser{src: s}
	ap.skipSpace()
	if ap.i >= len(s) || s[ap.i] != '{' {
		return textArray{}, ap.fail(`array value must start with "{"`)
	}
	if err := ap.level(0); err != nil {
		return textArray{}, err
	}
	ap.skipSpace()
	if ap.i < len(s) {
		return textArray{}, ap.fail("junk after closing right brace")
	}
	if len(ap.a.elems) == 0 {
		ap.a.dims = nil
	}
	return ap.a, nil
}

type arrayParser struct {
	src string
	i   int
	a   textArray
	// nested records, per depth, whether its items are elements (1) or
	// sub-arrays (2), once the first one has been read.
	nested []int8
}

func (ap *arrayParser) fail(detail string) error {
	return fmt.Errorf(`malformed array literal: "%s" (%s)`, ap.src, detail)
}

func (ap *arrayParser) skipSpace() {
	for ap.i < len(ap.src) && strings.IndexByte(" \t\n\r\v\f", ap.src[ap.i]) >= 0 {
		ap.i++
	}
}

// unexpected is the error for the character at the current position.
func (ap *arrayParser) unexpected() error {
	if ap.i >= len(ap.src) {
		return ap.fail("unexpected end of input")
	}
	return ap.fail(fmt.Sprintf(`unexpected "%c" character`, ap.src[ap.i]))
}

// level parses a {…} at the given depth; sub-arrays of a depth must all
// have the same length.
func (ap *arrayParser) level(depth int) error {
	ap.i++ // {
	if depth == len(ap.a.dims) {
		ap.a.dims = append(ap.a.dims, -1)
		ap.nested = append(ap.nested, 0)
	}
	ap.skipSpace()
	if depth == 0 && ap.i < len(ap.src) && ap.src[ap.i] == '}' {
		ap.i++
		return nil
	}
	count := 0
	for {
		ap.skipSpace()
		kind := int8(1)
		if ap.i < len(ap.src) && ap.src[ap.i] == '{' {
			kind = 2
		}
		if ap.nested[depth] != 0 && ap.nested[depth] != kind {
			return ap.unexpected()
		}
		ap.nested[depth] = kind
		if kind == 2 {
			if err := ap.level(depth + 1); err != nil {
				return err
			}
		} else {
			e, null, err := ap.element()
			if err != nil {
				return err
			}
			ap.a.elems, ap.a.nulls = append(ap.a.elems, e), append(ap.a.nulls, null)
		}
		count++
		ap.skipSpace()
		if ap.i >= len(ap.src) {
			return ap.unexpected()
		}
		switch ap.src[ap.i] {
		case ',':
			ap.i++
			continue
		case '}':
			ap.i++
			if d := ap.a.dims[depth]; d >= 0 && d != count {
				return ap.fail("multidimensional arrays must have sub-arrays with matching dimensions")
			}
			ap.a.dims[depth] = count
			return nil
		}
		return ap.unexpected()
	}
}

// element reads a quoted or unquoted element.
func (ap *arrayParser) element() (string, bool, error) {
	s := ap.src
	var b strings.Builder
	if ap.i < len(s) && s[ap.i] == '"' {
		for ap.i++; ap.i < len(s); ap.i++ {
			switch s[ap.i] {
			case '\\':
				if ap.i++; ap.i < len(s) {
					b.WriteByte(s[ap.i])
				}
			case '"':
				ap.i++
				return b.String(), false, nil
			default:
				b.WriteByte(s[ap.i])
			}
		}
		return "", false, ap.unexpected()
	}
	keep := 0 // length up to the last escaped character, which trimming keeps
	for ap.i < len(s) && strings.IndexByte(",}{\"", s[ap.i]) < 0 {
		if s[ap.i] == '\\' {
			if ap.i++; ap.i >= len(s) {
				return "", false, ap.unexpected()
			}
			b.WriteByte(s[ap.i])
			keep = b.Len()
		} else {
			b.WriteByte(s[ap.i])
		}
		ap.i++
	}
	text := b.String()
	text = text[:keep] + strings.TrimRight(text[keep:], " \t\n\r\v\f")
	switch {
	case text == "" || ap.i < len(s) && (s[ap.i] == '{' || s[ap.i] == '"'):
		return "", false, ap.unexpected()
	case keep == 0 && strings.EqualFold(text, "NULL"):
		return "", true, nil
	}
	return text, false, nil
}

// ---- aggregates ----

// jsonAgg is JSON_AGG / JSON_OBJECT_AGG (and the JSONB_ spellings): the
// rows are kept, then sorted (ORDER BY) and encoded when the group is
// complete. accumulator.addRow applies FILTER and DISTINCT before add.
type jsonAgg struct {
	fn   *Func
	rows []jsonAggRow
}

// jsonAggRow is one input row: the argument values and the ORDER BY keys.
type jsonAggRow struct {
	args, keys []Value
}

// add takes the row's arguments followed by its ORDER BY keys (aggInput).
func (a *jsonAgg) add(in []Value) error {
	n := len(a.fn.Args)
	if n == 2 && in[0].Null {
		return errJSONNullKey
	}
	a.rows = append(a.rows, jsonAggRow{args: slices.Clone(in[:n]), keys: slices.Clone(in[n:])})
	return nil
}

func (a *jsonAgg) result(env *evalEnv, _ ColType) (Value, error) {
	if len(a.rows) == 0 {
		return nullValue(typeString), nil
	}
	f, rows := a.fn, a.rows
	switch {
	case len(f.OrderBy) > 0:
		order := make([]planOrder, len(f.OrderBy))
		for i, o := range f.OrderBy {
			order[i] = planOrder{expr: o.Expr, desc: o.Desc, nullsFirst: o.Nulls == NullsFirst}
		}
		sort.SliceStable(rows, func(i, j int) bool { return lessKeys(rows[i].keys, rows[j].keys, order) })
	case f.Distinct:
		// Postgres finds the distinct values by sorting them.
		order := make([]planOrder, len(f.Args))
		sort.SliceStable(rows, func(i, j int) bool { return lessKeys(rows[i].args, rows[j].args, order) })
	}
	var b strings.Builder
	object := len(f.Args) == 2
	if object {
		b.WriteString("{ ")
	} else {
		b.WriteByte('[')
	}
	for i, r := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		if object {
			writeJSONString(&b, jsonText(r.args[0], env.zone()))
			b.WriteString(" : ")
		}
		writeJSONValue(&b, r.args[len(r.args)-1], isJSONExpr(f.Args[len(f.Args)-1]), env.zone())
	}
	if object {
		b.WriteString(" }")
	} else {
		b.WriteByte(']')
	}
	return jsonResult(f, b.String())
}

// ---- parsing ----

// jsonCast returns the cast of x to JSON or JSONB, which are not column
// types: the cast checks the text (and for JSONB normalizes it) and gives
// VARCHAR. ok is false for other types.
func jsonCast(spec sqlTypeSpec, x Expr) (Expr, bool) {
	switch spec.Name {
	case "JSON":
		return &Func{Name: "__JSON", Args: []Expr{x}}, true
	case "JSONB":
		return &Func{Name: "__JSONB", Args: []Expr{x}}, true
	}
	return nil, false
}

// parseSQLJSONFunc parses what follows JSON_VALUE(, JSON_QUERY(,
// JSON_EXISTS(, JSON_OBJECT( or JSON_ARRAY(.
func (p *parser) parseSQLJSONFunc(name string) (Expr, error) {
	switch name {
	case "JSON_OBJECT":
		return p.parseJSONObject()
	case "JSON_ARRAY":
		return p.parseJSONArray()
	}
	return p.parseJSONQuery(name)
}

// parseIsJSON parses what follows x IS [NOT] JSON:
// [VALUE | SCALAR | ARRAY | OBJECT] [{WITH | WITHOUT} UNIQUE [KEYS]].
func (p *parser) parseIsJSON(x Expr) Expr {
	c := &jsonClauses{returning: typeBool, is: "VALUE"}
	for _, kind := range []string{"VALUE", "SCALAR", "ARRAY", "OBJECT"} {
		if p.acceptKeyword(kind) {
			c.is = kind
			break
		}
	}
	p.parseUniqueKeys(c)
	return &Func{Name: "__IS_JSON", Args: []Expr{x}, JSON: c}
}

// parseUniqueKeys parses an optional {WITH | WITHOUT} UNIQUE [KEYS].
func (p *parser) parseUniqueKeys(c *jsonClauses) {
	switch {
	case p.acceptKeyword("WITH", "UNIQUE"):
		c.uniqueKeys = true
	case p.acceptKeyword("WITHOUT", "UNIQUE"):
	default:
		return
	}
	p.acceptKeyword("KEYS")
}

// acceptFormatJSON accepts FORMAT JSON [ENCODING UTF8].
func (p *parser) acceptFormatJSON() bool {
	if !p.acceptKeyword("FORMAT", "JSON") {
		return false
	}
	p.acceptKeyword("ENCODING", "UTF8")
	return true
}

// parseJSONReturning parses the type after RETURNING (and an optional
// FORMAT JSON).
func (p *parser) parseJSONReturning(c *jsonClauses) error {
	spec, err := p.parseTypeSpec()
	if err != nil {
		return err
	}
	switch spec.Name {
	case "JSON", "JSONB":
		c.returning, c.returnJSON, c.jsonb = typeString, true, spec.Name == "JSONB"
	default:
		t, err := colTypeFromSQL(spec)
		if err != nil {
			return &sqlError{msg: err.Error()}
		}
		c.returning, c.returnJSON, c.jsonb = t, false, false
	}
	p.acceptFormatJSON()
	return nil
}

// parseJSONQuery parses what follows JSON_VALUE(, JSON_QUERY( or
// JSON_EXISTS(:
//
//	doc [FORMAT JSON], path [RETURNING type]
//	[{WITHOUT | WITH [CONDITIONAL | UNCONDITIONAL]} [ARRAY] WRAPPER]   JSON_QUERY
//	[{KEEP | OMIT} QUOTES [ON SCALAR STRING]]                          JSON_QUERY
//	[behavior ON EMPTY] [behavior ON ERROR] )
//
// PASSING is not supported: paths have no variables.
func (p *parser) parseJSONQuery(name string) (Expr, error) {
	doc, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	p.acceptFormatJSON()
	if err := p.expectOp(","); err != nil {
		return nil, err
	}
	path, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.isKeyword("PASSING") {
		return nil, &sqlError{msg: name + ": PASSING is not supported"}
	}
	c := &jsonClauses{returning: typeString}
	f := &Func{Name: name, Args: []Expr{doc, path}, JSON: c}
	switch name {
	case "JSON_EXISTS":
		c.returning, c.onError.kind = typeBool, behaviorFalse
	case "JSON_QUERY":
		c.returnJSON, c.jsonb = true, true
	}
	if name != "JSON_EXISTS" && p.acceptKeyword("RETURNING") {
		if err := p.parseJSONReturning(c); err != nil {
			return nil, err
		}
	}
	if name == "JSON_QUERY" {
		switch {
		case p.acceptKeyword("WITHOUT"):
			p.acceptKeyword("ARRAY")
			if err := p.expectKeyword("WRAPPER"); err != nil {
				return nil, err
			}
		case p.acceptKeyword("WITH"):
			c.wrapper = wrapperUnconditional
			if p.acceptKeyword("CONDITIONAL") {
				c.wrapper = wrapperConditional
			} else {
				p.acceptKeyword("UNCONDITIONAL")
			}
			p.acceptKeyword("ARRAY")
			if err := p.expectKeyword("WRAPPER"); err != nil {
				return nil, err
			}
		}
		keep := p.acceptKeyword("KEEP", "QUOTES")
		if !keep && p.acceptKeyword("OMIT", "QUOTES") {
			c.omitQuotes = true
		}
		if keep || c.omitQuotes {
			p.acceptKeyword("ON", "SCALAR", "STRING")
		}
		if c.omitQuotes && c.wrapper != wrapperNone {
			return nil, &sqlError{msg: "SQL/JSON QUOTES behavior must not be specified when WITH WRAPPER is used"}
		}
	}
	// Which behaviors each function allows ON EMPTY / ON ERROR.
	allowed := map[string][]jsonBehaviorKind{
		"JSON_VALUE":  {behaviorError, behaviorNull, behaviorDefault},
		"JSON_QUERY":  {behaviorError, behaviorNull, behaviorEmptyArray, behaviorEmptyObject, behaviorDefault},
		"JSON_EXISTS": {behaviorError, behaviorTrue, behaviorFalse, behaviorUnknown},
	}[name]
	onEmpty, onError := false, false
	for !p.isOp(")") {
		b, err := p.parseJSONBehavior(f)
		if err != nil {
			return nil, err
		}
		var on string
		switch {
		case !onEmpty && !onError && name != "JSON_EXISTS" && p.acceptKeyword("ON", "EMPTY"):
			onEmpty, on, c.onEmpty = true, "EMPTY", b
		case !onError && p.acceptKeyword("ON", "ERROR"):
			onError, on, c.onError = true, "ERROR", b
		default:
			return nil, syntaxErr("expected ON EMPTY or ON ERROR near %q", p.peek().text)
		}
		if !slices.Contains(allowed, b.kind) {
			return nil, &sqlError{msg: fmt.Sprintf("invalid ON %s behavior for %s", on, name)}
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return f, nil
}

// parseJSONBehavior parses ERROR | NULL | DEFAULT expr | EMPTY [ARRAY] |
// EMPTY OBJECT | TRUE | FALSE | UNKNOWN. A DEFAULT expression is added to
// f's arguments.
func (p *parser) parseJSONBehavior(f *Func) (jsonBehavior, error) {
	switch {
	case p.acceptKeyword("ERROR"):
		return jsonBehavior{kind: behaviorError}, nil
	case p.acceptKeyword("NULL"):
		return jsonBehavior{kind: behaviorNull}, nil
	case p.acceptKeyword("TRUE"):
		return jsonBehavior{kind: behaviorTrue}, nil
	case p.acceptKeyword("FALSE"):
		return jsonBehavior{kind: behaviorFalse}, nil
	case p.acceptKeyword("UNKNOWN"):
		return jsonBehavior{kind: behaviorUnknown}, nil
	case p.acceptKeyword("EMPTY"):
		if p.acceptKeyword("OBJECT") {
			return jsonBehavior{kind: behaviorEmptyObject}, nil
		}
		p.acceptKeyword("ARRAY")
		return jsonBehavior{kind: behaviorEmptyArray}, nil
	case p.acceptKeyword("DEFAULT"):
		x, err := p.parseExpr()
		if err != nil {
			return jsonBehavior{}, err
		}
		f.Args = append(f.Args, x)
		return jsonBehavior{kind: behaviorDefault, arg: len(f.Args) - 1}, nil
	}
	return jsonBehavior{}, syntaxErr("expected ON EMPTY or ON ERROR near %q", p.peek().text)
}

// isJSONConstructorClause reports whether a clause of JSON_OBJECT or
// JSON_ARRAY follows.
func (p *parser) isJSONConstructorClause() bool {
	return (p.isKeyword("NULL") || p.isKeyword("ABSENT")) && p.isKeywordAt(1, "ON") ||
		(p.isKeyword("WITH") || p.isKeyword("WITHOUT")) && p.isKeywordAt(1, "UNIQUE") ||
		p.isKeyword("RETURNING")
}

// parseJSONConstructorReturning parses the optional RETURNING clause of
// JSON_OBJECT and JSON_ARRAY, which return JSON or text.
func (p *parser) parseJSONConstructorReturning(name string, c *jsonClauses) error {
	if !p.acceptKeyword("RETURNING") {
		return nil
	}
	if err := p.parseJSONReturning(c); err != nil {
		return err
	}
	if !c.returnJSON && c.returning.Kind != KindString {
		return &sqlError{msg: fmt.Sprintf("cannot use RETURNING type %s in %s", c.returning.SQLName(), name)}
	}
	return nil
}

// parseJSONValueExpr parses a constructor's value: expr [FORMAT JSON]. With
// FORMAT JSON, text is embedded as JSON.
func (p *parser) parseJSONValueExpr() (Expr, error) {
	x, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.acceptFormatJSON() {
		x = &Func{Name: "__JSON", Args: []Expr{x}}
	}
	return x, nil
}

// parseJSONObject parses what follows JSON_OBJECT(: the SQL/JSON form
//
//	[key {VALUE | :} value [FORMAT JSON], …] [{NULL | ABSENT} ON NULL]
//	[{WITH | WITHOUT} UNIQUE [KEYS]] [RETURNING type] )
//
// or Postgres's json_object(text[]) / json_object(keys, values), whose
// arrays are text array literals such as '{a,1,b,2}'.
func (p *parser) parseJSONObject() (Expr, error) {
	c := &jsonClauses{returning: typeString, returnJSON: true}
	f := &Func{Name: "__JSON_OBJECT", JSON: c}
	if !p.isOp(")") && !p.isJSONConstructorClause() {
		for {
			k, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if len(f.Args) == 0 && (p.isOp(",") || p.isOp(")")) {
				return p.finishCall("JSON_OBJECT", k)
			}
			if !p.acceptKeyword("VALUE") && !p.acceptOp(":") {
				return nil, syntaxErr("expected VALUE or : after a JSON_OBJECT key near %q", p.peek().text)
			}
			v, err := p.parseJSONValueExpr()
			if err != nil {
				return nil, err
			}
			f.Args = append(f.Args, k, v)
			if !p.acceptOp(",") {
				break
			}
		}
	}
	if p.acceptKeyword("ABSENT", "ON", "NULL") {
		c.absentOnNull = true
	} else {
		p.acceptKeyword("NULL", "ON", "NULL")
	}
	p.parseUniqueKeys(c)
	if err := p.parseJSONConstructorReturning("JSON_OBJECT", c); err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return f, nil
}

// parseJSONArray parses what follows JSON_ARRAY(:
//
//	[value [FORMAT JSON], …] [{NULL | ABSENT} ON NULL] [RETURNING type] )
func (p *parser) parseJSONArray() (Expr, error) {
	if p.isQueryStart() {
		return nil, &sqlError{msg: "JSON_ARRAY(SELECT …) is not supported; use JSON_AGG in a subquery"}
	}
	c := &jsonClauses{returning: typeString, returnJSON: true, absentOnNull: true}
	f := &Func{Name: "__JSON_ARRAY", JSON: c}
	if !p.isOp(")") && !p.isJSONConstructorClause() {
		for {
			v, err := p.parseJSONValueExpr()
			if err != nil {
				return nil, err
			}
			f.Args = append(f.Args, v)
			if !p.acceptOp(",") {
				break
			}
		}
	}
	if p.acceptKeyword("NULL", "ON", "NULL") {
		c.absentOnNull = false
	} else {
		p.acceptKeyword("ABSENT", "ON", "NULL")
	}
	if err := p.parseJSONConstructorReturning("JSON_ARRAY", c); err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return f, nil
}
