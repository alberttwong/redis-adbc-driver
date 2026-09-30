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
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ---- AST ----

type Expr interface{ exprNode() }

type Literal struct{ V Value }

// ColumnRef is a column reference, optionally qualified (alias.column).
// Binding resolves Name to the canonical column name. Outer > 0 marks a
// correlated reference to a column of an enclosing query, Outer levels up;
// OuterType is then that column's type.
type ColumnRef struct {
	Name      string
	Qualifier string
	Outer     int
	OuterType ColType
}

// Subquery is a SELECT used as an expression: a scalar subquery
// `(SELECT …)`, `EXISTS (SELECT …)`, or `X [NOT] IN (SELECT …)`.
type Subquery struct {
	Select *SelectStmt
	Kind   SubqueryKind
	X      Expr // IN operand
	Not    bool // NOT IN

	// Set while binding.
	plan       *selectPlan
	correlated bool
	// outerRefs are the outer columns the body reads, relative to the
	// environment that evaluates the subquery (used as the memo key).
	outerRefs []outerRef
}

type SubqueryKind int

const (
	SubqueryScalar SubqueryKind = iota
	SubqueryExists
	SubqueryIn
)

type outerRef struct {
	name string
	up   int
}
type Param struct{ Index int }
type Unary struct {
	Op string
	X  Expr
}
type Binary struct {
	Op   string
	L, R Expr
}
type IsNull struct {
	X   Expr
	Not bool
}
type Cast struct {
	X Expr
	T ColType
}

// Case is CASE [operand] WHEN … THEN … [ELSE …] END. With an operand, each
// WHEN value is compared to it with =; without one, each WHEN is a condition.
type Case struct {
	Operand Expr
	Whens   []WhenClause
	Else    Expr
}

type WhenClause struct {
	When Expr
	Then Expr
}

type Func struct {
	Name     string // upper-cased
	Args     []Expr
	Star     bool
	Distinct bool
}

func (*Literal) exprNode()   {}
func (*ColumnRef) exprNode() {}
func (*Param) exprNode()     {}
func (*Unary) exprNode()     {}
func (*Binary) exprNode()    {}
func (*IsNull) exprNode()    {}
func (*Cast) exprNode()      {}
func (*Func) exprNode()      {}
func (*Case) exprNode()      {}
func (*Subquery) exprNode()  {}

type TableName struct {
	Catalog string
	Schema  string
	Name    string
}

func (t TableName) String() string {
	parts := []string{}
	for _, p := range []string{t.Catalog, t.Schema, t.Name} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, ".")
}

type Stmt interface{ stmtNode() }

type SelectItem struct {
	Expr  Expr
	Alias string
	Star  bool
	Text  string
}

type OrderItem struct {
	Expr Expr
	Desc bool
}

// CTE is one `name [(columns)] AS (SELECT …)` of a WITH clause.
type CTE struct {
	Name    string
	Columns []string
	Select  *SelectStmt
}

// JoinClause is one `[kind] JOIN item [ON … | USING (…)]` (or `, item`) of
// a FROM clause. Kind is INNER, LEFT, RIGHT, FULL or CROSS.
type JoinClause struct {
	Kind   string
	Table  *TableName
	Select *SelectStmt
	Alias  string
	On     Expr
	Using  []string
}

type SelectStmt struct {
	With  []CTE
	Items []SelectItem
	// Joins are the FROM items after the first.
	Joins []JoinClause
	// FROM is a table (From) or a derived table (FromSelect); FromAlias is
	// the optional alias.
	From       *TableName
	FromSelect *SelectStmt
	FromAlias  string
	Where      Expr
	GroupBy    []Expr
	Having     Expr
	OrderBy    []OrderItem
	Limit      *int64
	Offset     *int64
}

type InsertStmt struct {
	Table   TableName
	Columns []string
	Rows    [][]Expr
	// Select is set for INSERT INTO … SELECT.
	Select *SelectStmt
}

type ColumnDef struct {
	Name    string
	Type    ColType
	NotNull bool
	// NoIndex keeps the column out of the RediSearch index (it is still
	// stored in the row HASH).
	NoIndex bool
}

type CreateTableStmt struct {
	Table       TableName
	IfNotExists bool
	Columns     []ColumnDef
	// AsSelect is set for CREATE TABLE … AS SELECT.
	AsSelect *SelectStmt
}

type DropTableStmt struct {
	Table    TableName
	IfExists bool
}

type SetClause struct {
	Column string
	Expr   Expr
}

type UpdateStmt struct {
	Table TableName
	Sets  []SetClause
	Where Expr
}

type DeleteStmt struct {
	Table TableName
	Where Expr
}

type CreateSchemaStmt struct {
	Name        string
	IfNotExists bool
}

type DropSchemaStmt struct {
	Name     string
	IfExists bool
}

func (*SelectStmt) stmtNode()       {}
func (*InsertStmt) stmtNode()       {}
func (*CreateTableStmt) stmtNode()  {}
func (*DropTableStmt) stmtNode()    {}
func (*UpdateStmt) stmtNode()       {}
func (*DeleteStmt) stmtNode()       {}
func (*CreateSchemaStmt) stmtNode() {}
func (*DropSchemaStmt) stmtNode()   {}
func (*CreateViewStmt) stmtNode()   {}
func (*DropViewStmt) stmtNode()     {}

// CreateViewStmt is CREATE [OR REPLACE] VIEW [IF NOT EXISTS] v [(cols)] AS
// SELECT …; Text is the SELECT's source text, which is what gets stored.
type CreateViewStmt struct {
	Name        TableName
	Columns     []string
	Select      *SelectStmt
	Text        string
	OrReplace   bool
	IfNotExists bool
}

type DropViewStmt struct {
	Name     TableName
	IfExists bool
}

// ParsedStmt is a statement with the number of parameters it references.
type ParsedStmt struct {
	Stmt      Stmt
	NumParams int
	Text      string
}

// ---- lexer ----

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokQuotedIdent
	tokNumber
	tokString
	tokHex
	tokOp
	tokParam
)

type token struct {
	kind tokKind
	text string // identifier/keyword text, literal contents, or operator
	pos  int    // byte offset of the token start
	end  int    // byte offset just past the token
}

type sqlError struct{ msg string }

func (e *sqlError) Error() string { return e.msg }

func syntaxErr(format string, args ...any) error {
	return &sqlError{msg: "syntax error: " + fmt.Sprintf(format, args...)}
}

func lex(src string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < len(src) && src[i+1] == '-':
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(src) && src[i+1] == '*':
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				return nil, syntaxErr("unterminated comment")
			}
			i += end + 4
		case (c == 'X' || c == 'x') && i+1 < len(src) && src[i+1] == '\'':
			s, n, err := lexQuoted(src[i+1:], '\'')
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{kind: tokHex, text: s, pos: i, end: i + 1 + n})
			i += 1 + n
		case (c == 'E' || c == 'e' || c == 'N' || c == 'n') && i+1 < len(src) && src[i+1] == '\'':
			// E'..' / N'..' prefixed strings; treated as plain strings.
			s, n, err := lexQuoted(src[i+1:], '\'')
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{kind: tokString, text: s, pos: i, end: i + 1 + n})
			i += 1 + n
		case c == '\'':
			s, n, err := lexQuoted(src[i:], '\'')
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{kind: tokString, text: s, pos: i, end: i + n})
			i += n
		case c == '"' || c == '`':
			s, n, err := lexQuoted(src[i:], c)
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{kind: tokQuotedIdent, text: s, pos: i, end: i + n})
			i += n
		case c == '[':
			end := strings.IndexByte(src[i:], ']')
			if end < 0 {
				return nil, syntaxErr("unterminated identifier")
			}
			toks = append(toks, token{kind: tokQuotedIdent, text: src[i+1 : i+end], pos: i, end: i + end + 1})
			i += end + 1
		case c >= '0' && c <= '9' || (c == '.' && i+1 < len(src) && src[i+1] >= '0' && src[i+1] <= '9'):
			start := i
			for i < len(src) && (src[i] >= '0' && src[i] <= '9' || src[i] == '.') {
				i++
			}
			if i < len(src) && (src[i] == 'e' || src[i] == 'E') {
				j := i + 1
				if j < len(src) && (src[j] == '+' || src[j] == '-') {
					j++
				}
				if j < len(src) && src[j] >= '0' && src[j] <= '9' {
					i = j
					for i < len(src) && src[i] >= '0' && src[i] <= '9' {
						i++
					}
				}
			}
			toks = append(toks, token{kind: tokNumber, text: src[start:i], pos: start, end: i})
		case c == '?':
			toks = append(toks, token{kind: tokParam, text: "", pos: i, end: i + 1})
			i++
		case c == '$' && i+1 < len(src) && src[i+1] >= '0' && src[i+1] <= '9':
			start := i
			i++
			for i < len(src) && src[i] >= '0' && src[i] <= '9' {
				i++
			}
			toks = append(toks, token{kind: tokParam, text: src[start+1 : i], pos: start, end: i})
		case isIdentStart(src[i:]):
			start := i
			for i < len(src) {
				r, size := utf8.DecodeRuneInString(src[i:])
				if r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r) {
					i += size
					continue
				}
				break
			}
			toks = append(toks, token{kind: tokIdent, text: src[start:i], pos: start, end: i})
		default:
			ops := []string{"<>", "!=", "<=", ">=", "||", "::", "==", "(", ")", ",", ";", "*", "+", "-", "/", "%", "=", "<", ">", "."}
			matched := false
			for _, op := range ops {
				if strings.HasPrefix(src[i:], op) {
					toks = append(toks, token{kind: tokOp, text: op, pos: i, end: i + len(op)})
					i += len(op)
					matched = true
					break
				}
			}
			if !matched {
				return nil, syntaxErr("unexpected character %q at offset %d", src[i], i)
			}
		}
	}
	toks = append(toks, token{kind: tokEOF, pos: len(src), end: len(src)})
	return toks, nil
}

func isIdentStart(s string) bool {
	r, _ := utf8.DecodeRuneInString(s)
	return r == '_' || unicode.IsLetter(r)
}

// lexQuoted reads a quoted token starting at s[0] (the quote character), with
// doubled quotes as escapes. It returns the unescaped contents and the number
// of bytes consumed.
func lexQuoted(s string, q byte) (string, int, error) {
	var b strings.Builder
	i := 1
	for i < len(s) {
		if s[i] == q {
			if i+1 < len(s) && s[i+1] == q {
				b.WriteByte(q)
				i += 2
				continue
			}
			return b.String(), i + 1, nil
		}
		b.WriteByte(s[i])
		i++
	}
	return "", 0, syntaxErr("unterminated quoted string")
}

// ---- parser ----

type parser struct {
	src       string
	toks      []token
	pos       int
	numParams int
	nextParam int
}

// ParseScript parses one or more ';'-separated statements.
func ParseScript(src string) ([]ParsedStmt, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{src: src, toks: toks}
	var out []ParsedStmt
	for {
		for p.isOp(";") {
			p.pos++
		}
		if p.peek().kind == tokEOF {
			break
		}
		p.numParams, p.nextParam = 0, 0
		start := p.peek().pos
		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		end := p.peek().pos
		if !p.isOp(";") && p.peek().kind != tokEOF {
			return nil, syntaxErr("unexpected %q", p.peek().text)
		}
		out = append(out, ParsedStmt{Stmt: stmt, NumParams: p.numParams, Text: strings.TrimSpace(src[start:end])})
	}
	if len(out) == 0 {
		return nil, syntaxErr("empty query")
	}
	return out, nil
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) peekAt(n int) token {
	if p.pos+n < len(p.toks) {
		return p.toks[p.pos+n]
	}
	return p.toks[len(p.toks)-1]
}
func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}
	return t
}

func (p *parser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tokIdent && strings.EqualFold(t.text, kw)
}

func (p *parser) isKeywordAt(n int, kw string) bool {
	t := p.peekAt(n)
	return t.kind == tokIdent && strings.EqualFold(t.text, kw)
}

func (p *parser) acceptKeyword(kws ...string) bool {
	for i, kw := range kws {
		if !p.isKeywordAt(i, kw) {
			return false
		}
	}
	p.pos += len(kws)
	return true
}

func (p *parser) expectKeyword(kws ...string) error {
	if !p.acceptKeyword(kws...) {
		return syntaxErr("expected %s near %q", strings.Join(kws, " "), p.peek().text)
	}
	return nil
}

func (p *parser) isOp(op string) bool {
	t := p.peek()
	return t.kind == tokOp && t.text == op
}

func (p *parser) acceptOp(op string) bool {
	if p.isOp(op) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expectOp(op string) error {
	if !p.acceptOp(op) {
		return syntaxErr("expected %q near %q", op, p.peek().text)
	}
	return nil
}

func (p *parser) parseIdent() (string, error) {
	t := p.peek()
	if t.kind == tokIdent || t.kind == tokQuotedIdent {
		p.pos++
		return t.text, nil
	}
	return "", syntaxErr("expected identifier near %q", t.text)
}

func (p *parser) parseTableName() (TableName, error) {
	var parts []string
	for {
		id, err := p.parseIdent()
		if err != nil {
			return TableName{}, err
		}
		parts = append(parts, id)
		if !p.acceptOp(".") {
			break
		}
	}
	switch len(parts) {
	case 1:
		return TableName{Name: parts[0]}, nil
	case 2:
		return TableName{Schema: parts[0], Name: parts[1]}, nil
	case 3:
		return TableName{Catalog: parts[0], Schema: parts[1], Name: parts[2]}, nil
	}
	return TableName{}, syntaxErr("invalid table name %s", strings.Join(parts, "."))
}

func (p *parser) parseStatement() (Stmt, error) {
	switch {
	case p.isKeyword("SELECT"), p.isKeyword("WITH"):
		return p.parseSelect()
	case p.isKeyword("INSERT"):
		return p.parseInsert()
	case p.isKeyword("CREATE"):
		return p.parseCreate()
	case p.isKeyword("DROP"):
		return p.parseDrop()
	case p.isKeyword("UPDATE"):
		return p.parseUpdate()
	case p.isKeyword("DELETE"):
		return p.parseDelete()
	}
	return nil, &sqlError{msg: fmt.Sprintf("unsupported statement starting with %q", p.peek().text)}
}

var reservedAfterExpr = map[string]bool{
	"FROM": true, "WHERE": true, "ORDER": true, "GROUP": true, "LIMIT": true,
	"OFFSET": true, "AND": true, "OR": true, "NOT": true, "AS": true, "IS": true,
	"ASC": true, "DESC": true, "HAVING": true, "UNION": true, "NULLS": true,
	"LIKE": true, "IN": true, "BETWEEN": true, "SET": true, "VALUES": true,
	"WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "FULL": true,
	"CROSS": true, "OUTER": true, "ON": true, "USING": true, "NATURAL": true,
}

// isQueryStart reports whether the next token begins a (sub)query.
func (p *parser) isQueryStart() bool { return p.isKeyword("SELECT") || p.isKeyword("WITH") }

// parseFromItem parses `table [[AS] alias]` or `(SELECT …) [AS] alias`.
func (p *parser) parseFromItem() (*TableName, *SelectStmt, string, error) {
	var table *TableName
	var sub *SelectStmt
	if p.isOp("(") && (p.isKeywordAt(1, "SELECT") || p.isKeywordAt(1, "WITH")) {
		p.pos++
		s, err := p.parseSubquery()
		if err != nil {
			return nil, nil, "", err
		}
		sub = s
	} else {
		t, err := p.parseTableName()
		if err != nil {
			return nil, nil, "", err
		}
		table = &t
	}
	alias := ""
	if p.acceptKeyword("AS") {
		a, err := p.parseIdent()
		if err != nil {
			return nil, nil, "", err
		}
		alias = a
	} else if t := p.peek(); t.kind == tokQuotedIdent || (t.kind == tokIdent && !reservedAfterExpr[strings.ToUpper(t.text)]) {
		p.pos++
		alias = t.text
	}
	return table, sub, alias, nil
}

// parseSubquery parses `SELECT … )` after an opening parenthesis.
func (p *parser) parseSubquery() (*SelectStmt, error) {
	st, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return st.(*SelectStmt), nil
}

func (p *parser) parseSelect() (Stmt, error) {
	var with []CTE
	if p.acceptKeyword("WITH") {
		if p.acceptKeyword("RECURSIVE") {
			return nil, &sqlError{msg: "WITH RECURSIVE is not supported"}
		}
		for {
			name, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			cte := CTE{Name: name}
			if p.acceptOp("(") {
				for {
					c, err := p.parseIdent()
					if err != nil {
						return nil, err
					}
					cte.Columns = append(cte.Columns, c)
					if !p.acceptOp(",") {
						break
					}
				}
				if err := p.expectOp(")"); err != nil {
					return nil, err
				}
			}
			if err := p.expectKeyword("AS"); err != nil {
				return nil, err
			}
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			body, err := p.parseSubquery()
			if err != nil {
				return nil, err
			}
			cte.Select = body
			with = append(with, cte)
			if !p.acceptOp(",") {
				break
			}
		}
	}
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &SelectStmt{With: with}
	p.acceptKeyword("ALL")
	for {
		start := p.peek().pos
		if p.acceptOp("*") {
			sel.Items = append(sel.Items, SelectItem{Star: true, Text: "*"})
		} else {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			item := SelectItem{Expr: e, Text: strings.TrimSpace(p.src[start:p.toks[p.pos-1].end])}
			if p.acceptKeyword("AS") {
				alias, err := p.parseIdent()
				if err != nil {
					return nil, err
				}
				item.Alias = alias
			} else if t := p.peek(); t.kind == tokQuotedIdent || (t.kind == tokIdent && !reservedAfterExpr[strings.ToUpper(t.text)]) {
				p.pos++
				item.Alias = t.text
			}
			sel.Items = append(sel.Items, item)
		}
		if !p.acceptOp(",") {
			break
		}
	}
	if p.acceptKeyword("FROM") {
		table, sub, alias, err := p.parseFromItem()
		if err != nil {
			return nil, err
		}
		sel.From, sel.FromSelect, sel.FromAlias = table, sub, alias
		for {
			kind := ""
			switch {
			case p.acceptOp(","):
				kind = "CROSS"
			case p.acceptKeyword("CROSS", "JOIN"):
				kind = "CROSS"
			case p.acceptKeyword("INNER", "JOIN"), p.acceptKeyword("JOIN"):
				kind = "INNER"
			case p.acceptKeyword("LEFT", "OUTER", "JOIN"), p.acceptKeyword("LEFT", "JOIN"):
				kind = "LEFT"
			case p.acceptKeyword("RIGHT", "OUTER", "JOIN"), p.acceptKeyword("RIGHT", "JOIN"):
				kind = "RIGHT"
			case p.acceptKeyword("FULL", "OUTER", "JOIN"), p.acceptKeyword("FULL", "JOIN"):
				kind = "FULL"
			case p.isKeyword("NATURAL"):
				return nil, &sqlError{msg: "NATURAL JOIN is not supported; use USING or ON"}
			}
			if kind == "" {
				break
			}
			jc := JoinClause{Kind: kind}
			if jc.Table, jc.Select, jc.Alias, err = p.parseFromItem(); err != nil {
				return nil, err
			}
			if kind != "CROSS" {
				switch {
				case p.acceptKeyword("ON"):
					if jc.On, err = p.parseExpr(); err != nil {
						return nil, err
					}
				case p.acceptKeyword("USING"):
					if err := p.expectOp("("); err != nil {
						return nil, err
					}
					for {
						c, err := p.parseIdent()
						if err != nil {
							return nil, err
						}
						jc.Using = append(jc.Using, c)
						if !p.acceptOp(",") {
							break
						}
					}
					if err := p.expectOp(")"); err != nil {
						return nil, err
					}
				default:
					return nil, syntaxErr("%s JOIN requires ON or USING", kind)
				}
			}
			sel.Joins = append(sel.Joins, jc)
		}
	}
	if p.acceptKeyword("WHERE") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Where = e
	}
	if p.acceptKeyword("GROUP", "BY") {
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			sel.GroupBy = append(sel.GroupBy, e)
			if !p.acceptOp(",") {
				break
			}
		}
	}
	if p.acceptKeyword("HAVING") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Having = e
	}
	if p.acceptKeyword("ORDER", "BY") {
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			item := OrderItem{Expr: e}
			if p.acceptKeyword("DESC") {
				item.Desc = true
			} else {
				p.acceptKeyword("ASC")
			}
			if p.acceptKeyword("NULLS") {
				if !p.acceptKeyword("FIRST") && !p.acceptKeyword("LAST") {
					return nil, syntaxErr("expected FIRST or LAST after NULLS")
				}
			}
			sel.OrderBy = append(sel.OrderBy, item)
			if !p.acceptOp(",") {
				break
			}
		}
	}
	for {
		if p.acceptKeyword("LIMIT") {
			n, err := p.parseCount()
			if err != nil {
				return nil, err
			}
			sel.Limit = &n
			if p.acceptOp(",") { // MySQL-style LIMIT offset, count
				m, err := p.parseCount()
				if err != nil {
					return nil, err
				}
				sel.Offset = &n
				sel.Limit = &m
			}
			continue
		}
		if p.acceptKeyword("OFFSET") {
			n, err := p.parseCount()
			if err != nil {
				return nil, err
			}
			sel.Offset = &n
			p.acceptKeyword("ROWS")
			continue
		}
		break
	}
	return sel, nil
}

func (p *parser) parseCount() (int64, error) {
	t := p.next()
	if t.kind != tokNumber {
		return 0, syntaxErr("expected a number near %q", t.text)
	}
	n, err := strconv.ParseInt(t.text, 10, 64)
	if err != nil || n < 0 {
		return 0, syntaxErr("invalid count %q", t.text)
	}
	return n, nil
}

func (p *parser) parseInsert() (Stmt, error) {
	if err := p.expectKeyword("INSERT", "INTO"); err != nil {
		return nil, err
	}
	t, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	ins := &InsertStmt{Table: t}
	if p.acceptOp("(") {
		for {
			c, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			ins.Columns = append(ins.Columns, c)
			if !p.acceptOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
	}
	if p.isQueryStart() {
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		ins.Select = sel.(*SelectStmt)
		return ins, nil
	}
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	for {
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		var row []Expr
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			row = append(row, e)
			if !p.acceptOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		ins.Rows = append(ins.Rows, row)
		if !p.acceptOp(",") {
			break
		}
	}
	return ins, nil
}

func (p *parser) parseCreate() (Stmt, error) {
	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}
	if p.acceptKeyword("SCHEMA") {
		st := &CreateSchemaStmt{}
		st.IfNotExists = p.acceptKeyword("IF", "NOT", "EXISTS")
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		st.Name = name
		return st, nil
	}
	orReplace := p.acceptKeyword("OR", "REPLACE")
	if p.acceptKeyword("TEMPORARY") || p.acceptKeyword("TEMP") {
		return nil, &sqlError{msg: "temporary tables and views are not supported"}
	}
	if p.acceptKeyword("VIEW") {
		st := &CreateViewStmt{OrReplace: orReplace}
		st.IfNotExists = p.acceptKeyword("IF", "NOT", "EXISTS")
		name, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		st.Name = name
		if p.acceptOp("(") {
			for {
				c, err := p.parseIdent()
				if err != nil {
					return nil, err
				}
				st.Columns = append(st.Columns, c)
				if !p.acceptOp(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
		}
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}
		start := p.peek().pos
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		st.Select = sel.(*SelectStmt)
		st.Text = strings.TrimSpace(p.src[start:p.toks[p.pos-1].end])
		return st, nil
	}
	if orReplace {
		return nil, syntaxErr("OR REPLACE is only supported for views")
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	st := &CreateTableStmt{}
	st.IfNotExists = p.acceptKeyword("IF", "NOT", "EXISTS")
	t, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	st.Table = t
	if p.acceptKeyword("AS") {
		parens := p.acceptOp("(")
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		if parens {
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
		}
		st.AsSelect = sel.(*SelectStmt)
		return st, nil
	}
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	for {
		// Table-level constraints are accepted and ignored.
		if p.isKeyword("PRIMARY") || p.isKeyword("UNIQUE") || p.isKeyword("CONSTRAINT") ||
			p.isKeyword("FOREIGN") || p.isKeyword("CHECK") {
			if err := p.skipBalanced(); err != nil {
				return nil, err
			}
		} else {
			name, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			spec, err := p.parseTypeSpec()
			if err != nil {
				return nil, err
			}
			ct, err := colTypeFromSQL(spec)
			if err != nil {
				return nil, &sqlError{msg: err.Error()}
			}
			col := ColumnDef{Name: name, Type: ct}
			// Column constraints.
			for {
				switch {
				case p.acceptKeyword("NOT", "NULL"):
					col.NotNull = true
				case p.acceptKeyword("NULL"):
				case p.acceptKeyword("PRIMARY", "KEY"):
				case p.acceptKeyword("UNIQUE"):
				case p.acceptKeyword("NOINDEX"):
					col.NoIndex = true
				case p.acceptKeyword("INDEX"):
				case p.acceptKeyword("DEFAULT"):
					if _, err := p.parseExpr(); err != nil {
						return nil, err
					}
				default:
					goto done
				}
			}
		done:
			st.Columns = append(st.Columns, col)
		}
		if !p.acceptOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return st, nil
}

// skipBalanced skips tokens up to the next top-level ',' or ')'.
func (p *parser) skipBalanced() error {
	depth := 0
	for {
		t := p.peek()
		switch {
		case t.kind == tokEOF:
			return syntaxErr("unexpected end of input")
		case t.kind == tokOp && t.text == "(":
			depth++
		case t.kind == tokOp && t.text == ")":
			if depth == 0 {
				return nil
			}
			depth--
		case t.kind == tokOp && t.text == "," && depth == 0:
			return nil
		}
		p.pos++
	}
}

func (p *parser) parseDrop() (Stmt, error) {
	if err := p.expectKeyword("DROP"); err != nil {
		return nil, err
	}
	if p.acceptKeyword("VIEW") {
		st := &DropViewStmt{}
		st.IfExists = p.acceptKeyword("IF", "EXISTS")
		name, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		st.Name = name
		return st, nil
	}
	if p.acceptKeyword("SCHEMA") {
		st := &DropSchemaStmt{}
		st.IfExists = p.acceptKeyword("IF", "EXISTS")
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		st.Name = name
		return st, nil
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	st := &DropTableStmt{}
	st.IfExists = p.acceptKeyword("IF", "EXISTS")
	t, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	st.Table = t
	return st, nil
}

func (p *parser) parseUpdate() (Stmt, error) {
	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}
	t, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	st := &UpdateStmt{Table: t}
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	for {
		col, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp("="); err != nil {
			return nil, err
		}
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		st.Sets = append(st.Sets, SetClause{Column: col, Expr: e})
		if !p.acceptOp(",") {
			break
		}
	}
	if p.acceptKeyword("WHERE") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		st.Where = e
	}
	return st, nil
}

func (p *parser) parseDelete() (Stmt, error) {
	if err := p.expectKeyword("DELETE", "FROM"); err != nil {
		return nil, err
	}
	t, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	st := &DeleteStmt{Table: t}
	if p.acceptKeyword("WHERE") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		st.Where = e
	}
	return st, nil
}

// parseTypeSpec parses a SQL type such as DOUBLE PRECISION, VARCHAR(10),
// NUMERIC(10, 2) or TIMESTAMP(3) WITH TIME ZONE.
func (p *parser) parseTypeSpec() (sqlTypeSpec, error) {
	first, err := p.parseIdent()
	if err != nil {
		return sqlTypeSpec{}, err
	}
	spec := sqlTypeSpec{Name: strings.ToUpper(first)}
	switch spec.Name {
	case "DOUBLE":
		if p.acceptKeyword("PRECISION") {
			spec.Name = "DOUBLE PRECISION"
		}
	case "CHARACTER", "CHAR", "BINARY":
		if p.acceptKeyword("VARYING") {
			spec.Name += " VARYING"
			if spec.Name == "CHAR VARYING" {
				spec.Name = "CHARACTER VARYING"
			}
		}
	}
	if p.acceptOp("(") {
		for {
			t := p.next()
			if t.kind == tokIdent && strings.EqualFold(t.text, "MAX") {
				spec.Params = append(spec.Params, -1)
			} else {
				neg := false
				if t.kind == tokOp && t.text == "-" {
					neg = true
					t = p.next()
				}
				if t.kind != tokNumber {
					return sqlTypeSpec{}, syntaxErr("expected type parameter near %q", t.text)
				}
				n, err := strconv.Atoi(t.text)
				if err != nil {
					return sqlTypeSpec{}, syntaxErr("invalid type parameter %q", t.text)
				}
				if neg {
					n = -n
				}
				spec.Params = append(spec.Params, n)
			}
			if !p.acceptOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return sqlTypeSpec{}, err
		}
	}
	if spec.Name == "TIMESTAMP" || spec.Name == "TIME" {
		if p.acceptKeyword("WITH", "TIME", "ZONE") {
			spec.WithTZ = true
		} else {
			p.acceptKeyword("WITHOUT", "TIME", "ZONE")
		}
	}
	// VARCHAR(MAX) etc: drop sentinel parameters.
	params := spec.Params[:0]
	for _, v := range spec.Params {
		if v != -1 {
			params = append(params, v)
		}
	}
	spec.Params = params
	return spec, nil
}

// ---- expressions ----

func (p *parser) parseCase() (Expr, error) {
	c := &Case{}
	if !p.isKeyword("WHEN") {
		op, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.Operand = op
	}
	for p.acceptKeyword("WHEN") {
		w, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("THEN"); err != nil {
			return nil, err
		}
		t, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.Whens = append(c.Whens, WhenClause{When: w, Then: t})
	}
	if len(c.Whens) == 0 {
		return nil, syntaxErr("CASE requires at least one WHEN")
	}
	if p.acceptKeyword("ELSE") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		c.Else = e
	}
	if err := p.expectKeyword("END"); err != nil {
		return nil, err
	}
	return c, nil
}

func (p *parser) parseExpr() (Expr, error) { return p.parseOr() }

func (p *parser) parseOr() (Expr, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("OR") {
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: "OR", L: l, R: r}
	}
	return l, nil
}

func (p *parser) parseAnd() (Expr, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("AND") {
		r, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: "AND", L: l, R: r}
	}
	return l, nil
}

func (p *parser) parseNot() (Expr, error) {
	if p.acceptKeyword("NOT") {
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &Unary{Op: "NOT", X: x}, nil
	}
	return p.parseComparison()
}

func (p *parser) parseComparison() (Expr, error) {
	l, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == tokOp {
			switch t.text {
			case "=", "==", "<>", "!=", "<", "<=", ">", ">=":
				p.pos++
				r, err := p.parseAdditive()
				if err != nil {
					return nil, err
				}
				op := t.text
				switch op {
				case "==":
					op = "="
				case "!=":
					op = "<>"
				}
				l = &Binary{Op: op, L: l, R: r}
				continue
			}
		}
		if p.acceptKeyword("IS") {
			not := p.acceptKeyword("NOT")
			if !p.acceptKeyword("NULL") {
				return nil, syntaxErr("expected NULL after IS")
			}
			l = &IsNull{X: l, Not: not}
			continue
		}
		if p.isKeyword("BETWEEN") || (p.isKeyword("NOT") && p.isKeywordAt(1, "BETWEEN")) {
			not := p.acceptKeyword("NOT")
			p.acceptKeyword("BETWEEN")
			lo, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			if err := p.expectKeyword("AND"); err != nil {
				return nil, err
			}
			hi, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			var e Expr = &Binary{Op: "AND", L: &Binary{Op: ">=", L: l, R: lo}, R: &Binary{Op: "<=", L: l, R: hi}}
			if not {
				e = &Unary{Op: "NOT", X: e}
			}
			l = e
			continue
		}
		if p.isKeyword("IN") || (p.isKeyword("NOT") && p.isKeywordAt(1, "IN")) {
			not := p.acceptKeyword("NOT")
			p.acceptKeyword("IN")
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			if p.isQueryStart() {
				sub, err := p.parseSubquery()
				if err != nil {
					return nil, err
				}
				l = &Subquery{Select: sub, Kind: SubqueryIn, X: l, Not: not}
				continue
			}
			var e Expr
			for {
				item, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				eq := &Binary{Op: "=", L: l, R: item}
				if e == nil {
					e = eq
				} else {
					e = &Binary{Op: "OR", L: e, R: eq}
				}
				if !p.acceptOp(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			if not {
				e = &Unary{Op: "NOT", X: e}
			}
			l = e
			continue
		}
		return l, nil
	}
}

func (p *parser) parseAdditive() (Expr, error) {
	l, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.isOp("+"), p.isOp("-"), p.isOp("||"):
			op := p.next().text
			r, err := p.parseMultiplicative()
			if err != nil {
				return nil, err
			}
			l = &Binary{Op: op, L: l, R: r}
		default:
			return l, nil
		}
	}
}

func (p *parser) parseMultiplicative() (Expr, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.isOp("*"), p.isOp("/"), p.isOp("%"):
			op := p.next().text
			r, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			l = &Binary{Op: op, L: l, R: r}
		default:
			return l, nil
		}
	}
}

func (p *parser) parseUnary() (Expr, error) {
	if p.isOp("-") || p.isOp("+") {
		op := p.next().text
		// Fold negative numeric literals so that e.g. -9223372036854775808
		// stays an integer.
		if op == "-" && p.peek().kind == tokNumber {
			t := p.next()
			v, err := numberLiteral("-" + t.text)
			if err != nil {
				return nil, syntaxErr("%v", err)
			}
			return p.parsePostfix(&Literal{V: v})
		}
		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if op == "+" {
			return x, nil
		}
		return &Unary{Op: "-", X: x}, nil
	}
	x, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	return p.parsePostfix(x)
}

func (p *parser) parsePostfix(x Expr) (Expr, error) {
	for p.acceptOp("::") {
		spec, err := p.parseTypeSpec()
		if err != nil {
			return nil, err
		}
		ct, err := colTypeFromSQL(spec)
		if err != nil {
			return nil, &sqlError{msg: err.Error()}
		}
		x = &Cast{X: x, T: ct}
	}
	return x, nil
}

func (p *parser) parsePrimary() (Expr, error) {
	t := p.peek()
	switch t.kind {
	case tokNumber:
		p.pos++
		v, err := numberLiteral(t.text)
		if err != nil {
			return nil, syntaxErr("%v", err)
		}
		return &Literal{V: v}, nil
	case tokString:
		p.pos++
		return &Literal{V: stringValue(t.text)}, nil
	case tokHex:
		p.pos++
		b, err := hex.DecodeString(t.text)
		if err != nil {
			return nil, syntaxErr("invalid hex literal X'%s'", t.text)
		}
		return &Literal{V: binaryValue(string(b))}, nil
	case tokParam:
		p.pos++
		idx := p.nextParam
		if t.text != "" {
			n, err := strconv.Atoi(t.text)
			if err != nil || n < 1 {
				return nil, syntaxErr("invalid parameter $%s", t.text)
			}
			idx = n - 1
		} else {
			p.nextParam++
		}
		if idx+1 > p.numParams {
			p.numParams = idx + 1
		}
		return &Param{Index: idx}, nil
	case tokOp:
		if t.text == "(" {
			p.pos++
			if p.isQueryStart() {
				sub, err := p.parseSubquery()
				if err != nil {
					return nil, err
				}
				return &Subquery{Select: sub, Kind: SubqueryScalar}, nil
			}
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			return e, nil
		}
		return nil, syntaxErr("unexpected %q", t.text)
	case tokQuotedIdent:
		p.pos++
		return p.parseColumnRef(t.text)
	case tokIdent:
		upper := strings.ToUpper(t.text)
		switch upper {
		case "NULL":
			p.pos++
			return &Literal{V: nullValue(typeNull)}, nil
		case "TRUE":
			p.pos++
			return &Literal{V: boolValue(true)}, nil
		case "FALSE":
			p.pos++
			return &Literal{V: boolValue(false)}, nil
		case "CASE":
			p.pos++
			return p.parseCase()
		case "EXISTS":
			if p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(" {
				p.pos += 2
				sub, err := p.parseSubquery()
				if err != nil {
					return nil, err
				}
				return &Subquery{Select: sub, Kind: SubqueryExists}, nil
			}
		case "CAST", "TRY_CAST":
			if p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(" {
				p.pos += 2
				x, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				if err := p.expectKeyword("AS"); err != nil {
					return nil, err
				}
				spec, err := p.parseTypeSpec()
				if err != nil {
					return nil, err
				}
				if err := p.expectOp(")"); err != nil {
					return nil, err
				}
				ct, err := colTypeFromSQL(spec)
				if err != nil {
					return nil, &sqlError{msg: err.Error()}
				}
				return &Cast{X: x, T: ct}, nil
			}
		case "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "DATETIME":
			// Typed literal: DATE '...', TIMESTAMP [WITH TIME ZONE] '...'
			save := p.pos
			p.pos++
			withTZ := upper == "TIMESTAMPTZ"
			if upper == "TIMESTAMP" || upper == "TIME" {
				if p.acceptKeyword("WITH", "TIME", "ZONE") {
					withTZ = true
				} else {
					p.acceptKeyword("WITHOUT", "TIME", "ZONE")
				}
			}
			if s := p.peek(); s.kind == tokString {
				p.pos++
				var v Value
				var err error
				switch upper {
				case "DATE":
					v, err = dateLiteral(s.text)
				case "TIME":
					v, err = timeLiteral(s.text)
				default:
					v, err = timestampLiteral(s.text, withTZ)
				}
				if err != nil {
					return nil, &sqlError{msg: err.Error()}
				}
				return &Literal{V: v}, nil
			}
			p.pos = save
		case "INTERVAL":
			return nil, &sqlError{msg: "INTERVAL is not supported"}
		}
		p.pos++
		if p.acceptOp("(") {
			f := &Func{Name: upper}
			if p.acceptOp("*") {
				f.Star = true
			} else if !p.isOp(")") {
				f.Distinct = p.acceptKeyword("DISTINCT")
				for {
					a, err := p.parseExpr()
					if err != nil {
						return nil, err
					}
					f.Args = append(f.Args, a)
					if !p.acceptOp(",") {
						break
					}
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			return f, nil
		}
		return p.parseColumnRef(t.text)
	}
	return nil, syntaxErr("unexpected end of input")
}

// parseColumnRef parses the rest of [schema.][table.]column after its first
// part; the part just before the column is kept as the qualifier.
func (p *parser) parseColumnRef(first string) (Expr, error) {
	parts := []string{first}
	for p.acceptOp(".") {
		n, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		parts = append(parts, n)
	}
	ref := &ColumnRef{Name: parts[len(parts)-1]}
	if len(parts) > 1 {
		ref.Qualifier = parts[len(parts)-2]
	}
	return ref, nil
}
