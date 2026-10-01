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
	// written is the name as written; binding always resolves from it, so
	// binding a node twice (it may be shared, e.g. by an expanded IN list)
	// gives the same result.
	written string
}

// Subquery is a SELECT used as an expression: a scalar subquery
// `(SELECT …)`, `EXISTS (SELECT …)`, `X [NOT] IN (SELECT …)`, or
// `X op ANY | SOME | ALL (SELECT …)`. `= ANY` is parsed as IN and `<> ALL`
// as NOT IN.
type Subquery struct {
	Select *SelectStmt
	Kind   SubqueryKind
	X      Expr // IN operand
	Not    bool // NOT IN
	// Width is set for a scalar subquery assigned to a column list (UPDATE
	// … SET (a, b) = (SELECT …)): the number of columns it must return.
	Width int
	// Op is the comparison operator of ANY / ALL (X is their operand).
	Op string

	// Set while binding.
	plan       *selectPlan
	correlated bool
	// outerRefs are the outer columns the body reads, relative to the
	// environment that evaluates the subquery (used as the memo key).
	outerRefs []outerRef
	// outerUses counts the outer references bound in the body (including
	// nested subqueries).
	outerUses int
	// semi is set when the subquery runs as a semi-join (see semijoin.go).
	semi *semiJoin
}

type SubqueryKind int

const (
	SubqueryScalar SubqueryKind = iota
	SubqueryExists
	SubqueryIn
	SubqueryAny
	SubqueryAll
	// SubqueryLateral is a LATERAL subquery in FROM (see lateral.go); it is
	// never part of an expression.
	SubqueryLateral
)

type outerRef struct {
	name string
	up   int
}

// RowColumn is column Index of the row returned by a subquery assigned to a
// column list: SET (a, b) = (SELECT x, y …) sets a to RowColumn 0 and b to
// RowColumn 1 of the same subquery, which runs once per row.
type RowColumn struct {
	Sub   *Subquery
	Index int
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

// Cast is CAST(x AS t) or x::t. OnError, if set, is the result when the
// value cannot be converted: NULL for TRY_CAST and SAFE_CAST, v for
// CAST(x AS t DEFAULT v ON CONVERSION ERROR).
type Cast struct {
	X       Expr
	T       ColType
	OnError Expr
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
	// OrderBy orders an aggregate's input: STRING_AGG(x, sep ORDER BY …),
	// JSON_AGG(x ORDER BY …), or WITHIN GROUP (ORDER BY …) (WithinGroup is
	// then set). For an ordered-set aggregate (PERCENTILE_CONT, …) the
	// WITHIN GROUP expression is the aggregated value and Args are its direct
	// arguments.
	OrderBy     []OrderItem
	WithinGroup bool
	// Filter is an aggregate's FILTER (WHERE …) condition.
	Filter Expr
	// Nulls is IGNORE NULLS / RESPECT NULLS (LAG, LEAD and the value
	// window functions).
	Nulls NullTreatment
	// JSON holds the clauses of a SQL/JSON function (RETURNING, ON ERROR, …).
	JSON *jsonClauses
}

// NullTreatment is the IGNORE NULLS / RESPECT NULLS of a function call.
type NullTreatment int

const (
	NullsUnspecified NullTreatment = iota
	RespectNulls
	IgnoreNulls
)

// WindowFunc is a window function call, fn(args) OVER (…). Over is fully
// resolved by the parser: references to named windows are replaced by the
// definitions they refer to.
type WindowFunc struct {
	Func *Func
	Over *WindowSpec
}

func (*WindowFunc) exprNode() {}

// WindowSpec is a window definition: [PARTITION BY …] [ORDER BY …] [frame].
type WindowSpec struct {
	// Ref names a window of the query's WINDOW clause this one is based on
	// (`OVER w` or `OVER (w ORDER BY …)`); it is empty once resolved.
	Ref         string
	PartitionBy []Expr
	OrderBy     []OrderItem
	Frame       *WindowFrame // nil: the default frame
	bare        bool         // OVER w, without parentheses
}

// FrameUnit is the unit of a window frame.
type FrameUnit int

const (
	FrameRows FrameUnit = iota
	FrameRange
	FrameGroups
)

// BoundKind is the kind of a window frame bound.
type BoundKind int

const (
	BoundUnboundedPreceding BoundKind = iota
	BoundPreceding
	BoundCurrentRow
	BoundFollowing
	BoundUnboundedFollowing
)

// FrameBound is one end of a window frame; Offset is set for n PRECEDING and
// n FOLLOWING.
type FrameBound struct {
	Kind   BoundKind
	Offset Expr
}

// FrameExclusion is a frame's EXCLUDE clause.
type FrameExclusion int

const (
	ExcludeNoOthers FrameExclusion = iota
	ExcludeCurrentRow
	ExcludeGroup // the current row and its peers
	ExcludeTies  // the current row's peers, but not the row itself
)

// WindowFrame is `{ROWS | RANGE | GROUPS} BETWEEN start AND end [EXCLUDE …]`.
type WindowFrame struct {
	Unit       FrameUnit
	Start, End FrameBound
	Exclude    FrameExclusion
}

// NamedWindow is one `name AS (…)` of a WINDOW clause.
type NamedWindow struct {
	Name string
	Spec *WindowSpec
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
func (*RowColumn) exprNode() {}

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
	// StarOf is the qualifier of `rel.*` (or `schema.rel.*`, …); nil for a
	// plain `*`.
	StarOf []string
	Text   string
}

// NullsOrder is an explicit NULLS FIRST / NULLS LAST.
type NullsOrder int

const (
	NullsDefault NullsOrder = iota // last, in either direction
	NullsFirst
	NullsLast
)

type OrderItem struct {
	Expr  Expr
	Desc  bool
	Nulls NullsOrder
}

// SetOperation is `Left op [ALL] Right` for op UNION, INTERSECT or EXCEPT.
type SetOperation struct {
	Op          string
	All         bool
	Left, Right *SelectStmt
}

// CTE is one `name [(columns)] AS (SELECT …)` of a WITH clause.
type CTE struct {
	Name    string
	Columns []string
	Select  *SelectStmt
	// Recursive is set for the CTEs of a WITH RECURSIVE list; those that
	// refer to themselves are evaluated by iteration (see recursive.go).
	Recursive bool
	// Search and Cycle are a recursive CTE's SEARCH and CYCLE clauses.
	Search *SearchClause
	Cycle  *CycleClause
}

// SearchClause is `SEARCH {BREADTH | DEPTH} FIRST BY col, … SET seq`.
type SearchClause struct {
	Breadth bool
	By      []string
	Set     string
}

// CycleClause is `CYCLE col, … SET mark [TO value DEFAULT value] USING
// path`; To and Default are nil for TRUE and FALSE.
type CycleClause struct {
	Columns     []string
	Set         string
	To, Default Expr
	Using       string
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
	// Func is a table function item (instead of Table or Select), Lateral
	// marks a LATERAL item, Columns are column aliases (`AS a(x, y)`), and
	// Natural is NATURAL JOIN (USING over the columns both sides have).
	Func    *Func
	Lateral bool
	Columns []string
	Natural bool
}

type SelectStmt struct {
	// SetOp is set for a set operation (UNION / INTERSECT / EXCEPT); only
	// With, OrderBy, Limit and Offset (of the combined result) are used then.
	SetOp *SetOperation
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
	Windows    []NamedWindow // WINDOW clause
	Qualify    Expr          // filters rows after window functions
	// GroupingSets is set when GROUP BY uses ROLLUP, CUBE, GROUPING SETS or
	// (): each grouping set lists the positions in GroupBy of its
	// expressions, and GroupBy holds the expressions of every set.
	GroupingSets [][]int
	// Distinct is SELECT DISTINCT; DistinctOn holds the expressions of
	// Postgres's SELECT DISTINCT ON (…).
	Distinct   bool
	DistinctOn []Expr
	OrderBy    []OrderItem
	Limit      *int64
	Offset     *int64
	// FromFunc is a table function as the first FROM item, FromLateral
	// marks it LATERAL, and FromColumns are its column aliases
	// (`AS a(x, y)`); see JoinClause.
	FromFunc    *Func
	FromLateral bool
	FromColumns []string
}

type InsertStmt struct {
	Table   TableName
	Columns []string
	Rows    [][]Expr
	// Select is set for INSERT INTO … SELECT.
	Select *SelectStmt
	// DefaultValues is set for INSERT INTO … DEFAULT VALUES (one row of
	// column defaults).
	DefaultValues bool
	// Returning is the RETURNING list (written like a select list), if any.
	Returning []SelectItem
}

// DefaultValue is the DEFAULT keyword written in place of a value in INSERT
// … VALUES or MERGE's INSERT VALUES: the column's default.
type DefaultValue struct{}

func (*DefaultValue) exprNode() {}

type ColumnDef struct {
	Name    string
	Type    ColType
	NotNull bool
	// NoIndex keeps the column out of the RediSearch index (it is still
	// stored in the row HASH).
	NoIndex bool
	// Default is the DEFAULT expression (nil without one), and DefaultText
	// its source text, which is what the table metadata stores.
	Default     Expr
	DefaultText string
	// Comment is the text of a COMMENT 'text' clause (MySQL / Snowflake).
	Comment string
	// Checks are the column's CHECK constraints (ADD COLUMN only: CREATE
	// TABLE moves them to CreateTableStmt.Checks).
	Checks []CheckDef
}

// CheckDef is a CHECK constraint: its CONSTRAINT name ("" if it has none),
// its expression, and the text the table metadata stores (see check.go).
type CheckDef struct {
	Name string
	Expr Expr
	Text string
}

type AlterAction int

const (
	AlterRenameTable AlterAction = iota + 1
	AlterRenameColumn
	AlterAddColumn
	AlterDropColumn
	AlterSetDefault
	AlterDropDefault
	AlterAddConstraint
	AlterDropConstraint
)

type AlterTableStmt struct {
	Table    TableName
	IfExists bool
	// View is set for ALTER VIEW (which only supports RENAME TO).
	View              bool
	Action            AlterAction
	NewTable          TableName // RENAME TO
	Column            string    // RENAME COLUMN (old) / DROP COLUMN / ALTER COLUMN
	NewColumn         string    // RENAME COLUMN (new)
	Def               ColumnDef // ADD COLUMN; SET DEFAULT (only the default)
	IfColumnExists    bool
	IfColumnNotExists bool
	// Check is the CHECK of ADD CONSTRAINT (nil for the PRIMARY KEY, UNIQUE
	// and FOREIGN KEY constraints, which are accepted and ignored).
	// Constraint is the name DROP CONSTRAINT drops.
	Check              *CheckDef
	Constraint         string
	IfConstraintExists bool
}

func (*AlterTableStmt) stmtNode() {}

type CreateTableStmt struct {
	Table       TableName
	IfNotExists bool
	Temporary   bool
	Columns     []ColumnDef
	// Checks are the CHECK constraints, column and table ones, in the order
	// written (which is the order their default names are chosen in).
	Checks []CheckDef
	// AsSelect is set for CREATE TABLE … AS SELECT.
	AsSelect *SelectStmt
	// Comment is the text of a table-level COMMENT [=] 'text' clause.
	Comment string
}

type DropTableStmt struct {
	Table    TableName
	IfExists bool
}

// SetClause is one `column = expr` of a SET list. Qualifier is the optional
// target table or alias written before the column (`t.col = …`). A column
// list assignment `(a, b) = …` is parsed into one SetClause per column.
type SetClause struct {
	Qualifier string
	Column    string
	Expr      Expr
}

// UpdateStmt is [WITH …] UPDATE t [[AS] alias] SET … [FROM …] [WHERE …]
// [RETURNING …].
type UpdateStmt struct {
	With  []CTE
	Table TableName
	Alias string
	Sets  []SetClause
	// From holds the items of UPDATE … FROM, as in a SELECT's FROM clause:
	// the first has an empty Kind, the rest are joins or comma items.
	From  []JoinClause
	Where Expr
	// Returning is the RETURNING list, if any.
	Returning []SelectItem
}

// DeleteStmt is [WITH …] DELETE FROM t [[AS] alias] [USING …] [WHERE …]
// [RETURNING …]; Using holds the items like UpdateStmt.From.
type DeleteStmt struct {
	With  []CTE
	Table TableName
	Alias string
	Using []JoinClause
	Where Expr
	// Returning is the RETURNING list, if any.
	Returning []SelectItem
}

// MergeStmt is [WITH …] MERGE INTO t [[AS] alias] USING source ON cond
// followed by WHEN clauses and an optional RETURNING list. Source is a FROM
// item (Kind is empty).
type MergeStmt struct {
	With    []CTE
	Table   TableName
	Alias   string
	Source  JoinClause
	On      Expr
	Clauses []MergeClause
	// Returning is the RETURNING list, if any.
	Returning []SelectItem
}

// MergeMatch says which rows a WHEN clause applies to.
type MergeMatch int

const (
	MergeMatched            MergeMatch = iota // WHEN MATCHED
	MergeNotMatched                           // WHEN NOT MATCHED [BY TARGET]
	MergeNotMatchedBySource                   // WHEN NOT MATCHED BY SOURCE
)

type MergeAction int

const (
	MergeDoNothing MergeAction = iota
	MergeUpdate
	MergeDelete
	MergeInsert
)

// MergeClause is WHEN [NOT] MATCHED [BY SOURCE | BY TARGET] [AND Cond] THEN
// UPDATE SET … | DELETE | INSERT [(Columns)] VALUES (…) | INSERT DEFAULT
// VALUES | DO NOTHING.
type MergeClause struct {
	Match   MergeMatch
	Cond    Expr
	Action  MergeAction
	Sets    []SetClause
	Columns []string
	Values  []Expr // nil for INSERT DEFAULT VALUES
}

type CreateSchemaStmt struct {
	Name        string
	IfNotExists bool
}

type DropSchemaStmt struct {
	Name     string
	IfExists bool
	// Cascade drops the schema's views and tables first; without it
	// (RESTRICT, the default) a schema that still has any is not dropped.
	Cascade bool
}

// TruncateStmt is TRUNCATE [TABLE] t [, …] [RESTART | CONTINUE IDENTITY].
type TruncateStmt struct {
	Tables []TableName
	// RestartIdentity restarts the row id sequence (__rowid) at 1.
	RestartIdentity bool
}

// CommentObject is the kind of object a COMMENT ON statement names.
type CommentObject int

const (
	CommentOnTable CommentObject = iota + 1
	CommentOnView
	CommentOnColumn
)

// CommentStmt is COMMENT ON {TABLE | VIEW | COLUMN} name IS 'text' | NULL.
// For a column, Table is its table or view and Column its name. Text is ""
// for NULL or an empty string, which removes the comment.
type CommentStmt struct {
	Object CommentObject
	Table  TableName
	Column string
	Text   string
}

func (*SelectStmt) stmtNode()       {}
func (*InsertStmt) stmtNode()       {}
func (*CreateTableStmt) stmtNode()  {}
func (*DropTableStmt) stmtNode()    {}
func (*UpdateStmt) stmtNode()       {}
func (*DeleteStmt) stmtNode()       {}
func (*MergeStmt) stmtNode()        {}
func (*CreateSchemaStmt) stmtNode() {}
func (*DropSchemaStmt) stmtNode()   {}
func (*CreateViewStmt) stmtNode()   {}
func (*DropViewStmt) stmtNode()     {}
func (*TruncateStmt) stmtNode()     {}
func (*CommentStmt) stmtNode()      {}

// CreateViewStmt is CREATE [OR REPLACE] [TEMP] VIEW [IF NOT EXISTS] v
// [(cols)] AS SELECT …; Text is the SELECT's source text, which is what gets
// stored.
type CreateViewStmt struct {
	Name        TableName
	Columns     []string
	Select      *SelectStmt
	Text        string
	OrReplace   bool
	IfNotExists bool
	Temporary   bool
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
		case c == '$':
			// A dollar-quoted string, $$…$$ or $tag$…$tag$, as in Postgres
			// (dbt quotes comments this way). Its contents are taken as they
			// are.
			n := dollarTag(src[i:])
			if n == 0 {
				return nil, syntaxErr("unexpected character %q at offset %d", c, i)
			}
			body := strings.Index(src[i+n:], src[i:i+n])
			if body < 0 {
				return nil, syntaxErr("unterminated dollar-quoted string")
			}
			toks = append(toks, token{kind: tokString, text: src[i+n : i+n+body], pos: i, end: i + 2*n + body})
			i += 2*n + body
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
		case c == '~' || (c == '!' && i+1 < len(src) && src[i+1] == '~'):
			// The regular-expression match operators ~, ~*, !~ and !~*.
			start := i
			if c == '!' {
				i++
			}
			i++
			if i < len(src) && src[i] == '*' {
				i++
			}
			toks = append(toks, token{kind: tokOp, text: src[start:i], pos: start, end: i})
		default:
			ops := []string{"->>", "->", "#>>", "#>", "<>", "!=", "<=", ">=", "||", "::", "==", "(", ")", ",", ";", "*", "+", "-", "/", "%", "=", "<", ">", ".", ":"}
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

// dollarTag returns the length of the delimiter that opens a dollar-quoted
// string at the start of s: $$, or $tag$ with a tag written like an
// identifier but without '$'. It is 0 if s starts with no such delimiter.
func dollarTag(s string) int {
	for i := 1; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == '$':
			return i + 1
		case r == '_' || unicode.IsLetter(r) || (i > 1 && unicode.IsDigit(r)):
			i += size
		default:
			return 0
		}
	}
	return 0
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
	// refs, when set, collects where each column reference is written (for
	// CHECK expressions, see check.go).
	refs *[]refSpan
}

// refSpan is where a column reference is written in the source: bytes
// [start, end), with its last part (the column's name) from name on.
type refSpan struct {
	ref              *ColumnRef
	start, name, end int
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
		if err := p.parseReturning(stmt); err != nil {
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

// parseDottedName parses identifiers separated by '.'.
func (p *parser) parseDottedName() ([]string, error) {
	var parts []string
	for {
		id, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		parts = append(parts, id)
		if !p.acceptOp(".") {
			return parts, nil
		}
	}
}

// tableNameOf makes a table name of [[catalog.]schema.]name; ok is false
// for more parts.
func tableNameOf(parts []string) (TableName, bool) {
	switch len(parts) {
	case 1:
		return TableName{Name: parts[0]}, true
	case 2:
		return TableName{Schema: parts[0], Name: parts[1]}, true
	case 3:
		return TableName{Catalog: parts[0], Schema: parts[1], Name: parts[2]}, true
	}
	return TableName{}, false
}

func (p *parser) parseTableName() (TableName, error) {
	parts, err := p.parseDottedName()
	if err != nil {
		return TableName{}, err
	}
	t, ok := tableNameOf(parts)
	if !ok {
		return TableName{}, syntaxErr("invalid table name %s", strings.Join(parts, "."))
	}
	return t, nil
}

func (p *parser) parseStatement() (Stmt, error) {
	switch {
	case p.isKeyword("WITH"):
		// WITH … SELECT, or WITH … UPDATE / DELETE / MERGE.
		with, err := p.parseWith()
		if err != nil {
			return nil, err
		}
		switch {
		case p.isKeyword("UPDATE"):
			st, err := p.parseUpdate()
			if err != nil {
				return nil, err
			}
			st.With = with
			return st, nil
		case p.isKeyword("DELETE"):
			st, err := p.parseDelete()
			if err != nil {
				return nil, err
			}
			st.With = with
			return st, nil
		case p.isKeyword("MERGE"):
			st, err := p.parseMerge()
			if err != nil {
				return nil, err
			}
			st.With = with
			return st, nil
		}
		return p.parseSelectBody(with)
	case p.isKeyword("SELECT"), p.isOp("("):
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
	case p.isKeyword("MERGE"):
		return p.parseMerge()
	case p.isKeyword("ALTER"):
		return p.parseAlter()
	case p.isKeyword("TRUNCATE"):
		return p.parseTruncate()
	case p.isKeyword("COMMENT"):
		return p.parseComment()
	}
	return nil, &sqlError{msg: fmt.Sprintf("unsupported statement starting with %q", p.peek().text)}
}

var reservedAfterExpr = map[string]bool{
	"FROM": true, "WHERE": true, "ORDER": true, "GROUP": true, "LIMIT": true,
	"OFFSET": true, "AND": true, "OR": true, "NOT": true, "AS": true, "IS": true,
	"ASC": true, "DESC": true, "HAVING": true, "UNION": true, "NULLS": true,
	"LIKE": true, "IN": true, "BETWEEN": true, "SET": true, "VALUES": true,
	"WHEN": true, "THEN": true, "ELSE": true, "END": true, "ILIKE": true, "ESCAPE": true,
	"INTERSECT": true, "EXCEPT": true, "MINUS": true,
	"JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "FULL": true,
	"CROSS": true, "OUTER": true, "ON": true, "USING": true, "NATURAL": true,
	"WINDOW": true, "QUALIFY": true, "RETURNING": true, "FETCH": true,
}

// isQueryStart reports whether the next token begins a (sub)query.
func (p *parser) isQueryStart() bool { return p.isKeyword("SELECT") || p.isKeyword("WITH") }

// isParenQueryStart reports whether the next tokens open a parenthesized
// query: `(SELECT`, `(WITH`, or `((` (a parenthesized set operation branch).
func (p *parser) isParenQueryStart() bool {
	return p.isOp("(") && (p.isKeywordAt(1, "SELECT") || p.isKeywordAt(1, "WITH") ||
		(p.peekAt(1).kind == tokOp && p.peekAt(1).text == "("))
}

// parseFromItem parses `table [[AS] alias]` or `(SELECT …) [AS] alias`.
func (p *parser) parseFromItem() (*TableName, *SelectStmt, string, error) {
	var table *TableName
	var sub *SelectStmt
	if p.isParenQueryStart() {
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
	alias, err := p.parseAlias()
	if err != nil {
		return nil, nil, "", err
	}
	return table, sub, alias, nil
}

// parseAlias parses an optional `[AS] alias` after a table or FROM item.
func (p *parser) parseAlias() (string, error) {
	if p.acceptKeyword("AS") {
		return p.parseIdent()
	}
	if t := p.peek(); t.kind == tokQuotedIdent || (t.kind == tokIdent && !reservedAfterExpr[strings.ToUpper(t.text)]) {
		p.pos++
		return t.text, nil
	}
	return "", nil
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
	with, err := p.parseWith()
	if err != nil {
		return nil, err
	}
	return p.parseSelectBody(with)
}

// parseWith parses an optional WITH list (nil if there is none).
func (p *parser) parseWith() ([]CTE, error) {
	if !p.acceptKeyword("WITH") {
		return nil, nil
	}
	recursive := p.acceptKeyword("RECURSIVE")
	var with []CTE
	for {
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		cte := CTE{Name: name, Recursive: recursive}
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
		if err := p.parseSearchCycle(&cte); err != nil {
			return nil, err
		}
		with = append(with, cte)
		if !p.acceptOp(",") {
			return with, nil
		}
	}
}

// parseSearchCycle parses a CTE's optional
// `SEARCH {BREADTH | DEPTH} FIRST BY col, … SET seq` and
// `CYCLE col, … SET mark [TO value DEFAULT value] USING path` clauses.
func (p *parser) parseSearchCycle(cte *CTE) error {
	idents := func() ([]string, error) {
		var out []string
		for {
			c, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			out = append(out, c)
			if !p.acceptOp(",") {
				return out, nil
			}
		}
	}
	var err error
	if p.acceptKeyword("SEARCH") {
		s := &SearchClause{}
		switch {
		case p.acceptKeyword("BREADTH", "FIRST", "BY"):
			s.Breadth = true
		case p.acceptKeyword("DEPTH", "FIRST", "BY"):
		default:
			return syntaxErr("expected BREADTH FIRST BY or DEPTH FIRST BY near %q", p.peek().text)
		}
		if s.By, err = idents(); err != nil {
			return err
		}
		if err := p.expectKeyword("SET"); err != nil {
			return err
		}
		if s.Set, err = p.parseIdent(); err != nil {
			return err
		}
		cte.Search = s
	}
	if p.acceptKeyword("CYCLE") {
		c := &CycleClause{}
		if c.Columns, err = idents(); err != nil {
			return err
		}
		if err := p.expectKeyword("SET"); err != nil {
			return err
		}
		if c.Set, err = p.parseIdent(); err != nil {
			return err
		}
		if p.acceptKeyword("TO") {
			if c.To, err = p.parseAdditive(); err != nil {
				return err
			}
			if err := p.expectKeyword("DEFAULT"); err != nil {
				return err
			}
			if c.Default, err = p.parseAdditive(); err != nil {
				return err
			}
		}
		if err := p.expectKeyword("USING"); err != nil {
			return err
		}
		if c.Using, err = p.parseIdent(); err != nil {
			return err
		}
		cte.Cycle = c
	}
	return nil
}

// parseSelectBody parses a query after its WITH list.
func (p *parser) parseSelectBody(with []CTE) (Stmt, error) {
	body, err := p.parseSetExpr()
	if err != nil {
		return nil, err
	}
	// ORDER BY / LIMIT / OFFSET after the last branch apply to the whole
	// query (the combined result of a set operation).
	sel := &SelectStmt{}
	if p.acceptKeyword("ORDER", "BY") {
		items, err := p.parseOrderItems()
		if err != nil {
			return nil, err
		}
		sel.OrderBy = items
	}
	for {
		if (p.isKeyword("LIMIT") || p.isKeyword("FETCH")) && sel.Limit != nil {
			return nil, &sqlError{msg: "multiple LIMIT clauses not allowed"}
		}
		if p.acceptKeyword("FETCH") {
			// FETCH {FIRST | NEXT} [n] {ROW | ROWS} ONLY is LIMIT n (1 if
			// n is left out).
			if !p.acceptKeyword("FIRST") && !p.acceptKeyword("NEXT") {
				return nil, syntaxErr("expected FIRST or NEXT after FETCH near %q", p.peek().text)
			}
			n := int64(1)
			if p.peek().kind == tokNumber {
				var err error
				if n, err = p.parseCount(); err != nil {
					return nil, err
				}
			}
			if !p.acceptKeyword("ROWS") && !p.acceptKeyword("ROW") {
				return nil, syntaxErr("expected ROW or ROWS near %q", p.peek().text)
			}
			if p.isKeyword("WITH") {
				return nil, &sqlError{msg: "FETCH … WITH TIES is not supported"}
			}
			if err := p.expectKeyword("ONLY"); err != nil {
				return nil, err
			}
			sel.Limit = &n
			continue
		}
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
			if !p.acceptKeyword("ROWS") {
				p.acceptKeyword("ROW")
			}
			continue
		}
		break
	}
	hasOuter := len(sel.OrderBy) > 0 || sel.Limit != nil || sel.Offset != nil
	innerHas := len(body.OrderBy) > 0 || body.Limit != nil || body.Offset != nil
	if (hasOuter && innerHas) || (with != nil && body.With != nil) {
		// e.g. (SELECT … LIMIT 3) ORDER BY x: wrap the parenthesized query.
		body = &SelectStmt{Items: []SelectItem{{Star: true, Text: "*"}}, FromSelect: body, FromAlias: "__q"}
	}
	if with != nil {
		body.With = with
	}
	if hasOuter {
		body.OrderBy, body.Limit, body.Offset = sel.OrderBy, sel.Limit, sel.Offset
	}
	if body.SetOp == nil {
		// ORDER BY may call window functions over the query's named windows.
		for _, o := range body.OrderBy {
			if err := resolveWindowRefs(body.Windows, o.Expr); err != nil {
				return nil, err
			}
		}
	}
	return body, nil
}

// parseOrderItems parses `expr [ASC | DESC] [NULLS FIRST | LAST], …`.
func (p *parser) parseOrderItems() ([]OrderItem, error) {
	var items []OrderItem
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
			switch {
			case p.acceptKeyword("FIRST"):
				item.Nulls = NullsFirst
			case p.acceptKeyword("LAST"):
				item.Nulls = NullsLast
			default:
				return nil, syntaxErr("expected FIRST or LAST after NULLS")
			}
		}
		items = append(items, item)
		if !p.acceptOp(",") {
			return items, nil
		}
	}
}

// parseSetExpr parses branches combined with UNION / EXCEPT (left to right),
// where INTERSECT binds tighter, as in standard SQL.
func (p *parser) parseSetExpr() (*SelectStmt, error) {
	left, err := p.parseSetTerm()
	if err != nil {
		return nil, err
	}
	for {
		op := ""
		switch {
		case p.acceptKeyword("UNION"):
			op = "UNION"
		case p.acceptKeyword("EXCEPT"), p.acceptKeyword("MINUS"):
			op = "EXCEPT"
		}
		if op == "" {
			return left, nil
		}
		all := p.acceptKeyword("ALL")
		if !all {
			p.acceptKeyword("DISTINCT")
		}
		right, err := p.parseSetTerm()
		if err != nil {
			return nil, err
		}
		left = &SelectStmt{SetOp: &SetOperation{Op: op, All: all, Left: left, Right: right}}
	}
}

func (p *parser) parseSetTerm() (*SelectStmt, error) {
	left, err := p.parseSetPrimary()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("INTERSECT") {
		all := p.acceptKeyword("ALL")
		if !all {
			p.acceptKeyword("DISTINCT")
		}
		right, err := p.parseSetPrimary()
		if err != nil {
			return nil, err
		}
		left = &SelectStmt{SetOp: &SetOperation{Op: "INTERSECT", All: all, Left: left, Right: right}}
	}
	return left, nil
}

// parseSetPrimary parses a SELECT, or a parenthesized query (which may have
// its own WITH, ORDER BY and LIMIT).
func (p *parser) parseSetPrimary() (*SelectStmt, error) {
	if p.isParenQueryStart() {
		p.pos++
		return p.parseSubquery()
	}
	return p.parseSelectCore()
}

// parseQualifiedStar parses `rel.*`, `schema.rel.*` or `catalog.schema.rel.*`
// as a select item; nothing is consumed unless it matches.
func (p *parser) parseQualifiedStar() ([]string, bool) {
	var parts []string
	for i := 0; ; i += 2 {
		t := p.peekAt(i)
		if t.kind != tokIdent && t.kind != tokQuotedIdent {
			return nil, false
		}
		if dot := p.peekAt(i + 1); dot.kind != tokOp || dot.text != "." {
			return nil, false
		}
		parts = append(parts, t.text)
		if star := p.peekAt(i + 2); star.kind == tokOp && star.text == "*" {
			p.pos += i + 3
			return parts, true
		}
	}
}

// parseSelectCore parses SELECT … FROM … WHERE … GROUP BY … HAVING ….
func (p *parser) parseSelectCore() (*SelectStmt, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &SelectStmt{}
	if p.acceptKeyword("DISTINCT") {
		sel.Distinct = true
		if p.acceptKeyword("ON") {
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			for {
				e, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				sel.DistinctOn = append(sel.DistinctOn, e)
				if !p.acceptOp(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
		}
	} else {
		p.acceptKeyword("ALL")
	}
	for {
		start := p.peek().pos
		if p.acceptOp("*") {
			sel.Items = append(sel.Items, SelectItem{Star: true, Text: "*"})
		} else if q, ok := p.parseQualifiedStar(); ok {
			sel.Items = append(sel.Items, SelectItem{Star: true, StarOf: q, Text: strings.TrimSpace(p.src[start:p.toks[p.pos-1].end])})
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
		items, err := p.parseFromList()
		if err != nil {
			return nil, err
		}
		sel.From, sel.FromSelect, sel.FromAlias = items[0].Table, items[0].Select, items[0].Alias
		sel.FromFunc, sel.FromLateral, sel.FromColumns = items[0].Func, items[0].Lateral, items[0].Columns
		sel.Joins = items[1:]
	}
	if p.acceptKeyword("WHERE") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Where = e
	}
	if p.acceptKeyword("GROUP", "BY") {
		if err := p.parseGroupBy(sel); err != nil {
			return nil, err
		}
	}
	if p.acceptKeyword("HAVING") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		sel.Having = e
	}
	// WINDOW and QUALIFY, in either order.
	for {
		if sel.Windows == nil && p.acceptKeyword("WINDOW") {
			if err := p.parseWindowClause(sel); err != nil {
				return nil, err
			}
			continue
		}
		if sel.Qualify == nil && p.acceptKeyword("QUALIFY") {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			sel.Qualify = e
			continue
		}
		break
	}
	exprs := []Expr{sel.Qualify}
	for _, it := range sel.Items {
		exprs = append(exprs, it.Expr)
	}
	for _, e := range exprs {
		if err := resolveWindowRefs(sel.Windows, e); err != nil {
			return nil, err
		}
	}
	return sel, nil
}

// parseFromEntry parses one FROM item: `[LATERAL] (SELECT …) [AS] alias`,
// `[LATERAL] function(args) [[AS] alias]` or `table [[AS] alias]`, each
// alias optionally followed by column aliases `(x, y, …)`.
func (p *parser) parseFromEntry() (JoinClause, error) {
	jc := JoinClause{Lateral: p.acceptKeyword("LATERAL")}
	var err error
	if t := p.peek(); (t.kind == tokIdent || t.kind == tokQuotedIdent) && p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(" {
		// A table function call (a table name is never followed by "(").
		p.pos += 2
		jc.Func = &Func{Name: strings.ToUpper(t.text)}
		for !p.isOp(")") {
			a, err := p.parseExpr()
			if err != nil {
				return jc, err
			}
			jc.Func.Args = append(jc.Func.Args, a)
			if !p.acceptOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return jc, err
		}
		if jc.Alias, err = p.parseAlias(); err != nil {
			return jc, err
		}
	} else {
		if jc.Lateral && !p.isParenQueryStart() {
			return jc, syntaxErr("LATERAL must be followed by a subquery or a function call near %q", p.peek().text)
		}
		if jc.Table, jc.Select, jc.Alias, err = p.parseFromItem(); err != nil {
			return jc, err
		}
	}
	if jc.Alias != "" && p.isOp("(") {
		if jc.Columns, err = p.parseColumnList(); err != nil {
			return jc, err
		}
	}
	return jc, nil
}

// parseFromList parses the items of a FROM clause: the first item (with an
// empty Kind), then any number of `, item`, `CROSS JOIN item`,
// `[INNER | LEFT | RIGHT | FULL] [OUTER] JOIN item ON … | USING (…)` and
// `NATURAL [INNER | LEFT | RIGHT | FULL] [OUTER] JOIN item`.
func (p *parser) parseFromList() ([]JoinClause, error) {
	first, err := p.parseFromEntry()
	if err != nil {
		return nil, err
	}
	items := []JoinClause{first}
	for {
		kind := ""
		natural := false
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
		case p.acceptKeyword("NATURAL"):
			natural = true
			switch {
			case p.acceptKeyword("INNER", "JOIN"), p.acceptKeyword("JOIN"):
				kind = "INNER"
			case p.acceptKeyword("LEFT", "OUTER", "JOIN"), p.acceptKeyword("LEFT", "JOIN"):
				kind = "LEFT"
			case p.acceptKeyword("RIGHT", "OUTER", "JOIN"), p.acceptKeyword("RIGHT", "JOIN"):
				kind = "RIGHT"
			case p.acceptKeyword("FULL", "OUTER", "JOIN"), p.acceptKeyword("FULL", "JOIN"):
				kind = "FULL"
			default:
				return nil, syntaxErr("expected [INNER | LEFT | RIGHT | FULL] JOIN after NATURAL near %q", p.peek().text)
			}
		}
		if kind == "" {
			return items, nil
		}
		jc, err := p.parseFromEntry()
		if err != nil {
			return nil, err
		}
		jc.Kind, jc.Natural = kind, natural
		if natural {
			if p.isKeyword("ON") || p.isKeyword("USING") {
				return nil, syntaxErr("NATURAL JOIN cannot have an ON or USING clause")
			}
		} else if kind != "CROSS" {
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
		items = append(items, jc)
	}
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
	if p.acceptKeyword("DEFAULT", "VALUES") {
		ins.DefaultValues = true
		return ins, nil
	}
	// `INSERT INTO t (SELECT …)`: a parenthesized query, not a column list.
	if !p.isParenQueryStart() {
		if ins.Columns, err = p.parseColumnList(); err != nil {
			return nil, err
		}
	}
	if p.isQueryStart() || p.isParenQueryStart() {
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
			e, err := p.parseValue()
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

// parseValue parses one value of a VALUES list, where DEFAULT on its own
// stands for the column's default.
func (p *parser) parseValue() (Expr, error) {
	if next := p.peekAt(1); p.isKeyword("DEFAULT") && next.kind == tokOp && (next.text == "," || next.text == ")") {
		p.pos++
		return &DefaultValue{}, nil
	}
	return p.parseExpr()
}

// parseDefault parses the expression of a DEFAULT clause and returns it with
// its source text.
func (p *parser) parseDefault() (Expr, string, error) {
	start := p.peek().pos
	e, err := p.parseExpr()
	if err != nil {
		return nil, "", err
	}
	return e, strings.TrimSpace(p.src[start:p.toks[p.pos-1].end]), nil
}

// parseExprText parses a standalone expression: a column default stored in
// the table metadata.
func parseExprText(src string) (Expr, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{src: src, toks: toks}
	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, syntaxErr("unexpected %q", p.peek().text)
	}
	return e, nil
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
	temporary := p.acceptKeyword("TEMPORARY") || p.acceptKeyword("TEMP")
	if p.acceptKeyword("VIEW") {
		st := &CreateViewStmt{OrReplace: orReplace, Temporary: temporary}
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
	st := &CreateTableStmt{Temporary: temporary}
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
		if p.isTableConstraint() {
			chk, err := p.parseTableConstraint(false)
			if err != nil {
				return nil, err
			}
			if chk != nil {
				st.Checks = append(st.Checks, *chk)
			}
		} else {
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			st.Checks = append(st.Checks, col.Checks...)
			col.Checks = nil
			st.Columns = append(st.Columns, col)
		}
		if !p.acceptOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	// A table comment, MySQL / Snowflake style.
	if p.acceptKeyword("COMMENT") {
		p.acceptOp("=")
		if st.Comment, err = p.parseCommentText(); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// parseCommentText parses the string of a COMMENT clause in CREATE TABLE.
func (p *parser) parseCommentText() (string, error) {
	t := p.peek()
	if t.kind != tokString {
		return "", syntaxErr("expected a string after COMMENT near %q", t.text)
	}
	p.pos++
	return t.text, nil
}

// isTableConstraint reports whether a table constraint starts here (in
// CREATE TABLE's list, or after ALTER TABLE … ADD).
func (p *parser) isTableConstraint() bool {
	return p.isKeyword("PRIMARY") || p.isKeyword("UNIQUE") || p.isKeyword("CONSTRAINT") ||
		p.isKeyword("FOREIGN") || p.isKeyword("CHECK")
}

// parseTableConstraint parses a table constraint, `[CONSTRAINT name] CHECK
// (expr)` or one of the PRIMARY KEY, UNIQUE and FOREIGN KEY constraints,
// which are accepted and ignored: everything up to the next ',' or ')' (in
// ALTER TABLE … ADD, the end of the statement) is skipped. It returns the
// CHECK, or nil for the others.
func (p *parser) parseTableConstraint(inAlter bool) (*CheckDef, error) {
	name := ""
	if p.acceptKeyword("CONSTRAINT") {
		var err error
		if name, err = p.parseIdent(); err != nil {
			return nil, err
		}
		if !p.isKeyword("CHECK") && !p.isKeyword("PRIMARY") && !p.isKeyword("UNIQUE") &&
			!p.isKeyword("FOREIGN") && !p.isKeyword("EXCLUDE") {
			return nil, syntaxErr("expected a constraint after CONSTRAINT %s near %q", name, p.peek().text)
		}
	}
	if !p.acceptKeyword("CHECK") {
		return nil, p.skipBalanced(inAlter)
	}
	chk, err := p.parseCheck(name)
	if err != nil {
		return nil, err
	}
	// Postgres's NO INHERIT and NOT VALID (which CREATE TABLE ignores) mean
	// nothing here.
	for p.acceptKeyword("NO", "INHERIT") || p.acceptKeyword("NOT", "VALID") {
	}
	return &chk, nil
}

// parseColumnDef parses `name TYPE [constraint …]`, where each constraint
// may be named with CONSTRAINT name. PRIMARY KEY, UNIQUE and REFERENCES are
// accepted and ignored.
func (p *parser) parseColumnDef() (ColumnDef, error) {
	name, err := p.parseIdent()
	if err != nil {
		return ColumnDef{}, err
	}
	spec, err := p.parseTypeSpec()
	if err != nil {
		return ColumnDef{}, err
	}
	ct, err := colTypeFromSQL(spec)
	if err != nil {
		return ColumnDef{}, &sqlError{msg: err.Error()}
	}
	col := ColumnDef{Name: name, Type: ct}
	for {
		named, constraint := "", false
		if p.acceptKeyword("CONSTRAINT") {
			if named, err = p.parseIdent(); err != nil {
				return ColumnDef{}, err
			}
			constraint = true
		}
		switch {
		case p.acceptKeyword("NOT", "NULL"):
			col.NotNull = true
		case p.acceptKeyword("NULL"):
		case p.acceptKeyword("CHECK"):
			chk, err := p.parseCheck(named)
			if err != nil {
				return ColumnDef{}, err
			}
			p.acceptKeyword("NO", "INHERIT")
			col.Checks = append(col.Checks, chk)
		case p.acceptKeyword("PRIMARY", "KEY"):
		case p.acceptKeyword("UNIQUE"):
		case p.acceptKeyword("REFERENCES"):
			if err := p.parseReferences(); err != nil {
				return ColumnDef{}, err
			}
		case p.acceptKeyword("DEFAULT"):
			if col.Default != nil {
				return ColumnDef{}, &sqlError{msg: fmt.Sprintf("multiple default values specified for column %q", name)}
			}
			if col.Default, col.DefaultText, err = p.parseDefault(); err != nil {
				return ColumnDef{}, err
			}
		case constraint:
			return ColumnDef{}, syntaxErr("expected a constraint after CONSTRAINT %s near %q", named, p.peek().text)
		case p.acceptKeyword("NOINDEX"):
			col.NoIndex = true
		case p.acceptKeyword("INDEX"):
		case p.acceptKeyword("COMMENT"):
			if col.Comment, err = p.parseCommentText(); err != nil {
				return ColumnDef{}, err
			}
		default:
			return col, nil
		}
	}
}

// parseCheck parses the `(expr)` of a CHECK constraint named name ("" for
// none). The text kept for the table metadata is the expression as written,
// except that qualified column references lose their qualifier (t.a becomes
// a), so that it still works after RENAME TO.
func (p *parser) parseCheck(name string) (CheckDef, error) {
	if err := p.expectOp("("); err != nil {
		return CheckDef{}, err
	}
	saved := p.refs
	var refs []refSpan
	p.refs = &refs
	start := p.peek().pos
	x, err := p.parseExpr()
	p.refs = saved
	if err != nil {
		return CheckDef{}, err
	}
	end := p.toks[p.pos-1].end
	if err := p.expectOp(")"); err != nil {
		return CheckDef{}, err
	}
	text := rewriteRefs(p.src[start:end], start, refs, func(r refSpan) (string, bool) {
		if r.name == r.start {
			return "", false
		}
		return p.src[r.name:r.end], true
	})
	return CheckDef{Name: name, Expr: x, Text: text}, nil
}

// parseReferences parses what follows REFERENCES in a column definition:
// t [(column)] [MATCH {FULL | PARTIAL | SIMPLE}] [ON DELETE action] [ON
// UPDATE action], as in Postgres. Foreign keys aren't enforced, so nothing
// is kept, and the table needn't exist.
func (p *parser) parseReferences() error {
	if _, err := p.parseTableName(); err != nil {
		return err
	}
	if p.acceptOp("(") {
		n := 0
		for {
			if _, err := p.parseIdent(); err != nil {
				return err
			}
			n++
			if !p.acceptOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return err
		}
		if n != 1 {
			return &sqlError{msg: "number of referencing and referenced columns for foreign key disagree"}
		}
	}
	if p.acceptKeyword("MATCH") && !p.acceptKeyword("FULL") && !p.acceptKeyword("PARTIAL") && !p.acceptKeyword("SIMPLE") {
		return syntaxErr("expected FULL, PARTIAL or SIMPLE after MATCH near %q", p.peek().text)
	}
	seen := map[string]bool{}
	for p.isKeyword("ON") && (p.isKeywordAt(1, "DELETE") || p.isKeywordAt(1, "UPDATE")) {
		event := strings.ToUpper(p.peekAt(1).text)
		if seen[event] {
			return syntaxErr("ON %s specified more than once", event)
		}
		seen[event] = true
		p.pos += 2
		switch {
		case p.acceptKeyword("NO", "ACTION"), p.acceptKeyword("RESTRICT"), p.acceptKeyword("CASCADE"):
		case p.acceptKeyword("SET", "NULL"), p.acceptKeyword("SET", "DEFAULT"):
			// Postgres 15 can name the columns: SET NULL (a, b).
			if p.acceptOp("(") {
				for {
					if _, err := p.parseIdent(); err != nil {
						return err
					}
					if !p.acceptOp(",") {
						break
					}
				}
				if err := p.expectOp(")"); err != nil {
					return err
				}
			}
		default:
			return syntaxErr("expected NO ACTION, RESTRICT, CASCADE, SET NULL or SET DEFAULT near %q", p.peek().text)
		}
	}
	return nil
}

// parseAlter parses ALTER TABLE [IF EXISTS] t followed by one of
// RENAME TO u | RENAME [COLUMN] a TO b | ADD [COLUMN] [IF NOT EXISTS] def |
// DROP [COLUMN] [IF EXISTS] c | ALTER [COLUMN] c {SET DEFAULT expr | DROP
// DEFAULT} | ADD table_constraint | DROP CONSTRAINT [IF EXISTS] name.
func (p *parser) parseAlter() (Stmt, error) {
	if err := p.expectKeyword("ALTER"); err != nil {
		return nil, err
	}
	st := &AlterTableStmt{}
	if p.acceptKeyword("VIEW") {
		st.View = true
	} else if err := p.expectKeyword("TABLE"); err != nil {
		return nil, syntaxErr("expected ALTER TABLE or ALTER VIEW")
	}
	st.IfExists = p.acceptKeyword("IF", "EXISTS")
	t, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	st.Table = t
	switch {
	case p.acceptKeyword("RENAME", "TO"):
		nt, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		st.Action, st.NewTable = AlterRenameTable, nt
	case p.acceptKeyword("RENAME"):
		p.acceptKeyword("COLUMN")
		if st.Column, err = p.parseIdent(); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("TO"); err != nil {
			return nil, err
		}
		if st.NewColumn, err = p.parseIdent(); err != nil {
			return nil, err
		}
		st.Action = AlterRenameColumn
	case p.acceptKeyword("ADD"):
		if p.isTableConstraint() {
			if st.Check, err = p.parseTableConstraint(true); err != nil {
				return nil, err
			}
			st.Action = AlterAddConstraint
			break
		}
		p.acceptKeyword("COLUMN")
		st.IfColumnNotExists = p.acceptKeyword("IF", "NOT", "EXISTS")
		if st.Def, err = p.parseColumnDef(); err != nil {
			return nil, err
		}
		st.Action = AlterAddColumn
	case p.acceptKeyword("DROP", "CONSTRAINT"):
		st.IfConstraintExists = p.acceptKeyword("IF", "EXISTS")
		if st.Constraint, err = p.parseIdent(); err != nil {
			return nil, err
		}
		p.parseDropBehavior()
		st.Action = AlterDropConstraint
	case p.acceptKeyword("DROP"):
		p.acceptKeyword("COLUMN")
		st.IfColumnExists = p.acceptKeyword("IF", "EXISTS")
		if st.Column, err = p.parseIdent(); err != nil {
			return nil, err
		}
		p.parseDropBehavior()
		st.Action = AlterDropColumn
	case p.acceptKeyword("ALTER"):
		p.acceptKeyword("COLUMN")
		if st.Column, err = p.parseIdent(); err != nil {
			return nil, err
		}
		switch {
		case p.acceptKeyword("SET", "DEFAULT"):
			if st.Def.Default, st.Def.DefaultText, err = p.parseDefault(); err != nil {
				return nil, err
			}
			st.Action = AlterSetDefault
		case p.acceptKeyword("DROP", "DEFAULT"):
			st.Action = AlterDropDefault
		default:
			return nil, &sqlError{msg: fmt.Sprintf("unsupported ALTER COLUMN action near %q (supported: SET DEFAULT, DROP DEFAULT)", p.peek().text)}
		}
	default:
		if st.View {
			return nil, &sqlError{msg: fmt.Sprintf("unsupported ALTER VIEW action near %q (supported: RENAME TO)", p.peek().text)}
		}
		return nil, &sqlError{msg: fmt.Sprintf("unsupported ALTER TABLE action near %q (supported: RENAME TO, RENAME COLUMN, ADD COLUMN, DROP COLUMN, ALTER COLUMN, ADD CONSTRAINT, DROP CONSTRAINT)", p.peek().text)}
	}
	if st.View && st.Action != AlterRenameTable {
		return nil, &sqlError{msg: "ALTER VIEW supports only RENAME TO"}
	}
	return st, nil
}

// skipBalanced skips tokens up to the next top-level ',' or ')', or with
// toEnd set, up to the end of the statement.
func (p *parser) skipBalanced(toEnd bool) error {
	depth := 0
	for {
		t := p.peek()
		switch {
		case toEnd && depth == 0 && (t.kind == tokEOF || (t.kind == tokOp && t.text == ";")):
			return nil
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
		p.parseDropBehavior()
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
		st.Cascade = p.parseDropBehavior()
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
	p.parseDropBehavior()
	return st, nil
}

// parseComment parses COMMENT ON {TABLE | VIEW | COLUMN} name IS 'text' |
// NULL, where a column is named [[catalog.]schema.]relation.column.
func (p *parser) parseComment() (Stmt, error) {
	if err := p.expectKeyword("COMMENT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	st := &CommentStmt{}
	switch {
	case p.acceptKeyword("TABLE"):
		st.Object = CommentOnTable
	case p.acceptKeyword("VIEW"):
		st.Object = CommentOnView
	case p.acceptKeyword("COLUMN"):
		st.Object = CommentOnColumn
	default:
		return nil, &sqlError{msg: fmt.Sprintf("unsupported COMMENT ON object %q (supported: TABLE, VIEW, COLUMN)", p.peek().text)}
	}
	parts, err := p.parseDottedName()
	if err != nil {
		return nil, err
	}
	rel := parts
	if st.Object == CommentOnColumn {
		if len(parts) == 1 {
			return nil, &sqlError{msg: "column name must be qualified"}
		}
		rel, st.Column = parts[:len(parts)-1], parts[len(parts)-1]
	}
	var ok bool
	if st.Table, ok = tableNameOf(rel); !ok {
		return nil, &sqlError{msg: "improper qualified name (too many dotted names): " + strings.Join(parts, ".")}
	}
	if err := p.expectKeyword("IS"); err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind == tokString {
		p.pos++
		st.Text = t.text
	} else if !p.acceptKeyword("NULL") {
		return nil, syntaxErr("expected a string or NULL near %q", t.text)
	}
	return st, nil
}

// parseDropBehavior accepts an optional CASCADE or RESTRICT and reports
// whether it was CASCADE. Only DROP SCHEMA acts on it: the driver doesn't
// track dependencies between tables and views, so dropping a table or view
// never drops (or checks for) views that read it.
func (p *parser) parseDropBehavior() bool {
	if p.acceptKeyword("CASCADE") {
		return true
	}
	p.acceptKeyword("RESTRICT")
	return false
}

func (p *parser) parseTruncate() (Stmt, error) {
	if err := p.expectKeyword("TRUNCATE"); err != nil {
		return nil, err
	}
	p.acceptKeyword("TABLE")
	p.acceptKeyword("ONLY")
	st := &TruncateStmt{}
	for {
		t, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		st.Tables = append(st.Tables, t)
		if !p.acceptOp(",") {
			break
		}
	}
	switch {
	case p.acceptKeyword("RESTART", "IDENTITY"):
		st.RestartIdentity = true
	case p.acceptKeyword("CONTINUE", "IDENTITY"):
	}
	// There are no foreign keys, so CASCADE and RESTRICT mean the same.
	p.parseDropBehavior()
	return st, nil
}

// parseUpdate parses UPDATE t [[AS] alias] SET … [FROM …] [WHERE …].
func (p *parser) parseUpdate() (*UpdateStmt, error) {
	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}
	t, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	st := &UpdateStmt{Table: t}
	if st.Alias, err = p.parseAlias(); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	if st.Sets, err = p.parseSetList(); err != nil {
		return nil, err
	}
	if p.acceptKeyword("FROM") {
		if st.From, err = p.parseFromList(); err != nil {
			return nil, err
		}
	}
	if p.acceptKeyword("WHERE") {
		if st.Where, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// parseSetList parses `[qualifier.]column = expr, …`, where an item may
// also assign a row to a column list (see parseRowAssignment).
func (p *parser) parseSetList() ([]SetClause, error) {
	var sets []SetClause
	for {
		if p.isOp("(") {
			row, err := p.parseRowAssignment()
			if err != nil {
				return nil, err
			}
			sets = append(sets, row...)
			if !p.acceptOp(",") {
				return sets, nil
			}
			continue
		}
		col, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		s := SetClause{Column: col}
		if p.acceptOp(".") {
			if s.Column, err = p.parseIdent(); err != nil {
				return nil, err
			}
			s.Qualifier = col
		}
		if err := p.expectOp("="); err != nil {
			return nil, err
		}
		if s.Expr, err = p.parseExpr(); err != nil {
			return nil, err
		}
		sets = append(sets, s)
		if !p.acceptOp(",") {
			return sets, nil
		}
	}
}

// parseRowAssignment parses `([qualifier.]column, …) = source` in a SET
// list. As in Postgres, the source is a row constructor, `ROW(…)` or `(x, y,
// …)` with at least two values, or a subquery returning one row of a value
// per column. It returns a SetClause per column, set to its value of the row
// constructor or to a RowColumn of the (shared) subquery.
func (p *parser) parseRowAssignment() ([]SetClause, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	var sets []SetClause
	for {
		col, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		s := SetClause{Column: col}
		if p.acceptOp(".") {
			if s.Column, err = p.parseIdent(); err != nil {
				return nil, err
			}
			s.Qualifier = col
		}
		sets = append(sets, s)
		if !p.acceptOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	if err := p.expectOp("="); err != nil {
		return nil, err
	}
	if p.isOp("(") && (p.isKeywordAt(1, "SELECT") || p.isKeywordAt(1, "WITH")) {
		p.pos++
		sel, err := p.parseSubquery()
		if err != nil {
			return nil, err
		}
		sq := &Subquery{Select: sel, Kind: SubqueryScalar, Width: len(sets)}
		for i := range sets {
			sets[i].Expr = &RowColumn{Sub: sq, Index: i}
		}
		return sets, nil
	}
	explicit := p.isKeyword("ROW") && p.peekAt(1).kind == tokOp && p.peekAt(1).text == "("
	if explicit {
		p.pos++
	}
	if !p.acceptOp("(") {
		return nil, &sqlError{msg: "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression"}
	}
	var values []Expr
	for !p.isOp(")") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		values = append(values, e)
		if !p.acceptOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	if !explicit && len(values) < 2 {
		// A single parenthesized value is just that value, not a row.
		return nil, &sqlError{msg: "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression"}
	}
	if len(values) != len(sets) {
		return nil, &sqlError{msg: "number of columns does not match number of values"}
	}
	for i := range sets {
		sets[i].Expr = values[i]
	}
	return sets, nil
}

// parseReturning parses the RETURNING list that may end an INSERT, UPDATE,
// DELETE or MERGE, written like a select list (`*`, `t.*`, `expr [[AS]
// alias]`), into the statement.
func (p *parser) parseReturning(st Stmt) error {
	var list *[]SelectItem
	switch st := st.(type) {
	case *InsertStmt:
		list = &st.Returning
	case *UpdateStmt:
		list = &st.Returning
	case *DeleteStmt:
		list = &st.Returning
	case *MergeStmt:
		list = &st.Returning
	}
	if list == nil || !p.acceptKeyword("RETURNING") {
		return nil
	}
	for {
		start := p.peek().pos
		if p.acceptOp("*") {
			*list = append(*list, SelectItem{Star: true, Text: "*"})
		} else if q, ok := p.parseQualifiedStar(); ok {
			*list = append(*list, SelectItem{Star: true, StarOf: q, Text: strings.TrimSpace(p.src[start:p.toks[p.pos-1].end])})
		} else {
			e, err := p.parseExpr()
			if err != nil {
				return err
			}
			item := SelectItem{Expr: e, Text: strings.TrimSpace(p.src[start:p.toks[p.pos-1].end])}
			if item.Alias, err = p.parseAlias(); err != nil {
				return err
			}
			*list = append(*list, item)
		}
		if !p.acceptOp(",") {
			return nil
		}
	}
}

// parseDelete parses DELETE FROM t [[AS] alias] [USING …] [WHERE …].
func (p *parser) parseDelete() (*DeleteStmt, error) {
	if err := p.expectKeyword("DELETE", "FROM"); err != nil {
		return nil, err
	}
	t, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	st := &DeleteStmt{Table: t}
	if st.Alias, err = p.parseAlias(); err != nil {
		return nil, err
	}
	if p.acceptKeyword("USING") {
		if st.Using, err = p.parseFromList(); err != nil {
			return nil, err
		}
	}
	if p.acceptKeyword("WHERE") {
		if st.Where, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// parseMerge parses
//
//	MERGE [INTO] t [[AS] alias] USING source [[AS] alias] ON cond
//	{ WHEN MATCHED [AND cond] THEN { UPDATE SET … | DELETE | DO NOTHING }
//	| WHEN NOT MATCHED [BY TARGET] [AND cond] THEN
//	    { INSERT [(cols)] { VALUES (…) | DEFAULT VALUES } | DO NOTHING }
//	| WHEN NOT MATCHED BY SOURCE [AND cond] THEN { UPDATE SET … | DELETE | DO NOTHING } } …
func (p *parser) parseMerge() (*MergeStmt, error) {
	if err := p.expectKeyword("MERGE"); err != nil {
		return nil, err
	}
	p.acceptKeyword("INTO")
	t, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	st := &MergeStmt{Table: t}
	if st.Alias, err = p.parseAlias(); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("USING"); err != nil {
		return nil, err
	}
	if st.Source.Table, st.Source.Select, st.Source.Alias, err = p.parseFromItem(); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	if st.On, err = p.parseExpr(); err != nil {
		return nil, err
	}
	for p.acceptKeyword("WHEN") {
		var c MergeClause
		switch {
		case p.acceptKeyword("MATCHED"):
			c.Match = MergeMatched
		case p.acceptKeyword("NOT", "MATCHED"):
			c.Match = MergeNotMatched
			if p.acceptKeyword("BY", "SOURCE") {
				c.Match = MergeNotMatchedBySource
			} else {
				p.acceptKeyword("BY", "TARGET")
			}
		default:
			return nil, syntaxErr("expected MATCHED or NOT MATCHED after WHEN near %q", p.peek().text)
		}
		if p.acceptKeyword("AND") {
			if c.Cond, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}
		if err := p.expectKeyword("THEN"); err != nil {
			return nil, err
		}
		action := strings.ToUpper(p.peek().text)
		switch {
		case p.acceptKeyword("UPDATE"):
			if err := p.expectKeyword("SET"); err != nil {
				return nil, err
			}
			if c.Sets, err = p.parseSetList(); err != nil {
				return nil, err
			}
			c.Action = MergeUpdate
		case p.acceptKeyword("DELETE"):
			c.Action = MergeDelete
		case p.acceptKeyword("INSERT"):
			c.Action = MergeInsert
			if p.acceptKeyword("DEFAULT", "VALUES") {
				break
			}
			if c.Columns, err = p.parseColumnList(); err != nil {
				return nil, err
			}
			if err := p.expectKeyword("VALUES"); err != nil {
				return nil, err
			}
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			for {
				e, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				c.Values = append(c.Values, e)
				if !p.acceptOp(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
		case p.acceptKeyword("DO", "NOTHING"):
			c.Action = MergeDoNothing
		default:
			return nil, syntaxErr("expected UPDATE, DELETE, INSERT or DO NOTHING after THEN near %q", p.peek().text)
		}
		switch {
		case c.Action == MergeInsert && c.Match != MergeNotMatched:
			return nil, syntaxErr("INSERT is only allowed in WHEN NOT MATCHED [BY TARGET] clauses")
		case c.Action != MergeInsert && c.Action != MergeDoNothing && c.Match == MergeNotMatched:
			return nil, syntaxErr("%s is not allowed in WHEN NOT MATCHED [BY TARGET] clauses (only INSERT or DO NOTHING)", action)
		}
		st.Clauses = append(st.Clauses, c)
	}
	if len(st.Clauses) == 0 {
		return nil, syntaxErr("MERGE needs at least one WHEN clause near %q", p.peek().text)
	}
	return st, nil
}

// parseColumnList parses an optional parenthesized list of column names.
func (p *parser) parseColumnList() ([]string, error) {
	if !p.acceptOp("(") {
		return nil, nil
	}
	var cols []string
	for {
		c, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		cols = append(cols, c)
		if !p.acceptOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return cols, nil
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
	if spec.Name == "INTERVAL" {
		// Optional field qualifier (INTERVAL DAY TO SECOND etc.); intervals
		// always carry months, days and nanoseconds.
		if isIntervalUnit(p.peek()) {
			p.pos++
			if p.acceptKeyword("TO") {
				if !isIntervalUnit(p.peek()) {
					return sqlTypeSpec{}, syntaxErr("expected an interval field after TO")
				}
				p.pos++
			}
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

// isIntervalUnit reports whether a token names an interval field.
func isIntervalUnit(t token) bool {
	if t.kind != tokIdent {
		return false
	}
	_, ok := intervalUnits[strings.ToLower(t.text)]
	return ok
}

// parseInterval parses what follows INTERVAL:
//
//	INTERVAL '1 day 2 hours'           a literal
//	INTERVAL '2' HOUR                  string with a unit
//	INTERVAL '1-2' YEAR TO MONTH       (the TO part is accepted; the string decides)
//	INTERVAL 7 DAY / INTERVAL ? DAY    an expression with a unit
func (p *parser) parseInterval() (Expr, error) {
	if t := p.peek(); t.kind == tokString {
		p.pos++
		if !isIntervalUnit(p.peek()) {
			v, err := intervalLiteral(t.text)
			if err != nil {
				return nil, &sqlError{msg: err.Error()}
			}
			return &Literal{V: v}, nil
		}
		unit := strings.ToLower(p.next().text)
		if p.acceptKeyword("TO") {
			if !isIntervalUnit(p.peek()) {
				return nil, syntaxErr("expected an interval field after TO")
			}
			p.pos++
			v, err := intervalLiteral(t.text)
			if err != nil {
				return nil, &sqlError{msg: err.Error()}
			}
			return &Literal{V: v}, nil
		}
		return &Func{Name: "__INTERVAL", Args: []Expr{&Literal{V: stringValue(t.text)}, &Literal{V: stringValue(unit)}}}, nil
	}
	x, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	if !isIntervalUnit(p.peek()) {
		return nil, syntaxErr("INTERVAL needs a quoted value or a unit (e.g. INTERVAL '1 day' or INTERVAL 1 DAY)")
	}
	unit := strings.ToLower(p.next().text)
	return &Func{Name: "__INTERVAL", Args: []Expr{x, &Literal{V: stringValue(unit)}}}, nil
}

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
	l, err := p.parseOtherOp()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == tokOp {
			switch t.text {
			case "=", "==", "<>", "!=", "<", "<=", ">", ">=":
				p.pos++
				op := t.text
				switch op {
				case "==":
					op = "="
				case "!=":
					op = "<>"
				}
				if q, ok, err := p.parseQuantified(l, op); err != nil {
					return nil, err
				} else if ok {
					l = q
					continue
				}
				r, err := p.parseOtherOp()
				if err != nil {
					return nil, err
				}
				l = &Binary{Op: op, L: l, R: r}
				continue
			}
		}
		if p.acceptKeyword("IS") {
			not := p.acceptKeyword("NOT")
			if p.acceptKeyword("JSON") {
				l = p.parseIsJSON(l)
				if not {
					l = &Unary{Op: "NOT", X: l}
				}
				continue
			}
			if !p.acceptKeyword("NULL") {
				return nil, syntaxErr("expected NULL after IS")
			}
			l = &IsNull{X: l, Not: not}
			continue
		}
		if p.isKeyword("LIKE") || p.isKeyword("ILIKE") ||
			(p.isKeyword("NOT") && (p.isKeywordAt(1, "LIKE") || p.isKeywordAt(1, "ILIKE"))) {
			not := p.acceptKeyword("NOT")
			name := "LIKE"
			if p.acceptKeyword("ILIKE") {
				name = "ILIKE"
			} else {
				p.acceptKeyword("LIKE")
			}
			pat, err := p.parseOtherOp()
			if err != nil {
				return nil, err
			}
			f := &Func{Name: name, Args: []Expr{l, pat}}
			if p.acceptKeyword("ESCAPE") {
				esc, err := p.parseOtherOp()
				if err != nil {
					return nil, err
				}
				f.Args = append(f.Args, esc)
			}
			l = f
			if not {
				l = &Unary{Op: "NOT", X: f}
			}
			continue
		}
		if (p.isKeyword("SIMILAR") && p.isKeywordAt(1, "TO")) ||
			(p.isKeyword("NOT") && p.isKeywordAt(1, "SIMILAR") && p.isKeywordAt(2, "TO")) {
			// [NOT] SIMILAR TO pattern [ESCAPE e]
			not := p.acceptKeyword("NOT")
			p.pos += 2
			pat, err := p.parseOtherOp()
			if err != nil {
				return nil, err
			}
			f := &Func{Name: "SIMILAR TO", Args: []Expr{l, pat}}
			if p.acceptKeyword("ESCAPE") {
				esc, err := p.parseOtherOp()
				if err != nil {
					return nil, err
				}
				f.Args = append(f.Args, esc)
			}
			l = f
			if not {
				l = &Unary{Op: "NOT", X: f}
			}
			continue
		}
		if p.isKeyword("BETWEEN") || (p.isKeyword("NOT") && p.isKeywordAt(1, "BETWEEN")) {
			not := p.acceptKeyword("NOT")
			p.acceptKeyword("BETWEEN")
			lo, err := p.parseOtherOp()
			if err != nil {
				return nil, err
			}
			if err := p.expectKeyword("AND"); err != nil {
				return nil, err
			}
			hi, err := p.parseOtherOp()
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

// parseOtherOp parses the operators Postgres puts at its "any other
// operator" precedence: ||, the regular-expression matches ~, ~*, !~ and
// !~*, and the JSON operators ->, ->>, #> and #>>, all left to right. They
// bind tighter than comparisons, IS, LIKE, BETWEEN and IN, and looser than
// arithmetic: `s ~ 'a' || 'b'` is (s ~ 'a') || 'b', j ->> 'a' || 'b' is
// (j ->> 'a') || 'b', and 'n=' || 1 + 1 is 'n=2'. `x ~ p` is the function
// "~" (case-sensitive), `x ~* p` is "~*", and the negated forms are NOT
// around them; the JSON operators are functions too (json.go).
func (p *parser) parseOtherOp() (Expr, error) {
	l, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind != tokOp {
			return l, nil
		}
		switch t.text {
		case "||", "~", "~*", "!~", "!~*", "->", "->>", "#>", "#>>":
		default:
			return l, nil
		}
		p.pos++
		r, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		switch t.text {
		case "||":
			l = &Binary{Op: "||", L: l, R: r}
		case "->", "->>", "#>", "#>>":
			l = &Func{Name: t.text, Args: []Expr{l, r}}
		default:
			l = &Func{Name: strings.TrimPrefix(t.text, "!"), Args: []Expr{l, r}}
			if strings.HasPrefix(t.text, "!") {
				l = &Unary{Op: "NOT", X: l}
			}
		}
	}
}

// parseQuantified parses `ANY | SOME | ALL (SELECT …)` after the comparison
// operator op (ok is false if they don't follow). `= ANY` is IN and `<> ALL`
// is NOT IN, so that they share IN's index unions, hash sets and semi-joins.
func (p *parser) parseQuantified(x Expr, op string) (Expr, bool, error) {
	all := p.isKeyword("ALL")
	if !(all || p.isKeyword("ANY") || p.isKeyword("SOME")) || !(p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(") {
		return nil, false, nil
	}
	word := strings.ToUpper(p.peek().text)
	p.pos += 2
	if !p.isQueryStart() && !p.isParenQueryStart() {
		return nil, false, syntaxErr("%s (…) needs a subquery near %q", word, p.peek().text)
	}
	sub, err := p.parseSubquery()
	if err != nil {
		return nil, false, err
	}
	switch {
	case op == "=" && !all:
		return &Subquery{Select: sub, Kind: SubqueryIn, X: x}, true, nil
	case op == "<>" && all:
		return &Subquery{Select: sub, Kind: SubqueryIn, X: x, Not: true}, true, nil
	case all:
		return &Subquery{Select: sub, Kind: SubqueryAll, X: x, Op: op}, true, nil
	}
	return &Subquery{Select: sub, Kind: SubqueryAny, X: x, Op: op}, true, nil
}

func (p *parser) parseAdditive() (Expr, error) {
	l, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.isOp("+"), p.isOp("-"):
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
			if op == "%" {
				// a % b is MOD(a, b), with its result type and its error on
				// a zero divisor.
				l = &Func{Name: "MOD", Args: []Expr{l, r}}
				continue
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
		if j, ok := jsonCast(spec, x); ok {
			x = j
			continue
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
		case "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_TIME", "LOCALTIMESTAMP", "LOCALTIME":
			// SQL-standard niladic functions (no parentheses needed).
			if !(p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(") {
				p.pos++
				return &Func{Name: upper}, nil
			}
		case "EXTRACT":
			// EXTRACT(field FROM expr) is DATE_PART('field', expr).
			if p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(" {
				p.pos += 2
				ft := p.next()
				if ft.kind != tokIdent && ft.kind != tokString && ft.kind != tokQuotedIdent {
					return nil, syntaxErr("expected a field name in EXTRACT near %q", ft.text)
				}
				if err := p.expectKeyword("FROM"); err != nil {
					return nil, err
				}
				x, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				if err := p.expectOp(")"); err != nil {
					return nil, err
				}
				return &Func{Name: "DATE_PART", Args: []Expr{&Literal{V: stringValue(strings.ToLower(ft.text))}, x}}, nil
			}
		case "SUBSTRING", "POSITION", "TRIM":
			// SUBSTRING(s FROM a FOR b), POSITION(a IN b), TRIM(BOTH x FROM s)
			if p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(" {
				p.pos += 2
				switch upper {
				case "SUBSTRING":
					return p.parseSubstring()
				case "POSITION":
					return p.parsePosition()
				}
				return p.parseTrim()
			}
		case "EXISTS":
			if p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(" {
				p.pos += 2
				sub, err := p.parseSubquery()
				if err != nil {
					return nil, err
				}
				return &Subquery{Select: sub, Kind: SubqueryExists}, nil
			}
		case "CAST", "TRY_CAST", "SAFE_CAST":
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
				var onError Expr
				if upper != "CAST" {
					onError = &Literal{V: nullValue(typeNull)}
				} else if p.acceptKeyword("DEFAULT") {
					// CAST(x AS t DEFAULT v ON CONVERSION ERROR), as in Oracle.
					if onError, err = p.parseExpr(); err != nil {
						return nil, err
					}
					if err := p.expectKeyword("ON", "CONVERSION", "ERROR"); err != nil {
						return nil, err
					}
				}
				if err := p.expectOp(")"); err != nil {
					return nil, err
				}
				if j, ok := jsonCast(spec, x); ok {
					return j, nil
				}
				ct, err := colTypeFromSQL(spec)
				if err != nil {
					return nil, &sqlError{msg: err.Error()}
				}
				return &Cast{X: x, T: ct, OnError: onError}, nil
			}
		case "DATEADD", "DATE_ADD", "DATE_SUB", "TIMESTAMPADD", "DATEDIFF", "TIMESTAMPDIFF":
			if p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(" {
				p.pos += 2
				return p.parseDateArith(upper)
			}
		case "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS", "JSON_OBJECT", "JSON_ARRAY":
			// SQL/JSON functions, with clauses such as RETURNING and ON ERROR.
			if p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(" {
				p.pos += 2
				return p.parseSQLJSONFunc(upper)
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
			p.pos++
			return p.parseInterval()
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
				if err := p.parseCallArgSuffix(f); err != nil {
					return nil, err
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			if err := p.parseCallSuffix(f); err != nil {
				return nil, err
			}
			if p.acceptKeyword("OVER") {
				spec, err := p.parseOver()
				if err != nil {
					return nil, err
				}
				return &WindowFunc{Func: f, Over: spec}, nil
			}
			return f, nil
		}
		return p.parseColumnRef(t.text)
	}
	return nil, syntaxErr("unexpected end of input")
}

// finishCall parses the remaining `, arg … )` of a function call whose first
// argument has been parsed.
func (p *parser) finishCall(name string, first Expr) (Expr, error) {
	f := &Func{Name: name, Args: []Expr{first}}
	for p.acceptOp(",") {
		a, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		f.Args = append(f.Args, a)
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return f, nil
}

// parseCallArgSuffix parses what may follow a call's arguments inside its
// parentheses: an aggregate's ORDER BY (STRING_AGG(x, ',' ORDER BY y)) and
// IGNORE NULLS / RESPECT NULLS (FIRST_VALUE(x IGNORE NULLS), as in BigQuery
// and DuckDB), in either order.
func (p *parser) parseCallArgSuffix(f *Func) error {
	if err := p.parseNullTreatment(f); err != nil {
		return err
	}
	if p.acceptKeyword("ORDER", "BY") {
		items, err := p.parseOrderItems()
		if err != nil {
			return err
		}
		f.OrderBy = items
	}
	return p.parseNullTreatment(f)
}

// parseCallSuffix parses what may follow a call's closing parenthesis, in
// this order: WITHIN GROUP (ORDER BY …), FILTER (WHERE …) and IGNORE NULLS /
// RESPECT NULLS (as in the SQL standard). The caller parses OVER.
func (p *parser) parseCallSuffix(f *Func) error {
	if p.acceptKeyword("WITHIN", "GROUP") {
		if len(f.OrderBy) > 0 {
			return &sqlError{msg: "cannot use multiple ORDER BY clauses with WITHIN GROUP"}
		}
		if err := p.expectOp("("); err != nil {
			return err
		}
		if err := p.expectKeyword("ORDER", "BY"); err != nil {
			return err
		}
		items, err := p.parseOrderItems()
		if err != nil {
			return err
		}
		if err := p.expectOp(")"); err != nil {
			return err
		}
		f.OrderBy, f.WithinGroup = items, true
	}
	// FILTER is also a valid column alias (`COUNT(*) filter`).
	if p.isKeyword("FILTER") && p.peekAt(1).kind == tokOp && p.peekAt(1).text == "(" {
		p.pos += 2
		if err := p.expectKeyword("WHERE"); err != nil {
			return err
		}
		cond, err := p.parseExpr()
		if err != nil {
			return err
		}
		if err := p.expectOp(")"); err != nil {
			return err
		}
		f.Filter = cond
	}
	return p.parseNullTreatment(f)
}

func (p *parser) parseNullTreatment(f *Func) error {
	nt := NullsUnspecified
	switch {
	case p.acceptKeyword("IGNORE", "NULLS"):
		nt = IgnoreNulls
	case p.acceptKeyword("RESPECT", "NULLS"):
		nt = RespectNulls
	default:
		return nil
	}
	if f.Nulls != NullsUnspecified {
		return syntaxErr("IGNORE NULLS / RESPECT NULLS specified more than once")
	}
	f.Nulls = nt
	return nil
}

// parseSubstring parses what follows `SUBSTRING(`: either an argument list
// or the SQL-standard `s FROM start [FOR count]` / `s FOR count`, or
// `s SIMILAR pattern ESCAPE e`. As in Postgres, the last is SUBSTRING(s,
// pattern, e), and the argument types decide which form runs (text
// arguments for FROM … FOR … also mean a SIMILAR TO pattern and escape).
func (p *parser) parseSubstring() (Expr, error) {
	s, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.acceptKeyword("SIMILAR") {
		pat, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("ESCAPE"); err != nil {
			return nil, err
		}
		esc, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		return &Func{Name: "SUBSTRING", Args: []Expr{s, pat, esc}}, nil
	}
	if !p.isKeyword("FROM") && !p.isKeyword("FOR") {
		return p.finishCall("SUBSTRING", s)
	}
	var start Expr = &Literal{V: intValue(typeInt64, 1)}
	if p.acceptKeyword("FROM") {
		if start, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	f := &Func{Name: "SUBSTRING", Args: []Expr{s, start}}
	if p.acceptKeyword("FOR") {
		count, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		f.Args = append(f.Args, count)
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return f, nil
}

// parsePosition parses what follows `POSITION(`: `sub IN s` (or `sub, s`).
// The operands cannot contain comparisons, so that IN is not read as the
// IN operator.
func (p *parser) parsePosition() (Expr, error) {
	sub, err := p.parseOtherOp()
	if err != nil {
		return nil, err
	}
	if !p.acceptKeyword("IN") {
		return p.finishCall("POSITION", sub)
	}
	s, err := p.parseOtherOp()
	if err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return &Func{Name: "POSITION", Args: []Expr{sub, s}}, nil
}

// parseTrim parses what follows `TRIM(`:
//
//	[BOTH | LEADING | TRAILING] [chars] FROM s
//	s [, chars]
//
// into BTRIM / LTRIM / RTRIM(s [, chars]).
func (p *parser) parseTrim() (Expr, error) {
	name := "BTRIM"
	if p.isKeyword("BOTH") || p.isKeyword("LEADING") || p.isKeyword("TRAILING") {
		// A column of that name is followed by an operator, `,` or `)`.
		if next := p.peekAt(1); next.kind != tokOp || next.text == "(" {
			switch strings.ToUpper(p.next().text) {
			case "LEADING":
				name = "LTRIM"
			case "TRAILING":
				name = "RTRIM"
			}
		}
	}
	var s, chars Expr
	var err error
	if p.acceptKeyword("FROM") {
		if s, err = p.parseExpr(); err != nil {
			return nil, err
		}
	} else {
		x, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		switch {
		case p.acceptKeyword("FROM"):
			chars = x
			if s, err = p.parseExpr(); err != nil {
				return nil, err
			}
		case p.acceptOp(","):
			s = x
			if chars, err = p.parseExpr(); err != nil {
				return nil, err
			}
		default:
			s = x
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	f := &Func{Name: name, Args: []Expr{s}}
	if chars != nil {
		f.Args = append(f.Args, chars)
	}
	return f, nil
}

// parseDateArith parses what follows `DATEADD(`, `DATEDIFF(` and their
// aliases. A bare date part is the part's name, not a column:
// DATEADD(day, 1, x) is DATEADD('day', 1, x). DATE_ADD(x, INTERVAL n part)
// and DATE_SUB(x, INTERVAL n part) (MySQL, BigQuery) become
// DATE_ADD('part', n, x) and DATE_SUB('part', n, x).
func (p *parser) parseDateArith(name string) (Expr, error) {
	var first Expr
	bare := ""
	var bareTok token
	if t := p.peek(); t.kind == tokIdent && p.peekAt(1).kind == tokOp && p.peekAt(1).text == "," {
		if _, ok := datePartOf(t.text); ok {
			p.pos++
			bare, bareTok = t.text, t
			first = &Literal{V: stringValue(strings.ToLower(t.text))}
		}
	}
	if first == nil {
		var err error
		if first, err = p.parseExpr(); err != nil {
			return nil, err
		}
	}
	call, err := p.finishCall(name, first)
	if err != nil {
		return nil, err
	}
	f := call.(*Func)
	if (name != "DATE_ADD" && name != "DATE_SUB") || (name == "DATE_ADD" && len(f.Args) == 3) {
		return f, nil
	}
	if len(f.Args) == 2 {
		if iv, ok := f.Args[1].(*Func); ok && iv.Name == "__INTERVAL" {
			x := f.Args[0]
			if bare != "" {
				ref := &ColumnRef{Name: bare} // a column named like a date part
				if p.refs != nil {
					*p.refs = append(*p.refs, refSpan{ref: ref, start: bareTok.pos, name: bareTok.pos, end: bareTok.end})
				}
				x = ref
			}
			return &Func{Name: name, Args: []Expr{iv.Args[1], iv.Args[0], x}}, nil
		}
	}
	if name == "DATE_ADD" {
		return nil, syntaxErr("DATE_ADD expects (part, n, x) or (x, INTERVAL n part)")
	}
	return nil, syntaxErr("DATE_SUB expects (x, INTERVAL n part)")
}

// parseColumnRef parses the rest of [schema.][table.]column after its first
// part; the part just before the column is kept as the qualifier.
func (p *parser) parseColumnRef(first string) (Expr, error) {
	start := p.toks[p.pos-1].pos
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
	if p.refs != nil {
		last := p.toks[p.pos-1]
		*p.refs = append(*p.refs, refSpan{ref: ref, start: start, name: last.pos, end: last.end})
	}
	return ref, nil
}
