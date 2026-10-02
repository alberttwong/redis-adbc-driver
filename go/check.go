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

// CHECK constraints.
//
// CREATE TABLE takes CHECK (expr) on a column or as a table constraint,
// ALTER TABLE … ADD COLUMN on the new column, and ALTER TABLE … ADD
// [CONSTRAINT name] CHECK (expr) on the table. As in Postgres, all of them
// mean the same: a condition on the whole row. PRIMARY KEY, UNIQUE and
// REFERENCES / FOREIGN KEY are accepted and not enforced (nothing is kept).
//
//   - Definition: the expression is bound over the table alone, so it may
//     read any of the table's columns but not __rowid or another table, and
//     it may not use subqueries, parameters, aggregates, GROUPING or window
//     functions. Its calls are resolved as in a query (funcs.go), so an
//     unknown function or a wrong argument count is an error then. It must
//     be boolean. A constraint without a CONSTRAINT name is named as
//     Postgres does: <table>_<column>_check if the expression reads exactly
//     one column, otherwise <table>_check, with 1, 2, … appended to a name
//     the table already has.
//   - Storage: the name and the expression's text, in the table's metadata
//     (tableMeta.Checks; an optional field, so older metadata reads
//     unchanged). The text is the expression as written, except that a
//     qualified column reference (t.a) loses its qualifier, so RENAME TO
//     doesn't break it. RENAME COLUMN rewrites the references to the column
//     in the text (rewriteRefs), DROP COLUMN drops the constraints that read
//     the column, as Postgres does, and RENAME TO (also re-keying), TRUNCATE
//     and the other metadata changes keep them.
//   - Enforcement: every statement that writes rows parses and binds the
//     table's constraints once (tableChecks) and checks each new row (INSERT
//     … VALUES / SELECT / DEFAULT VALUES, MERGE's INSERT and bulk ingest)
//     and each updated row (UPDATE, UPDATE … FROM, MERGE's UPDATE) where it
//     checks NOT NULL: after NOT NULL, while the changes are computed and
//     before anything is written, so a violation writes nothing. A row
//     violates a constraint only if the expression is FALSE; NULL passes.
//     As in Postgres, the constraints are checked in name order (a row that
//     fails several reports the first), and an UPDATE checks all of them,
//     also those that don't read the columns it sets (it reads the columns
//     they use).
//   - ADD COLUMN and ADD CONSTRAINT check the existing rows first (the new
//     column reads its missing value, see defaults.go). Without locks, rows
//     written meanwhile by statements that read the metadata before the
//     constraint was added aren't checked against it.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// checkMeta is a CHECK constraint as the table metadata stores it.
type checkMeta struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
}

// rewriteRefs returns text, which starts at byte offset of the source the
// spans point into, with each column reference for which repl returns true
// replaced by what it returns.
func rewriteRefs(text string, offset int, refs []refSpan, repl func(refSpan) (string, bool)) string {
	refs = slices.Clone(refs)
	slices.SortFunc(refs, func(a, b refSpan) int { return a.start - b.start })
	var b strings.Builder
	last := 0
	for _, r := range refs {
		s, ok := repl(r)
		if !ok {
			continue
		}
		b.WriteString(text[last : r.start-offset])
		b.WriteString(s)
		last = r.end - offset
	}
	b.WriteString(text[last:])
	return b.String()
}

// parseCheckText parses the stored text of a CHECK constraint, and returns
// where its column references are, in the order written.
func parseCheckText(text string) (Expr, []refSpan, error) {
	toks, err := lex(text)
	if err != nil {
		return nil, nil, err
	}
	var refs []refSpan
	p := &parser{src: text, toks: toks, refs: &refs}
	x, err := p.parseExpr()
	if err != nil {
		return nil, nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, nil, syntaxErr("unexpected %q", p.peek().text)
	}
	slices.SortFunc(refs, func(a, b refSpan) int { return a.start - b.start })
	return x, refs, nil
}

// checkReads reports whether a stored CHECK reads the column. Column names
// are unique regardless of case, so comparing them that way finds the same
// column as name resolution does.
func checkReads(text, column string) bool {
	_, refs, err := parseCheckText(text)
	if err != nil {
		return false
	}
	for _, r := range refs {
		if strings.EqualFold(r.ref.Name, column) {
			return true
		}
	}
	return false
}

// checkColumns returns the columns of meta that a stored CHECK reads, in
// table order.
func checkColumns(meta *tableMeta, text string) []string {
	_, refs, err := parseCheckText(text)
	if err != nil {
		return nil
	}
	var out []string
	for _, c := range meta.Columns {
		if slices.ContainsFunc(refs, func(r refSpan) bool { return strings.EqualFold(r.ref.Name, c.Name) }) {
			out = append(out, c.Name)
		}
	}
	return out
}

// renameInCheck rewrites a stored CHECK for RENAME COLUMN from TO to. The
// new name is written as it is if that reads back as the same column
// references, and double-quoted otherwise (a keyword, or a name that isn't
// a plain identifier).
func renameInCheck(text, from, to string) (string, error) {
	_, refs, err := parseCheckText(text)
	if err != nil {
		return "", err
	}
	want := make([]string, len(refs))
	for i, r := range refs {
		want[i] = r.ref.Name
		if strings.EqualFold(r.ref.Name, from) {
			want[i] = to
		}
	}
	rename := func(name string) string {
		return rewriteRefs(text, 0, refs, func(r refSpan) (string, bool) {
			return name, strings.EqualFold(r.ref.Name, from)
		})
	}
	out := rename(to)
	if _, got, err := parseCheckText(out); err == nil && len(got) == len(want) {
		same := true
		for i, r := range got {
			same = same && r.ref.Name == want[i]
		}
		if same {
			return out, nil
		}
	}
	return rename(`"` + strings.ReplaceAll(to, `"`, `""`) + `"`), nil
}

// defineChecks checks new CHECK constraints of a table and names them (see
// the top of this file). meta has the table's columns, including one being
// added, and its constraints so far.
func (e *executor) defineChecks(ctx context.Context, meta *tableMeta, defs []CheckDef) ([]checkMeta, error) {
	used := map[string]bool{}
	for _, c := range meta.Checks {
		used[strings.ToLower(c.Name)] = true
	}
	// Explicit names first, so that no default name takes one.
	for i, d := range defs {
		if d.Name == "" {
			continue
		}
		if slices.ContainsFunc(defs[:i], func(o CheckDef) bool { return strings.EqualFold(o.Name, d.Name) }) {
			return nil, errorf(adbc.StatusAlreadyExists, "check constraint %q already exists", d.Name)
		}
		if used[strings.ToLower(d.Name)] {
			return nil, errorf(adbc.StatusAlreadyExists, "constraint %q for relation %q already exists", d.Name, meta.Name)
		}
	}
	for _, d := range defs {
		if d.Name != "" {
			used[strings.ToLower(d.Name)] = true
		}
	}
	out := make([]checkMeta, len(defs))
	for i, d := range defs {
		cols, err := e.checkCheck(ctx, meta, d.Expr)
		if err != nil {
			return nil, err
		}
		name := d.Name
		if name == "" {
			base := meta.Name
			if len(cols) == 1 {
				base += "_" + cols[0]
			}
			name = base + "_check"
			for n := 1; used[strings.ToLower(name)]; n++ {
				name = fmt.Sprintf("%s_check%d", base, n)
			}
			used[strings.ToLower(name)] = true
		}
		out[i] = checkMeta{Name: name, Expr: d.Text}
	}
	return out, nil
}

// checkCheck checks the expression of a new CHECK constraint of meta, as
// Postgres does, and returns the columns it reads, in table order.
func (e *executor) checkCheck(ctx context.Context, meta *tableMeta, x Expr) ([]string, error) {
	var err error
	walkExpr(x, func(n Expr) {
		if err != nil {
			return
		}
		switch v := n.(type) {
		case *Subquery:
			err = errorf(adbc.StatusInvalidArgument, "cannot use subquery in check constraint")
		case *Param:
			err = errorf(adbc.StatusInvalidArgument, "cannot use parameter in check constraint")
		case *WindowFunc:
			err = errorf(adbc.StatusInvalidArgument, "window functions are not allowed in check constraints")
		case *Func:
			switch {
			case aggregateFuncs[v.Name]:
				err = errorf(adbc.StatusInvalidArgument, "aggregate functions are not allowed in check constraints")
			case v.Name == "GROUPING":
				err = errorf(adbc.StatusInvalidArgument, "grouping operations are not allowed in check constraints")
			}
		case *ColumnRef:
			if v.Qualifier != "" {
				// The qualifier names the table as in a query on it: t.col,
				// s.t.col or catalog.s.t.col (see resolveColumn).
				err = e.bindCheck(ctx, meta, v)
			}
			if err == nil && strings.EqualFold(v.Name, rowIDField) {
				err = errorf(adbc.StatusInvalidArgument, "system column %q reference in check constraint is invalid", rowIDField)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	if err := e.bindCheck(ctx, meta, x); err != nil {
		return nil, err
	}
	t, err := inferType(x, meta.types(), nil)
	if err != nil {
		return nil, invalidArg(err)
	}
	if t.Kind != KindBool && t.Kind != KindNull {
		return nil, errorf(adbc.StatusInvalidArgument, "argument of CHECK must be type boolean, not type %s", strings.ToLower(t.SQLName()))
	}
	read := map[string]bool{}
	columnRefs(x, read)
	var cols []string
	for _, c := range meta.Columns {
		if read[c.Name] {
			cols = append(cols, c.Name)
		}
	}
	return cols, nil
}

// bindCheck binds a CHECK expression over meta alone, outside the scopes of
// the statement that is running.
func (e *executor) bindCheck(ctx context.Context, meta *tableMeta, x Expr) error {
	scopes, sq := e.scopes, e.pendingSq
	e.scopes, e.pendingSq = nil, nil
	defer func() { e.scopes, e.pendingSq = scopes, sq }()
	_, err := e.bindIn(ctx, x, meta, "")
	return err
}

// tableChecks are a table's CHECK constraints, bound for one statement that
// writes its rows. A nil *tableChecks (a table without any) passes every
// row.
type tableChecks struct {
	meta  *tableMeta
	env   *evalEnv
	exprs []Expr // like meta.Checks
	order []int  // meta.Checks by name, the order they are checked in
	cols  []int  // the columns they read
}

// tableChecks parses and binds meta's CHECK constraints; nil if it has none.
func (e *executor) tableChecks(ctx context.Context, meta *tableMeta) (*tableChecks, error) {
	if len(meta.Checks) == 0 {
		return nil, nil
	}
	tc := &tableChecks{meta: meta, env: &evalEnv{ctx: ctx, exec: e, types: meta.types()}}
	read := map[string]bool{}
	for _, c := range meta.Checks {
		x, err := parseExprText(c.Expr)
		if err == nil {
			err = e.bindCheck(ctx, meta, x)
		}
		if err != nil {
			// Say a client older than CHECK support renamed or dropped a
			// column it reads, or the constraint calls a function that
			// doesn't exist (stored before calls were checked).
			return nil, errorf(adbc.StatusInvalidState,
				"check constraint %q of relation %q can't be evaluated (%s); drop it with ALTER TABLE … DROP CONSTRAINT",
				c.Name, meta.Name, errText(err))
		}
		columnRefs(x, read)
		tc.exprs = append(tc.exprs, x)
	}
	for i, c := range meta.Columns {
		if read[c.Name] {
			tc.cols = append(tc.cols, i)
		}
	}
	// Postgres checks a table's constraints in name order, so a row that
	// fails several reports the same one.
	for i := range meta.Checks {
		tc.order = append(tc.order, i)
	}
	slices.SortStableFunc(tc.order, func(a, b int) int { return strings.Compare(meta.Checks[a].Name, meta.Checks[b].Name) })
	return tc, nil
}

// errText is an error's message without the driver's "[redis] " prefix.
func errText(err error) string {
	var ae adbc.Error
	if errors.As(err, &ae) {
		return strings.TrimPrefix(ae.Msg, "[redis] ")
	}
	return err.Error()
}

// need adds the columns the constraints read, named prefix+column, to need:
// an UPDATE reads them to check the updated rows.
func (tc *tableChecks) need(need map[string]bool, prefix string) {
	if tc == nil {
		return
	}
	for _, i := range tc.cols {
		need[prefix+tc.meta.Columns[i].Name] = true
	}
}

// check checks a new row, ordered like meta.Columns.
func (tc *tableChecks) check(row []Value) error {
	if tc == nil {
		return nil
	}
	m := make(map[string]Value, len(tc.cols))
	for _, i := range tc.cols {
		m[tc.meta.Columns[i].Name] = row[i]
	}
	return tc.checkRow(m)
}

// checkChanged checks an updated row: row as read (columns named
// prefix+column) with the columns cols set to vals.
func (tc *tableChecks) checkChanged(row map[string]Value, prefix string, cols []int, vals []Value) error {
	if tc == nil {
		return nil
	}
	m := make(map[string]Value, len(tc.cols))
	for _, i := range tc.cols {
		name := tc.meta.Columns[i].Name
		if v, ok := row[prefix+name]; ok {
			m[name] = v
		}
	}
	for j, i := range cols {
		m[tc.meta.Columns[i].Name] = vals[j]
	}
	return tc.checkRow(m)
}

// checkRow checks a row given as column name → value.
func (tc *tableChecks) checkRow(row map[string]Value) error {
	i, err := tc.violated(row)
	if err != nil || i < 0 {
		return err
	}
	return errorf(adbc.StatusIntegrity, "new row for relation %q violates check constraint %q", tc.meta.Name, tc.meta.Checks[i].Name)
}

// violated returns the first constraint that row violates, -1 if none.
func (tc *tableChecks) violated(row map[string]Value) (int, error) {
	tc.env.row = row
	for _, i := range tc.order {
		v, err := tc.env.eval(tc.exprs[i])
		if err != nil {
			return -1, invalidArg(err)
		}
		if b, ok := truthy(v); ok && !b {
			return i, nil
		}
	}
	return -1, nil
}

// validateChecks checks the rows of a table against new CHECK constraints
// (ALTER TABLE … ADD COLUMN … CHECK, ADD CONSTRAINT), as Postgres does.
// stored is the table as it is; with is the table the constraints are on,
// as the ALTER TABLE leaves it. A column of with that stored has (with the
// same field) is read from the rows; one the statement adds is in added,
// with the value every existing row reads for it.
func (e *executor) validateChecks(ctx context.Context, stored, with *tableMeta, checks []checkMeta, added map[string]Value) error {
	m := *with
	m.Checks = checks
	tc, err := e.tableChecks(ctx, &m)
	if err != nil || tc == nil {
		return err
	}
	need := map[string]bool{}
	for _, i := range tc.cols {
		if name := with.Columns[i].Name; !hasKey(added, name) {
			need[name] = true
		}
	}
	violation := func(i int) error {
		return errorf(adbc.StatusIntegrity, "check constraint %q of relation %q is violated by some row", checks[i].Name, with.Name)
	}
	if len(need) == 0 {
		// Only new columns are read, so every row gives the same result:
		// the table is read only to see whether it has any rows.
		i, err := tc.violated(maps.Clone(added))
		if err != nil || i < 0 {
			return err
		}
		if _, rows, err := e.matchRows(ctx, stored, "", nil, nil, nil); err != nil || len(rows) == 0 {
			return err
		}
		return violation(i)
	}
	_, rows, err := e.matchRows(ctx, stored, "", nil, nil, need)
	if err != nil {
		return err
	}
	for _, row := range rows {
		maps.Copy(row, added)
		i, err := tc.violated(row)
		if err != nil {
			return err
		}
		if i >= 0 {
			return violation(i)
		}
	}
	return nil
}

func hasKey[V any](m map[string]V, k string) bool {
	_, ok := m[k]
	return ok
}

// sameChecksColumns reports whether the columns that new constraints read
// are still in the table as they were when the constraints were defined
// (they are added in a later metadata update).
func sameChecksColumns(was, now *tableMeta, checks []checkMeta) bool {
	for _, c := range checks {
		for _, name := range checkColumns(was, c.Expr) {
			i, ok := was.resolve(name)
			j, ok2 := now.resolve(name)
			if !ok || !ok2 || now.Columns[j].Name != was.Columns[i].Name || now.Columns[j].field() != was.Columns[i].field() {
				return false
			}
		}
	}
	return true
}
