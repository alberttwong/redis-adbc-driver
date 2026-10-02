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

// SEARCH and CYCLE clauses of recursive CTEs.
//
//	SEARCH {BREADTH | DEPTH} FIRST BY col, … SET seq
//	CYCLE col, … SET mark [TO value DEFAULT value] USING path
//
// add columns computed from the chain of rows that produced each row:
//
//   - seq orders the rows breadth first (by iteration, then the BY columns)
//     or depth first (by the BY columns of the rows from the root down). It
//     is the row's position in that order, a BIGINT, where Postgres has a
//     record or an array that sorts the same way.
//   - mark is TRUE (or the TO value) when the row's CYCLE columns equal
//     those of a row on its path, otherwise FALSE (or the DEFAULT value),
//     and such a row is not recursed into. Columns with a NULL never match.
//   - path is the text of the CYCLE columns of the rows from the root to
//     this one, rendered like Postgres's array of records: {(1),(2)}.
//
// As Postgres requires, the recursive term is a SELECT that reads the CTE at
// its top level, so each row it returns comes from one working table row. It
// is run once per working table row to tell which: one round trip per row
// rather than per iteration.

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/apache/arrow-adbc/go/adbc"
)

// tracer computes the SEARCH and CYCLE columns of a recursive CTE.
type tracer struct {
	def *CTE
	// search and cycle are the positions of the BY and CYCLE columns.
	search, cycle []int
	// mark and unmark are the CYCLE mark values, of type markType.
	mark, unmark Value
	markType     ColType
	traces       []trace // per result row
	zone         tzZone  // the session time zone of the path's text
}

// trace is what a row's SEARCH and CYCLE columns are computed from.
type trace struct {
	depth int // iterations since the non-recursive term
	// order are the BY columns of the rows from the root down (depth first)
	// or of this row (breadth first); path are the CYCLE columns of the
	// rows from the root down.
	order, path [][]Value
	cycle       bool
}

// newTracer checks a recursive CTE's SEARCH and CYCLE clauses (with
// Postgres's messages) against its working table wt.
func (e *executor) newTracer(ctx context.Context, def *CTE, rc *recursionCheck, wt *tableMeta) (*tracer, error) {
	op := def.Select.SetOp
	bad := func(format string, args ...any) (*tracer, error) {
		return nil, errorf(adbc.StatusInvalidArgument, format, args...)
	}
	switch {
	case op.Left.SetOp != nil:
		return bad("with a SEARCH or CYCLE clause, the left side of the UNION must be a SELECT")
	case op.Right.SetOp != nil:
		return bad("with a SEARCH or CYCLE clause, the right side of the UNION must be a SELECT")
	case rc.refDepth != 1:
		return bad("with a SEARCH or CYCLE clause, the recursive reference to WITH query %q must be at the top level of its right-hand SELECT", def.Name)
	}
	r := op.Right
	windowed := r.Qualify != nil
	for _, it := range r.Items {
		windowed = windowed || containsWindow(it.Expr)
	}
	if len(r.GroupBy) > 0 || r.GroupingSets != nil || r.Having != nil || r.Limit != nil || r.Offset != nil || windowed {
		return nil, errorf(adbc.StatusNotImplemented,
			"SEARCH and CYCLE are not supported with GROUP BY, HAVING, window functions, LIMIT or OFFSET in the recursive term")
	}
	used := func(name string) bool {
		_, ok := wt.resolve(name)
		return ok
	}
	positions := func(what string, cols []string) ([]int, error) {
		var out []int
		for _, c := range cols {
			i, ok := wt.resolve(c)
			if !ok {
				return nil, errorf(adbc.StatusInvalidArgument, "%s column %q not in WITH query column list", what, c)
			}
			for _, j := range out {
				if j == i {
					return nil, errorf(adbc.StatusInvalidArgument, "%s column %q specified more than once", what, c)
				}
			}
			out = append(out, i)
		}
		return out, nil
	}
	tr := &tracer{def: def, mark: boolValue(true), unmark: boolValue(false), markType: typeBool, zone: e.zone()}
	var err error
	if s := def.Search; s != nil {
		if tr.search, err = positions("search", s.By); err != nil {
			return nil, err
		}
		if used(s.Set) {
			return bad("search sequence column name %q already used in WITH query column list", s.Set)
		}
	}
	if c := def.Cycle; c != nil {
		if tr.cycle, err = positions("cycle", c.Columns); err != nil {
			return nil, err
		}
		switch {
		case used(c.Set):
			return bad("cycle mark column name %q already used in WITH query column list", c.Set)
		case used(c.Using):
			return bad("cycle path column name %q already used in WITH query column list", c.Using)
		case strings.EqualFold(c.Set, c.Using):
			return bad("cycle mark column name and cycle path column name are the same")
		}
		if c.To != nil {
			env := e.newEnv(ctx, nil, e.params)
			vals := make([]Value, 2)
			for i, x := range []Expr{c.To, c.Default} {
				if _, err := e.bindIn(ctx, x, nil, "", ""); err != nil {
					return nil, err
				}
				if vals[i], err = env.eval(x); err != nil {
					return nil, invalidArg(err)
				}
			}
			t, ok := setOpType(vals[0].T, vals[1].T)
			if !ok {
				return bad("CYCLE types %s and %s cannot be matched", vals[0].T.SQLName(), vals[1].T.SQLName())
			}
			if tr.mark, err = Coerce(vals[0], t); err != nil {
				return nil, invalidArg(err)
			}
			if tr.unmark, err = Coerce(vals[1], t); err != nil {
				return nil, invalidArg(err)
			}
			tr.markType = t
		}
	}
	if s, c := def.Search, def.Cycle; s != nil && c != nil {
		switch {
		case strings.EqualFold(s.Set, c.Set):
			return bad("search sequence column name and cycle mark column name are the same")
		case strings.EqualFold(s.Set, c.Using):
			return bad("search sequence column name and cycle path column name are the same")
		}
	}
	return tr, nil
}

// columns returns the CTE's columns and names with the SEARCH and CYCLE
// columns added.
func (tr *tracer) columns(cols []resultColumn, names []string) ([]resultColumn, []string) {
	if s := tr.def.Search; s != nil {
		cols = append(cols, resultColumn{Name: s.Set, Type: typeInt64})
		names = append(names, s.Set)
	}
	if c := tr.def.Cycle; c != nil {
		cols = append(cols, resultColumn{Name: c.Set, Type: tr.markType}, resultColumn{Name: c.Using, Type: typeString})
		names = append(names, c.Set, c.Using)
	}
	return cols, names
}

func pick(row []Value, cols []int) []Value {
	out := make([]Value, len(cols))
	for i, c := range cols {
		out[i] = row[c]
	}
	return out
}

// root returns the trace of a row of the non-recursive term.
func (tr *tracer) root(row []Value) trace {
	return trace{order: [][]Value{pick(row, tr.search)}, path: [][]Value{pick(row, tr.cycle)}}
}

// child returns the trace of a row the recursive term returned for the row
// traced by parent.
func (tr *tracer) child(parent trace, row []Value) trace {
	t := trace{depth: parent.depth + 1}
	if tr.def.Search != nil {
		t.order = [][]Value{pick(row, tr.search)}
		if !tr.def.Search.Breadth {
			t.order = append(append(make([][]Value, 0, len(parent.order)+1), parent.order...), t.order[0])
		}
	}
	if tr.def.Cycle != nil {
		key := pick(row, tr.cycle)
		for _, p := range parent.path {
			if sameRow(p, key) {
				t.cycle = true
				break
			}
		}
		t.path = append(append(make([][]Value, 0, len(parent.path)+1), parent.path...), key)
	}
	return t
}

// sameRow reports whether ROW(a) = ROW(b) is true: no NULLs, all equal.
func sameRow(a, b []Value) bool {
	for i := range a {
		if a[i].Null || b[i].Null {
			return false
		}
		if c, ok := compareValues(a[i], b[i]); !ok || c != 0 {
			return false
		}
	}
	return true
}

// key identifies a trace's SEARCH and CYCLE columns, for UNION's duplicate
// rows.
func (tr *tracer) key(t trace) string {
	var b strings.Builder
	if tr.def.Search != nil {
		if tr.def.Search.Breadth {
			b.WriteString(strconv.Itoa(t.depth))
		}
		for _, o := range t.order {
			b.WriteString(rowKey(o))
			b.WriteByte(1)
		}
	}
	if tr.def.Cycle != nil {
		b.WriteString(strconv.FormatBool(t.cycle))
		for _, p := range t.path {
			b.WriteString(rowKey(p))
			b.WriteByte(1)
		}
	}
	return b.String()
}

// finish returns the rows with their SEARCH and CYCLE columns.
func (tr *tracer) finish(rows [][]Value) [][]Value {
	var seq []int64
	if s := tr.def.Search; s != nil {
		idx := make([]int, len(rows))
		for i := range idx {
			idx[i] = i
		}
		sort.SliceStable(idx, func(a, b int) bool {
			x, y := tr.traces[idx[a]], tr.traces[idx[b]]
			if s.Breadth && x.depth != y.depth {
				return x.depth < y.depth
			}
			return compareTuples(x.order, y.order) < 0
		})
		seq = make([]int64, len(rows))
		for rank, i := range idx {
			seq[i] = int64(rank + 1)
		}
	}
	out := make([][]Value, len(rows))
	for i, r := range rows {
		row := append(make([]Value, 0, len(r)+3), r...)
		if seq != nil {
			row = append(row, intValue(typeInt64, seq[i]))
		}
		if tr.def.Cycle != nil {
			mark := tr.unmark
			if tr.traces[i].cycle {
				mark = tr.mark
			}
			row = append(row, mark, stringValue(pathText(tr.traces[i].path, tr.zone)))
		}
		out[i] = row
	}
	return out
}

// compareTuples orders lists of rows as Postgres orders arrays of records:
// element by element, a prefix first; within a row column by column, NULLs
// last.
func compareTuples(a, b [][]Value) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		for j := range a[i] {
			x, y := a[i][j], b[i][j]
			switch {
			case x.Null && y.Null:
				continue
			case x.Null:
				return 1
			case y.Null:
				return -1
			}
			if c, _ := compareValues(x, y); c != 0 {
				return c
			}
		}
	}
	return cmpInt(int64(len(a)), int64(len(b)))
}

// pathText renders rows like Postgres's text of an array of records:
// {(1,a),"(2,b c)"}.
func pathText(path [][]Value, z tzZone) string {
	quote := func(s, special string) bool {
		if s == "" {
			return true
		}
		for _, r := range s {
			if strings.ContainsRune(special, r) || unicode.IsSpace(r) {
				return true
			}
		}
		return false
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, row := range path {
		if i > 0 {
			b.WriteByte(',')
		}
		var rec strings.Builder
		rec.WriteByte('(')
		for j, v := range row {
			if j > 0 {
				rec.WriteByte(',')
			}
			if v.Null {
				continue
			}
			s := v.textIn(z)
			if !quote(s, `"\(),`) {
				rec.WriteString(s)
				continue
			}
			rec.WriteByte('"')
			for _, r := range s {
				if r == '"' || r == '\\' {
					rec.WriteRune(r) // doubled
				}
				rec.WriteRune(r)
			}
			rec.WriteByte('"')
		}
		rec.WriteByte(')')
		s := rec.String()
		if !quote(s, `{},"\`) && !strings.EqualFold(s, "NULL") {
			b.WriteString(s)
			continue
		}
		b.WriteByte('"')
		for _, r := range s {
			if r == '"' || r == '\\' {
				b.WriteByte('\\')
			}
			b.WriteRune(r)
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}
