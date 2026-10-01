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

// NATURAL JOIN.
//
// `l NATURAL [kind] JOIN r` joins on equality of every column name l and r
// have in common (a cross join if there is none), and merges each such pair
// into one column, as Postgres does:
//
//   - SELECT * lists the merged columns first, in the left side's order, then
//     the left side's other columns, then the right side's. In a chain of
//     joins the left side is everything joined so far, with its own merged
//     columns.
//   - The merged column is the left column for INNER and LEFT joins, the
//     right column for RIGHT joins, and COALESCE(left, right) for FULL joins,
//     which the join computes after that step under a key of its own.
//   - An unqualified reference means the merged column; a qualified one
//     (l.c, r.c) still reads that side's own column.

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// mergedPrefix starts the row keys of the columns merged by a NATURAL FULL
// JOIN (no alias starts with a NUL).
const mergedPrefix = "\x00natural"

// mergedColumn is a column merged by the NATURAL FULL JOIN of join item
// item: expr (a COALESCE) is stored in the joined rows under key.
type mergedColumn struct {
	item int
	key  string
	expr Expr
	typ  ColType
}

// outputColumn is a column of the items joined so far, in SELECT * order:
// its name, its column of the joined relation (Name is the row key), and an
// expression for it over the items' columns.
type outputColumn struct {
	name string
	col  columnMeta
	expr Expr
}

// planNatural works out the conditions, merged columns, SELECT * order and
// hidden columns of the NATURAL JOINs (see the file comment), adding the
// relations of FULL joins' merged columns to rels. Errors are left in jp.err
// for bindJoin.
func (jp *joinPlan) planNatural(joined *tableMeta, rels []relation) []relation {
	natural := false
	for _, it := range jp.items {
		natural = natural || it.natural
	}
	if !natural {
		return rels
	}
	columnsOf := func(it *joinItem) []outputColumn {
		out := make([]outputColumn, len(it.base.Columns))
		for i, c := range it.base.Columns {
			out[i] = outputColumn{
				name: c.Name,
				col:  columnMeta{Name: it.prefix + c.Name, label: c.Name, Type: c.Type, Nullable: true},
				expr: &ColumnRef{Qualifier: it.alias, Name: c.Name},
			}
		}
		return out
	}
	count := func(cols []outputColumn, name string) int {
		n := 0
		for _, c := range cols {
			if strings.EqualFold(c.name, name) {
				n++
			}
		}
		return n
	}
	fail := func(err error) {
		if jp.err == nil {
			jp.err = err
		}
	}
	out := columnsOf(jp.items[0])
	for k := 1; k < len(jp.items); k++ {
		it := jp.items[k]
		right := columnsOf(it)
		if !it.natural {
			out = append(out, right...)
			continue
		}
		var merged []outputColumn
		var conds []Expr
		var synth []columnMeta
		usedLeft, usedRight := map[int]bool{}, map[int]bool{}
		for li, l := range out {
			ri := -1
			for j, r := range right {
				if strings.EqualFold(l.name, r.name) {
					ri = j
					break
				}
			}
			if ri < 0 {
				continue
			}
			if count(out, l.name) > 1 {
				fail(errorf(adbc.StatusInvalidArgument, "common column name %q appears more than once in left table", l.name))
			}
			if count(right, l.name) > 1 {
				fail(errorf(adbc.StatusInvalidArgument, "common column name %q appears more than once in right table", l.name))
			}
			r := right[ri]
			t, ok := setOpType(l.col.Type, r.col.Type)
			if !ok {
				fail(errorf(adbc.StatusInvalidArgument, "JOIN/USING types %s and %s cannot be matched", l.col.Type.SQLName(), r.col.Type.SQLName()))
			}
			conds = append(conds, &Binary{Op: "=", L: l.expr, R: r.expr})
			m := l
			switch it.kind {
			case "RIGHT":
				m = r
			case "FULL":
				key := fmt.Sprintf("%s%d.%s", mergedPrefix, k, l.name)
				expr := &Func{Name: "COALESCE", Args: []Expr{l.expr, r.expr}}
				m = outputColumn{name: l.name, col: columnMeta{Name: key, label: l.name, Type: t, Nullable: true}, expr: expr}
				jp.merged = append(jp.merged, mergedColumn{item: k, key: key, expr: expr, typ: t})
				joined.Columns = append(joined.Columns, m.col)
				synth = append(synth, columnMeta{Name: l.name, Type: t, Nullable: true})
			}
			merged = append(merged, m)
			usedLeft[li], usedRight[ri] = true, true
		}
		it.usingOn = andAll(conds)
		next := merged
		for i, c := range out {
			if !usedLeft[i] {
				next = append(next, c)
			}
		}
		for i, c := range right {
			if !usedRight[i] {
				next = append(next, c)
			}
		}
		out = next
		if synth != nil {
			meta := &tableMeta{Name: "join", isMem: true, Columns: synth}
			rels = append(rels, relation{meta: meta, prefix: fmt.Sprintf("%s%d.", mergedPrefix, k)})
		}
	}
	jp.star = make([]columnMeta, len(out))
	visible := map[string]bool{}
	for i, c := range out {
		jp.star[i] = c.col
		visible[c.col.Name] = true
	}
	// Columns merged into another one are only reachable by qualified names.
	for i := range rels {
		for _, c := range rels[i].meta.Columns {
			if visible[rels[i].prefix+c.Name] {
				continue
			}
			if rels[i].hidden == nil {
				rels[i].hidden = map[string]bool{}
			}
			rels[i].hidden[c.Name] = true
		}
	}
	return rels
}

// fillMerged stores the columns merged by the NATURAL FULL JOIN of join
// item i in the joined rows.
func (jp *joinPlan) fillMerged(env *evalEnv, i int, rows []map[string]Value) error {
	for _, m := range jp.merged {
		if m.item != i {
			continue
		}
		for _, row := range rows {
			env.row = row
			v, err := env.eval(m.expr)
			if err != nil {
				return invalidArg(err)
			}
			if v, err = Coerce(v, m.typ); err != nil {
				return invalidArg(err)
			}
			row[m.key] = v
		}
	}
	return nil
}
