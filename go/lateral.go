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

// LATERAL subqueries and table functions in FROM.
//
// `LATERAL (SELECT …)` may read the columns of the FROM items before it, and
// so may the arguments of a table function (for which LATERAL is optional,
// as in Postgres). Both are join items whose rows are computed while the
// join runs, from the rows joined so far:
//
//   - A LATERAL subquery is planned as a correlated subquery whose enclosing
//     scope holds the items before it, and runs through subqueryRows, so it
//     is memoised on the outer values it reads.
//   - A table function's arguments are evaluated and its rows generated
//     (GENERATE_SERIES, see series.go), memoised on the argument values.
//
// An item that reads no earlier item (only constants, parameters or an
// enclosing query's columns) is computed once per join and joined like an
// in-memory relation: hash joins and every join kind work. One that does is
// computed for each row joined so far, which INNER, CROSS and LEFT joins
// allow (as in Postgres). A join with either kind of item keeps its written
// order.

import (
	"context"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// lateralItem is the part of a join item that is computed when the join
// runs: a LATERAL subquery (sq) or a table function (fn, whose column has
// type typ). need are the columns of the earlier items it reads; with any,
// it is computed for each row joined so far.
type lateralItem struct {
	sq   *Subquery
	fn   *Func
	typ  ColType
	need map[string]bool
}

// addFromItem resolves a FROM item and adds it to the join: a table, view,
// CTE or derived table (addJoinItem), or a LATERAL subquery or table
// function. Column aliases rename the item's first columns.
func (e *executor) addFromItem(ctx context.Context, jp *joinPlan, jc JoinClause) error {
	if jc.Func == nil && !jc.Lateral {
		if err := e.addJoinItem(ctx, jp, jc.Kind, jc.Table, jc.Select, jc.Alias); err != nil {
			return err
		}
		it := jp.items[len(jp.items)-1]
		it.natural = jc.Natural
		if jc.Columns != nil {
			m, err := renameColumns(it.base, it.alias, jc.Columns)
			if err != nil {
				return err
			}
			it.base = m
		}
		return nil
	}
	lat := &lateralItem{}
	alias := jc.Alias
	var base *tableMeta
	var err error
	// The item's own scope: the items before it, as seen from the query
	// whose FROM clause this is (the pending subquery, if any, is that
	// query's, and its scope is pushed later).
	rels := make([]relation, len(jp.items))
	for i, it := range jp.items {
		rels[i] = it.rel()
	}
	sc := &scope{rels: rels, sq: e.pendingSq, needs: map[string]bool{}}
	e.scopes = append(e.scopes, sc)
	if jc.Func != nil {
		if alias == "" {
			alias = strings.ToLower(jc.Func.Name)
		}
		lat.fn = jc.Func
		base, lat.typ, err = e.planTableFunc(ctx, jc.Func, alias, jc.Columns, sc)
	} else if alias == "" {
		err = errorf(adbc.StatusInvalidArgument, "a subquery in FROM must have an alias")
	} else {
		lat.sq = &Subquery{Select: jc.Select, Kind: SubqueryLateral}
		saved := e.pendingSq
		e.pendingSq = lat.sq
		var plan *selectPlan
		plan, err = e.planSelect(ctx, jc.Select, e.paramTypes)
		e.pendingSq = saved
		if err == nil {
			lat.sq.plan = plan
			if base, err = memTable(alias, plan.columns(), nil, nil); err == nil && jc.Columns != nil {
				base, err = renameColumns(base, alias, jc.Columns)
			}
		}
	}
	e.popScope()
	if err != nil {
		return err
	}
	lat.need = sc.needs
	if len(lat.need) > 0 && (jc.Kind == "RIGHT" || jc.Kind == "FULL") {
		return errorf(adbc.StatusInvalidArgument,
			"invalid reference to FROM-clause entry for table %q: the combining JOIN type must be INNER or LEFT for a LATERAL reference",
			jp.readBy(lat.need))
	}
	it, err := jp.add(jc.Kind, relation{name: alias, aliased: jc.Alias != "", meta: base})
	if err != nil {
		return err
	}
	it.lateral, it.natural = lat, jc.Natural
	return nil
}

// readBy returns the alias of the first join item one of cols belongs to.
func (jp *joinPlan) readBy(cols map[string]bool) string {
	for _, it := range jp.items {
		for c := range cols {
			if strings.HasPrefix(c, it.prefix) {
				return it.alias
			}
		}
	}
	return ""
}

// renameColumns applies column aliases (`AS a(x, y)`) to a FROM item's
// relation: its first len(cols) columns take the new names. A table keeps
// reading the columns' HASH fields.
func renameColumns(meta *tableMeta, alias string, cols []string) (*tableMeta, error) {
	if len(cols) > len(meta.Columns) {
		return nil, errorf(adbc.StatusInvalidArgument, "table %q has %d columns available but %d columns specified",
			alias, len(meta.Columns), len(cols))
	}
	m := *meta
	m.Columns = slices.Clone(meta.Columns)
	rename := make(map[string]string, len(cols))
	for i, n := range cols {
		c := &m.Columns[i]
		if !m.isMem {
			c.Field = c.field()
		}
		rename[c.Name] = n
		c.Name = n
	}
	seen := map[string]bool{}
	for _, c := range m.Columns {
		if seen[strings.ToLower(c.Name)] {
			return nil, errorf(adbc.StatusInvalidArgument, "column name %q appears more than once in %q", c.Name, alias)
		}
		seen[strings.ToLower(c.Name)] = true
	}
	if m.isMem {
		m.mem = make([]map[string]Value, len(meta.mem))
		for r, row := range meta.mem {
			out := make(map[string]Value, len(row))
			for k, v := range row {
				if n, ok := rename[k]; ok {
					k = n
				}
				out[k] = v
			}
			m.mem[r] = out
		}
	}
	return &m, nil
}

// lateralJoin runs step k of a join, whose item is a LATERAL subquery or a
// table function: it joins left (the rows joined so far) with the item's
// rows for each of them, or, if the item reads no earlier item, with its
// rows computed once.
func (e *executor) lateralJoin(env *evalEnv, k int, st joinStep, it *joinItem, wp wherePlan, left []map[string]Value, pairs []equiPair, filters []Expr) ([]map[string]Value, error) {
	memo := map[string][]map[string]Value{}
	if len(it.lateral.need) == 0 {
		env.row = nil
		right, err := e.lateralRows(env, it, wp, memo)
		if err != nil {
			return nil, err
		}
		if k > 0 {
			return e.joinRows(env, st.kind, left, right, pairs, filters)
		}
		if len(st.conds) == 0 {
			return right, nil
		}
		return e.joinRows(env, "INNER", right, []map[string]Value{{}}, nil, st.conds)
	}
	var out []map[string]Value
	for _, l := range left {
		env.row = l
		right, err := e.lateralRows(env, it, wp, memo)
		if err != nil {
			return nil, err
		}
		matched := false
		for _, r := range right {
			row := mergeRows(l, r)
			ok := true
			env.row = row
			for _, c := range st.conds {
				v, err := env.eval(c)
				if err != nil {
					return nil, invalidArg(err)
				}
				if b, valid := truthy(v); !valid || !b {
					ok = false
					break
				}
			}
			if ok {
				out = append(out, row)
				matched = true
			}
		}
		if !matched && st.kind == "LEFT" {
			out = append(out, l)
		}
	}
	return out, nil
}

// lateralRows computes a LATERAL item's rows (keyed alias.column) for the
// row in env, keeping those that pass the item's own filters (wp).
func (e *executor) lateralRows(env *evalEnv, it *joinItem, wp wherePlan, memo map[string][]map[string]Value) ([]map[string]Value, error) {
	lat := it.lateral
	fenv := *env
	var out []map[string]Value
	keep := func(m map[string]Value) error {
		if wp.residual != nil {
			fenv.row = m
			v, err := fenv.eval(wp.residual)
			if err != nil {
				return invalidArg(err)
			}
			if b, ok := truthy(v); !ok || !b {
				return nil
			}
		}
		out = append(out, m)
		return nil
	}
	if lat.sq != nil {
		rows, err := e.subqueryRows(env.ctx, lat.sq, env)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			m := make(map[string]Value, len(r))
			for i, c := range it.base.Columns {
				m[it.prefix+c.Name] = r[i]
			}
			if err := keep(m); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	args := make([]Value, len(lat.fn.Args))
	for i, a := range lat.fn.Args {
		v, err := env.eval(a)
		if err != nil {
			return nil, invalidArg(err)
		}
		args[i] = v
	}
	key := rowKey(args)
	if rows, ok := memo[key]; ok {
		return rows, nil
	}
	vals, err := generateSeries(lat.typ, args, env.zone())
	if err != nil {
		return nil, err
	}
	name := it.prefix + it.base.Columns[0].Name
	for _, v := range vals {
		if err := keep(map[string]Value{name: v}); err != nil {
			return nil, err
		}
	}
	memo[key] = out
	return out, nil
}
