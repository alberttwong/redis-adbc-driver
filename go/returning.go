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

// RETURNING.
//
// INSERT, UPDATE, DELETE and MERGE may end with a RETURNING list, written
// like a select list over the rows they change. It can read the target's
// columns (and __rowid, except in joins), and in UPDATE … FROM, DELETE …
// USING and MERGE the other relations' columns too. The statement then
// returns a row for each row it changed, as in Postgres:
//
//   - INSERT: the new row.
//   - UPDATE: the row after the change.
//   - DELETE: the deleted row.
//   - MERGE: the row inserted, updated or deleted, with the source row's
//     columns; merge_action() is 'INSERT', 'UPDATE' or 'DELETE'.
//
// The list is evaluated for each change while the changes are computed and
// checked, before anything is written (exec.go, dml.go), so an error in it
// leaves the table untouched, and its subqueries see the tables as they
// were before the statement, like Postgres's statement snapshot. INSERT
// allocates the new rows' ids first, so that RETURNING __rowid gives them.
//
// With RETURNING, ExecuteQuery returns those rows and ExecuteUpdate the
// number of rows changed. ExecuteSchema (and a statement with no parameter
// rows bound) plans the list without running the statement.

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/apache/arrow-adbc/go/adbc"
)

// returning is a bound RETURNING list and the rows it has returned.
type returning struct {
	items []planItem
	env   *evalEnv
	rows  [][]Value
}

// planReturning binds a RETURNING list over rels, the relations of the
// changed rows; `*` lists star's columns, which are named as in rels
// ("alias.column" in a join). It adds the columns the list reads to need
// (if not nil) and makes it the statement's RETURNING list. merge allows
// merge_action(). Without a list it returns nil.
func (e *executor) planReturning(ctx context.Context, list []SelectItem, rels []relation, star *tableMeta, merge bool, need map[string]bool) (*returning, error) {
	if len(list) == 0 {
		return nil, nil
	}
	sc := e.pushScope(rels)
	sc.mergeReturning = merge
	defer e.popScope()
	joined := rels[0].prefix != ""
	types := star.types()
	ret := &returning{env: e.newEnv(ctx, types, e.params)}
	for _, it := range list {
		if it.Star {
			cols, err := e.starColumns(star, rels, joined, it.StarOf)
			if err != nil {
				return nil, err
			}
			for _, c := range cols {
				ret.items = append(ret.items, planItem{expr: &ColumnRef{Name: c.Name}, name: c.outputName(), typ: c.Type})
			}
			continue
		}
		if isAggregate(it.Expr) {
			return nil, errorf(adbc.StatusInvalidArgument, "aggregate functions are not allowed in RETURNING")
		}
		if containsWindow(it.Expr) {
			return nil, errorf(adbc.StatusInvalidArgument, "window functions are not allowed in RETURNING")
		}
		if err := e.bind(ctx, it.Expr); err != nil {
			return nil, err
		}
		t, err := inferType(it.Expr, types, e.paramTypes)
		if err != nil {
			return nil, invalidArg(err)
		}
		name := it.Alias
		if name == "" {
			name = it.Text
			if c, ok := it.Expr.(*ColumnRef); ok {
				name = c.Name
				if col, ok := star.column(c.Name); ok {
					name = col.outputName()
				}
			}
		}
		ret.items = append(ret.items, planItem{expr: it.Expr, name: name, typ: t})
	}
	if need != nil {
		for _, it := range ret.items {
			columnRefs(it.expr, need)
		}
		maps.Copy(need, sc.needs)
	}
	e.returning = ret
	return ret, nil
}

// add evaluates the list for a changed row, keyed like the rows of the
// relations it was planned over; action is merge_action() (MERGE only). It
// does nothing without a RETURNING list.
func (r *returning) add(row map[string]Value, action string) error {
	if r == nil {
		return nil
	}
	r.env.row, r.env.action = row, action
	out := make([]Value, len(r.items))
	for i, it := range r.items {
		v, err := r.env.eval(it.expr)
		if err != nil {
			return invalidArg(err)
		}
		out[i] = v
	}
	r.rows = append(r.rows, out)
	return nil
}

// addChanged is add for row (which may be nil, and is not modified) with
// the columns cols of meta, named prefix+column, set to vals. With nil cols,
// vals is a new row, ordered like meta.Columns.
func (r *returning) addChanged(row map[string]Value, meta *tableMeta, prefix string, cols []int, vals []Value, action string) error {
	if r == nil {
		return nil
	}
	changed := maps.Clone(row)
	if changed == nil {
		changed = make(map[string]Value, len(vals))
	}
	for i, v := range vals {
		c := i
		if cols != nil {
			c = cols[i]
		}
		changed[prefix+meta.Columns[c].Name] = v
	}
	return r.add(changed, action)
}

func (r *returning) columns() []resultColumn {
	cols := make([]resultColumn, len(r.items))
	for i, it := range r.items {
		cols[i] = resultColumn{Name: it.name, Type: it.typ}
	}
	return cols
}

// dmlResult is the result of an INSERT, UPDATE, DELETE or MERGE that changed
// n rows: with RETURNING, a query result of the rows it returned.
func (e *executor) dmlResult(n int64, err error) (execResult, error) {
	if err != nil || e.returning == nil {
		return execResult{affected: n}, err
	}
	return execResult{isQuery: true, cols: e.returning.columns(), rows: e.returning.rows, affected: n}, nil
}

// insertRows writes new rows of meta. With RETURNING, their row ids are
// allocated and the rows returned before any is written.
func (e *executor) insertRows(ctx context.Context, meta *tableMeta, rows [][]Value) (int64, error) {
	if e.returning == nil {
		return e.store.insertRows(ctx, meta, rows)
	}
	a, err := e.store.allocRows(ctx, meta, rows)
	if err != nil {
		return 0, err
	}
	for i, vals := range rows {
		id := map[string]Value{rowIDField: intValue(typeInt64, a.first+int64(i))}
		if err := e.returning.addChanged(id, meta, "", nil, vals, ""); err != nil {
			return 0, err
		}
	}
	return e.store.writeRows(ctx, meta, a, rows)
}

// targetFirst returns the joined relation of UPDATE … FROM or DELETE …
// USING with the target's columns first: the target is joined last, but
// RETURNING * lists its columns before the other items', as in Postgres.
func (dj *dmlJoin) targetFirst() *tableMeta {
	m := *dj.joined
	n := len(m.Columns) - len(dj.meta.Columns)
	m.Columns = append(slices.Clone(m.Columns[n:]), m.Columns[:n]...)
	return &m
}

// hasReturning reports whether st is an INSERT, UPDATE, DELETE or MERGE with
// a RETURNING list.
func hasReturning(st Stmt) bool {
	switch st := st.(type) {
	case *InsertStmt:
		return len(st.Returning) > 0
	case *UpdateStmt:
		return len(st.Returning) > 0
	case *DeleteStmt:
		return len(st.Returning) > 0
	case *MergeStmt:
		return len(st.Returning) > 0
	}
	return false
}

// resultColumns plans the result columns of a statement without running it:
// a query's, or the RETURNING list's. ok is false if it has no result set.
func (e *executor) resultColumns(ctx context.Context, st Stmt, paramTypes []ColType) (cols []resultColumn, ok bool, err error) {
	// The statement is only planned: no derived table, CTE or view is run
	// (see empty.go).
	defer e.planningOnly(true)()
	if sel, isSelect := st.(*SelectStmt); isSelect {
		plan, err := e.planSelect(ctx, sel, paramTypes)
		if err != nil {
			return nil, false, err
		}
		return plan.columns(), true, nil
	}
	if show, isShow := st.(*ShowStmt); isShow {
		cols, err := showColumns(show)
		return cols, err == nil, err
	}
	if paramTypes != nil {
		e.paramTypes = paramTypes
	}
	return e.returningColumns(ctx, st)
}

// returningColumns plans the RETURNING list of an INSERT, UPDATE, DELETE or
// MERGE without running the statement, for its result columns; ok is false
// if the statement has none.
func (e *executor) returningColumns(ctx context.Context, st Stmt) (cols []resultColumn, ok bool, err error) {
	e.ensureCache()
	var ret *returning
	switch st := st.(type) {
	case *InsertStmt:
		if len(st.Returning) == 0 {
			return nil, false, nil
		}
		meta, err := e.loadTable(ctx, st.Table)
		if err != nil {
			return nil, false, err
		}
		ret, err = e.planReturning(ctx, st.Returning, []relation{tableRel(meta, "")}, meta, false, nil)
		if err != nil {
			return nil, false, err
		}
	case *UpdateStmt:
		if ret, err = e.planTargetReturning(ctx, st.With, st.Table, st.Alias, st.From, st.Returning); err != nil {
			return nil, false, err
		}
	case *DeleteStmt:
		if ret, err = e.planTargetReturning(ctx, st.With, st.Table, st.Alias, st.Using, st.Returning); err != nil {
			return nil, false, err
		}
	case *MergeStmt:
		if len(st.Returning) == 0 {
			return nil, false, nil
		}
		pop, err := e.pushCTEs(st.With)
		if err != nil {
			return nil, false, err
		}
		defer pop()
		if err := checkMergeNames(st); err != nil {
			return nil, false, err
		}
		dj, err := e.planDMLJoin(ctx, st.Table, st.Alias, []JoinClause{st.Source}, "LEFT", st.On)
		if err != nil {
			return nil, false, err
		}
		if ret, err = e.planReturning(ctx, st.Returning, dj.rels, dj.joined, true, nil); err != nil {
			return nil, false, err
		}
	}
	if ret == nil {
		return nil, false, nil
	}
	return ret.columns(), true, nil
}

// planTargetReturning plans the RETURNING list of UPDATE or DELETE on table
// [alias], with the FROM / USING items from.
func (e *executor) planTargetReturning(ctx context.Context, with []CTE, table TableName, alias string, from []JoinClause, list []SelectItem) (*returning, error) {
	if len(list) == 0 {
		return nil, nil
	}
	pop, err := e.pushCTEs(with)
	if err != nil {
		return nil, err
	}
	defer pop()
	if len(from) > 0 {
		dj, err := e.planDMLJoin(ctx, table, alias, from, "CROSS", nil)
		if err != nil {
			return nil, err
		}
		return e.planReturning(ctx, list, dj.rels, dj.targetFirst(), false, nil)
	}
	meta, err := e.loadTable(ctx, table)
	if err != nil {
		return nil, err
	}
	return e.planReturning(ctx, list, []relation{tableRel(meta, alias)}, meta, false, nil)
}

// checkMergeAction checks a call of merge_action() (whose arguments the
// registry checks): it may only be used in a MERGE's RETURNING list (here
// not in a subquery there either).
func (e *executor) checkMergeAction(f *Func) error {
	if n := len(e.scopes); n == 0 || !e.scopes[n-1].mergeReturning {
		return errorf(adbc.StatusInvalidArgument, "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command")
	}
	return nil
}

// mergeAction is merge_action(): what MERGE did to the row RETURNING is
// evaluated for.
func (env *evalEnv) mergeAction() (Value, error) {
	if env.action == "" {
		return Value{}, fmt.Errorf("MERGE_ACTION() can only be used in the RETURNING list of a MERGE command")
	}
	return stringValue(env.action), nil
}
