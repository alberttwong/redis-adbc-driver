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

// UPDATE … FROM, DELETE … USING and MERGE.
//
// All three join the target table with other relations through the join
// executor (join.go), then act on target rows by their HASH key, which the
// target's scan adds to every joined row (joinItem.tag):
//
//   - UPDATE t SET … FROM items WHERE … and DELETE FROM t USING items WHERE
//     …: the FROM / USING items are joined as in a SELECT, and the target is
//     cross-joined with them, so WHERE predicates linking it to them become
//     join conditions (a hash join, or an index lookup join on an indexed
//     target column), and WHERE predicates on the target alone run in its
//     index scan.
//   - MERGE INTO t USING src ON cond: src LEFT JOIN t ON cond, so ON
//     predicates on the target alone run in its index scan. With WHEN NOT
//     MATCHED BY SOURCE clauses it is a FULL JOIN instead, since those need
//     every target row.
//
// Every change is decided and checked before anything is written: SET and
// VALUES are evaluated and cast to the column types, NOT NULL and the CHECK
// constraints are checked (check.go), and so is the rule that MERGE changes
// a target row at most once. A RETURNING list is evaluated for each change
// then too (returning.go). The writes then go through the same paths as
// UPDATE, DELETE and INSERT (see exec.go): updates, then deletes, then
// inserts.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// Names of the values the join adds to joined rows; they can't clash with
// column names, which are "alias.column".
const (
	targetTag = "\x00target" // the target row's HASH key
	sourceTag = "\x00source" // set when the row has a MERGE source row
)

// dmlJoin is the target table of a DML statement joined with other
// relations.
type dmlJoin struct {
	meta   *tableMeta // the target table
	target *joinItem
	jp     *joinPlan
	joined *tableMeta  // the joined relation (columns alias.column)
	rels   []relation  // every item, for name resolution
	sel    *SelectStmt // the join clauses, for bindJoin
}

// planDMLJoin resolves the target table and the other FROM items (from, the
// first with an empty Kind) and joins the target to them last, with kind and
// condition on. The target must be a table (not a view or CTE), as for
// UPDATE and DELETE.
func (e *executor) planDMLJoin(ctx context.Context, table TableName, alias string, from []JoinClause, kind string, on Expr) (*dmlJoin, error) {
	meta, err := e.loadTable(ctx, table)
	if err != nil {
		return nil, err
	}
	rel := tableRel(meta, alias)
	if alias == "" {
		rel.name = table.Name
	}
	jp := &joinPlan{}
	for _, jc := range from {
		if err := e.addFromItem(ctx, jp, jc); err != nil {
			return nil, err
		}
	}
	target, err := jp.add(kind, rel)
	if err != nil {
		return nil, err
	}
	target.tag = targetTag
	joined, rels := jp.finish()
	joins := append(slices.Clone(from[1:]), JoinClause{Kind: kind, On: on})
	return &dmlJoin{meta: meta, target: target, jp: jp, joined: joined, rels: rels, sel: &SelectStmt{Joins: joins}}, nil
}

// rows runs the join and returns the joined rows that satisfy where (bound
// in the join's scope). need lists the columns the statement reads.
func (dj *dmlJoin) rows(ctx context.Context, e *executor, where Expr, need map[string]bool, params []Value) ([]map[string]Value, error) {
	dj.jp.planPushdown(where)
	wp := wherePlan{residual: where, none: e.neverTrue(ctx, where)}
	_, rows, err := e.scan(ctx, scanRequest{meta: dj.joined, where: wp, need: need}, params)
	return rows, err
}

// bindWhere binds the WHERE clause of UPDATE … FROM / DELETE … USING.
func (e *executor) bindWhere(ctx context.Context, where Expr) error {
	if where == nil {
		return nil
	}
	if isAggregate(where) {
		return errorf(adbc.StatusInvalidArgument, "aggregates are not allowed in WHERE")
	}
	if containsWindow(where) {
		return errWindowPlacement()
	}
	return e.bind(ctx, where)
}

// runUpdateFrom implements UPDATE t SET … FROM items [WHERE …].
//
// A target row that matches several FROM rows is updated once. PostgreSQL
// then uses one of the matches, without saying which; here the matches must
// agree on the new values, and it is an error if they don't.
func (e *executor) runUpdateFrom(ctx context.Context, st *UpdateStmt, params []Value) (int64, error) {
	// With a WHERE that is never true, nothing is read (see empty.go).
	defer e.planningOnly(e.neverTrue(ctx, st.Where))()
	dj, err := e.planDMLJoin(ctx, st.Table, st.Alias, st.From, "CROSS", nil)
	if err != nil {
		return 0, err
	}
	cols, err := setTargets(dj.meta, dj.target.alias, st.Sets)
	if err != nil {
		return 0, err
	}
	sc := e.pushScope(dj.rels)
	defer e.popScope()
	if err := e.bindJoin(ctx, dj.sel, dj.jp); err != nil {
		return 0, err
	}
	need := map[string]bool{}
	for _, s := range st.Sets {
		if containsWindow(s.Expr) {
			return 0, errWindowPlacement()
		}
		if err := e.bind(ctx, s.Expr); err != nil {
			return 0, err
		}
		columnRefs(s.Expr, need)
	}
	if err := e.bindWhere(ctx, st.Where); err != nil {
		return 0, err
	}
	maps.Copy(need, sc.needs)
	checks, err := e.tableChecks(ctx, dj.meta)
	if err != nil {
		return 0, err
	}
	checks.need(need, dj.target.prefix)
	ret, err := e.planReturning(ctx, st.Returning, dj.rels, dj.targetFirst(), false, need)
	if err != nil {
		return 0, err
	}
	rows, err := dj.rows(ctx, e, st.Where, need, params)
	if err != nil {
		return 0, err
	}
	env := e.newEnv(ctx, dj.joined.types(), params)
	var changes []rowChange
	var values [][]Value
	seen := map[string]int{}
	for _, row := range rows {
		key := row[targetTag].S
		env.row = row
		vals, err := setValues(env, dj.meta, cols, st.Sets)
		if err != nil {
			return 0, err
		}
		if err := checks.checkChanged(row, dj.target.prefix, cols, vals); err != nil {
			return 0, err
		}
		if i, ok := seen[key]; ok {
			for j, v := range vals {
				if !sameValue(values[i][j], v) {
					return 0, errorf(adbc.StatusInvalidArgument,
						"a row of %q matches more than one FROM row, and they set column %q to different values (%s and %s)",
						dj.meta.Name, dj.meta.Columns[cols[j]].Name, valueText(values[i][j]), valueText(v))
				}
			}
			continue
		}
		seen[key] = len(changes)
		values = append(values, vals)
		changes = append(changes, newRowChange(dj.meta, key, cols, vals))
		if err := ret.addChanged(row, dj.meta, dj.target.prefix, cols, vals, ""); err != nil {
			return 0, err
		}
	}
	if err := e.writeUpdates(ctx, dj.meta, changes); err != nil {
		return 0, err
	}
	return int64(len(changes)), nil
}

// runDeleteUsing implements DELETE FROM t USING items [WHERE …]. A target
// row that matches several USING rows is deleted once.
func (e *executor) runDeleteUsing(ctx context.Context, st *DeleteStmt, params []Value) (int64, error) {
	// With a WHERE that is never true, nothing is read (see empty.go).
	defer e.planningOnly(e.neverTrue(ctx, st.Where))()
	dj, err := e.planDMLJoin(ctx, st.Table, st.Alias, st.Using, "CROSS", nil)
	if err != nil {
		return 0, err
	}
	sc := e.pushScope(dj.rels)
	defer e.popScope()
	if err := e.bindJoin(ctx, dj.sel, dj.jp); err != nil {
		return 0, err
	}
	if err := e.bindWhere(ctx, st.Where); err != nil {
		return 0, err
	}
	need := maps.Clone(sc.needs)
	ret, err := e.planReturning(ctx, st.Returning, dj.rels, dj.targetFirst(), false, need)
	if err != nil {
		return 0, err
	}
	rows, err := dj.rows(ctx, e, st.Where, need, params)
	if err != nil {
		return 0, err
	}
	var keys []string
	seen := map[string]bool{}
	for _, row := range rows {
		if key := row[targetTag].S; !seen[key] {
			seen[key] = true
			keys = append(keys, key)
			if err := ret.add(row, ""); err != nil {
				return 0, err
			}
		}
	}
	if err := e.deleteKeys(ctx, dj.meta, keys); err != nil {
		return 0, err
	}
	return int64(len(keys)), nil
}

// mergeClause is a bound WHEN clause.
type mergeClause struct {
	*MergeClause
	cols    []int // UPDATE: the target column of each SET
	targets []int // INSERT: the target column of each value
}

// runMerge implements MERGE. Each joined row is either matched (a source row
// and a target row), not matched (a source row only) or not matched by
// source (a target row only); the first WHEN clause of that kind whose
// condition holds decides what happens to it. The result is the number of
// rows inserted, updated or deleted.
func (e *executor) runMerge(ctx context.Context, st *MergeStmt, params []Value) (int64, error) {
	pop, err := e.pushCTEs(st.With)
	if err != nil {
		return 0, err
	}
	defer pop()
	kind := "LEFT"
	for _, c := range st.Clauses {
		if c.Match == MergeNotMatchedBySource {
			kind = "FULL"
		}
	}
	if err := checkMergeNames(st); err != nil {
		return 0, err
	}
	dj, err := e.planDMLJoin(ctx, st.Table, st.Alias, []JoinClause{st.Source}, kind, st.On)
	if err != nil {
		return 0, err
	}
	dj.jp.items[0].tag = sourceTag

	// ON and WHEN MATCHED clauses see the source and the target, WHEN NOT
	// MATCHED clauses only the source (there is no target row), and WHEN
	// NOT MATCHED BY SOURCE clauses only the target.
	need := map[string]bool{}
	inScope := func(rels []relation, fn func() error) error {
		sc := e.pushScope(rels)
		defer e.popScope()
		if err := fn(); err != nil {
			return err
		}
		maps.Copy(need, sc.needs)
		return nil
	}
	if err := inScope(dj.rels, func() error { return e.bindJoin(ctx, dj.sel, dj.jp) }); err != nil {
		return 0, err
	}
	clauses := make([]mergeClause, len(st.Clauses))
	for i := range st.Clauses {
		c := &mergeClause{MergeClause: &st.Clauses[i]}
		rels := dj.rels
		switch c.Match {
		case MergeNotMatched:
			rels = dj.rels[:1]
		case MergeNotMatchedBySource:
			rels = dj.rels[1:]
		}
		if err := inScope(rels, func() error { return e.bindMergeClause(ctx, c.MergeClause, need) }); err != nil {
			return 0, mergeScopeHint(err, c.Match)
		}
		switch c.Action {
		case MergeUpdate:
			if c.cols, err = setTargets(dj.meta, dj.target.alias, c.Sets); err != nil {
				return 0, err
			}
		case MergeInsert:
			if c.targets, err = insertTargets(dj.meta, c.Columns); err != nil {
				return 0, err
			}
			if c.Values != nil && len(c.Values) != len(c.targets) {
				return 0, errorf(adbc.StatusInvalidArgument, "INSERT has %d target columns but %d values", len(c.targets), len(c.Values))
			}
		}
		clauses[i] = *c
	}
	ret, err := e.planReturning(ctx, st.Returning, dj.rels, dj.joined, true, need)
	if err != nil {
		return 0, err
	}
	checks, err := e.tableChecks(ctx, dj.meta)
	if err != nil {
		return 0, err
	}
	if slices.ContainsFunc(clauses, func(c mergeClause) bool { return c.Action == MergeUpdate }) {
		checks.need(need, dj.target.prefix)
	}
	rows, err := dj.rows(ctx, e, nil, need, params)
	if err != nil {
		return 0, err
	}

	// Decide and check every change before writing any.
	env := e.newEnv(ctx, dj.joined.types(), params)
	defs := e.columnDefaults(ctx, dj.meta)
	var updates []rowChange
	var deletes []string
	var inserts [][]Value
	changed := map[string]bool{}
	for _, row := range rows {
		env.row = row
		target, matched := row[targetTag]
		match := MergeMatched
		if !matched {
			match = MergeNotMatched
		} else if _, ok := row[sourceTag]; !ok {
			match = MergeNotMatchedBySource
		}
		c, err := firstMergeClause(env, clauses, match)
		if err != nil {
			return 0, err
		}
		if c == nil || c.Action == MergeDoNothing {
			continue
		}
		if c.Action == MergeInsert {
			r, err := insertRow(env, dj.meta, defs, checks, c.targets, c.Values)
			if err != nil {
				return 0, err
			}
			inserts = append(inserts, r)
			if err := ret.addChanged(row, dj.meta, dj.target.prefix, nil, r, "INSERT"); err != nil {
				return 0, err
			}
			continue
		}
		key := target.S
		if changed[key] {
			return 0, errorf(adbc.StatusInvalidArgument,
				"MERGE command cannot affect row a second time: a row of %q matches more than one source row", dj.meta.Name)
		}
		changed[key] = true
		if c.Action == MergeDelete {
			deletes = append(deletes, key)
			if err := ret.add(row, "DELETE"); err != nil {
				return 0, err
			}
			continue
		}
		vals, err := setValues(env, dj.meta, c.cols, c.Sets)
		if err != nil {
			return 0, err
		}
		if err := checks.checkChanged(row, dj.target.prefix, c.cols, vals); err != nil {
			return 0, err
		}
		updates = append(updates, newRowChange(dj.meta, key, c.cols, vals))
		if err := ret.addChanged(row, dj.meta, dj.target.prefix, c.cols, vals, "UPDATE"); err != nil {
			return 0, err
		}
	}
	if err := e.writeUpdates(ctx, dj.meta, updates); err != nil {
		return 0, err
	}
	if err := e.deleteKeys(ctx, dj.meta, deletes); err != nil {
		return 0, err
	}
	if _, err := e.store.insertRows(ctx, dj.meta, inserts); err != nil {
		return 0, err
	}
	return int64(len(updates) + len(deletes) + len(inserts)), nil
}

// bindMergeClause binds a WHEN clause's condition and values in the current
// scope, adding the columns they read to need.
func (e *executor) bindMergeClause(ctx context.Context, c *MergeClause, need map[string]bool) error {
	exprs := slices.Clone(c.Values)
	if c.Cond != nil {
		if isAggregate(c.Cond) {
			return errorf(adbc.StatusInvalidArgument, "aggregates are not allowed in WHEN conditions")
		}
		exprs = append(exprs, c.Cond)
	}
	for _, s := range c.Sets {
		exprs = append(exprs, s.Expr)
	}
	for _, x := range exprs {
		if containsWindow(x) {
			return errWindowPlacement()
		}
		if err := e.bind(ctx, x); err != nil {
			return err
		}
		columnRefs(x, need)
	}
	return nil
}

// checkMergeNames is Postgres's error for a MERGE source with the target's
// name, even when they are tables of different schemas (which a join
// allows).
func checkMergeNames(st *MergeStmt) error {
	target, source := st.Alias, st.Source.Alias
	if target == "" {
		target = st.Table.Name
	}
	if source == "" && st.Source.Table != nil {
		source = st.Source.Table.Name
	}
	if !strings.EqualFold(target, source) {
		return nil
	}
	return errorf(adbc.StatusInvalidArgument,
		"name %q specified more than once; the name is used both as MERGE target table and data source", target)
}

// mergeScopeHint explains an unknown column or table in a WHEN NOT MATCHED
// clause, which may refer to the source only (or, BY SOURCE, the target
// only).
func mergeScopeHint(err error, m MergeMatch) error {
	var ae adbc.Error
	if m == MergeMatched || !errors.As(err, &ae) || !(strings.HasPrefix(ae.Msg, "[redis] column ") ||
		strings.HasPrefix(ae.Msg, "[redis] missing FROM-clause entry ") || strings.HasPrefix(ae.Msg, "[redis] invalid reference to FROM-clause entry ")) {
		return err
	}
	if m == MergeNotMatched {
		ae.Msg += " (WHEN NOT MATCHED clauses can only refer to the source)"
	} else {
		ae.Msg += " (WHEN NOT MATCHED BY SOURCE clauses can only refer to the target)"
	}
	return ae
}

// firstMergeClause returns the first clause for rows of kind m whose
// condition holds for the row in env, or nil.
func firstMergeClause(env *evalEnv, clauses []mergeClause, m MergeMatch) (*mergeClause, error) {
	for i := range clauses {
		c := &clauses[i]
		if c.Match != m {
			continue
		}
		if c.Cond == nil {
			return c, nil
		}
		v, err := env.eval(c.Cond)
		if err != nil {
			return nil, invalidArg(err)
		}
		if b, ok := truthy(v); ok && b {
			return c, nil
		}
	}
	return nil, nil
}

// sameValue reports whether two values of the same column type are equal,
// with NULL equal to NULL.
func sameValue(a, b Value) bool {
	if a.Null || b.Null {
		return a.Null == b.Null
	}
	return encodeStored(a) == encodeStored(b)
}

func valueText(v Value) string {
	if v.Null {
		return "NULL"
	}
	return fmt.Sprintf("%q", v.Text())
}
