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
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// executor runs parsed statements against a store.
type executor struct {
	store *store
	// sess is the connection's settings (session.go), and schema its
	// current schema.
	sess     *session
	schema   string
	pushdown string // aggregate pushdown mode
	rekey    bool   // RENAME TO also moves the rows (see rekey.go)

	// Per-statement state (see bind.go).
	cache      *execCache
	params     []Value
	paramTypes []ColType
	outer      *evalEnv // row of the enclosing query (correlated subqueries)
	scopes     []*scope
	pendingSq  *Subquery
	ctes       []map[string]*CTE
	now        time.Time // CURRENT_TIMESTAMP etc., fixed per statement
	// noTemp stops unqualified names from resolving to temporary objects
	// (inside permanent views).
	noTemp bool
	// returning is the RETURNING list of the INSERT, UPDATE, DELETE or
	// MERGE being run (see returning.go).
	returning *returning
	// planOnly is set while planning queries whose rows are never read:
	// derived tables, CTEs and views are planned but not run (see empty.go).
	planOnly bool
	// script is set while running a script of several statements, which is
	// a transaction for SET LOCAL (see session.go).
	script bool
}

// execResult is the outcome of one statement.
type execResult struct {
	isQuery  bool
	cols     []resultColumn
	rows     [][]Value
	affected int64
}

func invalidArg(err error) error {
	var ae adbc.Error
	if errors.As(err, &ae) {
		return err
	}
	return errorf(adbc.StatusInvalidArgument, "%v", err)
}

// resolveTable returns the schema and name a table or view is stored under.
// An unqualified name is this connection's temporary table or view if it has
// one by that name, and otherwise in the current schema; pg_temp.<name> is
// always the temporary one.
func (e *executor) resolveTable(t TableName) (string, string, error) {
	schema := t.Schema
	if t.Catalog != "" && t.Catalog != catalogName {
		if schema == "" {
			schema = e.schema
		}
		return "", "", tableNotFound(schema, t.Name)
	}
	switch {
	case schema == "":
		if !e.noTemp && e.store.isTempObject(t.Name) {
			e.markTemp()
			return e.store.tempSchema(), t.Name, nil
		}
		return e.schema, t.Name, nil
	case isTempAlias(schema):
		e.markTemp()
		return e.store.tempSchema(), t.Name, nil
	case isTempSchema(schema):
		// Temporary schemas are only reachable as pg_temp by their owner.
		return "", "", errorf(adbc.StatusNotFound, "table %q.%q does not exist", schema, t.Name)
	}
	return schema, t.Name, nil
}

// resolveCreate returns the schema and name of a table or view being
// created: the connection's temporary schema (created on first use) for
// CREATE TEMP and for pg_temp.<name>, otherwise the given or current schema.
// Unlike resolveTable, an unqualified name never means a temporary object.
func (e *executor) resolveCreate(ctx context.Context, t TableName, temporary bool) (string, string, error) {
	if t.Catalog != "" && t.Catalog != catalogName {
		return "", "", errorf(adbc.StatusNotFound, "catalog %q does not exist", t.Catalog)
	}
	if temporary || isTempAlias(t.Schema) {
		if t.Schema != "" && !isTempAlias(t.Schema) {
			return "", "", errorf(adbc.StatusInvalidArgument,
				"cannot create a temporary table or view in schema %q (temporary objects are in pg_temp)", t.Schema)
		}
		schema, err := e.store.ensureTempSchema(ctx)
		return schema, t.Name, err
	}
	if isTempSchema(t.Schema) {
		return "", "", errorf(adbc.StatusInvalidArgument, "schema name %q is reserved for temporary tables and views", t.Schema)
	}
	schema := t.Schema
	if schema == "" {
		schema = e.schema
	}
	return schema, t.Name, nil
}

// markTemp records that the statement being planned refers to a temporary
// object (a permanent view must not).
func (e *executor) markTemp() {
	if e.cache != nil {
		e.cache.usedTemp = true
	}
}

func (e *executor) loadTable(ctx context.Context, t TableName) (*tableMeta, error) {
	schema, name, err := e.resolveTable(t)
	if err != nil {
		return nil, err
	}
	meta, err := e.store.getTable(ctx, schema, name)
	if err == nil && e.cache != nil {
		e.cache.loaded = append(e.cache.loaded, meta)
	}
	if err == nil {
		// String columns of an older table are checked once (tags.go).
		e.store.startTagCheck(meta)
	}
	return meta, err
}

func (e *executor) execute(ctx context.Context, ps ParsedStmt, params []Value, paramTypes []ColType) (res execResult, err error) {
	e.cache = newExecCache()
	// Tables read while their rows were being moved are checked once the
	// statement is done (see rekey.go).
	cache := e.cache
	defer func() {
		if err == nil {
			err = e.store.checkReads(ctx, cache.loaded)
		}
	}()
	e.params, e.paramTypes = params, paramTypes
	e.returning = nil
	if e.now.IsZero() {
		e.now = time.Now().UTC()
	}
	switch st := ps.Stmt.(type) {
	case *SelectStmt:
		plan, err := e.planSelect(ctx, st, paramTypes)
		if err != nil {
			return execResult{}, err
		}
		rows, err := e.runSelect(ctx, plan, params)
		if err != nil {
			return execResult{}, err
		}
		return execResult{isQuery: true, cols: plan.columns(), rows: rows, affected: int64(len(rows))}, nil
	case *InsertStmt:
		return e.dmlResult(e.runInsert(ctx, st, params))
	case *UpdateStmt:
		return e.dmlResult(e.runUpdate(ctx, st, params))
	case *DeleteStmt:
		return e.dmlResult(e.runDelete(ctx, st, params))
	case *MergeStmt:
		return e.dmlResult(e.runMerge(ctx, st, params))
	case *CreateTableStmt:
		if st.AsSelect != nil {
			n, err := e.runCreateTableAs(ctx, st)
			return execResult{affected: n}, err
		}
		return execResult{affected: -1}, e.runCreateTable(ctx, st)
	case *DropTableStmt:
		schema, name, err := e.resolveTable(st.Table)
		if err != nil {
			if st.IfExists {
				return execResult{affected: -1}, nil
			}
			return execResult{}, err
		}
		return execResult{affected: -1}, e.store.dropTable(ctx, schema, name, st.IfExists)
	case *AlterTableStmt:
		return execResult{affected: -1}, e.runAlter(ctx, st)
	case *CreateViewStmt:
		return execResult{affected: -1}, e.runCreateView(ctx, st, ps.NumParams)
	case *DropViewStmt:
		schema, name, err := e.resolveTable(st.Name)
		if err != nil {
			return execResult{}, err
		}
		return execResult{affected: -1}, e.store.dropView(ctx, schema, name, st.IfExists)
	case *CreateSchemaStmt:
		return execResult{affected: -1}, e.store.createSchema(ctx, st.Name, st.IfNotExists)
	case *DropSchemaStmt:
		return execResult{affected: -1}, e.store.dropSchema(ctx, st.Name, st.IfExists, st.Cascade)
	case *TruncateStmt:
		return execResult{affected: -1}, e.runTruncate(ctx, st)
	case *CommentStmt:
		return execResult{affected: -1}, e.runComment(ctx, st)
	case *TransactionStmt, *SetStmt, *ShowStmt:
		return e.runSession(ps.Stmt)
	}
	return execResult{}, errorf(adbc.StatusNotImplemented, "unsupported statement %T", ps.Stmt)
}

// runSession runs transaction control, SET and SHOW (session.go).
func (e *executor) runSession(st Stmt) (execResult, error) {
	if e.sess == nil {
		// An executor made without a connection (tests).
		e.sess = newSession(e.schema)
	}
	switch st := st.(type) {
	case *TransactionStmt:
		return execResult{affected: -1}, e.runTransaction(st)
	case *SetStmt:
		return execResult{affected: -1}, e.runSet(st)
	case *ShowStmt:
		return e.runShow(st)
	}
	return execResult{}, errorf(adbc.StatusNotImplemented, "unsupported statement %T", st)
}

// ---- DDL ----

// runTruncate empties each table. Every name is resolved first, so a missing
// table or a view in the list leaves all of them untouched.
func (e *executor) runTruncate(ctx context.Context, st *TruncateStmt) error {
	type target struct{ schema, name string }
	var targets []target
	for _, t := range st.Tables {
		schema, name, err := e.resolveTable(t)
		if err != nil {
			return err
		}
		if isInfoSchema(schema) {
			return infoSchemaReadOnly()
		}
		if isView, err := e.store.viewExists(ctx, schema, name); err != nil {
			return err
		} else if isView {
			return errorf(adbc.StatusInvalidArgument, "cannot truncate %q.%q: it is a view", displaySchema(schema), name)
		}
		if _, err := e.store.getTable(ctx, schema, name); err != nil {
			return err
		}
		targets = append(targets, target{schema, name})
	}
	for _, t := range targets {
		if err := e.store.truncateTable(ctx, t.schema, t.name, st.RestartIdentity); err != nil {
			return err
		}
	}
	return nil
}

func (e *executor) runCreateTable(ctx context.Context, st *CreateTableStmt) error {
	schema, name, err := e.resolveCreate(ctx, st.Table, st.Temporary)
	if err != nil {
		return err
	}
	meta := &tableMeta{Schema: schema, Name: name, Comment: st.Comment}
	noIndex := map[string]bool{}
	for _, c := range st.Columns {
		col := columnMeta{Name: c.Name, Type: c.Type, Nullable: !c.NotNull, Comment: c.Comment}
		if c.Default != nil {
			if col.Default, _, err = e.checkDefault(ctx, c); err != nil {
				return err
			}
		}
		meta.Columns = append(meta.Columns, col)
		if c.NoIndex {
			noIndex[c.Name] = true
		}
	}
	if err := meta.applyIndexPolicy(nil, noIndex); err != nil {
		return err
	}
	if meta.Checks, err = e.defineChecks(ctx, meta, st.Checks); err != nil {
		return err
	}
	_, err = e.store.createTable(ctx, meta, st.IfNotExists)
	return err
}

// runCreateTableAs implements CREATE TABLE … AS SELECT: the table's columns
// take the names and types of the query's result, and the result rows are
// inserted. It returns the number of rows inserted.
func (e *executor) runCreateTableAs(ctx context.Context, st *CreateTableStmt) (int64, error) {
	schema, name, err := e.resolveCreate(ctx, st.Table, st.Temporary)
	if err != nil {
		return 0, err
	}
	if st.IfNotExists {
		exists, err := e.store.tableExists(ctx, schema, name)
		if err != nil || exists {
			return 0, err
		}
	}
	plan, err := e.planSelect(ctx, st.AsSelect, nil)
	if err != nil {
		return 0, err
	}
	rows, err := e.runSelect(ctx, plan, nil)
	if err != nil {
		return 0, err
	}
	meta := &tableMeta{Schema: schema, Name: name}
	for j, c := range plan.columns() {
		t := c.Type
		if t.Kind == KindNull {
			t = typeString // SELECT NULL AS x: no type information
		}
		if t.Kind == KindString && t.Length > 0 && !fitsLength(rows, j, t) {
			// The query's values fit their type's length; should one not,
			// the column gets none rather than hold it.
			t = t.withoutLength()
		}
		meta.Columns = append(meta.Columns, columnMeta{Name: c.Name, Type: t, Nullable: true})
	}
	if err := meta.applyIndexPolicy(nil, nil); err != nil {
		return 0, err
	}
	if _, err := e.store.createTable(ctx, meta, st.IfNotExists); err != nil {
		return 0, err
	}
	coerced := make([][]Value, len(rows))
	for i, row := range rows {
		out := make([]Value, len(row))
		for j, v := range row {
			cv, err := Coerce(v, meta.Columns[j].Type)
			if err != nil {
				return 0, errorf(adbc.StatusInvalidArgument, "column %q: %v", meta.Columns[j].Name, err)
			}
			out[j] = cv
		}
		coerced[i] = out
	}
	return e.store.insertRows(ctx, meta, coerced)
}

// fitsLength reports whether column j of rows holds only values that a
// column of type t takes unchanged.
func fitsLength(rows [][]Value, j int, t ColType) bool {
	for _, row := range rows {
		cv, err := Coerce(row[j], t)
		if err != nil {
			return false
		}
		if fv, err := fitLength(t, cv); err != nil || fv.S != cv.S {
			return false
		}
	}
	return true
}

// replaceAliases rewrites references to SELECT aliases (that are not also
// table columns) into the aliased expressions.
func replaceAliases(e Expr, aliases map[string]Expr, meta *tableMeta) Expr {
	switch x := e.(type) {
	case *ColumnRef:
		if meta != nil {
			if _, ok := meta.column(x.Name); ok {
				return x
			}
		}
		if a, ok := aliases[strings.ToLower(x.Name)]; ok {
			return a
		}
		return x
	case *Unary:
		return &Unary{Op: x.Op, X: replaceAliases(x.X, aliases, meta)}
	case *Binary:
		return &Binary{Op: x.Op, L: replaceAliases(x.L, aliases, meta), R: replaceAliases(x.R, aliases, meta)}
	case *IsNull:
		return &IsNull{X: replaceAliases(x.X, aliases, meta), Not: x.Not}
	case *Cast:
		return &Cast{X: replaceAliases(x.X, aliases, meta), T: x.T, OnError: replaceAliases(x.OnError, aliases, meta)}
	case *Func:
		if aggregateFuncs[x.Name] {
			return x
		}
		f := *x
		f.Args = make([]Expr, len(x.Args))
		for i, a := range x.Args {
			f.Args[i] = replaceAliases(a, aliases, meta)
		}
		return &f
	case *Case:
		c := &Case{Else: nil}
		if x.Operand != nil {
			c.Operand = replaceAliases(x.Operand, aliases, meta)
		}
		for _, w := range x.Whens {
			c.Whens = append(c.Whens, WhenClause{When: replaceAliases(w.When, aliases, meta), Then: replaceAliases(w.Then, aliases, meta)})
		}
		if x.Else != nil {
			c.Else = replaceAliases(x.Else, aliases, meta)
		}
		return c
	}
	return e
}

// ---- INSERT ----

func (e *executor) runInsert(ctx context.Context, st *InsertStmt, params []Value) (int64, error) {
	meta, err := e.loadTable(ctx, st.Table)
	if err != nil {
		return 0, err
	}
	targets, err := insertTargets(meta, st.Columns)
	if err != nil {
		return 0, err
	}
	if _, err := e.planReturning(ctx, st.Returning, []relation{{name: meta.Name, meta: meta}}, meta, false, nil); err != nil {
		return 0, err
	}
	if st.Select != nil {
		return e.insertSelect(ctx, st, meta, targets)
	}
	values := st.Rows
	if st.DefaultValues {
		targets, values = nil, [][]Expr{nil}
	}
	env := e.newEnv(ctx, nil, params)
	defs := e.columnDefaults(ctx, meta)
	checks, err := e.tableChecks(ctx, meta)
	if err != nil {
		return 0, err
	}
	rows := make([][]Value, 0, len(values))
	for _, exprs := range values {
		if len(exprs) != len(targets) {
			return 0, errorf(adbc.StatusInvalidArgument, "INSERT has %d target columns but %d values", len(targets), len(exprs))
		}
		for _, expr := range exprs {
			if _, err := e.bindIn(ctx, expr, nil, ""); err != nil {
				return 0, err
			}
		}
		row, err := insertRow(env, meta, defs, checks, targets, exprs)
		if err != nil {
			return 0, err
		}
		rows = append(rows, row)
	}
	return e.insertRows(ctx, meta, rows)
}

// insertTargets resolves an INSERT column list (all columns if empty) to
// column indexes.
func insertTargets(meta *tableMeta, cols []string) ([]int, error) {
	targets := make([]int, 0, len(meta.Columns))
	if len(cols) == 0 {
		for i := range meta.Columns {
			targets = append(targets, i)
		}
		return targets, nil
	}
	for _, name := range cols {
		i, ok := meta.resolve(name)
		if !ok {
			return nil, errorf(adbc.StatusInvalidArgument, "column %q does not exist in table %q", name, meta.Name)
		}
		if slices.Contains(targets, i) {
			return nil, errorf(adbc.StatusInvalidArgument, "column %q specified more than once", name)
		}
		targets = append(targets, i)
	}
	return targets, nil
}

// insertRow evaluates the values of one inserted row into a full row ordered
// like meta.Columns (columns not listed, or given as DEFAULT, get their
// defaults), coerced to the column types and checked against NOT NULL and
// the table's CHECK constraints.
func insertRow(env *evalEnv, meta *tableMeta, defs *columnDefaults, checks *tableChecks, targets []int, exprs []Expr) ([]Value, error) {
	row := make([]Value, len(meta.Columns))
	given := make([]bool, len(meta.Columns))
	for j, expr := range exprs {
		if _, ok := expr.(*DefaultValue); ok {
			continue
		}
		v, err := env.eval(expr)
		if err != nil {
			return nil, invalidArg(err)
		}
		col := meta.Columns[targets[j]]
		cv, err := Coerce(v, col.Type)
		if err != nil {
			return nil, errorf(adbc.StatusInvalidArgument, "column %q: %v", col.Name, err)
		}
		row[targets[j]], given[targets[j]] = cv, true
	}
	if err := defs.fill(row, given); err != nil {
		return nil, err
	}
	if err := checkNewRow(meta, checks, row); err != nil {
		return nil, err
	}
	return row, nil
}

// checkNewRow checks a row to be inserted (ordered like meta.Columns)
// against the column lengths (cutting trailing spaces beyond them, see
// lengths.go), NOT NULL and then the table's CHECK constraints, as Postgres
// does.
func checkNewRow(meta *tableMeta, checks *tableChecks, row []Value) error {
	if err := fitLengths(meta, row); err != nil {
		return err
	}
	for i, c := range meta.Columns {
		if row[i].Null && !c.Nullable {
			return errorf(adbc.StatusIntegrity, "NULL value in column %q violates not-null constraint", c.Name)
		}
	}
	return checks.check(row)
}

// insertSelect implements INSERT INTO … SELECT.
func (e *executor) insertSelect(ctx context.Context, st *InsertStmt, meta *tableMeta, targets []int) (int64, error) {
	plan, err := e.planSelect(ctx, st.Select, e.paramTypes)
	if err != nil {
		return 0, err
	}
	if len(plan.items) != len(targets) {
		return 0, errorf(adbc.StatusInvalidArgument, "INSERT has %d target columns but the query returns %d", len(targets), len(plan.items))
	}
	results, err := e.runSelect(ctx, plan, e.params)
	if err != nil {
		return 0, err
	}
	defs := e.columnDefaults(ctx, meta)
	checks, err := e.tableChecks(ctx, meta)
	if err != nil {
		return 0, err
	}
	rows := make([][]Value, len(results))
	for r, res := range results {
		row := make([]Value, len(meta.Columns))
		given := make([]bool, len(meta.Columns))
		for j, v := range res {
			col := meta.Columns[targets[j]]
			cv, err := Coerce(v, col.Type)
			if err != nil {
				return 0, errorf(adbc.StatusInvalidArgument, "column %q: %v", col.Name, err)
			}
			row[targets[j]], given[targets[j]] = cv, true
		}
		if err := defs.fill(row, given); err != nil {
			return 0, err
		}
		if err := checkNewRow(meta, checks, row); err != nil {
			return 0, err
		}
		rows[r] = row
	}
	return e.insertRows(ctx, meta, rows)
}

// ---- SELECT ----

type planItem struct {
	expr Expr
	name string
	typ  ColType
}

type planOrder struct {
	expr       Expr
	desc       bool
	nullsFirst bool // NULLS FIRST (the default is NULLs last either way)
}

type selectPlan struct {
	sel       *SelectStmt
	meta      *tableMeta
	items     []planItem
	aggregate bool
	order     []planOrder
	// having is the HAVING predicate with output aliases resolved.
	having Expr
	// extraNeed are columns read only by correlated subqueries.
	extraNeed map[string]bool
	// setop is set for UNION / INTERSECT / EXCEPT.
	setop *setOpPlan
	// windows are the window function calls (SELECT list, ORDER BY,
	// QUALIFY); qualify is the QUALIFY predicate with aliases resolved.
	windows []windowCall
	qualify Expr
	// distinct removes duplicate output rows (SELECT DISTINCT). With
	// distinctOn, rows are kept per DISTINCT ON key instead: the first row
	// of each key in ORDER BY order (see runDistinct).
	distinct   bool
	distinctOn []planItem
	// grouping is set for a query with grouping sets (or GROUPING()): it
	// runs over the combined groups of its sets (see grouping.go).
	grouping *groupingPlan
	// noRows is set when the query returns no rows whatever the tables
	// hold (LIMIT 0, or a HAVING or QUALIFY that is never true); it then
	// reads nothing (see empty.go).
	noRows bool
}

// windowed reports whether the query computes window functions (or
// filters with QUALIFY) before its ORDER BY and LIMIT.
func (p *selectPlan) windowed() bool { return len(p.windows) > 0 || p.qualify != nil }

// withoutFrom reports whether the query has no FROM clause.
func (p *selectPlan) withoutFrom() bool {
	switch {
	case p.setop != nil:
		return false
	case p.grouping != nil:
		return p.grouping.base.meta == nil
	}
	return p.meta == nil
}

func (p *selectPlan) columns() []resultColumn {
	cols := make([]resultColumn, len(p.items))
	for i, it := range p.items {
		cols[i] = resultColumn{Name: it.name, Type: it.typ}
	}
	return cols
}

func (e *executor) planSelect(ctx context.Context, sel *SelectStmt, paramTypes []ColType) (*selectPlan, error) {
	e.ensureCache()
	if paramTypes != nil {
		e.paramTypes = paramTypes
	}
	plan := &selectPlan{sel: sel}
	pop, err := e.pushCTEs(sel.With)
	if err != nil {
		return nil, err
	}
	defer pop()
	// A query that returns no rows runs nothing, and one whose WHERE is never
	// true doesn't read its FROM items (see empty.go).
	plan.noRows = (sel.Limit != nil && *sel.Limit == 0) || e.neverTrue(ctx, sel.Having) || e.neverTrue(ctx, sel.Qualify)
	defer e.planningOnly(plan.noRows)()
	if sel.SetOp != nil {
		return e.planSetOp(ctx, sel, plan)
	}
	if g, ok := distinctAsGroupBy(sel); ok {
		// SELECT DISTINCT over plain expressions is GROUP BY those
		// expressions, which the index can often compute (FT.AGGREGATE
		// GROUPBY) instead of the driver reading every row. If that plan
		// doesn't work out, plan the DISTINCT itself: its errors are the
		// ones to report.
		if gp, err := e.planSelect(ctx, g, paramTypes); err == nil && checkDistinctOrder(gp) == nil {
			return gp, nil
		}
	}
	noInput := e.neverTrue(ctx, sel.Where)
	fromOnly := e.planningOnly(noInput)
	var types map[string]ColType
	var rels []relation
	var jp *joinPlan
	if len(sel.Joins) > 0 || sel.FromFunc != nil || sel.FromLateral || sel.FromColumns != nil {
		// Table functions, LATERAL items and column aliases are handled by
		// the join executor, even for a single FROM item.
		joined, jrels, plan2, err := e.planJoin(ctx, sel)
		if err != nil {
			return nil, err
		}
		plan.meta, rels, jp = joined, jrels, plan2
		types = joined.types()
	} else {
		meta, relName, err := e.fromRelation(ctx, sel)
		if err != nil {
			return nil, err
		}
		if meta != nil {
			plan.meta = meta
			types = meta.types()
			rels = []relation{{name: relName, meta: meta}}
		}
	}
	sc := e.pushScope(rels)
	defer func() {
		plan.extraNeed = sc.needs
		e.popScope()
	}()
	if jp != nil {
		if err := e.bindJoin(ctx, sel, jp); err != nil {
			return nil, err
		}
	}
	fromOnly()
	itemStart := make([]int, len(sel.Items))
	for i, it := range sel.Items {
		itemStart[i] = len(plan.items)
		if it.Star {
			if plan.meta == nil {
				return nil, errorf(adbc.StatusInvalidArgument, "SELECT %s requires a FROM clause", it.Text)
			}
			cols, err := starColumns(plan.meta, rels, jp != nil, it.StarOf)
			if err != nil {
				return nil, err
			}
			for _, c := range cols {
				plan.items = append(plan.items, planItem{expr: &ColumnRef{Name: c.Name}, name: c.outputName(), typ: c.Type})
			}
			continue
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
			if c, ok := it.Expr.(*ColumnRef); ok {
				// Output the column's own name, not alias.column.
				name = c.Name
				switch {
				case c.Outer > 0:
					if i := strings.LastIndex(name, "."); i >= 0 {
						name = name[i+1:]
					}
				case plan.meta != nil:
					if col, ok := plan.meta.column(c.Name); ok {
						name = col.outputName()
					}
				}
			} else {
				name = it.Text
			}
		}
		plan.items = append(plan.items, planItem{expr: it.Expr, name: name, typ: t})
		if isAggregate(it.Expr) {
			plan.aggregate = true
		}
	}
	if len(sel.GroupBy) > 0 {
		plan.aggregate = true
		aliases := map[string]Expr{}
		for i, it := range sel.Items {
			if !it.Star && it.Alias != "" {
				aliases[strings.ToLower(it.Alias)] = plan.items[itemStart[i]].expr
			}
		}
		for i, g := range sel.GroupBy {
			// GROUP BY <ordinal> or <output alias>
			if lit, ok := g.(*Literal); ok && lit.V.T.Kind.isInteger() && !lit.V.Null {
				n := int(lit.V.I)
				if n < 1 || n > len(plan.items) {
					return nil, errorf(adbc.StatusInvalidArgument, "GROUP BY position %d is out of range", n)
				}
				g = plan.items[n-1].expr
			} else {
				g = replaceAliases(g, aliases, plan.meta)
			}
			if err := e.bind(ctx, g); err != nil {
				return nil, err
			}
			if isAggregate(g) {
				return nil, errorf(adbc.StatusInvalidArgument, "aggregates are not allowed in GROUP BY")
			}
			if containsWindow(g) {
				return nil, errorf(adbc.StatusInvalidArgument, "window functions are not allowed in GROUP BY")
			}
			sel.GroupBy[i] = g
		}
	}
	if sel.Having != nil {
		plan.aggregate = true
		aliases := map[string]Expr{}
		for i, it := range sel.Items {
			if !it.Star && it.Alias != "" {
				aliases[strings.ToLower(it.Alias)] = plan.items[itemStart[i]].expr
			}
		}
		having := replaceAliases(sel.Having, aliases, plan.meta)
		if err := e.bind(ctx, having); err != nil {
			return nil, err
		}
		if containsWindow(having) {
			return nil, errorf(adbc.StatusInvalidArgument, "window functions are not allowed in HAVING")
		}
		plan.having = having
	}
	if sel.Where != nil {
		if isAggregate(sel.Where) {
			return nil, errorf(adbc.StatusInvalidArgument, "aggregates are not allowed in WHERE")
		}
		if containsWindow(sel.Where) {
			return nil, errorf(adbc.StatusInvalidArgument, "window functions are not allowed in WHERE; filter on them with QUALIFY or in an outer query")
		}
		restore := e.planningOnly(noInput)
		err := e.bind(ctx, sel.Where)
		restore()
		if err != nil {
			return nil, err
		}
	}
	if jp != nil {
		jp.planPushdown(sel.Where)
	}
	for _, o := range sel.OrderBy {
		expr := o.Expr
		// ORDER BY <ordinal>
		if lit, ok := expr.(*Literal); ok && lit.V.T.Kind.isInteger() && !lit.V.Null {
			n := int(lit.V.I)
			if n < 1 || n > len(plan.items) {
				return nil, errorf(adbc.StatusInvalidArgument, "ORDER BY position %d is out of range", n)
			}
			plan.order = append(plan.order, planOrder{expr: plan.items[n-1].expr, desc: o.Desc, nullsFirst: o.Nulls == NullsFirst})
			continue
		}
		// ORDER BY <output alias>
		if c, ok := expr.(*ColumnRef); ok {
			matched := false
			for i, it := range sel.Items {
				if !it.Star && it.Alias != "" && strings.EqualFold(it.Alias, c.Name) {
					plan.order = append(plan.order, planOrder{expr: plan.items[itemStart[i]].expr, desc: o.Desc, nullsFirst: o.Nulls == NullsFirst})
					matched = true
					break
				}
			}
			if matched {
				continue
			}
		}
		if err := e.bind(ctx, expr); err != nil {
			return nil, err
		}
		plan.order = append(plan.order, planOrder{expr: expr, desc: o.Desc, nullsFirst: o.Nulls == NullsFirst})
	}
	if sel.Qualify != nil {
		// QUALIFY filters after window functions; like HAVING, it may use
		// output aliases (QUALIFY rn = 1).
		aliases := map[string]Expr{}
		for i, it := range sel.Items {
			if !it.Star && it.Alias != "" {
				aliases[strings.ToLower(it.Alias)] = plan.items[itemStart[i]].expr
			}
		}
		qualify := replaceAliases(sel.Qualify, aliases, plan.meta)
		if err := e.bind(ctx, qualify); err != nil {
			return nil, err
		}
		if isAggregate(qualify) {
			plan.aggregate = true
		}
		plan.qualify = qualify
	}
	// The WINDOW clause's definitions are bound even if no call uses them,
	// as in Postgres (those that are used were bound with their calls).
	for _, nw := range sel.Windows {
		for _, x := range windowSpecExprs(nw.Spec) {
			if err := e.bind(ctx, x); err != nil {
				return nil, err
			}
		}
	}
	if err := e.planWindows(plan, types); err != nil {
		return nil, err
	}
	if sel.Distinct {
		if err := e.planDistinct(ctx, plan, itemStart, types); err != nil {
			return nil, err
		}
	}
	if sel.GroupingSets != nil || usesGrouping(sel) {
		return e.planGroupingSets(plan, types)
	}
	return plan, nil
}

// starColumns returns the columns `*` or `rel.*` expands to. In a join, a
// qualified star picks one item's columns (named alias.column in the joined
// relation); otherwise the qualifier must name the one FROM item.
func starColumns(meta *tableMeta, rels []relation, joined bool, qual []string) ([]columnMeta, error) {
	if len(qual) == 0 {
		if meta.join != nil && meta.join.star != nil {
			// NATURAL JOIN: merged columns first (see planNatural).
			return meta.join.star, nil
		}
		return meta.Columns, nil
	}
	name := qual[len(qual)-1]
	for _, r := range rels {
		if !strings.EqualFold(r.name, name) {
			continue
		}
		// schema.table.* (and catalog.schema.table.*) must match the table
		// itself, not an alias.
		if len(qual) >= 2 && (r.meta == nil || !strings.EqualFold(r.meta.Schema, qual[len(qual)-2]) ||
			!strings.EqualFold(r.meta.Name, name)) {
			continue
		}
		if len(qual) == 3 && !strings.EqualFold(qual[0], catalogName) {
			continue
		}
		if !joined {
			return meta.Columns, nil
		}
		var cols []columnMeta
		for _, c := range meta.Columns {
			if strings.HasPrefix(c.Name, r.prefix) {
				cols = append(cols, c)
			}
		}
		return cols, nil
	}
	return nil, errorf(adbc.StatusInvalidArgument, "missing FROM-clause entry for table %q", strings.Join(qual, "."))
}

// distinctAsGroupBy rewrites SELECT DISTINCT over plain expressions (no
// aggregates, windows, stars, subqueries or RANDOM()) as GROUP BY them.
func distinctAsGroupBy(sel *SelectStmt) (*SelectStmt, bool) {
	if !sel.Distinct || len(sel.DistinctOn) > 0 || len(sel.GroupBy) > 0 || sel.GroupingSets != nil || sel.Having != nil ||
		len(sel.Windows) > 0 || sel.Qualify != nil {
		return nil, false
	}
	g := *sel
	g.Distinct, g.With, g.GroupBy = false, nil, nil
	for _, it := range sel.Items {
		if it.Star || isAggregate(it.Expr) || containsWindow(it.Expr) || hasVolatile(it.Expr) || hasSubquery(it.Expr) {
			return nil, false
		}
		g.GroupBy = append(g.GroupBy, it.Expr)
	}
	for _, o := range sel.OrderBy {
		if containsWindow(o.Expr) {
			return nil, false
		}
	}
	return &g, true
}

func hasSubquery(e Expr) bool {
	found := false
	walkExpr(e, func(x Expr) {
		if _, ok := x.(*Subquery); ok {
			found = true
		}
	})
	return found
}

// checkDistinctOrder enforces Postgres's rule that with SELECT DISTINCT
// every ORDER BY expression appears in the select list (the duplicates
// removed would otherwise make the order ambiguous).
func checkDistinctOrder(plan *selectPlan) error {
	for _, o := range plan.order {
		found := false
		for _, it := range plan.items {
			if exprEqual(it.expr, o.expr) {
				found = true
				break
			}
		}
		if !found {
			return errorf(adbc.StatusInvalidArgument, "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
		}
	}
	return nil
}

// planDistinct plans SELECT DISTINCT and DISTINCT ON (…). DISTINCT ON
// expressions may be output positions or aliases, like ORDER BY, and must
// match the leading ORDER BY expressions (as in Postgres), so the row kept
// for each key is the first in ORDER BY order.
func (e *executor) planDistinct(ctx context.Context, plan *selectPlan, itemStart []int, types map[string]ColType) error {
	sel := plan.sel
	plan.distinct = true
	if len(sel.DistinctOn) == 0 {
		return checkDistinctOrder(plan)
	}
	for _, x := range sel.DistinctOn {
		expr := x
		if lit, ok := x.(*Literal); ok && lit.V.T.Kind.isInteger() && !lit.V.Null {
			n := int(lit.V.I)
			if n < 1 || n > len(plan.items) {
				return errorf(adbc.StatusInvalidArgument, "DISTINCT ON position %d is out of range", n)
			}
			expr = plan.items[n-1].expr
		} else if c, ok := x.(*ColumnRef); ok && c.Qualifier == "" {
			for i, it := range sel.Items {
				if !it.Star && it.Alias != "" && strings.EqualFold(it.Alias, c.Name) {
					expr = plan.items[itemStart[i]].expr
					break
				}
			}
		}
		if expr == x {
			if containsWindow(x) {
				return errorf(adbc.StatusInvalidArgument, "window functions are not allowed in DISTINCT ON")
			}
			if err := e.bind(ctx, x); err != nil {
				return err
			}
			if isAggregate(x) {
				plan.aggregate = true
			}
		}
		t, err := inferType(expr, types, e.paramTypes)
		if err != nil {
			return invalidArg(err)
		}
		plan.distinctOn = append(plan.distinctOn, planItem{expr: expr, name: "__distinct_on", typ: t})
	}
	matched := make([]bool, len(plan.distinctOn))
	left := len(plan.distinctOn)
	for _, o := range plan.order {
		if left == 0 {
			break
		}
		found := false
		for i, d := range plan.distinctOn {
			if exprEqual(d.expr, o.expr) {
				found = true
				if !matched[i] {
					matched[i] = true
					left--
				}
			}
		}
		if !found {
			return errorf(adbc.StatusInvalidArgument, "SELECT DISTINCT ON expressions must match initial ORDER BY expressions")
		}
	}
	return nil
}

func applyLimit[T any](rows []T, offset, limit *int64) []T {
	if offset != nil {
		if *offset >= int64(len(rows)) {
			return rows[:0]
		}
		rows = rows[*offset:]
	}
	if limit != nil && *limit < int64(len(rows)) {
		rows = rows[:*limit]
	}
	return rows
}

// lessKeys orders rows by sort keys; NULLs sort last in either direction,
// matching RediSearch SORTBY.
func lessKeys(a, b []Value, order []planOrder) bool {
	for i, o := range order {
		x, y := a[i], b[i]
		switch {
		case x.Null && y.Null:
			continue
		case x.Null:
			return o.nullsFirst
		case y.Null:
			return !o.nullsFirst
		}
		c, _ := compareValues(x, y)
		if c == 0 {
			continue
		}
		if o.desc {
			return c > 0
		}
		return c < 0
	}
	return false
}

func (e *executor) selectWithoutTable(ctx context.Context, plan *selectPlan, params []Value) ([][]Value, error) {
	if plan.aggregate {
		return nil, errorf(adbc.StatusNotImplemented, "aggregates require a FROM clause")
	}
	if plan.noRows || e.neverTrue(ctx, plan.sel.Where) {
		// Nothing to compute: its subqueries were only planned (see
		// empty.go).
		return nil, nil
	}
	env := e.newEnv(ctx, nil, params)
	if plan.sel.Where != nil {
		ok, err := env.eval(plan.sel.Where)
		if err != nil {
			return nil, invalidArg(err)
		}
		if b, valid := truthy(ok); !valid || !b {
			return nil, nil
		}
	}
	if plan.windowed() {
		// Windows over the single row.
		keep, err := e.applyWindows(env, plan, 1, func(int) {})
		if err != nil || len(keep) == 0 {
			return nil, err
		}
	}
	row := make([]Value, len(plan.items))
	for i, it := range plan.items {
		v, err := env.eval(it.expr)
		if err != nil {
			return nil, invalidArg(err)
		}
		row[i] = v
	}
	return applyLimit([][]Value{row}, plan.sel.Offset, plan.sel.Limit), nil
}

// ---- aggregates ----

// accumulator reduces one aggregate call over a group: COUNT, SUM, AVG, MIN
// and MAX here, the other aggregates in ext (aggfuncs.go). addRow applies
// FILTER and DISTINCT.
type accumulator struct {
	fn       *Func
	count    int64
	sumI     int64
	sumD     *big.Int
	sumScale int32
	sumF     float64
	best     Value
	has      bool
	overflow bool
	distinct map[string]bool
	ext      aggState
	in       []Value // input buffer (aggInput)
}

func newAccumulator(f *Func) *accumulator {
	return &accumulator{fn: f, ext: newAggState(f)}
}

func (a *accumulator) add(v Value) {
	if a.fn.Star {
		a.count++
		return
	}
	if v.Null {
		return
	}
	a.count++
	switch a.fn.Name {
	case "SUM", "AVG":
		switch {
		case v.T.Kind.isInteger() || v.T.Kind == KindBool:
			s := a.sumI + v.I
			if (s > a.sumI) != (v.I > 0) {
				a.overflow = true
			}
			a.sumI = s
			a.sumF += float64(v.I)
		case v.T.Kind == KindDecimal:
			if a.sumD == nil {
				a.sumD, a.sumScale = new(big.Int), v.T.Scale
			}
			a.sumD.Add(a.sumD, rescaleDecimal(v.D, v.T.Scale, a.sumScale))
			a.sumF += decimalToFloat(v.D, v.T.Scale)
		default:
			f, _ := v.asFloat()
			a.sumF += f
		}
	case "MIN", "MAX":
		if !a.has {
			a.best, a.has = v, true
			return
		}
		c, ok := compareValues(v, a.best)
		if ok && ((a.fn.Name == "MIN" && c < 0) || (a.fn.Name == "MAX" && c > 0)) {
			a.best = v
		}
	}
}

// result returns the aggregate's value; env is at the group's
// representative row (for the direct arguments of ordered-set aggregates).
func (a *accumulator) result(env *evalEnv, t ColType) (Value, error) {
	if a.ext != nil {
		return a.ext.result(env, t)
	}
	switch a.fn.Name {
	case "COUNT":
		return intValue(typeInt64, a.count), nil
	case "AVG":
		if a.count == 0 {
			return nullValue(typeFloat64), nil
		}
		return floatValue(typeFloat64, a.sumF/float64(a.count)), nil
	case "SUM":
		if a.count == 0 {
			return nullValue(t), nil
		}
		switch t.Kind {
		case KindInt64:
			if a.overflow {
				return Value{}, fmt.Errorf("integer overflow in SUM")
			}
			return intValue(typeInt64, a.sumI), nil
		case KindDecimal:
			return decimalValue(a.sumD, t.Precision, a.sumScale), nil
		}
		return floatValue(typeFloat64, a.sumF), nil
	default:
		if !a.has {
			return nullValue(t), nil
		}
		return a.best, nil
	}
}

func collectAggregates(e Expr, out *[]*Func) {
	walkExpr(e, func(x Expr) {
		if f, ok := x.(*Func); ok && aggregateFuncs[f.Name] && !slices.Contains(*out, f) {
			*out = append(*out, f)
		}
	})
}

// ---- UPDATE / DELETE ----
//
// UPDATE, DELETE and MERGE (dml.go) first find their rows and compute and
// check every new value (casts, column types, NOT NULL, CHECK constraints),
// and only then write.
// Writes are pipelined, pipelineChunk commands per round trip, each touching
// a single row key, so they work on a cluster, where a table's rows are
// spread over hash slots. There is no transaction: if a write fails part-way
// (say the connection drops), the chunks already sent stay applied.

func (e *executor) runUpdate(ctx context.Context, st *UpdateStmt, params []Value) (int64, error) {
	pop, err := e.pushCTEs(st.With)
	if err != nil {
		return 0, err
	}
	defer pop()
	if len(st.From) > 0 {
		return e.runUpdateFrom(ctx, st, params)
	}
	// With a WHERE that is never true, no row is read, and the subqueries of
	// the statement are only planned (see empty.go).
	defer e.planningOnly(e.neverTrue(ctx, st.Where))()
	meta, err := e.loadTable(ctx, st.Table)
	if err != nil {
		return 0, err
	}
	name := st.Alias
	if name == "" {
		name = meta.Name
	}
	cols, err := setTargets(meta, name, st.Sets)
	if err != nil {
		return 0, err
	}
	need := map[string]bool{}
	for _, s := range st.Sets {
		needs, err := e.bindIn(ctx, s.Expr, meta, name)
		if err != nil {
			return 0, err
		}
		columnRefs(s.Expr, need)
		maps.Copy(need, needs)
	}
	checks, err := e.tableChecks(ctx, meta)
	if err != nil {
		return 0, err
	}
	checks.need(need, "")
	ret, err := e.planReturning(ctx, st.Returning, []relation{{name: name, meta: meta}}, meta, false, need)
	if err != nil {
		return 0, err
	}
	keys, rows, err := e.matchRows(ctx, meta, name, st.Where, params, need)
	if err != nil {
		return 0, err
	}
	env := e.newEnv(ctx, meta.types(), params)
	changes := make([]rowChange, len(keys))
	for r, key := range keys {
		env.row = rows[r]
		vals, err := setValues(env, meta, cols, st.Sets)
		if err != nil {
			return 0, err
		}
		if err := checks.checkChanged(rows[r], "", cols, vals); err != nil {
			return 0, err
		}
		changes[r] = newRowChange(meta, key, cols, vals)
		if err := ret.addChanged(rows[r], meta, "", cols, vals, ""); err != nil {
			return 0, err
		}
	}
	if err := e.writeUpdates(ctx, meta, changes); err != nil {
		return 0, err
	}
	return int64(len(keys)), nil
}

func (e *executor) runDelete(ctx context.Context, st *DeleteStmt, params []Value) (int64, error) {
	pop, err := e.pushCTEs(st.With)
	if err != nil {
		return 0, err
	}
	defer pop()
	if len(st.Using) > 0 {
		return e.runDeleteUsing(ctx, st, params)
	}
	defer e.planningOnly(e.neverTrue(ctx, st.Where))() // as in runUpdate
	meta, err := e.loadTable(ctx, st.Table)
	if err != nil {
		return 0, err
	}
	name := st.Alias
	if name == "" {
		name = meta.Name
	}
	need := map[string]bool{}
	ret, err := e.planReturning(ctx, st.Returning, []relation{{name: name, meta: meta}}, meta, false, need)
	if err != nil {
		return 0, err
	}
	keys, rows, err := e.matchRows(ctx, meta, name, st.Where, params, need)
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if err := ret.add(row, ""); err != nil {
			return 0, err
		}
	}
	if err := e.deleteKeys(ctx, meta, keys); err != nil {
		return 0, err
	}
	return int64(len(keys)), nil
}

// setTargets resolves the columns of a SET list in the target table. A
// qualified column (`t.col`) must name the target by its alias or name.
func setTargets(meta *tableMeta, alias string, sets []SetClause) ([]int, error) {
	cols := make([]int, len(sets))
	for i, s := range sets {
		if s.Qualifier != "" && !strings.EqualFold(s.Qualifier, alias) && !strings.EqualFold(s.Qualifier, meta.Name) {
			return nil, errorf(adbc.StatusInvalidArgument, "SET column %s.%s is not in the table being updated (%q)", s.Qualifier, s.Column, alias)
		}
		idx, ok := meta.resolve(s.Column)
		if !ok {
			return nil, errorf(adbc.StatusInvalidArgument, "column %q does not exist in table %q", s.Column, meta.Name)
		}
		cols[i] = idx
	}
	return cols, nil
}

// setValues evaluates a SET list for the row in env: the new value of each
// target column, coerced to its type and checked against its length and
// then NOT NULL.
func setValues(env *evalEnv, meta *tableMeta, cols []int, sets []SetClause) ([]Value, error) {
	vals := make([]Value, len(sets))
	for i, s := range sets {
		col := meta.Columns[cols[i]]
		v, err := env.eval(s.Expr)
		if err != nil {
			return nil, invalidArg(err)
		}
		cv, err := Coerce(v, col.Type)
		if err != nil {
			return nil, errorf(adbc.StatusInvalidArgument, "column %q: %v", col.Name, err)
		}
		if vals[i], err = fitLength(col.Type, cv); err != nil {
			return nil, err
		}
	}
	for i, v := range vals {
		if col := meta.Columns[cols[i]]; v.Null && !col.Nullable {
			return nil, errorf(adbc.StatusIntegrity, "NULL value in column %q violates not-null constraint", col.Name)
		}
	}
	return vals, nil
}

// rowChange is a pending update of one row HASH: fields to set (HSET) and
// the fields of columns set to NULL (HDEL).
type rowChange struct {
	key string
	set []any
	del []string
}

func newRowChange(meta *tableMeta, key string, cols []int, vals []Value) rowChange {
	ch := rowChange{key: key}
	for i, v := range vals {
		col := meta.Columns[cols[i]]
		// A row that would read the column's missing value without a field
		// is marked when it is set to NULL (see defaults.go).
		marked := col.readsMissing(meta, key)
		if v.Null {
			ch.del = append(ch.del, col.field())
			if marked {
				ch.set = append(ch.set, nullMarker(col.field()), "1")
			}
		} else {
			ch.set = append(ch.set, col.field(), encodeStored(v))
			if marked {
				ch.del = append(ch.del, nullMarker(col.field()))
			}
		}
	}
	return ch
}

// writeUpdates applies row changes to a table with pipelined HSET / HDEL.
// The index follows the HASHes by itself, once the levels of the strings
// written are recorded (see tags.go). Like every write, it is refused
// while a re-key moves the table's rows (see checkWritable in rekey.go),
// and it checks the table's keys between pipelines as writeRows does.
func (e *executor) writeUpdates(ctx context.Context, meta *tableMeta, changes []rowChange) error {
	if len(changes) == 0 {
		return nil
	}
	if err := e.store.checkWritable(ctx, meta); err != nil {
		return err
	}
	if err := e.store.raiseTagLevels(ctx, meta, changeTagLevels(meta, changes)); err != nil {
		return err
	}
	for start := 0; start < len(changes); start += pipelineChunk {
		if start > 0 {
			if err := e.store.checkWritten(ctx, meta); err != nil {
				return err
			}
		}
		end := min(start+pipelineChunk, len(changes))
		pipe := e.store.client.Pipeline()
		for _, ch := range changes[start:end] {
			if len(ch.set) > 0 {
				// An HSET on a row the table's DROP deleted makes a new key.
				meta.wrote.keys = append(meta.wrote.keys, ch.key)
				pipe.HSet(ctx, ch.key, ch.set...)
			}
			if len(ch.del) > 0 {
				pipe.HDel(ctx, ch.key, ch.del...)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return wrapRedis(err, "failed to update rows")
		}
	}
	if len(changes) > pipelineChunk {
		return e.store.checkKeys(ctx, meta, true)
	}
	return e.store.checkWritten(ctx, meta)
}

// deleteKeys deletes row HASHes with pipelined DELs.
func (e *executor) deleteKeys(ctx context.Context, meta *tableMeta, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := e.store.checkWritable(ctx, meta); err != nil {
		return err
	}
	for start := 0; start < len(keys); start += pipelineChunk {
		end := min(start+pipelineChunk, len(keys))
		// One DEL per key: rows live in different hash slots.
		pipe := e.store.client.Pipeline()
		for _, k := range keys[start:end] {
			pipe.Del(ctx, k)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return wrapRedis(err, "failed to delete rows")
		}
	}
	return e.store.checkWritten(ctx, meta)
}
