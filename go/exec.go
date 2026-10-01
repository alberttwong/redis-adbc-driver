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
	"math/big"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// executor runs parsed statements against a store.
type executor struct {
	store    *store
	schema   string // current schema
	pushdown string // aggregate pushdown mode

	// Per-statement state (see bind.go).
	cache      *execCache
	params     []Value
	paramTypes []ColType
	outer      *evalEnv // row of the enclosing query (correlated subqueries)
	scopes     []*scope
	pendingSq  *Subquery
	ctes       []map[string]*CTE
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

func (e *executor) resolveTable(t TableName) (string, string, error) {
	schema := t.Schema
	if schema == "" {
		schema = e.schema
	}
	if t.Catalog != "" && t.Catalog != catalogName {
		return "", "", tableNotFound(schema, t.Name)
	}
	return schema, t.Name, nil
}

func (e *executor) loadTable(ctx context.Context, t TableName) (*tableMeta, error) {
	schema, name, err := e.resolveTable(t)
	if err != nil {
		return nil, err
	}
	return e.store.getTable(ctx, schema, name)
}

func (e *executor) execute(ctx context.Context, ps ParsedStmt, params []Value, paramTypes []ColType) (execResult, error) {
	e.cache = newExecCache()
	e.params, e.paramTypes = params, paramTypes
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
		n, err := e.runInsert(ctx, st, params)
		return execResult{affected: n}, err
	case *UpdateStmt:
		n, err := e.runUpdate(ctx, st, params)
		return execResult{affected: n}, err
	case *DeleteStmt:
		n, err := e.runDelete(ctx, st, params)
		return execResult{affected: n}, err
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
		return execResult{affected: -1}, e.store.dropSchema(ctx, st.Name, st.IfExists)
	}
	return execResult{}, errorf(adbc.StatusNotImplemented, "unsupported statement %T", ps.Stmt)
}

// ---- DDL ----

func (e *executor) runCreateTable(ctx context.Context, st *CreateTableStmt) error {
	schema, name, err := e.resolveTable(st.Table)
	if err != nil {
		return err
	}
	meta := &tableMeta{Schema: schema, Name: name}
	noIndex := map[string]bool{}
	for _, c := range st.Columns {
		meta.Columns = append(meta.Columns, columnMeta{Name: c.Name, Type: c.Type, Nullable: !c.NotNull})
		if c.NoIndex {
			noIndex[c.Name] = true
		}
	}
	if err := meta.applyIndexPolicy(nil, noIndex); err != nil {
		return err
	}
	_, err = e.store.createTable(ctx, meta, st.IfNotExists)
	return err
}

// runCreateTableAs implements CREATE TABLE … AS SELECT: the table's columns
// take the names and types of the query's result, and the result rows are
// inserted. It returns the number of rows inserted.
func (e *executor) runCreateTableAs(ctx context.Context, st *CreateTableStmt) (int64, error) {
	schema, name, err := e.resolveTable(st.Table)
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
	for _, c := range plan.columns() {
		t := c.Type
		if t.Kind == KindNull {
			t = typeString // SELECT NULL AS x: no type information
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
		return &Cast{X: replaceAliases(x.X, aliases, meta), T: x.T}
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
	targets := make([]int, 0, len(meta.Columns))
	if len(st.Columns) == 0 {
		for i := range meta.Columns {
			targets = append(targets, i)
		}
	} else {
		for _, name := range st.Columns {
			i, ok := meta.resolve(name)
			if !ok {
				return 0, errorf(adbc.StatusInvalidArgument, "column %q does not exist in table %q", name, meta.Name)
			}
			if slices.Contains(targets, i) {
				return 0, errorf(adbc.StatusInvalidArgument, "column %q specified more than once", name)
			}
			targets = append(targets, i)
		}
	}
	if st.Select != nil {
		return e.insertSelect(ctx, st, meta, targets)
	}
	env := e.newEnv(ctx, nil, params)
	rows := make([][]Value, 0, len(st.Rows))
	for _, exprs := range st.Rows {
		if len(exprs) != len(targets) {
			return 0, errorf(adbc.StatusInvalidArgument, "INSERT has %d target columns but %d values", len(targets), len(exprs))
		}
		row := make([]Value, len(meta.Columns))
		for i, c := range meta.Columns {
			row[i] = nullValue(c.Type)
		}
		for j, expr := range exprs {
			if _, err := e.bindIn(ctx, expr, nil, ""); err != nil {
				return 0, err
			}
			v, err := env.eval(expr)
			if err != nil {
				return 0, invalidArg(err)
			}
			col := meta.Columns[targets[j]]
			cv, err := Coerce(v, col.Type)
			if err != nil {
				return 0, errorf(adbc.StatusInvalidArgument, "column %q: %v", col.Name, err)
			}
			row[targets[j]] = cv
		}
		rows = append(rows, row)
	}
	return e.store.insertRows(ctx, meta, rows)
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
	rows := make([][]Value, len(results))
	for r, res := range results {
		row := make([]Value, len(meta.Columns))
		for i, c := range meta.Columns {
			row[i] = nullValue(c.Type)
		}
		for j, v := range res {
			col := meta.Columns[targets[j]]
			cv, err := Coerce(v, col.Type)
			if err != nil {
				return 0, errorf(adbc.StatusInvalidArgument, "column %q: %v", col.Name, err)
			}
			row[targets[j]] = cv
		}
		rows[r] = row
	}
	return e.store.insertRows(ctx, meta, rows)
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
	if len(sel.With) > 0 {
		defs := map[string]*CTE{}
		for i := range sel.With {
			key := strings.ToLower(sel.With[i].Name)
			if _, dup := defs[key]; dup {
				return nil, errorf(adbc.StatusInvalidArgument, "WITH query name %q specified more than once", sel.With[i].Name)
			}
			defs[key] = &sel.With[i]
		}
		e.ctes = append(e.ctes, defs)
		defer func() { e.ctes = e.ctes[:len(e.ctes)-1] }()
	}
	if sel.SetOp != nil {
		return e.planSetOp(ctx, sel, plan)
	}
	var types map[string]ColType
	var rels []relation
	var jp *joinPlan
	if len(sel.Joins) > 0 {
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
	itemStart := make([]int, len(sel.Items))
	for i, it := range sel.Items {
		itemStart[i] = len(plan.items)
		if it.Star {
			if plan.meta == nil {
				return nil, errorf(adbc.StatusInvalidArgument, "SELECT * requires a FROM clause")
			}
			for _, c := range plan.meta.Columns {
				plan.items = append(plan.items, planItem{expr: &ColumnRef{Name: c.Name}, name: c.field(), typ: c.Type})
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
						name = col.field()
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
		plan.having = having
	}
	if sel.Where != nil {
		if isAggregate(sel.Where) {
			return nil, errorf(adbc.StatusInvalidArgument, "aggregates are not allowed in WHERE")
		}
		if err := e.bind(ctx, sel.Where); err != nil {
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
	return plan, nil
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
}

func (a *accumulator) add(v Value) {
	if a.fn.Star {
		a.count++
		return
	}
	if v.Null {
		return
	}
	if a.fn.Distinct {
		key := v.T.Kind.String() + "\x00" + v.Text()
		if a.distinct == nil {
			a.distinct = map[string]bool{}
		}
		if a.distinct[key] {
			return
		}
		a.distinct[key] = true
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

func (a *accumulator) result(t ColType) (Value, error) {
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

func (e *executor) runUpdate(ctx context.Context, st *UpdateStmt, params []Value) (int64, error) {
	meta, err := e.loadTable(ctx, st.Table)
	if err != nil {
		return 0, err
	}
	need := map[string]bool{}
	targets := make([]int, len(st.Sets))
	for i, s := range st.Sets {
		idx, ok := meta.resolve(s.Column)
		if !ok {
			return 0, errorf(adbc.StatusInvalidArgument, "column %q does not exist in table %q", s.Column, meta.Name)
		}
		targets[i] = idx
		needs, err := e.bindIn(ctx, s.Expr, meta, meta.Name)
		if err != nil {
			return 0, err
		}
		columnRefs(s.Expr, need)
		for k := range needs {
			need[k] = true
		}
	}
	keys, rows, err := e.matchRows(ctx, meta, st.Where, params, need)
	if err != nil {
		return 0, err
	}
	env := e.newEnv(ctx, meta.types(), params)
	for start := 0; start < len(keys); start += pipelineChunk {
		end := min(start+pipelineChunk, len(keys))
		pipe := e.store.client.Pipeline()
		for r := start; r < end; r++ {
			env.row = rows[r]
			var set []any
			var del []string
			for i, s := range st.Sets {
				col := meta.Columns[targets[i]]
				v, err := env.eval(s.Expr)
				if err != nil {
					return 0, invalidArg(err)
				}
				cv, err := Coerce(v, col.Type)
				if err != nil {
					return 0, errorf(adbc.StatusInvalidArgument, "column %q: %v", col.Name, err)
				}
				if cv.Null {
					if !col.Nullable {
						return 0, errorf(adbc.StatusIntegrity, "NULL value in column %q violates not-null constraint", col.Name)
					}
					del = append(del, col.field())
				} else {
					set = append(set, col.field(), encodeStored(cv))
				}
			}
			if len(set) > 0 {
				pipe.HSet(ctx, keys[r], set...)
			}
			if len(del) > 0 {
				pipe.HDel(ctx, keys[r], del...)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, wrapRedis(err, "failed to update rows")
		}
	}
	return int64(len(keys)), nil
}

func (e *executor) runDelete(ctx context.Context, st *DeleteStmt, params []Value) (int64, error) {
	meta, err := e.loadTable(ctx, st.Table)
	if err != nil {
		return 0, err
	}
	keys, _, err := e.matchRows(ctx, meta, st.Where, params, nil)
	if err != nil {
		return 0, err
	}
	for start := 0; start < len(keys); start += pipelineChunk {
		end := min(start+pipelineChunk, len(keys))
		// One DEL per key: rows live in different hash slots.
		pipe := e.store.client.Pipeline()
		for _, k := range keys[start:end] {
			pipe.Del(ctx, k)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, wrapRedis(err, "failed to delete rows")
		}
	}
	return int64(len(keys)), nil
}
