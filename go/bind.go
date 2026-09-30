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

// Name resolution, subqueries, CTEs and derived tables.
//
// Every SELECT (and the target of UPDATE/DELETE) pushes a scope holding the
// relations its columns may come from. A column reference resolves in the
// innermost scope that has it; a reference that resolves in an enclosing
// scope is a correlated (outer) reference, and every subquery between the
// reference and its scope is marked correlated.
//
// Subqueries run through the same executor: uncorrelated ones once per
// statement (cached), correlated ones once per distinct set of outer values
// (memoised). CTEs and derived tables are run once and held in memory as a
// relation that the regular driver-side filter/sort/group paths read.

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

type relation struct {
	name string // alias, table or CTE name
	meta *tableMeta
}

type scope struct {
	rels []relation
	// sq is the subquery whose body this scope is (nil at the top level).
	sq *Subquery
	// needs are this scope's columns read by correlated subqueries; the
	// scope's query must fetch them even if it doesn't use them itself.
	needs map[string]bool
}

// execCache holds per-statement results shared by nested executors.
type execCache struct {
	sub           map[*Subquery][][]Value
	memo          map[*Subquery]map[string][][]Value
	ctes          map[*CTE]*tableMeta
	derived       map[*SelectStmt]*tableMeta
	materializing map[any]bool
}

func newExecCache() *execCache {
	return &execCache{
		sub:           map[*Subquery][][]Value{},
		memo:          map[*Subquery]map[string][][]Value{},
		ctes:          map[*CTE]*tableMeta{},
		derived:       map[*SelectStmt]*tableMeta{},
		materializing: map[any]bool{},
	}
}

func (e *executor) ensureCache() {
	if e.cache == nil {
		e.cache = newExecCache()
	}
}

// newEnv creates an evaluation environment wired to this executor, so that
// subqueries and outer references can be evaluated.
func (e *executor) newEnv(ctx context.Context, types map[string]ColType, params []Value) *evalEnv {
	return &evalEnv{ctx: ctx, exec: e, outer: e.outer, types: types, params: params}
}

func (e *executor) pushScope(rels []relation) *scope {
	sc := &scope{rels: rels, sq: e.pendingSq, needs: map[string]bool{}}
	e.pendingSq = nil
	e.scopes = append(e.scopes, sc)
	return sc
}

func (e *executor) popScope() { e.scopes = e.scopes[:len(e.scopes)-1] }

// bindIn binds an expression in a new scope over one relation (meta may be
// nil for expressions without a FROM). It returns the columns of that
// relation read by correlated subqueries.
func (e *executor) bindIn(ctx context.Context, expr Expr, meta *tableMeta, name string) (map[string]bool, error) {
	var rels []relation
	if meta != nil {
		rels = []relation{{name: name, meta: meta}}
	}
	sc := e.pushScope(rels)
	defer e.popScope()
	if err := e.bind(ctx, expr); err != nil {
		return nil, err
	}
	return sc.needs, nil
}

// bind resolves the column references of expr against the scope stack and
// plans its subqueries.
func (e *executor) bind(ctx context.Context, expr Expr) error {
	var err error
	walkExpr(expr, func(x Expr) {
		if err != nil {
			return
		}
		switch v := x.(type) {
		case *ColumnRef:
			err = e.resolveColumn(v)
		case *Subquery:
			err = e.planSubquery(ctx, v)
		}
	})
	return err
}

func (e *executor) resolveColumn(c *ColumnRef) error {
	top := len(e.scopes) - 1
	for depth := 0; depth <= top; depth++ {
		sc := e.scopes[top-depth]
		for _, rel := range sc.rels {
			if c.Qualifier != "" && !strings.EqualFold(c.Qualifier, rel.name) &&
				!strings.EqualFold(c.Qualifier, rel.meta.Name) {
				continue
			}
			col, ok := rel.meta.column(c.Name)
			if !ok {
				continue
			}
			c.Name = col.Name
			c.Outer = depth
			c.OuterType = col.Type
			if depth > 0 {
				sc.needs[col.Name] = true
				for k := 0; k < depth; k++ {
					sq := e.scopes[top-k].sq
					if sq == nil {
						continue
					}
					sq.correlated = true
					ref := outerRef{name: col.Name, up: depth - k - 1}
					if !containsRef(sq.outerRefs, ref) {
						sq.outerRefs = append(sq.outerRefs, ref)
					}
				}
			}
			return nil
		}
	}
	name := c.Name
	if c.Qualifier != "" {
		name = c.Qualifier + "." + c.Name
	}
	if top >= 0 && len(e.scopes[top].rels) == 1 {
		return errorf(adbc.StatusInvalidArgument, "column %q does not exist in table %q", name, e.scopes[top].rels[0].meta.Name)
	}
	return errorf(adbc.StatusInvalidArgument, "column %q does not exist", name)
}

func containsRef(refs []outerRef, r outerRef) bool {
	for _, x := range refs {
		if x == r {
			return true
		}
	}
	return false
}

// planSubquery plans the body of a subquery in a nested scope.
func (e *executor) planSubquery(ctx context.Context, sq *Subquery) error {
	if sq.X != nil {
		if err := e.bind(ctx, sq.X); err != nil {
			return err
		}
	}
	if sq.Kind == SubqueryExists && sq.Select.Limit == nil && len(sq.Select.GroupBy) == 0 && sq.Select.Having == nil {
		// Only existence matters.
		one := int64(1)
		sq.Select.Limit = &one
	}
	e.pendingSq = sq
	plan, err := e.planSelect(ctx, sq.Select, e.paramTypes)
	e.pendingSq = nil
	if err != nil {
		return err
	}
	if sq.Kind != SubqueryExists && len(plan.items) != 1 {
		return errorf(adbc.StatusInvalidArgument, "subquery must return exactly one column, got %d", len(plan.items))
	}
	sq.plan = plan
	return nil
}

// ---- in-memory relations (CTEs, derived tables) ----

// memTable builds a relation from query results.
func memTable(name string, cols []resultColumn, rows [][]Value, rename []string) (*tableMeta, error) {
	if rename != nil && len(rename) != len(cols) {
		return nil, errorf(adbc.StatusInvalidArgument, "%q has %d columns but %d column names were given", name, len(cols), len(rename))
	}
	meta := &tableMeta{Name: name, isMem: true}
	seen := map[string]bool{}
	for i, c := range cols {
		n := c.Name
		if rename != nil {
			n = rename[i]
		}
		if seen[strings.ToLower(n)] {
			return nil, errorf(adbc.StatusInvalidArgument, "column name %q appears more than once in %q; add aliases", n, name)
		}
		seen[strings.ToLower(n)] = true
		meta.Columns = append(meta.Columns, columnMeta{Name: n, Type: c.Type, Nullable: true})
	}
	meta.mem = make([]map[string]Value, len(rows))
	for r, row := range rows {
		m := make(map[string]Value, len(row))
		for i, v := range row {
			m[meta.Columns[i].Name] = v
		}
		meta.mem[r] = m
	}
	return meta, nil
}

// materialize runs a query that has no access to enclosing scopes.
func (e *executor) materialize(ctx context.Context, key any, name string, sel *SelectStmt, rename []string) (*tableMeta, error) {
	if e.cache.materializing[key] {
		return nil, errorf(adbc.StatusNotImplemented, "%q refers to itself; recursive queries are not supported", name)
	}
	e.cache.materializing[key] = true
	defer delete(e.cache.materializing, key)
	savedScopes, savedSq := e.scopes, e.pendingSq
	e.scopes, e.pendingSq = nil, nil
	defer func() { e.scopes, e.pendingSq = savedScopes, savedSq }()
	plan, err := e.planSelect(ctx, sel, e.paramTypes)
	if err != nil {
		return nil, err
	}
	rows, err := e.runSelect(ctx, plan, e.params)
	if err != nil {
		return nil, err
	}
	return memTable(name, plan.columns(), rows, rename)
}

// lookupCTE finds a CTE visible from the current query.
func (e *executor) lookupCTE(name string) *CTE {
	for i := len(e.ctes) - 1; i >= 0; i-- {
		if def, ok := e.ctes[i][strings.ToLower(name)]; ok {
			return def
		}
	}
	return nil
}

// fromRelation resolves the FROM clause of a SELECT to a relation.
func (e *executor) fromRelation(ctx context.Context, sel *SelectStmt) (*tableMeta, string, error) {
	switch {
	case sel.FromSelect != nil:
		if m, ok := e.cache.derived[sel.FromSelect]; ok {
			return m, sel.FromAlias, nil
		}
		m, err := e.materialize(ctx, sel.FromSelect, sel.FromAlias, sel.FromSelect, nil)
		if err != nil {
			return nil, "", err
		}
		e.cache.derived[sel.FromSelect] = m
		return m, sel.FromAlias, nil
	case sel.From != nil:
		alias := sel.FromAlias
		if alias == "" {
			alias = sel.From.Name
		}
		if sel.From.Schema == "" && sel.From.Catalog == "" {
			if def := e.lookupCTE(sel.From.Name); def != nil {
				if m, ok := e.cache.ctes[def]; ok {
					return m, alias, nil
				}
				m, err := e.materialize(ctx, def, def.Name, def.Select, def.Columns)
				if err != nil {
					return nil, "", err
				}
				e.cache.ctes[def] = m
				return m, alias, nil
			}
		}
		meta, err := e.loadTable(ctx, *sel.From)
		return meta, alias, err
	}
	return nil, "", nil
}

// ---- subquery evaluation ----

// subqueryRows returns the rows of a subquery evaluated in env.
func (e *executor) subqueryRows(ctx context.Context, sq *Subquery, env *evalEnv) ([][]Value, error) {
	if sq.plan == nil {
		return nil, errorf(adbc.StatusInternal, "subquery was not planned")
	}
	e.ensureCache()
	key := ""
	if sq.correlated {
		var b strings.Builder
		for _, ref := range sq.outerRefs {
			v, err := env.lookupUp(ref.name, ref.up)
			if err != nil {
				return nil, err
			}
			if v.Null {
				b.WriteString("N\x00")
			} else {
				fmt.Fprintf(&b, "V%s\x00", v.Text())
			}
		}
		key = b.String()
		if rows, ok := e.cache.memo[sq][key]; ok {
			return rows, nil
		}
	} else if rows, ok := e.cache.sub[sq]; ok {
		return rows, nil
	}
	child := *e
	child.scopes, child.pendingSq = nil, nil
	if sq.correlated {
		child.outer = env
	}
	rows, err := child.runSelect(ctx, sq.plan, e.params)
	if err != nil {
		return nil, err
	}
	if sq.correlated {
		if e.cache.memo[sq] == nil {
			e.cache.memo[sq] = map[string][][]Value{}
		}
		e.cache.memo[sq][key] = rows
	} else {
		e.cache.sub[sq] = rows
	}
	return rows, nil
}

// lookupUp reads a column of the row `up` environments above env.
func (env *evalEnv) lookupUp(name string, up int) (Value, error) {
	cur := env
	for i := 0; i < up; i++ {
		if cur.outer == nil {
			return Value{}, fmt.Errorf("outer reference %q has no enclosing row", name)
		}
		cur = cur.outer
	}
	if v, ok := cur.row[name]; ok {
		return v, nil
	}
	if t, ok := cur.types[name]; ok {
		return nullValue(t), nil
	}
	return Value{}, fmt.Errorf("unknown outer column %q", name)
}

func (env *evalEnv) evalSubquery(sq *Subquery) (Value, error) {
	if env.exec == nil {
		return Value{}, fmt.Errorf("subqueries are not supported here")
	}
	rows, err := env.exec.subqueryRows(env.ctx, sq, env)
	if err != nil {
		return Value{}, err
	}
	switch sq.Kind {
	case SubqueryExists:
		return boolValue(len(rows) > 0), nil
	case SubqueryScalar:
		switch len(rows) {
		case 0:
			return nullValue(sq.plan.items[0].typ), nil
		case 1:
			return rows[0][0], nil
		}
		return Value{}, fmt.Errorf("scalar subquery returned %d rows", len(rows))
	}
	// IN
	x, err := env.eval(sq.X)
	if err != nil {
		return Value{}, err
	}
	if len(rows) == 0 {
		return boolValue(sq.Not), nil
	}
	if x.Null {
		return nullValue(typeBool), nil
	}
	found, sawNull := false, false
	for _, row := range rows {
		v := row[0]
		if v.Null {
			sawNull = true
			continue
		}
		if c, ok := compareValues(x, v); ok && c == 0 {
			found = true
			break
		}
	}
	switch {
	case found:
		return boolValue(!sq.Not), nil
	case sawNull:
		return nullValue(typeBool), nil
	}
	return boolValue(sq.Not), nil
}
