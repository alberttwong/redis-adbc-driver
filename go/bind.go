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
// innermost scope that has it (for t.col, an item visible as t: by its
// alias if it has one, else by its name; see resolveColumn); a reference
// that resolves in an enclosing scope is a correlated (outer) reference, and
// every subquery between the reference and its scope is marked correlated.
//
// Subqueries run through the same executor: uncorrelated ones once per
// statement (cached), correlated ones once per distinct set of outer values
// (memoised), unless they can run once as a semi-join (semijoin.go). CTEs and
// derived tables are run once and held in memory as a relation that the
// regular driver-side filter/sort/group paths read.

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

type relation struct {
	name string // alias, table or CTE name
	// aliased is set when name is an alias. schema is the schema of a table
	// or view, "" for other items: without an alias, schema.name names the
	// item too.
	aliased bool
	schema  string
	meta    *tableMeta
	// prefix is prepended to resolved column names ("alias." in joins).
	prefix string
	// hidden are columns merged by a NATURAL JOIN: only a qualified
	// reference reads them, an unqualified one means the merged column.
	hidden map[string]bool
}

type scope struct {
	rels []relation
	// sq is the subquery whose body this scope is (nil at the top level).
	sq *Subquery
	// needs are this scope's columns read by correlated subqueries; the
	// scope's query must fetch them even if it doesn't use them itself.
	needs map[string]bool
	// mergeReturning is set for the RETURNING list of a MERGE, the only
	// place merge_action() may be called.
	mergeReturning bool
}

// execCache holds per-statement results shared by nested executors.
type execCache struct {
	sub           map[*Subquery][][]Value
	inSets        map[*Subquery]*inSet
	rowSets       map[*Subquery]*rowSet
	inLists       map[*Binary]*inList // nil: evaluate the OR chain as written
	memo          map[*Subquery]map[string][][]Value
	semi          map[*Subquery]*semiResult
	ctes          map[*CTE]*tableMeta
	derived       map[*SelectStmt]*tableMeta
	materializing map[any]bool
	// working is the working table of each recursive CTE being evaluated.
	working map[*CTE]*tableMeta
	// extremes are the bounds of uncorrelated ANY / ALL subqueries.
	extremes map[*Subquery]*quantExtremes
	// prefixes caches prefixComplete by index and query (tags.go).
	prefixes map[string]bool
	// usedTemp is set when a name resolves to a temporary object.
	usedTemp bool
	// loaded are the tables the statement read (see checkReads in rekey.go).
	loaded []*tableMeta
	// branchTypes are the result types of CASE and IIF expressions
	// (branchValue).
	branchTypes map[Expr]ColType
}

func newExecCache() *execCache {
	return &execCache{
		sub:           map[*Subquery][][]Value{},
		inSets:        map[*Subquery]*inSet{},
		rowSets:       map[*Subquery]*rowSet{},
		inLists:       map[*Binary]*inList{},
		memo:          map[*Subquery]map[string][][]Value{},
		semi:          map[*Subquery]*semiResult{},
		ctes:          map[*CTE]*tableMeta{},
		derived:       map[*SelectStmt]*tableMeta{},
		materializing: map[any]bool{},
		working:       map[*CTE]*tableMeta{},
		extremes:      map[*Subquery]*quantExtremes{},
		branchTypes:   map[Expr]ColType{},
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

// tableRel is the relation of a table read under alias ("" for none).
func tableRel(meta *tableMeta, alias string) relation {
	if alias == "" {
		return relation{name: meta.Name, schema: meta.Schema, meta: meta}
	}
	return relation{name: alias, aliased: true, schema: meta.Schema, meta: meta}
}

// bindIn binds an expression in a new scope over one table read under alias
// ("" for none); meta may be nil for expressions without a FROM. It returns
// the columns of that table read by correlated subqueries.
func (e *executor) bindIn(ctx context.Context, expr Expr, meta *tableMeta, alias string) (map[string]bool, error) {
	var rels []relation
	if meta != nil {
		rels = []relation{tableRel(meta, alias)}
	}
	sc := e.pushScope(rels)
	defer e.popScope()
	if err := e.bind(ctx, expr); err != nil {
		return nil, err
	}
	if containsWindow(expr) {
		return nil, errWindowPlacement()
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
		case *Binary:
			if _, ok := v.L.(*RowExpr); ok {
				err = e.rowOperatorError(ctx, v)
			} else if _, ok := v.R.(*RowExpr); ok {
				err = e.rowOperatorError(ctx, v)
			}
		case *IsNull:
			if _, ok := v.X.(*RowExpr); ok {
				// The parser rewrites a row's IS NULL: this row was nested.
				if err = e.bindRowItems(ctx, v.X); err == nil {
					err = errNestedRow()
				}
			}
		case *RowExpr:
			// Rows are rewritten or are a subquery's operand (which walkExpr
			// doesn't visit as a row): this one is out of place.
			if err = e.bindRowItems(ctx, v); err == nil {
				err = errRowValue()
			}
		case *Subquery:
			err = e.planSubquery(ctx, v)
		case *Func:
			// A window call's own *Func is not visited, so this is a call
			// without OVER.
			err = e.bindCall(ctx, v, nil)
			if err == nil && v.Name == "MERGE_ACTION" {
				err = e.checkMergeAction(v)
			}
		case *WindowFunc:
			err = e.bindCall(ctx, v.Func, v)
		}
	})
	return err
}

// bindCall resolves a call while it is bound (resolveCall in funcs.go); w is
// its OVER, if any. A call is visited before its arguments, so when it is
// wrong its arguments are bound first: as in Postgres, an unknown column or
// function in an argument is reported before the call's own error, and the
// argument types of an unknown function are known.
func (e *executor) bindCall(ctx context.Context, f *Func, w *WindowFunc) error {
	err := resolveCall(f, w != nil)
	if err == nil {
		return nil
	}
	for _, a := range callArgs(f, w) {
		if aerr := e.bind(ctx, a); aerr != nil {
			return aerr
		}
	}
	if err == errUnknownFunction {
		return noSuchFunction(f.Name, f.Star, inferArgTypes(f.Args, e.scopeTypes(), e.paramTypes))
	}
	return err
}

// scopeTypes returns the types of the columns of the innermost scope, by
// the names column references resolve to.
func (e *executor) scopeTypes() map[string]ColType {
	types := map[string]ColType{}
	if n := len(e.scopes); n > 0 {
		for _, rel := range e.scopes[n-1].rels {
			for _, c := range rel.meta.Columns {
				types[rel.prefix+c.Name] = c.Type
			}
		}
	}
	return types
}

// resolveColumn resolves a column reference as Postgres does. An unqualified
// column is one of the innermost scope that has it. A qualified one, t.col,
// is a column of the item named t (see names) in the innermost scope that
// has one; if that item has no such column, it is an error, even if an
// outer t has one. An item with an alias is visible only by its alias: in a
// subquery `SELECT … FROM t x`, t.col is an enclosing query's column (#102).
// s.t.col names only a table or view t of schema s without an alias (#111).
func (e *executor) resolveColumn(c *ColumnRef) error {
	if c.written == "" {
		c.written = c.Name
	}
	c.Name = c.written
	q := c.qual()
	if err := q.checkCatalog(c.written); err != nil {
		return err
	}
	top := len(e.scopes) - 1
	for depth := 0; depth <= top; depth++ {
		sc := e.scopes[top-depth]
		var match columnMeta
		var matchRel relation
		found, named := 0, 0
		for _, rel := range sc.rels {
			if c.Qualifier != "" {
				if !e.names(q, rel) {
					continue
				}
				if named++; named > 1 {
					// Two tables t of different schemas.
					return errorf(adbc.StatusInvalidArgument, "table reference %q is ambiguous", c.Qualifier)
				}
			}
			col, ok := rel.meta.column(c.Name)
			if !ok || (rel.prefix != "" && col.Name == rowIDField) || (c.Qualifier == "" && rel.hidden[col.Name]) {
				continue
			}
			found++
			if found == 1 {
				match, matchRel = col, rel
			}
		}
		if found > 1 {
			return errorf(adbc.StatusInvalidArgument, "column reference %q is ambiguous; qualify it with a table alias", c.Name)
		}
		if found == 1 {
			col := match
			col.Name = matchRel.prefix + col.Name
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
					sq.outerUses++
					ref := outerRef{name: col.Name, up: depth - k - 1}
					if !containsRef(sq.outerRefs, ref) {
						sq.outerRefs = append(sq.outerRefs, ref)
					}
				}
			}
			return nil
		}
		if named > 0 {
			// The item visible as the qualifier here has no such column.
			return noSuchColumn(c, sc)
		}
	}
	if c.Qualifier != "" {
		return e.missingFromEntry(q)
	}
	if top < 0 {
		return noSuchColumn(c, nil)
	}
	return noSuchColumn(c, e.scopes[top])
}

// qualName is the qualifier of a column reference or of rel.*: a FROM
// item's name, optionally with a schema and catalog. item is set for the
// planner's references (see ColumnRef.item).
type qualName struct{ catalog, schema, name, item string }

func (c *ColumnRef) qual() qualName {
	return qualName{catalog: c.Catalog, schema: c.Schema, name: c.Qualifier, item: c.item}
}

// String returns the qualifier as written.
func (q qualName) String() string {
	var parts []string
	for _, p := range []string{q.catalog, q.schema, q.name} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, ".")
}

// checkCatalog is Postgres's error for a qualifier naming another catalog;
// last is the rest of the reference (its column, or "*").
func (q qualName) checkCatalog(last string) error {
	if q.catalog == "" || strings.EqualFold(q.catalog, catalogName) {
		return nil
	}
	return errorf(adbc.StatusNotImplemented, "cross-database references are not implemented: %s.%s", q, last)
}

// names reports whether a qualifier names rel, as Postgres's
// refnameNamespaceItem matches it: t names the item visible as t (by its
// alias if it has one), and s.t only a table or view t of schema s that has
// no alias.
func (e *executor) names(q qualName, rel relation) bool {
	if q.item != "" {
		return rel.prefix == q.item
	}
	if !strings.EqualFold(rel.name, q.name) {
		return false
	}
	return q.schema == "" || (!rel.aliased && e.inSchema(rel, q.schema))
}

// inSchema reports whether rel is a table or view of schema, where pg_temp
// is the connection's temporary schema. Unlike resolveTable, it doesn't mark
// the statement as using a temporary object: only a FROM item does that.
func (e *executor) inSchema(rel relation, schema string) bool {
	if isTempAlias(schema) {
		schema = e.store.tempSchema()
	}
	return rel.schema != "" && strings.EqualFold(rel.schema, schema)
}

// readsTable reports whether rel is the table or view q names, under any
// alias. Without a schema, that is any table, view or CTE of that name
// (Postgres: the table the search path finds, or the CTE).
func (e *executor) readsTable(q qualName, rel relation) bool {
	if rel.meta == nil || !strings.EqualFold(rel.meta.Name, q.name) {
		return false
	}
	return q.schema == "" || e.inSchema(rel, q.schema)
}

// noSuchColumn is the error for a column that the scope sc (nil for none)
// doesn't have.
func noSuchColumn(c *ColumnRef, sc *scope) error {
	name := c.Name
	if c.Qualifier != "" {
		name = c.Qualifier + "." + c.Name
	}
	if sc != nil && len(sc.rels) == 1 {
		return errorf(adbc.StatusInvalidArgument, "column %q does not exist in table %q", name, sc.rels[0].meta.Name)
	}
	return errorf(adbc.StatusInvalidArgument, "column %q does not exist", name)
}

// missingFromEntry is Postgres's error (errorMissingRTE) for a qualifier
// that names no FROM item visible here. If an item of some level, innermost
// first, reads the table it names or has its name, the reference is
// invalid: Postgres's hint names the item's alias when it is the table
// under an alias that is visible here, and otherwise its detail says the
// item can't be referenced from here (as for s2.t where s1.t is read).
// Else the entry is missing.
func (e *executor) missingFromEntry(q qualName) error {
	for i := len(e.scopes) - 1; i >= 0; i-- {
		for j, rel := range e.scopes[i].rels {
			if rel.name == "" || !(strings.EqualFold(rel.name, q.name) || e.readsTable(q, rel)) {
				continue
			}
			if rel.aliased && !strings.EqualFold(rel.name, q.name) && e.visibleAt(i, j) {
				return errorf(adbc.StatusInvalidArgument,
					"invalid reference to FROM-clause entry for table %q; perhaps you meant to reference the table alias %q", q.name, rel.name)
			}
			return errorf(adbc.StatusInvalidArgument,
				"invalid reference to FROM-clause entry for table %q; there is an entry for table %q, but it cannot be referenced from this part of the query",
				q.name, rel.name)
		}
	}
	return errorf(adbc.StatusInvalidArgument, "missing FROM-clause entry for table %q", q.name)
}

// visibleAt reports whether item j of scope i is the one its name refers to
// from the innermost scope.
func (e *executor) visibleAt(i, j int) bool {
	name := e.scopes[i].rels[j].name
	for k := len(e.scopes) - 1; k >= 0; k-- {
		hit := -1
		for n, rel := range e.scopes[k].rels {
			if strings.EqualFold(rel.name, name) {
				if hit >= 0 {
					return false // ambiguous
				}
				hit = n
			}
		}
		if hit >= 0 {
			return k == i && hit == j
		}
	}
	return false
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
	width := 1 // of the operand
	if r, ok := sq.X.(*RowExpr); ok {
		if err := e.bindRowOperand(ctx, r); err != nil {
			return err
		}
		width = len(r.Items)
	} else if sq.X != nil {
		if err := e.bind(ctx, sq.X); err != nil {
			return err
		}
	}
	if sq.Kind == SubqueryExists && sq.Select.Limit == nil && len(sq.Select.GroupBy) == 0 && sq.Select.Having == nil {
		// Only existence matters.
		one := int64(1)
		sq.Select.Limit = &one
	}
	sq.outerUses, sq.semi = 0, nil
	e.pendingSq = sq
	plan, err := e.planSelect(ctx, sq.Select, e.paramTypes)
	e.pendingSq = nil
	if err != nil {
		return err
	}
	switch {
	case sq.rowCompare:
		// (a, b) = (SELECT x, y …)
		if err := subqueryWidthError(len(plan.items), sq.Width); err != nil {
			return err
		}
	case sq.Width > 0:
		if len(plan.items) != sq.Width {
			return errorf(adbc.StatusInvalidArgument, "number of columns does not match number of values")
		}
	case sq.Kind == SubqueryIn || sq.Kind == SubqueryAny || sq.Kind == SubqueryAll:
		if err := subqueryWidthError(len(plan.items), width); err != nil {
			return err
		}
	case sq.Kind != SubqueryExists && len(plan.items) != 1:
		return errorf(adbc.StatusInvalidArgument, "subquery must return exactly one column, got %d", len(plan.items))
	}
	sq.plan = plan
	e.planSemiJoin(ctx, sq)
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

// materialize runs a query that has no access to enclosing scopes. While
// only planning, it plans the query and returns an empty relation with its
// columns (see empty.go).
func (e *executor) materialize(ctx context.Context, key any, name string, sel *SelectStmt, rename []string) (*tableMeta, error) {
	if e.cache.materializing[key] {
		if def, ok := key.(*CTE); ok && def.Recursive {
			return nil, errorf(adbc.StatusNotImplemented, "mutual recursion between WITH items is not implemented")
		}
		return nil, errorf(adbc.StatusInvalidArgument, "%q refers to itself; recursive references need WITH RECURSIVE", name)
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
	if e.planOnly {
		return memTable(name, plan.columns(), nil, rename)
	}
	rows, err := e.runSelect(ctx, plan, e.params)
	if err != nil {
		return nil, err
	}
	return memTable(name, plan.columns(), rows, rename)
}

// pushCTEs makes a statement's WITH list visible while it is planned; the
// returned function removes it again.
func (e *executor) pushCTEs(with []CTE) (func(), error) {
	if len(with) == 0 {
		return func() {}, nil
	}
	defs := map[string]*CTE{}
	for i := range with {
		key := strings.ToLower(with[i].Name)
		if _, dup := defs[key]; dup {
			return nil, errorf(adbc.StatusInvalidArgument, "WITH query name %q specified more than once", with[i].Name)
		}
		defs[key] = &with[i]
	}
	e.ctes = append(e.ctes, defs)
	return func() { e.ctes = e.ctes[:len(e.ctes)-1] }, nil
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

// fromRelation resolves the FROM clause of a SELECT (without joins) to a
// relation.
func (e *executor) fromRelation(ctx context.Context, sel *SelectStmt) (relation, error) {
	return e.resolveFromItem(ctx, sel.From, sel.FromSelect, sel.FromAlias)
}

// resolveFromItem resolves one FROM item: a table, a view, a CTE, or a
// derived table. The relation is named by the alias, or else by the item's
// name; meta is nil if there is no item.
func (e *executor) resolveFromItem(ctx context.Context, table *TableName, sub *SelectStmt, alias string) (relation, error) {
	rel := relation{name: alias, aliased: alias != ""}
	switch {
	case sub != nil:
		if m, ok := e.cache.derived[sub]; ok {
			rel.meta = m
			return rel, nil
		}
		m, err := e.materialize(ctx, sub, alias, sub, nil)
		if err != nil {
			return relation{}, err
		}
		if !e.planOnly {
			// An empty stand-in isn't kept: another reference may read it.
			e.cache.derived[sub] = m
		}
		rel.meta = m
		return rel, nil
	case table != nil:
		if alias == "" {
			rel.name = table.Name
		}
		if isInfoSchema(table.Schema) && (table.Catalog == "" || table.Catalog == catalogName) {
			meta, err := e.infoSchemaTable(ctx, table.Name)
			rel.meta, rel.schema = meta, infoSchema
			return rel, err
		}
		if table.Schema == "" && table.Catalog == "" {
			if def := e.lookupCTE(table.Name); def != nil {
				if m, ok := e.cache.working[def]; ok {
					// The recursive reference of a recursive CTE.
					rel.meta = m
					return rel, nil
				}
				if m, ok := e.cache.ctes[def]; ok {
					rel.meta = m
					return rel, nil
				}
				m, err := e.materializeCTE(ctx, def)
				if err != nil {
					return relation{}, err
				}
				if !e.planOnly {
					e.cache.ctes[def] = m
				}
				rel.meta = m
				return rel, nil
			}
		}
		meta, err := e.loadTable(ctx, *table)
		var ae adbc.Error
		if err != nil && asAdbc(err, &ae) && ae.Code == adbc.StatusNotFound {
			// Not a table: maybe a view.
			schema, name, rerr := e.resolveTable(*table)
			if rerr != nil {
				return relation{}, err
			}
			v, verr := e.store.getView(ctx, schema, name)
			if verr != nil {
				return relation{}, err
			}
			meta, err = e.viewRelation(ctx, v, rel.name)
		}
		if err != nil {
			return relation{}, err
		}
		rel.meta, rel.schema = meta, meta.Schema
		return rel, nil
	}
	return relation{}, nil
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
	subqueryStats.runs.Add(1)
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
	if sq.semi != nil {
		if v, ok, err := env.exec.evalSemiJoin(env, sq); ok || err != nil {
			return v, err
		}
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
	case SubqueryAny, SubqueryAll:
		if r, ok := sq.X.(*RowExpr); ok {
			return env.evalRowQuantified(sq, r, rows)
		}
		return env.evalQuantified(sq, rows)
	}
	// IN
	if r, ok := sq.X.(*RowExpr); ok {
		return env.evalRowIn(sq, r, rows)
	}
	x, err := env.eval(sq.X)
	if err != nil {
		return Value{}, err
	}
	set := &inSet{rows: rows, noIndex: true, zone: env.zone()}
	if !sq.correlated {
		// The same rows for every outer row: probe a hash set.
		set = env.exec.cachedInSet(sq, rows)
	}
	return set.eval(x, sq.Not), nil
}

// evalRowColumn evaluates one column of a subquery assigned to a column
// list. Its rows are cached (or memoised per outer row), so the subquery
// runs once for all the columns. No row sets the columns to NULL; more than
// one is an error, as in Postgres.
func (env *evalEnv) evalRowColumn(rc *RowColumn) (Value, error) {
	if env.exec == nil {
		return Value{}, fmt.Errorf("subqueries are not supported here")
	}
	rows, err := env.exec.subqueryRows(env.ctx, rc.Sub, env)
	if err != nil {
		return Value{}, err
	}
	switch len(rows) {
	case 0:
		return nullValue(rc.Sub.plan.items[rc.Index].typ), nil
	case 1:
		return rows[0][rc.Index], nil
	}
	return Value{}, fmt.Errorf("more than one row returned by a subquery used as an expression")
}
