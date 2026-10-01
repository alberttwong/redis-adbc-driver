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

// Views.
//
// A view is stored as its SELECT text plus the column names and types it
// produced when it was created:
//
//	adbc:{meta}:view:<schema>:<view>   STRING JSON {schema, name, sql, columns}
//	adbc:{meta}:views:<schema>         SET    view names
//
// Tables and views share one namespace per schema. A query that reads a view
// plans the view's SELECT (in the view's schema, without the caller's CTEs or
// scopes):
//
//   - A simple view (one table, no GROUP BY / aggregates / LIMIT / OFFSET) is
//     lazy: predicates the outer query applies to the view are rewritten in
//     terms of the base table and pushed into the base table's index scan
//     together with the view's own WHERE.
//   - Any other view is computed once per statement, like a derived table.
//
// A temporary view (CREATE TEMP VIEW) lives in the connection's temporary
// schema (see temp.go). Unqualified names in its query resolve like they do
// in the connection's own statements: temporary objects first, then the
// schema that was current when the view was created. A permanent view can't
// refer to temporary objects, and temporary objects never shadow the names
// used inside a permanent view.

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

type viewMeta struct {
	Schema  string       `json:"schema"`
	Name    string       `json:"name"`
	SQL     string       `json:"sql"`
	Columns []columnMeta `json:"columns"`
	// DefaultSchema is where a temporary view's unqualified names resolve
	// (after temporary objects). Permanent views use their own schema.
	DefaultSchema string `json:"default_schema,omitempty"`
}

func viewKey(schema, name string) string { return metaPrefix + "view:" + tableKeySuffix(schema, name) }
func viewsKey(schema string) string      { return metaPrefix + "views:" + escapeKeyPart(schema) }

func viewNotFound(schema, name string) error {
	return errorf(adbc.StatusNotFound, "view %q.%q does not exist", displaySchema(schema), name)
}

func (s *store) getView(ctx context.Context, schema, name string) (*viewMeta, error) {
	raw, err := s.client.Get(ctx, viewKey(schema, name)).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, viewNotFound(schema, name)
	}
	if err != nil {
		return nil, wrapRedis(err, "failed to read view metadata")
	}
	var v viewMeta
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return nil, errorf(adbc.StatusInternal, "corrupt metadata for view %q.%q: %v", schema, name, err)
	}
	return &v, nil
}

func (s *store) viewExists(ctx context.Context, schema, name string) (bool, error) {
	n, err := s.client.Exists(ctx, viewKey(schema, name)).Result()
	if err != nil {
		return false, wrapRedis(err, "failed to check view")
	}
	return n > 0, nil
}

func (s *store) putView(ctx context.Context, v *viewMeta) error {
	if err := s.checkWritableSchema(v.Schema); err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return errorf(adbc.StatusInternal, "failed to encode view: %v", err)
	}
	pipe := s.client.TxPipeline()
	pipe.Set(ctx, viewKey(v.Schema, v.Name), raw, 0)
	pipe.SAdd(ctx, viewsKey(v.Schema), v.Name)
	if !isTempSchema(v.Schema) {
		pipe.SAdd(ctx, schemasKey, v.Schema)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return wrapRedis(err, "failed to create view")
	}
	s.trackTemp(v.Schema, v.Name, true)
	return nil
}

func (s *store) dropView(ctx context.Context, schema, name string, ifExists bool) error {
	exists, err := s.viewExists(ctx, schema, name)
	if err != nil {
		return err
	}
	if !exists {
		if ifExists {
			return nil
		}
		return viewNotFound(schema, name)
	}
	pipe := s.client.TxPipeline()
	pipe.Del(ctx, viewKey(schema, name))
	pipe.SRem(ctx, viewsKey(schema), name)
	if _, err := pipe.Exec(ctx); err != nil {
		return wrapRedis(err, "failed to drop view")
	}
	s.trackTemp(schema, name, false)
	return nil
}

// renameView renames a view within its schema. Its stored SELECT is
// unchanged; other views that read it by the old name stop working, as they
// do when a table is renamed. All keys are in the {meta} hash slot, so the
// check and the rename are one transaction even on a cluster.
func (s *store) renameView(ctx context.Context, schema, name string, to TableName) error {
	if to.Catalog != "" && to.Catalog != catalogName {
		return errorf(adbc.StatusInvalidArgument, "catalog %q does not exist", to.Catalog)
	}
	if to.Schema != "" && to.Schema != schema && !(isTempAlias(to.Schema) && isTempSchema(schema)) {
		return errorf(adbc.StatusNotImplemented, "RENAME TO cannot move a view to another schema")
	}
	newName := to.Name
	if newName == name {
		return nil
	}
	oldKey, newKey, tableKey := viewKey(schema, name), viewKey(schema, newName), metaKey(schema, newName)
	for attempt := 0; attempt < 20; attempt++ {
		err := s.client.Watch(ctx, func(tx *goredis.Tx) error {
			raw, err := tx.Get(ctx, oldKey).Result()
			if errors.Is(err, goredis.Nil) {
				return viewNotFound(schema, name)
			}
			if err != nil {
				return err
			}
			var v viewMeta
			if err := json.Unmarshal([]byte(raw), &v); err != nil {
				return errorf(adbc.StatusInternal, "corrupt metadata for view %q.%q: %v", schema, name, err)
			}
			if n, err := tx.Exists(ctx, newKey).Result(); err != nil {
				return err
			} else if n > 0 {
				return errorf(adbc.StatusAlreadyExists, "view %q.%q already exists", displaySchema(schema), newName)
			}
			if n, err := tx.Exists(ctx, tableKey).Result(); err != nil {
				return err
			} else if n > 0 {
				return errorf(adbc.StatusAlreadyExists, "%q.%q already exists as a table", displaySchema(schema), newName)
			}
			v.Name = newName
			out, err := json.Marshal(&v)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				p.Set(ctx, newKey, out, 0)
				p.Del(ctx, oldKey)
				p.SRem(ctx, viewsKey(schema), name)
				p.SAdd(ctx, viewsKey(schema), newName)
				return nil
			})
			return err
		}, oldKey, newKey, tableKey)
		if errors.Is(err, goredis.TxFailedErr) {
			continue // concurrent change: retry
		}
		if err == nil {
			s.trackTemp(schema, name, false)
			s.trackTemp(schema, newName, true)
		}
		return wrapRedis(err, "failed to rename view")
	}
	return errorf(adbc.StatusIO, "view %q.%q is being changed concurrently; try again", displaySchema(schema), name)
}

func (s *store) listViews(ctx context.Context, schema string) ([]string, error) {
	members, err := s.client.SMembers(ctx, viewsKey(schema)).Result()
	if err != nil {
		return nil, wrapRedis(err, "failed to list views")
	}
	slices.Sort(members)
	return members, nil
}

// ---- executor ----

// isolated runs fn as if at the top level of a statement in schema: no
// enclosing scopes or CTEs are visible (used for view bodies). Unqualified
// names resolve to temporary objects only if temp is set.
func (e *executor) isolated(schema string, temp bool, fn func() error) error {
	saved := struct {
		scopes []*scope
		sq     *Subquery
		ctes   []map[string]*CTE
		schema string
		outer  *evalEnv
		noTemp bool
	}{e.scopes, e.pendingSq, e.ctes, e.schema, e.outer, e.noTemp}
	e.scopes, e.pendingSq, e.ctes, e.schema, e.outer, e.noTemp = nil, nil, nil, schema, nil, !temp
	defer func() {
		e.scopes, e.pendingSq, e.ctes, e.schema, e.outer, e.noTemp = saved.scopes, saved.sq, saved.ctes, saved.schema, saved.outer, saved.noTemp
	}()
	return fn()
}

func (e *executor) runCreateView(ctx context.Context, st *CreateViewStmt, numParams int) error {
	if numParams > 0 {
		return errorf(adbc.StatusInvalidArgument, "views cannot contain parameters")
	}
	schema, name, err := e.resolveCreate(ctx, st.Name, st.Temporary)
	if err != nil {
		return err
	}
	temp := isTempSchema(schema)
	if exists, err := e.store.tableExists(ctx, schema, name); err != nil {
		return err
	} else if exists {
		return errorf(adbc.StatusAlreadyExists, "%q.%q already exists as a table", displaySchema(schema), name)
	}
	if exists, err := e.store.viewExists(ctx, schema, name); err != nil {
		return err
	} else if exists && !st.OrReplace {
		if st.IfNotExists {
			return nil
		}
		return errorf(adbc.StatusAlreadyExists, "view %q.%q already exists", displaySchema(schema), name)
	}
	// Plan the body now, both to reject invalid views and to record the
	// view's column names and types. A temporary view's unqualified names
	// resolve in the current schema.
	home := schema
	if temp {
		home = e.schema
	}
	var cols []resultColumn
	key := "view:" + schema + "." + name
	e.cache.materializing[key] = true
	defer delete(e.cache.materializing, key)
	e.cache.usedTemp = false
	err = e.isolated(home, true, func() error {
		plan, err := e.planSelect(ctx, st.Select, nil)
		if err != nil {
			return err
		}
		cols = plan.columns()
		return nil
	})
	if err != nil {
		return err
	}
	if !temp && e.cache.usedTemp {
		return errorf(adbc.StatusInvalidArgument, "view %q refers to a temporary table or view; use CREATE TEMP VIEW", name)
	}
	if st.Columns != nil && len(st.Columns) != len(cols) {
		return errorf(adbc.StatusInvalidArgument, "view has %d columns but %d column names were given", len(cols), len(st.Columns))
	}
	v := &viewMeta{Schema: schema, Name: name, SQL: st.Text}
	if temp {
		v.DefaultSchema = home
	}
	seen := map[string]bool{}
	for i, c := range cols {
		n := c.Name
		if st.Columns != nil {
			n = st.Columns[i]
		}
		if seen[strings.ToLower(n)] {
			return errorf(adbc.StatusInvalidArgument, "column name %q appears more than once in view %q; add aliases", n, name)
		}
		seen[strings.ToLower(n)] = true
		t := c.Type
		if t.Kind == KindNull {
			t = typeString
		}
		v.Columns = append(v.Columns, columnMeta{Name: n, Type: t, Nullable: true})
	}
	return e.store.putView(ctx, v)
}

// lazyView is a simple view evaluated at scan time, with outer predicates
// pushed into its base table.
type lazyView struct {
	plan  *selectPlan // the view body, bound to its base table
	names []string    // view column names, one per plan item
}

// viewRelation plans a view referenced in FROM.
func (e *executor) viewRelation(ctx context.Context, v *viewMeta, alias string) (*tableMeta, error) {
	key := "view:" + v.Schema + "." + v.Name
	if e.cache.materializing[key] {
		return nil, errorf(adbc.StatusNotImplemented, "view %q refers to itself", v.Name)
	}
	e.cache.materializing[key] = true
	defer delete(e.cache.materializing, key)

	parsed, err := ParseScript(v.SQL)
	if err != nil {
		return nil, errorf(adbc.StatusInternal, "view %q: %v", v.Name, err)
	}
	sel, ok := parsed[0].Stmt.(*SelectStmt)
	if len(parsed) != 1 || !ok {
		return nil, errorf(adbc.StatusInternal, "view %q is not a single SELECT", v.Name)
	}
	names := make([]string, len(v.Columns))
	for i, c := range v.Columns {
		names[i] = c.Name
	}
	home, temp := v.Schema, isTempSchema(v.Schema)
	if temp && v.DefaultSchema != "" {
		home = v.DefaultSchema
	}
	var out *tableMeta
	err = e.isolated(home, temp, func() error {
		plan, err := e.planSelect(ctx, sel, nil)
		if err != nil {
			return errorf(adbc.StatusInvalidArgument, "view %q is no longer valid: %v", v.Name, err)
		}
		if len(plan.items) != len(names) {
			return errorf(adbc.StatusInvalidArgument, "view %q is no longer valid: its query now returns %d columns, not %d", v.Name, len(plan.items), len(names))
		}
		if isSimpleView(plan) {
			out = &tableMeta{Schema: v.Schema, Name: v.Name, isMem: true, view: &lazyView{plan: plan, names: names}}
			for i, it := range plan.items {
				out.Columns = append(out.Columns, columnMeta{Name: names[i], Type: it.typ, Nullable: true})
			}
			return nil
		}
		rows, err := e.runSelect(ctx, plan, e.params)
		if err != nil {
			return err
		}
		out, err = memTable(v.Name, plan.columns(), rows, names)
		return err
	})
	if err != nil {
		return nil, err
	}
	out.Schema = v.Schema
	return out, nil
}

func isSimpleView(plan *selectPlan) bool {
	sel := plan.sel
	return plan.meta != nil && !plan.meta.isMem && !plan.aggregate &&
		len(sel.Joins) == 0 && sel.Limit == nil && sel.Offset == nil
}

// substitute rewrites column references (of the current scope) using repl;
// ok is false if a column has no replacement, the replacement is volatile
// (RANDOM() would be computed again), or the expression contains a subquery
// (whose references cannot be rewritten).
func substitute(e Expr, repl map[string]Expr) (Expr, bool) {
	switch x := e.(type) {
	case *ColumnRef:
		if x.Outer > 0 {
			return x, true
		}
		r, ok := repl[x.Name]
		if ok && hasVolatile(r) {
			return nil, false
		}
		return r, ok
	case *Literal, *Param:
		return x, true
	case *Unary:
		v, ok := substitute(x.X, repl)
		return &Unary{Op: x.Op, X: v}, ok
	case *Binary:
		l, ok1 := substitute(x.L, repl)
		r, ok2 := substitute(x.R, repl)
		return &Binary{Op: x.Op, L: l, R: r}, ok1 && ok2
	case *IsNull:
		v, ok := substitute(x.X, repl)
		return &IsNull{X: v, Not: x.Not}, ok
	case *Cast:
		v, ok := substitute(x.X, repl)
		return &Cast{X: v, T: x.T}, ok
	case *Func:
		f := *x
		f.Args = make([]Expr, len(x.Args))
		for i, a := range x.Args {
			v, ok := substitute(a, repl)
			if !ok {
				return nil, false
			}
			f.Args[i] = v
		}
		return &f, true
	case *Case:
		c := &Case{}
		var ok bool
		if x.Operand != nil {
			if c.Operand, ok = substitute(x.Operand, repl); !ok {
				return nil, false
			}
		}
		for _, w := range x.Whens {
			wv, ok1 := substitute(w.When, repl)
			tv, ok2 := substitute(w.Then, repl)
			if !ok1 || !ok2 {
				return nil, false
			}
			c.Whens = append(c.Whens, WhenClause{When: wv, Then: tv})
		}
		if x.Else != nil {
			if c.Else, ok = substitute(x.Else, repl); !ok {
				return nil, false
			}
		}
		return c, true
	}
	return nil, false
}

// runView scans a lazy view: the view's WHERE plus every outer predicate
// that can be rewritten over the base table run in the base table's index.
func (e *executor) runView(ctx context.Context, lv *lazyView, outerWhere Expr, need map[string]bool, params []Value) ([]map[string]Value, error) {
	base := lv.plan.meta
	repl := make(map[string]Expr, len(lv.names))
	for i, it := range lv.plan.items {
		repl[lv.names[i]] = it.expr
	}
	where := lv.plan.sel.Where
	if outerWhere != nil {
		for _, c := range conjuncts(outerWhere, nil) {
			if pushed, ok := substitute(c, repl); ok {
				if where == nil {
					where = pushed
				} else {
					where = &Binary{Op: "AND", L: where, R: pushed}
				}
			}
		}
	}
	wp, err := e.planWhere(ctx, where, base, params)
	if err != nil {
		return nil, err
	}
	baseNeed := map[string]bool{}
	for k := range lv.plan.extraNeed {
		baseNeed[k] = true
	}
	for i, it := range lv.plan.items {
		if need == nil || need[lv.names[i]] {
			columnRefs(it.expr, baseNeed)
		}
	}
	_, rows, err := e.scan(ctx, scanRequest{meta: base, where: wp, need: baseNeed}, params)
	if err != nil {
		return nil, err
	}
	env := e.newEnv(ctx, base.types(), params)
	out := make([]map[string]Value, len(rows))
	for r, row := range rows {
		env.row = row
		m := make(map[string]Value, len(lv.names))
		for i, it := range lv.plan.items {
			if need != nil && !need[lv.names[i]] {
				continue
			}
			v, err := env.eval(it.expr)
			if err != nil {
				return nil, invalidArg(err)
			}
			m[lv.names[i]] = v
		}
		out[r] = m
	}
	return out, nil
}
