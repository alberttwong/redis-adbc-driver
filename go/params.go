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

import "context"

// Parameter types (GetParameterSchema).
//
// Nothing in the planner gives a parameter a type: inferType types an
// unbound one as NULL. To describe a statement's parameters, it is planned
// without running (planningOnly) with the cache's paramHints set, and the
// type each parameter is used as is recorded in two places:
//   - noteParams, at the end of bind, for every expression the planner
//     binds (select lists, WHERE, ON, HAVING, SET values, VALUES,
//     RETURNING, …): a parameter compared with a column takes the column's
//     type, a LIKE pattern is a string, CAST($1 AS T) is a T, and so on;
//   - describeParams, for what binding can't see: an INSERT value or an
//     UPDATE SET value that is just a parameter takes its target column's
//     type.
//
// The first type recorded for a parameter wins. A parameter used nowhere a
// type can be taken from is NULL, as the ADBC spec says.

// noteParam records t as parameter i's type, unless it has one already or
// t is NULL. It does nothing unless types are being described.
func (c *execCache) noteParam(i int, t ColType) {
	if c == nil || c.paramHints == nil || t.Kind == KindNull {
		return
	}
	if _, ok := c.paramHints[i]; !ok {
		c.paramHints[i] = t
	}
}

// noteParamOf records t as the type of x if x is a parameter, or minus one.
func (c *execCache) noteParamOf(x Expr, t ColType) {
	if u, ok := x.(*Unary); ok && u.Op == "-" {
		x = u.X
	}
	if p, ok := x.(*Param); ok {
		c.noteParam(p.Index, t)
	}
}

var (
	arithmeticOps = map[string]bool{"+": true, "-": true, "*": true, "/": true, "%": true}
	regexOps      = map[string]bool{"~": true, "~*": true, "!~": true, "!~*": true}
	// sameTypeFuncs return one of their arguments, so each takes the type of
	// the others.
	sameTypeFuncs = map[string]bool{"COALESCE": true, "NULLIF": true, "GREATEST": true, "LEAST": true}
	stringFuncs   = map[string]bool{"LIKE": true, "ILIKE": true, "SIMILAR TO": true}
)

// noteParams records the types of the parameters in expr, which has just
// been bound in the current scope, from how they are used.
func (e *executor) noteParams(expr Expr) {
	if e.cache == nil || e.cache.paramHints == nil {
		return
	}
	c := e.cache
	types := e.scopeTypes()
	typeOf := func(x Expr) ColType {
		t, err := inferType(x, types, nil)
		if err != nil {
			return typeNull
		}
		return t
	}
	// commonOf is the common type of xs other than xs[skip].
	commonOf := func(xs []Expr, skip int) ColType {
		out := typeNull
		for i, x := range xs {
			if i != skip {
				out = commonType(out, typeOf(x))
			}
		}
		return out
	}
	walkExprPruned(expr, func(x Expr) bool {
		switch v := x.(type) {
		case *Binary:
			switch {
			case isComparison(v.Op) || isDistinctOp(v.Op):
				c.noteParamOf(v.L, typeOf(v.R))
				c.noteParamOf(v.R, typeOf(v.L))
			case v.Op == "AND" || v.Op == "OR":
				c.noteParamOf(v.L, typeBool)
				c.noteParamOf(v.R, typeBool)
			case v.Op == "||" || regexOps[v.Op]:
				c.noteParamOf(v.L, typeString)
				c.noteParamOf(v.R, typeString)
			case arithmeticOps[v.Op]:
				if t := typeOf(v.R); t.Kind.isNumeric() {
					c.noteParamOf(v.L, t)
				}
				if t := typeOf(v.L); t.Kind.isNumeric() {
					c.noteParamOf(v.R, t)
				}
			}
		case *Unary:
			if v.Op == "NOT" {
				c.noteParamOf(v.X, typeBool)
			}
		case *Cast:
			c.noteParamOf(v.X, v.T)
		case *Func:
			if aggregateFuncs[v.Name] {
				return false // binds its own arguments
			}
			switch {
			case stringFuncs[v.Name]:
				for _, a := range v.Args {
					c.noteParamOf(a, typeString)
				}
			case sameTypeFuncs[v.Name]:
				for i, a := range v.Args {
					c.noteParamOf(a, commonOf(v.Args, i))
				}
			}
		case *Case:
			results := make([]Expr, 0, len(v.Whens)+1)
			for _, w := range v.Whens {
				results = append(results, w.Then)
			}
			if v.Else != nil {
				results = append(results, v.Else)
			}
			for i, r := range results {
				c.noteParamOf(r, commonOf(results, i))
			}
			for _, w := range v.Whens {
				if v.Operand != nil {
					c.noteParamOf(w.When, typeOf(v.Operand))
					c.noteParamOf(v.Operand, typeOf(w.When))
				} else {
					c.noteParamOf(w.When, typeBool)
				}
			}
		case *Subquery:
			// $1 IN (SELECT …), $1 = ANY (SELECT …)
			if v.X != nil && v.plan != nil && len(v.plan.items) > 0 {
				c.noteParamOf(v.X, v.plan.items[0].typ)
			}
		}
		return true
	})
}

// parameterTypes returns the types of the parameters of a script, NULL for
// those whose type can't be told. A statement that fails to plan is an
// error when it is the script's only one; in a longer script it may need
// what the statements before it make, so its parameters are left NULL.
func (e *executor) parameterTypes(ctx context.Context, parsed []ParsedStmt) ([]ColType, error) {
	n := 0
	for _, ps := range parsed {
		n = max(n, ps.NumParams)
	}
	e.cache = newExecCache()
	e.cache.paramHints = map[int]ColType{}
	e.paramTypes = nil
	for _, ps := range parsed {
		if ps.NumParams == 0 {
			continue
		}
		if err := e.describeParams(ctx, ps.Stmt); err != nil && len(parsed) == 1 {
			return nil, err
		}
	}
	types := make([]ColType, n)
	for i := range types {
		types[i] = typeNull
		if t, ok := e.cache.paramHints[i]; ok {
			types[i] = t
		}
	}
	return types, nil
}

// describeParams plans st without running it, recording the types of its
// parameters (see parameterTypes).
func (e *executor) describeParams(ctx context.Context, st Stmt) error {
	defer e.planningOnly(true)()
	c := e.cache
	switch st := st.(type) {
	case *SelectStmt:
		_, err := e.planSelect(ctx, st, nil)
		return err
	case *CreateTableStmt:
		if st.AsSelect == nil {
			return nil
		}
		_, err := e.planSelect(ctx, st.AsSelect, nil)
		return err
	case *InsertStmt:
		meta, err := e.loadTable(ctx, st.Table)
		if err != nil {
			return err
		}
		targets, err := insertTargets(meta, st.Columns)
		if err != nil {
			return err
		}
		target := func(j int) ColType {
			if j < len(targets) {
				return meta.Columns[targets[j]].Type
			}
			return typeNull
		}
		if st.Select != nil {
			if st.Select.SetOp == nil {
				for j, item := range st.Select.Items {
					if !item.Star {
						c.noteParamOf(item.Expr, target(j))
					}
				}
			}
			if _, err := e.planSelect(ctx, st.Select, nil); err != nil {
				return err
			}
		}
		for _, row := range st.Rows {
			for j, x := range row {
				c.noteParamOf(x, target(j))
				if _, err := e.bindIn(ctx, x, nil, "", "VALUES"); err != nil {
					return err
				}
			}
		}
		_, err = e.planReturning(ctx, st.Returning, []relation{tableRel(meta, "")}, meta, false, nil)
		return err
	case *UpdateStmt:
		pop, err := e.pushCTEs(st.With)
		if err != nil {
			return err
		}
		defer pop()
		if len(st.From) > 0 {
			return e.describeJoinedDML(ctx, st.Table, st.Alias, st.From, st.Sets, st.Where, st.Returning, "UPDATE")
		}
		meta, err := e.loadTable(ctx, st.Table)
		if err != nil {
			return err
		}
		name := st.Alias
		if name == "" {
			name = meta.Name
		}
		cols, err := setTargets(meta, name, st.Sets)
		if err != nil {
			return err
		}
		for i, s := range st.Sets {
			c.noteParamOf(s.Expr, meta.Columns[cols[i]].Type)
			if _, err := e.bindIn(ctx, s.Expr, meta, st.Alias, "UPDATE"); err != nil {
				return err
			}
		}
		if st.Where != nil {
			if _, err := e.bindIn(ctx, st.Where, meta, st.Alias, "WHERE"); err != nil {
				return err
			}
		}
		_, err = e.planReturning(ctx, st.Returning, []relation{tableRel(meta, st.Alias)}, meta, false, map[string]bool{})
		return err
	case *DeleteStmt:
		pop, err := e.pushCTEs(st.With)
		if err != nil {
			return err
		}
		defer pop()
		if len(st.Using) > 0 {
			return e.describeJoinedDML(ctx, st.Table, st.Alias, st.Using, nil, st.Where, st.Returning, "WHERE")
		}
		meta, err := e.loadTable(ctx, st.Table)
		if err != nil {
			return err
		}
		if st.Where != nil {
			if _, err := e.bindIn(ctx, st.Where, meta, st.Alias, "WHERE"); err != nil {
				return err
			}
		}
		_, err = e.planReturning(ctx, st.Returning, []relation{tableRel(meta, st.Alias)}, meta, false, map[string]bool{})
		return err
	}
	// Other statements (MERGE, DDL, SET, …): their parameters stay NULL.
	return nil
}

// describeJoinedDML describes UPDATE … FROM and DELETE … USING, binding as
// runUpdateFrom and runDeleteUsing do.
func (e *executor) describeJoinedDML(ctx context.Context, table TableName, alias string, from []JoinClause,
	sets []SetClause, where Expr, returning []SelectItem, clause string) error {
	dj, err := e.planDMLJoin(ctx, table, alias, from, "CROSS", nil)
	if err != nil {
		return err
	}
	cols, err := setTargets(dj.meta, dj.target.alias, sets)
	if err != nil {
		return err
	}
	e.pushScope(dj.rels, clause)
	defer e.popScope()
	if err := e.bindJoin(ctx, dj.sel, dj.jp); err != nil {
		return err
	}
	for i, s := range sets {
		e.cache.noteParamOf(s.Expr, dj.meta.Columns[cols[i]].Type)
		if err := e.bind(ctx, s.Expr); err != nil {
			return err
		}
	}
	if err := e.bindWhere(ctx, where); err != nil {
		return err
	}
	_, err = e.planReturning(ctx, returning, dj.rels, dj.targetFirst(), false, map[string]bool{})
	return err
}
