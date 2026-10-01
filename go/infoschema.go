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

// information_schema: a read-only virtual schema built from the driver's
// metadata when queried. Its tables are in-memory relations, so any SQL
// works against them (filters, joins, grouping).
//
//	schemata (catalog_name, schema_name)
//	tables   (table_catalog, table_schema, table_name, table_type,
//	          key_prefix, index_name, comment)
//	columns  (table_catalog, table_schema, table_name, column_name,
//	          ordinal_position, column_default, data_type, is_nullable,
//	          character_maximum_length, character_octet_length,
//	          numeric_precision, numeric_scale, datetime_precision,
//	          is_indexed, comment)
//	views    (table_catalog, table_schema, table_name, view_definition)
//	routines (routine_catalog, routine_schema, routine_name, routine_type,
//	          function_kind, min_arguments, max_arguments, alias_of)
//
// The connection's own temporary tables and views are included under schema
// pg_temp (temporary tables with table_type LOCAL TEMPORARY, as in
// Postgres); other connections' temporary objects are not. key_prefix and
// index_name are a table's row key prefix and RediSearch index (NULL for
// views). comment is the COMMENT ON text of a table, view or column (NULL
// without one). character_maximum_length is the n of VARCHAR(n) / CHAR(n)
// (NULL without one) and character_octet_length its bytes at up to 4 per
// character (1073741824 without one), as in Postgres. routines lists the
// driver's functions (see routineRows).

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

const infoSchema = "information_schema"

func isInfoSchema(schema string) bool { return strings.EqualFold(schema, infoSchema) }

var infoSchemaColumns = map[string][]resultColumn{
	"schemata": {
		{Name: "catalog_name", Type: typeString},
		{Name: "schema_name", Type: typeString},
	},
	"tables": {
		{Name: "table_catalog", Type: typeString},
		{Name: "table_schema", Type: typeString},
		{Name: "table_name", Type: typeString},
		{Name: "table_type", Type: typeString},
		{Name: "key_prefix", Type: typeString},
		{Name: "index_name", Type: typeString},
		{Name: "comment", Type: typeString},
	},
	"columns": {
		{Name: "table_catalog", Type: typeString},
		{Name: "table_schema", Type: typeString},
		{Name: "table_name", Type: typeString},
		{Name: "column_name", Type: typeString},
		{Name: "ordinal_position", Type: typeInt32},
		{Name: "column_default", Type: typeString},
		{Name: "data_type", Type: typeString},
		{Name: "is_nullable", Type: typeString},
		{Name: "character_maximum_length", Type: typeInt32},
		{Name: "character_octet_length", Type: typeInt32},
		{Name: "numeric_precision", Type: typeInt32},
		{Name: "numeric_scale", Type: typeInt32},
		{Name: "datetime_precision", Type: typeInt32},
		{Name: "is_indexed", Type: typeString},
		{Name: "comment", Type: typeString},
	},
	"views": {
		{Name: "table_catalog", Type: typeString},
		{Name: "table_schema", Type: typeString},
		{Name: "table_name", Type: typeString},
		{Name: "view_definition", Type: typeString},
	},
	"routines": {
		{Name: "routine_catalog", Type: typeString},
		{Name: "routine_schema", Type: typeString},
		{Name: "routine_name", Type: typeString},
		{Name: "routine_type", Type: typeString},
		{Name: "function_kind", Type: typeString},
		{Name: "min_arguments", Type: typeInt32},
		{Name: "max_arguments", Type: typeInt32},
		{Name: "alias_of", Type: typeString},
	},
}

func yesNo(b bool) Value {
	if b {
		return stringValue("YES")
	}
	return stringValue("NO")
}

func optInt(n int32, ok bool) Value {
	if !ok {
		return nullValue(typeInt32)
	}
	return intValue(typeInt32, int64(n))
}

// optString is s, or NULL if it is empty.
func optString(s string) Value {
	if s == "" {
		return nullValue(typeString)
	}
	return stringValue(s)
}

// columnRow renders one information_schema.columns row.
func columnRow(schema, table string, pos int, c columnMeta) []Value {
	t := c.Type
	null := nullValue(typeInt32)
	prec, scale, dtPrec, chars, octets := null, null, null, null, null
	switch t.Kind {
	case KindString:
		if t.Length > 0 {
			chars, octets = optInt(t.Length, true), optInt(4*t.Length, true)
		} else {
			octets = optInt(1073741824, true)
		}
	case KindDecimal:
		prec, scale = optInt(t.Precision, true), optInt(t.Scale, true)
	case KindInt16:
		prec, scale = optInt(16, true), optInt(0, true)
	case KindInt32:
		prec, scale = optInt(32, true), optInt(0, true)
	case KindInt64:
		prec, scale = optInt(64, true), optInt(0, true)
	case KindFloat32:
		prec = optInt(24, true)
	case KindFloat64:
		prec = optInt(53, true)
	case KindDate:
		dtPrec = optInt(0, true)
	case KindTime, KindTimestamp:
		dtPrec = optInt(int32(precisionForUnit(t.Unit)), true)
	}
	return []Value{
		stringValue(catalogName), stringValue(schema), stringValue(table),
		stringValue(c.Name), intValue(typeInt32, int64(pos)), optString(c.Default), stringValue(t.SQLName()),
		yesNo(c.Nullable), chars, octets, prec, scale, dtPrec, yesNo(c.Indexed), optString(c.Comment),
	}
}

// infoSchemaTable builds one information_schema table.
func (e *executor) infoSchemaTable(ctx context.Context, name string) (*tableMeta, error) {
	key := strings.ToLower(name)
	cols, ok := infoSchemaColumns[key]
	if !ok {
		return nil, tableNotFound(infoSchema, name)
	}
	if e.planOnly {
		return memTable(key, cols, nil, nil) // only its columns are wanted (see empty.go)
	}
	if key == "routines" {
		return memTable(key, cols, routineRows(), nil)
	}
	schemas, err := e.store.listSchemas(ctx)
	if err != nil {
		return nil, err
	}
	// Each schema is listed under its own name, except the connection's
	// temporary schema (stored as pg_temp_<id>), which is listed as pg_temp.
	type space struct{ name, stored string }
	spaces := make([]space, 0, len(schemas)+1)
	for _, s := range schemas {
		spaces = append(spaces, space{s, s})
	}
	if e.store.hasTempObjects() {
		spaces = append(spaces, space{tempAlias, e.store.tempSchema()})
	}
	var rows [][]Value
	if key == "schemata" {
		for _, s := range append(spaces, space{name: infoSchema}) {
			rows = append(rows, []Value{stringValue(catalogName), stringValue(s.name)})
		}
		return memTable(key, cols, rows, nil)
	}
	for _, sp := range spaces {
		schema, stored := sp.name, sp.stored
		tableType := "BASE TABLE"
		if isTempSchema(stored) {
			tableType = "LOCAL TEMPORARY"
		}
		tables, err := e.store.listTables(ctx, stored)
		if err != nil {
			return nil, err
		}
		views, err := e.store.listViews(ctx, stored)
		if err != nil {
			return nil, err
		}
		switch key {
		case "tables":
			metas, err := e.store.getTables(ctx, stored, tables)
			if err != nil {
				return nil, err
			}
			for i, t := range tables {
				if metas[i] == nil {
					continue // dropped meanwhile
				}
				rows = append(rows, []Value{stringValue(catalogName), stringValue(schema), stringValue(t), stringValue(tableType),
					stringValue(metas[i].prefix()), stringValue(metas[i].index()), optString(metas[i].Comment)})
			}
			vms, err := e.store.getViews(ctx, stored, views)
			if err != nil {
				return nil, err
			}
			for i, v := range views {
				if vms[i] == nil {
					continue // dropped meanwhile
				}
				rows = append(rows, []Value{stringValue(catalogName), stringValue(schema), stringValue(v), stringValue("VIEW"),
					nullValue(typeString), nullValue(typeString), optString(vms[i].Comment)})
			}
		case "columns":
			for _, t := range tables {
				meta, err := e.store.getTable(ctx, stored, t)
				if err != nil {
					continue // dropped meanwhile
				}
				for i, c := range meta.Columns {
					rows = append(rows, columnRow(schema, t, i+1, c))
				}
			}
			for _, v := range views {
				vm, err := e.store.getView(ctx, stored, v)
				if err != nil {
					continue
				}
				for i, c := range vm.Columns {
					rows = append(rows, columnRow(schema, v, i+1, c))
				}
			}
		case "views":
			for _, v := range views {
				vm, err := e.store.getView(ctx, stored, v)
				if err != nil {
					continue
				}
				rows = append(rows, []Value{stringValue(catalogName), stringValue(schema), stringValue(v), stringValue(vm.SQL)})
			}
		}
	}
	return memTable(key, cols, rows, nil)
}

// getTables reads the metadata of several tables in one round trip. As in
// information_schema.columns, a table whose metadata can't be read (it was
// dropped meanwhile) is nil.
func (s *store) getTables(ctx context.Context, schema string, tables []string) ([]*tableMeta, error) {
	return getMetas[tableMeta](ctx, s, tables, func(t string) string { return metaKey(schema, t) })
}

// getViews is getTables for views.
func (s *store) getViews(ctx context.Context, schema string, views []string) ([]*viewMeta, error) {
	return getMetas[viewMeta](ctx, s, views, func(v string) string { return viewKey(schema, v) })
}

// getMetas reads the JSON metadata of several objects (at key(name)) in one
// round trip; an object whose metadata can't be read is nil.
func getMetas[T any](ctx context.Context, s *store, names []string, key func(string) string) ([]*T, error) {
	pipe := s.client.Pipeline()
	cmds := make([]*goredis.StringCmd, len(names))
	for i, n := range names {
		cmds[i] = pipe.Get(ctx, key(n))
	}
	if len(names) > 0 {
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
			return nil, wrapRedis(err, "failed to read metadata")
		}
	}
	metas := make([]*T, len(names))
	for i, cmd := range cmds {
		var meta T
		if raw, err := cmd.Result(); err == nil && json.Unmarshal([]byte(raw), &meta) == nil {
			metas[i] = &meta
		}
	}
	return metas, nil
}

func infoSchemaReadOnly() error {
	return errorf(adbc.StatusInvalidArgument, "information_schema is read-only")
}
