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
//	tables   (table_catalog, table_schema, table_name, table_type)
//	columns  (table_catalog, table_schema, table_name, column_name,
//	          ordinal_position, data_type, is_nullable, numeric_precision,
//	          numeric_scale, datetime_precision, is_indexed)
//	views    (table_catalog, table_schema, table_name, view_definition)

import (
	"context"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
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
	},
	"columns": {
		{Name: "table_catalog", Type: typeString},
		{Name: "table_schema", Type: typeString},
		{Name: "table_name", Type: typeString},
		{Name: "column_name", Type: typeString},
		{Name: "ordinal_position", Type: typeInt32},
		{Name: "data_type", Type: typeString},
		{Name: "is_nullable", Type: typeString},
		{Name: "numeric_precision", Type: typeInt32},
		{Name: "numeric_scale", Type: typeInt32},
		{Name: "datetime_precision", Type: typeInt32},
		{Name: "is_indexed", Type: typeString},
	},
	"views": {
		{Name: "table_catalog", Type: typeString},
		{Name: "table_schema", Type: typeString},
		{Name: "table_name", Type: typeString},
		{Name: "view_definition", Type: typeString},
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

// columnRow renders one information_schema.columns row.
func columnRow(schema, table string, pos int, c columnMeta) []Value {
	t := c.Type
	null := nullValue(typeInt32)
	prec, scale, dtPrec := null, null, null
	switch t.Kind {
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
		stringValue(c.Name), intValue(typeInt32, int64(pos)), stringValue(t.SQLName()),
		yesNo(c.Nullable), prec, scale, dtPrec, yesNo(c.Indexed),
	}
}

// infoSchemaTable builds one information_schema table.
func (e *executor) infoSchemaTable(ctx context.Context, name string) (*tableMeta, error) {
	key := strings.ToLower(name)
	cols, ok := infoSchemaColumns[key]
	if !ok {
		return nil, tableNotFound(infoSchema, name)
	}
	schemas, err := e.store.listSchemas(ctx)
	if err != nil {
		return nil, err
	}
	var rows [][]Value
	if key == "schemata" {
		for _, s := range append(schemas, infoSchema) {
			rows = append(rows, []Value{stringValue(catalogName), stringValue(s)})
		}
		return memTable(key, cols, rows, nil)
	}
	for _, schema := range schemas {
		tables, err := e.store.listTables(ctx, schema)
		if err != nil {
			return nil, err
		}
		views, err := e.store.listViews(ctx, schema)
		if err != nil {
			return nil, err
		}
		switch key {
		case "tables":
			for _, t := range tables {
				rows = append(rows, []Value{stringValue(catalogName), stringValue(schema), stringValue(t), stringValue("BASE TABLE")})
			}
			for _, v := range views {
				rows = append(rows, []Value{stringValue(catalogName), stringValue(schema), stringValue(v), stringValue("VIEW")})
			}
		case "columns":
			for _, t := range tables {
				meta, err := e.store.getTable(ctx, schema, t)
				if err != nil {
					continue // dropped meanwhile
				}
				for i, c := range meta.Columns {
					rows = append(rows, columnRow(schema, t, i+1, c))
				}
			}
			for _, v := range views {
				vm, err := e.store.getView(ctx, schema, v)
				if err != nil {
					continue
				}
				for i, c := range vm.Columns {
					rows = append(rows, columnRow(schema, v, i+1, c))
				}
			}
		case "views":
			for _, v := range views {
				vm, err := e.store.getView(ctx, schema, v)
				if err != nil {
					continue
				}
				rows = append(rows, []Value{stringValue(catalogName), stringValue(schema), stringValue(v), stringValue(vm.SQL)})
			}
		}
	}
	return memTable(key, cols, rows, nil)
}

func infoSchemaReadOnly() error {
	return errorf(adbc.StatusInvalidArgument, "information_schema is read-only")
}
