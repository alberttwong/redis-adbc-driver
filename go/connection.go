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
	"regexp"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	store   *store
	schema   string
	pushdown string
	version  string
}

func (c *connectionImpl) executor() *executor {
	return &executor{store: c.store, schema: c.schema, pushdown: c.pushdown}
}

func (c *connectionImpl) Close(ctx context.Context) error {
	if c.store != nil && c.store.client != nil {
		err := c.store.client.Close()
		c.store.client = nil
		return err
	}
	return nil
}

func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if c.version == "" {
		v, err := c.store.serverVersion(ctx)
		if err != nil {
			return err
		}
		c.version = v
	}
	if err := c.DriverInfo.RegisterInfoCode(adbc.InfoVendorName, "Redis"); err != nil {
		return err
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}

func (c *connectionImpl) Commit(ctx context.Context) error {
	return errorf(adbc.StatusInvalidState, "transactions are not supported (autocommit only)")
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	return errorf(adbc.StatusInvalidState, "transactions are not supported (autocommit only)")
}

func (c *connectionImpl) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	st := &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper),
		conn:              c,
		ingest:            driverbase.NewBulkIngestOptions(),
	}
	return driverbase.NewStatement(st), nil
}

func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, errorf(adbc.StatusNotImplemented, "ReadPartition is not supported")
}

// ---- CurrentNamespacer ----

func (c *connectionImpl) GetCurrentCatalog(ctx context.Context) (string, error) {
	return catalogName, nil
}

func (c *connectionImpl) GetCurrentDbSchema(ctx context.Context) (string, error) {
	return c.schema, nil
}

func (c *connectionImpl) SetCurrentCatalog(ctx context.Context, catalog string) error {
	if catalog != catalogName {
		return errorf(adbc.StatusNotFound, "catalog %q does not exist", catalog)
	}
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(ctx context.Context, schema string) error {
	ok, err := c.store.schemaExists(ctx, schema)
	if err != nil {
		return err
	}
	if !ok {
		return errorf(adbc.StatusNotFound, "schema %q does not exist", schema)
	}
	c.schema = schema
	return nil
}

// ---- metadata ----

func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE"}, nil
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	schema := c.schema
	if dbSchema != nil && *dbSchema != "" {
		schema = *dbSchema
	}
	if catalog != nil && *catalog != "" && *catalog != catalogName {
		return nil, tableNotFound(schema, tableName)
	}
	meta, err := c.store.getTable(ctx, schema, tableName)
	if err != nil {
		return nil, err
	}
	fields := make([]arrow.Field, len(meta.Columns))
	for i, col := range meta.Columns {
		fields[i] = arrow.Field{Name: col.Name, Type: col.Type.ArrowType(), Nullable: col.Nullable}
	}
	return arrow.NewSchema(fields, nil), nil
}

// matchPattern implements SQL LIKE matching with '%' and '_'.
func matchPattern(value string, pattern *string) bool {
	if pattern == nil {
		return true
	}
	var b strings.Builder
	b.WriteString("(?s)^")
	for _, r := range *pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(value)
}

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	if !matchPattern(catalogName, catalogFilter) {
		return nil, nil
	}
	return []string{catalogName}, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	if catalog != catalogName {
		return nil, nil
	}
	all, err := c.store.listSchemas(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, s := range all {
		if matchPattern(s, schemaFilter) {
			out = append(out, s)
		}
	}
	return out, nil
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	if catalog != catalogName {
		return nil, nil
	}
	names, err := c.store.listTables(ctx, schema)
	if err != nil {
		return nil, err
	}
	out := []driverbase.TableInfo{}
	for _, name := range names {
		if !matchPattern(name, tableFilter) {
			continue
		}
		info := driverbase.TableInfo{TableName: name, TableType: "TABLE"}
		if includeColumns {
			meta, err := c.store.getTable(ctx, schema, name)
			if err != nil {
				var ae adbc.Error
				if asAdbc(err, &ae) && ae.Code == adbc.StatusNotFound {
					continue
				}
				return nil, err
			}
			info.TableColumns = []driverbase.ColumnInfo{}
			for i, col := range meta.Columns {
				if !matchPattern(col.Name, columnFilter) {
					continue
				}
				pos := int32(i + 1)
				typeName := col.Type.SQLName()
				nullable := int16(0)
				isNullable := "NO"
				if col.Nullable {
					nullable = 1
					isNullable = "YES"
				}
				info.TableColumns = append(info.TableColumns, driverbase.ColumnInfo{
					ColumnName:      col.Name,
					OrdinalPosition: &pos,
					XdbcTypeName:    &typeName,
					XdbcNullable:    &nullable,
					XdbcIsNullable:  &isNullable,
				})
			}
			info.TableConstraints = []driverbase.ConstraintInfo{}
		}
		out = append(out, info)
	}
	return out, nil
}
