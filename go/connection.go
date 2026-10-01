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
	"slices"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	goredis "github.com/redis/go-redis/v9"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	store *store
	// sess holds the settings: the current schema, and what SET changes.
	sess     *session
	pushdown string
	version  string
	// rekey is adbc.redis.rename_rekey (from the database unless set on the
	// connection).
	rekey bool

	// clientOpts are the go-redis options the client was made from, and
	// timeouts its timeouts (see timeout.go). writeFollows is set while
	// the write timeout follows the read timeout.
	clientOpts   *goredis.Options
	timeouts     timeouts
	writeFollows bool
}

func (c *connectionImpl) executor() *executor {
	return &executor{store: c.store, sess: c.sess, schema: c.sess.currentSchema(), pushdown: c.pushdown, rekey: c.rekey}
}

func (c *connectionImpl) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionStringRenameRekey:
		return strconv.FormatBool(c.rekey), nil
	case OptionStringReadTimeout:
		return formatTimeout(c.timeouts.read), nil
	case OptionStringWriteTimeout:
		return formatTimeout(c.timeouts.write), nil
	}
	return c.ConnectionImplBase.GetOption(ctx, key)
}

func (c *connectionImpl) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case OptionStringRenameRekey:
		rekey, err := parseRenameRekey(value)
		if err != nil {
			return err
		}
		c.rekey = rekey
		return nil
	case OptionStringReadTimeout, OptionStringWriteTimeout:
		d, err := parseTimeout(key, value)
		if err != nil {
			return err
		}
		t, follows := c.timeouts, c.writeFollows
		if key == OptionStringReadTimeout {
			t.read = d
			if follows {
				t.write = d
			}
		} else {
			t.write, follows = d, false
		}
		if err := c.setTimeouts(ctx, t); err != nil {
			return err
		}
		c.writeFollows = follows
		return nil
	}
	return c.ConnectionImplBase.SetOption(ctx, key, value)
}

// setTimeouts gives the connection a client with other timeouts. go-redis
// fixes them when a client is made, so this makes a new one. The old one
// stays open until the connection closes, for background work that still
// uses it (cleanups, string column checks, the temporary schema's
// heartbeat).
func (c *connectionImpl) setTimeouts(ctx context.Context, t timeouts) error {
	if t == c.timeouts {
		return nil
	}
	if c.store == nil || c.store.client == nil {
		return errorf(adbc.StatusInvalidState, "the connection is closed")
	}
	_, cluster := c.store.client.(*goredis.ClusterClient)
	client := newClient(c.clientOpts, cluster, t)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return wrapRedis(err, "failed to connect to Redis")
	}
	c.store.mu.Lock()
	c.store.retired = append(c.store.retired, c.store.client)
	c.store.mu.Unlock()
	c.store.client = client
	c.timeouts = t
	return nil
}

func (c *connectionImpl) Close(ctx context.Context) error {
	if c.store != nil && c.store.client != nil {
		// Best effort: whatever can't be dropped now is swept by a later
		// connection (see temp.go).
		_ = c.store.dropTempSchema(ctx)
		err := c.store.client.Close()
		c.store.client = nil
		c.store.mu.Lock()
		retired := c.store.retired
		c.store.retired = nil
		c.store.mu.Unlock()
		for _, old := range retired {
			_ = old.Close()
		}
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
	return c.sess.currentSchema(), nil
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
	c.sess.setSchema(schema)
	return nil
}

// ---- metadata ----

func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE", "VIEW"}, nil
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	name := TableName{Name: tableName}
	if catalog != nil {
		name.Catalog = *catalog
	}
	if dbSchema != nil {
		name.Schema = *dbSchema
	}
	// Without a schema, a temporary table or view comes first.
	schema, _, err := c.executor().resolveTable(name)
	if err != nil {
		return nil, err
	}
	meta, err := c.store.getTable(ctx, schema, tableName)
	if err != nil {
		v, verr := c.store.getView(ctx, schema, tableName)
		if verr != nil {
			return nil, err
		}
		meta = &tableMeta{Schema: schema, Name: tableName, Columns: v.Columns}
	}
	fields := make([]arrow.Field, len(meta.Columns))
	for i, col := range meta.Columns {
		fields[i] = arrow.Field{Name: col.Name, Type: col.Type.ArrowType(), Nullable: col.Nullable}
		if col.Comment != "" {
			fields[i].Metadata = arrow.NewMetadata([]string{remarksKey}, []string{col.Comment})
		}
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
	// The connection's own temporary objects are listed under pg_temp.
	if c.store.hasTempObjects() {
		all = append(all, tempAlias)
		slices.Sort(all)
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
	if isTempAlias(schema) {
		schema = c.store.tempSchema()
	} else if isTempSchema(schema) {
		return []driverbase.TableInfo{}, nil
	}
	tables, err := c.store.listTables(ctx, schema)
	if err != nil {
		return nil, err
	}
	views, err := c.store.listViews(ctx, schema)
	if err != nil {
		return nil, err
	}
	type entry struct{ name, kind string }
	var entries []entry
	for _, n := range tables {
		entries = append(entries, entry{n, "TABLE"})
	}
	for _, n := range views {
		entries = append(entries, entry{n, "VIEW"})
	}
	slices.SortFunc(entries, func(a, b entry) int { return strings.Compare(a.name, b.name) })

	out := []driverbase.TableInfo{}
	for _, ent := range entries {
		if !matchPattern(ent.name, tableFilter) {
			continue
		}
		info := driverbase.TableInfo{TableName: ent.name, TableType: ent.kind}
		if includeColumns {
			var cols []columnMeta
			var meta *tableMeta
			var err error
			if ent.kind == "VIEW" {
				var v *viewMeta
				if v, err = c.store.getView(ctx, schema, ent.name); err == nil {
					cols = v.Columns
				}
			} else if meta, err = c.store.getTable(ctx, schema, ent.name); err == nil {
				cols = meta.Columns
			}
			if err != nil {
				var ae adbc.Error
				if asAdbc(err, &ae) && ae.Code == adbc.StatusNotFound {
					continue
				}
				return nil, err
			}
			info.TableColumns = []driverbase.ColumnInfo{}
			for i, col := range cols {
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
				ci := driverbase.ColumnInfo{
					ColumnName:      col.Name,
					OrdinalPosition: &pos,
					XdbcTypeName:    &typeName,
					XdbcNullable:    &nullable,
					XdbcIsNullable:  &isNullable,
				}
				if n := col.Type.Length; col.Type.Kind == KindString && n > 0 {
					// Characters, and bytes at up to 4 per UTF-8 character.
					size, octets := n, 4*n
					ci.XdbcColumnSize, ci.XdbcCharOctetLength = &size, &octets
				}
				if col.Default != "" {
					def := col.Default
					ci.XdbcColumnDef = &def
				}
				if col.Comment != "" {
					remarks := col.Comment
					ci.Remarks = &remarks
				}
				info.TableColumns = append(info.TableColumns, ci)
			}
			// CHECK constraints are the only ones kept (see check.go).
			info.TableConstraints = []driverbase.ConstraintInfo{}
			if meta != nil {
				for _, chk := range meta.Checks {
					name := chk.Name
					info.TableConstraints = append(info.TableConstraints, driverbase.ConstraintInfo{
						ConstraintName:        &name,
						ConstraintType:        "CHECK",
						ConstraintColumnNames: driverbase.RequiredList(checkColumns(meta, chk.Expr)),
					})
				}
			}
		}
		out = append(out, info)
	}
	return out, nil
}
