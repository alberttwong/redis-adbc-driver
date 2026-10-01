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

// Storage layout
//
// This is a hybrid index-row layout.
//
// Primary data store: every row is a flat HASH holding the complete row, one
// field per non-NULL column plus a hidden __rowid field (so rows whose columns
// are all NULL still exist). Full rows are always read from here (HMGET), never
// from the index:
//
//	<schema>:<table>:<rowid>        HASH   __rowid, col1, col2, ... colN
//
// A column added by ADD COLUMN … DEFAULT is the exception: rows that existed
// then read the default without a field (see "Missing values" in
// defaults.go).
//
// Secondary index: a RediSearch index over the row prefix that only covers the
// columns used for filtering, sorting and aggregation. Numeric-like columns
// are NUMERIC SORTABLE and strings are TAG SORTABLE UNF (which doesn't hold
// every string exactly, see tags.go); binary columns and columns declared
// NOINDEX are not indexed and just sit in the HASHes:
//
//	idx:<schema>:<table>            FT index ON HASH PREFIX 1 <schema>:<table>:
//
// Driver metadata lives under one hash tag ({meta}), so every metadata key
// is in the same hash slot and metadata transactions (MULTI/EXEC) also work
// on sharded databases. Row keys have no hash tag, so rows spread across
// shards:
//
//	adbc:{meta}:table:<schema>:<table>   STRING JSON column definitions
//	adbc:{meta}:seq:<schema>:<table>     STRING row id counter
//	adbc:{meta}:tables:<schema>          SET    table names
//	adbc:{meta}:schemas                  SET    schema names
//
// Temporary tables and views live in per-connection schemas (see temp.go).
//
// Schema and table names are percent-escaped in key names so that ':' in a
// name cannot make two tables share a key prefix, and '{' / '}' cannot form
// a hash tag.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

const (
	catalogName   = "redis"
	defaultSchema = "public"
	rowIDField    = "__rowid"
	metaPrefix    = "adbc:{meta}:"
	schemasKey    = metaPrefix + "schemas"

	// maxIndexed caps the number of indexed (SORTABLE) attributes per table;
	// further columns are stored but not indexed.
	maxIndexed = 200
	// tagSeparator is the TAG separator for string columns. String values are
	// indexed whole, so a character that should never appear in data is used.
	tagSeparator = "\x1f"
	// pipelineChunk is the number of commands sent per pipeline round trip.
	pipelineChunk = 1000
	// cursorCount is the FT.AGGREGATE cursor page size. Every page is a
	// round trip, so pages are large; pageValues caps the fields per page so
	// that pages of wide rows stay a bounded size.
	cursorCount = 10000
	pageValues  = 200_000
)

type columnMeta struct {
	Name     string  `json:"name"`
	Type     ColType `json:"type"`
	Nullable bool    `json:"nullable"`
	// Indexed columns are part of the RediSearch index.
	Indexed bool `json:"indexed"`
	// Field is the HASH field / index attribute when it differs from Name:
	// after RENAME COLUMN (the data keeps its original field), and in join
	// views (which name columns alias.column).
	Field string `json:"field,omitempty"`
	// Default is the SQL text of the column's DEFAULT expression, evaluated
	// for inserted rows that give the column no value.
	Default string `json:"default,omitempty"`
	// Missing is the value, encoded as in the HASHes, of a column added by
	// ADD COLUMN … DEFAULT for the rows that existed then: rows with __rowid
	// up to MissingThrough that have no field for it (see "Missing values"
	// in defaults.go). MissingThrough is 0 when there were none.
	Missing        string `json:"missing,omitempty"`
	MissingThrough int64  `json:"missing_through,omitempty"`
	// TagValues is how exactly the index holds the values of an indexed
	// string column: "" (all as they are), tagsNormalized or tagsTruncated
	// (see tags.go). TagsChecked says it covers every stored value; columns
	// of tables created before it was recorded are checked once.
	TagValues   string `json:"tag_values,omitempty"`
	TagsChecked bool   `json:"tags_checked,omitempty"`
	// label is the name a result column takes from the column when it
	// differs from Name: joined relations name columns alias.column but
	// output them as column.
	label string
}

// field returns the HASH field and index attribute name of the column.
func (c columnMeta) field() string {
	if c.Field != "" {
		return c.Field
	}
	return c.Name
}

// outputName returns the name of a result column that selects the column.
// It is not field(): a renamed column keeps its original HASH field.
func (c columnMeta) outputName() string {
	if c.label != "" {
		return c.label
	}
	return c.Name
}

// indexable reports whether a column type can be part of the index.
func indexable(t ColType) bool {
	return t.Kind.indexedAsNumeric() || t.Kind == KindString
}

// applyIndexPolicy decides which columns are indexed: every indexable column
// (up to maxIndexed) unless only is non-nil, in which case just those names.
func (t *tableMeta) applyIndexPolicy(only map[string]bool, noIndex map[string]bool) error {
	for name := range only {
		if _, ok := t.resolve(name); !ok {
			return errorf(adbc.StatusInvalidArgument, "index column %q does not exist in table %q", name, t.Name)
		}
	}
	n := 0
	for i := range t.Columns {
		c := &t.Columns[i]
		// Only names usable as RediSearch attributes are indexed.
		want := indexable(c.Type) && !noIndex[c.Name] && simpleName(c.Name)
		if only != nil {
			want = false
			for name := range only {
				if strings.EqualFold(name, c.Name) {
					want = indexable(c.Type) && simpleName(c.Name)
				}
			}
		}
		c.Indexed = want && n < maxIndexed
		if c.Indexed {
			n++
		}
	}
	return nil
}

type tableMeta struct {
	Schema  string       `json:"schema"`
	Name    string       `json:"name"`
	Columns []columnMeta `json:"columns"`

	// KeyPrefix and IndexName are fixed when the table is created, so that
	// RENAME TO only rewrites metadata. Empty for tables created before they
	// were recorded (then derived from the name).
	KeyPrefix string `json:"key_prefix,omitempty"`
	IndexName string `json:"index_name,omitempty"`
	// RetiredFields are HASH fields of dropped columns; they are never
	// reused, so dropped values cannot reappear. PendingCleanup are the ones
	// still being removed from existing rows in the background.
	RetiredFields  []string `json:"retired_fields,omitempty"`
	PendingCleanup []string `json:"pending_cleanup,omitempty"`
	// RekeyTo is set while RENAME TO moves the rows to a new key prefix (the
	// one named here), and writes are refused. PrefixGen is how often the
	// key prefix had been released when the table took it (see rekey.go).
	RekeyTo   string `json:"rekey_to,omitempty"`
	PrefixGen int64  `json:"prefix_gen,omitempty"`
	// readAt is when the metadata was read (or last found not to be moving),
	// for the checks in rekey.go.
	readAt time.Time

	// In-memory relations (CTEs, derived tables) hold their rows here and
	// have no index or HASHes.
	isMem bool
	mem   []map[string]Value
	// join is set for the relation produced by a FROM clause with joins; its
	// rows are computed when the query is scanned.
	join *joinPlan
	// view is set for a simple view, computed when the query is scanned.
	view *lazyView
}

// resolve finds a column by name: exact match first, then case-insensitive.
func (t *tableMeta) resolve(name string) (int, bool) {
	for i, c := range t.Columns {
		if c.Name == name {
			return i, true
		}
	}
	for i, c := range t.Columns {
		if strings.EqualFold(c.Name, name) {
			return i, true
		}
	}
	return -1, false
}

func (t *tableMeta) column(name string) (columnMeta, bool) {
	if !t.isMem && strings.EqualFold(name, rowIDField) {
		return columnMeta{Name: rowIDField, Type: typeInt64, Indexed: true}, true
	}
	if i, ok := t.resolve(name); ok {
		return t.Columns[i], true
	}
	return columnMeta{}, false
}

func (t *tableMeta) types() map[string]ColType {
	m := make(map[string]ColType, len(t.Columns)+1)
	m[rowIDField] = typeInt64
	for _, c := range t.Columns {
		m[c.Name] = c.Type
	}
	return m
}

func escapeKeyPart(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '%' || c == ':' || c == '{' || c == '}' || c == '*' || c == '?' || c == '[' || c == ']' || c == '\\' || c <= ' ' || c == 0x7f {
			fmt.Fprintf(&b, "%%%02X", c)
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

func tableKeySuffix(schema, table string) string {
	return escapeKeyPart(schema) + ":" + escapeKeyPart(table)
}

func metaKey(schema, table string) string {
	return metaPrefix + "table:" + tableKeySuffix(schema, table)
}
func seqKey(schema, table string) string    { return metaPrefix + "seq:" + tableKeySuffix(schema, table) }
func tablesKey(schema string) string        { return metaPrefix + "tables:" + escapeKeyPart(schema) }
func rowPrefix(schema, table string) string { return tableKeySuffix(schema, table) + ":" }
func indexName(schema, table string) string { return "idx:" + tableKeySuffix(schema, table) }

func (t *tableMeta) index() string {
	if t.IndexName != "" {
		return t.IndexName
	}
	return indexName(t.Schema, t.Name)
}

func (t *tableMeta) prefix() string {
	if t.KeyPrefix != "" {
		return t.KeyPrefix
	}
	return rowPrefix(t.Schema, t.Name)
}

// indexAttrArgs returns the FT.CREATE / FT.ALTER attribute definition of an
// indexed column.
func indexAttrArgs(c columnMeta) []any {
	if c.Type.Kind == KindString {
		return []any{c.field(), "TAG", "SEPARATOR", tagSeparator, "CASESENSITIVE", "INDEXEMPTY", "SORTABLE", "UNF"}
	}
	return []any{c.field(), "NUMERIC", "SORTABLE"}
}

// Registry of key prefixes and index names in use. A renamed table keeps its
// prefix and index, so a new table with the old name must get different ones.
const (
	prefixesKey    = metaPrefix + "prefixes"
	indexesKey     = metaPrefix + "indexes"
	registryMarker = metaPrefix + "registry"
	cleanupKey     = metaPrefix + "cleanup"
)

// claim atomically reserves the first free name among base, base_2, … in a
// registry set; suffix formats a candidate from base and a counter.
func (s *store) claim(ctx context.Context, set, base string, suffix func(string, int) string) (string, error) {
	for n := 1; n < 10000; n++ {
		cand := base
		if n > 1 {
			cand = suffix(base, n)
		}
		added, err := s.client.SAdd(ctx, set, cand).Result()
		if err != nil {
			return "", wrapRedis(err, "failed to reserve a key prefix")
		}
		if added == 1 {
			return cand, nil
		}
	}
	return "", errorf(adbc.StatusInternal, "could not reserve a unique name for %q", base)
}

// ensureRegistry records the prefixes and indexes of tables created before
// the registry existed (once per database).
func (s *store) ensureRegistry(ctx context.Context) error {
	n, err := s.client.Exists(ctx, registryMarker).Result()
	if err != nil || n > 0 {
		return wrapRedis(err, "failed to check the table registry")
	}
	schemas, err := s.listSchemas(ctx)
	if err != nil {
		return err
	}
	for _, schema := range schemas {
		tables, err := s.listTables(ctx, schema)
		if err != nil {
			return err
		}
		for _, t := range tables {
			meta, err := s.getTable(ctx, schema, t)
			if err != nil {
				continue
			}
			if err := s.client.SAdd(ctx, prefixesKey, meta.prefix()).Err(); err != nil {
				return wrapRedis(err, "failed to build the table registry")
			}
			if err := s.client.SAdd(ctx, indexesKey, meta.index()).Err(); err != nil {
				return wrapRedis(err, "failed to build the table registry")
			}
		}
	}
	return wrapRedis(s.client.Set(ctx, registryMarker, "1", 0).Err(), "failed to build the table registry")
}

// updateTable applies fn to a table's metadata with optimistic locking
// (WATCH/MULTI); extra adds commands to the same transaction.
func (s *store) updateTable(ctx context.Context, schema, table string, fn func(*tableMeta) error,
	extra func(goredis.Pipeliner)) error {
	key := metaKey(schema, table)
	for attempt := 0; attempt < 20; attempt++ {
		err := s.client.Watch(ctx, func(tx *goredis.Tx) error {
			raw, err := tx.Get(ctx, key).Result()
			if errors.Is(err, goredis.Nil) {
				return tableNotFound(schema, table)
			}
			if err != nil {
				return err
			}
			var meta tableMeta
			if err := json.Unmarshal([]byte(raw), &meta); err != nil {
				return errorf(adbc.StatusInternal, "corrupt metadata for table %q.%q: %v", schema, table, err)
			}
			if err := fn(&meta); err != nil {
				return err
			}
			out, err := json.Marshal(&meta)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				p.Set(ctx, key, out, 0)
				if extra != nil {
					extra(p)
				}
				return nil
			})
			return err
		}, key)
		if errors.Is(err, goredis.TxFailedErr) {
			continue // concurrent change: retry
		}
		return wrapRedis(err, "failed to update table metadata")
	}
	return errorf(adbc.StatusIO, "table %q.%q is being changed concurrently; try again", displaySchema(schema), table)
}

// ---- errors ----

func errorf(code adbc.Status, format string, args ...any) error {
	return adbc.Error{Code: code, Msg: "[redis] " + fmt.Sprintf(format, args...)}
}

func tableNotFound(schema, table string) error {
	return errorf(adbc.StatusNotFound, "table %q.%q does not exist", displaySchema(schema), table)
}

func wrapRedis(err error, context string) error {
	if err == nil {
		return nil
	}
	var ae adbc.Error
	if errors.As(err, &ae) {
		return err
	}
	return errorf(adbc.StatusIO, "%s: %v", context, err)
}

// ---- store ----

type store struct {
	// client is a *goredis.Client, or a *goredis.ClusterClient when the
	// server exposes the OSS Cluster API.
	client goredis.UniversalClient

	// Background dropped-column cleanups and string column checks (see
	// tags.go) running on this connection, and the number of cluster shards
	// (see store.shards).
	mu         sync.Mutex
	cleaning   map[string]bool
	tagChecks  map[string]time.Time
	shardCount int

	// The connection's temporary tables and views (see temp.go).
	temp tempSpace
}

// search returns the connection used for FT.* commands. On a cluster every
// search command for an index (including FT.CURSOR READ, whose cursor lives
// on the node that created it) goes to the same node, which coordinates the
// query across shards.
// searchConn is the subset of a client used for FT.* commands.
type searchConn interface {
	Do(ctx context.Context, args ...any) *goredis.Cmd
}

func (s *store) search(ctx context.Context, index string) (searchConn, error) {
	cc, ok := s.client.(*goredis.ClusterClient)
	if !ok {
		return s.client, nil
	}
	node, err := cc.MasterForKey(ctx, index)
	if err != nil {
		return nil, wrapRedis(err, "failed to pick a node for search")
	}
	return node, nil
}

// searchDo runs one FT.* command for an index.
func (s *store) searchDo(ctx context.Context, index string, args ...any) *goredis.Cmd {
	node, err := s.search(ctx, index)
	if err != nil {
		cmd := goredis.NewCmd(ctx, args...)
		cmd.SetErr(err)
		return cmd
	}
	return node.Do(ctx, args...)
}

// migrateLegacyMetadata moves metadata written by v0.0.1 (adbc:meta:*,
// adbc:seq:*, adbc:tables:*, adbc:schemas) to the hash-tagged key names.
// New keys are written before old ones are removed, so an interrupted
// migration is simply redone on the next connection.
func (s *store) migrateLegacyMetadata(ctx context.Context) error {
	const legacySchemas = "adbc:schemas"
	n, err := s.client.Exists(ctx, legacySchemas).Result()
	if err != nil || n == 0 {
		return wrapRedis(err, "failed to check for legacy metadata")
	}
	schemas, err := s.client.SMembers(ctx, legacySchemas).Result()
	if err != nil {
		return wrapRedis(err, "failed to read legacy metadata")
	}
	if !slices.Contains(schemas, defaultSchema) {
		schemas = append(schemas, defaultSchema)
	}
	for _, schema := range schemas {
		oldTables := "adbc:tables:" + escapeKeyPart(schema)
		tables, err := s.client.SMembers(ctx, oldTables).Result()
		if err != nil {
			return wrapRedis(err, "failed to read legacy metadata")
		}
		for _, table := range tables {
			suffix := tableKeySuffix(schema, table)
			for _, kv := range [][2]string{
				{"adbc:meta:" + suffix, metaKey(schema, table)},
				{"adbc:seq:" + suffix, seqKey(schema, table)},
			} {
				v, err := s.client.Get(ctx, kv[0]).Result()
				if errors.Is(err, goredis.Nil) {
					continue
				}
				if err != nil {
					return wrapRedis(err, "failed to read legacy metadata")
				}
				if err := s.client.Set(ctx, kv[1], v, 0).Err(); err != nil {
					return wrapRedis(err, "failed to migrate metadata")
				}
			}
			if err := s.client.SAdd(ctx, tablesKey(schema), table).Err(); err != nil {
				return wrapRedis(err, "failed to migrate metadata")
			}
			for _, k := range []string{"adbc:meta:" + suffix, "adbc:seq:" + suffix} {
				if err := s.client.Del(ctx, k).Err(); err != nil {
					return wrapRedis(err, "failed to remove legacy metadata")
				}
			}
		}
		if err := s.client.SAdd(ctx, schemasKey, schema).Err(); err != nil {
			return wrapRedis(err, "failed to migrate metadata")
		}
		if err := s.client.Del(ctx, oldTables).Err(); err != nil {
			return wrapRedis(err, "failed to remove legacy metadata")
		}
	}
	return wrapRedis(s.client.Del(ctx, legacySchemas).Err(), "failed to remove legacy metadata")
}

func (s *store) getTable(ctx context.Context, schema, table string) (*tableMeta, error) {
	readAt := time.Now()
	raw, err := s.client.Get(ctx, metaKey(schema, table)).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, tableNotFound(schema, table)
	}
	if err != nil {
		return nil, wrapRedis(err, "failed to read table metadata")
	}
	var meta tableMeta
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return nil, errorf(adbc.StatusInternal, "corrupt metadata for table %q.%q: %v", schema, table, err)
	}
	meta.readAt = readAt
	return &meta, nil
}

func (s *store) tableExists(ctx context.Context, schema, table string) (bool, error) {
	n, err := s.client.Exists(ctx, metaKey(schema, table)).Result()
	if err != nil {
		return false, wrapRedis(err, "failed to check table")
	}
	return n > 0, nil
}

// createTable registers the table metadata and creates its search index.
// It returns false (and no error) if the table exists and ifNotExists is set.
func (s *store) createTable(ctx context.Context, meta *tableMeta, ifNotExists bool) (bool, error) {
	if err := s.checkWritableSchema(meta.Schema); err != nil {
		return false, err
	}
	if len(meta.Columns) == 0 {
		return false, errorf(adbc.StatusInvalidArgument, "table %q must have at least one column", meta.Name)
	}
	seen := map[string]bool{}
	for _, c := range meta.Columns {
		if c.Name == "" {
			return false, errorf(adbc.StatusInvalidArgument, "column names must not be empty")
		}
		if c.Name == rowIDField || strings.HasPrefix(c.Name, "__") {
			return false, errorf(adbc.StatusInvalidArgument, "column name %q is reserved", c.Name)
		}
		if seen[strings.ToLower(c.Name)] {
			return false, errorf(adbc.StatusAlreadyExists, "duplicate column name %q", c.Name)
		}
		seen[strings.ToLower(c.Name)] = true
	}
	for i := range meta.Columns {
		meta.Columns[i].initTags()
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return false, errorf(adbc.StatusInternal, "failed to encode metadata: %v", err)
	}
	if exists, err := s.viewExists(ctx, meta.Schema, meta.Name); err != nil {
		return false, err
	} else if exists {
		if ifNotExists {
			return false, nil
		}
		return false, errorf(adbc.StatusAlreadyExists, "%q.%q already exists as a view", displaySchema(meta.Schema), meta.Name)
	}
	ok, err := s.client.SetNX(ctx, metaKey(meta.Schema, meta.Name), raw, 0).Result()
	if err != nil {
		return false, wrapRedis(err, "failed to create table")
	}
	if !ok {
		if ifNotExists {
			return false, nil
		}
		return false, errorf(adbc.StatusAlreadyExists, "table %q.%q already exists", displaySchema(meta.Schema), meta.Name)
	}

	// Reserve a key prefix and index name no other table uses (a renamed
	// table keeps the ones derived from its old name).
	release := func() {
		s.client.Del(ctx, metaKey(meta.Schema, meta.Name))
		if meta.KeyPrefix != "" {
			s.client.SRem(ctx, prefixesKey, meta.KeyPrefix)
		}
		if meta.IndexName != "" {
			s.client.SRem(ctx, indexesKey, meta.IndexName)
		}
	}
	if meta.KeyPrefix, err = s.claim(ctx, prefixesKey, rowPrefix(meta.Schema, meta.Name), func(b string, n int) string {
		return strings.TrimSuffix(b, ":") + "~" + strconv.Itoa(n) + ":"
	}); err != nil {
		release()
		return false, err
	}
	if meta.IndexName, err = s.claim(ctx, indexesKey, indexName(meta.Schema, meta.Name), func(b string, n int) string {
		return b + "~" + strconv.Itoa(n)
	}); err != nil {
		release()
		return false, err
	}
	if meta.PrefixGen, err = prefixGen(ctx, s.client, meta.KeyPrefix); err != nil {
		release()
		return false, wrapRedis(err, "failed to create table")
	}
	if raw, err = json.Marshal(meta); err == nil {
		err = s.client.Set(ctx, metaKey(meta.Schema, meta.Name), raw, 0).Err()
	}
	if err != nil {
		release()
		return false, wrapRedis(err, "failed to create table")
	}

	// The index name was free in the registry, so an index with that name is
	// left over from an interrupted CREATE/DROP and can be discarded.
	_ = s.searchDo(ctx, meta.index(), "FT.DROPINDEX", meta.index(), "DD").Err()

	if err := s.searchDo(ctx, meta.index(), indexCreateArgs(meta)...).Err(); err != nil {
		release()
		return false, wrapRedis(err, "failed to create search index")
	}
	pipe := s.client.TxPipeline()
	pipe.SAdd(ctx, tablesKey(meta.Schema), meta.Name)
	if !isTempSchema(meta.Schema) {
		pipe.SAdd(ctx, schemasKey, meta.Schema)
	}
	pipe.Del(ctx, seqKey(meta.Schema, meta.Name))
	if _, err := pipe.Exec(ctx); err != nil {
		return false, wrapRedis(err, "failed to register table")
	}
	s.trackTemp(meta.Schema, meta.Name, true)
	return true, nil
}

// indexCreateArgs is the FT.CREATE command for a table's index: every
// indexed column, plus __rowid so all-NULL rows are still documents.
func indexCreateArgs(meta *tableMeta) []any {
	args := []any{"FT.CREATE", meta.index(), "ON", "HASH", "PREFIX", 1, meta.prefix(),
		"SKIPINITIALSCAN", "SCHEMA", rowIDField, "NUMERIC", "SORTABLE"}
	for _, c := range meta.Columns {
		if c.Indexed {
			args = append(args, indexAttrArgs(c)...)
		}
	}
	return args
}

// truncateTable removes every row but keeps the table: its metadata, key
// prefix and index name. Dropping the index with DD deletes every document
// it knows about (as DROP TABLE does), and it is then recreated empty. Rows
// written by another connection while this runs may be left unindexed.
func (s *store) truncateTable(ctx context.Context, schema, table string, restartIdentity bool) error {
	meta, err := s.getTable(ctx, schema, table)
	if err != nil {
		return err
	}
	if err := s.checkWritable(ctx, meta); err != nil {
		return err
	}
	if err := s.searchDo(ctx, meta.index(), "FT.DROPINDEX", meta.index(), "DD").Err(); err != nil &&
		!isUnknownIndex(err) {
		return wrapRedis(err, "failed to truncate table")
	}
	if err := s.searchDo(ctx, meta.index(), indexCreateArgs(meta)...).Err(); err != nil {
		return wrapRedis(err, "failed to recreate search index")
	}
	if err := s.dropMissingValues(ctx, meta); err != nil {
		return err
	}
	if restartIdentity {
		if err := s.client.Del(ctx, seqKey(schema, table)).Err(); err != nil {
			return wrapRedis(err, "failed to restart the row id sequence")
		}
	}
	return s.checkWritten(ctx, meta)
}

// dropTable removes the index, every row, and the table metadata.
func (s *store) dropTable(ctx context.Context, schema, table string, ifExists bool) error {
	meta, err := s.getTable(ctx, schema, table)
	if err != nil {
		var ae adbc.Error
		if ifExists && asAdbc(err, &ae) && ae.Code == adbc.StatusNotFound {
			return nil
		}
		return err
	}
	// DD deletes every document the index knows about.
	if err := s.searchDo(ctx, meta.index(), "FT.DROPINDEX", meta.index(), "DD").Err(); err != nil &&
		!isUnknownIndex(err) {
		return wrapRedis(err, "failed to drop search index")
	}
	pipe := s.client.TxPipeline()
	pipe.Del(ctx, metaKey(schema, table), seqKey(schema, table))
	pipe.SRem(ctx, tablesKey(schema), table)
	pipe.SRem(ctx, prefixesKey, meta.prefix())
	pipe.SRem(ctx, indexesKey, meta.index())
	if !isTempSchema(schema) {
		pipe.HIncrBy(ctx, releasedKey, meta.prefix(), 1) // see rekey.go
	}
	pipe.SRem(ctx, cleanupKey, cleanupMember(schema, table))
	if _, err := pipe.Exec(ctx); err != nil {
		return wrapRedis(err, "failed to drop table")
	}
	s.trackTemp(schema, table, false)
	return nil
}

func isUnknownIndex(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown index") || strings.Contains(msg, "no such index") ||
		strings.Contains(msg, "not found")
}

func (s *store) listSchemas(ctx context.Context) ([]string, error) {
	members, err := s.client.SMembers(ctx, schemasKey).Result()
	if err != nil {
		return nil, wrapRedis(err, "failed to list schemas")
	}
	// Temporary schemas are never registered; skip them all the same.
	members = slices.DeleteFunc(members, reservedSchema)
	if !slices.Contains(members, defaultSchema) {
		members = append(members, defaultSchema)
	}
	slices.Sort(members)
	return members, nil
}

func (s *store) schemaExists(ctx context.Context, schema string) (bool, error) {
	if schema == defaultSchema {
		return true, nil
	}
	if reservedSchema(schema) {
		return false, nil
	}
	ok, err := s.client.SIsMember(ctx, schemasKey, schema).Result()
	if err != nil {
		return false, wrapRedis(err, "failed to check schema")
	}
	return ok, nil
}

func (s *store) createSchema(ctx context.Context, schema string, ifNotExists bool) error {
	if schema == "" {
		return errorf(adbc.StatusInvalidArgument, "schema name must not be empty")
	}
	if isInfoSchema(schema) {
		if ifNotExists {
			return nil
		}
		return errorf(adbc.StatusAlreadyExists, "schema %q already exists", schema)
	}
	if reservedSchema(schema) {
		return errorf(adbc.StatusInvalidArgument, "schema name %q is reserved for temporary tables and views", schema)
	}
	n, err := s.client.SAdd(ctx, schemasKey, schema).Result()
	if err != nil {
		return wrapRedis(err, "failed to create schema")
	}
	if (n == 0 || schema == defaultSchema) && !ifNotExists {
		return errorf(adbc.StatusAlreadyExists, "schema %q already exists", schema)
	}
	return nil
}

// dropSchema drops an empty schema or, with cascade, its views and tables
// first. Views are dropped before tables; views in other schemas that read
// these tables are not dropped (dependencies aren't tracked).
func (s *store) dropSchema(ctx context.Context, schema string, ifExists, cascade bool) error {
	exists, err := s.schemaExists(ctx, schema)
	if err != nil {
		return err
	}
	if !exists {
		if ifExists {
			return nil
		}
		return errorf(adbc.StatusNotFound, "schema %q does not exist", schema)
	}
	tables, err := s.listTables(ctx, schema)
	if err != nil {
		return err
	}
	views, err := s.listViews(ctx, schema)
	if err != nil {
		return err
	}
	if len(tables)+len(views) > 0 {
		if !cascade {
			return errorf(adbc.StatusInvalidState,
				"schema %q is not empty (%d tables, %d views); use DROP SCHEMA … CASCADE to drop them too",
				schema, len(tables), len(views))
		}
		for _, v := range views {
			if err := s.dropView(ctx, schema, v, true); err != nil {
				return err
			}
		}
		for _, t := range tables {
			if err := s.dropTable(ctx, schema, t, true); err != nil {
				return err
			}
		}
	}
	if err := s.client.SRem(ctx, schemasKey, schema).Err(); err != nil {
		return wrapRedis(err, "failed to drop schema")
	}
	return nil
}

func (s *store) listTables(ctx context.Context, schema string) ([]string, error) {
	members, err := s.client.SMembers(ctx, tablesKey(schema)).Result()
	if err != nil {
		return nil, wrapRedis(err, "failed to list tables")
	}
	slices.Sort(members)
	return members, nil
}

// insertRows writes rows (values ordered like meta.Columns and already
// coerced to the column types) as HASHes, returning the number written.
func (s *store) insertRows(ctx context.Context, meta *tableMeta, rows [][]Value) (int64, error) {
	a, err := s.allocRows(ctx, meta, rows)
	if err != nil {
		return 0, err
	}
	return s.writeRows(ctx, meta, a, rows)
}

// rowAlloc describes rows allocated by allocRows: their row ids are first,
// first+1, …, and added holds the fields (name, value, …) of the missing
// values they must be written with (see addedMissing).
type rowAlloc struct {
	first int64
	added []any
}

// allocRows checks rows to be inserted against NOT NULL and allocates their
// row ids. Ids allocated for rows that are then not written are not reused.
func (s *store) allocRows(ctx context.Context, meta *tableMeta, rows [][]Value) (rowAlloc, error) {
	if len(rows) == 0 {
		return rowAlloc{}, nil
	}
	for _, row := range rows {
		for i, c := range meta.Columns {
			if row[i].Null && !c.Nullable {
				return rowAlloc{}, errorf(adbc.StatusIntegrity, "NULL value in column %q violates not-null constraint", c.Name)
			}
		}
	}
	if err := s.checkWritable(ctx, meta); err != nil {
		return rowAlloc{}, err
	}
	// The row ids are allocated in one transaction with a read of the
	// metadata, which shows columns that ADD COLUMN … DEFAULT added since
	// meta was read: these rows get their missing values (see defaults.go).
	tx := s.client.TxPipeline()
	incr := tx.IncrBy(ctx, seqKey(meta.Schema, meta.Name), int64(len(rows)))
	cur := tx.Get(ctx, metaKey(meta.Schema, meta.Name))
	if _, err := tx.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		return rowAlloc{}, wrapRedis(err, "failed to allocate row ids")
	}
	last, err := incr.Result()
	if err != nil {
		return rowAlloc{}, wrapRedis(err, "failed to allocate row ids")
	}
	return rowAlloc{first: last - int64(len(rows)) + 1, added: addedMissing(meta, cur.Val())}, nil
}

// writeRows writes rows allocated by allocRows as HASHes.
func (s *store) writeRows(ctx context.Context, meta *tableMeta, a rowAlloc, rows [][]Value) (int64, error) {
	if err := s.raiseTagLevels(ctx, meta, rowTagLevels(meta, rows)); err != nil {
		return 0, err
	}
	prefix := meta.prefix()
	for start := 0; start < len(rows); start += pipelineChunk {
		end := min(start+pipelineChunk, len(rows))
		pipe := s.client.Pipeline()
		for r := start; r < end; r++ {
			id := strconv.FormatInt(a.first+int64(r), 10)
			fields := make([]any, 0, 2+2*len(meta.Columns)+len(a.added))
			fields = append(fields, rowIDField, id)
			for i, c := range meta.Columns {
				if v := rows[r][i]; !v.Null {
					fields = append(fields, c.field(), encodeStored(v))
				}
			}
			fields = append(fields, a.added...)
			pipe.HSet(ctx, prefix+id, fields...)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, wrapRedis(err, "failed to write rows")
		}
	}
	if len(rows) > 0 {
		if err := s.checkWritten(ctx, meta); err != nil {
			return 0, err
		}
	}
	return int64(len(rows)), nil
}

// fetchRows reads the given fields of each row HASH with pipelined HMGET.
// Rows that no longer exist (deleted since the index lookup) are returned as
// nil maps.
func (s *store) fetchRows(ctx context.Context, keys []string, fields []string) ([]aggRow, error) {
	fields = append([]string{rowIDField}, fields...)
	out := make([]aggRow, len(keys))
	for start := 0; start < len(keys); start += pipelineChunk {
		end := min(start+pipelineChunk, len(keys))
		pipe := s.client.Pipeline()
		cmds := make([]*goredis.SliceCmd, 0, end-start)
		for _, k := range keys[start:end] {
			cmds = append(cmds, pipe.HMGet(ctx, k, fields...))
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
			return nil, wrapRedis(err, "failed to read rows")
		}
		for i, cmd := range cmds {
			vals, err := cmd.Result()
			if err != nil {
				return nil, wrapRedis(err, "failed to read rows")
			}
			if vals[0] == nil {
				continue // row vanished
			}
			row := make(aggRow, len(fields))
			for j, v := range vals {
				if sv, ok := v.(string); ok {
					row[fields[j]] = sv
				}
			}
			out[start+i] = row
		}
	}
	return out, nil
}

func (s *store) serverVersion(ctx context.Context) (string, error) {
	info, err := s.client.Info(ctx, "server").Result()
	if err != nil {
		return "", wrapRedis(err, "failed to query server info")
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			return v, nil
		}
	}
	return "unknown", nil
}
