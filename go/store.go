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
//
// Key prefixes are never reused. A table that is created, truncated
// (truncateTables), or moved by a re-keying rename (rekey.go) takes the
// first <schema>:<table>: or <schema>:<table>~N: (and the index name
// idx:<schema>:<table>[~N]) that no table has ever had: a statement that
// read a dropped table's metadata may still be writing rows under its
// prefix, and rows left there must not become another table's.
// adbc:{meta}:names:next (HASH) holds the last N handed out for each name.
// See claimNames.

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
	// Comment is the column's COMMENT ON text (see comment.go).
	Comment string `json:"comment,omitempty"`
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
	// Comment is the table's COMMENT ON text (see comment.go).
	Comment string `json:"comment,omitempty"`
	// Checks are the table's CHECK constraints (see check.go).
	Checks []checkMeta `json:"checks,omitempty"`
	// readAt is when the metadata was read (or last found not to be moving),
	// for the checks in rekey.go.
	readAt time.Time
	// wrote lists what the statement wrote through this metadata, to be
	// deleted if the table turns out to have been dropped (discardWritten).
	wrote rowLog

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
func (t *tableMeta) resolve(name string) (int, bool) { return resolveColumn(t.Columns, name) }

// resolveColumn is resolve for a list of columns (a table's or a view's).
func resolveColumn(cols []columnMeta, name string) (int, bool) {
	for i, c := range cols {
		if c.Name == name {
			return i, true
		}
	}
	for i, c := range cols {
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
// namesNextKey holds, for each table name (its plain key prefix), the last N
// its prefixes were given.
const (
	prefixesKey    = metaPrefix + "prefixes"
	indexesKey     = metaPrefix + "indexes"
	registryMarker = metaPrefix + "registry"
	cleanupKey     = metaPrefix + "cleanup"
	namesNextKey   = metaPrefix + "names:next"
)

// tableNames are the key prefix and index name a table takes: for N = 1,
// <schema>:<table>: and idx:<schema>:<table>, otherwise the same with ~N.
type tableNames struct {
	base          string // the plain prefix, which namesNextKey counts by
	prefix, index string
	n             int64
	temp          bool
}

func namesFor(schema, table string, n int64) tableNames {
	base := rowPrefix(schema, table)
	nm := tableNames{base: base, prefix: base, index: indexName(schema, table), n: n, temp: isTempSchema(schema)}
	if n > 1 {
		suffix := "~" + strconv.FormatInt(n, 10)
		nm.prefix = strings.TrimSuffix(base, ":") + suffix + ":"
		nm.index += suffix
	}
	return nm
}

// lastNames returns the N of the last names handed out for a table name.
// The counter only saves probing: every candidate is still checked.
func (s *store) lastNames(ctx context.Context, c goredis.Cmdable, schema, table string) (int64, error) {
	if isTempSchema(schema) {
		return s.tempLastNames(rowPrefix(schema, table)), nil
	}
	n, err := c.HGet(ctx, namesNextKey, rowPrefix(schema, table)).Int64()
	if errors.Is(err, goredis.Nil) {
		return 0, nil
	}
	return n, err
}

// claimNames reserves the key prefix and index name of a new table: the
// first names after the last ones handed out for its name whose prefix no
// table has ever had (it is neither reserved in prefixesKey nor counted in
// releasedKey) and whose index name is free. A prefix that was released may
// still have rows: a statement that read a dropped table's metadata goes on
// writing until it finds out, and one that crashed never does.
//
// Each candidate is reserved with SADD, then checked against releasedKey in
// the same pipeline (one hash slot, so in order): a prefix that was free
// when it was added can't be released later, since only its owner releases
// it. Temporary schemas don't count releases; their connection remembers
// every prefix it handed out instead (see tempSpace).
func (s *store) claimNames(ctx context.Context, schema, table string) (tableNames, error) {
	last, err := s.lastNames(ctx, s.client, schema, table)
	if err != nil {
		return tableNames{}, wrapRedis(err, "failed to reserve a key prefix")
	}
	for n := last + 1; n < last+10000; n++ {
		nm := namesFor(schema, table, n)
		if nm.temp && s.tempUsedPrefix(nm.prefix) {
			continue
		}
		// The pipeline is sent once (see onceCmd): sent again, its SADDs
		// would find the names taken.
		pipe := s.client.Pipeline()
		addP := once(ctx, pipe, "SADD", prefixesKey, nm.prefix)
		addI := pipe.SAdd(ctx, indexesKey, nm.index)
		var released *goredis.BoolCmd
		if !nm.temp {
			released = pipe.HExists(ctx, releasedKey, nm.prefix)
			pipe.HSet(ctx, namesNextKey, nm.base, n)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return tableNames{}, wrapRedis(err, "failed to reserve a key prefix")
		}
		gotP, _ := addP.Int64()
		if gotP == 1 && addI.Val() == 1 && (released == nil || !released.Val()) {
			s.noteTempNames(nm)
			return nm, nil
		}
		// Give back what this candidate got.
		pipe = s.client.Pipeline()
		if gotP == 1 {
			pipe.SRem(ctx, prefixesKey, nm.prefix)
		}
		if addI.Val() == 1 {
			pipe.SRem(ctx, indexesKey, nm.index)
		}
		if pipe.Len() > 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return tableNames{}, wrapRedis(err, "failed to reserve a key prefix")
			}
		}
	}
	return tableNames{}, errorf(adbc.StatusInternal, "could not reserve a unique key prefix for %q", rowPrefix(schema, table))
}

// freeNames is claimNames inside a transaction that watches prefixesKey,
// indexesKey and releasedKey: it only reads, and the caller reserves the
// names it returns in the transaction (reserveNames).
func (s *store) freeNames(ctx context.Context, tx *goredis.Tx, schema, table string) (tableNames, error) {
	last, err := s.lastNames(ctx, tx, schema, table)
	if err != nil {
		return tableNames{}, err
	}
	for n := last + 1; n < last+10000; n++ {
		nm := namesFor(schema, table, n)
		if nm.temp && s.tempUsedPrefix(nm.prefix) {
			continue
		}
		var usedP, usedI, released *goredis.BoolCmd
		if _, err := tx.Pipelined(ctx, func(p goredis.Pipeliner) error {
			usedP = p.SIsMember(ctx, prefixesKey, nm.prefix)
			usedI = p.SIsMember(ctx, indexesKey, nm.index)
			if !nm.temp {
				released = p.HExists(ctx, releasedKey, nm.prefix)
			}
			return nil
		}); err != nil {
			return tableNames{}, err
		}
		if !usedP.Val() && !usedI.Val() && (released == nil || !released.Val()) {
			return nm, nil
		}
	}
	return tableNames{}, errorf(adbc.StatusInternal, "could not reserve a unique key prefix for %q", rowPrefix(schema, table))
}

// reserveNames adds the commands that reserve names found by freeNames to a
// transaction; noteTempNames must follow once it has committed.
func reserveNames(ctx context.Context, p goredis.Pipeliner, nm tableNames) {
	p.SAdd(ctx, prefixesKey, nm.prefix)
	p.SAdd(ctx, indexesKey, nm.index)
	if !nm.temp {
		p.HSet(ctx, namesNextKey, nm.base, nm.n)
	}
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
	return s.updateTableTx(ctx, schema, table, false, func(m *tableMeta, _ int64) error { return fn(m) }, extra)
}

// updateTableTx is updateTable for a change that may depend on the table's
// row id high-water mark: with atRowID, fn gets the last row id allocated
// (0 if none), and the change only commits if no row ids were allocated
// meanwhile. Without it, fn gets 0.
func (s *store) updateTableTx(ctx context.Context, schema, table string, atRowID bool,
	fn func(m *tableMeta, lastRowID int64) error, extra func(goredis.Pipeliner)) error {
	key, seq := metaKey(schema, table), seqKey(schema, table)
	watch := []string{key}
	if atRowID {
		watch = append(watch, seq)
	}
	for attempt := 0; attempt < 20; attempt++ {
		err := s.client.Watch(ctx, func(tx *goredis.Tx) error {
			m, err := s.getTableWith(ctx, tx, schema, table)
			if err != nil {
				return err
			}
			var last int64
			if atRowID {
				if last, err = tx.Get(ctx, seq).Int64(); err != nil && !errors.Is(err, goredis.Nil) {
					return err
				}
			}
			if err := fn(m, last); err != nil {
				return err
			}
			raw, err := marshalMeta(m)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				p.Set(ctx, key, raw, 0)
				if extra != nil {
					extra(p)
				}
				return nil
			})
			return err
		}, watch...)
		if errors.Is(err, goredis.TxFailedErr) {
			continue // concurrent change (or insert): retry
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

	// retired are clients replaced by another one with other timeouts (see
	// connectionImpl.setTimeouts), closed with the connection.
	retired []goredis.UniversalClient
}

// onceCmd is a command that go-redis must not send again when it fails.
// go-redis re-sends most commands after a timeout or a broken connection,
// though Redis may have run them already. That is harmless for most of the
// driver's commands, but not for these:
//   - Search commands. A repeated FT.CURSOR READ returns the next page, so
//     the rows of the page that timed out would be missing from the result;
//     a repeated FT.CREATE or FT.ALTER fails ("already exists", "Duplicate
//     field"); and repeating a slow FT.AGGREGATE just loads the server more.
//   - Commands whose reply the driver acts on: SET … NX and SADD, which
//     would report that the first attempt's key or member already exists.
//
// A pipeline with one of them in it isn't sent again either.
type onceCmd struct{ *goredis.Cmd }

func (onceCmd) NoRetry() bool { return true }

// processor is a client, or a cluster node's client.
type processor interface {
	Process(ctx context.Context, cmd goredis.Cmder) error
}

// once runs a command that must not be sent twice (see onceCmd).
func once(ctx context.Context, c processor, args ...any) *goredis.Cmd {
	cmd := goredis.NewCmd(ctx, args...)
	_ = c.Process(ctx, onceCmd{cmd})
	return cmd
}

// searchConn sends FT.* commands, each once (see onceCmd).
type searchConn struct{ c processor }

func (n searchConn) Do(ctx context.Context, args ...any) *goredis.Cmd { return once(ctx, n.c, args...) }

// search returns the connection used for FT.* commands. On a cluster every
// search command for an index (including FT.CURSOR READ, whose cursor lives
// on the node that created it) goes to the same node, which coordinates the
// query across shards.
func (s *store) search(ctx context.Context, index string) (searchConn, error) {
	cc, ok := s.client.(*goredis.ClusterClient)
	if !ok {
		return searchConn{s.client}, nil
	}
	node, err := cc.MasterForKey(ctx, index)
	if err != nil {
		return searchConn{}, wrapRedis(err, "failed to pick a node for search")
	}
	return searchConn{node}, nil
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
	if exists, err := s.viewExists(ctx, meta.Schema, meta.Name); err != nil {
		return false, err
	} else if exists {
		if ifNotExists {
			return false, nil
		}
		return false, errorf(adbc.StatusAlreadyExists, "%q.%q already exists as a view", displaySchema(meta.Schema), meta.Name)
	}
	key := metaKey(meta.Schema, meta.Name)
	exists := func() (bool, error) {
		if ifNotExists {
			return false, nil
		}
		return false, errorf(adbc.StatusAlreadyExists, "table %q.%q already exists", displaySchema(meta.Schema), meta.Name)
	}
	if n, err := s.client.Exists(ctx, key).Result(); err != nil {
		return false, wrapRedis(err, "failed to create table")
	} else if n > 0 {
		return exists()
	}

	// Reserve a key prefix and index name that no table has had (see
	// claimNames), and create the index. The metadata is written last, so
	// no statement sees the table before its keys are known and indexed.
	nm, err := s.claimNames(ctx, meta.Schema, meta.Name)
	if err != nil {
		return false, err
	}
	meta.KeyPrefix, meta.IndexName, meta.PrefixGen = nm.prefix, nm.index, 0
	created := false
	release := func() {
		bg := context.WithoutCancel(ctx)
		if created {
			_ = s.searchDo(bg, meta.index(), "FT.DROPINDEX", meta.index(), "DD").Err()
		}
		pipe := s.client.Pipeline()
		pipe.SRem(bg, prefixesKey, meta.KeyPrefix)
		pipe.SRem(bg, indexesKey, meta.IndexName)
		_, _ = pipe.Exec(bg)
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		release()
		return false, errorf(adbc.StatusInternal, "failed to encode metadata: %v", err)
	}

	// The index name was free in the registry, so an index with that name is
	// left over from an interrupted CREATE/DROP and can be discarded.
	_ = s.searchDo(ctx, meta.index(), "FT.DROPINDEX", meta.index(), "DD").Err()

	if err := s.searchDo(ctx, meta.index(), indexCreateArgs(meta)...).Err(); err != nil {
		release()
		return false, wrapRedis(err, "failed to create search index")
	}
	created = true
	// The metadata, a fresh row id sequence and the registration, in one
	// transaction, unless the table was created meanwhile.
	won := false
	for attempt := 0; attempt < 20 && !won; attempt++ {
		err = s.client.Watch(ctx, func(tx *goredis.Tx) error {
			if n, err := tx.Exists(ctx, key).Result(); err != nil || n > 0 {
				return err
			}
			_, err := tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				p.Set(ctx, key, raw, 0)
				p.Del(ctx, seqKey(meta.Schema, meta.Name))
				p.SAdd(ctx, tablesKey(meta.Schema), meta.Name)
				if !isTempSchema(meta.Schema) {
					p.SAdd(ctx, schemasKey, meta.Schema)
				}
				return nil
			})
			won = err == nil
			return err
		}, key)
		if !errors.Is(err, goredis.TxFailedErr) {
			break
		}
	}
	if !won {
		release()
		if err != nil {
			return false, wrapRedis(err, "failed to create table")
		}
		return exists()
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

// truncation is one table that a TRUNCATE empties.
type truncation struct {
	meta  *tableMeta // as the statement read it
	names tableNames // the new key prefix and index name
	// attrs are the new index's attributes (field → definition).
	attrs   map[string]string
	created bool
	// The names the table had when it switched, released then.
	oldPrefix, oldIndex string
}

// testHookTruncating, when set by a test, runs once truncateTables has
// created the new indexes, before it switches the tables to them.
var testHookTruncating func()

// truncateTables empties tables, all of them or none. Each table moves to a
// key prefix and index name that no table has had (claimNames), as if it
// were dropped and created again with the same metadata:
//
//  1. Reserve the new names and create the new, empty index.
//  2. One transaction, for every table, points the metadata to the new
//     names and releases the old ones, as DROP TABLE does. The new prefix
//     has no rows, so the missing values and dropped-column cleanups go,
//     and with RESTART IDENTITY the row id counter starts again at 1.
//  3. FT.DROPINDEX … DD on the old index deletes the old rows.
//
// A statement that is still writing through the old metadata finds out as
// it does after DROP TABLE (sameKeys, checkKeys): it stops, deletes what it
// wrote, and fails, and what it still writes goes to a prefix that no table
// has. When TRUNCATE kept the prefix, such rows stayed in the table (some
// of them not indexed), and after RESTART IDENTITY new rows took their keys
// and were merged into them.
func (s *store) truncateTables(ctx context.Context, metas []*tableMeta, restartIdentity bool) error {
	ts := make([]*truncation, len(metas))
	for i, meta := range metas {
		if err := s.checkWritable(ctx, meta); err != nil {
			return err
		}
		ts[i] = &truncation{meta: meta}
	}
	// release gives back the new names, unless the switch has happened.
	release := func() {
		bg := context.WithoutCancel(ctx)
		for _, t := range ts {
			if t.created {
				_ = s.searchDo(bg, t.names.index, "FT.DROPINDEX", t.names.index, "DD").Err()
			}
			if t.names.prefix != "" {
				pipe := s.client.Pipeline()
				pipe.SRem(bg, prefixesKey, t.names.prefix)
				pipe.SRem(bg, indexesKey, t.names.index)
				_, _ = pipe.Exec(bg)
			}
		}
	}
	for _, t := range ts {
		nm, err := s.claimNames(ctx, t.meta.Schema, t.meta.Name)
		if err != nil {
			release()
			return err
		}
		t.names = nm
		next := *t.meta
		next.KeyPrefix, next.IndexName = nm.prefix, nm.index
		// The index name was free in the registry, so an index with that
		// name is left over from an interrupted CREATE/DROP and can be
		// discarded.
		_ = s.searchDo(ctx, nm.index, "FT.DROPINDEX", nm.index, "DD").Err()
		if err := s.searchDo(ctx, nm.index, indexCreateArgs(&next)...).Err(); err != nil {
			release()
			return wrapRedis(err, "failed to create search index")
		}
		t.created = true
		t.attrs = map[string]string{}
		for _, c := range t.meta.Columns {
			if c.Indexed {
				t.attrs[c.field()] = fmt.Sprint(indexAttrArgs(c))
			}
		}
	}
	if testHookTruncating != nil {
		testHookTruncating()
	}

	keys := make([]string, len(ts))
	for i, t := range ts {
		keys[i] = metaKey(t.meta.Schema, t.meta.Name)
	}
	var err error
	sent := false
	for attempt := 0; attempt < 20; attempt++ {
		sent = false
		err = s.client.Watch(ctx, func(tx *goredis.Tx) error {
			raws := make([][]byte, len(ts))
			for i, t := range ts {
				cur, err := s.getTableWith(ctx, tx, t.meta.Schema, t.meta.Name)
				if err != nil {
					return err
				}
				if err := movingErr(cur); err != nil {
					return err
				}
				if err := s.addIndexedColumns(ctx, t, cur); err != nil {
					return err
				}
				// cur may be a table created again since the statement read
				// it: then that one is emptied.
				t.oldPrefix, t.oldIndex = cur.prefix(), cur.index()
				cur.KeyPrefix, cur.IndexName, cur.PrefixGen = t.names.prefix, t.names.index, 0
				for j := range cur.Columns {
					cur.Columns[j].Missing, cur.Columns[j].MissingThrough = "", 0
				}
				cur.PendingCleanup = nil
				if raws[i], err = marshalMeta(cur); err != nil {
					return err
				}
			}
			sent = true
			_, err := tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				for i, t := range ts {
					schema, name := t.meta.Schema, t.meta.Name
					p.Set(ctx, keys[i], raws[i], 0)
					if restartIdentity {
						p.Del(ctx, seqKey(schema, name))
					}
					p.SRem(ctx, prefixesKey, t.oldPrefix)
					p.SRem(ctx, indexesKey, t.oldIndex)
					if !isTempSchema(schema) {
						p.HIncrBy(ctx, releasedKey, t.oldPrefix, 1) // see rekey.go
					}
					p.SRem(ctx, cleanupKey, cleanupMember(schema, name))
				}
				return nil
			})
			return err
		}, keys...)
		if !errors.Is(err, goredis.TxFailedErr) {
			break
		}
	}
	if err != nil {
		// Nothing switched, unless EXEC was sent and its reply was lost: then
		// the new names may be in use, and are kept.
		if !sent || errors.Is(err, goredis.TxFailedErr) {
			release()
		}
		if errors.Is(err, goredis.TxFailedErr) {
			return errorf(adbc.StatusIO, "table %q.%q is being changed concurrently; try again",
				displaySchema(ts[0].meta.Schema), ts[0].meta.Name)
		}
		return wrapRedis(err, "failed to truncate table")
	}

	var first error
	for _, t := range ts {
		if err := s.searchDo(ctx, t.oldIndex, "FT.DROPINDEX", t.oldIndex, "DD").Err(); err != nil &&
			!isUnknownIndex(err) && first == nil {
			first = errorf(adbc.StatusIO, "table %q.%q is empty, but deleting its old rows failed: %v; they are left under the key prefix %q, which no table has any more",
				displaySchema(t.meta.Schema), t.meta.Name, err, t.oldPrefix)
		}
	}
	return first
}

// addIndexedColumns adds to a truncated table's new index the indexed
// columns that the table's current metadata has and the index doesn't: an
// ADD COLUMN that committed after TRUNCATE read the metadata added them to
// the old index only.
func (s *store) addIndexedColumns(ctx context.Context, t *truncation, cur *tableMeta) error {
	for _, c := range cur.Columns {
		if !c.Indexed {
			continue
		}
		def := fmt.Sprint(indexAttrArgs(c))
		have, ok := t.attrs[c.field()]
		if ok && have != def {
			// The table was dropped and created again with another type.
			return errorf(adbc.StatusIO, "table %q.%q was changed concurrently; try again", displaySchema(cur.Schema), cur.Name)
		}
		if ok {
			continue
		}
		args := append([]any{"FT.ALTER", t.names.index, "SCHEMA", "ADD"}, indexAttrArgs(c)...)
		if err := s.searchDo(ctx, t.names.index, args...).Err(); err != nil {
			return wrapRedis(err, "failed to add a column to the new search index")
		}
		t.attrs[c.field()] = def
	}
	return nil
}

// testHookDropping, when set by a test, runs once dropTable has dropped the
// table's index, before it removes the metadata.
var testHookDropping func()

// dropTable removes the index, every row, and the table metadata. The
// metadata goes only if the table still has the keys whose index was
// dropped: otherwise (a TRUNCATE moved it to new ones meanwhile) it starts
// again, so that the new index and key prefix don't outlive the table.
func (s *store) dropTable(ctx context.Context, schema, table string, ifExists bool) error {
	key := metaKey(schema, table)
	for attempt := 0; attempt < 20; attempt++ {
		meta, err := s.getTable(ctx, schema, table)
		if err != nil {
			var ae adbc.Error
			if ifExists && asAdbc(err, &ae) && ae.Code == adbc.StatusNotFound {
				return nil
			}
			return err
		}
		// DROP TABLE isn't refused while a re-key moves the table's rows (the
		// re-key then fails). One that its connection abandoned is rolled
		// back first, so that it doesn't outlive the table (see rekey.go).
		if meta.RekeyTo != "" {
			s.recoverStale(ctx, meta.prefix())
		}
		// DD deletes every document the index knows about.
		if err := s.searchDo(ctx, meta.index(), "FT.DROPINDEX", meta.index(), "DD").Err(); err != nil &&
			!isUnknownIndex(err) {
			return wrapRedis(err, "failed to drop search index")
		}
		if testHookDropping != nil {
			testHookDropping()
		}
		err = s.client.Watch(ctx, func(tx *goredis.Tx) error {
			raw, err := tx.Get(ctx, key).Result()
			if err != nil && !errors.Is(err, goredis.Nil) {
				return err
			}
			if !sameKeys(meta, raw) {
				return goredis.TxFailedErr // read it again
			}
			_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				p.Del(ctx, key, seqKey(schema, table))
				p.SRem(ctx, tablesKey(schema), table)
				p.SRem(ctx, prefixesKey, meta.prefix())
				p.SRem(ctx, indexesKey, meta.index())
				if !isTempSchema(schema) {
					p.HIncrBy(ctx, releasedKey, meta.prefix(), 1) // see rekey.go
				}
				p.SRem(ctx, cleanupKey, cleanupMember(schema, table))
				return nil
			})
			return err
		}, key)
		if errors.Is(err, goredis.TxFailedErr) {
			continue
		}
		if err != nil {
			return wrapRedis(err, "failed to drop table")
		}
		s.trackTemp(schema, table, false)
		return nil
	}
	return errorf(adbc.StatusIO, "table %q.%q is being changed concurrently; try again", displaySchema(schema), table)
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
	n, err := once(ctx, s.client, "SADD", schemasKey, schema).Int64()
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
	if !sameKeys(meta, cur.Val()) {
		// The table was dropped, renamed or created again since meta was
		// read, so these ids are not from its sequence: write nothing.
		if err := s.checkKeys(ctx, meta, true); err != nil {
			return rowAlloc{}, err
		}
		return rowAlloc{}, writeMovedErr(meta)
	}
	return rowAlloc{first: last - int64(len(rows)) + 1, added: addedMissing(meta, cur.Val())}, nil
}

// sameKeys reports whether raw, the current metadata under meta's name, is
// still meta's table: it has the same key prefix and generation.
func sameKeys(meta *tableMeta, raw string) bool {
	if raw == "" {
		return false
	}
	// The common case, without decoding the metadata: it has meta's prefix
	// and, like meta, no generation. JSON escapes the quotes inside string
	// values, so neither field name can match inside one.
	if meta.KeyPrefix != "" && meta.PrefixGen == 0 && !strings.Contains(raw, `"prefix_gen":`) {
		if enc, err := json.Marshal(meta.KeyPrefix); err == nil && strings.Contains(raw, `"key_prefix":`+string(enc)) {
			return true
		}
	}
	var cur struct {
		KeyPrefix string `json:"key_prefix"`
		PrefixGen int64  `json:"prefix_gen"`
	}
	if json.Unmarshal([]byte(raw), &cur) != nil {
		return false
	}
	prefix := cur.KeyPrefix
	if prefix == "" {
		prefix = rowPrefix(meta.Schema, meta.Name)
	}
	return prefix == meta.prefix() && cur.PrefixGen == meta.PrefixGen
}

// testHookRowsWritten, when set by a test, runs after each pipeline of rows
// that writeRows sends, with the number of rows written so far.
var testHookRowsWritten func(meta *tableMeta, written int)

// writeRows writes rows allocated by allocRows as HASHes. A write of more
// than one pipeline checks between pipelines, once rekeyFence has passed
// since the table's keys were last checked, and always at the end, that the
// table still has its keys (checkKeys): if it was dropped or its rows moved
// meanwhile, the statement stops, deletes the rows it wrote, and fails.
func (s *store) writeRows(ctx context.Context, meta *tableMeta, a rowAlloc, rows [][]Value) (int64, error) {
	if err := s.raiseTagLevels(ctx, meta, rowTagLevels(meta, rows)); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	prefix := meta.prefix()
	meta.wrote.ranges = append(meta.wrote.ranges, [2]int64{a.first, int64(len(rows))})
	for start := 0; start < len(rows); start += pipelineChunk {
		if start > 0 {
			if err := s.checkWritten(ctx, meta); err != nil {
				return 0, err
			}
		}
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
		if testHookRowsWritten != nil {
			testHookRowsWritten(meta, end)
		}
	}
	check := s.checkWritten
	if len(rows) > pipelineChunk {
		check = func(ctx context.Context, meta *tableMeta) error { return s.checkKeys(ctx, meta, true) }
	}
	if err := check(ctx, meta); err != nil {
		return 0, err
	}
	return int64(len(rows)), nil
}

// rowLog lists the keys a statement wrote through a table's metadata: the
// row ids it inserted (first id, count) and the rows it set fields of.
type rowLog struct {
	ranges [][2]int64
	keys   []string
}

// discardWritten deletes the keys a statement wrote under a table's key
// prefix once no table has that prefix (checkKeys): its table was dropped
// or its rows moved while the statement was writing, so these keys either
// were deleted with the table or are left over, and no table can take the
// prefix again (see claimNames). It is best effort; see the README for
// keys left behind.
func (s *store) discardWritten(ctx context.Context, meta *tableMeta) {
	log := meta.wrote
	meta.wrote = rowLog{}
	prefix := meta.prefix()
	ctx = context.WithoutCancel(ctx)
	pipe := s.client.Pipeline()
	flush := func(force bool) bool {
		if pipe.Len() == 0 || (!force && pipe.Len() < pipelineChunk) {
			return true
		}
		_, err := pipe.Exec(ctx)
		pipe = s.client.Pipeline()
		return err == nil
	}
	for _, r := range log.ranges {
		for id := r[0]; id < r[0]+r[1]; id++ {
			pipe.Del(ctx, prefix+strconv.FormatInt(id, 10))
			if !flush(false) {
				return
			}
		}
	}
	for _, k := range log.keys {
		pipe.Del(ctx, k)
		if !flush(false) {
			return
		}
	}
	flush(true)
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
