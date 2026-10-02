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

// Checking an existing HASH collection
//
// InspectHashes reads a sample of the HASHes under a key prefix, guesses a
// table's columns from them (hashinfer.go), and checks what each way of
// reading them needs:
//
//   - Arrow IPC: ScanHashes reads the HASHes themselves, so only the keys
//     and their values matter.
//   - SQL by copying: ScanHashes piped into a bulk ingest makes a new
//     driver table. That needs what every table needs (the Query Engine, a
//     database that isn't Redis Flex), names that are free, and memory for
//     a second copy of the data.
//
// It writes nothing.

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

// HashInspectOptions selects the collection that InspectHashes checks.
type HashInspectOptions struct {
	// Prefix is the collection's key prefix, such as "user:". Without one,
	// InspectHashes lists candidate collections instead.
	Prefix string
	// Schema and Table name the driver table the collection would be copied
	// into. The defaults are the database's schema and a name made from the
	// prefix ("user:" gives "user").
	Schema, Table string
	// Sample is the number of HASHes read to guess the columns: 1000 if 0,
	// all of them if negative.
	Sample int
	// KeyColumn names the column that holds each HASH's key ("" for none).
	KeyColumn string
	// Types sets columns' types (column → SQL type, such as BIGINT).
	Types map[string]string
	// Renames gives HASH fields other column names (field → column).
	Renames map[string]string
}

// HashReport is what InspectHashes found.
type HashReport struct {
	Prefix  string       `json:"prefix,omitempty"`
	Server  HashServer   `json:"server"`
	Keys    int64        `json:"keys"`    // HASH keys under the prefix
	Sampled int          `json:"sampled"` // HASHes read to guess the columns
	Schema  string       `json:"schema,omitempty"`
	Table   string       `json:"table,omitempty"`
	Columns []HashColumn `json:"columns,omitempty"`
	// Skipped are fields that aren't columns, and why.
	Skipped []string `json:"skipped_fields,omitempty"`
	// Indexes are the search indexes whose prefixes cover the collection.
	Indexes []HashIndex `json:"indexes,omitempty"`
	// UnreadableIndexes counts the search indexes that the ACL user may not
	// read, which the report leaves out.
	UnreadableIndexes int `json:"unreadable_indexes,omitempty"`
	// DriverTable is the driver table whose rows are under the prefix, if
	// any.
	DriverTable string `json:"driver_table,omitempty"`
	// Candidates are the collections found when no prefix was given.
	Candidates []HashCollection `json:"candidates,omitempty"`
	Checks     []Checklist      `json:"checks,omitempty"`
}

// HashServer describes the server.
type HashServer struct {
	Version string `json:"version,omitempty"`
	Cluster bool   `json:"cluster"`
	// Search is whether the Query Engine (RediSearch) is available, and
	// SearchError why not.
	Search      bool   `json:"search"`
	SearchError string `json:"search_error,omitempty"`
	Flex        bool   `json:"flex"`
}

// HashColumn is a guessed column.
type HashColumn struct {
	Name string `json:"name"`
	// Field is the HASH field it reads; empty for the key column.
	Field string `json:"field,omitempty"`
	// Type is the column's SQL type, and ArrowType its type in Arrow IPC.
	Type      string `json:"type"`
	ArrowType string `json:"arrow_type"`
	// InPlaceType is its type in a table adopted in place.
	InPlaceType string `json:"in_place_type,omitempty"`
	// Present is how many sampled HASHes have the field.
	Present int      `json:"present"`
	Notes   []string `json:"notes,omitempty"`
}

// HashIndex is a search index that covers the collection.
type HashIndex struct {
	Name       string   `json:"name"`
	Prefixes   []string `json:"prefixes"`
	Filter     string   `json:"filter,omitempty"`
	Documents  int64    `json:"documents"`
	Failures   int64    `json:"indexing_failures"`
	Attributes []string `json:"attributes"`
}

// HashCollection is a candidate collection: HASH keys that share a prefix
// (up to the last ':'), or a search index's prefix.
type HashCollection struct {
	Prefix string `json:"prefix"`
	// Keys is how many HASHes of the sampled keyspace are under the prefix.
	Keys  int64  `json:"sampled_keys"`
	Index string `json:"index,omitempty"`
	// Table is the driver table that uses the prefix.
	Table string `json:"driver_table,omitempty"`
}

// Checklist is what one way of reading the collection needs.
type Checklist struct {
	Name  string      `json:"name"`
	Title string      `json:"title"`
	Ready bool        `json:"ready"`
	Items []CheckItem `json:"items"`
	// Command runs it, when it is ready.
	Command string `json:"command,omitempty"`
}

// CheckItem is one requirement and whether it is met.
type CheckItem struct {
	// Status is CheckOK, CheckWarn, CheckMissing (the command provides it)
	// or CheckBlocker (needs a change before the command can work).
	Status string `json:"status"`
	Text   string `json:"text"`
	Fix    string `json:"fix,omitempty"`
}

const (
	CheckOK      = "ok"
	CheckWarn    = "warn"
	CheckMissing = "missing"
	CheckBlocker = "blocker"
)

// Checklist names.
const (
	ChecklistArrowIPC = "arrow_ipc"
	ChecklistSQLCopy  = "sql_copy"
)

func (c *Checklist) add(status, text, fix string) {
	c.Items = append(c.Items, CheckItem{Status: status, Text: text, Fix: fix})
}

func (c *Checklist) finish(command string) {
	c.Ready = !slices.ContainsFunc(c.Items, func(i CheckItem) bool { return i.Status == CheckBlocker })
	if c.Ready {
		c.Command = command
	}
}

// defaultSample is the number of HASHes read to guess the columns.
const defaultSample = 1000

// discoverKeys is how many keys of the keyspace are sampled to list
// candidate collections.
const discoverKeys = 10000

// InspectHashes checks a HASH collection, or lists candidates if
// o.Prefix is empty. The options are the driver's database options (the
// URI, credentials, timeouts, adbc.redis.cluster, …). It writes nothing.
func InspectHashes(ctx context.Context, options map[string]string, o HashInspectOptions) (*HashReport, error) {
	c, err := dialHashes(ctx, options)
	if err != nil {
		return nil, err
	}
	defer c.close(ctx)
	r := &HashReport{Prefix: o.Prefix}
	c.server(ctx, &r.Server)
	tables, err := c.driverTables(ctx)
	if err != nil {
		return nil, err
	}
	var indexes []searchIndex
	if r.Server.Search {
		if indexes, r.UnreadableIndexes, err = c.searchIndexes(ctx); err != nil {
			return nil, err
		}
	}
	if o.Prefix == "" {
		r.Candidates, err = c.discover(ctx, tables, indexes)
		return r, err
	}

	for _, t := range tables {
		if t.prefix() == o.Prefix {
			r.DriverTable = displaySchema(t.Schema) + "." + t.Name
		}
	}
	for _, ix := range indexes {
		if ix.keyType != "HASH" || !slices.ContainsFunc(ix.prefixes, func(p string) bool { return prefixesOverlap(p, o.Prefix) }) {
			continue
		}
		hi := HashIndex{Name: ix.name, Prefixes: ix.prefixes, Filter: ix.filter, Documents: ix.numDocs, Failures: ix.failures}
		for _, a := range ix.attrs {
			hi.Attributes = append(hi.Attributes, strings.TrimSpace(a.name+" "+a.typ+" "+strings.Join(a.options, " ")))
		}
		r.Indexes = append(r.Indexes, hi)
	}

	// Every key, and the first Sample of them read.
	sample := o.Sample
	if sample == 0 {
		sample = defaultSample
	}
	var sampled []string
	inSample := map[string]bool{}
	ks, err := c.scanKeys(ctx, o.Prefix, "hash")
	if err != nil {
		return nil, err
	}
	var stats keyStats
	for {
		keys, done, err := ks.next(ctx)
		if err != nil {
			return nil, err
		}
		r.Keys += int64(len(keys))
		ids, err := c.rowIDs(ctx, keys)
		if err != nil {
			return nil, err
		}
		for i, k := range keys {
			v := ids[i]
			stats.add(k, o.Prefix, deref(v), v != nil)
		}
		for _, k := range keys {
			if (sample < 0 || len(sampled) < sample) && !inSample[k] {
				inSample[k] = true
				sampled = append(sampled, k)
			}
		}
		if done {
			break
		}
	}
	r.Keys = max(r.Keys, int64(len(sampled)))
	rows, err := c.readHashes(ctx, sampled)
	if err != nil {
		return nil, err
	}
	r.Sampled = len(rows)
	gopts := guessOptions{keyColumn: o.KeyColumn, types: o.Types, renames: o.Renames}
	gs, err := newGuesserFor(gopts)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		gs.add(row)
	}
	cols, skipped, err := gs.columns(gopts)
	if err != nil {
		return nil, err
	}
	r.Skipped = skipped
	for _, col := range cols {
		hc := HashColumn{Name: col.name, Field: col.field, Type: col.t.SQLName(), ArrowType: col.t.ArrowType().String(), Notes: col.notes}
		if col.field != "" {
			hc.InPlaceType = col.inPlace.SQLName()
		}
		if col.guess != nil {
			hc.Present = col.guess.present
		} else {
			hc.Present = len(rows)
		}
		r.Columns = append(r.Columns, hc)
	}

	r.Schema, r.Table = o.Schema, o.Table
	if r.Schema == "" {
		r.Schema = c.db.schema
	}
	if r.Table == "" {
		r.Table = TableNameForPrefix(o.Prefix)
	}
	scanCmd := scanCommand(o)
	ipc := c.checkIPC(r, cols, rows, o, scanCmd)
	cp, err := c.checkCopy(ctx, r, cols, sampled, o, scanCmd)
	if err != nil {
		return nil, err
	}
	t := inPlaceTarget{prefix: o.Prefix, schema: r.Schema, table: r.Table, sampled: r.Sampled,
		command: adoptCommand(o.Prefix, r.Schema, r.Table, c.db.schema, o.Types, o.Renames)}
	for _, tm := range tables {
		if tm.Adopted && tm.prefix() == o.Prefix {
			t.existing = tm
			t.command = adoptCommand(o.Prefix, tm.Schema, tm.Name, c.db.schema, nil, nil)
		}
	}
	ip, err := c.checkInPlace(ctx, r, cols, &stats, tables, indexes, t)
	if err != nil {
		return nil, err
	}
	r.Checks = []Checklist{ipc, cp, ip}
	return r, nil
}

// server fills in what the server is and offers.
func (c *hashConn) server(ctx context.Context, s *HashServer) {
	s.Cluster = c.cluster()
	if info, err := c.client.Info(ctx, "server").Result(); err == nil {
		s.Version = infoField(info, "redis_version")
	}
	if err := c.st.searchDo(ctx, "", "FT._LIST").Err(); err != nil {
		s.SearchError = err.Error()
		return
	}
	s.Search = true
	s.Flex = c.st.refuseFlex(ctx) != nil
}

// infoField returns a field of an INFO reply.
func infoField(info, name string) string {
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), name+":"); ok {
			return v
		}
	}
	return ""
}

var nonNameChars = regexp.MustCompile(`[^A-Za-z0-9_]+`)

// TableNameForPrefix is the table name that a collection's key prefix
// suggests: "user:" gives "user" and "app:users:" gives "app_users".
func TableNameForPrefix(prefix string) string {
	name := strings.Trim(nonNameChars.ReplaceAllString(prefix, "_"), "_")
	if name == "" {
		return "hashes"
	}
	return name
}

// discover lists candidate collections: the prefixes of sampled HASH keys,
// and those of HASH search indexes.
func (c *hashConn) discover(ctx context.Context, tables []*tableMeta, indexes []searchIndex) ([]HashCollection, error) {
	byPrefix := map[string]*HashCollection{}
	get := func(p string) *HashCollection {
		hc := byPrefix[p]
		if hc == nil {
			hc = &HashCollection{Prefix: p}
			byPrefix[p] = hc
		}
		return hc
	}
	ks, err := c.scanKeys(ctx, "", "hash")
	if err != nil {
		return nil, err
	}
	seen := 0
	for seen < discoverKeys {
		keys, done, err := ks.next(ctx)
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			// The driver's metadata, and temporary tables' rows.
			if p := collectionPrefix(k); p != "" && !strings.HasPrefix(k, metaPrefix) && !strings.HasPrefix(k, tempSchemaBase) {
				get(p).Keys++
			}
		}
		seen += len(keys)
		if done {
			break
		}
	}
	for _, ix := range indexes {
		if ix.keyType != "HASH" {
			continue
		}
		for _, p := range ix.prefixes {
			if p != "" {
				get(p).Index = ix.name
			}
		}
	}
	for _, t := range tables {
		// A table's rows are under its prefix and also look like a
		// collection one level up ("public:"); show them as the table.
		p := t.prefix()
		if hc, ok := byPrefix[p]; ok {
			hc.Table = displaySchema(t.Schema) + "." + t.Name
		}
	}
	out := make([]HashCollection, 0, len(byPrefix))
	for _, hc := range byPrefix {
		out = append(out, *hc)
	}
	sort.Slice(out, func(i, j int) bool {
		if (out[i].Table == "") != (out[j].Table == "") {
			return out[i].Table == ""
		}
		if out[i].Keys != out[j].Keys {
			return out[i].Keys > out[j].Keys
		}
		return out[i].Prefix < out[j].Prefix
	})
	return out, nil
}

// checkIPC is the Arrow IPC checklist: ScanHashes needs only the HASHes.
func (c *hashConn) checkIPC(r *HashReport, cols []*guessedColumn, rows []hashRow, o HashInspectOptions, scanCmd string) Checklist {
	cl := Checklist{Name: ChecklistArrowIPC, Title: "Arrow IPC (redis-arrow scan)"}
	c.checkKeys(&cl, r, o)
	c.checkColumns(&cl, r, cols, rows)
	cl.finish(scanCmd + " -o " + shellQuote(r.Table+".arrow"))
	return cl
}

// checkKeys checks that there are HASHes to read, and how many were read.
func (c *hashConn) checkKeys(cl *Checklist, r *HashReport, o HashInspectOptions) {
	if r.Keys == 0 {
		cl.add(CheckBlocker, fmt.Sprintf("no HASH keys under %s", shellQuote(o.Prefix)),
			"check the prefix: redis-arrow check without -prefix lists the collections it finds")
		return
	}
	cl.add(CheckOK, fmt.Sprintf("%s under %s", plural(r.Keys, "HASH key"), shellQuote(o.Prefix)), "")
	if int64(r.Sampled) < r.Keys {
		cl.add(CheckWarn, fmt.Sprintf("the columns are guessed from %d of the %d HASHes", r.Sampled, r.Keys),
			"scan reads them all, and names any field these don't have; -sample -1 guesses from all of them")
	} else {
		cl.add(CheckOK, fmt.Sprintf("the columns are guessed from all %s", plural(int64(r.Sampled), "HASH", "HASHes")), "")
	}
}

// checkColumns reports what the guessed columns can't hold exactly.
func (c *hashConn) checkColumns(cl *Checklist, r *HashReport, cols []*guessedColumn, rows []hashRow) {
	if r.Sampled == 0 {
		return
	}
	cl.add(CheckOK, fmt.Sprintf("every field has a column type (%s)", plural(int64(len(cols)), "column")), "")
	for _, col := range cols {
		if col.outliers > 0 {
			cl.add(CheckWarn, fmt.Sprintf("%s is %s: %s", col.name, col.t.SQLName(), col.outlierNote),
				fmt.Sprintf("-type %s=%s -on-error null reads them as NULL", col.name, typeFlagText(col.suggest)))
		}
		if col.override == "" || col.field == "" {
			continue
		}
		bad, ex := 0, example{}
		for _, row := range rows {
			v, ok := row.get(col.field)
			if !ok {
				continue
			}
			if _, err := convertValue(v, col.t); err != nil {
				if bad == 0 {
					ex = example{row.key, v}
				}
				bad++
			}
		}
		if bad > 0 {
			cl.add(CheckWarn, fmt.Sprintf("-type %s=%s: %d sampled values don't convert (such as %s in %s)",
				col.name, col.override, bad, quoteValue(ex.value), ex.key),
				"scan fails on them; -on-error null reads them as NULL")
		}
	}
	for _, s := range r.Skipped {
		if !strings.HasPrefix(s, rowIDField+":") {
			cl.add(CheckWarn, "not a column: "+s, "")
		}
	}
}

// typeFlagText writes a type for a -type flag.
func typeFlagText(t ColType) string {
	if t.Kind == KindFloat64 {
		return "DOUBLE"
	}
	return strings.ReplaceAll(t.SQLName(), " ", "")
}

// checkCopy is the checklist for copying the collection into a new driver
// table with ScanHashes and bulk ingest.
func (c *hashConn) checkCopy(ctx context.Context, r *HashReport, cols []*guessedColumn, sampled []string, o HashInspectOptions, scanCmd string) (Checklist, error) {
	cl := Checklist{Name: ChecklistSQLCopy, Title: "SQL, copied into a driver table (redis-arrow scan | redis-arrow import)"}
	c.checkServer(&cl, r)
	if r.DriverTable != "" {
		cl.add(CheckWarn, fmt.Sprintf("%s holds the rows of the driver table %s, which SQL already reads",
			shellQuote(o.Prefix), r.DriverTable), "a copy duplicates it")
	}
	if r.Keys == 0 {
		cl.add(CheckBlocker, fmt.Sprintf("no HASH keys under %s", shellQuote(o.Prefix)), "")
	}
	target := displaySchema(r.Schema) + "." + r.Table
	importCmd := "redis-arrow import -table " + shellQuote(r.Table)
	if r.Schema != c.db.schema {
		importCmd += " -schema " + shellQuote(r.Schema)
	}
	if isTempSchema(r.Schema) || isTempAlias(r.Schema) {
		cl.add(CheckBlocker, fmt.Sprintf("%s is a temporary schema, dropped when the import's connection closes", r.Schema),
			"pick a permanent schema with -schema")
	}
	view, err := c.st.viewExists(ctx, r.Schema, r.Table)
	if err != nil {
		return cl, err
	}
	table, err := c.st.tableExists(ctx, r.Schema, r.Table)
	if err != nil {
		return cl, err
	}
	switch {
	case view:
		cl.add(CheckBlocker, fmt.Sprintf("%s is a view", target), "pick another table name with -table")
	case table:
		cl.add(CheckWarn, fmt.Sprintf("the table %s exists", target),
			"import -mode replace drops it and creates it again; -mode append adds the rows to it; or pick another name with -table")
	default:
		cl.add(CheckOK, fmt.Sprintf("%s doesn't exist yet: import creates it", target), "")
		// Without search there are no index names to check.
		if r.Server.Search {
			if err := c.checkNames(ctx, &cl, r, o); err != nil {
				return cl, err
			}
		}
	}
	checkColumnNames(&cl, cols)
	if err := c.checkMemory(ctx, &cl, r, sampled); err != nil {
		return cl, err
	}
	cl.finish(scanCmd + " | " + importCmd)
	return cl, nil
}

// checkServer checks for the Query Engine, and that the database isn't
// Redis Flex.
func (c *hashConn) checkServer(cl *Checklist, r *HashReport) {
	if !r.Server.Search {
		cl.add(CheckBlocker, "the server has no Query Engine (RediSearch): "+r.Server.SearchError,
			"use Redis 8 (it is built in), Redis Stack, or a Redis Cloud / Redis Software database with Search")
		return
	}
	cl.add(CheckOK, "the Query Engine (RediSearch) is available", "")
	if r.Server.Flex {
		cl.add(CheckBlocker, "this is a Redis Flex database, whose search has no FT.AGGREGATE or NUMERIC fields",
			"use a database that isn't Flex")
	}
}

// checkNames checks the key prefix and index name a new table would take
// (as claimNames picks them): keys already under that prefix would merge
// with the table's rows.
func (c *hashConn) checkNames(ctx context.Context, cl *Checklist, r *HashReport, o HashInspectOptions) error {
	nm, foreign, err := c.nextNames(ctx, r.Schema, r.Table)
	if err != nil {
		return err
	}
	if prefixesOverlap(nm.prefix, o.Prefix) {
		cl.add(CheckBlocker, fmt.Sprintf("the table's rows would be written under %s, among the collection's own keys", shellQuote(nm.prefix)),
			"pick another table name with -table")
		return nil
	}
	n, example, err := c.countKeys(ctx, nm.prefix)
	if err != nil {
		return err
	}
	if n > 0 {
		cl.add(CheckBlocker, fmt.Sprintf("%s already under %s (such as %s), where the table's rows would go and merge with them",
			plural(n, "key is", "keys are"), shellQuote(nm.prefix), example),
			"pick another table name with -table, or delete those keys")
		return nil
	}
	cl.add(CheckOK, fmt.Sprintf("its rows go under %s, which has no keys, and its index is %s", shellQuote(nm.prefix), nm.index), "")
	for _, f := range foreign {
		cl.add(CheckOK, fmt.Sprintf("the index %s isn't the driver's, so the table doesn't take its name (and leaves it alone)", f), "")
	}
	return nil
}

// nextNames is the names claimNames would give a new table, found without
// reserving them, and the indexes it would skip because they aren't the
// driver's.
func (c *hashConn) nextNames(ctx context.Context, schema, table string) (tableNames, []string, error) {
	last, err := c.st.lastNames(ctx, c.client, schema, table)
	if err != nil {
		return tableNames{}, nil, wrapRedis(err, "failed to read the table names counter")
	}
	var skipped []string
	for n := last + 1; n < last+10000; n++ {
		nm := namesFor(schema, table, n)
		pipe := c.client.Pipeline()
		usedP := pipe.SIsMember(ctx, prefixesKey, nm.prefix)
		usedI := pipe.SIsMember(ctx, indexesKey, nm.index)
		released := pipe.HExists(ctx, releasedKey, nm.prefix)
		if _, err := pipe.Exec(ctx); err != nil {
			return tableNames{}, nil, wrapRedis(err, "failed to read the table registry")
		}
		if usedP.Val() || usedI.Val() || released.Val() {
			continue
		}
		foreign, err := c.st.foreignIndex(ctx, nm)
		if err != nil {
			return tableNames{}, nil, err
		}
		if !foreign {
			return nm, skipped, nil
		}
		skipped = append(skipped, nm.index)
	}
	return tableNames{}, nil, errorf(adbc.StatusInternal, "could not find a free key prefix for %q", rowPrefix(schema, table))
}

// countKeys counts the keys of any type under a prefix, and returns one.
func (c *hashConn) countKeys(ctx context.Context, prefix string) (int64, string, error) {
	ks, err := c.scanKeys(ctx, prefix, "")
	if err != nil {
		return 0, "", err
	}
	var n int64
	var first string
	for {
		keys, done, err := ks.next(ctx)
		if err != nil {
			return 0, "", err
		}
		if first == "" && len(keys) > 0 {
			first = keys[0]
		}
		n += int64(len(keys))
		if done {
			return n, first, nil
		}
	}
}

// checkColumnNames checks which columns a table can index.
func checkColumnNames(cl *Checklist, cols []*guessedColumn) {
	n := 0
	for _, col := range cols {
		if !indexable(col.t) {
			continue
		}
		if !simpleName(col.name) {
			cl.add(CheckWarn, fmt.Sprintf("column %q isn't indexed: only names of letters, digits and _ are", col.name),
				fmt.Sprintf("filters on it read every row; -rename %s=<name> indexes it", col.fieldOrName()))
			continue
		}
		n++
	}
	if n > maxIndexed {
		cl.add(CheckWarn, fmt.Sprintf("%d columns can be indexed, but a table indexes at most %d", n, maxIndexed),
			"import -index-columns picks the ones to index")
	}
}

// checkMemory estimates whether there is room for a copy: about as much
// memory as the HASHes use (MEMORY USAGE of the sampled ones), plus an
// index.
func (c *hashConn) checkMemory(ctx context.Context, cl *Checklist, r *HashReport, sampled []string) error {
	if len(sampled) == 0 {
		return nil
	}
	pipe := c.client.Pipeline()
	cmds := make([]*goredis.IntCmd, len(sampled))
	for i, k := range sampled {
		cmds[i] = pipe.MemoryUsage(ctx, k)
	}
	_, _ = pipe.Exec(ctx)
	var total, n int64
	for _, cmd := range cmds {
		v, err := cmd.Result()
		if err == goredis.Nil {
			continue
		}
		if err != nil {
			cl.add(CheckWarn, "couldn't estimate the memory a copy needs: MEMORY USAGE failed: "+err.Error(), "")
			return nil
		}
		total += v
		n++
	}
	if n == 0 {
		return nil
	}
	need := total / n * r.Keys
	info, err := c.client.Info(ctx, "memory").Result()
	if err != nil {
		cl.add(CheckWarn, "couldn't read the memory limit: INFO memory failed: "+err.Error(), "")
		return nil
	}
	var used, limit int64
	if c.cluster() {
		// Sum over the primaries: each holds part of the data.
		cc := c.client.(*goredis.ClusterClient)
		var mu sync.Mutex
		unlimited := false
		err := cc.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			info, err := node.Info(ctx, "memory").Result()
			if err != nil {
				return err
			}
			u, _ := strconv.ParseInt(infoField(info, "used_memory"), 10, 64)
			l, _ := strconv.ParseInt(infoField(info, "maxmemory"), 10, 64)
			mu.Lock()
			defer mu.Unlock()
			used += u
			limit += l
			unlimited = unlimited || l == 0
			return nil
		})
		if unlimited {
			limit = 0
		}
		if err != nil {
			cl.add(CheckWarn, "couldn't read the memory limit: INFO memory failed: "+err.Error(), "")
			return nil
		}
	} else {
		used, _ = strconv.ParseInt(infoField(info, "used_memory"), 10, 64)
		limit, _ = strconv.ParseInt(infoField(info, "maxmemory"), 10, 64)
	}
	policy := infoField(info, "maxmemory_policy")
	est := fmt.Sprintf("a copy needs about %s (MEMORY USAGE of the sampled HASHes, scaled to all of them), plus its index", mb(need))
	switch {
	case limit == 0:
		cl.add(CheckOK, fmt.Sprintf("%s; %s are in use, and there is no maxmemory limit", est, mb(used)), "")
	case used+need <= limit*9/10:
		cl.add(CheckOK, fmt.Sprintf("%s; %s of the %s maxmemory are in use", est, mb(used), mb(limit)), "")
	case strings.HasPrefix(policy, "allkeys-"):
		cl.add(CheckBlocker, fmt.Sprintf("%s, but only %s of the %s maxmemory are free, and maxmemory-policy %s evicts any key: the originals too",
			est, mb(max(0, limit-used)), mb(limit), policy),
			"raise maxmemory, or copy part of the collection")
	case used+need > limit:
		cl.add(CheckBlocker, fmt.Sprintf("%s, but only %s of the %s maxmemory are free (maxmemory-policy %s)",
			est, mb(max(0, limit-used)), mb(limit), policy),
			"raise maxmemory, or copy part of the collection")
	default:
		cl.add(CheckWarn, fmt.Sprintf("%s, and %s of the %s maxmemory are free", est, mb(max(0, limit-used)), mb(limit)), "")
	}
	return nil
}

// mb renders a byte count in MB.
func mb(n int64) string {
	switch {
	case n >= 10<<20:
		return fmt.Sprintf("%d MB", n>>20)
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	}
	return fmt.Sprintf("%.2f MB", float64(n)/(1<<20))
}

// plural renders "1 thing" or "n things" (with n's thousands separated);
// forms gives the singular and plural when they aren't thing and things.
func plural(n int64, forms ...string) string {
	one, many := forms[0], forms[0]+"s"
	if len(forms) > 1 {
		many = forms[1]
	}
	if n == 1 {
		return "1 " + one
	}
	return groupDigits(n) + " " + many
}

func groupDigits(n int64) string {
	s := strconv.FormatInt(n, 10)
	if n < 0 || len(s) <= 4 {
		return s
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

var shellSafe = regexp.MustCompile(`^[A-Za-z0-9_./:=,@%+-]+$`)

// shellQuote quotes a value for a command line.
func shellQuote(s string) string {
	if shellSafe.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// adoptCommand is the redis-arrow adopt command line.
func adoptCommand(prefix, schema, table, defaultSchema string, types, renames map[string]string) string {
	args := []string{"redis-arrow", "adopt", "-prefix", shellQuote(prefix), "-table", shellQuote(table)}
	if schema != defaultSchema {
		args = append(args, "-schema", shellQuote(schema))
	}
	for _, k := range sortedKeys(types) {
		args = append(args, "-type", shellQuote(k+"="+types[k]))
	}
	for _, k := range sortedKeys(renames) {
		args = append(args, "-rename", shellQuote(k+"="+renames[k]))
	}
	return strings.Join(args, " ")
}

// scanCommand is the redis-arrow scan command line for the options.
func scanCommand(o HashInspectOptions) string {
	args := []string{"redis-arrow", "scan", "-prefix", shellQuote(o.Prefix)}
	if o.KeyColumn != "_key" {
		args = append(args, "-key-column", shellQuote(o.KeyColumn))
	}
	for _, k := range sortedKeys(o.Types) {
		args = append(args, "-type", shellQuote(k+"="+o.Types[k]))
	}
	for _, k := range sortedKeys(o.Renames) {
		args = append(args, "-rename", shellQuote(k+"="+o.Renames[k]))
	}
	return strings.Join(args, " ")
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
