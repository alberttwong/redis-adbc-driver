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

// Existing HASH collections
//
// InspectHashes (hashcheck.go) and ScanHashes (hashexport.go) work on HASHes
// that something other than the driver wrote, such as an application's
// user:1, user:2, …. They find the keys with SCAN, read them with HGETALL
// and write nothing: they connect as the driver does (dial), without the
// metadata upkeep that opening a connection does.

import (
	"context"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// scanCount is the COUNT of each SCAN call.
const scanCount = 1000

// hashConn is a read-only connection for the HASH tools.
type hashConn struct {
	db     *databaseImpl
	client goredis.UniversalClient
	st     *store
}

// dialHashes connects with the driver's database options (URI, credentials,
// timeouts, cluster), as Open does, but writes nothing.
func dialHashes(ctx context.Context, options map[string]string) (*hashConn, error) {
	db, err := newDriverImpl(memory.DefaultAllocator).newDatabaseImpl(ctx, options)
	if err != nil {
		return nil, err
	}
	client, _, _, _, err := db.dial(ctx)
	if err != nil {
		_ = db.Base().Close(ctx)
		return nil, err
	}
	return &hashConn{db: db, client: client, st: &store{client: client}}, nil
}

func (c *hashConn) close(ctx context.Context) {
	_ = c.client.Close()
	_ = c.db.Base().Close(ctx)
}

// cluster reports whether the connection is to a Redis Cluster.
func (c *hashConn) cluster() bool {
	_, ok := c.client.(*goredis.ClusterClient)
	return ok
}

// globEscape escapes the characters that SCAN MATCH reads as a pattern.
func globEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// keyScan iterates over the keys under a prefix, page by page, on every
// primary of a cluster in turn. SCAN may return a key more than once.
type keyScan struct {
	nodes  []goredis.Cmdable
	match  string
	typ    string // "hash", or "" for keys of every type
	node   int
	cursor uint64
}

func (c *hashConn) scanKeys(ctx context.Context, prefix, typ string) (*keyScan, error) {
	ks := &keyScan{match: globEscape(prefix) + "*", typ: typ}
	cc, ok := c.client.(*goredis.ClusterClient)
	if !ok {
		ks.nodes = []goredis.Cmdable{c.client}
		return ks, nil
	}
	var mu sync.Mutex
	var addrs []string
	byAddr := map[string]goredis.Cmdable{}
	if err := cc.ForEachMaster(ctx, func(_ context.Context, n *goredis.Client) error {
		mu.Lock()
		defer mu.Unlock()
		addrs = append(addrs, n.Options().Addr)
		byAddr[n.Options().Addr] = n
		return nil
	}); err != nil {
		return nil, wrapRedis(err, "failed to list the cluster's primaries")
	}
	sort.Strings(addrs)
	for _, a := range addrs {
		ks.nodes = append(ks.nodes, byAddr[a])
	}
	return ks, nil
}

// next returns the next page of keys, which may be empty; done is true once
// every node has been scanned.
func (ks *keyScan) next(ctx context.Context) (keys []string, done bool, err error) {
	if ks.node >= len(ks.nodes) {
		return nil, true, nil
	}
	n := ks.nodes[ks.node]
	var cursor uint64
	if ks.typ != "" {
		keys, cursor, err = n.ScanType(ctx, ks.cursor, ks.match, scanCount, ks.typ).Result()
	} else {
		keys, cursor, err = n.Scan(ctx, ks.cursor, ks.match, scanCount).Result()
	}
	if err != nil {
		return nil, false, wrapRedis(err, "SCAN failed")
	}
	ks.cursor = cursor
	if cursor == 0 {
		ks.node++
	}
	return keys, ks.node >= len(ks.nodes), nil
}

// hashRow is one HASH: its fields and values, in HGETALL's order.
type hashRow struct {
	key    string
	fields []string
	values []string
}

func (r hashRow) get(field string) (string, bool) {
	if i := slices.Index(r.fields, field); i >= 0 {
		return r.values[i], true
	}
	return "", false
}

// readHashes reads HASHes with pipelined HGETALL. A key that is gone (or
// no longer a HASH) is left out.
func (c *hashConn) readHashes(ctx context.Context, keys []string) ([]hashRow, error) {
	rows := make([]hashRow, 0, len(keys))
	for start := 0; start < len(keys); start += pipelineChunk {
		chunk := keys[start:min(start+pipelineChunk, len(keys))]
		pipe := c.client.Pipeline()
		cmds := make([]*goredis.Cmd, len(chunk))
		for i, k := range chunk {
			cmds[i] = pipe.Do(ctx, "HGETALL", k)
		}
		// Each command's error is checked below.
		_, _ = pipe.Exec(ctx)
		for i, cmd := range cmds {
			reply, err := cmd.Result()
			if err != nil {
				if isWrongType(err) {
					continue
				}
				return nil, wrapRedis(err, "HGETALL failed")
			}
			list, _ := reply.([]any)
			if len(list) == 0 {
				continue
			}
			row := hashRow{key: chunk[i], fields: make([]string, 0, len(list)/2), values: make([]string, 0, len(list)/2)}
			for j := 0; j+1 < len(list); j += 2 {
				f, _ := list[j].(string)
				v, _ := list[j+1].(string)
				row.fields = append(row.fields, f)
				row.values = append(row.values, v)
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// isWrongType reports a WRONGTYPE error: a key that changed type since SCAN
// listed it.
func isWrongType(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "WRONGTYPE")
}

// driverTables reads every permanent table's metadata, without the registry
// (which the database may not have yet).
func (c *hashConn) driverTables(ctx context.Context) ([]*tableMeta, error) {
	schemas, err := c.st.listSchemas(ctx)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(schemas, defaultSchema) {
		schemas = append(schemas, defaultSchema)
	}
	var out []*tableMeta
	for _, schema := range schemas {
		tables, err := c.st.listTables(ctx, schema)
		if err != nil {
			return nil, err
		}
		for _, t := range tables {
			meta, err := c.st.getTable(ctx, schema, t)
			if err != nil {
				var ae adbc.Error
				if asAdbc(err, &ae) && ae.Code == adbc.StatusNotFound {
					continue // dropped meanwhile
				}
				return nil, err
			}
			out = append(out, meta)
		}
	}
	return out, nil
}

// searchIndex is a search index's definition and size, from FT.INFO.
type searchIndex struct {
	name string
	indexInfo
}

// searchIndexes lists the server's search indexes, and counts those that
// the ACL user may not read (Redis refuses FT.INFO on an index whose key
// prefixes the user can't read).
func (c *hashConn) searchIndexes(ctx context.Context) (out []searchIndex, unreadable int, err error) {
	reply, err := c.st.searchDo(ctx, "", "FT._LIST").Result()
	if err != nil {
		return nil, 0, wrapRedis(err, "FT._LIST failed")
	}
	names, _ := reply.([]any)
	for _, n := range names {
		name, _ := n.(string)
		info, err := c.st.searchDo(ctx, name, "FT.INFO", name).Result()
		if err != nil {
			switch {
			case isUnknownIndex(err): // dropped meanwhile
				continue
			case strings.HasPrefix(err.Error(), "NOPERM"):
				unreadable++
				continue
			}
			return nil, 0, wrapRedis(err, "FT.INFO "+name+" failed")
		}
		out = append(out, searchIndex{name: name, indexInfo: indexInfoOf(info)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, unreadable, nil
}

// prefixesOverlap reports whether keys can be under both prefixes.
func prefixesOverlap(a, b string) bool {
	return strings.HasPrefix(a, b) || strings.HasPrefix(b, a)
}

// collectionPrefix is the part of a key up to and including its last ':'
// ("" for a key without one).
func collectionPrefix(key string) string {
	if i := strings.LastIndexByte(key, ':'); i >= 0 {
		return key[:i+1]
	}
	return ""
}
