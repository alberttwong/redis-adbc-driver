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

// String values in the index.
//
// An indexed string column is a TAG attribute (see indexAttrArgs), which
// doesn't hold every value as it is. The tags of a value are what is left
// once RediSearch has
//
//   - cut the value at its first NUL byte,
//   - split it at each separator (tagSeparator),
//   - trimmed ASCII whitespace (space, \t, \n, \v, \f, \r) from both ends of
//     each piece, and
//   - cut each piece to its first maxTagBytes bytes.
//
// So 'ab', 'ab ', ' ab' and 'ab\n' all have the one tag 'ab', which @c:{ab}
// finds, and ' ' has the tag ''. Other characters, Unicode spaces included,
// are kept. A TAG query looks its tag up as written: @c:{ab\ } finds nothing.
// The sorting vector, which index sorts, GROUPBY and LOAD @c read, holds the
// whole value, except that it too stops at a NUL byte.
//
// A value is held exactly when it is its own only tag. Each indexed string
// column records how exactly the index holds its values (TagValues): all of
// them exactly (""), not all as tags (tagsNormalized), or not all in the
// sorting vector either (tagsTruncated: a value with a NUL byte). Statements
// that write values (INSERT, UPDATE, MERGE, bulk ingest, ADD COLUMN …
// DEFAULT) raise the level the values need in a metadata transaction that
// commits before the rows are written, and the level never goes down, not
// even when the rows are deleted. Then:
//
//   - On a column held exactly, `c = 'v'` is the TAG query @c:{v}, which
//     answers it exactly, so COUNT, aggregates and LIMIT can still run in
//     the index.
//   - Otherwise, or if 'v' isn't held exactly itself, the query looks up the
//     tag of 'v' (@c:{ab} for 'ab '), which finds every row that may equal
//     it, and the predicate is re-checked on the rows. IN lists, index
//     lookup joins and LIKE prefixes look tags up the same way; they are
//     re-checked anyway.
//   - On a truncated column the index doesn't sort, group or load the
//     column either: it is read from the HASHes.
//   - The query syntax has no way to write ASCII control characters other
//     than whitespace, so a value with one isn't looked up in the index.
//
// Tables created before the levels were recorded have columns without
// TagsChecked, which count as truncated until checked: the first statement
// that reads such a table on a connection starts a background task that
// reads every row and records the levels. It records them only if the
// table's metadata hasn't changed since it started reading, so a writer
// raising a level meanwhile makes it start over. A connection whose check
// fails waits tagCheckRetry before it tries again.
//
// Prefix queries: RediSearch expands @c:{ab*} into at most
// MAXPREFIXEXPANSIONS tags (search-max-prefix-expansions, 200 by default;
// on a cluster, per shard) and ignores the others without an error.
// FT.PROFILE reports it for each shard, so before pushing a LIKE prefix down
// the driver profiles the prefix query (reading one row) and checks the
// prefix itself on fetched rows instead unless every shard's profile says
// there was no warning: one extra round trip per LIKE prefix per statement.
// The server's configuration is left alone.
//
// Concurrency: a statement that read a table's metadata before another
// connection raised a column's level, and that queries the index after that
// connection's rows are indexed, may take their values for what the index
// holds: a concurrently inserted 'ab ' may count as 'ab', and one with a NUL
// byte may be sorted, grouped or read up to that byte. As with any
// concurrent write, the statement may or may not see the row at all.
// Likewise, rows inserted between the profile of a prefix query and the
// query itself may make it reach the expansion limit. Older versions of the
// driver don't raise levels when they write.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

// maxTagBytes is the length RediSearch cuts tags to.
const maxTagBytes = 4096

// tagSpace is the whitespace RediSearch trims from tags (C isspace).
const tagSpace = " \t\n\v\f\r"

// Levels of columnMeta.TagValues; "" is the exact one.
const (
	tagsNormalized = "normalized" // some value is not its only tag
	tagsTruncated  = "truncated"  // some value has a NUL byte
)

// tagRank orders the levels. A level it doesn't know counts as truncated.
func tagRank(level string) int {
	switch level {
	case "":
		return 0
	case tagsNormalized:
		return 1
	}
	return 2
}

// tagLevelOf returns the level a stored string value needs.
func tagLevelOf(v string) string {
	switch {
	case strings.IndexByte(v, 0) >= 0:
		return tagsTruncated
	case len(v) > maxTagBytes || strings.Contains(v, tagSeparator),
		v != "" && (strings.IndexByte(tagSpace, v[0]) >= 0 || strings.IndexByte(tagSpace, v[len(v)-1]) >= 0):
		return tagsNormalized
	}
	return ""
}

// valueTags returns the tags the index holds for a string value.
func valueTags(v string) []string {
	if i := strings.IndexByte(v, 0); i >= 0 {
		v = v[:i]
	}
	tags := strings.Split(v, tagSeparator)
	for i, t := range tags {
		t = strings.Trim(t, tagSpace)
		if len(t) > maxTagBytes {
			t = t[:maxTagBytes]
		}
		tags[i] = t
	}
	return tags
}

// tagQueryable reports whether a TAG query can look a tag up: it can't
// contain ASCII control characters other than whitespace.
func tagQueryable(tag string) bool {
	for i := 0; i < len(tag); i++ {
		if c := tag[i]; (c < ' ' || c == 0x7f) && strings.IndexByte(tagSpace, c) < 0 {
			return false
		}
	}
	return true
}

// tagLookup returns the tag, escaped, that a TAG query on an indexed string
// column looks up to find the rows whose value is v: the tag the index holds
// for v (the longest if it has several). exact reports whether that finds
// just those rows; ok is false if the tag can't be written in a query.
func tagLookup(col columnMeta, v string) (lit string, exact, ok bool) {
	tag := ""
	for _, t := range valueTags(v) {
		if tagQueryable(t) && (!ok || len(t) > len(tag)) {
			tag, ok = t, true
		}
	}
	return escapeTag(tag), ok && col.tagsExact() && tagLevelOf(v) == "", ok
}

// prefixLookup returns the tag prefix, escaped, that a TAG prefix query
// looks up to find the values starting with prefix (among others, on a
// column not held exactly). The tags of a value starting with prefix start
// with the prefix's own tag. RediSearch drops backslashes at the end of a
// prefix (@c:{ab\\*} is @c:{ab*}), so they are left out. ok is false if
// there is no single tag of at least two characters (as RediSearch
// requires) that a query can write.
func prefixLookup(prefix string) (lit string, ok bool) {
	tags := valueTags(prefix)
	if len(tags) != 1 {
		return "", false
	}
	tag := strings.TrimRight(tags[0], `\`)
	if utf8.RuneCountInString(tag) < 2 || !tagQueryable(tag) {
		return "", false
	}
	return escapeTag(tag), true
}

// tagLevel returns how exactly the index holds the values of an indexed
// string column: truncated until the column has been checked.
func (c columnMeta) tagLevel() string {
	if !c.TagsChecked {
		return tagsTruncated
	}
	return c.TagValues
}

// tagsExact reports whether every value of an indexed string column is its
// own only tag.
func (c columnMeta) tagsExact() bool { return c.tagLevel() == "" }

// sortsExactly reports whether the index's sorting vector holds the values
// of an indexed column as stored: those of a string column may be cut at a
// NUL byte.
func (c columnMeta) sortsExactly() bool {
	return c.Type.Kind != KindString || tagRank(c.tagLevel()) < tagRank(tagsTruncated)
}

// initTags records that a new indexed string column has no values yet.
func (c *columnMeta) initTags() {
	if c.Indexed && c.Type.Kind == KindString {
		c.TagsChecked = true
	}
}

// ---- raising levels ----

// rowTagLevels returns the levels that rows to be written (values ordered
// like meta.Columns) need for meta's indexed string columns, by field, where
// they are above the recorded ones.
func rowTagLevels(meta *tableMeta, rows [][]Value) map[string]string {
	var need map[string]string
	for i, c := range meta.Columns {
		if !c.Indexed || c.Type.Kind != KindString {
			continue
		}
		have := tagRank(c.TagValues)
		for _, row := range rows {
			if v := row[i]; !v.Null {
				if l := tagLevelOf(v.S); tagRank(l) > have {
					if need == nil {
						need = map[string]string{}
					}
					need[c.field()], have = l, tagRank(l)
				}
			}
		}
	}
	return need
}

// changeTagLevels is rowTagLevels for row changes.
func changeTagLevels(meta *tableMeta, changes []rowChange) map[string]string {
	have := map[string]int{}
	for _, c := range meta.Columns {
		if c.Indexed && c.Type.Kind == KindString {
			have[c.field()] = tagRank(c.TagValues)
		}
	}
	var need map[string]string
	for _, ch := range changes {
		for i := 0; i+1 < len(ch.set); i += 2 {
			f, _ := ch.set[i].(string)
			r, ok := have[f]
			if !ok {
				continue
			}
			v, _ := ch.set[i+1].(string)
			if l := tagLevelOf(v); tagRank(l) > r {
				if need == nil {
					need = map[string]string{}
				}
				need[f], have[f] = l, tagRank(l)
			}
		}
	}
	return need
}

// raiseTagLevels records the levels values about to be written need (from
// rowTagLevels or changeTagLevels) in the table's metadata. meta keeps the
// levels it was read with, so each batch a statement writes is compared with
// those: the table may have been replaced in between.
func (s *store) raiseTagLevels(ctx context.Context, meta *tableMeta, need map[string]string) error {
	if len(need) == 0 {
		return nil
	}
	return s.updateTable(ctx, meta.Schema, meta.Name, func(m *tableMeta) error {
		for i := range m.Columns {
			c := &m.Columns[i]
			if l, ok := need[c.field()]; ok && tagRank(l) > tagRank(c.TagValues) {
				c.TagValues = l
			}
		}
		return nil
	}, nil)
}

// ---- checking older tables ----

// tagCheckRetry is how long a connection waits before it checks a table
// again after a check failed.
const tagCheckRetry = time.Minute

// startTagCheck checks, in the background, the indexed string columns of a
// table that were never checked: at most one task per table per connection,
// and after one fails, not again for tagCheckRetry.
func (s *store) startTagCheck(meta *tableMeta) {
	unchecked := false
	for _, c := range meta.Columns {
		unchecked = unchecked || (c.Indexed && c.Type.Kind == KindString && !c.TagsChecked)
	}
	if !unchecked || meta.isMem {
		return
	}
	key := cleanupMember(meta.Schema, meta.Name)
	s.mu.Lock()
	if s.tagChecks == nil {
		s.tagChecks = map[string]time.Time{}
	}
	// A zero time is a check that is running.
	if next, ok := s.tagChecks[key]; ok && (next.IsZero() || time.Now().Before(next)) {
		s.mu.Unlock()
		return
	}
	s.tagChecks[key] = time.Time{}
	s.mu.Unlock()
	// As for cleanups, a closed connection makes the task fail, and a later
	// connection starts it again.
	worker := &store{client: s.client}
	go func() {
		err := worker.runTagCheck(context.Background(), meta.Schema, meta.Name)
		s.mu.Lock()
		if err != nil {
			s.tagChecks[key] = time.Now().Add(tagCheckRetry)
		} else {
			delete(s.tagChecks, key)
		}
		s.mu.Unlock()
	}()
}

// runTagCheck reads every row of a table and records the levels of its
// unchecked columns, unless its metadata changed meanwhile; then it starts
// over (a few times).
func (s *store) runTagCheck(ctx context.Context, schema, table string) error {
	key := metaKey(schema, table)
	for attempt := 0; ; attempt++ {
		if attempt == 3 {
			return errorf(adbc.StatusIO, "table %q.%q kept changing while its string columns were checked", displaySchema(schema), table)
		}
		raw, err := s.client.Get(ctx, key).Result()
		if err != nil {
			return err // goredis.Nil: dropped
		}
		var meta tableMeta
		if err := json.Unmarshal([]byte(raw), &meta); err != nil {
			return err
		}
		levels, err := s.scanTagLevels(ctx, &meta)
		if err != nil || levels == nil {
			return err
		}
		err = s.client.Watch(ctx, func(tx *goredis.Tx) error {
			cur, err := tx.Get(ctx, key).Result()
			if err != nil {
				return err
			}
			if cur != raw {
				return goredis.TxFailedErr
			}
			for i := range meta.Columns {
				c := &meta.Columns[i]
				if l, ok := levels[c.field()]; ok {
					if tagRank(l) > tagRank(c.TagValues) {
						c.TagValues = l
					}
					c.TagsChecked = true
				}
			}
			out, err := json.Marshal(&meta)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				p.Set(ctx, key, out, 0)
				return nil
			})
			return err
		}, key)
		if !errors.Is(err, goredis.TxFailedErr) {
			return err
		}
	}
}

// scanTagLevels reads every row of a table from its HASHes (LOAD *) and
// returns the level each unchecked indexed string column needs, by field,
// or nil if there are none.
func (s *store) scanTagLevels(ctx context.Context, meta *tableMeta) (map[string]string, error) {
	var levels map[string]string
	for _, c := range meta.Columns {
		if c.Indexed && c.Type.Kind == KindString && !c.TagsChecked {
			if levels == nil {
				levels = map[string]string{}
			}
			// Rows written while the column was added get its missing value.
			levels[c.field()] = tagLevelOf(c.Missing)
		}
	}
	if levels == nil {
		return nil, nil
	}
	node, err := s.search(ctx, meta.index())
	if err != nil {
		return nil, err
	}
	page := (&aggRequest{width: len(meta.Columns) + 2}).pageRows()
	reply, err := node.Do(ctx, "FT.AGGREGATE", meta.index(), "*", "LOAD", "*",
		"TIMEOUT", 0, "WITHCURSOR", "COUNT", page, "DIALECT", 2).Result()
	if err != nil {
		return nil, wrapRedis(err, "FT.AGGREGATE failed")
	}
	for {
		rows, cursor, err := parseCursorReply(reply)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			for f, l := range levels {
				if v, ok := r[f]; ok {
					if vl := tagLevelOf(v); tagRank(vl) > tagRank(l) {
						levels[f] = vl
					}
				}
			}
		}
		if cursor == 0 {
			return levels, nil
		}
		if reply, err = node.Do(ctx, "FT.CURSOR", "READ", meta.index(), cursor, "COUNT", page).Result(); err != nil {
			return nil, wrapRedis(err, "FT.CURSOR READ failed")
		}
	}
}

// ---- prefix expansion ----

// prefixComplete reports whether the index expands every tag that the
// prefix terms of an index query match: FT.PROFILE runs the query, reading
// one row, and reports a warning for each shard that reached the expansion
// limit. Each query is profiled once per statement; a profile that can't be
// read (or planning without a server) counts as incomplete.
func (e *executor) prefixComplete(ctx context.Context, meta *tableMeta, query string) bool {
	if e.store == nil {
		return false
	}
	e.ensureCache()
	key := meta.index() + "\x00" + query
	if ok, seen := e.cache.prefixes[key]; seen {
		return ok
	}
	ok := false
	reply, err := e.store.searchDo(ctx, meta.index(), "FT.PROFILE", meta.index(), "AGGREGATE", "LIMITED",
		"QUERY", query, "LIMIT", 0, 1, "TIMEOUT", 0, "DIALECT", 2).Result()
	if parts, isList := reply.([]any); err == nil && isList && len(parts) == 2 {
		ok = profileComplete(parts[1], e.store.shards(ctx))
	}
	if e.cache.prefixes == nil {
		e.cache.prefixes = map[string]bool{}
	}
	e.cache.prefixes[key] = ok
	return ok
}

// shards returns the number of shards a search runs on: the masters of a
// cluster, counted once per connection (counting reloads the cluster's
// slots), otherwise one.
func (s *store) shards(ctx context.Context) int {
	cc, ok := s.client.(*goredis.ClusterClient)
	if !ok {
		return 1
	}
	s.mu.Lock()
	n := s.shardCount
	s.mu.Unlock()
	if n > 0 {
		return n
	}
	var count atomic.Int32
	if err := cc.ForEachMaster(ctx, func(context.Context, *goredis.Client) error {
		count.Add(1)
		return nil
	}); err != nil {
		return 1
	}
	s.mu.Lock()
	s.shardCount = int(count.Load())
	s.mu.Unlock()
	return int(count.Load())
}

// profileComplete reports whether an FT.PROFILE profile has the profiles of
// at least shards shards, and each has a Warning entry that says None.
func profileComplete(profile any, shards int) bool {
	list, _ := profileEntry(profile, "Shards").([]any)
	if len(list) == 0 || len(list) < shards {
		return false
	}
	for _, shard := range list {
		if w := profileEntry(shard, "Warning"); w == nil || !noWarning(w) {
			return false
		}
	}
	return true
}

// profileEntry returns the value of key in a profile's key, value, … list,
// or nil.
func profileEntry(x any, key string) any {
	list, _ := x.([]any)
	for i := 0; i+1 < len(list); i += 2 {
		if s, ok := list[i].(string); ok && s == key {
			return list[i+1]
		}
	}
	return nil
}

// noWarning reports whether a profile's Warning entry says None.
func noWarning(v any) bool {
	switch x := v.(type) {
	case string:
		return x == "None"
	case []any:
		for _, y := range x {
			if !noWarning(y) {
				return false
			}
		}
		return true
	}
	return false
}
