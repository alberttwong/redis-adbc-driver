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

// Adopting HASHes in place
//
// An application's HASHes can become a driver table without being copied
// (AdoptHashes, adopt.go). A table's rows need what the driver writes for
// its own rows:
//
//   - a key <prefix><rowid>, where the row id is an integer (WHERE __rowid
//     = N reads that key), and a __rowid field that holds it. Adopting
//     writes __rowid into each HASH whose key suffix is an integer from 1 to
//     2^53; the other keys under the prefix stay out of the table.
//   - a search index in the driver's format: __rowid NUMERIC SORTABLE, and
//     each column as indexAttrArgs makes it. The application's own index
//     can't serve (the driver never checks an index's attributes, and its
//     queries assume them), so adopting creates a second one, with FILTER
//     "exists(@__rowid)" so that HASHes without __rowid (the application's
//     new ones) aren't counted.
//   - values stored as the driver stores them (decodeStored): numbers as
//     numbers, but booleans as 0 and 1 and dates and times as integers. So
//     a column is only BIGINT, NUMERIC or DOUBLE PRECISION in place if every
//     value is such a number; true/false text or ISO dates are VARCHAR.
//   - the table's metadata, marked adopted (see refuseAdopted for what the
//     driver then refuses), and its prefix registered.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

// ChecklistSQLInPlace names the checklist for adopting HASHes in place.
const ChecklistSQLInPlace = "sql_in_place"

// maxRowID is the largest row id an adopted key may have: the index holds
// __rowid as a double, which is exact up to 2^53.
const maxRowID = 1 << 53

// suffixID returns a key's row id: its suffix after the prefix, if that is
// a canonical decimal integer from 1 to 2^53.
func suffixID(key, prefix string) (int64, bool) {
	s, ok := strings.CutPrefix(key, prefix)
	if !ok || s == "" || s[0] == '0' || len(s) > 16 {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id > maxRowID {
		return 0, false
	}
	return id, true
}

// adoptIndexName is the name of an adopted table's index. Escaping turns
// ':' into %3A in schema and table names, so no driver table's index can
// have it (namesFor).
func adoptIndexName(schema, table string) string { return indexName(schema, table) + ":adopted" }

// keyStats is what the keys under a prefix say about adopting them: their
// suffixes and __rowid fields.
type keyStats struct {
	keys       int64
	ids        int64 // keys with a row id suffix
	maxID      int64
	other      int64 // keys without one
	otherEx    string
	otherRowID int64 // … that have a __rowid field all the same
	otherIDEx  string
	idOK       int64 // __rowid equal to the suffix
	idMissing  int64
	idWrong    int64
	idWrongEx  string
}

// add counts a key, given its __rowid field (has is false without one).
func (k *keyStats) add(key, prefix, rowid string, has bool) (id int64, ok bool) {
	k.keys++
	id, ok = suffixID(key, prefix)
	if !ok {
		k.other++
		if k.otherEx == "" {
			k.otherEx = key
		}
		if has {
			k.otherRowID++
			if k.otherIDEx == "" {
				k.otherIDEx = key
			}
		}
		return 0, false
	}
	k.ids++
	k.maxID = max(k.maxID, id)
	switch {
	case !has:
		k.idMissing++
	case rowid == strconv.FormatInt(id, 10):
		k.idOK++
	default:
		k.idWrong++
		if k.idWrongEx == "" {
			k.idWrongEx = fmt.Sprintf("%s has %s", key, quoteValue(rowid))
		}
	}
	return id, true
}

// rowIDs reads the __rowid field of each key (nil for none).
func (c *hashConn) rowIDs(ctx context.Context, keys []string) ([]*string, error) {
	out := make([]*string, len(keys))
	for start := 0; start < len(keys); start += pipelineChunk {
		chunk := keys[start:min(start+pipelineChunk, len(keys))]
		pipe := c.client.Pipeline()
		cmds := make([]*goredis.StringCmd, len(chunk))
		for i, k := range chunk {
			cmds[i] = pipe.HGet(ctx, k, rowIDField)
		}
		_, _ = pipe.Exec(ctx)
		for i, cmd := range cmds {
			v, err := cmd.Result()
			switch {
			case err == goredis.Nil, isWrongType(err):
			case err != nil:
				return nil, wrapRedis(err, "HGET failed")
			default:
				out[start+i] = &v
			}
		}
	}
	return out, nil
}

// inPlaceTarget is the table that adopting would make.
type inPlaceTarget struct {
	prefix, schema, table string
	// full is whether the columns were guessed from every HASH (adopt)
	// rather than a sample (check).
	full     bool
	sampled  int
	command  string // the adopt command line
	existing *tableMeta
	// resuming is set when an earlier adopt into the same table stopped
	// half-way (adoptingKey), and reserved the names.
	resuming bool
}

func (t inPlaceTarget) name() string { return displaySchema(t.schema) + "." + t.table }

// verb picks the form for n things.
func verb(n int64, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// whyWeaker says why a column's type in place is weaker than a copy's.
func whyWeaker(col *guessedColumn) string {
	g := col.guess
	switch {
	case col.t.Kind == KindBool:
		return "the driver stores booleans as 0 and 1"
	case col.t.Kind == KindDate || col.t.Kind == KindTimestamp:
		return "the driver stores dates and times as numbers"
	case g != nil && g.empty > 0:
		return fmt.Sprintf("%d HASHes have it empty, and RediSearch doesn't index a HASH whose NUMERIC field isn't a number", g.empty)
	case g != nil && g.counts[classSpecial] > 0:
		return "it holds NaN or infinities, which the driver stores differently"
	}
	return "the values aren't stored as the driver stores that type"
}

// checkInPlace is the checklist for adopting the HASHes in place.
func (c *hashConn) checkInPlace(ctx context.Context, r *HashReport, cols []*guessedColumn, k *keyStats,
	tables []*tableMeta, indexes []searchIndex, t inPlaceTarget) (Checklist, error) {
	cl := Checklist{Name: ChecklistSQLInPlace, Title: "SQL, adopted in place (redis-arrow adopt)"}
	c.checkServer(&cl, r)
	if t.existing != nil {
		return c.checkAdopted(ctx, cl, k, t)
	}
	for _, tm := range tables {
		if tm.prefix() == t.prefix {
			cl.add(CheckOK, fmt.Sprintf("these are the rows of the driver table %s, which SQL already reads",
				displaySchema(tm.Schema)+"."+tm.Name), "")
			cl.finish("")
			return cl, nil
		}
	}
	rec, err := c.adoptingRecord(ctx, t.prefix)
	if err != nil {
		return cl, err
	}
	if rec != nil {
		if rec.Schema == t.schema && rec.Table == t.table {
			t.resuming = true
			cl.add(CheckWarn, fmt.Sprintf("an adopt of %s into %s stopped before it finished (or is running)", shellQuote(t.prefix), t.name()),
				"adopt -apply finishes it")
		} else {
			cl.add(CheckBlocker, fmt.Sprintf("an adopt of %s into %s.%s stopped before it finished (or is running)", shellQuote(t.prefix),
				displaySchema(rec.Schema), rec.Table), fmt.Sprintf("run adopt -apply with -table %s to finish it", rec.Table))
		}
	}
	if err := c.checkAdoptPrefix(ctx, &cl, tables, t); err != nil {
		return cl, err
	}
	if err := c.checkAdoptNames(ctx, &cl, indexes, t); err != nil {
		return cl, err
	}

	// The keys and their __rowid fields.
	switch {
	case k.ids == 0:
		cl.add(CheckBlocker, fmt.Sprintf("no HASH key under %s ends in an integer row id (from 1 to 2^53)", shellQuote(t.prefix)),
			"a table's rows have keys <prefix><rowid>: copy the HASHes instead (redis-arrow scan | redis-arrow import)")
	case k.other > 0:
		cl.add(CheckWarn, fmt.Sprintf("%s (such as %s) %s in an integer row id, and %s out of the table",
			plural(k.other, "key", "keys"), k.otherEx, verb(k.other, "doesn't end", "don't end"), verb(k.other, "stays", "stay")), "")
		cl.add(CheckOK, fmt.Sprintf("%s %s in a row id (up to %d)", plural(k.ids, "key", "keys"), verb(k.ids, "ends", "end"), k.maxID), "")
	default:
		cl.add(CheckOK, fmt.Sprintf("every key ends in a row id (up to %d)", k.maxID), "")
	}
	if k.otherRowID > 0 {
		cl.add(CheckBlocker, fmt.Sprintf("%s without a row id suffix %s a __rowid field (such as %s): the table would read them under another key",
			plural(k.otherRowID, "key", "keys"), verb(k.otherRowID, "has", "have"), k.otherIDEx), "remove those fields (HDEL <key> __rowid)")
	}
	if k.idWrong > 0 {
		cl.add(CheckBlocker, fmt.Sprintf("%s %s a __rowid that isn't the key's suffix (%s)", plural(k.idWrong, "HASH", "HASHes"), verb(k.idWrong, "has", "have"), k.idWrongEx),
			"set __rowid to the key's suffix, or remove it (HDEL <key> __rowid)")
	}
	switch {
	case k.idMissing > 0:
		cl.add(CheckMissing, fmt.Sprintf("%s %s no __rowid field", plural(k.idMissing, "HASH", "HASHes"), verb(k.idMissing, "has", "have")),
			"adopt writes each one's key suffix into it (HSET <key> __rowid <suffix>)")
	case k.ids > 0:
		cl.add(CheckOK, "every HASH has a __rowid field equal to its key's suffix", "")
	}

	// The columns.
	if !t.full && r.Keys > int64(t.sampled) {
		cl.add(CheckWarn, fmt.Sprintf("the in-place types are guessed from %d of the %d HASHes", t.sampled, r.Keys),
			"adopt reads them all, and refuses values that don't fit their column")
	}
	n := 0
	for _, col := range cols {
		if col.field == "" {
			continue // the key column of copies
		}
		g := col.guess
		if col.override != "" && g != nil && g.misfits > 0 {
			cl.add(CheckBlocker, fmt.Sprintf("-type %s=%s: %s %s (such as %s in %s: %v)", col.name, col.override,
				plural(int64(g.misfits), "value", "values"), verb(int64(g.misfits), "doesn't fit", "don't fit"),
				quoteValue(g.misfit.value), g.misfit.key, storedFits(g.misfit.value, col.inPlace)),
				"pick another type, or copy the HASHes instead")
			continue
		}
		if col.override == "" && col.inPlace.Kind != col.t.Kind && col.t.Kind != KindString {
			cl.add(CheckWarn, fmt.Sprintf("%s is %s in place (%s when copied): %s", col.name, col.inPlace.SQLName(),
				col.t.SQLName(), whyWeaker(col)), "")
		}
		if indexable(col.inPlace) {
			if !simpleName(col.fieldOrName()) {
				cl.add(CheckWarn, fmt.Sprintf("field %q isn't indexed: only names of letters, digits and _ are", col.field),
					"filters on it read every row")
				continue
			}
			n++
		}
	}
	if n > maxIndexed {
		cl.add(CheckWarn, fmt.Sprintf("%d columns can be indexed, but a table indexes at most %d", n, maxIndexed),
			"adopt -index-columns picks the ones to index")
	}
	for _, s := range r.Skipped {
		if !strings.HasPrefix(s, rowIDField+":") {
			cl.add(CheckWarn, "not a column: "+s, "")
		}
	}

	// What adopt creates.
	var apps []string
	for _, ix := range r.Indexes {
		apps = append(apps, ix.Name)
	}
	idx := fmt.Sprintf("a search index in the driver's format on %s", shellQuote(t.prefix))
	fix := fmt.Sprintf("adopt creates %s, with FILTER \"exists(@__rowid)\"; it takes memory of its own", adoptIndexName(t.schema, t.table))
	if len(apps) > 0 {
		fix += fmt.Sprintf(" (the application's %s stays as it is, and isn't used)", strings.Join(apps, ", "))
	}
	cl.add(CheckMissing, idx, fix)
	cl.add(CheckMissing, fmt.Sprintf("the table's metadata (%s)", metaKey(t.schema, t.table)), "adopt writes it")
	for _, h := range adoptHazards {
		cl.add(CheckWarn, h, "")
	}
	cl.finish(t.command)
	return cl, nil
}

// adoptHazards are what an adopted table's users should know.
var adoptHazards = []string{
	"HASHes the application writes later have no __rowid, so SQL doesn't see them until adopt -refresh runs",
	"a later write by the application of a value a numeric column can't hold (text, an empty value, NaN) takes that HASH out of the index, and so out of the table",
	"the __rowid field shows up in the application's own HGETALL and FT.SEARCH results",
	"SQL UPDATE and DELETE change the application's HASHes; INSERT, TRUNCATE, ADD COLUMN and a re-keying RENAME are refused, and DROP TABLE drops only the index and the metadata",
}

// checkAdoptPrefix checks that no driver key can be under the prefix.
func (c *hashConn) checkAdoptPrefix(ctx context.Context, cl *Checklist, tables []*tableMeta, t inPlaceTarget) error {
	if t.prefix == "" {
		cl.add(CheckBlocker, "the prefix is empty", "adopt needs the collection's own key prefix")
		return nil
	}
	if prefixesOverlap(t.prefix, metaPrefix) || prefixesOverlap(t.prefix, tempSchemaBase) {
		cl.add(CheckBlocker, fmt.Sprintf("keys under %s can be the driver's own (%s…, %s…)", shellQuote(t.prefix), metaPrefix, tempSchemaBase), "")
		return nil
	}
	for _, tm := range tables {
		if prefixesOverlap(tm.prefix(), t.prefix) {
			cl.add(CheckBlocker, fmt.Sprintf("keys under %s can be rows of the driver table %s (%s)", shellQuote(t.prefix),
				displaySchema(tm.Schema)+"."+tm.Name, tm.prefix()), "")
			return nil
		}
	}
	pipe := c.client.Pipeline()
	used := pipe.SMembers(ctx, prefixesKey)
	adopted := pipe.SMembers(ctx, adoptedKey)
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return wrapRedis(err, "failed to read the table registry")
	}
	for _, set := range [][]string{used.Val(), adopted.Val()} {
		if t.resuming {
			// The earlier adopt reserved the prefix itself.
			set = slices.DeleteFunc(slices.Clone(set), func(p string) bool { return p == t.prefix })
		}
		if p := adoptedOverlap(set, t.prefix); p != "" {
			cl.add(CheckBlocker, fmt.Sprintf("keys under %s can be under %s, a driver table's key prefix", shellQuote(t.prefix), p), "")
			return nil
		}
	}
	cl.add(CheckOK, fmt.Sprintf("no driver table's rows can be under %s", shellQuote(t.prefix)), "")
	return nil
}

// checkAdoptNames checks that the table, view and index names are free.
func (c *hashConn) checkAdoptNames(ctx context.Context, cl *Checklist, indexes []searchIndex, t inPlaceTarget) error {
	if reservedSchema(t.schema) {
		cl.add(CheckBlocker, fmt.Sprintf("%s is a temporary schema", t.schema), "pick a permanent schema with -schema")
		return nil
	}
	view, err := c.st.viewExists(ctx, t.schema, t.table)
	if err != nil {
		return err
	}
	table, err := c.st.tableExists(ctx, t.schema, t.table)
	if err != nil {
		return err
	}
	switch {
	case view:
		cl.add(CheckBlocker, fmt.Sprintf("%s is a view", t.name()), "pick another table name with -table")
	case table:
		cl.add(CheckBlocker, fmt.Sprintf("the table %s exists", t.name()), "pick another table name with -table")
	default:
		cl.add(CheckOK, fmt.Sprintf("%s doesn't exist yet", t.name()), "")
	}
	idx := adoptIndexName(t.schema, t.table)
	for _, ix := range indexes {
		if ix.name == idx && !t.resuming {
			cl.add(CheckBlocker, fmt.Sprintf("a search index named %s exists", idx), "pick another table name with -table")
		}
	}
	return nil
}

// adoptingRecord reads the record of an adopt of the prefix that hasn't
// finished, if any.
func (c *hashConn) adoptingRecord(ctx context.Context, prefix string) (*adoptRecord, error) {
	raw, err := c.client.HGet(ctx, adoptingKey, prefix).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, wrapRedis(err, "failed to read "+adoptingKey)
	}
	var rec adoptRecord
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return nil, errorf(adbc.StatusInternal, "corrupt %s entry for %q: %v", adoptingKey, prefix, err)
	}
	return &rec, nil
}

// checkAdopted checks a table that was adopted already: HASHes the
// application added since have no __rowid.
func (c *hashConn) checkAdopted(ctx context.Context, cl Checklist, k *keyStats, t inPlaceTarget) (Checklist, error) {
	m := t.existing
	cl.add(CheckOK, fmt.Sprintf("these HASHes are the adopted table %s", displaySchema(m.Schema)+"."+m.Name), "")
	if reply, err := c.st.searchDo(ctx, m.index(), "FT.INFO", m.index()).Result(); err == nil {
		if info := indexInfoOf(reply); info.failures > 0 {
			cl.add(CheckWarn, fmt.Sprintf("its index %s couldn't index %s (such as %s: %s), which the table leaves out",
				m.index(), plural(info.failures, "HASH", "HASHes"), info.lastErrorKey, info.lastError),
				"fix those values, and write them again")
		}
	}
	if k.idWrong > 0 {
		cl.add(CheckWarn, fmt.Sprintf("%s %s a __rowid that isn't the key's suffix (%s)", plural(k.idWrong, "HASH", "HASHes"), verb(k.idWrong, "has", "have"), k.idWrongEx), "")
	}
	if k.idMissing > 0 {
		cl.add(CheckMissing, fmt.Sprintf("%s written since %s no __rowid field, so the table leaves %s out",
			plural(k.idMissing, "HASH", "HASHes"), verb(k.idMissing, "has", "have"), verb(k.idMissing, "it", "them")), "adopt -refresh writes it")
		cl.finish(t.command + " -refresh")
		return cl, nil
	}
	cl.add(CheckOK, "every HASH with a row id suffix has its __rowid", "")
	cl.finish("")
	return cl, nil
}
