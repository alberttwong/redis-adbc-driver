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

// ALTER TABLE.
//
// Every change is a metadata update (WATCH/MULTI on the table's metadata key),
// so it takes the same time at any table size:
//
//   - RENAME TO: the table keeps its key prefix and index (fixed at creation
//     and reserved in a registry), so no row is touched.
//   - RENAME COLUMN: the column keeps its HASH field and index attribute;
//     only its SQL name changes.
//   - ADD COLUMN: the column gets a HASH field no current or dropped column
//     has used, and is added to the index with FT.ALTER. Existing rows read
//     it as NULL.
//   - DROP COLUMN: the column disappears immediately. Its field is retired
//     (never reused) and removed from existing rows by a background task;
//     the task is recorded in the metadata and resumed by later connections
//     if the process exits first. RediSearch cannot drop an attribute, so an
//     indexed column's attribute stays in the index, unused.

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

func cleanupMember(schema, table string) string { return schema + "\x00" + table }

func (e *executor) runAlter(ctx context.Context, st *AlterTableStmt) error {
	schema, name, err := e.resolveTable(st.Table)
	if err != nil {
		return err
	}
	isView, err := e.store.viewExists(ctx, schema, name)
	if err != nil {
		return err
	}
	if isView {
		// As in Postgres, ALTER TABLE … RENAME TO also renames a view.
		if st.Action != AlterRenameTable {
			return errorf(adbc.StatusInvalidArgument, "%q.%q is a view; ALTER TABLE on a view supports only RENAME TO", schema, name)
		}
		return e.store.renameView(ctx, schema, name, st.NewTable)
	}
	meta, err := e.store.getTable(ctx, schema, name)
	if err != nil {
		var ae adbc.Error
		if asAdbc(err, &ae) && ae.Code == adbc.StatusNotFound {
			if st.IfExists {
				return nil
			}
			if st.View {
				return viewNotFound(schema, name)
			}
		}
		return err
	}
	if st.View {
		return errorf(adbc.StatusInvalidArgument, "%q.%q is a table, not a view; use ALTER TABLE", schema, name)
	}
	switch st.Action {
	case AlterRenameTable:
		return e.renameTable(ctx, meta, st.NewTable)
	case AlterRenameColumn:
		return e.renameColumn(ctx, meta, st.Column, st.NewColumn)
	case AlterAddColumn:
		return e.addColumn(ctx, meta, st.Def, st.IfColumnNotExists)
	case AlterDropColumn:
		return e.dropColumn(ctx, meta, st.Column, st.IfColumnExists)
	}
	return errorf(adbc.StatusNotImplemented, "unsupported ALTER TABLE action")
}

func checkColumnName(name string) error {
	if name == "" {
		return errorf(adbc.StatusInvalidArgument, "column names must not be empty")
	}
	if strings.HasPrefix(name, "__") {
		return errorf(adbc.StatusInvalidArgument, "column name %q is reserved", name)
	}
	return nil
}

func (e *executor) renameTable(ctx context.Context, meta *tableMeta, to TableName) error {
	if to.Catalog != "" && to.Catalog != catalogName {
		return errorf(adbc.StatusInvalidArgument, "catalog %q does not exist", to.Catalog)
	}
	if to.Schema != "" && to.Schema != meta.Schema {
		return errorf(adbc.StatusNotImplemented, "RENAME TO cannot move a table to another schema")
	}
	newName := to.Name
	if newName == meta.Name {
		return nil
	}
	if exists, err := e.store.viewExists(ctx, meta.Schema, newName); err != nil {
		return err
	} else if exists {
		return errorf(adbc.StatusAlreadyExists, "%q.%q already exists as a view", meta.Schema, newName)
	}
	s := e.store
	oldKey, newKey := metaKey(meta.Schema, meta.Name), metaKey(meta.Schema, newName)
	oldSeq, newSeq := seqKey(meta.Schema, meta.Name), seqKey(meta.Schema, newName)
	// All keys are in the {meta} hash slot, so this is one transaction even
	// on a cluster.
	for attempt := 0; attempt < 20; attempt++ {
		err := s.client.Watch(ctx, func(tx *goredis.Tx) error {
			cur, err := s.getTableWith(ctx, tx, meta.Schema, meta.Name)
			if err != nil {
				return err
			}
			if n, err := tx.Exists(ctx, newKey).Result(); err != nil {
				return err
			} else if n > 0 {
				return errorf(adbc.StatusAlreadyExists, "table %q.%q already exists", meta.Schema, newName)
			}
			seq, err := tx.Get(ctx, oldSeq).Result()
			if err != nil && err != goredis.Nil {
				return err
			}
			pending, err := tx.SIsMember(ctx, cleanupKey, cleanupMember(meta.Schema, meta.Name)).Result()
			if err != nil {
				return err
			}
			// Pin the physical names before the logical name changes.
			cur.KeyPrefix, cur.IndexName = cur.prefix(), cur.index()
			cur.Name = newName
			raw, err := marshalMeta(cur)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				p.Set(ctx, newKey, raw, 0)
				p.Del(ctx, oldKey, oldSeq)
				if seq != "" {
					p.Set(ctx, newSeq, seq, 0)
				}
				p.SRem(ctx, tablesKey(meta.Schema), meta.Name)
				p.SAdd(ctx, tablesKey(meta.Schema), newName)
				if pending {
					p.SRem(ctx, cleanupKey, cleanupMember(meta.Schema, meta.Name))
					p.SAdd(ctx, cleanupKey, cleanupMember(meta.Schema, newName))
				}
				return nil
			})
			return err
		}, oldKey, newKey, oldSeq)
		if err == goredis.TxFailedErr {
			continue
		}
		return wrapRedis(err, "failed to rename table")
	}
	return errorf(adbc.StatusIO, "table %q.%q is being changed concurrently; try again", meta.Schema, meta.Name)
}

func (e *executor) renameColumn(ctx context.Context, meta *tableMeta, from, to string) error {
	if err := checkColumnName(to); err != nil {
		return err
	}
	return e.store.updateTable(ctx, meta.Schema, meta.Name, func(m *tableMeta) error {
		i, ok := m.resolve(from)
		if !ok {
			return errorf(adbc.StatusNotFound, "column %q does not exist in table %q", from, m.Name)
		}
		if j, ok := m.resolve(to); ok && j != i {
			return errorf(adbc.StatusAlreadyExists, "column %q already exists in table %q", to, m.Name)
		}
		m.Columns[i].Field = m.Columns[i].field()
		m.Columns[i].Name = to
		if m.Columns[i].Field == to {
			m.Columns[i].Field = ""
		}
		return nil
	}, nil)
}

// freeField returns a HASH field name for a new column that no current,
// retired, or pending-cleanup field uses.
func freeField(m *tableMeta, name string) string {
	used := map[string]bool{}
	for _, c := range m.Columns {
		used[strings.ToLower(c.field())] = true
	}
	for _, f := range m.RetiredFields {
		used[strings.ToLower(f)] = true
	}
	cand := name
	for n := 2; used[strings.ToLower(cand)] || cand == rowIDField; n++ {
		cand = fmt.Sprintf("%s_%d", name, n)
	}
	return cand
}

func (e *executor) addColumn(ctx context.Context, meta *tableMeta, def ColumnDef, ifNotExists bool) error {
	if err := checkColumnName(def.Name); err != nil {
		return err
	}
	if def.NotNull {
		return errorf(adbc.StatusNotImplemented, "cannot add a NOT NULL column: existing rows would have no value")
	}
	if def.HasDefault {
		return errorf(adbc.StatusNotImplemented, "column defaults are not supported")
	}
	if _, ok := meta.resolve(def.Name); ok {
		if ifNotExists {
			return nil
		}
		return errorf(adbc.StatusAlreadyExists, "column %q already exists in table %q", def.Name, meta.Name)
	}
	col := columnMeta{Name: def.Name, Type: def.Type, Nullable: true}
	if f := freeField(meta, def.Name); f != def.Name {
		col.Field = f
	}
	indexed := 0
	for _, c := range meta.Columns {
		if c.Indexed {
			indexed++
		}
	}
	col.Indexed = indexable(col.Type) && !def.NoIndex && simpleName(col.field()) && indexed < maxIndexed
	if col.Indexed {
		// Add the attribute first: an unused attribute is harmless if the
		// metadata update below fails.
		args := append([]any{"FT.ALTER", meta.index(), "SCHEMA", "ADD"}, indexAttrArgs(col)...)
		if err := e.store.searchDo(ctx, meta.index(), args...).Err(); err != nil {
			return wrapRedis(err, "failed to add the column to the search index")
		}
	}
	return e.store.updateTable(ctx, meta.Schema, meta.Name, func(m *tableMeta) error {
		if _, ok := m.resolve(def.Name); ok {
			if ifNotExists {
				return nil
			}
			return errorf(adbc.StatusAlreadyExists, "column %q already exists in table %q", def.Name, m.Name)
		}
		if freeField(m, col.field()) != col.field() {
			return errorf(adbc.StatusIO, "table %q changed concurrently; try again", m.Name)
		}
		m.Columns = append(m.Columns, col)
		return nil
	}, nil)
}

func (e *executor) dropColumn(ctx context.Context, meta *tableMeta, name string, ifExists bool) error {
	err := e.store.updateTable(ctx, meta.Schema, meta.Name, func(m *tableMeta) error {
		i, ok := m.resolve(name)
		if !ok {
			if ifExists {
				return nil
			}
			return errorf(adbc.StatusNotFound, "column %q does not exist in table %q", name, m.Name)
		}
		if len(m.Columns) == 1 {
			return errorf(adbc.StatusInvalidArgument, "cannot drop the only column of table %q", m.Name)
		}
		f := m.Columns[i].field()
		m.Columns = slices.Delete(m.Columns, i, i+1)
		m.RetiredFields = append(m.RetiredFields, f)
		m.PendingCleanup = append(m.PendingCleanup, f)
		return nil
	}, func(p goredis.Pipeliner) {
		p.SAdd(ctx, cleanupKey, cleanupMember(meta.Schema, meta.Name))
	})
	if err != nil {
		return err
	}
	e.store.startCleanup(meta.Schema, meta.Name)
	return nil
}

// ---- background cleanup of dropped columns ----

// startCleanup removes pending dropped-column fields from a table's rows in
// the background (at most one task per table per connection).
func (s *store) startCleanup(schema, table string) {
	key := cleanupMember(schema, table)
	s.mu.Lock()
	if s.cleaning == nil {
		s.cleaning = map[string]bool{}
	}
	if s.cleaning[key] {
		s.mu.Unlock()
		return
	}
	s.cleaning[key] = true
	s.mu.Unlock()
	// The task keeps its own reference to the client: if the connection is
	// closed, its commands fail and a later connection resumes the work.
	worker := &store{client: s.client}
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.cleaning, key)
			s.mu.Unlock()
		}()
		_ = worker.runCleanup(context.Background(), schema, table)
	}()
}

// resumeCleanups restarts the cleanups recorded by earlier connections.
func (s *store) resumeCleanups(ctx context.Context) error {
	members, err := s.client.SMembers(ctx, cleanupKey).Result()
	if err != nil {
		return wrapRedis(err, "failed to read pending cleanups")
	}
	for _, m := range members {
		if schema, table, ok := strings.Cut(m, "\x00"); ok {
			s.startCleanup(schema, table)
		}
	}
	return nil
}

func (s *store) runCleanup(ctx context.Context, schema, table string) error {
	meta, err := s.getTable(ctx, schema, table)
	if err != nil {
		var ae adbc.Error
		if asAdbc(err, &ae) && ae.Code == adbc.StatusNotFound {
			return s.client.SRem(ctx, cleanupKey, cleanupMember(schema, table)).Err()
		}
		return err
	}
	fields := slices.Clone(meta.PendingCleanup)
	if len(fields) > 0 {
		if err := s.deleteFields(ctx, meta, fields); err != nil {
			return err
		}
	}
	done := false
	return s.updateTable(ctx, schema, table, func(m *tableMeta) error {
		m.PendingCleanup = slices.DeleteFunc(m.PendingCleanup, func(f string) bool {
			return slices.Contains(fields, f)
		})
		// Something dropped meanwhile keeps the marker for a later pass.
		done = len(m.PendingCleanup) == 0
		return nil
	}, func(p goredis.Pipeliner) {
		if done {
			p.SRem(ctx, cleanupKey, cleanupMember(schema, table))
		}
	})
}

// deleteFields walks every row of a table through its index and removes the
// given HASH fields, a cursor page at a time.
func (s *store) deleteFields(ctx context.Context, meta *tableMeta, fields []string) error {
	node, err := s.search(ctx, meta.index())
	if err != nil {
		return err
	}
	reply, err := node.Do(ctx, "FT.AGGREGATE", meta.index(), "*", "LOAD", 1, "@__key",
		"TIMEOUT", 0, "WITHCURSOR", "COUNT", cursorCount, "DIALECT", 2).Result()
	if err != nil {
		return wrapRedis(err, "cleanup: FT.AGGREGATE failed")
	}
	for {
		rows, cursor, err := parseCursorReply(reply)
		if err != nil {
			return err
		}
		pipe := s.client.Pipeline()
		for _, r := range rows {
			if k, ok := r["__key"]; ok {
				pipe.HDel(ctx, k, fields...)
			}
		}
		if len(rows) > 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return wrapRedis(err, "cleanup: HDEL failed")
			}
		}
		if cursor == 0 {
			return nil
		}
		if reply, err = node.Do(ctx, "FT.CURSOR", "READ", meta.index(), cursor, "COUNT", cursorCount).Result(); err != nil {
			return wrapRedis(err, "cleanup: FT.CURSOR READ failed")
		}
	}
}

func marshalMeta(m *tableMeta) ([]byte, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, errorf(adbc.StatusInternal, "failed to encode metadata: %v", err)
	}
	return raw, nil
}

// getTableWith reads table metadata inside a WATCH transaction.
func (s *store) getTableWith(ctx context.Context, tx *goredis.Tx, schema, table string) (*tableMeta, error) {
	raw, err := tx.Get(ctx, metaKey(schema, table)).Result()
	if err == goredis.Nil {
		return nil, tableNotFound(schema, table)
	}
	if err != nil {
		return nil, err
	}
	var m tableMeta
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, errorf(adbc.StatusInternal, "corrupt metadata for table %q.%q: %v", schema, table, err)
	}
	return &m, nil
}
