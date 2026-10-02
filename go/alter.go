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
// so it takes the same time at any table size (except where a CHECK
// constraint is added, which reads the rows first). RENAME TO and RENAME
// COLUMN come alone; the other actions may come several to a statement,
// comma-separated, and are then one metadata update (see alterTable):
//
//   - RENAME TO: the table keeps its key prefix and index (fixed at creation
//     and reserved in a registry), so no row is touched. With the option
//     adbc.redis.rename_rekey, it also moves the rows and index to the new
//     name's, in time proportional to the number of rows (see rekey.go).
//   - RENAME COLUMN: the column keeps its HASH field and index attribute;
//     only its SQL name changes.
//   - ADD COLUMN: the column gets a HASH field no current or dropped column
//     has used, and is added to the index with FT.ALTER. Existing rows read
//     it as NULL, or with DEFAULT as its default, which is recorded as the
//     column's missing value instead of being written to them (see
//     defaults.go).
//   - DROP COLUMN: the column disappears immediately. Its field is retired
//     (never reused) and removed from existing rows by a background task;
//     the task is recorded in the metadata and resumed by later connections
//     if the process exits first. RediSearch cannot drop an attribute, so an
//     indexed column's attribute stays in the index, unused.
//   - ALTER COLUMN … SET / DROP DEFAULT: only rows inserted later see the
//     change.
//   - ADD COLUMN … CHECK and ADD CONSTRAINT … CHECK check the existing rows
//     against the new constraint, then add it; DROP CONSTRAINT removes one
//     (see check.go). DROP COLUMN drops the constraints that read the column,
//     and RENAME COLUMN rewrites them.

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
	rename := len(st.Cmds) == 1 && (st.Cmds[0].Action == AlterRenameTable || st.Cmds[0].Action == AlterRenameColumn)
	if isView {
		// As in Postgres, ALTER TABLE … RENAME TO also renames a view.
		if len(st.Cmds) != 1 || st.Cmds[0].Action != AlterRenameTable {
			return errorf(adbc.StatusInvalidArgument, "%q.%q is a view; ALTER TABLE on a view supports only RENAME TO", displaySchema(schema), name)
		}
		return e.store.renameView(ctx, schema, name, st.Cmds[0].NewTable)
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
		return errorf(adbc.StatusInvalidArgument, "%q.%q is a table, not a view; use ALTER TABLE", displaySchema(schema), name)
	}
	if err := e.store.checkWritable(ctx, meta); err != nil {
		return err
	}
	if rename {
		cmd := st.Cmds[0]
		if cmd.Action == AlterRenameTable {
			return e.renameTable(ctx, meta, cmd.NewTable)
		}
		return e.renameColumn(ctx, meta, cmd.Column, cmd.NewColumn)
	}
	return e.alterTable(ctx, meta, st.Cmds)
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
	sameSchema := to.Schema == "" || to.Schema == meta.Schema || (isTempAlias(to.Schema) && isTempSchema(meta.Schema))
	if !sameSchema {
		return errorf(adbc.StatusNotImplemented, "RENAME TO cannot move a table to another schema")
	}
	newName := to.Name
	if newName == meta.Name {
		return nil
	}
	if exists, err := e.store.viewExists(ctx, meta.Schema, newName); err != nil {
		return err
	} else if exists {
		return errorf(adbc.StatusAlreadyExists, "%q.%q already exists as a view", displaySchema(meta.Schema), newName)
	}
	if e.rekey {
		if err := refuseAdopted(meta, "RENAME TO with "+OptionStringRenameRekey+" (which moves the rows to new keys)"); err != nil {
			return err
		}
		return e.rekeyTable(ctx, meta, newName)
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
			if err := movingErr(cur); err != nil {
				return err
			}
			if n, err := tx.Exists(ctx, newKey).Result(); err != nil {
				return err
			} else if n > 0 {
				return errorf(adbc.StatusAlreadyExists, "table %q.%q already exists", displaySchema(meta.Schema), newName)
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
		if err == nil {
			s.trackTemp(meta.Schema, meta.Name, false)
			s.trackTemp(meta.Schema, newName, true)
		}
		return wrapRedis(err, "failed to rename table")
	}
	return errorf(adbc.StatusIO, "table %q.%q is being changed concurrently; try again", displaySchema(meta.Schema), meta.Name)
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
		old := m.Columns[i].Name
		m.Columns[i].Field = m.Columns[i].field()
		m.Columns[i].Name = to
		if m.Columns[i].Field == to {
			m.Columns[i].Field = ""
		}
		// CHECK constraints name the column in their text.
		for j, c := range m.Checks {
			if checkReads(c.Expr, old) {
				text, err := renameInCheck(c.Expr, old, to)
				if err != nil {
					return err
				}
				m.Checks[j].Expr = text
			}
		}
		return nil
	}, nil)
}

// freeField returns a HASH field name for a new column that no current,
// retired, or pending-cleanup field uses, nor an index attribute in taken
// (lower-cased; see alterTable).
func freeField(m *tableMeta, name string, taken map[string]bool) string {
	used := map[string]bool{}
	for _, c := range m.Columns {
		used[strings.ToLower(c.field())] = true
	}
	for _, f := range m.RetiredFields {
		used[strings.ToLower(f)] = true
	}
	cand := name
	for n := 2; used[strings.ToLower(cand)] || taken[strings.ToLower(cand)] || cand == rowIDField; n++ {
		cand = fmt.Sprintf("%s_%d", name, n)
	}
	return cand
}

// ---- ADD / DROP COLUMN, ALTER COLUMN … DEFAULT, ADD / DROP CONSTRAINT ----

// alterResult is what applying the actions of an ALTER TABLE (other than
// RENAME) to a table's metadata did.
type alterResult struct {
	// added are the columns added (and not dropped again), with the
	// missing value they get, for the index and the row checks.
	added []addedColumn
	// checks are the CHECK constraints added that are still there at the
	// end; existing rows must pass them.
	checks []checkMeta
	// dropped is set when a column of the table was dropped, so its field
	// must be removed from the rows.
	dropped bool
	// trace is each action's resolved effect, so that the actions applied
	// again in the transaction that commits them (to the metadata as it is
	// then) can be compared with what was planned, checked and indexed.
	trace []string
}

// addedColumn is a column added by ALTER TABLE. missing says the existing
// rows read Missing (a non-NULL DEFAULT, whose encoding may be "").
type addedColumn struct {
	columnMeta
	missing bool
}

func (r *alterResult) log(format string, args ...any) {
	r.trace = append(r.trace, fmt.Sprintf(format, args...))
}

// addedValues returns the value every existing row reads for each added
// column, by name: its missing value, or NULL.
func (r *alterResult) addedValues() (map[string]Value, error) {
	out := make(map[string]Value, len(r.added))
	for _, c := range r.added {
		v := nullValue(c.Type)
		if c.missing {
			var err error
			if v, err = decodeStored(c.Missing, c.Type); err != nil {
				return nil, err
			}
		}
		out[c.Name] = v
	}
	return out, nil
}

// cloneMeta copies the parts of a table's metadata that ALTER TABLE changes.
func cloneMeta(m *tableMeta) *tableMeta {
	c := *m
	c.Columns = slices.Clone(m.Columns)
	c.RetiredFields = slices.Clone(m.RetiredFields)
	c.PendingCleanup = slices.Clone(m.PendingCleanup)
	c.Checks = slices.Clone(m.Checks)
	return &c
}

// testHookAltered, when set by a test, runs once alterTable has changed the
// index (step 3), before it commits.
var testHookAltered func(meta *tableMeta)

// alterTable runs the actions of an ALTER TABLE other than RENAME, one or
// several, as one change of the table's metadata:
//
//  1. Plan: the actions are applied in the order written to a copy of the
//     metadata as read (applyAlter), each seeing what the ones before it
//     did. Every error in a definition (a column that exists or doesn't, a
//     DEFAULT or CHECK that isn't valid) is found here.
//  2. The existing rows are checked against the new CHECK constraints, in
//     one scan, as the table will be after the change.
//  3. The new indexed columns are added to the search index, in one FT.ALTER
//     (which adds all of them or none).
//  4. Commit: in one WATCH/MULTI on the metadata (and on the row id counter
//     if a column gets a missing value), the actions are applied again to
//     the metadata as it is then, and the result written if they did the
//     same as planned, and the table still has the index of step 3 (TRUNCATE
//     moves a table to a new one); otherwise the statement fails ("try
//     again").
//  5. The fields of dropped columns are removed from the rows in the
//     background, as for a single DROP COLUMN.
//
// So an action that fails leaves the table as it was: metadata, index and
// rows. Nothing writes rows before the commit. If the commit itself fails
// (a concurrent change, or a lost connection), the attributes step 3 added
// stay in the index unused, which is harmless: no column reads them, and a
// later ADD COLUMN doesn't reuse their names.
func (e *executor) alterTable(ctx context.Context, meta *tableMeta, cmds []AlterCmd) error {
	// Index attributes that no column uses (left by a commit that failed)
	// can't be added again, so new columns don't take their names.
	var taken map[string]bool
	if slices.ContainsFunc(cmds, func(c AlterCmd) bool { return c.Action == AlterAddColumn }) {
		var err error
		if taken, err = e.store.indexAttributes(ctx, meta.index()); err != nil {
			return err
		}
	}
	plan := cloneMeta(meta)
	res, err := e.applyAlter(ctx, plan, cmds, taken)
	if err != nil {
		return err
	}
	if len(res.checks) > 0 {
		added, err := res.addedValues()
		if err != nil {
			return err
		}
		if err := e.validateChecks(ctx, meta, plan, res.checks, added); err != nil {
			return err
		}
	}
	args := []any{"FT.ALTER", meta.index(), "SCHEMA", "ADD"}
	n := 0
	for _, c := range res.added {
		if c.Indexed {
			args = append(args, indexAttrArgs(c.columnMeta)...)
			n++
		}
	}
	if n > 0 {
		what := "the column"
		if n > 1 {
			what = "the columns"
		}
		if err := e.store.searchDo(ctx, meta.index(), args...).Err(); err != nil {
			return wrapRedis(err, "failed to add "+what+" to the search index")
		}
	}
	if testHookAltered != nil {
		testHookAltered(meta)
	}
	// The rows that exist when a column with a missing value is added, those
	// with ids up to the last one allocated, read it (see defaults.go).
	withMissing := slices.ContainsFunc(res.added, func(c addedColumn) bool { return c.missing })
	err = e.store.updateTableTx(ctx, meta.Schema, meta.Name, withMissing, func(m *tableMeta, lastRowID int64) error {
		// TRUNCATE moves a table to a new index (truncateTables).
		if m.index() != meta.index() || m.prefix() != meta.prefix() {
			return errorf(adbc.StatusIO, "table %q changed concurrently; try again", m.Name)
		}
		cur := cloneMeta(m)
		again, err := e.applyAlter(ctx, m, cmds, taken)
		if err != nil {
			return err
		}
		if !slices.Equal(again.trace, res.trace) || !sameChecksColumns(meta, cur, res.checks) {
			return errorf(adbc.StatusIO, "table %q changed concurrently; try again", m.Name)
		}
		for _, a := range again.added {
			if !a.missing {
				continue
			}
			i := slices.IndexFunc(m.Columns, func(c columnMeta) bool { return c.field() == a.field() })
			if lastRowID == 0 {
				m.Columns[i].Missing = ""
			} else {
				m.Columns[i].MissingThrough = lastRowID
			}
		}
		return nil
	}, func(p goredis.Pipeliner) {
		if res.dropped {
			p.SAdd(ctx, cleanupKey, cleanupMember(meta.Schema, meta.Name))
		}
	})
	if err != nil {
		return err
	}
	if res.dropped {
		e.store.startCleanup(meta.Schema, meta.Name)
	}
	return nil
}

// applyAlter applies ALTER TABLE actions (not RENAME) to m in order. It
// reads nothing from Redis: alterTable runs it on the metadata as read, and
// again on the metadata as it is when the change commits. taken are index
// attributes that new columns must not use as their field.
func (e *executor) applyAlter(ctx context.Context, m *tableMeta, cmds []AlterCmd, taken map[string]bool) (*alterResult, error) {
	res := &alterResult{}
	had := len(m.Columns)
	for _, cmd := range cmds {
		var err error
		switch cmd.Action {
		case AlterAddColumn:
			err = e.alterAddColumn(ctx, m, cmd, taken, res)
		case AlterDropColumn:
			err = alterDropColumn(m, cmd, res)
		case AlterSetDefault, AlterDropDefault:
			err = e.alterDefault(ctx, m, cmd, res)
		case AlterAddConstraint:
			err = e.alterAddConstraint(ctx, m, cmd.Check, res)
		case AlterDropConstraint:
			err = alterDropConstraint(m, cmd, res)
		default:
			err = errorf(adbc.StatusNotImplemented, "unsupported ALTER TABLE action")
		}
		if err != nil {
			return nil, err
		}
	}
	// Checked at the end, so that DROP COLUMN a, ADD COLUMN b works on a
	// table whose only column is a.
	if len(m.Columns) == 0 {
		if had == 1 {
			return nil, errorf(adbc.StatusInvalidArgument, "cannot drop the only column of table %q", m.Name)
		}
		return nil, errorf(adbc.StatusInvalidArgument, "cannot drop all the columns of table %q", m.Name)
	}
	return res, nil
}

// alterAddColumn applies ADD COLUMN. The column gets a HASH field no
// current or dropped column has used. With a DEFAULT, existing rows read the
// default as it is now, recorded as the column's missing value (see
// defaults.go), so it must be the same for all of them.
func (e *executor) alterAddColumn(ctx context.Context, m *tableMeta, cmd AlterCmd, taken map[string]bool, res *alterResult) error {
	// The application may have the field in its HASHes, and missing
	// values count on row ids that the driver hands out.
	if err := refuseAdopted(m, "ADD COLUMN"); err != nil {
		return err
	}
	def := cmd.Def
	if err := checkColumnName(def.Name); err != nil {
		return err
	}
	if _, ok := m.resolve(def.Name); ok {
		if cmd.IfColumnNotExists {
			res.log("add %q: exists", def.Name)
			return nil
		}
		return errorf(adbc.StatusAlreadyExists, "column %q already exists in table %q", def.Name, m.Name)
	}
	col := columnMeta{Name: def.Name, Type: def.Type, Nullable: !def.NotNull, Comment: def.Comment}
	missing := false
	if def.Default != nil {
		if hasVolatile(def.Default) {
			return errorf(adbc.StatusNotImplemented, "ADD COLUMN with a volatile DEFAULT is not supported: existing rows would each need their own value")
		}
		text, v, err := e.checkDefault(ctx, def)
		if err != nil {
			return err
		}
		col.Default = text
		if !v.Null {
			col.Missing, missing = encodeStored(v), true
		}
	}
	if def.NotNull && !missing {
		return errorf(adbc.StatusNotImplemented, "cannot add a NOT NULL column without a non-NULL DEFAULT: existing rows would have no value")
	}
	if f := freeField(m, def.Name, taken); f != def.Name {
		col.Field = f
	}
	indexed := 0
	for _, c := range m.Columns {
		if c.Indexed {
			indexed++
		}
	}
	col.Indexed = indexable(col.Type) && !def.NoIndex && simpleName(col.field()) && indexed < maxIndexed
	// Rows written meanwhile by statements that read the metadata before get
	// the missing value (see addedMissing).
	col.initTags()
	if col.TagsChecked {
		col.TagValues = tagLevelOf(col.Missing)
	}
	col.NaNs = col.Indexed && col.Type.Kind.isFloat() && col.Missing == nanStored
	m.Columns = append(m.Columns, col)
	res.added = append(res.added, addedColumn{col, missing})
	res.log("add %q field %q type %s indexed %v missing %v %q default %q", col.Name, col.field(), col.Type.SQLName(), col.Indexed, missing, col.Missing, col.Default)
	// The column's CHECK constraints are defined on the table with it, and
	// the existing rows read its missing value.
	if len(def.Checks) > 0 {
		checks, err := e.defineChecks(ctx, m, def.Checks)
		if err != nil {
			return err
		}
		res.addChecks(m, checks)
	}
	return nil
}

// alterDropColumn applies DROP COLUMN. The column disappears from the
// metadata; its field is retired (never reused) and removed from the rows
// later. As in Postgres, the CHECK constraints that read it go too.
func alterDropColumn(m *tableMeta, cmd AlterCmd, res *alterResult) error {
	i, ok := m.resolve(cmd.Column)
	if !ok {
		if cmd.IfColumnExists {
			res.log("drop %q: missing", cmd.Column)
			return nil
		}
		return errorf(adbc.StatusNotFound, "column %q does not exist in table %q", cmd.Column, m.Name)
	}
	c := m.Columns[i]
	f := c.field()
	if j := slices.IndexFunc(res.added, func(a addedColumn) bool { return a.field() == f }); j >= 0 {
		// Added by this statement: no row has its field.
		res.added = slices.Delete(res.added, j, j+1)
	} else if m.Adopted {
		// An application's field: it stays in the HASHes.
		m.RetiredFields = append(m.RetiredFields, f)
	} else {
		if c.MissingThrough > 0 {
			m.PendingCleanup = append(m.PendingCleanup, nullMarker(f))
		}
		m.RetiredFields = append(m.RetiredFields, f)
		m.PendingCleanup = append(m.PendingCleanup, f)
		res.dropped = true
	}
	var gone []string
	m.Checks = slices.DeleteFunc(m.Checks, func(k checkMeta) bool {
		if !checkReads(k.Expr, c.Name) {
			return false
		}
		gone = append(gone, k.Name)
		res.dropCheck(k.Name)
		return true
	})
	m.Columns = slices.Delete(m.Columns, i, i+1)
	res.log("drop %q field %q checks %q", c.Name, f, gone)
	return nil
}

// alterDefault applies ALTER COLUMN … SET DEFAULT and DROP DEFAULT. Only
// rows inserted later see the change: existing rows keep their values (and
// missing values).
func (e *executor) alterDefault(ctx context.Context, m *tableMeta, cmd AlterCmd, res *alterResult) error {
	i, ok := m.resolve(cmd.Column)
	if !ok {
		return errorf(adbc.StatusNotFound, "column %q does not exist in table %q", cmd.Column, m.Name)
	}
	col := m.Columns[i]
	text := ""
	if cmd.Action == AlterSetDefault {
		def := cmd.Def
		def.Name, def.Type = col.Name, col.Type
		var err error
		if text, _, err = e.checkDefault(ctx, def); err != nil {
			return err
		}
	}
	m.Columns[i].Default = text
	res.log("default %q field %q: %q", col.Name, col.field(), text)
	return nil
}

// alterAddConstraint applies ADD CONSTRAINT. chk is nil for the PRIMARY KEY,
// UNIQUE and FOREIGN KEY constraints, which are accepted and ignored.
func (e *executor) alterAddConstraint(ctx context.Context, m *tableMeta, chk *CheckDef, res *alterResult) error {
	if chk == nil {
		res.log("constraint ignored")
		return nil
	}
	checks, err := e.defineChecks(ctx, m, []CheckDef{*chk})
	if err != nil {
		return err
	}
	res.addChecks(m, checks)
	return nil
}

// alterDropConstraint applies DROP CONSTRAINT, for CHECK constraints (the
// only ones kept).
func alterDropConstraint(m *tableMeta, cmd AlterCmd, res *alterResult) error {
	i := slices.IndexFunc(m.Checks, func(c checkMeta) bool { return strings.EqualFold(c.Name, cmd.Constraint) })
	if i < 0 {
		if cmd.IfConstraintExists {
			res.log("drop constraint %q: missing", cmd.Constraint)
			return nil
		}
		return errorf(adbc.StatusNotFound, "constraint %q of relation %q does not exist", cmd.Constraint, m.Name)
	}
	name := m.Checks[i].Name
	res.dropCheck(name)
	m.Checks = slices.Delete(m.Checks, i, i+1)
	res.log("drop constraint %q", name)
	return nil
}

// addChecks adds new CHECK constraints to m and records them.
func (r *alterResult) addChecks(m *tableMeta, checks []checkMeta) {
	m.Checks = append(m.Checks, checks...)
	r.checks = append(r.checks, checks...)
	for _, c := range checks {
		r.log("check %q: %s", c.Name, c.Expr)
	}
}

// dropCheck forgets a constraint added by the statement that is dropped
// again by it.
func (r *alterResult) dropCheck(name string) {
	r.checks = slices.DeleteFunc(r.checks, func(c checkMeta) bool { return strings.EqualFold(c.Name, name) })
}

// indexAttributes returns the names of a search index's attributes, lower
// case.
func (s *store) indexAttributes(ctx context.Context, index string) (map[string]bool, error) {
	reply, err := s.searchDo(ctx, index, "FT.INFO", index).Result()
	if err != nil {
		return nil, wrapRedis(err, "failed to read the search index")
	}
	out := map[string]bool{}
	info, _ := reply.([]any)
	for i := 0; i+1 < len(info); i += 2 {
		if k, _ := info[i].(string); k != "attributes" {
			continue
		}
		attrs, _ := info[i+1].([]any)
		for _, a := range attrs {
			parts, _ := a.([]any)
			for j := 0; j+1 < len(parts); j += 2 {
				if k, _ := parts[j].(string); k == "identifier" {
					if name, ok := parts[j+1].(string); ok {
						out[strings.ToLower(name)] = true
					}
				}
			}
		}
	}
	return out, nil
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
