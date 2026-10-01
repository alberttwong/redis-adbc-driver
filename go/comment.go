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

// Comments.
//
// COMMENT ON {TABLE | VIEW | COLUMN} … IS 'text' stores the text in the
// object's metadata (WATCH/MULTI on its metadata key): tableMeta.Comment,
// viewMeta.Comment or the column's columnMeta.Comment. NULL or '' removes
// it, as in Postgres. Being metadata, a comment stays with its object
// through RENAME TO and RENAME COLUMN, and goes away with DROP TABLE, DROP
// VIEW and DROP COLUMN. When adbc.redis.rename_rekey moves a table's rows,
// COMMENT ON isn't refused meanwhile: the switch (and a rollback) rewrites
// the metadata as it is then, comments included (see rekey.go). As in
// Postgres:
//
//   - COMMENT ON TABLE needs a table and COMMENT ON VIEW a view; COMMENT ON
//     COLUMN takes a column of either.
//   - CREATE OR REPLACE VIEW keeps the comments of the view it replaces: the
//     view's, and those of the columns that keep their names (Postgres keeps
//     the view's OID and column numbers, and doesn't let a column change its
//     name).
//   - CREATE TABLE … AS and CREATE VIEW copy no comments from what they read.
//
// CREATE TABLE and ADD COLUMN also take MySQL / Snowflake COMMENT clauses:
// col TYPE COMMENT 'text', and COMMENT [=] 'text' after the column list.
//
// Comments are read back through information_schema (tables.comment and
// columns.comment, NULL without one), GetObjects (a column's remarks; its
// schema has no field for a table's comment) and GetTableSchema (field
// metadata under remarksKey, the key Flight SQL uses for column remarks).

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
)

// remarksKey is the Arrow field metadata key of a column's comment in
// GetTableSchema.
const remarksKey = "ARROW:FLIGHT:SQL:REMARKS"

func (e *executor) runComment(ctx context.Context, st *CommentStmt) error {
	if st.Table.Catalog != "" && st.Table.Catalog != catalogName {
		return errorf(adbc.StatusNotImplemented, "cross-database references are not implemented: %s", st.name())
	}
	schema, name, err := e.resolveTable(st.Table)
	if err != nil {
		return relationNotFound(st.Table)
	}
	if isInfoSchema(schema) {
		return infoSchemaReadOnly()
	}
	isView, err := e.store.viewExists(ctx, schema, name)
	if err != nil {
		return err
	}
	// found is set once the metadata was read, so that only a missing
	// object (not a missing column) is reported as one.
	found := false
	if isView {
		if st.Object == CommentOnTable {
			return errorf(adbc.StatusInvalidArgument, "%q is not a table", name)
		}
		err = e.store.updateView(ctx, schema, name, func(v *viewMeta) error {
			found = true
			return setComment(st, &v.Comment, v.Columns, name)
		})
	} else {
		err = e.store.updateTable(ctx, schema, name, func(m *tableMeta) error {
			found = true
			if st.Object == CommentOnView {
				return errorf(adbc.StatusInvalidArgument, "%q is not a view", name)
			}
			return setComment(st, &m.Comment, m.Columns, name)
		}, nil)
	}
	var ae adbc.Error
	if found || !asAdbc(err, &ae) || ae.Code != adbc.StatusNotFound {
		return err
	}
	if st.Table.Schema != "" && !isTempAlias(st.Table.Schema) {
		if ok, err := e.store.schemaExists(ctx, schema); err != nil {
			return err
		} else if !ok {
			return errorf(adbc.StatusNotFound, "schema %q does not exist", st.Table.Schema)
		}
	}
	return relationNotFound(st.Table)
}

// name is the object's name as written.
func (st *CommentStmt) name() string {
	if st.Object == CommentOnColumn {
		return st.Table.String() + "." + st.Column
	}
	return st.Table.String()
}

// relationNotFound is Postgres's error for a COMMENT ON a table or view that
// doesn't exist.
func relationNotFound(t TableName) error {
	return errorf(adbc.StatusNotFound, "relation %q does not exist", t.String())
}

// setComment sets the comment of a table or view (rel), whose comment and
// columns are given, or of one of its columns.
func setComment(st *CommentStmt, comment *string, cols []columnMeta, rel string) error {
	if st.Object != CommentOnColumn {
		*comment = st.Text
		return nil
	}
	i, ok := resolveColumn(cols, st.Column)
	if !ok {
		return errorf(adbc.StatusNotFound, "column %q of relation %q does not exist", st.Column, rel)
	}
	cols[i].Comment = st.Text
	return nil
}

// keepComments gives a view v that replaces old (CREATE OR REPLACE VIEW)
// old's comments: the view's, and those of the columns that keep their
// names.
func keepComments(old, v *viewMeta) {
	v.Comment = old.Comment
	for i := range v.Columns {
		if j, ok := resolveColumn(old.Columns, v.Columns[i].Name); ok {
			v.Columns[i].Comment = old.Columns[j].Comment
		}
	}
}
