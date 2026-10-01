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

// Column defaults.
//
// A column's DEFAULT is stored as SQL text in the table metadata
// (columnMeta.Default) and checked when it is defined: it may not read
// columns, run subqueries, take parameters or call aggregates or window
// functions, and it is evaluated once so that a value the column can't hold
// is an error then. A NULL default is not stored, as in Postgres. Inserted
// rows that give a column no value (or DEFAULT) get its default: INSERT …
// VALUES, INSERT … DEFAULT VALUES, INSERT … SELECT with a column list,
// MERGE's INSERT and bulk ingest of columns the Arrow data lacks. Each
// default is evaluated once per statement, so every row of a statement gets
// the same CURRENT_TIMESTAMP (which is fixed per statement anyway); only a
// volatile default (RANDOM()) is evaluated for every row.
//
// Missing values
//
// ADD COLUMN … DEFAULT v doesn't rewrite the existing rows. As Postgres does
// with its "missing value", the column records v (Missing) and the row id
// high-water mark at that moment (MissingThrough, read in the transaction
// that adds the column), and a row with __rowid <= MissingThrough that has
// no field for the column reads v. Rows inserted afterwards have higher ids,
// so for them an absent field still means NULL. The rest keeps every read
// and write consistent with that:
//
//   - A statement that read the metadata before the column was added
//     doesn't write it, but it allocates its row ids in one transaction with
//     a read of the metadata (store.insertRows), so when its ids are above
//     the mark it sees the new column and writes v.
//   - Setting the column of such an old row to NULL (UPDATE, MERGE) can't
//     just delete the field: it also writes a marker field (nullMarker),
//     which setting a value removes again (and DROP COLUMN with the field).
//   - The index has no entry for an absent field, so a predicate that v
//     satisfies is widened to the rows with __rowid <= MissingThrough in the
//     index and re-checked on the rows (widenMissing in query.go), and the
//     index never sorts or aggregates such a column.
//   - TRUNCATE drops the missing values together with the rows.

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

// nullMarker is the HASH field that says a row's column was set to NULL,
// for a row that would otherwise read the column's missing value. Columns
// can't start with "__", so it is no column's field.
func nullMarker(field string) string { return "__null_" + field }

// missingValue returns the value a row whose HASH has no field for the
// column reads: its missing value if the row existed when the column was
// added (and hasn't set it to NULL since), otherwise NULL.
func (c columnMeta) missingValue(row aggRow) (Value, error) {
	if c.MissingThrough > 0 {
		if _, ok := row[nullMarker(c.field())]; !ok {
			if id, err := strconv.ParseInt(row[rowIDField], 10, 64); err == nil && id <= c.MissingThrough {
				return decodeStored(c.Missing, c.Type)
			}
		}
	}
	return nullValue(c.Type), nil
}

// readsMissing reports whether the row with the given key reads the
// column's missing value when its HASH has no field for it.
func (c columnMeta) readsMissing(meta *tableMeta, key string) bool {
	if c.MissingThrough == 0 {
		return false
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(key, meta.prefix()), 10, 64)
	return err == nil && id <= c.MissingThrough
}

// addedMissing returns the fields (name, value, …) that rows written with
// meta need for the columns with a missing value that the table's current
// metadata (raw JSON) has and meta doesn't: the rows know nothing about
// those columns, which were added after meta was read.
func addedMissing(meta *tableMeta, raw string) []any {
	if !strings.Contains(raw, `"missing_through"`) {
		return nil // the common case, without decoding the metadata
	}
	var cur tableMeta
	if json.Unmarshal([]byte(raw), &cur) != nil {
		return nil
	}
	known := map[string]bool{}
	for _, c := range meta.Columns {
		known[c.field()] = true
	}
	var out []any
	for _, c := range cur.Columns {
		if c.MissingThrough > 0 && !known[c.field()] {
			out = append(out, c.field(), c.Missing)
		}
	}
	return out
}

// dropMissingValues forgets the missing values of a table whose rows have
// all been deleted (TRUNCATE): rows inserted later, which may reuse the old
// row ids after RESTART IDENTITY, must read an absent field as NULL.
func (s *store) dropMissingValues(ctx context.Context, meta *tableMeta) error {
	has := false
	for _, c := range meta.Columns {
		has = has || c.MissingThrough > 0
	}
	if !has {
		return nil
	}
	return s.updateTable(ctx, meta.Schema, meta.Name, func(m *tableMeta) error {
		for i := range m.Columns {
			m.Columns[i].Missing, m.Columns[i].MissingThrough = "", 0
		}
		return nil
	}, nil)
}

// updateTableAtRowID is updateTable for a change that depends on the table's
// row id high-water mark (the last row id allocated, 0 if none): fn gets it,
// and the change only commits if no row ids were allocated meanwhile.
func (s *store) updateTableAtRowID(ctx context.Context, schema, table string, fn func(m *tableMeta, lastRowID int64) error) error {
	key, seq := metaKey(schema, table), seqKey(schema, table)
	for attempt := 0; attempt < 20; attempt++ {
		err := s.client.Watch(ctx, func(tx *goredis.Tx) error {
			m, err := s.getTableWith(ctx, tx, schema, table)
			if err != nil {
				return err
			}
			last, err := tx.Get(ctx, seq).Int64()
			if err != nil && err != goredis.Nil {
				return err
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
				return nil
			})
			return err
		}, key, seq)
		if err == goredis.TxFailedErr {
			continue // concurrent change or insert: retry
		}
		return wrapRedis(err, "failed to update table metadata")
	}
	return errorf(adbc.StatusIO, "table %q.%q is being changed concurrently; try again", displaySchema(schema), table)
}

// checkDefault checks the DEFAULT of a column definition (see the top of
// this file) and returns the text to store, "" for a NULL default, and the
// value it gave.
func (e *executor) checkDefault(ctx context.Context, def ColumnDef) (string, Value, error) {
	var err error
	walkExpr(def.Default, func(x Expr) {
		if err != nil {
			return
		}
		switch v := x.(type) {
		case *ColumnRef:
			err = errorf(adbc.StatusInvalidArgument, "cannot use column reference in DEFAULT expression")
		case *Subquery:
			err = errorf(adbc.StatusInvalidArgument, "cannot use subquery in DEFAULT expression")
		case *Param:
			err = errorf(adbc.StatusInvalidArgument, "cannot use parameter in DEFAULT expression")
		case *WindowFunc:
			err = errorf(adbc.StatusInvalidArgument, "window functions are not allowed in DEFAULT expressions")
		case *Func:
			if aggregateFuncs[v.Name] {
				err = errorf(adbc.StatusInvalidArgument, "aggregate functions are not allowed in DEFAULT expressions")
			} else {
				// Also the calls a CASE branch or IIF doesn't evaluate.
				err = checkCall(v)
			}
		}
	})
	if err != nil {
		return "", Value{}, err
	}
	v, err := e.newEnv(ctx, nil, nil).eval(def.Default)
	if err != nil {
		return "", Value{}, invalidArg(err)
	}
	cv, err := Coerce(v, def.Type)
	if err != nil {
		return "", Value{}, errorf(adbc.StatusInvalidArgument, "column %q: %v", def.Name, err)
	}
	if cv.Null && !hasVolatile(def.Default) {
		return "", cv, nil
	}
	return def.DefaultText, cv, nil
}

// columnDefaults computes the defaults of a table's columns for the rows of
// one statement.
type columnDefaults struct {
	meta  *tableMeta
	env   *evalEnv
	exprs []Expr  // parsed when first needed
	vals  []Value // the defaults computed once for the statement
	done  []bool
}

func (e *executor) columnDefaults(ctx context.Context, meta *tableMeta) *columnDefaults {
	n := len(meta.Columns)
	return &columnDefaults{meta: meta, env: e.newEnv(ctx, nil, nil),
		exprs: make([]Expr, n), vals: make([]Value, n), done: make([]bool, n)}
}

// value returns the default of column i, coerced to its type: evaluated
// once per statement, or for every row if it is volatile.
func (d *columnDefaults) value(i int) (Value, error) {
	c := d.meta.Columns[i]
	if c.Default == "" {
		return nullValue(c.Type), nil
	}
	if d.done[i] {
		return d.vals[i], nil
	}
	if d.exprs[i] == nil {
		x, err := parseExprText(c.Default)
		if err != nil {
			return Value{}, errorf(adbc.StatusInternal, "invalid DEFAULT %q of column %q: %v", c.Default, c.Name, err)
		}
		d.exprs[i] = x
	}
	v, err := d.env.eval(d.exprs[i])
	if err != nil {
		return Value{}, invalidArg(err)
	}
	cv, err := Coerce(v, c.Type)
	if err != nil {
		return Value{}, errorf(adbc.StatusInvalidArgument, "column %q: %v", c.Name, err)
	}
	if !hasVolatile(d.exprs[i]) {
		d.vals[i], d.done[i] = cv, true
	}
	return cv, nil
}

// fill sets the columns of an inserted row that were given no value (given
// is false) to their defaults.
func (d *columnDefaults) fill(row []Value, given []bool) error {
	for i := range row {
		if given[i] {
			continue
		}
		v, err := d.value(i)
		if err != nil {
			return err
		}
		row[i] = v
	}
	return nil
}
