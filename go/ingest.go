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

import (
	"context"
	"errors"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
)

// metaFromArrow derives table metadata from an Arrow schema.
func metaFromArrow(schemaName, table string, schema *arrow.Schema) (*tableMeta, error) {
	meta := &tableMeta{Schema: schemaName, Name: table}
	for _, f := range schema.Fields() {
		t, err := colTypeFromArrow(f.Type)
		if err != nil {
			return nil, errorf(adbc.StatusNotImplemented, "column %q: %v", f.Name, err)
		}
		meta.Columns = append(meta.Columns, columnMeta{Name: f.Name, Type: t, Nullable: f.Nullable})
	}
	return meta, nil
}

// executeIngest implements bulk ingestion: every Arrow row becomes a HASH.
func (s *statementImpl) executeIngest(ctx context.Context) (int64, error) {
	opts := s.ingest
	if s.params == nil {
		return -1, errorf(adbc.StatusInvalidState, "bulk ingest requires data to be bound")
	}
	defer s.clearParams()
	if opts.Temporary {
		return -1, errorf(adbc.StatusNotImplemented, "temporary tables are not supported")
	}
	if opts.CatalogName != "" && opts.CatalogName != catalogName {
		return -1, errorf(adbc.StatusNotFound, "catalog %q does not exist", opts.CatalogName)
	}
	schemaName := opts.SchemaName
	if schemaName == "" {
		schemaName = s.conn.schema
	}
	st := s.conn.store
	arrowSchema := s.params.Schema()
	wanted, err := metaFromArrow(schemaName, opts.TableName, arrowSchema)
	if err != nil {
		return -1, err
	}
	if err := wanted.applyIndexPolicy(s.indexColumns, nil); err != nil {
		return -1, err
	}

	var meta *tableMeta
	switch opts.Mode {
	case adbc.OptionValueIngestModeCreate:
		if _, err := st.createTable(ctx, wanted, false); err != nil {
			return -1, err
		}
		meta = wanted
	case adbc.OptionValueIngestModeReplace:
		if err := st.dropTable(ctx, schemaName, opts.TableName, true); err != nil {
			return -1, err
		}
		if _, err := st.createTable(ctx, wanted, false); err != nil {
			return -1, err
		}
		meta = wanted
	case adbc.OptionValueIngestModeAppend:
		meta, err = st.getTable(ctx, schemaName, opts.TableName)
		if err != nil {
			return -1, err
		}
	case adbc.OptionValueIngestModeCreateAppend:
		created, err := st.createTable(ctx, wanted, true)
		if err != nil {
			return -1, err
		}
		if created {
			meta = wanted
		} else if meta, err = st.getTable(ctx, schemaName, opts.TableName); err != nil {
			return -1, err
		}
	default:
		return -1, errorf(adbc.StatusInvalidArgument, "unknown ingest mode %q", opts.Mode)
	}

	// Map each Arrow field onto a table column.
	mapping := make([]int, arrowSchema.NumFields())
	for i, f := range arrowSchema.Fields() {
		idx, ok := meta.resolve(f.Name)
		if !ok {
			return -1, errorf(adbc.StatusAlreadyExists, "column %q does not exist in table %q", f.Name, meta.Name)
		}
		mapping[i] = idx
	}

	var total int64
	for s.params.Next() {
		rec := s.params.RecordBatch()
		n := int(rec.NumRows())
		rows := make([][]Value, n)
		for r := 0; r < n; r++ {
			row := make([]Value, len(meta.Columns))
			for i, c := range meta.Columns {
				row[i] = nullValue(c.Type)
			}
			rows[r] = row
		}
		for c := 0; c < int(rec.NumCols()); c++ {
			col := meta.Columns[mapping[c]]
			arr := rec.Column(c)
			for r := 0; r < n; r++ {
				v, err := valueAt(arr, r)
				if err != nil {
					return -1, errorf(adbc.StatusInvalidArgument, "column %q: %v", col.Name, err)
				}
				cv, err := Coerce(v, col.Type)
				if err != nil {
					return -1, errorf(adbc.StatusInvalidArgument, "column %q: %v", col.Name, err)
				}
				rows[r][mapping[c]] = cv
			}
		}
		written, err := st.insertRows(ctx, meta, rows)
		if err != nil {
			return -1, err
		}
		total += written
	}
	if err := s.params.Err(); err != nil && !errors.Is(err, io.EOF) {
		return -1, errorf(adbc.StatusIO, "failed to read ingest data: %v", err)
	}
	return total, nil
}
