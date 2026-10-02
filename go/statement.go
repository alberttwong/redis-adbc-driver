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
	"slices"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func asAdbc(err error, target *adbc.Error) bool { return errors.As(err, target) }

type statementImpl struct {
	driverbase.StatementImplBase

	conn   *connectionImpl
	query  string
	parsed []ParsedStmt
	ingest driverbase.BulkIngestOptions

	// indexColumns restricts which columns bulk ingest indexes (nil = all
	// indexable columns).
	indexColumns map[string]bool
	// aggPushdown overrides the connection's aggregate pushdown mode.
	aggPushdown string

	// Bound parameters (or ingest data).
	params array.RecordReader
	closed bool
}

func (s *statementImpl) Base() *driverbase.StatementImplBase { return &s.StatementImplBase }

func (s *statementImpl) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case OptionStringIngestIndexColumns:
		if s.indexColumns == nil {
			return "*", nil
		}
		names := make([]string, 0, len(s.indexColumns))
		for n := range s.indexColumns {
			names = append(names, n)
		}
		slices.Sort(names)
		return strings.Join(names, ","), nil
	case OptionStringAggregatePushdown:
		return s.executor().pushdown, nil
	}
	return s.StatementImplBase.GetOption(ctx, key)
}

func (s *statementImpl) executor() *executor {
	e := s.conn.executor()
	if s.aggPushdown != "" {
		e.pushdown = s.aggPushdown
	}
	return e
}

func (s *statementImpl) checkOpen() error {
	if s.closed {
		return errorf(adbc.StatusInvalidState, "statement is closed")
	}
	return nil
}

func (s *statementImpl) clearParams() {
	if s.params != nil {
		s.params.Release()
		s.params = nil
	}
}

func (s *statementImpl) Close(ctx context.Context) error {
	if s.closed {
		return errorf(adbc.StatusInvalidState, "statement already closed")
	}
	s.closed = true
	s.clearParams()
	return nil
}

func (s *statementImpl) SetOption(ctx context.Context, key, val string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		s.query, s.parsed = "", nil
	case OptionStringIngestIndexColumns:
		if val == "" || val == "*" {
			s.indexColumns = nil
			return nil
		}
		s.indexColumns = map[string]bool{}
		for _, name := range strings.Split(val, ",") {
			if name = strings.TrimSpace(name); name != "" {
				s.indexColumns[name] = true
			}
		}
		return nil
	case OptionStringAggregatePushdown:
		if err := validatePushdown(val); err != nil {
			return err
		}
		s.aggPushdown = val
		return nil
	}
	handled, err := s.ingest.SetOption(&s.ErrorHelper, key, val)
	if err != nil {
		return err
	}
	if handled {
		return nil
	}
	return s.StatementImplBase.SetOption(ctx, key, val)
}

func (s *statementImpl) SetSqlQuery(ctx context.Context, query string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.query = query
	s.parsed = nil
	s.ingest.TableName = ""
	s.clearParams()
	return nil
}

func (s *statementImpl) parse() ([]ParsedStmt, error) {
	if s.parsed != nil {
		return s.parsed, nil
	}
	if s.query == "" {
		return nil, errorf(adbc.StatusInvalidState, "no query has been set")
	}
	parsed, err := ParseScript(s.query)
	if err != nil {
		return nil, invalidArg(err)
	}
	s.parsed = parsed
	return parsed, nil
}

func (s *statementImpl) Prepare(ctx context.Context) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if s.ingest.TableName != "" {
		return nil
	}
	_, err := s.parse()
	return err
}

func (s *statementImpl) SetSubstraitPlan(ctx context.Context, plan []byte) error {
	return errorf(adbc.StatusNotImplemented, "Substrait is not supported")
}

func (s *statementImpl) Bind(ctx context.Context, values arrow.RecordBatch) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.clearParams()
	// The reader retains the batch; the caller keeps (and releases) its own
	// reference.
	rdr, err := array.NewRecordReader(values.Schema(), []arrow.RecordBatch{values})
	if err != nil {
		return errorf(adbc.StatusInvalidArgument, "failed to bind: %v", err)
	}
	s.params = rdr
	return nil
}

func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	if err := s.checkOpen(); err != nil {
		stream.Release()
		return err
	}
	s.clearParams()
	s.params = stream
	return nil
}

// GetParameterSchema describes the statement's parameters, in order: $1,
// $2, … and ? numbered as they appear. A parameter's type is the one it is
// used as (see params.go), or NULL if it can't be told; names are empty.
func (s *statementImpl) GetParameterSchema(ctx context.Context) (*arrow.Schema, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if s.ingest.TableName != "" {
		return nil, errorf(adbc.StatusInvalidState, "a bulk ingest has no parameters")
	}
	parsed, err := s.parse()
	if err != nil {
		return nil, err
	}
	types, err := s.executor().parameterTypes(ctx, parsed)
	if err != nil {
		return nil, err
	}
	fields := make([]arrow.Field, len(types))
	for i, t := range types {
		fields[i] = arrow.Field{Type: t.ArrowType(), Nullable: true}
	}
	return arrow.NewSchema(fields, nil), nil
}

func (s *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, errorf(adbc.StatusNotImplemented, "ExecutePartitions is not supported")
}

// paramRows drains the bound parameters into rows of values. It returns nil
// (not an empty slice) when nothing is bound.
func (s *statementImpl) paramRows() ([][]Value, []ColType, error) {
	if s.params == nil {
		return nil, nil, nil
	}
	defer s.clearParams()
	schema := s.params.Schema()
	types := make([]ColType, schema.NumFields())
	for i, f := range schema.Fields() {
		if f.Type.ID() == arrow.NULL {
			types[i] = typeNull
			continue
		}
		t, err := colTypeFromArrow(f.Type)
		if err != nil {
			return nil, nil, errorf(adbc.StatusNotImplemented, "parameter %d: %v", i+1, err)
		}
		types[i] = t
	}
	rows := [][]Value{}
	for s.params.Next() {
		rec := s.params.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make([]Value, rec.NumCols())
			for c := 0; c < int(rec.NumCols()); c++ {
				v, err := valueAt(rec.Column(c), r)
				if err != nil {
					return nil, nil, errorf(adbc.StatusInvalidArgument, "parameter %d: %v", c+1, err)
				}
				row[c] = v
			}
			rows = append(rows, row)
		}
	}
	if err := s.params.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, nil, errorf(adbc.StatusIO, "failed to read parameters: %v", err)
	}
	return rows, types, nil
}

// run executes every statement of the script, once per bound parameter row.
func (s *statementImpl) run(ctx context.Context) (execResult, error) {
	parsed, err := s.parse()
	if err != nil {
		return execResult{}, err
	}
	paramRows, paramTypes, err := s.paramRows()
	if err != nil {
		return execResult{}, err
	}
	exec := s.executor()
	// A script of several statements is one transaction for SET LOCAL, as
	// in Postgres; what it set ends with it unless a BEGIN is open.
	exec.script = len(parsed) > 1
	defer exec.sess.endScript()
	var last execResult
	for _, ps := range parsed {
		if paramRows == nil {
			if ps.NumParams > 0 {
				return execResult{}, errorf(adbc.StatusInvalidState, "query has %d parameter(s) but none are bound", ps.NumParams)
			}
			last, err = exec.execute(ctx, ps, nil, nil)
			if err != nil {
				return execResult{}, err
			}
			continue
		}
		if len(paramTypes) < ps.NumParams {
			return execResult{}, errorf(adbc.StatusInvalidArgument, "query has %d parameter(s) but %d are bound", ps.NumParams, len(paramTypes))
		}
		combined := execResult{affected: 0}
		_, isShow := ps.Stmt.(*ShowStmt)
		if _, ok := ps.Stmt.(*SelectStmt); ok || isShow || hasReturning(ps.Stmt) {
			// Establish the result schema even when no rows are bound.
			exec.cache = newExecCache()
			exec.paramTypes = paramTypes
			if len(paramRows) > 0 {
				exec.params = paramRows[0]
			}
			cols, _, err := exec.resultColumns(ctx, ps.Stmt, paramTypes)
			if err != nil {
				return execResult{}, err
			}
			combined.isQuery = true
			combined.cols = cols
		}
		for _, row := range paramRows {
			res, err := exec.execute(ctx, ps, row, paramTypes)
			if err != nil {
				return execResult{}, err
			}
			if res.isQuery {
				combined.rows = append(combined.rows, res.rows...)
			}
			if res.affected >= 0 && combined.affected >= 0 {
				combined.affected += res.affected
			} else {
				combined.affected = -1
			}
		}
		last = combined
	}
	return last, nil
}

func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if err := s.checkOpen(); err != nil {
		return nil, -1, err
	}
	if s.ingest.TableName != "" {
		n, err := s.executeIngest(ctx)
		if err != nil {
			return nil, -1, err
		}
		rdr, err := recordsReader(s.conn.Alloc, nil, nil)
		return rdr, n, err
	}
	res, err := s.run(ctx)
	if err != nil {
		return nil, -1, err
	}
	if !res.isQuery {
		rdr, err := recordsReader(s.conn.Alloc, nil, nil)
		return rdr, res.affected, err
	}
	rdr, err := recordsReader(s.conn.Alloc, res.cols, res.rows)
	if err != nil {
		return nil, -1, invalidArg(err)
	}
	return rdr, int64(len(res.rows)), nil
}

func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if err := s.checkOpen(); err != nil {
		return -1, err
	}
	if s.ingest.TableName != "" {
		return s.executeIngest(ctx)
	}
	res, err := s.run(ctx)
	if err != nil {
		return -1, err
	}
	return res.affected, nil
}

func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	parsed, err := s.parse()
	if err != nil {
		return nil, err
	}
	var paramTypes []ColType
	if s.params != nil {
		for _, f := range s.params.Schema().Fields() {
			t, err := colTypeFromArrow(f.Type)
			if err != nil {
				t = typeNull
			}
			paramTypes = append(paramTypes, t)
		}
	}
	cols, ok, err := s.executor().resultColumns(ctx, parsed[len(parsed)-1].Stmt, paramTypes)
	if err != nil {
		return nil, err
	}
	if !ok {
		return arrow.NewSchema(nil, nil), nil
	}
	return resultSchema(cols), nil
}
