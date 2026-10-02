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
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Streamed results.
//
// A query result is normally built whole before ExecuteQuery returns. A
// query that reads one table through its index, row by row, is streamed
// instead: its rows are read from the FT.AGGREGATE cursor a page at a time
// as the result is read, so the driver holds about one batch of them.
// These queries stream:
//   - a single SELECT (not a script), run with no parameters or one row of
//     them;
//   - on a table (not a view, CTE, derived table or join), matched through
//     its index rather than looked up by key;
//   - without aggregates, GROUP BY, DISTINCT, set operations, window
//     functions, or correlated subqueries;
//   - in insertion order, or ordered by indexed columns the index sorts
//     exactly (see prepareScan).
//
// Other queries are read whole, as before. LIMIT and OFFSET apply as the
// rows stream when they can't run in the index.
//
// ExecuteQuery reads the first batch before it returns, so errors in it are
// reported there. If that is all of the result, it returns it whole with its
// row count; otherwise it returns a stream, and -1 rows. An error later on
// ends the stream: Next returns false and Err reports it.
//
// Releasing the stream deletes its cursor, as does closing the connection
// (after which reading the stream fails). So does cancelling the context
// given to ExecuteQuery, which the stream reads with. Redis deletes a cursor
// that isn't read for its idle time (300 seconds by default).

const defaultStreamBatchRows = 65536

// streamSettings are adbc.redis.stream_results and stream_batch_rows.
type streamSettings struct {
	off       bool // stream_results=false
	batchRows int  // 0: defaultStreamBatchRows
}

func (s streamSettings) rows() int {
	if s.batchRows > 0 {
		return s.batchRows
	}
	return defaultStreamBatchRows
}

func (s streamSettings) get(key string) string {
	if key == OptionStringStreamResults {
		return strconv.FormatBool(!s.off)
	}
	return strconv.Itoa(s.rows())
}

func (s *streamSettings) set(key, value string) error {
	if key == OptionStringStreamResults {
		on, err := parseStreamResults(value)
		if err == nil {
			s.off = !on
		}
		return err
	}
	n, err := parseStreamBatchRows(value)
	if err == nil {
		s.batchRows = n
	}
	return err
}

// streamOpts asks execute to stream a query's rows if it can.
type streamOpts struct {
	// cols are the result's columns, or nil for the plan's.
	cols      []resultColumn
	batchRows int
	mem       memory.Allocator
	conn      *connectionImpl
}

// streamable reports whether a query's plan allows streaming its rows,
// before prepareScan decides how they are read.
func (p *selectPlan) streamable() bool {
	return !p.noRows && !p.distinct && p.setop == nil && p.grouping == nil && p.meta != nil && !p.meta.isMem &&
		!p.aggregate && p.having == nil && len(p.outerAggs) == 0 && !p.windowed() && len(p.extraNeed) == 0 &&
		!p.hasCorrelated()
}

// hasCorrelated reports whether the query holds a correlated subquery,
// which runs once per distinct outer row and remembers each result.
func (p *selectPlan) hasCorrelated() bool {
	found := false
	visit := func(x Expr) {
		walkExprPruned(x, func(x Expr) bool {
			if sq, ok := x.(*Subquery); ok && sq.correlated {
				found = true
			}
			return !found
		})
	}
	for _, it := range p.items {
		visit(it.expr)
	}
	visit(p.sel.Where)
	for _, o := range p.order {
		visit(o.expr)
	}
	return found
}

// streamSelect runs a query whose plan is streamable. It reads the first
// batch of rows, and returns the result whole if that was all of it, or
// else a stream of it.
func (e *executor) streamSelect(ctx context.Context, plan *selectPlan, params []Value, so *streamOpts) (execResult, error) {
	sc, err := e.prepareScan(ctx, plan, params)
	if err != nil {
		return execResult{}, err
	}
	if sc.req.where.keys != nil || sc.req.where.none || !sc.indexSort {
		rows, err := e.selectScanned(ctx, plan, sc, params)
		if err != nil {
			return execResult{}, err
		}
		return e.fitResult(execResult{isQuery: true, cols: plan.columns(), rows: rows, affected: int64(len(rows))}, nil)
	}
	cols := so.cols
	if cols == nil {
		cols = plan.columns()
	}
	sctx, cancel := context.WithCancelCause(ctx)
	it, err := e.openScan(sctx, sc.req, params)
	if err != nil {
		cancel(nil)
		return execResult{}, err
	}
	r := &streamReader{
		ctx: sctx, cancel: cancel, exec: e, cache: e.cache, items: plan.items, it: it,
		env: e.newEnv(sctx, plan.meta.types(), params), zone: e.zone(),
		cols: cols, schema: resultSchema(cols), mem: so.mem, batchRows: so.batchRows, left: -1,
	}
	r.refs.Store(1)
	if !sc.pushLimit {
		if off := plan.sel.Offset; off != nil {
			r.skip = *off
		}
		if lim := plan.sel.Limit; lim != nil {
			r.left = *lim
		}
	}
	if err := r.fill(); err != nil {
		r.closeLocked()
		return execResult{}, err
	}
	if r.exhausted {
		rows := r.pending
		r.pending = nil
		r.closeLocked()
		return execResult{isQuery: true, cols: cols, rows: rows, affected: int64(len(rows))}, nil
	}
	r.conn = so.conn
	r.conn.addStream(r)
	return execResult{isQuery: true, cols: cols, stream: r, affected: -1}, nil
}

// streamReader is a streamed result (see the top of this file). Next and
// Release may be called on another goroutine than ExecuteQuery's, but not
// at the same time as each other.
type streamReader struct {
	refs atomic.Int64

	// mu guards everything below. Connection.Close takes it to close the
	// stream.
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelCauseFunc
	exec   *executor
	cache  *execCache // the statement's, for checkReads
	conn   *connectionImpl
	it     *scanIter
	items  []planItem
	env    *evalEnv
	zone   tzZone

	cols      []resultColumn
	schema    *arrow.Schema
	mem       memory.Allocator
	batchRows int
	// skip is the OFFSET still to skip and left the LIMIT still to return
	// (-1: none), when they don't run in the index.
	skip, left int64

	// pending are rows read and evaluated but not returned yet, and
	// exhausted is set once the scan has no more.
	pending   [][]Value
	exhausted bool
	cur       arrow.RecordBatch
	err       error
	closed    bool
}

// fill reads pages until a batch of rows is pending, or the rows end.
func (r *streamReader) fill() error {
	for !r.exhausted && len(r.pending) < r.batchRows {
		_, rows, err := r.it.next(r.ctx)
		if errors.Is(err, io.EOF) {
			r.exhausted = true
			break
		}
		if err != nil {
			return err
		}
		for _, vals := range rows {
			if r.skip > 0 {
				r.skip--
				continue
			}
			if r.left == 0 {
				break
			}
			r.env.row = vals
			row, err := evalItems(r.env, r.items)
			if err != nil {
				return err
			}
			if err := fitRow(row, r.cols, r.zone); err != nil {
				return err
			}
			r.pending = append(r.pending, row)
			if r.left > 0 {
				r.left--
			}
		}
		if r.left == 0 {
			// The LIMIT is reached: the rest of the cursor isn't needed.
			r.exhausted = true
			r.it.close(r.ctx)
		}
	}
	return nil
}

func (r *streamReader) Retain() { r.refs.Add(1) }

func (r *streamReader) Release() {
	if r.refs.Add(-1) != 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	defer func() { _ = recover() }() // a panic must not cross a cgo callback
	r.closeLocked()
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
}

func (r *streamReader) Schema() *arrow.Schema { return r.schema }

func (r *streamReader) Next() (ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer func() {
		if p := recover(); p != nil {
			r.err = errorf(adbc.StatusInternal, "failed to read the result: %v", p)
			ok = false
			func() {
				defer func() { _ = recover() }()
				r.closeLocked()
			}()
		}
	}()
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.closed {
		return false
	}
	if err := r.ctx.Err(); err != nil {
		r.fail(err)
		return false
	}
	if len(r.pending) < r.batchRows {
		if err := r.fill(); err != nil {
			r.fail(err)
			return false
		}
	}
	if len(r.pending) == 0 {
		// The end: check that the tables weren't re-keyed meanwhile (see
		// rekey.go), as a statement does once it is done.
		if err := r.exec.store.checkReads(r.ctx, r.cache.loaded); err != nil {
			r.err = err
		}
		r.closeLocked()
		return false
	}
	n := min(len(r.pending), r.batchRows)
	rec, err := buildRecord(r.mem, r.cols, r.pending[:n])
	if err != nil {
		r.fail(invalidArg(err))
		return false
	}
	clear(r.pending[:n])
	r.pending = r.pending[n:]
	r.cur = rec
	return true
}

func (r *streamReader) RecordBatch() arrow.RecordBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cur
}

// Record is RecordBatch.
//
// Deprecated: Use RecordBatch instead.
func (r *streamReader) Record() arrow.RecordBatch { return r.RecordBatch() }

func (r *streamReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// fail ends the stream with err, or why its context ended.
func (r *streamReader) fail(err error) {
	if r.ctx.Err() != nil {
		var ae adbc.Error
		if cause := context.Cause(r.ctx); errors.As(cause, &ae) {
			err = cause // closeWith's
		} else {
			err = errorf(adbc.StatusCancelled, "reading the result was cancelled: %v", cause)
		}
	}
	r.err = err
	r.closeLocked()
}

// closeLocked deletes the cursor and forgets the pending rows. The current
// batch stays until Next or Release.
func (r *streamReader) closeLocked() {
	if r.closed {
		return
	}
	r.closed = true
	r.it.close(r.ctx)
	r.pending = nil
	r.cancel(nil)
	if r.conn != nil {
		r.conn.removeStream(r)
	}
}

// closeWith closes the stream for a reason other than its end, which a
// later Next reports. It first cancels a Next under way.
func (r *streamReader) closeWith(err error) {
	r.cancel(err)
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.err = err
		r.closeLocked()
	}
}

// streams are a connection's open streams, closed with it.
type streams struct {
	mu   sync.Mutex
	open map[*streamReader]bool
}

func (c *connectionImpl) addStream(r *streamReader) {
	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()
	if c.streams.open == nil {
		c.streams.open = map[*streamReader]bool{}
	}
	c.streams.open[r] = true
}

func (c *connectionImpl) removeStream(r *streamReader) {
	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()
	delete(c.streams.open, r)
}

// closeStreams closes the connection's open streams, deleting their
// cursors. A stream's lock is taken after the connection's is released, as
// a closing stream takes them in the other order.
func (c *connectionImpl) closeStreams() {
	c.streams.mu.Lock()
	open := make([]*streamReader, 0, len(c.streams.open))
	for r := range c.streams.open {
		open = append(open, r)
	}
	c.streams.mu.Unlock()
	for _, r := range open {
		r.closeWith(errorf(adbc.StatusInvalidState, "the connection was closed while its result was being read"))
	}
}
