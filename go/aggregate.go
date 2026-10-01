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
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
)

// aggRequest describes one FT.AGGREGATE pipeline:
//
//	FT.AGGREGATE <index> <query>
//	  [APPLY ... GROUPBY n @field ... REDUCE ...]
//	  [LOAD 3k @field AS __sort<i> ... SORTBY 2k @__sort<i> ASC|DESC ... MAX m]
//	  [LIMIT offset num]
//	  [LOAD n @field ...] [LOAD *]
//	  WITHCURSOR COUNT <page>
//
// The fields are loaded last, so only the rows that survive sorting and
// limiting are read.
type aggRequest struct {
	index   string
	query   string
	groupBy []any     // raw APPLY/GROUPBY/REDUCE arguments
	sortBy  []sortKey // index attributes (not used with groupBy)
	limit   *[2]int64
	load    []string
	loadAll bool // also load every HASH field (LOAD *)
	width   int  // fields per result row, to size cursor pages (0 if small)
}

// pageRows returns the cursor page size.
func (r *aggRequest) pageRows() int {
	return max(1000, min(cursorCount, pageValues/max(r.width, 1)))
}

type sortKey struct {
	field string
	desc  bool
}

// aggRow is one result row: field name -> raw string value. Fields that are
// missing from the HASH (SQL NULL) are absent from the map.
type aggRow map[string]string

func (r *aggRequest) args(maxRows int64) []any {
	args := []any{"FT.AGGREGATE", r.index, r.query}
	args = append(args, r.groupBy...)
	if len(r.sortBy) > 0 {
		// Sort on copies of the keys. LOAD * replaces the fields with their
		// HASH strings, and a cluster coordinator merges the shards' sorted
		// results on those values, which would compare numbers as strings.
		args = append(args, "LOAD", 3*len(r.sortBy))
		for i, k := range r.sortBy {
			args = append(args, "@"+k.field, "AS", sortAlias(i))
		}
		args = append(args, "SORTBY", 2*len(r.sortBy))
		for i, k := range r.sortBy {
			dir := "ASC"
			if k.desc {
				dir = "DESC"
			}
			args = append(args, "@"+sortAlias(i), dir)
		}
		args = append(args, "MAX", max(maxRows, 1))
	}
	if r.limit != nil {
		args = append(args, "LIMIT", r.limit[0], r.limit[1])
	}
	if len(r.load) > 0 {
		args = append(args, "LOAD", len(r.load))
		for _, f := range r.load {
			args = append(args, "@"+f)
		}
	}
	if r.loadAll {
		args = append(args, "LOAD", "*")
	}
	args = append(args, "TIMEOUT", 0, "WITHCURSOR", "COUNT", r.pageRows(), "DIALECT", 2)
	return args
}

func sortAlias(i int) string { return fmt.Sprintf("__sort%d", i) }

// countMatches returns the number of documents matching the query.
func (s *store) countMatches(ctx context.Context, index, query string) (int64, error) {
	reply, err := s.searchDo(ctx, index, "FT.SEARCH", index, query, "LIMIT", 0, 0, "TIMEOUT", 0, "DIALECT", 2).Result()
	if err != nil {
		return 0, wrapRedis(err, "FT.SEARCH failed")
	}
	arr, ok := reply.([]any)
	if !ok || len(arr) == 0 {
		return 0, errorf(adbc.StatusInternal, "unexpected FT.SEARCH reply %T", reply)
	}
	n, ok := arr[0].(int64)
	if !ok {
		return 0, errorf(adbc.StatusInternal, "unexpected FT.SEARCH count %T", arr[0])
	}
	return n, nil
}

// aggregate runs the pipeline and reads every cursor page.
func (s *store) aggregate(ctx context.Context, req *aggRequest) ([]aggRow, error) {
	maxRows := int64(0)
	if len(req.sortBy) > 0 {
		if req.limit != nil {
			maxRows = req.limit[0] + req.limit[1]
		} else {
			n, err := s.countMatches(ctx, req.index, req.query)
			if err != nil {
				return nil, err
			}
			if n == 0 && len(req.groupBy) == 0 {
				return nil, nil
			}
			maxRows = n
		}
	}
	// The cursor lives on the node that ran the query, so every page is read
	// through the same connection.
	node, err := s.search(ctx, req.index)
	if err != nil {
		return nil, err
	}
	reply, err := node.Do(ctx, req.args(maxRows)...).Result()
	if err != nil {
		return nil, wrapRedis(err, "FT.AGGREGATE failed")
	}
	var rows []aggRow
	for {
		page, cursor, err := parseCursorReply(reply)
		if err != nil {
			return nil, err
		}
		rows = append(rows, page...)
		if cursor == 0 {
			return rows, nil
		}
		reply, err = node.Do(ctx, "FT.CURSOR", "READ", req.index, cursor, "COUNT", req.pageRows()).Result()
		if err != nil {
			_ = node.Do(ctx, "FT.CURSOR", "DEL", req.index, cursor).Err()
			return nil, wrapRedis(err, "FT.CURSOR READ failed")
		}
	}
}

// parseCursorReply parses a RESP2 WITHCURSOR reply: [[total, row...], cursor].
func parseCursorReply(reply any) ([]aggRow, int64, error) {
	outer, ok := reply.([]any)
	if !ok || len(outer) != 2 {
		return nil, 0, errorf(adbc.StatusInternal, "unexpected FT.AGGREGATE reply %T", reply)
	}
	cursor, ok := outer[1].(int64)
	if !ok {
		return nil, 0, errorf(adbc.StatusInternal, "unexpected cursor id %T", outer[1])
	}
	results, ok := outer[0].([]any)
	if !ok {
		return nil, 0, errorf(adbc.StatusInternal, "unexpected FT.AGGREGATE results %T", outer[0])
	}
	if len(results) == 0 {
		return nil, cursor, nil
	}
	rows := make([]aggRow, 0, len(results)-1)
	for _, r := range results[1:] {
		fields, ok := r.([]any)
		if !ok {
			return nil, 0, errorf(adbc.StatusInternal, "unexpected FT.AGGREGATE row %T", r)
		}
		row := make(aggRow, len(fields)/2)
		for i := 0; i+1 < len(fields); i += 2 {
			k, ok := fields[i].(string)
			if !ok {
				continue
			}
			switch v := fields[i+1].(type) {
			case string:
				row[k] = v
			case int64:
				row[k] = fmt.Sprint(v)
			}
		}
		rows = append(rows, row)
	}
	return rows, cursor, nil
}
