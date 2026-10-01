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

// Integration tests for result column names after ALTER TABLE … RENAME
// COLUMN, which keeps the column's original HASH field: results must use the
// new name everywhere.

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

// expectColumns runs a query and checks the names of its result columns
// (cols, as "a|b|…") and its rows, in order.
func (h *sqlHarness) expectColumns(sql, cols string, want ...string) {
	h.t.Helper()
	schema := h.expectRows(sql, want...)
	if names := fieldNames(schema); names != cols {
		h.t.Errorf("%s\n columns: %q, want %q", sql, names, cols)
	}
}

// expectTableSchema checks the column names GetTableSchema reports.
func (h *sqlHarness) expectTableSchema(table, cols string) {
	h.t.Helper()
	schema, err := h.conn.GetTableSchema(h.ctx, nil, nil, table)
	if err != nil {
		h.t.Fatal(err)
	}
	if names := fieldNames(schema); names != cols {
		h.t.Errorf("GetTableSchema(%s) columns: %q, want %q", table, names, cols)
	}
}

func TestSQLRenamedColumnNames(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_ren_v")
		h.exec("DROP VIEW IF EXISTS it_ren_va")
		for _, n := range []string{"it_ren", "it_ren_o", "it_ren_c"} {
			h.exec("DROP TABLE IF EXISTS " + n)
		}
	}
	drop()
	t.Cleanup(drop)
	h.exec(`CREATE TABLE it_ren (a INTEGER, b VARCHAR, n INTEGER)`)
	h.exec(`INSERT INTO it_ren VALUES (1, 'x', 10), (2, 'y', 20), (2, 'z', 30)`)
	h.exec(`ALTER TABLE it_ren RENAME COLUMN a TO renamed`)
	h.exec(`CREATE TABLE it_ren_o (oid INTEGER, note VARCHAR)`)
	h.exec(`INSERT INTO it_ren_o VALUES (1, 'one'), (2, 'two')`)
	h.exec(`ALTER TABLE it_ren_o RENAME COLUMN oid TO rid`)

	// Plain selects: *, the column itself (in any case, qualified), rel.*.
	h.expectColumns(`SELECT * FROM it_ren ORDER BY n`, "renamed|b|n", "1|x|10", "2|y|20", "2|z|30")
	h.expectColumns(`SELECT renamed FROM it_ren ORDER BY n`, "renamed", "1", "2", "2")
	h.expectColumns(`SELECT RENAMED, it_ren.b FROM it_ren WHERE renamed = 1`, "renamed|b", "1|x")
	h.expectColumns(`SELECT r.renamed, r.* FROM it_ren r WHERE r.renamed = 1`, "renamed|renamed|b|n", "1|1|x|10")
	h.expectColumns(`SELECT renamed AS a FROM it_ren WHERE n = 10`, "a", "1")

	// Joins, with a renamed column on each side.
	h.expectColumns(`SELECT * FROM it_ren r JOIN it_ren_o o ON o.rid = r.renamed ORDER BY r.n`,
		"renamed|b|n|rid|note", "1|x|10|1|one", "2|y|20|2|two", "2|z|30|2|two")
	h.expectColumns(`SELECT o.*, r.renamed FROM it_ren r JOIN it_ren_o o ON o.rid = r.renamed WHERE r.n = 10`,
		"rid|note|renamed", "1|one|1")
	h.expectColumns(`SELECT renamed, rid FROM it_ren r LEFT JOIN it_ren_o o ON o.rid = r.renamed WHERE r.n = 20`,
		"renamed|rid", "2|2")

	// GROUP BY, computed by the index (FT.AGGREGATE) and by the driver.
	grouped := `SELECT renamed, COUNT(*) AS c, SUM(n) AS s FROM it_ren GROUP BY renamed ORDER BY renamed`
	if _, _, indexAgg := h.planOf(grouped); !indexAgg {
		t.Errorf("%s: not aggregated by the index", grouped)
	}
	h.expectColumns(grouped, "renamed|c|s", "1|1|10", "2|2|50")
	grouped = `SELECT renamed, COUNT(*) AS c FROM it_ren WHERE LENGTH(b) = 1 GROUP BY 1 ORDER BY 1`
	if _, residual, indexAgg := h.planOf(grouped); indexAgg || !residual {
		t.Errorf("%s: residual = %v, aggregated by the index = %v; want a driver aggregation", grouped, residual, indexAgg)
	}
	h.expectColumns(grouped, "renamed|c", "1|1", "2|2")
	h.expectColumns(`SELECT DISTINCT renamed FROM it_ren ORDER BY renamed`, "renamed", "1", "2")
	h.expectColumns(`SELECT renamed, COUNT(*) AS c FROM it_ren GROUP BY ROLLUP (renamed) ORDER BY GROUPING(renamed), renamed`,
		"renamed|c", "1|1", "2|2", "NULL|3")
	h.expectColumns(`SELECT renamed, ROW_NUMBER() OVER (PARTITION BY renamed ORDER BY n) AS rn FROM it_ren ORDER BY n`,
		"renamed|rn", "1|1", "2|1", "2|2")

	// CTEs, derived tables, set operations.
	h.expectColumns(`WITH c AS (SELECT * FROM it_ren) SELECT * FROM c WHERE n = 10`, "renamed|b|n", "1|x|10")
	h.expectColumns(`SELECT * FROM (SELECT renamed, b FROM it_ren) d WHERE d.renamed = 1`, "renamed|b", "1|x")
	h.expectColumns(`SELECT renamed FROM it_ren WHERE n = 10 UNION ALL SELECT renamed FROM it_ren WHERE n = 20`,
		"renamed", "1", "2")

	// Views over the table: a simple one (scanned lazily) and an aggregate
	// one (materialized).
	h.exec(`CREATE VIEW it_ren_v AS SELECT * FROM it_ren`)
	h.exec(`CREATE VIEW it_ren_va AS SELECT renamed, COUNT(*) AS c FROM it_ren GROUP BY renamed`)
	h.expectColumns(`SELECT * FROM it_ren_v WHERE n = 10`, "renamed|b|n", "1|x|10")
	h.expectColumns(`SELECT * FROM it_ren_va ORDER BY renamed`, "renamed|c", "1|1", "2|2")
	h.expectTableSchema("it_ren_v", "renamed|b|n")
	h.expectRows(`SELECT column_name FROM information_schema.columns WHERE table_name = 'it_ren_v' ORDER BY ordinal_position`,
		"renamed", "b", "n")

	// CREATE TABLE AS: the new table's columns, and its HASH fields, take the
	// new name.
	h.exec(`CREATE TABLE it_ren_c AS SELECT * FROM it_ren`)
	h.expectTableSchema("it_ren_c", "renamed|b|n")
	h.expectColumns(`SELECT * FROM it_ren_c WHERE renamed = 1`, "renamed|b|n", "1|x|10")
	if f := h.rowFields(h.rawClient(), "public", "it_ren_c"); f["renamed"] != 3 || f["a"] != 0 {
		t.Errorf("CTAS row fields = %v, want 'renamed' fields", f)
	}

	// Metadata of the renamed table.
	h.expectTableSchema("it_ren", "renamed|b|n")
	h.expectRows(`SELECT column_name FROM information_schema.columns WHERE table_name = 'it_ren' ORDER BY ordinal_position`,
		"renamed", "b", "n")
	table := "it_ren"
	objs := h.objects(adbc.ObjectDepthColumns, nil, &table)
	if want := fmt.Sprintf("%s.public.it_ren TABLE renamed b n", catalogName); !slices.Contains(objs, want) {
		t.Errorf("GetObjects = %q, want %q", strings.Join(objs, ", "), want)
	}

	// RETURNING, also over a join.
	h.expectReturning(`UPDATE it_ren SET n = n + 1 WHERE renamed = 1 RETURNING *`, "renamed|b|n", "1|x|11")
	h.expectReturning(`UPDATE it_ren r SET n = n - 1 FROM it_ren_o o WHERE o.rid = r.renamed AND o.note = 'one' RETURNING r.renamed, o.*`,
		"renamed|rid|note", "1|1|one")
}
