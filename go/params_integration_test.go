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
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// paramTypes returns the types GetParameterSchema gives sql's parameters.
func (h *sqlHarness) paramTypes(sql string) ([]string, error) {
	h.t.Helper()
	schema, err := h.stmtWith(sql, nil).GetParameterSchema(h.ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		if f.Name != "" || !f.Nullable {
			h.t.Errorf("%s: parameter %d is %v, want unnamed and nullable", sql, i+1, f)
		}
		out[i] = f.Type.String()
	}
	return out, nil
}

func TestSQLParameterSchema(t *testing.T) {
	h := newSQLHarness(t)
	for _, tbl := range []string{"it_par_t", "it_par_s", "it_par_new"} {
		h.exec("DROP TABLE IF EXISTS " + tbl)
		t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS " + tbl) })
	}
	h.exec("CREATE TABLE it_par_t (id BIGINT NOT NULL, name VARCHAR(20), qty INTEGER, price NUMERIC(10,2), at TIMESTAMP, ok BOOLEAN)")
	h.exec("CREATE TABLE it_par_s (k BIGINT, label VARCHAR)")
	h.exec("INSERT INTO it_par_t VALUES (1, 'one', 5, 1.50, TIMESTAMP '2026-01-01 00:00:00', true)")

	// The parameters' expected types are their columns'.
	col := map[string]string{}
	for _, tbl := range []string{"it_par_t", "it_par_s"} {
		schema, err := h.conn.GetTableSchema(h.ctx, nil, nil, tbl)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range schema.Fields() {
			col[f.Name] = f.Type.String()
		}
	}
	str, i64, null, boolean := arrow.BinaryTypes.String.String(), arrow.PrimitiveTypes.Int64.String(),
		arrow.Null.String(), arrow.FixedWidthTypes.Boolean.String()

	for _, tc := range []struct {
		sql  string
		want []string
	}{
		// Comparisons, and IN, BETWEEN and ANY, which become comparisons.
		{"SELECT name FROM it_par_t WHERE id = $1", []string{col["id"]}},
		{"SELECT name FROM it_par_t WHERE id = ?", []string{col["id"]}},
		{"SELECT name FROM it_par_t WHERE $1 = qty AND price > $2", []string{col["qty"], col["price"]}},
		{"SELECT name FROM it_par_t WHERE qty BETWEEN $1 AND $2", []string{col["qty"], col["qty"]}},
		{"SELECT name FROM it_par_t WHERE id IN ($1, $2, $3)", []string{col["id"], col["id"], col["id"]}},
		{"SELECT name FROM it_par_t WHERE id IS DISTINCT FROM $1", []string{col["id"]}},
		{"SELECT name FROM it_par_t WHERE ok = $1 OR $2", []string{col["ok"], boolean}},
		{"SELECT name FROM it_par_t WHERE NOT $1", []string{boolean}},
		// Strings.
		{"SELECT name FROM it_par_t WHERE name LIKE $1", []string{str}},
		{"SELECT name FROM it_par_t WHERE name ILIKE $1 OR name SIMILAR TO $2", []string{str, str}},
		{"SELECT name || $1 FROM it_par_t", []string{str}},
		// Casts, arithmetic, and functions that return an argument.
		{"SELECT CAST($1 AS BIGINT), $2::VARCHAR", []string{i64, str}},
		{"SELECT qty * $1 FROM it_par_t", []string{col["qty"]}},
		{"SELECT COALESCE(name, $1) FROM it_par_t", []string{col["name"]}},
		{"SELECT CASE WHEN qty > $1 THEN name ELSE $2 END FROM it_par_t", []string{col["qty"], col["name"]}},
		{"SELECT CASE qty WHEN $1 THEN 'x' END FROM it_par_t", []string{col["qty"]}},
		// Subqueries: inside one, and its IN operand.
		{"SELECT name FROM it_par_t WHERE id IN (SELECT k FROM it_par_s WHERE label = $1)", []string{col["label"]}},
		{"SELECT name FROM it_par_t WHERE $1 IN (SELECT k FROM it_par_s)", []string{col["k"]}},
		// INSERT, UPDATE and DELETE: the target columns.
		{"INSERT INTO it_par_t (id, name, qty) VALUES ($1, $2, $3)", []string{col["id"], col["name"], col["qty"]}},
		{"INSERT INTO it_par_t VALUES (?, ?, ?, ?, ?, ?)",
			[]string{col["id"], col["name"], col["qty"], col["price"], col["at"], col["ok"]}},
		{"INSERT INTO it_par_t (id, qty) VALUES ($1, -$2)", []string{col["id"], col["qty"]}},
		{"INSERT INTO it_par_t (id, name) SELECT $1, label FROM it_par_s", []string{col["id"]}},
		{"UPDATE it_par_t SET qty = $1, price = $2 WHERE id = $3", []string{col["qty"], col["price"], col["id"]}},
		{"UPDATE it_par_t t SET name = $1 FROM it_par_s s WHERE s.k = t.id AND s.label = $2", []string{col["name"], col["label"]}},
		{"DELETE FROM it_par_t WHERE at < $1", []string{col["at"]}},
		{"DELETE FROM it_par_t USING it_par_s s WHERE s.k = it_par_t.id AND s.label = $1", []string{col["label"]}},
		{"INSERT INTO it_par_t (id) VALUES ($1) RETURNING id + $2", []string{col["id"], col["id"]}},
		// A type that can't be told, a gap, and no parameters.
		{"SELECT $1", []string{null}},
		{"SELECT $2 FROM it_par_t WHERE id = $2", []string{null, col["id"]}},
		{"SELECT 1", []string{}},
		// A script: its statements together; one that needs what an earlier
		// one makes can't be planned, and its parameters are NULL.
		{"UPDATE it_par_t SET qty = $1 WHERE id = $2; SELECT name FROM it_par_t WHERE id = $2", []string{col["qty"], col["id"]}},
		{"CREATE TABLE it_par_new (a BIGINT); INSERT INTO it_par_new VALUES ($1)", []string{null}},
	} {
		got, err := h.paramTypes(tc.sql)
		if err != nil {
			t.Errorf("%s: %v", tc.sql, err)
			continue
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s:\n got  %v\n want %v", tc.sql, got, tc.want)
		}
	}

	// The ADBC validation suite's statements: only the count is checked.
	for sql, n := range map[string]int{"SELECT 1 + ?": 1, "SELECT 1 + ? + ?": 2} {
		if got, err := h.paramTypes(sql); err != nil || len(got) != n {
			t.Errorf("%s: %v, %v; want %d parameters", sql, got, err, n)
		}
	}

	// A statement that can't be planned is an error.
	_, err := h.paramTypes("SELECT * FROM it_par_missing WHERE a = $1")
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusNotFound {
		t.Errorf("unknown table: err = %v, want NotFound", err)
	}

	// Describing a statement leaves it ready to run.
	st := h.stmtWith("SELECT name FROM it_par_t WHERE id = $1", nil)
	schema, err := st.GetParameterSchema(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecordBatch()
	defer rec.Release()
	if err := st.Bind(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	if !rdr.Next() || rdr.RecordBatch().Column(0).ValueStr(0) != "one" {
		t.Errorf("running the described statement didn't return 'one'")
	}
}
