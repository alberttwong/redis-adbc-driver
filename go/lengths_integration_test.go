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

// Integration tests for VARCHAR(n) / CHAR(n) lengths (lengths.go).

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// execErr runs a statement and returns its error.
func (h *sqlHarness) execErr(sql string) error {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	_, err = st.ExecuteUpdate(h.ctx)
	return err
}

// expectTooLong checks that a statement fails with Postgres's error for a
// value too long for typ, with ADBC status InvalidData.
func (h *sqlHarness) expectTooLong(sql, typ string) {
	h.t.Helper()
	err := h.execErr(sql)
	var ae adbc.Error
	switch {
	case err == nil:
		h.t.Errorf("%s: expected an error", sql)
	case !errors.As(err, &ae) || ae.Code != adbc.StatusInvalidData:
		h.t.Errorf("%s: error %v, want status InvalidData", sql, err)
	case ae.Msg != "[redis] value too long for type "+typ:
		h.t.Errorf("%s: error %q, want %q", sql, ae.Msg, "value too long for type "+typ)
	}
}

// columnSizes lists GetObjects' columns of a table as
// "name type size octets" (NULL for a missing size).
func (h *sqlHarness) columnSizes(table string) []string {
	h.t.Helper()
	rdr, err := h.conn.GetObjects(h.ctx, adbc.ObjectDepthColumns, nil, nil, &table, nil, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	defer rdr.Release()
	str := func(p *int32) string {
		if p == nil {
			return "NULL"
		}
		return fmt.Sprint(*p)
	}
	var out []string
	for rdr.Next() {
		var buf bytes.Buffer
		if err := array.RecordToJSON(rdr.RecordBatch(), &buf); err != nil {
			h.t.Fatal(err)
		}
		dec := json.NewDecoder(&buf)
		for dec.More() {
			var cat struct {
				Schemas []struct {
					Tables []struct {
						Name    string `json:"table_name"`
						Columns []struct {
							Name   string `json:"column_name"`
							Type   string `json:"xdbc_type_name"`
							Size   *int32 `json:"xdbc_column_size"`
							Octets *int32 `json:"xdbc_char_octet_length"`
						} `json:"table_columns"`
					} `json:"db_schema_tables"`
				} `json:"catalog_db_schemas"`
			}
			if err := dec.Decode(&cat); err != nil {
				h.t.Fatal(err)
			}
			for _, s := range cat.Schemas {
				for _, t := range s.Tables {
					if t.Name != table {
						continue
					}
					for _, c := range t.Columns {
						out = append(out, fmt.Sprintf("%s %s %s %s", c.Name, c.Type, str(c.Size), str(c.Octets)))
					}
				}
			}
		}
	}
	return out
}

// Explicit casts cut to the length in characters, and CHAR(n) pads.
func TestSQLLengthCasts(t *testing.T) {
	h := newSQLHarness(t)

	// At the boundary, with CAST and ::.
	h.expectRows(`SELECT CAST('ab' AS VARCHAR(3)), CAST('abc' AS VARCHAR(3)), CAST('abcd' AS VARCHAR(3)),
		'abcdef'::varchar(3), CAST('abcdef' AS CHARACTER VARYING(3)), CAST('abcdef' AS CHAR VARYING(2))`,
		"ab|abc|abc|abc|abc|ab")
	h.expectRows(`SELECT CAST('ab' AS CHAR(3)), CAST('abc' AS CHAR(3)), CAST('abcd' AS CHAR(3)),
		'abcdef'::character(2), CAST('abcdef' AS BPCHAR(4)), CAST('abcdef' AS NCHAR(1))`,
		"ab |abc|abc|ab|abcd|a")
	// CHAR is CHAR(1); BPCHAR, VARCHAR, VARCHAR(MAX) and TEXT have no length.
	h.expectRows(`SELECT CAST('abc' AS CHAR), CAST('abc' AS CHARACTER), CAST('abc ' AS BPCHAR), CAST('abcdef' AS VARCHAR),
		CAST('abcdef' AS VARCHAR(MAX)), CAST('abcdef' AS TEXT), CAST('abcdef' AS NVARCHAR(4))`,
		"a|a|abc |abcdef|abcdef|abcdef|abcd")
	// Characters, not bytes: 2- and 3-byte characters and an emoji.
	h.expectRows(`SELECT CAST('ééé€x' AS VARCHAR(4)), LENGTH(CAST('ééé€x' AS VARCHAR(4))),
		CAST('日本語テキスト' AS VARCHAR(3)), CAST('😀😀😀' AS VARCHAR(2)), CAST('é' AS CHAR(3)), LENGTH(CAST('é' AS CHAR(3)))`,
		"ééé€|4|日本語|😀😀|é  |1")
	// NULL stays NULL; non-strings are cut as text; TRY_CAST cuts too.
	h.expectRows(`SELECT CAST(NULL AS VARCHAR(3)), CAST(NULL AS CHAR(3)), CAST(12345 AS VARCHAR(3)), CAST(1.5 AS CHAR(5)),
		CAST(DATE '2024-01-02' AS VARCHAR(4)), TRY_CAST('abcdef' AS VARCHAR(3)), CAST(true AS CHAR(2))`,
		"NULL|NULL|123|1.5  |2024|abc|tr")
	// Trailing spaces are cut like any other character by a cast.
	h.expectRows(`SELECT CAST('ab    ' AS VARCHAR(3)), LENGTH(CAST('ab    ' AS VARCHAR(3))), CAST('ab    ' AS CHAR(4))`,
		"ab |3|ab  ")
	// CHAR to text drops the padding; CHAR to a shorter CHAR cuts.
	h.expectRows(`SELECT CAST(CAST('ab' AS CHAR(5)) AS VARCHAR(4)), CAST(CAST('ab' AS CHAR(5)) AS VARCHAR),
		CAST(CAST('ab' AS CHAR(5)) AS CHAR(3)), CAST(CAST('abcde' AS CHAR(5)) AS CHAR(3))`,
		"ab|ab|ab |abc")
	// Trailing spaces of a CHAR value don't count: length, comparisons,
	// functions and || see 'ab'; LIKE sees the padded 'ab '.
	h.expectRows(`SELECT LENGTH(CAST('ab' AS CHAR(3))), CAST('ab' AS CHAR(3)) = 'ab', CAST('ab' AS CHAR(3)) = 'ab  ',
		CAST('ab' AS CHAR(3)) = CAST('ab' AS CHAR(5)), CAST('ab' AS CHAR(3)) < 'ab!', CAST('ab' AS CHAR(3)) || 'x',
		UPPER(CAST('ab' AS CHAR(3))), CONCAT(CAST('ab' AS CHAR(3)), '|'), REPLACE(CAST('ab' AS CHAR(3)), ' ', '_')`,
		"2|true|true|true|true|abx|AB|ab||ab")
	h.expectRows(`SELECT CAST('ab' AS CHAR(3)) LIKE 'ab', CAST('ab' AS CHAR(3)) LIKE 'ab%', CAST('ab' AS CHAR(3)) LIKE 'ab_',
		CAST('ab' AS CHAR(3)) ~ 'b $', CAST('ab' AS CHAR(3)) SIMILAR TO 'ab'`,
		"false|true|true|true|false")
	// Text comparisons keep their trailing spaces.
	h.expectRows(`SELECT 'ab' = 'ab ', CAST('ab ' AS VARCHAR(5)) = 'ab', LENGTH(CAST('ab ' AS VARCHAR(5)))`, "false|false|3")
	// Lengths out of range, as in Postgres.
	for sql, want := range map[string]string{
		`SELECT CAST('a' AS VARCHAR(0))`:         "length for type varchar must be at least 1",
		`SELECT CAST('a' AS CHAR(0))`:            "length for type char must be at least 1",
		`SELECT 'a'::varchar(10485761)`:          "length for type varchar cannot exceed 10485760",
		`SELECT 'a'::char(10485761)`:             "length for type char cannot exceed 10485760",
		`SELECT CAST('a' AS VARCHAR(2, 1))`:      "invalid type modifier for type varchar",
		`CREATE TABLE it_len_bad (s varchar(0))`: "length for type varchar must be at least 1",
	} {
		h.expectError(sql, want)
	}
	h.expectRows(`SELECT 'abc'::varchar(10485760), 'abc'::char(1)`, "abc|a")
}

// Each write path rejects a value too long for its column with Postgres's
// error and writes nothing; spaces beyond the length are cut.
func TestSQLLengthWrites(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_len_w", "it_len_src", "it_len_nn")
	h.exec(`CREATE TABLE it_len_w (id integer, v varchar(3), c char(3), t text)`)
	h.exec(`INSERT INTO it_len_w VALUES (1, 'ab', 'ab', 'abcdef'), (2, 'abc', 'abc', NULL), (3, NULL, NULL, NULL)`)
	all := []string{"1|ab|ab |abcdef", "2|abc|abc|NULL", "3|NULL|NULL|NULL"}
	expectAll := func() {
		t.Helper()
		h.expectRows(`SELECT id, v, c, t FROM it_len_w ORDER BY id`, all...)
	}
	expectAll()

	h.exec(`CREATE TABLE it_len_src (id integer, s text)`)
	h.exec(`INSERT INTO it_len_src VALUES (1, 'xy'), (9, 'long!')`)
	merge := `MERGE INTO it_len_w t USING it_len_src s ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET v = s.s
		WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.s)`
	for _, tc := range []struct{ sql, typ string }{
		{`INSERT INTO it_len_w VALUES (4, 'abcd', 'a', 'x')`, "character varying(3)"},
		{`INSERT INTO it_len_w (id, c) VALUES (4, 'abcd')`, "character(3)"},
		{`INSERT INTO it_len_w (id, v) VALUES (4, 'éééé')`, "character varying(3)"},
		{`INSERT INTO it_len_w (id, v) VALUES (4, 'abc d')`, "character varying(3)"},
		// A later row fails: no row is written.
		{`INSERT INTO it_len_w VALUES (4, 'a', 'a', 'x'), (5, 'abcd', 'a', 'x')`, "character varying(3)"},
		{`INSERT INTO it_len_w SELECT 4, t, 'a', t FROM it_len_w WHERE id = 1`, "character varying(3)"},
		{`INSERT INTO it_len_w (id, c) SELECT id + 10, t FROM it_len_w`, "character(3)"},
		{`UPDATE it_len_w SET v = 'abcd' WHERE id = 1`, "character varying(3)"},
		{`UPDATE it_len_w SET c = t WHERE id = 1`, "character(3)"},
		// Row 1 fits ('abx'), row 2 doesn't ('abcx'): neither is written.
		{`UPDATE it_len_w SET v = v || 'x'`, "character varying(3)"},
		{`UPDATE it_len_w SET v = n.s FROM (SELECT 1 AS id, 'wxyz' AS s) AS n WHERE it_len_w.id = n.id`, "character varying(3)"},
		{`UPDATE it_len_w u SET c = s.s FROM it_len_src s WHERE u.id = 2 AND s.id = 9`, "character(3)"},
		// MERGE: the INSERT branch fails, so the UPDATE of row 1 isn't
		// written either.
		{merge, "character varying(3)"},
	} {
		h.expectTooLong(tc.sql, tc.typ)
		expectAll()
	}
	// MERGE's UPDATE branch fails.
	h.exec(`UPDATE it_len_src SET s = 'wxyz' WHERE id = 1`)
	h.exec(`UPDATE it_len_src SET s = 'ok' WHERE id = 9`)
	h.expectTooLong(merge, "character varying(3)")
	expectAll()
	h.exec(`UPDATE it_len_src SET s = 'xy ' WHERE id = 1`)
	if n := h.exec(merge); n != 2 {
		t.Errorf("MERGE changed %d rows", n)
	}
	all = []string{"1|xy |ab |abcdef", "2|abc|abc|NULL", "3|NULL|NULL|NULL", "9|ok|NULL|NULL"}
	expectAll()

	// Spaces beyond the length are cut to the length (VARCHAR keeps the
	// ones within it); shorter CHAR values are padded.
	h.exec(`INSERT INTO it_len_w VALUES (4, 'ab    ', 'ab    ', 'x  '), (5, 'abc  ', 'abc   ', NULL), (6, 'é  ', 'é', NULL)`)
	h.expectRows(`SELECT id, v, LENGTH(v), c, LENGTH(c), t FROM it_len_w WHERE id IN (4, 5, 6) ORDER BY id`,
		"4|ab |3|ab |2|x  ", "5|abc|3|abc|3|NULL", "6|é  |3|é  |1|NULL")
	h.exec(`UPDATE it_len_w SET v = 'xyz   ', c = 'q     ' WHERE id = 5`)
	h.expectRows(`SELECT v, c FROM it_len_w WHERE id = 5`, "xyz|q  ")
	// RETURNING shows the values as written.
	h.expectRows(`INSERT INTO it_len_w (id, v, c) VALUES (7, 'a     ', 'b') RETURNING v, c, LENGTH(v)`, "a  |b  |3")
	h.expectRows(`UPDATE it_len_w SET c = 'z' WHERE id = 7 RETURNING c`, "z  ")
	h.expectRows(`SELECT COUNT(*) FROM it_len_w`, "8")

	// Lengths are checked before NOT NULL, as in Postgres.
	h.exec(`CREATE TABLE it_len_nn (a varchar(2) not null, b varchar(2))`)
	h.expectTooLong(`INSERT INTO it_len_nn VALUES (NULL, 'abc')`, "character varying(2)")
	h.exec(`INSERT INTO it_len_nn VALUES ('a', 'b')`)
	h.expectTooLong(`UPDATE it_len_nn SET a = NULL, b = 'abc'`, "character varying(2)")
	h.expectError(`UPDATE it_len_nn SET a = NULL, b = 'ab'`, `NULL value in column "a" violates not-null constraint`)
	h.expectRows(`SELECT a, b FROM it_len_nn`, "a|b")

	// Parameters are checked like literals.
	mem := memory.DefaultAllocator
	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	sb.AppendValues([]string{"abcd"}, nil)
	col := sb.NewArray()
	defer col.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String}}, nil), []arrow.Array{col}, 1)
	defer rec.Release()
	_, err := h.stmtWith(`INSERT INTO it_len_w (id, v) VALUES (8, ?)`, rec).ExecuteUpdate(h.ctx)
	if err == nil || !strings.Contains(err.Error(), "value too long for type character varying(3)") {
		t.Errorf("INSERT of a too-long parameter: %v", err)
	}
	h.expectRows(`SELECT COUNT(*) FROM it_len_w`, "8")
}

// Defaults: a default too long for its column is an error when it is
// defined; one that fits is cut like a value and padded for CHAR.
func TestSQLLengthDefaults(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_len_d", "it_len_dbad")
	for sql, typ := range map[string]string{
		`CREATE TABLE it_len_dbad (v varchar(3) default 'abcd')`:            "character varying(3)",
		`CREATE TABLE it_len_dbad (v char(2) default repeat('x', 3))`:       "character(2)",
		`CREATE TABLE it_len_dbad (v varchar(2) default 'a' || 'bc')`:       "character varying(2)",
		`CREATE TABLE it_len_dbad (id integer, v varchar(2) default 'é é')`: "character varying(2)",
	} {
		h.expectTooLong(sql, typ)
	}
	h.expectRows(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'it_len_dbad'`, "0")

	h.exec(`CREATE TABLE it_len_d (id integer, v varchar(3) default 'ab   ', c char(3) default 'x')`)
	h.exec(`INSERT INTO it_len_d (id) VALUES (1)`)
	h.exec(`INSERT INTO it_len_d VALUES (2, DEFAULT, DEFAULT)`)
	h.exec(`INSERT INTO it_len_d DEFAULT VALUES`)
	h.expectRows(`SELECT id, v, LENGTH(v), c FROM it_len_d ORDER BY id`, "1|ab |3|x  ", "2|ab |3|x  ", "NULL|ab |3|x  ")
	h.expectTooLong(`ALTER TABLE it_len_d ALTER COLUMN v SET DEFAULT 'wxyz'`, "character varying(3)")
	h.expectTooLong(`ALTER TABLE it_len_d ADD COLUMN w char(2) DEFAULT 'abc'`, "character(2)")
	h.expectRows(`SELECT column_name, column_default FROM information_schema.columns WHERE table_name = 'it_len_d' ORDER BY ordinal_position`,
		"id|NULL", "v|'ab   '", "c|'x'")
	// The rows that exist when a column is added read its default, padded.
	h.exec(`ALTER TABLE it_len_d ADD COLUMN w char(4) DEFAULT 'ab'`)
	h.exec(`INSERT INTO it_len_d (id) VALUES (4)`)
	h.expectRows(`SELECT id, w, LENGTH(w) FROM it_len_d ORDER BY id`, "1|ab  |2", "2|ab  |2", "4|ab  |2", "NULL|ab  |2")
	h.expectRows(`SELECT COUNT(*) FROM it_len_d WHERE w = 'ab'`, "4")
	h.expectTooLong(`INSERT INTO it_len_d (id, w) VALUES (5, 'abcde')`, "character(4)")
}

// CHAR(n) columns hold padded values whose trailing spaces don't count in
// comparisons, also when the index answers them, or in joins, IN, GROUP BY
// and DISTINCT.
func TestSQLLengthChar(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_len_ch", "it_len_ch5")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_len_ch (id integer, c char(3), t text)`)
	h.exec(`INSERT INTO it_len_ch VALUES (1, 'ab', 'ab'), (2, 'abc', 'ab '), (3, 'a', 'a'), (4, NULL, NULL), (5, 'ab ', 'x')`)
	h.expectRows(`SELECT id, c, LENGTH(c) FROM it_len_ch ORDER BY id`, "1|ab |2", "2|abc|3", "3|a  |1", "4|NULL|NULL", "5|ab |2")

	for where, want := range map[string][]string{
		"c = 'ab'":                       {"1", "5"},
		"c = 'ab  '":                     {"1", "5"},
		"c = CAST('ab' AS CHAR(5))":      {"1", "5"},
		"c <> 'ab'":                      {"2", "3"},
		"c = 'abc'":                      {"2"},
		"c > 'ab'":                       {"2"},
		"c IN ('a', 'abc')":              {"2", "3"},
		"c IN ('a ', 'zz', 'q', 'r')":    {"3"},
		"c LIKE 'ab'":                    nil,
		"c LIKE 'ab%'":                   {"1", "2", "5"},
		"c LIKE 'ab_'":                   {"1", "2", "5"},
		"c LIKE '%b '":                   {"1", "5"},
		"c = t":                          {"1", "3"},
		"t = c":                          {"1", "3"},
		"c IN (SELECT t FROM it_len_ch)": {"1", "3", "5"},
	} {
		h.expectRows(`SELECT id FROM it_len_ch WHERE `+where+` ORDER BY id`, want...)
	}
	h.expectRows(`SELECT o.id FROM it_len_ch o WHERE EXISTS (SELECT 1 FROM it_len_ch x WHERE x.t = o.c AND x.id <> o.id) ORDER BY o.id`,
		"1", "5")
	// c = 'ab' looks up the tag 'ab' and checks the rows.
	for sql, want := range map[string]string{
		`SELECT id FROM it_len_ch WHERE c = 'ab'`:   "@c:{ab} residual",
		`SELECT id FROM it_len_ch WHERE c = 'ab  '`: "@c:{ab} residual",
		`SELECT id FROM it_len_ch WHERE c = 'abc'`:  "@c:{abc} residual",
	} {
		q, residual := h.pushedWhere(raw, sql)
		if residual {
			q += " residual"
		}
		if q != want {
			t.Errorf("%s: pushed down %q, want %q", sql, q, want)
		}
	}
	h.expectRows(`SELECT c, COUNT(*) FROM it_len_ch GROUP BY c ORDER BY c`, "a  |1", "ab |2", "abc|1", "NULL|1")
	h.expectRows(`SELECT COUNT(*) FROM it_len_ch WHERE c = 'ab'`, "2")
	h.expectRows(`SELECT DISTINCT c FROM it_len_ch WHERE c IS NOT NULL ORDER BY c`, "a  ", "ab ", "abc")
	h.expectRows(`SELECT MIN(c), MAX(c), COUNT(DISTINCT c) FROM it_len_ch`, "a  |abc|3")
	h.expectRows(`SELECT id, c || '|', UPPER(c), COALESCE(c, 'zz'), NULLIF(c, 'ab') FROM it_len_ch ORDER BY id`,
		"1|ab||AB|ab |NULL", "2|abc||ABC|abc|abc", "3|a||A|a  |a  ", "4|NULL|NULL|zz|NULL", "5|ab||AB|ab |NULL")
	h.expectRows(`SELECT id, c FROM it_len_ch ORDER BY c DESC, id LIMIT 3`, "2|abc", "1|ab ", "5|ab ")

	// Joins compare without trailing spaces when a side is CHAR, the text
	// side too: t = 'ab ' (row 2) matches c = 'ab', where Postgres compares
	// a CHAR with a text column as text.
	h.expectRows(`SELECT a.id, b.id FROM it_len_ch a JOIN it_len_ch b ON a.c = b.t ORDER BY a.id, b.id`,
		"1|1", "1|2", "3|3", "5|1", "5|2")
	h.exec(`CREATE TABLE it_len_ch5 (id integer, c char(5))`)
	h.exec(`INSERT INTO it_len_ch5 VALUES (10, 'ab'), (11, 'abc  '), (12, 'b')`)
	h.expectRows(`SELECT a.id, b.id, b.c FROM it_len_ch a JOIN it_len_ch5 b ON a.c = b.c ORDER BY a.id, b.id`,
		"1|10|ab   ", "2|11|abc  ", "5|10|ab   ")
	h.expectRows(`SELECT a.id FROM it_len_ch a LEFT JOIN it_len_ch5 b USING (c) WHERE b.id IS NULL ORDER BY a.id`, "3", "4")
	// Set operations: CHAR(3) and CHAR(5) values compare equal.
	h.expectRows(`SELECT COUNT(*) FROM (SELECT c FROM it_len_ch WHERE id = 1 UNION SELECT c FROM it_len_ch5 WHERE id = 10) u`, "1")
	h.expectRows(`SELECT c FROM it_len_ch WHERE id = 2 INTERSECT SELECT c FROM it_len_ch5`, "abc")
}

// Bulk ingest into VARCHAR(n) / CHAR(n) columns: a batch with a value too
// long writes nothing.
func TestSQLLengthIngest(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_len_ing")
	h.exec(`CREATE TABLE it_len_ing (id bigint, v varchar(3), c char(2))`)
	ingest := func(ids []int64, vs, cs []string) error {
		t.Helper()
		mem := memory.DefaultAllocator
		ib, vb, cb := array.NewInt64Builder(mem), array.NewStringBuilder(mem), array.NewStringBuilder(mem)
		defer ib.Release()
		defer vb.Release()
		defer cb.Release()
		ib.AppendValues(ids, nil)
		vb.AppendValues(vs, nil)
		cb.AppendValues(cs, nil)
		idCol, vCol, cCol := ib.NewArray(), vb.NewArray(), cb.NewArray()
		defer idCol.Release()
		defer vCol.Release()
		defer cCol.Release()
		rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "v", Type: arrow.BinaryTypes.String},
			{Name: "c", Type: arrow.BinaryTypes.String},
		}, nil), []arrow.Array{idCol, vCol, cCol}, int64(len(ids)))
		defer rec.Release()
		st, err := h.conn.NewStatement(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close(h.ctx)
		for k, v := range map[string]string{
			adbc.OptionKeyIngestTargetTable: "it_len_ing",
			adbc.OptionKeyIngestMode:        adbc.OptionValueIngestModeAppend,
		} {
			if err := st.SetOption(h.ctx, k, v); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.Bind(h.ctx, rec); err != nil {
			t.Fatal(err)
		}
		_, err = st.ExecuteUpdate(h.ctx)
		return err
	}
	if err := ingest([]int64{1, 2, 3}, []string{"ab", "abc  ", "日本語"}, []string{"x", "yz", "é "}); err != nil {
		t.Fatal(err)
	}
	all := []string{"1|ab|x ", "2|abc|yz", "3|日本語|é "}
	h.expectRows(`SELECT id, v, c FROM it_len_ing ORDER BY id`, all...)
	for _, tc := range []struct {
		vs, cs []string
		typ    string
	}{
		{[]string{"a", "abcd"}, []string{"a", "b"}, "character varying(3)"},
		{[]string{"a", "b"}, []string{"a", "xyz"}, "character(2)"},
		{[]string{"日本語!", "b"}, []string{"a", "b"}, "character varying(3)"},
	} {
		err := ingest([]int64{4, 5}, tc.vs, tc.cs)
		var ae adbc.Error
		if !errors.As(err, &ae) || ae.Code != adbc.StatusInvalidData || ae.Msg != "[redis] value too long for type "+tc.typ {
			t.Errorf("ingest of %q / %q: %v", tc.vs, tc.cs, err)
		}
		h.expectRows(`SELECT id, v, c FROM it_len_ing ORDER BY id`, all...)
	}
}

// The declared length is in the metadata: GetObjects,
// information_schema.columns, CREATE TABLE … AS, views and ADD COLUMN.
func TestSQLLengthMetadata(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_len_m", "it_len_ctas", "it_len_cagg", "it_len_cu")
	h.dropViews("it_len_v")
	raw := h.rawClient()
	h.exec(`CREATE TABLE it_len_m (a varchar(3), b character varying(10), c char(4), d character, e bpchar(2),
		f nchar(3), g nvarchar(5), h varchar, i text, j bpchar, k integer, l varchar(max))`)
	want := []string{
		"a VARCHAR(3) 3 12", "b VARCHAR(10) 10 40", "c CHAR(4) 4 16", "d CHAR(1) 1 4", "e CHAR(2) 2 8",
		"f CHAR(3) 3 12", "g VARCHAR(5) 5 20", "h VARCHAR NULL NULL", "i VARCHAR NULL NULL", "j BPCHAR NULL NULL",
		"k INTEGER NULL NULL", "l VARCHAR NULL NULL",
	}
	if got := h.columnSizes("it_len_m"); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("GetObjects columns\n got: %q\nwant: %q", got, want)
	}
	h.expectRows(`SELECT column_name, data_type, character_maximum_length, character_octet_length, numeric_precision
		FROM information_schema.columns WHERE table_name = 'it_len_m' ORDER BY ordinal_position`,
		"a|VARCHAR(3)|3|12|NULL", "b|VARCHAR(10)|10|40|NULL", "c|CHAR(4)|4|16|NULL", "d|CHAR(1)|1|4|NULL",
		"e|CHAR(2)|2|8|NULL", "f|CHAR(3)|3|12|NULL", "g|VARCHAR(5)|5|20|NULL", "h|VARCHAR|NULL|1073741824|NULL",
		"i|VARCHAR|NULL|1073741824|NULL", "j|BPCHAR|NULL|1073741824|NULL", "k|INTEGER|NULL|NULL|32",
		"l|VARCHAR|NULL|1073741824|NULL")
	// As stored: older metadata, without these fields, reads as VARCHAR.
	for col, want := range map[string]ColType{
		"a": {Kind: KindString, Length: 3}, "c": {Kind: KindString, Length: 4, Fixed: true},
		"h": typeString, "j": {Kind: KindString, Fixed: true},
	} {
		if got := h.tableColumn(raw, "it_len_m", col).Type; got != want {
			t.Errorf("column %s: type %+v, want %+v", col, got, want)
		}
	}
	stored, err := raw.Get(h.ctx, metaKey(defaultSchema, "it_len_m")).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, frag := range []string{`"name":"a","type":{"kind":"string","length":3}`,
		`"name":"c","type":{"kind":"string","length":4,"fixed":true}`, `"name":"h","type":{"kind":"string"}`} {
		if !strings.Contains(stored, frag) {
			t.Errorf("metadata %s lacks %s", stored, frag)
		}
	}

	h.exec(`INSERT INTO it_len_m (a, b, c, i) VALUES ('abc', 'b', 'cd', 'long text')`)
	// CREATE TABLE … AS keeps the length of a column, a cast, a scalar
	// subquery and a CASE / COALESCE of one type; computed values have none.
	h.exec(`CREATE TABLE it_len_ctas AS SELECT a, c, CAST(i AS VARCHAR(7)) AS x, a || '' AS y, UPPER(a) AS z,
		COALESCE(a, a) AS co, COALESCE(a, 'literal!') AS cl, COALESCE(a, b) AS cb, NULLIF(a, 'x') AS ni,
		CASE WHEN true THEN a END AS cs, CASE WHEN true THEN a ELSE b END AS cs2, CASE WHEN true THEN a ELSE 'zzzzz' END AS cs3,
		(SELECT a FROM it_len_m LIMIT 1) AS sq, MIN(a) OVER () AS mi, LAG(a) OVER (ORDER BY a) AS lg, CAST(c AS CHAR(2)) AS c2
		FROM it_len_m`)
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_len_ctas' ORDER BY ordinal_position`,
		"a|VARCHAR(3)", "c|CHAR(4)", "x|VARCHAR(7)", "y|VARCHAR", "z|VARCHAR", "co|VARCHAR(3)", "cl|VARCHAR",
		"cb|VARCHAR", "ni|VARCHAR(3)", "cs|VARCHAR(3)", "cs2|VARCHAR", "cs3|VARCHAR", "sq|VARCHAR(3)", "mi|VARCHAR",
		"lg|VARCHAR", "c2|CHAR(2)")
	h.expectRows(`SELECT a, c, x, cl, c2 FROM it_len_ctas`, "abc|cd  |long te|abc|cd")
	h.expectTooLong(`INSERT INTO it_len_ctas (a) VALUES ('abcd')`, "character varying(3)")
	h.expectTooLong(`INSERT INTO it_len_ctas (c2) VALUES ('abc')`, "character(2)")
	h.exec(`CREATE TABLE it_len_cagg AS SELECT c, MAX(a) AS m, MIN(c) AS mc FROM it_len_m GROUP BY c`)
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_len_cagg' ORDER BY ordinal_position`,
		"c|CHAR(4)", "m|VARCHAR", "mc|BPCHAR")
	h.exec(`CREATE TABLE it_len_cu AS SELECT a, a AS b FROM it_len_m UNION ALL SELECT a, b FROM it_len_m`)
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_len_cu' ORDER BY ordinal_position`,
		"a|VARCHAR(3)", "b|VARCHAR")

	// Views report the lengths of their columns.
	h.exec(`CREATE VIEW it_len_v AS SELECT a, c, a || 'x' AS ax, CAST(i AS CHAR(2)) AS ic FROM it_len_m`)
	want = []string{"a VARCHAR(3) 3 12", "c CHAR(4) 4 16", "ax VARCHAR NULL NULL", "ic CHAR(2) 2 8"}
	if got := h.columnSizes("it_len_v"); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("GetObjects view columns\n got: %q\nwant: %q", got, want)
	}
	h.expectRows(`SELECT a, c, ic FROM it_len_v`, "abc|cd  |lo")

	// ADD COLUMN.
	h.exec(`ALTER TABLE it_len_m ADD COLUMN m varchar(2)`)
	h.expectRows(`SELECT data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 'it_len_m' AND column_name = 'm'`,
		"VARCHAR(2)|2")
	h.expectTooLong(`UPDATE it_len_m SET m = 'abc'`, "character varying(2)")
	h.exec(`UPDATE it_len_m SET m = 'ab'`)
	h.expectRows(`SELECT m FROM it_len_m`, "ab")
}

// VARCHAR, TEXT and VARCHAR(MAX) take values of any length.
func TestSQLLengthUnbounded(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_len_u")
	h.exec(`CREATE TABLE it_len_u (a varchar, b text, c varchar(max), d string)`)
	long := strings.Repeat("x", 100000)
	h.exec(fmt.Sprintf(`INSERT INTO it_len_u VALUES ('%s', '%s', '%s', '%s')`, long, long, long, long))
	h.exec(`UPDATE it_len_u SET a = a || 'y'`)
	h.expectRows(`SELECT LENGTH(a), LENGTH(b), LENGTH(c), LENGTH(d) FROM it_len_u`, "100001|100000|100000|100000")
}
