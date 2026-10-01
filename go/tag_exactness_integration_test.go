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

// String values the index doesn't hold exactly, and prefix queries beyond
// its expansion limit (see tags.go). Every query on an indexed string column
// s must give the same rows as on its NOINDEX copy n, which the driver
// always evaluates itself.

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// tagValues are the strings of it_tag_t, by id (0 is NULL): ones the index
// trims, splits or cuts, ones it holds as they are, and ones a query can't
// look up.
var tagValues = []string{
	1: "ab", 2: "ab ", 3: " ab", 4: "\tab", 5: "ab\n", 6: "ab\r\n", 7: "ab\v", 8: "ab\f",
	9: "ab\u00a0", 10: "\u3000ab", 11: " ", 12: "", 13: "  ", 14: "a b", 15: "abc", 16: "AB",
	17: "ab  ", 18: "a\x1fb", 19: "x\x1fab", 20: "ab\x1f", 21: "€5", 22: "a—b", 23: "a\x01b",
	24: "ab\x7f", 25: "😀 x",
}

// tagLongValues are the strings of it_tag_long, by id: tags are cut to
// maxTagBytes bytes.
var tagLongValues = []string{
	1: strings.Repeat("L", 5000), 2: strings.Repeat("L", maxTagBytes) + "x", 3: strings.Repeat("L", maxTagBytes),
	4: strings.Repeat("L", maxTagBytes-1), 5: " " + strings.Repeat("L", maxTagBytes), 6: "LL",
}

// tagQuote renders a string as an SQL literal.
func tagQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// tagIDs returns the ids of the non-NULL values that satisfy keep, in
// order.
func tagIDs(values []string, keep func(v string) bool) []string {
	var out []string
	for id := 1; id < len(values); id++ {
		if keep(values[id]) {
			out = append(out, strconv.Itoa(id))
		}
	}
	return out
}

// tagAgreeOn runs sql with {c} replaced by an indexed column and by its
// NOINDEX copy, and checks that both give want.
func (h *sqlHarness) tagAgreeOn(indexed, plain, sql string, want ...string) {
	h.t.Helper()
	got, _ := h.query(strings.ReplaceAll(sql, "{c}", indexed))
	ref, _ := h.query(strings.ReplaceAll(sql, "{c}", plain))
	if strings.Join(got, "\n") != strings.Join(ref, "\n") {
		h.t.Errorf("%q\n indexed: %q\n NOINDEX: %q", sql, got, ref)
	} else if strings.Join(got, "\n") != strings.Join(want, "\n") {
		h.t.Errorf("%q\n got: %q\nwant: %q", sql, got, want)
	}
}

// tagAgree is tagAgreeOn for the columns s and n.
func (h *sqlHarness) tagAgree(sql string, want ...string) {
	h.t.Helper()
	h.tagAgreeOn("s", "n", sql, want...)
}

// tagSame checks that sql gives the same rows on s and on n.
func (h *sqlHarness) tagSame(sql string) {
	h.t.Helper()
	got, _ := h.query(strings.ReplaceAll(sql, "{c}", "s"))
	ref, _ := h.query(strings.ReplaceAll(sql, "{c}", "n"))
	if strings.Join(got, "\n") != strings.Join(ref, "\n") {
		h.t.Errorf("%q\n indexed: %q\n NOINDEX: %q", sql, got, ref)
	}
}

// tagUpdates runs a DML statement once with {c} as s and once as n and
// checks that they affect the same number of rows.
func (h *sqlHarness) tagUpdates(sql string, want int64) {
	h.t.Helper()
	a := h.exec(strings.ReplaceAll(sql, "{c}", "s"))
	b := h.exec(strings.ReplaceAll(sql, "{c}", "n"))
	if a != b || a != want {
		h.t.Errorf("%q\n affected %d rows on s and %d on n, want %d", sql, a, b, want)
	}
}

// expectTagPlan checks the index query of a SELECT, whether a residual is left
// and whether its aggregates run in the index.
func (h *sqlHarness) expectTagPlan(sql, query string, residual, indexAgg bool) {
	h.t.Helper()
	q, r, a := h.planOf(sql)
	if q != query || r != residual || a != indexAgg {
		h.t.Errorf("%s\n plan %q residual=%v indexAgg=%v; want %q residual=%v indexAgg=%v", sql, q, r, a, query, residual, indexAgg)
	}
}

// expectTagLevel checks the recorded level of an indexed string column.
func (h *sqlHarness) expectTagLevel(c goredis.UniversalClient, table, column, level string) {
	h.t.Helper()
	if col := h.tableColumn(c, table, column); !col.TagsChecked || col.TagValues != level {
		h.t.Errorf("%s.%s: tag level %q (checked %v), want %q", table, column, col.TagValues, col.TagsChecked, level)
	}
}

// setupTagTable creates table (id, s, n NOINDEX) holding values in s and n,
// and a NULL row 0.
func (h *sqlHarness) setupTagTable(table string, values []string) {
	h.t.Helper()
	h.exec("CREATE TABLE " + table + " (id INTEGER, s VARCHAR, n VARCHAR NOINDEX)")
	rows := []string{"(0, NULL, NULL)"}
	for id := 1; id < len(values); id++ {
		q := tagQuote(values[id])
		rows = append(rows, fmt.Sprintf("(%d, %s, %s)", id, q, q))
	}
	h.exec("INSERT INTO " + table + " VALUES " + strings.Join(rows, ", "))
}

// tagIngest bulk-ingests rows (id, s, n) with the same string in s and n.
func (h *sqlHarness) tagIngest(table, mode string, opts map[string]string, ids []int64, vals []string) {
	h.t.Helper()
	mem := memory.DefaultAllocator
	ib := array.NewInt64Builder(mem)
	defer ib.Release()
	ib.AppendValues(ids, nil)
	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	sb.AppendValues(vals, nil)
	nb := array.NewStringBuilder(mem)
	defer nb.Release()
	nb.AppendValues(vals, nil)
	idCol, sCol, nCol := ib.NewArray(), sb.NewArray(), nb.NewArray()
	defer idCol.Release()
	defer sCol.Release()
	defer nCol.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "n", Type: arrow.BinaryTypes.String},
	}, nil), []arrow.Array{idCol, sCol, nCol}, int64(len(ids)))
	defer rec.Release()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	opts[adbc.OptionKeyIngestTargetTable] = table
	opts[adbc.OptionKeyIngestMode] = mode
	for k, v := range opts {
		if err := st.SetOption(h.ctx, k, v); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := st.Bind(h.ctx, rec); err != nil {
		h.t.Fatal(err)
	}
	if n, err := st.ExecuteUpdate(h.ctx); err != nil || n != int64(len(ids)) {
		h.t.Fatalf("ingest into %s: n=%d err=%v", table, n, err)
	}
}

// clearTagLevels makes a table look like one created before the levels of
// its string columns were recorded.
func (h *sqlHarness) clearTagLevels(c goredis.UniversalClient, table string) {
	h.t.Helper()
	key := metaKey(defaultSchema, table)
	raw, err := c.Get(h.ctx, key).Result()
	if err != nil {
		h.t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		h.t.Fatal(err)
	}
	for _, col := range m["columns"].([]any) {
		delete(col.(map[string]any), "tag_values")
		delete(col.(map[string]any), "tags_checked")
	}
	out, err := json.Marshal(m)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := c.Set(h.ctx, key, out, 0).Err(); err != nil {
		h.t.Fatal(err)
	}
}

// tagValueQueries checks equality, <>, IN, OR, NOT IN and LIKE prefixes of
// each of the values of a table made by setupTagTable.
func (h *sqlHarness) tagValueQueries(table string, values []string) {
	h.t.Helper()
	for id := 1; id < len(values); id++ {
		v := values[id]
		q := tagQuote(v)
		h.tagAgree("SELECT id FROM "+table+" WHERE {c} = "+q+" ORDER BY id", tagIDs(values, func(x string) bool { return x == v })...)
		h.tagAgree("SELECT id FROM "+table+" WHERE {c} <> "+q+" ORDER BY id", tagIDs(values, func(x string) bool { return x != v })...)
		h.tagAgree("SELECT COUNT(*) FROM "+table+" WHERE {c} = "+q, strconv.Itoa(len(tagIDs(values, func(x string) bool { return x == v }))))
		other := values[id%(len(values)-1)+1]
		pair := func(x string) bool { return x == v || x == other }
		h.tagAgree("SELECT id FROM "+table+" WHERE {c} IN ("+q+", "+tagQuote(other)+") ORDER BY id", tagIDs(values, pair)...)
		h.tagAgree("SELECT id FROM "+table+" WHERE {c} = "+q+" OR {c} = "+tagQuote(other)+" ORDER BY id", tagIDs(values, pair)...)
		h.tagAgree("SELECT id FROM "+table+" WHERE {c} NOT IN ("+q+", "+tagQuote(other)+") ORDER BY id",
			tagIDs(values, func(x string) bool { return !pair(x) })...)
		for _, n := range []int{2, 3} {
			if r := []rune(v); len(r) >= n && !strings.ContainsAny(string(r[:n]), "%_") {
				p := string(r[:n])
				h.tagAgree("SELECT id FROM "+table+" WHERE {c} LIKE "+tagQuote(p+"%")+" ORDER BY id",
					tagIDs(values, func(x string) bool { return strings.HasPrefix(x, p) })...)
			}
		}
	}
}

// Equality, <>, IN, NOT IN and LIKE prefixes on values the index trims,
// splits or cuts, plus those it holds exactly, with COUNT, GROUP BY and
// ORDER BY … LIMIT over them.
func TestSQLTagExactness(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tag_t", "it_tag_clean", "it_tag_punct")
	raw := h.rawClient()
	h.setupTagTable("it_tag_t", tagValues)
	h.expectTagLevel(raw, "it_tag_t", "s", tagsNormalized)
	h.tagValueQueries("it_tag_t", tagValues)
	// Every ASCII punctuation character is escaped in TAG queries, also
	// before the * of a prefix query (RediSearch drops backslashes there).
	punct := []string{0: "", 1: `ab\`, 2: `\ab`, 3: `a\\b`, 4: `ab\\`, 5: "-ab", 6: "ab-"}
	for _, c := range "!\"#$&'()*+,-./:;<=>?@[\\]^`{|}~" {
		punct = append(punct, "a"+string(c)+"b")
	}
	h.setupTagTable("it_tag_punct", punct)
	h.expectTagLevel(raw, "it_tag_punct", "s", "")
	h.tagValueQueries("it_tag_punct", punct)

	h.tagAgree("SELECT id FROM it_tag_t WHERE {c} = 'a' OR {c} = 'b'")
	h.tagAgree("SELECT id FROM it_tag_t WHERE {c} IN ('ab ', ' ab') ORDER BY id", "2", "3")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_t WHERE {c} IN ('ab', 'ab ', ' ab', '')", "4")
	h.tagAgree("SELECT id FROM it_tag_t WHERE {c} LIKE ' ab%' ORDER BY id", "3")
	h.tagAgree("SELECT id FROM it_tag_t WHERE {c} LIKE 'ab %' ORDER BY id", "2", "17")
	h.tagAgree("SELECT id FROM it_tag_t WHERE {c} = 'ab' AND id < 10", "1")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_t WHERE {c} IS NULL", "1")

	// GROUP BY runs in the index on s: its sorting vector holds the values
	// untrimmed.
	h.tagSame("SELECT {c}, COUNT(*) FROM it_tag_t GROUP BY {c} ORDER BY {c}")
	h.tagSame("SELECT LENGTH({c}), COUNT(*) FROM it_tag_t WHERE id < 20 GROUP BY {c} ORDER BY 1, 2")
	h.tagAgree("SELECT COUNT(*) FROM (SELECT DISTINCT {c} FROM it_tag_t) d", "26")
	// ORDER BY … LIMIT sorts in the index on s.
	h.tagSame("SELECT id FROM it_tag_t ORDER BY {c}, id LIMIT 10")
	h.tagSame("SELECT id FROM it_tag_t ORDER BY {c} DESC, id LIMIT 10")
	h.tagSame("SELECT id FROM it_tag_t WHERE id > 1 ORDER BY {c} LIMIT 5 OFFSET 3")
	h.tagAgree("SELECT id FROM it_tag_t WHERE {c} = 'ab' ORDER BY id LIMIT 1", "1")
	h.tagAgree("SELECT id FROM it_tag_t WHERE {c} IN ('ab', ' ab', 'ab ') ORDER BY {c} LIMIT 2", "3", "1")

	// Once a column holds a value the index doesn't hold exactly, string
	// equality on it is re-checked, so the index doesn't count on its own.
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_t WHERE s = 'ab'", "@s:{ab}", true, false)
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_t WHERE s = 'ab '", "@s:{ab}", true, false)
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_t WHERE s = ' '", `@s:{""}`, true, false)
	h.expectTagPlan("SELECT id FROM it_tag_t WHERE s = 'a\x01b'", "*", true, false)

	// A column without such values keeps exact, index-only answers; a
	// constant the index doesn't hold exactly is looked up by its tag and
	// re-checked.
	h.exec("CREATE TABLE it_tag_clean (id INTEGER, s VARCHAR, n VARCHAR NOINDEX)")
	h.exec("INSERT INTO it_tag_clean VALUES (1, 'ab', 'ab'), (2, 'a b', 'a b'), (3, '€5', '€5'), (4, '', ''), (5, 'ab\u00a0', 'ab\u00a0'), (6, 'cd', 'cd')")
	h.expectTagLevel(raw, "it_tag_clean", "s", "")
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_clean WHERE s = 'ab'", "@s:{ab}", false, true)
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_clean WHERE s = '€5'", "@s:{€5}", false, true)
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_clean WHERE s = 'a b'", `@s:{a\ b}`, false, true)
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_clean WHERE s = ''", `@s:{""}`, false, true)
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_clean WHERE s = 'ab '", "@s:{ab}", true, false)
	h.expectTagPlan("SELECT s, COUNT(*) FROM it_tag_clean WHERE id > 1 GROUP BY s", "@id:[(1 +inf]", false, true)
	h.tagAgree("SELECT COUNT(*) FROM it_tag_clean WHERE {c} = 'ab'", "1")
	h.tagAgree("SELECT id FROM it_tag_clean WHERE {c} = 'ab '")
	h.tagAgree("SELECT id FROM it_tag_clean WHERE {c} = '€5'", "3")
	h.tagAgree("SELECT id FROM it_tag_clean WHERE {c} IN ('a b', 'ab\u00a0', 'x') ORDER BY id", "2", "5")
	h.tagAgree("SELECT id FROM it_tag_clean ORDER BY {c} DESC LIMIT 2", "3", "6")
}

// Values longer than a tag: the index holds their first maxTagBytes bytes.
func TestSQLTagLongValues(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tag_long", "it_tag_long8")
	h.setupTagTable("it_tag_long", tagLongValues)
	h.expectTagLevel(h.rawClient(), "it_tag_long", "s", tagsNormalized)
	for id := 1; id < len(tagLongValues); id++ {
		v := tagLongValues[id]
		h.tagAgree("SELECT id FROM it_tag_long WHERE {c} = "+tagQuote(v)+" ORDER BY id", strconv.Itoa(id))
		h.tagAgree("SELECT COUNT(*) FROM it_tag_long WHERE {c} <> "+tagQuote(v), strconv.Itoa(len(tagLongValues)-2))
	}
	long := strings.Repeat("L", maxTagBytes)
	h.tagAgree("SELECT id FROM it_tag_long WHERE {c} IN ("+tagQuote(long)+", "+tagQuote(long+"y")+", 'LL') ORDER BY id", "3", "6")
	h.tagAgree("SELECT id FROM it_tag_long WHERE {c} LIKE "+tagQuote(long+"L%")+" ORDER BY id", "1")
	h.tagAgree("SELECT id FROM it_tag_long WHERE {c} LIKE "+tagQuote(long+"%")+" ORDER BY id", "1", "2", "3")
	h.tagAgree("SELECT id FROM it_tag_long WHERE {c} LIKE 'LL%' ORDER BY id", "1", "2", "3", "4", "6")
	h.tagAgree("SELECT LENGTH({c}), COUNT(*) FROM it_tag_long GROUP BY {c} ORDER BY {c}",
		"4097|1", "2|1", "4095|1", "4096|1", "5000|1", "4097|1", "NULL|1")
	h.tagAgree("SELECT id FROM it_tag_long ORDER BY {c}, id LIMIT 4", "5", "6", "4", "3")
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_long WHERE s = "+tagQuote(long), "@s:{"+long+"}", true, false)

	// The cut can split a character: the tag ends with part of it.
	utf8Values := []string{1: strings.Repeat("w", maxTagBytes-1) + "é", 2: "x" + strings.Repeat("é", 2100),
		3: strings.Repeat("é", maxTagBytes/2), 4: strings.Repeat("w", maxTagBytes-1) + "ü", 5: strings.Repeat("w", maxTagBytes-2) + "€x"}
	h.setupTagTable("it_tag_long8", utf8Values)
	for id := 1; id < len(utf8Values); id++ {
		v := utf8Values[id]
		h.tagAgree("SELECT id FROM it_tag_long8 WHERE {c} = "+tagQuote(v), strconv.Itoa(id))
		r := []rune(v)
		for n := len(r) - 3; n <= len(r); n++ {
			p := string(r[:n])
			h.tagAgree("SELECT id FROM it_tag_long8 WHERE {c} LIKE "+tagQuote(p+"%")+" ORDER BY id",
				tagIDs(utf8Values, func(x string) bool { return strings.HasPrefix(x, p) })...)
		}
	}
}

// Joins, semi-joins, correlated subqueries and views on string keys, and
// UPDATE / DELETE / MERGE whose predicates are such equalities.
func TestSQLTagExactnessJoinsAndDML(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP VIEW IF EXISTS it_tag_v")
	h.dropTables("it_tag_t", "it_tag_k", "it_tag_w")
	t.Cleanup(func() { h.exec("DROP VIEW IF EXISTS it_tag_v") })
	h.setupTagTable("it_tag_t", tagValues)
	h.exec("CREATE TABLE it_tag_k (k VARCHAR, kn VARCHAR NOINDEX, label VARCHAR)")
	keys := map[string]string{"ab": "K1", "ab ": "K2", " ab": "K3", "a\x1fb": "K18", "😀 x": "K25", "zz": "Kzz", "": "K12"}
	var rows []string
	for k, label := range keys {
		rows = append(rows, fmt.Sprintf("(%s, %s, '%s')", tagQuote(k), tagQuote(k), label))
	}
	h.exec("INSERT INTO it_tag_k VALUES " + strings.Join(rows, ", "))
	matched := []string{"1", "2", "3", "12", "18", "25"}

	// Index lookup joins into s (it_tag_k is the smaller side), hash joins on
	// n; joins on the indexed key of it_tag_k.
	h.tagAgree("SELECT t.id, k.label FROM it_tag_k k JOIN it_tag_t t ON t.{c} = k.kn ORDER BY t.id",
		"1|K1", "2|K2", "3|K3", "12|K12", "18|K18", "25|K25")
	h.tagAgree("SELECT k.label, t.id FROM it_tag_k k LEFT JOIN it_tag_t t ON t.{c} = k.kn ORDER BY k.label",
		"K1|1", "K12|12", "K18|18", "K2|2", "K25|25", "K3|3", "Kzz|NULL")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_t t JOIN it_tag_k k ON k.k = t.{c}", "6")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_t t JOIN it_tag_k k ON k.k = t.{c} WHERE t.id < 3", "2")
	// Semi-joins and IN subqueries filter s in its index.
	h.tagAgree("SELECT id FROM it_tag_t WHERE {c} IN (SELECT kn FROM it_tag_k) ORDER BY id", matched...)
	h.tagAgree("SELECT id FROM it_tag_t t WHERE EXISTS (SELECT 1 FROM it_tag_k k WHERE k.kn = t.{c}) ORDER BY id", matched...)
	h.tagAgree("SELECT COUNT(*) FROM it_tag_t t WHERE NOT EXISTS (SELECT 1 FROM it_tag_k k WHERE k.kn = t.{c})", "20")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_t WHERE {c} NOT IN (SELECT kn FROM it_tag_k)", "19")
	// A correlated subquery runs per outer row with the key as a constant.
	h.tagAgree("SELECT k.label, (SELECT COUNT(*) FROM it_tag_t t WHERE t.{c} = k.kn) FROM it_tag_k k ORDER BY k.label",
		"K1|1", "K12|1", "K18|1", "K2|1", "K25|1", "K3|1", "Kzz|0")

	// A view's filters run in its base table's index.
	h.exec("CREATE VIEW it_tag_v AS SELECT id, s, n FROM it_tag_t WHERE id <> 1")
	h.tagAgree("SELECT id FROM it_tag_v WHERE {c} = 'ab ' ORDER BY id", "2")
	h.tagAgree("SELECT id FROM it_tag_v WHERE {c} = 'ab' ORDER BY id")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_v WHERE {c} IN ('ab', ' ab', ' ')", "2")
	h.tagAgree("SELECT id FROM it_tag_v WHERE {c} LIKE ' ab%' ORDER BY id", "3")
	h.tagAgree("SELECT v.id FROM it_tag_k k JOIN it_tag_v v ON v.{c} = k.kn ORDER BY v.id", "2", "3", "12", "18", "25")

	// UPDATE and DELETE match the same rows through s as through n: every
	// row's mark is raised once per statement on each column.
	h.exec("CREATE TABLE it_tag_w (id INTEGER, s VARCHAR, n VARCHAR NOINDEX, mark INTEGER)")
	h.exec("INSERT INTO it_tag_w SELECT id, s, n, 0 FROM it_tag_t")
	for where, want := range map[string]int64{
		"{c} = 'ab'":                 1,
		"{c} = 'ab '":                1,
		"{c} IN (' ', '', 'a\x1fb')": 3,
		"{c} LIKE 'ab %'":            2,
		"{c} <> 'ab'":                24,
		"{c} = 'ab' OR {c} = '€5'":   2,
		"{c} = 'a\x01b'":             1,
	} {
		h.tagUpdates("UPDATE it_tag_w SET mark = mark + 1 WHERE "+where, want)
	}
	h.tagUpdates("UPDATE it_tag_w SET mark = mark + 1 FROM it_tag_k k WHERE it_tag_w.{c} = k.kn", 6)
	h.tagUpdates("MERGE INTO it_tag_w w USING it_tag_k k ON w.{c} = k.kn WHEN MATCHED THEN UPDATE SET mark = mark + 1", 6)
	h.expectRows("SELECT id FROM it_tag_w WHERE MOD(mark, 2) = 1")
	h.expectAffected("DELETE FROM it_tag_w WHERE s IN ('ab ', ' ab', '', 'a\x1fb')", 4)
	h.expectRows("SELECT COUNT(*) FROM it_tag_w WHERE n IN ('ab ', ' ab', '', 'a\x1fb')", "0")
	h.expectAffected("DELETE FROM it_tag_w WHERE s LIKE 'ab%'", 10)
	h.expectRows("SELECT COUNT(*) FROM it_tag_w WHERE n LIKE 'ab%'", "0")
	h.expectRows("SELECT COUNT(*) FROM it_tag_w", "12")
}

// Every way of writing a value raises its column's level first: INSERT,
// INSERT … SELECT, CTAS, UPDATE, MERGE, defaults, ADD COLUMN … DEFAULT and
// bulk ingest; NUL bytes also keep the index from sorting, grouping and
// loading the column.
func TestSQLTagLevels(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tag_up", "it_tag_mg", "it_tag_def", "it_tag_ing", "it_tag_ing2", "it_tag_ctas", "it_tag_nul")
	raw := h.rawClient()

	h.exec("CREATE TABLE it_tag_up (id INTEGER, s VARCHAR, n VARCHAR NOINDEX)")
	h.exec("INSERT INTO it_tag_up VALUES (1, 'ab', 'ab'), (2, 'cd', 'cd')")
	h.expectTagLevel(raw, "it_tag_up", "s", "")
	h.exec("UPDATE it_tag_up SET s = s || ' ', n = n || ' ' WHERE id = 1")
	h.expectTagLevel(raw, "it_tag_up", "s", tagsNormalized)
	h.tagAgree("SELECT id FROM it_tag_up WHERE {c} = 'ab'")
	h.tagAgree("SELECT id FROM it_tag_up WHERE {c} = 'ab '", "1")
	h.tagAgree("SELECT id FROM it_tag_up WHERE {c} IN ('ab ', 'cd') ORDER BY id", "1", "2")
	// A new column's level starts from its missing value, which rows
	// written later also get.
	h.exec("ALTER TABLE it_tag_up ADD COLUMN s2 VARCHAR DEFAULT 'x '")
	h.exec("ALTER TABLE it_tag_up ADD COLUMN n2 VARCHAR DEFAULT 'x ' NOINDEX")
	h.exec("ALTER TABLE it_tag_up ADD COLUMN s3 VARCHAR DEFAULT 'y'")
	h.expectTagLevel(raw, "it_tag_up", "s2", tagsNormalized)
	h.expectTagLevel(raw, "it_tag_up", "s3", "")
	h.exec("INSERT INTO it_tag_up (id, s, n) VALUES (3, 'x', 'x')")
	h.exec("UPDATE it_tag_up SET s2 = 'x', n2 = 'x' WHERE id = 2")
	h.tagAgreeOn("s2", "n2", "SELECT id FROM it_tag_up WHERE {c} = 'x ' ORDER BY id", "1", "3")
	h.tagAgreeOn("s2", "n2", "SELECT id FROM it_tag_up WHERE {c} = 'x' ORDER BY id", "2")
	h.tagAgreeOn("s2", "n2", "SELECT {c}, COUNT(*) FROM it_tag_up GROUP BY {c} ORDER BY {c}", "x|1", "x |2")

	// MERGE's INSERT and UPDATE, and INSERT … SELECT.
	h.exec("CREATE TABLE it_tag_mg (id INTEGER, s VARCHAR, n VARCHAR NOINDEX)")
	h.exec("INSERT INTO it_tag_mg VALUES (1, 'p', 'p')")
	h.exec(`MERGE INTO it_tag_mg t USING (SELECT 2 AS id, ' q' AS v) src ON t.id = src.id
		WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.v, src.v)`)
	h.expectTagLevel(raw, "it_tag_mg", "s", tagsNormalized)
	h.tagAgree("SELECT id FROM it_tag_mg WHERE {c} = ' q'", "2")
	h.tagAgree("SELECT id FROM it_tag_mg WHERE {c} = 'q'")
	h.exec(`MERGE INTO it_tag_mg t USING (SELECT 1 AS id) src ON t.id = src.id
		WHEN MATCHED THEN UPDATE SET s = 'p
', n = 'p
'`)
	h.tagAgree("SELECT id FROM it_tag_mg WHERE {c} = 'p'")
	h.exec("INSERT INTO it_tag_mg SELECT id + 10, s || ' ', n || ' ' FROM it_tag_mg")
	h.tagAgree("SELECT id FROM it_tag_mg WHERE {c} = ' q ' ORDER BY id", "12")

	// CREATE TABLE … AS SELECT indexes the copy.
	h.exec("CREATE TABLE it_tag_ctas AS SELECT id, s FROM it_tag_mg")
	h.expectTagLevel(raw, "it_tag_ctas", "s", tagsNormalized)
	h.expectRows("SELECT id FROM it_tag_ctas WHERE s = ' q' ORDER BY id", "2")
	h.expectRows("SELECT id FROM it_tag_ctas WHERE s = 'q' ORDER BY id")

	// Defaults.
	h.exec("CREATE TABLE it_tag_def (id INTEGER, s VARCHAR DEFAULT ' pad', n VARCHAR DEFAULT ' pad' NOINDEX)")
	h.exec("INSERT INTO it_tag_def (id) VALUES (1)")
	h.exec("INSERT INTO it_tag_def VALUES (2, 'pad', 'pad')")
	h.expectTagLevel(raw, "it_tag_def", "s", tagsNormalized)
	h.tagAgree("SELECT id FROM it_tag_def WHERE {c} = ' pad'", "1")
	h.tagAgree("SELECT id FROM it_tag_def WHERE {c} = 'pad'", "2")

	// Bulk ingest: into an existing table, and creating one.
	h.exec("CREATE TABLE it_tag_ing (id BIGINT, s VARCHAR, n VARCHAR NOINDEX)")
	h.tagIngest("it_tag_ing", adbc.OptionValueIngestModeAppend, map[string]string{},
		[]int64{1, 2, 3, 4}, []string{"ab", "ab ", "\tab", " "})
	h.expectTagLevel(raw, "it_tag_ing", "s", tagsNormalized)
	h.tagAgree("SELECT id FROM it_tag_ing WHERE {c} = 'ab'", "1")
	h.tagAgree("SELECT id FROM it_tag_ing WHERE {c} = '\tab'", "3")
	h.tagAgree("SELECT id FROM it_tag_ing WHERE {c} = ''")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_ing WHERE {c} LIKE 'ab%'", "2")
	h.tagIngest("it_tag_ing2", adbc.OptionValueIngestModeCreate, map[string]string{OptionStringIngestIndexColumns: "id,s"},
		[]int64{1, 2, 3}, []string{"ab", " ab", "ab\n"})
	h.expectTagLevel(raw, "it_tag_ing2", "s", tagsNormalized)
	h.tagAgree("SELECT id FROM it_tag_ing2 WHERE {c} = 'ab'", "1")
	h.tagAgree("SELECT id FROM it_tag_ing2 WHERE {c} IN (' ab', 'ab\n') ORDER BY id", "2", "3")

	// NUL bytes cut the value in the sorting vector too: the index then
	// doesn't sort, group or load the column.
	h.exec("CREATE TABLE it_tag_nul (id BIGINT, s VARCHAR, n VARCHAR NOINDEX)")
	h.tagIngest("it_tag_nul", adbc.OptionValueIngestModeAppend, map[string]string{},
		[]int64{1, 2, 3, 4, 5, 6}, []string{"a\x00b", "\x00zz", "ab\x00", "a", "ab", "ab "})
	h.expectTagLevel(raw, "it_tag_nul", "s", tagsTruncated)
	h.tagAgree("SELECT id, {c}, LENGTH({c}) FROM it_tag_nul ORDER BY id",
		"1|a\x00b|3", "2|\x00zz|3", "3|ab\x00|3", "4|a|1", "5|ab|2", "6|ab |3")
	h.tagAgree("SELECT id FROM it_tag_nul WHERE {c} = 'a'", "4")
	h.tagAgree("SELECT id FROM it_tag_nul WHERE {c} = 'ab'", "5")
	h.tagAgree("SELECT id FROM it_tag_nul WHERE {c} = 'a\x00b'", "1")
	h.tagAgree("SELECT id FROM it_tag_nul WHERE {c} IN ('ab\x00', '\x00zz') ORDER BY id", "2", "3")
	h.tagAgree("SELECT id FROM it_tag_nul ORDER BY {c}, id", "2", "4", "1", "5", "3", "6")
	h.tagAgree("SELECT id FROM it_tag_nul ORDER BY {c} DESC LIMIT 2", "6", "3")
	h.tagAgree("SELECT LENGTH({c}), COUNT(*) FROM it_tag_nul GROUP BY {c} ORDER BY {c}",
		"3|1", "1|1", "3|1", "2|1", "3|1", "3|1")
	h.tagAgree("SELECT id FROM it_tag_nul WHERE {c} LIKE 'ab%' ORDER BY id", "3", "5", "6")
	h.tagAgree("SELECT COUNT({c}), COUNT(*) FROM it_tag_nul", "6|6")
	h.expectTagPlan("SELECT s, COUNT(*) FROM it_tag_nul GROUP BY s", "*", false, false)
}

// A LIKE prefix that matches more distinct values than RediSearch expands
// (200 by default, per shard) is checked by the driver; one below the limit
// is still a prefix query.
func TestSQLTagPrefixExpansion(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP VIEW IF EXISTS it_tag_wv")
	h.dropTables("it_tag_wide")
	t.Cleanup(func() { h.exec("DROP VIEW IF EXISTS it_tag_wv") })
	h.exec("CREATE TABLE it_tag_wide (id INTEGER, s VARCHAR, n VARCHAR NOINDEX)")
	var rows []string
	for i := 0; i < 1000; i++ {
		rows = append(rows, fmt.Sprintf("(%d, 'ab%04d', 'ab%04d')", i, i, i))
	}
	rows = append(rows, "(1000, 'ab', 'ab')", "(1001, 'xy', 'xy')")
	h.exec("INSERT INTO it_tag_wide VALUES " + strings.Join(rows, ", "))
	h.expectTagLevel(h.rawClient(), "it_tag_wide", "s", "")

	h.tagAgree("SELECT COUNT(*) FROM it_tag_wide WHERE {c} LIKE 'ab%'", "1001")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_wide WHERE {c} LIKE 'ab0%'", "1000")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_wide WHERE {c} LIKE 'ab01%'", "100")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_wide WHERE {c} LIKE 'ab0999%'", "1")
	h.tagAgree("SELECT id FROM it_tag_wide WHERE {c} LIKE 'ab%' AND id >= 996 ORDER BY id", "996", "997", "998", "999", "1000")
	h.tagAgree("SELECT {c} FROM it_tag_wide WHERE {c} LIKE 'ab%' ORDER BY {c} DESC LIMIT 3", "ab0999", "ab0998", "ab0997")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_wide WHERE {c} NOT LIKE 'ab%'", "1")
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_wide WHERE s LIKE 'ab%'", "*", true, false)
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_wide WHERE s LIKE 'ab01%'", "@s:{ab01*}", true, false)
	h.expectTagPlan("SELECT id FROM it_tag_wide WHERE s LIKE 'ab%' AND id >= 996", "@id:[996 +inf]", true, false)

	h.exec("CREATE VIEW it_tag_wv AS SELECT id, s, n FROM it_tag_wide WHERE id >= 0")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_wv WHERE {c} LIKE 'ab%'", "1001")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_wide w JOIN it_tag_wv v ON v.id = w.id WHERE v.{c} LIKE 'ab0%'", "1000")

	h.tagUpdates("UPDATE it_tag_wide SET id = id WHERE {c} LIKE 'ab%'", 1001)
	h.expectAffected("DELETE FROM it_tag_wide WHERE s LIKE 'ab05%'", 100)
	h.expectRows("SELECT COUNT(*) FROM it_tag_wide WHERE n LIKE 'ab05%'", "0")
	h.tagAgree("SELECT COUNT(*) FROM it_tag_wide WHERE {c} LIKE 'ab%'", "901")
}

// Tables created before the levels were recorded count as truncated until a
// background check has read their rows; it records what they need.
func TestSQLTagOlderTables(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_tag_old", "it_tag_old_exact", "it_tag_old_nul", "it_tag_old_w")
	raw := h.rawClient()
	checked := func(table string) func() bool {
		return func() bool { return h.tableColumn(raw, table, "s").TagsChecked }
	}

	// Padded values written by an older driver.
	h.setupTagTable("it_tag_old", tagValues)
	h.clearTagLevels(raw, "it_tag_old")
	if c := h.tableColumn(raw, "it_tag_old", "s"); c.TagsChecked || c.TagValues != "" {
		t.Fatalf("cleared column = %+v", c)
	}
	// Planning starts the check, so only the first plan is sure to be made
	// before it ends.
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_old WHERE s = 'ab'", "@s:{ab}", true, false)
	check := func() {
		for _, v := range []string{"ab", "ab ", " ab", " ", "", "a\x1fb", "€5"} {
			h.tagAgree("SELECT id FROM it_tag_old WHERE {c} = "+tagQuote(v)+" ORDER BY id", tagIDs(tagValues, func(x string) bool { return x == v })...)
		}
		h.tagAgree("SELECT COUNT(*) FROM it_tag_old WHERE {c} IN ('ab', 'ab ')", "2")
		h.tagSame("SELECT {c}, COUNT(*) FROM it_tag_old GROUP BY {c} ORDER BY {c}")
		h.tagSame("SELECT id FROM it_tag_old ORDER BY {c}, id LIMIT 10")
	}
	check()
	waitFor(t, "the check of it_tag_old", checked("it_tag_old"))
	h.expectTagLevel(raw, "it_tag_old", "s", tagsNormalized)
	check()

	// Only values held exactly: the check gives back exact answers.
	h.exec("CREATE TABLE it_tag_old_exact (id INTEGER, s VARCHAR, n VARCHAR NOINDEX)")
	h.exec("INSERT INTO it_tag_old_exact VALUES (1, 'ab', 'ab'), (2, 'cd', 'cd'), (3, NULL, NULL)")
	h.clearTagLevels(raw, "it_tag_old_exact")
	h.expectTagPlan("SELECT s, COUNT(*) FROM it_tag_old_exact GROUP BY s", "*", false, false)
	h.tagAgree("SELECT COUNT(*) FROM it_tag_old_exact WHERE {c} = 'ab'", "1")
	waitFor(t, "the check of it_tag_old_exact", checked("it_tag_old_exact"))
	h.expectTagLevel(raw, "it_tag_old_exact", "s", "")
	h.expectTagPlan("SELECT COUNT(*) FROM it_tag_old_exact WHERE s = 'ab'", "@s:{ab}", false, true)
	h.expectTagPlan("SELECT s, COUNT(*) FROM it_tag_old_exact GROUP BY s", "*", false, true)

	// NUL bytes: read from the HASHes before and after the check.
	h.exec("CREATE TABLE it_tag_old_nul (id BIGINT, s VARCHAR, n VARCHAR NOINDEX)")
	h.tagIngest("it_tag_old_nul", adbc.OptionValueIngestModeAppend, map[string]string{},
		[]int64{1, 2}, []string{"a\x00b", "ab"})
	h.clearTagLevels(raw, "it_tag_old_nul")
	h.tagAgree("SELECT id, LENGTH({c}) FROM it_tag_old_nul ORDER BY id", "1|3", "2|2")
	waitFor(t, "the check of it_tag_old_nul", checked("it_tag_old_nul"))
	h.expectTagLevel(raw, "it_tag_old_nul", "s", tagsTruncated)
	h.tagAgree("SELECT id, LENGTH({c}) FROM it_tag_old_nul ORDER BY {c}", "1|3", "2|2")

	// A writer raises the level of an unchecked column, and the check keeps
	// it whichever finishes first.
	h.exec("CREATE TABLE it_tag_old_w (id INTEGER, s VARCHAR, n VARCHAR NOINDEX)")
	h.exec("INSERT INTO it_tag_old_w VALUES (1, 'ab', 'ab')")
	h.clearTagLevels(raw, "it_tag_old_w")
	h.exec("INSERT INTO it_tag_old_w VALUES (2, 'ab ', 'ab ')")
	waitFor(t, "the check of it_tag_old_w", checked("it_tag_old_w"))
	h.expectTagLevel(raw, "it_tag_old_w", "s", tagsNormalized)
	h.tagAgree("SELECT id FROM it_tag_old_w WHERE {c} = 'ab'", "1")
	h.tagAgree("SELECT id FROM it_tag_old_w WHERE {c} = 'ab '", "2")
}

// The pure parts of the TAG model (no server needed).
func TestTagModel(t *testing.T) {
	for v, want := range map[string]string{
		"ab": "", "": "", "a b": "", "ab\u00a0": "", "€5": "", "a\x01b": "",
		strings.Repeat("x", maxTagBytes): "", strings.Repeat("x", maxTagBytes+1): tagsNormalized,
		"ab ": tagsNormalized, " ab": tagsNormalized, "\tab": tagsNormalized, "ab\v": tagsNormalized,
		" ": tagsNormalized, "a\x1fb": tagsNormalized, "a\x00b": tagsTruncated, " \x00": tagsTruncated,
	} {
		if got := tagLevelOf(v); got != want {
			t.Errorf("tagLevelOf(%q) = %q, want %q", v, got, want)
		}
	}
	for v, want := range map[string][]string{
		"ab":                                     {"ab"},
		" ab\t":                                  {"ab"},
		"  ":                                     {""},
		"a\x1f b \x1f":                           {"a", "b", ""},
		"a\x00b\x1fc":                            {"a"},
		"\x00":                                   {""},
		" " + strings.Repeat("y", maxTagBytes+9): {strings.Repeat("y", maxTagBytes)},
	} {
		if got := valueTags(v); strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("valueTags(%q) = %q, want %q", v, got, want)
		}
	}
	exact := columnMeta{Name: "s", Type: typeString, Indexed: true, TagsChecked: true}
	normalized := exact
	normalized.TagValues = tagsNormalized
	for _, c := range []struct {
		col   columnMeta
		v     string
		lit   string
		exact bool
		ok    bool
	}{
		{exact, "ab", "ab", true, true},
		{exact, "a b", `a\ b`, true, true},
		{exact, "é€", "é€", true, true},
		{exact, "", `""`, true, true},
		{exact, "ab ", "ab", false, true},
		{normalized, "ab", "ab", false, true},
		{normalized, " ", `""`, false, true},
		{normalized, "x\x1fabc", "abc", false, true},
		{normalized, "a\x01b", "", false, false},
		{normalized, "a\x01b\x1fcd", "cd", false, true},
		{columnMeta{Name: "s", Type: typeString, Indexed: true}, "ab", "ab", false, true},
	} {
		lit, ex, ok := tagLookup(c.col, c.v)
		if ok != c.ok || (ok && (lit != c.lit || ex != c.exact)) {
			t.Errorf("tagLookup(%+v, %q) = %q, %v, %v; want %q, %v, %v", c.col, c.v, lit, ex, ok, c.lit, c.exact, c.ok)
		}
	}
	for p, want := range map[string]string{"ab": "ab", " ab ": "ab", "a": "", "  a": "", "a\x00bc": "", "ab\x00c": "ab", "a\x02": "", `a\`: "", `ab\\`: "ab", `a\b`: `a\\b`} {
		lit, ok := prefixLookup(p)
		if (want == "") == ok || lit != want {
			t.Errorf("prefixLookup(%q) = %q, %v; want %q", p, lit, ok, want)
		}
	}
	none := []any{"Shard ID", "a", "Warning", []any{"None"}, "Iterators profile", []any{"Type", "EMPTY"}}
	reached := []any{"Shard ID", "b", "Warning", []any{"Max prefix expansions limit was reached"}}
	noEntry := []any{"Shard ID", "c", "Iterators profile", []any{"Warning", []any{"None"}}}
	for _, c := range []struct {
		profile any
		shards  int
		want    bool
	}{
		{[]any{"Shards", []any{none}, "Coordinator", []any{}}, 1, true},
		{[]any{"Shards", []any{none, none, none}, "Coordinator", []any{}}, 3, true},
		{[]any{"Shards", []any{none, reached, none}}, 3, false},
		{[]any{"Shards", []any{none, none}}, 3, false},
		{[]any{"Shards", []any{none, noEntry}}, 2, false},
		{[]any{"Shards", []any{}}, 1, false},
		{[]any{"Coordinator", []any{"Warning", []any{"None"}}}, 1, false},
	} {
		if got := profileComplete(c.profile, c.shards); got != c.want {
			t.Errorf("profileComplete(%v, %d) = %v, want %v", c.profile, c.shards, got, c.want)
		}
	}
}
