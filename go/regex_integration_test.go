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

// Integration tests for regular-expression matching on table data. They run
// against the Redis server at REDIS_URI and are skipped when it is unset.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSQLRegex(t *testing.T) {
	h := newSQLHarness(t)
	drop := func() {
		h.exec("DROP VIEW IF EXISTS it_re_v")
		h.exec("DROP TABLE IF EXISTS it_re")
		h.exec("DROP TABLE IF EXISTS it_re_pat")
		h.exec("DROP TABLE IF EXISTS it_re_ctas")
	}
	drop()
	t.Cleanup(drop)
	// s is indexed (a TAG field); u holds the same text in a NOINDEX column.
	h.exec(`CREATE TABLE it_re (id INTEGER, s VARCHAR, u VARCHAR NOINDEX, n INTEGER)`)
	h.exec(`INSERT INTO it_re VALUES
		(1, 'apple', 'apple', 10),
		(2, 'Apricot', 'Apricot', 20),
		(3, 'banana', 'banana', 30),
		(4, 'a.b', 'a.b', 40),
		(5, 'x%y_z', 'x%y_z', 50),
		(6, 'line1
line2', 'line1
line2', 60),
		(7, '', '', 70),
		(8, NULL, NULL, 80),
		(9, 'héllo wörld', 'héllo wörld', 90),
		(10, 'user@example.com', 'user@example.com', 100)`)

	// The issue's examples.
	h.expectRows(`SELECT s ~ '^a', s SIMILAR TO 'a%', REGEXP_LIKE(s, '^a'), SUBSTRING(s FROM '^.') FROM it_re WHERE id <= 3 ORDER BY id`,
		"true|true|true|a", "false|false|false|A", "false|false|false|b")
	h.expectRows(`SELECT REGEXP_SUBSTR('abc123', '[0-9]+')`, "123")

	// Filters give the same rows on the indexed and the NOINDEX column, and
	// NULL matches neither a pattern nor its negation.
	for _, c := range []struct {
		where string // %[1]s is the column
		rows  []string
	}{
		{`%[1]s ~ '^a'`, []string{"1", "4"}},
		{`%[1]s ~* '^a'`, []string{"1", "2", "4"}},
		{`%[1]s !~ 'a'`, []string{"2", "5", "6", "7", "9"}},
		{`%[1]s !~* 'a'`, []string{"5", "6", "7", "9"}},
		{`%[1]s ~ 'a\.b'`, []string{"4"}},
		{`%[1]s ~ 'p{2}'`, []string{"1"}},
		{`%[1]s ~ '^$'`, []string{"7"}},
		{`%[1]s ~ '[éö]'`, []string{"9"}},
		{`%[1]s ~ 'line1.line2'`, []string{"6"}}, // '.' matches the newline
		{`%[1]s ~ '^line2'`, nil},                // ^ anchors the whole string
		{`REGEXP_LIKE(%[1]s, '^line2', 'n')`, []string{"6"}},
		{`%[1]s SIMILAR TO 'a%%'`, []string{"1", "4"}},
		{`%[1]s SIMILAR TO '%%(an|ic)%%'`, []string{"2", "3"}},
		{`%[1]s NOT SIMILAR TO '%%a%%'`, []string{"2", "5", "6", "7", "9"}},
		{`%[1]s SIMILAR TO 'x#%%y#_z' ESCAPE '#'`, []string{"5"}},
		{`%[1]s SIMILAR TO 'x\%%y\_z'`, []string{"5"}},
		{`%[1]s SIMILAR TO 'x_y_z'`, []string{"5"}},
		{`%[1]s SIMILAR TO '[a-z]+@[a-z]+.com'`, []string{"10"}}, // '.' is literal
		{`%[1]s SIMILAR TO 'line1_line2'`, []string{"6"}},
		{`%[1]s SIMILAR TO ''`, []string{"7"}},
		{`REGEXP_LIKE(%[1]s, 'A', 'i') AND n > 20`, []string{"3", "4", "10"}},
		{`REGEXP_COUNT(%[1]s, 'a') >= 2`, []string{"3"}},
		{`REGEXP_INSTR(%[1]s, 'l') = 3`, []string{"9"}},
		{`SUBSTRING(%[1]s SIMILAR '%%@#"%%#".com' ESCAPE '#') = 'example'`, []string{"10"}},
		{`(%[1]s ~ '^a') IS NULL`, []string{"8"}},
	} {
		for _, col := range []string{"s", "u"} {
			h.expectRows(fmt.Sprintf("SELECT id FROM it_re WHERE "+c.where+" ORDER BY id", col), c.rows...)
		}
	}

	// Patterns are evaluated by the driver on the rows the index returns;
	// one over constants is computed once and still pushes down.
	for _, c := range []struct {
		sql, query string
		residual   bool
		rows       []string
	}{
		{`SELECT id FROM it_re WHERE s ~ '^ap' ORDER BY id`, "*", true, []string{"1"}},
		{`SELECT id FROM it_re WHERE s SIMILAR TO 'ap%' ORDER BY id`, "*", true, []string{"1"}},
		{`SELECT id FROM it_re WHERE n >= 30 AND s ~ 'a' ORDER BY id`, "@n:[30 +inf]", true, []string{"3", "4", "10"}},
		{`SELECT id FROM it_re WHERE s = REGEXP_REPLACE('b-a-n-a-n-a', '-', '', 'g')`, `@s:{banana}`, false, []string{"3"}},
	} {
		if q, residual, _ := h.planOf(c.sql); q != c.query || residual != c.residual {
			t.Errorf("%s: index query %q, residual %v; want %q, residual %v", c.sql, q, residual, c.query, c.residual)
		}
		h.expectRows(c.sql, c.rows...)
	}
	h.expectRows(`SELECT COUNT(*) FROM it_re WHERE s ~ 'a'`, "4")

	// The functions on table data, NULL in, NULL out.
	h.expectRows(`SELECT id, s ~ 'an', REGEXP_SUBSTR(s, '[aeiou]+', 1, 2), REGEXP_COUNT(s, '[aeiou]'), REGEXP_INSTR(s, 'a'),
			REGEXP_REPLACE(s, '[aeiou]', '*', 'g'), SUBSTRING(s FROM '^(.)'), REGEXP_MATCH(s, '(.)(.)$')
		FROM it_re WHERE id <> 6 ORDER BY id`,
		"1|false|e|2|1|*ppl*|a|{l,e}",
		"2|false|o|2|0|Apr*c*t|A|{o,t}",
		"3|true|a|3|2|b*n*n*|b|{n,a}",
		"4|false|NULL|1|1|*.b|a|{.,b}",
		"5|false|NULL|0|0|x%y_z|x|{_,z}",
		"7|false|NULL|0|0||NULL|NULL",
		"8|NULL|NULL|NULL|NULL|NULL|NULL|NULL",
		"9|false|NULL|1|0|héll* wörld|h|{l,d}",
		"10|false|e|6|8|*s*r@*x*mpl*.c*m|u|{o,m}")
	h.expectRows(`SELECT id, REGEXP_REPLACE(s, 'a', 'A', 2, 0), REGEXP_REPLACE(s, 'a', 'A', 1, 2), REGEXP_INSTR(s, 'a', 1, 2, 1),
			SUBSTRING(s SIMILAR '%#"n_#"%' ESCAPE '#'), SUBSTRING(s FROM 2 FOR 3)
		FROM it_re WHERE id IN (1, 3) ORDER BY id`,
		"1|apple|apple|0|NULL|ppl",
		"3|bAnAnA|banAna|5|na|ana")

	// In GROUP BY and ORDER BY, in a join condition, and in a view (whose
	// filter is rewritten over the base table).
	h.expectRows(`SELECT REGEXP_LIKE(s, 'a') AS has_a, COUNT(*) FROM it_re GROUP BY has_a ORDER BY has_a`,
		"false|5", "true|4", "NULL|1")
	h.expectRows(`SELECT id FROM it_re WHERE id <= 5 ORDER BY REGEXP_COUNT(s, '[ap]') DESC, id`, "1", "3", "2", "4", "5")
	h.exec(`CREATE TABLE it_re_pat (pid INTEGER, pat VARCHAR)`)
	h.exec(`INSERT INTO it_re_pat VALUES (1, '^a'), (2, 'an'), (3, '@'), (4, NULL)`)
	h.expectRows(`SELECT p.pid, r.id FROM it_re_pat p JOIN it_re r ON r.s ~ p.pat ORDER BY p.pid, r.id`,
		"1|1", "1|4", "2|3", "3|10")
	h.expectRows(`SELECT pid, (SELECT COUNT(*) FROM it_re r WHERE r.u ~ p.pat) FROM it_re_pat p ORDER BY pid`,
		"1|2", "2|1", "3|1", "4|0")
	h.exec(`CREATE VIEW it_re_v AS SELECT id, s FROM it_re WHERE s ~* 'a'`)
	h.expectRows(`SELECT id FROM it_re_v WHERE s SIMILAR TO '%n%' ORDER BY id`, "3")

	// Result types: CTAS columns and information_schema; the new columns
	// are indexed and can be filtered on.
	h.exec(`CREATE TABLE it_re_ctas AS SELECT id, s ~ 'a' AS m, s SIMILAR TO 'a%' AS sim, REGEXP_LIKE(s, 'a') AS l,
		REGEXP_COUNT(s, 'a') AS c, REGEXP_INSTR(s, 'a') AS i, REGEXP_SUBSTR(s, 'a.') AS sub, REGEXP_MATCH(s, 'a') AS mt,
		REGEXP_REPLACE(s, 'a', 'b', 1, 0) AS r, SUBSTRING(s FROM 'a.') AS sf, SUBSTRING(s SIMILAR 'a#"%#"' ESCAPE '#') AS ss
		FROM it_re`)
	h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_re_ctas' ORDER BY ordinal_position`,
		"id|INTEGER", "m|BOOLEAN", "sim|BOOLEAN", "l|BOOLEAN", "c|BIGINT", "i|BIGINT", "sub|VARCHAR", "mt|VARCHAR",
		"r|VARCHAR", "sf|VARCHAR", "ss|VARCHAR")
	h.expectRows(`SELECT id, m, sim, c, i, sub, mt, r, sf, ss FROM it_re_ctas WHERE id IN (1, 3, 4, 8) ORDER BY id`,
		"1|true|true|1|1|ap|{a}|bpple|ap|pple",
		"3|true|false|3|2|an|{a}|bbnbnb|an|NULL",
		"4|true|true|1|1|a.|{a}|b.b|a.|.b",
		"8|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT id FROM it_re_ctas WHERE m = TRUE ORDER BY id`, "1", "3", "4", "10")
	h.expectRows(`SELECT id FROM it_re_ctas WHERE ss = 'pple'`, "1")

	// In INSERT, UPDATE and DELETE.
	h.exec(`INSERT INTO it_re (id, s, u) VALUES (11, REGEXP_REPLACE('t-m-p', '-', '', 'g'), SUBSTRING('xyz' FROM 'y.'))`)
	h.expectRows(`SELECT s, u FROM it_re WHERE id = 11`, "tmp|yz")
	h.expectAffected(`UPDATE it_re SET u = REGEXP_REPLACE(u, '(\w+)@(\w+)', '\2 at \1') WHERE s ~ '@'`, 1)
	h.expectRows(`SELECT u FROM it_re WHERE id = 10`, "example at user.com")
	h.expectAffected(`DELETE FROM it_re WHERE s SIMILAR TO 't(m|x)p'`, 1)
	h.expectRows(`SELECT COUNT(*) FROM it_re`, "10")

	// Errors on table data. Patterns are only compiled for rows that are
	// evaluated, as in Postgres; argument counts are checked when planning.
	h.expectError(`SELECT id FROM it_re WHERE s ~ '('`, "invalid regular expression: missing closing )")
	h.expectError(`SELECT id FROM it_re WHERE u SIMILAR TO '(a'`, "invalid regular expression: missing closing )")
	h.expectError(`SELECT REGEXP_SUBSTR(s, 'a', 0) FROM it_re`, `REGEXP_SUBSTR: invalid value for parameter "start": 0`)
	h.expectError(`SELECT REGEXP_LIKE(s, 'a', 'g') FROM it_re`, `REGEXP_LIKE does not support the "global" option`)
	h.expectError(`SELECT id FROM it_re WHERE s SIMILAR TO 'a' ESCAPE 'ab'`, "invalid escape string")
	h.expectRows(`SELECT id FROM it_re WHERE id > 100 AND s ~ '('`)
	h.expectError(`SELECT REGEXP_LIKE(s) FROM it_re WHERE id > 100`, "REGEXP_LIKE expects 2 or 3 arguments")
}

// Bound parameters as patterns, flags and positions: a text parameter makes
// SUBSTRING … FROM a pattern match and an integer one a position.
func TestSQLRegexBind(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_re_bind")
	h.exec("CREATE TABLE it_re_bind (id INTEGER, s VARCHAR)")
	h.exec("INSERT INTO it_re_bind VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, NULL)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_re_bind") })

	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, `SELECT id, SUBSTRING(s FROM ?), SUBSTRING(s FROM ? FOR ?), REGEXP_REPLACE(s, ?, ?, ?)
		FROM it_re_bind WHERE s ~ ? ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	mem := memory.DefaultAllocator
	var fields []arrow.Field
	var cols []arrow.Array
	for i, p := range []any{"a(.)", int64(2), int64(3), "a", "A", "g", "a"} {
		switch v := p.(type) {
		case string:
			b := array.NewStringBuilder(mem)
			b.Append(v)
			fields = append(fields, arrow.Field{Name: fmt.Sprintf("p%d", i), Type: arrow.BinaryTypes.String})
			cols = append(cols, b.NewArray())
		case int64:
			b := array.NewInt64Builder(mem)
			b.Append(v)
			fields = append(fields, arrow.Field{Name: fmt.Sprintf("p%d", i), Type: arrow.PrimitiveTypes.Int64})
			cols = append(cols, b.NewArray())
		}
	}
	if err := st.Bind(h.ctx, array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 1)); err != nil {
		t.Fatal(err)
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	var got []string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			var cells []string
			for c := 0; c < int(rec.NumCols()); c++ {
				cells = append(cells, rec.Column(c).ValueStr(r))
			}
			got = append(got, strings.Join(cells, "|"))
		}
	}
	if want := []string{"1|p|ppl|Apple", "2|n|ana|bAnAnA"}; strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("bound parameters: got %q, want %q", got, want)
	}
}
