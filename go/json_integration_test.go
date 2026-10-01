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

// Integration tests for the JSON functions and operators on text (json.go,
// jsonpath.go). Expected results are Postgres's: the operators, constructors
// and aggregates were checked against Postgres 16, the SQL/JSON query
// functions follow Postgres 17.

import (
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// setupJSONTable creates an it_json_ table (id INTEGER, doc VARCHAR) with
// the given docs (SQL literals) as rows 1, 2, …
func (h *sqlHarness) setupJSONTable(name string, docs ...string) {
	h.t.Helper()
	h.exec("DROP TABLE IF EXISTS " + name)
	h.exec("CREATE TABLE " + name + " (id INTEGER NOT NULL, doc VARCHAR)")
	var rows []string
	for i, d := range docs {
		rows = append(rows, "("+strconv.Itoa(i+1)+", "+d+")")
	}
	h.exec("INSERT INTO " + name + " VALUES " + strings.Join(rows, ", "))
	h.t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS " + name) })
}

func TestSQLJSONOperators(t *testing.T) {
	h := newSQLHarness(t)
	h.setupJSONTable("it_json_events",
		`'{"user": {"id": 7, "name": "Ada"}, "tags": ["a", "b"], "amount": 10.50, "ok": true}'`,
		`'{"user": {"id": 8, "name": "Bo"}, "tags": [], "amount": 9007199254740993, "ok": false}'`,
		`'{"user": {"id": 7, "name": "Ada"}, "tags": ["c"], "amount": 1e400, "ok": null}'`,
		`'[1, 2, 3]'`,
		`NULL`)

	// -> keeps the value's text from the document; ->> unquotes strings and
	// gives NULL for JSON null.
	schema := h.expectRows(`SELECT id, doc -> 'user', doc -> 'user' ->> 'name' FROM it_json_events ORDER BY id`,
		`1|{"id": 7, "name": "Ada"}|Ada`, `2|{"id": 8, "name": "Bo"}|Bo`, `3|{"id": 7, "name": "Ada"}|Ada`,
		"4|NULL|NULL", "5|NULL|NULL")
	if dt := schema.Field(1).Type; dt.ID() != arrow.STRING {
		t.Errorf("-> result type = %s, want utf8", dt)
	}
	h.expectRows(`SELECT doc -> 'ok', doc ->> 'ok', doc ->> 'amount' FROM it_json_events ORDER BY id`,
		"true|true|10.50", "false|false|9007199254740993", "null|NULL|1e400", "NULL|NULL|NULL", "NULL|NULL|NULL")

	// Integer operands index arrays, negative ones from the end; a text
	// operand never indexes an array, an integer never reads an object.
	h.expectRows(`SELECT doc -> 'tags' -> 0, doc -> 'tags' ->> -1, doc -> 0, doc -> -1, doc -> -4, doc -> '0' FROM it_json_events ORDER BY id`,
		`"a"|b|NULL|NULL|NULL|NULL`, "NULL|NULL|NULL|NULL|NULL|NULL", `"c"|c|NULL|NULL|NULL|NULL`,
		"NULL|NULL|1|3|NULL|NULL", "NULL|NULL|NULL|NULL|NULL|NULL")

	// #> and #>> take a text array path; integer elements index arrays.
	h.expectRows(`SELECT doc #> '{user,name}', doc #>> '{user, id}', doc #> '{tags,-1}', doc #>> '{2}' FROM it_json_events ORDER BY id`,
		`"Ada"|7|"b"|NULL`, `"Bo"|8|NULL|NULL`, `"Ada"|7|"c"|NULL`, "NULL|NULL|NULL|3", "NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT doc #>> '{}', doc #> '{user,NULL}' FROM it_json_events WHERE id = 4`, "[1, 2, 3]|NULL")
	h.expectError(`SELECT doc #> 'user' FROM it_json_events`, `malformed array literal: "user"`)
	h.expectError(`SELECT doc #> '{user' FROM it_json_events`, `malformed array literal: "{user"`)

	// Filtering, sorting and grouping on extracted values (evaluated by the
	// driver on the rows the index returns).
	h.expectRows(`SELECT id FROM it_json_events WHERE doc -> 'user' ->> 'name' = 'Ada' ORDER BY id`, "1", "3")
	h.expectRows(`SELECT id FROM it_json_events WHERE (doc #>> '{user,id}')::INTEGER > 7`, "2")
	h.expectRows(`SELECT id FROM it_json_events WHERE doc ->> 'ok' = 'true' OR doc -> 1 IS NOT NULL ORDER BY id`, "1", "4")
	h.expectRows(`SELECT id FROM it_json_events WHERE doc IS JSON OBJECT ORDER BY doc -> 'user' ->> 'name' DESC, id`, "2", "1", "3")
	h.expectRows(`SELECT doc -> 'user' ->> 'name' AS name, COUNT(*) FROM it_json_events GROUP BY 1 ORDER BY 1`,
		"Ada|2", "Bo|1", "NULL|2")

	// Precedence as in Postgres: -> ->> #> #>> and || are one level, left to
	// right, below + and above comparisons.
	h.expectRows(`SELECT doc -> 'user' ->> 'name' || '!' FROM it_json_events WHERE id <= 2 ORDER BY id`, "Ada!", "Bo!")
	h.expectRows(`SELECT 'id:' || (doc #>> '{user,id}') FROM it_json_events WHERE id = 1`, "id:7")
	h.expectError(`SELECT 'id:' || doc #>> '{user,id}' FROM it_json_events WHERE id = 1`, "invalid input syntax for type json")
	h.expectRows(`SELECT '{"a":1}' -> 'a' || 'x', '[1,2,3]' -> 1 + 1, '{"a":{"b":2}}' -> 'a' ->> 'b' = '2'`, "1x|3|true")
	h.expectRows(`SELECT 'a' || 1 + 2, 1 + 2 || 'a', POSITION('b' || 'c' IN 'abcd')`, "a3|3a|2")
	h.expectRows(`SELECT '{"a":"x"}' ->> 'a' LIKE 'x%', '{"a":"x"}' ->> 'a' IN ('x', 'y'), '{"a":"m"}' ->> 'a' BETWEEN 'a' AND 'z'`,
		"true|true|true")

	// Wrong operand types.
	h.expectError(`SELECT doc -> 1.5 FROM it_json_events`, "operator does not exist: json -> NUMERIC(2,1)")
	h.expectError(`SELECT id -> 'a' FROM it_json_events`, "operator does not exist: INTEGER -> VARCHAR")
}

func TestSQLJSONFunctions(t *testing.T) {
	h := newSQLHarness(t)
	h.setupJSONTable("it_json_fn",
		`'{"a": {"b": [10, 20, {"c": "deep"}]}, "n": null}'`,
		`'[true, "x", 1.5, null, [], {}]'`,
		`'"just a string"'`,
		`NULL`)

	h.expectRows(`SELECT JSON_EXTRACT_PATH(doc, 'a', 'b', '2'), JSON_EXTRACT_PATH_TEXT(doc, 'a', 'b', '2', 'c'),
		JSON_EXTRACT_PATH_TEXT(doc, 'a', 'b', '-3'), JSON_EXTRACT_PATH(doc, 'a', NULL) FROM it_json_fn ORDER BY id`,
		`{"c": "deep"}|deep|10|NULL`, "NULL|NULL|NULL|NULL", "NULL|NULL|NULL|NULL", "NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT JSON_EXTRACT_PATH_TEXT(doc, '1'), JSON_EXTRACT_PATH(doc, '4') FROM it_json_fn WHERE id = 2`, "x|[]")

	h.expectRows(`SELECT JSON_TYPEOF(doc), JSON_TYPEOF(doc -> 'n'), JSON_TYPEOF(doc #> '{a,b}') FROM it_json_fn ORDER BY id`,
		"object|null|array", "array|NULL|NULL", "string|NULL|NULL", "NULL|NULL|NULL")
	schema := h.expectRows(`SELECT JSON_ARRAY_LENGTH(doc), JSON_ARRAY_LENGTH(doc #> '{a,b}') FROM it_json_fn WHERE id <= 2 AND id > 1`, "6|NULL")
	if dt := schema.Field(0).Type; dt.ID() != arrow.INT64 {
		t.Errorf("JSON_ARRAY_LENGTH type = %s, want int64", dt)
	}
	h.expectRows(`SELECT JSON_ARRAY_LENGTH(doc #> '{a,b}') FROM it_json_fn WHERE id = 1`, "3")
	h.expectError(`SELECT JSON_ARRAY_LENGTH(doc) FROM it_json_fn WHERE id = 1`, "cannot get array length of a non-array")
	h.expectError(`SELECT JSON_ARRAY_LENGTH(doc) FROM it_json_fn WHERE id = 3`, "cannot get array length of a scalar")
	h.expectError(`SELECT JSON_EXTRACT_PATH(doc) FROM it_json_fn`, "JSON_EXTRACT_PATH expects at least 2 argument(s)")
	h.expectError(`SELECT JSON_TYPEOF(id) FROM it_json_fn`, "JSON_TYPEOF expects JSON text, not INTEGER")

	// The JSONB_ spellings give jsonb's normalized text: keys sorted (shorter
	// first) without duplicates, numbers in NUMERIC form.
	h.expectRows(`SELECT JSONB_EXTRACT_PATH('{"a": {"yy": 1, "x": 1.0e1, "x": 2}}', 'a'), JSONB_EXTRACT_PATH_TEXT('{"a": {"x": 1E+2}}', 'a', 'x'),
		JSONB_TYPEOF('[]'), JSONB_ARRAY_LENGTH('[1, 2]')`,
		`{"x": 2, "yy": 1}|100|array|2`)

	// ::json checks the text and keeps it; ::jsonb normalizes it.
	h.expectRows(`SELECT '  {"b": 1, "a": [1,  2]} '::json, '{"b": 1, "a": [1,  2], "b": 0.50}'::jsonb, CAST('[1E2]' AS JSONB), CAST('{}' AS JSON) -> 'a'`,
		`  {"b": 1, "a": [1,  2]} |{"a": [1, 2], "b": 0.50}|[100]|NULL`)
	h.expectError(`SELECT 'nope'::json`, `invalid input syntax for type json: token "nope" is invalid`)
	h.expectError(`SELECT CAST(5 AS JSONB)`, "cannot cast type BIGINT to jsonb")

	// IS [NOT] JSON [VALUE | SCALAR | ARRAY | OBJECT] [WITH UNIQUE KEYS]
	h.expectRows(`SELECT id, doc IS JSON, doc IS JSON OBJECT, doc IS JSON ARRAY, doc IS JSON SCALAR, doc IS NOT JSON VALUE FROM it_json_fn ORDER BY id`,
		"1|true|true|false|false|false", "2|true|false|true|false|false", "3|true|false|false|true|false", "4|NULL|NULL|NULL|NULL|NULL")
	h.expectRows(`SELECT 'x' IS JSON, '' IS JSON, '  1 ' IS JSON, '{"a":1,"a":2}' IS JSON WITH UNIQUE KEYS,
		'{"a":1,"a":2}' IS JSON WITHOUT UNIQUE KEYS, '[{"a":{"b":1,"b":2}}]' IS JSON WITH UNIQUE, '{"a":1,"b":2}' IS JSON OBJECT WITH UNIQUE KEYS`,
		"false|false|true|false|true|false|true")
	h.expectError(`SELECT id IS JSON FROM it_json_fn`, "cannot use type INTEGER in IS JSON predicate")
}

func TestSQLJSONMalformed(t *testing.T) {
	h := newSQLHarness(t)
	h.setupJSONTable("it_json_bad", `'{"a": 1}'`, `'not json'`, `'{"a":'`, `''`, `'{"a": 1} x'`)

	// The Postgres operators and functions raise Postgres's error.
	h.expectError(`SELECT doc -> 'a' FROM it_json_bad WHERE id = 2`, `invalid input syntax for type json: token "not" is invalid`)
	h.expectError(`SELECT doc ->> 'a' FROM it_json_bad WHERE id = 3`, "invalid input syntax for type json: the input string ended unexpectedly")
	h.expectError(`SELECT JSON_TYPEOF(doc) FROM it_json_bad WHERE id = 4`, "invalid input syntax for type json: the input string ended unexpectedly")
	h.expectError(`SELECT doc #> '{a}' FROM it_json_bad WHERE id = 5`, `invalid input syntax for type json: token "x" is invalid`)
	h.expectError(`SELECT JSON_EXTRACT_PATH(doc, 'a') FROM it_json_bad`, "invalid input syntax for type json")
	h.expectError(`SELECT doc::json FROM it_json_bad WHERE id = 5`, `token "x" is invalid`)
	for sql, detail := range map[string]string{
		`'{"a" 1}'`:       `expected ":", but found "1"`,
		`'[1,]'`:          `expected JSON value, but found "]"`,
		`'[1 2]'`:         `expected "," or "]", but found "2"`,
		`'{"a":1 "b":2}'`: `expected "," or "}", but found ""b""`,
		`'{,}'`:           `expected string or "}", but found ","`,
		`'{"a":1,}'`:      `expected string, but found "}"`,
		`'1 2'`:           `expected end of input, but found "2"`,
		`'{"a":01}'`:      `token "01" is invalid`,
		`'.5'`:            `token "." is invalid`,
		`'"abc'`:          `token ""abc" is invalid`,
		`'["\q"]'`:        `escape sequence "\q" is invalid`,
		`'["\u12"]'`:      `"\u" must be followed by four hexadecimal digits`,
	} {
		h.expectError("SELECT "+sql+"::json", "invalid input syntax for type json: "+detail)
	}
	h.expectError("SELECT '\"a\nb\"'::json", "invalid input syntax for type json: character with value 0x0a must be escaped")

	// Guarded by IS JSON, the bad rows are never parsed.
	h.expectRows(`SELECT id, doc ->> 'a' FROM it_json_bad WHERE doc IS JSON ORDER BY id`, "1|1")

	// The SQL/JSON functions handle malformed documents with ON ERROR
	// (NULL by default, FALSE for JSON_EXISTS).
	h.expectRows(`SELECT id, JSON_VALUE(doc, '$.a'), JSON_VALUE(doc, '$.a' DEFAULT 'bad' ON ERROR), JSON_QUERY(doc, '$'),
		JSON_EXISTS(doc, '$.a'), JSON_EXISTS(doc, '$.a' UNKNOWN ON ERROR) FROM it_json_bad ORDER BY id`,
		`1|1|1|{"a": 1}|true|true`, "2|NULL|bad|NULL|false|NULL", "3|NULL|bad|NULL|false|NULL",
		"4|NULL|bad|NULL|false|NULL", "5|NULL|bad|NULL|false|NULL")
	h.expectError(`SELECT JSON_VALUE(doc, '$.a' ERROR ON ERROR) FROM it_json_bad WHERE id = 2`, `token "not" is invalid`)
	h.expectError(`SELECT JSON_QUERY(doc, '$' ERROR ON ERROR) FROM it_json_bad WHERE id = 3`, "the input string ended unexpectedly")
	h.expectError(`SELECT JSON_EXISTS(doc, '$' ERROR ON ERROR) FROM it_json_bad WHERE id = 4`, "the input string ended unexpectedly")
}

func TestSQLJSONExactNumbers(t *testing.T) {
	h := newSQLHarness(t)
	h.setupJSONTable("it_json_nums",
		`'{"n": 9007199254740993}'`,
		`'{"n": 1e400}'`,
		`'{"n": 0.1000000000000000055511151231257827}'`,
		`'{"n": -0.0}'`,
		`'{"n": 12345678901234567890.123456789012345678}'`,
		`'{"n": 1E+2}'`,
		`'{"n": 1.5e-3}'`)

	// -> and ->> return a number's text as written.
	h.expectRows(`SELECT doc -> 'n', doc ->> 'n' FROM it_json_nums ORDER BY id`,
		"9007199254740993|9007199254740993", "1e400|1e400",
		"0.1000000000000000055511151231257827|0.1000000000000000055511151231257827", "-0.0|-0.0",
		"12345678901234567890.123456789012345678|12345678901234567890.123456789012345678", "1E+2|1E+2", "1.5e-3|1.5e-3")
	// Constructors and aggregates embed it unchanged.
	h.expectRows(`SELECT JSON_BUILD_OBJECT('n', doc -> 'n') FROM it_json_nums WHERE id = 2`, `{"n" : 1e400}`)
	h.expectRows(`SELECT JSON_AGG(doc -> 'n' ORDER BY id) FROM it_json_nums`,
		"[9007199254740993, 1e400, 0.1000000000000000055511151231257827, -0.0, 12345678901234567890.123456789012345678, 1E+2, 1.5e-3]")

	// jsonb and the SQL/JSON functions use NUMERIC: exact, scale kept.
	zeros400 := "1" + strings.Repeat("0", 400)
	h.expectRows(`SELECT JSON_VALUE(doc, '$.n'), (doc::jsonb) ->> 'n' FROM it_json_nums ORDER BY id`,
		"9007199254740993|9007199254740993", zeros400+"|"+zeros400,
		"0.1000000000000000055511151231257827|0.1000000000000000055511151231257827", "0.0|0.0",
		"12345678901234567890.123456789012345678|12345678901234567890.123456789012345678", "100|100", "0.0015|0.0015")
	h.expectRows(`SELECT JSON_VALUE(doc, '$.n' RETURNING BIGINT), CAST(doc ->> 'n' AS NUMERIC(20,0)) FROM it_json_nums WHERE id = 1`,
		"9007199254740993|9007199254740993")
	schema := h.expectRows(`SELECT JSON_VALUE(doc, '$.n' RETURNING NUMERIC(38,18)) FROM it_json_nums WHERE id = 5`,
		"12345678901234567890.123456789012345678")
	if dt := schema.Field(0).Type; dt.ID() != arrow.DECIMAL128 {
		t.Errorf("RETURNING NUMERIC(38,18) type = %s, want decimal128", dt)
	}
	// Only an explicit cast to a double rounds.
	h.expectRows(`SELECT CAST(doc ->> 'n' AS DOUBLE PRECISION) FROM it_json_nums WHERE id = 1`, "9.007199254740992e+15")
}

func TestSQLJSONUnicode(t *testing.T) {
	h := newSQLHarness(t)
	h.setupJSONTable("it_json_text",
		"'{\"s\": \"caf\\u00e9\", \"e\": \"\\ud83d\\ude00\"}'",
		"'{\"s\": \"tab\\there\", \"q\": \"say \\\"hi\\\"\", \"b\": \"back\\\\slash\", \"sl\": \"a\\/b\"}'",
		"'{\"s\": \"\\u0000\"}'",
		"'{\"s\": \"\\ud800\"}'",
		"'{\"caf\\u00e9\": 1, \"\\u0041\": 2}'")

	// ->> decodes escapes, -> keeps the text as written.
	h.expectRows(`SELECT doc ->> 's', doc ->> 'e', doc -> 's' FROM it_json_text WHERE id = 1`, "café|😀|\"caf\\u00e9\"")
	h.expectRows(`SELECT doc ->> 's', doc ->> 'q', doc ->> 'b', doc ->> 'sl' FROM it_json_text WHERE id = 2`,
		"tab\there|say \"hi\"|back\\slash|a/b")
	h.expectRows(`SELECT doc -> 'café', doc ->> 'A', JSON_VALUE(doc, '$."café"') FROM it_json_text WHERE id = 5`, "1|2|1")

	// \u0000 and unpaired surrogates are valid json text but can't be decoded.
	h.expectRows(`SELECT doc IS JSON, JSON_TYPEOF(doc), doc::json = doc FROM it_json_text WHERE id >= 3 AND id <= 4 ORDER BY id`,
		"true|object|true", "true|object|true")
	h.expectError(`SELECT doc ->> 's' FROM it_json_text WHERE id = 3`, "unsupported Unicode escape sequence: \\u0000 cannot be converted to text")
	h.expectError(`SELECT doc::jsonb FROM it_json_text WHERE id = 3`, "unsupported Unicode escape sequence")
	h.expectError(`SELECT doc -> 's' FROM it_json_text WHERE id = 4`, "invalid input syntax for type json: Unicode low surrogate must follow a high surrogate")
	h.expectError("SELECT '[\"\\ud83d\\ud83d\"]' ->> 0", "Unicode high surrogate must not follow a high surrogate")
	h.expectRows(`SELECT JSON_VALUE(doc, '$.s'), JSON_VALUE(doc, '$.s' DEFAULT '?' ON ERROR) FROM it_json_text WHERE id = 4`, "NULL|?")

	// jsonb decodes escapes and writes the characters, escaping only
	// quotes, backslashes and control characters.
	h.expectRows(`SELECT doc::jsonb FROM it_json_text WHERE id <= 2 ORDER BY id`,
		`{"e": "😀", "s": "café"}`, `{"b": "back\\slash", "q": "say \"hi\"", "s": "tab\there", "sl": "a/b"}`)

	// Encoding text: quotes, backslashes and control characters are
	// escaped, other characters (including non-ASCII) are kept.
	h.exec("DROP TABLE IF EXISTS it_json_chars")
	h.exec("CREATE TABLE it_json_chars (s VARCHAR)")
	t.Cleanup(func() { h.exec("DROP TABLE IF EXISTS it_json_chars") })
	h.exec("INSERT INTO it_json_chars VALUES ('two\nlines\x01 café \"q\" \\ /')")
	h.expectRows(`SELECT TO_JSON(s), JSON_BUILD_ARRAY(s) ->> 0 = s, TO_JSONB(s) FROM it_json_chars`,
		"\"two\\nlines\\u0001 café \\\"q\\\" \\\\ /\"|true|\"two\\nlines\\u0001 café \\\"q\\\" \\\\ /\"")
}

func TestSQLJSONPath(t *testing.T) {
	h := newSQLHarness(t)
	h.setupJSONTable("it_json_orders",
		`'{"customer": {"name": "Ada", "tier": "gold"}, "items": [{"sku": "A1", "qty": 2, "price": 9.99}, {"sku": "B2", "qty": 1, "price": 25.00}], "shipped": true, "placed": "2024-03-01"}'`,
		`'{"customer": {"name": "Bo"}, "items": [], "shipped": false, "placed": "2024-03-05"}'`,
		`'{"customer": {"name": "Cy", "tier": null}, "items": [{"sku": "C3", "qty": 5, "price": 1.25}], "placed": "not a date"}'`)

	// JSON_VALUE: one scalar, as text or the RETURNING type; NULL ON EMPTY
	// and NULL ON ERROR by default.
	h.expectRows(`SELECT JSON_VALUE(doc, '$.customer.name'), JSON_VALUE(doc, '$.customer.tier'),
		JSON_VALUE(doc, '$.customer.tier' DEFAULT 'none' ON EMPTY), JSON_VALUE(doc, 'strict $.customer.tier' DEFAULT 'err' ON ERROR)
		FROM it_json_orders ORDER BY id`,
		"Ada|gold|gold|gold", "Bo|NULL|none|err", "Cy|NULL|NULL|NULL")
	schema := h.expectRows(`SELECT JSON_VALUE(doc, '$.items[0].qty' RETURNING INTEGER), JSON_VALUE(doc, '$.items[last].price' RETURNING NUMERIC(6,2)),
		JSON_VALUE(doc, '$.placed' RETURNING DATE), JSON_VALUE(doc, '$.shipped' RETURNING BOOLEAN) FROM it_json_orders ORDER BY id`,
		"2|25.00|2024-03-01|true", "NULL|NULL|2024-03-05|false", "5|1.25|NULL|NULL")
	for i, want := range []arrow.Type{arrow.INT32, arrow.DECIMAL128, arrow.DATE32, arrow.BOOL} {
		if dt := schema.Field(i).Type; dt.ID() != want {
			t.Errorf("JSON_VALUE RETURNING column %d type = %s, want %s", i, dt, want)
		}
	}
	h.expectError(`SELECT JSON_VALUE(doc, '$.placed' RETURNING DATE ERROR ON ERROR) FROM it_json_orders WHERE id = 3`, "invalid date")
	h.expectError(`SELECT JSON_VALUE(doc, '$.customer.tier' ERROR ON EMPTY) FROM it_json_orders WHERE id = 2`,
		"no SQL/JSON item found for specified path")
	h.expectRows(`SELECT JSON_VALUE(doc, '$.items[0].qty' RETURNING INTEGER DEFAULT -1 ON EMPTY),
		JSON_VALUE(doc, '$.customer' RETURNING INTEGER DEFAULT -2 ON ERROR) FROM it_json_orders ORDER BY id`,
		"2|-2", "-1|-2", "5|-2")

	// Several items, or an object or array, are errors for JSON_VALUE.
	h.expectRows(`SELECT JSON_VALUE(doc, '$.items[*].sku'), JSON_VALUE(doc, '$.items.sku'), JSON_VALUE(doc, 'strict $.items.sku'),
		JSON_VALUE(doc, '$.items[0]') FROM it_json_orders ORDER BY id`,
		"NULL|NULL|NULL|NULL", "NULL|NULL|NULL|NULL", "C3|C3|NULL|NULL")
	h.expectError(`SELECT JSON_VALUE(doc, '$.items[*].sku' ERROR ON ERROR) FROM it_json_orders WHERE id = 1`,
		"JSON path expression in JSON_VALUE must return single scalar item")
	h.expectError(`SELECT JSON_VALUE(doc, 'strict $.customer.tier' ERROR ON ERROR) FROM it_json_orders WHERE id = 2`,
		`JSON object does not contain key "tier"`)

	// JSON_QUERY returns JSON (jsonb's form); wrappers make an array of the
	// items.
	h.expectRows(`SELECT JSON_QUERY(doc, '$.items[0]'), JSON_QUERY(doc, '$.items[*].sku' WITH WRAPPER),
		JSON_QUERY(doc, '$.items[*].sku' WITH CONDITIONAL WRAPPER), JSON_QUERY(doc, '$.items[*].sku') FROM it_json_orders ORDER BY id`,
		`{"qty": 2, "sku": "A1", "price": 9.99}|["A1", "B2"]|["A1", "B2"]|NULL`, "NULL|NULL|NULL|NULL",
		`{"qty": 5, "sku": "C3", "price": 1.25}|["C3"]|["C3"]|"C3"`)
	h.expectRows(`SELECT JSON_QUERY(doc, '$.items' WITH CONDITIONAL WRAPPER), JSON_QUERY(doc, '$.customer.*' WITH UNCONDITIONAL ARRAY WRAPPER),
		JSON_QUERY(doc, '$.customer.name' RETURNING VARCHAR OMIT QUOTES), JSON_QUERY(doc, '$.customer.name' KEEP QUOTES ON SCALAR STRING)
		FROM it_json_orders WHERE id = 1`,
		`[{"qty": 2, "sku": "A1", "price": 9.99}, {"qty": 1, "sku": "B2", "price": 25.00}]|["Ada", "gold"]|Ada|"Ada"`)
	h.expectRows(`SELECT JSON_QUERY(doc, '$.missing'), JSON_QUERY(doc, '$.missing' EMPTY ARRAY ON EMPTY),
		JSON_QUERY(doc, '$.missing' EMPTY OBJECT ON EMPTY), JSON_QUERY(doc, '$.missing' DEFAULT '{"b":1, "a":2}' ON EMPTY),
		JSON_QUERY(doc, '$.items[0].qty' RETURNING INTEGER) FROM it_json_orders WHERE id = 1`,
		`NULL|[]|{}|{"a": 2, "b": 1}|2`)
	h.expectError(`SELECT JSON_QUERY(doc, '$.items[*]' ERROR ON ERROR) FROM it_json_orders WHERE id = 1`,
		"JSON path expression in JSON_QUERY must return single item when no wrapper is requested")
	h.expectRows(`SELECT JSON_QUERY('{"a": "[1,2]", "b": "x"}', '$.a' OMIT QUOTES), JSON_QUERY('{"a": "[1,2]", "b": "x"}', '$.b' OMIT QUOTES)`,
		"[1, 2]|NULL")
	h.expectError(`SELECT JSON_QUERY(doc, '$.items' WITH WRAPPER OMIT QUOTES) FROM it_json_orders`,
		"SQL/JSON QUOTES behavior must not be specified when WITH WRAPPER is used")

	// JSON_EXISTS: lax paths give false for missing items; strict-mode errors
	// go to ON ERROR (FALSE by default).
	h.expectRows(`SELECT id, JSON_EXISTS(doc, '$.customer.tier'), JSON_EXISTS(doc, '$.items[1]'), JSON_EXISTS(doc, 'strict $.customer.tier'),
		JSON_EXISTS(doc, 'strict $.customer.tier' TRUE ON ERROR) FROM it_json_orders ORDER BY id`,
		"1|true|true|true|true", "2|false|false|false|true", "3|true|false|true|true")
	h.expectRows(`SELECT id FROM it_json_orders WHERE JSON_EXISTS(doc, '$.items[0]') ORDER BY id`, "1", "3")
	h.expectError(`SELECT JSON_EXISTS(doc, 'strict $.customer.tier' ERROR ON ERROR) FROM it_json_orders WHERE id = 2`,
		`JSON object does not contain key "tier"`)

	// The path language: lax/strict, ."quoted" keys, .*, [*], subscripts n,
	// last, last - n, n to m and lists.
	h.expectRows(`SELECT JSON_QUERY('[1,2,3,4,5]', '$[1 to 3]' WITH WRAPPER), JSON_QUERY('[1,2,3,4,5]', '$[0, last - 1 to last]' WITH WRAPPER),
		JSON_QUERY('[1,2,3,4,5]', 'lax $[3 to 10]' WITH WRAPPER), JSON_QUERY('[1,2,3,4,5]', 'strict $[3 to 10]' WITH WRAPPER),
		JSON_VALUE('[1,2,3]', '$[-1]'), JSON_VALUE('{"a b": {"c": 1}}', '$."a b".c'), JSON_VALUE('7', '$[0]'), JSON_VALUE('7', 'strict $[0]'),
		JSON_QUERY('{"a": [1, [2]]}', '$.a[*]' WITH WRAPPER), JSON_VALUE('{"a": [{"b": 1}]}', '$.a.b'), JSON_VALUE('[[{"b": 1}]]', '$.b')`,
		"[2, 3, 4]|[1, 4, 5]|[4, 5]|NULL|NULL|1|7|NULL|[1, [2]]|1|NULL")
	// Documents are jsonb: the last of duplicate keys wins, and .* reads the
	// values in jsonb's key order.
	h.expectRows(`SELECT JSON_VALUE('{"a": 1, "a": 2}', '$.a'), JSON_QUERY('{"b": 1, "a": {"y": 1, "x": 2}}', '$.*' WITH WRAPPER)`,
		`2|[{"x": 2, "y": 1}, 1]`)
	for path, msg := range map[string]string{
		"bad":           `syntax error at or near "bad" of jsonpath input`,
		"$.":            "syntax error at end of jsonpath input",
		"$.a ? (@ > 1)": "jsonpath filter expressions are not supported",
		"$.a.size()":    "the jsonpath item method .size() is not supported",
		"$.**":          "the jsonpath accessor .** is not supported",
		"$x":            `could not find jsonpath variable "x"`,
		"$[1.5]":        `syntax error at or near "." of jsonpath input`,
	} {
		h.expectError("SELECT JSON_VALUE('{}', '"+path+"')", msg)
	}
	h.expectError(`SELECT JSON_VALUE('{}', '$' PASSING 1 AS x)`, "JSON_VALUE: PASSING is not supported")
	h.expectError(`SELECT JSON_VALUE('{}', '$' EMPTY ON ERROR)`, "invalid ON ERROR behavior for JSON_VALUE")
	h.expectError(`SELECT JSON_EXISTS('{}', '$' NULL ON ERROR)`, "invalid ON ERROR behavior for JSON_EXISTS")
	h.expectRows(`SELECT JSON_VALUE(NULL, '$'), JSON_VALUE('{}', NULL), JSON_EXISTS(NULL, '$')`, "NULL|NULL|NULL")
}

func TestSQLJSONConstructors(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_json_types")
	h.exec("DROP TABLE IF EXISTS it_json_ctas")
	h.exec(`CREATE TABLE it_json_types (id INTEGER NOT NULL, i BIGINT, n NUMERIC(10,2), f DOUBLE PRECISION, r REAL, b BOOLEAN,
		s VARCHAR, d DATE, ts TIMESTAMP(6), tz TIMESTAMP(3) WITH TIME ZONE, tm TIME(6), iv INTERVAL, bin VARBINARY)`)
	t.Cleanup(func() {
		h.exec("DROP TABLE IF EXISTS it_json_types")
		h.exec("DROP TABLE IF EXISTS it_json_ctas")
	})
	h.exec("INSERT INTO it_json_types VALUES (1, 42, 10.50, 1.5, 2.25, true, 'he said \"hi\"\n', DATE '2024-01-15', " +
		"TIMESTAMP '2024-01-15 10:30:00.250', TIMESTAMP WITH TIME ZONE '2024-01-15 10:30:00-08', TIME '08:05:00', INTERVAL '1 day 2 hours', X'00ff')")
	h.exec("INSERT INTO it_json_types (id) VALUES (2)")

	// SQL values are encoded as Postgres's to_json does.
	h.expectRows(`SELECT JSON_BUILD_OBJECT('i', i, 'n', n, 'f', f, 'r', r, 'b', b, 's', s, 'd', d, 'ts', ts, 'tz', tz, 't', tm, 'iv', iv, 'bin', bin)
		FROM it_json_types ORDER BY id`,
		`{"i" : 42, "n" : 10.50, "f" : 1.5, "r" : 2.25, "b" : true, "s" : "he said \"hi\"\n", "d" : "2024-01-15", "ts" : "2024-01-15T10:30:00.25", `+
			`"tz" : "2024-01-15T18:30:00+00:00", "t" : "08:05:00", "iv" : "1 day 02:00:00", "bin" : "\\x00ff"}`,
		`{"i" : null, "n" : null, "f" : null, "r" : null, "b" : null, "s" : null, "d" : null, "ts" : null, "tz" : null, "t" : null, "iv" : null, "bin" : null}`)
	h.expectRows(`SELECT JSON_BUILD_ARRAY(i, n, f, b, s, d), JSONB_BUILD_OBJECT('s', s, 'i', i, 'n', n) FROM it_json_types ORDER BY id`,
		`[42, 10.50, 1.5, true, "he said \"hi\"\n", "2024-01-15"]|{"i": 42, "n": 10.50, "s": "he said \"hi\"\n"}`,
		`[null, null, null, null, null, null]|{"i": null, "n": null, "s": null}`)
	h.expectRows(`SELECT TO_JSON(ts), TO_JSON(n), TO_JSONB(s), TO_JSON(i), TO_JSON(CAST('NaN' AS DOUBLE PRECISION)) FROM it_json_types WHERE id = 1`,
		`"2024-01-15T10:30:00.25"|10.50|"he said \"hi\"\n"|42|"NaN"`)
	h.expectRows(`SELECT JSON_BUILD_OBJECT(), JSON_BUILD_ARRAY(), JSON_BUILD_OBJECT(1, 'one', true, 'yes', DATE '2024-01-01', 0)`,
		`{}|[]|{"1" : "one", "true" : "yes", "2024-01-01" : 0}`)

	// JSON arguments (JSON functions, -> and #>, ::json) are embedded; other
	// text becomes a JSON string.
	h.expectRows(`SELECT JSON_BUILD_OBJECT('outer', JSON_BUILD_OBJECT('inner', i), 'arr', JSON_BUILD_ARRAY(1, 2), 'txt', '{"not":"json"}',
		'cast', '{"is":"json"}'::json, 'sub', '{"a": [1,  2]}' -> 'a', 'text', ('{"a": [1]}' -> 'a')::VARCHAR) FROM it_json_types WHERE id = 1`,
		`{"outer" : {"inner" : 42}, "arr" : [1, 2], "txt" : "{\"not\":\"json\"}", "cast" : {"is":"json"}, "sub" : [1,  2], "text" : "[1]"}`)
	h.expectRows(`SELECT JSONB_BUILD_OBJECT('outer', JSON_BUILD_OBJECT('inner', i, 'a', 0), 'cast', '{"z": 1, "is":"json"}'::json) FROM it_json_types WHERE id = 1`,
		`{"cast": {"z": 1, "is": "json"}, "outer": {"a": 0, "inner": 42}}`)
	h.expectRows(`SELECT JSON_BUILD_ARRAY(COALESCE(NULL, '{"a":1}' -> 'a'), CASE WHEN id = 1 THEN '{"x":1}'::json END) FROM it_json_types ORDER BY id`,
		`[1, {"x":1}]`, `[1, null]`)

	// SQL/JSON JSON_OBJECT and JSON_ARRAY: k VALUE v or k : v, NULL ON NULL
	// (JSON_OBJECT) or ABSENT ON NULL (JSON_ARRAY) by default.
	h.expectRows(`SELECT JSON_OBJECT('i' VALUE i, 'b': b), JSON_OBJECT('i' VALUE i, 'b': b ABSENT ON NULL),
		JSON_ARRAY(i, s, b), JSON_ARRAY(i, s, b NULL ON NULL) FROM it_json_types ORDER BY id`,
		`{"i" : 42, "b" : true}|{"i" : 42, "b" : true}|[42, "he said \"hi\"\n", true]|[42, "he said \"hi\"\n", true]`,
		`{"i" : null, "b" : null}|{}|[]|[null, null, null]`)
	h.expectRows(`SELECT JSON_OBJECT('b': 1, 'a': '[1,  2]' FORMAT JSON RETURNING JSONB), JSON_OBJECT('a': 1, 'a': 2),
		JSON_OBJECT('a': 1 RETURNING VARCHAR), JSON_ARRAY('[1]' FORMAT JSON, 2 RETURNING JSONB), JSON_OBJECT(), JSON_ARRAY()`,
		`{"a": [1, 2], "b": 1}|{"a" : 1, "a" : 2}|{"a" : 1}|[[1], 2]|{}|[]`)
	h.expectError(`SELECT JSON_OBJECT('a': 1, 'a': 2 WITH UNIQUE KEYS)`, `duplicate JSON object key value: "a"`)
	h.expectError(`SELECT JSON_OBJECT('a': 1 RETURNING INTEGER)`, "cannot use RETURNING type INTEGER in JSON_OBJECT")
	h.expectError(`SELECT JSON_ARRAY(SELECT 1)`, "JSON_ARRAY(SELECT …) is not supported")
	// Postgres's json_object over text arrays.
	h.expectRows(`SELECT JSON_OBJECT('{a,1,b,2}'), JSON_OBJECT('{a,b}', '{1,NULL}'), JSON_OBJECT('{{a,1},{"b c",2}}'), JSONB_OBJECT('{b,1,a,2}'), JSON_OBJECT('{}')`,
		`{"a" : "1", "b" : "2"}|{"a" : "1", "b" : null}|{"a" : "1", "b c" : "2"}|{"a": "2", "b": "1"}|{}`)
	h.expectError(`SELECT JSON_OBJECT('{a,1,b}')`, "array must have even number of elements")
	h.expectError(`SELECT JSON_OBJECT('{a,b}', '{1}')`, "mismatched array dimensions")

	// Keys must be non-NULL scalars.
	h.expectError(`SELECT JSON_BUILD_OBJECT(s, 1) FROM it_json_types WHERE id = 2`, "null value not allowed for object key")
	h.expectError(`SELECT JSON_BUILD_OBJECT('{"a":1}'::json, 1)`, "key value must be scalar, not array, composite, or json")
	h.expectError(`SELECT JSON_BUILD_OBJECT('a')`, "JSON_BUILD_OBJECT: argument list must have even number of elements")
	h.expectError(`SELECT JSON_OBJECT(NULL VALUE 1)`, "null value not allowed for object key")

	// Results are VARCHAR: they can be stored, updated and read back.
	h.exec(`CREATE TABLE it_json_ctas AS SELECT id, JSON_BUILD_OBJECT('i', i, 'n', n) AS j FROM it_json_types`)
	h.expectRows(`SELECT data_type FROM information_schema.columns WHERE table_name = 'it_json_ctas' AND column_name = 'j'`, "VARCHAR")
	h.expectRows(`SELECT id, j ->> 'i', j -> 'n' FROM it_json_ctas ORDER BY id`, "1|42|10.50", "2|NULL|null")
	h.exec(`UPDATE it_json_ctas SET j = JSONB_BUILD_OBJECT('old', j::json, 'k', 'v') WHERE id = 1`)
	h.exec(`INSERT INTO it_json_ctas VALUES (3, JSON_ARRAY(1, 2))`)
	h.expectRows(`SELECT id, j FROM it_json_ctas ORDER BY id`,
		`1|{"k": "v", "old": {"i": 42, "n": 10.50}}`, `2|{"i" : null, "n" : null}`, "3|[1, 2]")
}

func TestSQLJSONAggregates(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP TABLE IF EXISTS it_json_sales")
	h.exec("DROP TABLE IF EXISTS it_json_regions")
	h.exec("CREATE TABLE it_json_sales (id INTEGER NOT NULL, region VARCHAR, item VARCHAR, qty INTEGER, price NUMERIC(6,2))")
	h.exec("CREATE TABLE it_json_regions (region VARCHAR, manager VARCHAR)")
	t.Cleanup(func() {
		h.exec("DROP TABLE IF EXISTS it_json_sales")
		h.exec("DROP TABLE IF EXISTS it_json_regions")
	})
	h.exec(`INSERT INTO it_json_sales VALUES (1, 'east', 'apple', 3, 1.50), (2, 'east', 'pear', NULL, 2.00), (3, 'west', 'apple', 5, 1.50),
		(4, 'east', 'fig', 1, 3.25), (5, 'west', 'kiwi', 2, NULL), (6, 'north', NULL, 4, 9.99)`)
	h.exec(`INSERT INTO it_json_regions VALUES ('east', 'Ann'), ('west', 'Wes')`)

	// JSON_AGG keeps NULLs; ORDER BY inside the call orders the elements
	// (NULLs last by default in both directions, as in the driver's ORDER BY).
	h.expectRows(`SELECT region, JSON_AGG(item ORDER BY id), JSON_AGG(qty ORDER BY qty DESC), JSON_AGG(qty ORDER BY qty DESC NULLS FIRST)
		FROM it_json_sales GROUP BY region ORDER BY region`,
		`east|["apple", "pear", "fig"]|[3, 1, null]|[null, 3, 1]`, "north|[null]|[4]|[4]", `west|["apple", "kiwi"]|[5, 2]|[5, 2]`)
	h.expectRows(`SELECT JSON_AGG(item ORDER BY price DESC NULLS LAST, id), JSONB_AGG(price ORDER BY id) FROM it_json_sales WHERE region = 'east'`,
		`["fig", "pear", "apple"]|[1.50, 2.00, 3.25]`)
	// DISTINCT values come out sorted, as in Postgres.
	h.expectRows(`SELECT JSON_AGG(DISTINCT item), JSON_AGG(DISTINCT region ORDER BY region DESC) FROM it_json_sales`,
		`["apple", "fig", "kiwi", "pear", null]|["west", "north", "east"]`)
	h.expectError(`SELECT JSON_AGG(DISTINCT item ORDER BY id) FROM it_json_sales`,
		"in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list")

	// JSON_OBJECT_AGG keeps duplicate keys; JSONB_OBJECT_AGG keeps the last.
	h.expectRows(`SELECT region, JSON_OBJECT_AGG(item, qty ORDER BY id) FROM it_json_sales WHERE item IS NOT NULL GROUP BY region ORDER BY region`,
		`east|{ "apple" : 3, "pear" : null, "fig" : 1 }`, `west|{ "apple" : 5, "kiwi" : 2 }`)
	h.expectRows(`SELECT JSONB_OBJECT_AGG(item, price), JSON_OBJECT_AGG(id, qty ORDER BY id), JSONB_OBJECT_AGG(region, id ORDER BY id)
		FROM it_json_sales WHERE region = 'east'`,
		`{"fig": 3.25, "pear": 2.00, "apple": 1.50}|{ "1" : 3, "2" : null, "4" : 1 }|{"east": 4}`)
	h.expectError(`SELECT JSON_OBJECT_AGG(item, qty) FROM it_json_sales`, "null value not allowed for object key")

	// No rows: NULL, as for other aggregates other than COUNT.
	h.expectRows(`SELECT COUNT(*), JSON_AGG(item), JSON_OBJECT_AGG(item, id) FROM it_json_sales WHERE id > 100`, "0|NULL|NULL")

	// JSON values are embedded; aggregates can be mixed and used in HAVING.
	h.expectRows(`SELECT JSON_AGG(JSON_BUILD_OBJECT('item', item, 'qty', qty) ORDER BY id) FROM it_json_sales WHERE region = 'west'`,
		`[{"item" : "apple", "qty" : 5}, {"item" : "kiwi", "qty" : 2}]`)
	h.expectRows(`SELECT region, COUNT(*), SUM(qty), JSON_AGG(id ORDER BY id) FROM it_json_sales GROUP BY region ORDER BY region`,
		"east|3|4|[1, 2, 4]", "north|1|4|[6]", "west|2|7|[3, 5]")
	h.expectRows(`SELECT region FROM it_json_sales GROUP BY region HAVING JSON_ARRAY_LENGTH(JSON_AGG(item)) > 2`, "east")
	h.expectRows(`SELECT r.manager, JSON_AGG(s.item ORDER BY s.id) FROM it_json_sales s JOIN it_json_regions r ON s.region = r.region
		GROUP BY r.manager ORDER BY 1`,
		`Ann|["apple", "pear", "fig"]`, `Wes|["apple", "kiwi"]`)

	// The JSON aggregates run in the driver, even over indexed columns.
	for _, sql := range []string{
		`SELECT region, SUM(qty), JSON_AGG(qty) FROM it_json_sales GROUP BY region`,
		`SELECT JSON_OBJECT_AGG(region, qty) FROM it_json_sales`,
	} {
		if _, _, indexAgg := h.planOf(sql); indexAgg {
			t.Errorf("%s must not run in FT.AGGREGATE", sql)
		}
	}

	// A JSON result read back from a subquery is text: it becomes a JSON
	// string unless cast with ::json.
	h.expectRows(`WITH g AS (SELECT region, JSON_AGG(item ORDER BY id) AS items FROM it_json_sales GROUP BY region)
		SELECT JSON_AGG(items ORDER BY region), JSON_AGG(items::json ORDER BY region) FROM g`,
		`["[\"apple\", \"pear\", \"fig\"]", "[null]", "[\"apple\", \"kiwi\"]"]|[["apple", "pear", "fig"], [null], ["apple", "kiwi"]]`)

	h.expectError(`SELECT JSON_AGG(id) OVER () FROM it_json_sales`, "JSON_AGG is not a window function")
	h.expectError(`SELECT JSON_AGG(*) FROM it_json_sales`, "JSON_AGG does not accept *")
	h.expectError(`SELECT JSON_AGG(id, item) FROM it_json_sales`, "JSON_AGG expects 1 argument(s)")
	h.expectError(`SELECT JSON_OBJECT_AGG('{"a":1}'::json, id) FROM it_json_sales`, "key value must be scalar")
	h.expectError(`SELECT LOWER(item ORDER BY id) FROM it_json_sales`, "ORDER BY specified, but LOWER is not an aggregate function")
}

func TestSQLJSONInQueries(t *testing.T) {
	h := newSQLHarness(t)
	h.exec("DROP VIEW IF EXISTS it_json_qv")
	h.exec("DROP TABLE IF EXISTS it_json_qgroups")
	h.exec("DROP TABLE IF EXISTS it_json_qcopy")
	h.setupJSONTable("it_json_q", `'{"k": "x", "n": 1, "g": "a"}'`, `'{"k": "y", "n": 2, "g": "a"}'`, `'{"k": "x", "n": 3, "g": "b"}'`)
	h.exec("CREATE TABLE it_json_qgroups (g VARCHAR)")
	h.exec("CREATE TABLE it_json_qcopy (id INTEGER, name VARCHAR, tags VARCHAR)")
	t.Cleanup(func() {
		h.exec("DROP VIEW IF EXISTS it_json_qv")
		h.exec("DROP TABLE IF EXISTS it_json_qgroups")
		h.exec("DROP TABLE IF EXISTS it_json_qcopy")
	})
	h.exec("INSERT INTO it_json_qgroups VALUES ('a'), ('b'), ('c')")

	// Correlated subqueries, joins, DISTINCT, set operations, windows.
	h.expectRows(`SELECT g, (SELECT JSON_AGG(q.doc ->> 'k' ORDER BY q.id DESC) FROM it_json_q q WHERE q.doc ->> 'g' = gr.g) FROM it_json_qgroups gr ORDER BY g`,
		`a|["y", "x"]`, `b|["x"]`, "c|NULL")
	h.expectRows(`SELECT gr.g, JSON_AGG(q.id ORDER BY q.id) FROM it_json_qgroups gr LEFT JOIN it_json_q q ON q.doc ->> 'g' = gr.g GROUP BY gr.g ORDER BY 1`,
		"a|[1, 2]", "b|[3]", "c|[null]")
	h.expectRows(`SELECT DISTINCT doc ->> 'k' FROM it_json_q ORDER BY 1`, "x", "y")
	h.expectRows(`SELECT doc ->> 'k' FROM it_json_q UNION SELECT 'z' ORDER BY 1`, "x", "y", "z")
	h.expectRows(`SELECT id FROM it_json_q WHERE id IN (SELECT (doc ->> 'n')::INTEGER FROM it_json_q WHERE doc ->> 'k' = 'x') ORDER BY id`, "1", "3")
	h.expectRows(`SELECT doc ->> 'k', SUM((doc ->> 'n')::INTEGER), JSON_AGG(id ORDER BY id) FROM it_json_q GROUP BY doc ->> 'k' ORDER BY 1`,
		"x|4|[1, 3]", "y|2|[2]")
	h.expectRows(`SELECT id, ROW_NUMBER() OVER (ORDER BY JSON_VALUE(doc, '$.n' RETURNING INTEGER) DESC) FROM it_json_q ORDER BY id`,
		"1|3", "2|2", "3|1")

	// Views: a filter on an extracted column is evaluated on the base rows.
	h.exec(`CREATE VIEW it_json_qv AS SELECT id, doc ->> 'k' AS k, JSON_VALUE(doc, '$.n' RETURNING INTEGER) AS n FROM it_json_q`)
	h.expectRows(`SELECT id, k, n FROM it_json_qv WHERE k = 'x' ORDER BY id`, "1|x|1", "3|x|3")
	h.expectRows(`SELECT k, JSON_AGG(n ORDER BY n) FROM it_json_qv GROUP BY k ORDER BY k`, "x|[1, 3]", "y|[2]")

	// INSERT … SELECT, UPDATE and DELETE with JSON expressions.
	h.exec(`INSERT INTO it_json_qcopy SELECT id, doc ->> 'k', JSON_BUILD_ARRAY(doc ->> 'g', id) FROM it_json_q`)
	h.exec(`UPDATE it_json_qcopy SET tags = JSON_BUILD_ARRAY(name) WHERE tags ->> 0 = 'a'`)
	h.expectRows(`SELECT id, tags FROM it_json_qcopy ORDER BY id`, `1|["x"]`, `2|["y"]`, `3|["b", 3]`)
	h.exec(`DELETE FROM it_json_qcopy WHERE JSON_ARRAY_LENGTH(tags) = 1`)
	h.expectRows(`SELECT id, tags -> 1 FROM it_json_qcopy`, "3|3")
}

func TestSQLJSONBind(t *testing.T) {
	h := newSQLHarness(t)
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, `SELECT ? ->> ?, ? -> ?, JSON_VALUE(?, ?), JSON_BUILD_OBJECT(?, ?)`); err != nil {
		t.Fatal(err)
	}
	mem := memory.DefaultAllocator
	str := func(s string) arrow.Array {
		b := array.NewStringBuilder(mem)
		b.Append(s)
		return b.NewArray()
	}
	ib := array.NewInt64Builder(mem)
	ib.Append(-1)
	doc := `{"a": [1, 2, {"b": "x"}]}`
	fields := []arrow.Field{}
	cols := []arrow.Array{str(doc), str("a"), str(`[10, 20]`), ib.NewArray(), str(doc), str("$.a[2].b"), str("k"), str("v")}
	for i, c := range cols {
		fields = append(fields, arrow.Field{Name: "p" + strconv.Itoa(i), Type: c.DataType()})
	}
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 1)
	if err := st.Bind(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	rdr.Next()
	r := rdr.RecordBatch()
	var cells []string
	for i := 0; i < int(r.NumCols()); i++ {
		cells = append(cells, r.Column(i).ValueStr(0))
	}
	if got, want := strings.Join(cells, "|"), `[1, 2, {"b": "x"}]|20|x|{"k" : "v"}`; got != want {
		t.Errorf("bound parameters: got %q, want %q", got, want)
	}
}
