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
	"fmt"
	"strings"
	"testing"
)

// guessOne guesses the type of a field from its values, one HASH each.
func guessOne(t *testing.T, values ...string) *guessedColumn {
	t.Helper()
	rows := make([]hashRow, len(values))
	for i, v := range values {
		rows[i] = hashRow{key: fmt.Sprintf("k:%d", i+1), fields: []string{"f"}, values: []string{v}}
	}
	cols, _, err := guessTable(rows, guessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return cols[0]
}

func TestGuessColumnTypes(t *testing.T) {
	for _, c := range []struct {
		values []string
		want   string
		note   string // a note the column must have
	}{
		{[]string{"1", "-2", "+30"}, "BIGINT", ""},
		{[]string{"0", "1", "1"}, "BIGINT", "only 0 and 1"},
		{[]string{"true", "False", "t", "no"}, "BOOLEAN", "true/false text"},
		{[]string{"1", "true"}, "VARCHAR", ""},
		{[]string{"12.50", "3", "0.125"}, "NUMERIC(38,3)", ""},
		{[]string{"99999999999999999999"}, "NUMERIC(38,0)", ""},
		{[]string{"1.5", "2e3"}, "DOUBLE PRECISION", ""},
		{[]string{"0.3333333333333333"}, "DOUBLE PRECISION", ""},
		{[]string{"1.5", "NaN", "-inf"}, "DOUBLE PRECISION", "NaN or infinite"},
		{[]string{"007", "12345"}, "VARCHAR", "leading zeros"},
		{[]string{"2024-01-15", "2024-02-29"}, "DATE", "YYYY-MM-DD"},
		{[]string{"2024-02-30"}, "VARCHAR", ""},
		{[]string{"2024-01-15T10:00:00Z", "2024-01-15 10:00:00+02"}, "TIMESTAMP(6) WITH TIME ZONE", "ISO 8601"},
		{[]string{"2024-01-15T10:00:00.123456789"}, "TIMESTAMP(9)", ""},
		{[]string{"2024-01-15T10:00", "2024-01-16"}, "TIMESTAMP(6)", ""},
		{[]string{"2024-01-15T10:00:00Z", "2024-01-15T10:00:00"}, "TIMESTAMP(6) WITH TIME ZONE", "no UTC offset"},
		{[]string{"\xff\xfe"}, "VARBINARY", "not UTF-8"},
		{[]string{`{"a": 1}`, `[1, 2]`}, "VARCHAR", "JSON text"},
		{[]string{"", "5"}, "BIGINT", "1 empty values read as NULL"},
		{[]string{""}, "VARCHAR", ""},
		{[]string{"a\x00b"}, "VARCHAR", "NUL byte"},
	} {
		col := guessOne(t, c.values...)
		if got := col.t.SQLName(); got != c.want {
			t.Errorf("%q: %s, want %s", c.values, got, c.want)
		}
		if c.note != "" && !strings.Contains(strings.Join(col.notes, "; "), c.note) {
			t.Errorf("%q: notes %q, want one with %q", c.values, col.notes, c.note)
		}
	}
}

// A few values that don't fit make a column VARCHAR, and the type the rest
// fit is suggested.
func TestGuessOutliers(t *testing.T) {
	values := make([]string, 0, 50)
	for i := 0; i < 49; i++ {
		values = append(values, fmt.Sprint(i))
	}
	values = append(values, "n/a")
	col := guessOne(t, values...)
	if col.t.Kind != KindString || col.suggest.Kind != KindInt64 || col.outliers != 1 {
		t.Errorf("got %s, suggest %s, %d outliers", col.t.SQLName(), col.suggest.SQLName(), col.outliers)
	}
	if !strings.Contains(col.outlierNote, `1 of 50 values aren't BIGINT (such as "n/a" in k:50)`) {
		t.Errorf("note %q", col.outlierNote)
	}
	// Half of them is too many to call outliers.
	if col := guessOne(t, "1", "x"); col.outliers != 0 {
		t.Errorf("1 of 2: %d outliers", col.outliers)
	}
}

func TestGuessTable(t *testing.T) {
	rows := []hashRow{
		{key: "u:1", fields: []string{"name", "age", "__rowid", "__secret", "first-name"}, values: []string{"a", "1", "1", "x", "A"}},
		{key: "u:2", fields: []string{"name", "joined"}, values: []string{"b", "2024-01-01"}},
	}
	cols, skipped, err := guessTable(rows, guessOptions{keyColumn: "_key", types: map[string]string{"age": "smallint"},
		renames: map[string]string{"first-name": "first_name"}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range cols {
		got = append(got, c.name+" "+c.t.SQLName()+" "+c.field)
	}
	want := []string{"_key VARCHAR ", "name VARCHAR name", "age SMALLINT age", "first_name VARCHAR first-name", "joined DATE joined"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("columns %q, want %q", got, want)
	}
	if len(skipped) != 2 || !strings.HasPrefix(skipped[0], "__rowid:") || !strings.HasPrefix(skipped[1], "__secret:") {
		t.Errorf("skipped %q", skipped)
	}
	for _, c := range []struct {
		o    guessOptions
		want string
	}{
		{guessOptions{keyColumn: "Name"}, `the HASHes have a field "name", so the key can't be column "Name"`},
		{guessOptions{renames: map[string]string{"joined": "NAME"}}, `columns "name" and "NAME" differ only in case`},
		{guessOptions{renames: map[string]string{"nope": "x"}}, `-rename nope: no sampled HASH has the field "nope"`},
		{guessOptions{types: map[string]string{"nope": "INT"}}, `-type nope: there is no column "nope"`},
		{guessOptions{types: map[string]string{"age": "WIDGET"}}, `-type age=WIDGET:`},
		{guessOptions{types: map[string]string{"age": "INT extra"}}, `-type age=INT extra:`},
	} {
		if _, _, err := guessTable(rows, c.o); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%+v: got %v, want %q", c.o, err, c.want)
		}
	}
}

func TestConvertValue(t *testing.T) {
	for _, c := range []struct {
		v    string
		t    ColType
		want string
	}{
		{"", typeInt64, "NULL"},
		{"", typeString, ""},
		{"42", typeInt64, "42"},
		{"yes", typeBool, "true"},
		{"2024-01-15T10:00:00+02:00", fracType(KindTimestamp, 6, "UTC"), "2024-01-15 08:00:00+00"},
	} {
		v, err := convertValue(c.v, c.t)
		if err != nil {
			t.Errorf("%q as %s: %v", c.v, c.t.SQLName(), err)
			continue
		}
		v, err = Coerce(v, c.t)
		if err != nil {
			t.Fatal(err)
		}
		got := v.Text()
		if v.Null {
			got = "NULL"
		}
		if got != c.want {
			t.Errorf("%q as %s: %q, want %q", c.v, c.t.SQLName(), got, c.want)
		}
	}
	if _, err := convertValue("n/a", typeInt64); err == nil {
		t.Error(`"n/a" as BIGINT: no error`)
	}
}

func TestHashToolHelpers(t *testing.T) {
	for in, want := range map[string]string{"user:": "user", "app:users:": "app_users", "a-b c:": "a_b_c", ":::": "hashes"} {
		if got := TableNameForPrefix(in); got != want {
			t.Errorf("TableNameForPrefix(%q) = %q, want %q", in, got, want)
		}
	}
	if got := globEscape(`a*b?c[d]\e:`); got != `a\*b\?c\[d\]\\e:` {
		t.Errorf("globEscape: %q", got)
	}
	for in, want := range map[string]string{"user:": "user:", "": "''", "a b": "'a b'", "it's": `'it'\''s'`} {
		if got := shellQuote(in); got != want {
			t.Errorf("shellQuote(%q) = %s, want %s", in, got, want)
		}
	}
	for in, want := range map[int64]string{1: "1 key", 2: "2 keys", 12345: "12,345 keys", 1000: "1000 keys"} {
		if got := plural(in, "key"); got != want {
			t.Errorf("plural(%d) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{"user:1": "user:", "a:b:c": "a:b:", "plain": ""} {
		if got := collectionPrefix(in); got != want {
			t.Errorf("collectionPrefix(%q) = %q, want %q", in, got, want)
		}
	}
	if typ, err := parseTypeText("timestamp(3) with time zone"); err != nil || typ.SQLName() != "TIMESTAMP(3) WITH TIME ZONE" {
		t.Errorf("parseTypeText: %v, %v", typ.SQLName(), err)
	}
}
