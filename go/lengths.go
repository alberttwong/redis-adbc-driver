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

// String lengths: VARCHAR(n) and CHAR(n), as in Postgres.
//
//   - Types: VARCHAR(n) (also CHARACTER VARYING(n), CHAR VARYING(n) and
//     NVARCHAR(n)) holds at most n characters. CHAR(n) (also CHARACTER(n),
//     NCHAR(n) and BPCHAR(n)) holds exactly n: shorter values are padded
//     with spaces. CHAR without a length is CHAR(1), BPCHAR without one is
//     blank-padded without a limit, and VARCHAR without one, VARCHAR(MAX),
//     TEXT, STRING and CLOB have no limit. Lengths count characters (runes),
//     not bytes, and go from 1 to 10485760. The length and "blank-padded"
//     are part of the column type (ColType.Length and Fixed), so they are in
//     the table's metadata (optional fields: older metadata reads as
//     unbounded VARCHAR, which is what older versions created).
//   - Casts: CAST(x AS VARCHAR(n)) and x::char(n) cut the text to n
//     characters, and CHAR(n) pads it.
//   - Writes: a value written to a VARCHAR(n) or CHAR(n) column (INSERT …
//     VALUES / SELECT / DEFAULT VALUES, UPDATE, MERGE, bulk ingest) that is
//     longer than n is an error, `value too long for type character
//     varying(n)` (character(n)), unless the characters beyond n are all
//     spaces, which are cut. It is checked where NOT NULL and CHECK are
//     (checkNewRow, setValues), before them, so a failing statement writes
//     nothing. A column's DEFAULT is checked when it is defined.
//   - CHAR values: stored and returned padded. Their trailing spaces don't
//     count: a comparison with a CHAR value compares both sides without
//     trailing spaces (Postgres converts the other side to CHAR when it is
//     a literal, but compares a text column with trailing spaces as text),
//     and so do the keys of joins, IN lists, GROUP BY, DISTINCT and set
//     operations. Converting one to text (functions, ||, a VARCHAR column)
//     drops them, as Postgres does, except for LIKE, ILIKE, ~, ~* and
//     SIMILAR TO, which see the padded value.
//   - Result types keep a length where Postgres keeps the type modifier:
//     column references, casts, scalar subqueries, and CASE, COALESCE,
//     NULLIF, GREATEST, LEAST and set operations whose inputs all have the
//     same type. Function, aggregate and window results have none, so
//     CREATE TABLE … AS and views report the length of a column or cast
//     they select and none for a computed value.

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// maxStringLength is the longest declared length, Postgres's.
const maxStringLength = 10485760

// stringTypeFromSQL returns the type of VARCHAR(n) / CHAR(n) and their
// synonyms (fixed: the blank-padded CHAR types).
func stringTypeFromSQL(spec sqlTypeSpec, fixed bool) (ColType, error) {
	t := ColType{Kind: KindString, Fixed: fixed}
	if len(spec.Params) == 0 {
		if fixed && spec.Name != "BPCHAR" {
			t.Length = 1
		}
		return t, nil
	}
	if len(spec.Params) > 1 {
		return ColType{}, fmt.Errorf("invalid type modifier for type %s", strings.ToLower(spec.Name))
	}
	name := "varchar"
	if fixed {
		name = "char"
	}
	n := spec.Params[0]
	switch {
	case n < 1:
		return ColType{}, fmt.Errorf("length for type %s must be at least 1", name)
	case n > maxStringLength:
		return ColType{}, fmt.Errorf("length for type %s cannot exceed %d", name, maxStringLength)
	}
	t.Length = int32(n)
	return t, nil
}

// pgName is Postgres's name of a bounded string type, as in its errors.
func (t ColType) pgName() string {
	if t.Fixed {
		return fmt.Sprintf("character(%d)", t.Length)
	}
	return fmt.Sprintf("character varying(%d)", t.Length)
}

// withoutLength is t without a string length: the type of a computed value.
func (t ColType) withoutLength() ColType {
	t.Length = 0
	return t
}

// isChar reports whether t is a blank-padded (CHAR) type.
func (t ColType) isChar() bool { return t.Kind == KindString && t.Fixed }

// trimPadding drops the trailing spaces of a CHAR value's text.
func trimPadding(s string) string { return strings.TrimRight(s, " ") }

// charValue is the CHAR value of type t for text s, padded to t's length.
func charValue(s string, t ColType) Value {
	if t.Length > 0 {
		if n := int(t.Length) - runeCount(s); n > 0 {
			s += strings.Repeat(" ", n)
		}
	}
	return Value{T: t, S: s}
}

// runeCount counts the characters of s; each byte that isn't valid UTF-8
// counts as one.
func runeCount(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

// cutRunes returns the first n characters of s, and whether s is longer.
func cutRunes(s string, n int) (string, bool) {
	for i := range s {
		if n == 0 {
			return s[:i], true
		}
		n--
	}
	return s, false
}

// castValue is CAST(v AS t): Coerce, except that a cast to a bounded string
// type cuts the text to its length, as Postgres's explicit casts do.
func castValue(v Value, t ColType) (Value, error) {
	if t.Kind != KindString || t.Length == 0 || v.Null {
		return Coerce(v, t)
	}
	s, err := Coerce(v, ColType{Kind: KindString, Fixed: t.Fixed})
	if err != nil {
		return Value{}, err
	}
	s.S, _ = cutRunes(s.S, int(t.Length))
	return Coerce(s, t)
}

// fitLength checks a value (already coerced to t) that is written to a
// column of type t against its length, as Postgres's assignment does: a
// longer value is an error unless the characters beyond the length are all
// spaces, which are cut.
func fitLength(t ColType, v Value) (Value, error) {
	if t.Kind != KindString || t.Length == 0 || v.Null {
		return v, nil
	}
	head, long := cutRunes(v.S, int(t.Length))
	if !long {
		return v, nil
	}
	if strings.TrimLeft(v.S[len(head):], " ") != "" {
		return Value{}, errorf(adbc.StatusInvalidData, "value too long for type %s", t.pgName())
	}
	v.S = head
	return v, nil
}

// fitLengths applies fitLength to a row ordered like meta.Columns.
func fitLengths(meta *tableMeta, row []Value) error {
	for i, c := range meta.Columns {
		v, err := fitLength(c.Type, row[i])
		if err != nil {
			return err
		}
		row[i] = v
	}
	return nil
}

// sameText reports whether a cast to type to leaves the values of type from
// as they are (s::text of a VARCHAR(n) column).
func sameText(from, to ColType) bool {
	if from == to {
		return true
	}
	return from.Kind == KindString && to == typeString && !from.Fixed
}
