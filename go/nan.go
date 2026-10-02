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

// NaNs in the index.
//
// A REAL or DOUBLE PRECISION column holds NaN as Postgres does. An indexed
// one is a NUMERIC attribute (see indexAttrArgs), and RediSearch doesn't
// index a HASH with a NUMERIC field it can't read as a number, which "NaN"
// isn't: up to v0.0.7 a row with a NaN was in no index query, so only
// WHERE __rowid = N found it, and DROP TABLE and TRUNCATE (FT.DROPINDEX …
// DD) left its HASH behind (#110).
//
// So a NaN is stored as nanStored, "infinity": a spelling of +Infinity that
// RediSearch reads as +inf (it accepts inf and infinity in any case, with
// or without a sign) but that is neither what the driver writes for
// +Infinity ("+Inf", strconv.FormatFloat) nor what RediSearch replies with
// ("inf"), so decodeStored reads it back as NaN. Values written as "NaN" by
// earlier versions still read as NaN, where WHERE __rowid = N finds them.
//
// Postgres orders NaN above every other number, +Infinity included, and NaN
// equals NaN. As +Infinity, a NaN is in the right place in the index against
// every value but +Infinity:
//
//   - A comparison with a finite constant or -Infinity is pushed down as
//     before, exactly: x > 5 finds the NaNs, x < 5 doesn't.
//   - One with NaN or +Infinity has the bound +inf, inclusive, and is
//     re-checked on the rows: x = 'Infinity' finds both and keeps the
//     +Infinity, x > 'Infinity' keeps the NaNs. IN lists, index lookup joins
//     and semi-joins look such values up the same way (numericBound), and
//     are re-checked anyway. Index queries never contain nanStored or NaN,
//     which their syntax doesn't read.
//   - Each indexed float column records whether it may hold a NaN (NaNs).
//     Statements that write one (INSERT, UPDATE, MERGE, bulk ingest, ADD
//     COLUMN … DEFAULT) set it in the metadata transaction that raises the
//     tag levels (see tags.go), which commits before the rows are written,
//     and it is never cleared. On such a column the index doesn't sort or
//     group, and doesn't compute MIN, MAX, SUM or AVG (aggregate_pushdown
//     all): it would tie the NaNs with +Infinity and take them for it.
//     COUNT(x) still runs in the index, since exists() is true for both.
//
// Hash joins, IN sets, semi-joins, DISTINCT and set operations give all NaNs
// one key (joinKey), so NaN = NaN there too.
//
// Concurrency: as with tag levels, a statement that read a table's metadata
// before another connection set NaNs, and that queries the index after that
// connection's rows are indexed, may sort or aggregate their NaNs as
// +Infinity, in the same window in which it may or may not see the rows at
// all. Earlier versions of the driver write "NaN" and don't set NaNs.

import (
	"math"
	"strconv"
	"strings"
)

// nanStored is the HASH value of a float NaN, which the index reads as +inf.
const nanStored = "infinity"

// numericBound renders a value of an indexed numeric column's type as a
// bound of an index range query: as stored, except that the infinities are
// +inf and -inf, and a NaN, which the index holds as +inf, is +inf too.
func numericBound(v Value) string {
	if v.T.Kind.isFloat() {
		switch {
		case math.IsNaN(v.F), math.IsInf(v.F, 1):
			return "+inf"
		case math.IsInf(v.F, -1):
			return "-inf"
		}
	}
	return encodeStored(v)
}

// parseFloat is strconv.ParseFloat, but also reads -nan and +nan, which it
// doesn't. RediSearch writes a NaN's sign bit: on amd64, +inf + -inf (as in
// a SUM) is the NaN with the sign bit set, so a reducer replies -nan there
// and nan on arm64.
func parseFloat(s string, bitSize int) (float64, error) {
	if len(s) == 4 && (s[0] == '-' || s[0] == '+') && strings.EqualFold(s[1:], "nan") {
		return math.NaN(), nil
	}
	return strconv.ParseFloat(s, bitSize)
}

// pushable reports whether RediSearch returns exact results for aggregates
// over an indexed column and for groups of it: pushableKind of its type,
// except for a float column that may hold a NaN.
func (c columnMeta) pushable(mode string) bool {
	return pushableKind(c.Type.Kind, mode) && !c.NaNs
}
