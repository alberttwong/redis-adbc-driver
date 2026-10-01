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

// Function resolution. A call is looked up when its expression is bound
// (executor.bind), not when a row evaluates it: an unknown function, or a
// known one with the wrong number of arguments, is an error when the
// statement is planned, or when CREATE TABLE, ALTER TABLE or CREATE VIEW
// defines the expression, whether or not any row is read (an empty table,
// WHERE false, LIMIT 0). Errors that depend on values (1/0, a cast that
// fails) are still raised by the row that causes them, as in Postgres.
//
// The functions are listed by family: scalarArity (scalar.go), jsonArity
// and jsonFuncs (json.go), builtinArity below, aggregateFuncs (whose calls
// checkAggregate checks) and windowOnlyFuncs (checkWindow).

import "fmt"

// builtinArity gives the minimum and maximum number of arguments of the
// scalar functions evaluated in eval.go and datetime.go (-1: no maximum).
var builtinArity = map[string][2]int{
	"COALESCE": {1, -1}, "IFNULL": {2, 2}, "NVL": {2, 2}, "CONCAT": {1, -1}, "CONCAT_WS": {1, -1},
	"FROM_HEX": {1, 1}, "UNHEX": {1, 1}, "DECODE_HEX": {1, 1}, "TO_HEX": {1, 1}, "HEX": {1, 1},
	"LOWER": {1, 1}, "LCASE": {1, 1}, "UPPER": {1, 1}, "UCASE": {1, 1},
	"LENGTH": {1, 1}, "CHAR_LENGTH": {1, 1}, "CHARACTER_LENGTH": {1, 1}, "LEN": {1, 1},
	"LIKE": {2, 3}, "ILIKE": {2, 3}, "ABS": {1, 1}, "MERGE_ACTION": {0, 0},

	// An optional precision argument is accepted and ignored.
	"CURRENT_DATE": {0, 1}, "CURRENT_TIMESTAMP": {0, 1}, "NOW": {0, 1}, "TRANSACTION_TIMESTAMP": {0, 1},
	"STATEMENT_TIMESTAMP": {0, 1}, "LOCALTIMESTAMP": {0, 1}, "CURRENT_TIME": {0, 1}, "LOCALTIME": {0, 1},

	"DATE_PART": {2, 2}, "__INTERVAL": {2, 2}, "AGE": {1, 2}, "DATE_TRUNC": {2, 2},
	"YEAR": {1, 1}, "QUARTER": {1, 1}, "MONTH": {1, 1}, "WEEK": {1, 1}, "DAY": {1, 1}, "DAYOFMONTH": {1, 1},
	"DAYOFYEAR": {1, 1}, "HOUR": {1, 1}, "MINUTE": {1, 1}, "SECOND": {1, 1}, "EPOCH": {1, 1}, "EPOCH_MS": {1, 1},
	"DATE_DIFF": {3, 3}, "DATEDIFF": {3, 3}, "TIMESTAMPDIFF": {3, 3},
	"DATEADD": {3, 3}, "DATE_ADD": {3, 3}, "DATE_SUB": {3, 3}, "TIMESTAMPADD": {3, 3},
	"LAST_DAY": {1, 1}, "MAKE_DATE": {3, 3}, "MAKE_TIME": {3, 3}, "MAKE_TIMESTAMP": {6, 6}, "MAKE_TIMESTAMPTZ": {6, 6},
	"TO_TIMESTAMP": {1, 2}, "TO_DATE": {2, 2}, "TO_CHAR": {2, 2},
}

// checkArity resolves a call by name and checks its number of arguments.
// Aggregates are checked by checkAggregate (the JSON ones here too), window
// functions by checkWindow and GROUPING by the grouping-sets planner.
func checkArity(f *Func) error {
	switch {
	case aggregateFuncs[f.Name] || jsonFuncs[f.Name]:
		return checkJSONArity(f)
	case windowOnlyFuncs[f.Name] || f.Name == "GROUPING":
		return nil
	}
	a, ok := scalarArity[f.Name]
	if !ok {
		a, ok = builtinArity[f.Name]
	}
	if !ok {
		return fmt.Errorf("unsupported function %s", f.Name)
	}
	n := len(f.Args)
	switch {
	case f.Star || f.Distinct:
		return fmt.Errorf("%s does not accept * or DISTINCT", f.Name)
	case a[1] < 0 && n < a[0]:
		return fmt.Errorf("%s expects at least %d argument(s)", f.Name, a[0])
	case a[0] == a[1] && n != a[0]:
		return fmt.Errorf("%s expects %d argument(s)", f.Name, a[0])
	case a[1] > a[0]+1 && (n < a[0] || n > a[1]):
		return fmt.Errorf("%s expects %d to %d arguments", f.Name, a[0], a[1])
	case n < a[0] || (a[1] >= 0 && n > a[1]):
		return fmt.Errorf("%s expects %d or %d arguments", f.Name, a[0], a[1])
	}
	return nil
}
