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

// Quantified comparisons: x op ANY | SOME | ALL (SELECT …).
//
// `= ANY` is IN and `<> ALL` is NOT IN (the parser rewrites them, so they
// get IN's index unions, hash sets and semi-joins). The other forms compare
// x with every value of the subquery, with SQL's three-valued logic: ANY is
// true if a comparison is true, otherwise NULL if one is NULL, otherwise
// false; ALL is false if a comparison is false, otherwise NULL if one is
// NULL, otherwise true. So ANY over no rows is false and ALL is true, even
// for a NULL x.
//
// For an uncorrelated subquery and an ordering operator (<, <=, >, >=), the
// comparison with the smallest or largest value decides, when x and all the
// values are of one class that compares without conversion (sameEqClass):
// x > ANY (…) is x > MIN, x > ALL (…) is x > MAX. The bounds are computed
// once per statement.

// quantExtremes are the smallest and largest non-NULL values of an
// uncorrelated ANY / ALL subquery. ok is false when the values are not all
// of one class (or there are none); sawNull is set if one is NULL.
type quantExtremes struct {
	min, max Value
	ok       bool
	sawNull  bool
}

func newQuantExtremes(rows [][]Value) *quantExtremes {
	q := &quantExtremes{ok: true}
	has := false
	for _, r := range rows {
		v := r[0]
		if v.Null {
			q.sawNull = true
			continue
		}
		if !has {
			q.min, q.max, has = v, v, true
			continue
		}
		if !sameEqClass(q.min.T, v.T) {
			q.ok = false
			continue
		}
		if c, ok := compareValues(v, q.min); ok && c < 0 {
			q.min = v
		}
		if c, ok := compareValues(v, q.max); ok && c > 0 {
			q.max = v
		}
	}
	q.ok = q.ok && has
	return q
}

// evalQuantified evaluates x op ANY / ALL over a subquery's rows.
func (env *evalEnv) evalQuantified(sq *Subquery, rows [][]Value) (Value, error) {
	all := sq.Kind == SubqueryAll
	if len(rows) == 0 {
		return boolValue(all), nil
	}
	x, err := env.eval(sq.X)
	if err != nil {
		return Value{}, err
	}
	if x.Null {
		return nullValue(typeBool), nil
	}
	if !sq.correlated && sq.Op != "=" && sq.Op != "<>" {
		e := env.exec
		q, ok := e.cache.extremes[sq]
		if !ok {
			q = newQuantExtremes(rows)
			e.cache.extremes[sq] = q
		}
		if q.ok && sameEqClass(x.T, q.min.T) {
			// x > ANY is x > MIN, x > ALL is x > MAX, and the other way
			// round for < and <=.
			bound := q.min
			if (sq.Op == ">" || sq.Op == ">=") == all {
				bound = q.max
			}
			v, err := binaryOp(sq.Op, x, bound, env.zone())
			if err != nil {
				return Value{}, err
			}
			if b, _ := truthy(v); b != all {
				return boolValue(b), nil
			}
			if q.sawNull {
				return nullValue(typeBool), nil
			}
			return boolValue(all), nil
		}
	}
	sawNull := false
	for _, r := range rows {
		v, err := binaryOp(sq.Op, x, r[0], env.zone())
		if err != nil {
			return Value{}, err
		}
		b, ok := truthy(v)
		switch {
		case !ok:
			sawNull = true
		case b != all:
			// A true comparison decides ANY, a false one ALL.
			return boolValue(b), nil
		}
	}
	if sawNull {
		return nullValue(typeBool), nil
	}
	return boolValue(all), nil
}
