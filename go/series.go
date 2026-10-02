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

// GENERATE_SERIES(start, stop [, step]) in FROM.
//
// The overloads are Postgres's: integers (step 1 by default), numerics, and
// timestamps with an interval step. The column has the arguments' common
// type: BIGINT for literals, NUMERIC if one is a decimal (DOUBLE PRECISION
// if one is a double, which Postgres has no overload for), and for an
// interval step TIMESTAMP if start or stop is one, otherwise TIMESTAMP WITH
// TIME ZONE (so two dates give timestamps with time zone, as in Postgres).
// As in Postgres, a NULL argument gives no rows, a step of zero is an error,
// a step in the wrong direction gives no rows, and a timestamp series adds
// the step to the previous value (so months clamp: Jan 31, Feb 29, Mar 29).
//
// The rows are held in memory like any relation, so a series may have at
// most maxSeriesRows of them; a larger one is an error before any row is
// made (for timestamps, once the limit is reached).

import (
	"context"
	"math"
	"math/big"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
)

// maxSeriesRows caps the rows of one GENERATE_SERIES call (a variable so
// that tests can lower it).
var maxSeriesRows = 1_000_000

// planTableFunc binds a table function's arguments in its scope sc and
// returns its relation (one column, named after the column alias or the
// table alias) and the column's type.
func (e *executor) planTableFunc(ctx context.Context, f *Func, alias string, cols []string, sc *scope) (*tableMeta, ColType, error) {
	d, ok := lookupFunc(f.Name)
	if !ok || d.kind != tableKind {
		return nil, ColType{}, errorf(adbc.StatusNotImplemented, "table function %s is not supported (only GENERATE_SERIES is)", strings.ToLower(f.Name))
	}
	if f.Star || f.Distinct {
		return nil, ColType{}, errorf(adbc.StatusInvalidArgument, "syntax error in the arguments of generate_series")
	}
	types := map[string]ColType{}
	for _, rel := range sc.rels {
		for _, c := range rel.meta.Columns {
			types[rel.prefix+c.Name] = c.Type
		}
	}
	argTypes := make([]ColType, len(f.Args))
	for i, a := range f.Args {
		if isAggregate(a) {
			return nil, ColType{}, errorf(adbc.StatusInvalidArgument, "aggregate functions are not allowed in functions in FROM")
		}
		if containsWindow(a) {
			return nil, ColType{}, errorf(adbc.StatusInvalidArgument, "window functions are not allowed in functions in FROM")
		}
		if err := e.bind(ctx, a); err != nil {
			return nil, ColType{}, err
		}
		// Columns of the earlier items, read directly (those read by a
		// subquery in the argument are recorded when it is bound).
		columnRefs(a, sc.needs)
		t, err := inferType(a, types, e.paramTypes)
		if err != nil {
			return nil, ColType{}, invalidArg(err)
		}
		argTypes[i] = t
	}
	// As in an expression, the arguments are bound before the call's
	// argument count is checked.
	if err := d.argsError(f); err != nil {
		return nil, ColType{}, err
	}
	t, err := seriesType(argTypes)
	if err != nil {
		return nil, ColType{}, err
	}
	name := alias
	switch {
	case len(cols) > 1:
		return nil, ColType{}, errorf(adbc.StatusInvalidArgument, "table %q has 1 columns available but %d columns specified", alias, len(cols))
	case len(cols) == 1:
		name = cols[0]
	}
	meta := &tableMeta{Name: alias, isMem: true, Columns: []columnMeta{{Name: name, Type: t, Nullable: true}}}
	return meta, t, nil
}

// seriesType resolves GENERATE_SERIES's overload for its argument types and
// returns the column type.
func seriesType(args []ColType) (ColType, error) {
	bad := func() (ColType, error) { return ColType{}, noSuchFunction("generate_series", false, args) }
	if len(args) < 2 || len(args) > 3 {
		return bad()
	}
	temporal := len(args) == 3 && args[2].Kind == KindInterval
	for _, a := range args[:2] {
		temporal = temporal || a.Kind == KindDate || a.Kind == KindTimestamp
	}
	if temporal {
		if len(args) != 3 {
			return bad()
		}
		if k := args[2].Kind; k != KindInterval && k != KindString && k != KindNull {
			return bad()
		}
		plain, zoned := false, false
		unit := arrow.Microsecond
		for _, a := range args[:2] {
			switch a.Kind {
			case KindTimestamp:
				if a.TZ != "" {
					zoned = true
				} else {
					plain = true
				}
				if unitsPerSecond[a.Unit] > unitsPerSecond[unit] {
					unit = a.Unit
				}
			case KindDate, KindString, KindNull:
			default:
				return bad()
			}
		}
		if plain && !zoned {
			return timestampType(unit, ""), nil
		}
		return timestampType(unit, "UTC"), nil
	}
	out := typeNull
	for _, a := range args {
		switch {
		case a.Kind.isNumeric():
			out = commonType(out, a)
		case a.Kind == KindString || a.Kind == KindNull:
		default:
			return bad()
		}
	}
	switch out.Kind {
	case KindNull:
		return typeInt64, nil
	case KindDecimal:
		return decimalType(38, out.Scale), nil
	case KindFloat32:
		return typeFloat64, nil
	}
	return out, nil
}

func errTooManySeriesRows() error {
	return errorf(adbc.StatusInvalidArgument, "generate_series would return more than %d rows", maxSeriesRows)
}

// generateSeries returns the values of GENERATE_SERIES(args) for column type
// t (from seriesType), in the session time zone z.
func generateSeries(t ColType, args []Value, z tzZone) ([]Value, error) {
	for _, a := range args {
		if a.Null {
			return nil, nil
		}
	}
	step := intValue(typeInt64, 1)
	if len(args) == 3 {
		step = args[2]
	}
	if t.Kind == KindTimestamp {
		return timestampSeries(t, args[0], args[1], step, z)
	}
	vals := make([]Value, 3)
	for i, a := range []Value{args[0], args[1], step} {
		v, err := Coerce(a, t)
		if err != nil {
			return nil, invalidArg(err)
		}
		vals[i] = v
	}
	start, stop, step := vals[0], vals[1], vals[2]
	switch {
	case t.Kind.isInteger():
		if step.I == 0 {
			return nil, errorf(adbc.StatusInvalidArgument, "step size cannot equal zero")
		}
		if (step.I > 0 && start.I > stop.I) || (step.I < 0 && start.I < stop.I) {
			return nil, nil
		}
		// (stop - start) / step + 1, without overflow.
		n := new(big.Int).Sub(big.NewInt(stop.I), big.NewInt(start.I))
		n.Quo(n, big.NewInt(step.I))
		if !n.IsInt64() || n.Int64() >= int64(maxSeriesRows) {
			return nil, errTooManySeriesRows()
		}
		out := make([]Value, n.Int64()+1)
		for i := range out {
			out[i] = intValue(t, start.I+int64(i)*step.I)
		}
		return out, nil
	case t.Kind == KindDecimal:
		if step.D.Sign() == 0 {
			return nil, errorf(adbc.StatusInvalidArgument, "step size cannot equal zero")
		}
		if c := start.D.Cmp(stop.D); (step.D.Sign() > 0 && c > 0) || (step.D.Sign() < 0 && c < 0) {
			return nil, nil
		}
		n := new(big.Int).Sub(stop.D, start.D)
		n.Quo(n, step.D)
		if !n.IsInt64() || n.Int64() >= int64(maxSeriesRows) {
			return nil, errTooManySeriesRows()
		}
		out := make([]Value, n.Int64()+1)
		cur := new(big.Int).Set(start.D)
		for i := range out {
			out[i] = decimalValue(new(big.Int).Set(cur), t.Precision, t.Scale)
			cur.Add(cur, step.D)
		}
		return out, nil
	}
	a, b, c := start.F, stop.F, step.F
	for _, x := range []struct {
		v    float64
		what string
	}{{a, "start value"}, {b, "stop value"}, {c, "step size"}} {
		switch {
		case math.IsNaN(x.v):
			return nil, errorf(adbc.StatusInvalidArgument, "%s cannot be NaN", x.what)
		case math.IsInf(x.v, 0):
			return nil, errorf(adbc.StatusInvalidArgument, "%s cannot be infinity", x.what)
		}
	}
	if c == 0 {
		return nil, errorf(adbc.StatusInvalidArgument, "step size cannot equal zero")
	}
	if (c > 0 && a > b) || (c < 0 && a < b) {
		return nil, nil
	}
	n := math.Floor((b-a)/c) + 1
	if n > float64(maxSeriesRows) {
		return nil, errTooManySeriesRows()
	}
	out := make([]Value, 0, int(n))
	for i := 0; i < int(n); i++ {
		v := a + float64(i)*c
		if (c > 0 && v > b) || (c < 0 && v < b) {
			break
		}
		out = append(out, floatValue(t, v))
	}
	return out, nil
}

// timestampSeries adds the interval step to start until it passes stop. For
// a timestamp with time zone, months and days are added in the session time
// zone z, as Postgres does (addIntervalIn), and text and dates are local
// times there.
func timestampSeries(t ColType, start, stop, step Value, z tzZone) ([]Value, error) {
	var err error
	if start, err = coerceIn(start, t, z); err != nil {
		return nil, invalidArg(err)
	}
	if stop, err = coerceIn(stop, t, z); err != nil {
		return nil, invalidArg(err)
	}
	if step, err = Coerce(step, typeInterval); err != nil {
		return nil, invalidArg(err)
	}
	dir := intervalTotal(step).Sign()
	if dir == 0 {
		return nil, errorf(adbc.StatusInvalidArgument, "step size cannot equal zero")
	}
	cur, err := toTime(start)
	if err != nil {
		return nil, invalidArg(err)
	}
	end, err := toTime(stop)
	if err != nil {
		return nil, invalidArg(err)
	}
	var out []Value
	for (dir > 0 && !cur.After(end)) || (dir < 0 && !cur.Before(end)) {
		if len(out) == maxSeriesRows {
			return nil, errTooManySeriesRows()
		}
		v, err := fromTime(cur, t)
		if err != nil {
			return nil, invalidArg(err)
		}
		out = append(out, v)
		if t.TZ != "" {
			cur = addIntervalIn(cur, step, z)
		} else {
			cur = addInterval(cur, step)
		}
	}
	return out, nil
}
