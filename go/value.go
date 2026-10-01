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
	"bytes"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Value is a single typed SQL value.
//
//   - Integers, booleans (0/1), dates (days since epoch), times (units since
//     midnight) and timestamps (units since epoch) live in I.
//   - Floating-point values live in F.
//   - Strings and binary values live in S.
//   - Decimals live in D as an unscaled integer at scale T.Scale.
type Value struct {
	T    ColType
	Null bool
	I    int64
	F    float64
	S    string
	D    *big.Int
	// Intervals: Months and Days, with nanoseconds in I.
	Months int32
	Days   int32
}

func nullValue(t ColType) Value { return Value{T: t, Null: true} }
func intValue(t ColType, v int64) Value {
	return Value{T: t, I: v}
}
func boolValue(b bool) Value {
	if b {
		return Value{T: typeBool, I: 1}
	}
	return Value{T: typeBool}
}
func floatValue(t ColType, f float64) Value { return Value{T: t, F: f} }
func stringValue(s string) Value            { return Value{T: typeString, S: s} }
func binaryValue(s string) Value            { return Value{T: typeBinary, S: s} }
func decimalValue(unscaled *big.Int, precision, scale int32) Value {
	return Value{T: decimalType(precision, scale), D: unscaled}
}

var bigTen = big.NewInt(10)

func pow10(n int32) *big.Int {
	return new(big.Int).Exp(bigTen, big.NewInt(int64(n)), nil)
}

// parseDecimal parses a decimal literal like "-123.45" or "1.5e3" into an
// unscaled integer and a scale.
func parseDecimal(s string) (*big.Int, int32, error) {
	s = strings.TrimSpace(s)
	exp := int64(0)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.ParseInt(s[i+1:], 10, 32)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid decimal %q", s)
		}
		exp = e
		s = s[:i]
	}
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	intPart, frac, _ := strings.Cut(s, ".")
	digits := intPart + frac
	if digits == "" {
		return nil, 0, fmt.Errorf("invalid decimal %q", s)
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return nil, 0, fmt.Errorf("invalid decimal %q", s)
		}
	}
	v, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return nil, 0, fmt.Errorf("invalid decimal %q", s)
	}
	if neg {
		v.Neg(v)
	}
	scale := int64(len(frac)) - exp
	return v, int32(scale), nil
}

// rescaleDecimal converts an unscaled value from one scale to another,
// rounding half away from zero when digits are dropped.
func rescaleDecimal(v *big.Int, from, to int32) *big.Int {
	if from == to {
		return new(big.Int).Set(v)
	}
	if to > from {
		return new(big.Int).Mul(v, pow10(to-from))
	}
	div := pow10(from - to)
	q, r := new(big.Int).QuoRem(v, div, new(big.Int))
	twice := new(big.Int).Abs(r)
	twice.Mul(twice, big.NewInt(2))
	if twice.Cmp(div) >= 0 {
		if v.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return q
}

// formatDecimal renders an unscaled value at the given scale.
func formatDecimal(v *big.Int, scale int32) string {
	if scale <= 0 {
		return new(big.Int).Mul(v, pow10(-scale)).String()
	}
	neg := v.Sign() < 0
	digits := new(big.Int).Abs(v).String()
	for int32(len(digits)) <= scale {
		digits = "0" + digits
	}
	cut := int32(len(digits)) - scale
	out := digits[:cut] + "." + digits[cut:]
	if neg {
		out = "-" + out
	}
	return out
}

func decimalDigits(v *big.Int) int32 {
	return int32(len(new(big.Int).Abs(v).String()))
}

func decimalToFloat(v *big.Int, scale int32) float64 {
	f, _ := strconv.ParseFloat(formatDecimal(v, scale), 64)
	return f
}

// ---- temporal helpers ----

var unitsPerSecond = map[arrow.TimeUnit]int64{
	arrow.Second:      1,
	arrow.Millisecond: 1_000,
	arrow.Microsecond: 1_000_000,
	arrow.Nanosecond:  1_000_000_000,
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// convertUnit converts a count of `from` units into `to` units (truncating
// toward negative infinity), reporting overflow.
func convertUnit(v int64, from, to arrow.TimeUnit) (int64, error) {
	f, t := unitsPerSecond[from], unitsPerSecond[to]
	if f == t {
		return v, nil
	}
	if t > f {
		mult := t / f
		r := v * mult
		if v != 0 && (r/mult != v) {
			return 0, fmt.Errorf("value %d out of range for unit %s", v, unitNames[to])
		}
		return r, nil
	}
	return floorDiv(v, f/t), nil
}

// secondsNanosToUnit combines seconds and nanoseconds into a count of units
// without intermediate overflow.
func secondsNanosToUnit(sec, nanos int64, unit arrow.TimeUnit) (int64, error) {
	per := unitsPerSecond[unit]
	total := new(big.Int).Mul(big.NewInt(sec), big.NewInt(per))
	total.Add(total, big.NewInt(floorDiv(nanos, 1_000_000_000/per)))
	if !total.IsInt64() {
		return 0, fmt.Errorf("timestamp out of range for unit %s", unitNames[unit])
	}
	return total.Int64(), nil
}

// parseFraction parses up to 9 fractional digits into nanoseconds and returns
// the number of digits.
func parseFraction(frac string) (int64, int, error) {
	if frac == "" {
		return 0, 0, nil
	}
	if len(frac) > 9 {
		frac = frac[:9]
	}
	n, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	digits := len(frac)
	for i := digits; i < 9; i++ {
		n *= 10
	}
	return n, digits, nil
}

func parseClock(s string) (sec int64, nanos int64, digits int, err error) {
	clock, frac, _ := strings.Cut(s, ".")
	parts := strings.Split(clock, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, 0, 0, fmt.Errorf("invalid time %q", s)
	}
	var hms [3]int64
	for i, p := range parts {
		v, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid time %q", s)
		}
		hms[i] = v
	}
	if hms[0] > 24 || hms[1] > 59 || hms[2] > 60 {
		return 0, 0, 0, fmt.Errorf("invalid time %q", s)
	}
	nanos, digits, err = parseFraction(frac)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid time %q", s)
	}
	return hms[0]*3600 + hms[1]*60 + hms[2], nanos, digits, nil
}

func parseDateDays(s string) (int64, error) {
	t, err := time.Parse("2006-01-02", strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("invalid date %q", s)
	}
	return floorDiv(t.Unix(), 86400), nil
}

// parseTimestamp parses "YYYY-MM-DD[ T]HH:MM:SS[.fffffffff][Z|±HH[:MM]]".
// It returns seconds and nanoseconds since the Unix epoch (UTC), the number of
// fractional digits, and whether an explicit offset was present.
func parseTimestamp(s string) (sec, nanos int64, digits int, hasTZ bool, err error) {
	s = strings.TrimSpace(s)
	if len(s) < 10 {
		return 0, 0, 0, false, fmt.Errorf("invalid timestamp %q", s)
	}
	days, err := parseDateDays(s[:10])
	if err != nil {
		return 0, 0, 0, false, fmt.Errorf("invalid timestamp %q", s)
	}
	rest := strings.TrimLeft(s[10:], " T")
	offset := int64(0)
	if rest != "" {
		clockEnd := len(rest)
		for i, c := range rest {
			if c == '+' || c == '-' || c == 'Z' || c == 'z' || c == ' ' {
				clockEnd = i
				break
			}
		}
		tz := strings.TrimSpace(rest[clockEnd:])
		rest = rest[:clockEnd]
		switch {
		case tz == "":
		case tz == "Z" || tz == "z" || strings.EqualFold(tz, "UTC"):
			hasTZ = true
		case tz[0] == '+' || tz[0] == '-':
			hasTZ = true
			sign := int64(1)
			if tz[0] == '-' {
				sign = -1
			}
			hh := strings.ReplaceAll(tz[1:], ":", "")
			mm := ""
			if len(hh) > 2 {
				mm = hh[2:]
				hh = hh[:2]
			}
			h, err1 := strconv.ParseInt(hh, 10, 64)
			m := int64(0)
			var err2 error
			if mm != "" {
				m, err2 = strconv.ParseInt(mm, 10, 64)
			}
			if err1 != nil || err2 != nil {
				return 0, 0, 0, false, fmt.Errorf("invalid timestamp %q", s)
			}
			offset = sign * (h*3600 + m*60)
		default:
			loc, lerr := time.LoadLocation(tz)
			if lerr != nil {
				return 0, 0, 0, false, fmt.Errorf("invalid timestamp %q", s)
			}
			hasTZ = true
			_ = loc
			// Resolve the zone offset at that local wall time.
			clockSec, _, _, cerr := parseClock(rest)
			if cerr != nil {
				return 0, 0, 0, false, cerr
			}
			wall := time.Unix(days*86400+clockSec, 0).UTC()
			local := time.Date(wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute(), wall.Second(), 0, loc)
			_, off := local.Zone()
			offset = int64(off)
		}
	}
	var clockSec int64
	if rest != "" {
		clockSec, nanos, digits, err = parseClock(rest)
		if err != nil {
			return 0, 0, 0, false, fmt.Errorf("invalid timestamp %q", s)
		}
	}
	sec = days*86400 + clockSec - offset
	return sec, nanos, digits, hasTZ, nil
}

func unitForDigits(digits int) arrow.TimeUnit {
	if digits > 6 {
		return arrow.Nanosecond
	}
	return arrow.Microsecond
}

// timestampLiteral builds a timestamp value from a SQL literal string.
func timestampLiteral(s string, withTZ bool) (Value, error) {
	sec, nanos, digits, _, err := parseTimestamp(s)
	if err != nil {
		return Value{}, err
	}
	unit := unitForDigits(digits)
	v, err := secondsNanosToUnit(sec, nanos, unit)
	if err != nil {
		return Value{}, err
	}
	tz := ""
	if withTZ {
		tz = "UTC"
	}
	return intValue(timestampType(unit, tz), v), nil
}

func timeLiteral(s string) (Value, error) {
	sec, nanos, digits, err := parseClock(strings.TrimSpace(s))
	if err != nil {
		return Value{}, err
	}
	unit := unitForDigits(digits)
	v, err := secondsNanosToUnit(sec, nanos, unit)
	if err != nil {
		return Value{}, err
	}
	return intValue(timeType(unit), v), nil
}

func dateLiteral(s string) (Value, error) {
	d, err := parseDateDays(s)
	if err != nil {
		return Value{}, err
	}
	return intValue(typeDate, d), nil
}

// numberLiteral types a numeric literal: integers become BIGINT, literals
// with a decimal point become NUMERIC, literals with an exponent DOUBLE.
func numberLiteral(text string) (Value, error) {
	if strings.ContainsAny(text, "eE") {
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return Value{}, fmt.Errorf("invalid number %q", text)
		}
		return floatValue(typeFloat64, f), nil
	}
	if !strings.Contains(text, ".") {
		if i, err := strconv.ParseInt(text, 10, 64); err == nil {
			return intValue(typeInt64, i), nil
		}
	}
	d, scale, err := parseDecimal(text)
	if err != nil {
		return Value{}, err
	}
	if scale < 0 {
		d = rescaleDecimal(d, scale, 0)
		scale = 0
	}
	prec := max(decimalDigits(d), scale+1, 1)
	return decimalValue(d, prec, scale), nil
}

// ---- formatting ----

func formatTimestamp(v int64, unit arrow.TimeUnit, tz string) string {
	per := unitsPerSecond[unit]
	sec := floorDiv(v, per)
	frac := v - sec*per
	t := time.Unix(sec, frac*(1_000_000_000/per)).UTC()
	layout := "2006-01-02 15:04:05"
	switch unit {
	case arrow.Millisecond:
		layout += ".000"
	case arrow.Microsecond:
		layout += ".000000"
	case arrow.Nanosecond:
		layout += ".000000000"
	}
	out := t.Format(layout)
	if tz != "" {
		out += "+00"
	}
	return out
}

func formatTime(v int64, unit arrow.TimeUnit) string {
	per := unitsPerSecond[unit]
	sec := floorDiv(v, per)
	frac := v - sec*per
	out := fmt.Sprintf("%02d:%02d:%02d", sec/3600, (sec%3600)/60, sec%60)
	if unit != arrow.Second {
		width := precisionForUnit(unit)
		out += fmt.Sprintf(".%0*d", width, frac)
	}
	return out
}

// Text renders a value the way CAST(x AS VARCHAR) would.
func (v Value) Text() string {
	switch v.T.Kind {
	case KindBool:
		if v.I != 0 {
			return "true"
		}
		return "false"
	case KindInt16, KindInt32, KindInt64:
		return strconv.FormatInt(v.I, 10)
	case KindFloat32:
		return strconv.FormatFloat(v.F, 'g', -1, 32)
	case KindFloat64:
		return strconv.FormatFloat(v.F, 'g', -1, 64)
	case KindDecimal:
		return formatDecimal(v.D, v.T.Scale)
	case KindString:
		if v.T.Fixed {
			return trimPadding(v.S) // CHAR to text drops the padding
		}
		return v.S
	case KindBinary:
		return v.S
	case KindDate:
		return time.Unix(v.I*86400, 0).UTC().Format("2006-01-02")
	case KindTime:
		return formatTime(v.I, v.T.Unit)
	case KindTimestamp:
		return formatTimestamp(v.I, v.T.Unit, v.T.TZ)
	case KindInterval:
		return formatInterval(v)
	}
	return ""
}

// ---- coercion ----

func intRange(k Kind) (int64, int64) {
	switch k {
	case KindInt16:
		return math.MinInt16, math.MaxInt16
	case KindInt32:
		return math.MinInt32, math.MaxInt32
	default:
		return math.MinInt64, math.MaxInt64
	}
}

func (v Value) asFloat() (float64, bool) {
	switch v.T.Kind {
	case KindInt16, KindInt32, KindInt64, KindBool:
		return float64(v.I), true
	case KindFloat32, KindFloat64:
		return v.F, true
	case KindDecimal:
		return decimalToFloat(v.D, v.T.Scale), true
	}
	return 0, false
}

// Coerce converts a value to the given type, as done on INSERT and CAST.
func Coerce(v Value, t ColType) (Value, error) {
	if v.Null || t.Kind == KindNull {
		return nullValue(t), nil
	}
	fail := func() (Value, error) {
		return Value{}, fmt.Errorf("cannot convert %s value %q to %s", v.T.Kind, v.Text(), t.SQLName())
	}
	// Parse strings into the target type first.
	if v.T.Kind == KindString && t.Kind != KindString && t.Kind != KindBinary {
		parsed, err := parseString(v.S, t)
		if err != nil {
			return Value{}, err
		}
		return Coerce(parsed, t)
	}
	switch t.Kind {
	case KindBool:
		switch {
		case v.T.Kind == KindBool:
			return v, nil
		case v.T.Kind.isInteger():
			return boolValue(v.I != 0), nil
		}
	case KindInt16, KindInt32, KindInt64:
		var i int64
		switch {
		case v.T.Kind.isInteger() || v.T.Kind == KindBool:
			i = v.I
		case v.T.Kind.isFloat():
			if math.IsNaN(v.F) || math.IsInf(v.F, 0) || v.F >= 9.223372036854775807e18 || v.F < -9.223372036854775808e18 {
				return fail()
			}
			i = int64(math.Round(v.F))
		case v.T.Kind == KindDecimal:
			r := rescaleDecimal(v.D, v.T.Scale, 0)
			if !r.IsInt64() {
				return fail()
			}
			i = r.Int64()
		default:
			return fail()
		}
		lo, hi := intRange(t.Kind)
		if i < lo || i > hi {
			return Value{}, fmt.Errorf("value %d out of range for %s", i, t.SQLName())
		}
		return intValue(t, i), nil
	case KindFloat32, KindFloat64:
		f, ok := v.asFloat()
		if !ok {
			return fail()
		}
		if t.Kind == KindFloat32 {
			f = float64(float32(f))
		}
		return floatValue(t, f), nil
	case KindDecimal:
		var d *big.Int
		var scale int32
		switch {
		case v.T.Kind.isInteger() || v.T.Kind == KindBool:
			d, scale = big.NewInt(v.I), 0
		case v.T.Kind == KindDecimal:
			d, scale = v.D, v.T.Scale
		case v.T.Kind.isFloat():
			if math.IsNaN(v.F) || math.IsInf(v.F, 0) {
				return fail()
			}
			var err error
			d, scale, err = parseDecimal(strconv.FormatFloat(v.F, 'f', -1, 64))
			if err != nil {
				return fail()
			}
		default:
			return fail()
		}
		r := rescaleDecimal(d, scale, t.Scale)
		if decimalDigits(r) > t.Precision && r.Sign() != 0 {
			return Value{}, fmt.Errorf("value %s out of range for %s", formatDecimal(d, scale), t.SQLName())
		}
		return decimalValue(r, t.Precision, t.Scale), nil
	case KindString:
		// The length isn't checked here: casts cut (castValue) and writes
		// check it (fitLength). CHAR values are padded, and lose their
		// padding as other strings.
		switch {
		case t.Fixed && v.T.isChar():
			return charValue(v.S, t), nil
		case t.Fixed:
			return charValue(v.Text(), t), nil
		case v.T.Kind == KindString && !v.T.Fixed:
			return v, nil
		}
		return stringValue(v.Text()), nil
	case KindBinary:
		if v.T.Kind == KindBinary || v.T.Kind == KindString {
			return binaryValue(v.S), nil
		}
	case KindDate:
		switch v.T.Kind {
		case KindDate:
			return v, nil
		case KindTimestamp:
			return dateValue(floorDiv(v.I, unitsPerSecond[v.T.Unit]*86400))
		}
	case KindTime:
		if v.T.Kind == KindTime {
			i, err := convertUnit(v.I, v.T.Unit, t.Unit)
			if err != nil {
				return Value{}, err
			}
			return intValue(t, i), nil
		}
		if v.T.Kind == KindTimestamp {
			per := unitsPerSecond[v.T.Unit] * 86400
			i, err := convertUnit(v.I-floorDiv(v.I, per)*per, v.T.Unit, t.Unit)
			if err != nil {
				return Value{}, err
			}
			return intValue(t, i), nil
		}
	case KindInterval:
		if v.T.Kind == KindInterval {
			return v, nil
		}
	case KindTimestamp:
		switch v.T.Kind {
		case KindTimestamp:
			i, err := convertUnit(v.I, v.T.Unit, t.Unit)
			if err != nil {
				return Value{}, err
			}
			return intValue(t, i), nil
		case KindDate:
			i, err := secondsNanosToUnit(v.I*86400, 0, t.Unit)
			if err != nil {
				return Value{}, err
			}
			return intValue(t, i), nil
		}
	}
	return fail()
}

// castable reports whether Coerce converts some values of type from to type
// to; for the other combinations (such as DATE to BOOLEAN) it always fails.
func castable(from, to ColType) bool {
	f := from.Kind
	switch {
	case f == KindNull || f == KindString || to.Kind == KindNull || to.Kind == KindString:
		return true
	case to.Kind == KindBool:
		return f == KindBool || f.isInteger()
	case to.Kind.isNumeric():
		return f.isNumeric() || f == KindBool
	case to.Kind == KindDate || to.Kind == KindTime:
		return f == to.Kind || f == KindTimestamp
	case to.Kind == KindTimestamp:
		return f == KindTimestamp || f == KindDate
	}
	return f == to.Kind // BINARY, INTERVAL
}

// parseString converts text into a value of the given type.
func parseString(s string, t ColType) (Value, error) {
	trimmed := strings.TrimSpace(s)
	switch t.Kind {
	case KindBool:
		switch strings.ToLower(trimmed) {
		case "true", "t", "yes", "y", "1", "on":
			return boolValue(true), nil
		case "false", "f", "no", "n", "0", "off":
			return boolValue(false), nil
		}
	case KindInt16, KindInt32, KindInt64:
		if i, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return intValue(typeInt64, i), nil
		}
		return numberLiteral(trimmed)
	case KindFloat32, KindFloat64:
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return floatValue(typeFloat64, f), nil
		}
	case KindDecimal:
		return numberLiteral(trimmed)
	case KindDate:
		if len(trimmed) > 10 {
			return timestampLiteral(trimmed, false)
		}
		return dateLiteral(trimmed)
	case KindTime:
		return timeLiteral(trimmed)
	case KindTimestamp:
		return timestampLiteral(trimmed, t.TZ != "")
	case KindInterval:
		return intervalLiteral(trimmed)
	}
	return Value{}, fmt.Errorf("cannot convert %q to %s", s, t.SQLName())
}

// ---- storage encoding ----

// encodeStored renders a value (already coerced to its column type) as the
// string stored in the row HASH.
func encodeStored(v Value) string {
	switch v.T.Kind {
	case KindBool, KindInt16, KindInt32, KindInt64, KindDate, KindTime, KindTimestamp:
		return strconv.FormatInt(v.I, 10)
	case KindFloat32:
		return strconv.FormatFloat(v.F, 'g', -1, 32)
	case KindFloat64:
		return strconv.FormatFloat(v.F, 'g', -1, 64)
	case KindDecimal:
		return formatDecimal(v.D, v.T.Scale)
	case KindInterval:
		return encodeInterval(v)
	default:
		return v.S
	}
}

// decodeStored parses a string loaded from a row HASH.
func decodeStored(s string, t ColType) (Value, error) {
	switch t.Kind {
	case KindBool, KindInt16, KindInt32, KindInt64, KindDate, KindTime, KindTimestamp:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			// Values computed by RediSearch (e.g. reducers) may be floats.
			f, ferr := strconv.ParseFloat(s, 64)
			if ferr != nil {
				return Value{}, fmt.Errorf("corrupt %s value %q", t.Kind, s)
			}
			i = int64(f)
		}
		return intValue(t, i), nil
	case KindFloat32:
		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return Value{}, fmt.Errorf("corrupt float value %q", s)
		}
		return floatValue(t, f), nil
	case KindFloat64:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return Value{}, fmt.Errorf("corrupt double value %q", s)
		}
		return floatValue(t, f), nil
	case KindDecimal:
		d, scale, err := parseDecimal(s)
		if err != nil {
			return Value{}, err
		}
		return decimalValue(rescaleDecimal(d, scale, t.Scale), t.Precision, t.Scale), nil
	case KindBinary:
		return binaryValue(s), nil
	case KindInterval:
		return decodeInterval(s)
	case KindString:
		if t.Fixed {
			return Value{T: t, S: s}, nil
		}
		return stringValue(s), nil
	default:
		return stringValue(s), nil
	}
}

// ---- comparison ----

// compareValues compares two non-null values; ok is false when the values
// are not comparable.
func compareValues(a, b Value) (int, bool) {
	ak, bk := a.T.Kind, b.T.Kind
	switch {
	case ak == KindInterval && bk == KindInterval:
		return intervalTotal(a).Cmp(intervalTotal(b)), true
	case (ak == KindString || ak == KindBinary) && (bk == KindString || bk == KindBinary):
		as, bs := a.S, b.S
		if a.T.isChar() || b.T.isChar() {
			// Trailing spaces don't count next to a CHAR value (lengths.go).
			if ak == KindString {
				as = trimPadding(as)
			}
			if bk == KindString {
				bs = trimPadding(bs)
			}
		}
		return bytes.Compare([]byte(as), []byte(bs)), true
	case ak == KindString && bk != KindString:
		pb, err := Coerce(a, b.T)
		if err != nil {
			return 0, false
		}
		return compareValues(pb, b)
	case bk == KindString && ak != KindString:
		pa, err := Coerce(b, a.T)
		if err != nil {
			return 0, false
		}
		return compareValues(a, pa)
	case (ak.isInteger() || ak == KindBool) && (bk.isInteger() || bk == KindBool):
		return cmpInt(a.I, b.I), true
	case ak == KindDecimal || bk == KindDecimal:
		if ak.isFloat() || bk.isFloat() {
			af, _ := a.asFloat()
			bf, _ := b.asFloat()
			return cmpFloat(af, bf), true
		}
		ad, as, ok1 := asDecimal(a)
		bd, bs, ok2 := asDecimal(b)
		if !ok1 || !ok2 {
			return 0, false
		}
		s := max(as, bs)
		return rescaleDecimal(ad, as, s).Cmp(rescaleDecimal(bd, bs, s)), true
	case ak.isNumeric() && bk.isNumeric():
		af, _ := a.asFloat()
		bf, _ := b.asFloat()
		return cmpFloat(af, bf), true
	case ak == bk && (ak == KindDate):
		return cmpInt(a.I, b.I), true
	case (ak == KindTime && bk == KindTime) || (ak == KindTimestamp && bk == KindTimestamp):
		u := a.T.Unit
		if unitsPerSecond[b.T.Unit] > unitsPerSecond[u] {
			u = b.T.Unit
		}
		ai, err1 := convertUnit(a.I, a.T.Unit, u)
		bi, err2 := convertUnit(b.I, b.T.Unit, u)
		if err1 != nil || err2 != nil {
			return 0, false
		}
		return cmpInt(ai, bi), true
	case ak == KindTimestamp && bk == KindDate:
		pb, err := Coerce(b, a.T)
		if err != nil {
			return 0, false
		}
		return cmpInt(a.I, pb.I), true
	case ak == KindDate && bk == KindTimestamp:
		pa, err := Coerce(a, b.T)
		if err != nil {
			return 0, false
		}
		return cmpInt(pa.I, b.I), true
	}
	return 0, false
}

func asDecimal(v Value) (*big.Int, int32, bool) {
	switch {
	case v.T.Kind == KindDecimal:
		return v.D, v.T.Scale, true
	case v.T.Kind.isInteger() || v.T.Kind == KindBool:
		return big.NewInt(v.I), 0, true
	}
	return nil, 0, false
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	case math.IsNaN(a) && !math.IsNaN(b):
		return 1
	case !math.IsNaN(a) && math.IsNaN(b):
		return -1
	}
	return 0
}
