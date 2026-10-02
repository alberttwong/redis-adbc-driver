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

// Intervals and date/time arithmetic.
//
// An interval is (months, days, nanoseconds), like Postgres and Arrow's
// month-day-nano interval type: months and days are calendar units whose
// length depends on the date they are added to. Adding an interval applies
// months (clamping to the end of the month: Jan 31 + 1 month = Feb 29 in a
// leap year), then days, then the time part.
//
// Comparisons and equality (ORDER BY, GROUP BY, DISTINCT, joins) treat a
// month as 30 days and a day as 24 hours, as Postgres does, so
// INTERVAL '1 day' = INTERVAL '24 hours'.

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
)

const (
	nsPerSecond = int64(time.Second)
	nsPerMinute = 60 * nsPerSecond
	nsPerHour   = 60 * nsPerMinute
	nsPerDay    = 24 * nsPerHour
)

// intervalUnit says how one unit of a field maps onto the interval parts.
type intervalUnit struct {
	months float64
	days   float64
	nanos  float64
}

var intervalUnits = map[string]intervalUnit{}

func init() {
	add := func(u intervalUnit, names ...string) {
		for _, n := range names {
			intervalUnits[n] = u
		}
	}
	add(intervalUnit{months: 12000}, "millennium", "millennia", "millenniums")
	add(intervalUnit{months: 1200}, "century", "centuries")
	add(intervalUnit{months: 120}, "decade", "decades")
	add(intervalUnit{months: 12}, "year", "years", "y", "yr", "yrs")
	add(intervalUnit{months: 3}, "quarter", "quarters")
	add(intervalUnit{months: 1}, "month", "months", "mon", "mons")
	add(intervalUnit{days: 7}, "week", "weeks", "w")
	add(intervalUnit{days: 1}, "day", "days", "d")
	add(intervalUnit{nanos: float64(nsPerHour)}, "hour", "hours", "h", "hr", "hrs")
	add(intervalUnit{nanos: float64(nsPerMinute)}, "minute", "minutes", "m", "min", "mins")
	add(intervalUnit{nanos: float64(nsPerSecond)}, "second", "seconds", "s", "sec", "secs")
	add(intervalUnit{nanos: 1e6}, "millisecond", "milliseconds", "ms", "msec", "msecs")
	add(intervalUnit{nanos: 1e3}, "microsecond", "microseconds", "us", "usec", "usecs")
}

func intervalValue(months, days int64, nanos int64) (Value, error) {
	if months > math.MaxInt32 || months < math.MinInt32 || days > math.MaxInt32 || days < math.MinInt32 {
		return Value{}, fmt.Errorf("interval out of range")
	}
	return Value{T: typeInterval, Months: int32(months), Days: int32(days), I: nanos}, nil
}

// accumulator for fractional interval parts, cascading fractions downward
// like Postgres: a fractional month adds 30 days per month, a fractional day
// adds 24 hours per day.
type intervalParts struct {
	months, days float64
	nanos        float64
}

func (p *intervalParts) add(n float64, u intervalUnit) {
	p.months += n * u.months
	p.days += n * u.days
	p.nanos += n * u.nanos
}

func (p intervalParts) value() (Value, error) {
	m := math.Trunc(p.months)
	d := p.days + (p.months-m)*30
	dd := math.Trunc(d)
	ns := math.Round(p.nanos + (d-dd)*float64(nsPerDay))
	for _, f := range []float64{m, dd, ns} {
		// Go's conversion of a float past int64 (or NaN) to an integer is
		// platform-dependent: it saturates or wraps.
		if !(f >= -(1<<63) && f < 1<<63) {
			return Value{}, fmt.Errorf("interval out of range")
		}
	}
	return intervalValue(int64(m), int64(dd), int64(ns))
}

// intervalLiteral parses Postgres interval input: '1 year 2 months 3 days',
// '1.5 hours', '-2 weeks', '04:05:06.5', '3 04:05:06', '1-2' (years-months),
// '2 days ago', and ISO 8601 'P1Y2M3DT4H5M6S'.
func intervalLiteral(s string) (Value, error) {
	in := strings.TrimSpace(s)
	if in == "" {
		return Value{}, fmt.Errorf("invalid interval %q", s)
	}
	if in[0] == 'P' || in[0] == 'p' {
		return isoInterval(in)
	}
	var p intervalParts
	fields := strings.Fields(strings.ToLower(in))
	ago := false
	if n := len(fields); n > 0 && fields[n-1] == "ago" {
		ago = true
		fields = fields[:n-1]
	}
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		switch {
		case strings.Contains(f, ":"):
			ns, err := parseClockInterval(f)
			if err != nil {
				return Value{}, fmt.Errorf("invalid interval %q", s)
			}
			p.nanos += ns
		case isYearMonth(f):
			neg := strings.HasPrefix(f, "-")
			ys, ms, _ := strings.Cut(strings.TrimPrefix(f, "-"), "-")
			y, _ := strconv.ParseFloat(ys, 64)
			m, _ := strconv.ParseFloat(ms, 64)
			if neg {
				y, m = -y, -m
			}
			p.months += y*12 + m
		default:
			num, unit := splitNumberUnit(f)
			n, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return Value{}, fmt.Errorf("invalid interval %q", s)
			}
			if unit == "" && i+1 < len(fields) {
				if _, ok := intervalUnits[fields[i+1]]; ok {
					unit = fields[i+1]
					i++
				}
			}
			if unit == "" {
				// A bare number before a clock is days ('3 04:05:06');
				// otherwise seconds.
				if i+1 < len(fields) && strings.Contains(fields[i+1], ":") {
					unit = "day"
				} else {
					unit = "second"
				}
			}
			u, ok := intervalUnits[unit]
			if !ok {
				return Value{}, fmt.Errorf("invalid interval unit %q in %q", unit, s)
			}
			p.add(n, u)
		}
	}
	if ago {
		p.months, p.days, p.nanos = -p.months, -p.days, -p.nanos
	}
	return p.value()
}

func isYearMonth(f string) bool {
	g := strings.TrimPrefix(f, "-")
	a, b, ok := strings.Cut(g, "-")
	if !ok || a == "" || b == "" {
		return false
	}
	for _, r := range a + b {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func splitNumberUnit(f string) (string, string) {
	i := 0
	for i < len(f) && (f[i] == '+' || f[i] == '-' || f[i] == '.' || (f[i] >= '0' && f[i] <= '9')) {
		i++
	}
	return f[:i], f[i:]
}

// parseClockInterval parses [-]h:mm[:ss[.fff]] into nanoseconds.
func parseClockInterval(f string) (float64, error) {
	neg := strings.HasPrefix(f, "-")
	parts := strings.Split(strings.TrimLeft(f, "+-"), ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("invalid time %q", f)
	}
	var total float64
	mult := []float64{float64(nsPerHour), float64(nsPerMinute), float64(nsPerSecond)}
	for i, part := range parts {
		v, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0, err
		}
		total += v * mult[i]
	}
	if neg {
		total = -total
	}
	return total, nil
}

func isoInterval(s string) (Value, error) {
	var p intervalParts
	inTime := false
	num := ""
	for _, r := range strings.ToUpper(s[1:]) {
		switch {
		case r == 'T':
			inTime = true
		case unicode.IsDigit(r) || r == '.' || r == '-':
			num += string(r)
		default:
			n, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return Value{}, fmt.Errorf("invalid interval %q", s)
			}
			num = ""
			var unit string
			switch {
			case r == 'Y':
				unit = "year"
			case r == 'M' && !inTime:
				unit = "month"
			case r == 'W':
				unit = "week"
			case r == 'D':
				unit = "day"
			case r == 'H':
				unit = "hour"
			case r == 'M':
				unit = "minute"
			case r == 'S':
				unit = "second"
			default:
				return Value{}, fmt.Errorf("invalid interval %q", s)
			}
			p.add(n, intervalUnits[unit])
		}
	}
	if num != "" {
		return Value{}, fmt.Errorf("invalid interval %q", s)
	}
	return p.value()
}

// formatInterval renders an interval like Postgres: "1 year 2 mons 3 days
// 04:05:06.5"; the zero interval is "00:00:00". As in Postgres, a positive
// field that follows a negative one has a "+": "-1 days +02:00:00",
// "-1 years +2 mons".
func formatInterval(v Value) string {
	var parts []string
	afterNegative := false
	unit := func(n int64, one, many string) {
		if n == 0 {
			return
		}
		name := many
		if n == 1 {
			name = one
		}
		sign := ""
		if afterNegative && n > 0 {
			sign = "+"
		}
		parts = append(parts, fmt.Sprintf("%s%d %s", sign, n, name))
		afterNegative = n < 0
	}
	years, months := int64(v.Months)/12, int64(v.Months)%12
	unit(years, "year", "years")
	unit(months, "mon", "mons")
	unit(int64(v.Days), "day", "days")
	if v.I != 0 || len(parts) == 0 {
		// The magnitude is a uint64 so that MinInt64's fits.
		ns := uint64(v.I)
		sign := ""
		if v.I < 0 {
			sign, ns = "-", -ns
		} else if afterNegative {
			sign = "+"
		}
		h, rem := ns/uint64(nsPerHour), ns%uint64(nsPerHour)
		m, rem := rem/uint64(nsPerMinute), rem%uint64(nsPerMinute)
		sec, frac := rem/uint64(nsPerSecond), rem%uint64(nsPerSecond)
		clock := fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, sec)
		if frac != 0 {
			clock += strings.TrimRight(fmt.Sprintf(".%09d", frac), "0")
		}
		parts = append(parts, clock)
	}
	return strings.Join(parts, " ")
}

// intervalTotal is the interval's length with a month as 30 days.
func intervalTotal(v Value) *big.Int {
	t := big.NewInt(int64(v.Months) * 30)
	t.Add(t, big.NewInt(int64(v.Days)))
	t.Mul(t, big.NewInt(nsPerDay))
	return t.Add(t, big.NewInt(v.I))
}

func encodeInterval(v Value) string {
	return fmt.Sprintf("%d:%d:%d", v.Months, v.Days, v.I)
}

func decodeInterval(s string) (Value, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return Value{}, fmt.Errorf("corrupt interval value %q", s)
	}
	var n [3]int64
	for i, p := range parts {
		x, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("corrupt interval value %q", s)
		}
		n[i] = x
	}
	return intervalValue(n[0], n[1], n[2])
}

// ---- arithmetic ----

func daysIn(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// addMonths adds months, clamping the day to the end of the target month.
func addMonths(t time.Time, n int) time.Time {
	y, m, d := t.Date()
	total := int(m) - 1 + n
	ny := y + int(floorDiv(int64(total), 12))
	nm := time.Month(total - int(floorDiv(int64(total), 12))*12 + 1)
	if last := daysIn(ny, nm); d > last {
		d = last
	}
	return time.Date(ny, nm, d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

// addInterval adds an interval to a time.
func addInterval(t time.Time, iv Value) time.Time {
	t = addMonths(t, int(iv.Months))
	t = t.AddDate(0, 0, int(iv.Days))
	return t.Add(time.Duration(iv.I))
}

// addOrSub is a + b, or a - b for op "-", and whether it overflowed int64.
func addOrSub(op string, a, b int64) (int64, bool) {
	if op == "-" {
		s := a - b
		return s, (s < a) != (b > 0)
	}
	s := a + b
	return s, (s > a) != (b > 0)
}

func negateInterval(v Value) (Value, error) {
	if v.Months == math.MinInt32 || v.Days == math.MinInt32 || v.I == math.MinInt64 {
		return Value{}, fmt.Errorf("interval out of range")
	}
	return intervalValue(-int64(v.Months), -int64(v.Days), -v.I)
}

func scaleInterval(v Value, f float64) (Value, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return Value{}, fmt.Errorf("interval out of range")
	}
	p := intervalParts{months: float64(v.Months) * f, days: float64(v.Days) * f, nanos: float64(v.I) * f}
	return p.value()
}

// timeOfDay returns nanoseconds since midnight of a TIME value.
func timeOfDay(v Value) int64 {
	return v.I * (nsPerSecond / unitsPerSecond[v.T.Unit])
}

// temporalType is the result type of a date/time arithmetic operator, or ok
// = false if the operands are not a date/time combination. As in Postgres,
// a time or timestamp result doesn't keep its operand's declared precision
// (withoutTypmod): TIMESTAMP(2) + INTERVAL is a TIMESTAMP(3).
func temporalType(op string, a, b ColType) (ColType, bool, error) {
	ak, bk := a.Kind, b.Kind
	isInt := func(k Kind) bool { return k.isInteger() }
	iv := KindInterval
	bad := func() (ColType, bool, error) {
		return ColType{}, true, fmt.Errorf("operator %s is not supported for %s and %s", op, a.SQLName(), b.SQLName())
	}
	involved := ak == iv || bk == iv || ak == KindDate || bk == KindDate || ak == KindTimestamp || bk == KindTimestamp ||
		ak == KindTime || bk == KindTime
	if !involved {
		return ColType{}, false, nil
	}
	switch op {
	case "+":
		switch {
		case ak == KindTimestamp && bk == iv:
			return a.withoutTypmod(), true, nil
		case ak == iv && bk == KindTimestamp:
			return b.withoutTypmod(), true, nil
		case (ak == KindDate && bk == iv) || (ak == iv && bk == KindDate):
			return typeTimestamp, true, nil
		case ak == KindDate && isInt(bk), isInt(ak) && bk == KindDate:
			return typeDate, true, nil
		case ak == KindTime && bk == iv:
			return a.withoutTypmod(), true, nil
		case ak == iv && bk == KindTime:
			return b.withoutTypmod(), true, nil
		case ak == iv && bk == iv:
			return typeInterval, true, nil
		case ak == KindDate && bk == KindTime:
			return typeTimestamp, true, nil
		}
	case "-":
		switch {
		case ak == KindTimestamp && bk == iv:
			return a.withoutTypmod(), true, nil
		case ak == KindDate && bk == iv:
			return typeTimestamp, true, nil
		case ak == KindDate && isInt(bk):
			return typeDate, true, nil
		case ak == KindDate && bk == KindDate:
			return typeInt64, true, nil
		case (ak == KindTimestamp || ak == KindDate) && (bk == KindTimestamp || bk == KindDate):
			return typeInterval, true, nil
		case ak == KindTime && bk == iv:
			return a.withoutTypmod(), true, nil
		case ak == KindTime && bk == KindTime:
			return typeInterval, true, nil
		case ak == iv && bk == iv:
			return typeInterval, true, nil
		}
	case "*":
		if (ak == iv && (bk.isNumeric() || bk == KindNull)) || ((ak.isNumeric() || ak == KindNull) && bk == iv) {
			return typeInterval, true, nil
		}
	case "/":
		if ak == iv && (bk.isNumeric() || bk == KindNull) {
			return typeInterval, true, nil
		}
	}
	if ak == KindNull || bk == KindNull {
		// NULL with a date/time operand: the type of the other side.
		if ak == KindNull {
			return b.withoutTypmod(), true, nil
		}
		return a.withoutTypmod(), true, nil
	}
	return bad()
}

// temporalOp evaluates date/time arithmetic on non-NULL operands, in the
// session time zone z: months and days are added to a timestamp with time
// zone on its local time (addIntervalIn), and a TIMESTAMP or DATE
// subtracted from or by one is a local time.
func temporalOp(op string, l, r Value, rt ColType, z tzZone) (Value, error) {
	lk, rk := l.T.Kind, r.T.Kind
	switch {
	case lk == KindInterval && rk == KindInterval:
		sign := int64(1)
		if op == "-" {
			sign = -1
		}
		ns, overflow := addOrSub(op, l.I, r.I)
		if overflow {
			return Value{}, fmt.Errorf("interval out of range")
		}
		return intervalValue(int64(l.Months)+sign*int64(r.Months), int64(l.Days)+sign*int64(r.Days), ns)
	case op == "*" && lk == KindInterval:
		f, _ := r.asFloat()
		return scaleInterval(l, f)
	case op == "*" && rk == KindInterval:
		f, _ := l.asFloat()
		return scaleInterval(r, f)
	case op == "/" && lk == KindInterval:
		f, _ := r.asFloat()
		if f == 0 {
			return Value{}, fmt.Errorf("division by zero")
		}
		return scaleInterval(l, 1/f)
	case lk == KindDate && rk.isInteger(), lk.isInteger() && rk == KindDate:
		// date ± days, days + date
		d, n := l.I, r.I
		if lk != KindDate {
			d, n = n, d
		}
		days, overflow := addOrSub(op, d, n)
		if overflow {
			return Value{}, fmt.Errorf("date out of range")
		}
		return dateValue(days)
	case lk == KindDate && rk == KindDate:
		// Dates are date32, so the difference fits.
		return intValue(typeInt64, l.I-r.I), nil
	case (lk == KindTime) && rk == KindTime:
		return intervalValue(0, 0, timeOfDay(l)-timeOfDay(r))
	case lk == KindTime && rk == KindInterval, lk == KindInterval && rk == KindTime:
		t, iv := l, r
		if lk == KindInterval {
			t, iv = r, l
		}
		sign := int64(1)
		if op == "-" {
			sign = -1
		}
		ns := timeOfDay(t) + sign*(iv.I%nsPerDay) // reduced first, so it can't overflow
		ns -= floorDiv(ns, nsPerDay) * nsPerDay   // wrap around midnight
		return intValue(rt, ns/(nsPerSecond/unitsPerSecond[rt.Unit])), nil
	case lk == KindDate && rk == KindTime:
		tm, err := toTime(l)
		if err != nil {
			return Value{}, err
		}
		return fromTime(tm.Add(time.Duration(timeOfDay(r))), rt)
	case rt.Kind == KindInterval:
		// timestamp/date - timestamp/date: days and time, no months. With
		// a timestamp with time zone, both are instants.
		if isTimestampTZ(l.T) != isTimestampTZ(r.T) {
			var err error
			if l, err = asInstant(l, z); err == nil {
				r, err = asInstant(r, z)
			}
			if err != nil {
				return Value{}, err
			}
		}
		a, err := toTime(l)
		if err != nil {
			return Value{}, err
		}
		b, err := toTime(r)
		if err != nil {
			return Value{}, err
		}
		diff := new(big.Int).Sub(big.NewInt(a.Unix()), big.NewInt(b.Unix()))
		diff.Mul(diff, big.NewInt(nsPerSecond))
		diff.Add(diff, big.NewInt(int64(a.Nanosecond()-b.Nanosecond())))
		days, rem := new(big.Int).QuoRem(diff, big.NewInt(nsPerDay), new(big.Int))
		return intervalValue(0, days.Int64(), rem.Int64())
	default:
		// timestamp/date ± interval
		t, iv := l, r
		if lk == KindInterval {
			t, iv = r, l
		}
		if op == "-" {
			// As in Postgres, an interval that can't be negated is out of range.
			var err error
			if iv, err = negateInterval(iv); err != nil {
				return Value{}, err
			}
		}
		tm, err := toTime(t)
		if err != nil {
			return Value{}, err
		}
		if isTimestampTZ(rt) {
			return fromTime(addIntervalIn(tm, iv, z), rt)
		}
		return fromTime(addInterval(tm, iv), rt)
	}
}

// isTimestampTZ reports whether t is TIMESTAMP WITH TIME ZONE.
func isTimestampTZ(t ColType) bool { return t.Kind == KindTimestamp && t.TZ != "" }

// asInstant converts a TIMESTAMP or DATE to a TIMESTAMP WITH TIME ZONE, as
// a local time in z, keeping a timestamp's unit.
func asInstant(v Value, z tzZone) (Value, error) {
	switch {
	case isTimestampTZ(v.T):
		return v, nil
	case v.T.Kind == KindTimestamp:
		return coerceIn(v, timestampType(v.T.Unit, "UTC"), z)
	}
	return coerceIn(v, timestampType(arrow.Second, "UTC"), z)
}

// age implements AGE(a, b): a - b in years, months, days and time, by
// subtracting field by field and borrowing (Postgres's algorithm).
func age(a, b time.Time) (Value, error) {
	if a.Before(b) {
		v, err := age(b, a)
		if err != nil {
			return Value{}, err
		}
		return negateInterval(v)
	}
	ns := a.Nanosecond() - b.Nanosecond()
	sec := a.Second() - b.Second()
	mi := a.Minute() - b.Minute()
	hr := a.Hour() - b.Hour()
	day := a.Day() - b.Day()
	mon := int(a.Month()) - int(b.Month())
	yr := a.Year() - b.Year()
	if ns < 0 {
		ns += 1_000_000_000
		sec--
	}
	if sec < 0 {
		sec += 60
		mi--
	}
	if mi < 0 {
		mi += 60
		hr--
	}
	if hr < 0 {
		hr += 24
		day--
	}
	for day < 0 {
		// Borrow the length of the earlier date's month.
		day += daysIn(b.Year(), b.Month())
		mon--
	}
	if mon < 0 {
		mon += 12
		yr--
	}
	nanos := ((int64(hr)*60+int64(mi))*60+int64(sec))*nsPerSecond + int64(ns)
	return intervalValue(int64(yr)*12+int64(mon), int64(day), nanos)
}

// intervalPart implements EXTRACT on intervals.
func intervalPart(field string, v Value) (Value, error) {
	i := func(n int64) (Value, error) { return intValue(typeInt64, n), nil }
	switch normalizeField(field) {
	case "millennium":
		return i(int64(v.Months) / 12000)
	case "century":
		return i(int64(v.Months) / 1200)
	case "decade":
		return i(int64(v.Months) / 120)
	case "year":
		return i(int64(v.Months) / 12)
	case "quarter":
		return i(int64(v.Months)%12/3 + 1)
	case "month":
		return i(int64(v.Months) % 12)
	case "day":
		return i(int64(v.Days))
	case "hour":
		return i(v.I / nsPerHour)
	case "minute":
		return i(v.I % nsPerHour / nsPerMinute)
	case "second":
		return floatValue(typeFloat64, float64(v.I%nsPerMinute)/1e9), nil
	case "milliseconds":
		return floatValue(typeFloat64, float64(v.I%nsPerMinute)/1e6), nil
	case "microseconds":
		return i(v.I % nsPerMinute / 1000)
	case "epoch":
		// Postgres: a year is 365.25 days, a remaining month 30 days.
		years := float64(v.Months / 12)
		months := float64(v.Months % 12)
		secs := years*365.25*86400 + months*30*86400 + float64(v.Days)*86400 + float64(v.I)/1e9
		return floatValue(typeFloat64, secs), nil
	}
	return Value{}, fmt.Errorf("field %q is not valid for INTERVAL values", field)
}
