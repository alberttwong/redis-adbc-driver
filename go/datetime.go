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

// Date/time functions. All computations are in UTC (timestamps are stored
// as UTC instants); the current time is fixed once per statement.

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
)

var (
	typeTimestampTZ = timestampType(arrow.Microsecond, "UTC")
	typeTimestamp   = timestampType(arrow.Microsecond, "")
	typeTimeUS      = timeType(arrow.Microsecond)
)

// now returns the statement's fixed current time.
func (env *evalEnv) now() time.Time {
	if env.exec != nil {
		if env.exec.now.IsZero() {
			env.exec.now = time.Now().UTC()
		}
		return env.exec.now
	}
	return time.Now().UTC()
}

// toTime converts a date, time or timestamp value to a UTC time.Time (times
// of day are placed on 1970-01-01). Strings are parsed as timestamps.
func toTime(v Value) (time.Time, error) {
	switch v.T.Kind {
	case KindDate:
		return time.Unix(v.I*86400, 0).UTC(), nil
	case KindTimestamp, KindTime:
		per := unitsPerSecond[v.T.Unit]
		sec := floorDiv(v.I, per)
		return time.Unix(sec, (v.I-sec*per)*(1_000_000_000/per)).UTC(), nil
	case KindString:
		ts, err := Coerce(v, timestampType(arrow.Nanosecond, ""))
		if err != nil {
			ts, err = Coerce(v, typeTimestamp)
		}
		if err != nil {
			return time.Time{}, err
		}
		return toTime(ts)
	}
	return time.Time{}, fmt.Errorf("expected a date, time or timestamp, got %s", v.T.Kind)
}

// dateValue is the DATE days after the epoch. A DATE is an Arrow date32, so
// beyond the int32 range of days it is "date out of range", as in Postgres.
func dateValue(days int64) (Value, error) {
	if days < math.MinInt32 || days > math.MaxInt32 {
		return Value{}, fmt.Errorf("date out of range")
	}
	return intValue(typeDate, days), nil
}

// fromTime converts a UTC time.Time to a value of type t.
func fromTime(tm time.Time, t ColType) (Value, error) {
	switch t.Kind {
	case KindDate:
		return dateValue(floorDiv(tm.Unix(), 86400))
	case KindTime:
		day := tm.Unix() - floorDiv(tm.Unix(), 86400)*86400
		i, err := secondsNanosToUnit(day, int64(tm.Nanosecond()), t.Unit)
		return intValue(t, i), err
	default:
		i, err := secondsNanosToUnit(tm.Unix(), int64(tm.Nanosecond()), t.Unit)
		return intValue(t, i), err
	}
}

func epochSeconds(v Value, tm time.Time) float64 {
	if v.T.Kind == KindTime {
		return float64(tm.Hour()*3600+tm.Minute()*60+tm.Second()) + float64(tm.Nanosecond())/1e9
	}
	return float64(tm.Unix()) + float64(tm.Nanosecond())/1e9
}

// datePartType is the result type of DATE_PART / EXTRACT for a field.
func datePartType(field string) ColType {
	switch normalizeField(field) {
	case "second", "milliseconds", "epoch":
		return typeFloat64
	}
	return typeInt64
}

func normalizeField(f string) string {
	f = strings.ToLower(strings.TrimSpace(f))
	switch f {
	case "years", "y", "yr", "yrs":
		return "year"
	case "months", "mon", "mons":
		return "month"
	case "days", "d":
		return "day"
	case "hours", "h", "hr", "hrs":
		return "hour"
	case "minutes", "m", "min", "mins":
		return "minute"
	case "seconds", "s", "sec", "secs":
		return "second"
	case "millisecond", "ms", "msec", "msecs":
		return "milliseconds"
	case "microsecond", "us", "usec", "usecs":
		return "microseconds"
	case "weeks", "w":
		return "week"
	case "quarters":
		return "quarter"
	case "dayofweek", "weekday":
		return "dow"
	case "dayofyear":
		return "doy"
	case "decades":
		return "decade"
	case "centuries":
		return "century"
	case "millennia", "millenniums":
		return "millennium"
	}
	return f
}

// datePart implements DATE_PART / EXTRACT.
func datePart(field string, v Value) (Value, error) {
	tm, err := toTime(v)
	if err != nil {
		return Value{}, err
	}
	f := normalizeField(field)
	if v.T.Kind == KindTime {
		switch f {
		case "hour", "minute", "second", "milliseconds", "microseconds", "epoch":
		default:
			return Value{}, fmt.Errorf("field %q is not valid for TIME values", field)
		}
	}
	i := func(n int) (Value, error) { return intValue(typeInt64, int64(n)), nil }
	frac := float64(tm.Nanosecond()) / 1e9
	switch f {
	case "year":
		return i(tm.Year())
	case "isoyear":
		y, _ := tm.ISOWeek()
		return i(y)
	case "quarter":
		return i((int(tm.Month())-1)/3 + 1)
	case "month":
		return i(int(tm.Month()))
	case "week":
		_, w := tm.ISOWeek()
		return i(w)
	case "day":
		return i(tm.Day())
	case "dow":
		return i(int(tm.Weekday()))
	case "isodow":
		d := int(tm.Weekday())
		if d == 0 {
			d = 7
		}
		return i(d)
	case "doy":
		return i(tm.YearDay())
	case "hour":
		return i(tm.Hour())
	case "minute":
		return i(tm.Minute())
	case "second":
		return floatValue(typeFloat64, float64(tm.Second())+frac), nil
	case "milliseconds":
		// Whole and fractional parts separately, avoiding float error.
		return floatValue(typeFloat64, float64(tm.Second()*1000)+float64(tm.Nanosecond())/1e6), nil
	case "microseconds":
		return i(tm.Second()*1_000_000 + tm.Nanosecond()/1000)
	case "epoch":
		return floatValue(typeFloat64, epochSeconds(v, tm)), nil
	case "decade":
		return i(int(math.Floor(float64(tm.Year()) / 10)))
	case "century":
		y := tm.Year()
		if y > 0 {
			return i((y-1)/100 + 1)
		}
		return i(-((-y)/100 + 1))
	case "millennium":
		y := tm.Year()
		if y > 0 {
			return i((y-1)/1000 + 1)
		}
		return i(-((-y)/1000 + 1))
	}
	return Value{}, fmt.Errorf("unknown date/time field %q", field)
}

// truncTime truncates a time to the start of the given unit.
func truncTime(unit string, tm time.Time) (time.Time, error) {
	y, mo, d := tm.Date()
	switch normalizeField(unit) {
	case "microseconds":
		return tm.Truncate(time.Microsecond), nil
	case "milliseconds":
		return tm.Truncate(time.Millisecond), nil
	case "second":
		return time.Date(y, mo, d, tm.Hour(), tm.Minute(), tm.Second(), 0, time.UTC), nil
	case "minute":
		return time.Date(y, mo, d, tm.Hour(), tm.Minute(), 0, 0, time.UTC), nil
	case "hour":
		return time.Date(y, mo, d, tm.Hour(), 0, 0, 0, time.UTC), nil
	case "day":
		return time.Date(y, mo, d, 0, 0, 0, 0, time.UTC), nil
	case "week":
		// ISO weeks start on Monday.
		wd := (int(tm.Weekday()) + 6) % 7
		return time.Date(y, mo, d-wd, 0, 0, 0, 0, time.UTC), nil
	case "month":
		return time.Date(y, mo, 1, 0, 0, 0, 0, time.UTC), nil
	case "quarter":
		return time.Date(y, time.Month((int(mo)-1)/3*3+1), 1, 0, 0, 0, 0, time.UTC), nil
	case "year":
		return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC), nil
	case "decade":
		return time.Date(int(math.Floor(float64(y)/10))*10, 1, 1, 0, 0, 0, 0, time.UTC), nil
	case "century":
		c := (y-1)/100*100 + 1
		return time.Date(c, 1, 1, 0, 0, 0, 0, time.UTC), nil
	case "millennium":
		m := (y-1)/1000*1000 + 1
		return time.Date(m, 1, 1, 0, 0, 0, 0, time.UTC), nil
	}
	return time.Time{}, fmt.Errorf("unknown DATE_TRUNC unit %q", unit)
}

// dateTruncType: dates stay dates; strings become timestamps.
func dateTruncType(arg ColType) ColType {
	switch arg.Kind {
	case KindDate, KindTimestamp:
		return arg
	}
	return typeTimestamp
}

// dateDiff counts the unit boundaries crossed from a to b (b - a).
func dateDiff(unit string, a, b time.Time) (int64, error) {
	months := func(t time.Time) int64 { return int64(t.Year())*12 + int64(t.Month()) - 1 }
	switch u := normalizeField(unit); u {
	case "year":
		return int64(b.Year() - a.Year()), nil
	case "quarter":
		return floorDiv(months(b), 3) - floorDiv(months(a), 3), nil
	case "month":
		return months(b) - months(a), nil
	case "decade", "century", "millennium":
		ta, err := truncTime(u, a)
		if err != nil {
			return 0, err
		}
		tb, _ := truncTime(u, b)
		span := map[string]int{"decade": 10, "century": 100, "millennium": 1000}[u]
		return int64((tb.Year() - ta.Year()) / span), nil
	case "week":
		ta, _ := truncTime("week", a)
		tb, _ := truncTime("week", b)
		return floorDiv(tb.Unix()-ta.Unix(), 7*86400), nil
	default:
		ta, err := truncTime(u, a)
		if err != nil {
			return 0, err
		}
		tb, _ := truncTime(u, b)
		per := map[string]int64{"day": 86400e9, "hour": 3600e9, "minute": 60e9, "second": 1e9,
			"milliseconds": 1e6, "microseconds": 1e3}[u]
		d := tb.Sub(ta)
		if d == math.MaxInt64 || d == math.MinInt64 {
			// Out of time.Duration range: fall back to seconds.
			return floorDiv(tb.Unix()-ta.Unix(), per/1e9), nil
		}
		return floorDiv(int64(d), per), nil
	}
}

// datePartNames maps the date parts of DATEADD and DATEDIFF, and their
// abbreviations in Snowflake and SQL Server, to the part. As in Snowflake, m
// is minute and w is week (SQL Server reads them as month and weekday).
var datePartNames = func() map[string]string {
	m := map[string]string{}
	for part, aliases := range map[string][]string{
		"year":        {"years", "y", "yy", "yyy", "yyyy", "yr", "yrs"},
		"quarter":     {"quarters", "q", "qq", "qtr", "qtrs"},
		"month":       {"months", "mm", "mon", "mons"},
		"week":        {"weeks", "w", "wk", "ww", "wy", "woy", "weekofyear"},
		"day":         {"days", "d", "dd", "dayofmonth"},
		"hour":        {"hours", "h", "hh", "hr", "hrs"},
		"minute":      {"minutes", "m", "mi", "n", "min", "mins"},
		"second":      {"seconds", "s", "ss", "sec", "secs"},
		"millisecond": {"milliseconds", "ms", "msec", "msecs"},
		"microsecond": {"microseconds", "us", "usec", "usecs", "mcs"},
		"decade":      {"decades"},
		"century":     {"centuries"},
		"millennium":  {"millennia", "millenniums"},
	} {
		m[part] = part
		for _, a := range aliases {
			m[a] = part
		}
	}
	return m
}()

// datePartOf returns the date part that s names in DATEADD or DATEDIFF.
func datePartOf(s string) (string, bool) {
	p, ok := datePartNames[strings.ToLower(strings.TrimSpace(s))]
	return p, ok
}

// dateAddType is the result type of DATEADD(part, n, x). As in Snowflake, a
// date stays a date when the part is a day or longer and becomes a timestamp
// otherwise; text is read as a timestamp.
func dateAddType(part Expr, x ColType) ColType {
	switch x.Kind {
	case KindTimestamp, KindTime:
		return x
	case KindDate:
		if lit, ok := part.(*Literal); ok && !lit.V.Null {
			if p, ok := datePartOf(lit.V.Text()); ok && intervalUnits[p].nanos == 0 {
				return typeDate
			}
		}
	}
	return typeTimestamp
}

// dateAdd implements DATEADD(part, n, x), and DATE_SUB with sign -1: x plus
// n parts, as a value of type t. Months clamp to the end of the month, as
// with intervals.
func dateAdd(name string, args []Value, sign int64, t ColType) (Value, error) {
	part, ok := datePartOf(args[0].Text())
	if !ok {
		return Value{}, fmt.Errorf("%s: unknown date part %q", name, args[0].Text())
	}
	n, err := intArg(name, "number of units", args[1])
	if err != nil {
		return Value{}, err
	}
	if sign < 0 {
		if n == math.MinInt64 {
			return Value{}, fmt.Errorf("%s: interval out of range", name)
		}
		n = -n
	}
	u := intervalUnits[part]
	x := args[2]
	switch x.T.Kind {
	case KindDate, KindTimestamp:
	case KindTime:
		if u.nanos == 0 {
			return Value{}, fmt.Errorf("%s: date part %s is not valid for TIME values", name, part)
		}
	case KindString:
		if x, err = Coerce(x, t); err != nil {
			return Value{}, fmt.Errorf("%s: %v", name, err)
		}
	default:
		return Value{}, fmt.Errorf("%s expects a date, time or timestamp, got %s", name, x.T.SQLName())
	}
	// The interval of n parts. A part shorter than a day carries whole days
	// into the days field, so that large counts don't overflow nanoseconds.
	var iv Value
	if u.nanos == 0 {
		if n < math.MinInt32 || n > math.MaxInt32 {
			return Value{}, fmt.Errorf("%s: interval out of range", name)
		}
		iv, err = intervalValue(n*int64(u.months), n*int64(u.days), 0)
	} else {
		ns := int64(u.nanos)
		iv, err = intervalValue(0, n/(nsPerDay/ns), n%(nsPerDay/ns)*ns)
	}
	if err == nil {
		x, err = temporalOp("+", x, iv, t)
	}
	if err != nil {
		return Value{}, fmt.Errorf("%s: %v", name, err)
	}
	return x, nil
}

// ---- TO_CHAR / TO_DATE / TO_TIMESTAMP formats ----

type fmtToken struct {
	pat  string // pattern (empty for literal text)
	text string // literal text
	fm   bool   // FM: no padding
}

// Patterns, longest first so prefixes don't shadow them.
var fmtPatterns = []string{
	"HH24", "HH12", "IYYY", "YYYY", "MONTH", "Month", "month", "DDD", "DAY", "Day", "day",
	"A.M.", "P.M.", "a.m.", "p.m.",
	"MON", "Mon", "mon", "YYY", "HH", "MI", "MM", "MS", "US", "SS", "DD", "DY", "Dy", "dy",
	"IW", "ID", "TZ", "tz", "AM", "PM", "am", "pm", "YY", "Q", "D", "Y",
}

func tokenizeFormat(f string) []fmtToken {
	var out []fmtToken
	fm := false
	for i := 0; i < len(f); {
		if strings.HasPrefix(strings.ToUpper(f[i:]), "FM") {
			fm = true
			i += 2
			continue
		}
		if f[i] == '"' {
			end := strings.IndexByte(f[i+1:], '"')
			if end < 0 {
				out = append(out, fmtToken{text: f[i+1:]})
				break
			}
			out = append(out, fmtToken{text: f[i+1 : i+1+end]})
			i += end + 2
			continue
		}
		matched := false
		for _, p := range fmtPatterns {
			// Name patterns are case-sensitive (they select the output
			// case); numeric ones are not.
			if strings.HasPrefix(f[i:], p) || (isNumericPattern(p) && strings.HasPrefix(strings.ToUpper(f[i:]), p)) {
				out = append(out, fmtToken{pat: p, fm: fm})
				fm = false
				i += len(p)
				matched = true
				break
			}
		}
		if !matched {
			out = append(out, fmtToken{text: f[i : i+1]})
			i++
		}
	}
	return out
}

func isNumericPattern(p string) bool {
	switch p {
	case "HH24", "HH12", "IYYY", "YYYY", "DDD", "YYY", "HH", "MI", "MM", "MS", "US", "SS", "DD", "IW", "ID", "YY", "Q", "D", "Y":
		return true
	}
	return false
}

func padName(s string, fm bool) string {
	if fm {
		return s
	}
	return fmt.Sprintf("%-9s", s)
}

// toChar formats a date/time value like Postgres TO_CHAR.
func toChar(v Value, format string) (string, error) {
	tm, err := toTime(v)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	num := func(n, width int, fm bool) {
		if fm {
			b.WriteString(strconv.Itoa(n))
		} else {
			fmt.Fprintf(&b, "%0*d", width, n)
		}
	}
	h12 := tm.Hour() % 12
	if h12 == 0 {
		h12 = 12
	}
	isoY, isoW := tm.ISOWeek()
	for _, t := range tokenizeFormat(format) {
		if t.pat == "" {
			b.WriteString(t.text)
			continue
		}
		month, day := tm.Month().String(), tm.Weekday().String()
		switch t.pat {
		case "YYYY":
			num(tm.Year(), 4, t.fm)
		case "YYY":
			num(tm.Year()%1000, 3, t.fm)
		case "YY":
			num(tm.Year()%100, 2, t.fm)
		case "Y":
			num(tm.Year()%10, 1, t.fm)
		case "IYYY":
			num(isoY, 4, t.fm)
		case "MM":
			num(int(tm.Month()), 2, t.fm)
		case "MONTH":
			b.WriteString(padName(strings.ToUpper(month), t.fm))
		case "Month":
			b.WriteString(padName(month, t.fm))
		case "month":
			b.WriteString(padName(strings.ToLower(month), t.fm))
		case "MON":
			b.WriteString(strings.ToUpper(month[:3]))
		case "Mon":
			b.WriteString(month[:3])
		case "mon":
			b.WriteString(strings.ToLower(month[:3]))
		case "DD":
			num(tm.Day(), 2, t.fm)
		case "DDD":
			num(tm.YearDay(), 3, t.fm)
		case "D":
			num(int(tm.Weekday())+1, 1, t.fm)
		case "ID":
			d := int(tm.Weekday())
			if d == 0 {
				d = 7
			}
			num(d, 1, t.fm)
		case "DAY":
			b.WriteString(padName(strings.ToUpper(day), t.fm))
		case "Day":
			b.WriteString(padName(day, t.fm))
		case "day":
			b.WriteString(padName(strings.ToLower(day), t.fm))
		case "DY":
			b.WriteString(strings.ToUpper(day[:3]))
		case "Dy":
			b.WriteString(day[:3])
		case "dy":
			b.WriteString(strings.ToLower(day[:3]))
		case "IW":
			num(isoW, 2, t.fm)
		case "Q":
			num((int(tm.Month())-1)/3+1, 1, t.fm)
		case "HH24":
			num(tm.Hour(), 2, t.fm)
		case "HH12", "HH":
			num(h12, 2, t.fm)
		case "MI":
			num(tm.Minute(), 2, t.fm)
		case "SS":
			num(tm.Second(), 2, t.fm)
		case "MS":
			num(tm.Nanosecond()/1_000_000, 3, t.fm)
		case "US":
			num(tm.Nanosecond()/1_000, 6, t.fm)
		case "AM", "PM":
			b.WriteString(map[bool]string{true: "PM", false: "AM"}[tm.Hour() >= 12])
		case "am", "pm":
			b.WriteString(map[bool]string{true: "pm", false: "am"}[tm.Hour() >= 12])
		case "A.M.", "P.M.":
			b.WriteString(map[bool]string{true: "P.M.", false: "A.M."}[tm.Hour() >= 12])
		case "a.m.", "p.m.":
			b.WriteString(map[bool]string{true: "p.m.", false: "a.m."}[tm.Hour() >= 12])
		case "TZ":
			b.WriteString("UTC")
		case "tz":
			b.WriteString("utc")
		}
	}
	return b.String(), nil
}

var monthNames = map[string]time.Month{}

func init() {
	for m := time.January; m <= time.December; m++ {
		monthNames[strings.ToLower(m.String())] = m
		monthNames[strings.ToLower(m.String()[:3])] = m
	}
}

// parseWithFormat parses text with a TO_CHAR-style format (the subset with
// fixed meaning: years, months, days, hours, minutes, seconds, fractions,
// AM/PM and month names). Whitespace in the input is flexible.
func parseWithFormat(s, format string) (time.Time, error) {
	year, month, day := 1970, 1, 1
	hour, minute, sec, nanos := 0, 0, 0, 0
	pm, has12 := false, false
	i := 0
	skipSpace := func() {
		for i < len(s) && unicode.IsSpace(rune(s[i])) {
			i++
		}
	}
	readNum := func(maxDigits int) (int, error) {
		skipSpace()
		start := i
		neg := false
		if i < len(s) && s[i] == '-' && maxDigits >= 4 {
			neg = true
			i++
			start = i
		}
		for i < len(s) && i-start < maxDigits && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == start {
			return 0, fmt.Errorf("expected digits at position %d of %q", start+1, s)
		}
		n, _ := strconv.Atoi(s[start:i])
		if neg {
			n = -n
		}
		return n, nil
	}
	var err error
	for _, t := range tokenizeFormat(format) {
		if t.pat == "" {
			for _, r := range t.text {
				if unicode.IsSpace(r) {
					skipSpace()
					continue
				}
				skipSpace()
				if i < len(s) && rune(s[i]) == r {
					i++
				} else if i < len(s) && !unicode.IsLetter(r) && !unicode.IsDigit(r) &&
					!unicode.IsLetter(rune(s[i])) && !unicode.IsDigit(rune(s[i])) {
					i++ // any separator matches any separator
				} else {
					return time.Time{}, fmt.Errorf("%q does not match format %q", s, format)
				}
			}
			continue
		}
		switch strings.ToUpper(t.pat) {
		case "YYYY", "IYYY":
			year, err = readNum(4)
		case "YY":
			var y int
			if y, err = readNum(2); err == nil {
				year = 2000 + y
				if y >= 70 {
					year = 1900 + y
				}
			}
		case "MM":
			month, err = readNum(2)
		case "DD":
			day, err = readNum(2)
		case "HH24":
			hour, err = readNum(2)
		case "HH12", "HH":
			hour, err = readNum(2)
			has12 = true
		case "MI":
			minute, err = readNum(2)
		case "SS":
			sec, err = readNum(2)
		case "MS":
			var ms int
			if ms, err = readNum(3); err == nil {
				nanos = ms * 1_000_000
			}
		case "US":
			var us int
			if us, err = readNum(6); err == nil {
				nanos = us * 1_000
			}
		case "MONTH", "MON":
			skipSpace()
			j := i
			for j < len(s) && unicode.IsLetter(rune(s[j])) {
				j++
			}
			m, ok := monthNames[strings.ToLower(s[i:j])]
			if !ok {
				return time.Time{}, fmt.Errorf("invalid month name in %q", s)
			}
			month, i = int(m), j
		case "AM", "PM", "A.M.", "P.M.":
			skipSpace()
			rest := strings.ToUpper(s[i:])
			switch {
			case strings.HasPrefix(rest, "PM"), strings.HasPrefix(rest, "P.M."):
				pm = true
			case strings.HasPrefix(rest, "AM"), strings.HasPrefix(rest, "A.M."):
			default:
				return time.Time{}, fmt.Errorf("expected AM or PM in %q", s)
			}
			if strings.HasPrefix(rest, "P.M.") || strings.HasPrefix(rest, "A.M.") {
				i += 4
			} else {
				i += 2
			}
		default:
			return time.Time{}, fmt.Errorf("format pattern %q cannot be parsed", t.pat)
		}
		if err != nil {
			return time.Time{}, err
		}
	}
	skipSpace()
	if i != len(s) {
		return time.Time{}, fmt.Errorf("unexpected trailing text %q", s[i:])
	}
	if has12 {
		if hour < 1 || hour > 12 {
			return time.Time{}, fmt.Errorf("hour %d is not valid with a 12-hour format", hour)
		}
		hour %= 12
		if pm {
			hour += 12
		}
	}
	tm := time.Date(year, time.Month(month), day, hour, minute, sec, nanos, time.UTC)
	if tm.Month() != time.Month(month) || tm.Day() != day || hour > 23 || minute > 59 || sec > 59 {
		return time.Time{}, fmt.Errorf("date/time field value out of range in %q", s)
	}
	return tm, nil
}

// ---- dispatch ----

// dateTimeFuncType returns the result type of a date/time function.
func dateTimeFuncType(f *Func, args []ColType) (ColType, bool) {
	switch f.Name {
	case "CURRENT_DATE", "MAKE_DATE", "TO_DATE", "LAST_DAY":
		return typeDate, true
	case "CURRENT_TIMESTAMP", "NOW", "TRANSACTION_TIMESTAMP", "STATEMENT_TIMESTAMP", "TO_TIMESTAMP", "MAKE_TIMESTAMPTZ":
		return typeTimestampTZ, true
	case "LOCALTIMESTAMP", "MAKE_TIMESTAMP":
		return typeTimestamp, true
	case "CURRENT_TIME", "LOCALTIME", "MAKE_TIME":
		return typeTimeUS, true
	case "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "DAYOFMONTH", "DAYOFYEAR", "HOUR", "MINUTE", "SECOND",
		"DATE_DIFF", "DATEDIFF", "TIMESTAMPDIFF", "EPOCH_MS":
		return typeInt64, true
	case "DATEADD", "DATE_ADD", "DATE_SUB", "TIMESTAMPADD":
		if len(args) == 3 {
			return dateAddType(f.Args[0], args[2]), true
		}
		return typeTimestamp, true
	case "EPOCH":
		return typeFloat64, true
	case "TO_CHAR":
		return typeString, true
	case "__INTERVAL", "AGE":
		return typeInterval, true
	case "DATE_PART":
		if len(f.Args) > 0 {
			if lit, ok := f.Args[0].(*Literal); ok && !lit.V.Null {
				return datePartType(lit.V.Text()), true
			}
		}
		return typeFloat64, true
	case "DATE_TRUNC":
		if len(args) == 2 {
			return dateTruncType(args[1]), true
		}
		return typeTimestamp, true
	}
	return ColType{}, false
}

// evalDateTimeFunc evaluates a date/time function; ok is false if f is not
// one. Arguments are non-NULL (NULLs are handled by the caller), and there
// are as many as the registry allows (funcs.go).
func (env *evalEnv) evalDateTimeFunc(f *Func, args []Value) (Value, bool, error) {
	t, ok := dateTimeFuncType(f, argTypes(args))
	if !ok {
		return Value{}, false, nil
	}
	intArg := func(v Value) (int, error) {
		c, err := Coerce(v, typeInt64)
		if err != nil {
			return 0, err
		}
		return int(c.I), nil
	}
	done := func(v Value, err error) (Value, bool, error) { return v, true, err }
	switch f.Name {
	case "CURRENT_DATE", "CURRENT_TIMESTAMP", "NOW", "TRANSACTION_TIMESTAMP", "STATEMENT_TIMESTAMP",
		"LOCALTIMESTAMP", "CURRENT_TIME", "LOCALTIME":
		// An optional precision argument is accepted and ignored.
		return done(fromTime(env.now(), t))
	case "DATE_PART":
		if args[1].T.Kind == KindInterval {
			return done(intervalPart(args[0].Text(), args[1]))
		}
		return done(datePart(args[0].Text(), args[1]))
	case "__INTERVAL":
		// INTERVAL n UNIT / INTERVAL 'n' UNIT
		u, ok := intervalUnits[args[1].Text()]
		if !ok {
			return done(Value{}, fmt.Errorf("unknown interval unit %q", args[1].Text()))
		}
		n, err := Coerce(args[0], typeFloat64)
		if err != nil {
			return done(Value{}, fmt.Errorf("INTERVAL %s needs a number, got %q", strings.ToUpper(args[1].Text()), args[0].Text()))
		}
		var p intervalParts
		p.add(n.F, u)
		return done(p.value())
	case "AGE":
		var a, b time.Time
		var err error
		if len(args) == 1 {
			// AGE(x) is measured from the start of the current day.
			n := env.now()
			a = time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, time.UTC)
			b, err = toTime(args[0])
		} else {
			if a, err = toTime(args[0]); err == nil {
				b, err = toTime(args[1])
			}
		}
		if err != nil {
			return done(Value{}, err)
		}
		return done(age(a, b))
	case "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "DAYOFMONTH", "DAYOFYEAR", "HOUR", "MINUTE", "SECOND":
		field := map[string]string{"DAYOFMONTH": "day", "DAYOFYEAR": "doy"}[f.Name]
		if field == "" {
			field = strings.ToLower(f.Name)
		}
		v, err := datePart(field, args[0])
		if err == nil && v.T.Kind == KindFloat64 {
			v = intValue(typeInt64, int64(math.Floor(v.F))) // SECOND() is whole seconds
		}
		return done(v, err)
	case "EPOCH", "EPOCH_MS":
		tm, err := toTime(args[0])
		if err != nil {
			return done(Value{}, err)
		}
		if f.Name == "EPOCH_MS" {
			return done(intValue(typeInt64, tm.Unix()*1000+int64(tm.Nanosecond()/1_000_000)), nil)
		}
		return done(floatValue(typeFloat64, epochSeconds(args[0], tm)), nil)
	case "DATE_TRUNC":
		tm, err := toTime(args[1])
		if err != nil {
			return done(Value{}, err)
		}
		tr, err := truncTime(args[0].Text(), tm)
		if err != nil {
			return done(Value{}, err)
		}
		return done(fromTime(tr, t))
	case "DATE_DIFF", "DATEDIFF", "TIMESTAMPDIFF":
		a, err := toTime(args[1])
		if err != nil {
			return done(Value{}, err)
		}
		b, err := toTime(args[2])
		if err != nil {
			return done(Value{}, err)
		}
		unit := args[0].Text()
		if p, ok := datePartOf(unit); ok {
			unit = p
		}
		n, err := dateDiff(unit, a, b)
		return done(intValue(typeInt64, n), err)
	case "DATEADD", "DATE_ADD", "DATE_SUB", "TIMESTAMPADD":
		sign := int64(1)
		if f.Name == "DATE_SUB" {
			sign = -1
		}
		return done(dateAdd(f.Name, args, sign, t))
	case "LAST_DAY":
		tm, err := toTime(args[0])
		if err != nil {
			return done(Value{}, err)
		}
		return done(fromTime(time.Date(tm.Year(), tm.Month()+1, 0, 0, 0, 0, 0, time.UTC), typeDate))
	case "MAKE_DATE":
		var ymd [3]int
		for i := range ymd {
			n, err := intArg(args[i])
			if err != nil {
				return done(Value{}, err)
			}
			ymd[i] = n
		}
		ymdText := fmt.Sprintf("%d-%02d-%02d", ymd[0], ymd[1], ymd[2])
		// A year past int32 is far outside the date range, and could
		// overflow time.Date.
		if ymd[0] < math.MinInt32 || ymd[0] > math.MaxInt32 {
			return done(Value{}, fmt.Errorf("date out of range: %s", ymdText))
		}
		tm := time.Date(ymd[0], time.Month(ymd[1]), ymd[2], 0, 0, 0, 0, time.UTC)
		if int(tm.Month()) != ymd[1] || tm.Day() != ymd[2] {
			return done(Value{}, fmt.Errorf("date field value out of range: %s", ymdText))
		}
		v, err := fromTime(tm, typeDate)
		if err != nil {
			return done(Value{}, fmt.Errorf("date out of range: %s", ymdText))
		}
		return done(v, nil)
	case "MAKE_TIMESTAMP", "MAKE_TIMESTAMPTZ", "MAKE_TIME":
		want := 6
		if f.Name == "MAKE_TIME" {
			want = 3
		}
		parts := make([]int, want-1)
		for i := range parts {
			n, err := intArg(args[i])
			if err != nil {
				return done(Value{}, err)
			}
			parts[i] = n
		}
		secV, err := Coerce(args[want-1], typeFloat64)
		if err != nil {
			return done(Value{}, err)
		}
		whole := math.Floor(secV.F)
		nanos := int(math.Round((secV.F - whole) * 1e9))
		if f.Name == "MAKE_TIME" {
			if parts[0] < 0 || parts[0] > 23 || parts[1] < 0 || parts[1] > 59 || whole < 0 || whole > 59 {
				return done(Value{}, fmt.Errorf("time field value out of range"))
			}
			return done(fromTime(time.Date(1970, 1, 1, parts[0], parts[1], int(whole), nanos, time.UTC), t))
		}
		tm := time.Date(parts[0], time.Month(parts[1]), parts[2], parts[3], parts[4], int(whole), nanos, time.UTC)
		if int(tm.Month()) != parts[1] || tm.Day() != parts[2] || parts[3] > 23 || parts[4] > 59 || whole > 59 {
			return done(Value{}, fmt.Errorf("date/time field value out of range"))
		}
		return done(fromTime(tm, t))
	case "TO_TIMESTAMP", "TO_DATE":
		if f.Name == "TO_TIMESTAMP" && len(args) == 1 {
			// Seconds since the epoch.
			sv, err := Coerce(args[0], typeFloat64)
			if err != nil {
				return done(Value{}, err)
			}
			whole := math.Floor(sv.F)
			return done(fromTime(time.Unix(int64(whole), int64(math.Round((sv.F-whole)*1e9))).UTC(), t))
		}
		tm, err := parseWithFormat(args[0].Text(), args[1].Text())
		if err != nil {
			return done(Value{}, err)
		}
		return done(fromTime(tm, t))
	case "TO_CHAR":
		s, err := toChar(args[0], args[1].Text())
		return done(stringValue(s), err)
	}
	return Value{}, false, nil
}
