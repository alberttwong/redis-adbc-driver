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

// TO_CHAR(x, format) for dates, times and timestamps, as Postgres's
// formatting.c does, and the format templates that TO_DATE and
// TO_TIMESTAMP read (parseWithFormat, datetime.go).
//
// A format is read left to right. At each position, an optional prefix
// modifier (FM or fm: no padding; TM or tm: translated names, which are the
// English ones, so no padding) is followed by the first template pattern of
// dchKeywords that the text starts with, then by an optional suffix (TH or
// th: an ordinal suffix on a number; SP: spelled out, which Postgres
// accepts and ignores too). Patterns are case-sensitive: there are upper-
// and lower-case spellings, and for names (MONTH, Month, month) the case is
// the output's. A prefix not followed by a pattern is dropped. Anything
// else is copied: text in double quotes (where \ escapes the next
// character), \" outside quotes (a literal "), and every other character.

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
)

// dchKeyword is a template pattern: its spelling in a format and the
// pattern it is (lower-case spellings of numbers are the same pattern).
type dchKeyword struct{ name, id string }

// dchKeywords are Postgres's template patterns, in the order it matches
// them: the first one the text starts with is the pattern.
var dchKeywords = func() []dchKeyword {
	var out []dchKeyword
	add := func(ids ...string) {
		for _, id := range ids {
			out = append(out, dchKeyword{name: id, id: id})
		}
	}
	lower := func(names ...string) {
		for _, n := range names {
			out = append(out, dchKeyword{name: strings.ToLower(n), id: n})
		}
	}
	add("A.D.", "A.M.", "AD", "AM", "B.C.", "BC", "CC", "DAY", "DDD", "DD", "DY", "Day", "Dy", "D",
		"FF1", "FF2", "FF3", "FF4", "FF5", "FF6", "FX", "HH24", "HH12", "HH",
		"IDDD", "ID", "IW", "IYYY", "IYY", "IY", "I", "J", "MI", "MM", "MONTH", "MON", "MS", "Month", "Mon",
		"OF", "P.M.", "PM", "Q", "RM")
	out = append(out, dchKeyword{"SSSSS", "SSSS"})
	add("SSSS", "SS", "TZH", "TZM", "TZ", "US", "WW", "W", "Y,YYY", "YYYY", "YYY", "YY", "Y")
	// The lower-case spellings: names and meridiem / era indicators give
	// lower-case output, numbers are the same patterns.
	add("a.d.", "a.m.", "ad", "am", "b.c.", "bc")
	lower("CC")
	add("day")
	lower("DDD", "DD")
	add("dy")
	lower("D", "FF1", "FF2", "FF3", "FF4", "FF5", "FF6", "FX", "HH24", "HH12", "HH",
		"IDDD", "ID", "IW", "IYYY", "IYY", "IY", "I", "J", "MI", "MM")
	add("month", "mon")
	lower("MS", "OF")
	add("p.m.", "pm")
	lower("Q")
	add("rm")
	out = append(out, dchKeyword{"sssss", "SSSS"})
	lower("SSSS", "SS", "TZH", "TZM")
	add("tz")
	lower("US", "WW", "W", "Y,YYY", "YYYY", "YYY", "YY", "Y")
	return out
}()

// fmtToken is one element of a format: a pattern with its modifiers, or a
// literal character.
type fmtToken struct {
	key    *dchKeyword // nil for literal text
	text   string      // the literal
	fm, tm bool        // FM (no padding), TM (translated names)
	th     byte        // 'T' for TH, 't' for th, 0 for neither
}

func matchKeyword(s string) *dchKeyword {
	for i := range dchKeywords {
		if strings.HasPrefix(s, dchKeywords[i].name) {
			return &dchKeywords[i]
		}
	}
	return nil
}

// tokenizeFormat splits a format into patterns and literal characters, as
// Postgres's parse_format does (see the top of the file).
func tokenizeFormat(f string) []fmtToken {
	var out []fmtToken
	for i := 0; i < len(f); {
		var fm, tm bool
		switch {
		case strings.HasPrefix(f[i:], "FM"), strings.HasPrefix(f[i:], "fm"):
			fm = true
			i += 2
		case strings.HasPrefix(f[i:], "TM"), strings.HasPrefix(f[i:], "tm"):
			tm = true
			i += 2
		}
		if i >= len(f) {
			break
		}
		if k := matchKeyword(f[i:]); k != nil {
			t := fmtToken{key: k, fm: fm, tm: tm}
			i += len(k.name)
			switch {
			case strings.HasPrefix(f[i:], "TH"):
				t.th = 'T'
				i += 2
			case strings.HasPrefix(f[i:], "th"):
				t.th = 't'
				i += 2
			case strings.HasPrefix(f[i:], "SP"):
				i += 2
			}
			out = append(out, t)
			continue
		}
		if f[i] == '"' {
			// Quoted text, where a backslash escapes the next character.
			for i++; i < len(f); {
				if f[i] == '"' {
					i++
					break
				}
				if f[i] == '\\' && i+1 < len(f) {
					i++
				}
				_, n := utf8.DecodeRuneInString(f[i:])
				out = append(out, fmtToken{text: f[i : i+n]})
				i += n
			}
			continue
		}
		if f[i] == '\\' && i+1 < len(f) && f[i+1] == '"' {
			i++ // \" is a literal "
		}
		_, n := utf8.DecodeRuneInString(f[i:])
		out = append(out, fmtToken{text: f[i : i+n]})
		i += n
	}
	return out
}

// ordinal appends the English ordinal suffix to the number s, in upper
// case for TH: 1st, 2nd, 3rd, 4th, 11th, 12th, 13th, 21st.
func ordinal(s string, th byte) string {
	suffix := "th"
	if n := len(s); n > 0 && (n < 2 || s[n-2] != '1') {
		switch s[n-1] {
		case '1':
			suffix = "st"
		case '2':
			suffix = "nd"
		case '3':
			suffix = "rd"
		}
	}
	if th == 'T' {
		suffix = strings.ToUpper(suffix)
	}
	return s + suffix
}

var romanMonths = []string{"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"}

// toChar formats a date, time or timestamp as Postgres's TO_CHAR does, in
// the session time zone z. A timestamp with time zone is formatted as its
// local time in z, a date as one at local midnight (Postgres converts it to
// one): TZ is z's abbreviation there (UTC, PST, IST), and TZH, TZM and OF
// its offset (+00, 00 and +00 in UTC). For a timestamp TZ is empty and the
// offset +00. A time is formatted as on 1970-01-01.
func toChar(v Value, format string, z tzZone) (string, error) {
	if v.T.Kind == KindDate {
		// As the timestamp with time zone of its local midnight (01:00 if
		// midnight is in a DST gap).
		var err error
		if v, err = coerceIn(v, timestampType(arrow.Second, "UTC"), z); err != nil {
			return "", err
		}
	}
	tm, err := toTime(v)
	if err != nil {
		return "", err
	}
	tzName, off := "", 0
	if isTimestampTZ(v.T) {
		off, tzName = z.offsetAt(tm.Unix()), z.abbrevAt(tm.Unix())
		tm = tm.Add(time.Duration(off) * time.Second)
	}
	tzName = strings.ToUpper(tzName)
	// The UTC offset, for TZH, TZM and OF.
	offSign, offAbs := '+', off
	if off < 0 {
		offSign, offAbs = '-', -off
	}
	y := tm.Year() // astronomical: 0 is 1 BC
	bc := y <= 0
	// era is a year as written with an era (1 BC is 1).
	era := func(y int) int {
		if y <= 0 {
			return 1 - y
		}
		return y
	}
	isoY, isoW := tm.ISOWeek()
	isoDow := (int(tm.Weekday())+6)%7 + 1
	month, day := tm.Month().String(), tm.Weekday().String()
	h12 := tm.Hour() % 12
	if h12 == 0 {
		h12 = 12
	}
	nanos := tm.Nanosecond()
	days := floorDiv(tm.Unix(), 86400)

	var b strings.Builder
	for _, t := range tokenizeFormat(format) {
		if t.key == nil {
			b.WriteString(t.text)
			continue
		}
		// num writes a number zero-padded to width (no padding with FM),
		// with its ordinal suffix for TH / th.
		num := func(n, width int) {
			if t.fm {
				width = 0
			}
			s := fmt.Sprintf("%0*d", width, n)
			if t.th != 0 {
				s = ordinal(s, t.th)
			}
			b.WriteString(s)
		}
		// fixed writes a number always padded to width (fractions of a
		// second, which FM doesn't change).
		fixed := func(n, width int) {
			s := fmt.Sprintf("%0*d", width, n)
			if t.th != 0 {
				s = ordinal(s, t.th)
			}
			b.WriteString(s)
		}
		// name writes a name padded to 9 characters (no padding with FM or
		// TM).
		name := func(s string) {
			if t.fm || t.tm {
				b.WriteString(s)
			} else {
				fmt.Fprintf(&b, "%-9s", s)
			}
		}
		pick := func(cond bool, yes, no string) {
			if cond {
				b.WriteString(yes)
			} else {
				b.WriteString(no)
			}
		}
		pm := tm.Hour() >= 12
		switch t.key.id {
		case "AM", "PM":
			pick(pm, "PM", "AM")
		case "am", "pm":
			pick(pm, "pm", "am")
		case "A.M.", "P.M.":
			pick(pm, "P.M.", "A.M.")
		case "a.m.", "p.m.":
			pick(pm, "p.m.", "a.m.")
		case "HH", "HH12":
			num(h12, 2)
		case "HH24":
			num(tm.Hour(), 2)
		case "MI":
			num(tm.Minute(), 2)
		case "SS":
			num(tm.Second(), 2)
		case "MS", "FF3":
			fixed(nanos/1_000_000, 3)
		case "US", "FF6":
			fixed(nanos/1_000, 6)
		case "FF1":
			fixed(nanos/100_000_000, 1)
		case "FF2":
			fixed(nanos/10_000_000, 2)
		case "FF4":
			fixed(nanos/100_000, 4)
		case "FF5":
			fixed(nanos/10_000, 5)
		case "SSSS":
			num(tm.Hour()*3600+tm.Minute()*60+tm.Second(), 0)
		case "TZ":
			b.WriteString(tzName)
		case "tz":
			b.WriteString(strings.ToLower(tzName))
		case "TZH":
			fmt.Fprintf(&b, "%c%02d", offSign, offAbs/3600)
		case "TZM":
			fmt.Fprintf(&b, "%02d", offAbs%3600/60)
		case "OF":
			width := 2
			if t.fm {
				width = 0
			}
			fmt.Fprintf(&b, "%c%0*d", offSign, width, offAbs/3600)
			if offAbs%3600 != 0 {
				fmt.Fprintf(&b, ":%02d", offAbs%3600/60)
			}
		case "AD", "BC":
			pick(bc, "BC", "AD")
		case "ad", "bc":
			pick(bc, "bc", "ad")
		case "A.D.", "B.C.":
			pick(bc, "B.C.", "A.D.")
		case "a.d.", "b.c.":
			pick(bc, "b.c.", "a.d.")
		case "MONTH":
			name(strings.ToUpper(month))
		case "Month":
			name(month)
		case "month":
			name(strings.ToLower(month))
		case "MON":
			b.WriteString(strings.ToUpper(month[:3]))
		case "Mon":
			b.WriteString(month[:3])
		case "mon":
			b.WriteString(strings.ToLower(month[:3]))
		case "MM":
			num(int(tm.Month()), 2)
		case "DAY":
			name(strings.ToUpper(day))
		case "Day":
			name(day)
		case "day":
			name(strings.ToLower(day))
		case "DY":
			b.WriteString(strings.ToUpper(day[:3]))
		case "Dy":
			b.WriteString(day[:3])
		case "dy":
			b.WriteString(strings.ToLower(day[:3]))
		case "DDD":
			num(tm.YearDay(), 3)
		case "IDDD":
			num((isoW-1)*7+isoDow, 3)
		case "DD":
			num(tm.Day(), 2)
		case "D":
			num(int(tm.Weekday())+1, 0)
		case "ID":
			num(isoDow, 0)
		case "WW":
			num((tm.YearDay()-1)/7+1, 2)
		case "IW":
			num(isoW, 2)
		case "W":
			num((tm.Day()-1)/7+1, 0)
		case "Q":
			num((int(tm.Month())-1)/3+1, 0)
		case "CC":
			// The 21st century starts on 2001-01-01, and the 1st century BC
			// is -01.
			c := y/100 - 1
			if y > 0 {
				c = (y-1)/100 + 1
			}
			width := 2
			if c < 0 {
				width = 3
			}
			if c > 99 || c < -99 {
				width = 0
			}
			num(c, width)
		case "Y,YYY":
			s := fmt.Sprintf("%d,%03d", era(y)/1000, era(y)%1000)
			if t.th != 0 {
				s = ordinal(s, t.th)
			}
			b.WriteString(s)
		case "YYYY":
			num(era(y), 4)
		case "IYYY":
			num(era(isoY), 4)
		case "YYY":
			num(era(y)%1000, 3)
		case "IYY":
			num(era(isoY)%1000, 3)
		case "YY":
			num(era(y)%100, 2)
		case "IY":
			num(era(isoY)%100, 2)
		case "Y":
			num(era(y)%10, 1)
		case "I":
			num(era(isoY)%10, 1)
		case "RM", "rm":
			// Padded to 4 characters, as in Postgres (no padding with FM).
			s := romanMonths[tm.Month()-1]
			if t.key.id == "rm" {
				s = strings.ToLower(s)
			}
			if !t.fm {
				s = fmt.Sprintf("%-4s", s)
			}
			b.WriteString(s)
		case "J":
			// The Julian day: days since November 24, 4714 BC.
			num(int(days+2440588), 0)
		case "FX":
			// Fixed format: only for TO_DATE and TO_TIMESTAMP.
		default:
			return "", fmt.Errorf("TO_CHAR pattern %q is not supported", t.key.name)
		}
	}
	return b.String(), nil
}
