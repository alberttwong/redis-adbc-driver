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

// Time zone conversion: x AT TIME ZONE z (TIMEZONE(z, x)), x AT LOCAL
// (TIMEZONE(x)) and CONVERT_TIMEZONE([src,] tgt, x).
//
// The session time zone is UTC: a TIMESTAMP WITH TIME ZONE is an instant,
// stored and shown in UTC, and a TIMESTAMP is a wall-clock time. As in
// Postgres:
//
//   - timestamp AT TIME ZONE z reads the timestamp as a local time in z and
//     gives the instant (a timestamp with time zone);
//   - timestamptz AT TIME ZONE z gives the local time in z of the instant (a
//     timestamp);
//   - a date, and text, are read as a timestamp with time zone (Postgres's
//     preferred type), so they give a timestamp. There is no TIME WITH TIME
//     ZONE, so a TIME is an error.
//
// A zone is, in Postgres's order: a time zone abbreviation (a fixed offset;
// zoneAbbrevs), an IANA zone name (case-insensitive, from the tz database
// embedded in the driver), or a POSIX-style offset such as 'UTC+5' or
// '+05:30', whose sign is west of Greenwich as in POSIX ('UTC+5' is five
// hours behind UTC). An INTERVAL zone is an offset east of Greenwich
// (INTERVAL '+05:30' is ahead of UTC), and has no months or days. Anything
// else is `time zone "x" not recognized`.
//
// A local time that a DST transition skips (02:30 on a spring-forward day)
// is read with the offset before the transition, and one that it repeats
// (01:30 on a fall-back day) with the offset after it, as Postgres does.

import (
	"fmt"
	"strings"
	"sync"
	"time"
	_ "time/tzdata" // the IANA zones, so that builds don't depend on the host's
)

// tzZone is a time zone: an IANA zone, or a fixed offset when loc is nil.
type tzZone struct {
	loc *time.Location
	off int // fixed offset, seconds east of UTC
}

// offsetAt is the zone's offset, in seconds east of UTC, at the instant
// utc (seconds since the epoch).
func (z tzZone) offsetAt(utc int64) int {
	if z.loc == nil {
		return z.off
	}
	_, off := time.Unix(utc, 0).In(z.loc).Zone()
	return off
}

// localToUTC converts a local time in the zone (seconds since the epoch,
// read as if it were UTC) to the instant, as Postgres's
// DetermineTimeZoneOffset does: it finds the first transition after a day
// before the local time, and uses the offset before or after it. A local
// time in a spring-forward gap gets the offset before the transition, and
// one in a fall-back overlap the offset after it.
func (z tzZone) localToUTC(local int64) int64 {
	if z.loc == nil {
		return local - int64(z.off)
	}
	prev := time.Unix(local-86400, 0).In(z.loc)
	_, beforeOff := prev.Zone()
	_, end := prev.ZoneBounds()
	if end.IsZero() {
		return local - int64(beforeOff) // no transition after it
	}
	boundary := end.Unix()
	_, afterOff := end.In(z.loc).Zone()
	before, after := local-int64(beforeOff), local-int64(afterOff)
	switch {
	case before < boundary && after < boundary:
		return before
	case before > boundary && after >= boundary:
		return after
	case before > after: // a gap: the offset before the transition
		return before
	}
	return after // an overlap: the offset after it
}

// zoneAbbrevs are the time zone abbreviations, from Postgres's default set,
// in seconds east of UTC. Postgres looks them up before zone names, so
// 'CET' is always UTC+1 (the IANA zone CET observes summer time).
var zoneAbbrevs = map[string]int{
	"utc": 0, "ut": 0, "uct": 0, "gmt": 0, "z": 0, "zulu": 0,
	// North America
	"est": -5 * 3600, "edt": -4 * 3600, "cst": -6 * 3600, "cdt": -5 * 3600,
	"mst": -7 * 3600, "mdt": -6 * 3600, "pst": -8 * 3600, "pdt": -7 * 3600,
	"akst": -9 * 3600, "akdt": -8 * 3600, "hst": -10 * 3600,
	"ast": -4 * 3600, "adt": -3 * 3600, "nst": -(3*3600 + 1800), "ndt": -(2*3600 + 1800),
	// Europe
	"wet": 0, "west": 3600, "bst": 3600, "cet": 3600, "cest": 2 * 3600, "eet": 2 * 3600, "eest": 3 * 3600,
	// Asia and the Pacific
	"jst": 9 * 3600, "kst": 9 * 3600, "hkt": 8 * 3600, "awst": 8 * 3600,
	"acst": 9*3600 + 1800, "acdt": 10*3600 + 1800, "aest": 10 * 3600, "aedt": 11 * 3600,
	"nzst": 12 * 3600, "nzdt": 13 * 3600,
}

// zoneCache holds resolved zones, also unknown ones (ok false), by name.
var zoneCache = struct {
	sync.Mutex
	m map[string]cachedZone
}{m: map[string]cachedZone{}}

type cachedZone struct {
	z  tzZone
	ok bool
}

const zoneCacheSize = 4096

// resolveZone finds the zone a name names (see the top of the file).
func resolveZone(name string) (tzZone, error) {
	zoneCache.Lock()
	c, hit := zoneCache.m[name]
	zoneCache.Unlock()
	if !hit {
		c.z, c.ok = lookupZone(name)
		zoneCache.Lock()
		if len(zoneCache.m) < zoneCacheSize {
			zoneCache.m[name] = c
		}
		zoneCache.Unlock()
	}
	if !c.ok {
		return tzZone{}, fmt.Errorf("time zone \"%s\" not recognized", name)
	}
	return c.z, nil
}

func lookupZone(name string) (tzZone, bool) {
	if off, ok := zoneAbbrevs[strings.ToLower(name)]; ok {
		return tzZone{off: off}, true
	}
	// time.LoadLocation reads "" as UTC and "Local" as the host's zone,
	// neither of which is a zone name.
	if name != "" && !strings.EqualFold(name, "local") {
		for _, c := range zoneSpellings(name) {
			if loc, err := time.LoadLocation(c); err == nil {
				return tzZone{loc: loc}, true
			}
		}
	}
	if off, ok := posixOffset(name); ok {
		return tzZone{off: off}, true
	}
	return tzZone{}, false
}

// zoneSpellings are the spellings of an IANA zone name to try, since zone
// names are case-insensitive in Postgres and the tz database's aren't:
// the name as written, then each word (between "/", "_" and "-")
// capitalized or in upper case, with a few words that the database keeps in
// lower case (Port-au-Prince, Port_of_Spain, Dar_es_Salaam) or camel case
// (McMurdo, DumontDUrville).
func zoneSpellings(name string) []string {
	out := []string{name}
	if strings.ContainsAny(name, ".\\") || len(name) > 64 {
		return out
	}
	// The words, and the separators after them.
	var words, seps []string
	start := 0
	for i := 0; i < len(name); i++ {
		if c := name[i]; c == '/' || c == '_' || c == '-' {
			words, seps = append(words, name[start:i]), append(seps, string(c))
			start = i + 1
		}
	}
	words, seps = append(words, name[start:]), append(seps, "")
	if len(words) > 6 {
		return out
	}
	variants := make([][]string, len(words))
	for i, w := range words {
		lw := strings.ToLower(w)
		switch {
		case zoneLowerWords[lw]:
			variants[i] = []string{lw}
		case zoneCamelWords[lw] != "":
			variants[i] = []string{zoneCamelWords[lw]}
		case lw == "":
			variants[i] = []string{""}
		default:
			title := strings.ToUpper(lw[:1]) + lw[1:]
			variants[i] = []string{title}
			if up := strings.ToUpper(lw); up != title {
				variants[i] = append(variants[i], up)
			}
		}
	}
	// Every combination, with capitalized words first.
	var build func(i int, prefix string)
	build = func(i int, prefix string) {
		if i == len(words) {
			if prefix != name {
				out = append(out, prefix)
			}
			return
		}
		for _, v := range variants[i] {
			build(i+1, prefix+v+seps[i])
		}
	}
	build(0, "")
	return out
}

// zoneLowerWords are the words of zone names written in lower case.
var zoneLowerWords = map[string]bool{"au": true, "of": true, "es": true}

// zoneCamelWords are the words of zone names written in camel case.
var zoneCamelWords = map[string]string{
	"mcmurdo": "McMurdo", "dumontdurville": "DumontDUrville", "denoronha": "DeNoronha",
	"bajanorte": "BajaNorte", "bajasur": "BajaSur", "comodrivadavia": "ComodRivadavia",
	"easterisland": "EasterIsland",
}

// posixOffset reads a POSIX-style time zone without daylight saving time,
// as Postgres accepts it: an optional name (up to the first digit, sign or
// comma, or anything in <>) and an offset [+-]hh[:mm[:ss]] (hh up to 167)
// west of Greenwich, so 'UTC+5' and '+05' are UTC-05:00 and
// '<+0530>-05:30' is UTC+05:30. It returns the offset east of UTC.
func posixOffset(s string) (int, bool) {
	i := 0
	if strings.HasPrefix(s, "<") {
		end := strings.IndexByte(s, '>')
		if end < 0 {
			return 0, false
		}
		i = end + 1
	} else {
		for i < len(s) && !(s[i] >= '0' && s[i] <= '9') && s[i] != ',' && s[i] != '-' && s[i] != '+' {
			i++
		}
	}
	if i == len(s) {
		return 0, false // a name without an offset
	}
	sign := 1
	switch s[i] {
	case '-':
		sign = -1
		i++
	case '+':
		i++
	}
	num := func(max int) (int, bool) {
		start, n := i, 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			n = n*10 + int(s[i]-'0')
			if n > max {
				return 0, false
			}
			i++
		}
		return n, i > start
	}
	h, ok := num(24*7 - 1)
	if !ok {
		return 0, false
	}
	secs := h * 3600
	if i < len(s) && s[i] == ':' {
		i++
		m, ok := num(59)
		if !ok {
			return 0, false
		}
		secs += m * 60
		if i < len(s) && s[i] == ':' {
			i++
			sec, ok := num(60)
			if !ok {
				return 0, false
			}
			secs += sec
		}
	}
	if i != len(s) {
		return 0, false // daylight saving time rules aren't supported
	}
	return -sign * secs, true
}

// intervalZone is the zone of an INTERVAL in TIMEZONE: an offset east of
// UTC, without months or days (fractions of a second are dropped, as in
// Postgres).
func intervalZone(v Value) (tzZone, error) {
	if v.Months != 0 || v.Days != 0 {
		return tzZone{}, fmt.Errorf("interval time zone \"%s\" must not include months or days", formatInterval(v))
	}
	return tzZone{off: int(v.I / int64(time.Second))}, nil
}

// zoneTimestamp reports whether x has a type that TIMEZONE and
// CONVERT_TIMEZONE convert: a timestamp, a date, or text (read as a
// timestamp).
func zoneTimestamp(x Value) bool {
	return x.T.Kind == KindTimestamp || x.T.Kind == KindDate || x.T.Kind == KindString
}

// timezoneType is the result type of TIMEZONE(z, x): a timestamp gives a
// timestamp with time zone, and a timestamp with time zone (or a date, or
// text) a timestamp. The unit is kept.
func timezoneType(x ColType) ColType {
	if x.Kind == KindTimestamp {
		if x.TZ == "" {
			return timestampType(x.Unit, "UTC")
		}
		return timestampType(x.Unit, "")
	}
	return typeTimestamp
}

// convertTimezoneType is the result type of CONVERT_TIMEZONE: a timestamp,
// with the unit of a timestamp argument.
func convertTimezoneType(x ColType) ColType {
	if x.Kind == KindTimestamp {
		return timestampType(x.Unit, "")
	}
	return typeTimestamp
}

// atTimeZone evaluates TIMEZONE(z, x) (x AT TIME ZONE z), or with one
// argument TIMEZONE(x) (x AT LOCAL: the session time zone, UTC). Arguments
// are non-NULL; t is the result type.
func atTimeZone(f *Func, args []Value, t ColType) (Value, error) {
	zv, x := stringValue("UTC"), args[0]
	if len(args) == 2 {
		zv, x = args[0], args[1]
	}
	if !zoneTimestamp(x) || (zv.T.Kind != KindString && zv.T.Kind != KindInterval) {
		return Value{}, noSuchFunction(f.Name, false, argTypes(args))
	}
	var z tzZone
	var err error
	if zv.T.Kind == KindInterval {
		z, err = intervalZone(zv)
	} else {
		z, err = resolveZone(zv.S)
	}
	if err != nil {
		return Value{}, err
	}
	tm, err := toTime(x)
	if err != nil {
		return Value{}, err
	}
	sec := tm.Unix()
	if x.T.Kind == KindTimestamp && x.T.TZ == "" {
		sec = z.localToUTC(sec) // a local time in z: the instant
	} else {
		sec += int64(z.offsetAt(sec)) // an instant: the local time in z
	}
	return fromTime(time.Unix(sec, int64(tm.Nanosecond())).UTC(), t)
}

// convertTimezone evaluates CONVERT_TIMEZONE(src, tgt, x) as Snowflake and
// Redshift do: x read as a local time in src, as the local time in tgt. A
// timestamp with time zone is read as its UTC time, as Snowflake reads it
// as a TIMESTAMP_NTZ. CONVERT_TIMEZONE(tgt, x) reads x in UTC and gives a
// timestamp, as in Redshift (Snowflake's TIMESTAMP_TZ, an instant with the
// target's offset, has no equivalent in the driver). Zones are text.
func convertTimezone(f *Func, args []Value, t ColType) (Value, error) {
	zones, x := args[:len(args)-1], args[len(args)-1]
	if !zoneTimestamp(x) {
		return Value{}, noSuchFunction(f.Name, false, argTypes(args))
	}
	for _, z := range zones {
		if z.T.Kind != KindString {
			return Value{}, noSuchFunction(f.Name, false, argTypes(args))
		}
	}
	var src tzZone // UTC
	var err error
	if len(zones) == 2 {
		if src, err = resolveZone(zones[0].S); err != nil {
			return Value{}, err
		}
	}
	tgt, err := resolveZone(zones[len(zones)-1].S)
	if err != nil {
		return Value{}, err
	}
	tm, err := toTime(x)
	if err != nil {
		return Value{}, err
	}
	utc := src.localToUTC(tm.Unix())
	return fromTime(time.Unix(utc+int64(tgt.offsetAt(utc)), int64(tm.Nanosecond())).UTC(), t)
}
