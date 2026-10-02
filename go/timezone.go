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

// Time zones: x AT TIME ZONE z (TIMEZONE(z, x)), x AT LOCAL (TIMEZONE(x)),
// CONVERT_TIMEZONE([src,] tgt, x), and the session time zone (SET TIME
// ZONE, the TimeZone parameter).
//
// A TIMESTAMP WITH TIME ZONE is an instant, stored as UTC, and a TIMESTAMP
// is a wall-clock time. As in Postgres:
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
//
// The session time zone (UTC unless SET TIME ZONE or adbc.redis.time_zone
// changes it) is where a timestamp with time zone is shown and read as
// local time, as in Postgres: its text (casts, ||, TO_CHAR, JSON), text
// without an offset read as one, conversions to and from TIMESTAMP and
// DATE, its fields (EXTRACT, DATE_TRUNC, AGE, …), adding months and days to
// one, CURRENT_DATE / LOCALTIMESTAMP / CURRENT_TIME, and AT LOCAL. Stored
// values, Arrow results and index queries are UTC instants, whatever it is.
// Code that has an evalEnv gets the zone from env.zone(); the functions
// without one (Coerce, compareValues, Text) use UTC, and their …In forms
// take the zone.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata" // the IANA zones, so that builds don't depend on the host's

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
)

// tzZone is a time zone: an IANA zone, or a fixed offset when loc is nil.
// The zero value is UTC.
type tzZone struct {
	loc *time.Location
	off int // fixed offset, seconds east of UTC
	// abbr is a fixed offset's abbreviation, TO_CHAR's TZ ("" for UTC).
	abbr string
}

// utcZone is UTC, the session time zone unless it is set.
var utcZone = tzZone{}

// isUTC reports whether the zone is UTC at every instant (so conversions
// can be skipped).
func (z tzZone) isUTC() bool {
	return (z.loc == nil && z.off == 0) || z.loc == time.UTC
}

// abbrevAt is the zone's abbreviation at the instant utc (seconds since the
// epoch), as TO_CHAR's TZ writes it: PST or PDT for America/Los_Angeles,
// IST for Asia/Kolkata, UTC for UTC.
func (z tzZone) abbrevAt(utc int64) string {
	if z.loc != nil {
		name, _ := time.Unix(utc, 0).In(z.loc).Zone()
		return name
	}
	if z.abbr == "" && z.off == 0 {
		return "UTC"
	}
	return z.abbr
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
	if z, ok := abbrevZone(name); ok {
		return z, true
	}
	if z, ok := ianaZone(name); ok {
		return z, true
	}
	return posixZone(name)
}

// abbrevZone is the zone of a time zone abbreviation (zoneAbbrevs).
func abbrevZone(name string) (tzZone, bool) {
	off, ok := zoneAbbrevs[strings.ToLower(name)]
	return tzZone{off: off, abbr: strings.ToUpper(name)}, ok
}

// ianaZone is the IANA zone a name names, case-insensitive.
func ianaZone(name string) (tzZone, bool) {
	// time.LoadLocation reads "" as UTC and "Local" as the host's zone,
	// neither of which is a zone name.
	if name != "" && !strings.EqualFold(name, "local") {
		for _, c := range zoneSpellings(name) {
			if loc, err := time.LoadLocation(c); err == nil {
				return tzZone{loc: loc}, true
			}
		}
	}
	return tzZone{}, false
}

// posixZone is the zone of a POSIX-style offset (posixOffset). Its
// abbreviation is the name before the offset: UTC for 'UTC+5', +0530 for
// '<+0530>-05:30'.
func posixZone(name string) (tzZone, bool) {
	off, ok := posixOffset(name)
	if !ok {
		return tzZone{}, false
	}
	abbr := name
	if strings.HasPrefix(name, "<") {
		abbr = name[1:strings.IndexByte(name, '>')]
	} else if i := strings.IndexAny(name, "0123456789,+-"); i >= 0 {
		abbr = name[:i]
	}
	return tzZone{off: off, abbr: strings.ToUpper(abbr)}, true
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
// argument TIMEZONE(x) (x AT LOCAL: the session time zone, session).
// Arguments are non-NULL; t is the result type. A date or text is read as a
// timestamp with time zone, in the session time zone.
func atTimeZone(f *Func, args []Value, t ColType, session tzZone) (Value, error) {
	z, x := session, args[len(args)-1]
	if !zoneTimestamp(x) {
		return Value{}, noSuchFunction(f.Name, false, argTypes(args))
	}
	if len(args) == 2 {
		var err error
		switch zv := args[0]; zv.T.Kind {
		case KindInterval:
			z, err = intervalZone(zv)
		case KindString:
			z, err = resolveZone(zv.S)
		default:
			return Value{}, noSuchFunction(f.Name, false, argTypes(args))
		}
		if err != nil {
			return Value{}, err
		}
	}
	var err error
	switch x.T.Kind {
	case KindString:
		x, err = parseStringIn(x.S, typeTimestampTZ, session)
	case KindDate:
		x, err = coerceIn(x, typeTimestampTZ, session)
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

// ---- the session time zone ----

// pgEpoch is Postgres's epoch, 2000-01-01 00:00:00 UTC, in seconds since
// the Unix epoch. Postgres rounds timestamps half away from it.
const pgEpoch = 946684800

// maxZoneOffset bounds a session time zone's offset, as Postgres's POSIX
// parser does (hours up to 167).
const maxZoneOffset = 168 * 3600

// sessionZone resolves a value of the TimeZone parameter (SET TIME ZONE,
// SET timezone, adbc.redis.time_zone) as Postgres's check_timezone does. It
// returns the zone and the text SHOW shows:
//
//   - INTERVAL 'x' [qualifier], also as text: an offset east of Greenwich,
//     without months or days. Shown as Postgres names such a zone,
//     <+05:30>-05:30.
//   - A number: hours east of Greenwich (SQL's sign, which is the reverse
//     of POSIX's), shown the same way: -8 is <-08>+08.
//   - An IANA zone name, case-insensitive, shown as the tz database spells
//     it (america/new_york is America/New_York), then a POSIX-style offset
//     ('UTC+5', west of Greenwich), shown in upper case.
//   - Unlike Postgres, which refuses them here, a time zone abbreviation
//     (zoneAbbrevs) that isn't also a zone name, as a fixed offset, shown in
//     upper case: PST.
//
// Anything else is `invalid value for parameter "TimeZone": "x"`.
func sessionZone(text string) (tzZone, string, error) {
	invalid := func() (tzZone, string, error) {
		return tzZone{}, "", invalidValue("TimeZone", text)
	}
	trimmed := strings.TrimSpace(text)
	if len(trimmed) >= 8 && strings.EqualFold(trimmed[:8], "interval") {
		return intervalSessionZone(text)
	}
	if hours, ok := zoneHours(trimmed); ok {
		off := int(hours * 3600)
		if off <= -maxZoneOffset || off >= maxZoneOffset {
			return invalid()
		}
		return offsetSessionZone(off)
	}
	if z, ok := ianaZone(text); ok {
		return z, canonicalZoneName(z.loc.String()), nil
	}
	if z, ok := posixZone(text); ok {
		return z, strings.ToUpper(text), nil
	}
	if z, ok := abbrevZone(text); ok {
		return z, strings.ToUpper(text), nil
	}
	return invalid()
}

// zoneDirs are where time.LoadLocation looks for zone files before the
// tz database embedded in the driver (as in Go's zoneinfo_unix.go).
var zoneDirs = []string{"/usr/share/zoneinfo/", "/usr/share/lib/zoneinfo/", "/usr/lib/locale/TZ/", "/etc/zoneinfo/"}

// canonicalZoneName is the tz database's spelling of a zone name that
// time.LoadLocation loaded. On a case-insensitive file system (macOS) it
// loads a zone file in any case, so the spelling is looked up in the
// directory listing, as Postgres looks up zone names. The embedded tz
// database is case-sensitive: a name loaded from it is spelled right.
func canonicalZoneName(name string) string {
	dirs := zoneDirs
	if z := os.Getenv("ZONEINFO"); z != "" {
		dirs = append([]string{z}, dirs...)
	}
	for _, dir := range dirs {
		if exact, ok := exactZonePath(dir, name); ok {
			return exact
		}
	}
	return name
}

// exactZonePath finds the file name names under dir, ignoring case, and
// returns its path as spelled in the directory.
func exactZonePath(dir, name string) (string, bool) {
	parts := strings.Split(name, "/")
	for i, part := range parts {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return "", false
		}
		found := ""
		for _, e := range entries {
			if e.Name() == part {
				found = part
				break
			}
			if found == "" && strings.EqualFold(e.Name(), part) {
				found = e.Name()
			}
		}
		if found == "" {
			return "", false
		}
		parts[i] = found
		dir = filepath.Join(dir, found)
	}
	return strings.Join(parts, "/"), true
}

// zoneHours reads a TimeZone value that is a number, as Postgres's strtod
// does: hours, possibly with a fraction or an exponent.
func zoneHours(s string) (float64, bool) {
	digit := false
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
			digit = true
		case c == '.' || c == '+' || c == '-' || c == 'e' || c == 'E':
		default:
			return 0, false
		}
	}
	if !digit {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

// intervalSessionZone reads INTERVAL 'x' [qualifier] as a TimeZone value.
func intervalSessionZone(text string) (tzZone, string, error) {
	invalid := invalidValue("TimeZone", text)
	x, err := parseExprText(text)
	if err != nil {
		return tzZone{}, "", invalid
	}
	v, err := (&evalEnv{}).eval(x)
	if err != nil || v.Null || v.T.Kind != KindInterval {
		return tzZone{}, "", invalid
	}
	shown := "INTERVAL '" + formatInterval(v) + "'"
	switch {
	case v.Months != 0:
		return tzZone{}, "", errorf(adbc.StatusInvalidArgument,
			"invalid value for parameter %q: %q (cannot specify months in time zone interval)", "TimeZone", shown)
	case v.Days != 0:
		return tzZone{}, "", errorf(adbc.StatusInvalidArgument,
			"invalid value for parameter %q: %q (cannot specify days in time zone interval)", "TimeZone", shown)
	}
	off := int(v.I / int64(time.Second))
	if off <= -maxZoneOffset || off >= maxZoneOffset {
		return tzZone{}, "", invalidValue("TimeZone", shown)
	}
	return offsetSessionZone(off)
}

// offsetSessionZone is the session zone of a fixed offset (seconds east),
// with the name Postgres gives it: <+05:30>-05:30 for 05:30 east, <-08>+08
// for 8 hours west (POSIX's sign after the brackets).
func offsetSessionZone(off int) (tzZone, string, error) {
	abs := off
	if abs < 0 {
		abs = -abs
	}
	hm := fmt.Sprintf("%02d", abs/3600)
	if r := abs % 3600; r != 0 {
		hm += fmt.Sprintf(":%02d", r/60)
		if r%60 != 0 {
			hm += fmt.Sprintf(":%02d", r%60)
		}
	}
	east, west := "+"+hm, "-"+hm
	if off < 0 {
		east, west = west, east
	}
	return tzZone{off: off, abbr: east}, "<" + east + ">" + west, nil
}

// errTimestampRange is the error for a timestamp that a time zone moves
// out of the range of its unit.
var errTimestampRange = errors.New("timestamp out of range")

// localUnits converts v, units since the epoch of an instant (a timestamp
// with time zone), to the local time in z, in the same units.
func (z tzZone) localUnits(v int64, unit arrow.TimeUnit) (int64, error) {
	if z.isUTC() {
		return v, nil
	}
	per := unitsPerSecond[unit]
	d := int64(z.offsetAt(floorDiv(v, per))) * per
	r := v + d
	if (d > 0) != (r > v) && d != 0 {
		return 0, errTimestampRange
	}
	return r, nil
}

// utcUnits converts a local time in z (units since the epoch, read as if
// it were UTC) to the instant, in the same units, resolving DST gaps and
// overlaps as Postgres does (localToUTC).
func (z tzZone) utcUnits(local int64, unit arrow.TimeUnit) (int64, error) {
	if z.isUTC() {
		return local, nil
	}
	per := unitsPerSecond[unit]
	sec := floorDiv(local, per)
	d := (z.localToUTC(sec) - sec) * per
	r := local + d
	if (d > 0) != (r > local) && d != 0 {
		return 0, errTimestampRange
	}
	return r, nil
}

// localTime is the local time in z of an instant, as a time.Time in UTC
// with the local wall clock.
func (z tzZone) localTime(tm time.Time) time.Time {
	if z.isUTC() {
		return tm
	}
	return tm.Add(time.Duration(z.offsetAt(tm.Unix())) * time.Second)
}

// instant is the instant of a local wall-clock time in z (a time.Time in
// UTC), resolving DST gaps and overlaps as Postgres does.
func (z tzZone) instant(local time.Time) time.Time {
	if z.isUTC() {
		return local
	}
	return time.Unix(z.localToUTC(local.Unix()), int64(local.Nanosecond())).UTC()
}

// localFields is the wall-clock time of a date/time value whose fields
// (year, hour, …) a function reads: a timestamp with time zone's local time
// in z, and the others' own (toTime).
func localFields(v Value, z tzZone) (time.Time, error) {
	tm, err := toTime(v)
	if err == nil && v.T.Kind == KindTimestamp && v.T.TZ != "" {
		tm = z.localTime(tm)
	}
	return tm, err
}

// addIntervalIn adds an interval to an instant as Postgres adds one to a
// timestamp with time zone: months, then days, on the local time in z (each
// read back with Postgres's DST rule), then the time as elapsed time. So a
// day is 23 or 25 hours across a DST transition.
func addIntervalIn(tm time.Time, iv Value, z tzZone) time.Time {
	if z.isUTC() {
		return addInterval(tm, iv)
	}
	if iv.Months != 0 {
		tm = z.instant(addMonths(z.localTime(tm), int(iv.Months)))
	}
	if iv.Days != 0 {
		tm = z.instant(z.localTime(tm).AddDate(0, 0, int(iv.Days)))
	}
	return tm.Add(time.Duration(iv.I))
}

// offsetRange is the least and the greatest offset (seconds east of UTC)
// the zone has within span seconds of the instant utc.
func (z tzZone) offsetRange(utc, span int64) (int, int) {
	if z.loc == nil {
		return z.off, z.off
	}
	lo, hi := z.offsetAt(utc), z.offsetAt(utc)
	t := utc - span
	for range 64 {
		tm := time.Unix(t, 0).In(z.loc)
		_, off := tm.Zone()
		lo, hi = min(lo, off), max(hi, off)
		_, end := tm.ZoneBounds()
		if end.IsZero() || end.Unix() > utc+span {
			break
		}
		t = end.Unix()
	}
	return lo, hi
}

// localBounds is for index queries on a TIMESTAMP or DATE column of type
// ct compared with a TIMESTAMP WITH TIME ZONE cv, in a session time zone z
// other than UTC (ok is false otherwise). Postgres reads each row's value
// as a local time in z, which isn't monotonic (a local time in a DST gap
// reads as a later instant than the times just after the gap), so cv can't
// simply be converted to a local time: lo and hi (stored values) bound the
// local times that read as cv, from cv's local time with the least and
// the greatest offset z has around it. A row with a value below lo reads
// as an instant before cv, one above hi as one after it; the comparison is
// re-checked on the rows between. err is set if a bound is out of range.
func localBounds(cv Value, ct ColType, z tzZone) (lo, hi string, ok bool, err error) {
	if !isTimestampTZ(cv.T) || z.isUTC() || !(ct.Kind == KindDate || (ct.Kind == KindTimestamp && ct.TZ == "")) {
		return "", "", false, nil
	}
	per := unitsPerSecond[cv.T.Unit]
	offLo, offHi := z.offsetRange(floorDiv(cv.I, per), 2*86400)
	bound := func(off int) (string, error) {
		d := int64(off) * per
		local := cv.I + d
		if (d > 0) != (local > cv.I) && d != 0 {
			return "", errTimestampRange
		}
		v, err := Coerce(intValue(timestampType(cv.T.Unit, ""), local), ct)
		if err != nil {
			return "", err
		}
		return encodeStored(v), nil
	}
	if lo, err = bound(offLo); err == nil {
		hi, err = bound(offHi)
	}
	return lo, hi, true, err
}
