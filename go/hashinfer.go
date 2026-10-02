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

// Guessing a table's columns from HASHes
//
// Every HASH field becomes a column. Its type is the narrowest one that
// fits the text of every sampled value: BOOLEAN; BIGINT, then NUMERIC(38, s)
// for fixed-point text, then DOUBLE PRECISION; DATE, then TIMESTAMP or
// TIMESTAMP WITH TIME ZONE for ISO 8601 text; BYTEA for values that aren't
// UTF-8; VARCHAR otherwise. The text is then read with parseString, as a
// SQL cast reads it. An empty value doesn't count, and reads as NULL in a
// column that isn't a string.

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-adbc/go/adbc"
)

// valueClass is what a value's text looks like.
type valueClass int

const (
	classBool      valueClass = iota // true, false, t, f, yes, no (any case)
	classInt                         // a decimal integer that fits in 64 bits
	classDecimal                     // fixed-point text such as 12.50, or a longer integer
	classFloat                       // a finite number with an exponent, such as 1e-3
	classSpecial                     // NaN or an infinity
	classDate                        // YYYY-MM-DD
	classTimestamp                   // ISO 8601 date and time
	classText                        // anything else that is UTF-8
	classBinary                      // not UTF-8
	numValueClasses
)

var (
	intTextRe       = regexp.MustCompile(`^[+-]?[0-9]+$`)
	decimalTextRe   = regexp.MustCompile(`^[+-]?([0-9]+\.[0-9]*|\.[0-9]+)$`)
	floatTextRe     = regexp.MustCompile(`^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)[eE][+-]?[0-9]+$`)
	dateTextRe      = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	timestampTextRe = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?(Z|z|[+-][0-9]{2}(:?[0-9]{2})?)?$`)
)

// maxDecimalScale is the largest scale a guessed NUMERIC takes; text with
// more decimals is DOUBLE PRECISION.
const maxDecimalScale = 9

// classified is a value's class and what else its text says.
type classified struct {
	class      valueClass
	scale      int  // decimals of a classDecimal
	intDigits  int  // digits before the point of a classInt or classDecimal
	zoned      bool // a classTimestamp with an offset
	fracDigits int  // fractional-second digits of a classTimestamp
	leadZero   bool // an integer written with leading zeros (classText)
}

func classify(s string) classified {
	if !utf8.ValidString(s) {
		return classified{class: classBinary}
	}
	switch strings.ToLower(s) {
	case "true", "false", "t", "f", "yes", "no":
		return classified{class: classBool}
	case "nan", "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity":
		return classified{class: classSpecial}
	}
	switch {
	case intTextRe.MatchString(s):
		digits := strings.TrimLeft(s, "+-")
		if len(digits) > 1 && digits[0] == '0' {
			// 007 or a ZIP code: a number would lose the zeros.
			return classified{class: classText, leadZero: true}
		}
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return classified{class: classInt, intDigits: len(digits)}
		}
		if len(digits) <= 38 {
			return classified{class: classDecimal, intDigits: len(digits)}
		}
	case decimalTextRe.MatchString(s):
		whole, frac, _ := strings.Cut(strings.TrimLeft(s, "+-"), ".")
		whole = strings.TrimLeft(whole, "0")
		if len(frac) <= maxDecimalScale && len(whole)+len(frac) <= 38 {
			return classified{class: classDecimal, scale: len(frac), intDigits: len(whole)}
		}
		return classified{class: classFloat}
	case floatTextRe.MatchString(s):
		if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsInf(f, 0) {
			return classified{class: classFloat}
		}
	case dateTextRe.MatchString(s):
		if _, err := parseString(s, typeDate); err == nil {
			return classified{class: classDate}
		}
	case timestampTextRe.MatchString(s):
		if _, _, digits, zoned, err := parseTimestamp(s); err == nil {
			return classified{class: classTimestamp, zoned: zoned, fracDigits: digits}
		}
	}
	return classified{class: classText}
}

// fieldGuess collects what the sampled values of one field look like.
type fieldGuess struct {
	field      string
	present    int // HASHes that have the field
	empty      int
	counts     [numValueClasses]int
	examples   [numValueClasses]example
	scale      int
	intDigits  int
	zoned      int
	unzoned    int
	fracDigits int
	leadZero   int
	zeroOne    bool // every integer is 0 or 1
	zero, one  bool // some integer is 0, or 1
	json       int
	nul        int
	// tagLevel is the level (see tags.go) that the values need as an
	// indexed string column.
	tagLevel string
	// fits is the in-place type that -type gave the column, which every
	// stored value must be readable as; misfits counts those that aren't.
	fits    *ColType
	misfits int
	misfit  example
}

// example is a value and the key of the HASH it is in.
type example struct{ key, value string }

func (g *fieldGuess) add(key, v string) {
	g.present++
	if l := tagLevelOf(v); tagRank(l) > tagRank(g.tagLevel) {
		g.tagLevel = l
	}
	if g.fits != nil && storedFits(v, *g.fits) != nil {
		if g.misfits == 0 {
			g.misfit = example{key, v}
		}
		g.misfits++
	}
	if v == "" {
		g.empty++
		return
	}
	c := classify(v)
	if g.counts[c.class] == 0 {
		g.examples[c.class] = example{key, v}
	}
	g.counts[c.class]++
	g.scale = max(g.scale, c.scale)
	g.intDigits = max(g.intDigits, c.intDigits)
	switch c.class {
	case classInt:
		if g.counts[classInt] == 1 {
			g.zeroOne = true
		}
		g.zeroOne = g.zeroOne && (v == "0" || v == "1")
		g.zero, g.one = g.zero || v == "0", g.one || v == "1"
	case classTimestamp:
		if c.zoned {
			g.zoned++
		} else {
			g.unzoned++
		}
		g.fracDigits = max(g.fracDigits, c.fracDigits)
	case classText:
		if c.leadZero {
			g.leadZero++
		}
		if (v[0] == '{' || v[0] == '[') && json.Valid([]byte(v)) {
			g.json++
		}
	}
	if strings.IndexByte(v, 0) >= 0 {
		g.nul++
	}
}

// values is the number of non-empty values.
func (g *fieldGuess) values() int {
	n := 0
	for _, c := range g.counts {
		n += c
	}
	return n
}

// typeOf is the narrowest type that holds values of the given classes
// (counts > 0), with g's scale and time zones.
func (g *fieldGuess) typeOf(counts [numValueClasses]int) ColType {
	has := func(cs ...valueClass) bool { return onlyClasses(counts, cs...) }
	found := false
	for _, c := range counts {
		found = found || c > 0
	}
	switch {
	case !found:
		return typeString
	case counts[classBinary] > 0:
		return typeBinary
	case has(classBool):
		return typeBool
	case has(classInt):
		return typeInt64
	case has(classInt, classDecimal):
		return decimalType(38, int32(g.scale))
	case has(classInt, classDecimal, classFloat, classSpecial):
		return typeFloat64
	case has(classDate):
		return typeDate
	case has(classDate, classTimestamp):
		tz := ""
		if g.zoned > 0 {
			tz = "UTC"
		}
		return fracType(KindTimestamp, max(6, g.fracDigits), tz)
	}
	return typeString
}

// onlyClasses reports whether every value counted is of one of the classes.
func onlyClasses(counts [numValueClasses]int, cs ...valueClass) bool {
	for c := valueClass(0); c < numValueClasses; c++ {
		if counts[c] > 0 && !slices.Contains(cs, c) {
			return false
		}
	}
	return true
}

// inPlaceType is the narrowest type whose stored text the driver reads
// (decodeStored) as the values are, and that RediSearch indexes: integers,
// fixed-point and other finite numbers in every HASH that has the field,
// otherwise VARCHAR (or VARBINARY). An empty value or a NaN in a NUMERIC
// attribute makes RediSearch leave the HASH out of the index, and booleans,
// dates and times are stored as numbers.
func (g *fieldGuess) inPlaceType() ColType {
	c := g.counts
	switch {
	case c[classBinary] > 0:
		return typeBinary
	case g.values() == 0 || g.empty > 0:
		return typeString
	case onlyClasses(c, classInt):
		return typeInt64
	case onlyClasses(c, classInt, classDecimal):
		return decimalType(38, int32(g.scale))
	case onlyClasses(c, classInt, classDecimal, classFloat):
		return typeFloat64
	}
	return typeString
}

// storedFits checks that the driver reads a stored value as a value of
// type t exactly, as it is (decodeStored would also read "1.5" as the
// integer 1, and "infinity" as NaN).
func storedFits(v string, t ColType) error {
	switch t.Kind {
	case KindString, KindBinary:
		return nil
	case KindBool:
		if v == "0" || v == "1" {
			return nil
		}
		return fmt.Errorf("the driver stores booleans as 0 and 1")
	case KindInt16, KindInt32, KindInt64, KindDate, KindTime, KindTimestamp:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			what := "the driver stores them as integers"
			switch t.Kind {
			case KindDate:
				what = "the driver stores dates as days since 1970-01-01"
			case KindTime, KindTimestamp:
				what = "the driver stores them as integers in the type's unit"
			}
			return fmt.Errorf("isn't an integer: %s", what)
		}
		if t.Kind.isInteger() {
			if lo, hi := intRange(t.Kind); i < lo || i > hi {
				return fmt.Errorf("is out of range for %s", t.SQLName())
			}
		}
		return nil
	case KindFloat32, KindFloat64:
		switch classify(v).class {
		case classInt, classDecimal, classFloat:
			return nil
		}
		return fmt.Errorf("isn't a finite number")
	case KindDecimal:
		switch classify(v).class {
		case classInt, classDecimal:
			d, scale, err := parseDecimal(v)
			if err == nil && (scale > t.Scale || decimalDigits(rescaleDecimal(d, scale, t.Scale)) > t.Precision) {
				err = fmt.Errorf("doesn't fit %s", t.SQLName())
			}
			return err
		}
		return fmt.Errorf("isn't a fixed-point number")
	case KindInterval:
		_, err := decodeInterval(v)
		return err
	}
	return fmt.Errorf("can't be stored as %s", t.SQLName())
}

// guessedColumn is a column of the guessed table.
type guessedColumn struct {
	name  string
	field string // "" for the key column
	t     ColType
	guess *fieldGuess
	// override is the -type text the column's type came from.
	override string
	// inPlace is the column's type when the HASHes are adopted in place
	// (inPlaceType, or -type's).
	inPlace ColType
	// suggest is the type most values fit when a few didn't (and made the
	// column VARCHAR); outliers counts those few.
	suggest     ColType
	outliers    int
	outlierNote string
	notes       []string
}

// guessOptions are the choices that shape a guessed table.
type guessOptions struct {
	keyColumn string            // column for the key; "" for none
	types     map[string]string // column → SQL type text
	renames   map[string]string // field → column
}

// guesser collects what the values of HASHes' fields look like, one HASH
// at a time.
type guesser struct {
	byField map[string]*fieldGuess
	order   []string
	rows    int
	// fits are in-place types to check every value against, by field.
	fits map[string]ColType
}

func newGuesser() *guesser { return &guesser{byField: map[string]*fieldGuess{}} }

// newGuesserFor is a guesser that checks every value of a column with a
// -type against that type, as stored in place.
func newGuesserFor(o guessOptions) (*guesser, error) {
	gs := newGuesser()
	types, err := o.overrideTypes()
	if err != nil {
		return nil, err
	}
	if len(types) > 0 {
		gs.fits = map[string]ColType{}
		for name, t := range types {
			gs.fits[o.fieldOf(name)] = t
		}
	}
	return gs, nil
}

func (gs *guesser) add(r hashRow) {
	gs.rows++
	for i, f := range r.fields {
		g := gs.byField[f]
		if g == nil {
			g = &fieldGuess{field: f}
			if t, ok := gs.fits[f]; ok {
				g.fits = &t
			}
			gs.byField[f] = g
			gs.order = append(gs.order, f)
		}
		g.add(r.key, r.values[i])
	}
}

// guessTable guesses a table's columns from sampled HASHes. Fields that
// can't be columns are reported in skipped.
func guessTable(rows []hashRow, o guessOptions) (cols []*guessedColumn, skipped []string, err error) {
	gs := newGuesser()
	for _, r := range rows {
		gs.add(r)
	}
	return gs.columns(o)
}

// columnName is the column a field becomes.
func (o guessOptions) columnName(field string) string {
	if r, ok := o.renames[field]; ok {
		return r
	}
	return field
}

// fieldOf is the field a column reads (the column's name, unless a field
// was renamed to it).
func (o guessOptions) fieldOf(column string) string {
	for f, c := range o.renames {
		if strings.EqualFold(c, column) {
			return f
		}
	}
	return column
}

// overrideTypes parses the -type overrides, by column.
func (o guessOptions) overrideTypes() (map[string]ColType, error) {
	out := map[string]ColType{}
	for name, text := range o.types {
		t, err := parseTypeText(text)
		if err != nil {
			return nil, errorf(adbc.StatusInvalidArgument, "-type %s=%s: %v", name, text, err)
		}
		out[name] = t
	}
	return out, nil
}

// columns are the guessed table's columns.
func (gs *guesser) columns(o guessOptions) (cols []*guessedColumn, skipped []string, err error) {
	for f := range o.renames {
		if gs.byField[f] == nil {
			return nil, nil, errorf(adbc.StatusInvalidArgument, "-rename %s: no sampled HASH has the field %q", f, f)
		}
	}
	if o.keyColumn != "" {
		for _, f := range gs.order {
			if strings.EqualFold(o.columnName(f), o.keyColumn) {
				return nil, nil, errorf(adbc.StatusInvalidArgument,
					"the HASHes have a field %q, so the key can't be column %q: name the key's column with -key-column (empty for none), or rename the field with -rename",
					f, o.keyColumn)
			}
		}
		cols = append(cols, &guessedColumn{name: o.keyColumn, t: typeString, inPlace: typeString,
			notes: []string{"each HASH's key"}})
	}
	for _, f := range gs.order {
		g := gs.byField[f]
		name := o.columnName(f)
		switch {
		case f == rowIDField:
			skipped = append(skipped, fmt.Sprintf("%s: the driver's row id field, not a column", f))
			continue
		case name == "":
			skipped = append(skipped, `"": a field with an empty name (give it one with -rename)`)
			continue
		case strings.HasPrefix(name, "__"):
			skipped = append(skipped, fmt.Sprintf("%s: names starting with __ are reserved (-rename %s=<name> keeps it)", f, f))
			continue
		}
		c := &guessedColumn{name: name, field: f, guess: g, t: g.typeOf(g.counts), inPlace: g.inPlaceType()}
		c.describe()
		cols = append(cols, c)
	}
	seen := map[string]string{}
	for _, c := range cols {
		low := strings.ToLower(c.name)
		if other, ok := seen[low]; ok {
			return nil, nil, errorf(adbc.StatusInvalidArgument,
				"columns %q and %q differ only in case; rename one with -rename %s=<name>", other, c.name, c.fieldOrName())
		}
		seen[low] = c.name
	}
	types, err := o.overrideTypes()
	if err != nil {
		return nil, nil, err
	}
	for name, t := range types {
		var col *guessedColumn
		for _, c := range cols {
			if strings.EqualFold(c.name, name) {
				col = c
			}
		}
		if col == nil {
			return nil, nil, errorf(adbc.StatusInvalidArgument, "-type %s: there is no column %q", name, name)
		}
		col.t, col.inPlace, col.override = t, t, o.types[name]
	}
	return cols, skipped, nil
}

func (c *guessedColumn) fieldOrName() string {
	if c.field != "" {
		return c.field
	}
	return c.name
}

// describe notes what the sampled values say about the guessed type.
func (c *guessedColumn) describe() {
	g := c.guess
	n := g.values()
	if c.t.Kind == KindString && g.counts[classText] > 0 && g.leadZero == 0 {
		// A few values (at most 5%) that aren't of the type the others fit.
		rest := g.counts
		rest[classText] = 0
		if s := g.typeOf(rest); s.Kind != KindString && s.Kind != KindBinary &&
			g.counts[classText]*20 <= n {
			c.suggest, c.outliers = s, g.counts[classText]
			ex := g.examples[classText]
			c.outlierNote = fmt.Sprintf("%d of %d values aren't %s (such as %s in %s)",
				c.outliers, n, s.SQLName(), quoteValue(ex.value), ex.key)
			c.notes = append(c.notes, c.outlierNote)
		}
	}
	switch {
	case c.t.Kind == KindInt64 && g.zeroOne && g.zero && g.one:
		c.notes = append(c.notes, "only 0 and 1")
	case c.t.Kind == KindBool:
		c.notes = append(c.notes, "true/false text")
	case c.t.Kind == KindTimestamp && g.counts[classTimestamp] > 0:
		if g.zoned > 0 && g.unzoned > 0 {
			c.notes = append(c.notes, fmt.Sprintf("%d values have no UTC offset and are read as UTC", g.unzoned))
		} else {
			c.notes = append(c.notes, "ISO 8601 text")
		}
	case c.t.Kind == KindDate:
		c.notes = append(c.notes, "YYYY-MM-DD text")
	case c.t.Kind == KindBinary:
		c.notes = append(c.notes, "not UTF-8")
	case c.t.Kind == KindFloat64 && g.counts[classSpecial] > 0:
		c.notes = append(c.notes, "has NaN or infinite values")
	}
	if g.leadZero > 0 && c.t.Kind == KindString {
		c.notes = append(c.notes, "numbers with leading zeros, kept as text")
	}
	if g.json > 0 && g.json == g.counts[classText] {
		c.notes = append(c.notes, "JSON text")
	}
	if g.empty > 0 && c.t.Kind != KindString && c.t.Kind != KindBinary {
		c.notes = append(c.notes, fmt.Sprintf("%d empty values read as NULL", g.empty))
	}
	if g.nul > 0 {
		c.notes = append(c.notes, fmt.Sprintf("%d values have a NUL byte", g.nul))
	}
}

// quoteValue renders a sampled value for a message, shortened.
func quoteValue(v string) string {
	if len(v) > 40 {
		v = v[:37] + "..."
	}
	return strconv.Quote(v)
}

// convertValue reads a HASH value as a value of column type t: empty text
// is NULL in a column that isn't a string.
func convertValue(v string, t ColType) (Value, error) {
	switch t.Kind {
	case KindString:
		if t.Fixed || t.Length > 0 {
			return Coerce(stringValue(v), t)
		}
		return stringValue(v), nil
	case KindBinary:
		return binaryValue(v), nil
	}
	if v == "" {
		return nullValue(t), nil
	}
	return parseString(v, t)
}
