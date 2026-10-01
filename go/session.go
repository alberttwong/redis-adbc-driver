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

// Transaction control and session settings.
//
// The driver is autocommit only: every statement commits on its own (see
// the README). SQL written for Postgres still sends transaction control and
// SET, so they are accepted as Postgres accepts them in autocommit mode:
//
//   - BEGIN [WORK | TRANSACTION] [modes], START TRANSACTION [modes], COMMIT
//     [WORK | TRANSACTION] [AND [NO] CHAIN], END, ROLLBACK and ABORT do
//     nothing to the data: the statements before a ROLLBACK have already
//     committed. Postgres warns where there is nothing to commit or roll
//     back; ADBC has no warnings, so nothing is reported. The connection
//     only remembers whether a BEGIN is open, for SET LOCAL.
//   - SAVEPOINT, RELEASE and ROLLBACK TO are errors when the script is
//     parsed, so none of its statements run.
//   - SET [SESSION | LOCAL] name {= | TO} value | DEFAULT, RESET and SHOW
//     work on the parameters in the settings table below; other names are
//     errors (unrecognized configuration parameter "x"). search_path sets
//     the connection's current schema; the others mean nothing to the
//     driver, or have one value it supports, and are accepted, checked and
//     shown.
//   - SET LOCAL lasts until the transaction ends, as in Postgres: COMMIT or
//     ROLLBACK after a BEGIN, or else the end of the script it is in (a
//     script of several statements is Postgres's implicit transaction). A
//     SET LOCAL on its own, outside BEGIN, is checked and has no effect.
//     SET (SESSION) lasts until the connection closes; ROLLBACK doesn't undo
//     it.

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// ---- statements ----

type TxnKind int

const (
	TxnBegin TxnKind = iota + 1
	TxnCommit
	TxnRollback
)

// TransactionStmt is BEGIN / START TRANSACTION, COMMIT / END or ROLLBACK /
// ABORT.
type TransactionStmt struct {
	Kind TxnKind
	// Word is the statement's keyword, upper case, for messages.
	Word string
	// Chain is set for COMMIT / ROLLBACK AND CHAIN.
	Chain bool
}

func (*TransactionStmt) stmtNode() {}

// SetStmt is SET, SET LOCAL and RESET of a parameter, or RESET ALL. SET
// TRANSACTION and SET SESSION CHARACTERISTICS AS TRANSACTION are Ignored.
type SetStmt struct {
	// Name is the parameter, lower case ("" for RESET ALL).
	Name  string
	Local bool
	// Default is set for … TO DEFAULT and RESET; otherwise Values holds the
	// values given.
	Default bool
	Values  []SetValue
	All     bool
	Ignored bool
}

func (*SetStmt) stmtNode() {}

// SetValue is one value of a SET.
type SetValue struct {
	Text string
	Kind SetValueKind
}

type SetValueKind int

const (
	SetWord   SetValueKind = iota + 1 // an identifier or keyword: ON, UTC, public
	SetQuoted                         // a double-quoted identifier
	SetString                         // a string literal
	SetNumber                         // a number, with its sign
	SetOther                          // anything else Postgres takes (INTERVAL '…'), as written
)

// ShowStmt is SHOW name or SHOW ALL.
type ShowStmt struct {
	Name string // lower case; "" for ALL
	All  bool
}

func (*ShowStmt) stmtNode() {}

// ---- parsing ----

// autocommitOnly is the error for savepoints and two-phase commit.
func autocommitOnly(what string) error {
	return &sqlError{msg: what + " is not supported (autocommit only)"}
}

// parseTransaction parses BEGIN, START TRANSACTION, COMMIT, END, ROLLBACK
// and ABORT, and rejects the savepoint forms.
func (p *parser) parseTransaction() (Stmt, error) {
	word := strings.ToUpper(p.next().text)
	st := &TransactionStmt{Word: word}
	switch word {
	case "BEGIN", "START":
		if word == "START" {
			if err := p.expectKeyword("TRANSACTION"); err != nil {
				return nil, err
			}
		} else if !p.acceptKeyword("WORK") {
			p.acceptKeyword("TRANSACTION")
		}
		st.Kind = TxnBegin
		return st, p.parseTransactionModes()
	case "COMMIT", "END":
		st.Kind = TxnCommit
	default: // ROLLBACK, ABORT
		st.Kind = TxnRollback
	}
	if word == "COMMIT" || word == "ROLLBACK" {
		if p.isKeyword("PREPARED") {
			return nil, autocommitOnly(word + " PREPARED")
		}
	}
	if !p.acceptKeyword("WORK") {
		p.acceptKeyword("TRANSACTION")
	}
	if word == "ROLLBACK" && p.isKeyword("TO") {
		return nil, autocommitOnly("ROLLBACK TO SAVEPOINT")
	}
	if p.acceptKeyword("AND") {
		no := p.acceptKeyword("NO")
		if err := p.expectKeyword("CHAIN"); err != nil {
			return nil, err
		}
		st.Chain = !no
	}
	return st, nil
}

// parseTransactionModes parses Postgres's transaction modes, separated by
// commas or spaces: ISOLATION LEVEL …, READ WRITE, READ ONLY, [NOT]
// DEFERRABLE. They mean nothing here.
func (p *parser) parseTransactionModes() error {
	for n := 0; ; n++ {
		comma := n > 0 && p.acceptOp(",")
		switch {
		case p.acceptKeyword("ISOLATION", "LEVEL"):
			if !p.acceptKeyword("SERIALIZABLE") && !p.acceptKeyword("REPEATABLE", "READ") &&
				!p.acceptKeyword("READ", "COMMITTED") && !p.acceptKeyword("READ", "UNCOMMITTED") {
				return syntaxErr("expected an isolation level near %q", p.peek().text)
			}
		case p.acceptKeyword("READ", "WRITE"), p.acceptKeyword("READ", "ONLY"),
			p.acceptKeyword("DEFERRABLE"), p.acceptKeyword("NOT", "DEFERRABLE"):
		default:
			if comma {
				return syntaxErr("expected a transaction mode near %q", p.peek().text)
			}
			return nil
		}
	}
}

// parseSettingName parses a parameter name and returns it in lower case.
// A qualified name (Postgres's custom parameters) is kept whole, and is
// unrecognized.
func (p *parser) parseSettingName() (string, error) {
	parts, err := p.parseDottedName()
	if err != nil {
		return "", err
	}
	return strings.ToLower(strings.Join(parts, ".")), nil
}

// parseSet parses SET [SESSION | LOCAL] name {= | TO} {value [, …] |
// DEFAULT}, SET … TIME ZONE {value | LOCAL | DEFAULT}, SET … SCHEMA 'value',
// SET … NAMES ['value' | DEFAULT], SET TRANSACTION modes and SET SESSION
// CHARACTERISTICS AS TRANSACTION modes.
func (p *parser) parseSet() (Stmt, error) {
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	if p.acceptKeyword("SESSION", "CHARACTERISTICS") {
		if err := p.expectKeyword("AS", "TRANSACTION"); err != nil {
			return nil, err
		}
		return &SetStmt{Ignored: true}, p.parseTransactionModes()
	}
	if p.acceptKeyword("TRANSACTION") {
		if p.isKeyword("SNAPSHOT") {
			return nil, autocommitOnly("SET TRANSACTION SNAPSHOT")
		}
		return &SetStmt{Ignored: true}, p.parseTransactionModes()
	}
	st := &SetStmt{}
	if p.acceptKeyword("LOCAL") {
		st.Local = true
	} else {
		p.acceptKeyword("SESSION")
	}
	switch {
	case p.acceptKeyword("TIME", "ZONE"):
		st.Name = "timezone"
		switch {
		case p.acceptKeyword("LOCAL"), p.acceptKeyword("DEFAULT"):
			st.Default = true
		case p.isKeyword("INTERVAL"):
			// INTERVAL '-08:00' [HOUR TO MINUTE]: an offset, not UTC.
			start := p.peek().pos
			for p.peek().kind != tokEOF && !p.isOp(";") {
				p.pos++
			}
			st.Values = []SetValue{{Text: strings.TrimSpace(p.src[start:p.peek().pos]), Kind: SetOther}}
		default:
			v, err := p.parseSetValue()
			if err != nil {
				return nil, err
			}
			st.Values = []SetValue{v}
		}
		return st, nil
	case p.acceptKeyword("SCHEMA"):
		st.Name = "search_path"
		t := p.peek()
		if t.kind != tokString {
			return nil, syntaxErr("expected a string after SET SCHEMA near %q", t.text)
		}
		p.pos++
		st.Values = []SetValue{{Text: t.text, Kind: SetString}}
		return st, nil
	case p.acceptKeyword("NAMES"):
		st.Name = "client_encoding"
		if p.acceptKeyword("DEFAULT") || p.isOp(";") || p.peek().kind == tokEOF {
			st.Default = true
			return st, nil
		}
		v, err := p.parseSetValue()
		if err != nil {
			return nil, err
		}
		st.Values = []SetValue{v}
		return st, nil
	}
	name, err := p.parseSettingName()
	if err != nil {
		return nil, err
	}
	st.Name = name
	if !p.acceptOp("=") && !p.acceptKeyword("TO") {
		return nil, syntaxErr("expected = or TO after SET %s near %q", name, p.peek().text)
	}
	if p.acceptKeyword("DEFAULT") {
		st.Default = true
		return st, nil
	}
	for {
		v, err := p.parseSetValue()
		if err != nil {
			return nil, err
		}
		st.Values = append(st.Values, v)
		if !p.acceptOp(",") {
			return st, nil
		}
	}
}

// parseSetValue parses one value of a SET: a word (an identifier or a
// keyword such as ON), a quoted identifier, a string or a signed number.
func (p *parser) parseSetValue() (SetValue, error) {
	t := p.peek()
	switch t.kind {
	case tokIdent:
		p.pos++
		return SetValue{Text: t.text, Kind: SetWord}, nil
	case tokQuotedIdent:
		p.pos++
		return SetValue{Text: t.text, Kind: SetQuoted}, nil
	case tokString:
		p.pos++
		return SetValue{Text: t.text, Kind: SetString}, nil
	case tokNumber:
		p.pos++
		return SetValue{Text: t.text, Kind: SetNumber}, nil
	case tokOp:
		if (t.text == "-" || t.text == "+") && p.peekAt(1).kind == tokNumber {
			p.pos += 2
			text := p.toks[p.pos-1].text
			if t.text == "-" {
				text = "-" + text
			}
			return SetValue{Text: text, Kind: SetNumber}, nil
		}
	}
	return SetValue{}, syntaxErr("expected a value near %q", t.text)
}

// parseReset parses RESET name, RESET TIME ZONE and RESET ALL.
func (p *parser) parseReset() (Stmt, error) {
	if err := p.expectKeyword("RESET"); err != nil {
		return nil, err
	}
	switch {
	case p.acceptKeyword("ALL"):
		return &SetStmt{All: true, Default: true}, nil
	case p.acceptKeyword("TIME", "ZONE"):
		return &SetStmt{Name: "timezone", Default: true}, nil
	}
	name, err := p.parseSettingName()
	if err != nil {
		return nil, err
	}
	return &SetStmt{Name: name, Default: true}, nil
}

// parseShow parses SHOW name, SHOW TIME ZONE and SHOW ALL.
func (p *parser) parseShow() (Stmt, error) {
	if err := p.expectKeyword("SHOW"); err != nil {
		return nil, err
	}
	switch {
	case p.acceptKeyword("ALL"):
		return &ShowStmt{All: true}, nil
	case p.acceptKeyword("TIME", "ZONE"):
		return &ShowStmt{Name: "timezone"}, nil
	}
	name, err := p.parseSettingName()
	if err != nil {
		return nil, err
	}
	return &ShowStmt{Name: name}, nil
}

// ---- the session ----

// paramValue is a parameter's value: its text, as SHOW shows it, and for
// search_path the schema it selects.
type paramValue struct {
	text   string
	schema string
}

// session is a connection's settings.
type session struct {
	// defaultSchema is adbc.redis.default_schema, which RESET search_path
	// goes back to.
	defaultSchema string
	// vals are the values set for the session (by SET, or the current
	// schema by SetCurrentDbSchema); local those set by SET LOCAL, until
	// the transaction ends. Both by lower-case name.
	vals  map[string]paramValue
	local map[string]paramValue
	// inBlock is set between BEGIN and COMMIT / ROLLBACK.
	inBlock bool
}

func newSession(schema string) *session {
	return &session{defaultSchema: schema, vals: map[string]paramValue{}}
}

// value returns a parameter's current value.
func (s *session) value(name string) paramValue {
	if v, ok := s.local[name]; ok {
		return v
	}
	if v, ok := s.vals[name]; ok {
		return v
	}
	return settings[name].initial(s)
}

// currentSchema is the schema unqualified names use.
func (s *session) currentSchema() string { return s.value("search_path").schema }

// setSchema makes schema the current schema for the session (the ADBC
// current schema).
func (s *session) setSchema(schema string) {
	s.vals["search_path"] = paramValue{text: quoteSchemaName(schema), schema: schema}
	delete(s.local, "search_path")
}

// endTransaction drops what SET LOCAL set.
func (s *session) endTransaction() { s.local = nil }

// endScript runs when a script has run: outside BEGIN, the script was the
// transaction.
func (s *session) endScript() {
	if !s.inBlock {
		s.endTransaction()
	}
}

// ---- running the statements ----

func (e *executor) runTransaction(st *TransactionStmt) error {
	s := e.sess
	switch st.Kind {
	case TxnBegin:
		// Postgres warns if a BEGIN is already open.
		s.inBlock = true
	default:
		if st.Chain && !s.inBlock {
			return errorf(adbc.StatusInvalidState, "%s AND CHAIN can only be used in transaction blocks", st.Word)
		}
		// Nothing to commit or undo: every statement has committed. Postgres
		// warns when no BEGIN is open.
		s.endTransaction()
		s.inBlock = st.Chain
	}
	e.schema = s.currentSchema()
	return nil
}

func unrecognizedParam(name string) error {
	return errorf(adbc.StatusInvalidArgument, "unrecognized configuration parameter %q", name)
}

func (e *executor) runSet(st *SetStmt) error {
	if st.Ignored {
		return nil
	}
	s := e.sess
	if st.All {
		s.vals, s.local = map[string]paramValue{}, nil
		e.schema = s.currentSchema()
		return nil
	}
	def, ok := settings[st.Name]
	if !ok {
		return unrecognizedParam(st.Name)
	}
	var v paramValue
	if st.Default {
		v = def.initial(s)
	} else {
		var err error
		if v, err = def.parse(st.Values, s.value(st.Name)); err != nil {
			return err
		}
	}
	switch {
	case st.Local && !s.inBlock && !e.script:
		// Outside a transaction, SET LOCAL has no effect (Postgres warns).
	case st.Local:
		if s.local == nil {
			s.local = map[string]paramValue{}
		}
		s.local[st.Name] = v
	default:
		s.vals[st.Name] = v
		delete(s.local, st.Name)
	}
	e.schema = s.currentSchema()
	return nil
}

// showColumns are the result columns of a SHOW.
func showColumns(st *ShowStmt) ([]resultColumn, error) {
	if st.All {
		return []resultColumn{{Name: "name", Type: typeString}, {Name: "setting", Type: typeString}, {Name: "description", Type: typeString}}, nil
	}
	def, ok := settings[st.Name]
	if !ok {
		return nil, unrecognizedParam(st.Name)
	}
	return []resultColumn{{Name: def.name, Type: typeString}}, nil
}

func (e *executor) runShow(st *ShowStmt) (execResult, error) {
	cols, err := showColumns(st)
	if err != nil {
		return execResult{}, err
	}
	var rows [][]Value
	if st.All {
		names := make([]string, 0, len(settings))
		for n := range settings {
			names = append(names, n)
		}
		slices.Sort(names)
		for _, n := range names {
			def := settings[n]
			rows = append(rows, []Value{stringValue(def.name), stringValue(e.sess.value(n).text), stringValue(def.desc)})
		}
	} else {
		rows = [][]Value{{stringValue(e.sess.value(st.Name).text)}}
	}
	return execResult{isQuery: true, cols: cols, rows: rows, affected: int64(len(rows))}, nil
}

// ---- the parameters ----

// setting is a parameter SET, RESET and SHOW accept.
type setting struct {
	name string // as Postgres spells it: SHOW's column name
	desc string // SHOW ALL's description
	// initial is the value before any SET; parse checks the values of a
	// SET and returns the new value (cur is the current one).
	initial func(s *session) paramValue
	parse   func(vals []SetValue, cur paramValue) (paramValue, error)
}

func fixed(text string) func(*session) paramValue {
	return func(*session) paramValue { return paramValue{text: text} }
}

// settings are the parameters, by lower-case name.
var settings = map[string]*setting{
	"search_path": {
		name: "search_path",
		desc: "Sets the schema for unqualified names: the first schema of the list, " +
			`skipping "$user", pg_catalog and pg_temp (adbc.redis.default_schema by default).`,
		initial: func(s *session) paramValue {
			return paramValue{text: quoteSchemaName(s.defaultSchema), schema: s.defaultSchema}
		},
		parse: parseSearchPath,
	},
	"timezone": {
		name:    "TimeZone",
		desc:    "Time zone for displaying and interpreting time stamps. Only UTC is supported.",
		initial: fixed("UTC"),
		parse: func(vals []SetValue, _ paramValue) (paramValue, error) {
			v, err := oneValue("TimeZone", vals)
			if err != nil {
				return paramValue{}, err
			}
			if v.Kind == SetNumber || v.Kind == SetOther || !strings.EqualFold(v.Text, "UTC") {
				return paramValue{}, onlyValue("TimeZone", v.Text, "UTC")
			}
			return paramValue{text: "UTC"}, nil
		},
	},
	"client_encoding": {
		name:    "client_encoding",
		desc:    "Client's character set encoding. Only UTF8 is supported.",
		initial: fixed("UTF8"),
		parse: func(vals []SetValue, _ paramValue) (paramValue, error) {
			v, err := oneValue("client_encoding", vals)
			if err != nil {
				return paramValue{}, err
			}
			switch strings.ToUpper(v.Text) {
			case "UTF8", "UTF-8", "UNICODE":
				return paramValue{text: "UTF8"}, nil
			}
			return paramValue{}, onlyValue("client_encoding", v.Text, "UTF8")
		},
	},
	"application_name": {
		name:    "application_name",
		desc:    "Name of the application. Accepted and shown; the driver doesn't use it.",
		initial: fixed(""),
		parse: func(vals []SetValue, _ paramValue) (paramValue, error) {
			v, err := oneValue("application_name", vals)
			return paramValue{text: v.Text}, err
		},
	},
	"standard_conforming_strings": {
		name:    "standard_conforming_strings",
		desc:    "'...' strings treat backslashes literally. Only on is supported.",
		initial: fixed("on"),
		parse: func(vals []SetValue, _ paramValue) (paramValue, error) {
			v, err := oneValue("standard_conforming_strings", vals)
			if err != nil {
				return paramValue{}, err
			}
			b, ok := parseBoolSetting(v.Text)
			if !ok {
				return paramValue{}, errorf(adbc.StatusInvalidArgument, "parameter %q requires a Boolean value", "standard_conforming_strings")
			}
			if !b {
				return paramValue{}, onlyValue("standard_conforming_strings", v.Text, "on")
			}
			return paramValue{text: "on"}, nil
		},
	},
	"statement_timeout":                   timeoutSetting("statement_timeout", "Maximum allowed duration of any statement. Accepted and shown, not enforced."),
	"lock_timeout":                        timeoutSetting("lock_timeout", "Maximum allowed duration of any wait for a lock. Accepted and shown; the driver takes no locks."),
	"idle_in_transaction_session_timeout": timeoutSetting("idle_in_transaction_session_timeout", "Maximum allowed idle time in a transaction. Accepted and shown; there are no transactions."),
	"extra_float_digits": {
		name:    "extra_float_digits",
		desc:    "Extra digits for floating-point values shown as text. Accepted and shown; results are Arrow values.",
		initial: fixed("1"),
		parse: func(vals []SetValue, _ paramValue) (paramValue, error) {
			v, err := oneValue("extra_float_digits", vals)
			if err != nil {
				return paramValue{}, err
			}
			n, err := strconv.Atoi(strings.TrimSpace(v.Text))
			if err != nil {
				return paramValue{}, invalidValue("extra_float_digits", v.Text)
			}
			if n < -15 || n > 3 {
				return paramValue{}, errorf(adbc.StatusInvalidArgument, `%d is outside the valid range for parameter "extra_float_digits" (-15 .. 3)`, n)
			}
			return paramValue{text: strconv.Itoa(n)}, nil
		},
	},
	"datestyle": {
		name:    "DateStyle",
		desc:    "Display format for date and time values. Only ISO is supported; the order (MDY, DMY, YMD) has no effect, as only ISO dates are read.",
		initial: fixed("ISO, MDY"),
		parse:   parseDateStyle,
	},
	"intervalstyle": {
		name:    "IntervalStyle",
		desc:    "Display format for interval values. Only postgres is supported.",
		initial: fixed("postgres"),
		parse: func(vals []SetValue, _ paramValue) (paramValue, error) {
			v, err := oneValue("IntervalStyle", vals)
			if err != nil {
				return paramValue{}, err
			}
			switch strings.ToLower(v.Text) {
			case "postgres":
				return paramValue{text: "postgres"}, nil
			case "postgres_verbose", "sql_standard", "iso_8601":
				return paramValue{}, onlyValue("IntervalStyle", v.Text, "postgres")
			}
			return paramValue{}, invalidValue("IntervalStyle", v.Text)
		},
	},
}

func invalidValue(name, text string) error {
	return errorf(adbc.StatusInvalidArgument, "invalid value for parameter %q: %q", name, text)
}

// onlyValue is the error for a value Postgres accepts that the driver
// doesn't.
func onlyValue(name, text, only string) error {
	return errorf(adbc.StatusNotImplemented, "invalid value for parameter %q: %q (only %s is supported)", name, text, only)
}

// oneValue returns the value of a parameter that takes one.
func oneValue(name string, vals []SetValue) (SetValue, error) {
	if len(vals) != 1 {
		return SetValue{}, errorf(adbc.StatusInvalidArgument, "SET %s takes only one argument", name)
	}
	return vals[0], nil
}

// parseBoolSetting parses a Boolean value as Postgres does.
func parseBoolSetting(s string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "on", "true", "yes", "1", "t", "y":
		return true, true
	case "off", "false", "no", "0", "f", "n", "of":
		return false, true
	}
	return false, false
}

// timeoutSetting is a parameter in milliseconds, as Postgres's timeouts.
func timeoutSetting(name, desc string) *setting {
	return &setting{
		name:    name,
		desc:    desc,
		initial: fixed("0"),
		parse: func(vals []SetValue, _ paramValue) (paramValue, error) {
			v, err := oneValue(name, vals)
			if err != nil {
				return paramValue{}, err
			}
			ms, ok := parseMillis(v.Text)
			if !ok {
				return paramValue{}, errorf(adbc.StatusInvalidArgument,
					`invalid value for parameter %q: %q (valid units are "us", "ms", "s", "min", "h" and "d")`, name, v.Text)
			}
			if ms < 0 || ms > math.MaxInt32 {
				return paramValue{}, errorf(adbc.StatusInvalidArgument,
					"%.0f ms is outside the valid range for parameter %q (0 .. %d)", ms, name, math.MaxInt32)
			}
			return paramValue{text: formatMillis(int64(ms))}, nil
		},
	}
}

var millisUnits = map[string]float64{"us": 0.001, "ms": 1, "s": 1000, "min": 60_000, "h": 3_600_000, "d": 86_400_000}

// parseMillis parses a number of milliseconds, or a number with a unit (5s,
// 1.5 min), rounded to the millisecond as Postgres does.
func parseMillis(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.' || s[i] == '-' || s[i] == '+') {
		i++
	}
	n, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, false
	}
	factor := 1.0
	if unit := strings.TrimSpace(s[i:]); unit != "" {
		f, ok := millisUnits[unit]
		if !ok {
			return 0, false
		}
		factor = f
	}
	return math.Round(n * factor), true
}

// formatMillis shows milliseconds in the largest unit that holds them
// exactly, as Postgres does: 5000 is 5s, 1500 is 1500ms.
func formatMillis(ms int64) string {
	if ms == 0 {
		return "0"
	}
	for _, u := range []struct {
		name string
		ms   int64
	}{{"d", 86_400_000}, {"h", 3_600_000}, {"min", 60_000}, {"s", 1000}} {
		if ms%u.ms == 0 {
			return fmt.Sprintf("%d%s", ms/u.ms, u.name)
		}
	}
	return fmt.Sprintf("%dms", ms)
}

// parseDateStyle parses DateStyle: an output style (only ISO) and an order
// for reading ambiguous dates, in any order, in one or several values.
func parseDateStyle(vals []SetValue, cur paramValue) (paramValue, error) {
	order := "MDY"
	if _, o, ok := strings.Cut(cur.text, ", "); ok {
		order = o
	}
	for _, v := range vals {
		for _, part := range strings.FieldsFunc(v.Text, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
			switch strings.ToUpper(part) {
			case "ISO":
			case "SQL", "POSTGRES", "GERMAN":
				return paramValue{}, onlyValue("DateStyle", v.Text, "ISO")
			case "MDY", "US", "NONEURO", "NONEUROPEAN":
				order = "MDY"
			case "DMY", "EURO", "EUROPEAN":
				order = "DMY"
			case "YMD":
				order = "YMD"
			default:
				return paramValue{}, invalidValue("DateStyle", v.Text)
			}
		}
	}
	return paramValue{text: "ISO, " + order}, nil
}

// parseSearchPath parses search_path. Unqualified names use its first
// schema, skipping "$user" (Postgres's default list starts with it, and it
// rarely names a schema), pg_catalog (whose objects the driver doesn't have
// as tables) and pg_temp (temporary objects are always found first). A
// schema isn't checked to exist, as for adbc.redis.default_schema.
func parseSearchPath(vals []SetValue, _ paramValue) (paramValue, error) {
	names := make([]string, len(vals))
	schema := ""
	for i, v := range vals {
		if v.Kind == SetNumber {
			return paramValue{}, invalidValue("search_path", v.Text)
		}
		names[i] = quoteSchemaName(v.Text)
		switch {
		case schema != "", v.Text == "$user", strings.EqualFold(v.Text, "pg_catalog"), isTempAlias(v.Text), v.Text == "":
		case isTempSchema(v.Text):
			return paramValue{}, errorf(adbc.StatusInvalidArgument, "schema name %q is reserved for temporary tables and views", v.Text)
		default:
			schema = v.Text
		}
	}
	text := strings.Join(names, ", ")
	if schema == "" {
		return paramValue{}, errorf(adbc.StatusInvalidArgument,
			`search_path names no schema to use (%s; "$user", pg_catalog and pg_temp are skipped)`, text)
	}
	return paramValue{text: text, schema: schema}, nil
}

// quoteSchemaName writes a schema name as SHOW search_path shows it: as it
// is if it is a lower-case identifier, otherwise double-quoted.
func quoteSchemaName(name string) string {
	plain := name != "" && !(name[0] >= '0' && name[0] <= '9')
	for _, r := range name {
		plain = plain && (r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}
	if plain {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
