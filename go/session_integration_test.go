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

// Integration tests for transaction control, SET, RESET and SHOW (issue
// #92, session.go).

import (
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

// currentSchema is the connection's ADBC current schema.
func (h *sqlHarness) currentSchema() string {
	h.t.Helper()
	s, err := h.conn.(adbc.GetSetOptionsWithContext).GetOption(h.ctx, adbc.OptionKeyCurrentDbSchema)
	if err != nil {
		h.t.Fatal(err)
	}
	return s
}

// Transaction control is accepted and does nothing: every statement has
// committed on its own.
func TestSQLTransactionStatements(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_txn")
	h.exec(`CREATE TABLE it_txn (id integer)`)

	for _, sql := range []string{
		`BEGIN`, `begin work`, `BEGIN TRANSACTION`, `BEGIN ISOLATION LEVEL SERIALIZABLE`,
		`BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY`, `BEGIN READ WRITE NOT DEFERRABLE`,
		`BEGIN ISOLATION LEVEL REPEATABLE READ DEFERRABLE`,
		`START TRANSACTION`, `start transaction isolation level read uncommitted, read write`,
		`COMMIT`, `commit;`, `COMMIT WORK`, `COMMIT TRANSACTION`, `COMMIT AND NO CHAIN`,
		`END`, `END WORK`, `END TRANSACTION`, `end and no chain`,
		`ROLLBACK`, `ROLLBACK WORK`, `ROLLBACK TRANSACTION`, `ROLLBACK AND NO CHAIN`,
		`ABORT`, `ABORT WORK`, `abort transaction`,
		`SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`, `SET TRANSACTION READ ONLY, DEFERRABLE`,
		`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED`,
		// AND CHAIN in a block ends it and starts another.
		`BEGIN; COMMIT AND CHAIN; ROLLBACK AND CHAIN; COMMIT`,
	} {
		if n := h.exec(sql); n != -1 {
			t.Errorf("%s: %d rows affected", sql, n)
		}
	}

	for _, c := range []struct{ sql, want string }{
		{`COMMIT AND CHAIN`, `COMMIT AND CHAIN can only be used in transaction blocks`},
		{`END AND CHAIN`, `END AND CHAIN can only be used in transaction blocks`},
		{`ROLLBACK AND CHAIN`, `ROLLBACK AND CHAIN can only be used in transaction blocks`},
		{`SAVEPOINT s1`, `SAVEPOINT is not supported (autocommit only)`},
		{`RELEASE SAVEPOINT s1`, `RELEASE SAVEPOINT is not supported (autocommit only)`},
		{`RELEASE s1`, `RELEASE SAVEPOINT is not supported (autocommit only)`},
		{`ROLLBACK TO SAVEPOINT s1`, `ROLLBACK TO SAVEPOINT is not supported (autocommit only)`},
		{`ROLLBACK TO s1`, `ROLLBACK TO SAVEPOINT is not supported (autocommit only)`},
		{`ROLLBACK WORK TO s1`, `ROLLBACK TO SAVEPOINT is not supported (autocommit only)`},
		{`COMMIT PREPARED 'x'`, `COMMIT PREPARED is not supported (autocommit only)`},
		{`ROLLBACK PREPARED 'x'`, `ROLLBACK PREPARED is not supported (autocommit only)`},
		{`SET TRANSACTION SNAPSHOT '00000003-0000001B-1'`, `SET TRANSACTION SNAPSHOT is not supported (autocommit only)`},
		{`BEGIN ISOLATION LEVEL CHAOS`, `syntax error: expected an isolation level near "CHAOS"`},
		{`BEGIN READ ONLY,`, `syntax error: expected a transaction mode near ""`},
		{`BEGIN NOW`, `syntax error: unexpected "NOW"`},
		{`START`, `syntax error: expected TRANSACTION near ""`},
		{`COMMIT AND`, `syntax error: expected CHAIN near ""`},
	} {
		h.expectErrorText(c.sql, c.want)
	}
	// A script with a savepoint fails before any of its statements runs.
	h.expectErrorText(`INSERT INTO it_txn VALUES (1); SAVEPOINT a; INSERT INTO it_txn VALUES (2)`,
		`SAVEPOINT is not supported (autocommit only)`)
	h.expectRows(`SELECT COUNT(*) FROM it_txn`, "0")

	// dbt's run_hooks sends a literal commit; before the hooks outside the
	// transaction, and scripts mix COMMIT with other statements.
	h.exec(`commit;`)
	h.exec(`INSERT INTO it_txn VALUES (1); commit; INSERT INTO it_txn VALUES (2); COMMIT;`)
	h.expectRows(`SELECT COUNT(*) FROM it_txn`, "2")
	h.expectRows(`begin; insert into it_txn values (3); commit; select id from it_txn order by id`, "1", "2", "3")
	h.expectRows(`COMMIT; SELECT COUNT(*) FROM it_txn`, "3")

	// ROLLBACK undoes nothing: the statements before it have committed.
	h.exec(`BEGIN; INSERT INTO it_txn VALUES (4); ROLLBACK`)
	h.expectRows(`SELECT COUNT(*) FROM it_txn`, "4")
	h.exec(`BEGIN`)
	h.exec(`DELETE FROM it_txn WHERE id = 4`)
	h.exec(`ABORT`)
	h.expectRows(`SELECT id FROM it_txn ORDER BY id`, "1", "2", "3")
}

// SET search_path sets the connection's current schema, as
// adbc.redis.default_schema does, for that connection only.
func TestSQLSetSearchPath(t *testing.T) {
	h, h2 := newSQLHarness(t), newSQLHarness(t)
	h.exec(`CREATE SCHEMA IF NOT EXISTS it_sp_a`)
	h.exec(`CREATE SCHEMA IF NOT EXISTS it_sp_b`)
	h.dropTables("it_sp_a.it_sp_x", "it_sp_b.it_sp_x", "it_sp_x", "it_sp_a.it_sp_new", "it_sp_new")
	for _, s := range []string{"it_sp_a", "it_sp_b", "public"} {
		h.exec(`CREATE TABLE ` + s + `.it_sp_x (v varchar)`)
		h.exec(`INSERT INTO ` + s + `.it_sp_x VALUES ('` + s + `')`)
	}

	h.expectColumns(`SHOW search_path`, "search_path", "public")
	h.exec(`SET search_path = it_sp_a`)
	h.expectRows(`SELECT v FROM it_sp_x`, "it_sp_a")
	h.expectRows(`SHOW search_path`, "it_sp_a")
	if s := h.currentSchema(); s != "it_sp_a" {
		t.Errorf("current schema %q", s)
	}
	// Another connection keeps its own.
	h2.expectRows(`SELECT v FROM it_sp_x`, "public")
	h2.expectRows(`SHOW search_path`, "public")
	if s := h2.currentSchema(); s != "public" {
		t.Errorf("current schema of the other connection %q", s)
	}

	// CREATE and INSERT without a schema use it too.
	h.exec(`CREATE TABLE it_sp_new (k integer)`)
	h.exec(`INSERT INTO it_sp_new VALUES (1)`)
	h2.expectRows(`SELECT k FROM it_sp_a.it_sp_new`, "1")
	h2.expectErrorText(`SELECT k FROM it_sp_new`, `table "public"."it_sp_new" does not exist`)

	// The first schema is used; "$user", pg_catalog and pg_temp are skipped.
	h.exec(`SET search_path TO "$user", pg_catalog, pg_temp, it_sp_b, it_sp_a`)
	h.expectRows(`SHOW search_path`, `"$user", pg_catalog, pg_temp, it_sp_b, it_sp_a`)
	h.expectRows(`SELECT v FROM it_sp_x`, "it_sp_b")
	h.exec(`SET search_path = 'it_sp_a', public`)
	h.expectRows(`SELECT v FROM it_sp_x`, "it_sp_a")
	h.exec(`SET SCHEMA 'it_sp_b'`)
	h.expectRows(`SELECT v FROM it_sp_x`, "it_sp_b")
	h.exec(`SET SESSION search_path TO "Mixed Case"`)
	h.expectRows(`SHOW search_path`, `"Mixed Case"`)
	h.expectErrorText(`SELECT v FROM it_sp_x`, `table "Mixed Case"."it_sp_x" does not exist`)
	h.exec(`RESET search_path`)
	h.expectRows(`SELECT v FROM it_sp_x`, "public")
	h.exec(`SET search_path = it_sp_a`)
	h.exec(`SET search_path TO DEFAULT`)
	h.expectRows(`SHOW search_path`, "public")

	// Later statements of the same script see it.
	h.expectRows(`SET search_path = it_sp_b; SELECT v FROM it_sp_x`, "it_sp_b")
	h.expectRows(`SELECT v FROM it_sp_x`, "it_sp_b")

	// The ADBC current schema is the same setting.
	if err := h.setConnOption(adbc.OptionKeyCurrentDbSchema, "it_sp_a"); err != nil {
		t.Fatal(err)
	}
	h.expectRows(`SHOW search_path`, "it_sp_a")
	h.expectRows(`SELECT v FROM it_sp_x`, "it_sp_a")
	h.exec(`RESET ALL`)
	h.expectRows(`SHOW search_path`, "public")

	for _, c := range []struct{ sql, want string }{
		{`SET search_path = "$user", pg_catalog`,
			`search_path names no schema to use ("$user", pg_catalog; "$user", pg_catalog and pg_temp are skipped)`},
		{`SET search_path = pg_temp_7`, `schema name "pg_temp_7" is reserved for temporary tables and views`},
		{`SET search_path = 1`, `invalid value for parameter "search_path": "1"`},
		{`SET SCHEMA it_sp_a`, `syntax error: expected a string after SET SCHEMA near "it_sp_a"`},
		{`SET search_path`, `syntax error: expected = or TO after SET search_path near ""`},
		{`SET search_path = ?`, `syntax error: expected a value near ""`},
	} {
		h.expectErrorText(c.sql, c.want)
	}
	h.expectRows(`SHOW search_path`, "public")
}

// SET LOCAL lasts until the transaction ends, as in Postgres: COMMIT or
// ROLLBACK after BEGIN, or the end of a script of several statements.
func TestSQLSetLocal(t *testing.T) {
	h := newSQLHarness(t)
	h.exec(`CREATE SCHEMA IF NOT EXISTS it_sl_a`)
	h.dropTables("it_sl_a.it_sl_x", "it_sl_x")
	h.exec(`CREATE TABLE it_sl_a.it_sl_x (v varchar)`)
	h.exec(`INSERT INTO it_sl_a.it_sl_x VALUES ('a')`)
	h.exec(`CREATE TABLE it_sl_x (v varchar)`)
	h.exec(`INSERT INTO it_sl_x VALUES ('public')`)

	// Alone, outside BEGIN: no effect.
	h.exec(`SET LOCAL search_path = it_sl_a`)
	h.expectRows(`SHOW search_path`, "public")
	// In a script of several statements: until the script ends.
	h.expectRows(`SET LOCAL search_path = it_sl_a; SELECT v FROM it_sl_x`, "a")
	h.expectRows(`SET LOCAL search_path = it_sl_a; SHOW search_path`, "it_sl_a")
	h.expectRows(`SELECT v FROM it_sl_x`, "public")
	// A COMMIT in the script ends it there.
	h.expectRows(`SET LOCAL search_path = it_sl_a; COMMIT; SELECT v FROM it_sl_x`, "public")
	// After BEGIN: across statements until COMMIT, ROLLBACK, END or ABORT.
	for _, end := range []string{"COMMIT", "ROLLBACK", "END", "ABORT"} {
		h.exec(`BEGIN`)
		h.exec(`SET LOCAL search_path = it_sl_a`)
		h.expectRows(`SELECT v FROM it_sl_x`, "a")
		h.expectRows(`SHOW search_path`, "it_sl_a")
		h.exec(end)
		h.expectRows(`SELECT v FROM it_sl_x`, "public")
	}
	h.exec(`BEGIN; SET LOCAL search_path = it_sl_a`)
	h.expectRows(`SELECT v FROM it_sl_x`, "a")
	h.exec(`COMMIT`)
	h.expectRows(`SHOW search_path`, "public")

	// SET after SET LOCAL stays; SET LOCAL after SET ends at the SET value.
	h.exec(`BEGIN`)
	h.exec(`SET LOCAL application_name = 'local'`)
	h.exec(`SET application_name = 'session'`)
	h.exec(`ROLLBACK`)
	h.expectRows(`SHOW application_name`, "session")
	h.exec(`BEGIN`)
	h.exec(`SET application_name = 'session2'`)
	h.exec(`SET LOCAL application_name = 'local2'`)
	h.expectRows(`SHOW application_name`, "local2")
	h.exec(`END`)
	h.expectRows(`SHOW application_name`, "session2")
	h.exec(`RESET application_name`)

	// SET LOCAL checks its parameter and value even where it has no effect.
	h.expectErrorText(`SET LOCAL nosuch = 1`, `unrecognized configuration parameter "nosuch"`)
	h.expectErrorText(`SET LOCAL TIME ZONE 'Europe/Paris'`, `invalid value for parameter "TimeZone": "Europe/Paris" (only UTC is supported)`)
}

// The parameters SET, RESET and SHOW accept, with their values.
func TestSQLSetParameters(t *testing.T) {
	h := newSQLHarness(t)
	for _, c := range []struct{ set, show, col, want string }{
		{`SET TIME ZONE 'UTC'`, `SHOW timezone`, "TimeZone", "UTC"},
		{`set time zone utc`, `SHOW TIME ZONE`, "TimeZone", "UTC"},
		{`SET TIME ZONE LOCAL`, `SHOW TimeZone`, "TimeZone", "UTC"},
		{`SET TIME ZONE DEFAULT`, `SHOW timezone`, "TimeZone", "UTC"},
		{`SET timezone = 'utc'`, `SHOW timezone`, "TimeZone", "UTC"},
		{`SET SESSION TimeZone TO DEFAULT`, `SHOW timezone`, "TimeZone", "UTC"},
		{`RESET TIME ZONE`, `SHOW timezone`, "TimeZone", "UTC"},
		{`SET client_encoding = 'UTF8'`, `SHOW client_encoding`, "client_encoding", "UTF8"},
		{`SET client_encoding TO 'utf-8'`, `SHOW client_encoding`, "client_encoding", "UTF8"},
		{`SET NAMES 'unicode'`, `SHOW client_encoding`, "client_encoding", "UTF8"},
		{`SET NAMES DEFAULT`, `SHOW client_encoding`, "client_encoding", "UTF8"},
		{`SET application_name = 'dbt'`, `SHOW application_name`, "application_name", "dbt"},
		{`SET application_name TO myApp`, `SHOW application_name`, "application_name", "myApp"},
		{`SET standard_conforming_strings = on`, `SHOW standard_conforming_strings`, "standard_conforming_strings", "on"},
		{`SET standard_conforming_strings TO 'true'`, `SHOW standard_conforming_strings`, "standard_conforming_strings", "on"},
		{`SET statement_timeout = 5000`, `SHOW statement_timeout`, "statement_timeout", "5s"},
		{`SET statement_timeout = '1500ms'`, `SHOW statement_timeout`, "statement_timeout", "1500ms"},
		{`SET statement_timeout TO '2min'`, `SHOW statement_timeout`, "statement_timeout", "2min"},
		{`SET statement_timeout = '1.5s'`, `SHOW statement_timeout`, "statement_timeout", "1500ms"},
		{`SET statement_timeout = '90 s'`, `SHOW statement_timeout`, "statement_timeout", "90s"},
		{`SET statement_timeout = 0`, `SHOW statement_timeout`, "statement_timeout", "0"},
		{`SET lock_timeout = '1h'`, `SHOW lock_timeout`, "lock_timeout", "1h"},
		{`SET idle_in_transaction_session_timeout = 86400000`, `SHOW idle_in_transaction_session_timeout`,
			"idle_in_transaction_session_timeout", "1d"},
		{`SET extra_float_digits = 3`, `SHOW extra_float_digits`, "extra_float_digits", "3"},
		{`SET extra_float_digits = -15`, `SHOW extra_float_digits`, "extra_float_digits", "-15"},
		{`SET datestyle = 'ISO, DMY'`, `SHOW datestyle`, "DateStyle", "ISO, DMY"},
		{`SET DateStyle TO ISO`, `SHOW DATESTYLE`, "DateStyle", "ISO, DMY"},
		{`SET datestyle = iso, ymd`, `SHOW datestyle`, "DateStyle", "ISO, YMD"},
		{`SET datestyle = 'European'`, `SHOW datestyle`, "DateStyle", "ISO, DMY"},
		{`SET intervalstyle = postgres`, `SHOW IntervalStyle`, "IntervalStyle", "postgres"},
		{`SET IntervalStyle TO 'POSTGRES'`, `SHOW intervalstyle`, "IntervalStyle", "postgres"},
	} {
		h.exec(c.set)
		h.expectColumns(c.show, c.col, c.want)
	}

	// sql_header-style SETs before a statement, in one script.
	h.dropTables("it_set_ctas")
	h.exec(`set statement_timeout = '5min'; set time zone 'UTC'; set search_path to public;
		create table it_set_ctas as select 1 as id`)
	h.expectRows(`SELECT id FROM it_set_ctas`, "1")

	h.exec(`RESET ALL`)
	h.expectColumns(`SHOW ALL`, "name|setting|description",
		`application_name||Name of the application. Accepted and shown; the driver doesn't use it.`,
		`client_encoding|UTF8|Client's character set encoding. Only UTF8 is supported.`,
		`DateStyle|ISO, MDY|Display format for date and time values. Only ISO is supported; the order (MDY, DMY, YMD) has no effect, as only ISO dates are read.`,
		`extra_float_digits|1|Extra digits for floating-point values shown as text. Accepted and shown; results are Arrow values.`,
		`idle_in_transaction_session_timeout|0|Maximum allowed idle time in a transaction. Accepted and shown; there are no transactions.`,
		`IntervalStyle|postgres|Display format for interval values. Only postgres is supported.`,
		`lock_timeout|0|Maximum allowed duration of any wait for a lock. Accepted and shown; the driver takes no locks.`,
		`search_path|public|Sets the schema for unqualified names: the first schema of the list, skipping "$user", pg_catalog and pg_temp (adbc.redis.default_schema by default).`,
		`standard_conforming_strings|on|'...' strings treat backslashes literally. Only on is supported.`,
		`statement_timeout|0|Maximum allowed duration of any statement. Accepted and shown, not enforced.`,
		`TimeZone|UTC|Time zone for displaying and interpreting time stamps. Only UTC is supported.`)

	// ExecuteSchema of SHOW gives its column.
	for sql, cols := range map[string]string{`SHOW datestyle`: "DateStyle", `SHOW ALL`: "name|setting|description"} {
		schema, err := h.executeSchema(sql)
		if err != nil {
			t.Fatal(err)
		}
		if got := fieldNames(schema); got != cols {
			t.Errorf("ExecuteSchema(%s): %q, want %q", sql, got, cols)
		}
	}

	for _, c := range []struct{ sql, want string }{
		{`SET TIME ZONE 'Europe/Paris'`, `invalid value for parameter "TimeZone": "Europe/Paris" (only UTC is supported)`},
		{`SET TIME ZONE -7`, `invalid value for parameter "TimeZone": "-7" (only UTC is supported)`},
		{`SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE`, `invalid value for parameter "TimeZone": "INTERVAL '-08:00' HOUR TO MINUTE" (only UTC is supported)`},
		{`SET timezone = 'America/New_York'`, `invalid value for parameter "TimeZone": "America/New_York" (only UTC is supported)`},
		{`SET timezone = 'UTC', 'GMT'`, `SET TimeZone takes only one argument`},
		{`SET client_encoding = 'LATIN1'`, `invalid value for parameter "client_encoding": "LATIN1" (only UTF8 is supported)`},
		{`SET NAMES 'SQL_ASCII'`, `invalid value for parameter "client_encoding": "SQL_ASCII" (only UTF8 is supported)`},
		{`SET standard_conforming_strings = off`, `invalid value for parameter "standard_conforming_strings": "off" (only on is supported)`},
		{`SET standard_conforming_strings = maybe`, `parameter "standard_conforming_strings" requires a Boolean value`},
		{`SET statement_timeout = '5 parsecs'`, `invalid value for parameter "statement_timeout": "5 parsecs" (valid units are "us", "ms", "s", "min", "h" and "d")`},
		{`SET statement_timeout = -1`, `-1 ms is outside the valid range for parameter "statement_timeout" (0 .. 2147483647)`},
		{`SET lock_timeout = '30d'`, `2592000000 ms is outside the valid range for parameter "lock_timeout" (0 .. 2147483647)`},
		{`SET statement_timeout = 1, 2`, `SET statement_timeout takes only one argument`},
		{`SET extra_float_digits = 4`, `4 is outside the valid range for parameter "extra_float_digits" (-15 .. 3)`},
		{`SET extra_float_digits = 'x'`, `invalid value for parameter "extra_float_digits": "x"`},
		{`SET datestyle = 'SQL, DMY'`, `invalid value for parameter "DateStyle": "SQL, DMY" (only ISO is supported)`},
		{`SET datestyle = 'nonsense'`, `invalid value for parameter "DateStyle": "nonsense"`},
		{`SET intervalstyle = iso_8601`, `invalid value for parameter "IntervalStyle": "iso_8601" (only postgres is supported)`},
		{`SET intervalstyle = 'nope'`, `invalid value for parameter "IntervalStyle": "nope"`},
		{`SET nosuch = 1`, `unrecognized configuration parameter "nosuch"`},
		{`SET my.custom = 'x'`, `unrecognized configuration parameter "my.custom"`},
		{`SET LOCAL nosuch TO DEFAULT`, `unrecognized configuration parameter "nosuch"`},
		{`RESET nosuch`, `unrecognized configuration parameter "nosuch"`},
		{`SHOW nosuch`, `unrecognized configuration parameter "nosuch"`},
		{`SHOW transaction_isolation`, `unrecognized configuration parameter "transaction_isolation"`},
	} {
		h.expectErrorText(c.sql, c.want)
	}
	h.expectSchemaErrorText(`SHOW nosuch`, `unrecognized configuration parameter "nosuch"`)
	// A failed SET changes nothing.
	h.expectRows(`SHOW timezone`, "UTC")
	h.expectRows(`SHOW statement_timeout`, "0")
}
