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

// Integration tests with Redis ACL users: the command lists that the README
// documents ("ACL permissions"), a read-only user, the errors of commands
// refused inside MULTI, and what key patterns don't cover. The tests create
// users it_acl_* (on every master of a cluster) and delete them afterwards,
// so the user at adminURI needs the ACL command.
//
// To run the whole suite as a user with only the driver's commands, set
// REDIS_URI to that user and REDIS_ADMIN_URI to one that can do everything
// (the tests' own checks use KEYS, ACL, …).

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

// The ACL rules the README documents, by feature.
var (
	aclBaseCommands = []string{
		"+ping", "+info",
		"+ft._list", "+ft.create", "+ft.alter", "+ft.info", "+ft.dropindex", "+ft.aggregate", "+ft.cursor", "+ft.search", "+ft.profile",
		"+get", "+set", "+del", "+exists", "+incrby",
		"+hset", "+hget", "+hmget", "+hgetall", "+hdel", "+hincrby", "+hexists",
		"+sadd", "+srem", "+smembers", "+sismember",
		"+multi", "+exec", "+watch", "+unwatch",
	}
	aclClusterCommands = []string{"+cluster|slots", "+command"}
	aclRekeyCommands   = []string{"+incr", "+dump", "+restore"}
	aclReadOnly        = []string{
		"+ping", "+info",
		"+ft._list", "+ft.aggregate", "+ft.cursor", "+ft.search", "+ft.profile",
		"+get", "+exists", "+hget", "+hmget", "+hgetall", "+smembers", "+sismember",
	}
)

const aclPassword = "it-acl-password"

// aclHarness creates ACL users and connects as them.
type aclHarness struct {
	*sqlHarness
	raw     goredis.UniversalClient
	cluster bool
}

func newACLHarness(t *testing.T) *aclHarness {
	t.Helper()
	h := newSQLHarness(t)
	raw := h.rawClient()
	_, cluster := raw.(*goredis.ClusterClient)
	a := &aclHarness{sqlHarness: h, raw: raw, cluster: cluster}
	if err := a.eachNode(func(n *goredis.Client) error { return n.Do(h.ctx, "ACL", "WHOAMI").Err() }); err != nil {
		t.Skipf("the user at adminURI can't manage ACL users: %v", err)
	}
	return a
}

// eachNode runs fn on the server, or on every master of a cluster (ACL
// users are per node).
func (a *aclHarness) eachNode(fn func(*goredis.Client) error) error {
	if cc, ok := a.raw.(*goredis.ClusterClient); ok {
		return cc.ForEachMaster(a.ctx, func(_ context.Context, n *goredis.Client) error { return fn(n) })
	}
	return fn(a.raw.(*goredis.Client))
}

// user creates an ACL user with the given rules (on top of reset), resets
// the ACL log, and deletes the user when the test ends.
func (a *aclHarness) user(name string, rules ...string) {
	a.t.Helper()
	args := append([]any{"ACL", "SETUSER", name, "reset", "on", ">" + aclPassword, "resetchannels"}, anySlice(rules)...)
	err := a.eachNode(func(n *goredis.Client) error {
		if err := n.Do(a.ctx, args...).Err(); err != nil {
			return err
		}
		return n.Do(a.ctx, "ACL", "LOG", "RESET").Err()
	})
	if err != nil {
		a.t.Fatal(err)
	}
	a.t.Cleanup(func() {
		_ = a.eachNode(func(n *goredis.Client) error { return n.Do(context.Background(), "ACL", "DELUSER", name).Err() })
	})
}

func anySlice(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

// driverCommands are the commands of the driver on this server.
func (a *aclHarness) driverCommands(rekey bool) []string {
	cmds := slices.Clone(aclBaseCommands)
	if a.cluster {
		cmds = append(cmds, aclClusterCommands...)
	}
	if rekey {
		cmds = append(cmds, aclRekeyCommands...)
	}
	return cmds
}

func (a *aclHarness) readOnlyCommands() []string {
	cmds := slices.Clone(aclReadOnly)
	if a.cluster {
		cmds = append(cmds, aclClusterCommands...)
	}
	return cmds
}

// userURI is adminURI as an ACL user.
func (a *aclHarness) userURI(name string) string {
	a.t.Helper()
	u, err := url.Parse(adminURI())
	if err != nil {
		a.t.Fatal(err)
	}
	u.User = url.UserPassword(name, aclPassword)
	return u.String()
}

// connect opens a driver connection as an ACL user.
func (a *aclHarness) connect(name string, opts map[string]string) *sqlHarness {
	a.t.Helper()
	return openWith(a.t, a.userURI(name), opts)
}

// denials returns what the ACL log recorded for a user: "reason object".
func (a *aclHarness) denials(name string) []string {
	a.t.Helper()
	var out []string
	err := a.eachNode(func(n *goredis.Client) error {
		entries, err := n.Do(a.ctx, "ACL", "LOG", 1000).Slice()
		if err != nil {
			return err
		}
		for _, e := range entries {
			fields, _ := e.([]any)
			m := map[string]string{}
			for i := 0; i+1 < len(fields); i += 2 {
				k, _ := fields[i].(string)
				v, _ := fields[i+1].(string)
				m[k] = v
			}
			if m["username"] == name {
				out = append(out, m["reason"]+" "+m["object"])
			}
		}
		return nil
	})
	if err != nil {
		a.t.Fatal(err)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func (a *aclHarness) expectNoDenials(name string) {
	a.t.Helper()
	if d := a.denials(name); len(d) > 0 {
		a.t.Errorf("ACL log for %s: %v", name, d)
	}
}

// The README's recipes are the rules tested here.
func TestACLReadme(t *testing.T) {
	raw, err := os.ReadFile("../README.md")
	if err != nil {
		t.Skip(err)
	}
	readme := string(raw)
	for what, rules := range map[string][]string{
		"base":      aclBaseCommands,
		"cluster":   aclClusterCommands,
		"rekey":     aclRekeyCommands,
		"read-only": aclReadOnly,
	} {
		if line := strings.Join(rules, " "); !strings.Contains(readme, line) {
			t.Errorf("the README doesn't list the %s commands as tested:\n%s", what, line)
		}
	}
}

// A user with exactly the documented commands and keys runs a workload of
// every kind of statement, without a single command refused.
func TestACLDriverUser(t *testing.T) {
	a := newACLHarness(t)
	a.exec(`DROP SCHEMA IF EXISTS it_acl_s CASCADE`)
	t.Cleanup(func() { a.exec(`DROP SCHEMA IF EXISTS it_acl_s CASCADE`) })
	a.dropTables("it_acl_d", "it_acl_d2", "it_acl_d3", "it_acl_ing")
	keys := []string{"~adbc:*", "~public:*", "~it_acl_s:*", "~pg_temp_*"}
	a.user("it_acl_base", append(a.driverCommands(false), keys...)...)
	a.user("it_acl_rekey", append(a.driverCommands(true), keys...)...)

	u := a.connect("it_acl_base", map[string]string{OptionStringReadTimeout: "60s"})
	for _, sql := range []string{
		`CREATE SCHEMA it_acl_s`,
		`CREATE TABLE it_acl_s.t (id INTEGER NOT NULL, label VARCHAR, amount NUMERIC(10,2), note VARCHAR NOINDEX)`,
		`INSERT INTO it_acl_s.t VALUES (1, 'apple', 1.50, 'n1'), (2, 'banana', NULL, NULL), (3, 'avocado', 3.25, 'n3')`,
		`UPDATE it_acl_s.t SET amount = 2.00 WHERE id = 2`,
		`DELETE FROM it_acl_s.t WHERE id = 3`,
		`INSERT INTO it_acl_s.t (id, label) VALUES (4, 'apricot')`,
		`ALTER TABLE it_acl_s.t ADD COLUMN qty INTEGER DEFAULT 7`,
		`ALTER TABLE it_acl_s.t DROP COLUMN note`,
		`ALTER TABLE it_acl_s.t RENAME COLUMN qty TO quantity`,
		`ALTER TABLE it_acl_s.t ADD CONSTRAINT pos CHECK (id > 0)`,
		`COMMENT ON TABLE it_acl_s.t IS 'fruit'`,
		`CREATE VIEW it_acl_s.v AS SELECT id, label FROM it_acl_s.t WHERE label LIKE 'a%'`,
		`CREATE TABLE it_acl_d AS SELECT id, label FROM it_acl_s.t`,
		`MERGE INTO it_acl_d d USING it_acl_s.t s ON d.id = s.id WHEN MATCHED THEN UPDATE SET label = s.label || '!'`,
		`ALTER TABLE it_acl_d RENAME TO it_acl_d2`,
		`CREATE TEMP TABLE it_acl_tmp (id INTEGER, label VARCHAR)`,
		`INSERT INTO it_acl_tmp SELECT id, label FROM it_acl_s.t`,
	} {
		u.exec(sql)
	}
	u.expectRows(`SELECT id, label, amount, quantity FROM it_acl_s.t ORDER BY id`, "1|apple|1.50|7", "2|banana|2.00|7", "4|apricot|NULL|7")
	u.expectRows(`SELECT label FROM it_acl_s.t WHERE label LIKE 'ap%' ORDER BY label`, "apple", "apricot")
	u.expectRows(`SELECT COUNT(*), SUM(amount) FROM it_acl_s.t WHERE id >= 2`, "2|2.00")
	u.expectRows(`SELECT id FROM it_acl_s.v ORDER BY id`, "1", "4")
	u.expectRows(`SELECT id, label FROM it_acl_d2 ORDER BY id`, "1|apple!", "2|banana!", "4|apricot!")
	u.expectRows(`SELECT COUNT(*) FROM it_acl_tmp`, "3")
	u.expectRows(`SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'it_acl_s' ORDER BY table_name`,
		"t|BASE TABLE", "v|VIEW")
	if _, err := u.conn.GetTableSchema(u.ctx, nil, ptr("it_acl_s"), "t"); err != nil {
		t.Error(err)
	}
	rdr, err := u.conn.GetObjects(u.ctx, adbc.ObjectDepthAll, nil, ptr("it_acl_s"), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for rdr.Next() {
	}
	rdr.Release()

	// Bulk ingest, into a temporary table too.
	if err := u.ingest("it_acl_ing", adbc.OptionValueIngestModeCreate, false, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := u.ingest("it_acl_ing", adbc.OptionValueIngestModeAppend, false, 3); err != nil {
		t.Fatal(err)
	}
	if err := u.ingest("it_acl_ing", adbc.OptionValueIngestModeReplace, false, 4, 5); err != nil {
		t.Fatal(err)
	}
	if err := u.ingest("it_acl_ingt", adbc.OptionValueIngestModeCreate, true, 6); err != nil {
		t.Fatal(err)
	}
	u.expectRows(`SELECT id FROM it_acl_ing ORDER BY id`, "4", "5")
	u.expectRows(`SELECT id FROM it_acl_ingt`, "6")

	for _, sql := range []string{
		`TRUNCATE it_acl_d2`,
		`DROP VIEW it_acl_s.v`,
		`DROP TABLE it_acl_d2`,
		`DROP TABLE it_acl_ing`,
	} {
		u.exec(sql)
	}
	// rename_rekey needs three more commands.
	if err := u.setConnOption(OptionStringRenameRekey, "true"); err != nil {
		t.Fatal(err)
	}
	u.expectErrorExact(`ALTER TABLE it_acl_s.t RENAME TO t2`, `I/O: [redis] failed to start the rename: `+
		`NOPERM User it_acl_base has no permissions to run the 'incr' command`)
	// Closing drops the temporary objects; the background cleanup of the
	// dropped column has had time to run.
	waitFor(t, "the dropped column's cleanup", func() bool {
		m, _ := a.raw.SCard(a.ctx, cleanupKey).Result()
		return m == 0
	})
	if err := u.conn.Close(u.ctx); err != nil {
		t.Error(err)
	}
	// Nothing but the rename's INCR was refused.
	if d := a.denials("it_acl_base"); !slices.Equal(d, []string{"command incr"}) {
		t.Errorf("ACL log for it_acl_base: %v", d)
	}

	r := a.connect("it_acl_rekey", map[string]string{OptionStringRenameRekey: "true", OptionStringReadTimeout: "60s"})
	r.exec(`ALTER TABLE it_acl_s.t RENAME TO t2`)
	r.expectRows(`SELECT COUNT(*) FROM it_acl_s.t2`, "3")
	r.namesN(a.raw, "it_acl_s", "t2", "t2")
	a.expectNoDenials("it_acl_rekey")
}

func ptr[T any](v T) *T { return &v }

// The documented read-only user: it reads tables, views and metadata, and
// every change is refused, with Redis's reason, before anything changed.
func TestACLReadOnlyUser(t *testing.T) {
	a := newACLHarness(t)
	a.dropTables("it_acl_ro", "it_acl_ro2")
	a.exec(`DROP VIEW IF EXISTS it_acl_rov`)
	t.Cleanup(func() { a.exec(`DROP VIEW IF EXISTS it_acl_rov`) })
	a.exec(`CREATE TABLE it_acl_ro (id INTEGER, label VARCHAR)`)
	a.exec(`INSERT INTO it_acl_ro VALUES (1, 'a'), (2, 'ab'), (3, 'b')`)
	a.exec(`CREATE VIEW it_acl_rov AS SELECT id FROM it_acl_ro WHERE label LIKE 'a%'`)
	a.user("it_acl_ro", append(a.readOnlyCommands(), "%R~adbc:*", "%R~public:*")...)

	u := a.connect("it_acl_ro", map[string]string{OptionStringReadTimeout: "60s"})
	u.expectRows(`SELECT id, label FROM it_acl_ro WHERE label LIKE 'a%' ORDER BY id`, "1|a", "2|ab")
	u.expectRows(`SELECT COUNT(*), SUM(id) FROM it_acl_ro WHERE id > 1`, "2|5")
	u.expectRows(`SELECT label, COUNT(*) FROM it_acl_ro GROUP BY label ORDER BY label`, "a|1", "ab|1", "b|1")
	u.expectRows(`SELECT id FROM it_acl_rov ORDER BY id`, "1", "2")
	u.expectRows(`SELECT column_name FROM information_schema.columns WHERE table_name = 'it_acl_ro' ORDER BY ordinal_position`, "id", "label")
	if _, err := u.conn.GetTableSchema(u.ctx, nil, nil, "it_acl_ro"); err != nil {
		t.Error(err)
	}
	a.expectNoDenials("it_acl_ro")

	noperm := func(cmd string) string {
		return fmt.Sprintf("NOPERM User it_acl_ro has no permissions to run the '%s' command", cmd)
	}
	for _, c := range []struct{ sql, want string }{
		{`INSERT INTO it_acl_ro VALUES (4, 'c')`, "failed to allocate row ids: " + noperm("multi")},
		{`UPDATE it_acl_ro SET label = 'x'`, "failed to update rows: " + noperm("hset")},
		{`DELETE FROM it_acl_ro`, "failed to delete rows: " + noperm("del")},
		{`TRUNCATE it_acl_ro`, "failed to reserve a key prefix: " + noperm("sadd")},
		{`DROP TABLE it_acl_ro`, "failed to drop search index: " + noperm("FT.DROPINDEX")},
		{`CREATE TABLE it_acl_ro2 (id INTEGER)`, "failed to reserve a key prefix: " + noperm("sadd")},
		{`CREATE VIEW it_acl_rov2 AS SELECT 1 AS x`, "failed to create view: " + noperm("watch")},
		{`ALTER TABLE it_acl_ro ADD COLUMN z INTEGER`, "failed to read the search index: " + noperm("FT.INFO")},
		{`COMMENT ON TABLE it_acl_ro IS 'x'`, "failed to update table metadata: " + noperm("watch")},
		{`CREATE TEMP TABLE it_acl_t (id INTEGER)`, "failed to allocate a temporary schema: " + noperm("incrby")},
	} {
		u.expectErrorExact(c.sql, "I/O: [redis] "+c.want)
	}
	a.expectRows(`SELECT id, label FROM it_acl_ro ORDER BY id`, "1|a", "2|ab", "3|b")
	a.expectRows(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'it_acl_ro%'`, "2")
}

// A command refused inside MULTI fails the transaction with EXECABORT; the
// error reports the command Redis refused and its key instead.
func TestACLExecAbort(t *testing.T) {
	a := newACLHarness(t)
	a.dropTables("it_acl_x")
	a.exec(`DROP VIEW IF EXISTS it_acl_xv`)
	t.Cleanup(func() { a.exec(`DROP VIEW IF EXISTS it_acl_xv`) })
	a.exec(`CREATE TABLE it_acl_x (id INTEGER)`)
	a.exec(`INSERT INTO it_acl_x VALUES (1)`)
	a.exec(`CREATE VIEW it_acl_xv AS SELECT id FROM it_acl_x`)
	// Every command the driver needs, but read-only keys.
	a.user("it_acl_keys", append(a.driverCommands(true), "%R~adbc:*", "%R~public:*")...)
	u := a.connect("it_acl_keys", map[string]string{OptionStringReadTimeout: "60s"})
	const denied = "NOPERM No permissions to access a key"
	for _, c := range []struct{ sql, want string }{
		// WATCH … MULTI … EXEC
		{`CREATE VIEW it_acl_xv2 AS SELECT id FROM it_acl_x`,
			"failed to create view: " + denied + " ('set' on adbc:{meta}:view:public:it_acl_xv2, in a MULTI transaction that Redis discarded)"},
		{`COMMENT ON TABLE it_acl_x IS 'x'`,
			"failed to update table metadata: " + denied + " ('set' on adbc:{meta}:table:public:it_acl_x, in a MULTI transaction that Redis discarded)"},
		// MULTI … EXEC
		{`INSERT INTO it_acl_x VALUES (2)`,
			"failed to allocate row ids: " + denied + " ('incrby' on adbc:{meta}:seq:public:it_acl_x, in a MULTI transaction that Redis discarded)"},
	} {
		u.expectErrorExact(c.sql, "I/O: [redis] "+c.want)
	}
	// Both of DROP VIEW's commands are refused. A cluster client then shows
	// no EXECABORT, only the first command's error.
	dropView := "failed to drop view: " + denied + " ('del' on adbc:{meta}:view:public:it_acl_xv, in a MULTI transaction that Redis discarded)"
	if a.cluster {
		dropView = "failed to drop view: " + denied
	}
	u.expectErrorExact(`DROP VIEW it_acl_xv`, "I/O: [redis] "+dropView)
	a.expectRows(`SELECT id FROM it_acl_xv`, "1")

	// go-redis still sees EXECABORT through the rewritten error (it then
	// knows that EXEC ended the WATCH).
	opts, err := goredis.ParseURL(a.userURI("it_acl_keys"))
	if err != nil {
		t.Fatal(err)
	}
	opts.Protocol, opts.DisableIdentity = 2, true // as the driver's
	client := newClient(opts, a.cluster, timeouts{read: time.Minute, write: time.Minute})
	defer client.Close()
	key := metaKey("public", "it_acl_x")
	err = client.Watch(a.ctx, func(tx *goredis.Tx) error {
		_, err := tx.TxPipelined(a.ctx, func(p goredis.Pipeliner) error {
			p.Get(a.ctx, key)
			p.Set(a.ctx, key, "x", 0)
			return nil
		})
		return err
	}, key)
	if !goredis.IsExecAbortError(err) || err.Error() != denied+" ('set' on "+key+", in a MULTI transaction that Redis discarded)" {
		t.Errorf("raw transaction: %v (EXECABORT: %v)", err, goredis.IsExecAbortError(err))
	}
	var q *queuedError
	if !errors.As(err, &q) {
		t.Errorf("%T", err)
	}
}

// What key patterns do for the search commands (Redis 8.6): queries are
// refused on an index whose key prefix the user can't read, but the
// commands that change indexes declare no keys, so key patterns don't
// restrict them, and FT.DROPINDEX … DD deletes rows the user may only read.
// If this test fails, Redis changed that, and the README's ACL section
// needs updating.
func TestACLSearchCommandsAndKeys(t *testing.T) {
	a := newACLHarness(t)
	a.exec(`DROP SCHEMA IF EXISTS it_acl_other CASCADE`)
	t.Cleanup(func() { a.exec(`DROP SCHEMA IF EXISTS it_acl_other CASCADE`) })
	a.dropTables("it_acl_victim")
	a.exec(`CREATE TABLE it_acl_victim (id INTEGER)`)
	a.exec(`INSERT INTO it_acl_victim VALUES (1), (2), (3)`)
	a.exec(`CREATE SCHEMA it_acl_other`)
	a.exec(`CREATE TABLE it_acl_other.t (id INTEGER, secret VARCHAR)`)
	a.exec(`INSERT INTO it_acl_other.t VALUES (1, 's1'), (2, 's2')`)
	prefix, index := a.tableNames(a.raw, "public", "it_acl_victim")
	_, otherIndex := a.tableNames(a.raw, "it_acl_other", "t")

	rawAs := func(name string) *store {
		opts, err := goredis.ParseURL(a.userURI(name))
		if err != nil {
			t.Fatal(err)
		}
		opts.Protocol, opts.DisableIdentity = 2, true // as the driver's
		c := newClient(opts, a.cluster, timeouts{read: time.Minute, write: time.Minute})
		t.Cleanup(func() { _ = c.Close() })
		return &store{client: c}
	}
	// A user who may read only the public schema's keys can't query another
	// schema's index, through the driver or directly.
	a.user("it_acl_schema", append(a.readOnlyCommands(), "%R~adbc:*", "%R~public:*")...)
	const refused = "NOPERM User does not have the required permissions to query the index"
	u := a.connect("it_acl_schema", map[string]string{OptionStringReadTimeout: "60s"})
	u.expectErrorExact(`SELECT id FROM it_acl_other.t`, "I/O: [redis] FT.SEARCH failed: "+refused)
	u.expectRows(`SELECT COUNT(*) FROM it_acl_victim`, "3")
	_, err := rawAs("it_acl_schema").aggregate(a.ctx, &aggRequest{index: otherIndex, query: "*", load: []string{"secret"}})
	if err == nil || err.Error() != "I/O: [redis] FT.AGGREGATE failed: "+refused {
		t.Errorf("FT.AGGREGATE … LOAD on another schema's index: %v", err)
	}
	// One who may only read keys, but run FT.DROPINDEX, deletes a table's
	// rows.
	dropper := []string{"+ping", "+ft.dropindex", "%R~*"}
	if a.cluster {
		dropper = append(dropper, aclClusterCommands...)
	}
	a.user("it_acl_dropper", dropper...)
	if err := rawAs("it_acl_dropper").searchDo(a.ctx, index, "FT.DROPINDEX", index, "DD").Err(); err != nil {
		t.Fatalf("FT.DROPINDEX … DD as a read-only user: %v", err)
	}
	if n := a.prefixKeyCount(a.raw, prefix); n != 0 {
		t.Errorf("%d rows left after FT.DROPINDEX … DD", n)
	}
	a.expectNoDenials("it_acl_dropper")
	a.exec(`DROP TABLE it_acl_victim`)
}

// A connection that takes over an abandoned move but can't roll it back
// (here: no FT.DROPINDEX) gives it up at once, so that the next connection
// can, rather than after the takeover's lease has expired.
func TestACLRekeyTakeoverReleased(t *testing.T) {
	a := newACLHarness(t)
	a.dropTables("it_acl_rk", "it_acl_rk2")
	a.exec(`CREATE TABLE it_acl_rk (id INTEGER)`)
	a.exec(`INSERT INTO it_acl_rk VALUES (1), (2)`)
	st := &store{client: a.raw}
	job := a.abandon(st, a.raw, "it_acl_rk", "it_acl_rk2")
	cmds := slices.DeleteFunc(a.driverCommands(true), func(c string) bool { return c == "+ft.dropindex" })
	a.user("it_acl_nodrop", append(cmds, "~*")...)

	u := a.connect("it_acl_nodrop", map[string]string{OptionStringReadTimeout: "60s"})
	if keys := a.aliveKeys(a.raw); len(keys) != 0 {
		t.Errorf("lease keys left by the failed takeover: %v", keys)
	}
	u.expectErrorExact(`INSERT INTO it_acl_rk VALUES (3)`, `I/O: [redis] table "public"."it_acl_rk" is being renamed `+
		`(adbc.redis.rename_rekey), but the connection renaming it has gone away, and rolling the rename back failed: `+
		`failed to drop the new search index: NOPERM User it_acl_nodrop has no permissions to run the 'FT.DROPINDEX' command. `+
		`The next connection to open, or the next statement that changes the table, tries again`)
	if rec := a.rekeyRecord(a.raw, job.OldPrefix); rec == nil || !rec.Recovering {
		t.Fatalf("record: %+v", rec)
	}
	// The next connection that can, does, straight away.
	h2 := newSQLHarness(t)
	if a.rekeyPending(a.raw, job.OldPrefix) {
		t.Error("the move was not rolled back")
	}
	h2.expectReleased(a.raw, job.KeyPrefix, job.IndexName)
	u.exec(`INSERT INTO it_acl_rk VALUES (3)`)
	h2.expectRows(`SELECT id FROM it_acl_rk ORDER BY id`, "1", "2", "3")
}
