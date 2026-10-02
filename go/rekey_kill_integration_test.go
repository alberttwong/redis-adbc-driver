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

// Integration tests for re-keys abandoned by their connection (rekey.go):
// a renaming process that is killed, connections that open and close
// straight away, and connections that wait for another one's rollback.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// The test binary is also the renaming process that TestSQLRekeyKilled
// kills.
func TestMain(m *testing.M) {
	if args := os.Getenv("ADBC_REDIS_REKEY_CHILD"); args != "" {
		os.Exit(rekeyChild(args))
	}
	os.Exit(m.Run())
}

// rekeyChild renames a table with rename_rekey ("<table> <new name>"), with
// the lease ADBC_REDIS_REKEY_TTL, and is meant to be killed before it ends.
func rekeyChild(args string) int {
	from, to, _ := strings.Cut(args, " ")
	if d, err := time.ParseDuration(os.Getenv("ADBC_REDIS_REKEY_TTL")); err == nil {
		rekeyTTL = d
	}
	ctx := context.Background()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: os.Getenv("REDIS_URI"), OptionStringRenameRekey: "true"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	conn, err := db.Open(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	_, err = tryExec(ctx, conn, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", from, to))
	fmt.Fprintln(os.Stderr, "the rename ended before the process was killed:", err)
	return 3
}

// dbSize counts the keys of the database (on every master of a cluster).
func (h *sqlHarness) dbSize(raw goredis.UniversalClient) int64 {
	h.t.Helper()
	if cc, ok := raw.(*goredis.ClusterClient); ok {
		var total atomic.Int64 // ForEachMaster runs fn concurrently
		err := cc.ForEachMaster(h.ctx, func(ctx context.Context, n *goredis.Client) error {
			v, err := n.DBSize(ctx).Result()
			total.Add(v)
			return err
		})
		if err != nil {
			h.t.Fatal(err)
		}
		return total.Load()
	}
	n, err := raw.DBSize(h.ctx).Result()
	if err != nil {
		h.t.Fatal(err)
	}
	return n
}

// rekeyRecord returns the re-key recorded for a prefix (nil if none).
func (h *sqlHarness) rekeyRecord(raw goredis.UniversalClient, prefix string) *rekeyJob {
	h.t.Helper()
	job, err := (&store{client: raw}).readRekey(h.ctx, prefix)
	if err != nil {
		h.t.Fatal(err)
	}
	return job
}

// aliveKeys lists the re-key lease keys.
func (h *sqlHarness) aliveKeys(raw goredis.UniversalClient) []string {
	h.t.Helper()
	node := raw
	if cc, ok := raw.(*goredis.ClusterClient); ok { // the {meta} slot's node
		n, err := cc.MasterForKey(h.ctx, metaPrefix)
		if err != nil {
			h.t.Fatal(err)
		}
		node = n
	}
	keys, err := node.Keys(h.ctx, rekeyAliveKey("*")).Result()
	if err != nil {
		h.t.Fatal(err)
	}
	return keys
}

// expectErrorExact runs a statement and checks its whole error message.
func (h *sqlHarness) expectErrorExact(sql, want string) {
	h.t.Helper()
	_, err := tryExec(h.ctx, h.conn, sql)
	if err == nil || err.Error() != want {
		h.t.Errorf("%s:\n got: %v\nwant: %s", sql, err, want)
	}
}

// movingMsg is movingErr's message for a table in the public schema.
func movingMsg(table string) string {
	return fmt.Sprintf(`I/O: [redis] table "public".%q is being renamed and its rows moved to new keys (adbc.redis.rename_rekey); `+
		`try again when that has finished. If the connection renaming it has gone away, the next connection to open, `+
		`or the next statement that changes the table, rolls the rename back once its lease (%s) has expired`, table, rekeyTTL)
}

// Issue #87: a process killed while it moves a table's rows. Connections
// that open after its lease expired, even ones that close straight away,
// roll the move back before they are open, so the rename can be run again;
// a connection that was open all along rolls it back at its next write.
func TestSQLRekeyKilled(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_kill", "it_rk_kill_backup")
	raw := h.rawClient()
	const n = 50000
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	if err := h.ingest("it_rk_kill", adbc.OptionValueIngestModeCreate, false, ids...); err != nil {
		t.Fatal(err)
	}
	oldPrefix, oldIndex := h.tableNames(raw, "public", "it_rk_kill")
	const ttl = 4 * time.Second
	defer func(was time.Duration) { rekeyTTL = was }(rekeyTTL)
	rekeyTTL = ttl
	// A connection that stays open, as dbt's main connection does.
	long := newSQLHarness(t)

	// kill starts the rename in a child process, and kills it once it has
	// copied some rows. It returns the job it left.
	kill := func() *rekeyJob {
		t.Helper()
		before := h.dbSize(raw)
		var out bytes.Buffer
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), "ADBC_REDIS_REKEY_CHILD=it_rk_kill it_rk_kill_backup", "ADBC_REDIS_REKEY_TTL="+ttl.String())
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(60 * time.Second)
		for h.dbSize(raw) < before+500 {
			if time.Now().After(deadline) {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("the child didn't start copying rows: %s", out.String())
			}
			time.Sleep(5 * time.Millisecond)
		}
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = cmd.Wait()
		if strings.Contains(out.String(), "ended before") {
			t.Fatalf("child: %s", out.String())
		}
		// What the killed process left: the job, its lease, the table
		// marked as moving, and some of the copies.
		job := h.rekeyRecord(raw, oldPrefix)
		if job == nil || job.Switched || job.Recovering || !strings.HasPrefix(job.KeyPrefix, "public:it_rk_kill_backup") ||
			!strings.HasPrefix(job.IndexName, "idx:public:it_rk_kill_backup") {
			t.Fatalf("job after the kill: %+v (child: %s)", job, out.String())
		}
		if ok, _ := (&store{client: raw}).ownerAlive(h.ctx, job); !ok {
			t.Fatal("the killed process's lease expired already")
		}
		if got := h.prefixKeyCount(raw, job.KeyPrefix); got == 0 || got >= n {
			t.Fatalf("%d copies after the kill", got)
		}
		return job
	}
	waitExpired := func(job *rekeyJob) {
		t.Helper()
		deadline := time.Now().Add(3 * ttl)
		for {
			ok, err := (&store{client: raw}).ownerAlive(h.ctx, job)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("the lease didn't expire")
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	expectIntact := func(h *sqlHarness, table string, job *rekeyJob, extra int64) {
		t.Helper()
		if h.rekeyPending(raw, oldPrefix) {
			t.Error("the move is still recorded")
		}
		if keys := h.aliveKeys(raw); len(keys) != 0 {
			t.Errorf("lease keys left: %v", keys)
		}
		h.expectReleased(raw, job.KeyPrefix, job.IndexName)
		h.expectRows(fmt.Sprintf(`SELECT COUNT(*), SUM(id), MIN(id), MAX(id) FROM %s`, table),
			fmt.Sprintf("%d|%d|1|%d", n+extra, n*(n+1)/2+extra*(n+1)+extra*(extra-1)/2, n+extra))
	}

	job := kill()
	// While its lease lasts, the move looks alive: writes and renames are
	// refused, on connections opened before and after the kill.
	long.expectErrorExact(`INSERT INTO it_rk_kill VALUES (0)`, movingMsg("it_rk_kill"))
	rk := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	rk.expectErrorExact(`ALTER TABLE it_rk_kill RENAME TO it_rk_kill_backup`, movingMsg("it_rk_kill"))
	h.expectRows(`SELECT COUNT(*) FROM it_rk_kill`, fmt.Sprint(n))

	// Once it has expired, a connection that is opened and closed straight
	// away (as dbt's metadata connections are) rolls the move back first.
	waitExpired(job)
	ctx := context.Background()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{adbc.OptionKeyURI: os.Getenv("REDIS_URI")})
	if err != nil {
		t.Fatal(err)
	}
	short, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := short.Close(ctx); err != nil {
		t.Fatal(err)
	}
	expectIntact(h, "it_rk_kill", job, 0)
	if prefix, index := h.tableNames(raw, "public", "it_rk_kill"); prefix != oldPrefix || index != oldIndex {
		t.Errorf("names after the rollback: %q, %q", prefix, index)
	}

	// The rename can be run again, on a new connection, and works.
	again := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	again.exec(`ALTER TABLE it_rk_kill RENAME TO it_rk_kill_backup`)
	// It takes new names: the rolled-back move's are never handed out again.
	h.namesN(raw, "public", "it_rk_kill_backup", "it_rk_kill_backup")
	if prefix, _ := h.tableNames(raw, "public", "it_rk_kill_backup"); prefix == job.KeyPrefix {
		t.Errorf("prefix after the rename: %q, the rolled-back move's", prefix)
	}
	h.expectReleased(raw, oldPrefix, oldIndex)
	h.expectRows(`SELECT COUNT(*), SUM(id) FROM it_rk_kill_backup`, fmt.Sprintf("%d|%d", n, n*(n+1)/2))
	again.exec(`ALTER TABLE it_rk_kill_backup RENAME TO it_rk_kill`)
	oldPrefix, oldIndex = h.tableNames(raw, "public", "it_rk_kill")

	// Killed again: this time the connection that was open all along is
	// the first to write once the lease has expired. It rolls the move
	// back, and its statement goes on.
	job = kill()
	waitExpired(job)
	long.exec(`INSERT INTO it_rk_kill VALUES (50001)`)
	expectIntact(long, "it_rk_kill", job, 1)
	if err := long.setConnOption(OptionStringRenameRekey, "true"); err != nil {
		t.Fatal(err)
	}
	long.exec(`ALTER TABLE it_rk_kill RENAME TO it_rk_kill_backup`)
	long.exec(`INSERT INTO it_rk_kill_backup VALUES (50002)`)
	h.expectRows(`SELECT COUNT(*), SUM(id), MAX(id) FROM it_rk_kill_backup`,
		fmt.Sprintf("%d|%d|%d", n+2, (n+2)*(n+3)/2, n+2))
	h.expectReleased(raw, oldPrefix, oldIndex)
	if keys := h.aliveKeys(raw); len(keys) != 0 {
		t.Errorf("lease keys left: %v", keys)
	}
}

// abandon starts a re-key, copies the rows, and stops as a killed process
// does (the lease expires: here its key is deleted).
func (h *sqlHarness) abandon(st *store, raw goredis.UniversalClient, from, to string) *rekeyJob {
	h.t.Helper()
	job, start, err := st.beginRekey(h.ctx, "public", from, to)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := st.moveRows(h.ctx, job, start, time.Now()); err != nil {
		h.t.Fatal(err)
	}
	job.lease.release()
	if err := raw.Del(h.ctx, rekeyAliveKey(job.Owner)).Err(); err != nil {
		h.t.Fatal(err)
	}
	return job
}

// While one connection rolls back an abandoned move, connections that open
// wait for it before they are open, and statements that would change the
// table wait before they go on.
func TestSQLRekeyTakeoverWait(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_tw", "it_rk_tw2")
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`CREATE TABLE it_rk_tw (id INTEGER)`)
	h.exec(`INSERT INTO it_rk_tw VALUES (1), (2)`)
	job := h.abandon(st, raw, "it_rk_tw", "it_rk_tw2")

	// Another connection takes the job over, and is slow to roll it back.
	taken, err := st.takeOver(h.ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if rec := h.rekeyRecord(raw, job.OldPrefix); rec == nil || !rec.Recovering || rec.Owner != taken.Owner {
		t.Fatalf("record after the takeover: %+v", rec)
	}
	type opened struct {
		conn adbc.ConnectionWithContext
		err  error
		at   time.Time
	}
	openedCh := make(chan opened, 1)
	go func() {
		db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(h.ctx, map[string]string{adbc.OptionKeyURI: os.Getenv("REDIS_URI")})
		if err != nil {
			openedCh <- opened{err: err}
			return
		}
		conn, err := db.Open(h.ctx)
		openedCh <- opened{conn, err, time.Now()}
	}()
	insertCh := make(chan error, 1)
	var insertedAt time.Time
	go func() {
		_, err := tryExec(h.ctx, h.conn, `INSERT INTO it_rk_tw VALUES (3)`)
		insertedAt = time.Now()
		insertCh <- err
	}()
	time.Sleep(500 * time.Millisecond)
	rolledBack := time.Now()
	if err := st.rollbackRekey(h.ctx, taken); err != nil {
		t.Fatal(err)
	}
	taken.lease.release()

	o := <-openedCh
	if o.err != nil {
		t.Fatal(o.err)
	}
	defer o.conn.Close(h.ctx)
	if o.at.Before(rolledBack) {
		t.Error("a connection opened while another one was rolling back an abandoned move")
	}
	if err := <-insertCh; err != nil {
		t.Errorf("the waiting statement: %v", err)
	}
	if insertedAt.Before(rolledBack) {
		t.Error("a statement went on while another connection was rolling the move back")
	}
	if _, err := tryExec(h.ctx, o.conn, `INSERT INTO it_rk_tw VALUES (4)`); err != nil {
		t.Errorf("a write on the new connection: %v", err)
	}
	h.expectReleased(raw, job.KeyPrefix, job.IndexName)
	h.expectRows(`SELECT id FROM it_rk_tw ORDER BY id`, "1", "2", "3", "4")
}

// RENAME TO with rename_rekey of a table whose key prefix still has an
// earlier move recorded: a table dropped while its rows were being moved,
// then created again with the same name by an earlier version of the
// driver (v0.0.7), which gave it the dropped table's prefix. This version
// never hands a prefix out again (claimNames), so the test sets that up.
func TestSQLRekeyEarlierRename(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_er", "it_rk_er2", "it_rk_er3")
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`CREATE TABLE it_rk_er (id INTEGER)`)
	n := h.namesN(raw, "public", "it_rk_er", "it_rk_er")
	h.exec(`INSERT INTO it_rk_er VALUES (1), (2)`)
	job, start, err := st.beginRekey(h.ctx, "public", "it_rk_er", "it_rk_er2")
	if err != nil {
		t.Fatal(err)
	}
	defer job.lease.release()
	if err := st.moveRows(h.ctx, job, start, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.exec(`DROP TABLE it_rk_er`)
	// v0.0.7 gave the new table the released prefix, with the generation
	// that DROP TABLE counted: let CREATE TABLE take the prefix, then put
	// the count back and record it in the table's metadata.
	gen, err := raw.HGet(h.ctx, releasedKey, job.OldPrefix).Int64()
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.HDel(h.ctx, releasedKey, job.OldPrefix).Err(); err != nil {
		t.Fatal(err)
	}
	if err := raw.HSet(h.ctx, namesNextKey, rowPrefix("public", "it_rk_er"), n-1).Err(); err != nil {
		t.Fatal(err)
	}
	h.exec(`CREATE TABLE it_rk_er (id INTEGER)`)
	if err := raw.HSet(h.ctx, releasedKey, job.OldPrefix, gen).Err(); err != nil {
		t.Fatal(err)
	}
	if err := st.updateTable(h.ctx, "public", "it_rk_er", func(m *tableMeta) error { m.PrefixGen = gen; return nil }, nil); err != nil {
		t.Fatal(err)
	}
	h.exec(`INSERT INTO it_rk_er VALUES (10)`)
	if prefix, index := h.tableNames(raw, "public", "it_rk_er"); prefix != job.OldPrefix || index != job.OldIndex {
		t.Fatalf("the new table has names %q, %q, not %q, %q", prefix, index, job.OldPrefix, job.OldIndex)
	}
	rk := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	rk.expectErrorExact(`ALTER TABLE it_rk_er RENAME TO it_rk_er3`,
		`I/O: [redis] an earlier rename of table "public"."it_rk_er" is still moving its rows (adbc.redis.rename_rekey); `+
			`try again when it has finished. If the connection doing it has gone away, the next connection to open, `+
			`or the next such rename, rolls it back once its lease (30s) has expired`)

	// Its connection goes away: the next rename rolls it back first.
	job.lease.release()
	if err := raw.Del(h.ctx, rekeyAliveKey(job.Owner)).Err(); err != nil {
		t.Fatal(err)
	}
	rk.exec(`ALTER TABLE it_rk_er RENAME TO it_rk_er3`)
	h.expectReleased(raw, job.KeyPrefix, job.IndexName)
	h.expectReleased(raw, job.OldPrefix, job.OldIndex)
	if h.rekeyPending(raw, job.OldPrefix) {
		t.Error("the earlier move is still recorded")
	}
	h.expectRows(`SELECT id FROM it_rk_er3`, "10")
	h.namesN(raw, "public", "it_rk_er3", "it_rk_er3")
}

// The messages of a statement that runs into a move whose connection went
// away, when it can't go on.
func TestSQLRekeyAbandonedMessages(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_am", "it_rk_am2")
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`CREATE TABLE it_rk_am (id INTEGER)`)
	h.exec(`INSERT INTO it_rk_am VALUES (1)`)

	// Another connection is rolling it back, and takes longer than the
	// statement waits (rekeyTTL, shortened here).
	defer func(was time.Duration) { rekeyTTL = was }(rekeyTTL)
	rekeyTTL = 2 * time.Second
	job := h.abandon(st, raw, "it_rk_am", "it_rk_am2")
	taken, err := st.takeOver(h.ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	h.expectErrorExact(`INSERT INTO it_rk_am VALUES (2)`, `I/O: [redis] table "public"."it_rk_am" was being renamed `+
		`(adbc.redis.rename_rekey) by a connection that has gone away, and another connection is still rolling the rename back; try again in a moment`)
	if err := st.rollbackRekey(h.ctx, taken); err != nil {
		t.Fatal(err)
	}
	taken.lease.release()

	// The table changed otherwise after the rollback (a column was added):
	// the statement can't go on with what it read.
	job = h.abandon(st, raw, "it_rk_am", "it_rk_am2")
	meta, err := st.getTable(h.ctx, "public", "it_rk_am")
	if err != nil {
		t.Fatal(err)
	}
	h2 := newSQLHarness(t) // rolls it back
	h2.exec(`ALTER TABLE it_rk_am ADD COLUMN v INTEGER`)
	err = st.checkWritable(h.ctx, meta)
	want := `I/O: [redis] table "public"."it_rk_am" was being renamed (adbc.redis.rename_rekey) when this statement started, ` +
		`and that has ended since; try again`
	if err == nil || err.Error() != want {
		t.Errorf("got  %v\nwant %s", err, want)
	}

	// The move had switched: it is finished, and the table has its new name.
	job, start, err := st.beginRekey(h.ctx, "public", "it_rk_am", "it_rk_am2")
	if err != nil {
		t.Fatal(err)
	}
	meta, err = st.getTable(h.ctx, "public", "it_rk_am")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.moveRows(h.ctx, job, start, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.switchRekey(h.ctx, job, start); err != nil {
		t.Fatal(err)
	}
	job.lease.release()
	if err := raw.Del(h.ctx, rekeyAliveKey(job.Owner)).Err(); err != nil {
		t.Fatal(err)
	}
	err = st.checkWritable(h.ctx, meta)
	want = `I/O: [redis] table "public"."it_rk_am" was being renamed to "it_rk_am2" (adbc.redis.rename_rekey) by a connection ` +
		`that has gone away; that rename has now been completed`
	if err == nil || err.Error() != want {
		t.Errorf("got  %v\nwant %s", err, want)
	}
	h.expectRows(`SELECT id, v FROM it_rk_am2`, "1|NULL")
	h.expectReleased(raw, job.OldPrefix, job.OldIndex)
	raw2, _ := json.Marshal(h.aliveKeys(raw))
	if string(raw2) != "[]" {
		t.Errorf("lease keys left: %s", raw2)
	}
}

// DROP TABLE of a table whose move was abandoned rolls the move back first,
// so that the move (its record, copies and new names) doesn't outlive the
// table.
func TestSQLRekeyDropAbandoned(t *testing.T) {
	h := newSQLHarness(t)
	h.dropTables("it_rk_drop", "it_rk_drop2")
	raw := h.rawClient()
	st := &store{client: raw}
	h.exec(`CREATE TABLE it_rk_drop (id INTEGER)`)
	h.exec(`INSERT INTO it_rk_drop VALUES (1), (2)`)
	job := h.abandon(st, raw, "it_rk_drop", "it_rk_drop2")
	h.exec(`DROP TABLE it_rk_drop`)
	if h.rekeyPending(raw, job.OldPrefix) {
		t.Error("the abandoned move outlived the table")
	}
	h.expectReleased(raw, job.KeyPrefix, job.IndexName)
	h.expectReleased(raw, job.OldPrefix, job.OldIndex)
	h.exec(`CREATE TABLE it_rk_drop (id INTEGER)`)
	h.exec(`INSERT INTO it_rk_drop VALUES (3)`)
	rk := newRekeyHarness(t, map[string]string{OptionStringRenameRekey: "true"})
	rk.exec(`ALTER TABLE it_rk_drop RENAME TO it_rk_drop2`)
	h.expectRows(`SELECT id FROM it_rk_drop2`, "3")
}
