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

// Integration tests for the client timeouts (timeout.go).

import (
	"context"
	"net/url"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

// uriWithoutTimeouts is REDIS_URI without its timeout parameters, so that
// the driver's defaults apply.
func uriWithoutTimeouts(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(os.Getenv("REDIS_URI"))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Del("read_timeout")
	q.Del("write_timeout")
	u.RawQuery = q.Encode()
	return u.String()
}

// openWith opens a connection to a URI with database options.
func openWith(t *testing.T, uri string, opts map[string]string) *sqlHarness {
	t.Helper()
	ctx := context.Background()
	all := map[string]string{adbc.OptionKeyURI: uri}
	for k, v := range opts {
		all[k] = v
	}
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, all)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return &sqlHarness{t: t, ctx: ctx, conn: conn}
}

func (h *sqlHarness) getConnOption(key string) string {
	h.t.Helper()
	v, err := h.conn.(adbc.GetSetOptionsWithContext).GetOption(h.ctx, key)
	if err != nil {
		h.t.Fatal(err)
	}
	return v
}

// queryErr runs a query and returns its error.
func (h *sqlHarness) queryErr(sql string) error {
	h.t.Helper()
	st, err := h.conn.NewStatement(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer st.Close(h.ctx)
	if err := st.SetSqlQuery(h.ctx, sql); err != nil {
		h.t.Fatal(err)
	}
	rdr, _, err := st.ExecuteQuery(h.ctx)
	if err == nil {
		for rdr.Next() {
		}
		err = rdr.Err()
		rdr.Release()
	}
	return err
}

// The options on the database and the connection, and that a connection
// keeps working (temporary tables included) when its timeouts change.
func TestTimeoutOptions(t *testing.T) {
	newSQLHarness(t) // skips without REDIS_URI
	uri := uriWithoutTimeouts(t)
	def := openWith(t, uri, nil)
	if r, w := def.getConnOption(OptionStringReadTimeout), def.getConnOption(OptionStringWriteTimeout); r != "5m0s" || w != "5m0s" {
		t.Errorf("defaults: read %s, write %s", r, w)
	}
	h := openWith(t, uri, map[string]string{OptionStringReadTimeout: "45s"})
	if r, w := h.getConnOption(OptionStringReadTimeout), h.getConnOption(OptionStringWriteTimeout); r != "45s" || w != "45s" {
		t.Errorf("database option: read %s, write %s", r, w)
	}
	h.exec(`CREATE TEMP TABLE it_to_tmp (id INTEGER)`)
	h.exec(`INSERT INTO it_to_tmp VALUES (1)`)

	steps := []struct{ key, value, read, write string }{
		{OptionStringReadTimeout, "2m", "2m0s", "2m0s"},
		{OptionStringWriteTimeout, "10", "2m0s", "10s"},
		{OptionStringReadTimeout, "0", "0s", "10s"},
		{OptionStringReadTimeout, "0", "0s", "10s"},
	}
	for _, s := range steps {
		if err := h.setConnOption(s.key, s.value); err != nil {
			t.Fatalf("%s=%s: %v", s.key, s.value, err)
		}
		if r, w := h.getConnOption(OptionStringReadTimeout), h.getConnOption(OptionStringWriteTimeout); r != s.read || w != s.write {
			t.Errorf("after %s=%s: read %s, write %s; want %s, %s", s.key, s.value, r, w, s.read, s.write)
		}
		h.exec(`INSERT INTO it_to_tmp VALUES (2)`)
	}
	err := h.setConnOption(OptionStringReadTimeout, "-3s")
	want := `Invalid Argument: [redis] invalid adbc.redis.read_timeout "-3s" (want a duration such as 30s or 10m, a number of seconds, or 0 for no timeout)`
	if err == nil || err.Error() != want {
		t.Errorf("invalid value: %v", err)
	}
	h.expectRows(`SELECT COUNT(*) FROM it_to_tmp`, "5")
	h.expectRows(`SELECT table_schema, table_type FROM information_schema.tables WHERE table_name = 'it_to_tmp'`,
		"pg_temp|LOCAL TEMPORARY")
}

// pauseTarget returns the node to pause: the server, or on a cluster a
// shard that doesn't hold the driver's metadata, so that the query reads
// its table's metadata and then waits for that shard's part of the search.
func pauseTarget(t *testing.T, ctx context.Context, raw goredis.UniversalClient) (*goredis.Client, bool) {
	t.Helper()
	cc, ok := raw.(*goredis.ClusterClient)
	if !ok {
		return raw.(*goredis.Client), false
	}
	meta, err := cc.MasterForKey(ctx, metaPrefix)
	if err != nil {
		t.Fatal(err)
	}
	var target *goredis.Client
	err = cc.ForEachMaster(ctx, func(ctx context.Context, n *goredis.Client) error {
		if n.Options().Addr != meta.Options().Addr && target == nil {
			target = n
		}
		return nil
	})
	if err != nil || target == nil {
		t.Fatalf("no shard to pause (%v)", err)
	}
	return target, true
}

// A server (or one shard of a cluster) that doesn't answer for a while:
// statements wait with the default timeouts, and fail with a message that
// names the setting with a short one. This pauses the server at REDIS_URI
// for about half a minute, so it only runs with REDIS_PAUSE_TESTS=1.
func TestTimeoutServerPaused(t *testing.T) {
	if os.Getenv("REDIS_PAUSE_TESTS") == "" {
		t.Skip("set REDIS_PAUSE_TESTS=1 to run tests that pause the Redis server at REDIS_URI (CLIENT PAUSE)")
	}
	h := newSQLHarness(t)
	h.dropTables("it_to_paused")
	h.exec(`CREATE TABLE it_to_paused (id INTEGER, label VARCHAR)`)
	h.exec(`INSERT INTO it_to_paused VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	raw := h.rawClient()
	node, cluster := pauseTarget(t, h.ctx, raw)
	var pausedUntil time.Time
	waitPause := func() { time.Sleep(time.Until(pausedUntil) + 100*time.Millisecond) }
	t.Cleanup(waitPause)
	pause := func(d time.Duration) {
		t.Helper()
		waitPause()
		if err := node.Do(h.ctx, "CLIENT", "PAUSE", d.Milliseconds(), "ALL").Err(); err != nil {
			t.Fatal(err)
		}
		pausedUntil = time.Now().Add(d)
	}
	const q = `SELECT COUNT(*), SUM(id) FROM it_to_paused WHERE id >= 1`
	long := 12 * time.Second
	if cluster {
		long = 8 * time.Second
	}
	// With a short timeout, the error depends on which command waited.
	failed := regexp.MustCompile(`^I/O: \[redis\] failed to read table metadata: read tcp \S+->\S+: i/o timeout ` +
		`\(Redis did not reply within the read timeout, 1s: raise adbc\.redis\.read_timeout, or set it to 0 for no timeout\)$`)
	if cluster {
		failed = regexp.MustCompile(`^I/O: \[redis\] FT\.AGGREGATE failed: read tcp \S+->\S+: i/o timeout ` +
			`\(Redis did not reply within the read timeout, 1s: raise adbc\.redis\.read_timeout, or set it to 0 for no timeout\)$`)
	}
	uri := uriWithoutTimeouts(t)

	// The defaults wait.
	def := openWith(t, uri, nil)
	pause(long)
	start := time.Now()
	def.expectRows(q, "3|6")
	if took := time.Since(start); took < long-2*time.Second {
		t.Errorf("the query took %v: was the server paused?", took)
	} else {
		t.Logf("default timeouts: the query took %v", took.Round(10*time.Millisecond))
	}

	// A database option of 1s fails, and the connection works again once
	// the server answers.
	short := openWith(t, uri, map[string]string{OptionStringReadTimeout: "1s"})
	pause(8 * time.Second)
	start = time.Now()
	err := short.queryErr(q)
	if err == nil || !failed.MatchString(err.Error()) {
		t.Errorf("read_timeout=1s: %v", err)
	}
	t.Logf("read_timeout=1s: the query failed after %v", time.Since(start).Round(10*time.Millisecond))
	waitPause()
	short.expectRows(q, "3|6")

	// The same on the connection, and 0 (no timeout) waits.
	if err := def.setConnOption(OptionStringReadTimeout, "1s"); err != nil {
		t.Fatal(err)
	}
	pause(8 * time.Second)
	if err := def.queryErr(q); err == nil || !failed.MatchString(err.Error()) {
		t.Errorf("connection read_timeout=1s: %v", err)
	}
	waitPause()
	if err := def.setConnOption(OptionStringReadTimeout, "0"); err != nil {
		t.Fatal(err)
	}
	pause(3 * time.Second)
	start = time.Now()
	def.expectRows(q, "3|6")
	if took := time.Since(start); took < time.Second {
		t.Errorf("read_timeout=0: the query took %v: was the server paused?", took)
	}
}
