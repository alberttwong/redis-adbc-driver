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

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

// hashFixture is an application's HASHes: it:hc:user:1 … N, written by
// the test, not by the driver.
type hashFixture struct {
	h      *sqlHarness
	raw    goredis.UniversalClient
	opts   map[string]string
	prefix string
}

func newHashFixture(t *testing.T, prefix string) *hashFixture {
	t.Helper()
	h := newSQLHarness(t)
	f := &hashFixture{h: h, raw: h.rawClient(), opts: map[string]string{adbc.OptionKeyURI: os.Getenv("REDIS_URI")}, prefix: prefix}
	f.clear()
	t.Cleanup(f.clear)
	return f
}

// clear deletes the fixture's keys, on every primary of a cluster.
func (f *hashFixture) clear() {
	f.h.t.Helper()
	del := func(ctx context.Context, c *goredis.Client) error {
		keys, err := c.Keys(ctx, globEscape(f.prefix)+"*").Result()
		if err != nil || len(keys) == 0 {
			return err
		}
		for _, k := range keys {
			if err := c.Del(ctx, k).Err(); err != nil {
				return err
			}
		}
		return nil
	}
	var err error
	if cc, ok := f.raw.(*goredis.ClusterClient); ok {
		err = cc.ForEachMaster(f.h.ctx, del)
	} else {
		err = del(f.h.ctx, f.raw.(*goredis.Client))
	}
	if err != nil {
		f.h.t.Fatal(err)
	}
}

func (f *hashFixture) hset(key string, fv ...any) {
	f.h.t.Helper()
	if err := f.raw.HSet(f.h.ctx, key, fv...).Err(); err != nil {
		f.h.t.Fatal(err)
	}
}

// users writes n HASHes of an application's users: user 5 has age "n/a",
// every tenth an avatar that isn't UTF-8.
func (f *hashFixture) users(n int) {
	for i := 1; i <= n; i++ {
		age := fmt.Sprint(20 + i%50)
		if i == 5 {
			age = "n/a"
		}
		fv := []any{"name", fmt.Sprintf("User %d", i), "age", age, "active", []string{"true", "false"}[i%2],
			"joined", fmt.Sprintf("2024-01-%02dT10:00:00Z", 1+i%28), "balance", fmt.Sprintf("%d.%02d", i, i%100),
			"zip", fmt.Sprintf("%05d", i*7)}
		if i%10 == 0 {
			fv = append(fv, "avatar", string([]byte{0xff, byte(i)}))
		}
		f.hset(fmt.Sprintf("%s%d", f.prefix, i), fv...)
	}
}

func (f *hashFixture) inspect(o HashInspectOptions) *HashReport {
	f.h.t.Helper()
	o.Prefix = f.prefix
	r, err := InspectHashes(f.h.ctx, f.opts, o)
	if err != nil {
		f.h.t.Fatal(err)
	}
	return r
}

func checklist(t *testing.T, r *HashReport, name string) Checklist {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no checklist %s in %+v", name, r.Checks)
	return Checklist{}
}

// expectItem checks that a checklist has an item with the status whose
// text has want.
func expectItem(t *testing.T, cl Checklist, status, want string) {
	t.Helper()
	if !slices.ContainsFunc(cl.Items, func(i CheckItem) bool { return i.Status == status && strings.Contains(i.Text, want) }) {
		t.Errorf("%s: no %s item with %q in %+v", cl.Name, status, want, cl.Items)
	}
}

func TestHashInspect(t *testing.T) {
	f := newHashFixture(t, "it:hc:user:")
	f.users(40)
	f.hset("it:hc:user:alice", "name", "Alice", "age", "33")
	f.h.exec(`DROP TABLE IF EXISTS it_hc_user`)
	r := f.inspect(HashInspectOptions{KeyColumn: "_key", Table: "it_hc_user"})
	if r.Keys != 41 || r.Sampled != 41 {
		t.Errorf("keys %d, sampled %d", r.Keys, r.Sampled)
	}
	var cols []string
	for _, c := range r.Columns {
		cols = append(cols, c.Name+" "+c.Type)
	}
	want := []string{"_key VARCHAR", "name VARCHAR", "age VARCHAR", "active BOOLEAN", "joined TIMESTAMP(6) WITH TIME ZONE",
		"balance NUMERIC(38,2)", "zip VARCHAR", "avatar VARBINARY"}
	slices.Sort(cols)
	slices.Sort(want)
	if strings.Join(cols, "|") != strings.Join(want, "|") {
		t.Errorf("columns %q\nwant %q", cols, want)
	}
	ipc := checklist(t, r, ChecklistArrowIPC)
	if !ipc.Ready || !strings.HasPrefix(ipc.Command, "redis-arrow scan -prefix it:hc:user:") {
		t.Errorf("IPC: %+v", ipc)
	}
	expectItem(t, ipc, CheckWarn, `age is VARCHAR: 1 of 41 values aren't BIGINT (such as "n/a" in it:hc:user:5)`)
	cp := checklist(t, r, ChecklistSQLCopy)
	if !cp.Ready || cp.Command != "redis-arrow scan -prefix it:hc:user: | redis-arrow import -table it_hc_user" {
		t.Errorf("copy: %+v", cp)
	}
	expectItem(t, cp, CheckOK, "public.it_hc_user doesn't exist yet")
	expectItem(t, cp, CheckOK, "a copy needs about")

	// A sample of 10 says so; -type checks the sampled values.
	r = f.inspect(HashInspectOptions{Sample: 10, Types: map[string]string{"zip": "INTEGER"}})
	if r.Keys != 41 || r.Sampled != 10 {
		t.Errorf("keys %d, sampled %d", r.Keys, r.Sampled)
	}
	expectItem(t, checklist(t, r, ChecklistArrowIPC), CheckWarn, "the columns are guessed from 10 of the 41 HASHes")
}

// What a copy can't do: the names it would take are a view's, or have keys.
func TestHashInspectCopyBlockers(t *testing.T) {
	f := newHashFixture(t, "it:hc:blk:")
	f.users(3)
	f.h.exec(`DROP VIEW IF EXISTS it_hc_view`)
	f.h.exec(`DROP TABLE IF EXISTS it_hc_exists`)
	f.h.exec(`CREATE VIEW it_hc_view AS SELECT 1 AS x`)
	f.h.exec(`CREATE TABLE it_hc_exists (x INTEGER)`)
	t.Cleanup(func() {
		f.h.exec(`DROP VIEW IF EXISTS it_hc_view`)
		f.h.exec(`DROP TABLE IF EXISTS it_hc_exists`)
	})

	cp := checklist(t, f.inspect(HashInspectOptions{Table: "it_hc_view"}), ChecklistSQLCopy)
	if cp.Ready || cp.Command != "" {
		t.Errorf("view: %+v", cp)
	}
	expectItem(t, cp, CheckBlocker, "public.it_hc_view is a view")
	cp = checklist(t, f.inspect(HashInspectOptions{Table: "it_hc_exists"}), ChecklistSQLCopy)
	if !cp.Ready {
		t.Errorf("existing table: %+v", cp)
	}
	expectItem(t, cp, CheckWarn, "the table public.it_hc_exists exists")

	// Keys already under the prefix the table would take.
	st := &store{client: f.raw}
	last, err := st.lastNames(f.h.ctx, f.raw, "public", "it_hc_taken")
	if err != nil {
		t.Fatal(err)
	}
	taken := namesFor("public", "it_hc_taken", last+1).prefix + "1"
	f.hset(taken, "x", "1")
	t.Cleanup(func() { _ = f.raw.Del(f.h.ctx, taken).Err() })
	cp = checklist(t, f.inspect(HashInspectOptions{Table: "it_hc_taken"}), ChecklistSQLCopy)
	if cp.Ready {
		t.Errorf("taken prefix: %+v", cp)
	}
	expectItem(t, cp, CheckBlocker, "1 key is already under")

	// The collection itself is under the prefix a table would take.
	cp = checklist(t, f.inspect(HashInspectOptions{Schema: "it", Table: "hc"}), ChecklistSQLCopy)
	expectItem(t, cp, CheckBlocker, "among the collection's own keys")

	// No HASHes at all.
	g := newHashFixture(t, "it:hc:none:")
	r := g.inspect(HashInspectOptions{})
	expectItem(t, checklist(t, r, ChecklistArrowIPC), CheckBlocker, "no HASH keys under it:hc:none:")
}

// Without a prefix, the collections are listed: the application's, and
// driver tables as tables.
func TestHashDiscover(t *testing.T) {
	f := newHashFixture(t, "it:hc:disc:")
	f.users(3)
	f.h.exec(`DROP TABLE IF EXISTS it_hc_disc`)
	f.h.exec(`CREATE TABLE it_hc_disc (x INTEGER)`)
	f.h.exec(`INSERT INTO it_hc_disc VALUES (1)`)
	t.Cleanup(func() { f.h.exec(`DROP TABLE IF EXISTS it_hc_disc`) })
	r, err := InspectHashes(f.h.ctx, f.opts, HashInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prefix := f.h.tablePrefix(f.raw, "public", "it_hc_disc")
	var app, table bool
	for _, c := range r.Candidates {
		app = app || (c.Prefix == f.prefix && c.Table == "" && c.Keys == 3)
		table = table || (c.Prefix == prefix && c.Table == "public.it_hc_disc")
	}
	if !app || !table {
		t.Errorf("candidates %+v: app %v, table %v", r.Candidates, app, table)
	}
}

// scanAll reads every batch of a scan into rows of "k=v" cells, by key.
func scanAll(t *testing.T, s *HashScan) map[string]string {
	t.Helper()
	out := map[string]string{}
	for s.Next() {
		rec := s.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			var cells []string
			key := ""
			for c := 0; c < int(rec.NumCols()); c++ {
				v := "NULL"
				if !rec.Column(c).IsNull(r) {
					v = rec.Column(c).ValueStr(r)
				}
				if rec.ColumnName(c) == "_key" {
					key = v
					continue
				}
				cells = append(cells, rec.ColumnName(c)+"="+v)
			}
			if _, dup := out[key]; dup {
				t.Errorf("%s read twice", key)
			}
			out[key] = strings.Join(cells, " ")
		}
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestHashScan(t *testing.T) {
	f := newHashFixture(t, "it:hc:scan:")
	f.users(2500) // more than a batch's worth of SCAN pages
	s, err := ScanHashes(f.h.ctx, f.opts, HashScanOptions{Prefix: f.prefix, KeyColumn: "_key", Sample: -1})
	if err != nil {
		t.Fatal(err)
	}
	rows := scanAll(t, s)
	s.Release()
	if len(rows) != 2500 {
		t.Fatalf("%d rows", len(rows))
	}
	if got, want := rows["it:hc:scan:10"], "name=User 10 age=30 active=true joined=2024-01-11T10:00:00Z balance=10.1 zip=00070 avatar=/wo="; got != want {
		t.Errorf("it:hc:scan:10: %s\nwant %s", got, want)
	}
	if got := rows["it:hc:scan:11"]; !strings.HasSuffix(got, "avatar=NULL") {
		t.Errorf("it:hc:scan:11: %s", got)
	}

	// Guessed from a sample without the "n/a": the error says what to do.
	s, err = ScanHashes(f.h.ctx, f.opts, HashScanOptions{Prefix: f.prefix, KeyColumn: "_key", Sample: 3})
	if err != nil {
		t.Fatal(err)
	}
	for s.Next() {
	}
	if err := s.Err(); err == nil || !strings.Contains(err.Error(), "the type guessed from 3 sampled HASHes") {
		t.Errorf("got %v", err)
	}
	s.Release()

	// -type age=BIGINT: "n/a" fails the scan, or reads as NULL.
	opts := HashScanOptions{Prefix: f.prefix, KeyColumn: "_key", Types: map[string]string{"age": "BIGINT"}}
	s, err = ScanHashes(f.h.ctx, f.opts, opts)
	if err != nil {
		t.Fatal(err)
	}
	for s.Next() {
	}
	if err := s.Err(); err == nil || !strings.Contains(err.Error(), `it:hc:scan:5, field "age": "n/a" doesn't convert to BIGINT`) {
		t.Errorf("got %v", err)
	}
	s.Release()
	opts.NullOnError = true
	s, err = ScanHashes(f.h.ctx, f.opts, opts)
	if err != nil {
		t.Fatal(err)
	}
	rows = scanAll(t, s)
	if !strings.Contains(rows["it:hc:scan:5"], "age=NULL") || s.Stats().Nulls["age"] != 1 {
		t.Errorf("it:hc:scan:5: %s; nulls %v", rows["it:hc:scan:5"], s.Stats().Nulls)
	}
	s.Release()
}

// A field that the sample doesn't have is skipped and counted.
func TestHashScanUnknownFields(t *testing.T) {
	f := newHashFixture(t, "it:hc:unk:")
	for i := 1; i <= 5; i++ {
		f.hset(fmt.Sprintf("%s%d", f.prefix, i), "a", i, fmt.Sprintf("only%d", i), "x")
	}
	s, err := ScanHashes(f.h.ctx, f.opts, HashScanOptions{Prefix: f.prefix, Sample: 1, KeyColumn: "_key"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	scanAll(t, s)
	if got := len(s.Stats().UnknownFields); got != 4 || s.Stats().Rows != 5 {
		t.Errorf("unknown fields %v, rows %d", s.Stats().UnknownFields, s.Stats().Rows)
	}
	if _, err := ScanHashes(f.h.ctx, f.opts, HashScanOptions{Prefix: "it:hc:nothing:"}); err == nil ||
		!strings.Contains(err.Error(), `no HASH keys under "it:hc:nothing:"`) {
		t.Errorf("empty prefix: %v", err)
	}
}

// The scan bulk-ingests into a driver table, which SQL then reads.
func TestHashScanIngest(t *testing.T) {
	f := newHashFixture(t, "it:hc:cp:")
	f.users(30)
	f.h.exec(`DROP TABLE IF EXISTS it_hc_copy`)
	t.Cleanup(func() { f.h.exec(`DROP TABLE IF EXISTS it_hc_copy`) })
	s, err := ScanHashes(f.h.ctx, f.opts, HashScanOptions{Prefix: f.prefix, KeyColumn: "_key",
		Types: map[string]string{"age": "INTEGER"}, NullOnError: true})
	if err != nil {
		t.Fatal(err)
	}
	st, err := f.h.conn.NewStatement(f.h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(f.h.ctx)
	if err := st.SetOption(f.h.ctx, adbc.OptionKeyIngestTargetTable, "it_hc_copy"); err != nil {
		t.Fatal(err)
	}
	if err := st.BindStream(f.h.ctx, s); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ExecuteUpdate(f.h.ctx); err != nil || n != 30 {
		t.Fatalf("ingest: %d, %v", n, err)
	}
	f.h.expectRows(`SELECT COUNT(*), COUNT(age), SUM(balance) FROM it_hc_copy WHERE active`, "15|15|242.40")
	f.h.expectRows(`SELECT _key, joined, zip FROM it_hc_copy WHERE age = 31 ORDER BY _key`,
		"it:hc:cp:11|2024-01-12T10:00:00Z|00077")
	f.h.expectRows(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'it_hc_copy' AND column_name IN ('active', 'age', 'joined') ORDER BY 1`,
		"active|BOOLEAN", "age|INTEGER", "joined|TIMESTAMP(6) WITH TIME ZONE")
}

// The ACL rules the README documents for scan and check.
var (
	aclScanCommands      = []string{"+ping", "+info", "+scan", "+hgetall"}
	aclCheckCommands     = []string{"+ft._list", "+ft.info", "+ft.aggregate", "+memory|usage"}
	aclCheckMetaCommands = []string{"+get", "+exists", "+smembers", "+sismember", "+hget", "+hexists"}
)

// Users with exactly the documented rules run scan and check without a
// command refused, and check leaves out the indexes they can't read.
func TestACLHashTools(t *testing.T) {
	raw, err := os.ReadFile("../README.md")
	if err != nil {
		t.Skip(err)
	}
	for _, rules := range [][]string{aclScanCommands, aclCheckCommands, aclCheckMetaCommands} {
		if line := strings.Join(rules, " "); !strings.Contains(string(raw), line) {
			t.Errorf("the README doesn't list these commands as tested: %s", line)
		}
	}
	a := newACLHarness(t)
	f := newHashFixture(t, "it:hc:acl:")
	f.users(5)
	keys := []string{"%R~it:hc:acl:*"}
	scanRules := slices.Clone(aclScanCommands)
	if a.cluster {
		scanRules = append(scanRules, aclClusterCommands...)
	}
	a.user("it_hc_scan", append(scanRules, keys...)...)
	checkRules := slices.Concat(scanRules, aclCheckCommands, aclCheckMetaCommands, keys, []string{"%R~adbc:*"})
	a.user("it_hc_check", checkRules...)

	s, err := ScanHashes(a.ctx, map[string]string{adbc.OptionKeyURI: a.userURI("it_hc_scan")}, HashScanOptions{Prefix: f.prefix, KeyColumn: "_key"})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(scanAll(t, s)); n != 5 {
		t.Errorf("scan read %d rows", n)
	}
	s.Release()
	a.expectNoDenials("it_hc_scan")

	r, err := InspectHashes(a.ctx, map[string]string{adbc.OptionKeyURI: a.userURI("it_hc_check")}, HashInspectOptions{Prefix: f.prefix})
	if err != nil {
		t.Fatal(err)
	}
	for _, cl := range r.Checks {
		for _, it := range cl.Items {
			if strings.Contains(it.Text, "couldn't") || it.Status == CheckBlocker {
				t.Errorf("%s: %+v", cl.Name, it)
			}
		}
	}
	if !r.Server.Search || r.Keys != 5 {
		t.Errorf("search %v, keys %d", r.Server.Search, r.Keys)
	}
	a.expectNoDenials("it_hc_check")
}
