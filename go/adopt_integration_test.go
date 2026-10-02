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
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

// adopt runs AdoptHashes on the fixture's prefix.
func (f *hashFixture) adopt(o HashAdoptOptions) *HashAdoption {
	f.h.t.Helper()
	o.Prefix = f.prefix
	res, err := AdoptHashes(f.h.ctx, f.opts, o)
	if err != nil {
		f.h.t.Fatal(err)
	}
	return res
}

// dropAdopted drops a table (an adopted one keeps the HASHes).
func (f *hashFixture) dropAdopted(table string) {
	f.h.exec(`DROP TABLE IF EXISTS ` + table)
}

func TestAdoptHashes(t *testing.T) {
	f := newHashFixture(t, "it:ad:user:")
	f.dropAdopted("it_ad_users")
	f.dropAdopted("it_ad_users_r")
	t.Cleanup(func() {
		f.dropAdopted("it_ad_users")
		f.dropAdopted("it_ad_users_r")
	})
	f.users(30)
	f.hset("it:ad:user:alice", "name", "Alice", "age", "33")
	// One HASH already has its __rowid.
	f.hset("it:ad:user:7", rowIDField, 7)

	// A dry run changes nothing.
	res := f.adopt(HashAdoptOptions{Table: "it_ad_users"})
	cl := res.Report.Checks[0]
	if res.Applied || !cl.Ready || len(res.Steps) == 0 {
		t.Fatalf("dry run: %+v", res)
	}
	expectItem(t, cl, CheckMissing, "29 HASHes have no __rowid field")
	expectItem(t, cl, CheckWarn, "1 key (such as it:ad:user:alice) doesn't end in an integer row id")
	expectItem(t, cl, CheckWarn, "active is VARCHAR in place (BOOLEAN when copied)")
	if n, _ := f.raw.Exists(f.h.ctx, metaKey("public", "it_ad_users")).Result(); n != 0 {
		t.Fatal("the dry run wrote the metadata")
	}
	if v, _ := f.raw.HGet(f.h.ctx, "it:ad:user:1", rowIDField).Result(); v != "" {
		t.Fatalf("the dry run wrote __rowid %q", v)
	}

	res = f.adopt(HashAdoptOptions{Table: "it_ad_users", Apply: true})
	if !res.Applied || res.RowIDsWritten != 29 {
		t.Fatalf("adopt: %+v", res)
	}
	h := f.h
	h.expectRows(`SELECT COUNT(*) FROM it_ad_users`, "30")
	h.expectRows(`SELECT name, age, balance, zip FROM it_ad_users WHERE __rowid = 12`, "User 12|32|12.12|00084")
	// age holds "n/a" (user 5), so it is VARCHAR; balance is NUMERIC in the
	// index, so this runs there.
	h.expectRows(`SELECT COUNT(*), SUM(balance) FROM it_ad_users WHERE balance >= 25`, "6|166.65")
	h.expectRows(`SELECT name FROM it_ad_users WHERE zip = '00070' AND active = 'true'`, "User 10")
	h.expectRows(`SELECT data_type FROM information_schema.columns WHERE table_name = 'it_ad_users' AND column_name = 'balance'`,
		"NUMERIC(38,2)")

	// UPDATE and DELETE change the application's HASHes.
	h.exec(`UPDATE it_ad_users SET name = 'Renamed' WHERE __rowid = 3`)
	if v, _ := f.raw.HGet(h.ctx, "it:ad:user:3", "name").Result(); v != "Renamed" {
		t.Errorf("UPDATE wrote %q", v)
	}
	h.exec(`DELETE FROM it_ad_users WHERE __rowid = 4`)
	if n, _ := f.raw.Exists(h.ctx, "it:ad:user:4").Result(); n != 0 {
		t.Error("DELETE left it:ad:user:4")
	}

	// A HASH the application adds is left out until -refresh.
	f.hset("it:ad:user:31", "name", "User 31", "age", "40", "balance", "1.00", "zip", "00217", "extra", "x")
	f.hset("it:ad:user:32", "name", "User 32", "balance", "lots")
	h.expectRows(`SELECT COUNT(*) FROM it_ad_users`, "29")
	r := f.inspect(HashInspectOptions{})
	expectItem(t, checklist(t, r, ChecklistSQLInPlace), CheckMissing, "2 HASHes written since have no __rowid field")
	res = f.adopt(HashAdoptOptions{Refresh: true})
	if res.Applied || res.Pending != 1 || res.Left != 1 || !strings.Contains(res.LeftExample, `it:ad:user:32: balance "lots"`) {
		t.Errorf("refresh dry run: %+v", res)
	}
	res = f.adopt(HashAdoptOptions{Refresh: true, Apply: true})
	if !res.Applied || res.RowIDsWritten != 1 || res.UnknownFields["extra"] != 1 {
		t.Errorf("refresh: %+v", res)
	}
	h.expectRows(`SELECT COUNT(*) FROM it_ad_users`, "30")
	h.expectRows(`SELECT name FROM it_ad_users WHERE __rowid = 31`, "User 31")

	// What an adopted table refuses, and what DROP TABLE keeps.
	for _, c := range []struct{ sql, want string }{
		{`INSERT INTO it_ad_users (name) VALUES ('x')`, "adding rows (INSERT, MERGE … INSERT, bulk ingest) isn't supported"},
		{`TRUNCATE it_ad_users`, "TRUNCATE isn't supported"},
		{`ALTER TABLE it_ad_users ADD COLUMN extra VARCHAR`, "ADD COLUMN isn't supported"},
	} {
		h.expectError(c.sql, c.want)
	}
	if err := h.setConnOption(OptionStringRenameRekey, "true"); err != nil {
		t.Fatal(err)
	}
	h.expectError(`ALTER TABLE it_ad_users RENAME TO it_ad_users2`, "RENAME TO with adbc.redis.rename_rekey")
	if err := h.setConnOption(OptionStringRenameRekey, "false"); err != nil {
		t.Fatal(err)
	}
	h.exec(`ALTER TABLE it_ad_users RENAME TO it_ad_users_r`)
	h.expectRows(`SELECT COUNT(*) FROM it_ad_users_r WHERE __rowid <= 10`, "9")
	h.exec(`ALTER TABLE it_ad_users_r RENAME TO it_ad_users`)
	h.exec(`ALTER TABLE it_ad_users DROP COLUMN zip`)
	if v, _ := f.raw.HGet(h.ctx, "it:ad:user:1", "zip").Result(); v != "00007" {
		t.Errorf("DROP COLUMN removed the application's field: %q", v)
	}
	h.exec(`DROP TABLE it_ad_users`)
	if n := f.h.prefixKeyCount(f.raw, f.prefix); n != 32 {
		t.Errorf("DROP TABLE left %d of the 32 HASHes", n)
	}
	if v, _ := f.raw.HGet(h.ctx, "it:ad:user:1", rowIDField).Result(); v != "1" {
		t.Errorf("__rowid after DROP TABLE: %q", v)
	}

	// Adopted again after the DROP: the HASHes have their __rowid already.
	if err := f.raw.Del(h.ctx, "it:ad:user:32").Err(); err != nil {
		t.Fatal(err)
	}
	res = f.adopt(HashAdoptOptions{Table: "it_ad_users", Apply: true})
	if !res.Applied || res.RowIDsWritten != 0 {
		t.Errorf("adopt again: %+v", res)
	}
	h.expectRows(`SELECT COUNT(*) FROM it_ad_users`, "30")
}

// A schema named after the adopted prefix can't get tables: their rows
// would be among the application's HASHes.
func TestAdoptedPrefixOverlap(t *testing.T) {
	f := newHashFixture(t, "it_ad_s:")
	f.dropAdopted("it_ad_ov")
	f.h.exec(`DROP SCHEMA IF EXISTS it_ad_s CASCADE`)
	t.Cleanup(func() {
		f.dropAdopted("it_ad_ov")
		f.h.exec(`DROP SCHEMA IF EXISTS it_ad_s CASCADE`)
	})
	f.hset("it_ad_s:1", "n", "1")
	f.adopt(HashAdoptOptions{Table: "it_ad_ov", Apply: true})
	f.h.exec(`CREATE SCHEMA it_ad_s`)
	f.h.expectError(`CREATE TABLE it_ad_s.t (a INTEGER)`, `under the key prefix "it_ad_s:" of an adopted table's HASHes`)
}

// What adopt refuses before it writes anything.
func TestAdoptBlockers(t *testing.T) {
	f := newHashFixture(t, "it:ad:blk:")
	f.dropAdopted("it_ad_blk")
	t.Cleanup(func() { f.dropAdopted("it_ad_blk") })
	f.hset("it:ad:blk:1", "n", "1", rowIDField, "9")
	f.hset("it:ad:blk:x", "n", "2", rowIDField, "2")
	f.hset("it:ad:blk:3", "n", "3", "flag", "true")
	res, err := AdoptHashes(f.h.ctx, f.opts, HashAdoptOptions{Prefix: f.prefix, Table: "it_ad_blk", Apply: true,
		Types: map[string]string{"flag": "BOOLEAN"}})
	if err != nil {
		t.Fatal(err)
	}
	cl := res.Report.Checks[0]
	if res.Applied || cl.Ready {
		t.Fatalf("adopted: %+v", res)
	}
	expectItem(t, cl, CheckBlocker, `1 HASH has a __rowid that isn't the key's suffix (it:ad:blk:1 has "9")`)
	expectItem(t, cl, CheckBlocker, "1 key without a row id suffix has a __rowid field (such as it:ad:blk:x)")
	expectItem(t, cl, CheckBlocker, `-type flag=BOOLEAN: 1 value doesn't fit (such as "true" in it:ad:blk:3: the driver stores booleans as 0 and 1)`)
	if n, _ := f.raw.Exists(f.h.ctx, metaKey("public", "it_ad_blk")).Result(); n != 0 {
		t.Error("a refused adopt wrote the metadata")
	}
	if ok, _ := f.raw.SIsMember(f.h.ctx, adoptedKey, f.prefix).Result(); ok {
		t.Error("a refused adopt reserved the prefix")
	}

	// A prefix that a driver table's rows are under.
	f.h.exec(`DROP TABLE IF EXISTS it_ad_tbl`)
	f.h.exec(`CREATE TABLE it_ad_tbl (a INTEGER)`)
	t.Cleanup(func() { f.h.exec(`DROP TABLE IF EXISTS it_ad_tbl`) })
	res, err = AdoptHashes(f.h.ctx, f.opts, HashAdoptOptions{Prefix: "public:", Table: "it_ad_pub"})
	if err != nil {
		t.Fatal(err)
	}
	expectItem(t, res.Report.Checks[0], CheckBlocker, "can be rows of the driver table")
}

// adopt stops and undoes what it did if the index can't index a HASH
// (here a value written between its read and the index).
func TestAdoptIndexFailure(t *testing.T) {
	f := newHashFixture(t, "it:ad:fail:")
	f.dropAdopted("it_ad_fail")
	t.Cleanup(func() { f.dropAdopted("it_ad_fail") })
	for i := 1; i <= 5; i++ {
		f.hset(fmt.Sprintf("%s%d", f.prefix, i), "n", i)
	}
	o := HashAdoptOptions{Prefix: f.prefix, Table: "it_ad_fail", Apply: true}
	o.Progress = func(msg string) {
		if strings.HasPrefix(msg, "created the index") {
			f.hset(f.prefix+"2", "n", "two", rowIDField, 2)
		}
	}
	_, err := AdoptHashes(f.h.ctx, f.opts, o)
	if err == nil || !strings.Contains(err.Error(), "couldn't index 1 HASH (such as it:ad:fail:2") {
		t.Fatalf("got %v", err)
	}
	st := &store{client: f.raw}
	idx := adoptIndexName("public", "it_ad_fail")
	if err := st.searchDo(f.h.ctx, idx, "FT.INFO", idx).Err(); err == nil || !isUnknownIndex(err) {
		t.Errorf("the index is still there: %v", err)
	}
	for _, k := range []string{adoptedKey, prefixesKey} {
		if ok, _ := f.raw.SIsMember(f.h.ctx, k, f.prefix).Result(); ok {
			t.Errorf("%s still has the prefix", k)
		}
	}
	if n := f.h.prefixKeyCount(f.raw, f.prefix); n != 5 {
		t.Errorf("%d HASHes left", n)
	}
	// Fixed, it adopts.
	f.hset(f.prefix+"2", "n", "2")
	o.Progress = nil
	if res, err := AdoptHashes(f.h.ctx, f.opts, o); err != nil || !res.Applied {
		t.Fatalf("%+v, %v", res, err)
	}
	f.h.expectRows(`SELECT SUM(n) FROM it_ad_fail`, "15")
}

// The commands adopt needs besides the driver's (README).
var aclAdoptCommands = []string{"+scan", "+eval", "+evalsha", "+script|load"}

// A user with the driver's rules and adopt's adopts, and then queries.
func TestACLAdopt(t *testing.T) {
	raw, err := os.ReadFile("../README.md")
	if err != nil {
		t.Skip(err)
	}
	if line := strings.Join(aclAdoptCommands, " "); !strings.Contains(string(raw), line) {
		t.Errorf("the README doesn't list adopt's commands as tested: %s", line)
	}
	a := newACLHarness(t)
	f := newHashFixture(t, "it:ad:acl:")
	f.dropAdopted("it_ad_acl")
	t.Cleanup(func() { f.dropAdopted("it_ad_acl") })
	f.users(5)
	a.user("it_ad_acl", slices.Concat(a.driverCommands(false), aclAdoptCommands, []string{"~adbc:*", "~public:*", "~it:ad:acl:*"})...)
	res, err := AdoptHashes(a.ctx, map[string]string{adbc.OptionKeyURI: a.userURI("it_ad_acl")},
		HashAdoptOptions{Prefix: f.prefix, Table: "it_ad_acl", Apply: true})
	if err != nil || !res.Applied {
		t.Fatalf("%+v, %v", res, err)
	}
	u := a.connect("it_ad_acl", nil)
	u.expectRows(`SELECT COUNT(*) FROM it_ad_acl`, "5")
	a.expectNoDenials("it_ad_acl")

	// A reader needs the read-only rules and read access to the HASHes.
	a.user("it_ad_reader", slices.Concat(a.readOnlyCommands(), []string{"%R~adbc:*", "%R~it:ad:acl:*"})...)
	reader := a.connect("it_ad_reader", nil)
	reader.expectRows(`SELECT name FROM it_ad_acl WHERE __rowid = 2`, "User 2")
	reader.expectRows(`SELECT COUNT(*) FROM it_ad_acl WHERE active = 'true'`, "2")
	a.expectNoDenials("it_ad_reader")
}

// An adopt that stopped after reserving the names (its process killed)
// is finished by running it again; another table can't take the prefix
// meanwhile.
func TestAdoptResume(t *testing.T) {
	f := newHashFixture(t, "it:ad:res:")
	f.dropAdopted("it_ad_res")
	t.Cleanup(func() {
		f.dropAdopted("it_ad_res")
		_ = f.raw.HDel(f.h.ctx, adoptingKey, f.prefix).Err()
		for _, k := range []string{prefixesKey, adoptedKey} {
			_ = f.raw.SRem(f.h.ctx, k, f.prefix).Err()
		}
	})
	for i := 1; i <= 3; i++ {
		f.hset(fmt.Sprintf("%s%d", f.prefix, i), "n", i)
	}
	idx := adoptIndexName("public", "it_ad_res")
	rec := `{"schema":"public","table":"it_ad_res","index":"` + idx + `"}`
	pipe := f.raw.TxPipeline()
	pipe.SAdd(f.h.ctx, prefixesKey, f.prefix)
	pipe.SAdd(f.h.ctx, adoptedKey, f.prefix)
	pipe.SAdd(f.h.ctx, indexesKey, idx)
	pipe.HSet(f.h.ctx, adoptingKey, f.prefix, rec)
	if _, err := pipe.Exec(f.h.ctx); err != nil {
		t.Fatal(err)
	}
	res := f.adopt(HashAdoptOptions{Table: "it_ad_other"})
	expectItem(t, res.Report.Checks[0], CheckBlocker, "an adopt of it:ad:res: into public.it_ad_res stopped before it finished")
	res = f.adopt(HashAdoptOptions{Table: "it_ad_res"})
	expectItem(t, res.Report.Checks[0], CheckWarn, "an adopt of it:ad:res: into public.it_ad_res stopped before it finished")
	if !res.Report.Checks[0].Ready {
		t.Fatalf("not ready: %+v", res.Report.Checks[0])
	}
	if res = f.adopt(HashAdoptOptions{Table: "it_ad_res", Apply: true}); !res.Applied {
		t.Fatalf("%+v", res)
	}
	f.h.expectRows(`SELECT SUM(n) FROM it_ad_res`, "6")
	if n, _ := f.raw.HExists(f.h.ctx, adoptingKey, f.prefix).Result(); n {
		t.Error("the adopting record is still there")
	}
}
