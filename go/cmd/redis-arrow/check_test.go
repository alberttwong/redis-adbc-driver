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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	redis "github.com/adbc-drivers/redis/go"
	"github.com/apache/arrow-go/v18/arrow/array"
	goredis "github.com/redis/go-redis/v9"
)

// rawClient connects to Redis without the driver, to write an
// application's HASHes.
func rawClient(t *testing.T, uri string) goredis.UniversalClient {
	t.Helper()
	ctx := context.Background()
	opts, err := goredis.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	var c goredis.UniversalClient = goredis.NewClient(opts)
	if info, err := c.Info(ctx, "cluster").Result(); err == nil && strings.Contains(info, "cluster_enabled:1") {
		_ = c.Close()
		c = goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{opts.Addr}, Username: opts.Username,
			Password: opts.Password, TLSConfig: opts.TLSConfig})
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// deleteKeys deletes the keys under a prefix, on every primary.
func deleteKeys(t *testing.T, c goredis.UniversalClient, prefix string) {
	t.Helper()
	del := func(ctx context.Context, n *goredis.Client) error {
		keys, err := n.Keys(ctx, prefix+"*").Result()
		for _, k := range keys {
			if err == nil {
				err = n.Del(ctx, k).Err()
			}
		}
		return err
	}
	ctx := context.Background()
	var err error
	if cc, ok := c.(*goredis.ClusterClient); ok {
		err = cc.ForEachMaster(ctx, del)
	} else {
		err = del(ctx, c.(*goredis.Client))
	}
	if err != nil {
		t.Fatal(err)
	}
}

func runCmd(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), args, strings.NewReader(""), &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

// check, then scan into a file and import it: the HASHes become a table.
func TestCheckScanImport(t *testing.T) {
	uri := os.Getenv("REDIS_URI")
	if uri == "" {
		t.Skip("set REDIS_URI to run the redis-arrow integration test")
	}
	const prefix, table = "it:cli:user:", "it_cli_user"
	c := rawClient(t, uri)
	ctx := context.Background()
	cleanup := func() {
		deleteKeys(t, c, prefix)
		execSQL(t, uri, "DROP TABLE IF EXISTS "+table)
	}
	cleanup()
	t.Cleanup(cleanup)
	for i := 1; i <= 20; i++ {
		if err := c.HSet(ctx, fmt.Sprintf("%s%d", prefix, i), "name", fmt.Sprintf("u%d", i), "age", 20+i,
			"active", []string{"true", "false"}[i%2], "since", fmt.Sprintf("2024-02-%02d", i)).Err(); err != nil {
			t.Fatal(err)
		}
	}

	out, _, err := runCmd(t, "check", "-uri", uri, "-prefix", prefix, "-table", table)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Collection it:cli:user: on Redis",
		"20 HASH keys; the columns are guessed from 20 of them",
		"Guessed columns (as table public.it_cli_user):",
		"Arrow IPC (redis-arrow scan): READY",
		"run: redis-arrow scan -prefix it:cli:user: -o it_cli_user.arrow",
		"SQL, copied into a driver table (redis-arrow scan | redis-arrow import): READY",
		"run: redis-arrow scan -prefix it:cli:user: | redis-arrow import -table it_cli_user",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("check output lacks %q:\n%s", want, out)
		}
	}
	out, _, err = runCmd(t, "check", "-uri", uri, "-prefix", prefix, "-json")
	if err != nil {
		t.Fatal(err)
	}
	var r redis.HashReport
	if err := json.Unmarshal([]byte(out), &r); err != nil || r.Keys != 20 || len(r.Checks) != 2 || !r.Checks[1].Ready {
		t.Errorf("check -json: %v, %+v", err, r)
	}

	path := filepath.Join(t.TempDir(), "users.arrow")
	_, stderr, err := runCmd(t, "scan", "-uri", uri, "-prefix", prefix, "-o", path)
	if err != nil || stderr != "20 rows\n" {
		t.Fatalf("scan: %v, %q", err, stderr)
	}
	rec := readPath(t, path)
	if got := rec.Schema().String(); !strings.Contains(got, "active: type=bool") || !strings.Contains(got, "since: type=date32") {
		t.Errorf("schema %s", got)
	}
	rec.Release()
	importFile(t, "-uri", uri, "-table", table, path)
	if n := count(t, uri, table); n != 20 {
		t.Errorf("%d rows imported", n)
	}
	got, _ := export(t, "-uri", uri, "SELECT SUM(age) AS s FROM "+table+" WHERE active AND since < DATE '2024-02-10'")
	rec = readBytes(t, got)
	defer rec.Release()
	// i = 2, 4, 6, 8: ages 22 + 24 + 26 + 28.
	if s := rec.Column(0).(*array.Int64).Value(0); s != 100 {
		t.Errorf("SUM(age) = %d", s)
	}
}
