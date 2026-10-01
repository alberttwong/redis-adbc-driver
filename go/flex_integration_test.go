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

// Connecting to a Redis Flex database fails with a clear error. The test runs
// against the Flex database at REDIS_FLEX_URI (flex-setup.sh creates one) and
// is skipped when it is unset:
//
//	REDIS_FLEX_URI=redis://localhost:12000/0 go test -run TestFlex ./...

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

func TestFlexRefused(t *testing.T) {
	uri := os.Getenv("REDIS_FLEX_URI")
	if uri == "" {
		t.Skip("set REDIS_FLEX_URI to a Redis Flex database to run this test")
	}
	ctx := context.Background()
	opts, err := goredis.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	raw := goredis.NewClient(opts)
	defer raw.Close()
	keysBefore, err := raw.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}

	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{adbc.OptionKeyURI: uri})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	conn, err := db.Open(ctx)
	if err == nil {
		_ = conn.Close(ctx)
		t.Fatal("connected to a Redis Flex database; want an error")
	}
	var ae adbc.Error
	if !errors.As(err, &ae) || ae.Code != adbc.StatusNotImplemented ||
		!strings.Contains(ae.Msg, "Redis Flex databases are not supported") {
		t.Fatalf("got %v, want a NotImplemented error about Redis Flex", err)
	}

	// The driver refuses before writing its metadata.
	if keysAfter, err := raw.DBSize(ctx).Result(); err != nil || keysAfter != keysBefore {
		t.Errorf("database has %d keys after the refused connection, %d before (err %v)", keysAfter, keysBefore, err)
	}
}
