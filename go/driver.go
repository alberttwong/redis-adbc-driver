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

// Package redis is an ADBC driver for Redis.
//
// Tables are stored as one flat HASH per row and queried through a
// RediSearch index per table with FT.AGGREGATE (see store.go for the key
// layout). SQL is parsed by the driver and translated into FT.AGGREGATE
// pipelines: WHERE predicates on numeric columns become index range queries,
// string equality becomes FILTER steps, ORDER BY becomes SORTBY, LIMIT
// becomes LIMIT, and COUNT(*) becomes GROUPBY 0 REDUCE COUNT. Anything that
// cannot be expressed exactly in RediSearch is evaluated by the driver on the
// rows returned by the pipeline.
package redis

import (
	"context"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// OptionStringDefaultSchema sets the schema (key namespace) used for
	// unqualified table names. Defaults to "public".
	OptionStringDefaultSchema = "adbc.redis.default_schema"
	// OptionStringAddress sets host:port when no URI is given.
	OptionStringAddress = "adbc.redis.address"
	// OptionIntDB selects the logical Redis database number.
	OptionIntDB = "adbc.redis.db"

	// OptionStringIngestIndexColumns (statement) lists the columns that bulk
	// ingest adds to the RediSearch index, comma-separated. "*" (the default)
	// indexes every numeric, temporal, boolean and string column.
	OptionStringIngestIndexColumns = "adbc.redis.ingest.index_columns"
	// OptionStringAggregatePushdown (database or statement) controls which
	// aggregates run inside RediSearch (GROUPBY/REDUCE over SORTABLE fields):
	//   - "exact" (default): only aggregates whose result RediSearch returns
	//     exactly (COUNT, and SUM/MIN/MAX/AVG over 16/32-bit integer, boolean
	//     and date columns); others are computed by the driver from the HASHes.
	//   - "all": also push down floating-point, decimal and 64-bit aggregates,
	//     which RediSearch reports with 12 significant digits.
	//   - "none": always compute aggregates in the driver.
	OptionStringAggregatePushdown = "adbc.redis.aggregate_pushdown"

	// OptionStringCluster (database) selects the client: "auto" (default)
	// uses the OSS Cluster API client when the server reports
	// cluster_enabled:1 and a single-endpoint client otherwise (standalone
	// Redis, or Redis Cloud / Redis Software through their proxy); "true"
	// forces the cluster client; "false" forces the single-endpoint client.
	OptionStringCluster = "adbc.redis.cluster"

	// OptionStringRenameRekey (database or connection) controls ALTER TABLE
	// … RENAME TO: "false" (default) only rewrites metadata, so the table
	// keeps the key prefix and index name it was created with; "true" also
	// moves its rows and index to the new name's (see rekey.go), which
	// takes time proportional to the number of rows.
	OptionStringRenameRekey = "adbc.redis.rename_rekey"

	// OptionStringReadTimeout (database or connection) is how long the
	// client waits for each reply: a duration such as "30s" or "10m", or a
	// number of seconds; "0" means no timeout. It defaults to the URI's
	// read_timeout parameter, or else defaultReadTimeout (see timeout.go).
	// The connection option overrides the database's.
	OptionStringReadTimeout = "adbc.redis.read_timeout"
	// OptionStringWriteTimeout (database or connection) is how long the
	// client waits to send each command, in the same format. It follows the
	// read timeout unless it is set here or in the URI (write_timeout).
	OptionStringWriteTimeout = "adbc.redis.write_timeout"
	// OptionStringTimeZone (database or connection) is the session time
	// zone, as SET TIME ZONE takes it: an IANA zone name, a POSIX-style or
	// numeric offset, or INTERVAL '…'. It is where timestamps with time zone
	// are shown and read as local times (UTC by default), and what RESET
	// timezone goes back to. The connection option overrides the database's
	// and replaces a SET's value; reading it gives the current value, as
	// SHOW timezone does.
	OptionStringTimeZone = "adbc.redis.time_zone"

	PushdownExact = "exact"
	PushdownAll   = "all"
	PushdownNone  = "none"

	DefaultAddress = "localhost:6379"
	driverName     = "ADBC Driver for Redis"
)

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Redis driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) driverbase.DriverWithContext {
	return driverbase.NewDriver(newDriverImpl(alloc))
}

func newDriverImpl(alloc memory.Allocator) *driverImpl {
	info := driverbase.DefaultDriverInfo("Redis")
	info.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      driverName,
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})
	base := driverbase.NewDriverImplBase(info, alloc)
	base.ErrorHelper.DriverName = "redis"
	return &driverImpl{DriverImplBase: base}
}

func validatePushdown(v string) error {
	switch v {
	case PushdownExact, PushdownAll, PushdownNone:
		return nil
	}
	return errorf(adbc.StatusInvalidArgument, "invalid %s %q (want exact, all or none)", OptionStringAggregatePushdown, v)
}

func parseRenameRekey(v string) (bool, error) {
	switch v {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, errorf(adbc.StatusInvalidArgument, "invalid %s %q (want true or false)", OptionStringRenameRekey, v)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.DatabaseWithContext, error) {
	db, err := d.newDatabaseImpl(ctx, opts)
	if err != nil {
		return nil, err
	}
	return driverbase.NewDatabase(db), nil
}

// newDatabaseImpl is a database with the given options set.
func (d *driverImpl) newDatabaseImpl(ctx context.Context, opts map[string]string) (*databaseImpl, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase, driverbase.TracingOptions{})
	if err != nil {
		return nil, err
	}
	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		address:          DefaultAddress,
		schema:           defaultSchema,
		pushdown:         PushdownExact,
		cluster:          "auto",
	}
	if err := db.SetOptions(ctx, opts); err != nil {
		return nil, err
	}
	return db, nil
}
