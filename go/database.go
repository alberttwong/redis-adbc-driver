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
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	uri      string
	address  string
	username string
	password string
	db       int
	schema   string
	pushdown string
	cluster  string
	rekey    bool
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.ConnectionWithContext, error) {
	var opts *goredis.Options
	if d.uri != "" {
		parsed, err := goredis.ParseURL(d.uri)
		if err != nil {
			return nil, errorf(adbc.StatusInvalidArgument, "invalid URI: %v", err)
		}
		opts = parsed
	} else {
		opts = &goredis.Options{Addr: d.address, DB: d.db}
	}
	if d.username != "" {
		opts.Username = d.username
	}
	if d.password != "" {
		opts.Password = d.password
	}
	// The driver parses RESP2 replies of FT.AGGREGATE / FT.CURSOR.
	opts.Protocol = 2
	opts.DisableIdentity = true
	client, err := d.connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	st := &store{client: client}
	if err := st.searchDo(ctx, "", "FT._LIST").Err(); err != nil {
		_ = client.Close()
		return nil, errorf(adbc.StatusNotImplemented, "the Redis server does not provide the search (RediSearch) module: %v", err)
	}
	if err := st.refuseFlex(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	if err := st.migrateLegacyMetadata(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	if err := st.ensureRegistry(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	// Finish removing dropped columns' fields left over by earlier connections.
	if err := st.resumeCleanups(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	// Roll back or finish renames that were moving rows when their
	// connection went away (see rekey.go).
	if err := st.resumeRekeys(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	// Drop temporary objects of connections that exited without closing.
	st.sweepTemp(ctx)
	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		store:              st,
		schema:             d.schema,
		pushdown:           d.pushdown,
		rekey:              d.rekey,
	}
	return driverbase.NewConnectionBuilder(conn).
		WithCurrentNamespacer(conn).
		WithDriverInfoPreparer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
}

// flexProbeIndex is an index name the driver never creates.
const flexProbeIndex = "adbc:{meta}:probe"

// refuseFlex fails on Redis Flex (RAM + flash) databases, before anything
// is written. Search on Flex has no FT.AGGREGATE and no NUMERIC fields (the
// Redis Cloud Pro Preview, and Redis Software 8.2 with Search 8.6, which
// also lacks SORTABLE), and every table needs them. Asking for a missing
// index tells Flex apart: other servers answer "No such index", so a Flex
// release that adds FT.AGGREGATE passes.
func (s *store) refuseFlex(ctx context.Context) error {
	err := s.searchDo(ctx, flexProbeIndex, "FT.AGGREGATE", flexProbeIndex, "*", "LIMIT", 0, 0).Err()
	if err != nil && strings.Contains(err.Error(), "not supported in Redis Flex") {
		return errorf(adbc.StatusNotImplemented, "Redis Flex databases are not supported: their search has no "+
			"FT.AGGREGATE and no NUMERIC or SORTABLE fields, which the driver needs (%v)", err)
	}
	return nil
}

// connect opens a single-endpoint or OSS Cluster API client, depending on
// the cluster option and (for "auto") what the server reports.
func (d *databaseImpl) connect(ctx context.Context, opts *goredis.Options) (goredis.UniversalClient, error) {
	useCluster := d.cluster == "true"
	if d.cluster != "true" {
		single := goredis.NewClient(opts)
		if err := single.Ping(ctx).Err(); err != nil {
			_ = single.Close()
			return nil, errorf(adbc.StatusIO, "failed to connect to Redis: %v", err)
		}
		if d.cluster == "false" {
			return single, nil
		}
		// Proxied databases (Redis Cloud, Redis Software) report
		// cluster_enabled:0 or reject the section; both mean single endpoint.
		info, err := single.Info(ctx, "cluster").Result()
		if err != nil || !strings.Contains(info, "cluster_enabled:1") {
			return single, nil
		}
		_ = single.Close()
		useCluster = true
	}
	if !useCluster {
		return nil, errorf(adbc.StatusInternal, "unreachable")
	}
	if opts.DB != 0 {
		return nil, errorf(adbc.StatusInvalidArgument, "Redis Cluster only supports database 0 (got %d)", opts.DB)
	}
	cluster := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:           []string{opts.Addr},
		Username:        opts.Username,
		Password:        opts.Password,
		TLSConfig:       opts.TLSConfig,
		Protocol:        opts.Protocol,
		DisableIdentity: opts.DisableIdentity,
		DialTimeout:     opts.DialTimeout,
		ReadTimeout:     opts.ReadTimeout,
		WriteTimeout:    opts.WriteTimeout,
	})
	if err := cluster.Ping(ctx).Err(); err != nil {
		_ = cluster.Close()
		return nil, errorf(adbc.StatusIO, "failed to connect to Redis Cluster: %v", err)
	}
	return cluster, nil
}

func (d *databaseImpl) Close(ctx context.Context) error { return nil }

func (d *databaseImpl) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case adbc.OptionKeyURI:
		return d.uri, nil
	case adbc.OptionKeyUsername:
		return d.username, nil
	case OptionStringAddress:
		return d.address, nil
	case OptionIntDB:
		return strconv.Itoa(d.db), nil
	case OptionStringDefaultSchema:
		return d.schema, nil
	case OptionStringAggregatePushdown:
		return d.pushdown, nil
	case OptionStringCluster:
		return d.cluster, nil
	case OptionStringRenameRekey:
		return strconv.FormatBool(d.rekey), nil
	}
	return d.DatabaseImplBase.GetOption(ctx, key)
}

func (d *databaseImpl) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case adbc.OptionKeyURI:
		d.uri = value
	case adbc.OptionKeyUsername:
		d.username = value
	case adbc.OptionKeyPassword:
		d.password = value
	case OptionStringAddress:
		d.address = value
	case OptionIntDB:
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return errorf(adbc.StatusInvalidArgument, "invalid %s: %q", key, value)
		}
		d.db = n
	case OptionStringDefaultSchema:
		if value == "" {
			return errorf(adbc.StatusInvalidArgument, "%s must not be empty", key)
		}
		if reservedSchema(value) {
			return errorf(adbc.StatusInvalidArgument, "%s: schema name %q is reserved for temporary tables and views", key, value)
		}
		d.schema = value
	case OptionStringAggregatePushdown:
		if err := validatePushdown(value); err != nil {
			return err
		}
		d.pushdown = value
	case OptionStringCluster:
		switch value {
		case "auto", "true", "false":
			d.cluster = value
		default:
			return errorf(adbc.StatusInvalidArgument, "invalid %s %q (want auto, true or false)", key, value)
		}
	case OptionStringRenameRekey:
		rekey, err := parseRenameRekey(value)
		if err != nil {
			return err
		}
		d.rekey = rekey
	default:
		return d.DatabaseImplBase.SetOption(ctx, key, value)
	}
	return nil
}

func (d *databaseImpl) SetOptions(ctx context.Context, options map[string]string) error {
	for k, v := range options {
		if err := d.SetOption(ctx, k, v); err != nil {
			return err
		}
	}
	return nil
}
