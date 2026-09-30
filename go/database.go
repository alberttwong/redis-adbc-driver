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
	client := goredis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, errorf(adbc.StatusIO, "failed to connect to Redis: %v", err)
	}
	st := &store{client: client}
	if err := st.client.Do(ctx, "FT._LIST").Err(); err != nil {
		_ = client.Close()
		return nil, errorf(adbc.StatusNotImplemented, "the Redis server does not provide the search (RediSearch) module: %v", err)
	}
	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		store:              st,
		schema:             d.schema,
		pushdown:           d.pushdown,
	}
	return driverbase.NewConnectionBuilder(conn).
		WithCurrentNamespacer(conn).
		WithDriverInfoPreparer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
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
		d.schema = value
	case OptionStringAggregatePushdown:
		if err := validatePushdown(value); err != nil {
			return err
		}
		d.pushdown = value
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
