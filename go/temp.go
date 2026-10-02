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

// Temporary tables and views.
//
// A temporary table or view belongs to the ADBC connection that created it:
// only that connection sees it, and it is dropped when the connection is
// closed. The first time a connection creates one, it takes a connection id
// from a counter and gets a private schema, pg_temp_<id>. That schema is laid
// out like any other (the same metadata, row and index keys), but it is never
// added to adbc:{meta}:schemas, so other connections don't list it:
//
//	pg_temp_<id>:<table>:<rowid>               HASH   rows
//	idx:pg_temp_<id>:<table>                   FT index
//	adbc:{meta}:table:pg_temp_<id>:<table>     STRING table metadata (also
//	                                                  seq:, tables:, view:, views:)
//
// Ownership and liveness, in the same {meta} hash slot:
//
//	adbc:{meta}:temp:next          STRING counter for connection ids
//	adbc:{meta}:temp:owners        SET    ids of connections with a temporary schema
//	adbc:{meta}:temp:alive:<id>    STRING exists while the owner is open; it has a
//	                                      TTL that a heartbeat keeps refreshing
//
// Names: a connection sees its own temporary schema as pg_temp. An unqualified
// name resolves to a temporary object first and then to the current schema, so
// a temporary table shadows a permanent one of the same name. A qualified name
// (public.t) never resolves to a temporary object, except pg_temp.t. The names
// pg_temp and pg_temp_<digits> are reserved and can't be used as schemas.
//
// Cleanup: closing a connection drops its temporary objects and removes its
// owner entry. A connection that goes away without closing stops refreshing
// its alive key; once the key has expired, the next connection to open drops
// the leftovers. It checks that the alive key is still missing before each
// object and before removing the owner entry, so a live connection's objects
// are never dropped.

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

const (
	tempAlias      = "pg_temp"
	tempSchemaBase = "pg_temp_"
	tempNextKey    = metaPrefix + "temp:next"
	tempOwnersKey  = metaPrefix + "temp:owners"
)

// tempTTL is how long a connection's temporary objects outlive its last
// heartbeat (if the process dies without closing it). The heartbeat runs
// every tempTTL/4.
var tempTTL = 2 * time.Minute

func tempAliveKey(id string) string { return metaPrefix + "temp:alive:" + id }

func isTempAlias(schema string) bool { return strings.EqualFold(schema, tempAlias) }

// tempSchemaID returns the connection id of a temporary schema name
// (pg_temp_<digits>).
func tempSchemaID(schema string) (string, bool) {
	n := len(tempSchemaBase)
	if len(schema) <= n || !strings.EqualFold(schema[:n], tempSchemaBase) {
		return "", false
	}
	for _, c := range schema[n:] {
		if c < '0' || c > '9' {
			return "", false
		}
	}
	return schema[n:], true
}

func isTempSchema(schema string) bool {
	_, ok := tempSchemaID(schema)
	return ok
}

// reservedSchema reports whether a schema name is reserved for temporary
// objects.
func reservedSchema(schema string) bool { return isTempAlias(schema) || isTempSchema(schema) }

// displaySchema is the name a schema is reported as: pg_temp for a temporary
// schema.
func displaySchema(schema string) string {
	if isTempSchema(schema) {
		return tempAlias
	}
	return schema
}

// tempSpace is a connection's temporary schema.
type tempSpace struct {
	mu sync.Mutex
	// id is "" until the connection creates its first temporary object.
	id string
	// objects are the names of its temporary tables and views.
	objects map[string]bool
	stop    context.CancelFunc
	done    chan struct{}
	// Key prefixes, which are never reused (see claimNames): lastNames is
	// the last N handed out per table name (plain prefix), and used holds
	// every prefix handed out. Only this connection creates tables in its
	// schema, and releases aren't counted in adbc:{meta}:released.
	lastNames map[string]int64
	used      map[string]bool
}

func (s *store) tempLastNames(base string) int64 {
	s.temp.mu.Lock()
	defer s.temp.mu.Unlock()
	return s.temp.lastNames[base]
}

func (s *store) tempUsedPrefix(prefix string) bool {
	s.temp.mu.Lock()
	defer s.temp.mu.Unlock()
	return s.temp.used[prefix]
}

// noteTempNames records names reserved in a temporary schema.
func (s *store) noteTempNames(nm tableNames) {
	if !nm.temp {
		return
	}
	s.temp.mu.Lock()
	defer s.temp.mu.Unlock()
	if s.temp.lastNames == nil {
		s.temp.lastNames, s.temp.used = map[string]int64{}, map[string]bool{}
	}
	s.temp.lastNames[nm.base] = max(s.temp.lastNames[nm.base], nm.n)
	s.temp.used[nm.prefix] = true
}

// tempSchema returns the connection's temporary schema, or pg_temp (which
// never holds anything) if it has none yet.
func (s *store) tempSchema() string {
	s.temp.mu.Lock()
	defer s.temp.mu.Unlock()
	if s.temp.id == "" {
		return tempAlias
	}
	return tempSchemaBase + s.temp.id
}

// isTempObject reports whether the connection has a temporary table or view
// with this name.
func (s *store) isTempObject(name string) bool {
	s.temp.mu.Lock()
	defer s.temp.mu.Unlock()
	return s.temp.objects[name]
}

func (s *store) hasTempObjects() bool {
	s.temp.mu.Lock()
	defer s.temp.mu.Unlock()
	return len(s.temp.objects) > 0
}

// trackTemp records that a table or view was created or dropped; it does
// nothing unless schema is this connection's temporary schema.
func (s *store) trackTemp(schema, name string, present bool) {
	s.temp.mu.Lock()
	defer s.temp.mu.Unlock()
	if s.temp.id == "" || schema != tempSchemaBase+s.temp.id {
		return
	}
	if present {
		s.temp.objects[name] = true
	} else {
		delete(s.temp.objects, name)
	}
}

// ownTempSchema reports whether schema is this connection's temporary schema.
func (s *store) ownTempSchema(schema string) bool {
	s.temp.mu.Lock()
	defer s.temp.mu.Unlock()
	return s.temp.id != "" && schema == tempSchemaBase+s.temp.id
}

// checkWritableSchema rejects creating objects in a reserved schema other
// than this connection's own temporary schema.
func (s *store) checkWritableSchema(schema string) error {
	if isInfoSchema(schema) {
		return infoSchemaReadOnly()
	}
	if reservedSchema(schema) && !s.ownTempSchema(schema) {
		return errorf(adbc.StatusInvalidArgument, "schema name %q is reserved for temporary tables and views", schema)
	}
	return nil
}

// ensureTempSchema returns the connection's temporary schema, creating it on
// first use: it takes a new connection id, marks it alive, registers it as an
// owner, and starts the heartbeat.
func (s *store) ensureTempSchema(ctx context.Context) (string, error) {
	s.temp.mu.Lock()
	defer s.temp.mu.Unlock()
	if s.temp.id != "" {
		return tempSchemaBase + s.temp.id, nil
	}
	ttl := tempTTL
	for attempt := 0; attempt < 100; attempt++ {
		n, err := s.client.IncrBy(ctx, tempNextKey, 1).Result()
		if err != nil {
			return "", wrapRedis(err, "failed to allocate a temporary schema")
		}
		id := strconv.FormatInt(n, 10)
		// Ids are unique unless the counter was lost; skip any id that is
		// alive or still owns objects.
		err = once(ctx, s.client, "SET", tempAliveKey(id), "1", "PX", ttl.Milliseconds(), "NX").Err()
		ok := err == nil
		if errors.Is(err, goredis.Nil) {
			err = nil
		}
		if err != nil {
			return "", wrapRedis(err, "failed to allocate a temporary schema")
		}
		if !ok {
			continue
		}
		added, err := once(ctx, s.client, "SADD", tempOwnersKey, id).Int64()
		if err != nil || added == 0 {
			s.client.Del(ctx, tempAliveKey(id))
			if err != nil {
				return "", wrapRedis(err, "failed to allocate a temporary schema")
			}
			continue
		}
		hb, cancel := context.WithCancel(context.Background())
		s.temp.id, s.temp.objects = id, map[string]bool{}
		s.temp.stop, s.temp.done = cancel, make(chan struct{})
		go heartbeat(hb, s.client, id, ttl, s.temp.done)
		return tempSchemaBase + id, nil
	}
	return "", errorf(adbc.StatusInternal, "could not allocate a temporary schema")
}

// heartbeat keeps a connection's alive key from expiring. It also re-adds
// the owner entry, in case the connection was swept while it couldn't reach
// the server, so that objects it creates afterwards are still cleaned up.
func heartbeat(ctx context.Context, client goredis.UniversalClient, id string, ttl time.Duration, done chan struct{}) {
	defer close(done)
	tick := time.NewTicker(ttl / 4)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			pipe := client.TxPipeline()
			pipe.Set(ctx, tempAliveKey(id), "1", ttl)
			pipe.SAdd(ctx, tempOwnersKey, id)
			_, _ = pipe.Exec(ctx)
		}
	}
}

// stopHeartbeat stops the heartbeat and forgets the temporary schema
// in memory, returning its id ("" if there was none).
func (s *store) stopHeartbeat() string {
	s.temp.mu.Lock()
	id, stop, done := s.temp.id, s.temp.stop, s.temp.done
	s.temp.id, s.temp.objects, s.temp.stop, s.temp.done = "", nil, nil, nil
	s.temp.mu.Unlock()
	if id != "" {
		stop()
		<-done
	}
	return id
}

// dropTempSchema drops the connection's temporary objects (on Close). If
// that fails, the alive key is deleted so that the next connection sweeps
// what is left straight away.
func (s *store) dropTempSchema(ctx context.Context) error {
	id := s.stopHeartbeat()
	if id == "" {
		return nil
	}
	if err := s.dropTempObjects(ctx, tempSchemaBase+id, nil); err != nil {
		s.client.Del(ctx, tempAliveKey(id))
		return err
	}
	pipe := s.client.TxPipeline()
	pipe.SRem(ctx, tempOwnersKey, id)
	pipe.Del(ctx, tempAliveKey(id))
	_, err := pipe.Exec(ctx)
	return wrapRedis(err, "failed to remove the temporary schema")
}

var errOwnerAlive = errors.New("the owner of the temporary schema is alive")

// dropTempObjects drops every view and table in a temporary schema. When
// stillDead is set, it is checked before each object, and the sweep stops if
// the owner turns out to be alive.
func (s *store) dropTempObjects(ctx context.Context, schema string, stillDead func() bool) error {
	views, err := s.listViews(ctx, schema)
	if err != nil {
		return err
	}
	tables, err := s.listTables(ctx, schema)
	if err != nil {
		return err
	}
	var first error
	keep := func(err error) {
		if first == nil {
			first = err
		}
	}
	for _, v := range views {
		if stillDead != nil && !stillDead() {
			return errOwnerAlive
		}
		if err := s.dropView(ctx, schema, v, true); err != nil {
			keep(err)
		}
	}
	for _, t := range tables {
		if stillDead != nil && !stillDead() {
			return errOwnerAlive
		}
		if err := s.dropTable(ctx, schema, t, true); err != nil {
			keep(err)
		}
	}
	return first
}

// sweepTemp drops the temporary objects of connections that went away
// without closing (their alive key has expired). It is best effort: errors
// are ignored and the next connection tries again.
func (s *store) sweepTemp(ctx context.Context) {
	ids, err := s.client.SMembers(ctx, tempOwnersKey).Result()
	if err != nil || len(ids) == 0 {
		return
	}
	pipe := s.client.Pipeline()
	alive := make([]*goredis.IntCmd, len(ids))
	for i, id := range ids {
		alive[i] = pipe.Exists(ctx, tempAliveKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return
	}
	for i, id := range ids {
		if alive[i].Val() == 0 {
			_ = s.sweepTempOwner(ctx, id)
		}
	}
}

func (s *store) sweepTempOwner(ctx context.Context, id string) error {
	key := tempAliveKey(id)
	stillDead := func() bool {
		n, err := s.client.Exists(ctx, key).Result()
		return err == nil && n == 0
	}
	if isTempSchema(tempSchemaBase + id) {
		if err := s.dropTempObjects(ctx, tempSchemaBase+id, stillDead); err != nil {
			return err
		}
	}
	// Remove the owner entry unless the owner came back meanwhile.
	err := s.client.Watch(ctx, func(tx *goredis.Tx) error {
		n, err := tx.Exists(ctx, key).Result()
		if err != nil || n > 0 {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
			p.SRem(ctx, tempOwnersKey, id)
			return nil
		})
		return err
	}, key)
	return wrapRedis(err, "failed to remove a stale temporary schema")
}
