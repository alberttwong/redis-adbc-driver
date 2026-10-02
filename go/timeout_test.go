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

// Tests of the client timeouts and of errorHook's messages (timeout.go)
// that need no Redis server.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	goredis "github.com/redis/go-redis/v9"
)

func TestTimeoutParse(t *testing.T) {
	for _, c := range []struct {
		in   string
		want time.Duration
	}{
		{"30s", 30 * time.Second},
		{"10m", 10 * time.Minute},
		{"1h30m", 90 * time.Minute},
		{"250ms", 250 * time.Millisecond},
		{"45", 45 * time.Second},
		{"1.5", 1500 * time.Millisecond},
		{" 2m ", 2 * time.Minute},
		{"0", 0},
		{"0s", 0},
	} {
		got, err := parseTimeout(OptionStringReadTimeout, c.in)
		if err != nil || got != c.want {
			t.Errorf("parseTimeout(%q) = %v, %v; want %v", c.in, got, err, c.want)
		}
	}
	for _, in := range []string{"", "abc", "-1", "-5s", "10 minutes", "1e12"} {
		_, err := parseTimeout(OptionStringWriteTimeout, in)
		want := `Invalid Argument: [redis] invalid adbc.redis.write_timeout "` + in +
			`" (want a duration such as 30s or 10m, a number of seconds, or 0 for no timeout)`
		if err == nil || err.Error() != want {
			t.Errorf("parseTimeout(%q): %v\nwant: %s", in, err, want)
		}
	}
}

// The timeouts a client gets, from the options, the URI and the defaults.
func TestTimeoutResolve(t *testing.T) {
	d := func(v time.Duration) *time.Duration { return &v }
	for _, c := range []struct {
		uri         string
		set         timeoutSettings
		read, write time.Duration
		follows     bool
	}{
		{"redis://localhost:6379/0", timeoutSettings{}, 5 * time.Minute, 5 * time.Minute, true},
		{"redis://localhost:6379/0?read_timeout=30s", timeoutSettings{}, 30 * time.Second, 30 * time.Second, true},
		{"redis://localhost:6379/0?read_timeout=20", timeoutSettings{}, 20 * time.Second, 20 * time.Second, true},
		{"redis://localhost:6379/0?read_timeout=-1", timeoutSettings{}, 0, 0, true},
		{"redis://localhost:6379/0?read_timeout=0", timeoutSettings{}, 0, 0, true},
		{"redis://localhost:6379/0?write_timeout=10s", timeoutSettings{}, 5 * time.Minute, 10 * time.Second, false},
		{"redis://localhost:6379/0?read_timeout=30s", timeoutSettings{read: d(time.Second)}, time.Second, time.Second, true},
		{"redis://localhost:6379/0?read_timeout=30s&write_timeout=40s", timeoutSettings{read: d(time.Second)}, time.Second, 40 * time.Second, false},
		{"redis://localhost:6379/0", timeoutSettings{read: d(0), write: d(time.Minute)}, 0, time.Minute, false},
	} {
		opts, err := goredis.ParseURL(c.uri)
		if err != nil {
			t.Fatal(err)
		}
		got, follows := c.set.resolve(opts)
		if got.read != c.read || got.write != c.write || follows != c.follows || got.dial != 5*time.Second {
			t.Errorf("%s %+v: got %+v (follows %v), want read %v write %v (follows %v)", c.uri, c.set, got, follows, c.read, c.write, c.follows)
		}
		// go-redis's own encoding: -1 is none.
		got.apply(opts)
		none := func(v time.Duration) time.Duration {
			if v == 0 {
				return -1
			}
			return v
		}
		if opts.ReadTimeout != none(c.read) || opts.WriteTimeout != none(c.write) {
			t.Errorf("%s: applied %v / %v", c.uri, opts.ReadTimeout, opts.WriteTimeout)
		}
	}

	// The database options, before any connection.
	ctx := context.Background()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: "redis://localhost:1/0?write_timeout=7s"})
	if err != nil {
		t.Fatal(err)
	}
	get := func(key, want string) {
		t.Helper()
		if v, err := db.(adbc.GetSetOptionsWithContext).GetOption(ctx, key); err != nil || v != want {
			t.Errorf("GetOption(%s) = %q, %v; want %q", key, v, err, want)
		}
	}
	set := func(key, value, wantErr string) {
		t.Helper()
		err := db.(adbc.GetSetOptionsWithContext).SetOption(ctx, key, value)
		if (err == nil) != (wantErr == "") || (err != nil && err.Error() != wantErr) {
			t.Errorf("SetOption(%s, %q) = %v; want %q", key, value, err, wantErr)
		}
	}
	get(OptionStringReadTimeout, "5m0s")
	get(OptionStringWriteTimeout, "7s")
	set(OptionStringReadTimeout, "90s", "")
	set(OptionStringWriteTimeout, "0", "")
	get(OptionStringReadTimeout, "1m30s")
	get(OptionStringWriteTimeout, "0s")
	set(OptionStringReadTimeout, "soon", `Invalid Argument: [redis] invalid adbc.redis.read_timeout "soon" (want a duration such as 30s or 10m, a number of seconds, or 0 for no timeout)`)
	get(OptionStringReadTimeout, "1m30s")
}

// errorHook's messages for timeouts, and the errors it leaves alone.
func TestTimeoutMessages(t *testing.T) {
	h := errorHook{t: timeouts{read: time.Second, write: 0, dial: 5 * time.Second}}
	opErr := func(op string) error {
		return &net.OpError{Op: op, Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6379}, Err: os.ErrDeadlineExceeded}
	}
	for _, c := range []struct {
		err  error
		want string
	}{
		{opErr("read"), "read tcp 127.0.0.1:6379: i/o timeout (Redis did not reply within the read timeout, 1s: raise adbc.redis.read_timeout, or set it to 0 for no timeout)"},
		{opErr("write"), "write tcp 127.0.0.1:6379: i/o timeout (could not send the command within the write timeout, 0s: raise adbc.redis.write_timeout, or set it to 0 for no timeout)"},
		{opErr("dial"), "dial tcp 127.0.0.1:6379: i/o timeout (could not connect within the dial timeout, 5s: raise the URI's dial_timeout parameter)"},
		{context.DeadlineExceeded, "context deadline exceeded"},
		{errors.New("ERR unknown command"), "ERR unknown command"},
		{&net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}, "read tcp: connection reset by peer"},
	} {
		got := h.explainTimeout(c.err)
		if got.Error() != c.want {
			t.Errorf("got  %s\nwant %s", got, c.want)
		}
		if !errors.Is(got, c.err) {
			t.Errorf("%v does not wrap %v", got, c.err)
		}
		// Rewriting is idempotent (cluster commands pass two hooks).
		if again := h.explainTimeout(got); again != got {
			t.Errorf("rewritten twice: %v", again)
		}
	}
	if h.explainTimeout(nil) != nil {
		t.Error("nil error rewritten")
	}
}

// redisErr is an error reply from Redis.
type redisErr string

func (e redisErr) Error() string { return string(e) }
func (redisErr) RedisError()     {}

// EXECABORT is replaced by the error of the command Redis refused to queue.
func TestTimeoutExecAbortMessage(t *testing.T) {
	ctx := context.Background()
	abort := redisErr("EXECABORT Transaction discarded because of previous errors.")
	noperm := redisErr("NOPERM No permissions to access a key")
	cmds := func() []goredis.Cmder {
		multi := goredis.NewStatusCmd(ctx, "multi")
		get := goredis.NewStringCmd(ctx, "get", "adbc:{meta}:view:public:v")
		set := goredis.NewStatusCmd(ctx, "set", "adbc:{meta}:view:public:v", `{"secret":"value"}`, 0)
		sadd := goredis.NewIntCmd(ctx, "sadd", "adbc:{meta}:views:public", "v")
		exec := goredis.NewSliceCmd(ctx, "exec")
		get.SetErr(goredis.Nil)
		set.SetErr(noperm)
		sadd.SetErr(abort)
		return []goredis.Cmder{multi, get, set, sadd, exec}
	}
	run := func(cmds []goredis.Cmder, returned error) error {
		return errorHook{}.ProcessPipelineHook(func(context.Context, []goredis.Cmder) error { return returned })(ctx, cmds)
	}
	want := "NOPERM No permissions to access a key ('set' on adbc:{meta}:view:public:v, in a MULTI transaction that Redis discarded)"
	// A single-endpoint client returns EXECABORT, a cluster client the
	// command's error.
	for _, returned := range []error{abort, noperm} {
		err := run(cmds(), returned)
		if err == nil || err.Error() != want {
			t.Errorf("got  %v\nwant %s", err, want)
		}
		// (That go-redis still sees EXECABORT through it needs the typed
		// error a server returns: see TestACLExecAbort.)
		if wrapped := wrapRedis(err, "failed to create view"); wrapped.Error() != "I/O: [redis] failed to create view: "+want {
			t.Errorf("wrapped: %v", wrapped)
		}
	}
	// Other pipelines and errors stay as they are.
	plain := cmds()[1:4]
	if err := run(plain, noperm); err != noperm {
		t.Errorf("pipeline: %v", err)
	}
	other := errors.New("redis: client is closed")
	if err := run(cmds(), other); err != other {
		t.Errorf("client error: %v", err)
	}
	// MULTI refused: every command has its error, and nothing was queued.
	multi := redisErr("NOPERM User u has no permissions to run the 'multi' command")
	refused := cmds()
	for _, c := range refused {
		c.SetErr(multi)
	}
	if err := run(refused, multi); err != multi {
		t.Errorf("MULTI refused: %v", err)
	}
}

// A server that accepts connections but never replies: the read timeout
// fires, and the message names the setting.
func TestTimeoutNoReply(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("can't listen on localhost: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { // read and drop everything
				buf := make([]byte, 4096)
				for {
					if _, err := conn.Read(buf); err != nil {
						_ = conn.Close()
						return
					}
				}
			}()
		}
	}()
	ctx := context.Background()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI:       "redis://" + ln.Addr().String() + "/0",
		OptionStringReadTimeout: "200ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = db.Open(ctx)
	want := regexp.MustCompile(`^I/O: \[redis\] failed to connect to Redis: read tcp 127\.0\.0\.1:\d+->127\.0\.0\.1:\d+: i/o timeout ` +
		`\(Redis did not reply within the read timeout, 200ms: raise adbc\.redis\.read_timeout, or set it to 0 for no timeout\)$`)
	if err == nil || !want.MatchString(err.Error()) {
		t.Fatalf("got %v", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("took %v", took)
	}
}

// fakeRedis is a server that speaks just enough RESP: it replies +PONG to
// PING, nil to HGET and an error to other commands (HELLO among them). On a
// command in hang it stops replying on that connection, as a server that
// hangs does, so the replies to a pipeline don't get out of step. It counts
// the commands it receives.
type fakeRedis struct {
	addr string
	mu   sync.Mutex
	seen map[string]int
}

func newFakeRedis(t *testing.T, hang ...string) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("can't listen on localhost: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	f := &fakeRedis{addr: ln.Addr().String(), seen: map[string]int{}}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				rd := bufio.NewReader(conn)
				hung := false
				for {
					args, err := readRESPArray(rd)
					if err != nil {
						return
					}
					name := strings.ToUpper(args[0])
					f.mu.Lock()
					f.seen[name]++
					f.mu.Unlock()
					switch {
					case hung || slices.Contains(hang, name):
						hung = true
					case name == "PING":
						_, _ = conn.Write([]byte("+PONG\r\n"))
					case name == "HGET":
						_, _ = conn.Write([]byte("$-1\r\n"))
					default:
						_, _ = conn.Write([]byte("-ERR unknown command\r\n"))
					}
				}
			}()
		}
	}()
	return f
}

func (f *fakeRedis) count(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen[name]
}

func readRESPArray(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil || n < 1 {
		return nil, fmt.Errorf("bad array header %q", line)
	}
	args := make([]string, n)
	for i := range args {
		if line, err = rd.ReadString('\n'); err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "$")))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}

// go-redis sends most commands again after a reply timed out. It must not
// for search commands (a repeated FT.CURSOR READ would skip a page of rows)
// and for SET … NX and SADD, whose replies the driver acts on (onceCmd), or
// for a pipeline with one of them.
func TestTimeoutNotRetried(t *testing.T) {
	f := newFakeRedis(t, "GET", "FT.CURSOR", "FT.AGGREGATE", "SET", "SADD")
	ctx := context.Background()
	opts := &goredis.Options{Addr: f.addr, Protocol: 2, DisableIdentity: true}
	client := newClient(opts, false, timeouts{read: 200 * time.Millisecond, write: 200 * time.Millisecond})
	defer client.Close()
	st := &store{client: client}

	// An idempotent command is sent again (go-redis's default).
	if err := client.Get(ctx, "k").Err(); err == nil {
		t.Fatal("GET: no error")
	}
	if n := f.count("GET"); n < 2 {
		t.Errorf("GET sent %d times; expected go-redis to send it again", n)
	}
	// These are sent once.
	err := st.searchDo(ctx, "idx", "FT.CURSOR", "READ", "idx", 1, "COUNT", 10).Err()
	want := regexp.MustCompile(`^read tcp \S+: i/o timeout \(Redis did not reply within the read timeout, 200ms: ` +
		`raise adbc\.redis\.read_timeout, or set it to 0 for no timeout\)$`)
	if err == nil || !want.MatchString(err.Error()) {
		t.Errorf("FT.CURSOR READ: %v", err)
	}
	if _, err := st.aggregate(ctx, &aggRequest{index: "idx", query: "*"}); err == nil {
		t.Error("FT.AGGREGATE: no error")
	}
	// claimNames reserves a prefix and an index name with two SADDs in one
	// pipeline.
	if _, err := st.claimNames(ctx, "public", "t"); err == nil {
		t.Error("claimNames: no error")
	}
	if err := once(ctx, client, "SET", "k", "v", "NX").Err(); err == nil {
		t.Error("SET NX: no error")
	}
	for name, want := range map[string]int{"FT.CURSOR": 1, "FT.AGGREGATE": 1, "SADD": 2, "SET": 1} {
		if n := f.count(name); n != want {
			t.Errorf("%s sent %d times, want %d", name, n, want)
		}
	}
}
