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

// The client's timeouts, and errors that say what caused them.
//
// Every command the driver sends gets one reply, and how long Redis takes
// for some grows with the data: an FT.AGGREGATE page that groups or sorts a
// big table, FT.DROPINDEX … DD, a pipeline of 1,000 writes. A busy or briefly
// blocked server (a fork, a failover, a slow shard of a cluster) delays every
// reply. So the client waits defaultReadTimeout for each reply, on standalone
// servers and clusters alike, rather than go-redis's 5 seconds.
// adbc.redis.read_timeout and adbc.redis.write_timeout change that, as do
// the URI parameters read_timeout, write_timeout and dial_timeout.
//
// errorHook rewrites two kinds of errors on their way out of the client:
//   - A socket timeout names the setting to raise.
//   - EXECABORT, which Redis returns from EXEC when it refused a command
//     queued in the transaction (NOPERM from an ACL, for example), is
//     replaced by that command's error, with the command and its key.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	goredis "github.com/redis/go-redis/v9"
)

const (
	// defaultReadTimeout is how long the client waits for a reply, and to
	// send a command, unless the options or the URI say otherwise.
	defaultReadTimeout = 5 * time.Minute
	// defaultDialTimeout is go-redis's.
	defaultDialTimeout = 5 * time.Second
)

// timeouts are a client's socket timeouts. 0 means no timeout (dial is only
// used in messages).
type timeouts struct {
	read, write, dial time.Duration
}

// timeoutSettings are the timeouts set by the options: nil when not set.
type timeoutSettings struct {
	read, write *time.Duration
}

// parseTimeout parses the value of adbc.redis.read_timeout or write_timeout:
// a Go duration ("30s", "10m", "1h30m"), or a number of seconds; 0 means no
// timeout.
func parseTimeout(key, value string) (time.Duration, error) {
	v := strings.TrimSpace(value)
	d, err := time.ParseDuration(v)
	if err != nil {
		if f, ferr := strconv.ParseFloat(v, 64); ferr == nil && f >= 0 && f < 1e9 {
			d, err = time.Duration(f*float64(time.Second)), nil
		}
	}
	if err != nil || d < 0 {
		return 0, errorf(adbc.StatusInvalidArgument,
			"invalid %s %q (want a duration such as 30s or 10m, a number of seconds, or 0 for no timeout)", key, value)
	}
	return d, nil
}

// formatTimeout is the value GetOption returns.
func formatTimeout(d time.Duration) string { return d.String() }

// resolve returns the timeouts of a client: the options' if set, else the
// URI's (go-redis parses read_timeout, write_timeout and dial_timeout, -1
// meaning none), else the defaults. The write timeout follows the read
// timeout unless it is set itself. followsRead reports that it does.
func (o timeoutSettings) resolve(opts *goredis.Options) (t timeouts, followsRead bool) {
	fromURI := func(d time.Duration) (time.Duration, bool) {
		switch {
		case d < 0:
			return 0, true
		case d > 0:
			return d, true
		}
		return 0, false
	}
	t.read = defaultReadTimeout
	if d, ok := fromURI(opts.ReadTimeout); ok {
		t.read = d
	}
	if o.read != nil {
		t.read = *o.read
	}
	t.write, followsRead = t.read, true
	if d, ok := fromURI(opts.WriteTimeout); ok {
		t.write, followsRead = d, false
	}
	if o.write != nil {
		t.write, followsRead = *o.write, false
	}
	t.dial = defaultDialTimeout
	if opts.DialTimeout > 0 {
		t.dial = opts.DialTimeout
	}
	return t, followsRead
}

// apply sets the timeouts in go-redis options, where 0 means the default
// and -1 means none.
func (t timeouts) apply(opts *goredis.Options) {
	none := func(d time.Duration) time.Duration {
		if d == 0 {
			return -1
		}
		return d
	}
	opts.ReadTimeout, opts.WriteTimeout = none(t.read), none(t.write)
}

// newClient creates a single-endpoint or OSS Cluster API client from
// options parsed from the URI (or built from the address), with the given
// timeouts, and adds errorHook to it.
func newClient(base *goredis.Options, cluster bool, t timeouts) goredis.UniversalClient {
	opts := *base
	t.apply(&opts)
	hook := errorHook{t: t}
	if !cluster {
		c := goredis.NewClient(&opts)
		c.AddHook(hook)
		return c
	}
	c := goredis.NewClusterClient(&goredis.ClusterOptions{
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
	// Transactions (WATCH … EXEC) and search commands go to a node's client
	// directly, so the nodes have the hook too (errorHook leaves errors it
	// already rewrote alone).
	c.OnNewNode(func(node *goredis.Client) { node.AddHook(hook) })
	c.AddHook(hook)
	return c
}

// errorHook is a go-redis hook that makes errors say what caused them (see
// above).
type errorHook struct{ t timeouts }

func (h errorHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h errorHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := next(ctx, cmd)
		if e := h.explainTimeout(err); e != err {
			cmd.SetErr(e)
			return e
		}
		return err
	}
}

func (h errorHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		err := next(ctx, cmds)
		if err == nil {
			return nil
		}
		for _, cmd := range cmds {
			if cerr := cmd.Err(); cerr != nil {
				if e := h.explainTimeout(cerr); e != cerr {
					cmd.SetErr(e)
				}
			}
		}
		if q := firstQueuedError(cmds, err); q != nil {
			return q
		}
		return h.explainTimeout(err)
	}
}

// timeoutError is a socket timeout, with what to do about it.
type timeoutError struct {
	err  error
	hint string
}

func (e *timeoutError) Error() string { return e.err.Error() + " (" + e.hint + ")" }
func (e *timeoutError) Unwrap() error { return e.err }

// explainTimeout adds the setting to raise to a socket timeout error.
func (h errorHook) explainTimeout(err error) error {
	if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	}
	var te *timeoutError
	if errors.As(err, &te) {
		return err
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		return err
	}
	var op string
	var oe *net.OpError
	if errors.As(err, &oe) {
		op = oe.Op
	}
	var hint string
	switch op {
	case "dial":
		hint = fmt.Sprintf("could not connect within the dial timeout, %s: raise the URI's dial_timeout parameter", h.t.dial)
	case "write":
		hint = fmt.Sprintf("could not send the command within the write timeout, %s: raise %s, or set it to 0 for no timeout",
			h.t.write, OptionStringWriteTimeout)
	default:
		hint = fmt.Sprintf("Redis did not reply within the read timeout, %s: raise %s, or set it to 0 for no timeout",
			h.t.read, OptionStringReadTimeout)
	}
	return &timeoutError{err: err, hint: hint}
}

// queuedError is the error of a command that Redis refused to queue in a
// MULTI transaction (NOPERM from an ACL, for example), after which EXEC
// failed with EXECABORT and Redis discarded the whole transaction.
type queuedError struct {
	cause error // the command's error
	err   error // the client's: EXECABORT, or (from a cluster client) cause
	cmd   goredis.Cmder
}

func (e *queuedError) Error() string {
	return fmt.Sprintf("%v (%s, in a MULTI transaction that Redis discarded)", e.cause, describeCmd(e.cmd))
}

// Unwrap keeps EXECABORT visible to go-redis (goredis.IsExecAbortError),
// which then knows that EXEC ended the WATCH.
func (e *queuedError) Unwrap() []error { return []error{e.cause, e.err} }

// firstQueuedError returns, for a transaction (MULTI … EXEC) that Redis
// discarded, the error of the first command it refused, or nil. A
// single-endpoint client returns EXECABORT for such a transaction, and a
// cluster client the first refused command's error; either way the
// commands after it have EXECABORT. (When a cluster client's transaction
// has only refused commands, nothing shows that EXEC failed, so the error
// stays as it is, as it does when MULTI itself is refused.)
func firstQueuedError(cmds []goredis.Cmder, err error) error {
	var re goredis.Error
	if len(cmds) < 2 || cmds[0].Name() != "multi" || cmds[len(cmds)-1].Name() != "exec" || !errors.As(err, &re) {
		return nil
	}
	queued := cmds[1 : len(cmds)-1]
	aborted := goredis.IsExecAbortError(err)
	for _, cmd := range queued {
		aborted = aborted || goredis.IsExecAbortError(cmd.Err())
	}
	if !aborted {
		return nil
	}
	for _, cmd := range queued {
		cause := cmd.Err()
		if cause != nil && !goredis.IsExecAbortError(cause) && !errors.Is(cause, goredis.Nil) && errors.As(cause, &re) {
			return &queuedError{cause: cause, err: err, cmd: cmd}
		}
	}
	return nil
}

// describeCmd names a command and its key, not its values: 'sadd' on k.
func describeCmd(cmd goredis.Cmder) string {
	if args := cmd.Args(); len(args) > 1 {
		if key, ok := args[1].(string); ok {
			return fmt.Sprintf("'%s' on %s", cmd.Name(), key)
		}
	}
	return fmt.Sprintf("'%s'", cmd.Name())
}
