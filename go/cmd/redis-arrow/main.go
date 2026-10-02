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

// Command redis-arrow moves data between Redis and Arrow IPC files through
// the driver: export writes a query's result as an Arrow IPC file or stream,
// and import bulk-ingests one into a table.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"

	redis "github.com/adbc-drivers/redis/go"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const defaultURI = "redis://localhost:6379/0"

// fileMagic starts every Arrow IPC file. A stream starts with a
// continuation marker (0xFFFFFFFF) instead.
var fileMagic = []byte("ARROW1")

var alloc = memory.DefaultAllocator

const usage = `usage: redis-arrow <command> [flags] [args]

Commands:
  export   run SQL and write the result as an Arrow IPC file or stream
  import   bulk-ingest an Arrow IPC file or stream into a table
  check    check an existing HASH collection: guess its columns, and list
           what Arrow IPC and SQL need
  scan     read an existing HASH collection as an Arrow IPC file or stream

Run "redis-arrow <command> -h" for a command's flags.
`

// usageError is a bad command line; the flag package has already printed
// what is wrong.
type usageError struct{ err error }

func (e usageError) Error() string { return e.err.Error() }
func (e usageError) Unwrap() error { return e.err }

func main() {
	err := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	var ue usageError
	switch {
	case err == nil:
	case errors.Is(err, flag.ErrHelp):
	case errors.As(err, &ue):
		os.Exit(2)
	default:
		fmt.Fprintf(os.Stderr, "redis-arrow: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return usageError{errors.New("no command given")}
	}
	switch args[0] {
	case "export":
		return runExport(ctx, args[1:], stdout, stderr)
	case "import":
		return runImport(ctx, args[1:], stdin, stderr)
	case "check":
		return runCheck(ctx, args[1:], stdout, stderr)
	case "scan":
		return runScan(ctx, args[1:], stdout, stderr)
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		return nil
	}
	fmt.Fprint(stderr, usage)
	return usageError{fmt.Errorf("unknown command %q", args[0])}
}

// options collects repeated -option key=value flags.
type options map[string]string

func (o options) String() string {
	keys := make([]string, 0, len(o))
	for k := range o {
		keys = append(keys, k+"="+o[k])
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func (o options) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("want key=value, got %q", s)
	}
	o[k] = v
	return nil
}

// connFlags are the flags that every command takes to reach Redis.
type connFlags struct {
	uri  string
	opts options
}

func (c *connFlags) register(fs *flag.FlagSet) {
	c.opts = options{}
	// The default is resolved when connecting, so that -h doesn't print the
	// credentials in $REDIS_URI.
	fs.StringVar(&c.uri, "uri", "", "Redis URI (default $REDIS_URI, else "+defaultURI+"); $REDIS_URI keeps a password off the command line")
	fs.Var(c.opts, "option", "database option `key=value`, such as adbc.redis.read_timeout=30m (repeatable)")
}

// options are the database options: the -option flags and the URI.
func (c *connFlags) options() map[string]string {
	uri := c.uri
	if uri == "" {
		uri = os.Getenv("REDIS_URI")
	}
	if uri == "" {
		uri = defaultURI
	}
	opts := map[string]string{}
	maps.Copy(opts, c.opts)
	opts[adbc.OptionKeyURI] = uri
	return opts
}

// open connects to Redis. The returned func closes the connection and the
// database.
func (c *connFlags) open(ctx context.Context) (adbc.ConnectionWithContext, func(), error) {
	db, err := redis.NewDriver(alloc).NewDatabaseWithContext(ctx, c.options())
	if err != nil {
		return nil, nil, err
	}
	conn, err := db.Open(ctx)
	if err != nil {
		_ = db.Close(ctx)
		return nil, nil, err
	}
	return conn, func() {
		_ = conn.Close(ctx)
		_ = db.Close(ctx)
	}, nil
}

func newFlagSet(name, args string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: redis-arrow %s [flags] %s\n\nFlags:\n", name, args)
		fs.PrintDefaults()
	}
	return fs
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usageError{err}
	}
	return nil
}

// resolveFormat reports whether export writes the IPC file format (true)
// or the stream format (false).
func resolveFormat(format, out string) (bool, error) {
	switch format {
	case "file":
		return true, nil
	case "stream":
		return false, nil
	case "auto":
		return out != "-" && !strings.EqualFold(filepath.Ext(out), ".arrows"), nil
	}
	return false, fmt.Errorf("unknown -format %q (want file, stream or auto)", format)
}

func compressionOptions(name string) ([]ipc.Option, error) {
	switch name {
	case "none", "":
		return nil, nil
	case "lz4":
		return []ipc.Option{ipc.WithLZ4()}, nil
	case "zstd":
		return []ipc.Option{ipc.WithZstd()}, nil
	}
	return nil, fmt.Errorf("unknown -compression %q (want none, lz4 or zstd)", name)
}

func runExport(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("export", "[SQL]", stderr)
	var cf connFlags
	cf.register(fs)
	query := fs.String("sql", "", "the query to run (or give it as the arguments)")
	out := fs.String("o", "-", "output `path`, - for stdout")
	format := fs.String("format", "auto", "file, stream or auto: stream for stdout and *.arrows, file otherwise")
	compression := fs.String("compression", "none", "body compression: none, lz4 or zstd")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	sql := *query
	if fs.NArg() > 0 {
		if sql != "" {
			return errors.New("give the query either with -sql or as the arguments, not both")
		}
		sql = strings.Join(fs.Args(), " ")
	}
	if strings.TrimSpace(sql) == "" {
		return errors.New("no query given")
	}
	fileFormat, err := resolveFormat(*format, *out)
	if err != nil {
		return err
	}
	ipcOpts, err := compressionOptions(*compression)
	if err != nil {
		return err
	}

	conn, closeConn, err := cf.open(ctx)
	if err != nil {
		return err
	}
	defer closeConn()
	st, err := conn.NewStatement(ctx)
	if err != nil {
		return err
	}
	defer st.Close(ctx)
	if err := st.SetSqlQuery(ctx, sql); err != nil {
		return err
	}
	rdr, _, err := st.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer rdr.Release()
	if rdr.Schema().NumFields() == 0 {
		return errors.New("the statement ran but returned no result set, so nothing was written")
	}

	rows, err := writeOutput(*out, stdout, rdr, fileFormat, ipcOpts)
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "%d %s\n", rows, plural(rows, "row", "rows"))
	return nil
}

// writeOutput writes every batch of rdr to the path out, or to stdout for
// "-", and returns the number of rows. A file is written next to its target
// and renamed, so that a failure leaves no partial file behind.
func writeOutput(out string, stdout io.Writer, rdr array.RecordReader, fileFormat bool, ipcOpts []ipc.Option) (int64, error) {
	if out == "-" {
		return writeIPC(stdout, rdr, fileFormat, ipcOpts)
	}
	f, err := os.CreateTemp(filepath.Dir(out), "."+filepath.Base(out)+".*.tmp")
	if err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = f.Close()
			_ = os.Remove(f.Name())
		}
	}()
	rows, err := writeIPC(f, rdr, fileFormat, ipcOpts)
	if err != nil {
		return 0, err
	}
	if err := f.Chmod(0o644); err != nil {
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(f.Name(), out); err != nil {
		return 0, err
	}
	committed = true
	return rows, nil
}

// writeIPC writes every batch of rdr to w and returns the number of rows.
func writeIPC(w io.Writer, rdr array.RecordReader, fileFormat bool, opts []ipc.Option) (int64, error) {
	bw := bufio.NewWriterSize(w, 1<<20)
	opts = append([]ipc.Option{ipc.WithSchema(rdr.Schema()), ipc.WithAllocator(alloc)}, opts...)
	var iw interface {
		Write(arrow.RecordBatch) error
		Close() error
	}
	if fileFormat {
		fw, err := ipc.NewFileWriter(bw, opts...)
		if err != nil {
			return 0, err
		}
		iw = fw
	} else {
		iw = ipc.NewWriter(bw, opts...)
	}
	var rows int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		if err := iw.Write(rec); err != nil {
			_ = iw.Close()
			return 0, err
		}
		rows += rec.NumRows()
	}
	if err := rdr.Err(); err != nil {
		_ = iw.Close()
		return 0, err
	}
	if err := iw.Close(); err != nil {
		return 0, err
	}
	return rows, bw.Flush()
}

var ingestModes = map[string]string{
	"create":        adbc.OptionValueIngestModeCreate,
	"append":        adbc.OptionValueIngestModeAppend,
	"replace":       adbc.OptionValueIngestModeReplace,
	"create_append": adbc.OptionValueIngestModeCreateAppend,
}

func runImport(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer) error {
	fs := newFlagSet("import", "[PATH | -]", stderr)
	var cf connFlags
	cf.register(fs)
	table := fs.String("table", "", "the table to ingest into (required)")
	schema := fs.String("schema", "", "the table's schema (default: the connection's current schema)")
	mode := fs.String("mode", "create", "create, append, replace or create_append")
	indexColumns := fs.String("index-columns", "", "comma-separated `columns` to index (default: every filterable column)")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *table == "" {
		return errors.New("-table is required")
	}
	ingestMode, ok := ingestModes[*mode]
	if !ok {
		return fmt.Errorf("unknown -mode %q (want create, append, replace or create_append)", *mode)
	}
	if fs.NArg() > 1 {
		return fmt.Errorf("want one input path, got %d (flags go before the path)", fs.NArg())
	}
	path := "-"
	if fs.NArg() == 1 {
		path = fs.Arg(0)
	}

	rdr, closeInput, err := openInput(path, stdin)
	if err != nil {
		return err
	}
	defer closeInput()
	bound := false
	defer func() {
		if !bound {
			rdr.Release()
		}
	}()

	conn, closeConn, err := cf.open(ctx)
	if err != nil {
		return err
	}
	defer closeConn()
	st, err := conn.NewStatement(ctx)
	if err != nil {
		return err
	}
	defer st.Close(ctx)
	opts := [][2]string{
		{adbc.OptionKeyIngestTargetTable, *table},
		{adbc.OptionKeyIngestMode, ingestMode},
	}
	if *schema != "" {
		opts = append(opts, [2]string{adbc.OptionValueIngestTargetDBSchema, *schema})
	}
	if *indexColumns != "" {
		opts = append(opts, [2]string{redis.OptionStringIngestIndexColumns, *indexColumns})
	}
	for _, kv := range opts {
		if err := st.SetOption(ctx, kv[0], kv[1]); err != nil {
			return err
		}
	}
	// The statement owns the reader from here on and releases it.
	bound = true
	if err := st.BindStream(ctx, rdr); err != nil {
		return err
	}
	n, err := st.ExecuteUpdate(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "%d %s ingested into %s\n", n, plural(n, "row", "rows"), *table)
	return nil
}

// openInput opens an Arrow IPC file or stream, telling them apart by the
// file format's magic bytes. The returned func closes the input once the
// reader is no longer used.
func openInput(path string, stdin io.Reader) (array.RecordReader, func(), error) {
	if path == "-" {
		br := bufio.NewReaderSize(stdin, 1<<20)
		head, _ := br.Peek(len(fileMagic))
		if !bytes.Equal(head, fileMagic) {
			rdr, err := ipc.NewReader(br, ipc.WithAllocator(alloc))
			if err != nil {
				return nil, nil, err
			}
			return rdr, func() {}, nil
		}
		// The file format's footer is at the end, so read it all.
		data, err := io.ReadAll(br)
		if err != nil {
			return nil, nil, err
		}
		return fileRecordReader(bytes.NewReader(data), func() {})
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	head := make([]byte, len(fileMagic))
	n, _ := io.ReadFull(f, head)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if bytes.Equal(head[:n], fileMagic) {
		return fileRecordReader(f, func() { _ = f.Close() })
	}
	rdr, err := ipc.NewReader(bufio.NewReaderSize(f, 1<<20), ipc.WithAllocator(alloc))
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("%s is neither an Arrow IPC file nor a stream: %w", path, err)
	}
	return rdr, func() { _ = f.Close() }, nil
}

// fileRecordReader reads an Arrow IPC file's batches in order. closeFile
// closes what r reads from.
func fileRecordReader(r ipc.ReadAtSeeker, closeFile func()) (array.RecordReader, func(), error) {
	fr, err := ipc.NewFileReader(r, ipc.WithAllocator(alloc))
	if err != nil {
		closeFile()
		return nil, nil, err
	}
	rdr := array.ReaderFromIter(fr.Schema(), func(yield func(arrow.RecordBatch, error) bool) {
		for i := range fr.NumRecords() {
			// RecordBatchAt hands the batch to the reader, which releases
			// it on the next Next.
			rec, err := fr.RecordBatchAt(i)
			if !yield(rec, err) || err != nil {
				return
			}
		}
	})
	return rdr, func() {
		_ = fr.Close()
		closeFile()
	}, nil
}

func plural(n int64, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
