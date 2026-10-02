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
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	redis "github.com/adbc-drivers/redis/go"
)

func runScan(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("scan", "", stderr)
	var cf connFlags
	cf.register(fs)
	var hf hashFlags
	hf.register(fs)
	out := fs.String("o", "-", "output `path`, - for stdout")
	format := fs.String("format", "auto", "file, stream or auto: stream for stdout and *.arrows, file otherwise")
	compression := fs.String("compression", "none", "body compression: none, lz4 or zstd")
	onError := fs.String("on-error", "fail", "a value that doesn't convert to its column's type: fail, or null to read it as NULL")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q (flags go before them)", fs.Args())
	}
	if hf.prefix == "" {
		return errors.New("-prefix is required (redis-arrow check lists the collections)")
	}
	if *onError != "fail" && *onError != "null" {
		return fmt.Errorf("unknown -on-error %q (want fail or null)", *onError)
	}
	fileFormat, err := resolveFormat(*format, *out)
	if err != nil {
		return err
	}
	ipcOpts, err := compressionOptions(*compression)
	if err != nil {
		return err
	}
	sc, err := redis.ScanHashes(ctx, cf.options(), redis.HashScanOptions{
		Prefix: hf.prefix, Sample: hf.sample, KeyColumn: hf.keyColumn,
		Types: hf.types, Renames: hf.renames, NullOnError: *onError == "null",
	})
	if err != nil {
		return err
	}
	defer sc.Release()
	rows, err := writeOutput(*out, stdout, sc, fileFormat, ipcOpts)
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "%d %s\n", rows, plural(rows, "row", "rows"))
	stats := sc.Stats()
	if len(stats.UnknownFields) > 0 {
		fmt.Fprintf(stderr, "skipped fields that the sample didn't have (-sample -1 guesses the columns from every HASH): %s\n",
			countList(stats.UnknownFields, "HASH", "HASHes"))
	}
	if len(stats.Nulls) > 0 {
		fmt.Fprintf(stderr, "values read as NULL because they didn't convert: %s\n", countList(stats.Nulls, "value", "values"))
	}
	return nil
}

// countList renders "a (3 HASHes), b (1 HASH)", by name.
func countList(m map[string]int64, one, many string) string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, k := range names {
		parts[i] = fmt.Sprintf("%s (%d %s)", k, m[k], plural(m[k], one, many))
	}
	return strings.Join(parts, ", ")
}
