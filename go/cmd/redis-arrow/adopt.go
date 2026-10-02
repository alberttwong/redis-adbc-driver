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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	redis "github.com/adbc-drivers/redis/go"
)

func runAdopt(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("adopt", "", stderr)
	var cf connFlags
	cf.register(fs)
	prefix := fs.String("prefix", "", "the collection's key `prefix`, such as user:")
	schema := fs.String("schema", "", "the table's schema (default: the database's)")
	table := fs.String("table", "", "the table's name (default: made from the prefix)")
	types, renames := options{}, options{}
	fs.Var(types, "type", "a column's type in place, `column=TYPE` such as age=SMALLINT (repeatable)")
	fs.Var(renames, "rename", "a field's column name, `field=column` (repeatable)")
	indexColumns := fs.String("index-columns", "", "comma-separated `columns` to index (default: every one that can be)")
	trust := fs.Bool("trust-strings", false, "trust the string values to stay as they are now (see the README)")
	refresh := fs.Bool("refresh", false, "write __rowid into the HASHes the application added to an adopted table since")
	apply := fs.Bool("apply", false, "do it; without -apply, adopt only checks and prints what it would do")
	asJSON := fs.Bool("json", false, "write the result as JSON")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q (flags go before them)", fs.Args())
	}
	if *prefix == "" {
		return errors.New("-prefix is required (redis-arrow check lists the collections)")
	}
	o := redis.HashAdoptOptions{Prefix: *prefix, Schema: *schema, Table: *table, Types: types, Renames: renames,
		TrustStrings: *trust, Refresh: *refresh, Apply: *apply,
		Progress: func(msg string) { fmt.Fprintln(stderr, msg) }}
	if *indexColumns != "" {
		for _, c := range strings.Split(*indexColumns, ",") {
			o.IndexColumns = append(o.IndexColumns, strings.TrimSpace(c))
		}
	}
	res, err := redis.AdoptHashes(ctx, cf.options(), o)
	if res != nil && *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if jerr := enc.Encode(res); jerr != nil && err == nil {
			err = jerr
		}
		return err
	}
	if res != nil {
		printAdoption(stdout, res)
	}
	return err
}

func printAdoption(w io.Writer, res *redis.HashAdoption) {
	r := res.Report
	if len(r.Checks) > 0 {
		printReport(w, r)
	} else {
		fmt.Fprintf(w, "Collection %s on %s: the adopted table %s\n", r.Prefix, serverLine(r), res.Table)
	}
	ready := len(r.Checks) == 0 || r.Checks[0].Ready
	switch {
	case res.Applied && res.Pending > 0 && len(r.Checks) == 0:
		fmt.Fprintf(w, "\nWrote __rowid into %d %s, which SQL now reads.\n", res.RowIDsWritten, plural(res.RowIDsWritten, "HASH", "HASHes"))
	case res.Applied && len(r.Checks) == 0:
		fmt.Fprintln(w, "\nEvery HASH with a row id suffix was in the table already.")
	case res.Applied:
		fmt.Fprintf(w, "\nAdopted: %s now reads the HASHes under %s (index %s; wrote __rowid into %d %s).\n",
			res.Table, r.Prefix, res.Index, res.RowIDsWritten, plural(res.RowIDsWritten, "HASH", "HASHes"))
	case !ready:
		fmt.Fprintln(w, "\nNothing was changed: fix the blockers first.")
		return
	default:
		if len(r.Checks) == 0 {
			fmt.Fprintf(w, "\n%d %s no __rowid yet.\n", res.Pending, plural(res.Pending, "HASH has", "HASHes have"))
		}
		fmt.Fprintln(w, "\nNothing was changed. With -apply, adopt would:")
		for i, s := range res.Steps {
			fmt.Fprintf(w, "  %d. %s\n", i+1, s)
		}
	}
	if res.Left > 0 {
		fmt.Fprintf(w, "Left out %d %s whose values don't fit their columns, such as %s.\n", res.Left, plural(res.Left, "HASH", "HASHes"), res.LeftExample)
	}
	if len(res.UnknownFields) > 0 {
		fmt.Fprintf(w, "Fields that aren't columns: %s.\n", countList(res.UnknownFields, "HASH", "HASHes"))
	}
}
