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
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	redis "github.com/adbc-drivers/redis/go"
)

// hashFlags are the flags that pick a HASH collection and shape its
// guessed columns (check, scan).
type hashFlags struct {
	prefix    string
	sample    int
	keyColumn string
	types     options
	renames   options
}

func (h *hashFlags) register(fs *flag.FlagSet) {
	h.types, h.renames = options{}, options{}
	fs.StringVar(&h.prefix, "prefix", "", "the collection's key `prefix`, such as user:")
	fs.IntVar(&h.sample, "sample", 1000, "HASHes read to guess the columns; -1 reads all of them")
	fs.StringVar(&h.keyColumn, "key-column", "_key", "column that holds each HASH's key; empty for none")
	fs.Var(h.types, "type", "a column's type, `column=TYPE` such as age=BIGINT (repeatable)")
	fs.Var(h.renames, "rename", "a field's column name, `field=column` (repeatable)")
}

func runCheck(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("check", "", stderr)
	var cf connFlags
	cf.register(fs)
	var hf hashFlags
	hf.register(fs)
	schema := fs.String("schema", "", "schema of the table a copy makes (default: the database's)")
	table := fs.String("table", "", "name of the table a copy makes (default: made from the prefix)")
	asJSON := fs.Bool("json", false, "write the report as JSON")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q (flags go before them)", fs.Args())
	}
	r, err := redis.InspectHashes(ctx, cf.options(), redis.HashInspectOptions{
		Prefix: hf.prefix, Schema: *schema, Table: *table, Sample: hf.sample,
		KeyColumn: hf.keyColumn, Types: hf.types, Renames: hf.renames,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	}
	if hf.prefix == "" {
		printCandidates(stdout, r)
		return nil
	}
	printReport(stdout, r)
	return nil
}

func serverLine(r *redis.HashReport) string {
	kind := "standalone"
	if r.Server.Cluster {
		kind = "cluster"
	}
	if r.Server.Version != "" {
		return fmt.Sprintf("Redis %s (%s)", r.Server.Version, kind)
	}
	return "Redis (" + kind + ")"
}

// shownTables is how many driver tables check lists without a prefix.
const shownTables = 10

func printCandidates(w io.Writer, r *redis.HashReport) {
	var apps, tables []redis.HashCollection
	for _, c := range r.Candidates {
		if c.Table != "" {
			tables = append(tables, c)
		} else {
			apps = append(apps, c)
		}
	}
	fmt.Fprintf(w, "HASH collections on %s, from a sample of the keyspace:\n\n", serverLine(r))
	if len(apps) == 0 {
		fmt.Fprintln(w, "  (none besides driver tables: no other HASH key has a ':' in its name, and no other search index is on HASHes)")
	} else {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  PREFIX\tSAMPLED KEYS\tSEARCH INDEX")
		for _, c := range apps {
			fmt.Fprintf(tw, "  %s\t%d\t%s\n", c.Prefix, c.Keys, c.Index)
		}
		_ = tw.Flush()
	}
	if len(tables) > 0 {
		fmt.Fprintf(w, "\nDriver tables, which SQL already reads (%d):\n", len(tables))
		for i, c := range tables {
			if i == shownTables {
				fmt.Fprintf(w, "  … and %d more\n", len(tables)-shownTables)
				break
			}
			fmt.Fprintf(w, "  %s (%s)\n", c.Table, c.Prefix)
		}
	}
	fmt.Fprintln(w, "\nCheck one with: redis-arrow check -prefix <prefix>")
}

func printReport(w io.Writer, r *redis.HashReport) {
	fmt.Fprintf(w, "Collection %s on %s\n", r.Prefix, serverLine(r))
	fmt.Fprintf(w, "  %d %s; the columns are guessed from %d of them\n", r.Keys, plural(r.Keys, "HASH key", "HASH keys"), r.Sampled)
	if r.DriverTable != "" {
		fmt.Fprintf(w, "  these are the rows of the driver table %s\n", r.DriverTable)
	}
	for _, ix := range r.Indexes {
		fmt.Fprintf(w, "  search index %s (prefix %s): %d documents, %d indexing failures\n",
			ix.Name, strings.Join(ix.Prefixes, " "), ix.Documents, ix.Failures)
	}
	if r.UnreadableIndexes > 0 {
		fmt.Fprintf(w, "  (%d search indexes are on keys this user may not read, and left out)\n", r.UnreadableIndexes)
	}
	if len(r.Columns) > 0 {
		fmt.Fprintf(w, "\nGuessed columns (as table %s.%s):\n", r.Schema, r.Table)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  COLUMN\tTYPE\tARROW TYPE\tPRESENT\tNOTES")
		for _, c := range r.Columns {
			name := c.Name
			if c.Field != "" && c.Field != c.Name {
				name += " (field " + c.Field + ")"
			}
			present := "100%"
			if r.Sampled > 0 && c.Present < r.Sampled {
				present = fmt.Sprintf("%.1f%%", 100*float64(c.Present)/float64(r.Sampled))
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\n", name, c.Type, c.ArrowType, present, strings.Join(c.Notes, "; "))
		}
		_ = tw.Flush()
	}
	for _, cl := range r.Checks {
		verdict := "READY"
		if !cl.Ready {
			verdict = "NOT READY"
		}
		fmt.Fprintf(w, "\n%s: %s\n", cl.Title, verdict)
		for _, it := range cl.Items {
			fmt.Fprintf(w, "  %-9s %s\n", "["+it.Status+"]", it.Text)
			if it.Fix != "" {
				fmt.Fprintf(w, "  %-9s fix: %s\n", "", it.Fix)
			}
		}
		if cl.Command != "" {
			fmt.Fprintf(w, "  run: %s\n", cl.Command)
		}
	}
}
