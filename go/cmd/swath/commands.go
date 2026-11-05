// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
)

// errNoCommand: an interrupted run that stopped before its journal header
// was durable.
var errNoCommand = errors.New("the interrupted run stopped before recording its command; nothing was published: run swath list again with --overwrite")

// resume continues the interrupted run in DIR with the command its journal
// recorded; only the runtime flags may be given again.
func resume(ctx context.Context, args []string, stdout io.Writer) error {
	var o options
	f := flag.NewFlagSet("resume", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	all := newFlags(&o, true)
	all.VisitAll(func(fl *flag.Flag) {
		if slices.Contains(runtimeFlags, fl.Name) {
			f.Var(fl.Value, fl.Name, fl.Usage)
		}
	})
	var dir string
	var overrides []string
	for rest := args; ; {
		before := len(rest)
		if err := f.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				fmt.Fprintln(stdout, "usage: swath resume DIR [--"+strings.Join(runtimeFlags, " ... --")+" ...]")
				return err
			}
			return usageError{fmt.Errorf("resume: %w (only %s may change)", err, strings.Join(runtimeFlags, ", "))}
		}
		overrides = append(overrides, rest[:before-len(f.Args())]...)
		if f.NArg() == 0 {
			break
		}
		if dir != "" {
			return usageError{fmt.Errorf("unexpected argument %q", f.Arg(0))}
		}
		dir, rest = f.Arg(0), f.Args()[1:]
	}
	if dir == "" {
		return usageError{errors.New("usage: swath resume DIR [runtime flags]")}
	}
	if dataset.Committed(dir) {
		return listing.ErrComplete
	}
	st, err := journal.Read(dir)
	if errors.Is(err, journal.ErrNoJournal) {
		if dataset.Resumable(dir) {
			// The journal's header never became durable: nothing was
			// published, and the command was not recorded.
			return errNoCommand
		}
		return listing.ErrNothingToResume
	}
	if err != nil {
		return err
	}
	argv := st.Header.Argv
	diag := len(argv) >= 2 && argv[0] == "diag" && argv[1] == "list"
	var base []string
	switch {
	case diag:
		base = argv[2:]
	case len(argv) >= 1 && argv[0] == "list":
		base = argv[1:]
	default:
		return fmt.Errorf("resume: the journal in %s records no command (written by an older build): finish it with that build or start over", dir)
	}
	// The recorded command without --overwrite (the output is resumed, not
	// replaced), then the overrides, then the directory as given (it may
	// have moved).
	var cmd []string
	for _, a := range base {
		if n := strings.TrimLeft(a, "-"); a != n && (n == "overwrite" || n == "overwrite=true") {
			continue
		}
		cmd = append(cmd, a)
	}
	cmd = append(append(cmd, overrides...), "--output", dir)
	return runWith(ctx, cmd, stdout, diag, argv, true)
}

// show prints what an output directory holds: a committed dataset or an
// interrupted run; --journal dumps the journal's records as JSON lines.
func show(args []string, stdout io.Writer) error {
	f := flag.NewFlagSet("show", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	raw := f.Bool("journal", false, "dump the journal's header and records as JSON lines")
	if err := f.Parse(args); err != nil {
		return usageError{err}
	}
	if f.NArg() != 1 {
		return usageError{errors.New("usage: swath show [--journal] DIR")}
	}
	dir := f.Arg(0)
	if *raw {
		return journal.Dump(stdout, dir)
	}
	if dataset.Committed(dir) {
		var m dataset.Manifest
		b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &m); err != nil {
			return fmt.Errorf("manifest: %w", err)
		}
		var rows, bytes int64
		for _, p := range m.Parts {
			rows += p.Rows
			bytes += p.Bytes
		}
		fmt.Fprintf(stdout, "dataset %s (format %s %d, schema %s, published %s)\n", dir, m.Format, m.FormatVersion, m.SchemaID, m.PublishedAt.Format("2006-01-02T15:04:05Z"))
		fmt.Fprintf(stdout, "source %s, scope %s %q\n", m.Source.URI, m.Scope.Kind, m.Scope.Prefix)
		c := m.Coverage
		fmt.Fprintf(stdout, "coverage complete=%v stop=%s skipped=%d filters=%d exclusions=%d\n", c.Complete, c.StopReason, c.SkippedEntries, len(c.Filters), len(c.Exclusions))
		fmt.Fprintf(stdout, "%d parts, %d rows, %d bytes", len(m.Parts), rows, bytes)
		if n := len(m.Parts); n > 0 {
			fmt.Fprintf(stdout, ", keys %q .. %q", string(m.Parts[0].MinKey), string(m.Parts[n-1].MaxKey))
		}
		fmt.Fprintln(stdout)
		return nil
	}
	st, err := journal.Read(dir)
	if errors.Is(err, journal.ErrNoJournal) {
		return fmt.Errorf("%s holds neither a dataset nor an interrupted run", dir)
	}
	if err != nil {
		return err
	}
	h := st.Header
	fmt.Fprintf(stdout, "interrupted run in %s (journal version %d, started %s)\n", dir, h.Version, h.Created.Format("2006-01-02T15:04:05Z"))
	fmt.Fprintf(stdout, "command: swath %s\n", strings.Join(h.Argv, " "))
	id := h.Identity
	fmt.Fprintf(stdout, "identity: provider %s, schema %s, writer %s, part size %d", id.Provider, id.SchemaID, id.Writer, id.PartSize)
	for _, f := range id.Source {
		fmt.Fprintf(stdout, ", %s %s", f.Name, f.Value)
	}
	fmt.Fprintln(stdout)
	if last := st.Last(); last != nil {
		fmt.Fprintf(stdout, "%d parts committed, %d rows; swath resume lists after %q", len(st.Records), last.RowsTotal, string(last.Part.MaxKey))
		if last.Final {
			fmt.Fprint(stdout, " (the final part is durable: resume only publishes)")
		}
		fmt.Fprintln(stdout)
	} else {
		fmt.Fprintln(stdout, "no part committed; swath resume lists from the start")
	}
	if st.TornBytes > 0 {
		fmt.Fprintf(stdout, "torn tail: %d bytes (ignored)\n", st.TornBytes)
	}
	return nil
}
