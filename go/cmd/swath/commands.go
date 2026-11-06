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
	"github.com/varveio/swath/go/internal/textout"
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
		if slices.Contains(resumeFlags, fl.Name) {
			f.Var(fl.Value, fl.Name, fl.Usage)
		}
	})
	var dir string
	var overrides []string
	for rest := args; ; {
		before := len(rest)
		if err := f.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				fmt.Fprintln(stdout, "usage: swath resume DIR [--"+strings.Join(resumeFlags, " ... --")+" ...]")
				return err
			}
			return usageError{fmt.Errorf("resume: %w (only %s may change)", err, strings.Join(resumeFlags, ", "))}
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
	// Every refusal below settles: it writes the report (--report). Its
	// destination, and the given durations, are checked first, as a list
	// command checks them, so no settlement replaces the dataset's own
	// files or the text file being refused.
	if err := nonNegativeDurations(f); err != nil {
		return usageError{err}
	}
	target := resumeTarget(o, dir)
	if err := checkSidePaths(&target); err != nil {
		return usageError{err}
	}
	o.report = target.report // the destination checked, which refuse writes
	// The run a refusal below is about, as read now: it is settled only
	// if it is still that run once this process holds the lock.
	seen := journal.Witness(dir)
	att := newAttempt()
	stop := func(err error) error {
		return refuse(stdout, dir, seen, err, att, o.report, o.reportFormat, redactArgv(append([]string{"resume"}, args...)))
	}
	if f, _ := textDest(dir); f != "" || dir == "-" || isFile(dir) {
		// A text run keeps no journal: there is nothing to continue.
		return stop(usageError{errText})
	}
	if dataset.Committed(dir) {
		return stop(listing.ErrComplete)
	}
	st, err := journal.Read(dir)
	if errors.Is(err, journal.ErrNoJournal) {
		if dataset.Resumable(dir) {
			// The journal's header never became durable: nothing was
			// published, and the command was not recorded.
			return stop(errNoCommand)
		}
		return stop(listing.ErrNothingToResume)
	}
	if err == nil {
		err = st.Resumable()
	}
	if err != nil {
		return stop(err)
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
		return stop(fmt.Errorf("resume: the journal in %s records no command (written by an older build): finish it with that build or start over", dir))
	}
	// The recorded command without --overwrite (the output is resumed, not
	// replaced), then the overrides, then the directory as given (it may
	// have moved).
	var cmd []string
	var secrets []string // secret flags the run had, whose values were not recorded
	for _, a := range base {
		n := strings.TrimLeft(a, "-")
		if name, v, _ := strings.Cut(n, "="); a != n && (name == "overwrite" || name == "force") && (v == "" || v == "true") {
			continue
		}
		if name, v, ok := strings.Cut(n, "="); a != n && ok && v == redacted && slices.Contains(secretFlags, name) {
			secrets = append(secrets, name)
			continue
		}
		cmd = append(cmd, a)
	}
	for _, name := range secrets {
		given := false
		f.Visit(func(fl *flag.Flag) { given = given || fl.Name == name })
		if !given {
			return stop(usageError{fmt.Errorf("the run in %s was started with --%s, which is never recorded: give it again (swath resume %s --%s ...)", dir, name, dir, name)})
		}
	}
	cmd = append(append(cmd, overrides...), "--output", dir)
	// Parsed with the diagnostic flag set, a superset of swath list's: the
	// runtime overrides include diagnostic ones (--spill-reserve,
	// --spill-compress), and a recorded swath list command holds none
	// that could change meaning.
	return runWith(ctx, cmd, stdout, true, argv, true, att)
}

// resumeTarget is o (the resume's flags) with the output a resume of dir
// names: the text file (or stdout) of a text run, refused below, or the
// dataset directory.
func resumeTarget(o options, dir string) options {
	switch {
	case dir == "-":
		o.text = &textout.Config{}
	case isFile(dir):
		o.text = &textout.Config{Path: dir}
	default:
		if f, _ := textDest(dir); f != "" {
			o.text = &textout.Config{Path: dir}
		} else {
			o.out = dir
		}
	}
	return o
}

// refuse settles a resume that stopped before it could run: its outcome
// replaces the interrupted run's in error.json (under the output lock), so
// a supervisor never acts on a stale one; a committed dataset, a directory
// without a run, an output another process owns and a run that changed
// since seen (its journal.Witness) keep theirs, and the outcome goes to
// stderr. The report (--report, if given) holds it in every case.
//
// A dataset already committed and complete is no refusal: the resume
// exits 0 (idempotent, as Java's), its report saying complete.
func refuse(stdout io.Writer, dir, seen string, err error, att *listing.Attempt, report, format string, argv []string) error {
	o := classify(err, nil, dir)
	o.Attempt = att
	r := listing.Unstarted(argv, dir, att, err)
	r.Invocation.Resume = true
	account(&r.Outcome, o)
	if errors.Is(err, listing.ErrComplete) {
		committedAccount(dir, &r)
	}
	done := o.ExitCode == exitCompleted
	if done {
		alreadyComplete(stdout, format, dir, &r)
	} else if recorded, why := settleOutside(dir, seen, o); !recorded {
		r.Outcome.Unrecorded = why
		reportUnrecorded(dir, o, why)
	}
	writeReport(report, r)
	if done {
		if format == "json" {
			enc := json.NewEncoder(stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(r)
		}
		return nil
	}
	return &exitError{err, o.ExitCode}
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
	if u := st.Unresumable; u != nil {
		fmt.Fprintf(stdout, "marked unresumable (%s) at %s: %s; swath resume refuses it, swath list --overwrite starts over\n", u.Cause, u.At.Format("2006-01-02T15:04:05Z"), u.Reason)
	}
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
		fmt.Fprintf(stdout, "torn tail: %d bytes (dropped by swath resume; its part, if written, must be reproduced exactly)\n", st.TornBytes)
	}
	if ents, _ := os.ReadDir(filepath.Join(dir, dataset.Workspace, dataset.SuspectDir)); len(ents) > 0 {
		fmt.Fprintf(stdout, "set aside: %s (a part whose record read as torn; swath resume must reproduce it exactly)\n", ents[0].Name())
	}
	return nil
}

// isFile reports whether path is an existing non-directory.
func isFile(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir()
}
