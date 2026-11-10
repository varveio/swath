// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/textout"
)

// errNoCommand: an interrupted run that stopped before its journal header
// was durable.
var errNoCommand = errors.New("the interrupted run stopped before recording its command; nothing was published: run swath list again with --overwrite")

// resume continues the interrupted run in DIR with the command its journal
// recorded; only the runtime flags may be given again.
func resume(ctx context.Context, args []string, stdout io.Writer) error {
	var o options
	f := newFlags(&o, true)
	p, err := parseFlags(f, "resume", args, func(fl *pflag.Flag) error {
		if !mayChange(fl.Name) {
			return usageError{fmt.Errorf("--%s is fixed for the run and cannot change on resume (see 'swath resume --help')", fl.Name)}
		}
		return nil
	})
	setLog(o.term, "")
	if errors.Is(err, pflag.ErrHelp) {
		resumeUsage(stdout, f, styleFor(stdout, o.term.color, p.longHelp))
		return err
	}
	switch {
	case err != nil:
		return err
	case len(p.operands) == 0:
		return seeHelp{usageError{errors.New("missing DIR")}, "resume"}
	case len(p.operands) > 1:
		return seeHelp{usageError{fmt.Errorf("unexpected argument %q", registry.Redact(p.operands[1]))}, "resume"}
	}
	dir, overrides := p.operands[0], p.given
	// Every refusal below settles: it writes the report (--report). Its
	// destination is checked first, as a list command checks it, so no
	// settlement replaces the dataset's own files or the text file being
	// refused.
	target := resumeTarget(o, dir)
	if err := checkSidePaths(&target); err != nil {
		return usageError{err}
	}
	o.report = target.report // the destination checked, which refuse writes
	// The run a refusal below is about, as read now: it is settled only
	// if it is still that run once this process holds the lock.
	seen := journal.Witness(dir)
	att := newAttempt()
	setLog(o.term, att.ID)
	stop := func(err error) error {
		return refuse(stdout, dir, seen, err, att, o.report, commandLine("resume", p, p.operands...), o.term.verbose > 0)
	}
	if textRun(dir) {
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
	// The recorded command is canonical (parseList): its flags, each one
	// argument (--name=value, or --name), then SOURCE, after "--" when it
	// looks like a flag.
	flags, operand := base, []string(nil)
	if i := slices.IndexFunc(base, func(a string) bool { return a == "--" || !strings.HasPrefix(a, "--") }); i >= 0 {
		flags, operand = base[:i], base[i:]
	}
	flags = renameRecorded(flags)
	if name := unknownFlag(f, flags); name != "" {
		return stop(usageError{fmt.Errorf("resume: the run in %s was recorded with --%s, which this build does not take (renamed or removed): finish it with the build that started it, or start over with swath list --overwrite", dir, name)})
	}
	// The recorded flags without --overwrite (the output is resumed, not
	// replaced), then the overrides, then the directory as given (it may
	// have moved), then SOURCE.
	var cmd []string
	var secrets []string // secret flags the run had, whose values were not recorded
	for _, a := range flags {
		name, v, eq := strings.Cut(strings.TrimPrefix(a, "--"), "=")
		if name == "overwrite" && (!eq || v == "true") {
			continue
		}
		if eq && v == redacted && slices.Contains(secretFlags, name) {
			secrets = append(secrets, name)
			continue
		}
		cmd = append(cmd, a)
	}
	for _, name := range secrets {
		given := f.Changed(name)
		// The provider reads the credential from its variable when the
		// flag is absent (registry.Secret.Env), so a rotated secret may
		// come from either source.
		if env := secretEnv[name]; env != "" && os.Getenv(env) != "" {
			given = true
		}
		if !given {
			hint := ""
			if env := secretEnv[name]; env != "" {
				hint = " or set " + env
			}
			return stop(usageError{fmt.Errorf("the run in %s was started with --%s, which is never recorded: give it again (swath resume %s --%s ...)%s", dir, name, dir, name, hint)})
		}
	}
	cmd = append(append(append(cmd, overrides...), "--output="+dir), operand...)
	// Parsed with the diagnostic flag set, a superset of swath list's: the
	// runtime overrides include diagnostic ones (--spill-reserve,
	// --no-spill-compress), and a recorded swath list command holds none
	// that could change meaning.
	return runWith(ctx, command{diag: true}, cmd, stdout, argv, true, att)
}

// renamedFlags are the flags renamed since a journal could record them,
// by the name recorded: the same flag and value under a new spelling, so
// a resume replays the recorded value under the new name. A flag whose
// value changed meaning (--memory N is --sort-buffer N/2) is not here: its
// journal is refused.
var renamedFlags = map[string]string{"row-budget": "sort-buffer"}

// renameRecorded is the canonical recorded flags (--name=value, --name)
// with each renamed flag under its new name (renamedFlags). The journal
// keeps the command as it was recorded.
func renameRecorded(flags []string) []string {
	out := make([]string, len(flags))
	for i, a := range flags {
		name, v, eq := strings.Cut(strings.TrimPrefix(a, "--"), "=")
		if to, ok := renamedFlags[name]; ok {
			a = "--" + to
			if eq {
				a += "=" + v
			}
		}
		out[i] = a
	}
	return out
}

// unknownFlag is the first of the canonical flags (--name=value, --name)
// that f does not define ("" if none): a recorded command from a build
// whose flags were since renamed or removed.
func unknownFlag(f *pflag.FlagSet, flags []string) string {
	for _, a := range flags {
		name, _, _ := strings.Cut(strings.TrimPrefix(a, "--"), "=")
		if f.Lookup(name) == nil {
			return name
		}
	}
	return ""
}

// resumeTarget is o (the resume's flags) with the output a resume of dir
// names: the text file (or stdout) of a text run, refused below, or the
// dataset directory.
func resumeTarget(o options, dir string) options {
	switch {
	case dir == "-":
		o.text = &textout.Config{}
	case textRun(dir):
		o.text = &textout.Config{Path: dir}
	default:
		o.out = dir
	}
	return o
}

// textRun says DIR names a text run's output, which keeps no journal:
// stdout, a file, or a text file's name (rows.tsv) that is not a
// directory. What is on disk decides: a directory named rows.tsv (-o
// rows.tsv/) holds a dataset or a run.
func textRun(dir string) bool {
	if dir == "-" || isFile(dir) {
		return true
	}
	fi, err := os.Stat(dir)
	f, _ := textDest(dir)
	return f != "" && (err != nil || !fi.IsDir())
}

// refuse settles a resume that stopped before it could run: its outcome
// replaces the interrupted run's in error.json (under the output lock), so
// a supervisor never acts on a stale one; a committed dataset, a directory
// without a run, an output another process owns and a run that changed
// since seen (its journal.Witness) keep theirs, and the outcome goes to
// stderr under -v (verbose). The report (--report, if given) holds it in
// every case.
//
// A dataset already committed and complete is no refusal: the resume
// exits 0 (idempotent, as Java's), its report saying complete.
func refuse(stdout io.Writer, dir, seen string, err error, att *listing.Attempt, report string, argv []string, verbose bool) error {
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
		alreadyComplete(stdout, report, dir, &r)
	} else if recorded, why := settleOutside(dir, seen, o); !recorded {
		// A refusal is one line on stderr (conclude); -v adds why the
		// outcome is not in error.json.
		r.Outcome.Unrecorded = why
		if verbose {
			reportUnrecorded(dir, why)
		}
	}
	rerr := writeReport(report, r, stdout)
	if done {
		return reportFailed(nil, rerr)
	}
	return reportFailed(&exitError{error: err, code: o.ExitCode, out: o}, rerr)
}

// show prints what an output directory holds: a committed dataset or an
// interrupted run; --journal dumps the journal's records as JSON lines.
func show(args []string, stdout io.Writer) error {
	f := pflag.NewFlagSet("show", pflag.ContinueOnError)
	f.SetOutput(io.Discard)
	var raw bool
	var t termOptions
	t.registerGlobal(f)
	source.Define(f, source.Flag{Name: "journal", Value: source.Switch(&raw), Usage: "dump the journal's header and records as JSON lines"})
	p, err := parseFlags(f, "show", args, nil)
	setLog(t, "")
	if errors.Is(err, pflag.ErrHelp) {
		s := styleFor(stdout, t.color, p.longHelp)
		var b strings.Builder
		showHelp.write(&b, s)
		s.heading(&b, "Options")
		writeFlags(&b, []*pflag.Flag{f.Lookup("journal")}, s, column([]*pflag.Flag{f.Lookup("journal")}))
		fmt.Fprint(stdout, b.String())
		return err
	}
	switch {
	case err != nil:
		return err
	case len(p.operands) != 1:
		return seeHelp{usageError{errors.New("swath show takes one DIR")}, "show"}
	}
	if err := t.validate(); err != nil {
		return usageError{err}
	}
	dir := p.operands[0]
	if raw {
		st, err := journal.Read(dir)
		switch {
		case errors.Is(err, journal.ErrNoJournal) && dataset.Committed(dir):
			return fmt.Errorf("%s is a committed dataset, which keeps no journal (swath show %s prints its summary)", dir, dir)
		case errors.Is(err, journal.ErrNoJournal):
			return fmt.Errorf("%s holds no journal: no interrupted run", dir)
		case err != nil:
			return err
		}
		st.Header.Argv = redactArgv(st.Header.Argv)
		st.Header.Identity = st.Header.Identity.Redacted()
		return st.Dump(stdout)
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
		format := "SeekParquet format " + strconv.Itoa(m.FormatVersion)
		if m.DraftRevision > 0 {
			format += " draft " + strconv.Itoa(m.DraftRevision)
		}
		metadata := "no swath metadata"
		if m.MetadataVersion > 0 {
			metadata = "swath metadata version " + strconv.Itoa(m.MetadataVersion)
		}
		fmt.Fprintf(stdout, "dataset %s: %s, swath schema %s, %s\n", dir, format, m.SchemaID, metadata)
		fmt.Fprintf(stdout, "key %s unique=%v, published %s\n", m.Key.Column, m.Key.Unique, m.PublishedAt.Format("2006-01-02T15:04:05Z"))
		fmt.Fprintf(stdout, "source %s, scope %s %q\n", registry.Redact(m.Source.URI), m.Scope.Kind, m.Scope.Prefix)
		c := m.Coverage
		fmt.Fprintf(stdout, "coverage complete=%v stop=%s skipped=%d filters=%d exclusions=%d\n", c.Complete, c.StopReason, c.SkippedEntries, len(c.Filters), len(c.Exclusions))
		fmt.Fprintf(stdout, "%d %s, %d %s, %d %s", len(m.Parts), plural(len(m.Parts), "part", "parts"), rows, plural(rows, "row", "rows"), bytes, plural(bytes, "byte", "bytes"))
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
	fmt.Fprintf(stdout, "command: swath %s\n", strings.Join(redactArgv(h.Argv), " "))
	id := h.Identity.Redacted()
	fmt.Fprintf(stdout, "identity: provider %s, schema %s, writer %s, part size %d", id.Provider, id.SchemaID, id.Writer, id.PartSize)
	for _, f := range id.Source {
		fmt.Fprintf(stdout, ", %s %s", f.Name, f.Value)
	}
	fmt.Fprintln(stdout)
	if u := st.Unresumable; u != nil {
		fmt.Fprintf(stdout, "marked unresumable (%s) at %s: %s; swath resume refuses it, swath list --overwrite starts over\n", u.Cause, u.At.Format("2006-01-02T15:04:05Z"), u.Reason)
	}
	if last := st.Last(); last != nil {
		fmt.Fprintf(stdout, "%d %s committed, %d %s; swath resume lists after %q", len(st.Records), plural(len(st.Records), "part", "parts"),
			last.RowsTotal, plural(last.RowsTotal, "row", "rows"), string(last.Part.MaxKey))
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
