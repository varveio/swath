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
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/textout"
	"github.com/varveio/swath/reader"
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
			return usageError{fmt.Errorf("--%s is fixed for the run and cannot change on resume", fl.Name)}
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
	if err := checkDatasetName(dir); err != nil {
		return stop(usageError{fmt.Errorf("resume %w", err)})
	}
	if textRun(dir) {
		// A text run keeps no journal: there is nothing to continue.
		return stop(usageError{errText})
	}
	if dataset.NextFound(dir) {
		// A replacement committed whose installation a crash cut short is
		// installed; the leftovers of one installed (a crash before their
		// removal) are removed.
		if err := listing.InstallReplacement(dir); err != nil {
			return stop(err)
		}
	}
	// The run to continue: in dir, or a replacement of the dataset there
	// (--overwrite) in its own directory.
	at := dir
	if dataset.Staged(dir) {
		at = dataset.NextDir(dir)
	}
	if dataset.Committed(dir) && !dataset.Unfinalized(dir) && at == dir {
		// A bucket dataset's work directory committed but not finalized goes
		// on: the run finishes the commit's aftermath, then reports complete.
		return stop(finalizedSample(dir, listing.ErrComplete))
	}
	if dataset.InterruptedV03(dir) {
		return stop(dataset.ErrPriorRelease)
	}
	st, err := journal.Read(at)
	if errors.Is(err, journal.ErrNoJournal) {
		if dataset.Resumable(at) {
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
	// replaced) and without --report (each attempt's report is its own: a
	// resume writes one only when given one, never over the earlier
	// attempt's), then the overrides, then the directory as given (it may
	// have moved: the dataset directory, or a bucket dataset's work
	// directory, whose -o stays as recorded), then SOURCE.
	var cmd []string
	var secrets []string // secret flags the run had, whose values were not recorded
	remote := false      // a bucket -o: DIR is its work directory, the bucket stays as recorded
	for _, a := range flags {
		name, v, eq := strings.Cut(strings.TrimPrefix(a, "--"), "=")
		if name == "overwrite" && (!eq || v == "true") || name == "report" {
			continue
		}
		if name == "work-dir" {
			remote = true
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
			return stop(usageError{fmt.Errorf("the run in %s was started with --%s, which is never recorded: give it again (swath resume %s --%s ...)%s", dir, name, shellOperand(dir), name, hint)})
		}
	}
	output := "--output=" + dir
	if remote {
		output = "--work-dir=" + dir
	}
	cmd = append(append(append(cmd, overrides...), output), operand...)
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
	done := o.ExitCode == exitOK
	if done {
		alreadyComplete(stdout, report, dir, &r)
		elog().ended(o.withCoverage(r.Outcome))
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
	o = o.withCoverage(r.Outcome)
	return reportFailed(&exitError{error: err, code: o.ExitCode, out: o}, rerr)
}

// showFlags defines swath show's own flags on f, the ones its help lists.
func showFlags(f *pflag.FlagSet, raw *bool) []*pflag.Flag {
	source.Define(f, source.Flag{Name: "journal", Value: source.Switch(raw), Usage: "dump the journal's header and records as JSON lines (diagnostic: the journal's private format, which may change in any release)"})
	return []*pflag.Flag{f.Lookup("journal")}
}

// show prints what an output directory holds: a committed dataset or an
// interrupted run; -v adds the dataset's format id and the run's journal
// version and frozen identity; --journal dumps the journal's records as
// JSON lines.
func show(args []string, stdout io.Writer) error {
	f := pflag.NewFlagSet("show", pflag.ContinueOnError)
	f.SetOutput(io.Discard)
	var raw bool
	var t termOptions
	t.registerGlobal(f)
	own := showFlags(f, &raw)
	p, err := parseFlags(f, "show", args, nil)
	setLog(t, "")
	if errors.Is(err, pflag.ErrHelp) {
		s := styleFor(stdout, t.color, p.longHelp)
		var b strings.Builder
		showHelp.write(&b, s)
		s.heading(&b, "Options")
		writeFlags(&b, own, s, column(own))
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
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%s: no such file or directory", dir)
	}
	if fi, err := os.Stat(dir); err == nil && !fi.IsDir() {
		return fmt.Errorf("%s holds neither a dataset nor an interrupted run", dir) // a file (a text listing, say)
	}
	if raw {
		at := dir
		if dataset.Staged(dir) {
			at = dataset.NextDir(dir) // the run that replaces the dataset in dir
		}
		st, err := journal.Read(at)
		switch {
		case errors.Is(err, journal.ErrNoJournal) && dataset.Committed(dir):
			return fmt.Errorf("%s is a committed dataset, which keeps no journal (swath show %s prints its summary)", dir, shellOperand(dir))
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
		name, commit := dir, ""
		b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
		if errors.Is(err, os.ErrNotExist) {
			// A bucket dataset's work directory: the dataset is the
			// destination its commit marker names, its manifest the one
			// the work directory committed there.
			rec, merr := commitMarker(dir)
			if merr != nil {
				err = merr
			} else if rec != nil {
				b, err = dataset.CommittedCandidate(dir)
				name = registry.Redact(rec.Destination)
				commit = "committed from the work directory " + dir + " (finalized)"
				if !rec.Finalized {
					commit = "committed from the work directory " + dir + ", its cleanup unfinished (swath resume " + shellOperand(dir) + " finishes it)"
				}
			}
		}
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &m); err != nil {
			return fmt.Errorf("manifest: %w", err)
		}
		writeSummary(stdout, name, &m, stdoutWidth(), t.verbose > 0)
		if commit != "" {
			fmt.Fprintln(stdout, commit)
		}
		if dataset.Staged(dir) {
			fmt.Fprintf(stdout, "a run that replaces it (--overwrite) was interrupted: swath resume %s continues it, and this dataset stays until it commits\n", shellOperand(dir))
		}
		return nil
	}
	if dataset.InstallPending(dir) {
		fmt.Fprintf(stdout, "%s holds a replacement dataset whose installation was cut short: swath resume %s installs it\n", dir, shellOperand(dir))
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
	fmt.Fprintf(stdout, "%s: interrupted run\n", dir)
	started := localTime(h.Created.Time)
	if t.verbose > 0 {
		started += " (journal version " + strconv.Itoa(h.Version) + ")"
	}
	label(stdout, "started", started)
	label(stdout, "command", "swath "+shellLine(redactArgv(h.Argv)))
	if t.verbose > 0 {
		label(stdout, "identity", identityLine(h.Identity.Redacted()))
	}
	if u := st.Unresumable; u != nil {
		label(stdout, "refused", fmt.Sprintf("marked unresumable (%s) at %s: %s; swath resume refuses it, swath list --overwrite starts over", u.Cause, localTime(u.At.Time), u.Reason))
	}
	if last := st.Last(); last != nil {
		saved := fmt.Sprintf("%s %s, %s %s; swath resume lists after %q", count(int64(len(st.Records))), plural(len(st.Records), "part", "parts"),
			count(last.RowsTotal), plural(last.RowsTotal, "row", "rows"), string(last.Part.MaxKey))
		if last.Final {
			saved += " (the final part is durable: resume only publishes)"
		}
		label(stdout, "saved", saved)
	} else {
		label(stdout, "saved", "no part; swath resume lists from the start")
	}
	if st.TornBytes > 0 {
		fmt.Fprintf(stdout, "torn tail: %s bytes (dropped by swath resume; its part, if written, must be reproduced exactly)\n", count(st.TornBytes))
	}
	if ents, _ := os.ReadDir(filepath.Join(dir, dataset.Workspace, dataset.SuspectDir)); len(ents) > 0 {
		fmt.Fprintf(stdout, "set aside: %s (a part whose record read as torn; swath resume must reproduce it exactly)\n", ents[0].Name())
	}
	return nil
}

// identityLine is a run's frozen identity (what its resume must match) for
// people: each setting and its value, a setting without one left out.
func identityLine(id journal.Identity) string {
	var fields []string
	add := func(name, value string) {
		if value != "" {
			fields = append(fields, name+" "+value)
		}
	}
	add("provider", id.Provider)
	add("schema", id.SchemaID)
	add("writer", id.Writer)
	if id.PartSize > 0 {
		add("part size", textout.Size(id.PartSize))
	}
	for _, f := range id.Source {
		add(f.Name, f.Value)
	}
	return strings.Join(fields, ", ")
}

// writeSummary prints a committed dataset's summary for people, in
// reading order under labels: the verdict on its coverage, the source,
// when it was listed, what it holds (its rows, the object bytes, the key
// range, clipped to width when that is not 0 and left out where it cannot
// fit), the coverage's account and the storage last: the Parquet parts and
// the schema, and with verbose the SeekParquet format id too.
func writeSummary(w io.Writer, name string, m *dataset.Manifest, width int, verbose bool) {
	c := &m.Coverage
	fmt.Fprintf(w, "%s: %s\n", escapeControl(name), coverageVerdict(c.Complete, c.StopReason, c.Unreadable))
	src := registry.Redact(m.Source.URI)
	if m.Scope.Kind == "whole" {
		whole := "bucket"
		switch m.Source.Kind {
		case "local":
			whole = "tree"
		case "azure":
			whole = "container"
		}
		src += " (whole " + whole + ")"
	}
	label(w, "source", escapeControl(src))
	if from, to := c.ListingStartedAt.Time, c.ListingFinishedAt.Time; !from.IsZero() && !to.IsZero() {
		label(w, "listed", localSpan(from, to)+" ("+elapsed(to.Sub(from))+")")
	}
	spec, _ := reader.Schema(reader.SchemaID(m.SchemaID))
	n := rowNouns(m.Source.Kind == "local", spec.Versions.ID != "")
	t := m.Totals()
	contents := n.of(t.Rows)
	if b := t.CommittedObjectBytes; b != nil {
		contents += ", " + textout.Size(*b)
	}
	if k := len(m.Parts); k > 0 {
		lo, hi := strconv.Quote(string(m.Parts[0].MinKey)), strconv.Quote(string(m.Parts[k-1].MaxKey))
		room := width - len(indent+labelWidth+contents+"; keys  .. ")
		switch {
		case width == 0 || len(lo)+len(hi) <= room:
			contents += "; keys " + lo + " .. " + hi
		case room >= 2*len(clipped):
			contents += "; keys " + clip(lo, room/2) + " .. " + clip(hi, room-room/2)
		}
	}
	label(w, "contents", contents)
	cov := []string{"nothing unreadable"}
	if c.Unreadable > 0 {
		cov[0] = count(c.Unreadable) + " " + plural(c.Unreadable, "entry", "entries") + " unreadable"
	}
	if len(c.Filters) > 0 {
		cov = append(cov, "filtered: "+strings.Join(filter.Texts(c.Filters), ", "))
	} else {
		cov = append(cov, "no filters")
	}
	if len(c.Exclusions) > 0 {
		cov = append(cov, "excluded: "+strings.Join(c.Exclusions, ", "))
	} else {
		cov = append(cov, "no exclusions")
	}
	label(w, "coverage", escapeControl(strings.Join(cov, "; ")))
	format := ""
	if verbose {
		format = "SeekParquet format " + strconv.Itoa(m.FormatVersion)
		if m.DraftRevision > 0 {
			format += " draft " + strconv.Itoa(m.DraftRevision)
		}
		format += ", "
	}
	parts := int64(len(m.Parts))
	label(w, "storage", fmt.Sprintf("%s %s, %s of Parquet (%sschema %s)", count(parts), plural(parts, "part", "parts"), textout.Size(t.Bytes), format, m.SchemaID))
}

// labelWidth is the room a summary's labels take: the longest, contents,
// and two blanks.
const labelWidth = "contents  "

// label prints one labelled line of a summary.
func label(w io.Writer, name, text string) {
	fmt.Fprintf(w, "%s%-*s%s\n", indent, len(labelWidth), name, text)
}

// localTime is t as the tables show a time, in local time to the second,
// with its zone's abbreviation.
func localTime(t time.Time) string { return t.Local().Format("2006-01-02 15:04:05 MST") }

// localSpan is from and to in local time, to's date left out when it is
// from's.
func localSpan(from, to time.Time) string {
	from, to = from.Local(), to.Local()
	if from.Format(time.DateOnly) == to.Format(time.DateOnly) {
		return from.Format("2006-01-02 15:04:05") + " to " + to.Format("15:04:05 MST")
	}
	return localTime(from) + " to " + localTime(to)
}

// clipped is the shortest key clip leaves: a quote, a character, the
// ellipsis and the closing quote. A narrower room leaves the range out.
const clipped = "\"k...\""

// clip cuts a quoted key to n bytes or fewer (n at least len(clipped)),
// its start kept and an ellipsis after it.
func clip(q string, n int) string {
	const more = "...\""
	if len(q) <= n {
		return q
	}
	r := []rune(q)
	for len(r) > 1 && len(string(r))+len(more) > n {
		r = r[:len(r)-1]
	}
	return string(r) + more
}

// commitMarker is the commit marker of dir, a bucket dataset's work
// directory.
func commitMarker(dir string) (*dataset.RemoteCommitRecord, error) {
	r, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return dataset.ReadCommitMarker(r)
}

// shellLine is args as one shell command line, each argument quoted as
// shellQuote does: a printed command can be pasted as it is.
func shellLine(args []string) string {
	q := make([]string, len(args))
	for i, a := range args {
		q[i] = shellQuote(a)
	}
	return strings.Join(q, " ")
}

// shellQuote is s as one word of this platform's shell (quoteFor).
func shellQuote(s string) string { return quoteFor(runtime.GOOS, s) }

// quoteFor is s as one shell word on goos: as it is when it holds only
// characters no shell there treats specially and does not begin with '='
// (zsh expands =cmd to its path), otherwise quoted: in POSIX single quotes,
// or on Windows, where cmd and PowerShell take neither, in double quotes
// (a Windows path holds no '"'; one in another argument is escaped as the
// program's argument parser reads it).
func quoteFor(goos, s string) string {
	plain := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./=:,+@%"
	if goos == "windows" {
		plain = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./=:\\"
	}
	if s != "" && !strings.HasPrefix(s, "=") && strings.Trim(s, plain) == "" {
		return s
	}
	if goos != "windows" {
		return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
	}
	// The Windows argument rules (syscall.EscapeArg's): backslashes before
	// a '"', or before the closing quote, are doubled, and the '"' escaped.
	var b strings.Builder
	b.WriteByte('"')
	slashes := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			slashes++
		case '"':
			b.WriteString(strings.Repeat(`\`, slashes+1))
			slashes = 0
		default:
			slashes = 0
		}
		b.WriteByte(s[i])
	}
	b.WriteString(strings.Repeat(`\`, slashes))
	b.WriteByte('"')
	return b.String()
}

// shellOperand is the path dir as a command's operand, quoted (shellQuote):
// one that begins with '-' is spelled ./-dir, which no parser takes for a
// flag.
func shellOperand(dir string) string {
	if strings.HasPrefix(dir, "-") {
		dir = "./" + dir
	}
	return shellQuote(dir)
}

// isFile reports whether path is an existing non-directory.
func isFile(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir()
}
