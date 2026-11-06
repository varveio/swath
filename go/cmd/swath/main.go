// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command swath lists a local directory tree or an S3 bucket and writes a globally
// key-sorted Parquet dataset in Swath's sorted dataset layout.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/filter"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/textout"

	// Providers register themselves (scheme and flag group); the command
	// imports them for nothing else.
	_ "github.com/varveio/swath/go/internal/source/gcs"
	_ "github.com/varveio/swath/go/internal/source/localfs"
	_ "github.com/varveio/swath/go/internal/source/s3"
)

// shutdownGrace bounds the time a stopped run (a signal, the deadline, a
// stall) spends stopping.
const shutdownGrace = 15 * time.Second

// exitAfterGrace exits the process with code if it is still running
// shutdownGrace after a stop began: blocked disk I/O or code that does not
// watch its context cannot outlive a signal, --max-duration or the stall
// timeout. main sets it; tests run the command in process and leave it nil.
var exitAfterGrace func(code int, why string)

// silentErrors is -qq: main does not print the command's error.
var silentErrors atomic.Bool

// forensics is the running attempt, for the goroutine dump and the
// outcome written at a forced exit.
var forensics atomic.Pointer[forensicTarget]

type forensicTarget struct {
	ctx    context.Context // the run's: its cause is why it stopped
	dir    string
	pr     *listing.Progress
	report string // --report
	argv   []string
	att    *listing.Attempt
	t      *terminal // the run's terminal result
}

func main() {
	ctx, cancel := context.WithCancelCause(context.Background())
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	// A closed stdout pipe is an error to the writer, not a fatal signal:
	// a text stream whose reader went away ends with exit 0.
	signal.Ignore(syscall.SIGPIPE)
	finished := make(chan struct{})
	defer close(finished)
	exitAfterGrace = func(code int, why string) {
		go func() {
			select {
			case <-finished:
			case <-time.After(shutdownGrace):
				// The backstop is armed first, before any lock, file,
				// snapshot or diagnostic: whatever blocks below (a
				// classification on a disk that does not return, a full
				// stderr pipe nobody drains), the process exits within
				// forcedAllowance, with the code chosen so far.
				var exit atomic.Int64
				exit.Store(int64(code))
				time.AfterFunc(forcedAllowance, func() { os.Exit(int(exit.Load())) })
				f := forensics.Load()
				if f == nil {
					f = &forensicTarget{}
				}
				// One snapshot for the exit code and every file written;
				// one that comes late (a commit that was under way) still
				// sets the code the run's own accounts carry.
				s, ok := forcedSnapshot(f, code, why, func(s snapshot) { exit.Store(int64(s.out.ExitCode)) })
				if ok {
					exit.Store(int64(s.out.ExitCode)) // the run's classification stands
				}
				note(errOut, fmt.Sprintf("swath: still stopping (%s) after %s; exiting (an interrupted journaled run can be resumed)\n", why, shutdownGrace))
				if ok {
					forcedAccount(f, s, why)
				} else {
					note(errOut, "swath: the run's result could not be read in time; no outcome is written\n")
				}
				dumpStacks(f.dir, f.pr, "forced-exit", why, f.pr.Snapshot())
				os.Exit(int(exit.Load()))
			}
		}()
	}
	go func() {
		select {
		case <-finished:
		case sig := <-sigs:
			cancel(&signalError{sig}) // graceful: the run stops and cleans up
			code := exitInterrupted
			if sig == syscall.SIGTERM {
				code = exitTerminated
			}
			exitAfterGrace(code, sig.String())
		}
	}()
	killAt(os.Getenv("SWATH_TEST_KILL_AT"), kill)
	killAt(os.Getenv("SWATH_TEST_HANG_AT"), func() { select {} })
	err := dispatch(ctx, os.Args[1:], os.Stdout)
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return
	}
	if !silentErrors.Load() { // -qq: the exit code and the reports stand alone
		fmt.Fprintln(os.Stderr, "swath:", err)
	}
	var u usageError
	var ee *exitError
	switch {
	case errors.As(err, &ee):
		os.Exit(ee.code)
	case errors.As(err, &u):
		os.Exit(exitUsage)
	}
	os.Exit(exitFatal)
}

// classifyRun is the terminal classification of a run's result: a
// committed dataset that does not cover its scope (a key limit) never
// exits 0 (a supervisor must not take it as complete), and the provider
// names the class of its errors.
func classifyRun(ctx context.Context, dir string, provider func(error) string, sum listing.Report, err error) (error, outcome) {
	if err == nil && dir != "" && !sum.Outcome.Complete {
		err = errIncomplete
	}
	if err != nil && provider != nil {
		if c := provider(err); c != "" {
			err = &classified{err, c}
		}
	}
	return err, classify(err, context.Cause(ctx), dir)
}

// newAttempt identifies this invocation (listing.Attempt).
func newAttempt() *listing.Attempt {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return &listing.Attempt{ID: hex.EncodeToString(b[:]), Started: time.Now().UTC()}
}

// cfgOut is the output directory a run with o would own ("" with
// --count-only).
func cfgOut(o options) string {
	if o.countOnly || o.text != nil {
		return ""
	}
	return o.out
}

// reported holds the attempts whose terminal report is claimed.
var reported sync.Map

// writeReport writes the run report to path (--report), atomically, when
// the attempt ends. That file, at a path the supervisor names outside the
// output, is the attempt's outcome of record; a failure to write it goes to
// stderr and the exit code stands alone.
//
// An attempt has one terminal report: the first of its writers (the
// graceful path or the forced exit) claims it and a later one leaves it.
func writeReport(path string, r listing.Report) {
	if path == "" {
		return
	}
	if r.Attempt != nil {
		if _, taken := reported.LoadOrStore(r.Attempt.ID, true); taken {
			return
		}
	}
	if err := dataset.ReplaceJSON(path, r, dataset.Durable); err != nil {
		// The attempt is named, so a supervisor never takes an older
		// error.json (another attempt's) for this one's outcome.
		id := ""
		if r.Attempt != nil {
			id = r.Attempt.ID
		}
		fmt.Fprintf(errOut, "swath: the report of attempt %s is not written to %s: %v\n", id, path, err)
	}
}

// stopEarly ends an attempt that stopped before a run began (an unknown
// scheme, say): the report (--report) holds its outcome.
func stopEarly(report string, argv []string, dir string, att *listing.Attempt, err error) error {
	o := classify(err, nil, "")
	r := listing.Unstarted(argv, dir, att, err)
	account(&r.Outcome, o)
	writeReport(report, r)
	return &exitError{err, o.ExitCode}
}

// errText refuses swath resume of a text run.
var errText = errors.New("text output (table, tsv, jsonl) is not resumable: it keeps no journal; run swath list again")

// brokenPipe reports whether err is the stdout reader closing the stream.
func brokenPipe(err error) bool { return errors.Is(err, syscall.EPIPE) }

// usageError is a malformed command line.
type usageError struct{ error }

// dispatch runs one command: list, resume, show or diag list.
func dispatch(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		commands(os.Stderr)
		return usageError{errors.New("no command")}
	}
	switch args[0] {
	case "list":
		return run(ctx, args[1:], stdout, false)
	case "diag":
		if len(args) < 2 || args[1] != "list" {
			return usageError{errors.New("usage: swath diag list SOURCE [flags]")}
		}
		return run(ctx, args[2:], stdout, true)
	case "resume":
		return resume(ctx, args[1:], stdout)
	case "show":
		return show(args[1:], stdout)
	case "version", "-version", "--version":
		b := listing.BuildInfo()
		fmt.Fprintf(stdout, "%s %s", b.Tool, b.Version)
		if b.Revision != "" {
			fmt.Fprintf(stdout, " (%s)", b.Revision)
		}
		fmt.Fprintln(stdout)
		return nil
	case "help", "-h", "-help", "--help":
		commands(stdout)
		return nil
	}
	commands(os.Stderr)
	return usageError{fmt.Errorf("unknown command %q", args[0])}
}

// commands prints the command summary.
func commands(w io.Writer) {
	fmt.Fprint(w, `usage:
  swath list SOURCE --output DIR [flags]   list SOURCE into a sorted dataset in DIR
  swath list SOURCE [-o FILE|-] [flags]    list SOURCE as text rows (TSV, JSONL, a table) to a file or stdout
  swath resume DIR [runtime flags]         continue the interrupted run in DIR
  swath show [--journal] DIR               show a dataset or an interrupted run
  swath diag list SOURCE [flags]           list with diagnostic flags (comparisons, policy toggles)
  swath --version                          print the build (tool, module version, VCS revision)
SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]. swath list -h lists the flags.
`)
}

// killAt is a test hook: "point:n" runs act (SWATH_TEST_KILL_AT: the
// process SIGKILLs itself; SWATH_TEST_HANG_AT: the goroutine blocks
// forever, as on stuck I/O) the n-th time the process reaches crash point
// point (package faults).
func killAt(spec string, act func()) {
	point, n, ok := strings.Cut(spec, ":")
	if !ok {
		return
	}
	nth, err := strconv.ParseInt(n, 10, 64)
	if err != nil {
		return
	}
	var seen atomic.Int64
	faults.Set(func(p string) error {
		if p == point && seen.Add(1) == nth {
			act()
		}
		return nil
	})
}

type options struct {
	out                    string
	overwrite              bool
	countOnly              bool
	reportFormat, report   string
	maxDuration, stall     time.Duration
	partInterval           time.Duration
	workers, encoders      int
	partSize, rowBudget    size
	spillMax, spillReserve zsize
	resume, noJournal      bool
	noSpill, spillCompress bool
	sources                map[string]any // each provider's options value, by scheme
	term                   termOptions
	format                 string
	text                   *textout.Config // resolved: a text run's destination
	filter                 filter.Spec
	partMaxRows            int64
}

// runtimeFlags may change on swath resume: they do not change the output
// (resources, reporting, time limits) ...
var runtimeFlags = append([]string{"workers", "encoders", "row-budget", "spill-max", "spill-reserve", "spill-compress", "part-interval", "report-format", "report", "max-duration", "stall-timeout"}, termFlags...)

// ... nor do operationalFlags, how requests are authenticated, timed and
// admitted, which no identity holds: the source and output identity (the
// route, the scope, the schema) is checked on resume and refuses a change,
// such as another region resolved from a new profile.
var operationalFlags = []string{"profile", "anonymous", "request-timeout", "gcs-anonymous", "gcs-credentials", "gcs-request-timeout", "limiter", "limiter-slow-start", "request-rate",
	"bearer-token-command", "bearer-token-refresh-interval"}

// secretFlags hold a secret, or a command that prints one: their value is
// never recorded (journal, reports, the goroutine dump's outcome), and a
// resume must be given it again.
var secretFlags = []string{"bearer-token-command"}

// redacted replaces a secret flag's value in a recorded command line.
const redacted = "[redacted]"

// redactArgv is argv with every secret flag's value replaced by redacted,
// in both spellings (--flag VALUE and --flag=VALUE).
func redactArgv(argv []string) []string {
	out := make([]string, 0, len(argv))
	for i := 0; i < len(argv); i++ {
		a := argv[i]
		name, _, eq := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if a == "--" || !strings.HasPrefix(a, "-") || !slices.Contains(secretFlags, name) {
			out = append(out, a)
			continue
		}
		dashes := a[:len(a)-len(strings.TrimLeft(a, "-"))]
		out = append(out, dashes+name+"="+redacted)
		if !eq && i+1 < len(argv) {
			i++ // the value
		}
	}
	return out
}

// resumeFlags are the flags swath resume accepts.
var resumeFlags = append(slices.Clone(runtimeFlags), operationalFlags...)

func newFlags(o *options, diag bool) *flag.FlagSet {
	f := flag.NewFlagSet("list", flag.ContinueOnError)
	f.StringVar(&o.out, "output", "", "output: a dataset `directory`, a .tsv or .jsonl file (optionally .gz or .zst), or - for stdout (the default without --count-only)")
	f.StringVar(&o.out, "o", "", "short for --output")
	f.StringVar(&o.format, "format", "", "stdout rows: auto, table, tsv or jsonl (auto: the table on a terminal, TSV otherwise); a -o file's name says its format")
	f.BoolVar(&o.overwrite, "overwrite", false, "replace a previous dataset in --output")
	f.BoolVar(&o.overwrite, "force", false, "Java swath's name for --overwrite")
	o.partSize = listing.DefaultPartSize
	f.Var(&o.partSize, "part-size", "target `bytes` per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB)")
	o.rowBudget = listing.DefaultRowBudget
	f.Var(&o.rowBudget, "row-budget", "`bytes` of rows held between listing and encoding (reorder storage and the encoders' queues); not a cap on resident memory")
	f.IntVar(&o.workers, "workers", 0, "listing goroutines (default 64, whatever the CPU count)")
	f.IntVar(&o.encoders, "encoders", 0, "Parquet encoder goroutines (default GOMAXPROCS, lowered to what --row-budget and the descriptor limit carry, never below one); an explicit count is kept, with a warning when they cannot carry it")
	f.DurationVar(&o.partInterval, "part-interval", listing.DefaultPartInterval, "close an open part with rows after this `duration` although not full, bounding the age of the durable progress; 0: parts close by size only")
	f.BoolVar(&o.countOnly, "count-only", false, "walk and count without writing a dataset")
	f.Var(&o.spillMax, "spill-max", "the spill disk budget: cap on spill `bytes` on disk (0: half the free space on the output filesystem at start, after a reserve of one part per encoder and at least 1 GiB); past it, or at the reserve, listing waits instead of spilling")
	f.StringVar(&o.reportFormat, "report-format", "text", "`text` (one line) or json (the run report, as in run.json)")
	f.StringVar(&o.report, "report", "", "also write the run report (JSON, with the outcome and the attempt id) to this `file` when the attempt ends, on failure too")
	f.DurationVar(&o.maxDuration, "max-duration", 0, "stop the run after this `duration` (exit 124; a journaled run is resumable); 0: no limit")
	f.DurationVar(&o.stall, "stall-timeout", 10*time.Minute, "stop the run as stuck after this long without progress (exit 75; resumable); 0: never")
	f.Int64Var(&o.partMaxRows, "part-rotation-max-rows", 0, "also close a part at `N` rows (Java's flag; 0: by size and --part-interval only); part of the resume identity")
	o.term.register(f)
	f.StringVar(&o.filter.Include, "include", "", "keep keys in which this `regex` (Go RE2 syntax) matches anywhere; ^ with a literal prefix also narrows the listed prefix")
	f.StringVar(&o.filter.Exclude, "exclude", "", "drop keys in which this `regex` (Go RE2 syntax) matches anywhere")
	f.StringVar(&o.filter.MinSize, "min-size", "", "keep objects of at least this `size` (1k, 256mb, 1.5GiB: binary units)")
	f.StringVar(&o.filter.MaxSize, "max-size", "", "keep objects of at most this `size`")
	f.StringVar(&o.filter.Since, "modified-since", "", "keep objects modified at or after this `time` (2026-01-01, 2026-01-01T12:00:00, an offset or Z; UTC without one)")
	f.StringVar(&o.filter.Until, "modified-until", "", "keep objects modified at or before this `time`")
	f.Func("storage-class", "keep objects in these storage `classes` (STANDARD,GLACIER; repeatable)", func(v string) error {
		for _, c := range strings.Split(v, ",") {
			if c = strings.TrimSpace(c); c != "" {
				o.filter.StorageClasses = append(o.filter.StorageClasses, c)
			}
		}
		return nil
	})
	if diag {
		f.BoolVar(&o.noJournal, "no-journal", false, "diagnostic: publish parts only at the end, without the journal that swath resume needs")
		f.BoolVar(&o.noSpill, "no-spill", false, "diagnostic: wait on the reorder budget instead of spilling rows ahead of the frontier to disk")
		f.Var(&o.spillReserve, "spill-reserve", "diagnostic: free `bytes` spill leaves on its filesystem (0: derived, one part per encoder and at least 1 GiB)")
		f.BoolVar(&o.spillCompress, "spill-compress", false, "diagnostic: compress spill files (zstd level 1)")
	}
	// Each provider owns its flag group; a group several share (the range
	// engine's) is registered once and passed to each of them.
	o.sources = map[string]any{}
	shared := map[string]any{}
	for _, p := range registry.Factories() {
		var g any
		if p.Shared != "" {
			var ok bool
			if g, ok = shared[p.Shared]; !ok {
				g = p.SharedFlags(f, diag)
				shared[p.Shared] = g
			}
		}
		o.sources[p.Scheme] = p.Flags(f, diag, g)
	}
	f.SetOutput(io.Discard)
	return f
}

// rateFitsStall refuses a --request-rate whose spacing between attempts
// exceeds half the stall timeout: the watchdog sees no answered request
// while an attempt waits for its token, so a healthy listing at that rate
// would stop as stuck (exit 75). Half leaves the other half for the
// request itself.
func rateFitsStall(f *flag.FlagSet, stall time.Duration) error {
	fl := f.Lookup("request-rate")
	if fl == nil || stall <= 0 {
		return nil
	}
	rate, err := strconv.ParseFloat(fl.Value.String(), 64)
	if err != nil || rate <= 0 {
		return nil
	}
	if every := time.Duration(float64(time.Second) / rate); every > stall/2 {
		return fmt.Errorf("--request-rate %g spaces attempts %s apart, more than half of --stall-timeout %s: a healthy run would stop as stuck; raise the rate or the stall timeout",
			rate, every.Round(time.Millisecond), stall)
	}
	return nil
}

// usage prints the flags generated from the flag set itself.
func usage(w io.Writer, f *flag.FlagSet, diag bool) {
	if diag {
		fmt.Fprintln(w, "usage: swath diag list SOURCE [flags]   (the list flags plus diagnostic ones)")
	} else {
		fmt.Fprintln(w, "usage: swath list SOURCE [--output DIR|FILE|-] [flags]")
	}
	fmt.Fprintln(w, "  SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]")
	fmt.Fprintln(w)
	f.SetOutput(w)
	f.PrintDefaults()
	f.SetOutput(io.Discard)
	fmt.Fprintln(w, "\nswath resume DIR [--workers N ...] continues an interrupted run with its recorded command;")
	fmt.Fprintln(w, "only "+strings.Join(runtimeFlags, ", ")+" (runtime) and")
	fmt.Fprintln(w, strings.Join(operationalFlags, ", ")+" (operational) may change.")
	fmt.Fprintln(w, "swath show [--journal] DIR shows a dataset or an interrupted run.")
	fmt.Fprintln(w, "\nProfiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.")
}

func run(ctx context.Context, args []string, stdout io.Writer, diag bool) error {
	return runWith(ctx, args, stdout, diag, nil, false, nil)
}

// parseList resolves a list command line: its options and SOURCE, with
// the usage refusals (a help request prints the usage to stdout).
func parseList(args []string, stdout io.Writer, diag bool) (o options, root string, err error) {
	f := newFlags(&o, diag)
	for rest := args; ; {
		if err := f.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				usage(stdout, f, diag)
				return o, "", err
			}
			return o, "", usageError{err}
		}
		if f.NArg() == 0 {
			break
		}
		if root != "" {
			return o, "", usageError{fmt.Errorf("unexpected argument %q", f.Arg(0))}
		}
		root, rest = f.Arg(0), f.Args()[1:]
	}
	switch {
	case root == "":
		return o, "", usageError{errors.New("missing SOURCE")}
	case o.reportFormat != "text" && o.reportFormat != "json":
		return o, "", usageError{fmt.Errorf("--report-format %q: want text or json", o.reportFormat)}
	}
	if err := rateFitsStall(f, o.stall); err != nil {
		return o, "", usageError{err}
	}
	if err := o.term.validate(); err != nil {
		return o, "", usageError{err}
	}
	if o.partMaxRows < 0 {
		return o, "", usageError{fmt.Errorf("--part-rotation-max-rows %d: negative", o.partMaxRows)}
	}
	if err := o.filter.Validate(); err != nil {
		return o, "", usageError{err}
	}
	text, err := resolveOutput(&o, stdoutTerminal())
	if err != nil {
		return o, "", usageError{err}
	}
	if o.text = text; text != nil && text.Path == "" {
		text.Stdout = stdout
		if o.reportFormat == "json" {
			return o, "", usageError{errors.New("--report-format json prints on stdout, which carries the rows: use --report FILE")}
		}
	}
	return o, root, nil
}

// runWith lists; argv0 is the recorded command, resumed tells a resume, att
// is the attempt (nil: a new one).
func runWith(ctx context.Context, args []string, stdout io.Writer, diag bool, recorded []string, resumed bool, att *listing.Attempt) error {
	if att == nil {
		att = newAttempt()
	}
	o, root, err := parseList(args, stdout, diag)
	if err != nil {
		return err
	}
	if o.text != nil && resumed {
		return usageError{errText}
	}
	toStdout := o.text != nil && o.text.Path == ""
	silentErrors.Store(o.term.quiet >= 2)
	argv := recorded
	if argv == nil {
		argv = append([]string{"list"}, args...)
		if diag {
			argv = append([]string{"diag", "list"}, args...)
		}
		argv = redactArgv(argv)
	}
	pf, err := registry.Lookup(root)
	if err != nil {
		return stopEarly(o.report, argv, cfgOut(o), att, usageError{err})
	}
	root = narrowRoot(root, &o.filter)
	cfg := listing.Config{
		Source: listing.SourceConfig{URI: root, Options: o.sources[pf.Scheme]},
		Output: listing.OutputConfig{Replace: o.overwrite, Resume: resumed, NoJournal: o.noJournal, Text: o.text},
		Limits: listing.Limits{Workers: o.workers, Encoders: o.encoders, RowBudget: int64(o.rowBudget), PartSize: int64(o.partSize), PartInterval: o.partInterval, PartMaxRows: o.partMaxRows,
			Spill: listing.SpillLimits{Off: o.noSpill, Compress: o.spillCompress, Max: int64(o.spillMax), Reserve: int64(o.spillReserve)}},
		Argv:    argv,
		Attempt: att,
		Filter:  &o.filter,
		Warn:    func(m string) { note(errOut, "swath: warning: "+m+"\n") },
	}
	if o.term.quiet > 0 {
		cfg.Warn = func(string) {}
	}
	if !o.countOnly && o.text == nil {
		cfg.Output.Dir = o.out
	}
	stopProfile, err := profile()
	if err != nil {
		return stopEarly(o.report, argv, cfgOut(o), att, err)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if o.maxDuration > 0 {
		var stopTimer context.CancelFunc
		ctx, stopTimer = context.WithTimeoutCause(ctx, o.maxDuration, errDeadline)
		defer stopTimer()
	}
	if exitAfterGrace != nil {
		// The deadline and the stall timeout stop the run as a signal
		// does, and as surely: after the grace the process exits.
		stopGrace := context.AfterFunc(ctx, func() {
			switch c := context.Cause(ctx); {
			case errors.Is(c, errDeadline):
				exitAfterGrace(exitDeadline, "--max-duration")
			case errors.Is(c, errStuck):
				exitAfterGrace(exitStuck, "--stall-timeout")
			}
		})
		defer stopGrace()
	}
	cfg.Progress = &listing.Progress{}
	term := &terminal{ctx: ctx, dir: cfg.Output.Dir, provider: pf.Classify, att: att}
	forensics.Store(&forensicTarget{ctx: ctx, dir: cfg.Output.Dir, pr: cfg.Progress, report: o.report, argv: argv, att: att, t: term})
	// One terminal result (terminal): the classification is applied to the
	// report before run.json, error.json (Settle) or --report is written,
	// and every one of them derives from it. error.json is written while
	// the run still owns the output (Settle), never by a run that did not.
	cfg.Finalize = func(r *listing.Report, err error) {
		if toStdout && brokenPipe(err) {
			// The reader closed the stream: exit 0, as Java's, and the
			// report says the listing did not reach its end.
			err = nil
			r.Outcome.Status, r.Outcome.Complete, r.Outcome.StopReason, r.Outcome.Error = listing.StatusStopped, false, listing.StopDownstreamClosed, ""
		}
		term.account(r, err)
	}
	cfg.Commit = term.commit // the commit and its account, one step against a forced exit
	settled, unrecorded := false, ""
	cfg.Settle = func(sum listing.Report, err error) {
		term.account(&sum, err) // a no-op for the accounted report Settle gets
		if werr := writeOutcome(cfg.Output.Dir, outcomeOf(sum)); werr != nil {
			unrecorded = "write " + errorName + ": " + werr.Error()
		} else {
			settled = true
		}
	}
	tt := startTerminal(o.term, att, root, cfg.Output.Dir)
	defer tt.close()
	stopWatch := supervise(ctx, cancel, cfg.Progress, cfg.Output.Dir, o.stall, cfg.Warn, tt.disp)
	sum, err := listing.Run(ctx, cfg)
	_ = faults.At("run-returned") // tests: a stop cause arriving while the run settles
	stopWatch()
	tt.disp.stop()
	stopProfile()
	err, out := term.classify(sum, err)
	return finishList(stdout, o, cfg.Output.Dir, sum, err, out, settled, unrecorded, tt)
}

// finishList is a list command's terminal settlement after its run: the
// accounts derived from the frozen outcome out (the report, -json, the
// outcome on stderr when error.json does not hold it) and the error main
// exits with. settled says error.json holds it; unrecorded why not.
func finishList(stdout io.Writer, o options, dir string, sum listing.Report, err error, out outcome, settled bool, unrecorded string, tt *termUI) error {
	if err != nil && out.ExitCode == exitCompleted {
		// A resume that found the run committed by another process
		// meanwhile: nothing to do, as swath resume on a committed dataset.
		account(&sum.Outcome, out)
		alreadyComplete(stdout, o.reportFormat, dir, &sum)
		writeReport(o.report, sum)
		if o.reportFormat == "json" {
			enc := json.NewEncoder(stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(sum)
		}
		return nil
	}
	if dir != "" && !settled && (err != nil || unrecorded != "") {
		// Refused before ownership (another owner's outcome stands, the
		// run changed meanwhile, or there is none to settle), or the write
		// failed: the outcome goes to stderr instead, with the reason.
		if unrecorded == "" {
			unrecorded = sum.Outcome.Unrecorded
		}
		if unrecorded == "" {
			unrecorded = "stopped before owning the output"
		}
		reportUnrecorded(dir, out, unrecorded)
	}
	account(&sum.Outcome, out)
	if errors.Is(err, listing.ErrComplete) {
		committedAccount(dir, &sum)
	}
	sum.Outcome.Unrecorded = unrecorded
	writeReport(o.report, sum)
	tt.stats(&sum, sum.Outcome.StopReason == listing.StopDownstreamClosed)
	if o.reportFormat == "json" {
		// The report goes out on failure too: its outcome names the error.
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if jerr := enc.Encode(sum); err == nil {
			return jerr
		}
	}
	if err != nil {
		return &exitError{explain(err), out.ExitCode}
	}
	if o.text != nil && o.text.Path == "" {
		return nil // stdout carries the rows; --stats prints the account on stderr
	}
	if r := sum.SourceReport(); r != nil && r.Brief != "" {
		fmt.Fprint(stdout, r.Brief, " ")
	}
	parts, outBytes := sum.Output.PartsWritten+sum.Output.PartsRetained, sum.Output.Bytes
	if t := sum.Output.Text; t != nil {
		parts, outBytes = 1, t.Bytes // one text file
	}
	fmt.Fprintf(stdout, "rows=%d parts=%d out_bytes=%d wall=%.3fs user=%.2fs sys=%.2fs peak_rss=%.1fMiB max_buffered=%d\n",
		sum.Output.RowsListed, parts, outBytes, float64(sum.Timings.WallMs)/1000,
		float64(sum.Timings.UserCPUMs)/1000, float64(sum.Timings.SysCPUMs)/1000, float64(sum.Resources.PeakRSSBytes)/(1<<20), sum.Resources.MaxBufferedBytes)
	return nil
}

// explain adds command-line advice to the errors a user can act on; the
// packages' own messages name no flag.
func explain(err error) error {
	var mm *journal.MismatchError
	var ve *journal.VersionError
	switch {
	case errors.Is(err, listing.ErrAmbiguousTail):
		return fmt.Errorf("%w (the part is kept in DIR/.swath/suspect; swath list --overwrite starts over, or removing that directory accepts the source as it is now)", err)
	case errors.Is(err, listing.ErrInterrupted):
		return fmt.Errorf("%w (swath resume DIR continues it; --overwrite starts over)", err)
	case errors.Is(err, dataset.ErrNotEmpty):
		return fmt.Errorf("%w (use --overwrite to replace a previous dataset)", err)
	case errors.Is(err, dataset.ErrOutIsRoot), errors.Is(err, dataset.ErrInsideRoot), errors.Is(err, dataset.ErrNotDataset):
		return fmt.Errorf("--output: %w", err)
	case errors.As(err, &mm):
		return fmt.Errorf("%w (start over with swath list --overwrite, or resume with the original settings)", err)
	case errors.As(err, &ve):
		return fmt.Errorf("%w (finish it with the build that started it, or start over with --overwrite)", err)
	}
	return err
}

// profile starts the CPU profile and arranges the allocation profile named
// by the environment, for development.
func profile() (stop func(), err error) {
	stops := []func(){}
	if path := os.Getenv("SWATH_CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return nil, err
		}
		stops = append(stops, func() { pprof.StopCPUProfile(); f.Close() })
	}
	if path := os.Getenv("SWATH_MEMPROFILE"); path != "" {
		runtime.MemProfileRate = 64 << 10
		stops = append(stops, func() {
			if f, err := os.Create(path); err == nil {
				_ = pprof.Lookup("allocs").WriteTo(f, 0)
				f.Close()
			}
		})
	}
	return func() {
		for _, s := range stops {
			s()
		}
	}, nil
}

// size is a byte count flag accepting binary and decimal suffixes.
type size int64

// zsize is a size that may also be 0 (off).
type zsize size

func (s *zsize) String() string { return (*size)(s).String() }

func (s *zsize) Set(v string) error {
	if strings.TrimSpace(v) == "0" {
		*s = 0
		return nil
	}
	return (*size)(s).Set(v)
}

func (s *size) String() string { return strconv.FormatInt(int64(*s), 10) }

func (s *size) Set(v string) error {
	units := []struct {
		suffix string
		mult   int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"B", 1}}
	mult := int64(1)
	for _, u := range units {
		if n, ok := strings.CutSuffix(v, u.suffix); ok {
			v, mult = n, u.mult
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n <= 0 {
		return fmt.Errorf("invalid size %q", v)
	}
	if n > math.MaxInt64/mult {
		return fmt.Errorf("invalid size %q: too large", v)
	}
	*s = size(n * mult)
	return nil
}

// narrowRoot is an object-store root narrowed to the prefix an anchored
// --include implies (filter.Spec.NarrowPrefix): the same rows, fewer
// requests. A resume replays the same command, so it narrows the same way.
func narrowRoot(root string, f *filter.Spec) string {
	scheme, rest, ok := strings.Cut(root, "://")
	if !ok || (scheme != "s3" && scheme != "gs") {
		return root
	}
	bucket, prefix, _ := strings.Cut(rest, "/")
	if n := f.NarrowPrefix(prefix); n != prefix {
		return scheme + "://" + bucket + "/" + n
	}
	return root
}
