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
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/textout"
	"github.com/varveio/swath/go/internal/totals"

	// Providers register themselves (scheme and flag group); the command
	// imports them for nothing else.
	_ "github.com/varveio/swath/go/internal/source/azblob"
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
	testHooks()
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

// cfgOut is the output directory a run with o would own ("" for swath
// count).
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

// usageError is a malformed command line.
type usageError struct{ error }

// dispatch runs one command: list, count, resume, show, diag list or diag
// count.
func dispatch(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		commands(os.Stderr)
		return usageError{errors.New("no command")}
	}
	switch args[0] {
	case "list":
		return run(ctx, args[1:], stdout, false)
	case "count":
		return runCount(ctx, args[1:], stdout, false)
	case "diag":
		switch {
		case len(args) == 2 && isHelp(args[1]):
			fmt.Fprint(stdout, diagUsage)
			return flag.ErrHelp
		case len(args) >= 2 && args[1] == "list":
			return run(ctx, args[2:], stdout, true)
		case len(args) >= 2 && args[1] == "count":
			return runCount(ctx, args[2:], stdout, true)
		}
		return usageError{errors.New(strings.TrimSpace(diagUsage))}
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

// isHelp reports a help request flag.
func isHelp(a string) bool { return a == "-h" || a == "-help" || a == "--help" }

// diagUsage is swath diag's own help.
const diagUsage = `usage: swath diag list SOURCE [flags]    swath list with the diagnostic flags
       swath diag count SOURCE [flags]   swath count with the diagnostic flags
The diagnostic flags exist to run an experiment or a comparison, not to operate the tool.
swath diag list --help lists them.
`

// commands prints the command summary.
func commands(w io.Writer) {
	fmt.Fprint(w, `usage:
  swath list SOURCE -o DIR [flags]        list SOURCE into a sorted Parquet dataset in DIR
  swath list SOURCE [-o FILE|-] [flags]   list SOURCE as text rows to a .tsv or .jsonl file (.gz, .zst) or stdout
  swath count SOURCE [flags]              count SOURCE's objects and bytes, writing nothing
  swath count SOURCE --depth N [flags]    total them per key prefix down to N levels, as du
  swath resume DIR [flags]                continue the interrupted run in DIR
  swath show [--journal] DIR              show a dataset or an interrupted run
  swath diag list|count SOURCE [flags]    list or count with the diagnostic flags (experiments, comparisons)
  swath --version                         print the build (tool, module version, VCS revision)
SOURCE is `+sourceForms()+`.
swath COMMAND --help lists a command's flags.
`)
}

// testHooks installs the test hooks the environment asks for: on the
// crash points (package faults), in this order, SWATH_TEST_SERIAL_SEALS
// (serialSeals), then SWATH_TEST_KILL_AT (the process SIGKILLs itself) and
// SWATH_TEST_HANG_AT (the goroutine blocks forever, as on stuck I/O), each
// "point:n": the n-th time the process reaches crash point point; and on
// the order stage's events (order.SetDurableEvents), serialSeals' and
// SWATH_TEST_LAST_RANGE_FILE (lastRangeFile).
func testHooks() {
	var hooks, events []func(string)
	if os.Getenv("SWATH_TEST_SERIAL_SEALS") != "" {
		h, e := serialSeals()
		hooks, events = append(hooks, h), append(events, e)
	}
	if path := os.Getenv("SWATH_TEST_LAST_RANGE_FILE"); path != "" {
		events = append(events, lastRangeFile(path))
	}
	if len(events) > 0 {
		order.SetDurableEvents(func(e string) {
			for _, f := range events {
				f(e)
			}
		})
	}
	killAt := os.Getenv("SWATH_TEST_KILL_AT")
	killed := func() {
		fmt.Fprintln(os.Stderr, killMarker(os.Getenv("SWATH_TEST_KILL_ID"), killAt)) // the harness tells this kill from its own
		kill()
	}
	for _, h := range []func(string){atNth(killAt, killed), atNth(os.Getenv("SWATH_TEST_HANG_AT"), func() { select {} })} {
		if h != nil {
			hooks = append(hooks, h)
		}
	}
	if len(hooks) == 0 {
		return
	}
	faults.Set(func(p string) error {
		for _, h := range hooks {
			h(p)
		}
		return nil
	})
}

// killMarker is the line SWATH_TEST_KILL_AT=spec writes to stderr, a line
// of its own, just before the process kills itself: id is the harness's
// attempt (SWATH_TEST_KILL_ID), so the line names this attempt and point.
func killMarker(id, spec string) string { return "swath-test-kill " + id + " " + spec }

// atNth is a hook running act the n-th time the process reaches point,
// for spec "point:n" (nil for another spec).
func atNth(spec string, act func()) func(string) {
	point, n, ok := strings.Cut(spec, ":")
	if !ok {
		return nil
	}
	nth, err := strconv.ParseInt(n, 10, 64)
	if err != nil {
		return nil
	}
	var seen atomic.Int64
	return func(p string) {
		if p == point && seen.Add(1) == nth {
			act()
		}
	}
}

// serialSeals is SWATH_TEST_SERIAL_SEALS, the real-process ledger barrier
// (cmd/swath's SIGKILL matrix): each durable seal waits at its crash point
// until the flusher has taken every earlier seal's record in a cycle of
// its own (order's "cycle-records" events), so seal n's record is the
// n-th ledger append and the matrix's fourth-cycle arms are reached by
// construction, not by how the seals fell in the flusher's 100 ms
// windows. A wait over 10 s gives up, and the arm it served then fails as
// unreached (the same gate as listing's in-process sealGate). It returns
// the crash-point hook and the order-event hook it counts cycles with.
func serialSeals() (hook, events func(string)) {
	var cycles atomic.Int64
	events = func(e string) {
		if e == "cycle-records" {
			cycles.Add(1)
		}
	}
	var mu sync.Mutex
	var seals int64 // seals past their crash point (mu)
	hook = func(p string) {
		if p != "spill-seal" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for deadline := time.Now().Add(10 * time.Second); cycles.Load() < seals && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
		seals++
	}
	return hook, events
}

// lastRangeFile is SWATH_TEST_LAST_RANGE_FILE: the file is created once a
// range is done with one left (order's "one-range-open") and a durable
// file has been sealed ("sealed"), in whichever order they come. A test
// that holds one range's listing from outside the process waits for the
// file, a signal from the run's own state, instead of for the fake's
// requests to go quiet (cmd/swath's TestSegmentsReplayDurableFiles).
func lastRangeFile(path string) func(string) {
	var alone, sealed, made atomic.Bool
	return func(e string) {
		switch e {
		case "one-range-open":
			alone.Store(true)
		case "sealed":
			sealed.Store(true)
		default:
			return
		}
		if alone.Load() && sealed.Load() && made.CompareAndSwap(false, true) {
			if f, err := os.Create(path); err == nil {
				f.Close()
			}
		}
	}
}

type options struct {
	diag                   bool // swath diag list or count: the diagnostic flags too
	out                    string
	overwrite              bool
	countOnly              bool // swath count
	reportFormat, report   string
	cpuProfile, memProfile string // SWATH_CPUPROFILE, SWATH_MEMPROFILE, resolved (checkSidePaths)
	maxDuration, stall     time.Duration
	partInterval           time.Duration
	workers, encoders      int
	partSize, rowBudget    size
	spillBudget            zsize
	spillReserve           zsize
	resume, noJournal      bool
	noSpill, spillCompress bool
	spillPlain             bool
	sources                map[string]any      // each provider's options value, by scheme
	owners                 map[string][]string // a provider flag's name: the schemes it applies to
	term                   termOptions
	format                 string
	text                   *textout.Config // resolved: a text run's destination
	filter                 filter.Spec
	partMaxRows            int64
	seedFrom               string
	// swath count's per-prefix totals: --depth (depthGiven: the flag was
	// given; totals: the totals' output, resolved) and --max-prefixes.
	depth, maxPrefixes int
	depthGiven         bool
	totals             *totalsOut
}

// totalsOut is where swath count --depth writes its prefix rows: stdout,
// in format (textout's TSV, JSONL or Table).
type totalsOut struct {
	format string
	tally  *totals.Tally
}

// runtimeFlags may change on swath resume: they do not change the output
// (resources, reporting, time limits) ...
var runtimeFlags = append([]string{"workers", "encoders", "row-budget", "spill-budget", "spill-reserve", "spill-compress", "spill-plain", "part-interval", "report-format", "report", "max-duration", "stall-timeout", "seed-from"}, termFlags...)

// ... nor do operationalFlags, how requests are authenticated, timed and
// admitted, which no identity holds: the source and output identity (the
// route, the scope, the schema) is checked on resume and refuses a change,
// such as another region resolved from a new profile.
var operationalFlags = []string{"profile", "anonymous", "request-timeout", "gcs-anonymous", "gcs-credentials", "gcs-request-timeout", "limiter", "limiter-slow-start", "request-rate",
	"bearer-token-command", "bearer-token-refresh-interval", "azblob-anonymous", "azblob-sas", "azblob-request-timeout"}

// secretFlags hold a secret, or a command that prints one: their value is
// never recorded (journal, reports, the goroutine dump's outcome), and a
// resume must be given it again.
// They and secretEnv are the registered providers' (registry.Factory.Secrets).
var secretFlags, secretEnv = providerSecrets()

// providerSecrets collects the registered providers' secret flags and the
// environment variable that may stand in for each on resume.
func providerSecrets() ([]string, map[string]string) {
	var flags []string
	env := map[string]string{}
	for _, f := range registry.Factories() {
		for _, s := range f.Secrets {
			flags = append(flags, s.Flag)
			if s.Env != "" {
				env[s.Flag] = s.Env
			}
		}
	}
	return flags, env
}

// sourceForms is the SOURCE line of the help: the registered providers'
// root forms (registry.Factory.Usage), by scheme, the local directory
// first.
func sourceForms() string {
	var forms []string
	for _, f := range registry.Factories() {
		if f.Usage != "" {
			forms = append(forms, f.Usage)
		}
	}
	if n := len(forms); n > 1 {
		return strings.Join(forms[:n-1], ", ") + " or " + forms[n-1]
	}
	return strings.Join(forms, "")
}

// redacted replaces a secret flag's value in a recorded command line.
const redacted = "[redacted]"

// redactArgv is argv with every secret flag's value replaced by redacted,
// in both spellings (--flag VALUE and --flag=VALUE), and a credential in a
// root by its provider's redaction (registry.Redact; a resume must be given
// it again).
func redactArgv(argv []string) []string {
	out := make([]string, 0, len(argv))
	for i := 0; i < len(argv); i++ {
		a := argv[i]
		name, _, eq := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if a == "--" || !strings.HasPrefix(a, "-") || !slices.Contains(secretFlags, name) {
			out = append(out, registry.Redact(a))
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

// newFlags is the flag set of swath list (diag false) or swath diag list
// (diag true). Both hold every flag, the diagnostic ones included, so that
// one given to swath list is refused with a message naming swath diag
// (placement) rather than as unknown; the help shows only the command's own
// (helpGroups).
func newFlags(o *options, diag bool) *flag.FlagSet {
	f := flag.NewFlagSet("list", flag.ContinueOnError)
	o.diag = diag
	registerFlags(f, o, true)
	return f
}

// registerFlags registers the list flags on f, with the diagnostic ones
// when diag.
func registerFlags(f *flag.FlagSet, o *options, diag bool) {
	f.StringVar(&o.out, "o", "", "output: a dataset `directory`, a .tsv or .jsonl file (optionally .gz or .zst), or - for stdout (the default)")
	f.StringVar(&o.format, "format", "", "stdout rows (swath count: --depth's prefix rows): auto, table, tsv or jsonl (auto: the table on a terminal, TSV otherwise); a -o file's name says its format")
	f.BoolVar(&o.overwrite, "overwrite", false, "replace a previous dataset in -o")
	o.partSize = listing.DefaultPartSize
	f.Var(&o.partSize, "part-size", "target `bytes` per Parquet part (64MiB, 1.5g: see Sizes in docs/cli.md)")
	o.rowBudget = listing.DefaultRowBudget
	f.Var(&o.rowBudget, "row-budget", "`bytes` of rows held between listing and encoding (reorder storage and the encoders' queues); not a cap on resident memory")
	f.StringVar(&o.seedFrom, "seed-from", "", "start from a previous listing's key distribution instead of the delimiter seed: a committed dataset `path` of the same source and scope, or a cut file (one key per line, ascending bytewise, optionally TAB and the keys expected up to it); the engine still splits adaptively")
	f.IntVar(&o.workers, "workers", 0, "listing goroutines, the most requests in flight (default 64, whatever the CPU count)")
	f.IntVar(&o.encoders, "encoders", 0, "Parquet encoder goroutines, or a .zst text file's compression jobs (default GOMAXPROCS, lowered to what --row-budget and the descriptor limit carry, never below one); an explicit count is kept, with a warning when they cannot carry it")
	f.DurationVar(&o.partInterval, "part-interval", listing.DefaultPartInterval, "close an open part with rows after this `duration` although not full (a rotation target: the part is then encoded and published in order, so the recovery point can be older, and a stalled frontier leaves no part to close); 0: parts close by size only")
	f.Int64Var(&o.partMaxRows, "part-rotation-max-rows", 0, "also close a part at `N` rows (Java's flag; 0: by size and --part-interval only); part of the resume identity")
	f.Var(&o.spillBudget, "spill-budget", "cap on spill `bytes` on disk (0: half the free space on the output filesystem at start, after a reserve of one part per encoder and at least 1 GiB); past it, or at the reserve, listing waits instead of spilling")
	f.StringVar(&o.reportFormat, "report-format", "text", "`text` (one line) or json (the run report, as in run.json)")
	f.StringVar(&o.report, "report", "", "also write the run report (JSON, with the outcome and the attempt id) to this `file` when the attempt ends, on failure too")
	f.DurationVar(&o.maxDuration, "max-duration", 0, "stop the run after this `duration` (exit 124; a journaled run is resumable); 0: no limit")
	f.DurationVar(&o.stall, "stall-timeout", 10*time.Minute, "stop the run as stuck after this long without progress (exit 75; resumable); 0: never")
	o.term.register(f)
	f.StringVar(&o.filter.Include, "include", "", "keep keys in which this `regex` (Go RE2 syntax) matches anywhere; ^ with a literal prefix also narrows the listed prefix")
	f.StringVar(&o.filter.Exclude, "exclude", "", "drop keys in which this `regex` (Go RE2 syntax) matches anywhere")
	f.StringVar(&o.filter.MinSize, "min-size", "", "keep objects of at least this `size` (1k, 256mb, 1.5GiB: binary units)")
	f.StringVar(&o.filter.MaxSize, "max-size", "", "keep objects of at most this `size`")
	f.StringVar(&o.filter.Since, "modified-since", "", "keep objects modified at or after this `time` (2026-01-01, 2026-01-01T12:00:00, an offset or Z; UTC without one)")
	f.StringVar(&o.filter.Until, "modified-until", "", "keep objects modified at or before this `time`")
	f.IntVar(&o.depth, "depth", 0, "swath count: also total each key prefix down to `N` levels (a prefix ends at a '/'; 0: the total alone), each including what is below it, as du: rows of prefix, objects, bytes and the newest time, on stdout in --format")
	f.IntVar(&o.maxPrefixes, "max-prefixes", totals.DefaultMaxPrefixes, "swath count --depth: fail rather than hold more than `N` distinct prefixes (memory grows with them, never with the objects)")
	f.Func("storage-class", "keep objects in these storage `classes` (STANDARD,GLACIER; repeatable)", func(v string) error {
		for _, c := range strings.Split(v, ",") {
			if c = strings.TrimSpace(c); c != "" {
				o.filter.StorageClasses = append(o.filter.StorageClasses, c)
			}
		}
		return nil
	})
	o.spillCompress = true
	if diag {
		// The spill frame format is a runtime choice (and on resume: spill
		// is disposable, so a resume's frames never meet the crashed run's).
		f.BoolVar(&o.spillCompress, "spill-compress", true, "compress spill frames (zstd level 1); --spill-compress=false writes them uncompressed")
		f.BoolVar(&o.spillPlain, "spill-plain", false, "write spill frames without delta coding (each key and payload whole; a comparison arm)")
		f.BoolVar(&o.noJournal, "no-journal", false, "publish parts only at the end, without the journal that swath resume needs")
		f.BoolVar(&o.noSpill, "no-spill", false, "wait on the reorder budget instead of spilling rows ahead of the frontier to disk")
		f.Var(&o.spillReserve, "spill-reserve", "free `bytes` spill leaves on its filesystem (0: derived, one part per encoder and at least 1 GiB)")
	}
	// Each provider owns its flag group; a group several share (the range
	// engine's) is registered once and passed to each of them.
	// owners records which providers each provider flag applies to (the
	// flags a registration adds), so a flag given for another provider is
	// refused rather than ignored (foreignFlags).
	o.sources, o.owners = map[string]any{}, map[string][]string{}
	shared, groupFlags := map[string]any{}, map[string][]string{}
	added := func(register func()) []string {
		before := map[string]bool{}
		f.VisitAll(func(fl *flag.Flag) { before[fl.Name] = true })
		register()
		var names []string
		f.VisitAll(func(fl *flag.Flag) {
			if !before[fl.Name] {
				names = append(names, fl.Name)
			}
		})
		return names
	}
	for _, p := range registry.Factories() {
		var g any
		if p.Shared != "" {
			var ok bool
			if g, ok = shared[p.Shared]; !ok {
				groupFlags[p.Shared] = added(func() { g = p.SharedFlags(f, diag) })
				shared[p.Shared] = g
			}
			for _, n := range groupFlags[p.Shared] {
				o.owners[n] = append(o.owners[n], p.Scheme)
			}
		}
		for _, n := range added(func() { o.sources[p.Scheme] = p.Flags(f, diag, g) }) {
			o.owners[n] = append(o.owners[n], p.Scheme)
		}
	}
	f.SetOutput(io.Discard)
}

// nonNegativeDurations refuses a negative value of any duration flag given
// (the command's and every provider's): a negative deadline or timeout
// would otherwise read as "off". Zero keeps its documented meaning.
func nonNegativeDurations(f *flag.FlagSet) (err error) {
	f.Visit(func(fl *flag.Flag) {
		g, ok := fl.Value.(flag.Getter)
		if !ok {
			return
		}
		if d, ok := g.Get().(time.Duration); ok && d < 0 && err == nil {
			err = fmt.Errorf("--%s %s: negative", fl.Name, d)
		}
	})
	return err
}

// foreignFlags refuses a provider flag given explicitly for a root of
// another provider (gs://... --anonymous: S3's flag, which GCS would
// ignore; GCS's is --gcs-anonymous). A root no provider takes is left to
// the lookup's own refusal.
func foreignFlags(f *flag.FlagSet, owners map[string][]string, root string) (err error) {
	pf, lerr := registry.Lookup(root)
	if lerr != nil {
		return nil
	}
	f.Visit(func(fl *flag.Flag) {
		own, ok := owners[fl.Name]
		if !ok || slices.Contains(own, pf.Scheme) || err != nil {
			return
		}
		names := make([]string, len(own))
		for i, s := range own {
			names[i] = schemeName(s)
		}
		err = fmt.Errorf("--%s applies to %s sources, not to %s", fl.Name, strings.Join(names, " and "), schemeName(pf.Scheme))
	})
	return err
}

// schemeName names a provider by its root form.
func schemeName(scheme string) string {
	if scheme == "" {
		return "local"
	}
	return scheme + "://"
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

// run is swath list (diag: swath diag list).
func run(ctx context.Context, args []string, stdout io.Writer, diag bool) error {
	return runWith(ctx, command{diag: diag}, args, stdout, nil, false, nil)
}

// runCount is swath count (diag: swath diag count).
func runCount(ctx context.Context, args []string, stdout io.Writer, diag bool) error {
	return runWith(ctx, command{diag: diag, count: true}, args, stdout, nil, false, nil)
}

// command is one of the list commands: swath list, swath count, and their
// swath diag forms.
type command struct{ diag, count bool }

// name is the command as typed.
func (c command) name() string {
	n := "list"
	if c.count {
		n = "count"
	}
	if c.diag {
		n = "diag " + n
	}
	return n
}

// parseList resolves a list command line: its options and SOURCE, with
// the usage refusals (a help request prints the usage to stdout).
func parseList(args []string, stdout io.Writer, cmd command) (o options, root string, err error) {
	f := newFlags(&o, cmd.diag)
	o.countOnly = cmd.count
	for rest := args; ; {
		if err := f.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				usage(stdout, f, cmd)
				return o, "", err
			}
			return o, "", usageError{err}
		}
		if f.NArg() == 0 {
			break
		}
		if root != "" {
			return o, "", usageError{fmt.Errorf("unexpected argument %q", registry.Redact(f.Arg(0)))}
		}
		root, rest = f.Arg(0), f.Args()[1:]
	}
	switch {
	case root == "":
		return o, "", usageError{errors.New("missing SOURCE")}
	case o.reportFormat != "text" && o.reportFormat != "json":
		return o, "", usageError{fmt.Errorf("--report-format %q: want text or json", o.reportFormat)}
	}
	if err := placement(f, cmd); err != nil {
		return o, "", usageError{err}
	}
	if err := nonNegativeDurations(f); err != nil {
		return o, "", usageError{err}
	}
	if err := foreignFlags(f, o.owners, root); err != nil {
		return o, "", usageError{err}
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
	o.text = text
	if err := resolveTotals(f, &o, root, stdoutTerminal()); err != nil {
		return o, "", usageError{err}
	}
	if pf, err := registry.Lookup(root); err == nil && pf.BinaryKeys && text != nil {
		return o, "", usageError{fmt.Errorf("a %s source needs -o DIR (a dataset), or swath count: its keys are bytes, which text output cannot carry", schemeName(pf.Scheme))}
	}
	if err := checkSidePaths(&o); err != nil {
		return o, "", usageError{err}
	}
	if (text != nil && text.Path == "" || o.totals != nil) && o.reportFormat == "json" {
		return o, "", usageError{errors.New("--report-format json prints on stdout, which carries the rows: use --report FILE")}
	}
	if text != nil && text.Path == "" {
		text.Stdout = stdout
	}
	return o, root, nil
}

// runWith runs a list command; recorded is the recorded command (a
// resume's), resumed tells a resume, att is the attempt (nil: a new one).
func runWith(ctx context.Context, cmd command, args []string, stdout io.Writer, recorded []string, resumed bool, att *listing.Attempt) error {
	if att == nil {
		att = newAttempt()
	}
	o, root, err := parseList(args, stdout, cmd)
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
		argv = redactArgv(append(strings.Fields(cmd.name()), args...))
	}
	pf, err := registry.Lookup(root)
	if err != nil {
		return stopEarly(o.report, argv, cfgOut(o), att, usageError{err})
	}
	root = narrowRoot(root, &o.filter)
	cfg := listing.Config{
		Source: listing.SourceConfig{URI: root, Options: o.sources[pf.Scheme], SeedFrom: o.seedFrom},
		Output: listing.OutputConfig{Replace: o.overwrite, Resume: resumed, NoJournal: o.noJournal, Text: o.text},
		Limits: listing.Limits{Workers: o.workers, Encoders: o.encoders, RowBudget: int64(o.rowBudget), PartSize: int64(o.partSize), PartInterval: o.partInterval, PartMaxRows: o.partMaxRows,
			Spill: listing.SpillLimits{Off: o.noSpill, Plain: o.spillPlain, Uncompressed: !o.spillCompress, Max: int64(o.spillBudget), Reserve: int64(o.spillReserve)}},
		Argv:    argv,
		Attempt: att,
		Filter:  &o.filter,
		Warn:    func(m string) { note(errOut, "swath: warning: "+m+"\n") },
	}
	if o.totals != nil {
		cfg.Totals = o.totals.tally
	}
	if o.term.quiet > 0 {
		cfg.Warn = func(string) {}
	}
	if !o.countOnly && o.text == nil {
		cfg.Output.Dir = o.out
	}
	stopProfile, err := profile(o.cpuProfile, o.memProfile)
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
	tt := startTerminal(o.term, att, registry.Redact(root), cfg.Output.Dir)
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
	if t := o.totals; t != nil {
		// The prefix rows, once the count is complete (a run that failed
		// or stopped printed nothing above); stdout carries them, as a
		// text run's rows, so the summary line is not printed.
		err := textout.WriteTotals(stdout, t.format, false, t.tally.Result())
		if err != nil && !brokenPipe(err) {
			return err
		}
		return nil
	}
	if r := sum.SourceReport(); r != nil && r.Brief != "" {
		fmt.Fprint(stdout, r.Brief, " ")
	}
	cpu := fmt.Sprintf("wall=%.3fs user=%s sys=%s peak_rss=%s", float64(sum.Timings.WallMs)/1000,
		measured(sum.Timings.UserCPUMs, 1000, "%.2fs"), measured(sum.Timings.SysCPUMs, 1000, "%.2fs"),
		measured(sum.Resources.PeakRSSBytes, 1<<20, "%.1fMiB"))
	if o.countOnly {
		fmt.Fprintf(stdout, "rows=%d object_bytes=%s %s\n", sum.Output.RowsEmitted, measured(sum.Output.ObjectBytes, 1, "%.0f"), cpu)
		return nil
	}
	parts, outBytes := sum.Output.PartsWritten+sum.Output.PartsRetained, sum.Output.OutputBytes
	if t := sum.Output.Text; t != nil {
		parts, outBytes = 1, t.Bytes // one text file
	}
	fmt.Fprintf(stdout, "rows=%d parts=%d out_bytes=%d %s max_buffered=%d\n", sum.Output.RowsEmitted, parts, outBytes, cpu, sum.Resources.MaxBufferedBytes)
	return nil
}

// measured formats a measurement divided by unit, or "-" where the
// platform did not measure it.
func measured(v *int64, unit float64, format string) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf(format, float64(*v)/unit)
}

// explain adds command-line advice to the errors a user can act on; the
// packages' own messages name no flag.
func explain(err error) error {
	var mm *journal.MismatchError
	var ve *journal.VersionError
	var le *totals.LimitError
	switch {
	case errors.Is(err, listing.ErrAmbiguousTail):
		return fmt.Errorf("%w (the part is kept in DIR/.swath/suspect; swath list --overwrite starts over, or removing that directory accepts the source as it is now)", err)
	case errors.Is(err, dataset.ErrPartLimit):
		return fmt.Errorf("%w (the published parts stay, but a resume would stop at the same limit: start over with --overwrite and a larger --part-size, or without --part-rotation-max-rows)", err)
	case errors.Is(err, listing.ErrUnsafeLayout):
		return fmt.Errorf("%w (refused before swath created, renamed or removed anything through that path; parts, journal and suspects are as they were, and the outcome is in the report, and in error.json when swath could take the output lock to settle it (a refused workspace or journal can leave error.json as it was, so --report carries the refusal); inspect the output, or start over with swath list --overwrite)", err)
	case errors.Is(err, listing.ErrInterrupted):
		return fmt.Errorf("%w (swath resume DIR continues it; --overwrite starts over)", err)
	case errors.Is(err, dataset.ErrNotEmpty):
		return fmt.Errorf("%w (use --overwrite to replace a previous dataset)", err)
	case errors.Is(err, dataset.ErrOutIsRoot), errors.Is(err, dataset.ErrInsideRoot), errors.Is(err, dataset.ErrNotDataset):
		return fmt.Errorf("-o: %w", err)
	case errors.As(err, &mm):
		return fmt.Errorf("%w (start over with swath list --overwrite, or resume with the original settings)", err)
	case errors.As(err, &ve):
		return fmt.Errorf("%w (finish it with the build that started it, or start over with --overwrite)", err)
	case errors.As(err, &le):
		return fmt.Errorf("%w (lower --depth, or raise --max-prefixes if memory allows: about 150 bytes a prefix)", err)
	}
	return err
}

// profile starts the CPU profile and arranges the allocation profile, the
// files SWATH_CPUPROFILE and SWATH_MEMPROFILE name (as checkSidePaths
// resolved them), for development.
func profile(cpu, mem string) (stop func(), err error) {
	stops := []func(){}
	if path := cpu; path != "" {
		f, err := createNoFollow(path)
		if err != nil {
			return nil, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return nil, err
		}
		stops = append(stops, func() { pprof.StopCPUProfile(); f.Close() })
	}
	if path := mem; path != "" {
		runtime.MemProfileRate = 64 << 10
		stops = append(stops, func() {
			if f, err := createNoFollow(path); err == nil {
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

// createNoFollow creates or truncates the profile file path, refusing a
// symbolic link at its name (the Lstat; on Unix O_NOFOLLOW too, so a link
// planted after the Lstat fails the open): checkSidePaths checked where the
// link pointed then, and a link is not followed to where it points now.
// The report needs no such open: dataset.Replace renames a temporary file
// over the name, which replaces a link there and never writes its target.
func createNoFollow(path string) (*os.File, error) {
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s is a symbolic link: a profile is not written through one", path)
	}
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|oNoFollow, 0o644)
}

// size is a byte count flag, positive, parsed as the size filters are
// (filter.ParseSize: binary units, any case, an exact decimal quantity).
type size int64

// zsize is a size that may also be 0 (off).
type zsize size

func (s *zsize) String() string { return (*size)(s).String() }

func (s *zsize) Set(v string) error {
	n, err := filter.ParseSize(v)
	if err != nil {
		return err
	}
	*s = zsize(n)
	return nil
}

func (s *size) String() string { return strconv.FormatInt(int64(*s), 10) }

func (s *size) Set(v string) error {
	n, err := filter.ParseSize(v)
	if err != nil {
		return err
	}
	if n <= 0 {
		return fmt.Errorf("invalid size %q: must be positive", v)
	}
	*s = size(n)
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
