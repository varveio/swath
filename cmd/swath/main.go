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
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"

	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/textout"
	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/objfile"

	// Providers register themselves (scheme and flag group); the command
	// imports them for nothing else.
	_ "github.com/varveio/swath/internal/source/azblob"
	_ "github.com/varveio/swath/internal/source/gcs"
	_ "github.com/varveio/swath/internal/source/localfs"
	_ "github.com/varveio/swath/internal/source/s3"
)

// shutdownGrace bounds the time a stopped run (a signal, the deadline, a
// stall) spends stopping.
const shutdownGrace = 15 * time.Second

// exitAfterGrace exits the process with code if it is still running
// shutdownGrace after a stop began: blocked disk I/O or code that does not
// watch its context cannot outlive a signal, --max-duration or the stall
// timeout. main sets it; tests run the command in process and leave it nil.
var exitAfterGrace func(code int, why string)

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

// The Azure SDK's own log (azcore writes it to stderr when
// AZURE_SDK_GO_LOGGING=all) quotes request URLs, a SAS included: swath
// turns it off before any SDK use (TestAzureSDKLogSuppressed).
func init() { azlog.SetListener(nil) }

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
				elog().warn(fmt.Sprintf("still stopping (%s) after %s; exiting (an interrupted journaled run can be resumed)", why, shutdownGrace))
				if ok {
					forcedAccount(f, s, why)
				} else {
					elog().warn("the run's result could not be read in time; no outcome is written")
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
	statusOnSignal()
	testHooks()
	if code := conclude(dispatch(ctx, os.Args[1:], os.Stdout)); code != exitCompleted {
		os.Exit(code)
	}
}

// classifyRun is the terminal classification of a run's result: a
// committed dataset that does not cover its scope (a key limit) never
// exits 0 (a supervisor must not take it as complete), and the provider
// names the class of its errors.
func classifyRun(ctx context.Context, dir string, provider func(error) string, sum listing.Report, err error) (error, outcome) {
	var pe *textout.ProjectionError
	switch {
	case err == nil && dir != "" && !sum.Outcome.Complete:
		err = errIncomplete
	case errors.As(err, &pe):
		// Found when the schema is bound, before the listing's first
		// request: a usage error, as at parse time.
		err = usageError{fmt.Errorf("--columns: %w", pe)}
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
	switch {
	case o.countOnly || o.text != nil:
		return ""
	case o.remote != nil:
		return o.workDir
	}
	return o.out
}

// reported holds the attempts whose terminal report is claimed.
var reported sync.Map

// writeReport writes the run report to path (--report), atomically, when
// the attempt ends. That file, at a path the supervisor names outside the
// output, is the attempt's outcome of record; a failure to write it is
// returned (reportFailed), naming the attempt.
//
// An attempt has one terminal report: the first of its writers (the
// graceful path or the forced exit) claims it and a later one leaves it.
//
// --report - prints it on stdout instead (nil: an account with no stdout
// of its own, a forced exit's, writes none).
func writeReport(path string, r listing.Report, stdout io.Writer) error {
	switch {
	case path == "-" && stdout != nil:
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	case path == "" || path == "-":
		return nil
	}
	if r.Attempt != nil {
		if _, taken := reported.LoadOrStore(r.Attempt.ID, true); taken {
			return nil
		}
	}
	if err := dataset.ReplaceJSON(path, r, dataset.Durable); err != nil {
		// The attempt is named, so a supervisor never takes an older
		// report (another attempt's) for this one's outcome.
		id := ""
		if r.Attempt != nil {
			id = r.Attempt.ID
		}
		return fmt.Errorf("the report of attempt %s is not written to %s: %w", id, path, err)
	}
	return nil
}

// reportFailed is the command's error once the report could not be
// written (rerr): the run's own error with it, or, for a run that
// succeeded, that failure (exit 1); the run's outcome stands in its
// accounts.
func reportFailed(err, rerr error) error {
	var ee *exitError
	switch {
	case rerr == nil:
		return err
	case errors.As(err, &ee):
		return &exitError{error: fmt.Errorf("%w; %w", ee.error, rerr), code: ee.code, out: ee.out}
	case err != nil:
		return fmt.Errorf("%w; %w", err, rerr)
	}
	return &exitError{error: rerr, code: exitFatal, out: outcome{Class: listing.ClassFatal, ExitCode: exitFatal, Message: rerr.Error()}}
}

// stopEarly ends an attempt that stopped before a run began (an unknown
// scheme, say): the report (--report, or stdout for --report -) holds its
// outcome.
func stopEarly(stdout io.Writer, report string, argv []string, dir string, att *listing.Attempt, err error) error {
	o := classify(err, nil, "")
	o.Attempt = att
	r := listing.Unstarted(argv, dir, att, err)
	account(&r.Outcome, o)
	return reportFailed(&exitError{error: err, code: o.ExitCode, out: o}, writeReport(report, r, stdout))
}

// errReportStdout refuses --report - where stdout carries the rows.
var errReportStdout = errors.New("--report - prints the report on stdout, which carries the rows: give --report FILE")

// errText refuses swath resume of a text run.
var errText = errors.New("text output (table, tsv, jsonl) is not resumable: it keeps no journal; run swath list again")

// usageError is a malformed command line.
type usageError struct{ error }

// dispatch runs one command: list (ls), du, resume, show, diag list or
// diag count, help or version. The global flags (globalFlags) may come
// before the command word; they are handed to the command after it.
func dispatch(ctx context.Context, args []string, stdout io.Writer) error {
	g := pflag.NewFlagSet("swath", pflag.ContinueOnError)
	g.SetInterspersed(false)
	g.SortFlags = false
	g.SetOutput(io.Discard)
	var t termOptions
	t.register(g)
	var version bool
	source.Define(g, source.Flag{Name: "version", Short: "V", Value: source.Switch(&version)})
	p, err := parseFlags(g, "", args, func(fl *pflag.Flag) error {
		if fl.Name != "version" && !slices.Contains(globalFlags, fl.Name) {
			return usageError{fmt.Errorf("--%s goes after the command (swath list ... --%s)", fl.Name, fl.Name)}
		}
		return nil
	})
	// A refusal here (a bad root flag, an unknown command or diag form)
	// is reported in the format the whole command line asks for, read
	// with every command's flags (newFlags with the diagnostic ones takes
	// them all), so that a value is skipped as a value. By rule, a
	// --log-format after -- is an operand (swath -- oops --log-format
	// json reports in text), and for a line that does not parse
	// (--bogus show --output --log-format json) the format is best effort.
	if flags, _ := argFlags(newFlags(&options{}, true), args, true); askedFormat(flags) != "" {
		t.logFormat = askedFormat(flags)
	}
	setLog(t, "")
	switch {
	case errors.Is(err, pflag.ErrHelp):
		rootHelp(stdout, styleFor(stdout, t.color, p.longHelp))
		return err
	case err != nil:
		return err
	case version:
		printVersion(stdout)
		return nil
	case len(p.operands) == 0:
		return seeHelp{usageError{errors.New("no command")}, ""}
	}
	word, rest := p.operands[0], p.operands[1:]
	with := func(args []string) []string { return append(slices.Clone(p.given), args...) }
	switch word {
	case "list", "ls":
		return run(ctx, with(rest), stdout, false)
	case "du":
		return runDu(ctx, with(rest), stdout)
	case "diag":
		switch {
		case len(rest) == 0:
		case rest[0] == "-h" || rest[0] == "--help":
			var b strings.Builder
			diagHelp.write(&b, styleFor(stdout, t.color, false))
			fmt.Fprint(stdout, b.String())
			return pflag.ErrHelp
		case rest[0] == "list":
			return run(ctx, with(rest[1:]), stdout, true)
		case rest[0] == "count":
			return runCount(ctx, with(rest[1:]), stdout)
		}
		return seeHelp{usageError{errors.New("swath diag takes list or count")}, "diag"}
	case "resume":
		return resume(ctx, with(rest), stdout)
	case "show":
		return show(with(rest), stdout)
	case "completion":
		return completion(with(rest), stdout)
	case "version":
		printVersion(stdout)
		return nil
	case "help":
		if len(rest) == 0 {
			rootHelp(stdout, styleFor(stdout, t.color, true))
			return nil
		}
		return dispatch(ctx, append(with(rest), "--help"), stdout)
	}
	return seeHelp{usageError{fmt.Errorf("unknown command %q%s", registry.Redact(word), suggestCommand(word))}, ""}
}

// suggestCommand is the " (did you mean ...?)" of an unknown command
// word: a source given without a command (a URL, an existing directory)
// suggests swath list of it; another word the command within two edits.
func suggestCommand(word string) string {
	if strings.Contains(word, "://") || isDir(word) {
		return fmt.Sprintf(" (did you mean %q?)", "swath list "+registry.Redact(word))
	}
	best, dist := "", 3
	for _, c := range commandWords {
		if d := editDistance(word, c); d < dist {
			best, dist = c, d
		}
	}
	if best == "" {
		return ""
	}
	return fmt.Sprintf(" (did you mean %q?)", best)
}

// isDir reports whether path is an existing directory.
func isDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// printVersion prints swath VERSION (REVISION).
func printVersion(w io.Writer) {
	b := listing.BuildInfo()
	fmt.Fprintf(w, "swath %s", b.Version)
	if b.Revision != "" {
		fmt.Fprintf(w, " (%s)", b.Revision)
	}
	fmt.Fprintln(w)
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
	countOnly              bool     // swath du or swath diag count: rows counted, none written
	du                     bool     // swath du: the per-prefix totals
	report                 string   // --report: a file, or - for stdout
	parsed                 bool     // the flags parsed: a refusal after it writes the report
	argv                   []string // the command line as recorded: canonical flags, then SOURCE
	cpuProfile, memProfile string   // SWATH_CPUPROFILE, SWATH_MEMPROFILE, resolved (checkSidePaths)
	memProfileRate         int      // SWATH_MEMPROFILERATE, with memProfile
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
	allVersions            *pflag.Flag         // --all-versions (nil without a provider on the range engine)
	term                   termOptions
	format                 string
	columns                []string        // --columns
	text                   *textout.Config // resolved: a text run's destination
	filter                 filter.Spec
	partMaxRows            int64
	seedFrom               string
	// swath du's per-prefix totals: --depth, --max-prefixes, and the
	// local --allocated, --count-links-once and --max-inodes; totals is
	// their output, resolved.
	depth, maxPrefixes, maxInodes int
	allocated, linksOnce          bool
	sortBy                        string // --sort: sortName or sortSize
	totals                        *totalsOut
	// A bucket -o (gs://, s3:// or az://): its work directory, its
	// credentials, region and endpoint (--work-dir,
	// --output-gcs-credentials-file, --output-s3-profile,
	// --output-azure-sas-token, --output-s3-region, --output-endpoint-url),
	// and the location, resolved (nil for any other output).
	workDir, outputCreds, outputEndpoint, outputProfile, outputSAS, outputRegion string
	remote                                                                       *objfile.Location
}

// swath du's --sort orders.
const (
	sortName = "name"
	sortSize = "size"
)

// totalsOut is where swath du writes its prefix rows: stdout, in format
// (textout's TSV, JSONL or Table), by size when bySize (totals.SortBySize).
type totalsOut struct {
	format string
	bySize bool
	tally  *totals.Tally
}

// write writes the tally's rows to w.
func (t *totalsOut) write(w io.Writer) error {
	l := textout.TotalsLayout{Allocated: t.tally.Options().BytesColumn == totals.AllocatedBytes, Local: t.tally.Dirs()}
	rows := t.tally.Result()
	if t.bySize {
		totals.SortBySize(rows)
	}
	return textout.WriteTotals(w, t.format, l, rows)
}

// runtimeFlags may change on swath resume: they do not change the output
// (resources, reporting, time limits) ...
var runtimeFlags = append([]string{"workers", "encoders", "sort-buffer", "spill-budget", "spill-reserve", "no-spill-compress", "spill-plain", "part-interval", "report", "max-duration", "stall-timeout", "seed-from",
	"output-gcs-credentials-file", "output-s3-profile", "output-azure-sas-token"}, termFlags...)

// ... nor do operationalFlags, how requests are authenticated, timed and
// admitted, which no identity holds: the source and output identity (the
// route, the scope, the schema) is checked on resume and refuses a change,
// such as another region resolved from a new profile. They are the shared
// flag groups' and the providers' (source.FlagGroup.Operational,
// registry.Factory.Operational), secrets included.
var operationalFlags = providerOperational()

// providerOperational collects the operational flags of the shared flag
// groups, then of each provider: its secrets, then its other operational
// flags.
func providerOperational() []string {
	var flags []string
	for _, g := range registry.Groups() {
		flags = append(flags, g.Operational...)
	}
	for _, f := range registry.Factories() {
		for _, s := range f.Secrets {
			flags = append(flags, s.Flag)
		}
		flags = append(flags, f.Operational...)
	}
	return flags
}

// secretFlags hold a secret, or a command that prints one: their value is
// never recorded (journal, reports, the goroutine dump's outcome), and a
// resume must be given it again.
// They and secretEnv are the output's (registry.OutputSecretFlags) and the
// registered providers' (registry.Factory.Secrets).
var secretFlags, secretEnv = providerSecrets()

// providerSecrets collects the output's secret flags, then the registered
// providers' and the environment variable that may stand in for each on
// resume.
func providerSecrets() ([]string, map[string]string) {
	flags := slices.Clone(registry.OutputSecretFlags)
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

// redactArgv is the recorded form shared with listing.Run.
func redactArgv(argv []string) []string { return registry.RedactArgv(argv) }

// resumeFlags are the flags swath resume accepts.
var resumeFlags = append(slices.Clone(runtimeFlags), operationalFlags...)

// foreignFlags refuses a provider flag given explicitly for a root of
// another provider (one provider's credential flag with another's root,
// which that provider would ignore), and a shared group's flag for a
// provider that names no such group. A root no provider takes is left to
// the lookup's own refusal.
func foreignFlags(f *pflag.FlagSet, owners map[string][]string, root string) (err error) {
	pf, lerr := registry.Lookup(root)
	if lerr != nil {
		return nil
	}
	f.Visit(func(fl *pflag.Flag) {
		own, ok := owners[fl.Name]
		if !ok || slices.Contains(own, pf.Scheme) || err != nil {
			return
		}
		names := make([]string, len(own))
		for i, s := range own {
			names[i] = schemeName(s)
		}
		err = fmt.Errorf("--%s applies to %s sources, not to %s sources", fl.Name, strings.Join(names, " and "), schemeName(pf.Scheme))
	})
	return err
}

// schemeName names the provider of scheme as the help does
// (registry.Factory.Title).
func schemeName(scheme string) string {
	for _, f := range registry.Factories() {
		if f.Scheme == scheme {
			return f.Title
		}
	}
	return scheme
}

// rateFitsStall refuses a --request-rate whose spacing between attempts
// exceeds half the stall timeout: the watchdog sees no answered request
// while an attempt waits for its token, so a healthy listing at that rate
// would stop as stuck (exit 75). Half leaves the other half for the
// request itself.
func rateFitsStall(f *pflag.FlagSet, stall time.Duration) error {
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

// runCount is swath diag count.
func runCount(ctx context.Context, args []string, stdout io.Writer) error {
	return runWith(ctx, command{diag: true, count: true}, args, stdout, nil, false, nil)
}

// runDu is swath du.
func runDu(ctx context.Context, args []string, stdout io.Writer) error {
	return runWith(ctx, command{count: true, du: true}, args, stdout, nil, false, nil)
}

// command is one of the list commands: swath list, swath du (a count that
// totals per prefix), and the swath diag forms of list and count.
type command struct{ diag, count, du bool }

// name is the command as typed.
func (c command) name() string {
	n := "list"
	switch {
	case c.du:
		n = "du"
	case c.count:
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
	o.countOnly, o.du = cmd.count, cmd.du
	p, err := parseFlags(f, cmd.name(), args, nil)
	setLog(o.term, "")
	if errors.Is(err, pflag.ErrHelp) {
		usage(stdout, f, cmd, styleFor(stdout, o.term.color, p.longHelp))
		return o, "", err
	}
	o.parsed = err == nil
	o.argv = commandLine(cmd.name(), p, p.operands...)
	switch {
	case err != nil:
		return o, "", err
	case len(p.operands) == 0:
		return o, "", seeHelp{usageError{errors.New("missing SOURCE")}, cmd.name()}
	case len(p.operands) > 1:
		return o, "", seeHelp{usageError{fmt.Errorf("unexpected argument %q", registry.Redact(p.operands[1]))}, cmd.name()}
	}
	root = registry.Canonical(p.operands[0])
	o.argv = commandLine(cmd.name(), p, root)
	if err := placement(f, cmd); err != nil {
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
	o.filter.Resolve(time.Now())
	for i, a := range o.argv {
		for _, t := range []struct{ flag, v string }{{"--modified-since=", o.filter.Since}, {"--modified-until=", o.filter.Until}} {
			if strings.HasPrefix(a, t.flag) {
				o.argv[i] = t.flag + t.v
			}
		}
	}
	if err := o.filter.Validate(); err != nil {
		return o, "", usageError{err}
	}
	if pf, err := registry.Lookup(root); err == nil && pf.CheckFilter != nil {
		if err := pf.CheckFilter(o.sources[pf.Scheme], &o.filter); err != nil {
			return o, "", usageError{err}
		}
	}
	o.filter.Base, _ = keyOrigin(root) // a root it cannot read is refused when the run opens it
	text, err := resolveOutput(&o, stdoutTerminal())
	if err != nil {
		return o, "", usageError{err}
	}
	o.text = text
	if err := resolveTotals(&o, root, stdoutTerminal()); err != nil {
		return o, "", usageError{err}
	}
	if err := checkSidePaths(&o); err != nil {
		return o, "", usageError{err}
	}
	if o.memProfile != "" {
		if o.memProfileRate, err = memProfileRate(os.Getenv("SWATH_MEMPROFILERATE")); err != nil {
			return o, "", usageError{err}
		}
	}
	if (text != nil && text.Path == "" || o.totals != nil) && o.report == "-" {
		return o, "", usageError{errReportStdout}
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
	if u := (usageError{}); err != nil && o.parsed && o.report != "" && !(errors.As(err, &u) && u.error == errReportStdout) {
		// A refusal of a command line that parsed has a report, unless
		// its destination is itself refused.
		if checkSidePaths(&o) == nil {
			return stopEarly(stdout, o.report, o.argv, "", att, err)
		}
	}
	if err != nil {
		return err
	}
	if o.text != nil && resumed {
		return usageError{errText}
	}
	toStdout := o.text != nil && o.text.Path == ""
	setLog(o.term, att.ID)
	argv := recorded
	if argv == nil {
		argv = o.argv
	}
	argv = redactArgv(argv)
	pf, err := registry.Lookup(root)
	if err != nil {
		return stopEarly(stdout, o.report, argv, cfgOut(o), att, usageError{err})
	}
	root, narrowing := narrowRoot(root, &o.filter)
	cfg := listing.Config{
		Source: listing.SourceConfig{URI: root, Options: o.sources[pf.Scheme], SeedFrom: o.seedFrom, Narrowing: narrowing},
		Output: listing.OutputConfig{Replace: o.overwrite, Resume: resumed, NoJournal: o.noJournal, Text: o.text},
		Limits: listing.Limits{Workers: o.workers, Encoders: o.encoders, RowBudget: int64(o.rowBudget), PartSize: int64(o.partSize), PartInterval: o.partInterval, PartMaxRows: o.partMaxRows,
			Spill: listing.SpillLimits{Off: o.noSpill, Plain: o.spillPlain, Uncompressed: !o.spillCompress, Max: int64(o.spillBudget), Reserve: int64(o.spillReserve)}},
		Argv:    argv,
		Attempt: att,
		Filter:  &o.filter,
		Warn:    func(m string) { elog().warn(m) },
	}
	if t := o.totals; t != nil {
		// swath du's rows are its output: written once the count is
		// complete, before the run's end and outcome are settled.
		cfg.Totals = t.tally
		cfg.Deliver = func(context.Context) error { return t.write(stdout) }
	}
	if !o.countOnly && o.text == nil {
		cfg.Output.Dir = o.out
	}
	if o.remote != nil {
		// The dataset goes to the bucket; the work directory is owned as a
		// dataset directory is (lock, journal, spill, receipts).
		cfg.Output.Dir = o.workDir
		cfg.Output.Remote = &listing.RemoteOutput{Location: o.remote.String(), Endpoint: o.outputEndpoint, CredentialsFile: o.outputCreds,
			Profile: o.outputProfile, SAS: o.outputSAS, Region: o.outputRegion}
	}
	prof, err := startProfiles(o.cpuProfile, o.memProfile, o.memProfileRate, func(m string) { elog().warn(m) })
	if err != nil {
		return stopEarly(stdout, o.report, argv, cfgOut(o), att, err)
	}
	if prof != nil {
		cfg.ListingDone = prof.listingDone
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
	fw := newFailureWatch(pf.Title)
	term := &terminal{ctx: ctx, dir: cfg.Output.Dir, provider: pf.Classify, att: att}
	forensics.Store(&forensicTarget{ctx: ctx, dir: cfg.Output.Dir, pr: cfg.Progress, report: o.report, argv: argv, att: att, t: term})
	// One terminal result (terminal): the classification is applied to the
	// report before run.json, error.json (Settle) or --report is written,
	// and every one of them derives from it. error.json is written while
	// the run still owns the output (Settle), never by a run that did not.
	cfg.Finalize = func(r *listing.Report, err error) {
		if (toStdout || o.totals != nil) && brokenPipe(err) {
			// The reader closed the stream: exit 0, as Java's, and the
			// report says the listing did not reach its end.
			err = nil
			r.Outcome.Status, r.Outcome.Complete, r.Outcome.StopReason, r.Outcome.Error = listing.StatusStopped, false, listing.StopDownstreamClosed, ""
		}
		if r.Requests != nil {
			r.Requests.FailureWarnings, r.Requests.LogDropped = fw.warnings.Load(), dropped.Load()
		}
		r.Profiles = prof.account()
		term.account(r, err)
	}
	cfg.Commit = term.commit // the commit and its account, one step against a forced exit
	if prof != nil {
		// The profiles' phase switch, which may block on a slow file,
		// finishes before the commit's gate is taken, never inside it.
		cfg.Commit = func(step func() error) error {
			prof.wait()
			return term.commit(step)
		}
	}
	settled, unrecorded := false, ""
	cfg.Settle = func(sum listing.Report, err error) {
		term.account(&sum, err) // a no-op for the accounted report Settle gets
		if werr := writeOutcome(cfg.Output.Dir, outcomeOf(sum)); werr != nil {
			unrecorded = "write " + errorName + ": " + werr.Error()
		} else {
			settled = true
		}
	}
	// The empty scheme is a local path. stdout carries rows to a reader on
	// the same screen: swath list's text there, and swath du's table.
	n := rowNouns(pf.Scheme == "", o.versions())
	tt := startTerminal(o.term, att, startOf(registry.Redact(root), o, cfg.Output.Dir), cfg.Output.Dir != "", n, (toStdout || o.totals != nil) && stdoutTerminal())
	defer tt.close()
	defer watchRequests(fw)() // removed before stderr is restored
	status := func() { tt.status(cfg.Progress.Snapshot()) }
	statusNow.Store(&status)
	defer statusNow.Store(nil)
	stopWatch := supervise(ctx, cancel, cfg.Progress, cfg.Output.Dir, o.stall, cfg.Warn, tt.disp)
	sum, err := listing.Run(ctx, cfg)
	_ = faults.At("run-returned") // tests: a stop cause arriving while the run settles
	stopWatch()
	tt.disp.stop()
	if prof != nil {
		prof.stop()
		sum.Profiles = prof.account() // the files' final status, for --report
	}
	err, out := term.classify(sum, err)
	end := runEnd{settled: settled, unrecorded: unrecorded, started: cfg.Progress.Snapshot().Liveness() > 0, failing: fw.outstanding(time.Now())}
	return finishList(stdout, o, cfg.Output.Dir, sum, err, out, end, tt)
}

// versions says --all-versions is on: the run lists every version.
func (o *options) versions() bool {
	return o.allVersions != nil && o.allVersions.Value.String() == "true"
}

// runEnd is what finishList knows of a run besides its report: settled
// says error.json holds its outcome, unrecorded why not; started that a
// listing request was answered or a directory read; failing the
// persistent request failure outstanding at its end (failureWatch).
type runEnd struct {
	settled    bool
	unrecorded string
	started    bool
	failing    string
}

// oneLine says a failed run's stderr is its error line alone (D1, D4 of
// the CLI review): a refusal or usage error (exit 2), or a failure before
// any listing request was answered or directory read, other than a stop
// (a signal, the deadline, the stall timeout, a full disk), whose
// statistics stand.
func oneLine(sum listing.Report, out outcome, started bool) bool {
	switch {
	case out.ExitCode == exitCompleted:
		return false
	case out.ExitCode == exitUsage:
		return true
	case started || resumableStops[out.Class] || sum.Output.RowsFetched+sum.Output.RowsRetained > 0:
		return false
	}
	return true
}

// finishList is a list command's terminal settlement after its run: the
// accounts derived from the frozen outcome out (the report, -json, the
// outcome on stderr when error.json does not hold it) and the error main
// exits with.
func finishList(stdout io.Writer, o options, dir string, sum listing.Report, err error, out outcome, end runEnd, tt *termUI) error {
	settled, unrecorded := end.settled, end.unrecorded
	brief := oneLine(sum, out, end.started)
	failing := ""
	if err != nil && end.failing != "" && (out.Class == listing.ClassStuck || out.Class == listing.ClassDeadline) {
		// The report's error and the outcome's message name the cause
		// too, not only the error line.
		failing = "; every request was failing: " + end.failing
		out.Message += failing
	}
	if err != nil && out.ExitCode == exitCompleted {
		// A resume that found the run committed by another process
		// meanwhile: nothing to do, as swath resume on a committed dataset.
		account(&sum.Outcome, out)
		alreadyComplete(stdout, o.report, dir, &sum)
		return reportFailed(nil, writeReport(o.report, sum, stdout))
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
		if !brief || o.term.verbose > 0 {
			reportUnrecorded(dir, unrecorded)
		}
	}
	account(&sum.Outcome, out)
	if errors.Is(err, listing.ErrComplete) {
		committedAccount(dir, &sum)
	}
	sum.Outcome.Unrecorded = unrecorded
	// The report goes out on failure too: its outcome names the error.
	rerr := writeReport(o.report, sum, stdout)
	if !brief || o.term.stats.set {
		tt.stats(&sum, sum.Outcome.StopReason == listing.StopDownstreamClosed)
	}
	// Entries left out say so, whatever the statistics' gating.
	reportUnreadable(&sum, o.du)
	switch {
	case err != nil:
		err = explain(err)
		if failing != "" {
			err = fmt.Errorf("%w%s", err, failing)
		}
		return reportFailed(&exitError{error: err, code: out.ExitCode, out: out}, rerr)
	case rerr != nil:
		return reportFailed(nil, rerr)
	case o.report == "-":
		return nil // stdout carries the report
	case !o.countOnly || o.du:
		return nil // stdout carries the rows (swath du's: Deliver), or nothing: the data went to -o; --stats prints the account on stderr
	}
	if r := sum.SourceReport(); r != nil && r.Brief != "" {
		fmt.Fprint(stdout, r.Brief, " ")
	}
	cpu := fmt.Sprintf("wall=%.3fs user=%s sys=%s peak_rss=%s", float64(sum.Timings.WallMs)/1000,
		measured(sum.Timings.UserCPUMs, 1000, "%.2fs"), measured(sum.Timings.SysCPUMs, 1000, "%.2fs"),
		measured(sum.Resources.PeakRSSBytes, 1<<20, "%.1fMiB"))
	// swath diag count's output is its one line of counts.
	fmt.Fprintf(stdout, "rows=%d object_bytes=%s %s\n", sum.Output.RowsEmitted, measured(sum.Output.ObjectBytes, 1, "%.0f"), cpu)
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
	var ce interface { // objhttp.CredentialError
		CredentialRefused() bool
		CredentialSource() string
		CredentialDiscovered() bool
		Unwrap() error
	}
	switch {
	case errors.Is(err, listing.ErrAmbiguousTail):
		return fmt.Errorf("%w (the part is kept in DIR/.swath/suspect; swath list --overwrite starts over, or removing that directory accepts the source as it is now)", err)
	case errors.Is(err, dataset.ErrPartLimit):
		return fmt.Errorf("%w (the published parts stay, but a resume would stop at the same limit: start over with --overwrite and a larger --part-size, or without --part-rotation-max-rows)", err)
	case errors.Is(err, listing.ErrUnsafeLayout):
		return fmt.Errorf("%w (refused before swath created, renamed or removed anything through that path; parts, journal and suspects are as they were, and the outcome is in the report, and in error.json when swath could take the output lock to settle it (a refused workspace or journal can leave error.json as it was, so --report carries the refusal); inspect the output, or start over with swath list --overwrite)", err)
	case errors.Is(err, listing.ErrInterrupted):
		return fmt.Errorf("%w (swath resume DIR continues it; --overwrite starts over)", err)
	case errors.Is(err, dataset.ErrInsideRoot):
		// Not empty, or --overwrite given: either way --overwrite cannot
		// help here.
		return fmt.Errorf("-o: %w (write the dataset to a new directory outside the listed root)", err)
	case errors.Is(err, dataset.ErrNotEmpty):
		return fmt.Errorf("%w (use --overwrite to replace a previous dataset)", err)
	case errors.Is(err, dataset.ErrOutIsRoot), errors.Is(err, dataset.ErrNotDataset):
		return fmt.Errorf("-o: %w", err)
	case errors.As(err, &mm):
		return fmt.Errorf("%w (start over with swath list --overwrite, or resume with the original settings)", err)
	case errors.As(err, &ve):
		return fmt.Errorf("%w (finish it with the build that started it, or start over with --overwrite)", err)
	case errors.As(err, &le) && le.Inodes:
		return fmt.Errorf("%w (raise --max-inodes if memory allows, or count every link without --count-links-once)", err)
	case errors.As(err, &le):
		return fmt.Errorf("%w (lower --depth, or raise --max-prefixes if memory allows: about 150 bytes a prefix)", err)
	case errors.As(err, &ce) && ce.CredentialRefused():
		// No credentials, or none that load: swath never falls back to
		// anonymous requests on its own.
		line := credentialLine(ce.CredentialSource(), ce.Unwrap().Error(), ce.CredentialDiscovered(), elog().shows(lvInfo))
		return &credentialRefusal{error: err, line: line}
	}
	return err
}

// credentialRefusal is a credential failure that will not pass, shown as
// its line (credentialLine). The whole cause (an SDK's chain report, a
// token endpoint's answer) is the outcome's message, which -v shows
// (conclude).
type credentialRefusal struct {
	error
	line string
}

func (e *credentialRefusal) Error() string { return e.line }

func (e *credentialRefusal) Unwrap() error { return e.error }

// causeCap bounds the cause's first line in a credential refusal's line,
// in runes.
const causeCap = 200

// credentialLine is a refused credential's line: its source and its
// cause's first line that is not blank (capped at causeCap), control
// characters in either escaped; the --anonymous advice when swath was
// searching for credentials on its own (discovered), not when one was
// configured; and, without -v (verbose), that -v shows what the line
// leaves out (lines before or after it, or past the cap), when it leaves
// something out.
func credentialLine(source, cause string, discovered, verbose bool) string {
	lines := strings.Split(strings.ReplaceAll(cause, "\r", "\n"), "\n")
	first, more := "", false
	for i, l := range lines {
		if first = strings.TrimSpace(l); first != "" {
			more = i > 0 || strings.TrimSpace(strings.Join(lines[i+1:], "")) != ""
			break
		}
	}
	if r := []rune(first); len(r) > causeCap {
		first, more = strings.TrimSpace(string(r[:causeCap]))+"...", true
	}
	msg := "could not obtain credentials (" + escapeControl(source) + ")"
	if first != "" {
		msg += ": " + escapeControl(first)
	}
	var advice []string
	if discovered {
		advice = append(advice, "for public storage use --anonymous")
	}
	if more && !verbose {
		advice = append(advice, "-v shows the details")
	}
	if len(advice) == 0 {
		return msg
	}
	if !strings.HasSuffix(msg, ".") {
		msg += "."
	}
	a := strings.Join(advice, "; ")
	return msg + " " + strings.ToUpper(a[:1]) + a[1:]
}

// escapeControl writes each control character of s as its Go escape (\n,
// \x1b), so the text stays on one line and draws nothing.
func escapeControl(s string) string {
	if !strings.ContainsFunc(s, unicode.IsControl) {
		return s
	}
	var b strings.Builder
	for _, r := range s {
		if unicode.IsControl(r) {
			b.WriteString(strings.Trim(strconv.QuoteRune(r), "'"))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
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

func (s *size) Type() string { return "size" }

func (s *zsize) Type() string { return "size" }

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

// narrowRoot is an object-store root narrowed to the prefix the includes
// imply (filter.Spec.NarrowPrefix), and the narrowing's engagement: the
// same rows, fewer requests. A resume replays the same command, so it
// narrows the same way. Only a provider that can name another prefix
// (registry.Factory.WithPrefix) narrows.
func narrowRoot(root string, f *filter.Spec) (string, string) {
	pf, err := registry.Lookup(root)
	if err != nil {
		return root, ""
	}
	prefix := "" // a local tree's keys are relative to its root
	if pf.KeyPrefix != nil {
		if prefix, err = pf.KeyPrefix(root); err != nil {
			return root, ""
		}
	}
	n, why := f.NarrowPrefix(prefix)
	switch {
	case why != filter.Narrowed:
		return root, why
	case pf.WithPrefix == nil:
		return root, filter.NarrowUnsupported
	}
	return pf.WithPrefix(root, n), why
}
