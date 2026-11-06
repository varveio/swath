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
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/registry"

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
	frozen atomic.Pointer[outcome] // the run's classification, once made
}

func main() {
	ctx, cancel := context.WithCancelCause(context.Background())
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	finished := make(chan struct{})
	defer close(finished)
	exitAfterGrace = func(code int, why string) {
		go func() {
			select {
			case <-finished:
			case <-time.After(shutdownGrace):
				// The exit does not depend on the diagnostics: whatever
				// blocks below (a full stderr pipe nobody drains, a disk
				// that does not return), the process exits within
				// forcedAllowance.
				f := forensics.Load()
				if f == nil {
					f = &forensicTarget{}
				}
				if fr := f.frozen.Load(); fr != nil {
					code = fr.ExitCode // the run's classification stands
				}
				time.AfterFunc(forcedAllowance, func() { os.Exit(code) })
				note(errOut, fmt.Sprintf("swath: still stopping (%s) after %s; exiting (an interrupted journaled run can be resumed)\n", why, shutdownGrace))
				dumpStacks(f.dir, f.pr, "forced-exit", why, f.pr.Snapshot())
				forcedAccount(f, code, why)
				os.Exit(code)
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
	fmt.Fprintln(os.Stderr, "swath:", err)
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

// settled is a run's frozen classification: the error as classified and
// its outcome.
type settled struct {
	err error
	out outcome
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
	if o.countOnly {
		return ""
	}
	return o.out
}

// writeReport writes the run report to path (--report), atomically, when
// the attempt ends. That file, at a path the supervisor names outside the
// output, is the attempt's outcome of record; a failure to write it goes to
// stderr and the exit code stands alone.
func writeReport(path string, r listing.Report) {
	if path == "" {
		return
	}
	if err := writeFileAtomic(path, r); err != nil {
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
	r.Outcome.Class, r.Outcome.ExitCode, r.Outcome.Cause = o.Class, o.ExitCode, o.Cause
	writeReport(report, r)
	return &exitError{err, o.ExitCode}
}

// partInterval maps --part-interval (0: off) to listing.Limits (negative:
// off; 0 would take the default).
func partInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return -1
	}
	return d
}

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
	partSize, memory       size
	spillMax, spillReserve zsize
	resume, noJournal      bool
	noSpill, spillCompress bool
	sources                map[string]any // each provider's options value, by scheme
}

// runtimeFlags may change on swath resume: they do not change the output
// (resources, reporting, time limits) ...
var runtimeFlags = []string{"workers", "encoders", "memory", "spill-max", "spill-reserve", "spill-compress", "part-interval", "report-format", "report", "max-duration", "stall-timeout"}

// ... nor do operationalFlags, how requests are authenticated, timed and
// admitted, which no identity holds: the source and output identity (the
// route, the scope, the schema) is checked on resume and refuses a change,
// such as another region resolved from a new profile.
var operationalFlags = []string{"profile", "anonymous", "request-timeout", "gcs-anonymous", "gcs-credentials", "gcs-request-timeout", "limiter", "limiter-slow-start", "request-rate"}

// resumeFlags are the flags swath resume accepts.
var resumeFlags = append(slices.Clone(runtimeFlags), operationalFlags...)

func newFlags(o *options, diag bool) *flag.FlagSet {
	f := flag.NewFlagSet("list", flag.ContinueOnError)
	f.StringVar(&o.out, "output", "", "output dataset `directory` (required unless --count-only)")
	f.StringVar(&o.out, "o", "", "short for --output")
	f.BoolVar(&o.overwrite, "overwrite", false, "replace a previous dataset in --output")
	o.partSize = listing.DefaultPartSize
	f.Var(&o.partSize, "part-size", "target `bytes` per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB)")
	o.memory = listing.DefaultMemory
	f.Var(&o.memory, "memory", "`bytes` of rows held between listing and encoding; sets the reorder bound")
	f.IntVar(&o.workers, "workers", 0, "listing goroutines (default min(64, max(4, 3*GOMAXPROCS)))")
	f.IntVar(&o.encoders, "encoders", 0, "Parquet encoder goroutines (default GOMAXPROCS); fewer are admitted when --memory or the descriptor limit cannot carry them, never fewer than one")
	f.DurationVar(&o.partInterval, "part-interval", listing.DefaultPartInterval, "close an open part with rows after this `duration` although not full, bounding the age of the durable progress; 0: parts close by size only")
	f.BoolVar(&o.countOnly, "count-only", false, "walk and count without writing a dataset")
	f.BoolVar(&o.spillCompress, "spill-compress", false, "compress spill files (zstd level 1)")
	f.Var(&o.spillMax, "spill-max", "cap on spill `bytes` on disk (0: half the free space on the spill filesystem at start, after --spill-reserve); past it, listing waits instead of spilling")
	o.spillReserve = 1 << 30
	f.Var(&o.spillReserve, "spill-reserve", "free `bytes` spill leaves on its filesystem, for the parts still to be written")
	f.StringVar(&o.reportFormat, "report-format", "text", "`text` (one line) or json (the run report, as in run.json)")
	f.StringVar(&o.report, "report", "", "also write the run report (JSON, with the outcome and the attempt id) to this `file` when the attempt ends, on failure too")
	f.DurationVar(&o.maxDuration, "max-duration", 0, "stop the run after this `duration` (exit 124; a journaled run is resumable); 0: no limit")
	f.DurationVar(&o.stall, "stall-timeout", 10*time.Minute, "stop the run as stuck after this long without progress (exit 75; resumable); 0: never")
	if diag {
		f.BoolVar(&o.noJournal, "no-journal", false, "diagnostic: publish parts only at the end, without the journal that swath resume needs")
		f.BoolVar(&o.noSpill, "no-spill", false, "diagnostic: wait on the reorder budget instead of spilling rows ahead of the frontier to disk")
	}
	// Each provider owns its flag group.
	o.sources = map[string]any{}
	for _, p := range registry.Factories() {
		o.sources[p.Scheme] = p.Flags(f, diag)
	}
	f.SetOutput(io.Discard)
	return f
}

// usage prints the flags generated from the flag set itself.
func usage(w io.Writer, f *flag.FlagSet, diag bool) {
	if diag {
		fmt.Fprintln(w, "usage: swath diag list SOURCE [flags]   (the list flags plus diagnostic ones)")
	} else {
		fmt.Fprintln(w, "usage: swath list SOURCE --output DIR [flags]")
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

// runWith lists; argv0 is the recorded command, resumed tells a resume, att
// is the attempt (nil: a new one).
func runWith(ctx context.Context, args []string, stdout io.Writer, diag bool, recorded []string, resumed bool, att *listing.Attempt) error {
	if att == nil {
		att = newAttempt()
	}
	var o options
	f := newFlags(&o, diag)
	var root string
	for rest := args; ; {
		if err := f.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				usage(stdout, f, diag)
				return err
			}
			return usageError{err}
		}
		if f.NArg() == 0 {
			break
		}
		if root != "" {
			return usageError{fmt.Errorf("unexpected argument %q", f.Arg(0))}
		}
		root, rest = f.Arg(0), f.Args()[1:]
	}
	if root == "" {
		return usageError{errors.New("missing SOURCE")}
	}
	if !o.countOnly && o.out == "" {
		return usageError{errors.New("--output is required unless --count-only")}
	}
	if o.reportFormat != "text" && o.reportFormat != "json" {
		return usageError{fmt.Errorf("--report-format %q: want text or json", o.reportFormat)}
	}
	argv := recorded
	if argv == nil {
		argv = append([]string{"list"}, args...)
		if diag {
			argv = append([]string{"diag", "list"}, args...)
		}
	}
	pf, err := registry.Lookup(root)
	if err != nil {
		return stopEarly(o.report, argv, cfgOut(o), att, err)
	}
	cfg := listing.Config{
		Source: listing.SourceConfig{URI: root, Options: o.sources[pf.Scheme]},
		Output: listing.OutputConfig{Replace: o.overwrite, Resume: resumed, NoJournal: o.noJournal},
		Limits: listing.Limits{Workers: o.workers, Encoders: o.encoders, Memory: int64(o.memory), PartSize: int64(o.partSize), PartInterval: partInterval(o.partInterval),
			Spill: listing.SpillLimits{Off: o.noSpill, Compress: o.spillCompress, Max: int64(o.spillMax), Reserve: int64(o.spillReserve)}},
		Argv:    argv,
		Attempt: att,
		Warn:    func(m string) { note(errOut, "swath: warning: "+m+"\n") },
	}
	if !o.countOnly {
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
	forensics.Store(&forensicTarget{ctx: ctx, dir: cfg.Output.Dir, pr: cfg.Progress, report: o.report, argv: argv, att: att})
	// The outcome: error.json is written while the run still owns the
	// output (Settle), and never by a run that did not own it.
	// One frozen classification: the first computation (Finalize, before
	// any account is written) is reused by Settle, the command's return
	// and a forced exit, so a stop cause that arrives later (the deadline
	// passing while the run settles) cannot reclassify a run whose
	// error.json is written.
	var frozenMu sync.Mutex
	var frozen *settled
	settle := func(sum listing.Report, err error) (error, outcome) {
		frozenMu.Lock()
		defer frozenMu.Unlock()
		if frozen != nil {
			return frozen.err, frozen.out
		}
		err, out := classifyRun(ctx, cfg.Output.Dir, pf.Classify, sum, err)
		out.Attempt = att
		frozen = &settled{err, out}
		if f := forensics.Load(); f != nil {
			f.frozen.Store(&out)
		}
		return err, out
	}
	// One terminal account: the classification is applied to the report
	// before run.json, error.json (Settle) or --report is written.
	cfg.Finalize = func(r *listing.Report, err error) {
		_, out := settle(*r, err)
		r.Outcome.Class, r.Outcome.ExitCode, r.Outcome.Resumable, r.Outcome.Cause = out.Class, out.ExitCode, out.Resumable, out.Cause
	}
	settled, unrecorded := false, ""
	cfg.Settle = func(sum listing.Report, err error) {
		_, out := settle(sum, err)
		if werr := writeOutcome(cfg.Output.Dir, out); werr != nil {
			unrecorded = "write " + errorName + ": " + werr.Error()
		} else {
			settled = true
		}
	}
	stopWatch := supervise(ctx, cancel, cfg.Progress, cfg.Output.Dir, o.stall, cfg.Warn)
	sum, err := listing.Run(ctx, cfg)
	_ = faults.At("run-returned") // tests: a stop cause arriving while the run settles
	stopWatch()
	stopProfile()
	err, out := settle(sum, err)
	if cfg.Output.Dir != "" && !settled && (err != nil || unrecorded != "") {
		// Refused before ownership (another owner's outcome stands, the
		// run changed meanwhile, or there is none to settle), or the write
		// failed: the outcome goes to stderr instead, with the reason.
		if unrecorded == "" {
			unrecorded = sum.Outcome.Unrecorded
		}
		if unrecorded == "" {
			unrecorded = "stopped before owning the output"
		}
		reportUnrecorded(cfg.Output.Dir, out, unrecorded)
	}
	sum.Outcome.Class, sum.Outcome.ExitCode, sum.Outcome.Resumable, sum.Outcome.Cause = out.Class, out.ExitCode, out.Resumable, out.Cause
	sum.Outcome.Unrecorded = unrecorded
	writeReport(o.report, sum)
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
	if r := sum.SourceReport(); r != nil && r.Brief != "" {
		fmt.Fprint(stdout, r.Brief, " ")
	}
	fmt.Fprintf(stdout, "rows=%d parts=%d out_bytes=%d wall=%.3fs user=%.2fs sys=%.2fs peak_rss=%.1fMiB max_buffered=%d\n",
		sum.Output.RowsListed, sum.Output.PartsWritten+sum.Output.PartsRetained, sum.Output.Bytes, float64(sum.Timings.WallMs)/1000,
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
	*s = size(n * mult)
	return nil
}
