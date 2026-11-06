// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command swath lists a local directory tree or an S3 bucket and writes a globally
// key-sorted Parquet dataset in Swath's sorted dataset layout.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
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

// shutdownGrace bounds the time an interrupted run spends stopping.
const shutdownGrace = 15 * time.Second

func main() {
	ctx, cancel := context.WithCancelCause(context.Background())
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-finished:
			return
		case sig := <-sigs:
			cancel(&signalError{sig}) // graceful: the run stops and cleans up
		}
		select { // bounded shutdown: an interrupted run gets 15 s
		case <-finished:
		case <-time.After(shutdownGrace):
			fmt.Fprintf(os.Stderr, "swath: still stopping after %s; exiting (an interrupted journaled run can be resumed)\n", shutdownGrace)
			os.Exit(exitInterrupted)
		}
	}()
	killAt(os.Getenv("SWATH_TEST_KILL_AT"))
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

// killAt is a test hook: "point:n" makes the process SIGKILL itself the
// n-th time it reaches crash point point (package faults).
func killAt(spec string) {
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
			kill()
		}
		return nil
	})
}

type options struct {
	out                    string
	overwrite              bool
	countOnly              bool
	reportFormat           string
	maxDuration, stall     time.Duration
	workers, encoders      int
	partSize, memory       size
	spillMax, spillReserve zsize
	resume, noJournal      bool
	noSpill, spillCompress bool
	sources                map[string]any // each provider's options value, by scheme
}

// runtimeFlags may change on swath resume: they do not change the output.
var runtimeFlags = []string{"workers", "encoders", "memory", "spill-max", "spill-reserve", "spill-compress", "report-format", "max-duration", "stall-timeout"}

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
	f.IntVar(&o.encoders, "encoders", 0, "Parquet encoder goroutines (default GOMAXPROCS)")
	f.BoolVar(&o.countOnly, "count-only", false, "walk and count without writing a dataset")
	f.BoolVar(&o.spillCompress, "spill-compress", false, "compress spill files (zstd level 1)")
	f.Var(&o.spillMax, "spill-max", "cap on spill `bytes` on disk (0: no cap); past it, listing waits instead of spilling")
	o.spillReserve = 1 << 30
	f.Var(&o.spillReserve, "spill-reserve", "free `bytes` spill leaves on its filesystem, for the parts still to be written")
	f.StringVar(&o.reportFormat, "report-format", "text", "`text` (one line) or json (the run report, as in run.json)")
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
	fmt.Fprintln(w, "only "+strings.Join(runtimeFlags, ", ")+" may change.")
	fmt.Fprintln(w, "swath show [--journal] DIR shows a dataset or an interrupted run.")
	fmt.Fprintln(w, "\nProfiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.")
}

func run(ctx context.Context, args []string, stdout io.Writer, diag bool) error {
	return runWith(ctx, args, stdout, diag, nil, false)
}

// runWith lists; argv0 is the recorded command, resumed tells a resume.
func runWith(ctx context.Context, args []string, stdout io.Writer, diag bool, recorded []string, resumed bool) error {
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
		return err
	}
	cfg := listing.Config{
		Source: listing.SourceConfig{URI: root, Options: o.sources[pf.Scheme]},
		Output: listing.OutputConfig{Replace: o.overwrite, Resume: resumed, NoJournal: o.noJournal},
		Limits: listing.Limits{Workers: o.workers, Encoders: o.encoders, Memory: int64(o.memory), PartSize: int64(o.partSize),
			Spill: listing.SpillLimits{Off: o.noSpill, Compress: o.spillCompress, Max: int64(o.spillMax), Reserve: int64(o.spillReserve)}},
		Argv: argv,
		Warn: func(m string) { fmt.Fprintln(os.Stderr, "swath: warning:", m) },
	}
	if !o.countOnly {
		cfg.Output.Dir = o.out
	}
	stopProfile, err := profile()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if o.maxDuration > 0 {
		var stopTimer context.CancelFunc
		ctx, stopTimer = context.WithTimeoutCause(ctx, o.maxDuration, errDeadline)
		defer stopTimer()
	}
	cfg.Progress = &listing.Progress{}
	// The outcome: error.json is written while the run still owns the
	// output (Settle), and never by a run that did not own it.
	settle := func(sum listing.Report, err error) (error, outcome) {
		if err == nil && cfg.Output.Dir != "" && !sum.Outcome.Complete {
			// A committed dataset that does not cover its scope (a key
			// limit) never exits 0: a supervisor must not take it as complete.
			err = errIncomplete
		}
		if err != nil && pf.Classify != nil {
			if c := pf.Classify(err); c != "" {
				err = &classified{err, c}
			}
		}
		return err, classify(err, context.Cause(ctx), cfg.Output.Dir)
	}
	cfg.Settle = func(sum listing.Report, err error) {
		_, out := settle(sum, err)
		writeOutcome(cfg.Output.Dir, out)
	}
	stopWatch := supervise(ctx, cancel, cfg.Progress, cfg.Output.Dir, o.stall, cfg.Warn)
	sum, err := listing.Run(ctx, cfg)
	stopWatch()
	stopProfile()
	err, out := settle(sum, err)
	sum.Outcome.Class, sum.Outcome.ExitCode, sum.Outcome.Resumable, sum.Outcome.Cause = out.Class, out.ExitCode, out.Resumable, out.Cause
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
