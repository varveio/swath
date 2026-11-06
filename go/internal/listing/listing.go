// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package listing runs the pipeline: provider → order → part router →
// dataset publication.
package listing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// Config is one listing, in three groups: what to list, where the dataset
// goes, and the execution limits. Zero values take the documented defaults;
// Run resolves them once and rejects invalid values and combinations with a
// *ConfigError.
type Config struct {
	Source SourceConfig
	Output OutputConfig
	Limits Limits
	Argv   []string // the command line, recorded in the run report and the journal
	// Attempt, if set, identifies this invocation in its report.
	Attempt *Attempt
	Warn    func(string) // one-time warnings for the user (nil: dropped)
	// Progress, if set, is updated while the run goes (watchdog, progress
	// file); Progress.Owned says whether this run owns the output.
	Progress *Progress
	// Settle, if set, is called once with the run's report and error while
	// the run still owns the output directory (after its cleanup and
	// publication, before the lock is released): the place for the
	// caller's own writes into the output, such as an outcome file. A
	// resume or replacement that fails before it owns the output (its
	// source cannot be opened) takes the lock to settle when the output
	// holds an unfinished run. It is not called for an output the run
	// never owned (refused before the lock as unusable, complete or
	// empty; locked by another process; no output directory).
	Settle func(Report, error)
	// Finalize, if set, applies the caller's terminal account of the run
	// (the outcome's class, exit code, resumable, cause) to its report
	// before any account of it is written: run.json, Settle (the outcome
	// file) and the report Run returns, so all of them agree.
	Finalize func(*Report, error)
}

func (cfg *Config) finalize(r *Report, err error) {
	if cfg.Finalize != nil {
		cfg.Finalize(r, err)
	}
}

// SourceConfig is what to list.
type SourceConfig struct {
	URI string // a local directory, or scheme://...; the registered provider for the scheme lists it
	// Options is the provider's options value (registry.Factory.Flags or
	// Defaults); opaque here. nil takes the provider's defaults.
	Options any
}

// OutputConfig is where the dataset goes.
type OutputConfig struct {
	Dir       string // dataset directory; empty for a dry run
	Replace   bool   // replace a previous dataset in Dir
	Resume    bool   // continue the interrupted journaled run in Dir
	NoJournal bool   // publish parts only at the end, without a journal (no resume)
}

// Limits are the execution limits; none changes the dataset's content.
type Limits struct {
	Workers  int   // listing goroutines; default min(64, max(4, 3*GOMAXPROCS))
	Encoders int   // Parquet encoders; default GOMAXPROCS, admitted against memory and descriptors (admitEncoders)
	Memory   int64 // budget in bytes for rows held between listing and encoding; default 256 MiB
	PartSize int64 // target bytes per part; default 64 MiB (frozen for a resume)
	// PartInterval bounds the open part's age: a part open that long with
	// rows is closed although not full, so durable progress advances on a
	// slow frontier (0: DefaultPartInterval; negative: by size only, which
	// keeps part boundaries independent of timing).
	PartInterval time.Duration
	Spill        SpillLimits
}

// SpillLimits bound spill.
type SpillLimits struct {
	Off      bool  // wait on the reorder budget instead of spilling ahead-of-frontier rows
	Compress bool  // zstd level 1 on spill frames
	Max      int64 // cap on live spill bytes (0: half the free space on the spill filesystem at start; negative: none)
	Reserve  int64 // free bytes spill leaves on its filesystem (0: none; the CLI default is 1 GiB)
}

// ConfigError is an invalid configuration: a value or a combination.
type ConfigError struct{ Reason string }

func (e *ConfigError) Error() string { return "invalid configuration: " + e.Reason }

// Spill describes the run's spill (see order.Stats).
type Spill struct {
	Rows      int64 `json:"rows"`   // rows written to spill files and replayed
	Bytes     int64 `json:"bytes"`  // bytes written
	Ranges    int64 `json:"ranges"` // ranges that spilled
	PeakBytes int64 `json:"peak_bytes"`
	Refused   int64 `json:"refused"` // frames the disk budget refused (the emitter waited instead)
	// MaxBytes is the cap on live spill bytes in effect (0: none).
	MaxBytes    int64 `json:"max_bytes"`
	PeakReaders int64 `json:"peak_readers"` // spill readers open at once (at most order.MaxSpillReaders)
}

// spillMax is the live spill cap for spill under dir: the configured one,
// none for a negative one, else half the space free on dir's filesystem
// (its nearest existing ancestor) when the run starts, after the reserve.
func spillMax(l SpillLimits, dir string) int64 {
	switch {
	case l.Max > 0:
		return l.Max
	case l.Max < 0:
		return 0
	}
	for {
		if _, err := os.Stat(dir); err == nil || filepath.Dir(dir) == dir {
			break
		}
		dir = filepath.Dir(dir)
	}
	free := order.FreeBytes(dir)
	if free == order.Unlimited {
		return 0
	}
	return max(1, (free-l.Reserve)/2)
}

// Defaults.
const (
	DefaultMemory   = 256 << 20
	DefaultPartSize = 64 << 20
	// DefaultPartInterval is the default part age bound. Java rotated
	// parts every 30 s; a cut closes a part far below its target size, so
	// the default is twice that.
	DefaultPartInterval = 60 * time.Second
	// minMemory is the smallest budget accepted.
	minMemory = 256 << 10
)

// resolve validates c and fills its defaults, once.
func (c *Config) resolve() error {
	bad := func(f string, a ...any) error { return &ConfigError{fmt.Sprintf(f, a...)} }
	l, o := &c.Limits, &c.Output
	switch {
	case c.Source.URI == "":
		return bad("no source")
	case l.Workers < 0 || l.Encoders < 0 || l.Memory < 0 || l.PartSize < 0 || l.Spill.Reserve < 0:
		return bad("negative limit: %+v", *l)
	case o.Resume && o.Dir == "":
		return bad("resume needs an output directory")
	case o.Resume && o.Replace:
		return bad("resume and replace exclude each other")
	case o.Resume && o.NoJournal:
		return bad("resume needs the journal")
	case o.Replace && o.Dir == "":
		return bad("replace needs an output directory")
	}
	if l.Workers == 0 {
		l.Workers = min(64, max(4, 3*runtime.GOMAXPROCS(0)))
	}
	if l.Memory == 0 {
		l.Memory = DefaultMemory
	}
	if l.Encoders == 0 {
		l.Encoders = min(runtime.GOMAXPROCS(0), admitEncoders(l.Memory, l.Workers, fdLimit()))
	}
	if l.PartSize == 0 {
		l.PartSize = DefaultPartSize
	}
	if l.PartInterval == 0 {
		l.PartInterval = DefaultPartInterval
	}
	if l.Memory < minMemory {
		return bad("memory budget %d is below %d bytes", l.Memory, minMemory)
	}
	return nil
}

// admitEncoders is how many Parquet encoders the run's resources carry,
// never fewer than one: each holds a writer's working set
// (parquetout.EncoderBytes) in the half of memory outside the row budget,
// and one descriptor beside the spill readers, the workers' connections
// and spill writes, and a reserve (fds 0 when the limit is unknown).
func admitEncoders(memory int64, workers int, fds int64) int {
	n := (memory - memory/2) / parquetout.EncoderBytes
	if fds > 0 {
		n = min(n, fds-int64(order.MaxSpillReaders)-2*int64(workers)-fdReserve)
	}
	return int(max(1, n))
}

// fdReserve is the descriptors kept for the journal, lock, output and
// spill directories, standard streams and the runtime.
const fdReserve = 32

// Run lists cfg.Source.URI and, unless cfg.Output.Dir is empty, publishes the dataset.
// On any error or cancellation nothing is published and unpublished parts
// are removed (a journaled run keeps them for -resume); the error is the
// cause (ctx.Err() on cancellation).
func Run(ctx context.Context, cfg Config) (Report, error) {
	rep, err := run(ctx, cfg)
	cfg.finalize(&rep, err)
	return rep, err
}

func run(ctx context.Context, cfg Config) (Report, error) {
	start := time.Now()
	sum := Report{ReportVersion: ReportVersion, Build: build(), Attempt: cfg.Attempt, Timings: Timings{StartedAt: start.UTC()},
		Outcome: Outcome{Status: "failed"}}
	asked := cfg.Limits.Encoders
	if err := cfg.resolve(); err != nil {
		return sum.failed(err)
	}
	if n := admitEncoders(cfg.Limits.Memory, cfg.Limits.Workers, fdLimit()); asked > n && cfg.Warn != nil {
		cfg.Warn(fmt.Sprintf("%d encoders need about %d MiB of writer memory outside the row budget and %d descriptors; --memory %d MiB and the descriptor limit carry %d",
			asked, int64(asked)*parquetout.EncoderBytes>>20, asked, cfg.Limits.Memory>>20, n))
	}
	argv := cfg.Argv
	if argv == nil {
		argv = []string{}
	}
	sum.Invocation = Invocation{Argv: argv, Source: cfg.Source.URI, Out: cfg.Output.Dir, Resume: cfg.Output.Resume, Workers: cfg.Limits.Workers,
		Encoders: cfg.Limits.Encoders, MemoryBytes: cfg.Limits.Memory, PartSizeBytes: cfg.Limits.PartSize}
	sum.Resources.RowBudgetBytes = cfg.Limits.Memory / 2
	f, err := registry.Lookup(cfg.Source.URI)
	if err != nil {
		return sum.failed(err)
	}
	opts := cfg.Source.Options
	if opts == nil {
		opts = f.Defaults()
	}
	warn := cfg.Warn
	if warn == nil {
		warn = func(string) {}
	}
	// The unfinished run this invocation means to continue or replace, as
	// it is now: an early failure settles only that run (settleUnowned).
	seen := ""
	if cfg.Output.Dir != "" && (cfg.Output.Resume || cfg.Output.Replace) {
		seen = journal.Witness(cfg.Output.Dir)
	}
	src, err := f.Open(ctx, registry.Request{Root: cfg.Source.URI, Options: opts, Workers: cfg.Limits.Workers, Warn: warn})
	if err != nil {
		// Before ownership (credentials, say): the outcome of the run this
		// invocation would have continued or replaced is recorded too, so
		// an earlier one is never left standing as current.
		rep, err := sum.failed(err)
		cfg.finalize(&rep, err)
		rep.Outcome.Unrecorded = settleUnowned(cfg, seen, rep, err)
		return rep, err
	}
	s, loc := src.Schema(), src.Locator()
	sum.Invocation.Source, sum.Invocation.SchemaID = loc.Root, s.ID
	jr, err := openJournal(cfg, src, identity(src, s, cfg))
	if err != nil {
		// A refusal before the lock leaves the output as it was (jr nil);
		// a failure under it (a damaged journal, an identity mismatch, a
		// failed recovery) is settled before the lock is released, except
		// a dataset completed or a run gone by the time the lock was held:
		// another process's outcome stands.
		rep, err := sum.failed(err)
		cfg.finalize(&rep, err)
		if errors.Is(err, ErrComplete) || errors.Is(err, ErrNothingToResume) {
			jr.release(cfg)
		} else {
			jr.close(cfg, rep, err)
		}
		if cfg.Settle != nil {
			rep.Outcome.Unrecorded = unowned(jr, err)
		}
		return rep, err
	}
	rep, err := owned(ctx, cfg, sum, src, s, jr)
	cfg.finalize(&rep, err)
	jr.close(cfg, rep, err)
	return rep, err
}

// owned is the run while it owns its output (jr is nil without one).
func owned(ctx context.Context, cfg Config, sum Report, src registry.Source, s schema.Info, jr *runJournal) (Report, error) {
	if ex, ok := src.(source.Excluder); ok && cfg.Output.Dir != "" {
		if err := ex.Exclude(cfg.Output.Dir); err != nil {
			return sum.failed(err)
		}
	}
	if last := jr.last(); last != nil {
		if err := jr.src.Restart(string(last.Part.MaxKey)); err != nil {
			return sum.failed(err)
		}
	}
	sum.Output.PartsRetained, sum.Output.RowsRetained = jr.resumedParts(), jr.resumedRows()
	return pipeline(ctx, cfg, sum, src, s, jr)
}

// identity is the run's resume identity: what the pipeline freezes, and
// what the source names (source.Identified).
func identity(src registry.Source, s schema.Info, cfg Config) journal.Identity {
	id := journal.Identity{Tool: "swath-go", Format: dataset.FormatVersion, SchemaID: s.ID, KeyCap: s.MaxKeyBytes, Writer: parquetout.WriterID,
		PartSize: cfg.Limits.PartSize, Provider: src.Locator().Kind}
	if idf, ok := src.(source.Identified); ok {
		sid := idf.Identity()
		id.Provider = sid.Provider
		for _, f := range sid.Fields {
			id.Source = append(id.Source, journal.Field{Name: f.Name, Value: f.Value, Canonical: f.Canonical})
		}
	}
	return id
}

// manifestMeta is the manifest's account of the source, the scope and the
// coverage, from the source's locator and report.
func manifestMeta(s schema.Info, loc source.Locator, rep *source.Report, started, listed time.Time) dataset.Meta {
	m := dataset.Meta{SchemaID: s.ID, Source: dataset.Source{Kind: loc.Kind, URI: loc.Root, Container: loc.Container},
		Scope: dataset.Scope{Kind: "whole"}}
	m.Coverage = dataset.Coverage{ListingStartedAt: started.UTC(), ListingFinishedAt: listed.UTC()}
	if rep == nil {
		m.Coverage.StopReason = "unreported" // a source without a report claims nothing
		return m
	}
	c := rep.Coverage
	if c.Prefix != "" {
		m.Scope = dataset.Scope{Kind: "prefix", Prefix: c.Prefix}
	}
	m.Coverage.Complete, m.Coverage.StopReason, m.Coverage.SkippedEntries = c.Complete, c.StopReason, c.Skipped
	m.Coverage.Exclusions, m.Coverage.Filters = c.Exclusions, c.Filters
	return m
}

// pipeline runs provider → order → router → publish. p is src, or Empty
// when a resume has nothing left to list.
func pipeline(ctx context.Context, cfg Config, sum Report, src registry.Source, s schema.Info, jr *runJournal) (Report, error) {
	var published struct {
		parts, bytes, rows atomic.Int64
	}
	// One shared cause: the first asynchronous failure (an encoder, the
	// disk, the journal) cancels listing at once, and is what Run returns.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// One byte budget for rows in flight, shared by the order stage and the
	// encoders' queues (half of -memory; the rest is headroom for pages
	// being parsed, codecs and the garbage collector).
	shared := &order.Shared{}
	// The final part is durable: listing again could only add a second
	// final part on a changed source. Publish what is recorded.
	env := registry.Env{Empty: jr.complete()}
	if pr := cfg.Progress; pr != nil {
		pr.started.Store(sum.Timings.StartedAt.UnixNano())
		listed := src.Listed
		pr.listed.Store(&listed)
		if l, ok := src.(source.Live); ok {
			answered := l.Answered
			pr.answered.Store(&answered)
		}
		if f, ok := src.(source.Faulted); ok {
			faults := f.Faults
			pr.faults.Store(&faults)
		}
		env.Ordered = &pr.ordered
	}
	if cfg.Output.Dir != "" {
		ropt := parquetout.Options{Out: cfg.Output.Dir, PartSize: cfg.Limits.PartSize, Encoders: cfg.Limits.Encoders, Account: shared.Add,
			Interval: max(0, cfg.Limits.PartInterval)}
		jr.routerOptions(&ropt)
		if pr := cfg.Progress; pr != nil {
			ropt.Encoded = &pr.encoded
		}
		if pub := ropt.Publish; pub != nil {
			// Each part published here is durable (journaled): a failed
			// run reports them (durable).
			ropt.Publish = func(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
				p, err := pub(ordinal, p, limit, rows, final)
				if err == nil {
					published.parts.Add(1)
					published.bytes.Add(p.Bytes)
					published.rows.Store(rows)
					if pr := cfg.Progress; pr != nil {
						pr.parts.Add(1)
						pr.lastPart.Store(time.Now().UnixNano())
					}
				}
				return p, err
			}
		}
		ropt.Fail = func(err error) { cancel(fmt.Errorf("encode: %w", err)) }
		env.Router = &ropt
	}
	var router parquetout.Encoder
	// durable puts the parts that stay in the output after a failure (the
	// recovered ones and those journaled by this invocation) in the
	// report; a run without a journal keeps none.
	durable := func() {
		if !jr.journaled() {
			return
		}
		sum.Output.PartsWritten, sum.Output.Bytes = int(published.parts.Load()), published.bytes.Load()
		sum.Output.RowsDurable = sum.Output.RowsRetained
		if r := published.rows.Load(); r > 0 {
			sum.Output.RowsDurable = r
		}
		if jr.state != nil {
			for _, rec := range jr.state.Records {
				sum.Output.Bytes += rec.Part.Bytes
			}
		}
	}
	fail := func(err error) (Report, error) {
		if router != nil {
			router.Abort()
			// A crash leaves the output as a kill would; a journaled run
			// keeps its temporary parts too: one may be recorded already
			// (its rename failed), and -resume decides which are.
			if !errors.Is(err, faults.ErrCrash) && !jr.journaled() {
				dataset.Cleanup(cfg.Output.Dir)
			}
		}
		durable()
		return sum.failed(err)
	}
	gauges := &order.Gauges{}
	oopt := order.Options{Workers: cfg.Limits.Workers, Budget: sum.Resources.RowBudgetBytes, Shared: shared, SpillCompress: cfg.Limits.Spill.Compress, SpillReserve: cfg.Limits.Spill.Reserve,
		After: jr.after(), Gauges: gauges}
	if pr := cfg.Progress; pr != nil {
		pr.gauges.Store(gauges)
	}
	// Spill files are disposable: a crash relists their rows.
	if cfg.Output.Dir != "" {
		spill := filepath.Join(cfg.Output.Dir, dataset.JournalDir, "spill")
		// Reclaim a crashed run's orphans (the output lock is held), with
		// spill on or off.
		if err := os.RemoveAll(spill); err != nil && cfg.Warn != nil {
			cfg.Warn(fmt.Sprintf("a previous run's spill files could not be removed: %v", err))
		}
		if !cfg.Limits.Spill.Off {
			oopt.SpillDir = spill
		}
	} else if !cfg.Limits.Spill.Off {
		oopt.SpillDir = os.TempDir()
	}
	if oopt.SpillDir != "" {
		oopt.SpillMax = spillMax(cfg.Limits.Spill, oopt.SpillDir)
	}
	env.Order = oopt
	ost, router, err := src.Run(ctx, env)
	if router == nil && cfg.Output.Dir != "" && err != nil {
		return sum.failed(err) // the router could not start: nothing was written
	}
	if cfg.Output.Dir != "" && oopt.SpillDir != "" {
		_ = os.Remove(oopt.SpillDir) // only if empty; order.Run removed its own files
		_ = os.Remove(filepath.Dir(oopt.SpillDir))
	}
	sum.Resources.MaxBufferedBytes, sum.Output.RowsListed = ost.MaxBuffered, ost.Rows
	sum.Resources.Features = map[string]bool{"spill": oopt.SpillDir != ""}
	if oopt.SpillDir != "" {
		sum.Resources.Spill = &Spill{Rows: ost.SpillRows, Bytes: ost.SpillBytes, Ranges: ost.SpillFiles, PeakBytes: ost.SpillPeak, Refused: ost.SpillRefused,
			MaxBytes: oopt.SpillMax, PeakReaders: ost.PeakReaders}
	}
	sum.Resources.PeakNodes, sum.Resources.PeakMarkers = ost.PeakNodes, ost.PeakMarkers
	sum.Resources.FrontierWaitNs, sum.Resources.BudgetWaitNs = int64(ost.FrontierWait), int64(ost.BudgetWait)
	if router != nil {
		w := int64(router.EncoderWait())
		sum.Resources.EncoderWaitNs = &w
		sum.Resources.PartTimeCuts = router.TimeCuts()
	}
	listed := time.Now()
	sum.Timings.ListingFinishedAt, sum.Timings.ListingMs = listed.UTC(), listed.Sub(sum.Timings.StartedAt).Milliseconds()
	if r, ok := src.(source.Reporter); ok {
		sum.setSource(r.Report())
	}
	want := src.Listed()
	if err != nil {
		return fail(err)
	}
	// Reconciliation, hop by hop, before anything is published: rows the
	// provider emitted, the order stage received and handed on (equal by
	// order.Run's own check), and the router routed; the router's gate
	// then checks rows written against rows routed. A row lost anywhere
	// upstream of the writer shows as a mismatch, never as a smaller
	// dataset. (A resume reconciles its own attempt; retained parts were
	// counted by the attempt that wrote them.)
	routed := ost.Rows
	if router != nil {
		routed = router.Routed()
	}
	if err := reconcile(want, ost.Received, ost.Rows, routed); err != nil {
		return fail(err)
	}
	if router == nil {
		sum.Outcome.Status = "listed"
		sum.finish()
		return sum, nil
	}
	parts, err := router.Close()
	if err != nil {
		return fail(fmt.Errorf("encode: %w", err))
	}
	if err := jr.unresolved(); err != nil {
		return fail(err)
	}
	sum.Output.PartsWritten = len(parts) - sum.Output.PartsRetained
	for _, part := range parts {
		sum.Output.Bytes += part.Bytes
	}
	if err := dataset.Publish(cfg.Output.Dir, manifestMeta(s, src.Locator(), sum.report, jr.listingStarted(sum.Timings.StartedAt), listed), parts); err != nil {
		if !errors.Is(err, faults.ErrCrash) && !jr.journaled() {
			dataset.Cleanup(cfg.Output.Dir)
		}
		durable()
		return sum.failed(fmt.Errorf("publish: %w", err))
	}
	if pr := cfg.Progress; pr != nil && !jr.journaled() {
		pr.parts.Add(int64(sum.Output.PartsWritten))
	}
	// Committed: the report follows and can no longer revoke the dataset.
	sum.Outcome.Status, sum.Outcome.Class = "committed", "completed"
	sum.Output.RowsCommitted = sum.Output.RowsRetained + sum.Output.RowsListed
	sum.Output.RowsDurable = sum.Output.RowsCommitted
	sum.finish()
	cfg.finalize(&sum, nil)
	if err := dataset.WriteReport(cfg.Output.Dir, sum); err != nil && cfg.Warn != nil {
		cfg.Warn(err.Error())
	}
	return sum, nil
}

// reconcile compares the row counts of one attempt at each hop.
func reconcile(listed, received, ordered, routed int64) error {
	if listed == received && received == ordered && ordered == routed {
		return nil
	}
	return &dataset.InvariantError{Reason: fmt.Sprintf("row count mismatch: provider emitted %d, order stage received %d and handed on %d, router routed %d",
		listed, received, ordered, routed)}
}

// failed records err as the outcome and returns the report with it.
func (r Report) failed(err error) (Report, error) {
	r.Outcome.Status, r.Outcome.Error = "failed", err.Error()
	r.finish()
	return r, err
}
