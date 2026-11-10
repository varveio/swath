// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package listing runs the pipeline: provider → order → part router →
// dataset publication.
package listing

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/filter"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/textout"
	"github.com/varveio/swath/go/internal/totals"
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
	// Filter, if set, drops the rows it does not accept (package filter):
	// part of the resume identity, recorded in the manifest's
	// coverage.filters.
	Filter *filter.Spec
	// Totals, if set, sums a count's rows per key prefix (swath du); the
	// report's output.totals is its account. It needs a dry run (no
	// dataset, no text).
	Totals *totals.Tally
	// Deliver, if set, writes a count's result once every row is counted
	// (swath du's per-prefix totals), before the report's end is stamped:
	// its time is the run's, and its error, or a stop that comes before or
	// while it writes, fails the run as a listing's would. It needs a dry
	// run.
	Deliver func(ctx context.Context) error
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
	// file) and the report Run returns, so all of them agree. It is called
	// exactly once per run, on its terminal report: at the commit (before
	// run.json), at a failure (before Settle), or as Run returns for a run
	// that stopped before either.
	Finalize func(*Report, error)
	// Commit, if set, runs step, the commit: the manifest's durable
	// installation and, once it is installed, the committed report's
	// Finalize, so the caller can order the commit against its other
	// terminal accounts (cmd/swath's forced exit). An error returned
	// without running step means not committed. nil runs step.
	Commit func(step func() error) error
}

// finalize applies Finalize to r once: the first call on a terminal report
// accounts it, and later ones on the same report (its copies included) are
// no-ops, so every path may finalize before its first write without
// counting on the caller's idempotence.
func (cfg *Config) finalize(r *Report, err error) {
	if r.finalized {
		return
	}
	r.finalized = true
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
	// SeedFrom is a prior to start the range engine from instead of its
	// delimiter seed (--seed-from): a committed dataset directory of the
	// same source and scope, or a cut file. "" for none.
	SeedFrom string
	// Narrowing is how the includes narrowed URI (filter.Narrowed, or why
	// not; "" without includes): the report's invocation.narrowing.
	Narrowing string
}

// OutputConfig is where the dataset goes.
type OutputConfig struct {
	Dir       string // dataset directory; empty for a dry run
	Replace   bool   // replace a previous dataset in Dir
	Resume    bool   // continue the interrupted journaled run in Dir
	NoJournal bool   // publish parts only at the end, without a journal (no resume)
	// Text, if set, writes the rows as text (stdout or one file) instead of
	// a dataset; Dir must be empty. Such a run keeps no journal.
	Text *textout.Config
}

// Limits are the execution limits; none changes the dataset's content.
type Limits struct {
	Workers  int // listing goroutines; default DefaultWorkers
	Encoders int // Parquet encoders; default GOMAXPROCS, admitted against the row budget and descriptors (admitEncoders)
	// RowBudget bounds the bytes of rows and scheduler metadata held
	// between listing and encoding (reorder storage and the encoders'
	// queues); 0 is the default, 1/16 of the least detected memory limit
	// (physical, cgroup, GOMEMLIMIT), at least minRowBudget and at most
	// 1 GiB (DefaultRowBudget when none is detected: rowBudgetFor). It is
	// not a cap on resident memory.
	RowBudget int64
	PartSize  int64 // target bytes per part; default 64 MiB (frozen for a resume)
	// PartInterval bounds the open part's age: a part open that long with
	// rows is closed although not full, so durable progress advances on a
	// slow frontier. Zero or negative: by size only, which keeps part
	// boundaries independent of timing; the command's default is
	// DefaultPartInterval, resolved at its flag, not here.
	PartInterval time.Duration
	// PartMaxRows, if positive, closes a part at that many rows as well
	// (frozen for a resume: boundaries depend on it).
	PartMaxRows int64
	Spill       SpillLimits
	// maxEncoders is the encoders admitted once listing has ended
	// (admitAfterListing): the default's, or Encoders when given
	// (encodersGiven).
	maxEncoders   int
	encodersGiven bool
	// encoderBytes is the working set admitted per Parquet encoder, the
	// source's schema's (encoderPrice).
	encoderBytes int64
	// rowBudgetWhy is how RowBudget was chosen (rowBudgetFor).
	rowBudgetWhy RowBudget
}

// SpillLimits bound spill.
type SpillLimits struct {
	Off   bool // wait on the reorder budget instead of spilling ahead-of-frontier rows
	Plain bool // plain spill frames instead of delta-coded ones (a comparison arm)
	// Uncompressed writes spill frames without zstd (level 1 by default: it
	// still finds the ETag and key-suffix bytes delta coding leaves).
	Uncompressed bool
	Max          int64 // cap on live spill bytes (0: half the free space on the spill filesystem at start, after Reserve; negative: none)
	Reserve      int64 // free bytes spill leaves on its filesystem (0: derived, spillReserve; negative: none)
}

// ConfigError is an invalid configuration: a value or a combination.
type ConfigError struct{ Reason string }

func (e *ConfigError) Error() string { return "invalid configuration: " + e.Reason }

// Spill describes the run's spill (see order.Stats).
type Spill struct {
	Rows  int64 `json:"rows"`  // rows written to spill files and replayed
	Bytes int64 `json:"bytes"` // bytes written
	// LogicalBytes is the spilled rows' logical size (key and column
	// values, as the part router counts them): Bytes against it is what
	// the frame coding saved. Format names the frames: "delta" or "plain",
	// "+zstd" unless --no-spill-compress.
	LogicalBytes int64  `json:"logical_bytes"`
	Format       string `json:"format"`
	// DecodeRefused counts frames refused at read for decoding past their
	// storage bound (a malformed frame; the run fails).
	DecodeRefused int64 `json:"decode_refused"`
	Ranges        int64 `json:"ranges"` // ranges that spilled
	// Frames is the row frames written (not a file's header, capture or
	// trailer frames); Opens the spill file opens attempted (order's
	// spiller.opens): one per frame written and per frame read at an
	// offset, beside the readers, trailer reads, seals and certificate
	// replays. Against Ranges and Frames, the per-file and per-frame
	// overhead of many small spills.
	Frames    int64 `json:"frames"`
	Opens     int64 `json:"opens"`
	PeakBytes int64 `json:"peak_bytes"`
	Refused   int64 `json:"refused"` // frames the disk budget refused (the emitter waited instead)
	// MaxBytes is the cap on live spill bytes in effect (0: none);
	// ReserveBytes the free space spill left on its filesystem (0: none).
	MaxBytes     int64 `json:"max_bytes"`
	ReserveBytes int64 `json:"reserve_bytes"`
	PeakReaders  int64 `json:"peak_readers"` // spill readers open at once (at most order.MaxSpillReaders)
	// Durable is the durable spill's account (nil when it was off: no
	// journal, a provider without fences, a spill cap
	// below 16 MiB); DurableOff says why it was off when it could have run.
	Durable    *order.DurableReport `json:"durable,omitempty"`
	DurableOff string               `json:"durable_off,omitempty"`
	// Dir is the spill directory; FSType the filesystem it is on (Linux:
	// tmpfs, ext4, xfs, ...; empty where it is not read). A spill on tmpfs
	// is held in memory, outside peak_rss_bytes.
	Dir    string `json:"dir"`
	FSType string `json:"fs_type,omitempty"`
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
	free := order.FreeBytes(existing(dir))
	if free == order.Unlimited {
		return 0
	}
	return max(1, (free-l.Reserve)/2)
}

// existing is dir or its nearest existing ancestor: the filesystem a
// directory not yet created will be on.
func existing(dir string) string {
	for {
		if _, err := os.Stat(dir); err == nil || filepath.Dir(dir) == dir {
			return dir
		}
		dir = filepath.Dir(dir)
	}
}

// spillReserve is the free space spill leaves on its filesystem when none
// is configured: room for every encoder's open part at full size, since
// parts are written on the same filesystem and a full disk fails the run
// where a refused spill frame only waits; at least minSpillReserve, a
// margin for the journal, the manifest and whatever else writes there.
// The spill cap (--spill-budget) and this reserve protect different things:
// the cap bounds spill's own use, the reserve bounds what spill may take
// from the parts and from other processes, whose use the cap cannot see.
// A product past int64 saturates (no filesystem holds it: every spill
// frame is refused) instead of wrapping to a small or negative reserve.
func spillReserve(encoders int, partSize int64) int64 {
	if encoders > 0 && partSize > math.MaxInt64/int64(encoders) {
		return math.MaxInt64
	}
	return max(minSpillReserve, int64(encoders)*partSize)
}

// Defaults.
const (
	// minSpillReserve is spillReserve's floor.
	minSpillReserve = 1 << 30
	// DefaultRowBudget is the row budget when the host's memory is unknown
	// (rowBudgetFor), 128 MiB: enough for every worker's pages at the
	// default 64 workers to queue behind a slow frontier, and the
	// allowance that admits 4 encoders of the object stores' schemas
	// (admitEncoders).
	DefaultRowBudget = 128 << 20
	// DefaultWorkers is the default listing concurrency, whatever the CPU
	// count: Java's proven default (--concurrency 64). Listing waits on
	// the service, not the CPU.
	DefaultWorkers  = 64
	DefaultPartSize = 64 << 20
	// DefaultPartInterval is the default part age bound: Java's rotation
	// interval (30 s), its proven value.
	DefaultPartInterval = 30 * time.Second
	// minRowBudget is the smallest row budget accepted: below it a publish
	// batch alone would fill the budget.
	minRowBudget = 128 << 10
)

// resolve validates c and fills its defaults, once.
func (c *Config) resolve() error {
	bad := func(f string, a ...any) error { return &ConfigError{fmt.Sprintf(f, a...)} }
	l, o := &c.Limits, &c.Output
	switch {
	case c.Source.URI == "":
		return bad("no source")
	case l.Workers < 0 || l.Encoders < 0 || l.RowBudget < 0 || l.PartSize < 0:
		return bad("negative limit: %+v", *l)
	case o.Resume && o.Dir == "":
		return bad("resume needs an output directory")
	case o.Resume && o.Replace:
		return bad("resume and replace exclude each other")
	case o.Resume && o.NoJournal:
		return bad("resume needs the journal")
	case o.Replace && o.Dir == "":
		return bad("replace needs an output directory")
	case o.Text != nil && (o.Dir != "" || o.Resume):
		return bad("text output excludes a dataset directory and resume")
	case (c.Totals != nil || c.Deliver != nil) && !dryRun(*c):
		return bad("per-prefix totals need a count (no dataset, no text)")
	}
	if l.Workers == 0 {
		l.Workers = defaultWorkers(c.Source.URI)
	}
	l.RowBudget, l.rowBudgetWhy = rowBudgetFor(l.RowBudget, readMemoryLimits())
	l.maxEncoders, l.encodersGiven = l.Encoders, l.Encoders > 0
	l.encoderBytes = encoderPrice(c.Source.URI)
	if l.Encoders == 0 {
		l.Encoders = min(runtime.GOMAXPROCS(0), admit(*c))
		if c.Output.Text == nil { // Parquet writers only: a text output's jobs are not raised
			l.maxEncoders = max(l.Encoders, min(runtime.GOMAXPROCS(0), admitAfterListing(l.RowBudget, l.encoderBytes, l.Workers, fdLimit())))
		}
	}
	if l.PartSize == 0 {
		l.PartSize = DefaultPartSize
	}
	switch {
	case l.Spill.Reserve == 0:
		l.Spill.Reserve = spillReserve(l.Encoders, l.PartSize)
	case l.Spill.Reserve < 0:
		l.Spill.Reserve = 0
	}
	if l.RowBudget < minRowBudget {
		return bad("row budget %d is below %d bytes", l.RowBudget, minRowBudget)
	}
	return nil
}

// defaultWorkers is the provider of root's default --workers
// (registry.Factory.Workers), DefaultWorkers for a provider that states none.
func defaultWorkers(root string) int {
	if f, err := registry.Lookup(root); err == nil && f.Workers != nil {
		return f.Workers()
	}
	return DefaultWorkers
}

// encoderPrice is the working set admitted per Parquet encoder of the
// provider of root (registry.Factory.EncoderBytes: its schema's widest
// pages and the shared codec's state, parquetout.EncoderBytes), known
// before the source is opened; parquetout.DefaultEncoderBytes for a
// provider that states none.
func encoderPrice(root string) int64 {
	if f, err := registry.Lookup(root); err == nil && f.EncoderBytes > 0 {
		return f.EncoderBytes
	}
	return parquetout.DefaultEncoderBytes
}

// admitEncoders is how many Parquet encoders the run's resources carry,
// never fewer than one. Encoder memory is admitted apart from the row
// budget but sized by it: per encoder bytes (encoderPrice: one writer and
// one codec state) each within an allowance equal to the row budget, so a host given a
// larger budget for rows also runs more writers to drain them; and one
// descriptor each beside the spill readers, the workers' connections and
// spill writes, and a reserve (fds 0 when the limit is unknown).
func admitEncoders(rowBudget, per int64, workers int, fds int64) int {
	n := rowBudget / per
	if fds > 0 {
		n = min(n, fds-int64(order.MaxSpillReaders)-2*int64(workers)-fdReserve)
	}
	return int(max(1, n))
}

// admitAfterListing is how many encoders the run carries once the
// provider has finished listing (order.Options.ListingDone): no page is
// in flight any more, so the memory the workers' pages held beside the
// row budget (listingPageBytes each) goes to writers instead, on top of
// the allowance equal to the row budget; descriptors as admitEncoders
// (the workers' idle connections stay open). Each encoder costs per
// (encoderPrice). 128 MiB and 64 workers carry 6 of the object stores'
// schemas, 256 workers 12.
func admitAfterListing(rowBudget, per int64, workers int, fds int64) int {
	return admitEncoders(rowBudget+int64(workers)*listingPageBytes, per, workers, fds)
}

// segmentWorkers caps the workers writing the tail as parallel parts
// (order.Options.SegmentWorkers): each feeds one part's writer, so no more
// than the encoders admitted after listing (their working set is that
// admission's, encoderPrice each), and the order stage keeps
// them to one per spare processor. Each reads one spill frame at a time on
// its own descriptor: the 16 spill descriptors reserved at admission
// (order.MaxSpillReaders), plus what the limit leaves beside the workers'
// connections, the encoders' part files and the reserve (fds 0: unknown,
// no cap). Beside its writer a worker holds one decoded frame, one batch
// and a two-batch queue: about 3 × 4,096 rows.
func segmentWorkers(encoders, workers int, fds int64) int {
	n := int64(encoders)
	if fds > 0 {
		free := fds - int64(order.MaxSpillReaders) - 2*int64(workers) - fdReserve - int64(encoders)
		n = min(n, int64(order.MaxSpillReaders)+max(0, free))
	}
	return int(max(1, n))
}

// listingPageBytes is what one worker's page in flight holds outside the
// row budget: a 1,000-key S3 response (about 300 KB of XML) and the arena
// its strings are copied into, rounded up.
const listingPageBytes = 1 << 20

// admitTextJobs is how many zstd jobs a text output's compressor runs at
// once, never fewer than one: each within textout.JobBytes of an allowance
// equal to the row budget, as Parquet encoders are admitted (admitEncoders),
// but with the compressor's own working set and no descriptor (the jobs
// write one file).
func admitTextJobs(rowBudget int64) int {
	return int(max(1, rowBudget/textout.JobBytes))
}

// admit is the encoders (Parquet writers, or a text output's compression
// jobs) c's resources carry.
func admit(c Config) int {
	if c.Output.Text != nil {
		return admitTextJobs(c.Limits.RowBudget)
	}
	return admitEncoders(c.Limits.RowBudget, c.Limits.encoderBytes, c.Limits.Workers, fdLimit())
}

// textJobs is the compression jobs of a text output and why that many
// (TextCompressor), from the encoders asked (0: none) and the resolved
// limits; nil when the output is uncompressed.
func textJobs(c Config, asked int) *TextCompressor {
	switch c.Output.Text.Compression {
	case textout.Zstd:
	case textout.Gzip:
		return &TextCompressor{Jobs: 1, JobsReason: JobsGzip}
	default:
		return nil
	}
	t := &TextCompressor{Jobs: c.Limits.Encoders, JobsReason: JobsCores}
	switch {
	case asked > 0:
		t.JobsReason = JobsAsked
	case admitTextJobs(c.Limits.RowBudget) < runtime.GOMAXPROCS(0):
		t.JobsReason = JobsRowBudget
	}
	return t
}

// fdReserve is the descriptors kept for the journal, lock, output and
// spill directories, standard streams and the runtime.
const fdReserve = 32

// Run lists cfg.Source.URI and, unless cfg.Output.Dir is empty, publishes the dataset.
// On any error or cancellation nothing is published and unpublished parts
// are removed (a journaled run keeps them for -resume); the error is the
// cause (ctx.Err() on cancellation).
func Run(ctx context.Context, cfg Config) (Report, error) {
	cfg.Argv = registry.RedactArgv(cfg.Argv)
	rep, err := run(ctx, cfg)
	cfg.finalize(&rep, err)
	return rep, err
}

func run(ctx context.Context, cfg Config) (Report, error) {
	start := time.Now()
	sum := Report{ReportVersion: ReportVersion, Build: buildinfo.Read(), Attempt: cfg.Attempt, Timings: Timings{StartedAt: start.UTC()},
		Outcome: Outcome{Status: StatusFailed}}
	asked := cfg.Limits.Encoders
	if err := cfg.resolve(); err != nil {
		return sum.failed(err)
	}
	switch n := admit(cfg); {
	case asked <= n || cfg.Warn == nil:
	case cfg.Output.Text != nil:
		cfg.Warn(fmt.Sprintf("%d encoders (compression jobs) need about %d MiB of compressor memory beside the row budget; --sort-buffer %d MiB carries %d",
			asked, int64(asked)*textout.JobBytes>>20, cfg.Limits.RowBudget>>20, n))
	default:
		cfg.Warn(fmt.Sprintf("%d encoders need about %d MiB of writer memory beside the row budget and %d descriptors; --sort-buffer %d MiB and the descriptor limit carry %d",
			asked, int64(asked)*cfg.Limits.encoderBytes>>20, asked, cfg.Limits.RowBudget>>20, n))
	}
	if t := cfg.Output.Text; t != nil {
		sum.Output.Text = &TextOutput{Format: t.Format, Destination: t.Path, Compression: t.Compression, Compressor: textJobs(cfg, asked)}
		if t.Path == "" {
			sum.Output.Text.Destination = "-"
		}
		if t.Compression == "" {
			sum.Output.Text.Compression = textout.None
		}
	}
	argv := cfg.Argv
	if argv == nil {
		argv = []string{}
	}
	sum.Invocation = Invocation{Argv: argv, Source: registry.Redact(cfg.Source.URI), Narrowing: cfg.Source.Narrowing, Out: cfg.Output.Dir, Resume: cfg.Output.Resume, Workers: cfg.Limits.Workers,
		Encoders: cfg.Limits.Encoders, RowBudgetBytes: cfg.Limits.RowBudget, RowBudget: cfg.Limits.rowBudgetWhy, PartSizeBytes: cfg.Limits.PartSize}
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
	if cfg.Source.SeedFrom != "" {
		pr, err := seedFrom(cfg, src)
		switch {
		case err == nil:
			sum.Invocation.Prior = pr
		case cfg.Output.Resume:
			// A prior shapes the tiling only: a resume whose prior is gone
			// or changed lists as a run without one.
			warn(fmt.Sprintf("%v: resuming without it, as a run without --seed-from", err))
		default:
			rep, err := sum.failed(err)
			cfg.finalize(&rep, err)
			rep.Outcome.Unrecorded = settleUnowned(cfg, seen, rep, err)
			return rep, err
		}
	}
	id := identity(src, s, cfg)
	sum.ArgsHash, sum.src = id.Hash(), src
	jr, err := openJournal(ctx, cfg, src, id)
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
	id := journal.Identity{Tool: buildinfo.Tool, Format: int(seekparquet.FormatVersion), SchemaID: s.ID, KeyCap: s.MaxKeyBytes, Writer: parquetout.Recipe,
		PartSize: cfg.Limits.PartSize, PartMaxRows: cfg.Limits.PartMaxRows, Provider: src.Locator().Kind, Filters: cfg.Filter.Descriptions()}
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
	m := dataset.Meta{SchemaID: s.ID, Groups: s.Groups, Source: dataset.Source{Kind: loc.Kind, URI: loc.Root, Container: loc.Container},
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
// when a resume has nothing left to list. Its phases: wire the
// environment (progress, router, order stage, filters, text output), run
// the source, account the attempt, reconcile the hops, then commit.
func pipeline(ctx context.Context, cfg Config, sum Report, src registry.Source, s schema.Info, jr *runJournal) (Report, error) {
	// One shared cause: the first asynchronous failure (an encoder, the
	// disk, the journal) cancels listing at once, and is what Run returns.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if cfg.Progress == nil {
		cfg.Progress = &Progress{} // the history reads it
	}
	// One byte budget for rows in flight, shared by the order stage and the
	// encoders' queues (--sort-buffer; pages being parsed, codecs and the
	// garbage collector are outside it).
	shared := &order.Shared{}
	// The final part is durable: listing again could only add a second
	// final part on a changed source. Publish what is recorded.
	env := registry.Env{Empty: jr.complete()}
	watchSource(cfg.Progress, &sum, src, &env)
	if cfg.Output.Dir != "" {
		ropt, err := routerOptions(cfg, jr, shared, cancel)
		if err != nil {
			return sum.failed(err)
		}
		env.Router = ropt
	}
	// A dry run counts: nothing consumes its rows in order, so it holds
	// no reorder storage and never spills (order.Count).
	env.Count = dryRun(cfg)
	a := &attempt{cfg: cfg, jr: jr, sum: sum}
	oopt, scratch, err := orderOptions(cfg, jr, sum.Invocation.RowBudgetBytes, shared)
	if err != nil {
		return a.sum.failed(err)
	}
	if scratch != "" {
		// This attempt's own directory (textScratch): after the order
		// stage has removed its spill files, after success or failure.
		defer os.RemoveAll(scratch)
	}
	// The cache is closed however the attempt ends, a failed Start
	// included (it holds the ledger's descriptor; closing writes nothing).
	defer jr.closeCache(false)
	if err := a.startCache(&oopt, src, cancel); err != nil {
		return a.sum.failed(err)
	}
	env.Order = oopt
	var dropped atomic.Int64
	env.Filter, env.Filtered = cfg.Filter, &dropped
	var text *textout.Out
	if cfg.Output.Text != nil {
		tc := *cfg.Output.Text
		if c := sum.Output.Text.Compressor; c != nil {
			tc.Jobs = c.Jobs
		}
		if tc.Path != "" {
			// A file's rows are formatted by as many goroutines as the
			// encoders admitted, their queued batches charged to the
			// shared budget; a stream's by the order stage, flushed per
			// batch to its reader.
			tc.Formatters, tc.Account = cfg.Limits.Encoders, shared.Add
			// A formatters' write failure stops the listing at once,
			// with its cause, as an encoder's does (routerOptions).
			tc.Fail = func(err error) { cancel(fmt.Errorf("text output: %w", err)) }
		}
		t, err := textout.Open(tc)
		if err != nil {
			return a.sum.failed(err)
		}
		text, env.Text = t, t
	}
	sizes := &registry.Sizes{}
	env.Sizes = sizes
	versions := &registry.VersionCounts{}
	env.Versions = versions
	env.Totals = cfg.Totals
	a.retained.Store(a.sum.Output.RowsRetained)
	a.sum.sampler = startHistory(a.sample, historyDisk(cfg, oopt))
	defer a.sum.sampler.finish()
	heap := sampleHeap()
	ost, router, err := src.Run(ctx, env)
	a.sum.Resources.PeakHeapLiveBytes = heap()
	a.router = router
	a.sum.Output.RowsFetched = src.Listed()
	if sizes.Measured {
		b := sizes.Bytes.Load()
		a.sum.Output.ObjectBytes = &b
	}
	if cfg.Totals != nil {
		st := cfg.Totals.Stats()
		a.sum.Output.Totals = &st
	}
	if sd, ok := src.(source.SeedTimed); ok {
		if d, ok := sd.SeedTime(); ok {
			ms := d.Milliseconds()
			a.sum.Timings.SeedMs = &ms
		}
	}
	if text != nil {
		err = a.settleText(text, err)
	}
	if router == nil && cfg.Output.Dir != "" && err != nil {
		a.closeCache(false)
		return a.sum.failed(err) // the router could not start: nothing was written
	}
	if cfg.Output.Dir != "" && oopt.SpillDir != "" && jr.verify() == nil {
		_ = os.Remove(oopt.SpillDir) // only if empty; order.Run removed its own files
		_ = os.Remove(filepath.Dir(oopt.SpillDir))
	}
	a.account(ost, oopt, dropped.Load())
	if versions.Enabled {
		n, live, distinct := versions.Versions.Load(), versions.Live.Load(), ost.DistinctKeys
		a.sum.Output.Versions, a.sum.Output.LiveObjects, a.sum.Output.DistinctKeys = &n, &live, &distinct
		if versions.HasDeleteMarkers {
			markers := versions.DeleteMarkers.Load()
			a.sum.Output.DeleteMarkers = &markers
		} else {
			noncurrent := versions.Noncurrent.Load()
			a.sum.Output.Noncurrent = &noncurrent
		}
	}
	listed := time.Now()
	a.sum.Timings.ListingFinishedAt, a.sum.Timings.ListingMs = listed.UTC(), listed.Sub(a.sum.Timings.StartedAt).Milliseconds()
	if r, ok := src.(source.Reporter); ok {
		a.sum.setSource(r.Report())
	}
	// Rows the filters dropped were listed but never reached the order
	// stage.
	a.sum.Output.RowsFiltered = dropped.Load()
	// Replayed rows were listed by an earlier attempt: they reach the order
	// stage without the provider (live_emitted - live_filtered +
	// replayed_above_K = received).
	want := src.Listed() - a.sum.Output.RowsFiltered + ost.ReplayedRows
	if err != nil {
		return a.fail(err)
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
		return a.fail(err)
	}
	if router == nil {
		if cfg.Deliver != nil {
			err := context.Cause(ctx)
			if err == nil {
				err = cfg.Deliver(ctx)
			}
			if err == nil {
				err = context.Cause(ctx) // a stop while it wrote: the result may be cut short
			}
			if err != nil {
				return a.fail(err)
			}
		}
		a.sum.Outcome.Status = StatusListed
		if text != nil {
			a.sum.Outcome.Status = StatusWritten
		}
		a.sum.finish()
		return a.sum, nil
	}
	return a.commit(s, src, listed)
}

// sample is the run's progress now, for the history.
func (a *attempt) sample() Sample {
	p := a.cfg.Progress.Snapshot()
	v := Sample{RowsFetched: p.Rows, RowsEmitted: p.Ordered, RowsDurable: a.retained.Load(),
		HTTPAttempts: p.Requests, Inflight: p.Inflight, Limit: p.Limit}
	if r := a.jr.ackedRows(); r > 0 {
		v.RowsDurable = r
	}
	if c := a.committed.Load(); c > 0 {
		v.RowsDurable = c
	}
	if p.Order != nil {
		v.SpillBytes = p.Order.SpillLiveBytes
	}
	return v
}

// historyDisk is the directory whose filesystem a run writes to: the spill
// directory's, else the dataset's, else the text file's ("" for a run that
// writes nothing).
func historyDisk(cfg Config, oopt order.Options) string {
	switch {
	case oopt.SpillDir != "":
		return oopt.SpillDir
	case cfg.Output.Dir != "":
		return cfg.Output.Dir
	case cfg.Output.Text != nil && cfg.Output.Text.Path != "":
		return filepath.Dir(cfg.Output.Text.Path)
	}
	return ""
}

// watchSource hands the progress file the source's live counters and the
// order stage's counter of rows handed on.
func watchSource(pr *Progress, sum *Report, src registry.Source, env *registry.Env) {
	if pr == nil {
		return
	}
	pr.started.Store(sum.Timings.StartedAt.UnixNano())
	listed := src.Listed
	pr.listed.Store(&listed)
	if l, ok := src.(source.Live); ok {
		answered := l.Answered
		pr.answered.Store(&answered)
	}
	if a, ok := src.(source.Advancing); ok {
		frontier := a.Frontier
		pr.frontier.Store(&frontier)
	}
	if f, ok := src.(source.Faulted); ok {
		faults := f.Faults
		pr.faults.Store(&faults)
	}
	if c, ok := src.(source.Contended); ok {
		contention := c.Contention
		pr.contention.Store(&contention)
	}
	if r, ok := src.(source.Requested); ok {
		requests := r.Requests
		pr.requests.Store(&requests)
		if p, ok := src.(source.Priced); ok {
			if rate, basis := p.Tariff(); basis != "" {
				pr.tariff.Store(math.Float64bits(rate))
			}
		}
	}
	pr.retained.Store(sum.Output.RowsRetained)
	env.Ordered = &pr.ordered
}

// routerOptions configures the Parquet router: parts journaled as they
// close, their queues charged to the shared row budget, and an encoder
// failure cancelling the run with its cause.
func routerOptions(cfg Config, jr *runJournal, shared *order.Shared, cancel context.CancelCauseFunc) (*parquetout.Options, error) {
	ropt := parquetout.Options{Out: cfg.Output.Dir, PartSize: cfg.Limits.PartSize, Encoders: cfg.Limits.Encoders, MaxEncoders: cfg.Limits.maxEncoders, Account: shared.Add,
		Interval: max(0, cfg.Limits.PartInterval), MaxRows: cfg.Limits.PartMaxRows}
	jr.routerOptions(&ropt)
	if err := jr.verifyOption(&ropt); err != nil {
		return nil, err
	}
	if pr := cfg.Progress; pr != nil {
		ropt.Encoded = &pr.encoded
	}
	if pub := ropt.Publish; pub != nil {
		// A failed run reports the parts the journal acknowledged
		// (durable, counted by publish); progress counts those renamed.
		ropt.Publish = func(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
			p, err := pub(ordinal, p, limit, rows, final)
			if err == nil {
				if pr := cfg.Progress; pr != nil {
					pr.parts.Add(1)
					pr.lastPart.Store(time.Now().UnixNano())
				}
			}
			return p, err
		}
	}
	ropt.Fail = func(err error) { cancel(fmt.Errorf("encode: %w", err)) }
	return &ropt, nil
}

// orderOptions configures the order stage: the row budget, the resume
// key, and spill (its directory, after reclaiming a crashed run's orphans,
// and its disk budget). scratch is a directory this attempt made for its
// spill (textScratch), for the caller to remove when the stage is done.
func orderOptions(cfg Config, jr *runJournal, budget int64, shared *order.Shared) (oopt order.Options, scratch string, err error) {
	gauges := &order.Gauges{}
	oopt = order.Options{Workers: cfg.Limits.Workers, Budget: budget, Shared: shared, SpillPlain: cfg.Limits.Spill.Plain, SpillCompress: !cfg.Limits.Spill.Uncompressed, SpillReserve: cfg.Limits.Spill.Reserve,
		After: jr.after(), Gauges: gauges, SegmentWorkers: segmentWorkers(cfg.Limits.maxEncoders, cfg.Limits.Workers, fdLimit())}
	if pr := cfg.Progress; pr != nil {
		pr.gauges.Store(gauges)
	}
	// Spill files are disposable: a crash relists their rows.
	if cfg.Output.Dir != "" {
		spill := filepath.Join(cfg.Output.Dir, dataset.JournalDir, "spill")
		// Reclaim a crashed run's orphans (the output lock is held), with
		// spill on or off; a recovered durable spill stays (its directory
		// is the cache's).
		if err := jr.verify(); err != nil {
			return oopt, "", err
		}
		if err := removeSpill(spill, jr.cacheOf() != nil); err != nil && cfg.Warn != nil {
			cfg.Warn(fmt.Sprintf("a previous run's spill files could not be removed: %v", err))
		}
		if !cfg.Limits.Spill.Off {
			oopt.SpillDir = spill
			if jr != nil && jr.lock != nil {
				oopt.SpillVerify = jr.lock.Verify
			}
		}
	} else if !cfg.Limits.Spill.Off && !dryRun(cfg) {
		// A text file spills beside itself, on the filesystem it is
		// written to; a stream has no directory of its own and spills
		// under the temporary directory ($TMPDIR), which the user points
		// at a disk.
		oopt.SpillDir = os.TempDir()
		if path := cfg.Output.Text.Path; path != "" {
			if scratch, err = textScratch(path, cfg.Attempt); err != nil {
				return oopt, "", err
			}
			oopt.SpillDir = filepath.Join(scratch, "spill")
		}
	}
	if oopt.SpillDir != "" {
		oopt.SpillMax = spillMax(cfg.Limits.Spill, oopt.SpillDir)
		if fs := order.FSType(existing(oopt.SpillDir)); order.InMemory(fs) && cfg.Warn != nil {
			fix := "write the output to a disk"
			if scratch == "" && cfg.Output.Dir == "" {
				fix = "set TMPDIR to a disk"
			}
			cfg.Warn(fmt.Sprintf("the spill directory %s is on %s: rows spilled behind a slow frontier are held in memory (outside the peak RSS); %s", oopt.SpillDir, fs, fix))
		}
	}
	return oopt, scratch, nil
}

// removeSpill removes the spill directory, or with keep every entry in it
// but the durable spill's.
func removeSpill(spill string, keep bool) error {
	if !keep {
		return os.RemoveAll(spill)
	}
	ents, err := os.ReadDir(spill)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var errs []error
	for _, e := range ents {
		if e.Name() != filepath.Base(order.DurableDir) {
			errs = append(errs, os.RemoveAll(filepath.Join(spill, e.Name())))
		}
	}
	return errors.Join(errs...)
}

// textScratch makes the scratch directory of one attempt writing the text
// file path: beside it, named for it and the attempt
// (.NAME.swath-ATTEMPT-RANDOM, created exclusively), so its spill is on
// the output's filesystem, and two attempts writing sibling outputs, or
// the same one, never share or remove each other's. Only the attempt that
// made it removes it; a killed attempt leaves it (with its .NAME.*.tmp)
// for the user to delete.
func textScratch(path string, att *Attempt) (string, error) {
	prefix := "." + filepath.Base(path) + ".swath-"
	if att != nil && len(att.ID) >= 8 {
		prefix += att.ID[:8] + "-"
	}
	dir, err := os.MkdirTemp(filepath.Dir(path), prefix+"*")
	if err != nil {
		return "", fmt.Errorf("text output: scratch directory: %w", err)
	}
	return dir, nil
}

// dryRun reports whether cfg writes nothing (no dataset, no text): its
// rows are only counted (swath du, swath diag count).
func dryRun(cfg Config) bool { return cfg.Output.Dir == "" && cfg.Output.Text == nil }

// attempt is one pipeline run's report and the state its failure and
// commit paths share.
type attempt struct {
	cfg    Config
	jr     *runJournal
	router parquetout.Encoder
	sum    Report
	// retained and committed are the rows the history reads as durable
	// besides the journal's acknowledgements: those kept from an
	// interrupted run, and the committed dataset's once the manifest is
	// installed (a run without a journal has no acknowledgement).
	retained, committed atomic.Int64
}

// settleText commits the text output after a successful run, or aborts
// it, and reports what it wrote; the error is the run's, or the commit's.
func (a *attempt) settleText(text *textout.Out, err error) error {
	if err == nil {
		err = text.Commit()
	} else {
		text.Abort()
		// The output's failure is the run's, whatever stop followed it
		// (a deadline or the watchdog, when the cause could not be set).
		if werr := text.Failed(); werr != nil && !errors.Is(err, werr) {
			err = fmt.Errorf("text output: %w", werr)
		}
	}
	st, out := text.Stats(), a.sum.Output.Text
	out.Rows, out.Bytes, out.SinkWaitNs, out.OutputWaitNs, out.Formatters = text.Rows(), text.Bytes(), int64(st.SinkWait), int64(st.OutputWait), st.Formatters
	if c := out.Compressor; c != nil {
		c.BytesIn, c.BytesOut = st.In, st.Out
	}
	return err
}

// startCache starts the attempt's durable spill (its flusher), with the
// certificate cap of the spill cap in effect, and hands the provider the
// reused certificates to list around. Durable spill engages only with
// spill on, a provider that declares fences and lists around reused
// certificates; otherwise every certificate a recovery accepted is
// retired (relisted) and no slot opens (cap 0).
func (a *attempt) startCache(oopt *order.Options, src registry.Source, cancel context.CancelCauseFunc) error {
	c := a.jr.cacheOf()
	if c == nil {
		return nil
	}
	f, fences := src.(source.Fencing)
	ru, reusing := src.(source.Reusing)
	slots := order.CertificateCap(oopt.SpillMax)
	switch {
	case oopt.SpillDir == "":
		slots = 0
	case !fences || !f.Fences() || !reusing:
		slots, a.jr.off = 0, "the provider declares no coverage fences"
	case slots == 0:
		a.jr.off = "the spill budget (--spill-budget) is below 16 MiB"
	}
	if err := c.Start(slots, oopt.SpillMax, func(err error) { cancel(err) }); err != nil {
		return err
	}
	if slots == 0 {
		return nil
	}
	oopt.Durable = c
	spans := c.Spans()
	if len(spans) > 0 {
		ss := make([]source.Span, len(spans))
		for i, s := range spans {
			ss[i] = source.Span{Lo: s.Lo, Hi: s.Hi, HiOpen: s.HiOpen}
		}
		ru.Reuse(ss)
	}
	c.Gaps(gaps(spans, a.jr.after()))
	return nil
}

// gaps is the number of live gaps a resume lists: the pieces of (K, ⊤]
// outside spans (ascending, disjoint, each ending above K).
func gaps(spans []order.Span, k *string) int {
	pos, open := "", true // the start of the current gap; open: (pos, ⊤] still to cover
	if k != nil {
		pos = *k
	}
	n := 0
	for _, s := range spans {
		if s.Lo > pos {
			n++
		}
		if s.HiOpen {
			open = false
			break
		}
		pos = s.Hi
	}
	if open {
		n++
	}
	return n
}

// closeCache closes the attempt's durable spill (runJournal.closeCache, not
// committed) and puts its account in the report.
func (a *attempt) closeCache(committed bool) error {
	err := a.jr.closeCache(committed)
	if sp := a.sum.Resources.Spill; sp != nil && a.jr.cacheOf() != nil {
		sp.Durable = a.jr.cache.Report()
	}
	return err
}

// account puts the order stage's and the router's resources in the report.
func (a *attempt) account(ost order.Stats, oopt order.Options, filtered int64) {
	res := &a.sum.Resources
	res.MaxBufferedBytes, a.sum.Output.RowsEmitted = ost.MaxBuffered, ost.Rows
	a.sum.Output.RowsFiltered = filtered
	// order: false for a dry run, which counts rows without ordering them
	// (order.Count: no reorder storage, max_buffered_bytes 0).
	res.Features = map[string]bool{"spill": oopt.SpillDir != "", "order": !dryRun(a.cfg)}
	if oopt.SpillDir != "" {
		res.Spill = &Spill{Rows: ost.SpillRows, Bytes: ost.SpillBytes, LogicalBytes: ost.SpillLogical, Format: ost.SpillFormat, DecodeRefused: ost.SpillDecodeRefused, Ranges: ost.SpillFiles, Frames: ost.SpillFrames, Opens: ost.SpillOpens, PeakBytes: ost.SpillPeak, Refused: ost.SpillRefused,
			MaxBytes: oopt.SpillMax, ReserveBytes: oopt.SpillReserve, PeakReaders: ost.PeakReaders,
			Dir: oopt.SpillDir, FSType: order.FSType(existing(oopt.SpillDir)), DurableOff: a.jr.offReason()}
		if c := a.jr.cacheOf(); c != nil && ost.Durable {
			res.Spill.Durable = c.Report()
		}
	}
	res.PeakAheadRows, res.FrontierStuckMaxNs = ost.PeakAhead, int64(ost.FrontierStuck)
	res.PeakNodes, res.PeakMarkers = ost.PeakNodes, ost.PeakMarkers
	res.FrontierWaitNs, res.BudgetWaitNs = int64(ost.FrontierWait), int64(ost.BudgetWait)
	if !ost.ListingDone.IsZero() {
		a.sum.Timings.ListingDoneAt = ost.ListingDone.UTC()
	}
	if !dryRun(a.cfg) {
		res.Drain = a.drain(ost, oopt)
	}
	if a.router != nil {
		w := int64(a.router.EncoderWait())
		res.EncoderWaitNs = &w
		res.PartTimeCuts = a.router.TimeCuts()
		res.PartRowCuts = a.router.RowCuts()
	}
}

// drain is the report's account of the drain: its tail after listing,
// the workers' idle time on either side, the drainer's time, the spill
// read-ahead, and the encoders admitted.
func (a *attempt) drain(ost order.Stats, oopt order.Options) *Drain {
	dt := ost.Drainer
	d := &Drain{WorkerIdleAfterNs: int64(ost.IdleAfter),
		ReplayReadNs: int64(dt.ReplayRead), ReplayDecodeNs: int64(dt.ReplayDecode), AheadWaitNs: int64(dt.AheadWait),
		OrderCheckNs: int64(dt.OrderCheck), SinkWriteNs: int64(dt.SinkWrite)}
	if !ost.ListingDone.IsZero() && !ost.DrainEnd.IsZero() {
		d.TailNs = int64(ost.DrainEnd.Sub(ost.ListingDone))
	}
	if oopt.SpillDir != "" {
		ra := ost.ReadAhead
		d.ReadAhead = &ReadAhead{Frames: ra.Frames, Rows: ra.Rows, DrainerFrames: ra.DrainerFrames, Unplanned: ra.Unplanned, Waits: ra.Waits,
			CappedWaits: ra.Capped, ReadNs: int64(ra.Read), DecodeNs: int64(ra.Decode), MaxReads: ra.MaxReads}
	}
	if a.router != nil {
		s := ost.Segments
		d.Segments = &Segments{Reason: s.Reason, Segments: s.Segments, Workers: s.Workers, Rows: s.Rows, MaxRows: s.MaxRows, Seams: s.Seams,
			TimeNs: int64(s.Time), MaxNs: int64(s.MaxTime), PlanNs: int64(s.Plan), ReadNs: int64(s.Read), DecodeNs: int64(s.Decode), WriteNs: int64(s.Write),
			MaxWorkers: oopt.SegmentWorkers, BudgetWaits: s.BudgetWaits, BudgetWaitNs: int64(s.BudgetWait), MaxHeldBytes: s.MaxHeld,
			WorkersBy: s.WorkersBy, PerWorkerBytes: s.PerWorker, FreeBytes: s.Free, TreeBytes: s.Tree,
			DurableFilesRead: s.DurableFiles, DurableBoundaries: s.DurableBoundaries, DeferredRows: s.Deferred}
		l := a.cfg.Limits
		d.EncodersAtStart, d.EncodersAfterListing = l.Encoders, a.router.Encoders()
		switch {
		case l.encodersGiven:
			d.EncoderAdmission = AdmissionExplicit
		case l.maxEncoders == l.Encoders:
			d.EncoderAdmission = AdmissionAtCap
		case ost.ListingDone.IsZero():
			d.EncoderAdmission = AdmissionNotReached
		default:
			d.EncoderAdmission = AdmissionListingDone
		}
	}
	return d
}

// durable puts the parts that stay in the output after a failure (the
// recovered ones and those journaled by this invocation) in the report; a
// run without a journal keeps none.
func (a *attempt) durable() {
	jr, out := a.jr, &a.sum.Output
	if !jr.journaled() {
		return
	}
	// Acknowledged by the journal: an append that succeeded is durable
	// although its rename then failed (the resume renames it).
	out.PartsWritten, out.OutputBytes = int(jr.acked.parts.Load()), jr.acked.bytes.Load()
	out.RowsDurable = out.RowsRetained
	if r := jr.acked.rows.Load(); r > 0 {
		out.RowsDurable = r
	}
	if jr.state != nil {
		for _, rec := range jr.state.Records {
			out.OutputBytes += rec.Part.Bytes
		}
	}
}

// fail settles a run that failed after the source ran: the router is
// aborted, an unjournaled run's output cleaned, the durable parts reported.
func (a *attempt) fail(err error) (Report, error) {
	jr := a.jr
	if verr := jr.verify(); verr != nil && !errors.Is(err, journal.ErrOutputReplaced) {
		// A failure by pathname (a part or spill file not found) is the
		// replacement's consequence: name the cause.
		err = fmt.Errorf("%w: %w", verr, err)
	}
	if a.router != nil {
		a.router.Abort()
	}
	if errors.Is(err, faults.ErrCrash) {
		a.jr.cacheOf().Abort(err) // a kill leaves the durable spill as it is
	} else if cerr := a.closeCache(false); cerr != nil && !errors.Is(err, cerr) {
		err = fmt.Errorf("%w (durable spill: %w)", err, cerr)
	}
	if a.router != nil {
		// A crash leaves the output as a kill would; a journaled run
		// keeps its temporary parts too: one may be recorded already
		// (its rename failed), and -resume decides which are.
		if !errors.Is(err, faults.ErrCrash) && !jr.journaled() && jr.verify() == nil {
			dataset.Cleanup(a.cfg.Output.Dir)
		}
	}
	a.durable()
	return a.sum.failed(err)
}

// commit closes the router, publishes the manifest (the commit) and
// writes the run report.
func (a *attempt) commit(s schema.Info, src registry.Source, listed time.Time) (Report, error) {
	cfg, jr, sum := a.cfg, a.jr, &a.sum
	parts, err := a.router.Close()
	if err != nil {
		return a.fail(fmt.Errorf("encode: %w", err))
	}
	closed := time.Now()
	drain := closed.Sub(listed).Milliseconds()
	sum.Timings.DrainMs = &drain
	// Retirement outlives publication: the cache closes after the router
	// (its last part records may retire files), before the manifest.
	if err := a.closeCache(false); err != nil {
		return a.fail(err)
	}
	if err := jr.unresolved(); err != nil {
		return a.fail(err)
	}
	sum.Output.PartsWritten = len(parts) - sum.Output.PartsRetained
	for _, part := range parts {
		sum.Output.OutputBytes += part.Bytes
	}
	if err := jr.verify(); err != nil {
		return a.fail(err)
	}
	meta := manifestMeta(s, src.Locator(), sum.report, jr.listingStarted(sum.Timings.StartedAt), listed)
	meta.Coverage.Filters = append(meta.Coverage.Filters, cfg.Filter.Descriptions()...)
	// Committed once the manifest is installed: the terminal account is
	// frozen then, in the same step, before anything that can block or
	// fail; the report follows and can no longer revoke the dataset.
	committed := false
	step := func(install func() error) error {
		if err := install(); err != nil {
			return err
		}
		committed = true
		pub := time.Since(closed).Milliseconds()
		sum.Timings.PublicationMs = &pub
		if pr := cfg.Progress; pr != nil && !jr.journaled() {
			pr.parts.Add(int64(sum.Output.PartsWritten))
		}
		sum.Outcome.Status, sum.Outcome.Class = StatusCommitted, ClassCompleted
		sum.Output.RowsCommitted = sum.Output.RowsRetained + sum.Output.RowsEmitted
		sum.Output.RowsDurable = sum.Output.RowsCommitted
		a.committed.Store(sum.Output.RowsCommitted) // before finish takes the last sample
		sum.finish()
		cfg.finalize(sum, nil)
		return nil
	}
	commit := step
	if cfg.Commit != nil {
		commit = func(install func() error) error { return cfg.Commit(func() error { return step(install) }) }
	}
	if err := dataset.Publish(cfg.Output.Dir, meta, parts, commit); err != nil {
		if committed { // only an injected crash follows the commit
			return *sum, err
		}
		if !errors.Is(err, faults.ErrCrash) && !jr.journaled() && jr.verify() == nil {
			dataset.Cleanup(cfg.Output.Dir)
		}
		a.durable()
		return sum.failed(fmt.Errorf("publish: %w", err))
	}
	// Committed: the durable spill is the dataset's now; its directory goes.
	if err := jr.closeCache(true); err != nil && cfg.Warn != nil {
		cfg.Warn("durable spill directory not removed: " + err.Error())
	}
	if err := jr.verify(); err != nil {
		if cfg.Warn != nil {
			cfg.Warn("run report not written: " + err.Error())
		}
	} else if err := dataset.WriteReport(cfg.Output.Dir, *sum); err != nil && cfg.Warn != nil {
		cfg.Warn(err.Error())
	}
	return *sum, nil
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
	r.Outcome.Status, r.Outcome.Error = StatusFailed, err.Error()
	r.finish()
	return r, err
}
