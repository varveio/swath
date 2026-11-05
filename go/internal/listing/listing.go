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
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/durable"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/parquet"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// Config is one listing. Zero values take the documented defaults.
type Config struct {
	Root    string // a local directory, or scheme://...; the registered provider for the scheme lists it
	Out     string // dataset directory; empty for a dry run
	Replace bool   // replace a previous dataset in Out
	// Source is the provider's options value (registry.Factory.Flags or
	// Defaults); opaque here. nil takes the provider's defaults.
	Source        any
	Warn          func(string) // one-time warnings for the user (nil: dropped)
	NoJournal     bool         // publish parts only at the end, without a journal (no resume)
	Workers       int          // listing goroutines; default min(64, max(4, 3*GOMAXPROCS))
	Encoders      int          // Parquet encoders; default GOMAXPROCS
	Memory        int64        // budget in bytes for rows held between listing and encoding; default 256 MiB
	PartSize      int64        // target bytes per part; default 64 MiB
	Argv          []string     // the command line, recorded in the run summary
	Resume        bool         // continue the interrupted journaled run in Out
	NoSpill       bool         // wait on the reorder budget instead of spilling ahead-of-frontier rows
	SpillCompress bool         // zstd level 1 on spill frames
	SpillMax      int64        // cap on live spill bytes (0: none)
	SpillReserve  int64        // free bytes spill leaves on its filesystem (0: none; the CLI default is 1 GiB)
}

// Spill describes the run's spill (see order.Stats).
type Spill struct {
	Rows      int64 `json:"rows"`   // rows written to spill files and replayed
	Bytes     int64 `json:"bytes"`  // bytes written
	Ranges    int64 `json:"ranges"` // ranges that spilled
	PeakBytes int64 `json:"peak_bytes"`
	Refused   int64 `json:"refused"` // frames the disk budget refused (the emitter waited instead)
}

// Defaults.
const (
	DefaultMemory   = 256 << 20
	DefaultPartSize = 64 << 20
	// minMemory is the smallest budget accepted.
	minMemory = 256 << 10
)

func (c *Config) setDefaults() error {
	if c.Root == "" {
		return errors.New("no root")
	}
	if c.Workers <= 0 {
		c.Workers = min(64, max(4, 3*runtime.GOMAXPROCS(0)))
	}
	if c.Encoders <= 0 {
		c.Encoders = runtime.GOMAXPROCS(0)
	}
	if c.Memory <= 0 {
		c.Memory = DefaultMemory
	}
	if c.PartSize <= 0 {
		c.PartSize = DefaultPartSize
	}
	if c.Memory < minMemory {
		return fmt.Errorf("memory budget %d is below %d bytes", c.Memory, minMemory)
	}
	return nil
}

// Run lists cfg.Root and, unless cfg.Out is empty, publishes the dataset.
// On any error or cancellation nothing is published and unpublished parts
// are removed (a journaled run keeps them for -resume); the error is the
// cause (ctx.Err() on cancellation).
func Run(ctx context.Context, cfg Config) (Report, error) {
	start := time.Now()
	sum := Report{ReportVersion: ReportVersion, Build: build(), Timings: Timings{StartedAt: start.UTC()},
		Outcome: Outcome{Status: "failed"}}
	if err := cfg.setDefaults(); err != nil {
		return sum.failed(err)
	}
	argv := cfg.Argv
	if argv == nil {
		argv = []string{}
	}
	sum.Invocation = Invocation{Argv: argv, Source: cfg.Root, Out: cfg.Out, Resume: cfg.Resume, Workers: cfg.Workers,
		Encoders: cfg.Encoders, MemoryBytes: cfg.Memory, PartSizeBytes: cfg.PartSize}
	sum.Resources.RowBudgetBytes = cfg.Memory / 2
	f, err := registry.Lookup(cfg.Root)
	if err != nil {
		return sum.failed(err)
	}
	opts := cfg.Source
	if opts == nil {
		opts = f.Defaults()
	}
	warn := cfg.Warn
	if warn == nil {
		warn = func(string) {}
	}
	src, err := f.Open(ctx, registry.Request{Root: cfg.Root, Options: opts, Workers: cfg.Workers, Warn: warn})
	if err != nil {
		return sum.failed(err)
	}
	s, loc := src.Schema(), src.Locator()
	sum.Invocation.Source, sum.Invocation.SchemaID = loc.Root, s.ID
	jr, err := openJournal(cfg, src, identity(src, s, cfg))
	if err != nil {
		return sum.failed(err)
	}
	if ex, ok := src.(source.Excluder); ok && cfg.Out != "" {
		if err := ex.Exclude(cfg.Out); err != nil {
			jr.close()
			return sum.failed(err)
		}
	}
	if last := jr.last(); last != nil {
		if err := jr.src.Restart(string(last.Part.MaxKey), jr.state.Checkpoints); err != nil {
			jr.close()
			return sum.failed(err)
		}
	}
	sum.Output.PartsRetained, sum.Output.RowsRetained = jr.resumedParts(), jr.resumedRows()
	return pipeline(ctx, cfg, sum, src, s, jr)
}

// identity is the run's resume identity: what the pipeline freezes, and
// what the source names (source.Identified).
func identity(src registry.Source, s schema.Info, cfg Config) durable.Identity {
	id := durable.Identity{Tool: "swath-go", Format: dataset.FormatVersion, SchemaID: s.ID, KeyCap: s.MaxKeyBytes, Writer: parquet.WriterID,
		PartSize: cfg.PartSize, Provider: src.Locator().Kind}
	if idf, ok := src.(source.Identified); ok {
		sid := idf.Identity()
		id.Provider = sid.Provider
		for _, f := range sid.Fields {
			id.Source = append(id.Source, durable.Field{Name: f.Name, Value: f.Value, Canonical: f.Canonical})
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
func pipeline(ctx context.Context, cfg Config, sum Report, src registry.Source, s schema.Info, jr *journal) (Report, error) {
	defer jr.close()
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
	env := registry.Env{After: jr.after(), Empty: jr.complete()}
	if cfg.Out != "" {
		ropt := parquet.Options{Out: cfg.Out, PartSize: cfg.PartSize, Encoders: cfg.Encoders, Account: shared.Add}
		jr.routerOptions(&ropt)
		ropt.Fail = func(err error) { cancel(fmt.Errorf("encode: %w", err)) }
		env.Router = &ropt
	}
	var router parquet.Encoder
	fail := func(err error) (Report, error) {
		if router != nil {
			router.Abort()
			// A crash leaves the output as a kill would; a journaled run
			// keeps its temporary parts too: one may be recorded already
			// (its rename failed), and -resume decides which are.
			if !errors.Is(err, faults.ErrCrash) && !jr.journaled() {
				dataset.Cleanup(cfg.Out)
			}
		}
		return sum.failed(err)
	}
	oopt := order.Options{Workers: cfg.Workers, Budget: sum.Resources.RowBudgetBytes, Shared: shared, SpillCompress: cfg.SpillCompress, SpillMax: cfg.SpillMax, SpillReserve: cfg.SpillReserve}
	if !cfg.NoSpill {
		// Spill files are disposable: a crash relists their rows.
		oopt.SpillDir = os.TempDir()
		if cfg.Out != "" {
			oopt.SpillDir = filepath.Join(cfg.Out, dataset.JournalDir, "spill")
			// Reclaim a crashed run's orphans (the output lock is held).
			_ = os.RemoveAll(oopt.SpillDir)
		}
	}
	env.Order = oopt
	ost, router, err := src.Run(ctx, env)
	if router == nil && cfg.Out != "" && err != nil {
		return sum.failed(err) // the router could not start: nothing was written
	}
	if cfg.Out != "" && oopt.SpillDir != "" {
		_ = os.Remove(oopt.SpillDir) // only if empty; order.Run removed its own files
		_ = os.Remove(filepath.Dir(oopt.SpillDir))
	}
	sum.Resources.MaxBufferedBytes, sum.Output.RowsListed = ost.MaxBuffered, ost.Rows
	if oopt.SpillDir != "" {
		sum.Resources.Spill = &Spill{Rows: ost.SpillRows, Bytes: ost.SpillBytes, Ranges: ost.SpillFiles, PeakBytes: ost.SpillPeak, Refused: ost.SpillRefused}
	} else {
		sum.Resources.Disabled = append(sum.Resources.Disabled, "spill")
	}
	sum.Resources.FrontierWaitNs, sum.Resources.BudgetWaitNs = int64(ost.FrontierWait), int64(ost.BudgetWait)
	if router != nil {
		w := int64(router.EncoderWait())
		sum.Resources.EncoderWaitNs = &w
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
	if sum.Output.RowsListed != want {
		return fail(fmt.Errorf("row count mismatch: listed %d, ordered %d", want, sum.Output.RowsListed))
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
	sum.Output.PartsWritten = len(parts) - sum.Output.PartsRetained
	for _, part := range parts {
		sum.Output.Bytes += part.Bytes
	}
	if err := dataset.Publish(cfg.Out, manifestMeta(s, src.Locator(), sum.report, jr.listingStarted(sum.Timings.StartedAt), listed), parts); err != nil {
		if !errors.Is(err, faults.ErrCrash) && !jr.journaled() {
			dataset.Cleanup(cfg.Out)
		}
		return sum.failed(fmt.Errorf("publish: %w", err))
	}
	// Committed: the report follows and can no longer revoke the dataset.
	sum.Outcome.Status = "committed"
	sum.Output.RowsCommitted = sum.Output.RowsRetained + sum.Output.RowsListed
	sum.finish()
	if err := dataset.WriteReport(cfg.Out, sum); err != nil && cfg.Warn != nil {
		cfg.Warn(err.Error())
	}
	return sum, nil
}

// failed records err as the outcome and returns the report with it.
func (r Report) failed(err error) (Report, error) {
	r.Outcome.Status, r.Outcome.Error = "failed", err.Error()
	r.finish()
	return r, err
}
