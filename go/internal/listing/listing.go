// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package listing runs the pipeline: provider → order → part router →
// dataset publication.
package listing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/durable"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/parquet"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// Config is one listing. Zero values take the documented defaults.
type Config struct {
	Root    string // a local directory, or scheme://...; the registered provider for the scheme lists it
	Out     string // dataset directory; empty for a dry run
	Replace bool   // replace a previous dataset in Out
	// Source is the provider's options value (source.Factory.Flags or
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

// Summary is the run report (also written as the dataset's summary file).
type Summary struct {
	Tool        string `json:"tool"`
	SchemaID    string `json:"schema_id"`
	Root        string `json:"root"`
	Out         string `json:"out,omitempty"`
	Fields      string `json:"fields,omitempty"`
	Workers     int    `json:"workers"`
	Encoders    int    `json:"encoders"`
	RowBudget   int64  `json:"row_budget_bytes"` // rows in flight: reorder storage and encoder queues
	MaxBuffered int64  `json:"max_buffered_bytes"`
	Spill       *Spill `json:"spill,omitempty"` // nil: spill was off ("spill" is listed under Disabled)
	// The order stage's waits (see order.Stats) and the time the order
	// stage waited for an encoder (nil without output).
	FrontierWaitNs int64  `json:"frontier_wait_ns"`
	BudgetWaitNs   int64  `json:"budget_wait_ns"`
	EncoderWaitNs  *int64 `json:"encoder_wait_ns,omitempty"`
	// Disabled lists the run's policies that were turned off, so a zero
	// counter always means "on, never fired" (see docs/internals/counters.md).
	Disabled []string `json:"disabled,omitempty"`
	// The source's report goes here, under its own name ("s3", "listing");
	// see MarshalJSON.
	Source       json.RawMessage `json:"__source__,omitempty"`
	report       *source.Report
	Rows         int64   `json:"rows"`
	Parts        int     `json:"parts"`
	OutBytes     int64   `json:"output_bytes"`
	ResumedParts int     `json:"resumed_parts,omitempty"` // parts kept from the interrupted run
	ResumedRows  int64   `json:"resumed_rows,omitempty"`
	Seconds      float64 `json:"seconds"`
	// ListingSeconds is the time to the end of listing (the order stage).
	ListingSeconds float64   `json:"listing_seconds"`
	UserSeconds    float64   `json:"user_cpu_seconds"`
	SysSeconds     float64   `json:"sys_cpu_seconds"`
	PeakRSS        int64     `json:"peak_rss_bytes"`
	Started        time.Time `json:"started"`
}

// Errors is the listing's error count.
func (s *Summary) Errors() int64 {
	if s.report != nil {
		return s.report.Errors
	}
	return 0
}

// Report is the source's report (nil before the listing ends, or for a
// source without one). Its Stats are the provider's own type.
func (s *Summary) Report() *source.Report { return s.report }

func (s *Summary) setReport(r source.Report) {
	s.report = &r
	s.Fields = r.Fields
}

// MarshalJSON writes the source's stats under the source's name, in the
// place the placeholder field holds.
func (s Summary) MarshalJSON() ([]byte, error) {
	type plain Summary
	p := plain(s)
	if s.report != nil && s.report.Stats != nil {
		b, err := json.Marshal(s.report.Stats)
		if err != nil {
			return nil, err
		}
		p.Source = b
	}
	out, err := json.Marshal(p)
	if err != nil || s.report == nil {
		return out, err
	}
	return bytes.Replace(out, []byte(`"__source__":`), []byte(strconv.Quote(s.report.Name)+":"), 1), nil
}

// Run lists cfg.Root and, unless cfg.Out is empty, publishes the dataset.
// On any error or cancellation nothing is published and unpublished parts
// are removed (a journaled run keeps them for -resume); the error is the
// cause (ctx.Err() on cancellation).
func Run(ctx context.Context, cfg Config) (Summary, error) {
	start := time.Now()
	if err := cfg.setDefaults(); err != nil {
		return Summary{}, err
	}
	sum := Summary{
		Tool: "swath-go", Out: cfg.Out, Workers: cfg.Workers, Encoders: cfg.Encoders,
		RowBudget: cfg.Memory / 2, Started: start,
	}
	f, err := source.Lookup(cfg.Root)
	if err != nil {
		return sum, err
	}
	opts := cfg.Source
	if opts == nil {
		opts = f.Defaults()
	}
	warn := cfg.Warn
	if warn == nil {
		warn = func(string) {}
	}
	src, err := f.Open(ctx, source.Request{Root: cfg.Root, Options: opts, Workers: cfg.Workers, Warn: warn})
	if err != nil {
		return sum, err
	}
	s, loc := src.Schema(), src.Locator()
	sum.Root, sum.SchemaID = loc.Root, s.ID
	jr, err := openJournal(cfg, src, identity(src, s, cfg))
	if err != nil {
		return sum, err
	}
	if ex, ok := src.(source.Excluder); ok && cfg.Out != "" {
		if err := ex.Exclude(cfg.Out); err != nil {
			jr.close()
			return sum, err
		}
	}
	if last := jr.last(); last != nil {
		if err := jr.src.Restart(last.Part.MaxKey, jr.state.Checkpoints); err != nil {
			jr.close()
			return sum, err
		}
	}
	sum.ResumedParts, sum.ResumedRows = jr.resumedParts(), jr.resumedRows()
	var p source.Provider[any] = src
	if jr.complete() {
		// The final part is durable: listing again could only add a
		// second final part on a changed source. Publish what is recorded.
		p = source.Empty{}
	}
	return pipeline(ctx, cfg, sum, src, p, s, jr)
}

// identity is the run's resume identity: what the pipeline freezes, and
// what the source names (source.Identified).
func identity(src source.Source, s *schema.Schema, cfg Config) durable.Identity {
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
func manifestMeta(s *schema.Schema, loc source.Locator, rep *source.Report, started, listed time.Time) dataset.Meta {
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

// reports names the JSON run reports published beside the manifest: swath's
// run summary for a source that states its coverage, else this summary.
func reports(cfg Config, sum *Summary) []dataset.File {
	if r := sum.report; r != nil && r.Activity != nil {
		return []dataset.File{{Name: dataset.SwathSummaryName, Value: swathSummary(cfg, sum, r)}}
	}
	return []dataset.File{{Name: dataset.SummaryName, Value: sum}}
}

// pipeline runs provider → order → router → publish. p is src, or Empty
// when a resume has nothing left to list.
func pipeline(ctx context.Context, cfg Config, sum Summary, src source.Source, p source.Provider[any], s *schema.Schema,
	jr *journal) (Summary, error) {
	defer jr.close()
	// One shared cause: the first asynchronous failure (an encoder, the
	// disk, the journal) cancels listing at once, and is what Run returns.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var sink order.Sink = discard{}
	var router *parquet.Router
	if cfg.Out != "" {
		var err error
		ropt := parquet.Options{Out: cfg.Out, Schema: s, PartSize: cfg.PartSize, Encoders: cfg.Encoders}
		jr.routerOptions(&ropt)
		ropt.Fail = func(err error) { cancel(fmt.Errorf("encode: %w", err)) }
		router, err = parquet.NewRouter(ropt)
		if err != nil {
			return sum, err
		}
		sink = jr.guard(router)
	}
	fail := func(err error) (Summary, error) {
		if router != nil {
			router.Abort()
			// A crash leaves the output as a kill would; a journaled run
			// keeps its temporary parts too: one may be recorded already
			// (its rename failed), and -resume decides which are.
			if !errors.Is(err, faults.ErrCrash) && !jr.journaled() {
				dataset.Cleanup(cfg.Out)
			}
		}
		finish(&sum)
		return sum, err
	}
	// One byte budget for rows in flight, shared by the order stage and the
	// encoders' queues (half of -memory; the rest is headroom for pages
	// being parsed, codecs and the garbage collector).
	shared := &order.Shared{}
	if router != nil {
		router.Account(shared.Add)
	}
	oopt := order.Options{Workers: cfg.Workers, Budget: sum.RowBudget, Shared: shared, SpillCompress: cfg.SpillCompress, SpillMax: cfg.SpillMax, SpillReserve: cfg.SpillReserve}
	if !cfg.NoSpill {
		// Spill files are disposable: a crash relists their rows.
		oopt.SpillDir = os.TempDir()
		if cfg.Out != "" {
			oopt.SpillDir = filepath.Join(cfg.Out, dataset.JournalDir, "spill")
			// Reclaim a crashed run's orphans (the output lock is held).
			_ = os.RemoveAll(oopt.SpillDir)
		}
	}
	ost, err := order.Run(ctx, p, sink, oopt)
	if cfg.Out != "" && oopt.SpillDir != "" {
		_ = os.Remove(oopt.SpillDir) // only if empty; order.Run removed its own files
		_ = os.Remove(filepath.Dir(oopt.SpillDir))
	}
	sum.MaxBuffered, sum.Rows = ost.MaxBuffered, ost.Rows
	if oopt.SpillDir != "" {
		sum.Spill = &Spill{Rows: ost.SpillRows, Bytes: ost.SpillBytes, Ranges: ost.SpillFiles, PeakBytes: ost.SpillPeak, Refused: ost.SpillRefused}
	} else {
		sum.Disabled = append(sum.Disabled, "spill")
	}
	sum.FrontierWaitNs, sum.BudgetWaitNs = int64(ost.FrontierWait), int64(ost.BudgetWait)
	if router != nil {
		w := int64(router.EncoderWait())
		sum.EncoderWaitNs = &w
	}
	listed := time.Now()
	sum.ListingSeconds = listed.Sub(sum.Started).Seconds()
	if r, ok := src.(source.Reporter); ok {
		sum.setReport(r.Report())
	}
	want := src.Listed()
	if err != nil {
		return fail(err)
	}
	if sum.Rows != want {
		return fail(fmt.Errorf("row count mismatch: listed %d, ordered %d", want, sum.Rows))
	}
	if router == nil {
		finish(&sum)
		return sum, nil
	}
	parts, err := router.Close()
	if err != nil {
		return fail(fmt.Errorf("encode: %w", err))
	}
	sum.Parts = len(parts)
	for _, part := range parts {
		sum.OutBytes += part.Bytes
	}
	finish(&sum)
	err = dataset.Publish(cfg.Out, manifestMeta(s, src.Locator(), sum.report, jr.listingStarted(sum.Started), listed), parts, reports(cfg, &sum))
	var rerr *dataset.ReportError
	if errors.As(err, &rerr) { // committed: the dataset stands
		if cfg.Warn != nil {
			cfg.Warn(rerr.Error())
		}
		err = nil
	}
	if err != nil {
		if !errors.Is(err, faults.ErrCrash) && !jr.journaled() {
			dataset.Cleanup(cfg.Out)
		}
		return sum, fmt.Errorf("publish: %w", err)
	}
	finish(&sum)
	return sum, nil
}

func finish(s *Summary) {
	s.Seconds = time.Since(s.Started).Seconds()
	u, sys := cpuTimes()
	s.UserSeconds, s.SysSeconds = u.Seconds(), sys.Seconds()
	s.PeakRSS = peakRSS()
}

type discard struct{}

func (discard) Write(context.Context, [][]schema.Row) error { return nil }
