// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package listing runs the pipeline: provider → order → part router →
// dataset publication.
package listing

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/varveio/swath/tools/swath-local-spike/internal/dataset"
	"github.com/varveio/swath/tools/swath-local-spike/internal/order"
	"github.com/varveio/swath/tools/swath-local-spike/internal/parquet"
	"github.com/varveio/swath/tools/swath-local-spike/internal/schema"
	"github.com/varveio/swath/tools/swath-local-spike/internal/source/fs"
)

// Config is one listing. Zero values take the documented defaults.
type Config struct {
	Root     string
	Out      string // dataset directory; empty for a dry run
	Replace  bool   // replace a previous dataset in Out
	Fields   fs.Fields
	NoDirs   bool
	NoLinks  bool  // do not read symlink targets
	Workers  int   // listing goroutines; default min(64, max(4, 3*GOMAXPROCS))
	Encoders int   // Parquet encoders; default GOMAXPROCS
	Memory   int64 // budget in bytes for rows held between listing and encoding; default 256 MiB
	PartSize int64 // target bytes per part; default 64 MiB
	Portable bool  // use the portable filesystem engine (tests, comparisons)
}

// Defaults.
const (
	DefaultMemory   = 256 << 20
	DefaultPartSize = 64 << 20
	// bytesPerBufferedRow converts the memory budget into the reorder bound:
	// about 2 KiB per buffered row covers the row, its key and the slab and
	// heap overhead around it (256 MiB → 131,072 rows; 100k measured best).
	bytesPerBufferedRow = 2 << 10
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
	if c.Memory < bytesPerBufferedRow {
		return fmt.Errorf("memory budget %d is below %d bytes", c.Memory, bytesPerBufferedRow)
	}
	return nil
}

// Summary is the run report (also written as the dataset's summary file).
type Summary struct {
	Tool        string    `json:"tool"`
	SchemaID    string    `json:"schema_id"`
	Root        string    `json:"root"`
	Out         string    `json:"out,omitempty"`
	Fields      string    `json:"fields"`
	Workers     int       `json:"workers"`
	Encoders    int       `json:"encoders"`
	ReorderRows int       `json:"reorder_rows"`
	MaxBuffered int64     `json:"max_buffered_rows"`
	Listing     fs.Stats  `json:"listing"`
	Rows        int64     `json:"rows"`
	Parts       int       `json:"parts"`
	OutBytes    int64     `json:"output_bytes"`
	Seconds     float64   `json:"seconds"`
	UserSeconds float64   `json:"user_cpu_seconds"`
	SysSeconds  float64   `json:"sys_cpu_seconds"`
	PeakRSS     int64     `json:"peak_rss_bytes"`
	Started     time.Time `json:"started"`
}

// Run lists cfg.Root and, unless cfg.Out is empty, publishes the dataset.
// On any error or cancellation nothing is published and unpublished parts
// are removed; the error is the cause (ctx.Err() on cancellation).
func Run(ctx context.Context, cfg Config) (Summary, error) {
	start := time.Now()
	if err := cfg.setDefaults(); err != nil {
		return Summary{}, err
	}
	root, err := filepath.Abs(cfg.Root)
	if err != nil {
		return Summary{}, err
	}
	s := &schema.Local
	opt := fs.Options{Fields: cfg.Fields, NoDirs: cfg.NoDirs, LinkTarget: !cfg.NoLinks, Portable: cfg.Portable}
	if _, err := fs.New(root, opt); err != nil { // validate the root before touching the output
		return Summary{}, fmt.Errorf("root: %w", err)
	}
	sum := Summary{
		Tool: "swath-local", SchemaID: s.ID, Root: root, Out: cfg.Out, Fields: cfg.Fields.String(),
		Workers: cfg.Workers, Encoders: cfg.Encoders, ReorderRows: int(cfg.Memory / bytesPerBufferedRow), Started: start,
	}
	var sink order.Sink = discard{}
	var router *parquet.Router
	if cfg.Out != "" {
		if err := dataset.Prepare(cfg.Out, root, s.ID, cfg.Replace); err != nil {
			return sum, fmt.Errorf("prepare output: %w", err)
		}
		if rel, inside, err := dataset.LocateInside(root, cfg.Out); err != nil {
			return sum, fmt.Errorf("locate output: %w", err)
		} else if inside {
			opt.Skip = map[string]bool{rel + "/": true}
		}
		router, err = parquet.NewRouter(parquet.Options{Out: cfg.Out, Schema: s, PartSize: cfg.PartSize, Encoders: cfg.Encoders})
		if err != nil {
			return sum, err
		}
		sink = router
	}
	p, err := fs.New(root, opt)
	if err != nil {
		return sum, fmt.Errorf("root: %w", err)
	}
	fail := func(err error) (Summary, error) {
		if router != nil {
			router.Abort()
			dataset.Cleanup(cfg.Out)
		}
		finish(&sum, start)
		return sum, err
	}
	ost, err := order.Run(ctx, p, sink, order.Options{Workers: cfg.Workers, Budget: sum.ReorderRows})
	sum.Listing, sum.MaxBuffered, sum.Rows = p.Stats(), ost.MaxBuffered, ost.Rows
	if err != nil {
		return fail(err)
	}
	if want := sum.Listing.Files + sum.Listing.Dirs; (cfg.NoDirs && sum.Rows != sum.Listing.Files) || (!cfg.NoDirs && sum.Rows != want) {
		return fail(fmt.Errorf("row count mismatch: listed %d, ordered %d", want, sum.Rows))
	}
	if router == nil {
		finish(&sum, start)
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
	finish(&sum, start)
	meta := dataset.Meta{SchemaID: s.ID, FileSchema: s.Message(), SortKey: s.SortOrder, SourceRoot: root}
	if err := dataset.Publish(cfg.Out, meta, parts, sum); err != nil {
		dataset.Cleanup(cfg.Out)
		return sum, fmt.Errorf("publish: %w", err)
	}
	finish(&sum, start)
	return sum, nil
}

func finish(s *Summary, start time.Time) {
	s.Seconds = time.Since(start).Seconds()
	u, sys := cpuTimes()
	s.UserSeconds, s.SysSeconds = u.Seconds(), sys.Seconds()
	s.PeakRSS = peakRSS()
}

type discard struct{}

func (discard) Write(context.Context, [][]schema.Row) error { return nil }
