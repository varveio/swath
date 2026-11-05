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
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/parquet"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/fs"
	"github.com/varveio/swath/go/internal/source/s3"
)

// Config is one listing. Zero values take the documented defaults.
type Config struct {
	Root     string // a local directory, or s3://bucket[/prefix]
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
	S3       S3Config
}

// S3Config configures an s3:// root.
type S3Config struct {
	Endpoint, Region string
	PathStyle        bool
	Profile          string        // shared-file profile
	Anonymous        bool          // never sign
	Timeout          time.Duration // per request (0: 30 s)
	Warn             func(string)  // one-time warnings (nil: dropped)
	FetchOwner       bool
	Sequential       bool  // one continuation-token chain (the reference oracle)
	Seed             bool  // tile the scope from one delimiter page first
	MaxKeys          int64 // stop after this many objects (sequential only; experiments)
	Client           s3.API
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
	Fields      string    `json:"fields,omitempty"`
	Workers     int       `json:"workers"`
	Encoders    int       `json:"encoders"`
	ReorderRows int       `json:"reorder_rows"`
	MaxBuffered int64     `json:"max_buffered_rows"`
	Listing     *fs.Stats `json:"listing,omitempty"`
	S3          *s3.Stats `json:"s3,omitempty"`
	Rows        int64     `json:"rows"`
	Parts       int       `json:"parts"`
	OutBytes    int64     `json:"output_bytes"`
	Seconds     float64   `json:"seconds"`
	UserSeconds float64   `json:"user_cpu_seconds"`
	SysSeconds  float64   `json:"sys_cpu_seconds"`
	PeakRSS     int64     `json:"peak_rss_bytes"`
	Started     time.Time `json:"started"`
}

// Errors is the listing's error count.
func (s *Summary) Errors() int64 {
	if s.Listing != nil {
		return s.Listing.Errors()
	}
	return 0
}

// Run lists cfg.Root and, unless cfg.Out is empty, publishes the dataset.
// On any error or cancellation nothing is published and unpublished parts
// are removed; the error is the cause (ctx.Err() on cancellation).
func Run(ctx context.Context, cfg Config) (Summary, error) {
	start := time.Now()
	if err := cfg.setDefaults(); err != nil {
		return Summary{}, err
	}
	sum := Summary{
		Tool: "swath-local", Out: cfg.Out, Workers: cfg.Workers, Encoders: cfg.Encoders,
		ReorderRows: int(cfg.Memory / bytesPerBufferedRow), Started: start,
	}
	if strings.HasPrefix(cfg.Root, "s3://") {
		return runS3(ctx, cfg, sum)
	}
	return runFS(ctx, cfg, sum)
}

func runFS(ctx context.Context, cfg Config, sum Summary) (Summary, error) {
	root, err := filepath.Abs(cfg.Root)
	if err != nil {
		return sum, err
	}
	s := &schema.Local
	sum.Root, sum.SchemaID, sum.Fields = root, s.ID, cfg.Fields.String()
	opt := fs.Options{Fields: cfg.Fields, NoDirs: cfg.NoDirs, LinkTarget: !cfg.NoLinks, Portable: cfg.Portable}
	if _, err := fs.New(root, opt); err != nil { // validate the root before touching the output
		return sum, fmt.Errorf("root: %w", err)
	}
	if cfg.Out != "" {
		if err := dataset.Prepare(cfg.Out, root, s.ID, cfg.Replace); err != nil {
			return sum, fmt.Errorf("prepare output: %w", err)
		}
		if rel, inside, err := dataset.LocateInside(root, cfg.Out); err != nil {
			return sum, fmt.Errorf("locate output: %w", err)
		} else if inside {
			opt.Skip = map[string]bool{rel + "/": true}
		}
	}
	p, err := fs.New(root, opt)
	if err != nil {
		return sum, fmt.Errorf("root: %w", err)
	}
	meta := dataset.Meta{SchemaID: s.ID, FileSchema: s.Message(), SortKey: s.SortOrder, SourceKind: "local-fs", SourceRoot: root}
	listed := func(sum *Summary) int64 {
		st := p.Stats()
		sum.Listing = &st
		if cfg.NoDirs {
			return st.Files
		}
		return st.Files + st.Dirs
	}
	return pipeline(ctx, cfg, sum, p, s, meta, listed)
}

func runS3(ctx context.Context, cfg Config, sum Summary) (Summary, error) {
	bucket, prefix, err := s3.ParseURL(cfg.Root)
	if err != nil {
		return sum, err
	}
	s := &schema.S3
	sum.Root, sum.SchemaID = cfg.Root, s.ID
	p, err := s3.New(ctx, s3.Options{
		Bucket: bucket, Prefix: prefix, Endpoint: cfg.S3.Endpoint, Region: cfg.S3.Region, PathStyle: cfg.S3.PathStyle,
		FetchOwner: cfg.S3.FetchOwner, Sequential: cfg.S3.Sequential, Seed: cfg.S3.Seed, Workers: cfg.Workers,
		MaxKeys: cfg.S3.MaxKeys, Client: cfg.S3.Client,
		Profile: cfg.S3.Profile, Anonymous: cfg.S3.Anonymous, Timeout: cfg.S3.Timeout, Warn: cfg.S3.Warn,
	})
	if err != nil {
		return sum, err
	}
	if cfg.Out != "" {
		if err := dataset.Prepare(cfg.Out, "", s.ID, cfg.Replace); err != nil {
			return sum, fmt.Errorf("prepare output: %w", err)
		}
	}
	// The Swath run summary records the listed target, so a reader can tell
	// a whole-bucket dataset from a prefix (or truncated) one.
	run := dataset.RunSummary{SchemaVersion: 2, Completed: true}
	run.Config.Target, run.Config.Filters = "s3://"+bucket, []string{}
	if prefix != "" {
		run.Config.Target += "/" + prefix
	}
	meta := dataset.Meta{SchemaID: s.ID, FileSchema: s.Message(), SortKey: s.SortOrder, SourceKind: "s3",
		SourceBucket: bucket, SourceRoot: cfg.Root, SwathSummary: &run}
	listed := func(sum *Summary) int64 {
		st := p.Stats()
		sum.S3 = &st
		run.Completed = !st.Truncated
		return st.Objects
	}
	return pipeline(ctx, cfg, sum, p, s, meta, listed)
}

// pipeline runs provider → order → router → publish for one provider.
func pipeline[R any](ctx context.Context, cfg Config, sum Summary, p source.Provider[R], s *schema.Schema, meta dataset.Meta, listed func(*Summary) int64) (Summary, error) {
	var sink order.Sink = discard{}
	var router *parquet.Router
	if cfg.Out != "" {
		var err error
		router, err = parquet.NewRouter(parquet.Options{Out: cfg.Out, Schema: s, PartSize: cfg.PartSize, Encoders: cfg.Encoders})
		if err != nil {
			return sum, err
		}
		sink = router
	}
	fail := func(err error) (Summary, error) {
		if router != nil {
			router.Abort()
			dataset.Cleanup(cfg.Out)
		}
		finish(&sum)
		return sum, err
	}
	ost, err := order.Run(ctx, p, sink, order.Options{Workers: cfg.Workers, Budget: sum.ReorderRows})
	sum.MaxBuffered, sum.Rows = ost.MaxBuffered, ost.Rows
	want := listed(&sum)
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
	if err := dataset.Publish(cfg.Out, meta, parts, sum); err != nil {
		dataset.Cleanup(cfg.Out)
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
