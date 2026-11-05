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
	"github.com/varveio/swath/go/internal/durable"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/registry"

	// Providers register themselves (scheme and flag group); the command
	// imports them for nothing else.
	_ "github.com/varveio/swath/go/internal/source/fs"
	_ "github.com/varveio/swath/go/internal/source/s3"
)

// shutdownGrace bounds the time an interrupted run spends stopping.
const shutdownGrace = 15 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) == 3 && os.Args[1] == "inspect" {
		if err := durable.Dump(os.Stdout, os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "swath:", err)
			os.Exit(1)
		}
		return
	}
	killAt(os.Getenv("SWATH_TEST_KILL_AT"))
	if len(os.Args) < 2 || os.Args[1] != "list" {
		usage(os.Stderr, newFlags(&options{}))
		os.Exit(2)
	}
	finished := make(chan struct{})
	defer close(finished)
	go func() { // bounded shutdown: an interrupted run gets 15 s to clean up
		select {
		case <-finished:
			return
		case <-ctx.Done():
		}
		select {
		case <-finished:
		case <-time.After(shutdownGrace):
			fmt.Fprintf(os.Stderr, "swath: still stopping after %s; exiting (an interrupted S3 run can be resumed)\n", shutdownGrace)
			os.Exit(130)
		}
	}()
	if err := run(ctx, os.Args[2:], os.Stdout); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "swath:", err)
		}
		os.Exit(1)
	}
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
	countOnly, json        bool
	workers, encoders      int
	partSize, memory       size
	spillMax, spillReserve zsize
	resume, noJournal      bool
	noSpill, spillCompress bool
	sources                map[string]any // each provider's options value, by scheme
}

func newFlags(o *options) *flag.FlagSet {
	f := flag.NewFlagSet("list", flag.ContinueOnError)
	f.StringVar(&o.out, "out", "", "output dataset `directory` (required unless -count-only)")
	f.BoolVar(&o.overwrite, "overwrite", false, "replace a previous swath-local dataset in -out")
	o.partSize = listing.DefaultPartSize
	f.Var(&o.partSize, "part-size", "target `bytes` per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB)")
	o.memory = listing.DefaultMemory
	f.Var(&o.memory, "memory", "`bytes` of rows held between listing and encoding; sets the reorder bound")
	f.IntVar(&o.workers, "workers", 0, "listing goroutines (default min(64, max(4, 3*GOMAXPROCS)))")
	f.IntVar(&o.encoders, "encoders", 0, "Parquet encoder goroutines (default GOMAXPROCS)")
	f.BoolVar(&o.countOnly, "count-only", false, "walk and count without writing a dataset")
	f.BoolVar(&o.noSpill, "no-spill", false, "wait on the reorder budget instead of spilling rows ahead of the frontier to disk")
	f.BoolVar(&o.spillCompress, "spill-compress", false, "compress spill files (zstd level 1)")
	f.Var(&o.spillMax, "spill-max", "cap on spill `bytes` on disk (0: no cap); past it, listing waits instead of spilling")
	o.spillReserve = 1 << 30
	f.Var(&o.spillReserve, "spill-reserve", "free `bytes` spill leaves on its filesystem, for the parts still to be written")
	f.BoolVar(&o.resume, "resume", false, "S3: continue the interrupted run in -out (same source and output settings)")
	f.BoolVar(&o.noJournal, "no-journal", false, "S3: publish parts only at the end, without the journal that -resume needs")
	f.BoolVar(&o.json, "json", false, "print the summary as JSON")
	// Each provider owns its flag group.
	o.sources = map[string]any{}
	for _, p := range registry.Factories() {
		o.sources[p.Scheme] = p.Flags(f)
	}
	f.SetOutput(io.Discard)
	return f
}

// usage prints the flags generated from the flag set itself.
func usage(w io.Writer, f *flag.FlagSet) {
	fmt.Fprintln(w, "usage: swath list <root> [flags]")
	fmt.Fprintln(w, "  <root> is a local directory or s3://bucket[/prefix]")
	fmt.Fprintln(w)
	f.SetOutput(w)
	f.PrintDefaults()
	f.SetOutput(io.Discard)
	fmt.Fprintln(w, "\nswath inspect <out> prints an S3 run's journal (.swath/journal) as JSON lines.")
	fmt.Fprintln(w, "\nProfiling: SWATH_LOCAL_CPUPROFILE=FILE and SWATH_LOCAL_MEMPROFILE=FILE.")
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	var o options
	f := newFlags(&o)
	var root string
	for rest := args; ; {
		if err := f.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				usage(stdout, f)
			}
			return err
		}
		if f.NArg() == 0 {
			break
		}
		if root != "" {
			return fmt.Errorf("unexpected argument %q", f.Arg(0))
		}
		root, rest = f.Arg(0), f.Args()[1:]
	}
	if root == "" {
		return errors.New("missing <root>")
	}
	if !o.countOnly && o.out == "" {
		return errors.New("-out is required unless -count-only")
	}
	pf, err := registry.Lookup(root)
	if err != nil {
		return err
	}
	cfg := listing.Config{
		Root: root, Replace: o.overwrite, Source: o.sources[pf.Scheme],
		Workers: o.workers, Encoders: o.encoders, Memory: int64(o.memory), PartSize: int64(o.partSize),
		Argv: append([]string{"list"}, args...), Resume: o.resume, NoJournal: o.noJournal,
		NoSpill: o.noSpill, SpillCompress: o.spillCompress, SpillMax: int64(o.spillMax), SpillReserve: int64(o.spillReserve),
		Warn: func(m string) { fmt.Fprintln(os.Stderr, "swath: warning:", m) },
	}
	if !o.countOnly {
		cfg.Out = o.out
	}
	stopProfile, err := profile()
	if err != nil {
		return err
	}
	sum, err := listing.Run(ctx, cfg)
	stopProfile()
	if err != nil {
		return explain(err)
	}
	if o.json {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(sum)
	}
	if r := sum.Report(); r != nil && r.Brief != "" {
		fmt.Fprint(stdout, r.Brief, " ")
	}
	fmt.Fprintf(stdout, "rows=%d parts=%d out_bytes=%d total=%.3fs user=%.2fs sys=%.2fs peak_rss=%.1fMiB max_buffered=%d\n",
		sum.Rows, sum.Parts, sum.OutBytes, sum.Seconds,
		sum.UserSeconds, sum.SysSeconds, float64(sum.PeakRSS)/(1<<20), sum.MaxBuffered)
	return nil
}

// explain maps package errors to flag names.
func explain(err error) error {
	switch {
	case errors.Is(err, dataset.ErrNotEmpty):
		return fmt.Errorf("%w (use -overwrite to replace a previous swath-local dataset)", err)
	case errors.Is(err, dataset.ErrOutIsRoot), errors.Is(err, dataset.ErrInsideRoot), errors.Is(err, dataset.ErrNotDataset):
		return fmt.Errorf("-out: %w", err)
	}
	return err
}

// profile starts the CPU profile and arranges the allocation profile named
// by the environment, for development.
func profile() (stop func(), err error) {
	stops := []func(){}
	if path := os.Getenv("SWATH_LOCAL_CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return nil, err
		}
		stops = append(stops, func() { pprof.StopCPUProfile(); f.Close() })
	}
	if path := os.Getenv("SWATH_LOCAL_MEMPROFILE"); path != "" {
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
