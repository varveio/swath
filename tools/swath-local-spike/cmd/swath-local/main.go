// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command swath-local lists a local directory tree and writes a globally
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
	"syscall"

	"github.com/varveio/swath/tools/swath-local-spike/internal/dataset"
	"github.com/varveio/swath/tools/swath-local-spike/internal/listing"
	"github.com/varveio/swath/tools/swath-local-spike/internal/source/fs"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) < 2 || os.Args[1] != "list" {
		usage(os.Stderr, newFlags(&options{}))
		os.Exit(2)
	}
	if err := run(ctx, os.Args[2:], os.Stdout); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "swath-local:", err)
		}
		os.Exit(1)
	}
}

type options struct {
	out, fields              string
	overwrite, noDirs, noLnk bool
	countOnly, json          bool
	workers, encoders        int
	partSize, memory         size
}

func newFlags(o *options) *flag.FlagSet {
	f := flag.NewFlagSet("list", flag.ContinueOnError)
	f.StringVar(&o.out, "out", "", "output dataset `directory` (required unless -count-only)")
	f.BoolVar(&o.overwrite, "overwrite", false, "replace a previous swath-local dataset in -out")
	f.StringVar(&o.fields, "fields", "stat", "metadata per entry: names (no stat), stat, statx (adds birth_time; Linux)")
	f.BoolVar(&o.noDirs, "no-dirs", false, "omit kind=DIR rows")
	f.BoolVar(&o.noLnk, "no-link-target", false, "do not read symlink targets")
	o.partSize = listing.DefaultPartSize
	f.Var(&o.partSize, "part-size", "target `bytes` per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB)")
	o.memory = listing.DefaultMemory
	f.Var(&o.memory, "memory", "`bytes` of rows held between listing and encoding; sets the reorder bound")
	f.IntVar(&o.workers, "workers", 0, "listing goroutines (default min(64, max(4, 3*GOMAXPROCS)))")
	f.IntVar(&o.encoders, "encoders", 0, "Parquet encoder goroutines (default GOMAXPROCS)")
	f.BoolVar(&o.countOnly, "count-only", false, "walk and count without writing a dataset")
	f.BoolVar(&o.json, "json", false, "print the summary as JSON")
	f.SetOutput(io.Discard)
	return f
}

// usage prints the flags generated from the flag set itself.
func usage(w io.Writer, f *flag.FlagSet) {
	fmt.Fprintln(w, "usage: swath-local list <root> [flags]")
	fmt.Fprintln(w)
	f.SetOutput(w)
	f.PrintDefaults()
	f.SetOutput(io.Discard)
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
	fields, err := fs.ParseFields(o.fields)
	if err != nil {
		return fmt.Errorf("-fields: %w", err)
	}
	cfg := listing.Config{
		Root: root, Replace: o.overwrite, Fields: fields, NoDirs: o.noDirs, NoLinks: o.noLnk,
		Workers: o.workers, Encoders: o.encoders, Memory: int64(o.memory), PartSize: int64(o.partSize),
		Portable: os.Getenv("SWATH_LOCAL_ENGINE") == "portable",
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
	st := sum.Listing
	fmt.Fprintf(stdout, "files=%d dirs=%d bytes=%d errors=%d rows=%d parts=%d out_bytes=%d total=%.3fs user=%.2fs sys=%.2fs peak_rss=%.1fMiB max_buffered=%d\n",
		st.Files, st.Dirs, st.Bytes, st.Errors(), sum.Rows, sum.Parts, sum.OutBytes, sum.Seconds,
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
