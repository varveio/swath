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
	"syscall"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/fs"
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
			fmt.Fprintln(os.Stderr, "swath:", err)
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
	endpoint, region         string
	profile                  string
	anonymous                bool
	timeout                  time.Duration
	pathStyle, fetchOwner    bool
	fips, dualStack          bool
	seed, sequential         bool
	maxKeys                  int64
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
	f.StringVar(&o.endpoint, "endpoint", "", "S3: endpoint `URL` of an S3-compatible store")
	f.StringVar(&o.region, "region", "", "S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)")
	f.StringVar(&o.profile, "profile", "", "S3: shared config `profile` (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)")
	f.BoolVar(&o.anonymous, "anonymous", false, "S3: send unsigned requests, ignoring any credentials")
	f.DurationVar(&o.timeout, "request-timeout", 30*time.Second, "S3: per-request timeout; a request that exceeds it is retried")
	f.BoolVar(&o.pathStyle, "path-style", false, "S3: path-style addressing")
	f.BoolVar(&o.fips, "fips", false, "S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)")
	f.BoolVar(&o.dualStack, "dualstack", false, "S3: use the dual-stack (IPv6) endpoint")
	f.BoolVar(&o.fetchOwner, "fetch-owner", false, "S3: fill owner_id and owner_display_name")
	f.BoolVar(&o.seed, "seed", false, "S3: tile the keyspace from one delimiter page before splitting")
	f.BoolVar(&o.sequential, "sequential", false, "S3: list with one continuation-token chain (no ranges; the reference)")
	f.Int64Var(&o.maxKeys, "max-keys", 0, "S3: with -sequential, stop after this many objects (experiments; the dataset is marked incomplete)")
	f.BoolVar(&o.json, "json", false, "print the summary as JSON")
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
		Portable: os.Getenv("SWATH_LOCAL_ENGINE") == "portable", Argv: append([]string{"list"}, args...),
		S3: listing.S3Config{Endpoint: o.endpoint, Region: o.region, PathStyle: o.pathStyle, FIPS: o.fips, DualStack: o.dualStack,
			Profile: o.profile, Anonymous: o.anonymous, Timeout: o.timeout, Warn: func(m string) { fmt.Fprintln(os.Stderr, "swath: warning:", m) }, FetchOwner: o.fetchOwner, Seed: o.seed, Sequential: o.sequential, MaxKeys: o.maxKeys},
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
	if st := sum.S3; st != nil {
		fmt.Fprintf(stdout, "objects=%d bytes=%d pages=%d requests=%d retries=%d splits=%d child_empty=%d max_active=%d truncated=%v ",
			st.Objects, st.Bytes, st.Pages, st.Requests, st.Retries, st.Split.AcceptedBounded+st.Split.AcceptedFrontier,
			st.ChildEmpty.Lower+st.ChildEmpty.Upper, st.MaxActive, st.Truncated)
	}
	if st := sum.Listing; st != nil {
		fmt.Fprintf(stdout, "files=%d dirs=%d bytes=%d errors=%d ", st.Files, st.Dirs, st.Bytes, st.Errors())
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
