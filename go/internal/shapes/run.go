// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package shapes

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/source/scalefake"
)

// Arm is one listing of a shape.
type Arm struct {
	Name    string
	Shape   string
	Scale   int // the shape's scale (Shape.Keys)
	Workers int
	// Fake is the service: its latency and behaviours. Its Bucket is
	// ignored.
	Fake scalefake.Options
	// Flags are the S3 provider's and the engine's flags, as on the
	// command line (diagnostic ones included: "-seed=false",
	// "-limiter=false", "-slow-split"); the rest is the command's defaults.
	Flags []string
	// Out, if set, is a dataset directory to write (it must not exist);
	// otherwise the run counts only. Parts are cut by size only, so the
	// dataset is the same for the same keys at any worker count.
	Out string
	// RTT is the mean page latency the ideal wall time assumes; 0: the
	// fake's Latency plus half its Jitter.
	RTT time.Duration
}

// Result is what one arm measured: the counters that explain its wall
// time, and whether its output is the shape's keys.
type Result struct {
	Name    string `json:"name"`
	Shape   string `json:"shape"`
	Keys    int    `json:"keys"`
	Workers int    `json:"workers"`
	// WallMs is the listing's (report timings.listing_ms); IdealMs is
	// (pages/workers + 7 + seed round trips) x RTT, and Ratio their
	// quotient: what an arm loses to its shape and service.
	WallMs  int64   `json:"wall_ms"`
	IdealMs float64 `json:"ideal_ms"`
	Ratio   float64 `json:"wall_over_ideal"`
	// RequestsPer1k is the engine's list requests per 1,000 keys;
	// AttemptsPer1k every HTTP attempt's (retries and throttled ones
	// included).
	RequestsPer1k float64 `json:"requests_per_1k"`
	AttemptsPer1k float64 `json:"attempts_per_1k"`
	Pages         int64   `json:"pages"`
	Depth         int64   `json:"split_max_depth"`
	MeanActive    float64 `json:"mean_active"`
	Splits        int64   `json:"splits"`
	EmptyChildren int64   `json:"empty_children"`
	SeedTrips     int64   `json:"seed_round_trips"`
	Throttles     int64   `json:"throttles"`
	Rows          int64   `json:"rows"`
	// Identical: the dataset's keys are the shape's, in order (Out arms).
	Identical *bool `json:"identical,omitempty"`
}

// Run lists keys (the shape's, sorted) as the arm says, through a fake
// bucket served in process, and measures it. The keyspace may be shared
// between arms.
func Run(ctx context.Context, ks *scalefake.Keyspace, keys []string, a Arm) (Result, error) {
	fake := a.Fake
	fake.Bucket = "bkt"
	srv := httptest.NewServer(scalefake.NewServer(ks, fake))
	defer srv.Close()
	f, err := registry.Lookup("s3://bkt")
	if err != nil {
		return Result{}, err
	}
	fs := flag.NewFlagSet(a.Name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := f.Flags(fs, true, f.SharedFlags(fs, true))
	args := append([]string{"-endpoint", srv.URL, "-anonymous", "-region", "us-east-1"}, a.Flags...)
	if err := fs.Parse(args); err != nil {
		return Result{}, fmt.Errorf("%s: flags %q: %w", a.Name, a.Flags, err)
	}
	if fs.NArg() > 0 {
		return Result{}, fmt.Errorf("%s: stray arguments %q", a.Name, fs.Args())
	}
	cfg := listing.Config{Source: listing.SourceConfig{URI: "s3://bkt", Options: opts},
		Limits: listing.Limits{Workers: a.Workers, PartInterval: -1}}
	if a.Out != "" {
		cfg.Output.Dir, cfg.Output.NoJournal = a.Out, true
	}
	rep, err := listing.Run(ctx, cfg)
	if err != nil {
		return Result{}, fmt.Errorf("%s: %w", a.Name, err)
	}
	var st *s3.Stats
	if sr := rep.SourceReport(); sr != nil {
		st, _ = sr.Stats.(*s3.Stats)
	}
	if st == nil {
		return Result{}, fmt.Errorf("%s: no S3 statistics in the report", a.Name)
	}
	v := st.Counters.Value
	n := max(1, float64(len(keys)))
	r := Result{Name: a.Name, Shape: a.Shape, Keys: len(keys), Workers: a.Workers, WallMs: rep.Timings.ListingMs,
		RequestsPer1k: float64(v("requests")) * 1000 / n, Pages: v("pages"), Depth: v("split.max_depth"),
		MeanActive: st.MeanActive, Splits: v("split.accepted_bounded") + v("split.accepted_frontier"),
		EmptyChildren: v("child_empty.lower") + v("child_empty.upper"), SeedTrips: v("seed.round_trips"),
		Rows: rep.Output.RowsEmitted}
	if rep.Requests != nil {
		r.AttemptsPer1k = float64(rep.Requests.HTTPAttempts) * 1000 / n
		r.Throttles = rep.Requests.Throttles
	}
	rtt := a.RTT
	if rtt == 0 {
		rtt = fake.Latency + fake.Jitter/2
	}
	pageKeys := 1000
	if fake.PageCap > 0 {
		pageKeys = min(pageKeys, fake.PageCap)
	}
	pages := float64(len(keys)) / float64(pageKeys)
	r.IdealMs = (pages/float64(a.Workers) + 7 + float64(r.SeedTrips)) * float64(rtt) / float64(time.Millisecond)
	if r.IdealMs > 0 {
		r.Ratio = float64(r.WallMs) / r.IdealMs
	}
	if a.Out != "" {
		got, err := DatasetKeys(a.Out)
		if err != nil {
			return r, fmt.Errorf("%s: %w", a.Name, err)
		}
		same := slices.Equal(got, keys)
		r.Identical = &same
	}
	return r, nil
}

// DatasetKeys reads the keys of a committed dataset's parts, in the
// manifest's order.
func DatasetKeys(dir string) ([]string, error) {
	b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
	if err != nil {
		return nil, err
	}
	var m dataset.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	var keys []string
	for _, p := range m.Parts {
		if keys, err = partKeys(filepath.Join(dir, filepath.FromSlash(p.Path)), keys); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func partKeys(path string, keys []string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	pf, err := pq.OpenFile(f, fi.Size())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	r := pq.NewReader(pf)
	defer r.Close()
	buf := make([]pq.Row, 512)
	for {
		n, err := r.ReadRows(buf)
		for _, row := range buf[:n] {
			keys = append(keys, string(row[0].ByteArray()))
		}
		if errors.Is(err, io.EOF) {
			return keys, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}
}
