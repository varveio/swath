// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortab

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/s3"
)

// Arm A is the product's pipeline as the command runs it (listing.Run:
// order stage, part router and writer, journal, resume), over the trace
// registered as the scheme "sortab". Its recovery is the product's: a
// resume keeps the journaled parts and relists after the last one's key.

var (
	armMu   sync.Mutex
	armNext *Provider
)

// aSource is the trace bound to the pipeline: resumable after a key.
type aSource struct {
	registry.Bound[s3.Object]
	p *Provider
}

func (a *aSource) Locator() source.Locator {
	return source.Locator{Kind: "sortab", Root: "sortab://" + a.p.l.Name}
}

func (a *aSource) Listed() int64 { return a.p.Listed() }

func (a *aSource) Capabilities() source.Capabilities {
	return source.Capabilities{Resume: true, Seek: source.SeekFabricable, Order: source.OrderBytes}
}

func (a *aSource) Identity() source.Identity {
	l := a.p.l
	return source.Identity{Provider: "sortab", Fields: []source.IdentityField{
		{Name: "shape", Value: fmt.Sprintf("%s/%d/%d/%d/%d/%d", l.Name, l.Keys, l.Ranges, l.PageKeys, l.SlowKeys, l.Seed)}}}
}

// Restart lists only the keys after the last published key: the range
// holding it from the next index (one new chain), later ranges whole.
func (a *aSource) Restart(after string) error {
	k, err := indexOf(after)
	if err != nil {
		return err
	}
	a.p.Start = func(r int, lo, hi int64) (int64, bool) {
		if hi <= k+1 {
			return 0, true
		}
		return max(lo, k+1), false
	}
	return nil
}

func init() {
	registry.Register(registry.Factory{Scheme: "sortab", Defaults: func() any { return nil },
		Open: func(ctx context.Context, req registry.Request) (registry.Source, error) {
			armMu.Lock()
			defer armMu.Unlock()
			if armNext == nil {
				return nil, fmt.Errorf("sortab: no trace set")
			}
			p := armNext
			armNext = nil
			return &aSource{Bound: registry.Bind[Range, s3.Object](p, &s3.Schema), p: p}, nil
		}})
}

// Limits are the execution limits both arms share.
type Limits struct {
	Workers  int   `json:"workers"`
	Memory   int64 `json:"memory"`    // arm A runs at --sort-buffer memory/2; arm B sorts within memory/4
	Encoders int   `json:"encoders"`  // Parquet encoders
	PartSize int64 `json:"part_size"` // target bytes per part
	// SpillMax caps A's live spill bytes (negative: none). B's sorted runs
	// hold every row and cannot be capped; their peak is reported.
	SpillMax     int64 `json:"spill_max"`
	SpillReserve int64 `json:"spill_reserve"`
	// PartIntervalMs is the part age bound (0: the default; negative: by
	// size only).
	PartIntervalMs int64 `json:"part_interval_ms"`
}

// runA lists p into out with the product's pipeline (a resume when
// resume is set) and returns its report.
func runA(ctx context.Context, p *Provider, out string, lim Limits, resume bool, pr *listing.Progress) (listing.Report, error) {
	armMu.Lock()
	armNext = p
	armMu.Unlock()
	cfg := listing.Config{Source: listing.SourceConfig{URI: "sortab://" + p.l.Name}, Output: listing.OutputConfig{Dir: out, Resume: resume},
		Limits: listing.Limits{Workers: lim.Workers, RowBudget: lim.Memory / 2, Encoders: lim.Encoders, PartSize: lim.PartSize,
			Spill: listing.SpillLimits{Max: lim.SpillMax, Reserve: lim.SpillReserve}},
		Argv: []string{"sortab", "A", strconv.FormatBool(resume)}, Progress: pr}
	switch {
	case lim.PartIntervalMs > 0:
		cfg.Limits.PartInterval = msDuration(lim.PartIntervalMs)
	case lim.PartIntervalMs == 0:
		cfg.Limits.PartInterval = listing.DefaultPartInterval
	}
	return listing.Run(ctx, cfg)
}
