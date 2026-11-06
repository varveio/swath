// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"

	"github.com/varveio/swath/go/internal/source"
)

const (
	seedDelimiter = "/"
	maxSeeds      = 1000 // the most cuts a seed makes
)

// expandSeed sends one delimiter page and tiles the scope at its common
// prefixes; it emits no rows (the tiles list every key, including the
// page's own objects).
//
// Swath's hybrid seed descent (a walk of the folder hierarchy that
// classified each truncated level, sampled ambiguous folders and cut dense
// flat folders into radix bands) was ported and measured here and deleted:
// it gained nothing on any recording and cost 30-60 % more requests on the
// shallow ones (REPORT.md "Seed descent" and stage D). Git keeps it
// (seedplan.go before its deletion).
func (p *Engine[P]) expandSeed(ctx context.Context, e source.Emitter[Range, P]) error {
	in := p.query()
	in.Probe, in.Delimiter = true, seedDelimiter
	out, err := p.list(ctx, in)
	if err != nil {
		return err
	}
	var cuts []string
	p.lock()
	p.c.Set(cSeedPrefixes, int64(len(out.Prefixes)))
	seedTruncated := out.Truncated
	if seedTruncated {
		p.c.Set(cSeedTruncated, 1)
	}
	for _, k := range out.Prefixes {
		if p.safe(k) {
			cuts = append(cuts, k)
		} else {
			p.c.Inc(cSeedUnsafe)
		}
	}
	if limit := min(maxSeeds, 4*p.opt.Workers); len(cuts) > limit {
		picked := make([]string, 0, limit)
		for i := range limit {
			picked = append(picked, cuts[(i*len(cuts)+limit/2)/limit])
		}
		cuts = picked
		p.c.Set(cSeedSubsampled, 1)
	}
	p.c.Set(cSeedCuts, int64(len(cuts)))
	// Closing sentinel: when the page holds the whole top level and ends with
	// a prefix, bound the last tile at prefixCeil(pk) so it is not the open
	// frontier (the bounded midpoint then applies to it).
	var sentinel string
	if k := len(cuts); k > 0 {
		pk := cuts[k-1]
		lastObject := ""
		if n := len(out.Rows); n > 0 {
			lastObject = out.Rows[n-1].Key
		}
		lastPrefix := out.Prefixes[len(out.Prefixes)-1]
		u, ok := prefixCeil(pk)
		switch {
		case seedTruncated:
			p.c.Inc(cSentinelDeclinedTruncated)
		case lastObject > lastPrefix || pk != lastPrefix:
			p.c.Inc(cSentinelDeclinedObjectLast)
		case !ok || u <= pk:
			p.c.Inc(cSentinelDeclinedNoCeiling)
		case !p.safe(u):
			p.c.Inc(cSentinelDeclinedUnsafe)
		default:
			sentinel = u
			p.c.Set(cSeedSentinel, 1)
		}
	}
	tiles := make([]Range, 0, len(cuts)+2)
	lo := ""
	for _, c := range cuts {
		tiles = append(tiles, Range{lo: lo, hi: c, id: p.ids.Add(1), born: fromSeed})
		lo = c
	}
	if sentinel != "" {
		tiles = append(tiles, Range{lo: lo, hi: sentinel, id: p.ids.Add(1), born: fromSeed})
		lo = sentinel
	}
	tiles = append(tiles, Range{lo: lo, open: true, id: p.ids.Add(1), born: fromSeed})
	p.queued += len(tiles)
	p.c.Max(cMaxQueued, int64(p.queued))
	p.c.Inc(cPages)
	p.unlock()
	for _, t := range tiles {
		if err := e.Child(t); err != nil {
			return err
		}
	}
	return nil
}
