// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/varveio/swath/internal/source"
)

// TilePages is the pages a counted prior's tile holds in a planned run
// (Planner): its keys fill that many requests less one key, so a fresh
// tile costs that many requests. With 1,000-key requests and 1,024-row key
// pages a tile of q requests holds q-1 whole key pages: at 32, 31,744 keys
// in 32 requests, 0.8 % more requests than whole pages would take (16: 4.2
// %); a deleted region costs one request per tile. Options.TilePages
// overrides it (swath diag list --seed-from-tile-pages).
const TilePages = 32

// maxTilePages bounds --seed-from-tile-pages: a tile of 4,096 pages is
// four million keys at 1,000 a page.
const maxTilePages = 4096

// Planner makes a counted prior's tiling a plan the order stage admits
// tile by tile (source.Planning): Root becomes the plan's, and the tiles
// are made lazily from the prior's cuts in key order. Without a counted
// prior there is none, and Root is as before; order.Count never asks, so a
// count tiles the prior at once (expandPrior).
func (p *Engine[P]) Planner() source.Planner[Range] {
	pr := p.opt.Prior
	if pr == nil || !pr.Counted || p.opt.Sequential {
		return nil
	}
	if p.plan == nil {
		q := p.opt.TilePages
		if q <= 0 {
			q = TilePages
		}
		stream := pr.Stream
		if stream == nil {
			stream = &cutSlice{cuts: pr.Cuts}
		}
		p.plan = &plan[P]{p: p, stream: stream, fill: float64(q*p.opt.PageSize - 1), after: p.resumeKey(), first: true}
		p.plan.lo, p.plan.last = p.plan.after, p.plan.after
		p.c.Set(cSeedFromTilePages, int64(q))
		p.c.Set(cSeedFromParts, int64(pr.Parts))
	}
	return p.plan
}

// plan is a counted prior's tiling made lazily (Engine.Planner), in key
// order: each cut read is checked (source.CheckCut, ascending), cuts at or
// below the resume key dropped, a repeated key merged into its cut (only
// once a greater key or the end shows the cut complete, across batches), an
// unsafe cut replaced by its longest accepted prefix or merged into the
// next, as tilePrior does; a tile ends at the last cut that keeps it at
// most fill keys, or at its first when that alone holds more, and the open
// frontier follows the last. Reused certificates are cut out of each tile
// as it is made (cutReuse, one state across tiles). Only the order stage
// calls Next, under its lock; the engine's state is changed under p.mu.
type plan[P any] struct {
	p      *Engine[P]
	stream source.CutStream
	fill   float64
	after  string // the resume key ("" none)

	read    int        // cuts read
	prev    string     // the last cut read
	cut     source.Cut // the cut being merged: complete once a greater key comes
	hasCut  bool
	pending float64 // unsafe cuts' keys, merged into the next usable cut
	last    string  // the last usable cut (an unsafe cut's prefix must sort after it)

	lo     string  // the tile being filled starts after lo
	acc    float64 // its keys
	hi     string  // its last usable cut
	filled bool    // it holds a cut
	first  bool    // no range made yet: the first starts at the resume key's safe start

	reuse reuseCut
	ready []Range // ranges made and not yet admitted
	eof   bool
	err   error
}

// Start reads the prior ahead (source.Planner).
func (pl *plan[P]) Start(ctx context.Context, wake func()) func() {
	return pl.stream.Start(ctx, pl.after, wake)
}

// RowBytes is the prior's stored bytes per row (source.Planner).
func (pl *plan[P]) RowBytes() float64 {
	if pr := pl.p.opt.Prior; pr.Rows > 0 {
		return float64(pr.Bytes) / float64(pr.Rows)
	}
	return 0
}

// Next hands out the next range of the plan if admit takes its expected
// keys (source.Planner); called under the order stage's lock only.
func (pl *plan[P]) Next(admit func(rows int64) bool) (Range, bool, bool, error) {
	p := pl.p
	for len(pl.ready) == 0 {
		switch {
		case pl.err != nil:
			return Range{}, false, false, pl.err
		case pl.eof:
			return Range{}, false, true, nil
		}
		b, err := pl.stream.Next()
		switch {
		case errors.Is(err, io.EOF):
			pl.end()
		case err != nil:
			pl.fail(fmt.Errorf("--seed-from: %w", err))
		case b == nil:
			return Range{}, false, false, nil
		default:
			pl.batch(b)
		}
	}
	if pl.err != nil {
		return Range{}, false, false, pl.err
	}
	r := pl.ready[0]
	rows := int64(0)
	if r.prior {
		rows = int64(r.est)
	}
	if !admit(rows) {
		return Range{}, false, false, nil
	}
	pl.ready[0] = Range{}
	pl.ready = pl.ready[1:]
	p.lock()
	p.account(time.Now())
	p.queued += p.queue([]Range{r})
	p.c.Max(cMaxQueued, int64(p.queued))
	if p.opt.Resume != nil {
		p.c.Inc(cResumedRanges)
	}
	p.unlock()
	if p.tr != nil {
		p.traceTiles([]Range{r})
	}
	return r, true, false, nil
}

// fail latches the plan's failure: every later Next returns it.
func (pl *plan[P]) fail(err error) {
	if pl.err == nil {
		pl.err = err
		pl.p.c.Inc(cSeedFromPlanErrors)
	}
}

// batch takes a batch of cuts.
func (pl *plan[P]) batch(b *source.CutBatch) {
	p := pl.p
	if b.Part {
		if b.Indexed {
			p.c.Inc(cSeedFromIndexed)
		} else {
			p.c.Inc(cSeedFromIndexFailed)
		}
	}
	for _, c := range b.Cuts {
		pl.read++
		if err := source.CheckCut(c, pl.read, p.opt.Prefix, maxKeyBytes); err != nil {
			pl.fail(err)
			return
		}
		if pl.read > 1 && c.Key < pl.prev {
			pl.fail(fmt.Errorf("%w: cut %d %q sorts before cut %d %q", source.ErrPrior, pl.read, c.Key, pl.read-1, pl.prev))
			return
		}
		pl.prev = c.Key
		p.c.Inc(cSeedFromCuts)
		switch {
		case c.Key <= pl.after: // published: its keys are not expected
		case pl.hasCut && c.Key == pl.cut.Key:
			pl.cut.Rows += c.Rows
			p.c.Inc(cSeedFromMerged)
		default:
			if pl.hasCut {
				pl.take(pl.cut)
			}
			pl.cut, pl.hasCut = c, true
		}
	}
}

// take adds a complete cut to the tile being filled, closing the tile
// first when the cut would take it past fill.
func (pl *plan[P]) take(c source.Cut) {
	p := pl.p
	w, key := float64(c.Rows), c.Key
	if !p.safe(key) {
		p.c.Inc(cSeedFromUnsafe)
		key = p.safeStart(key)
		if key <= pl.last || !strings.HasPrefix(key, p.opt.Prefix) {
			pl.pending += w
			return
		}
	}
	mass := pl.pending + w
	pl.pending, pl.last = 0, key
	if pl.acc > 0 && pl.acc+mass > pl.fill {
		pl.close()
	}
	pl.acc += mass
	pl.hi, pl.filled = key, true
}

// close makes the tile (lo, hi] with the keys it holds.
func (pl *plan[P]) close() {
	p := pl.p
	r := pl.tile(Range{lo: pl.lo, hi: pl.hi, est: pl.acc, estOK: true, prior: true})
	p.lock()
	p.tiles = append(p.tiles, tileAccount{est: pl.acc})
	r.tile = int32(len(p.tiles))
	p.unlock()
	p.c.Add(cSeedFromExpected, int64(pl.acc))
	pl.lo, pl.acc, pl.filled = pl.hi, 0, false
	pl.ready = p.cutReuse(&pl.reuse, r, pl.ready)
}

// end closes the plan at the end of the prior: its last cut completes, its
// last tile closes, and the open frontier after it lists what the prior
// did not know.
func (pl *plan[P]) end() {
	if pl.hasCut {
		pl.take(pl.cut)
		pl.hasCut = false
	}
	if pl.filled {
		pl.close()
	}
	r := pl.tile(Range{lo: pl.lo, open: true})
	pl.ready = pl.p.cutReuse(&pl.reuse, r, pl.ready)
	pl.eof = true
}

// tile gives r its identity; the first range starts at the resume key's
// longest accepted prefix and drops the keys up to the key itself.
func (pl *plan[P]) tile(r Range) Range {
	p := pl.p
	r.id, r.born = p.ids.Add(1), fromSeed
	if pl.first {
		pl.first = false
		if s := p.safeStart(pl.after); s != pl.after {
			r.lo, r.after = s, pl.after
		}
	}
	p.c.Inc(cSeedFromTiles)
	return r
}

// cutSlice is a prior's cuts held in memory (a cut file) as one batch.
type cutSlice struct {
	cuts []source.Cut
	sent bool
}

func (s *cutSlice) Start(context.Context, string, func()) func() { return func() {} }

func (s *cutSlice) Next() (*source.CutBatch, error) {
	if s.sent {
		return nil, io.EOF
	}
	s.sent = true
	return &source.CutBatch{Cuts: s.cuts}, nil
}
