// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
)

// Parallel core: Swath's range tiling, seed, demand-driven owner split
// and safe pivot (docs/internals/range-engine.md).
//
// A range is (lo, hi]: the lower bound is exclusive, so a boundary key belongs
// to the left range. lo "" is ⊥; open marks the frontier (hi = ⊤). The
// order stage makes a range's children claimable only when its Expand
// returns, so splits are owner-side: at a page boundary, when workers are
// idle, the owner emits (cursor, pivot] and (pivot, hi] and returns.

type kind uint8

const (
	interval kind = iota
	whole         // sequential oracle: the scope as one continuation chain
	seed          // delimiter listings of the folder hierarchy that emit the first tiling (of (after, ⊤] on a resume)
	resume        // (after, ⊤] after a journaled run's last published key, without the seed
	warm          // the tiling at a prior's cuts (SeedFrom), of (after, ⊤] on a resume
	reused        // a reused coverage certificate: the order stage replays it (Replay), the engine never lists it
)

type origin uint8

const (
	fromRoot origin = iota
	fromSeed
	fromLower
	fromUpper
)

// pivotSource is the synthesis that made a split's pivot; srcBound marks a
// bound no split made (a seed cut, the scope's end, ⊤, a resume key).
type pivotSource uint8

const (
	srcBound     pivotSource = iota
	srcReflect               // forwardReflect: the cursor reflected from its chain's anchor
	srcMidpoint              // between: a safe scalar between the cursor and hi
	srcSliver                // between: c + " ", or capFallback just above c (refused: never a split's pivot)
	srcDensity               // the galloped density step (gallop)
	srcStructure             // a folder boundary from a structure probe
)

func (s pivotSource) String() string {
	return [...]string{"bound", "reflect", "midpoint", "sliver", "density", "structure"}[s]
}

// Range is one unit of listing work.
type Range struct {
	kind   kind
	lo, hi string
	open   bool
	sb     structure // folder boundaries known inside (lo, hi), from the parent's structure probe
	after  string    // on resume: keys <= after are already published; lo is a safe start-after at or below it
	id     uint64
	born   origin
	depth  int64       // splits between the root or a seed tile and this range
	pivot  pivotSource // the pivot source of the split that made this range (srcBound: none)
	hiSrc  pivotSource // what made hi: the source of the pivot it is, or srcBound
	// handout: the range lies inside the lower child of a frontier split
	// (it is that child or one of its descendants).
	handout bool
	// gen is the gallop generation: consecutive upper children along the
	// chain (a lower child is 0, an upper child its parent's + 1, at most
	// gallopMaxGen).
	gen uint8
	// zeroes is the structure probes in a row, along the range's ancestry,
	// that found no folder (structureZeroLimit stops probing).
	zeroes uint8
	// stall: the split that made this upper child kept its pivot inside
	// the cursor's own folder, the evidence on which a bounded range
	// probes structure (StructureAuto).
	stall bool
	// anchor is where the chain's reflection measures the covered
	// distance from (propose): on the open frontier a seed or resume
	// tile's lo, or the chain's first pivot when it started at ⊥; on a
	// bounded chain the first key its first range listed. "": the range
	// starts a chain (a lower child, a tile, or after a reflection reached
	// the ceiling).
	anchor string
	// est is the keys the split that made the range estimated in it, from
	// the parent's last page (estOK false: none, as for a tile): its
	// remainder until its own first page gives one (running.rem).
	est   float64
	estOK bool
	// prior: est is a counted prior's (a tile's expected keys, or its
	// share of its parent's), which the range's pages do not replace by
	// their gauge until it is listed (running.held).
	prior bool
	// tile: 1 + the index of the prior tile (expandPrior) the range lies
	// in, inherited by its children; 0 for none. Its keys add to the
	// tile's, which the report compares with the prior (priorErrors).
	tile int32
	// lat is the page latency (ns) the range starts with: its parent's
	// (SlowSplit; 0 unknown). slow: a child of a slow split (slowQueued).
	lat  float64
	slow bool
	// cert is a reused range's certificate (its index in the resume's
	// spans); gapEnd: hi is a reused certificate's start, so keys listed
	// past it are boundary overfetch (resume.reuse.boundary_overfetch_keys).
	cert   int
	gapEnd bool
	// scope is the listing prefix of a range a caller tiled itself
	// (Within), inherited by its children; "" is the engine's scope.
	scope string
}

// estimate is the range's remainder before its first page: +Inf on the
// frontier, its split's estimate, or NaN (unknown).
func (r Range) estimate() float64 {
	switch {
	case r.open:
		return math.Inf(1)
	case r.estOK:
		return r.est
	}
	return math.NaN()
}

// running is the split bookkeeping of a range in Expand (under p.mu).
type running struct {
	keys         int64
	open         bool
	continuing   bool // last page full and not at the bound
	unsplittable bool // proven: no pivot exists above a safe cursor
	ended        bool // the range listed through its bound (a leaf)
	split        bool // the range split and handed on its rest
	id           uint64
	sb           structure      // known folder boundaries (structure probes)
	pending      chan structure // a structure probe in flight
	zeroes       uint8          // consecutive structure probes that found no folder, the ancestry's included
	rem          float64        // estimated keys left in (cursor, hi]: +Inf on the frontier, NaN unknown (the rank)
	refused      int            // consecutive split decisions the admission gates refused
	lat          float64        // page latency, ns: an EWMA of its pages (weight slowLatencyWeight), from its parent's (0: unknown)
	slowLine     bool           // a slow split made it: its time left is not asked again (slowSplitWanted)
	groupOpen    bool           // group mode: its last emitted key's group may continue, or it emitted none yet (no split, unranked)
	prior        float64        // a counted prior's expected keys in the range (Range.prior; 0: none)
}

// held is the prior's count of keys still to list in st, its expected
// keys less those listed, or 0 where it has none or they are listed. A
// prior's count is exact for a fresh prior, where a page's gauge errs
// long by orders of magnitude on hierarchical keys (admission.go), so it
// stays the range's remainder while it lasts; mu held.
func (st *running) held() float64 {
	return max(st.prior-float64(st.keys), 0)
}

// slowKeys is st's keys left for the slow-range rule: its remainder, or,
// where that is +Inf (a bound in another folder: a seed tile's hi is the
// next folder, and a page cannot see where its own folder ends), the keys
// it has listed, as a range that has listed k keys likely holds k more
// (the frontier's doubling assumption); mu held.
func (st *running) slowKeys() float64 {
	if r := st.remainder(); !math.IsInf(r, 1) {
		return r
	}
	return float64(st.keys)
}

// Slow-range splitting (Options.SlowSplit): demand is idle workers and
// the rank counts keys, so a range whose pages are slow (one folder at 2 s
// a page among pages of 200 ms) stays on one worker until the rest
// drains. With SlowSplit the rank reads time left, keys left x the range's
// page latency, and a range that is a latency outlier splits without an
// idle worker.
const (
	// slowLatency: a range's page latency must be at least this multiple
	// of the run's mean: a latency outlier, not a large range (a large
	// range at the run's latency is the rank's and demand's business), and
	// not jitter (uniform 20-500 ms pages average 260 ms: no range's mean
	// reaches 1 s), nor a slow half of the keyspace (it is half the mean).
	slowLatency = 4.0
	// slowTimeLeft: its time left must be at least this multiple of the
	// running ranges' mean. A page cannot see where its folder ends, so
	// keys left are mostly keys listed (slowKeys) and the mean errs long;
	// twice it is an outlier past that error, and a range of a few slow
	// pages already listed is not split for its last one.
	slowTimeLeft = 2.0
	// slowLatencyWeight: the weight of a range's newest page in its
	// latency, so a child's inherited latency gives way within two pages.
	slowLatencyWeight = 0.5
)

// runLatency is the mean page latency of the run's ranges (ns; 0: none
// yet); mu held.
func (p *Engine[P]) runLatency() float64 {
	if p.latN == 0 {
		return 0
	}
	return p.latSum / float64(p.latN)
}

// timeLeft is st's remainder in pages times its page latency (the run's
// when it has none yet; 1 ns before any page, so the rank is by keys and
// +Inf stays +Inf); mu held.
func (p *Engine[P]) timeLeft(st *running, run float64) float64 {
	lat := st.lat
	if lat == 0 {
		lat = max(run, 1)
	}
	return st.remainder() / float64(p.opt.PageSize) * lat
}

// slowTime is timeLeft over slowKeys; mu held.
func (p *Engine[P]) slowTime(st *running, run float64) float64 {
	lat := st.lat
	if lat == 0 {
		lat = max(run, 1)
	}
	return st.slowKeys() / float64(p.opt.PageSize) * lat
}

// slowSplitWanted: with SlowSplit and no split of this kind waiting, st is
// a bounded range with an estimated remainder whose page latency is at
// least slowLatency times the run's mean and whose time left is at least
// slowTimeLeft times the mean over the running continuing ranges. At most
// one slow split's children wait at a time (slowQueued): the engine does
// not split faster than workers free up to take them; mu held.
func (p *Engine[P]) slowSplitWanted(st *running, r Range) bool {
	if !p.opt.SlowSplit || r.open || p.slowQueued > 0 || math.IsInf(st.slowKeys(), 0) {
		return false
	}
	run := p.runLatency()
	if run == 0 || st.lat < slowLatency*run {
		return false
	}
	if st.slowLine {
		// A slow split's child that is still a latency outlier halves
		// again: its keys left restarted at its own (a page cannot see
		// where the folder ends), so the time-left test would stop the
		// halving after one split.
		return true
	}
	sum, n := 0.0, 0
	for _, o := range p.running {
		if o.continuing && !o.unsplittable && !math.IsInf(o.slowKeys(), 0) {
			sum += p.slowTime(o, run)
			n++
		}
	}
	return n > 0 && p.slowTime(st, run) >= slowTimeLeft*sum/float64(n)
}

// Root is the scope: the sequential chain, the seed (which tiles a
// resumed scope (K, ⊤] too), a resumed (K, ⊤], or (⊥, ⊤].
func (p *Engine[P]) Root() Range {
	switch {
	case p.opt.Sequential:
		return Range{kind: whole}
	case p.opt.Prior != nil:
		return Range{kind: warm}
	case p.opt.Seed:
		return Range{kind: seed}
	case p.opt.Resume != nil || len(p.reuse) > 0:
		return Range{kind: resume}
	}
	return Range{kind: interval, open: true}
}

// Expand lists one range.
func (p *Engine[P]) Expand(ctx context.Context, r Range, e source.Emitter[Range, P]) error {
	switch r.kind {
	case whole:
		return p.expandSequential(ctx, e)
	case seed:
		return p.expandSeed(ctx, e)
	case warm:
		return p.expandPrior(e)
	case resume:
		return p.expandResume(e)
	case reused:
		return fmt.Errorf("keyrange: reused range %d expanded: only the order stage replays it", r.cert)
	}
	return p.expandRange(ctx, r, e)
}

// Fences reports that the engine declares coverage fences (source.Fencing):
// every range but the sequential chain, whose one continuation chain is
// not a range.
func (p *Engine[P]) Fences() bool { return !p.opt.Sequential }

// Replay reports a reused range's certificate (source.Replaying).
func (p *Engine[P]) Replay(r Range) (int, bool) { return r.cert, r.kind == reused }

// Reuse lists around reused certificates (source.Reusing): the resume's
// tiling emits a reused range at each span and lists only the gaps.
func (p *Engine[P]) Reuse(spans []source.Span) { p.reuse = spans }

// resumeKey is the run's resume key K ("" on a fresh run).
func (p *Engine[P]) resumeKey() string {
	if p.opt.Resume == nil {
		return ""
	}
	return p.opt.Resume.After
}

// aroundReuse cuts tiles, the tiling of (K, ⊤] in key order, around the
// reused certificates' spans: a span's part above K becomes one reused
// range, at its position, and each tile keeps only the gaps, its pieces
// bounded by the spans' edges (a piece starting at the tile's start keeps
// its bounds, the resume tile's safe start below K included; a prior
// tile's pieces keep its tile, so their keys count against the prior).
// Spans are ascending and disjoint, each ending above K. Without spans,
// tiles.
func (p *Engine[P]) aroundReuse(tiles []Range) []Range {
	if len(p.reuse) == 0 {
		return tiles
	}
	k := p.resumeKey()
	start := func(i int) string { return max(p.reuse[i].Lo, k) }
	out := make([]Range, 0, len(tiles)+2*len(p.reuse))
	piece := func(t Range, lo, hi string, open, gapEnd bool) {
		if !open && hi <= lo {
			return
		}
		g := Range{lo: lo, hi: hi, open: open, id: p.ids.Add(1), born: t.born, gapEnd: gapEnd, tile: t.tile}
		if lo == max(t.lo, t.after) {
			g.lo, g.after = t.lo, t.after
		}
		out = append(out, g)
	}
	ci, begun := 0, false // the next span, and whether its reused range was emitted
	for _, t := range tiles {
		pos := max(t.lo, t.after)
		inside := func(x string) bool { return t.open || x < t.hi }
		for {
			if ci < len(p.reuse) && begun {
				// A span that began in an earlier tile (or at this tile's start).
				s := p.reuse[ci]
				if s.HiOpen || !inside(s.Hi) {
					if !s.HiOpen && !t.open && s.Hi == t.hi {
						ci, begun = ci+1, false
					}
					pos = ""
					break // the span covers the rest of the tile
				}
				pos, ci, begun = s.Hi, ci+1, false
				continue
			}
			if ci < len(p.reuse) && inside(start(ci)) {
				piece(t, pos, start(ci), false, true)
				out = append(out, Range{kind: reused, cert: ci, lo: start(ci), hi: p.reuse[ci].Hi, open: p.reuse[ci].HiOpen, id: p.ids.Add(1), born: fromSeed})
				begun = true
				continue
			}
			piece(t, pos, t.hi, t.open, false)
			break
		}
	}
	return out
}

// expandResume hands on (after, ⊤], the rest of the scope after a
// journaled run's last published key, when the seed is off (with it,
// expandSeed tiles the same scope); splits ramp it up as on a fresh run.
// It lists nothing itself.
func (p *Engine[P]) expandResume(e source.Emitter[Range, P]) error {
	after := p.resumeKey()
	p.lock()
	t := Range{lo: after, open: true, id: p.ids.Add(1), born: fromSeed}
	// K is a real key and may not be a bound the provider accepts: start at
	// its longest accepted prefix and drop keys up to K there.
	if s := p.safeStart(after); s != after {
		t.lo, t.after = s, after
	}
	tiles := p.aroundReuse([]Range{t})
	p.account(time.Now())
	p.queued += p.queue(tiles)
	p.c.Max(cMaxQueued, int64(p.queued))
	p.c.Set(cResumedRanges, int64(len(tiles)))
	p.unlock()
	for _, t := range tiles {
		if err := e.Child(t); err != nil {
			return err
		}
	}
	return nil
}

// queue counts tiles' ranges for a worker (a reused range is the order
// stage's), and the reused ranges and gaps among them; mu held.
func (p *Engine[P]) queue(tiles []Range) int {
	n := 0
	for _, t := range tiles {
		if t.kind == reused {
			p.c.Inc(cReuseRanges)
			continue
		}
		n++
		if len(p.reuse) > 0 {
			p.c.Inc(cReuseGaps)
		}
	}
	return n
}

// account integrates the active count over time, and the idle demand
// (max(0, hunger)) while a range is in Expand; mu held. Every change of
// running or queued is preceded by a call.
func (p *Engine[P]) account(now time.Time) {
	dt := now.Sub(p.areaAt)
	p.area += float64(len(p.running)) * dt.Seconds()
	if hunger := p.opt.Workers - len(p.running) - p.queued; hunger > 0 && len(p.running) > 0 {
		p.c.Add(cIdleDemandNs, int64(hunger)*int64(dt))
	}
	p.areaAt = now
}

func (p *Engine[P]) enter(r Range) *running {
	p.lock()
	defer p.unlock()
	p.account(time.Now())
	if r.born != fromRoot {
		p.queued--
	}
	st := &running{id: r.id, open: r.open, sb: r.sb, zeroes: r.zeroes, rem: r.estimate(), lat: r.lat, slowLine: r.slow}
	if r.prior && r.estOK {
		st.prior = r.est
	}
	p.running[r.id] = st
	if r.slow {
		p.slowQueued--
	}
	p.c.Max(cMaxActive, int64(len(p.running)))
	return st
}

func (p *Engine[P]) leave(r Range, st *running, start time.Time) {
	p.lock()
	defer p.unlock()
	p.account(time.Now())
	delete(p.running, r.id)
	p.c.Add(cBusyNs, int64(time.Since(start)))
	if r.tile > 0 {
		p.tiles[r.tile-1].keys += st.keys
	}
	child := r.born == fromLower || r.born == fromUpper
	if child && st.keys == 0 {
		upper := 0
		if r.born == fromLower {
			p.c.Inc(cChildEmptyLower)
		} else {
			p.c.Inc(cChildEmptyUpper)
			upper = 1
		}
		p.c.Inc(emptyBy[r.pivot][upper])
		if r.hiSrc == srcBound {
			p.c.Inc(cChildEmptyHiBound)
		} else {
			p.c.Inc(cChildEmptyHiPivot)
		}
	}
	if !st.ended && !st.split {
		return // failed or cancelled: no realized outcome
	}
	if child && st.keys < int64(p.opt.PageSize) {
		if r.born == fromLower {
			p.c.Inc(cChildSmallLower)
		} else {
			p.c.Inc(cChildSmallUpper)
		}
	}
	if r.handout {
		p.c.Add(cFrontierHandoutKeys, st.keys)
	}
	if st.ended {
		p.c.Inc(cLeaves)
		p.c.Add(cLeafDepthSum, r.depth)
	}
}

// expandRange lists (lo, hi]: the first page starts after lo, later pages
// follow continuation tokens; keys past hi are trimmed and end the range.
func (p *Engine[P]) expandRange(ctx context.Context, r Range, e source.Emitter[Range, P]) error {
	start := time.Now()
	st := p.enter(r)
	defer p.leave(r, st, start)
	// A structure probe still in flight when the range ends is cancelled
	// (its result could only serve this range's next split) and awaited,
	// so no request outlives the range.
	pctx, stop := context.WithCancel(ctx)
	defer func() {
		stop()
		p.lock()
		ch := st.pending
		p.unlock()
		if ch != nil {
			<-ch
		}
	}()
	in := p.query()
	if r.scope != "" {
		in.Prefix = r.scope
	}
	in.After = r.lo
	in.Through, in.Bounded = r.hi, !r.open
	c := r.lo
	head := ""        // the first key this range listed
	var grp groups[P] // group mode: the chain's held rows and open group
	// Coverage fences (source.Fencer): the range's logical start, each page
	// end, the split cursor and the end. llo is above the request bound
	// after an unsafe resume key, whose keys up to K are dropped; a fence is
	// declared only at a bound the provider accepts or at K itself.
	fence, _ := e.(source.Fencer)
	llo := max(r.lo, r.after)
	k := p.resumeKey()
	fenceAt := func(x string, top bool) {
		if fence != nil && (top || (x >= llo && x >= k && (p.safe(x) || x == k))) {
			fence.Fence(x, top)
		}
	}
	if fence != nil {
		fence.Span(llo, r.hi, r.open)
	}
	p.maybeProbe(pctx, st, r, c)
	for {
		t0 := time.Now()
		out, err := p.list(ctx, in)
		if err != nil {
			return err
		}
		pageNs := float64(time.Since(t0))
		skipped := 0
		if r.after != "" { // resume: drop keys already published
			n := 0
			for n < len(out.Rows) && out.Rows[n].Key <= r.after {
				n++
			}
			if n > 0 {
				// In group mode the cursor stays on emitted keys: a skipped
				// key's group may continue on the next page (suppressed
				// there too), and the split cursor must be complete.
				if skipped = n; !p.d.Groups {
					c = out.Rows[n-1].Key
				}
				out.Rows = out.Rows[n:]
				if p.d.Groups {
					out.Rows = p.d.Detach(out.Rows)
				}
				p.c.Add(cOverfetchKeys, int64(n))
			}
		}
		truncated := out.Truncated
		if truncated && out.Token == "" {
			return fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
		}
		var kept int
		var reached, done bool
		var rows []schema.Row[P] // the rows emitted: the page's, or in group mode its complete groups
		if p.d.Groups {
			prev, same := grp.prev(c)
			kept, reached, err = p.groupOrder(out.Rows, prev, same, r.lo, r.hi, r.open)
			if err != nil {
				return err
			}
			// A bounded range whose last key is hi still lists until a key
			// above it or the chain's end: hi's group may continue.
			done = reached || !truncated
			keptRows := out.Rows[:kept]
			if kept < len(out.Rows) {
				keptRows = p.d.Detach(keptRows)
			}
			rows, c, _ = p.take(&grp, keptRows, done, c)
		} else {
			kept, reached = len(out.Rows), false
			for i := range out.Rows {
				k := out.Rows[i].Key
				if (c != "" || i > 0) && k <= c {
					return fmt.Errorf("%w: listing out of order in (%q, %q]: %q after %q", p.d.invalid(), r.lo, r.hi, k, c)
				}
				if !r.open && k > r.hi {
					kept, reached = i, true
					break
				}
				c = k
			}
			done = reached || !truncated || (!r.open && kept > 0 && c == r.hi)
			rows = out.Rows[:kept]
		}
		// The page's strings are views of its parser arena (S3), and c
		// outlives the page: the next page's order check, a structure
		// probe's start and, at a split, the lower child's lo, which stays
		// queued, or done ahead of the drain, for as long as the frontier
		// takes. One clone per page keeps a retained range from pinning the
		// whole page through its bound. (Group mode holds one page's
		// trailing group back across one page: at most one page more.)
		if len(rows) > 0 || skipped > 0 {
			c = strings.Clone(c)
		}
		if truncated && kept == 0 && !reached && skipped == 0 && !(p.d.Groups && len(rows) > 0) {
			return fmt.Errorf("%w: truncated page with no key in (%q, %q]", p.d.invalid(), r.lo, r.hi)
		}
		// The density and split inputs are read before the rows are handed
		// over: the emitter owns them then, and a filter compacts and clears
		// them in place. In group mode they are the emitted rows' distinct
		// keys (density is of keys; st.keys counts rows, the work).
		n := len(rows)
		if n > 0 && head == "" {
			// Kept for the range's life and handed on as an upper child's
			// anchor: a copy, not a view of this page.
			head = strings.Clone(rows[0].Key)
		}
		var page []string
		var g *gauge
		rem := math.NaN()
		if !done {
			if p.d.Groups {
				page = distinct(rows)
			} else {
				page = make([]string, n)
				for i := range page {
					page[i] = rows[i].Key
				}
			}
			// The remainder estimate (the rank's, and the first of the
			// admission gates') is read here, outside the decision mutex.
			if g = newGauge(page); g != nil && !r.open {
				rem = g.keys(c, r.hi)
			}
		}
		overfetch := len(out.Rows) - kept
		if p.d.Groups {
			p.c.Add(cGroupsOverfetchRows, int64(overfetch))
		}
		if err := p.emit(rows, e); err != nil {
			return err
		}
		switch {
		case done:
			fenceAt(r.hi, r.open)
		case p.d.Groups && grp.open:
			// A hot key's group is still open: rows of key c may follow on
			// the next page, so c is not yet a fence (every row up to it
			// emitted). Fences, and so a certificate's cs and ce, fall only
			// at complete keys: none cuts a version group.
			p.c.Inc(cGroupsFenceDeferred)
		default:
			fenceAt(c, false)
		}
		p.lock()
		p.c.Add(cOverfetchKeys, int64(overfetch))
		if r.gapEnd && reached {
			p.c.Add(cReuseBoundaryOverfetch, int64(overfetch))
		}
		st.keys += int64(n)
		if st.unsplittable {
			p.c.Add(cUnsplittableKeys, int64(n))
		}
		st.ended = done
		st.groupOpen = p.d.Groups && grp.unsplittable()
		// held: the prior's remainder, which the admission gates read in
		// place of the gauge's (NaN: none).
		held := math.NaN()
		switch h := st.held(); {
		case h > 0 && !done:
			held, st.rem = h, h
			p.c.Inc(cSeedFromHeld)
		case st.prior > 0 && !done:
			st.prior = 0
			p.c.Inc(cSeedFromOutrun)
			fallthrough
		default:
			if !math.IsNaN(rem) {
				st.rem = rem
			}
		}
		if p.opt.SlowSplit {
			if st.lat == 0 {
				st.lat = pageNs
			} else {
				st.lat += slowLatencyWeight * (pageNs - st.lat)
			}
			p.latSum += pageNs
			p.latN++
			p.c.Max(cSlowMaxRangeLatency, int64(st.lat))
			p.c.Set(cSlowRunLatency, int64(p.runLatency()))
		}
		hunger := p.opt.Workers - len(p.running) - p.queued
		slow := !done && hunger <= 0 && p.slowSplitWanted(st, r)
		wanted := !done && !st.unsplittable && !st.groupOpen && (hunger > 0 || p.opt.SplitCoin != nil || slow)
		p.unlock()
		if done {
			p.covered(r.lo, r.hi, r.open)
			return faults.At("range-done")
		}
		// The pivot is placed outside the decision mutex, on this snapshot
		// of the demand (the density step's big-number arithmetic is the
		// costly part of a decision); decide takes the decision under it.
		var pr proposal
		if wanted {
			ceil, bounded := p.ceiling(r)
			// A slow split asks the stride of a quarter of the workers: its
			// children wait for workers to free up, so handing out one
			// page each (hunger 1) would peel the slow folder page by page.
			want := max(hunger, 1)
			if slow {
				want = max(want, p.opt.Workers/4)
			}
			pr = propose(r, page, head, c, ceil, bounded, want)
			pr.gauge, pr.rem = g, rem
			if !math.IsNaN(held) {
				pr.rem, pr.held = held, true
			}
		}
		if pv, ok := p.decide(st, r, c, pr); ok {
			if p.d.Groups && grp.unsplittable() {
				// decide refuses a range whose group is open (the lower
				// child would start after the key, past its other rows)
				// or which emitted nothing yet (no progress).
				return fmt.Errorf("keyrange: split at %q inside an open group or before a complete key", c)
			}
			if len(grp.held) > 0 {
				p.c.Add(cGroupsRelistedRows, int64(len(grp.held)))
				grp.held = nil
			}
			lower, upper := p.commit(st, r, c, pv)
			if err := faults.At("split"); err != nil {
				return err
			}
			p.covered(r.lo, c, false)
			fenceAt(c, false)
			if err := e.Child(lower); err != nil {
				return err
			}
			return e.Child(upper)
		}
		in.Token = out.Token
		p.maybeProbe(pctx, st, r, c)
	}
}

func (p *Engine[P]) covered(lo, hi string, open bool) {
	if p.opt.Trace != nil {
		p.opt.Trace(lo, hi, open)
	}
}

// ceiling is what a pivot in r must stay below: hi for a bounded range;
// on the open frontier the scope's prefixCeil, or nothing (bounded false).
func (p *Engine[P]) ceiling(r Range) (string, bool) {
	if r.open {
		if r.scope != "" {
			return prefixCeil(r.scope)
		}
		return prefixCeil(p.opt.Prefix)
	}
	return r.hi, true
}

// Within is a range a caller tiles itself: the keys of scope after after,
// listed with scope as the request prefix (the tree core hands the engine
// one directory's rest this way, docs/internals/tree-sources.md). It is
// (after, prefixCeil(scope)] when scope has a ceiling, else open. The
// caller emits it as a child; it counts as queued work until expanded, and
// its children inherit its scope. Seed, prior and structure probes are the
// engine's scope's and do not apply to it.
func (p *Engine[P]) Within(scope, after string) Range {
	hi, bounded := prefixCeil(scope)
	r := Range{lo: after, hi: hi, open: !bounded, scope: scope, born: fromSeed, id: p.ids.Add(1)}
	p.lock()
	p.account(time.Now())
	p.queued++
	p.c.Max(cMaxQueued, int64(p.queued))
	p.unlock()
	return r
}

// proposal is a split's pivot as the policy places it (propose), computed
// outside the decision mutex from the page and a snapshot of the demand,
// with what it observed; decide counts and applies it under the mutex.
type proposal struct {
	ready    bool // propose ran for this page
	m        string
	src      pivotSource
	found    bool      // a pivot exists (else the guard's no_pivot)
	sliver   bool      // only a sliver lies between: refused, not split
	why      extReason // the density step's refusal (extOK: it gave a pivot)
	halvings int       // the density step's stride halvings
	anchored bool      // the reflection had an anchor (not ⊥)
	ceiling  bool      // the reflection reached the ceiling: the midpoint between(c, ceil) instead
	shrink   bool      // the density step lay nearer than the reflection and was not taken
	anchor   string    // the anchor the reflection measured from ("": ⊥)
	gauge    *gauge    // the page's gauge (nil: no estimate), for the admission gates and the children's estimates
	rem      float64   // the gauge's remainder (c, hi] of a bounded range, or the prior's (held)
	held     bool      // rem is the prior's count (running.held): the children share it
}

// children is the keys a split of (c, hi] at m leaves each child by the
// page's gauge, or, where the remainder is a prior's count (held), that
// count shared in the gauge's proportion (halves where the gauge cannot
// read a child).
func (pr *proposal) children(c, m, hi string) (lower, upper float64) {
	lower, upper = pr.gauge.keys(c, m), pr.gauge.keys(m, hi)
	if !pr.held {
		return lower, upper
	}
	if s := lower + upper; s > 0 && !math.IsInf(s, 1) {
		// Normalize first: a finite gauge weight times the retained count
		// can overflow even when the weight's share is at most one.
		return pr.rem * (lower / s), pr.rem * (upper / s)
	}
	return pr.rem / 2, pr.rem / 2
}

// propose places the pivot of a split of r at cursor c below ceil
// (bounded false: none), with demand hunger; head is the first key r
// listed. Two steps, the further of which is the pivot:
//   - the reflection of c from its chain's anchor (forwardReflect at the
//     scalar where they diverge): the chain's covered distance doubles per
//     split, and climbs folder levels by itself (day=03, 05, 09, then the
//     next month), where a page's density cannot reach. A reflection at or
//     past the ceiling (hi, or the scope's prefixCeil on the frontier)
//     falls back to the safe midpoint between(c, ceil), and the upper child
//     starts a new chain, measured from its own keys;
//   - the galloped density step (gallop): max(4, hunger) << gen page spans
//     in the page's alphabet, at most halfway to hi. It may move the pivot
//     further, never nearer: on the frontier the reflection is what grows.
//
// A sliver (c + " ", or the cap's bump just above c) leaves the lower
// child nearly certain to be empty: it is refused, and the range keeps
// listing and decides again after its next page. A folder boundary known
// from a structure probe takes precedence over all of this (decide).
func propose(r Range, page []string, head, c, ceil string, bounded bool, hunger int) proposal {
	pr := proposal{ready: true}
	e, why, halvings := gallop(page, c, ceil, bounded, hunger, r.gen)
	pr.why, pr.halvings = why, halvings
	anchor := r.anchor
	if anchor == "" {
		anchor = head
		if r.open {
			anchor = r.lo // a tile's lo; ⊥ at the root
		}
	}
	pr.m, pr.found = forwardReflect(anchor, c)
	if !pr.found && anchor != "" {
		// An anchor that is not UTF-8 (a first key) measures nothing: the
		// reflection is taken from ⊥, and the upper child anchors anew.
		anchor = ""
		pr.m, pr.found = forwardReflect(anchor, c)
	}
	pr.anchor, pr.anchored = anchor, anchor != ""
	pr.src = srcReflect
	if bounded && (!pr.found || pr.m >= ceil) {
		// The midpoint below the ceiling; when it fails too, only a sliver
		// lies there: the one proof that (c, ceil) holds no pivot.
		pr.ceiling = pr.found
		pr.m, pr.src, pr.found = betweenFrom(c, ceil)
	}
	if why == extOK {
		switch {
		case !pr.found || pr.src == srcSliver || e > pr.m:
			pr.m, pr.src, pr.found = e, srcDensity, true
		case e < pr.m && pr.src == srcReflect:
			pr.shrink = true
		}
	}
	pr.sliver = pr.found && pr.src == srcSliver
	return pr
}

// decide runs after a full (continuing) page of r with cursor c: split iff
// workers are idle (hunger = workers - active - queued > 0), r ranks among
// the hunger largest eligible ranges (the open frontier first, then by
// estimated remainder), the guard allows the pivot and, for a bounded
// range, the admission gates do: by the page's gauge the remainder (c, hi]
// holds at least admitRemainderPages and each child, (c, m] and (m, hi],
// at least admitChildPages. The pivot is a structure boundary when one is
// known inside (c, hi), else the proposal's (propose). On success the two
// children are reserved in the demand count; the caller then commits the
// split.
func (p *Engine[P]) decide(st *running, r Range, c string, pr proposal) (pv pivot, ok bool) {
	p.lock()
	defer p.unlock()
	st.continuing = true
	if st.unsplittable {
		return
	}
	if st.groupOpen {
		// A hot key's rows run on past the page: no split inside them,
		// whatever the gate (the coin and structure boundaries included).
		p.c.Inc(cSplitSkippedGroupOpen)
		return
	}
	if coin := p.opt.SplitCoin; coin != nil {
		if !coin() {
			return
		}
	}
	hunger := p.opt.Workers - len(p.running) - p.queued
	if p.opt.SplitCoin == nil {
		switch {
		case !pr.ready: // the demand rose after the page's snapshot
			p.c.Inc(cSplitSkippedNoDemand)
			return
		case hunger <= 0 && p.slowSplitWanted(st, r):
			pv.slow = true // a latency outlier: split without an idle worker, unranked
		case hunger <= 0:
			p.c.Inc(cSplitSkippedNoDemand)
			return
		}
		if !pv.slow {
			rank := 0
			run := p.runLatency()
			for _, o := range p.running {
				if o == st || !o.continuing || o.unsplittable || o.groupOpen {
					continue
				}
				if (p.opt.SlowSplit && outranksTime(o, st, p.timeLeft(o, run), p.timeLeft(st, run))) || (!p.opt.SlowSplit && outranks(o, st)) {
					rank++
				}
			}
			p.c.Inc(cSplitConsidered)
			if rank >= hunger {
				p.c.Inc(cSplitSkippedOutranked)
				return
			}
		}
	}
	ceil, bounded := p.ceiling(r)
	var found, structural bool
	var m string
	if p.opt.Structure != StructureOff {
		p.absorb(st)
		m, structural = boundary(st.sb, c, r.hi)
		// On the frontier a folder boundary is taken when it lies further
		// than the policy's pivot: both are far moves, and the real
		// boundary's lower child halves on the boundaries it inherits.
		if structural && r.open && pr.found && !pr.sliver && m <= pr.m {
			structural = false
		}
		found = structural
	}
	if structural {
		pv.src, pv.anchored = srcStructure, pr.anchored // the chain keeps its anchor either way
	} else {
		if pr.why != extOK {
			p.c.Inc(extRefused[pr.why])
		}
		p.c.Add(cGallopHalvings, int64(pr.halvings))
		if pr.ceiling {
			p.c.Inc(cReflectCeiling)
		}
		if pr.shrink {
			p.c.Inc(cDensityShrinkPrevented)
		}
		if pr.sliver {
			p.c.Inc(cSplitSliverRefused)
			return
		}
		m, found, pv.src, pv.anchored = pr.m, pr.found, pr.src, pr.anchored
	}
	// The upper child keeps the chain's anchor; a chain from ⊥ anchors at
	// its first pivot; after a reflection reached the ceiling it starts anew.
	if !pr.ceiling {
		pv.chain = cmp.Or(pr.anchor, m)
	}
	pv.m = m
	switch guard(c, m, found, ceil, bounded, p.safe) {
	case clauseNoPivot:
		p.c.Inc(cSplitRejectedNoPivot)
		// No pivot after a safe cursor is a fact about the interval: a later
		// cursor only narrows (c, hi]. After an unsafe one it is the
		// cursor's, as below.
		if p.safe(c) {
			st.unsplittable = true
			p.c.Inc(cUnsplittable)
		}
		return
	case clauseUnsafeCursor:
		// The cursor cannot be the lower child's start-after: decline this
		// page's split only. The next page ends at another key, usually a
		// safe one, and is evaluated afresh.
		p.c.Inc(cSplitRejectedUnsafeCursor)
		return
	case clauseCursor:
		p.c.Inc(cSplitRejectedCursor)
		return
	case clauseUnsafePivot:
		p.c.Inc(cSplitRejectedUnsafePivot)
		return
	case clauseBound:
		p.c.Inc(cSplitRejectedBound)
		return
	}
	// Admission (bounded ranges; the frontier's remainder is unbounded, and
	// a frontier refused for a near pivot would keep its anchor at ⊥ and
	// could stay unsplit). A refusal declines this page's split only: the
	// range decides again after its next page, and after admitRefusals
	// refusals in a row with demand the split is taken: the policy can
	// propose the same pivot page after page (e11bio-prism: ".../0/1>", in
	// the gap past ".../0/19..." below hi ".../0/2"), whose upper child the
	// gauge rightly reads as empty while the lower holds a million keys;
	// refused for the range's life, they went to one worker (4 to 13 wide).
	if !r.open {
		if g := pr.gauge; g == nil {
			p.c.Inc(cSplitEstimateNone)
		} else {
			// Pages as this range's pages come: a provider may return fewer
			// keys than asked for.
			page := float64(len(g.page))
			lower, upper := pr.children(c, m, r.hi)
			refuse := counters.ID(0) // the refusing gate; 0 (cObjects) for none
			switch {
			case pr.rem < admitRemainderPages*page:
				refuse = cSplitSkippedRemainder
			case lower < admitChildPages*page:
				refuse = cSplitSkippedLowerTiny
			case upper < admitChildPages*page:
				refuse = cSplitSkippedUpperTiny
			}
			switch {
			case refuse != 0 && st.refused >= admitRefusals:
				p.c.Inc(cSplitAdmittedPersistent)
			case refuse != 0:
				p.c.Inc(refuse)
				st.refused++
				return pivot{}, false
			}
		}
	}
	st.refused = 0
	if pr.gauge != nil {
		pv.lower, pv.upper = pr.children(c, m, r.hi)
		pv.estOK, pv.prior = true, pr.held
	}
	p.account(time.Now())
	p.queued += 2 // the children, reserved until commit hands them out
	if pv.slow {
		p.slowQueued += 2
	}
	return pv, true
}

// pivot is a decided split's pivot m and the synthesis that made it;
// anchored: the reflection had an anchor; chain: the upper child's anchor.
type pivot struct {
	m        string
	src      pivotSource
	anchored bool
	chain    string
	// lower, upper: the children's estimated keys (estOK: from the page's
	// gauge), their remainders until their own first pages.
	lower, upper float64
	estOK        bool
	prior        bool // lower and upper share a prior's count (Range.prior)
	slow         bool // a slow-range split, taken without an idle worker
}

// commit accepts a decided split: the two children and the counters.
// Splits are not journaled: a resume restarts above the last published key
// and splits afresh.
func (p *Engine[P]) commit(st *running, r Range, c string, pv pivot) (lower, upper Range) {
	p.lock()
	m := pv.m
	st.split = true
	switch {
	case pv.src != srcDensity:
		p.c.Inc(pivotBy[pv.src])
	case r.open:
		p.c.Inc(cSplitPivotDensityFrontier)
	default:
		p.c.Inc(cSplitPivotDensityBounded)
	}
	if r.open {
		p.c.Inc(cSplitAcceptedFrontier)
		if pv.src == srcStructure {
			p.c.Inc(cStructureFrontierPivots)
		}
		if pv.anchored {
			p.c.Inc(cFrontierAnchored)
		}
	} else {
		p.c.Inc(cSplitAcceptedBounded)
	}
	if pv.slow {
		p.c.Inc(cSplitReasonSlowRange)
	}
	p.c.Max(cGallopMaxGen, int64(r.gen))
	p.c.Max(cMaxQueued, int64(p.queued))
	depth := r.depth + 1
	p.c.Max(cSplitMaxDepth, depth)
	lower = Range{lo: c, hi: m, scope: r.scope, id: p.ids.Add(1), born: fromLower, depth: depth, pivot: pv.src, hiSrc: pv.src, tile: r.tile,
		handout: r.open || r.handout, sb: structure{bounds: within(st.sb.bounds, c, m), capped: st.sb.capped}, zeroes: st.zeroes}
	upper = Range{lo: m, hi: r.hi, open: r.open, scope: r.scope, id: p.ids.Add(1), born: fromUpper, depth: depth, pivot: pv.src, hiSrc: r.hiSrc, tile: r.tile,
		handout: r.handout, sb: structure{bounds: within(st.sb.bounds, m, r.hi), capped: st.sb.capped},
		gen: min(r.gen+1, gallopMaxGen), anchor: pv.chain, zeroes: st.zeroes,
		stall: stalled(c, m, r, len(p.opt.Prefix)), gapEnd: r.gapEnd}
	lower.lat, upper.lat, lower.slow, upper.slow = st.lat, st.lat, pv.slow, pv.slow
	lower.est, lower.estOK, lower.prior = pv.lower, pv.estOK, pv.prior
	upper.est, upper.estOK, upper.prior = pv.upper, pv.estOK && !r.open, pv.prior && !r.open
	// A resumed range split before its cursor passed the resume key hands
	// the key on: keys up to it are already published.
	if r.after > c {
		lower.after = r.after
	}
	if r.after > m {
		upper.after = r.after
	}
	p.unlock()
	if os.Getenv("SWATH_SPLIT_TRACE") != "" {
		fmt.Fprintf(os.Stderr, "split id=%d lo=%q c=%q m=%q hi=%q open=%v keys=%d src=%s depth=%d g=%d anchor=%q\n",
			r.id, r.lo, c, m, r.hi, r.open, st.keys, pv.src, depth, r.gen, pv.chain)
	}
	return lower, upper
}

// stalled is the evidence that a bounded chain peels one folder at a time
// (Range.stall): the pivot m of its split at cursor c stayed inside the
// cursor's own folder below the scope while hi lies beyond that folder, so
// the remainder holds folders the pivot could not reach.
func stalled(c, m string, r Range, floor int) bool {
	f := folder(c)
	return !r.open && len(f) > floor && folder(m) == f && !strings.HasPrefix(r.hi, f)
}

// outranks is a strict total order of remainders, largest first: the open
// frontier, then bounded ranges whose page could not read their bound
// (+Inf: a bound in another folder, likely far), then the finite estimates
// by size; the older range within a class and on ties. It must be
// transitive: a cycle (by size, then by age across classes) gave every
// range a rank of one, and with one idle worker none split.
func outranks(a, b *running) bool {
	if a.open != b.open {
		return a.open
	}
	sa, sb := a.remainder(), b.remainder()
	if sa != sb {
		return sa > sb // +Inf above every finite estimate
	}
	return a.id < b.id
}

// outranksTime is outranks with time left (keys left x page latency, ta
// and tb) in place of keys left (Options.SlowSplit): the same classes and
// tie-break, so the order stays total.
func outranksTime(a, b *running, ta, tb float64) bool {
	if a.open != b.open {
		return a.open
	}
	if ta != tb {
		return ta > tb
	}
	return a.id < b.id
}

// remainder is the rank's estimate of the keys left in the range: its
// last page's gauge over (cursor, hi], before that its split's estimate,
// +Inf on the frontier; without either (a page with no span), the keys it
// has listed, as a range that has listed k keys is not done.
func (st *running) remainder() float64 {
	if math.IsNaN(st.rem) {
		return float64(st.keys)
	}
	return st.rem
}

type clause uint8

const (
	clauseOK clause = iota
	clauseNoPivot
	clauseUnsafeCursor
	clauseCursor
	clauseUnsafePivot
	clauseBound
)

// guard checks a split of (lo, H] at cursor c into (c, m] and (m, H]:
//   - unsafe cursor: c becomes the lower child's start-after;
//   - cursor: m must be > c, or the upper child would list (m, c] again;
//   - unsafe pivot: m becomes a bound; the provider may veto it
//     (Description.AcceptBound);
//   - bound: m must be < H (or below prefixCeil(scope) on the frontier), or
//     the lower child would list keys of the next range.
func guard(c, m string, found bool, ceil string, bounded bool, safe func(string) bool) clause {
	switch {
	case !found:
		return clauseNoPivot
	case !safe(c):
		return clauseUnsafeCursor
	case m <= c:
		return clauseCursor
	case !safe(m):
		return clauseUnsafePivot
	case bounded && m >= ceil:
		return clauseBound
	}
	return clauseOK
}
