// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"github.com/varveio/swath/go/internal/faults"
	"math"
	"os"
	"time"

	"github.com/varveio/swath/go/internal/source"
)

// Parallel core: Swath's range tiling, seed, demand-driven owner split
// and safe pivot (docs/internals/providers.md, "Range engine").
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
	seed          // one delimiter page that emits the first tiling
	resume        // (after, ⊤] after a journaled run's last published key
)

type origin uint8

const (
	fromRoot origin = iota
	fromSeed
	fromLower
	fromUpper
)

// Range is one unit of listing work.
type Range struct {
	kind   kind
	lo, hi string
	open   bool
	sb     structure // folder boundaries known inside (lo, hi), from the parent's structure probe
	after  string    // on resume: keys <= after are already published; lo is a safe start-after at or below it
	id     uint64
	born   origin
}

// running is the split bookkeeping of a range in Expand (under p.mu).
type running struct {
	keys         int64
	open         bool
	continuing   bool // last page full and not at the bound
	unsplittable bool
	id           uint64
	cooldown     int // pages to skip after a probe found no key past the pivot
	probeFails   int
	dens         density        // page density, for the floors
	sb           structure      // known folder boundaries (structure probes)
	pending      chan structure // a structure probe in flight
	zeroes       int            // consecutive structure probes that found no folder
}

// Root is the scope: the sequential chain, a resumed tiling, the seed, or
// (⊥, ⊤].
func (p *Engine[P]) Root() Range {
	switch {
	case p.opt.Sequential:
		return Range{kind: whole}
	case p.opt.Resume != nil:
		return Range{kind: resume}
	case p.opt.Seed:
		return Range{kind: seed}
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
	case resume:
		return p.expandResume(e)
	}
	return p.expandRange(ctx, r, e)
}

// expandResume hands on (after, ⊤], the rest of the scope after a
// journaled run's last published key; splits ramp it up as on a fresh run.
// It lists nothing itself.
func (p *Engine[P]) expandResume(e source.Emitter[Range, P]) error {
	after := p.opt.Resume.After
	p.mu.Lock()
	t := Range{lo: after, open: true, id: p.ids.Add(1), born: fromSeed}
	// K is a real key and may not be a bound the provider accepts: start at
	// its longest accepted prefix and drop keys up to K there.
	if s := p.safeStart(after); s != after {
		t.lo, t.after = s, after
	}
	p.queued++
	p.c.Max(cMaxQueued, int64(p.queued))
	p.c.Set(cResumedRanges, 1)
	p.mu.Unlock()
	return e.Child(t)
}

// account integrates the active count over time; mu held.
func (p *Engine[P]) account(now time.Time) {
	p.area += float64(len(p.running)) * now.Sub(p.areaAt).Seconds()
	p.areaAt = now
}

func (p *Engine[P]) enter(r Range) *running {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.account(time.Now())
	if r.born != fromRoot {
		p.queued--
	}
	st := &running{id: r.id, open: r.open, sb: r.sb}
	p.running[r.id] = st
	p.c.Max(cMaxActive, int64(len(p.running)))
	return st
}

func (p *Engine[P]) leave(r Range, st *running, start time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.account(time.Now())
	delete(p.running, r.id)
	p.c.Add(cBusyNs, int64(time.Since(start)))
	if st.keys == 0 {
		switch r.born {
		case fromLower:
			p.c.Inc(cChildEmptyLower)
		case fromUpper:
			p.c.Inc(cChildEmptyUpper)
		}
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
		p.mu.Lock()
		ch := st.pending
		p.mu.Unlock()
		if ch != nil {
			<-ch
		}
	}()
	in := p.query()
	in.After = r.lo
	in.Through, in.Bounded = r.hi, !r.open
	c := r.lo
	p.maybeProbe(pctx, st, r, c)
	for {
		out, err := p.list(ctx, in)
		if err != nil {
			return err
		}
		skipped := 0
		if r.after != "" { // resume: drop keys already published
			n := 0
			for n < len(out.Rows) && out.Rows[n].Key <= r.after {
				n++
			}
			if n > 0 {
				c, skipped = out.Rows[n-1].Key, n
				out.Rows = out.Rows[n:]
				p.mu.Lock()
				p.c.Add(cOverfetchKeys, int64(n))
				p.mu.Unlock()
			}
		}
		kept, reached := len(out.Rows), false
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
		truncated := out.Truncated
		if truncated && out.Token == "" {
			return fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
		}
		if truncated && kept == 0 && !reached && skipped == 0 {
			return fmt.Errorf("%w: truncated page with no key in (%q, %q]", p.d.invalid(), r.lo, r.hi)
		}
		if err := p.emit(out.Rows[:kept], e); err != nil {
			return err
		}
		p.mu.Lock()
		p.c.Add(cOverfetchKeys, int64(len(out.Rows)-kept))
		st.keys += int64(kept)
		if kept > 0 {
			st.dens.observe(out.Rows[0].Key, out.Rows[kept-1].Key, r.lo, r.hi, kept)
		}
		p.mu.Unlock()
		done := reached || !truncated || (!r.open && kept > 0 && c == r.hi)
		if done {
			p.covered(r.lo, r.hi, r.open)
			return faults.At("range-done")
		}
		page := make([]string, kept)
		for i := range page {
			page[i] = out.Rows[i].Key
		}
		if m, ext, ok := p.decide(st, r, page, c); ok {
			m, ok, err := p.probe(ctx, r, c, m)
			if err != nil {
				p.commit(st, r, c, m, ext, false)
				return err
			}
			lower, upper := p.commit(st, r, c, m, ext, ok)
			if !ok {
				in.Token = out.Token
				p.maybeProbe(pctx, st, r, c)
				continue
			}
			if err := faults.At("split"); err != nil {
				return err
			}
			p.covered(r.lo, c, false)
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

// decide runs after a full (continuing) page of r with cursor c: split iff
// workers are idle (hunger = workers - active - queued > 0) and r ranks
// among the hunger largest eligible ranges (the open frontier first, then by
// keys listed). The pivot is the spec's: the safe midpoint of (c, hi], or a
// forward reflection for the open frontier, refined by the last page's
// density (see below). On success the two children are
// reserved in the demand count; the caller probes the pivot and then
// commits or releases the split.
func (p *Engine[P]) decide(st *running, r Range, page []string, c string) (m string, extrapolated, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st.continuing = true
	if st.unsplittable {
		return
	}
	if st.cooldown > 0 {
		st.cooldown--
		return
	}
	if coin := p.opt.SplitCoin; coin != nil {
		if !coin() {
			return
		}
	}
	// Remaining-estimate floor (bounded ranges; the frontier's estimate is
	// unbounded): counted per page, before demand, as Java does.
	est := math.Inf(1)
	if p.opt.Floors && !r.open {
		est = estRemaining(c, r.lo, r.hi, st.keys)
		if belowRemainingFloor(est) {
			p.c.Inc(cSkippedRemainingFloor)
			return
		}
	}
	hunger := p.opt.Workers - len(p.running) - p.queued
	if p.opt.SplitCoin == nil {
		if hunger <= 0 {
			p.c.Inc(cSplitSkippedNoDemand)
			return
		}
		rank := 0
		for _, o := range p.running {
			if o != st && o.continuing && !o.unsplittable && outranks(o, st) {
				rank++
			}
		}
		p.c.Inc(cSplitConsidered)
		if rank >= hunger {
			p.c.Inc(cSplitSkippedOutranked)
			return
		}
	}
	ceil, bounded := r.hi, !r.open
	if r.open {
		ceil, bounded = prefixCeil(p.opt.Prefix)
	}
	var found, structural bool
	if p.opt.Structure && !r.open {
		p.absorb(st)
		m, structural = boundary(st.sb, c, r.hi)
		found = structural
	}
	switch {
	case structural:
	case r.open:
		m, found = forwardReflect(r.lo, c)
	default:
		m, found = between(c, r.hi)
	}
	if found && m > c && !structural {
		// Refinement from the page's density (span·max(4, hunger) past the
		// cursor, at most halfway to hi): taken when it is nearer the cursor
		// than the spec pivot, or when the spec pivot is within one page span
		// of the cursor, where its lower child would be empty. The probe
		// below catches a refinement that overshoots the data.
		e, eok := extrapolate(page, c, ceil, bounded, int64(max(4, hunger)))
		one, ook := extrapolate(page, c, ceil, bounded, 1)
		if eok && c < e && (e < m || (ook && m < one)) {
			m, extrapolated = e, true
		}
	}
	switch guard(c, m, found, ceil, bounded, false, p.safe) {
	case clauseNoPivot:
		p.c.Inc(cSplitRejectedNoPivot)
		st.unsplittable = true
		return
	case clauseUnsafeCursor:
		p.c.Inc(cSplitRejectedUnsafeCursor)
		st.unsplittable = true
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
	case clauseDone:
		p.c.Inc(cSplitRejectedDone)
		return
	}
	// Child-tail floor: the upper child must realize more than two pages.
	if p.opt.Floors && !r.open {
		f := pivotFraction(c, m, r.lo, r.hi)
		if tailBelowFloor(est, f, st.dens.ratio(c, r.lo, r.hi, st.keys)) {
			p.c.Inc(cSkippedTailFloor)
			return "", false, false
		}
	}
	p.queued += 2 // reserved while the pivot is probed
	if structural {
		p.c.Inc(cSplitPivotStructure)
	}
	return m, extrapolated, true
}

// maxProbes bounds the 1-key probes spent on one split.
const maxProbes = 3

// probe is Swath's 1-key probe: one request for 1 key after the
// pivot tells whether the upper child (m, hi] holds a key. If it is empty
// the pivot moves to the safe midpoint of (c, m] and is probed again, up to
// maxProbes times; then the split is skipped. Called without mu.
func (p *Engine[P]) probe(ctx context.Context, r Range, c, m string) (string, bool, error) {
	if !p.opt.Probe {
		return m, true, nil
	}
	for range maxProbes {
		in := p.query()
		in.After, in.Max, in.Probe = m, 1, true
		p.c.Inc(cProbeRequests)
		out, err := p.list(ctx, in)
		if err != nil {
			return "", false, err
		}
		if len(out.Rows) > 0 && (r.open || out.Rows[0].Key <= r.hi) {
			return m, true, nil
		}
		p.c.Inc(cProbeUpperEmpty) // an empty child avoided
		next, found := between(c, m)
		if !found || !(c < next && next < m) || !p.safe(next) {
			return "", false, nil
		}
		m = next
		p.c.Inc(cProbeMoved)
	}
	return "", false, nil
}

// commit accepts a probed split (children, counters, boundaries for the
// journal) or releases its reservation.
func (p *Engine[P]) commit(st *running, r Range, c, m string, extrapolated, ok bool) (lower, upper Range) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !ok {
		p.queued -= 2
		p.c.Inc(cSplitRejectedProbe)
		st.probeFails++
		st.cooldown = 1 << min(st.probeFails, 6) // back off: 2, 4, … 64 pages
		return
	}
	st.probeFails = 0
	if extrapolated {
		p.c.Inc(cSplitExtrapolated)
	}
	if r.open {
		p.c.Inc(cSplitAcceptedFrontier)
	} else {
		p.c.Inc(cSplitAcceptedBounded)
	}
	p.c.Max(cMaxQueued, int64(p.queued))
	lower = Range{lo: c, hi: m, id: p.ids.Add(1), born: fromLower,
		sb: structure{bounds: within(st.sb.bounds, c, m), capped: st.sb.capped}}
	upper = Range{lo: m, hi: r.hi, open: r.open, id: p.ids.Add(1), born: fromUpper,
		sb: structure{bounds: within(st.sb.bounds, m, r.hi), capped: st.sb.capped}}
	// A resumed range split before its cursor passed the resume key hands
	// the key on: keys up to it are already published.
	if r.after > c {
		lower.after = r.after
	}
	if r.after > m {
		upper.after = r.after
	}
	if os.Getenv("SWATH_SPLIT_TRACE") != "" {
		fmt.Fprintf(os.Stderr, "split id=%d lo=%q c=%q m=%q hi=%q open=%v keys=%d\n", r.id, r.lo, c, m, r.hi, r.open, st.keys)
	}
	return lower, upper
}

// outranks reports whether a is a larger remainder than b: the open
// frontier first, then more keys listed, then the older range.
func outranks(a, b *running) bool {
	sa, sb := float64(a.keys), float64(b.keys)
	if a.open {
		sa = math.Inf(1)
	}
	if b.open {
		sb = math.Inf(1)
	}
	if sa != sb {
		return sa > sb
	}
	return a.id < b.id
}

type clause uint8

const (
	clauseOK clause = iota
	clauseNoPivot
	clauseUnsafeCursor
	clauseCursor
	clauseUnsafePivot
	clauseBound
	clauseDone
)

// guard checks a split of (lo, H] at cursor c into (c, m] and (m, H]:
//   - done: never split a range whose last page completed it;
//   - unsafe cursor: c becomes the lower child's start-after;
//   - cursor: m must be > c, or the upper child would list (m, c] again;
//   - unsafe pivot: m becomes a bound; the provider may veto it
//     (Description.AcceptBound);
//   - bound: m must be < H (or below prefixCeil(scope) on the frontier), or
//     the lower child would list keys of the next range.
func guard(c, m string, found bool, ceil string, bounded, done bool, safe func(string) bool) clause {
	switch {
	case done:
		return clauseDone
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

// expandSeedPage sends one delimiter page and tiles the scope at its common
// prefixes; it emits no rows (the tiles list every key, including the
// page's own objects). The seed with --seed-descent=false.
func (p *Engine[P]) expandSeedPage(ctx context.Context, e source.Emitter[Range, P]) error {
	in := p.query()
	in.Probe, in.Delimiter = true, "/"
	out, err := p.list(ctx, in)
	if err != nil {
		return err
	}
	var cuts []string
	p.mu.Lock()
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
	if limit := min(1000, 4*p.opt.Workers); len(cuts) > limit {
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
	p.mu.Unlock()
	for _, t := range tiles {
		if err := e.Child(t); err != nil {
			return err
		}
	}
	return nil
}
