// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/varveio/swath/go/internal/source"
)

// Parallel S3 core: Swath's range tiling, seed, demand-driven owner split
// and safe pivot, without probes, checkpointing or pacing (docs/internals/providers.md).
//
// A range is (lo, hi]: start-after is exclusive, so a boundary key belongs
// to the left range. lo "" is ⊥; open marks the frontier (hi = ⊤). The
// order stage makes a range's children claimable only when its Expand
// returns, so splits are owner-side: at a page boundary, when workers are
// idle, the owner emits (cursor, pivot] and (pivot, hi] and returns.

type kind uint8

const (
	interval kind = iota
	whole         // sequential oracle: the scope as one continuation chain
	seed          // one delimiter page that emits the first tiling
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
}

// Root is the scope: the sequential chain, the seed, or (⊥, ⊤].
func (p *Provider) Root() Range {
	switch {
	case p.opt.Sequential:
		return Range{kind: whole}
	case p.opt.Seed:
		return Range{kind: seed}
	}
	p.mu.Lock()
	p.stats.Seed.Skipped = true
	p.mu.Unlock()
	return Range{kind: interval, open: true}
}

// Expand lists one range.
func (p *Provider) Expand(ctx context.Context, r Range, e source.Emitter[Range]) error {
	switch r.kind {
	case whole:
		return p.expandSequential(ctx, e)
	case seed:
		return p.expandSeed(ctx, e)
	}
	return p.expandRange(ctx, r, e)
}

// account integrates the active count over time; mu held.
func (p *Provider) account(now time.Time) {
	p.area += float64(len(p.running)) * now.Sub(p.areaAt).Seconds()
	p.areaAt = now
}

func (p *Provider) enter(r Range) *running {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.account(time.Now())
	if r.born != fromRoot {
		p.queued--
	}
	st := &running{id: r.id, open: r.open}
	p.running[r.id] = st
	p.stats.MaxActive = max(p.stats.MaxActive, len(p.running))
	return st
}

func (p *Provider) leave(r Range, st *running, start time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.account(time.Now())
	delete(p.running, r.id)
	p.stats.BusyNs += int64(time.Since(start))
	if st.keys == 0 {
		switch r.born {
		case fromLower:
			p.stats.ChildEmpty.Lower++
		case fromUpper:
			p.stats.ChildEmpty.Upper++
		}
	}
}

// expandRange lists (lo, hi]: the first page starts after lo, later pages
// follow continuation tokens; keys past hi are trimmed and end the range.
func (p *Provider) expandRange(ctx context.Context, r Range, e source.Emitter[Range]) error {
	start := time.Now()
	st := p.enter(r)
	defer p.leave(r, st, start)
	in := p.input()
	in.StartAfter = r.lo
	c := r.lo
	for {
		out, err := p.list(ctx, in)
		if err != nil {
			return err
		}
		kept, reached := len(out.Contents), false
		for i := range out.Contents {
			k := out.Contents[i].Key
			if (c != "" || i > 0) && k <= c {
				return fmt.Errorf("listing out of order in (%q, %q]: %q after %q", r.lo, r.hi, k, c)
			}
			if !r.open && k > r.hi {
				kept, reached = i, true
				break
			}
			c = k
		}
		truncated := out.IsTruncated
		if truncated && out.NextContinuationToken == "" {
			return errors.New("truncated page without a continuation token")
		}
		if truncated && kept == 0 && !reached {
			return fmt.Errorf("truncated page with no key in (%q, %q]", r.lo, r.hi)
		}
		if err := p.emit(p.rows(out, kept), e); err != nil {
			return err
		}
		p.mu.Lock()
		p.stats.OverfetchKeys += int64(len(out.Contents) - kept)
		st.keys += int64(kept)
		p.mu.Unlock()
		done := reached || !truncated || (!r.open && kept > 0 && c == r.hi)
		if done {
			p.covered(r.lo, r.hi, r.open)
			return nil
		}
		page := make([]string, kept)
		for i := range page {
			page[i] = out.Contents[i].Key
		}
		if lower, upper, ok := p.decide(st, r, page, c); ok {
			p.covered(r.lo, c, false)
			if err := e.Child(lower); err != nil {
				return err
			}
			return e.Child(upper)
		}
		in.StartAfter, in.ContinuationToken = "", out.NextContinuationToken
	}
}

func (p *Provider) covered(lo, hi string, open bool) {
	if p.opt.trace != nil {
		p.opt.trace(lo, hi, open)
	}
}

// decide runs after a full (continuing) page of r with cursor c: split iff
// workers are idle (hunger = workers - active - queued > 0) and r ranks
// among the hunger largest eligible ranges (the open frontier first, then by
// keys listed). The pivot is the safe midpoint of (c, hi], or a forward
// reflection for the frontier.
func (p *Provider) decide(st *running, r Range, page []string, c string) (lower, upper Range, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st.continuing = true
	if st.unsplittable {
		return
	}
	if coin := p.opt.splitCoin; coin != nil {
		if !coin() {
			return
		}
	}
	hunger := p.opt.Workers - len(p.running) - p.queued
	if p.opt.splitCoin == nil {
		if hunger <= 0 {
			p.stats.Split.Skipped.NoDemand++
			return
		}
		rank := 0
		for _, o := range p.running {
			if o != st && o.continuing && !o.unsplittable && outranks(o, st) {
				rank++
			}
		}
		p.stats.Split.Considered++
		if rank >= hunger {
			p.stats.Split.Skipped.Outranked++
			return
		}
	}
	ceil, bounded := r.hi, !r.open
	if r.open {
		ceil, bounded = prefixCeil(p.opt.Prefix)
	}
	// Density extrapolation from the last page (span·max(4, hunger) ahead,
	// at most halfway to hi); the safe midpoint or forward reflection when
	// it cannot produce a safe key.
	m, found := extrapolate(page, c, ceil, bounded, int64(max(4, hunger)))
	if found {
		p.stats.Split.Extrapolated++
	} else if r.open {
		m, found = forwardReflect(r.lo, c)
	} else {
		m, found = between(c, r.hi)
	}
	switch guard(c, m, found, ceil, bounded, false) {
	case clauseNoPivot:
		p.stats.Split.Rejected.NoPivot++
		st.unsplittable = true
		return
	case clauseUnsafeCursor:
		p.stats.Split.Rejected.UnsafeCursor++
		st.unsplittable = true
		return
	case clauseCursor:
		p.stats.Split.Rejected.Cursor++
		return
	case clauseBound:
		p.stats.Split.Rejected.Bound++
		return
	case clauseDone:
		p.stats.Split.Rejected.Done++
		return
	}
	if r.open {
		p.stats.Split.AcceptedFrontier++
	} else {
		p.stats.Split.AcceptedBounded++
	}
	p.queued += 2
	p.stats.MaxQueued = max(p.stats.MaxQueued, p.queued)
	lower = Range{lo: c, hi: m, id: p.ids.Add(1), born: fromLower}
	upper = Range{lo: m, hi: r.hi, open: r.open, id: p.ids.Add(1), born: fromUpper}
	if os.Getenv("SWATH_LOCAL_SPLIT_TRACE") != "" {
		fmt.Fprintf(os.Stderr, "split id=%d lo=%q c=%q m=%q hi=%q open=%v keys=%d\n", r.id, r.lo, c, m, r.hi, r.open, st.keys)
	}
	return lower, upper, true
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
	clauseBound
	clauseDone
)

// guard checks a split of (lo, H] at cursor c into (c, m] and (m, H]:
//   - done: never split a range whose last page completed it;
//   - unsafe cursor: c becomes the lower child's start-after;
//   - cursor: m must be > c, or the upper child would list (m, c] again;
//   - bound: m must be < H (or below prefixCeil(scope) on the frontier), or
//     the lower child would list keys of the next range.
func guard(c, m string, found bool, ceil string, bounded, done bool) clause {
	switch {
	case done:
		return clauseDone
	case !found:
		return clauseNoPivot
	case !safeString(c):
		return clauseUnsafeCursor
	case m <= c:
		return clauseCursor
	case bounded && m >= ceil:
		return clauseBound
	}
	return clauseOK
}

// expandSeed sends one delimiter page and tiles the scope at its common
// prefixes; it emits no rows (the tiles list every key, including the
// page's own objects).
func (p *Provider) expandSeed(ctx context.Context, e source.Emitter[Range]) error {
	in := p.input()
	in.FetchOwner, in.Delimiter = false, "/"
	out, err := p.list(ctx, in)
	if err != nil {
		return err
	}
	var cuts []string
	p.mu.Lock()
	ss := &p.stats.Seed
	ss.Prefixes = int64(len(out.CommonPrefixes))
	ss.Truncated = out.IsTruncated
	for _, k := range out.CommonPrefixes {
		if safeString(k) {
			cuts = append(cuts, k)
		} else {
			ss.Unsafe++
		}
	}
	if limit := min(1000, 4*p.opt.Workers); len(cuts) > limit {
		picked := make([]string, 0, limit)
		for i := range limit {
			picked = append(picked, cuts[(i*len(cuts)+limit/2)/limit])
		}
		cuts, ss.Subsampled = picked, true
	}
	ss.Cuts = int64(len(cuts))
	// Closing sentinel: when the page holds the whole top level and ends with
	// a prefix, bound the last tile at prefixCeil(pk) so it is not the open
	// frontier (the bounded midpoint then applies to it).
	var sentinel string
	if k := len(cuts); k > 0 {
		pk := cuts[k-1]
		lastObject := ""
		if n := len(out.Contents); n > 0 {
			lastObject = out.Contents[n-1].Key
		}
		lastPrefix := out.CommonPrefixes[len(out.CommonPrefixes)-1]
		u, ok := prefixCeil(pk)
		switch {
		case ss.Truncated:
			ss.Declined["truncated"]++
		case lastObject > lastPrefix || pk != lastPrefix:
			ss.Declined["object_last"]++
		case !ok || u <= pk:
			ss.Declined["no_ceiling"]++
		case !safeString(u):
			ss.Declined["unsafe"]++
		default:
			sentinel, ss.Sentinel = u, true
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
	p.stats.MaxQueued = max(p.stats.MaxQueued, p.queued)
	p.stats.Pages++
	p.mu.Unlock()
	for _, t := range tiles {
		if err := e.Child(t); err != nil {
			return err
		}
	}
	return nil
}
