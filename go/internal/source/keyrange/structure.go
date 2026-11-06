// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"slices"
	"strings"
	"sync"
)

// Structure probes, after Swath's thief structure probes: a delimiter page
// (max-keys 32) after the cursor lists the folders that follow it, and a
// split pivots on one of them (the median, or the furthest when the page
// was capped), so both children hold real folders instead of a byte-space
// guess that can land in a gap. Swath probes the levels serially, coarse to
// fine, after a 1-key probe found the upper child empty; here every level is
// probed at once, alongside the range's next page, and the result is used
// at a later split decision if it has arrived; a split never waits for it.
// Children inherit the folders inside their bounds, so one probe serves
// several generations of splits.

const (
	structureKeys = 32 // max-keys of a structure probe
	// structureLevels: the probe covers the folder where the cursor and the
	// bound diverge and the next folder below it on the cursor's path. Java
	// backs out up to 3 levels below the first; on fourcastnetgfs the first
	// two levels gave 93 % of the folder pivots, so the probe stops there.
	structureLevels = 2
	// structureZeroLimit consecutive probes with no folder at any level stop
	// probing in a range (Java STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD).
	structureZeroLimit = 8
)

// structure is a sorted set of folder boundaries known inside a range;
// capped marks a partial sample (the probe page continues: more folders
// lie past it, however many it showed), where the furthest boundary is
// taken.
type structure struct {
	bounds []string
	capped bool
	zero   bool // no level returned any folder
}

// structurePrefixes are the probe prefixes for cursor c below bound hi,
// coarse to fine: c up to its last '/' before c and hi diverge (at least
// the scope prefix), then each further folder of c.
func structurePrefixes(c, hi string, floor int) []string {
	n := commonLen(c, hi)
	l := 0
	if i := strings.LastIndexByte(c[:n], '/'); i >= 0 {
		l = i + 1
	}
	l = max(l, floor)
	out := []string{c[:l]}
	for len(out) < structureLevels {
		i := strings.IndexByte(c[l:], '/')
		if i < 0 {
			break
		}
		l += i + 1
		out = append(out, c[:l])
	}
	return out
}

// within returns the boundaries strictly inside (lo, hi).
func within(bounds []string, lo, hi string) []string {
	i, _ := slices.BinarySearch(bounds, lo)
	if i < len(bounds) && bounds[i] == lo {
		i++
	}
	j, _ := slices.BinarySearch(bounds, hi)
	if i >= j {
		return nil
	}
	return bounds[i:j]
}

// boundary picks the pivot among the boundaries inside (c, hi): the median,
// or the furthest of a capped sample.
func boundary(s structure, c, hi string) (string, bool) {
	b := within(s.bounds, c, hi)
	if len(b) == 0 {
		return "", false
	}
	if s.capped {
		return b[len(b)-1], true
	}
	return b[len(b)/2], true
}

// hierarchical: the cursor has a folder below the scope prefix.
func hierarchical(c string, floor int) bool {
	return len(c) > floor && strings.IndexByte(c[floor:], '/') >= 0
}

// probeStructure lists the folders after c at every level at once and
// returns the coarsest level that has any inside (c, hi).
func (p *Engine[P]) probeStructure(ctx context.Context, c, hi string) structure {
	prefixes := structurePrefixes(c, hi, len(p.opt.Prefix))
	type level struct {
		cps    []string
		capped bool
	}
	got := make([]level, len(prefixes))
	var wg sync.WaitGroup
	for i, pre := range prefixes {
		wg.Go(func() {
			in := p.query()
			in.Prefix, in.After, in.Delimiter, in.Max, in.Probe = pre, c, "/", min(structureKeys, p.opt.PageSize), true
			p.lock()
			p.c.Inc(cStructureRequests)
			p.unlock()
			out, err := p.list(ctx, in)
			if err != nil {
				return // structure is advice: a failed probe is no folder
			}
			got[i] = level{out.Prefixes, out.Truncated}
		})
	}
	wg.Wait()
	s := structure{zero: true}
	for _, l := range got {
		if len(l.cps) > 0 {
			s.zero = false
		}
		var b []string
		for _, cp := range l.cps {
			if cp > c && cp < hi && p.safe(cp) {
				b = append(b, cp)
			}
		}
		if len(b) > 0 {
			slices.Sort(b)
			s.bounds, s.capped = slices.Compact(b), l.capped
			return s
		}
	}
	return s
}

// maybeProbe starts a structure probe for r at cursor c when one could help
// the next split: structure on, a bounded range with a folder below the
// scope at the cursor, no known boundary left inside (c, hi], none in
// flight, not suppressed, and an idle worker now. Called without mu.
func (p *Engine[P]) maybeProbe(ctx context.Context, st *running, r Range, c string) {
	if !p.opt.Structure || r.open || !hierarchical(c, len(p.opt.Prefix)) || !p.safe(c) {
		return
	}
	p.lock()
	defer p.unlock()
	if st.pending != nil || len(within(st.sb.bounds, c, r.hi)) > 0 || p.opt.Workers-len(p.running)-p.queued <= 0 {
		return
	}
	if st.zeroes >= structureZeroLimit {
		p.c.Inc(cStructureSuppressed)
		return
	}
	ch := make(chan structure, 1)
	st.pending = ch
	go func() { ch <- p.probeStructure(ctx, c, r.hi) }()
}

// absorb takes a finished probe's result into st; mu held.
func (p *Engine[P]) absorb(st *running) {
	if st.pending == nil {
		return
	}
	select {
	case s := <-st.pending:
		st.pending = nil
		switch {
		case s.zero:
			st.zeroes++
			p.c.Inc(cStructureZero)
		case len(s.bounds) == 0:
			st.zeroes = 0
			p.c.Inc(cStructureNoBoundary)
		default:
			st.zeroes = 0
			st.sb = s
			if s.capped {
				p.c.Inc(cStructureCapped)
			}
		}
	default:
		p.c.Inc(cStructureLate)
	}
}
