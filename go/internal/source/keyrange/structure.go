// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"slices"
	"strings"
	"sync"
)

// Structure probes: a delimiter page (max-keys 32) after the cursor lists
// the folders that follow it, and a split pivots on one of them, so both
// children hold real folders instead of a guess that can land in a gap.
// The page's density is leaf-local (one page's span lives in the last
// folder's digits) and the anchor reflection overshoots into gaps on
// sparse folder names: only a delimiter listing gives real boundaries
// across folders. Every level is probed at once, alongside the range's
// next page, and the result is used at a later split decision if it has
// arrived; a split never waits for it. Children inherit the folders inside
// their bounds, so one probe serves several generations of splits, and the
// suppression count, so a flat keyspace stops probing.
//
// When a range probes (Structure, the mode):
//   - the open frontier, while its cursor has a folder below the scope: the
//     scope root and the two folders above the cursor's own, whose furthest
//     boundary moves the frontier past whole folders;
//   - a bounded range only on evidence that its chain is stalled inside one
//     folder: its parent's pivot stayed in the cursor's own folder while
//     hi lies beyond it (Range.stall, stalled), the shape where the chain
//     peels one folder at a time.
//     StructureOn probes every bounded range, as a diagnostic.

const (
	structureKeys = 32 // max-keys of a structure probe
	// structureLevels: the probe covers the folder where the cursor and the
	// bound diverge and the next folder below it on the cursor's path. Java
	// backs out up to 3 levels below the first; on fourcastnetgfs the first
	// two levels gave 93 % of the folder pivots, so the probe stops there.
	structureLevels = 2
	// structureZeroLimit consecutive probes with no folder at any level stop
	// probing in a range and its descendants, which inherit the count: a
	// flat keyspace pays at most this many probes along any chain.
	structureZeroLimit = 8
)

// Structure is when structure probes are sent (Options.Structure).
type Structure uint8

const (
	StructureOff  Structure = iota // no probes
	StructureAuto                  // the frontier while hierarchical, bounded ranges on evidence of a stall (the command's default)
	StructureOn                    // every hierarchical range, without the evidence gate (diagnostic)
)

func (s Structure) String() string { return [...]string{"off", "auto", "on"}[s] }

// probeWhy is the reason a probe was sent, for its counter.
type probeWhy uint8

const (
	probeFrontier probeWhy = iota
	probeStall
	probeForced
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

// frontierPrefixes are the open frontier's probe prefixes for cursor c,
// coarse to fine: the scope root, then the second and first folders above
// the cursor's own (its own lists the cursor's siblings, rarely a folder).
func frontierPrefixes(c string, floor int) []string {
	var folders []int // ends of c's folders below the scope, after their '/'
	for i := floor; i < len(c); i++ {
		if c[i] == '/' {
			folders = append(folders, i+1)
		}
	}
	out := []string{c[:floor]}
	for _, k := range []int{len(folders) - 3, len(folders) - 2} {
		if k >= 0 && folders[k] > len(out[len(out)-1]) {
			out = append(out, c[:folders[k]])
		}
	}
	return out
}

// folder is s up to and including its last '/' ("" without one).
func folder(s string) string { return s[:strings.LastIndexByte(s, '/')+1] }

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

// within returns the boundaries strictly inside (lo, hi); hi "" is ⊤ (no
// bounded range has the hi ⊥).
func within(bounds []string, lo, hi string) []string {
	i, _ := slices.BinarySearch(bounds, lo)
	if i < len(bounds) && bounds[i] == lo {
		i++
	}
	j := len(bounds)
	if hi != "" {
		j, _ = slices.BinarySearch(bounds, hi)
	}
	if i >= j {
		return nil
	}
	return bounds[i:j]
}

// boundary picks the pivot among the boundaries inside (c, hi): the median,
// or the furthest of a capped sample or on the open frontier (hi ""),
// where the lower child inherits the rest and halves on them.
func boundary(s structure, c, hi string) (string, bool) {
	b := within(s.bounds, c, hi)
	if len(b) == 0 {
		return "", false
	}
	if s.capped || hi == "" {
		return b[len(b)-1], true
	}
	return b[len(b)/2], true
}

// hierarchical: the cursor has a folder below the scope prefix.
func hierarchical(c string, floor int) bool {
	return len(c) > floor && strings.IndexByte(c[floor:], '/') >= 0
}

// probeStructure lists the folders after c at every level at once and
// returns the coarsest level that has any inside (c, hi); hi "" is the
// open frontier.
func (p *Engine[P]) probeStructure(ctx context.Context, c, hi string) structure {
	prefixes := structurePrefixes(c, hi, len(p.opt.Prefix))
	if hi == "" {
		prefixes = frontierPrefixes(c, len(p.opt.Prefix))
	}
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
			p.c.Inc(cStructureRequests)
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
			if cp > c && (hi == "" || cp < hi) && p.safe(cp) {
				b = append(b, cp)
			}
		}
		if len(b) > 0 {
			slices.Sort(b)
			// The bounds outlive the probe page: children inherit them.
			s.bounds, s.capped = detach(slices.Compact(b)), l.capped
			return s
		}
	}
	return s
}

// maybeProbe starts a structure probe for r at cursor c when one could help
// the next split: structure on, a folder below the scope at the cursor,
// the open frontier or a bounded range on evidence of a stall (or every
// one, StructureOn), no known boundary left inside (c, hi], none in
// flight, not suppressed, and an idle worker now. Called without mu.
func (p *Engine[P]) maybeProbe(ctx context.Context, st *running, r Range, c string) {
	mode := p.opt.Structure
	if mode == StructureOff || !hierarchical(c, len(p.opt.Prefix)) || !p.safe(c) {
		return
	}
	why := probeFrontier
	switch {
	case r.open:
	case mode == StructureOn:
		why = probeForced
	case r.stall:
		why = probeStall
	default:
		return
	}
	p.lock()
	if st.pending != nil || len(within(st.sb.bounds, c, r.hi)) > 0 || p.opt.Workers-len(p.running)-p.queued <= 0 {
		p.unlock()
		return
	}
	if st.zeroes >= structureZeroLimit {
		p.c.Inc(cStructureSuppressed)
		if r.zeroes >= structureZeroLimit {
			p.c.Inc(cStructureSuppressedInherited)
		}
		p.unlock()
		return
	}
	p.c.Inc(probeSent[why])
	ch := make(chan structure, 1)
	st.pending = ch
	p.unlock()
	probe := func() { ch <- p.probeStructure(ctx, c, r.hi) }
	if probeInline {
		probe()
		return
	}
	go probe()
}

// probeInline (tests) runs a structure probe on its range's goroutine
// before the range's next page, so every probe has landed by the next
// split decision. The engine never waits for one: whether a probe lands
// in time is the race between its listing and the range's next page
// (structure.late counts the decisions it lost).
var probeInline bool

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
			p.c.Add(cStructureBoundaries, int64(len(s.bounds)))
			if s.capped {
				p.c.Inc(cStructureCapped)
			}
		}
	default:
		p.c.Inc(cStructureLate)
	}
}

// detach copies ss into one fresh buffer, in place. A page's strings are
// views of its parser arena (S3) or its directory's key arena (local), so a
// page string kept as a range bound would keep the whole page alive for as
// long as the range waits; one copy per page holds only the bytes kept.
func detach(ss []string) []string {
	n := 0
	for _, s := range ss {
		n += len(s)
	}
	var b strings.Builder
	b.Grow(n)
	for _, s := range ss {
		b.WriteString(s)
	}
	all := b.String()
	for i, s := range ss {
		ss[i], all = all[:len(s)], all[len(s):]
	}
	return ss
}
