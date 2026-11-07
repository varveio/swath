// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"testing"

	"github.com/varveio/swath/go/internal/counters"
)

// TestBetweenSources: between names the synthesis that made its pivot.
func TestBetweenSources(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want pivotSource
	}{
		{"a", "c", srcMidpoint},
		{"a", "a!", srcMidpoint}, // a prefix: a safe scalar below '!'
		{"a", "b", srcSliver},    // adjacent scalars: a + " "
		{"a", "a  x", srcSliver}, // a prefix, nothing safe below ' ': a + " "
	} {
		m, src, ok := betweenFrom(c.a, c.b)
		if !ok || src != c.want || m <= c.a || m >= c.b {
			t.Fatalf("betweenFrom(%q, %q) = %q, %v, %v; want a %v pivot", c.a, c.b, m, src, ok, c.want)
		}
		if m2, ok2 := between(c.a, c.b); m2 != m || ok2 != ok {
			t.Fatalf("between(%q, %q) = %q, %v; betweenFrom %q", c.a, c.b, m2, ok2, m)
		}
	}
}

// TestExtrapolateReasons: every refusal of the density refinement has its
// reason, and extrapolate agrees with extrapolateWhy.
func TestExtrapolateReasons(t *testing.T) {
	for _, c := range []struct {
		page    []string
		c, hi   string
		bounded bool
		want    extReason
	}{
		{[]string{"a"}, "a", "", false, extShort},
		{[]string{"a%1", "a%2"}, "a%2", "", false, extAlphabet},
		{[]string{"aé", "aë"}, "aë", "", false, extAlphabet},
		{[]string{"x", "x"}, "x", "", false, extZeroSpan},
		{[]string{"a", "b"}, "b", "", false, extPastTop},
		{[]string{"abb", "b"}, "b", "", false, extHole},
		{[]string{"ab", "ac"}, "ac", "ad", true, extOutside},
		{[]string{"k0001", "k0100"}, "k0100", "", false, extOK},
	} {
		m, why := extrapolateWhy(c.page, c.c, c.hi, c.bounded, 1)
		if why != c.want {
			t.Fatalf("extrapolateWhy(%q, %q, %q) = %q, reason %d; want %d", c.page, c.c, c.hi, m, why, c.want)
		}
		if m2, ok := extrapolate(c.page, c.c, c.hi, c.bounded, 1); m2 != m || ok != (why == extOK) {
			t.Fatalf("extrapolate disagrees: %q %v", m2, ok)
		}
	}
}

// TestDecideCountsRefusals: a split decision whose density refinement is
// refused counts the reason, and keeps the spec pivot (a frontier's
// reflection here). The refinement jumps max(4, hunger) page spans.
func TestDecideCountsRefusals(t *testing.T) {
	for _, c := range []struct {
		page []string
		id   counters.ID
	}{
		{[]string{"x", "x"}, cDensityRefusedZeroSpan},
		{[]string{"abbbb", "b"}, cDensityRefusedHole}, // b + 4·span = digits b, end, end, a, a: a hole
	} {
		p, err := newTest(newSorted(nil, false, 1), Options{Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		cur := c.page[len(c.page)-1]
		pv, ok := p.decidePage(&running{id: 1, open: true}, Range{open: true, id: 1}, c.page, cur)
		want, _ := forwardReflect("", cur)
		if !ok || pv.src != srcReflect || pv.m != want || p.c.Load(c.id) != 1 {
			t.Fatalf("%q: %+v %v, %s = %d", c.page, pv, ok, Counters[c.id].Key, p.c.Load(c.id))
		}
	}
}

// TestUnsplittableDrain: a range with no pivot above a safe cursor (hi is
// the cursor plus a space, and the remaining keys sit below it) is proven
// unsplittable and drains its remainder; the report counts both. An
// unsafe cursor (a '%' key) declines that page's split only.
func TestUnsplittableDrain(t *testing.T) {
	keys := make([]string, 1000)
	for i := range 100 {
		keys[i] = fmt.Sprintf("k%04d", i)
	}
	for i := 100; i < 1000; i++ {
		keys[i] = fmt.Sprintf("k0099\x01%04d", i)
	}
	p, err := newTest(newSorted(keys, false, 1), Options{Workers: 4, PageSize: 100, SplitCoin: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	e := &clearing{}
	if err := p.expandRange(t.Context(), Range{lo: "", hi: "k0099 ", id: 1, born: fromSeed}, e); err != nil {
		t.Fatal(err)
	}
	if len(e.children) != 0 || p.c.Load(cUnsplittable) != 1 || p.c.Load(cUnsplittableKeys) != 900 || p.c.Load(cSplitRejectedNoPivot) != 1 {
		t.Fatalf("children=%d unsplittable=%d keys=%d no_pivot=%d", len(e.children), p.c.Load(cUnsplittable), p.c.Load(cUnsplittableKeys), p.c.Load(cSplitRejectedNoPivot))
	}
	if p.c.Load(cLeaves) != 1 || p.c.Load(cLeafDepthSum) != 0 {
		t.Fatalf("leaves=%d depth sum=%d", p.c.Load(cLeaves), p.c.Load(cLeafDepthSum))
	}

	for i := range keys {
		keys[i] = fmt.Sprintf("k%%%04d", i)
	}
	got, st, _ := run(t, newSorted(keys, false, 1), Options{PageSize: 100}, 8)
	if len(got) != len(keys) {
		t.Fatalf("%d keys", len(got))
	}
	if st.Get(cUnsplittable) != 0 || st.Get(cSplitRejectedUnsafeCursor) != 9 {
		t.Fatalf("unsplittable=%d unsafe_cursor=%d", st.Get(cUnsplittable), st.Get(cSplitRejectedUnsafeCursor))
	}
}

// TestSliverRefused: a range whose only pivot is a sliver just above the
// cursor (the reflection from its first key, "aé", passes hi; no safe
// scalar lies between the cursor and hi, "aé1"; non-ASCII keys refuse the
// density step) is not split, since its lower child would be all but
// certain to be empty. It is not made unsplittable either: it decides
// again after every page, and lists through its bound alone.
func TestSliverRefused(t *testing.T) {
	keys := []string{"aé", "b"}
	for i := range 400 {
		keys = append(keys, fmt.Sprintf("aé%04d", i))
	}
	p, err := newTest(newSorted(keys, false, 1), Options{Workers: 4, PageSize: 100, SplitCoin: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	e := &clearing{}
	if err := p.expandRange(t.Context(), Range{lo: "", hi: "aé1", id: 1, born: fromSeed}, e); err != nil {
		t.Fatal(err)
	}
	got := [...]int64{p.c.Load(cSplitSliverRefused), p.c.Load(cReflectCeiling), p.c.Load(cDensityRefusedAlphabet),
		p.c.Load(cUnsplittable), p.c.Load(cLeaves), int64(len(e.children))}
	if got != [...]int64{4, 4, 4, 0, 1, 0} {
		t.Fatalf("sliver refused, reflect ceiling, density refused (alphabet), unsplittable, leaves, children = %v", got)
	}
}

// TestStructurePivotEmpties: a folder boundary the range already knows is
// the pivot; a boundary with no key below it (past the cursor; the keys
// resume at a/5000, so the page's density reads the gap below a/2 as many
// pages and the admission gates allow the split) empties the lower child,
// one with none above it empties the upper.
func TestStructurePivotEmpties(t *testing.T) {
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("a/%04d", i%100+i/100*5000)
	}
	for _, c := range []struct {
		bound string
		empty counters.ID
	}{{"a/2", cChildEmptyStructureLower}, {"m/", cChildEmptyStructureUpper}} {
		p, err := newTest(newSorted(keys, false, 1), Options{Workers: 4, PageSize: 100, Structure: StructureOn, SplitCoin: func() bool { return true }})
		if err != nil {
			t.Fatal(err)
		}
		e := &clearing{}
		r := Range{lo: "", hi: "y", id: 1, born: fromSeed, sb: structure{bounds: []string{c.bound}}}
		if err := p.expandRange(t.Context(), r, e); err != nil {
			t.Fatal(err)
		}
		if len(e.children) != 2 || e.children[0].hi != c.bound || p.c.Load(cSplitPivotStructure) != 1 {
			t.Fatalf("%s: children %+v", c.bound, e.children)
		}
		for _, ch := range e.children {
			p.opt.SplitCoin = func() bool { return false }
			if err := p.expandRange(t.Context(), ch, &clearing{}); err != nil {
				t.Fatal(err)
			}
		}
		if p.c.Load(c.empty) != 1 || p.c.Load(cChildEmptyLower)+p.c.Load(cChildEmptyUpper) != 1 {
			t.Fatalf("%s: %s = %d", c.bound, Counters[c.empty].Key, p.c.Load(c.empty))
		}
	}
}

// TestInstrumentationEngages runs the metamorphic variants over the
// fixtures and checks the report's identities on each run: the pivot
// sources sum to the accepted splits, the empty children by source and by
// hi sum to child_empty, small children include the empty ones, the mean
// leaf depth is at most the longest chain, a frontier split's hand-out is
// part of the listing. Over all runs, every new engagement counter fires
// (the sliver, structure, unsplittable, zero-span and hole paths, whose
// engagement depends on the fixture or on timing, have their own tests).
func TestInstrumentationEngages(t *testing.T) {
	fired := map[counters.ID]int64{}
	variants := []Options{{}, {Seed: true}, {Structure: StructureOn}, {Seed: true, Structure: StructureOn, Floors: true}, {Structure: StructureAuto}}
	spaces := keyspaces()
	for k, v := range hostile() {
		spaces[k] = v
	}
	for name, keys := range spaces {
		for i, opt := range variants {
			for _, w := range []int{4, 64} {
				api := newSorted(keys, true, int64(i+1))
				api.PageCap = 13
				got, st, _ := run(t, api, opt, w)
				what := fmt.Sprintf("%s/%d/w%d", name, i, w)
				check(t, what, st, int64(len(got)))
				for id := range numCounters {
					fired[id] += st.Get(id)
				}
			}
		}
	}
	for _, id := range []counters.ID{cDensityRefusedAlphabet, cDensityRefusedPastTop, cDensityRefusedOutside, cSplitPivotReflect, cSplitPivotMidpoint, cSplitPivotDensityBounded, cSplitPivotDensityFrontier,
		cDensityRefusedShort, cDensityShrinkPrevented, cLeaves, cLeafDepthSum, cIdleDemandNs,
		cChildEmptyReflectLower, cChildEmptyReflectUpper, cChildEmptyMidpointLower, cChildEmptyMidpointUpper,
		cChildEmptyDensityLower, cChildEmptyDensityUpper,
		cChildEmptyHiPivot, cChildEmptyHiBound, cChildSmallLower, cChildSmallUpper, cFrontierHandoutKeys} {
		if fired[id] == 0 {
			t.Errorf("%s never fired", Counters[id].Key)
		}
	}
}

func check(t *testing.T, what string, st Stats, listed int64) {
	t.Helper()
	g := st.Get
	sum := func(ids ...counters.ID) (n int64) {
		for _, id := range ids {
			n += g(id)
		}
		return n
	}
	accepted := g(cSplitAcceptedBounded) + g(cSplitAcceptedFrontier)
	if s := sum(cSplitPivotReflect, cSplitPivotMidpoint, cSplitPivotDensityBounded, cSplitPivotDensityFrontier, cSplitPivotStructure); s != accepted {
		t.Fatalf("%s: pivot sources sum to %d, %d accepted", what, s, accepted)
	}
	empty := g(cChildEmptyLower) + g(cChildEmptyUpper)
	var by int64
	for src, ids := range emptyBy {
		if s := pivotSource(src); s != srcBound && s != srcSliver {
			by += g(ids[0]) + g(ids[1])
		}
	}
	if by != empty || g(cChildEmptyHiPivot)+g(cChildEmptyHiBound) != empty {
		t.Fatalf("%s: %d empty children, %d by source, %d+%d by hi", what, empty, by, g(cChildEmptyHiPivot), g(cChildEmptyHiBound))
	}
	if g(cChildSmallLower) < g(cChildEmptyLower) || g(cChildSmallUpper) < g(cChildEmptyUpper) {
		t.Fatalf("%s: small children %d/%d below empty %d/%d", what, g(cChildSmallLower), g(cChildSmallUpper), g(cChildEmptyLower), g(cChildEmptyUpper))
	}
	if g(cLeaves) == 0 || g(cLeafDepthSum) > g(cLeaves)*g(cSplitMaxDepth) {
		t.Fatalf("%s: leaves %d, depth sum %d, max depth %d", what, g(cLeaves), g(cLeafDepthSum), g(cSplitMaxDepth))
	}
	if g(cFrontierHandoutKeys) > listed || (g(cSplitAcceptedFrontier) == 0 && g(cFrontierHandoutKeys) != 0) {
		t.Fatalf("%s: hand-out %d keys of %d, %d frontier splits", what, g(cFrontierHandoutKeys), listed, g(cSplitAcceptedFrontier))
	}
}

// TestAnchoredStructuralFrontierSplit: a frontier split whose chain has an
// anchor counts in split.frontier.anchored also when a folder boundary
// beyond the policy's pivot wins.
func TestAnchoredStructuralFrontierSplit(t *testing.T) {
	p, err := newTest(newSorted(nil, false, 1), Options{Workers: 4, Structure: StructureOn})
	if err != nil {
		t.Fatal(err)
	}
	st, r := &running{id: 1, open: true, sb: structure{bounds: []string{"z/"}}}, Range{open: true, id: 1, anchor: "a/"}
	pv, ok := p.decidePage(st, r, []string{"b/a", "b/x"}, "b/x")
	if !ok || pv.src != srcStructure || pv.m != "z/" || pv.chain != "a/" {
		t.Fatalf("%+v %v", pv, ok)
	}
	p.commit(st, r, "b/x", pv)
	if p.c.Load(cFrontierAnchored) != 1 || p.c.Load(cStructureFrontierPivots) != 1 {
		t.Fatalf("anchored=%d structure frontier=%d", p.c.Load(cFrontierAnchored), p.c.Load(cStructureFrontierPivots))
	}
}
