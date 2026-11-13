// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/counters"
)

// TestGaugeEstimates: the gauge reads a page of decimal counters in its
// own digits (a remainder of two pages reads near two pages, not 25 times
// that as base 256 would), reads a pivot past the page's digits as the end
// of them, reads a page that crosses folders by its run under the
// interval's prefix, and gives no estimate (+Inf) where the page cannot
// read the interval: a bound in another folder.
func TestGaugeEstimates(t *testing.T) {
	page := make([]string, 1000)
	for i := range page {
		page[i] = fmt.Sprintf("logs/p/%09d", 8000+i)
	}
	c := page[999]
	cross := make([]string, 1000)
	for i := range cross {
		if i < 500 {
			cross[i] = fmt.Sprintf("v2/MIROC6/%d.%d", 100+i/10, i%10)
		} else {
			cross[i] = fmt.Sprintf("v2/NorESM2/%d.%d", 100+(i-500)/10, i%10)
		}
	}
	for _, tc := range []struct {
		page     []string
		x, y     string
		min, max float64
	}{
		{page, c, "logs/p/000010999", 1500, 4000},                    // two pages (ten digits read in radix 11)
		{page, c, "logs/p/00001", 800, 1500},                         // one page, to a truncated pivot
		{page, "logs/p/00001l", "logs/p/00002", 0, 0},                // past the digits: nothing
		{page, c, "logs/p/000208999", 150_000, 300_000},              // 200 pages
		{page, c, "logs/q", math.Inf(1), math.Inf(1)},                // another folder
		{page, c, "logs/p0", math.Inf(1), math.Inf(1)},               // past the page's folder
		{cross, cross[999], "v2/NorESM2/180", 200, 600},              // the run under v2/NorESM2/1
		{cross, cross[999], "v2/NorESM2~", math.Inf(1), math.Inf(1)}, // past the page's folder
	} {
		g := newGauge(tc.page)
		if got := g.keys(tc.x, tc.y); got < tc.min || got > tc.max {
			t.Errorf("keys(%q, %q) = %v, want in [%v, %v]", tc.x, tc.y, got, tc.min, tc.max)
		}
	}
	if newGauge(page[:1]) != nil {
		t.Error("a one-key page has a gauge")
	}
}

// TestAdmissionEngages: on the synthetic shapes at 64 workers, each gate
// refuses splits (remainder, lower child, upper child), the listing is the
// keyspace and the ranges tile it; on flat keys the refusals leave fewer
// empty children than a sixth of the splits (without the gates: half).
// The split tree stays within 4·log2(pages)+8, and the report shows the
// paths that placed its pivots: frontier anchor reflections, bounded
// gallops, and midpoint fallbacks on the hierarchical shapes.
func TestAdmissionEngages(t *testing.T) {
	gates := []counters.ID{cSplitSkippedRemainder, cSplitSkippedLowerTiny, cSplitSkippedUpperTiny}
	fired := map[counters.ID]int64{}
	all := shapes()
	for _, name := range []string{"date", "nbm", "flat", "hash"} {
		keys := all[name]
		f := newSorted(keys, false, 1)
		f.Latency = time.Millisecond
		got, st, leaves := runBudget(t, f, Options{PageSize: shapePage}, 64, 1<<20)
		if !slices.Equal(got, slices.Sorted(slices.Values(keys))) {
			t.Fatalf("%s: %d keys, want %d", name, len(got), len(keys))
		}
		checkTiling(t, name, leaves)
		check(t, name, st, int64(len(got)))
		g := st.Get
		splits := g(cSplitAcceptedBounded) + g(cSplitAcceptedFrontier)
		t.Logf("%s: requests=%d splits=%d leaves=%d empty=%d/%d remainder=%d lower_tiny=%d upper_tiny=%d estimate_none=%d depth=%d",
			name, g(cRequests), splits, g(cLeaves), g(cChildEmptyLower), g(cChildEmptyUpper), g(cSplitSkippedRemainder),
			g(cSplitSkippedLowerTiny), g(cSplitSkippedUpperTiny), g(cSplitEstimateNone), g(cSplitMaxDepth))
		for _, id := range gates {
			fired[id] += g(id)
		}
		if name == "flat" && 6*(g(cChildEmptyLower)+g(cChildEmptyUpper)) > splits {
			t.Errorf("flat: %d empty children of %d splits", g(cChildEmptyLower)+g(cChildEmptyUpper), splits)
		}
		t.Logf("%s: depth=%d (bound %d) requests=%d frontier=%d bounded=%d reflect=%d midpoint=%d density=%d/%d ceiling=%d shrink_prevented=%d max_gen=%d halvings=%d sliver_refused=%d empty=%d/%d",
			name, g(cSplitMaxDepth), depthBound(len(keys), shapePage, 4), g(cRequests), g(cSplitAcceptedFrontier), g(cSplitAcceptedBounded),
			g(cSplitPivotReflect), g(cSplitPivotMidpoint), g(cSplitPivotDensityBounded), g(cSplitPivotDensityFrontier), g(cReflectCeiling),
			g(cDensityShrinkPrevented), g(cGallopMaxGen), g(cGallopHalvings), g(cSplitSliverRefused), g(cChildEmptyLower), g(cChildEmptyUpper))
		if d := g(cSplitMaxDepth); d > depthBound(len(keys), shapePage, 4) {
			t.Errorf("%s: split.max_depth %d over %d", name, d, depthBound(len(keys), shapePage, 4))
		}
		pivots := []counters.ID{cSplitAcceptedFrontier, cFrontierAnchored, cSplitPivotReflect, cSplitPivotDensityBounded, cGallopMaxGen}
		if name == "date" || name == "nbm" {
			pivots = append(pivots, cReflectCeiling, cSplitPivotMidpoint)
		}
		for _, id := range pivots {
			if g(id) == 0 {
				t.Errorf("%s: %s never fired", name, Counters[id].Key)
			}
		}
	}
	for _, id := range gates {
		if fired[id] == 0 {
			t.Errorf("%s never fired", Counters[id].Key)
		}
	}
}

// TestAdmissionFlat: a flat list of 1,000 pages at 64 workers ends with
// fewer than 360 ranges (measured 270-330 at 100,000 keys in pages of 100,
// race and GOMAXPROCS=1 included), about twice the 100-150 a 64-wide run
// needs (without the gates: 630-810), within the depth bound and still
// about 64 wide. The million-key, 1,000-key-page run of the same law is the
// performance tier's (SWATH_PERF=1; measured 270-310 ranges).
func TestAdmissionFlat(t *testing.T) {
	n, page, lat := 100_000, shapePage, time.Millisecond
	if os.Getenv("SWATH_PERF") != "" {
		n, page, lat = 1_000_000, PageKeys, 5*time.Millisecond
	}
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("logs/p/%09d", i)
	}
	f := newSorted(keys, false, 1)
	f.Latency = lat
	got, st, leaves := runBudget(t, f, Options{PageSize: page}, 64, 131_072)
	if len(got) != len(keys) {
		t.Fatalf("%d keys", len(got))
	}
	checkTiling(t, "flat", leaves)
	g := st.Get
	t.Logf("keys=%d requests=%d leaves=%d empty=%d/%d remainder=%d lower_tiny=%d upper_tiny=%d depth=%d max_active=%d",
		n, g(cRequests), g(cLeaves), g(cChildEmptyLower), g(cChildEmptyUpper), g(cSplitSkippedRemainder), g(cSplitSkippedLowerTiny),
		g(cSplitSkippedUpperTiny), g(cSplitMaxDepth), g(cMaxActive))
	if g(cLeaves) >= 360 {
		t.Errorf("%d ranges, want fewer than 360", g(cLeaves))
	}
	if d := g(cSplitMaxDepth); d > depthBound(len(keys), page, 4) {
		t.Errorf("split.max_depth %d over %d", d, depthBound(len(keys), page, 4))
	}
	if g(cMaxActive) < 32 {
		t.Errorf("max_active %d: the gates starved the workers", g(cMaxActive))
	}
}

// TestAdmissionRedecides: a refusal declines one page's split, never the
// range's: a range whose first page reads under two pages left (100 keys
// k0000-k0099 before hi k0199/9; the 1,000 keys under k0199/ are invisible
// to it) is not split then, and splits after its second page, which cannot
// read past k0199 and gives no estimate.
func TestAdmissionRedecides(t *testing.T) {
	var keys []string
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("k%04d", i))
	}
	for i := range 1000 {
		keys = append(keys, fmt.Sprintf("k0199/%04d", i))
	}
	p, err := newTest(newSorted(keys, false, 1), Options{Workers: 4, PageSize: 100, SplitCoin: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	e := &clearing{}
	if err := p.expandRange(t.Context(), Range{lo: "", hi: "k0199/9", id: 1, born: fromSeed}, e); err != nil {
		t.Fatal(err)
	}
	if p.c.Load(cSplitSkippedRemainder) != 1 || len(e.children) != 2 || p.c.Load(cUnsplittable) != 0 || p.c.Load(cPages) != 2 {
		t.Fatalf("remainder=%d children=%d unsplittable=%d pages=%d", p.c.Load(cSplitSkippedRemainder), len(e.children),
			p.c.Load(cUnsplittable), p.c.Load(cPages))
	}
	if e.children[0].lo != "k0199" {
		t.Fatalf("lower child %+v", e.children[0])
	}
}

// TestAdmissionOverride: the gates never hold a range back for its life.
// A policy that proposes the same refused pivot after every page (the
// e11bio-prism shape: ".../1>" in the gap past ".../19..." below hi
// ".../2", an upper child the gauge rightly reads as empty) is refused
// admitRefusals times in a row, and the next decision takes the split.
func TestAdmissionOverride(t *testing.T) {
	p, err := newTest(newSorted(nil, false, 1), Options{Workers: 64})
	if err != nil {
		t.Fatal(err)
	}
	var page []string
	for i := range 10 {
		for j := range 100 {
			page = append(page, fmt.Sprintf("z/0/16.%d.%02d", i, j))
		}
	}
	c := page[len(page)-1]
	r := Range{lo: "z/0/16", hi: "z/0/2", id: 1, born: fromSeed}
	st := &running{id: 1, keys: 1000}
	p.running[1] = st
	g := newGauge(page)
	pr := proposal{ready: true, m: "z/0/1>", src: srcMidpoint, found: true, gauge: g, rem: g.keys(c, r.hi)}
	for i := range admitRefusals + 1 {
		_, ok := p.decide(st, r, c, pr)
		if want := i == admitRefusals; ok != want {
			t.Fatalf("decision %d: split %v, want %v", i, ok, want)
		}
	}
	if p.c.Load(cSplitSkippedUpperTiny) != admitRefusals || p.c.Load(cSplitAdmittedPersistent) != 1 || st.refused != 0 {
		t.Fatalf("upper_tiny=%d overridden=%d refused=%d", p.c.Load(cSplitSkippedUpperTiny), p.c.Load(cSplitAdmittedPersistent), st.refused)
	}
}

// TestRank: the open frontier first; then bounded ranges whose page could
// not read their bound (+Inf); then the larger finite remainder; the older
// range within a class and on ties. Over random triples the order is a
// strict total order (irreflexive, asymmetric, transitive).
func TestRank(t *testing.T) {
	inf := math.Inf(1)
	for _, c := range []struct {
		a, b running
		want bool
	}{
		{running{id: 9, open: true, rem: inf}, running{id: 1, rem: inf}, true},
		{running{id: 1, rem: inf}, running{id: 9, open: true, rem: inf}, false},
		{running{id: 9, rem: 5000}, running{id: 1, rem: 900}, true},
		{running{id: 9, rem: inf}, running{id: 1, rem: 900}, true},
		{running{id: 1, rem: 900}, running{id: 9, rem: inf}, false},
		{running{id: 1, rem: inf}, running{id: 9, rem: inf}, true},
		{running{id: 1, rem: math.NaN(), keys: 4000}, running{id: 9, rem: 3000}, true},
	} {
		if got := outranks(&c.a, &c.b); got != c.want {
			t.Errorf("outranks(%d %v %v, %d %v %v) = %v", c.a.id, c.a.open, c.a.rem, c.b.id, c.b.open, c.b.rem, got)
		}
	}
	rng := rand.New(rand.NewSource(5))
	values := []float64{inf, math.NaN(), 0, 900, 900, 5000}
	gen := func(id uint64) *running {
		return &running{id: id, open: rng.Intn(6) == 0, rem: values[rng.Intn(len(values))], keys: int64(rng.Intn(3)) * 1000}
	}
	for range 20000 {
		ids := rng.Perm(3)
		x, y, z := gen(uint64(ids[0])), gen(uint64(ids[1])), gen(uint64(ids[2]))
		if outranks(x, x) {
			t.Fatalf("irreflexive: %+v", *x)
		}
		if outranks(x, y) == outranks(y, x) {
			t.Fatalf("not total or not asymmetric: %+v %+v", *x, *y)
		}
		if outranks(x, y) && outranks(y, z) && !outranks(x, z) {
			t.Fatalf("not transitive: %+v > %+v > %+v", *x, *y, *z)
		}
	}
}

// TestRankProgress: three continuing bounded ranges, A (900 keys left), B
// (+Inf) and C (5,000), with four workers and nothing queued: one worker is
// idle, the top-ranked range (B) splits on its decision and the others,
// deciding first, are outranked (a cyclic order outranked all three).
func TestRankProgress(t *testing.T) {
	p, err := newTest(newSorted(nil, false, 1), Options{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	rems := []float64{900, math.Inf(1), 5000}
	sts := make([]*running, len(rems))
	for i, rem := range rems {
		sts[i] = &running{id: uint64(i + 1), keys: 1000, rem: rem, continuing: true}
		p.running[sts[i].id] = sts[i]
	}
	split := 0
	for _, i := range []int{0, 2, 1} { // the split's reservation leaves no demand after it
		st := sts[i]
		lo := fmt.Sprintf("r%d/", i)
		r := Range{lo: lo, hi: lo + "z", id: st.id, born: fromSeed}
		pr := proposal{ready: true, m: lo + "m", src: srcMidpoint, found: true, why: extShort}
		if _, ok := p.decide(st, r, lo+"a", pr); ok {
			split++
			if i != 1 {
				t.Errorf("range %d split; want the +Inf range", i)
			}
		}
	}
	if split != 1 || p.c.Load(cSplitSkippedOutranked) != 2 {
		t.Fatalf("splits=%d outranked=%d", split, p.c.Load(cSplitSkippedOutranked))
	}
}
