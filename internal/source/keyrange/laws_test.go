// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/counters"
)

func TestHostileFixturesMatchSequential(t *testing.T) {
	for name, keys := range hostile() {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		seq, _, _ := run(t, newSorted(keys, false, 1), Options{Sequential: true}, 1)
		if !slices.Equal(seq, want) {
			t.Fatalf("%s: sequential lists %d keys, want %d", name, len(seq), len(want))
		}
		for _, seed := range []bool{false, true} {
			got, _, leaves := run(t, newSorted(keys, true, 2), Options{Seed: seed}, 16)
			if !slices.Equal(got, want) {
				t.Fatalf("%s seed=%v: %d keys, want %d", name, seed, len(got), len(want))
			}
			checkTiling(t, name, leaves)
		}
	}
}

// TestMetamorphicLaws: the listing is invariant to the page size, to the
// range decomposition (seed on and off, 1, 4 and 64 workers, forced random
// splits, a resume), to the memory budget and to the policy toggles (the
// seed and its descent, structure probes). One table, over the fixture keyspaces and the hostile ones; the reference is
// the sorted keyspace itself. Eligible parallel runs also check the
// report's split, child, leaf-depth and hand-out identities; every
// engagement counter without a targeted test fires across the matrix.
func TestMetamorphicLaws(t *testing.T) {
	fired := map[counters.ID]int64{}
	type variant struct {
		name    string
		opt     Options
		workers int
		budget  int // rows
		short   bool
		pageCap int
		coin    bool // forced random splits
		resume  bool // resume after the key a third of the way in
		// accept is the provider's bound policy (nil: Safe); SafeKey sends
		// real keys' '%' as bounds, as S3 does.
		accept func(string) bool
	}
	variants := []variant{
		{name: "sequential", opt: Options{Sequential: true}, workers: 1, budget: 500},
		{name: "sequential/page=7", opt: Options{Sequential: true}, workers: 1, budget: 500, pageCap: 7},
		{name: "w1", workers: 1, budget: 500},
		{name: "w4/short-pages", workers: 4, budget: 500, short: true},
		{name: "w64/page=13", workers: 64, budget: 500, pageCap: 13},
		{name: "w64/seed", opt: Options{Seed: true}, workers: 64, budget: 500},
		{name: "w8/forced-splits", workers: 8, budget: 500, coin: true},
		{name: "w8/seed/forced-splits/page=3", opt: Options{Seed: true}, workers: 8, budget: 500, coin: true, pageCap: 3},
		{name: "w16/budget=20", workers: 16, budget: 20},
		{name: "w16/budget=1M", workers: 16, budget: 1 << 20},
		{name: "w16/seed/short", opt: Options{Seed: true}, workers: 16, budget: 500, short: true},
		{name: "w64/seed/page=13", opt: Options{Seed: true}, workers: 64, budget: 500, pageCap: 13},
		{name: "w16/structure/short", opt: Options{Structure: StructureOn}, workers: 16, budget: 500, short: true},
		{name: "w64/structure-on-frontier", opt: Options{Structure: StructureAuto}, workers: 64, budget: 500},
		{name: "w8/structure-auto/forced-splits/page=7", opt: Options{Structure: StructureAuto}, workers: 8, budget: 500, coin: true, pageCap: 7},
		{name: "w32/seed/structure/page=7", opt: Options{Seed: true, Structure: StructureOn}, workers: 32, budget: 500, pageCap: 7},
		// Pages of 3 make the seed paginate and descend in several waves.
		{name: "w64/seed-descent", opt: Options{Seed: true}, workers: 64, budget: 500, pageCap: 3},
		{name: "w16/seed-descent/resume", opt: Options{Seed: true}, workers: 16, budget: 500, pageCap: 3, coin: true, resume: true},
		{name: "w64/seed/percent-bounds", opt: Options{Seed: true}, workers: 64, budget: 500, accept: SafeKey},
		// The seed's first data page (the command's default): a scope of
		// one page is listed by it alone; a larger one's first tile starts
		// after its last key.
		{name: "w16/slow-split/seed-first-page/short", opt: Options{Seed: true, SeedFirstPage: true, SlowSplit: true}, workers: 16, budget: 500, short: true},
		{name: "w64/seed-first-page", opt: Options{Seed: true, SeedFirstPage: true}, workers: 64, budget: 500},
		{name: "w16/seed-first-page/page=3/resume", opt: Options{Seed: true, SeedFirstPage: true}, workers: 16, budget: 500, pageCap: 3, coin: true, resume: true},
		{name: "w8/seed-first-page/percent-bounds/short", opt: Options{Seed: true, SeedFirstPage: true, Structure: StructureAuto}, workers: 8, budget: 500, short: true, accept: SafeKey},
		{name: "w8/percent-bounds/forced-splits/page=7", workers: 8, budget: 500, pageCap: 7, coin: true, accept: SafeKey},
		{name: "w16/seed-descent/percent-bounds/resume", opt: Options{Seed: true, Structure: StructureAuto}, workers: 16, budget: 500, pageCap: 3, resume: true, accept: SafeKey},
	}
	spaces := keyspaces()
	for k, v := range hostile() {
		spaces[k] = v
	}
	names := slices.Sorted(func(yield func(string) bool) {
		for k := range spaces {
			if !yield(k) {
				return
			}
		}
	})
	for _, name := range names {
		keys := spaces[name]
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		for i, v := range variants {
			// Short mode keeps every third variant and the forced structural
			// pivots, whose counter identities nothing else checks there.
			if testing.Short() && i%3 != 0 && v.opt.Structure != StructureOn {
				continue
			}
			api := newSorted(keys, v.short, int64(i+1))
			api.PageCap = v.pageCap
			opt := v.opt
			if v.coin {
				rng := rand.New(rand.NewSource(int64(i)))
				var mu sync.Mutex
				opt.SplitCoin = func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(2) == 0 }
			}
			want := want
			if v.resume && len(want) > 0 {
				opt.Resume = &Resume{After: want[len(want)/3]}
				want = want[len(want)/3+1:]
			}
			got, st, _ := runPolicy(t, api, opt, v.workers, v.budget, v.accept)
			if !opt.Sequential && !opt.SeedFirstPage {
				check(t, name+" "+v.name, st, int64(len(got)))
			}
			for id := range numCounters {
				fired[id] += st.Get(id)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("%s %s: %d keys, want %d", name, v.name, len(got), len(want))
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
