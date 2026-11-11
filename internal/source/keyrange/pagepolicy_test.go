// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/internal/source/keyrange/keyrangetest"
)

func TestGrowPage(t *testing.T) {
	inf, nan := math.Inf(1), math.NaN()
	grow := GrowPage(5000)
	for _, c := range []struct {
		name string
		h    PageHint
		want int
	}{
		{"first request", PageHint{First: true, Rem: inf, Keys: 0}, 1000},
		{"first request of a child with a large estimate", PageHint{First: true, Rem: 1e6}, 1000},
		{"short page", PageHint{Asked: 1000, Full: 0, Rem: 1e6, Keys: 5000}, 1000},
		{"no estimate", PageHint{Asked: 1000, Full: 1, Rem: nan, Keys: 5000}, 1000},
		{"double after a full page", PageHint{Asked: 1000, Full: 1, Rem: 1e6, Keys: 1000}, 2000},
		{"double again, capped", PageHint{Asked: 4000, Full: 2, Rem: 1e6, Keys: 7000}, 5000},
		{"half the estimate left", PageHint{Asked: 4000, Full: 3, Rem: 3000, Keys: 7000}, 1500},
		{"too little left", PageHint{Asked: 4000, Full: 3, Rem: 1800, Keys: 7000}, 1000},
		{"frontier: keys listed, one page", PageHint{Asked: 1000, Full: 1, Rem: inf, Keys: 1000}, 1000},
		{"frontier: keys listed, three pages", PageHint{Asked: 1000, Full: 3, Rem: inf, Keys: 3000}, 1500},
		{"frontier: keys listed, long", PageHint{Asked: 2000, Full: 6, Rem: inf, Keys: 30000}, 4000},
	} {
		c.h.Nominal = 1000
		if got := grow(c.h); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
}

func TestEstimatePage(t *testing.T) {
	est := EstimatePage(5000)
	for _, c := range []struct {
		rem  float64
		want int
	}{{math.NaN(), 1000}, {math.Inf(1), 1000}, {0, 1000}, {2500, 2502}, {4998, 5000}, {1e9, 5000}} {
		if got := est(PageHint{Rem: c.rem, Nominal: 1000}); got != c.want {
			t.Errorf("rem %v: %d, want %d", c.rem, got, c.want)
		}
	}
}

// TestEstimatePageLargeFiniteRemainder uses the gauge of valid, sparse
// fixed-width keys: a finite estimate can exceed the integer range.
func TestEstimatePageLargeFiniteRemainder(t *testing.T) {
	page := make([]string, 1000)
	for i := range page {
		page[i] = fmt.Sprintf("%030d", i)
	}
	rem := newGauge(page).keys(page[len(page)-1], "1")
	if math.IsInf(rem, 0) || math.IsNaN(rem) || rem <= float64(math.MaxInt64) {
		t.Fatalf("expected a finite estimate beyond the integer range, got %g", rem)
	}
	if got := EstimatePage(5000)(PageHint{Rem: rem, Nominal: 1000}); got != 5000 {
		t.Fatalf("estimate %g: page size %d, want 5000", rem, got)
	}
}

// The explicit budget counts every initial child, including the closing
// sentinel and frontier. Zero retains the legacy seed/prior tiling.
func TestSeedTiles(t *testing.T) {
	for _, c := range []struct {
		name      string
		keys      []string
		seedTiles int
	}{
		{"sentinel", []string{"a/1", "b/1", "c/1"}, 5},
		{"object-last", []string{"a/1", "b/1", "c/1", "z"}, 4},
	} {
		for _, prior := range []bool{false, true} {
			for _, budget := range []int{0, 1, 2, 3, 17} {
				opt := Options{Seed: true, SeedTiles: budget, Workers: 2}
				if prior {
					opt.Prior = priorOf(c.keys, 1)
				}
				p, err := New(opt, Description[Object]{Target: "test", Listing: Listing}, fetchFrom(newSorted(c.keys, false, 1)))
				if err != nil {
					t.Fatal(err)
				}
				rec := &tileRec{}
				if err := p.Expand(context.Background(), p.Root(), rec); err != nil {
					t.Fatal(err)
				}
				legacy := c.seedTiles
				if prior {
					legacy = len(c.keys) + 1
				}
				want := min(budget, legacy)
				if budget == 0 {
					want = legacy
				}
				what := fmt.Sprintf("%s prior=%v budget=%d", c.name, prior, budget)
				if len(rec.tiles) != want {
					t.Fatalf("%s: %d tiles, want %d", what, len(rec.tiles), want)
				}
				spans := make([]span, len(rec.tiles))
				for i, r := range rec.tiles {
					spans[i] = span{lo: r.lo, hi: r.hi, open: r.open}
				}
				checkTiling(t, what, spans)
				if !prior {
					st := p.Stats()
					if got := st.Get(cSeedCuts) + st.Get(cSeedSentinel) + 1; got != int64(len(rec.tiles)) {
						t.Fatalf("seed counters describe %d tiles, emitted %d", got, len(rec.tiles))
					}
				}
			}
		}
	}
}

// Capping either seed must preserve exact coverage, also after unsafe
// resume keys and when unsafe cuts are shortened or discarded.
func TestSeedTilesCoverage(t *testing.T) {
	var keys []string
	for i := range 12 {
		for j := range 25 {
			keys = append(keys, fmt.Sprintf("d%02d/k%03d", i, j))
		}
	}
	keys = append(keys, "bad\n/child")
	slices.Sort(keys)
	accept := func(k string) bool { return Safe(k) && !strings.Contains(k, "05") }
	for _, prior := range []bool{false, true} {
		for _, budget := range []int{1, 2, 3, 17} {
			for _, after := range []string{"", "d05/k007"} {
				opt := Options{Seed: true, SeedTiles: budget, PageSize: 7}
				if prior {
					opt.Prior = priorOf(keys, 13)
				}
				if after != "" {
					opt.Resume = &Resume{After: after}
				}
				// Count the actual root children before any later adaptive splits.
				rootOpt := opt
				rootOpt.Workers = 8
				p, err := New(rootOpt, Description[Object]{Target: "test", Listing: Listing, AcceptBound: accept}, fetchFrom(newSorted(keys, false, 1)))
				if err != nil {
					t.Fatal(err)
				}
				rec := &tileRec{}
				if err := p.Expand(context.Background(), p.Root(), rec); err != nil {
					t.Fatal(err)
				}
				if len(rec.tiles) < 1 || len(rec.tiles) > budget {
					t.Fatalf("prior=%v budget=%d after=%q: %d tiles", prior, budget, after, len(rec.tiles))
				}
				got, _, leaves := runPolicy(t, newSorted(keys, true, 1), opt, 8, 1<<20, accept)
				from := 0
				if after != "" {
					from = slices.Index(keys, after) + 1
				}
				if !slices.Equal(got, keys[from:]) {
					t.Fatalf("prior=%v budget=%d after=%q: got %d keys, want %d", prior, budget, after, len(got), len(keys)-from)
				}
				if after == "" {
					checkTiling(t, fmt.Sprintf("prior=%v budget=%d", prior, budget), leaves)
				}
			}
		}
	}
}

func TestSeedTilesGroups(t *testing.T) {
	keys := groupKeys(100)
	for _, prior := range []bool{false, true} {
		for _, budget := range []int{1, 2, 3} {
			for _, inclusive := range []bool{false, true} {
				v := newVersioned(keys, groupKeyspace(len(keys), 7, 30), 1)
				v.short, v.empties, v.inclusive = true, true, inclusive
				opt := Options{Seed: true, SeedTiles: budget, Workers: 8, PageSize: 7}
				if prior {
					opt.Prior = priorOf(keys, 10)
				}
				p := newGroupEngine(t, v, opt)
				rec := &tileRec{}
				if err := p.Expand(context.Background(), p.Root(), rec); err != nil {
					t.Fatal(err)
				}
				if len(rec.tiles) < 1 || len(rec.tiles) > budget {
					t.Fatalf("prior=%v budget=%d inclusive=%v: %d tiles", prior, budget, inclusive, len(rec.tiles))
				}
				runGroups(t, v, opt)
			}
		}
	}
}

// maxCheck is a fake API that refuses a page size outside 1..limit.
type maxCheck struct {
	*sorted
	limit int
	sizes sync.Map // MaxKeys -> true
}

func (m *maxCheck) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	if in.MaxKeys < 1 || in.MaxKeys > m.limit {
		return nil, fmt.Errorf("page size %d outside 1..%d", in.MaxKeys, m.limit)
	}
	m.sizes.Store(in.MaxKeys, true)
	return m.sorted.List(ctx, in)
}

// TestPageForCoversEveryKeyOnce: a PageFor that changes the page size on
// every request (0 and far past PageMax among its answers), within token
// chains, under random splits and short pages, lists every key once in
// order, the covered intervals tile, every request's size is clamped to
// 1..PageMax, and each range's first request is the only one marked First.
func TestPageForCoversEveryKeyOnce(t *testing.T) {
	seeds := 12
	if testing.Short() {
		seeds = 3
	}
	for name, keys := range keyspaces() {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		for s := range seeds {
			for _, w := range []int{1, 8, 64} {
				rng := rand.New(rand.NewSource(int64(s*1000 + w)))
				var mu sync.Mutex
				var firsts, requests atomic.Int64
				coin := func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
				pageFor := func(h PageHint) int {
					requests.Add(1)
					if h.First {
						firsts.Add(1)
						if h.Asked != 0 || h.Full != 0 || h.Keys != 0 {
							t.Errorf("a first request with evidence: %+v", h)
						}
					}
					mu.Lock()
					defer mu.Unlock()
					return []int{0, 1, 2, 7, 13, 1000, 4999, 1 << 20}[rng.Intn(8)]
				}
				api := &maxCheck{sorted: newSorted(keys, true, int64(s)), limit: 5000}
				opt := Options{SplitCoin: coin, Seed: s%2 == 1, PageSize: 13, PageFor: pageFor, PageMax: 5000}
				got, st, leaves := runBudget(t, api, opt, w, 500)
				what := fmt.Sprintf("%s seed=%d w=%d", name, s, w)
				if !slices.Equal(got, want) {
					t.Fatalf("%s: %d keys, want %d", what, len(got), len(want))
				}
				checkTiling(t, what, leaves)
				if firsts.Load() > requests.Load() || requests.Load() > 0 && (firsts.Load() == 0 || st.Get(cPageAskedKeys) == 0) {
					t.Fatalf("%s: %d first requests of %d, asked %d keys", what, firsts.Load(), requests.Load(), st.Get(cPageAskedKeys))
				}
				if _, ok := api.sizes.Load(5000); requests.Load() > 50 && (!ok || st.Get(cPageGrown) == 0 || st.Get(cPageTrimmed) == 0) {
					t.Errorf("%s: clamped to 5000 %v, grown %d, trimmed %d", what, ok, st.Get(cPageGrown), st.Get(cPageTrimmed))
				}
			}
		}
	}
}

// TestPageForOff: without PageFor every range request asks PageSize, and
// the page_policy counters are off.
func TestPageForOff(t *testing.T) {
	keys := keyspaces()["exactly3000"]
	api := &maxCheck{sorted: newSorted(keys, false, 1), limit: 7}
	_, st, _ := runBudget(t, api, Options{PageSize: 7}, 8, 500)
	n := 0
	api.sizes.Range(func(k, _ any) bool { n++; return k.(int) == 7 })
	if n != 1 {
		t.Errorf("%d page sizes asked, want only 7", n)
	}
	if st.Counters.On("page_policy") {
		t.Error("page.asked_keys reported without a page policy")
	}
}
