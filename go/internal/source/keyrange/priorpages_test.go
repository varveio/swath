// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/keyrange/keyrangetest"
)

// maxKeys records the largest page each request asked for.
type maxKeys struct {
	*sorted
	mu   sync.Mutex
	asks map[int]int
}

func (m *maxKeys) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	m.mu.Lock()
	m.asks[in.MaxKeys]++
	m.mu.Unlock()
	return m.sorted.List(ctx, in)
}

// seeded runs keys with a prior given through SeedFrom (as the command
// does) and opt, at workers, and returns the keys, the stats, the tiles'
// expected keys and the page sizes asked.
func seeded(t *testing.T, keys []string, pr *Prior, opt Options, workers int) ([]string, Stats, []float64, map[int]int) {
	t.Helper()
	api := &maxKeys{sorted: newSorted(keys, false, 1), asks: map[int]int{}}
	opt.Workers = workers
	p, err := newTest(api, opt)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SeedFrom(pr); err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: 1 << 26}); err != nil {
		t.Fatal(err)
	}
	var est []float64
	for _, a := range p.tiles {
		est = append(est, a.est)
	}
	return keysOf(c.rows), p.Stats(), est, api.asks
}

// TestPriorWholePages: a counted prior sets the provider's prior page size
// (PriorPageSize) and cuts its tiles into whole pages: each bounded tile
// holds at most tile_pages pages less one key, and all but the last at
// least that less one cut, where the next cut would take it past; its
// count stays each range's remainder (held_pages), and the listing is
// exact. A prior without counts keeps the page size and the equal-count
// tiling.
func TestPriorWholePages(t *testing.T) {
	var keys []string
	for i := range 60000 {
		keys = append(keys, fmt.Sprintf("d%02d/f%06d", i%37, i))
	}
	slices.Sort(keys)
	const page, workers, step = 500, 4, 50
	pr := priorOf(keys, step)
	got, st, est, asks := seeded(t, keys, pr, Options{PageSize: 100, PriorPageSize: page}, workers)
	if !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	q := int(math.Ceil(float64(len(keys)) / (priorTilesPerWorker * workers * page)))
	fill := float64(q*page - 1)
	if st.Get(cSeedFromPageSize) != page || st.Get(cSeedFromTilePages) != int64(q) || st.Get(cSeedFromHeld) == 0 {
		t.Fatalf("page size %d, tile pages %d (want %d), held pages %d", st.Get(cSeedFromPageSize), st.Get(cSeedFromTilePages), q,
			st.Get(cSeedFromHeld))
	}
	for i, e := range est {
		if e > fill || (i < len(est)-1 && e <= fill-step) {
			t.Fatalf("tile %d expects %v keys, want (%v, %v]", i, e, fill-step, fill)
		}
	}
	for n := range asks {
		if n != page {
			t.Fatalf("pages asked: %v, want %d only", asks, page)
		}
	}

	cuts := priorOf(keys, step)
	cuts.Counted = false
	got, st, _, asks = seeded(t, keys, cuts, Options{PageSize: 100, PriorPageSize: page}, workers)
	if !slices.Equal(got, keys) || st.Get(cSeedFromPageSize) != 0 || st.Get(cSeedFromTilePages) != 0 || asks[page] != 0 ||
		st.Get(cSeedFromTiles) != int64(Tiles(workers)) {
		t.Fatalf("cuts only: %d keys, page size %d, tile pages %d, %d tiles, asked %v", len(got), st.Get(cSeedFromPageSize),
			st.Get(cSeedFromTilePages), st.Get(cSeedFromTiles), asks)
	}
}

// TestPriorCountHeld: a fresh counted prior's count, not the page's gauge,
// is a tile's remainder: on keys whose gauge errs long (folders of a few
// keys), a tile with fewer than two pages left by the prior is not split
// (split.skipped.remainder), where the gauge would admit it; a tile that
// lists past its count hands its remainder back to the gauge
// (seed_from.outrun), and both list every key once.
func TestPriorCountHeld(t *testing.T) {
	var keys []string
	for i := range 4000 {
		keys = append(keys, fmt.Sprintf("y%04d/m%02d/f%d", i/24, i%12, i%2))
	}
	keys = slices.Compact(slices.Sorted(slices.Values(keys)))
	pr := priorOf(keys, 10)
	// Pages of 100 and tiles of at most 299 keys: after a tile's first
	// page, at most 199 are left by the prior, under two pages.
	var mu sync.Mutex
	coin := func() bool { mu.Lock(); defer mu.Unlock(); return true }
	got, st, _, _ := seeded(t, keys, pr, Options{PageSize: 100, PriorPageSize: 100, SplitCoin: coin}, 4)
	if !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	if st.Get(cSeedFromHeld) == 0 || st.Get(cSplitSkippedRemainder) == 0 || st.Get(cSplitAcceptedBounded) != st.Get(cSplitAdmittedPersistent) {
		t.Fatalf("held %d, refused for the remainder %d, bounded splits %d (persistent %d)", st.Get(cSeedFromHeld),
			st.Get(cSplitSkippedRemainder), st.Get(cSplitAcceptedBounded), st.Get(cSplitAdmittedPersistent))
	}
	// A prior that counted a tenth of the keys: every tile outruns it.
	small := priorOf(keys, 10)
	for i := range small.Cuts {
		small.Cuts[i].Rows = 1
	}
	got, st, _, _ = seeded(t, keys, small, Options{PageSize: 100, PriorPageSize: 100}, 4)
	if !slices.Equal(got, keys) || st.Get(cSeedFromOutrun) == 0 {
		t.Fatalf("stale prior: %d keys, outrun %d", len(got), st.Get(cSeedFromOutrun))
	}
}

// A gauge may read a finite number near the float limit from valid long
// keys. Sharing a finite prior must conserve its count, even when the
// unnormalized product would overflow, or the gauge cannot read a child.
func TestPriorChildrenConserveCount(t *testing.T) {
	const remaining = 1_000_000.0
	for _, tc := range []struct {
		name   string
		prefix int
	}{
		{"ordinary", 0},
		{"huge_finite", 640},
		{"unread_bound", 700},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := strings.Repeat("0", tc.prefix)
			page := []string{prefix + "0", prefix + "1"}
			c, m, hi := page[1], prefix+"2", "2"
			pr := proposal{gauge: newGauge(page), rem: remaining, held: true}
			a, b := pr.gauge.keys(c, m), pr.gauge.keys(m, hi)
			if tc.name == "huge_finite" && (math.IsInf(a+b, 0) || !math.IsInf(remaining*b, 1)) {
				t.Fatalf("expected finite weights with an overflowing product, got %g, %g", a, b)
			}
			if tc.name == "unread_bound" && !math.IsInf(a+b, 1) {
				t.Fatalf("expected an unread child, got %g, %g", a, b)
			}
			lower, upper := pr.children(c, m, hi)
			if !(lower >= 0 && lower <= remaining && upper >= 0 && upper <= remaining) ||
				math.Abs(lower+upper-remaining) > remaining*1e-12 {
				t.Fatalf("finite prior %g became children %g, %g (weights %g, %g)", remaining, lower, upper, a, b)
			}
			if math.IsInf(a+b, 1) && (lower != remaining/2 || upper != remaining/2) {
				t.Fatalf("unread child: %g, %g, want halves", lower, upper)
			}
		})
	}
}

// Exercise the actual pivot, admission override and child inheritance:
// an extreme but finite gauge must not install an infinite retained
// count that no finite number of listed keys can exhaust.
func TestPriorChildrenOutrunAfterAdmittedSplit(t *testing.T) {
	prefix := strings.Repeat("0", 290)
	page := make([]string, PageKeys)
	for i := range page {
		page[i] = prefix + fmt.Sprintf("%03d", i)
	}
	const remaining = 1_000_000.0
	c := page[len(page)-1]
	p, err := newTest(newSorted(nil, false, 1), Options{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	r := Range{hi: "2", id: p.ids.Add(1), born: fromSeed}
	st := &running{id: r.id, keys: int64(len(page)), prior: remaining + float64(len(page)), rem: remaining}
	p.running[r.id] = st
	pr := propose(r, page, page[0], c, r.hi, true, 3)
	pr.gauge, pr.rem, pr.held = newGauge(page), remaining, true
	a, b := pr.gauge.keys(c, pr.m), pr.gauge.keys(pr.m, r.hi)
	if math.IsInf(a+b, 0) || !math.IsInf(remaining*b, 1) {
		t.Fatalf("expected finite weights with an overflowing product, got %g, %g", a, b)
	}
	var pv pivot
	var ok bool
	for range admitRefusals + 1 {
		pv, ok = p.decide(st, r, c, pr)
		if ok {
			break
		}
	}
	if !ok || p.c.Load(cSplitAdmittedPersistent) != 1 {
		t.Fatal("split was not admitted after persistent refusals")
	}
	lower, upper := p.commit(st, r, c, pv)
	if math.Abs(lower.est+upper.est-remaining) > remaining*1e-12 {
		t.Fatalf("children lost the retained count: %g, %g", lower.est, upper.est)
	}
	for _, r := range []Range{lower, upper} {
		child := p.enter(r)
		if !r.prior || !r.estOK || child.held() != r.est || !(r.est >= 0 && r.est <= remaining) {
			t.Fatalf("child did not inherit a finite count: range %+v, held %g", r, child.held())
		}
		child.keys = 2_000_000
		if got := child.held(); got != 0 {
			t.Fatalf("child could not exhaust its prior: %g left", got)
		}
	}
}
