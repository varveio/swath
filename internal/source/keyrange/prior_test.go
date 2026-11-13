// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"errors"
	"fmt"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/keyrange/keyrangetest"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

type (
	Prior = source.Prior
	Cut   = source.Cut
)

// priorOf is a counted prior cut every step keys of keys (the last key
// closing the last cut), as a dataset's page index gives one.
func priorOf(keys []string, step int) *Prior {
	pr := &Prior{Source: "test", Counted: true}
	var n int64
	for i, k := range keys {
		n++
		if (i+1)%step == 0 || i == len(keys)-1 {
			pr.Cuts = append(pr.Cuts, Cut{Key: k, Rows: n})
			n = 0
		}
	}
	return pr
}

// TestTilePriorBounds: for random cut sets (ASCII, multi-byte scalars,
// cuts the provider vetoes), tile counts and resume keys, the tiles ascend
// strictly bytewise, all lie after the resume key and inside the scope,
// number at most k-1, end at the last usable cut, and carry the mass of
// every cut after the resume key, whether cut by equal mass or into
// whole pages (fill).
func TestTilePriorBounds(t *testing.T) {
	alpha := []string{"a", "b", "z", "/", "0", "9", "é", "日", "\U0001F600", "\U0010FFFF", "5"}
	veto := func(s string) bool { return Safe(s) && !strings.Contains(s, "5") }
	safeStart := func(s string) string {
		for s != "" && !veto(s) {
			_, n := utf8.DecodeLastRuneInString(s)
			s = s[:len(s)-n]
		}
		return s
	}
	for seed := range 300 {
		rng := rand.New(rand.NewSource(int64(seed)))
		scope := []string{"", "p/", "日/"}[seed%3]
		var keys []string
		for range 1 + rng.Intn(200) {
			var b strings.Builder
			b.WriteString(scope)
			for range 1 + rng.Intn(6) {
				b.WriteString(alpha[rng.Intn(len(alpha))])
			}
			keys = append(keys, b.String())
		}
		keys = slices.Compact(slices.Sorted(slices.Values(keys)))
		pr := &Prior{Counted: seed%2 == 0, Scope: scope}
		for _, k := range keys {
			pr.Cuts = append(pr.Cuts, Cut{Key: k, Rows: int64(rng.Intn(5000))})
		}
		if err := pr.Check(scope, maxKeyBytes); err != nil {
			t.Fatal(err)
		}
		after := ""
		if seed%4 == 1 {
			after = keys[rng.Intn(len(keys))]
		}
		k := 2 + rng.Intn(40)
		fill := 0.0
		if pr.Counted && seed%3 > 0 {
			fill = float64(1 + rng.Intn(40000)) // whole-page tiles, also past k-1 of them
		}
		tiles, _ := tilePrior(pr, k, fill, after, scope, veto, safeStart)
		what := fmt.Sprintf("seed %d k %d fill %v after %q", seed, k, fill, after)
		if len(tiles) > k-1 {
			t.Fatalf("%s: %d tiles, at most %d", what, len(tiles), k-1)
		}
		var want, got float64
		var lastUsable string
		prev := after
		for _, c := range pr.Cuts {
			if c.Key > after {
				w := 1.0
				if pr.Counted {
					w = float64(c.Rows)
				}
				want += w
				if s := safeStart(c.Key); s > after && strings.HasPrefix(s, scope) && s > lastUsable {
					lastUsable = s
				}
			}
		}
		for i, tl := range tiles {
			if tl.hi <= prev || !strings.HasPrefix(tl.hi, scope) || !veto(tl.hi) {
				t.Fatalf("%s: tile %d hi %q after %q (scope %q)", what, i, tl.hi, prev, scope)
			}
			prev = tl.hi
			got += tl.est
		}
		if len(tiles) > 0 && tiles[len(tiles)-1].hi != lastUsable {
			t.Fatalf("%s: last tile ends at %q, the last usable cut is %q", what, tiles[len(tiles)-1].hi, lastUsable)
		}
		// Mass of cuts merged away past the last usable cut is the open
		// frontier's; everything else is in a tile.
		if got > want {
			t.Fatalf("%s: tiles carry %v keys, the cuts %v", what, got, want)
		}
	}
}

// TestPriorOracle: on every fixture keyspace, a run seeded from a prior
// lists exactly the keys of a run without one, its ranges tile the scope,
// whether the prior is the keyspace's own (fresh), another keyspace's
// (unrelated), or cut-only (no counts), with random splits and on demand,
// fresh and resumed.
func TestPriorOracle(t *testing.T) {
	spaces := keyspaces()
	for name, raw := range spaces {
		keys := slices.Compact(slices.Sorted(slices.Values(raw)))
		priors := map[string]*Prior{"fresh": priorOf(keys, 37), "unrelated": priorOf(slices.Compact(slices.Sorted(slices.Values(spaces["dateTree"]))), 101)}
		cutsOnly := priorOf(keys, 211)
		cutsOnly.Counted = false
		priors["cutsOnly"] = cutsOnly
		for pname, pr := range priors {
			for s, w := range []int{1, 4, 64} {
				what := fmt.Sprintf("%s/%s/w%d", name, pname, w)
				rng := rand.New(rand.NewSource(int64(s)))
				var mu sync.Mutex
				coin := func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
				for _, opt := range []Options{{Prior: pr}, {Prior: pr, SplitCoin: coin}, {Prior: pr, Structure: StructureAuto}} {
					got, st, leaves := run(t, newSorted(keys, true, int64(s+1)), opt, w)
					if !slices.Equal(got, keys) {
						t.Fatalf("%s: %d keys, want %d", what, len(got), len(keys))
					}
					checkTiling(t, what, leaves)
					if st.Get(cSeedFromTiles) < 1 {
						t.Fatalf("%s: %d tiles", what, st.Get(cSeedFromTiles))
					}
				}
				if len(keys) > 2 {
					k := keys[rng.Intn(len(keys)-1)]
					got, _, _ := run(t, newSorted(keys, true, int64(s+1)), Options{Prior: pr, Resume: &Resume{After: k}}, w)
					if want := keys[slices.Index(keys, k)+1:]; !slices.Equal(got, want) {
						t.Fatalf("%s: resumed after %q: %d keys, want %d", what, k, len(got), len(want))
					}
				}
			}
		}
	}
}

// TestPriorStale: a prior whose distribution has changed (one segment
// grown 10x, one emptied, a new folder between two cuts) still lists
// every key; the grown segment is measured as grown on its first page and
// split inside, the emptied one measured as shrunk.
//
// The grown tile's second page is held until every other range is done
// (Run's "one-range-open"): the split needs demand, idle workers, while
// the grown tile lists. Draining the other tiles in about 30 page
// latencies while the grown one holds 100 pages did not guarantee it: on
// macOS 163 split decisions across the run saw no idle worker (#332).
// Released this way, its next page decides with three idle workers and
// every page after it still to list.
func TestPriorStale(t *testing.T) {
	var base []string
	for i := range 100000 {
		base = append(base, fmt.Sprintf("logs/%07d", i*10))
	}
	// 10 cuts of 10,000 keys (four workers start with up to 15 tiles, so
	// none merge); the grown one holds 100 pages.
	pr := priorOf(base, 10000)
	grownLo, grownHi := pr.Cuts[4].Key, pr.Cuts[5].Key
	emptyLo, emptyHi := pr.Cuts[7].Key, pr.Cuts[8].Key
	var keys []string
	for _, k := range base {
		switch {
		case k > grownLo && k <= grownHi:
			for d := range 10 { // ten keys where there was one
				keys = append(keys, fmt.Sprintf("%s%d", k, d))
			}
		case k > emptyLo && k < emptyHi: // the cut key itself stays
		default:
			keys = append(keys, k)
		}
	}
	for i := range 3000 { // a new folder between two cuts
		keys = append(keys, fmt.Sprintf("logs/%07d/new/%05d", 250005, i))
	}
	keys = slices.Compact(slices.Sorted(slices.Values(keys)))
	pr.Scope = ""
	// A reorder budget that holds the whole listing: the tiles after the
	// grown one finish instead of blocking on the frontier (spill does
	// this in the command), so their workers go idle and ask for work.
	h := &tileHold{sorted: newSorted(keys, false, 1), lo: grownLo, release: make(chan struct{})}
	var once sync.Once
	defer order.SetDurableEvents(func(e string) {
		if e == "one-range-open" {
			once.Do(func() { close(h.release) })
		}
	})()
	got, st, leaves := runBudget(t, h, Options{Prior: pr}, 4, 1<<20)
	if !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	checkTiling(t, "stale", leaves)
	if st.Get(cSeedFromErrGrown) < 1 || st.Get(cSeedFromErrShrunk) < 1 || st.Get(cSeedFromErrMaxGrown) < 5 {
		t.Fatalf("error counters: grown %d shrunk %d max %d", st.Get(cSeedFromErrGrown), st.Get(cSeedFromErrShrunk), st.Get(cSeedFromErrMaxGrown))
	}
	inside := 0
	for _, l := range leaves {
		if !l.open && l.lo >= grownLo && l.hi <= grownHi && (l.lo != grownLo || l.hi != grownHi) {
			inside++
		}
	}
	if inside == 0 || st.Get(cSplitAcceptedBounded) == 0 {
		t.Fatalf("the grown segment (%q, %q] was not split: %d leaves inside, %d bounded splits; considered %d outranked %d nodemand %d remainder %d lower %d upper %d estnone %d", grownLo, grownHi, inside, st.Get(cSplitAcceptedBounded),
			st.Get(cSplitConsidered), st.Get(cSplitSkippedOutranked), st.Get(cSplitSkippedNoDemand), st.Get(cSplitSkippedRemainder), st.Get(cSplitSkippedLowerTiny), st.Get(cSplitSkippedUpperTiny), st.Get(cSplitEstimateNone))
	}
	if st.Get(cSeedRequests) != 0 || st.Get(cSeedFromTiles) != int64(len(pr.Cuts))+1 {
		t.Fatalf("seed requests %d, tiles %d", st.Get(cSeedRequests), st.Get(cSeedFromTiles))
	}
}

// tileHold holds the continuation pages of the range listing after lo (a
// tile's own requests: its children start after their cursor or pivot)
// until released; every other request passes.
type tileHold struct {
	*sorted
	lo      string
	release chan struct{}
}

func (h *tileHold) List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	if in.StartAfter == h.lo && in.ContinuationToken != "" {
		select {
		case <-h.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.sorted.List(ctx, in)
}

// TestPriorRefused: a prior of another scope, out of order, repeated,
// outside the scope or not UTF-8 is refused (ErrPrior), and so is any
// prior for the sequential listing.
func TestPriorRefused(t *testing.T) {
	cuts := func(ks ...string) []Cut {
		var out []Cut
		for _, k := range ks {
			out = append(out, Cut{Key: k})
		}
		return out
	}
	for name, c := range map[string]struct {
		pr  Prior
		opt Options
	}{
		"scope":      {Prior{Scope: "a/", Cuts: cuts("a/1")}, Options{Prefix: "b/"}},
		"order":      {Prior{Unscoped: true, Cuts: cuts("b", "a")}, Options{}},
		"repeat":     {Prior{Unscoped: true, Cuts: cuts("a", "a")}, Options{}},
		"outside":    {Prior{Unscoped: true, Cuts: cuts("a/1", "b/1")}, Options{Prefix: "a/"}},
		"utf8":       {Prior{Unscoped: true, Cuts: cuts("a\xff")}, Options{}},
		"sequential": {Prior{Unscoped: true, Cuts: cuts("a")}, Options{Sequential: true}},
	} {
		p, err := newTest(newSorted(nil, false, 1), c.opt)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.SeedFrom(&c.pr); !errors.Is(err, source.ErrPrior) {
			t.Fatalf("%s: %v, want ErrPrior", name, err)
		}
	}
	p, _ := newTest(newSorted(nil, false, 1), Options{Prefix: "a/"})
	if err := p.SeedFrom(&Prior{Unscoped: true, Cuts: cuts("a/1", "a/2")}); err != nil {
		t.Fatal(err)
	}
}
