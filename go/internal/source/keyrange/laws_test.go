// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"math/rand"
	"slices"
	"sync"
	"testing"
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
// splits), to the memory budget and to the policy toggles (the probe, the
// floors, the seed, structure probes). One
// table, over the fixture keyspaces and the hostile ones; the reference is
// the sorted keyspace itself.
func TestMetamorphicLaws(t *testing.T) {
	type variant struct {
		name    string
		opt     Options
		workers int
		budget  int // rows
		short   bool
		pageCap int
		coin    bool // forced random splits
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
		{name: "w64/floors/page=13", opt: Options{Floors: true}, workers: 64, budget: 500, pageCap: 13},
		{name: "w16/seed/floors/short", opt: Options{Seed: true, Floors: true}, workers: 16, budget: 500, short: true},
		{name: "w64/seed/page=13", opt: Options{Seed: true}, workers: 64, budget: 500, pageCap: 13},
		{name: "w16/structure/short", opt: Options{Structure: true}, workers: 16, budget: 500, short: true},
		{name: "w32/seed/structure/floors/page=7", opt: Options{Seed: true, Structure: true, Floors: true}, workers: 32, budget: 500, pageCap: 7},
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
			if testing.Short() && i%3 != 0 {
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
			got, _, _ := runBudget(t, api, opt, v.workers, v.budget)
			if !slices.Equal(got, want) {
				t.Fatalf("%s %s: %d keys, want %d", name, v.name, len(got), len(want))
			}
		}
	}
}
