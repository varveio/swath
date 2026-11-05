// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
)

// hostile are adversarial keyspaces on the fake bucket.
func hostile() map[string][]string {
	ks := map[string][]string{}
	// The cardinality bomb: one prefix with an enormous fan-out of keys that
	// differ only in their last bytes, after a long shared run.
	var bomb []string
	for i := range 6000 {
		bomb = append(bomb, "bomb/"+strings.Repeat("a", 200)+fmt.Sprintf("%04d", i))
	}
	bomb = append(bomb, "a", "bomb", "bomb0", "bomb/", "zz")
	ks["cardinalityBomb"] = bomb
	// The encoding minefield: every awkward byte sequence a key may hold.
	mine := []string{"%", "%%", "%2F", "%2f", "a%2Fb", "%25", "+", "a+b", " ", "a b", "  ", "a ", " a", "?", "#", "&", "=",
		"a?b=c&d", "\\", "\"", "'", "<", ">", "<a>&amp;", "/", "//", "a//b", "/a", "a/", "~", "\x7f", "\u00a0", "\u2028",
		"\ufeff", "\ufffd", "\U0001F600", "\U0010FFFF", "e\u0301", "\u00e9", "\u03a9", "\u65e5\u672c\u8a9e/\u30ad\u30fc"}
	for c := 1; c < 0x20; c++ {
		mine = append(mine, "ctl"+string(rune(c))+"x", string(rune(c)))
	}
	for _, b := range []string{"a", "z", "~", "/", "%", " ", "\x7f", "\u00ff", "\U0010FFFF"} {
		for _, n := range []int{1, 2, 63, 64, 255, 256, 511} {
			mine = append(mine, strings.Repeat(b, n))
		}
	}
	for i := range 2000 { // enough keys to split among the mines
		mine = append(mine, fmt.Sprintf("m/%%%02X/%04d", i%256, i))
	}
	ks["encodingMinefield"] = mine
	return ks
}

// TestHostileFixturesMatchSequential: both hostile keyspaces list
// row-identical to the sequential oracle, parallel and seeded.
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
// floors, the seed descent). One
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
		{name: "w16/probe", opt: Options{Probe: true}, workers: 16, budget: 500},
		{name: "w16/seed/probe/short", opt: Options{Seed: true, Probe: true}, workers: 16, budget: 500, short: true},
		{name: "w64/floors/page=13", opt: Options{Floors: true}, workers: 64, budget: 500, pageCap: 13},
		{name: "w16/seed/floors/short", opt: Options{Seed: true, Floors: true}, workers: 16, budget: 500, short: true},
		{name: "w64/descent/page=13", opt: Options{Seed: true, SeedDescent: true}, workers: 64, budget: 500, pageCap: 13},
		{name: "w8/descent/forced-splits", opt: Options{Seed: true, SeedDescent: true}, workers: 8, budget: 500, coin: true},
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
			api.pageCap = v.pageCap
			opt := v.opt
			if v.coin {
				rng := rand.New(rand.NewSource(int64(i)))
				var mu sync.Mutex
				opt.splitCoin = func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(2) == 0 }
			}
			got, _, _ := runBudget(t, api, opt, v.workers, v.budget)
			if !slices.Equal(got, want) {
				t.Fatalf("%s %s: %d keys, want %d", name, v.name, len(got), len(want))
			}
		}
	}
}
