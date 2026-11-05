// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
)

// TestResumeAfterKey: a resume lists (K, ⊤] after K, the last published
// key, from the normal start-up (no recorded split history). For random
// keyspaces, split coins and K, the resumed listing returns exactly the
// keys after K, once each, and its ranges tile (K, ⊤].
func TestResumeAfterKey(t *testing.T) {
	seeds := 40
	if testing.Short() {
		seeds = 5
	}
	spaces := keyspaces()
	names := slices.Sorted(func(yield func(string) bool) {
		for k := range spaces {
			if !yield(k) {
				return
			}
		}
	})
	for _, name := range names {
		keys := slices.Compact(slices.Sorted(slices.Values(spaces[name])))
		if len(keys) < 2 {
			continue
		}
		for s := range seeds {
			rng := rand.New(rand.NewSource(int64(s)))
			var mu sync.Mutex
			coin := func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
			k := keys[rng.Intn(len(keys)-1)]
			got, _, leaves := run(t, newSorted(keys, true, int64(s+1)), Options{Resume: &Resume{After: k}, splitCoin: coin}, 8)
			want := keys[slices.Index(keys, k)+1:]
			what := fmt.Sprintf("%s seed=%d K=%q", name, s, k)
			if !slices.Equal(got, want) {
				t.Fatalf("%s: resumed %d keys, want %d", what, len(got), len(want))
			}
			// The resumed ranges tile (K, ⊤]; the first starts at K or at the
			// safe prefix of K it lists from.
			for i := range leaves {
				if leaves[i].lo == k || leaves[i].lo == safePrefix(k) {
					leaves[i].lo = ""
				}
			}
			checkTiling(t, what, leaves)
		}
	}
}
