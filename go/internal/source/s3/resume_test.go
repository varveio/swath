// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
)

// TestSplitStateModel: a split moves DECIDED → DURABLE → CHILDREN_EMITTED.
// DECIDED is in memory (the provider's boundary list), DURABLE is taken
// into a journal record at a part close, CHILDREN_EMITTED is the children
// queued. A crash can land in any state, with any prefix of the keys
// published. For random runs, crash points (the last published key K) and
// durable prefixes of the boundary history, the resumed listing must
// return exactly the keys after K, once each, and its ranges must tile
// (K, ⊤]: undurable splits cost parallelism, never keys.
func TestSplitStateModel(t *testing.T) {
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
			// The interrupted run: collect its boundary history.
			p, err := New(context.Background(), Options{Bucket: "b", Client: newSorted(keys, true, int64(s)), Workers: 8, splitCoin: coin, Seed: s%2 == 1})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := order.Run[Range](context.Background(), p, &collect{}, order.Options{Workers: 8, Budget: 200 * 256}); err != nil {
				t.Fatal(err)
			}
			history := p.TakeBoundaries()
			// The crash: K is a published key, the journal holds a prefix of
			// the history (the rest was only DECIDED or not yet made).
			k := keys[rng.Intn(len(keys)-1)]
			durable := history[:rng.Intn(len(history)+1)]
			got, _, leaves := run(t, newSorted(keys, true, int64(s+1)), Options{Resume: &Resume{After: k, Boundaries: durable}, splitCoin: coin}, 8)
			want := keys[slices.Index(keys, k)+1:]
			what := fmt.Sprintf("%s seed=%d K=%q durable=%d/%d", name, s, k, len(durable), len(history))
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
