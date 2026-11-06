// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
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
			got, _, leaves := run(t, newSorted(keys, true, int64(s+1)), Options{Resume: &Resume{After: k}, SplitCoin: coin}, 8)
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

// TestBoundVetoOnEveryRequest: the provider's bound predicate guards every
// request, not only pivots: a resume after "a5" with a provider vetoing
// '5' starts at the accepted prefix "a" and drops keys up to "a5"; a capped
// top seed page ending in the object "b5" tiles at accepted bounds only. Every
// transmitted bound is accepted and every unpublished key appears once.
func TestBoundVetoOnEveryRequest(t *testing.T) {
	veto := func(s string) bool { return !strings.ContainsRune(s, '5') }
	resumed := []string{"a/x", "a1", "a4", "a5", "a50", "a6", "b5", "c1", "c2", "c3", "c4", "d/y", "e1"}
	seeded := []string{"a/x", "a/y", "b5", "c1", "c2", "c3", "d/y", "e1"} // the top page: a/, b5 (capped)
	for _, c := range []struct {
		name string
		keys []string
		opt  Options
	}{
		{"resume/sequential", resumed, Options{Sequential: true, Resume: &Resume{After: "a5"}}},
		{"resume/parallel", resumed, Options{Resume: &Resume{After: "a5"}}},
		{"seed page, capped top", seeded, Options{Seed: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			keys := c.keys
			f := newSorted(keys, false, 1)
			f.PageCap = 2
			var mu sync.Mutex
			var sent []string
			fetch := fetchFrom(f)
			d := Description[Object]{Target: "test", Listing: Listing, AcceptBound: veto}
			opt := c.opt
			opt.Workers = 4
			p, err := New(opt, d, func(ctx context.Context, q *Query) (*Page[Object], error) {
				if q.Token == "" && q.After != "" {
					mu.Lock()
					sent = append(sent, q.After)
					mu.Unlock()
				}
				return fetch(ctx, q)
			})
			if err != nil {
				t.Fatal(err)
			}
			col := &collect{}
			if _, err := order.Run[Range, Object](context.Background(), p, col, &Schema, order.Options{Workers: 4, Budget: 1 << 20}); err != nil {
				t.Fatal(err)
			}
			want := keys
			if opt.Resume != nil {
				want = keys[slices.Index(keys, "a5")+1:]
			}
			if got := keysOf(col.rows); !slices.Equal(got, want) {
				t.Fatalf("keys %q, want %q; sent %q", got, want, sent)
			}
			for _, b := range sent {
				if !veto(b) {
					t.Fatalf("sent a vetoed bound %q (all: %q)", b, sent)
				}
			}
		})
	}
}
