// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source/keyrange"
)

// A pruned directory must never reach the lister, even when its row is
// recovered as a pivot candidate in a segment of its parent's listing.
type pruningLister struct {
	*hns
	prune func(string) bool
}

func (l pruningLister) List(ctx context.Context, q *DirQuery) (*DirPage[obj], error) {
	if l.prune(q.Dir) {
		return nil, fmt.Errorf("opened excluded directory %q", q.Dir)
	}
	return l.hns.List(ctx, q)
}

func TestOrderedRecursiveExcludes(t *testing.T) {
	keys := append(manyKeys(3000), "early/hidden", "z/visible")
	spec := filter.Spec{Exclude: []string{"early/**", "big/part/**", "big/zz/**"}}
	prune, _, err := spec.PruneDirectories()
	if err != nil {
		t.Fatal(err)
	}
	for _, workers := range []int{1, 8} {
		for _, page := range []int{7, 1000, 10000} {
			t.Run(fmt.Sprintf("w%d/p%d", workers, page), func(t *testing.T) {
				h := newHNS(keys)
				want := slices.DeleteFunc(h.oracle(), func(key string) bool {
					for _, prefix := range []string{"early/", "big/part/", "big/zz/"} {
						if key != prefix && strings.HasPrefix(key, prefix) {
							return true
						}
					}
					return false
				})
				o, err := NewOrdered[obj](pruningLister{h, prune}, keyrange.Options{
					Workers: workers, PageSize: page, SplitCoin: func() bool { return true },
				}, keyrange.Description[obj]{Target: "hns", Listing: keyrange.Listing})
				if err != nil {
					t.Fatal(err)
				}
				w := NewWalker[struct{}, keyrange.Range, obj](OrderedRange{}, o, Options[obj]{})
				w.SetPruneDirectories(prune)
				c := &collect{}
				_, err = order.Run[OrderedRange, obj](context.Background(), w, c, &testSchema, order.Options{Workers: workers, Budget: 1 << 24})
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(c.keys, want) || len(c.bad) != 0 {
					t.Fatalf("output differs: got %d keys, want %d, bad payloads %q", len(c.keys), len(want), c.bad)
				}
				if st := w.Stats(); st.Excluded != 3 || st.Pruned != 0 || st.Files+st.Dirs != int64(len(want)) {
					t.Fatalf("counts %+v, want 3 excludes and %d rows", st, len(want))
				}
				if page < 3000 && counter(o, "tree.dirs_split") == 0 {
					t.Fatal("large directory did not use segments")
				}
			})
		}
	}
}
