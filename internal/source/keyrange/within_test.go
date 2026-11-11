// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
)

// withinRoot is a provider whose root emits one Within range per scope, as
// the tree core hands the engine the rest of each large directory.
type withinRoot struct {
	*Engine[Object]
	tiles []Range
}

type rootRange struct {
	r    Range
	root bool
}

func (w *withinRoot) Root() rootRange { return rootRange{root: true} }

func (w *withinRoot) Expand(ctx context.Context, r rootRange, e source.Emitter[rootRange, Object]) error {
	if r.root {
		for _, t := range w.tiles {
			if err := e.Child(rootRange{r: t}); err != nil {
				return err
			}
		}
		return nil
	}
	return w.Engine.Expand(ctx, r.r, wrapEmitter{e})
}

type wrapEmitter struct {
	e source.Emitter[rootRange, Object]
}

func (w wrapEmitter) Rows(rows []Row) error { return w.e.Rows(rows) }
func (w wrapEmitter) Child(r Range) error   { return w.e.Child(rootRange{r: r}) }

// TestWithin: ranges a caller tiles itself list their own scope after
// their start, split (children keep the scope: no key of another scope
// appears) and together give exactly the keys after each start.
func TestWithin(t *testing.T) {
	var keys []string
	for _, d := range []string{"a/", "b/", "c/"} {
		for i := range 3000 {
			keys = append(keys, fmt.Sprintf("%sk%05d", d, i))
		}
	}
	s := newSorted(keys, false, 1)
	var mu sync.Mutex
	prefixes := map[string]bool{}
	fetch := func(ctx context.Context, q *Query) (*Page[Object], error) {
		mu.Lock()
		prefixes[q.Prefix] = true
		mu.Unlock()
		return fetchFrom(s)(ctx, q)
	}
	for _, workers := range []int{1, 8} {
		e, err := New(Options{Workers: workers, PageSize: 50, SplitCoin: func() bool { return true }},
			Description[Object]{Target: "test", Listing: Listing}, fetch)
		if err != nil {
			t.Fatal(err)
		}
		w := &withinRoot{Engine: e, tiles: []Range{e.Within("a/", "a/k00099"), e.Within("c/", "c/")}}
		col := &collect{}
		if _, err := order.Run[rootRange, Object](context.Background(), w, col, &Schema, order.Options{Workers: workers, Budget: 1 << 22}); err != nil {
			t.Fatal(err)
		}
		var want []string
		for _, k := range keys {
			if k > "a/k00099" && strings.HasPrefix(k, "a/") || strings.HasPrefix(k, "c/") {
				want = append(want, k)
			}
		}
		if got := keysOf(col.rows); !slices.Equal(got, want) {
			t.Fatalf("workers %d: %d keys, want %d", workers, len(got), len(want))
		}
		if st := e.Stats(); st.Get(cSplitAcceptedBounded) == 0 {
			t.Fatalf("workers %d: no split", workers)
		}
	}
	if prefixes[""] || !prefixes["a/"] || !prefixes["c/"] {
		t.Fatalf("request prefixes %v: want the tiles' scopes only", prefixes)
	}
}
