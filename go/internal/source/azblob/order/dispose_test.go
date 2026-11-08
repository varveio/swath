// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/varveio/swath/go/internal/source/azblob/order/ordertest"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// TestAbandonedChains is §7.5: chains abandoned after every page, and
// after a failed page, stay registered until Release, which drops them
// all and refuses their tokens; a chain read to its end releases itself;
// the adapter starts no goroutine.
func TestAbandonedChains(t *testing.T) {
	keys := []string{"a", "a" + E, "a" + E + S, "a" + S, "a" + S + E, "b", "c", "d"}
	goroutines := runtime.NumGoroutine()
	st := ordertest.New(keys)
	st.Short = every(1)
	a := New(st, "c", nil, 0)
	for round := range 3 {
		var tokens []keyrange.Query
		for stop := 0; ; stop++ {
			q := keyrange.Query{Max: 1}
			done := false
			for range stop + 1 {
				p, err := a.Fetch(context.Background(), &q)
				if err != nil {
					t.Fatal(err)
				}
				if !p.Truncated {
					done = true
					break
				}
				q.Token = p.Token
			}
			if done {
				break
			}
			tokens = append(tokens, q)
		}
		// A page that fails leaves its chain as it was.
		failing := ordertest.New(keys)
		failing.Fail = ordertest.FailAt(1)
		b := New(failing, "c", nil, 0)
		q := keyrange.Query{Max: 1}
		p, err := b.Fetch(context.Background(), &q)
		if err != nil {
			t.Fatal(err)
		}
		q.Token = p.Token
		if _, err := b.Fetch(context.Background(), &q); !errors.Is(err, ordertest.ErrInjected) || b.Len() != 1 {
			t.Fatalf("a failed page: %v, %d chains", err, b.Len())
		}
		b.Release()
		if b.Len() != 0 {
			t.Fatal("Release left a chain whose last page failed")
		}

		if a.Len() != len(tokens) || len(tokens) < len(keys)-1 {
			t.Fatalf("round %d: %d chains registered, %d abandoned", round, a.Len(), len(tokens))
		}
		a.Release()
		if a.Len() != 0 {
			t.Fatalf("round %d: %d chains after Release", round, a.Len())
		}
		for _, q := range tokens {
			if _, err := a.Fetch(context.Background(), &q); !errors.Is(err, ErrToken) {
				t.Fatalf("an abandoned chain's token after Release: %v", err)
			}
		}
	}
	if n := runtime.NumGoroutine(); n > goroutines {
		t.Errorf("%d goroutines, %d before", n, goroutines)
	}
}
