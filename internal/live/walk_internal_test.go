// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/varveio/swath/reader/listing"
)

// unreading is a store whose walk reads nothing under its prefix and
// reports n subtrees it could not read, refused by permission.
type unreading struct{ n int }

func (unreading) Location() string { return "unreading" }

func (unreading) List(context.Context, []byte, string, int) (*Level, error) {
	return &Level{}, nil
}

func (unreading) Lookup(context.Context, []byte) (listing.LookupResult, error) {
	return listing.LookupResult{State: listing.LookupAbsent}, nil
}

func (unreading) Close() error { return nil }

func (u unreading) Walk(_ context.Context, w Walk, sink Sink) error {
	for i := range u.n {
		sink.Unread(listing.Unread{Key: fmt.Appendf(nil, "%sx%08d", w.Prefix, i), Scope: listing.UnreadSubtree, Class: listing.UnreadPermission})
	}
	return nil
}

// TestWalkDropsUnreadPastItsBound: a walk that meets more unread extents
// than it holds hands emit as many as it holds and says it dropped the
// rest (a nil Evidence.Unlocated); a Summarize over it then leaves a
// target whose own entry it did not read unknown, never absent.
func TestWalkDropsUnreadPastItsBound(t *testing.T) {
	for _, c := range []struct {
		n     int
		lost  bool
		state listing.LookupState
	}{
		{maxPendingUnread, false, listing.LookupAbsent},
		{maxPendingUnread + 10, true, listing.LookupUnknown},
	} {
		src := &Source{store: unreading{n: c.n}}
		got := 0
		sum, err := src.Walk(context.Background(), listing.WalkRequest{Prefix: []byte("b/")}, func(b listing.WalkBatch) error {
			got += len(b.Unread)
			return nil
		})
		if err != nil || got != min(c.n, maxPendingUnread) || (sum.Evidence.Unlocated == nil) != c.lost {
			t.Fatalf("%d extents: %d emitted, %+v, %v", c.n, got, sum.Evidence, err)
		}

		out, ev, err := src.Summarize(context.Background(), []listing.Target{{Key: []byte("b/"), Subtree: true}}, listing.SummaryOptions{})
		if err != nil || (ev.Unlocated == nil) != c.lost || out[0].State != c.state {
			t.Fatalf("%d extents: summary %+v, evidence %+v, %v", c.n, out[0].State, ev, err)
		}
		if c.lost && !errors.Is(out[0].Reason, listing.ErrBudgetExceeded) {
			t.Errorf("%d extents: reason %v", c.n, out[0].Reason)
		}
	}
}

// TestWalkCancelledWhileHandingUnread: a context that ends while emit
// takes a batch of extents not read ends the walk with its error, and
// emit is handed nothing more.
func TestWalkCancelledWhileHandingUnread(t *testing.T) {
	src := &Source{store: unreading{n: 3 * unreadBatch}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	_, err := src.Walk(ctx, listing.WalkRequest{}, func(listing.WalkBatch) error {
		calls++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err %v after %d batches", err, calls)
	}
}
