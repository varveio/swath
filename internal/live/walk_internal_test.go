// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/varveio/swath/reader/listing"
)

// errRefused is every failing place's Reason, shared so that the store
// itself keeps nothing per place.
var errRefused = fmt.Errorf("%w: refused", listing.ErrAuth)

// failing is a store whose walk reads nothing under its prefix and
// reports n places it could not read, each with a key of its own. After
// the last it measures the live heap into heap, if set: what the walk
// still holds of the places it handed on.
type failing struct {
	n    int
	heap *uint64
}

func (failing) Location() string { return "failing" }

func (failing) List(context.Context, []byte, string, int) (*Level, error) {
	return &Level{}, nil
}

func (failing) Lookup(context.Context, []byte) (listing.LookupResult, error) {
	return listing.LookupResult{State: listing.LookupAbsent}, nil
}

func (failing) Close() error { return nil }

func (f failing) Walk(_ context.Context, w Walk, sink Sink) error {
	for i := range f.n {
		sink.Unread(listing.Unread{Key: fmt.Appendf(nil, "%sx%08d", w.Prefix, i), Reason: errRefused})
	}
	if f.heap != nil {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		*f.heap = m.HeapAlloc
	}
	return nil
}

// TestWalkStreamsUnread: a walk hands emit every place it could not read,
// as it meets each, keeps none of them, and counts them; the heap a walk
// of 20,000 such places holds at its end is no larger, within noise, than
// a walk of 200's.
func TestWalkStreamsUnread(t *testing.T) {
	held := func(n int) uint64 {
		least := ^uint64(0)
		for range 3 {
			var heap uint64
			got := 0
			src := &Source{store: failing{n: n, heap: &heap}}
			sum, err := src.Walk(context.Background(), listing.WalkRequest{Prefix: []byte("b/")}, func(b listing.WalkBatch) error {
				for _, u := range b.Unread {
					if !errors.Is(u.Reason, listing.ErrAuth) {
						t.Fatalf("reason %v", u.Reason)
					}
				}
				got += len(b.Unread)
				return nil
			})
			if err != nil || got != n || sum.Unread != int64(n) || sum.Evidence.Stop != listing.StopCompleted {
				t.Fatalf("%d places: %d emitted, %+v, %v", n, got, sum, err)
			}
			least = min(least, heap)
		}
		return least
	}
	few, many := held(200), held(20_000)
	if many > few && many-few > 256<<10 {
		t.Errorf("the heap at the end of the walk grew by %d bytes from 200 places to 20,000", many-few)
	}
}

// TestWalkCancelled: a context that ends while emit takes a batch ends
// the walk with its error; emit is handed nothing more, and the summary
// counts what it took and says the walk did not finish.
func TestWalkCancelled(t *testing.T) {
	src := &Source{store: failing{n: 1000}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	sum, err := src.Walk(ctx, listing.WalkRequest{}, func(listing.WalkBatch) error {
		calls++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 || sum.Unread != 1 || sum.Evidence.Stop == listing.StopCompleted || sum.Evidence.Unlocated != nil {
		t.Fatalf("err %v after %d batches, %+v", err, calls, sum)
	}
}
