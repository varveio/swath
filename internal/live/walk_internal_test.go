// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

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
			if err != nil || got != n || sum.Unread != int64(n) || sum.Evidence.Unlocated == nil {
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
	if !errors.Is(err, context.Canceled) || calls != 1 || sum.Unread != 1 || sum.Evidence.Unlocated != nil {
		t.Fatalf("err %v after %d batches, %+v", err, calls, sum)
	}
}

// stalling is a store whose walk hands over one valid batch, then, if
// bad, a batch of an object without a size, and keeps one worker
// running until finish is closed.
type stalling struct {
	failing
	bad    bool
	finish <-chan struct{}
}

func (s stalling) Walk(ctx context.Context, w Walk, sink Sink) error {
	size := int64(1)
	err := sink.Entries(ctx, []listing.Entry{{Key: fmt.Appendf(nil, "%sa", w.Prefix), Kind: listing.KindObject, Size: &size}})
	if err == nil && s.bad {
		err = sink.Entries(ctx, []listing.Entry{{Key: fmt.Appendf(nil, "%sb", w.Prefix), Kind: listing.KindObject}})
	}
	<-s.finish
	if err != nil {
		return err
	}
	return ctx.Err()
}

// TestWalkDeadlineAfterInvalidPage (#666): a walk that refused a page
// returns ErrInvalidPage, not the context's error, when the context's
// deadline passes before its workers finish; with no page refused it
// returns the deadline's error. The first batch is emitted either way.
// The deadline runs on synctest's clock, which reaches it only once the
// walk waits on its last worker.
func TestWalkDeadlineAfterInvalidPage(t *testing.T) {
	for _, tc := range []struct {
		name string
		bad  bool
		want error
	}{
		{"invalid page", true, listing.ErrInvalidPage},
		{"no invalid page", false, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				src := &Source{store: stalling{bad: tc.bad, finish: ctx.Done()}}
				emitted := 0
				sum, err := src.Walk(ctx, listing.WalkRequest{Prefix: []byte("p/")}, func(b listing.WalkBatch) error {
					emitted += len(b.Entries)
					return nil
				})
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Fatalf("the walk returned before its deadline: %v", ctx.Err())
				}
				if !errors.Is(err, tc.want) || emitted != 1 || sum.Entries != 1 || sum.Complete {
					t.Fatalf("err %v, %d entries emitted, %+v", err, emitted, sum)
				}
			})
		})
	}
}
