// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/reader/listing"
)

// unreading is a root that reports a place it did not read, hands over n
// children, stops the count, reports its own rest and returns; a child is
// never expanded, and says it is dropped by its number. With panics, the
// root panics instead of returning.
type unreading struct {
	n      int
	cancel context.CancelFunc
	panics bool
}

func (u *unreading) Root() int { return -1 }

func (u *unreading) Expand(ctx context.Context, r int, e source.Emitter[int, tp]) error {
	if r >= 0 {
		return nil
	}
	ur := e.(source.Unreader)
	ur.Unread(listing.Unread{Key: []byte("met"), Scope: listing.UnreadSubtree, Class: listing.UnreadPermission})
	for i := range u.n {
		if err := e.Child(i); err != nil {
			return err
		}
	}
	if u.panics {
		panic("a provider's bug")
	}
	u.cancel()
	ur.Unread(listing.Unread{Key: []byte("rest"), Scope: listing.UnreadListing, Class: listing.UnreadBudget})
	return ctx.Err()
}

func (u *unreading) Dropped(r int) (listing.Unread, bool) {
	return listing.Unread{Key: []byte(strconv.Itoa(r)), Scope: listing.UnreadSubtree, Class: listing.UnreadBudget}, true
}

// unreadSink keeps what a count tells it was not read, each "scope class
// key".
type unreadSink struct {
	mu  sync.Mutex
	got []string
}

func (s *unreadSink) Write(context.Context, [][]schema.Row[tp]) error { return nil }

func (s *unreadSink) Unread(u listing.Unread) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, string(u.Scope)+" "+string(u.Class)+" "+string(u.Key))
}

// TestCountTellsUnread: a count whose sink is an UnreadSink is told what
// the provider reports through its emitter, during its Expand and when it
// stops, and the extent of each range the stop drops unexpanded
// (source.Dropper), before the count returns; through Erase's adapters as
// directly.
func TestCountTellsUnread(t *testing.T) {
	want := []string{"listing budget rest", "subtree budget 0", "subtree budget 1", "subtree budget 2", "subtree permission met"}
	for _, erased := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		u := &unreading{n: 3, cancel: cancel}
		sink := &unreadSink{}
		var err error
		if erased {
			_, err = CountWithSink(ctx, source.Erase[int, tp](u), nil, Options{Workers: 1}, sink)
		} else {
			_, err = CountWithSink(ctx, u, nil, Options{Workers: 1}, sink)
		}
		cancel()
		if err == nil {
			t.Fatalf("erased %v: a stopped count returned no error", erased)
		}
		if got := slices.Sorted(slices.Values(sink.got)); !slices.Equal(got, want) {
			t.Errorf("erased %v: told %q, want %q", erased, got, want)
		}
	}
}

// TestCountRecoversAPanic: a panic in a provider's Expand ends the count
// with an error naming it, not the process, and the ranges it handed over
// are dropped as at any stop.
func TestCountRecoversAPanic(t *testing.T) {
	u := &unreading{n: 2, panics: true, cancel: func() {}}
	sink := &unreadSink{}
	_, err := CountWithSink(context.Background(), u, nil, Options{Workers: 2}, sink)
	if err == nil || !strings.Contains(err.Error(), "panicked: a provider's bug") {
		t.Fatalf("err %v", err)
	}
	if got := slices.Sorted(slices.Values(sink.got)); !slices.Contains(got, "subtree budget 0") || !slices.Contains(got, "subtree budget 1") {
		t.Errorf("told %q", got)
	}
}
