// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/reader/listing"
)

// unreading is a root that reports a place it could not read, hands over
// n children, stops the count and returns; a child is never expanded.
// With panics, the root panics instead of returning.
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
	ur.Unread(listing.Unread{Key: []byte("met"), Reason: listing.ErrAuth})
	for i := range u.n {
		if err := e.Child(i); err != nil {
			return err
		}
	}
	if u.panics {
		panic("a provider's bug")
	}
	u.cancel()
	return ctx.Err()
}

// unreadSink keeps the keys of what a count tells it was not read.
type unreadSink struct {
	mu  sync.Mutex
	got []string
}

func (s *unreadSink) Write(context.Context, [][]schema.Row[tp]) error { return nil }

func (s *unreadSink) Unread(u listing.Unread) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, string(u.Key))
}

// TestCountTellsUnread: a count whose sink is an UnreadSink is told what
// the provider reports through its emitter, and nothing of the ranges a
// stop drops; through Erase's adapters as directly.
func TestCountTellsUnread(t *testing.T) {
	want := []string{"met"}
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

// recoveringSink is an unreadSink that asks for a provider's panic as
// the count's error (Recovering).
type recoveringSink struct{ unreadSink }

func (*recoveringSink) Recovers() bool { return true }

// TestCountRecoversAPanic: for a Recovering sink, a panic in a provider's
// Expand ends the count with an error naming it, not the process. (Any
// other count lets the panic end the process, which a test cannot
// observe.)
func TestCountRecoversAPanic(t *testing.T) {
	u := &unreading{n: 2, panics: true, cancel: func() {}}
	sink := &recoveringSink{}
	_, err := CountWithSink(context.Background(), u, nil, Options{Workers: 2}, sink)
	if err == nil || !strings.Contains(err.Error(), "panicked: a provider's bug") {
		t.Fatalf("err %v", err)
	}
}
