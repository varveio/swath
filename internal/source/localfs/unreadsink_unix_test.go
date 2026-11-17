// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/reader/listing"
)

// unreadSink is a count's sink that also keeps what the count tells it
// was not read (order.UnreadSink), each "scope class key" as unreads
// keeps them.
type unreadSink struct {
	order.Sink[Entry]
	mu  sync.Mutex
	got []string
}

func (s *unreadSink) Unread(u listing.Unread) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, fmt.Sprintf("%s %s %s", u.Scope, u.Class, u.Key))
}

func (s *unreadSink) sorted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(slices.Values(s.got))
}

// TestUnreadReachesTheSink: every place the walk reports it did not read
// (Provider.unread) reaches a count's order.UnreadSink through the
// emitter, with either engine: the directories a MaxDepth keeps it from
// entering, and, when it stops, the ranges it did not finish, those the
// count drops unexpanded (Dropped) included.
func TestUnreadReachesTheSink(t *testing.T) {
	root := t.TempDir()
	for i := range 4 {
		for j := range 4 {
			mk(t, root, fmt.Sprintf("d%d/e%d/f", i, j), false)
		}
	}
	f := holdDir(t, root)
	for _, portable := range []bool{false, true} {
		for _, c := range []struct {
			name  string
			depth int
			stop  int // rows after which the walk is cancelled; 0 for none
		}{{"too deep", 1, 0}, {"stopped early", 0, 1}, {"stopped later", 0, 20}} {
			var u unreads
			o := u.to(mustOpenDir(t, f, Options{Fields: Stat, Portable: portable, MaxDepth: c.depth}))
			ctx, cancel := context.WithCancel(context.Background())
			sink := &unreadSink{Sink: &collect{}}
			if c.stop > 0 {
				sink.Sink = serial(&cancelAfter{n: c.stop, cancel: cancel}, &collect{})
			}
			err := countOpened(ctx, o, 4, sink)
			cancel()
			if c.stop > 0 && !errors.Is(err, context.Canceled) || c.stop == 0 && err != nil {
				t.Fatalf("portable=%v %s: %v", portable, c.name, err)
			}
			if got, want := sink.sorted(), u.sorted(); len(want) == 0 || !slices.Equal(got, want) {
				t.Errorf("portable=%v %s: the sink was told %q, the walk reported %q", portable, c.name, got, want)
			}
		}
	}
}
