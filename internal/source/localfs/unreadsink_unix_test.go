// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/reader/listing"
)

// unreadSink is a count's sink that also keeps what the count tells it
// could not be read (order.UnreadSink), each "class key" as unreads keeps
// them.
type unreadSink struct {
	order.Sink[Entry]
	mu  sync.Mutex
	got []string
}

func (s *unreadSink) Unread(u listing.Unread) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, classOf(u.Reason)+" "+string(u.Key))
}

func (s *unreadSink) sorted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(slices.Values(s.got))
}

// TestUnreadReachesTheSink: every place the walk reports it could not
// read (Provider.unread), here directories it may not open, reaches a
// count's order.UnreadSink through the emitter, with either engine.
func TestUnreadReachesTheSink(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads a directory of mode 0")
	}
	root := t.TempDir()
	var want []string
	for i := range 4 {
		mk(t, root, fmt.Sprintf("d%d/f", i), false)
		mk(t, root, fmt.Sprintf("d%d/locked/g", i), false)
		want = append(want, fmt.Sprintf("auth d%d/locked/", i))
		locked := filepath.Join(root, fmt.Sprintf("d%d/locked", i))
		if err := os.Chmod(locked, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(locked, 0o755) })
	}
	f := holdDir(t, root)
	for _, portable := range []bool{false, true} {
		var u unreads
		o := u.to(mustOpenDir(t, f, Options{Fields: Stat, Portable: portable}))
		sink := &unreadSink{Sink: &collect{}}
		if err := countOpened(context.Background(), o, 4, sink); err != nil {
			t.Fatalf("portable=%v: %v", portable, err)
		}
		if got := sink.sorted(); !slices.Equal(got, want) || !slices.Equal(u.sorted(), want) {
			t.Errorf("portable=%v: the sink was told %q, the walk reported %q, want %q", portable, got, u.sorted(), want)
		}
	}
}
