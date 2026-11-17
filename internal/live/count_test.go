// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/reader/listing"
)

// tiles is a provider whose root emits its first keys and hands each
// other set of keys to a child range; every range's keys ascend.
type tiles [][]string

func (t tiles) Root() int { return 0 }

func (t tiles) Expand(ctx context.Context, r int, e source.Emitter[int, s3.Object]) error {
	var rows []s3.Row
	for _, k := range t[r] {
		rows = append(rows, s3.Row{Key: k})
	}
	if err := e.Rows(rows); err != nil {
		return err
	}
	if r == 0 {
		for i := 1; i < len(t); i++ {
			if err := e.Child(i); err != nil {
				return err
			}
		}
	}
	return nil
}

// keys is a Sink that keeps the keys it is handed.
type keys struct {
	mu  sync.Mutex
	got []string
}

func (k *keys) Entries(_ context.Context, entries []listing.Entry) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, e := range entries {
		k.got = append(k.got, string(e.Key))
	}
	return nil
}

func (k *keys) Unread(listing.Unread) {}

func entries(rows []s3.Row) []listing.Entry {
	out := make([]listing.Entry, len(rows))
	for i := range rows {
		out[i] = listing.Entry{Key: []byte(rows[i].Key), Kind: listing.KindObject}
	}
	return out
}

// TestCountChecksRangesDisjoint: an ordered count hands on the keys of
// ranges that do not overlap, and refuses, as source.ErrProtocol, a range
// whose keys overlap another's, before any key reaches the sink twice.
func TestCountChecksRangesDisjoint(t *testing.T) {
	for _, c := range []struct {
		name  string
		tiles tiles
		ok    bool
	}{
		{"disjoint", tiles{{"a", "b"}, {"c", "d"}, {"e"}}, true},
		{"overlapping", tiles{{"a", "c"}, {"b", "d"}}, false},
		{"repeated", tiles{{"a"}, {"b"}, {"b"}}, false},
		{"inside", tiles{{"a", "z"}, {"m"}}, false},
	} {
		for _, workers := range []int{1, 3} {
			b, err := bind.Provider[int, s3.Object](c.tiles, &s3.Schema)
			if err != nil {
				t.Fatal(err)
			}
			sink := &keys{}
			err = live.Count(context.Background(), b, order.Options{Workers: workers}, entries, sink)

			if c.ok != (err == nil) || err != nil && !errors.Is(err, source.ErrProtocol) {
				t.Errorf("%s at %d: %v", c.name, workers, err)
			}
			got := slices.Sorted(slices.Values(sink.got))
			if len(slices.Compact(slices.Clone(got))) != len(got) {
				t.Errorf("%s at %d: a key reached the sink twice: %q", c.name, workers, got)
			}
		}
	}
}
