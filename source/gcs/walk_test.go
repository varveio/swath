// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// TestWalkEachEntryOnce: a walk of the strict fake, which answers every
// seventh request with an empty page that goes on, hands emit every object
// under its prefix once, the zero-length object at exactly the prefix
// included, through many ranges at eight workers and through one request
// chain at one; it completes with nothing unread, and every request it
// sends reaches the observer on its context.
func TestWalkEachEntryOnce(t *testing.T) {
	keys := append(gcstest.Keys(10_000), "d03/")
	slices.Sort(keys)
	f := gcstest.NewStrict(keys)
	f.Lean, f.Empty, f.Delay = true, 7, time.Millisecond
	src := open(t, serve(t, f), "bkt")

	for _, c := range []struct {
		prefix  string
		workers int
	}{{"", 8}, {"", 1}, {"d03/", 8}, {"d0", 4}, {"none/", 8}} {
		before := len(f.Sent())
		obs := &observed{}
		ctx := objfile.WithObserver(context.Background(), obs)
		var got []string
		var unread int
		sum, err := src.Walk(ctx, listing.WalkRequest{Prefix: []byte(c.prefix), Workers: c.workers}, func(b listing.WalkBatch) error {
			for _, e := range b.Entries {
				if e.Kind != listing.KindObject || e.Size == nil || *e.Size != int64(len(e.Key)) || e.LastModified == nil {
					return fmt.Errorf("entry %q: kind %v, size %v", e.Key, e.Kind, e.Size)
				}
				got = append(got, string(e.Key))
			}
			unread += len(b.Unread)
			return nil
		})
		if err != nil {
			t.Fatalf("%q at %d: %v", c.prefix, c.workers, err)
		}

		slices.Sort(got)
		var want []string
		for _, k := range keys {
			if strings.HasPrefix(k, c.prefix) {
				want = append(want, k)
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%q at %d: %d keys, want %d", c.prefix, c.workers, len(got), len(want))
		}
		if ev := sum.Evidence; sum.Entries != int64(len(got)) || unread != 0 || ev.Unlocated == nil || *ev.Unlocated != 0 {
			t.Errorf("%q at %d: %+v, unread %d", c.prefix, c.workers, sum, unread)
		}

		sent := f.Sent()[before:]
		starts := 0
		for _, q := range sent {
			if q.Get("startOffset") != "" && q.Get("pageToken") == "" {
				starts++
			}
		}
		switch {
		case c.workers == 1 && starts != 0:
			t.Errorf("sequential: %d ranges started after a key", starts)
		case c.prefix == "" && c.workers > 1 && starts < 2:
			t.Errorf("%q at %d: %d ranges started after a key, want a walk spread over several", c.prefix, c.workers, starts)
		}
		if len(obs.seen) != len(sent) {
			t.Errorf("%q at %d: the observer saw %d requests, the store %d", c.prefix, c.workers, len(obs.seen), len(sent))
		}
	}
}
