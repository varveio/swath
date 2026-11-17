// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// queries records the List Blobs queries a fake store is sent.
type queries struct {
	mu   sync.Mutex
	all  []url.Values
	sent atomic.Int64 // every request, the namespace's included
}

func (q *queries) record(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q.sent.Add(1)
		if v := r.URL.Query(); v.Get("comp") == "list" {
			q.mu.Lock()
			q.all = append(q.all, v)
			q.mu.Unlock()
		}
		time.Sleep(time.Millisecond)
		h.ServeHTTP(w, r)
	})
}

// TestWalkEachEntryOnce: a walk of a flat account's container, whose names
// Azure lists in UTF-16 order (azblobtest.Keys), hands emit every blob
// under its prefix once, through many ranges at eight workers and through
// one request chain at one; it completes with nothing unread, and every
// request it sends, the namespace's admission included, reaches the
// observer on its context.
func TestWalkEachEntryOnce(t *testing.T) {
	noCredentials(t)
	keys := azblobtest.Keys()
	rec := &queries{}
	src := open(t, serve(t, rec.record(namespace("false", new(atomic.Int32), azblobtest.Handler(azblobtest.NewStore(keys), account, container)))))

	for _, c := range []struct {
		prefix  string
		workers int
	}{{"", 8}, {"", 1}, {"f/", 8}, {"p", 4}, {"none/", 8}} {
		rec.mu.Lock()
		before := len(rec.all)
		rec.mu.Unlock()
		sentBefore := rec.sent.Load()
		obs := &observed{}
		ctx := objfile.WithObserver(context.Background(), obs)
		var got []string
		var unread int
		sum, err := src.Walk(ctx, listing.WalkRequest{Prefix: []byte(c.prefix), Workers: c.workers}, func(b listing.WalkBatch) error {
			for _, e := range b.Entries {
				if e.Kind != listing.KindObject || e.Size == nil || *e.Size != int64(len(e.Key)) {
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
		slices.Sort(want)
		want = slices.Compact(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%q at %d: %d keys, want %d", c.prefix, c.workers, len(got), len(want))
		}
		if ev := sum.Evidence; sum.Entries != int64(len(got)) || unread != 0 || ev.Stop != listing.StopCompleted || ev.Unlocated == nil || *ev.Unlocated != 0 {
			t.Errorf("%q at %d: %+v, unread %d", c.prefix, c.workers, sum, unread)
		}

		rec.mu.Lock()
		lists := rec.all[before:]
		rec.mu.Unlock()
		starts := 0
		for _, q := range lists {
			if q.Get("startFrom") != "" && q.Get("marker") == "" {
				starts++
			}
		}
		switch {
		case c.workers == 1 && starts != 0:
			t.Errorf("sequential: %d ranges started after a key", starts)
		case c.prefix == "" && c.workers > 1 && starts < 2:
			t.Errorf("%q at %d: %d ranges started after a key, want a walk spread over several", c.prefix, c.workers, starts)
		}
		if n := rec.sent.Load() - sentBefore; int64(len(obs.seen)) != n {
			t.Errorf("%q at %d: the observer saw %d requests, the store %d", c.prefix, c.workers, len(obs.seen), n)
		}
	}
}

// TestWalkHierarchicalRefused: a walk of a hierarchical-namespace account
// is listing.ErrUnsupported once admission has read the namespace, before
// any List Blobs request.
func TestWalkHierarchicalRefused(t *testing.T) {
	noCredentials(t)
	rec := &queries{}
	src := open(t, serve(t, rec.record(namespace("true", new(atomic.Int32), azblobtest.Handler(azblobtest.NewStore([]string{"a/b"}), account, container)))))

	_, err := src.Walk(context.Background(), listing.WalkRequest{}, func(listing.WalkBatch) error { return nil })
	if !errors.Is(err, listing.ErrUnsupported) {
		t.Fatalf("walk of a hierarchical namespace: %v, want ErrUnsupported", err)
	}
	if len(rec.all) != 0 {
		t.Errorf("%d List Blobs requests, want none", len(rec.all))
	}
}
