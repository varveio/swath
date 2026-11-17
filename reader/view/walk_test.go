// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// walked is what a walk handed emit: every entry, copied, and every
// Unread.
type walked struct {
	entries []listing.Entry
	unread  []listing.Unread
}

// walk runs req on inv, keeping copies of what it hands emit, and checks
// that emit is never called concurrently.
func walk(t *testing.T, inv *Inventory, req listing.WalkRequest) (walked, listing.WalkSummary, error) {
	t.Helper()
	var w walked
	var inEmit atomic.Int32
	s, err := inv.Walk(context.Background(), req, func(b listing.WalkBatch) error {
		if inEmit.Add(1) != 1 {
			t.Error("emit called concurrently")
		}
		defer inEmit.Add(-1)
		for i := range b.Entries {
			w.entries = append(w.entries, *cloneEntry(&b.Entries[i]))
		}
		w.unread = append(w.unread, b.Unread...)
		return nil
	})
	return w, s, err
}

func keysOf(entries []listing.Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = string(e.Key) + " " + e.Kind.String()
	}
	slices.Sort(out)
	return out
}

// under lists what a walk of prefix hands on, from the records alone:
// every record whose key begins with it.
func under(rs []rec, prefix string, isLocal bool) []string {
	var out []string
	for _, r := range rs {
		if strings.HasPrefix(r.key, prefix) {
			out = append(out, r.key+" "+kindOf(r, isLocal).String())
		}
	}
	slices.Sort(out)
	return out
}

// TestWalkEveryEntryOnce walks every level of every schema's fixture,
// sequentially and in parallel shards: each entry the records give comes
// back once, with its values, and no other.
func TestWalkEveryEntryOnce(t *testing.T) {
	for _, id := range append(slices.Clone(objectSchemas), reader.LocalV1) {
		t.Run(string(id), func(t *testing.T) {
			isLocal := id == reader.LocalV1
			rs := objects()
			if isLocal {
				rs = tree()
			}
			byKey := map[string]rec{}
			for _, r := range rs {
				byKey[r.key] = r
			}
			inv := open(t, write(t, fixture{id: id, rows: rs}))
			for _, prefix := range scopes(rs, isLocal) {
				for _, req := range []listing.WalkRequest{
					{Prefix: []byte(prefix), Workers: 1},
					{Prefix: []byte(prefix), Workers: 4},
				} {
					w, s, err := walk(t, inv, req)
					if err != nil {
						t.Fatalf("walk %+v: %v", req, err)
					}
					want := under(rs, prefix, isLocal)
					if got := keysOf(w.entries); !slices.Equal(got, want) {
						t.Fatalf("walk %q workers %d:\n got %q\nwant %q", prefix, req.Workers, got, want)
					}
					if s.Entries != int64(len(w.entries)) || len(w.unread) != 0 || s.Evidence.Stop != listing.StopCompleted {
						t.Fatalf("walk %+v: summary %+v, unread %v", req, s, w.unread)
					}
					for _, e := range w.entries {
						if r := byKey[string(e.Key)]; !sameEntry(e, r, isLocal) {
							t.Fatalf("entry %+v, want %+v", e, r)
						}
					}
				}
			}
		})
	}
}

// TestWalkEntryBudget: a walk stops exactly at MaxEntries and hands on
// the interval after the last key it handed as unread; a walk resumed
// after it reads the rest.
func TestWalkEntryBudget(t *testing.T) {
	rs := objects()
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: rs}))
	for _, max := range []int64{1, 7, 40, int64(len(rs)) - 1} {
		w, s, err := walk(t, inv, listing.WalkRequest{Budget: listing.Budget{MaxEntries: max}})
		if err != nil || s.Evidence.Stop != listing.StopEntryBudget || s.Entries != max || int64(len(w.entries)) != max {
			t.Fatalf("max %d: %+v, %d entries, %v", max, s, len(w.entries), err)
		}
		last := w.entries[max-1].Key
		if len(w.unread) != 1 || string(w.unread[0].Key) != string(last) || w.unread[0].Hi != nil ||
			w.unread[0].Scope != listing.UnreadInterval || w.unread[0].Class != listing.UnreadBudget {
			t.Fatalf("max %d: unread %+v after %q", max, w.unread, last)
		}
		var rest []string
		for _, r := range rs {
			if r.key > string(last) {
				rest = append(rest, r.key)
			}
		}
		if len(rest) != len(rs)-int(max) {
			t.Fatalf("max %d: %d keys after %q, want %d", max, len(rest), last, len(rs)-int(max))
		}
	}

	// A budget the walk does not exceed completes it.
	if _, s, err := walk(t, inv, listing.WalkRequest{Budget: listing.Budget{MaxEntries: int64(len(rs))}}); err != nil || s.Evidence.Stop != listing.StopCompleted {
		t.Fatalf("whole budget: %+v, %v", s, err)
	}
}

// TestWalkTimeBudget: a walk its MaxDuration ends hands on the interval
// after the last key it handed as unread.
func TestWalkTimeBudget(t *testing.T) {
	rs := objects()
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: rs}))
	var entries []string
	var unread []listing.Unread
	s, err := inv.Walk(context.Background(), listing.WalkRequest{Budget: listing.Budget{MaxDuration: 5 * time.Millisecond}}, func(b listing.WalkBatch) error {
		for _, e := range b.Entries {
			entries = append(entries, string(e.Key))
		}
		unread = append(unread, b.Unread...)
		if len(b.Entries) > 0 {
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	})
	if err != nil || s.Evidence.Stop != listing.StopTimeBudget || len(entries) == 0 || len(entries) >= len(rs) {
		t.Fatalf("%+v, %d entries, %v", s, len(entries), err)
	}
	if len(unread) != 1 || string(unread[0].Key) != entries[len(entries)-1] || unread[0].Class != listing.UnreadBudget {
		t.Fatalf("unread %+v after %q", unread, entries[len(entries)-1])
	}
}

// TestWalkEmitError: an error from emit stops the walk and comes back
// unchanged, whichever way it walks.
func TestWalkEmitError(t *testing.T) {
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()}))
	stop := errors.New("stop here")
	for _, req := range []listing.WalkRequest{{Workers: 1}, {Workers: 4}, {Budget: listing.Budget{MaxEntries: 5}}} {
		calls := 0
		_, err := inv.Walk(context.Background(), req, func(listing.WalkBatch) error {
			calls++
			if calls == 2 {
				return stop
			}
			return nil
		})
		if err != stop || calls != 2 {
			t.Errorf("%+v: %v after %d calls", req, err, calls)
		}
	}
}

// TestWalkScopeAndDirectories: a walk outside the scope is ErrOutOfScope;
// a local directory the inventory has as another kind, or below a
// symbolic link, is ErrNotDirectory,
// one it proves absent ErrNotFound, one at or under an entry of unknown
// kind neither; an empty one is its own entry.
func TestWalkScopeAndDirectories(t *testing.T) {
	ext := extension(reader.S3ObjectsV1)
	ext["scope"] = map[string]any{"kind": "prefix", "prefix": "a/"}
	prefixed := open(t, write(t, fixture{id: reader.S3ObjectsV1, ext: ext, rows: objects()[1:5]}))
	if _, _, err := walk(t, prefixed, listing.WalkRequest{}); !errors.Is(err, listing.ErrOutOfScope) {
		t.Errorf("walk outside the scope: %v", err)
	}
	if w, s, err := walk(t, prefixed, listing.WalkRequest{Prefix: []byte("a/c/")}); err != nil || len(w.entries) != 1 || string(s.Evidence.Scope) != "a/" {
		t.Errorf("walk inside the scope: %+v, %+v, %v", w, s, err)
	}

	files := open(t, write(t, fixture{id: reader.LocalV1, rows: tree()}))
	for prefix, want := range map[string]error{"missing/": listing.ErrNotFound, "a.txt/": listing.ErrNotDirectory, "link/x/": listing.ErrNotDirectory} {
		if _, _, err := walk(t, files, listing.WalkRequest{Prefix: []byte(prefix)}); !errors.Is(err, want) {
			t.Errorf("walk %q: %v, want %v", prefix, err, want)
		}
	}
	for _, prefix := range []string{"u/", "u/x/"} {
		if w, _, err := walk(t, files, listing.WalkRequest{Prefix: []byte(prefix)}); err != nil || len(w.entries) != 0 {
			t.Errorf("walk %q under an entry of unknown kind: %+v, %v", prefix, w, err)
		}
	}
	if w, _, err := walk(t, files, listing.WalkRequest{Prefix: []byte("empty/")}); err != nil || len(w.entries) != 1 || w.entries[0].Kind != listing.KindDir {
		t.Errorf("walk of an empty directory: %+v, %v", w, err)
	}
}

// TestWalkCancelled: a cancelled ctx ends the walk with its error.
func TestWalkCancelled(t *testing.T) {
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()}))
	for _, req := range []listing.WalkRequest{{Workers: 1}, {Workers: 4}} {
		ctx, cancel := context.WithCancel(context.Background())
		_, err := inv.Walk(ctx, req, func(listing.WalkBatch) error {
			cancel()
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%+v: %v, want the context's error", req, err)
		}
		cancel()
	}
}

// TestWalkLargeParallel: a parallel walk of a dataset of many parts hands
// every key once.
func TestWalkLargeParallel(t *testing.T) {
	rs := make([]rec, 5000)
	for i := range rs {
		rs[i] = rec{key: fmt.Sprintf("k/%06d", i), size: ptr(int64(i))}
	}
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: rs, pageRows: 64, partBytes: 16 << 10}))
	seen := map[string]int{}
	w, _, err := walk(t, inv, listing.WalkRequest{Workers: 8})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range w.entries {
		seen[string(e.Key)]++
	}
	if len(seen) != len(rs) {
		t.Fatalf("%d keys, want %d", len(seen), len(rs))
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("%q handed %d times", k, n)
		}
	}
}
