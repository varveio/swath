// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// TestSummarizeMatchesTheRecords summarizes every prefix as a subtree and
// every key as an exact target, overlapping, in one call per kind
// selection, and checks each against the records alone: what is at the
// key, and the counted entries' totals.
func TestSummarizeMatchesTheRecords(t *testing.T) {
	all := []listing.Kind(nil)
	for _, c := range []struct {
		id    reader.SchemaID
		rows  []rec
		kinds [][]listing.Kind
	}{
		{reader.S3ObjectsV1, objects(), [][]listing.Kind{all, {listing.KindObject}, {listing.KindFile}}},
		{reader.GCSObjectsV1, objects(), [][]listing.Kind{all}},
		{reader.VercelObjectsV1, vercelObjects(), [][]listing.Kind{all}},
		{reader.AzureObjectsV1, objects(), [][]listing.Kind{all}},
		{reader.LocalV1, tree(), [][]listing.Kind{all, {listing.KindFile}, {listing.KindDir, listing.KindSymlink}, {listing.KindOther}, {listing.KindUnknown}, {listing.KindPrefix}}},
		{reader.LocalV1, namesOnly(tree()), [][]listing.Kind{all, {listing.KindFile}}},
	} {
		isLocal := c.id == reader.LocalV1
		inv := open(t, write(t, fixture{id: c.id, rows: c.rows}))
		byKey := map[string]rec{}
		var targets []listing.Target
		for _, p := range scopes(c.rows, isLocal) {
			targets = append(targets, listing.Target{Key: []byte(p), Subtree: true})
		}
		for _, r := range c.rows {
			byKey[r.key] = r
			targets = append(targets, listing.Target{Key: []byte(r.key)})
		}
		targets = append(targets, listing.Target{Key: []byte("missing")}, listing.Target{Key: []byte("d")}, listing.Target{Key: []byte("missing/"), Subtree: true})

		for _, kinds := range c.kinds {
			sums, e, err := inv.Summarize(context.Background(), targets, listing.SummaryOptions{Kinds: kinds})
			if err != nil || len(sums) != len(targets) {
				t.Fatalf("%s %v: %d summaries, %+v, %v", c.id, kinds, len(sums), e, err)
			}
			for i, target := range targets {
				s, key := sums[i], string(target.Key)
				name := key
				if isLocal && !strings.HasSuffix(key, "/") {
					if _, ok := byKey[key+"/"]; ok {
						name = key + "/"
					}
				}
				r, present := byKey[name]
				switch {
				case key == "":
					if s.State != listing.LookupUnknown || !errors.Is(s.Reason, errRoot) || s.Self != nil {
						t.Fatalf("%s: root %v %v", c.id, s.State, s.Reason)
					}
				case present:
					if s.State != listing.LookupPresent || s.Self == nil || !sameEntry(*s.Self, r, isLocal) {
						t.Fatalf("%s: %q = %v %+v, want %+v", c.id, key, s.State, s.Self, r)
					}
				default:
					if s.State != listing.LookupAbsent || s.Self != nil {
						t.Fatalf("%s: %q = %v %+v, want absent", c.id, key, s.State, s.Self)
					}
				}
				keep := func(r rec) bool { return r.key == name }
				if target.Subtree {
					keep = func(r rec) bool { return strings.HasPrefix(r.key, key) }
				}
				if want := totalsOf(c.rows, isLocal, kinds, keep); !sameTotals(s.Totals, want) || !s.Complete {
					t.Fatalf("%s %v: %q subtree %t = %s, complete %t; want %s", c.id, kinds, key, target.Subtree, showTotals(s.Totals), s.Complete, showTotals(want))
				}
			}
		}
	}
}

// TestSummarizeUnknownKind: an entry of unknown kind is counted only when
// KindUnknown is selected, never as a file or another kind, and nothing
// at or under it is absent.
func TestSummarizeUnknownKind(t *testing.T) {
	inv := open(t, write(t, fixture{id: reader.LocalV1, rows: tree()}))
	targets := []listing.Target{
		{Subtree: true}, {Key: []byte("u")}, {Key: []byte("u/"), Subtree: true}, {Key: []byte("u/x")}, {Key: []byte("fifo/x")},
	}
	for _, c := range []struct {
		kinds       []listing.Kind
		root, entry int64
	}{
		{nil, 5, 0},
		{[]listing.Kind{listing.KindFile}, 5, 0},
		{[]listing.Kind{listing.KindOther}, 1, 0},
		{[]listing.Kind{listing.KindUnknown}, 1, 1},
	} {
		sums, _, err := inv.Summarize(context.Background(), targets, listing.SummaryOptions{Kinds: c.kinds})
		if err != nil {
			t.Fatal(err)
		}
		if sums[0].Entries != c.root || sums[1].Entries != c.entry || sums[1].State != listing.LookupPresent || sums[1].Self.Kind != listing.KindUnknown {
			t.Errorf("%v: the root counts %d, u %d (%v %+v); want %d and %d", c.kinds, sums[0].Entries, sums[1].Entries, sums[1].State, sums[1].Self, c.root, c.entry)
		}
		for _, s := range sums[2:4] {
			if s.State != listing.LookupUnknown || !errors.Is(s.Reason, errUnresolvedKind) || s.Entries != 0 {
				t.Errorf("%v: under u: %v %v %d", c.kinds, s.State, s.Reason, s.Entries)
			}
		}
		if s := sums[4]; s.State != listing.LookupAbsent || s.Reason != nil {
			t.Errorf("%v: under a FIFO: %v %v", c.kinds, s.State, s.Reason)
		}
	}
}

// TestSummarizeNullsAndOverflow: a counted entry without a size or a time,
// and a sum that overflows, make that total nil, never a partial sum.
func TestSummarizeNullsAndOverflow(t *testing.T) {
	rs := []rec{
		{key: "big/1", size: ptr[int64](math.MaxInt64), modified: ptr(base)},
		{key: "big/2", size: ptr[int64](1), modified: ptr(base.Add(time.Hour))},
		{key: "n/1", size: ptr[int64](4), modified: ptr(base)},
		{key: "n/2", modified: ptr(base)},
		{key: "t/1", size: ptr[int64](4)},
	}
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: rs}))
	targets := []listing.Target{{Key: []byte("big/"), Subtree: true}, {Key: []byte("n/"), Subtree: true}, {Key: []byte("t/"), Subtree: true}, {Key: []byte("n/2")}}
	sums, _, err := inv.Summarize(context.Background(), targets, listing.SummaryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if s := sums[0]; s.Entries != 2 || s.Bytes != nil || s.Newest == nil || !s.Newest.Equal(base.Add(time.Hour)) || s.AllocatedBytes != nil {
		t.Errorf("overflow: %s", showTotals(s.Totals))
	}
	if s := sums[1]; s.Entries != 2 || s.Bytes != nil || s.Newest == nil {
		t.Errorf("a missing size: %s", showTotals(s.Totals))
	}
	if s := sums[2]; s.Entries != 1 || s.Bytes == nil || *s.Bytes != 4 || s.Newest != nil {
		t.Errorf("a missing time: %s", showTotals(s.Totals))
	}
	if s := sums[3]; s.Entries != 1 || s.Bytes != nil || s.Newest == nil {
		t.Errorf("an exact target without a size: %s", showTotals(s.Totals))
	}
}

// TestSummarizeScopeAndCoverage: a target outside the scope is unknown
// and counts nothing; a key absent from a listing that may lack entries
// is unknown, while what it holds is still counted, and no target of such
// a listing is complete.
func TestSummarizeScopeAndCoverage(t *testing.T) {
	ctx := context.Background()
	ext := extension(reader.S3ObjectsV1)
	ext["scope"] = map[string]any{"kind": "prefix", "prefix": "a/"}
	rs := objects()[1:5]
	prefixed := open(t, write(t, fixture{id: reader.S3ObjectsV1, ext: ext, rows: rs}))
	sums, e, err := prefixed.Summarize(ctx, []listing.Target{{Subtree: true}, {Key: []byte("a"), Subtree: true}, {Key: []byte("z")}, {Key: []byte("a/"), Subtree: true}}, listing.SummaryOptions{})
	if err != nil || string(e.Scope) != "a/" {
		t.Fatalf("%+v, %v", e, err)
	}
	for i := range 3 {
		if s := sums[i]; s.State != listing.LookupUnknown || !errors.Is(s.Reason, listing.ErrOutOfScope) || s.Entries != 0 || s.Complete {
			t.Errorf("target %d outside the scope: %+v", i, s)
		}
	}
	if s := sums[3]; s.State != listing.LookupPresent || s.Entries != 4 || *s.Bytes != 115 || !s.Complete {
		t.Errorf("the scope: %+v %s", s, showTotals(s.Totals))
	}

	partial := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects(), ext: coverage(extension(reader.S3ObjectsV1), map[string]any{"complete": false, "unreadable": 5})}))
	sums, e, err = partial.Summarize(ctx, []listing.Target{{Key: []byte("missing")}, {Key: []byte("a/"), Subtree: true}}, listing.SummaryOptions{})
	if err != nil || *e.Unlocated != 5 || sums[0].State != listing.LookupUnknown || sums[0].Reason != nil || sums[0].Complete || sums[1].Complete || sums[1].Entries != 4 {
		t.Fatalf("partial: %+v, %+v, %v", sums, e, err)
	}
}

// TestSummarizeCancelled: a cancelled ctx ends the call with its error.
func TestSummarizeCancelled(t *testing.T) {
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := inv.Summarize(ctx, []listing.Target{{Subtree: true}}, listing.SummaryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Summarize = %v", err)
	}
	if sums, e, err := inv.Summarize(context.Background(), nil, listing.SummaryOptions{}); err != nil || len(sums) != 0 {
		t.Fatalf("no targets: %v, %+v, %v", sums, e, err)
	}
}
