// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"errors"
	"math"
	"slices"
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
		{reader.AzureObjectsV1, objects(), [][]listing.Kind{all}},
		{reader.LocalV1, tree(), [][]listing.Kind{all, {listing.KindFile}, {listing.KindDir, listing.KindSymlink}, {listing.KindOther}, {listing.KindPrefix}}},
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
			if err != nil || len(sums) != len(targets) || e.Stop != listing.StopCompleted || e.Origin != listing.OriginInventory {
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
				if want := totalsOf(c.rows, isLocal, kinds, keep); !sameTotals(s.Totals, want) || len(s.Unread) != 0 {
					t.Fatalf("%s %v: %q subtree %t = %s, unread %v; want %s", c.id, kinds, key, target.Subtree, showTotals(s.Totals), s.Unread, showTotals(want))
				}
			}
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
// is unknown, while what it holds is still counted.
func TestSummarizeScopeAndCoverage(t *testing.T) {
	ctx := context.Background()
	ext := extension(reader.S3ObjectsV1)
	ext["scope"] = map[string]any{"kind": "prefix", "prefix": "a/"}
	rs := objects()[1:5]
	prefixed := open(t, write(t, fixture{id: reader.S3ObjectsV1, ext: ext, rows: rs}))
	sums, e, err := prefixed.Summarize(ctx, []listing.Target{{Subtree: true}, {Key: []byte("a"), Subtree: true}, {Key: []byte("z")}, {Key: []byte("a/"), Subtree: true}}, listing.SummaryOptions{})
	if err != nil || e.Stop != listing.StopCompleted || string(e.Scope) != "a/" {
		t.Fatalf("%+v, %v", e, err)
	}
	for i := range 3 {
		if s := sums[i]; s.State != listing.LookupUnknown || !errors.Is(s.Reason, listing.ErrOutOfScope) || s.Entries != 0 {
			t.Errorf("target %d outside the scope: %+v", i, s)
		}
	}
	if s := sums[3]; s.State != listing.LookupPresent || s.Entries != 4 || *s.Bytes != 115 {
		t.Errorf("the scope: %+v %s", s, showTotals(s.Totals))
	}

	partial := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects(), ext: coverage(extension(reader.S3ObjectsV1), map[string]any{"complete": false, "unreadable": 5})}))
	sums, e, err = partial.Summarize(ctx, []listing.Target{{Key: []byte("missing")}, {Key: []byte("a/"), Subtree: true}}, listing.SummaryOptions{})
	if err != nil || *e.Unlocated != 5 || sums[0].State != listing.LookupUnknown || sums[0].Reason != nil || sums[1].Entries != 4 {
		t.Fatalf("partial: %+v, %+v, %v", sums, e, err)
	}
}

// TestSummarizeBudgets: a subtree a bound keeps the call from counting
// counts nothing and is unread as a whole, and Evidence.Stop names the
// bound; an exact target is counted from its own entry whatever the
// budget.
func TestSummarizeBudgets(t *testing.T) {
	ctx := context.Background()
	rs := objects()
	dir := write(t, fixture{id: reader.S3ObjectsV1, rows: rs})
	inv := open(t, dir)
	targets := []listing.Target{{Subtree: true}, {Key: []byte("a/"), Subtree: true}, {Key: []byte("a/c/"), Subtree: true}, {Key: []byte("z")}}
	unread := func(s listing.TargetSummary) bool {
		return len(s.Unread) == 1 && s.Unread[0].Key == nil && s.Unread[0].Hi == nil && s.Unread[0].Scope == listing.UnreadInterval &&
			s.Unread[0].Class == listing.UnreadBudget && s.Entries == 0
	}

	// The entry budget admits the subtrees in order while their union
	// fits: not the root, then a/, and a/c/ inside it adds nothing.
	sums, e, err := inv.Summarize(ctx, targets, listing.SummaryOptions{Budget: listing.Budget{MaxEntries: 4}})
	if err != nil || e.Stop != listing.StopEntryBudget || !unread(sums[0]) || sums[1].Entries != 4 || sums[2].Entries != 1 || sums[3].Entries != 1 || len(sums[1].Unread) != 0 {
		t.Fatalf("entries: %+v, %+v, %v", sums, e, err)
	}
	if sums[0].State != listing.LookupUnknown || sums[1].State != listing.LookupPresent {
		t.Fatalf("entries: states %v, %v", sums[0].State, sums[1].State)
	}

	sums, e, err = inv.Summarize(ctx, targets, listing.SummaryOptions{Budget: listing.Budget{MaxValueBytes: 1}})
	if err != nil || e.Stop != listing.StopValueBudget || !unread(sums[0]) || !unread(sums[1]) || !unread(sums[2]) || sums[3].Entries != 1 || sums[3].State != listing.LookupPresent {
		t.Fatalf("value bytes: %+v, %+v, %v", sums, e, err)
	}

	sums, e, err = inv.Summarize(ctx, targets, listing.SummaryOptions{Budget: listing.Budget{MaxEntries: 4, MaxValueBytes: 1}})
	if err != nil || e.Stop != listing.StopEntryBudget || !slices.ContainsFunc(sums[:3], unread) {
		t.Fatalf("both: %+v, %+v, %v", sums, e, err)
	}

	slow := openWith(t, dir, newSlowSource(t, dir, 50*time.Millisecond))
	sums, e, err = slow.Summarize(ctx, targets, listing.SummaryOptions{Budget: listing.Budget{MaxDuration: 5 * time.Millisecond}})
	if err != nil || e.Stop != listing.StopTimeBudget || !unread(sums[0]) || !unread(sums[3]) || sums[3].State != listing.LookupUnknown {
		t.Fatalf("time: %+v, %+v, %v", sums, e, err)
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
	if sums, e, err := inv.Summarize(context.Background(), nil, listing.SummaryOptions{}); err != nil || len(sums) != 0 || e.Stop != listing.StopCompleted {
		t.Fatalf("no targets: %v, %+v, %v", sums, e, err)
	}
}

// TestRowBudget: the rows of the union admitted count once.
func TestRowBudget(t *testing.T) {
	if got := merge([][2]int64{{5, 9}, {0, 2}, {2, 4}, {8, 12}}); !slices.Equal(got, [][2]int64{{0, 4}, {5, 12}}) {
		t.Errorf("merge = %v", got)
	}
}
