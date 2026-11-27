// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// TestSummarizeSplitMatchesTheRecords summarizes every prefix as a
// subtree and every key as an exact target, overlapping and repeated, with
// each split, and checks each target's groups against the records alone,
// and that they recombine to its totals.
func TestSummarizeSplitMatchesTheRecords(t *testing.T) {
	splits := []listing.Split{
		{},
		{StorageClass: true},
		{SizeBounds: []int64{10, 100}},
		{StorageClass: true, SizeBounds: []int64{1, 10, 100, 1000}},
	}
	for _, c := range []struct {
		id    reader.SchemaID
		rows  []rec
		kinds []listing.Kind
	}{
		{reader.S3ObjectsV1, objects(), nil},
		{reader.AzureObjectsV1, objects(), nil},
		{reader.VercelObjectsV1, vercelObjects(), nil},
		{reader.LocalV1, tree(), nil},
		{reader.LocalV1, tree(), []listing.Kind{listing.KindFile, listing.KindDir}},
		{reader.LocalV1, namesOnly(tree()), nil},
	} {
		isLocal := c.id == reader.LocalV1
		inv := open(t, write(t, fixture{id: c.id, rows: c.rows}))
		var targets []listing.Target
		for _, p := range scopes(c.rows, isLocal) {
			targets = append(targets, listing.Target{Key: []byte(p), Subtree: true})
		}
		for _, r := range c.rows {
			targets = append(targets, listing.Target{Key: []byte(r.key)})
		}
		targets = append(targets, targets[0], targets[len(targets)-1], listing.Target{Key: []byte("missing/"), Subtree: true})

		for _, split := range splits {
			opts := listing.SummaryOptions{Kinds: c.kinds, Split: split}
			sums, _, err := inv.Summarize(context.Background(), targets, opts)
			if err != nil || len(sums) != len(targets) {
				t.Fatalf("%s %+v: %d summaries, %v", c.id, split, len(sums), err)
			}
			for i, target := range targets {
				key := string(target.Key)
				keep := func(r rec) bool { return r.key == key }
				if target.Subtree {
					keep = func(r rec) bool { return strings.HasPrefix(r.key, key) }
				}
				want := groupsOf(c.rows, isLocal, c.kinds, keep, split)
				got := sums[i]
				if !sameGroups(got.Groups, want) {
					t.Fatalf("%s %+v: %q subtree %t: groups %s, want %s", c.id, split, key, target.Subtree, showGroups(got.Groups), showGroups(want))
				}
				divided := split.StorageClass || len(split.SizeBounds) > 0
				if divided && !sameTotals(got.Totals, recombine(got.Groups)) {
					t.Fatalf("%s %+v: %q: totals %s, groups %s", c.id, split, key, showTotals(got.Totals), showGroups(got.Groups))
				}
				if all := totalsOf(c.rows, isLocal, c.kinds, keep); !sameTotals(got.Totals, all) {
					t.Fatalf("%s %+v: %q: totals %s, want %s", c.id, split, key, showTotals(got.Totals), showTotals(all))
				}
			}
		}
	}
}

// TestSummarizeSplitBands: a size on a bound is in the band above it, an
// entry without a size is in band -1, an exact target has its entry's
// group, and groups whose sums are exact recombine to a sum that
// overflows, which is nil.
func TestSummarizeSplitBands(t *testing.T) {
	rs := []rec{
		{key: "a/1", size: ptr[int64](9), modified: ptr(base), class: ptr("STANDARD")},
		{key: "a/2", size: ptr[int64](10), modified: ptr(base.Add(time.Hour)), class: ptr("STANDARD")},
		{key: "a/3", modified: ptr(base), class: ptr("GLACIER")},
		{key: "a/4", size: ptr[int64](math.MaxInt64), modified: ptr(base)},
		{key: "a/5", size: ptr[int64](100), class: ptr("STANDARD")},
		{key: "b/1", size: ptr[int64](math.MaxInt64), modified: ptr(base)},
		{key: "b/2", size: ptr[int64](1), modified: ptr(base.Add(time.Hour)), class: ptr("STANDARD")},
	}
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: rs}))
	targets := []listing.Target{{Key: []byte("a/"), Subtree: true}, {Key: []byte("b/"), Subtree: true}, {Key: []byte("a/3")}}
	split := listing.Split{StorageClass: true, SizeBounds: []int64{10, 100}}
	sums, _, err := inv.Summarize(context.Background(), targets, listing.SummaryOptions{Split: split})
	if err != nil {
		t.Fatal(err)
	}

	type want struct {
		class   string
		band    int
		entries int64
		bytes   string
		dated   bool
	}
	check := func(name string, got []listing.Group, wants []want) {
		t.Helper()
		if len(got) != len(wants) {
			t.Fatalf("%s: groups %s", name, showGroups(got))
		}
		for i, w := range wants {
			g := got[i]
			class := ""
			if g.StorageClass != nil {
				class = *g.StorageClass
			}
			if class != w.class || (g.StorageClass != nil) != (w.class != "") || g.SizeBand != w.band || g.Entries != w.entries || show(g.Bytes) != w.bytes || (g.Newest != nil) != w.dated || g.AllocatedBytes != nil {
				t.Errorf("%s: group %d: %s, want %+v", name, i, showGroups(got[i:i+1]), w)
			}
		}
	}
	check("a/", sums[0].Groups, []want{
		{"", 2, 1, fmt.Sprint(int64(math.MaxInt64)), true},
		{"GLACIER", -1, 1, "nil", true},
		{"STANDARD", 0, 1, "9", true},
		{"STANDARD", 1, 1, "10", true},
		{"STANDARD", 2, 1, "100", false},
	})
	if s := sums[0]; s.Entries != 5 || s.Bytes != nil || s.Newest != nil || s.AllocatedBytes != nil {
		t.Errorf("a/: totals %s", showTotals(s.Totals))
	}
	check("b/", sums[1].Groups, []want{
		{"", 2, 1, fmt.Sprint(int64(math.MaxInt64)), true},
		{"STANDARD", 0, 1, "1", true},
	})
	if s := sums[1]; s.Entries != 2 || s.Bytes != nil || s.Newest == nil || !s.Newest.Equal(base.Add(time.Hour)) {
		t.Errorf("b/: totals %s", showTotals(s.Totals))
	}
	check("a/3", sums[2].Groups, []want{{"GLACIER", -1, 1, "nil", true}})
}

// TestSummarizeSplitLocalClass: a local inventory has no storage class,
// so a split by class puts every entry in a group with none.
func TestSummarizeSplitLocalClass(t *testing.T) {
	inv := open(t, write(t, fixture{id: reader.LocalV1, rows: tree()}))
	targets := []listing.Target{{Subtree: true}, {Key: []byte("a.txt")}}
	sums, _, err := inv.Summarize(context.Background(), targets, listing.SummaryOptions{Split: listing.Split{StorageClass: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sums) != 2 || sums[0].Entries != 5 || sums[1].Entries != 1 {
		t.Fatalf("summaries %d, want the root's 5 entries and a.txt's 1: %+v", len(sums), sums)
	}
	for i, s := range sums {
		if len(s.Groups) != 1 || s.Groups[0].StorageClass != nil || s.Groups[0].SizeBand != 0 || !sameTotals(s.Groups[0].Totals, s.Totals) {
			t.Errorf("target %d: %s, totals %s", i, showGroups(s.Groups), showTotals(s.Totals))
		}
	}
}

// TestSummarizeSplitInvalidBounds: bounds that are not positive and
// strictly ascending are refused before anything is read, even with a
// context that has ended.
func TestSummarizeSplitInvalidBounds(t *testing.T) {
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, bounds := range [][]int64{{0}, {-1, 10}, {10, 10}, {100, 10}} {
		opts := listing.SummaryOptions{Split: listing.Split{SizeBounds: bounds}}
		if _, _, err := inv.Summarize(ctx, []listing.Target{{Subtree: true}}, opts); !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("bounds %v: %v, want ErrInvalidRequest", bounds, err)
		}
	}
}

// wantKey is a group's storage class ("" with none) and size band.
type wantKey struct {
	class   string
	classed bool
	band    int
}

// groupsOf groups the records counted by keep as the contract says,
// independently of the view: every group that holds a record, in the
// contract's order, and nil for the zero split.
func groupsOf(rs []rec, isLocal bool, kinds []listing.Kind, keep func(rec) bool, split listing.Split) []listing.Group {
	if !split.StorageClass && len(split.SizeBounds) == 0 {
		return nil
	}
	if len(kinds) == 0 {
		kinds = []listing.Kind{listing.KindFile, listing.KindObject}
	}
	keyOf := func(r rec) wantKey {
		var k wantKey
		if split.StorageClass && !isLocal && r.class != nil {
			k.class, k.classed = *r.class, true
		}
		switch {
		case len(split.SizeBounds) == 0:
		case r.size == nil:
			k.band = -1
		default:
			for _, b := range split.SizeBounds {
				if *r.size >= b {
					k.band++
				}
			}
		}
		return k
	}

	var keys []wantKey
	for _, r := range rs {
		if keep(r) && slices.Contains(kinds, kindOf(r, isLocal)) && !slices.Contains(keys, keyOf(r)) {
			keys = append(keys, keyOf(r))
		}
	}
	slices.SortFunc(keys, func(a, b wantKey) int {
		switch {
		case !a.classed && b.classed:
			return -1
		case a.classed && !b.classed:
			return 1
		}
		return cmp.Or(strings.Compare(a.class, b.class), cmp.Compare(a.band, b.band))
	})

	var out []listing.Group
	for _, k := range keys {
		g := listing.Group{SizeBand: k.band, Totals: totalsOf(rs, isLocal, kinds, func(r rec) bool { return keep(r) && keyOf(r) == k })}
		if k.classed {
			g.StorageClass = ptr(k.class)
		}
		out = append(out, g)
	}
	return out
}

// recombine is the totals of groups by the contract's rules: entries add,
// a sum is nil when a group's is or it overflows, and the newest is the
// latest, nil when a group's is.
func recombine(groups []listing.Group) listing.Totals {
	t := listing.Totals{Bytes: new(int64), AllocatedBytes: new(int64)}
	add := func(sum **int64, v *int64) {
		if *sum == nil || v == nil || **sum > math.MaxInt64-*v {
			*sum = nil
			return
		}
		**sum += *v
	}
	for i, g := range groups {
		t.Entries += g.Entries
		add(&t.Bytes, g.Bytes)
		add(&t.AllocatedBytes, g.AllocatedBytes)
		switch {
		case i == 0 || t.Newest != nil && g.Newest != nil && g.Newest.After(*t.Newest):
			t.Newest = g.Newest
		case g.Newest == nil:
			t.Newest = nil
		}
	}
	return t
}

func sameGroups(a, b []listing.Group) bool {
	return slices.EqualFunc(a, b, func(x, y listing.Group) bool {
		sameClass := x.StorageClass == nil && y.StorageClass == nil || x.StorageClass != nil && y.StorageClass != nil && *x.StorageClass == *y.StorageClass
		return sameClass && x.SizeBand == y.SizeBand && sameTotals(x.Totals, y.Totals)
	}) && (a == nil) == (b == nil)
}

func showGroups(groups []listing.Group) string {
	if groups == nil {
		return "nil"
	}
	var b strings.Builder
	for _, g := range groups {
		class := "none"
		if g.StorageClass != nil {
			class = *g.StorageClass
		}
		fmt.Fprintf(&b, "[%s band %d %s]", class, g.SizeBand, showTotals(g.Totals))
	}
	return b.String()
}

func show(p *int64) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprint(*p)
}
