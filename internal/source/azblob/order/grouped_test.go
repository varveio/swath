// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/keyrange"
)

// allVersions is the include of an --all-versions listing.
var allVersions = client.Include{Versions: true, Snapshots: true, Deleted: true}

// entries gives each key 1 to 4 entries in Azure's order within a name
// (snapshots, then noncurrent or deleted versions, then the base blob or
// current version, which may itself be deleted), each a distinct Size, so
// a listing's entries are identified by it.
func entries(r *rand.Rand, keys []string) []client.Item {
	var out []client.Item
	for _, k := range keys {
		n := 1 + r.IntN(4)
		for i := range n {
			it := client.Item{Name: k, Size: int64(len(out))}
			switch last := i == n-1; {
			case !last && r.IntN(2) == 0:
				it.Snapshot = fmt.Sprintf("2026-01-01T00:00:%02d.0000000Z", i)
				it.Deleted = r.IntN(4) == 0
			case !last:
				it.VersionID = fmt.Sprintf("2026-01-02T00:00:%02d.0000000Z", i)
				it.Deleted = r.IntN(4) == 0
			default:
				it.Deleted = r.IntN(5) == 0
			}
			out = append(out, it)
		}
	}
	return out
}

// groupedOracle is the scope of q over the entries a listing with inc
// lists: a name's every entry in Azure's order within it, names in byte
// order; a delimiter's roll-ups once (over the view without snapshots,
// which a delimiter query leaves out).
func groupedOracle(all []client.Item, q keyrange.Query, inc client.Include) []client.Item {
	inc = include(inc, &q)
	var out []client.Item
	for _, it := range all {
		if !azblobtest.Listed(&it, inc) || !strings.HasPrefix(it.Name, q.Prefix) {
			continue
		}
		if q.Delimiter != "" {
			if at := strings.Index(it.Name[len(q.Prefix):], q.Delimiter); at >= 0 {
				it = client.Item{Name: it.Name[:len(q.Prefix)+at+len(q.Delimiter)], Prefix: true}
			}
		}
		if q.After < it.Name && (!q.Bounded || it.Name <= q.Through) {
			out = append(out, it)
		}
	}
	slices.SortStableFunc(out, func(a, b client.Item) int { return strings.Compare(a.Name, b.Name) })
	return slices.CompactFunc(out, func(a, b client.Item) bool { return a.Prefix && b.Prefix && a.Name == b.Name })
}

func identities(items []client.Item) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = fmt.Sprintf("%q#%d", it.Name, it.Size)
		if it.Prefix {
			out[i] = fmt.Sprintf("%q/", it.Name)
		}
	}
	return out
}

// TestGroupedProperty is TestProperty over a grouped listing: random names
// over the alphabet (names in U+E000..U+FFFF beside names above U+FFFF, so
// spans split), each with 1 to 4 entries, random bounds, prefix, delimiter
// and Max (1 and 2 put page seams inside a name's entries, at After and at
// Through included), with short, empty and failed responses: the pages
// concatenate to every entry in scope, a name's entries consecutive and in
// Azure's order, After's entries never listed, Through's always all.
func TestGroupedProperty(t *testing.T) {
	n := 20000
	if testing.Short() {
		n = 2000
	}
	set := counters.New(CounterDefs)
	failed := 0
	for seed := range uint64(n) {
		c := newCase(seed)
		all := entries(rand.New(rand.NewPCG(seed, 0x9e0)), c.keys)
		st := azblobtest.NewEntryStore(all)
		draw := func(n, hook int) *rand.Rand { return rand.New(rand.NewPCG(c.seed, uint64(3*n+hook))) }
		st.Short = func(n int) int {
			if r := draw(n, 0); r.IntN(4) < c.shortRate {
				return 1 + r.IntN(3)
			}
			return 0
		}
		st.Empty = func(n int) bool { return draw(n, 1).IntN(10) < c.emptyRate }
		st.Fail = func(n int) error {
			if draw(n, 2).IntN(20) < c.failRate {
				return azblobtest.ErrInjected
			}
			return nil
		}
		a := New(st, "c", set, 0)
		a.Include = allVersions
		items, err := flat(walk(a, c.q, 1<<20, injected))
		msg := ""
		switch got, want := identities(items), identities(groupedOracle(all, c.q, allVersions)); {
		case err != nil:
			msg = err.Error()
		case !slices.Equal(got, want):
			msg = fmt.Sprintf("listed %v\nwant   %v", got, want)
		case a.Len() != 0:
			msg = fmt.Sprintf("%d chains registered after the last page", a.Len())
		}
		if msg == "" {
			continue
		}
		t.Errorf("case %d: %s\nentries %v\nquery prefix=%q delimiter=%q after=%q through=%q bounded=%v max=%d\n%s",
			seed, msg, identities(all), c.q.Prefix, c.q.Delimiter, c.q.After, c.q.Through, c.q.Bounded, c.q.Max, st)
		if failed++; failed == 5 {
			t.FailNow()
		}
	}
	for _, i := range []int{CSpans, CSplits, CSeeks, COverlapRows, CEmptyPages} {
		if set.Load(counters.ID(i)) == 0 {
			t.Errorf("no case engaged %s", CounterDefs[i].Key)
		}
	}
}

// TestGroupedThrough: a range ending at a name whose entries cross a page
// seam lists them all (the Through shortcut of an ungrouped listing would
// end the span at the first), and one starting after a name lists none of
// its entries, however many pages they fill.
func TestGroupedThrough(t *testing.T) {
	var all []client.Item
	for i, name := range []string{"a", "b", "b", "b", "b", "b", "c"} {
		it := client.Item{Name: name, Size: int64(i)}
		if name == "b" && i < 5 {
			it.VersionID = fmt.Sprintf("v%d", i)
		}
		all = append(all, it)
	}
	for _, q := range []keyrange.Query{
		{Through: "b", Bounded: true, Max: 2},
		{After: "a", Through: "b", Bounded: true, Max: 1},
		{After: "b", Max: 1},
		{After: "a", Max: 3},
	} {
		st := azblobtest.NewEntryStore(all)
		a := New(st, "c", nil, 0)
		a.Include = allVersions
		items, err := flat(walk(a, q, 0, injected))
		if got, want := identities(items), identities(groupedOracle(all, q, allVersions)); err != nil || !slices.Equal(got, want) {
			t.Errorf("after=%q through=%q max=%d: %v, %v; want %v\n%s", q.After, q.Through, q.Max, got, err, want, st)
		}
	}
	// Versions alone group the listing too.
	a := New(azblobtest.NewEntryStore(all[:4]), "c", nil, 0)
	a.Include = client.Include{Versions: true}
	if _, err := flat(walk(a, keyrange.Query{Max: 10}, 0, injected)); err != nil {
		t.Fatalf("grouped by versions: %v", err)
	}
}

// TestProbeInclude: a probe leaves out what only rows carry, a delimiter
// query leaves out snapshots (List Blobs documents the pair as a 400), and
// a data query sends the listing's whole include; every page of a chain
// sends the same.
func TestProbeInclude(t *testing.T) {
	inc := client.Include{Versions: true, Snapshots: true, Deleted: true, Metadata: true, Tags: true, Immutability: true}
	for _, c := range []struct {
		q    keyrange.Query
		want client.Include
	}{
		{keyrange.Query{Max: 1}, inc},
		{keyrange.Query{Max: 1, Probe: true}, client.Include{Versions: true, Snapshots: true, Deleted: true}},
		{keyrange.Query{Max: 1, Probe: true, Delimiter: "/"}, client.Include{Versions: true, Deleted: true}},
	} {
		st := azblobtest.NewEntryStore([]client.Item{{Name: "a"}, {Name: "b"}, {Name: "c"}})
		a := New(st, "c", nil, 0)
		a.Include = inc
		if _, err := walk(a, c.q, 0, injected); err != nil {
			t.Fatal(err)
		}
		for _, r := range st.Log() {
			if r.In.Include != c.want {
				t.Errorf("probe=%v delimiter=%q: sent %+v, want %+v", c.q.Probe, c.q.Delimiter, r.In.Include, c.want)
			}
		}
	}
}
