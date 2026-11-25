// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"cmp"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/varveio/swath/reader/listing"
)

// optional is an int64 a row may lack.
type optional struct {
	v  int64
	ok bool
}

// item is one counted entry as a summary groups it: its storage class
// when the split asks for one, and its size, space on disk and last
// change in microseconds.
type item struct {
	class     []byte
	classed   bool
	size      optional
	allocated optional
	modified  optional
}

// itemOf is the item of an entry, its class taken only when the split
// asks for it.
func itemOf(e *listing.Entry, split listing.Split) item {
	it := item{size: optionalOf(e.Size), allocated: optionalOf(e.AllocatedBytes)}
	if e.LastModified != nil {
		it.modified = optional{v: e.LastModified.UnixMicro(), ok: true}
	}
	if split.StorageClass && e.StorageClass != nil {
		it.class, it.classed = []byte(*e.StorageClass), true
	}
	return it
}

func optionalOf(p *int64) optional {
	if p == nil {
		return optional{}
	}
	return optional{v: *p, ok: true}
}

// band is the size band of a size among bounds: the number of bounds not
// above it; -1 for no size when there are bounds, and 0 when there are
// none.
func band(size optional, bounds []int64) int {
	switch {
	case len(bounds) == 0:
		return 0
	case !size.ok:
		return -1
	}
	n, found := slices.BinarySearch(bounds, size.v)
	if found {
		n++
	}
	return n
}

// sum is a sum of byte counts, none negative: unknown once a counted
// entry lacks one or the sum overflows.
type sum struct {
	v       int64
	unknown bool
}

func (s *sum) add(x optional) {
	switch {
	case s.unknown:
	case !x.ok || s.v > math.MaxInt64-x.v:
		s.unknown = true
	default:
		s.v += x.v
	}
}

func (s *sum) merge(from sum) {
	s.add(optional{v: from.v, ok: !from.unknown})
}

func (s sum) totals() *int64 {
	if s.unknown {
		return nil
	}
	return new(s.v)
}

// groupKey names a group: its storage class, if any, and size band.
type groupKey struct {
	class   string
	classed bool
	band    int
}

// tally is the totals of one group, which holds at least one entry.
type tally struct {
	key groupKey

	entries   int64
	bytes     sum
	allocated sum
	// newest is the latest change in microseconds; undated says a counted
	// entry has none, which leaves the group's Newest nil.
	newest  int64
	undated bool
}

// is reports whether it, in band, falls in the group.
func (t *tally) is(it item, band int) bool {
	return t.key.band == band && t.key.classed == it.classed && t.key.class == string(it.class)
}

func (t *tally) add(it item) {
	t.entries++
	t.bytes.add(it.size)
	t.allocated.add(it.allocated)
	switch {
	case !it.modified.ok:
		t.undated = true
	case it.modified.v > t.newest:
		t.newest = it.modified.v
	}
}

func (t *tally) merge(from *tally) {
	t.entries += from.entries
	t.bytes.merge(from.bytes)
	t.allocated.merge(from.allocated)
	t.undated = t.undated || from.undated
	t.newest = max(t.newest, from.newest)
}

// tallies are the groups of a set's counted entries, in no order. A split
// names few groups, so they are a short list, and last is the index of
// the group the previous entry fell in, which the next one most often
// shares.
type tallies struct {
	list []tally
	last int
}

// add counts it in its group, which it starts if none has it.
func (ts *tallies) add(it item, bounds []int64) {
	b := band(it.size, bounds)
	if ts.last < len(ts.list) && ts.list[ts.last].is(it, b) {
		ts.list[ts.last].add(it)
		return
	}
	for i := range ts.list {
		if ts.list[i].is(it, b) {
			ts.last = i
			ts.list[i].add(it)
			return
		}
	}

	t := tally{key: groupKey{class: string(it.class), classed: it.classed, band: b}, newest: math.MinInt64}
	t.add(it)
	ts.list = append(ts.list, t)
	ts.last = len(ts.list) - 1
}

// merge adds from's groups to into's. It copies them, and leaves from
// unchanged: from may be merged into several results.
func merge(into, from *tallies) {
	for k := range from.list {
		f := &from.list[k]
		i := slices.IndexFunc(into.list, func(t tally) bool { return t.key == f.key })
		if i < 0 {
			into.list = append(into.list, *f)
			continue
		}
		into.list[i].merge(f)
	}
}

// divides reports whether a split asks for groups.
func divides(s listing.Split) bool {
	return s.StorageClass || len(s.SizeBounds) > 0
}

// summarized returns a set's totals, the recombination of its groups,
// and the groups when the split asks for them and there are any, in the
// contract's order.
func summarized(ts tallies, s listing.Split) (listing.Totals, []listing.Group) {
	groups := make([]listing.Group, len(ts.list))
	for i := range ts.list {
		groups[i] = group(&ts.list[i])
	}
	slices.SortFunc(groups, order)

	totals := recombined(groups)
	if !divides(s) || len(groups) == 0 {
		return totals, nil
	}
	return totals, groups
}

func group(t *tally) listing.Group {
	g := listing.Group{SizeBand: t.key.band}
	if t.key.classed {
		g.StorageClass = new(t.key.class)
	}
	g.Entries = t.entries
	g.Bytes = t.bytes.totals()
	g.AllocatedBytes = t.allocated.totals()
	if !t.undated {
		g.Newest = new(time.UnixMicro(t.newest).UTC())
	}
	return g
}

// order orders groups by storage class, none first, then by size band.
func order(a, b listing.Group) int {
	switch {
	case a.StorageClass == nil && b.StorageClass != nil:
		return -1
	case a.StorageClass != nil && b.StorageClass == nil:
		return 1
	case a.StorageClass != nil:
		if c := strings.Compare(*a.StorageClass, *b.StorageClass); c != 0 {
			return c
		}
	}
	return cmp.Compare(a.SizeBand, b.SizeBand)
}

// recombined is the totals of groups by the rules of a target's: counts
// and sums add, a sum is nil when a group's is or it overflows, and
// Newest is the latest, nil when a group's is or there is no group.
func recombined(groups []listing.Group) listing.Totals {
	var bytes, allocated sum
	var t listing.Totals
	for i, g := range groups {
		t.Entries += g.Entries
		bytes.add(optionalOf(g.Bytes))
		allocated.add(optionalOf(g.AllocatedBytes))
		switch {
		case i == 0:
			t.Newest = g.Newest
		case g.Newest == nil || t.Newest == nil:
			t.Newest = nil
		case g.Newest.After(*t.Newest):
			t.Newest = g.Newest
		}
	}
	t.Bytes = bytes.totals()
	t.AllocatedBytes = allocated.totals()
	if t.Newest != nil {
		t.Newest = new(*t.Newest)
	}
	return t
}
