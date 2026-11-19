// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package du

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/reader/listing"
)

// tree is a small random tree as it is (its directories and files, each
// file once with all its names) and the entries a listing of it gives,
// whose evidence may be degraded.
type tree struct {
	dirs    map[string]int64 // the directories fed, and their own blocks
	files   []treeFile
	entries []listing.Entry
}

type treeFile struct {
	kind              listing.Kind
	size, alloc, time int64
	names             []string
}

var (
	treeDirs     = []string{"", "a/", "a/b/", "a/b/c/", "b/", "d/"}
	treePrefixes = []string{"", "a/", "a/b/", "a/b/c/", "b/", "d/", "e/"}
)

// randomTree draws a tree of up to ten files, each with up to three names
// in random directories; degrade drops the size or time of some files and
// the allocation or identity of some names.
func randomTree(rng *rand.Rand, degrade bool) tree {
	tr := tree{dirs: map[string]int64{}}
	drop := func() bool { return degrade && rng.IntN(6) == 0 }
	for _, d := range treeDirs {
		if rng.IntN(5) == 0 {
			continue
		}
		alloc := 4096 * rng.Int64N(3)
		tr.dirs[d] = alloc
		e := listing.Entry{Key: []byte(d), Kind: listing.KindDir, AllocatedBytes: &alloc}
		if drop() {
			e.AllocatedBytes = nil
		}
		tr.entries = append(tr.entries, e)
	}

	for id := range rng.IntN(11) {
		kinds := []listing.Kind{listing.KindFile, listing.KindFile, listing.KindSymlink, listing.KindOther, listing.KindObject}
		f := treeFile{kind: kinds[rng.IntN(len(kinds))], size: rng.Int64N(10_000), alloc: 512 * rng.Int64N(20), time: rng.Int64N(1000)}
		names := 1
		if f.kind != listing.KindObject {
			names += rng.IntN(3)
		}
		links := uint32(names)
		if f.kind != listing.KindObject && rng.IntN(4) == 0 {
			links++ // a name outside the listing
		}
		dev, ino := uint64(1+rng.IntN(2)), uint64(id+1)
		// A file's names share one stat: its size and time are missing
		// on all of them or none. A name without allocation or identity
		// counts nowhere, so those drop per name.
		size, mtime := &f.size, at(f.time)
		if drop() {
			size = nil
		}
		if drop() {
			mtime = nil
		}
		for n := range names {
			key := fmt.Sprintf("%sf%d-%d", treeDirs[rng.IntN(len(treeDirs))], id, n)
			f.names = append(f.names, key)
			e := listing.Entry{Key: []byte(key), Kind: f.kind, Size: size, LastModified: mtime}
			if f.kind != listing.KindObject {
				e.AllocatedBytes, e.Device, e.Inode, e.Links = &f.alloc, &dev, &ino, &links
			}
			if drop() {
				e.AllocatedBytes = nil
			}
			if drop() {
				e.Links = nil
			}
			if drop() {
				e.Inode = nil
			}
			tr.entries = append(tr.entries, e)
		}
		tr.files = append(tr.files, f)
	}

	tr.entries = append(tr.entries,
		listing.Entry{Key: []byte("a/u"), Kind: listing.KindUnknown, Size: i64(1 << 40)},
		listing.Entry{Key: []byte("a/b/"), Kind: listing.KindPrefix, Size: i64(1 << 40)})
	return tr
}

// truth is prefix p's figures over the tree as it is: each file under it
// once, each directory's own blocks.
func (tr tree) truth(p string) Totals {
	s := Totals{Prefix: p}
	for d, alloc := range tr.dirs {
		if strings.HasPrefix(d, p) {
			s.Allocated += alloc
		}
	}
	for _, f := range tr.files {
		if !slices.ContainsFunc(f.names, func(n string) bool { return strings.HasPrefix(n, p) }) {
			continue
		}
		s.Size += f.size
		if f.kind == listing.KindObject || f.kind == listing.KindFile {
			s.Files++
			if s.Newest == nil || at(f.time).After(*s.Newest) {
				s.Newest = at(f.time)
			}
		}
		if f.kind != listing.KindObject {
			s.Allocated += f.alloc
		}
	}
	return s
}

// feed adds the entries in a random order and random slices, calling
// after with the totals after each Add.
func feed(t *testing.T, rng *rand.Rand, tl *Tally, entries []listing.Entry, after func([]Totals)) {
	t.Helper()
	entries = slices.Clone(entries)
	rng.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
	for len(entries) > 0 {
		n := 1 + rng.IntN(len(entries))
		add(t, tl, entries[:n]...)
		entries = entries[n:]
		after(tl.Result())
	}
}

// before reports whether a is at or before b, nil being before every
// time.
func before(a, b *time.Time) bool {
	return a == nil || b != nil && !a.After(*b)
}

// TestLowerBound draws random small trees (seed fixed) and feeds each
// tree's entries in random orders and slices to a tally of random nested
// and sibling prefixes. For every prefix, no figure ever decreases as Add
// takes entries; Files, Size, Allocated and Newest never exceed the
// tree's own figures; with complete evidence they equal them and no
// missing counter is set; and the totals do not depend on the order or
// the slicing.
func TestLowerBound(t *testing.T) {
	rng := rand.New(rand.NewPCG(661, 1))
	for i := range 2000 {
		degrade := i%2 == 1
		tr := randomTree(rng, degrade)
		prefixes := slices.Clone(treePrefixes)
		rng.Shuffle(len(prefixes), func(i, j int) { prefixes[i], prefixes[j] = prefixes[j], prefixes[i] })
		prefixes = prefixes[:rng.IntN(len(prefixes)+1)]

		tl := tally(t, prefixes...)
		last := tl.Result()
		feed(t, rng, tl, tr.entries, func(now []Totals) {
			for j, s := range now {
				was := last[j]
				if s.Files < was.Files || s.Symlinks < was.Symlinks || s.Size < was.Size || s.Allocated < was.Allocated || !before(was.Newest, s.Newest) {
					t.Fatalf("tree %d, prefix %q decreased:\n was %s\n now %s", i, s.Prefix, show(was), show(s))
				}
			}
			last = now
		})

		for _, s := range last {
			want := tr.truth(s.Prefix)
			if s.Files > want.Files || s.Size > want.Size || s.Allocated > want.Allocated || !before(s.Newest, want.Newest) {
				t.Fatalf("tree %d, prefix %q above the tree's figures:\n got %s\nwant %s", i, s.Prefix, show(s), show(want))
			}
			if !degrade {
				want.Symlinks = s.Symlinks
				if !equal(s, want) {
					t.Fatalf("tree %d, prefix %q with complete evidence:\n got %s\nwant %s", i, s.Prefix, show(s), show(want))
				}
			}
		}

		again := tally(t, prefixes...)
		feed(t, rng, again, tr.entries, func([]Totals) {})
		check(t, again.Result(), last...)
	}
}
