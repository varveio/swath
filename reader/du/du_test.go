// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package du

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/varveio/swath/reader/listing"
)

func i64(v int64) *int64   { return &v }
func u64(v uint64) *uint64 { return &v }
func u32(v uint32) *uint32 { return &v }

func at(sec int64) *time.Time {
	v := time.Unix(sec, 0).UTC()
	return &v
}

// local is a local entry of kind k with every figure, identified with
// links names on device 1 at inode ino.
func local(k listing.Kind, key string, size, alloc int64, links uint32, ino uint64) listing.Entry {
	return listing.Entry{
		Key:            []byte(key),
		Kind:           k,
		Size:           i64(size),
		AllocatedBytes: i64(alloc),
		LastModified:   at(1000),
		Device:         u64(1),
		Inode:          u64(ino),
		Links:          u32(links),
	}
}

// file is a regular file of 4,000 bytes on 4,096 allocated, with links
// names, at inode ino.
func file(key string, links uint32, ino uint64) listing.Entry {
	return local(listing.KindFile, key, 4000, 4096, links, ino)
}

func dir(key string, alloc int64) listing.Entry {
	return listing.Entry{Key: []byte(key), Kind: listing.KindDir, AllocatedBytes: i64(alloc)}
}

func object(key string, size, sec int64) listing.Entry {
	return listing.Entry{Key: []byte(key), Kind: listing.KindObject, Size: i64(size), LastModified: at(sec)}
}

// counted is the totals of prefix p holding n counted files of file's
// figures.
func counted(p string, n int64) Totals {
	return Totals{Prefix: p, Files: n, Size: 4000 * n, Allocated: 4096 * n, Newest: at(1000)}
}

func tally(t *testing.T, prefixes ...string) *Tally {
	t.Helper()
	tl, err := New(Options{Prefixes: prefixes})
	if err != nil {
		t.Fatal(err)
	}
	return tl
}

func add(t *testing.T, tl *Tally, entries ...listing.Entry) {
	t.Helper()
	if err := tl.Add(entries); err != nil {
		t.Fatal(err)
	}
}

// equal compares two Totals, their Newest by value.
func equal(a, b Totals) bool {
	an, bn := a.Newest, b.Newest
	a.Newest, b.Newest = nil, nil
	return a == b && (an == nil) == (bn == nil) && (an == nil || an.Equal(*bn))
}

func show(s Totals) string {
	newest := "nil"
	if s.Newest != nil {
		newest = s.Newest.Format(time.RFC3339)
	}
	s.Newest = nil
	return fmt.Sprintf("%+v newest %s", s, newest)
}

// check fails unless got is want, prefix by prefix.
func check(t *testing.T, got []Totals, want ...Totals) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d totals, want %d", len(got), len(want))
	}
	for i := range want {
		if !equal(got[i], want[i]) {
			t.Errorf("prefix %q:\n got %s\nwant %s", want[i].Prefix, show(got[i]), show(want[i]))
		}
	}
}

// TestKinds: what one entry of each kind, with each figure or identity
// missing, adds to the prefix that holds it.
func TestKinds(t *testing.T) {
	without := func(e listing.Entry, drop func(*listing.Entry)) listing.Entry {
		drop(&e)
		return e
	}
	f := file("a/x", 1, 1)
	link := local(listing.KindSymlink, "a/x", 7, 4096, 1, 1)
	other := local(listing.KindOther, "a/x", 0, 512, 1, 1)
	for _, c := range []struct {
		name string
		e    listing.Entry
		want Totals
	}{
		{"file", f, counted("a/", 1)},
		{"file without a size", without(f, func(e *listing.Entry) { e.Size = nil }),
			Totals{Files: 1, Allocated: 4096, Newest: at(1000), SizeMissing: 1}},
		{"file without a time", without(f, func(e *listing.Entry) { e.LastModified = nil }),
			Totals{Files: 1, Size: 4000, Allocated: 4096, NewestMissing: 1}},
		{"file without allocation", without(f, func(e *listing.Entry) { e.AllocatedBytes = nil }),
			Totals{AllocatedMissing: 1}},
		{"file without links", without(f, func(e *listing.Entry) { e.Links = nil }),
			Totals{Unidentified: 1}},
		{"file with no link", without(f, func(e *listing.Entry) { e.Links = u32(0) }),
			Totals{Unidentified: 1}},
		{"linked file without an inode", without(file("a/x", 2, 1), func(e *listing.Entry) { e.Inode = nil }),
			Totals{Unidentified: 1}},
		{"linked file without a device", without(file("a/x", 2, 1), func(e *listing.Entry) { e.Device = nil }),
			Totals{Unidentified: 1}},
		{"file without allocation or links", without(f, func(e *listing.Entry) { e.AllocatedBytes, e.Links = nil, nil }),
			Totals{AllocatedMissing: 1}},
		{"symlink", link, Totals{Symlinks: 1, Size: 7, Allocated: 4096}},
		{"symlink without a size", without(link, func(e *listing.Entry) { e.Size = nil }),
			Totals{Symlinks: 1, Allocated: 4096}},
		{"symlink without allocation", without(link, func(e *listing.Entry) { e.AllocatedBytes = nil }),
			Totals{Symlinks: 1, AllocatedMissing: 1}},
		{"symlink without links", without(link, func(e *listing.Entry) { e.Links = nil }),
			Totals{Symlinks: 1, Unidentified: 1}},
		{"other", other, Totals{Allocated: 512}},
		{"other without allocation", without(other, func(e *listing.Entry) { e.AllocatedBytes = nil }),
			Totals{AllocatedMissing: 1}},
		{"object", without(object("a/x", 10, 2000), func(e *listing.Entry) { e.AllocatedBytes = i64(99) }),
			Totals{Files: 1, Size: 10, Newest: at(2000)}},
		{"object without a size or a time", listing.Entry{Key: []byte("a/x"), Kind: listing.KindObject},
			Totals{Files: 1, SizeMissing: 1, NewestMissing: 1}},
		{"directory", dir("a/d/", 4096), Totals{Allocated: 4096}},
		{"directory without allocation", listing.Entry{Key: []byte("a/d/"), Kind: listing.KindDir},
			Totals{AllocatedMissing: 1}},
		{"prefix", listing.Entry{Key: []byte("a/p/"), Kind: listing.KindPrefix, Size: i64(5)}, Totals{}},
		{"unknown", without(f, func(e *listing.Entry) { e.Kind = listing.KindUnknown }), Totals{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tl := tally(t, "a/", "b/")
			add(t, tl, c.e)
			c.want.Prefix = "a/"
			check(t, tl.Result(), c.want, Totals{Prefix: "b/"})
		})
	}
}

// TestInvalidEntries: an entry of no kind, or with a negative figure,
// fails Add whatever prefix holds it; the slice is not taken, and every
// later Add fails with the same error.
func TestInvalidEntries(t *testing.T) {
	for _, c := range []struct {
		name string
		e    listing.Entry
	}{
		{"zero kind", listing.Entry{Key: []byte("a/x")}},
		{"kind past the last", listing.Entry{Key: []byte("a/x"), Kind: listing.KindUnknown + 1}},
		{"outside every prefix", listing.Entry{Key: []byte("z/x"), Kind: 200}},
		{"negative size", object("a/x", -1, 0)},
		{"negative allocation", dir("a/d/", -512)},
	} {
		t.Run(c.name, func(t *testing.T) {
			tl := tally(t, "a/")
			add(t, tl, file("a/f", 1, 1))
			err := tl.Add([]listing.Entry{file("a/g", 1, 2), c.e})
			if !errors.Is(err, listing.ErrInvalidRequest) {
				t.Fatalf("Add: %v, want listing.ErrInvalidRequest", err)
			}
			if again := tl.Add([]listing.Entry{file("a/h", 1, 3)}); again != err {
				t.Errorf("a later Add: %v, want %v", again, err)
			}
			check(t, tl.Result(), counted("a/", 1))
		})
	}
}

// TestHardLinks: a file with several names counts once within each
// requested prefix that holds any of them, and nowhere else (the worked
// example).
func TestHardLinks(t *testing.T) {
	for _, c := range []struct {
		name     string
		prefixes []string
		names    []string
		want     []Totals
	}{
		{"siblings", []string{"a/", "b/"}, []string{"a/x", "b/y"},
			[]Totals{counted("a/", 1), counted("b/", 1)}},
		{"parent and child", []string{"p/", "p/c/"}, []string{"p/a", "p/c/z"},
			[]Totals{counted("p/", 1), counted("p/c/", 1)}},
		{"a name outside", []string{"b/", "c/"}, []string{"a/x", "b/y", "c/z"},
			[]Totals{counted("b/", 1), counted("c/", 1)}},
		{"two names in one prefix", []string{"", "a/"}, []string{"a/x", "a/y"},
			[]Totals{counted("", 1), counted("a/", 1)}},
		{"outside every prefix", []string{"b/"}, []string{"a/x", "a/y"},
			[]Totals{{Prefix: "b/"}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tl := tally(t, c.prefixes...)
			for _, n := range c.names {
				add(t, tl, file(n, uint32(len(c.names)), 7))
			}
			check(t, tl.Result(), c.want...)
			if c.name == "outside every prefix" && len(tl.seen) != 0 {
				t.Errorf("remembers %d files with no name in a prefix", len(tl.seen))
			}
		})
	}
}

// permutations is every order of n items, as index slices.
func permutations(n int) [][]int {
	if n == 0 {
		return [][]int{{}}
	}
	var out [][]int
	for _, p := range permutations(n - 1) {
		for i := 0; i <= len(p); i++ {
			out = append(out, slices.Insert(slices.Clone(p), i, n-1))
		}
	}
	return out
}

// TestNestedChainInEveryOrder: one file named at each level of a chain of
// nested prefixes and once outside them counts once in each, in every
// arrival order, fed one name per Add or all in one.
func TestNestedChainInEveryOrder(t *testing.T) {
	names := []string{"a/x", "a/b/y", "a/b/c/z", "d/w"}
	orders := permutations(len(names))
	if len(orders) != 24 {
		t.Fatalf("%d orders", len(orders))
	}
	for _, order := range orders {
		for _, oneSlice := range []bool{false, true} {
			tl := tally(t, "a/", "a/b/", "a/b/c/")
			var all []listing.Entry
			for _, i := range order {
				e := file(names[i], 4, 9)
				if oneSlice {
					all = append(all, e)
				} else {
					add(t, tl, e)
				}
			}
			if oneSlice {
				add(t, tl, all...)
			}
			check(t, tl.Result(), counted("a/", 1), counted("a/b/", 1), counted("a/b/c/", 1))
			m := tl.seen[fileID{1, 9}]
			if got := append([]int32{m.first}, m.more...); len(tl.seen) != 1 || len(got) != 3 {
				t.Errorf("order %v, one slice %v: remembered %v", order, oneSlice, tl.seen)
			}
		}
	}
}

// TestIdentifiedAndUnidentifiedAlias: a name whose identity is missing
// is Unidentified and counts nowhere, whichever name comes first; the
// identified name counts.
func TestIdentifiedAndUnidentifiedAlias(t *testing.T) {
	known := file("a/x", 2, 5)
	unknown := file("a/y", 2, 5)
	unknown.Inode = nil
	for _, order := range [][]listing.Entry{{known, unknown}, {unknown, known}} {
		tl := tally(t, "a/")
		add(t, tl, order[0])
		add(t, tl, order[1])
		want := counted("a/", 1)
		want.Unidentified = 1
		check(t, tl.Result(), want)
	}
}

// TestMissingPerName: every name of a hard-linked file without a size or
// a time counts in SizeMissing and NewestMissing, though the file counts
// once, whether its names come in one slice or in two.
func TestMissingPerName(t *testing.T) {
	alias := func(key string) listing.Entry {
		e := file(key, 2, 5)
		e.Size, e.LastModified = nil, nil
		return e
	}
	want := []Totals{
		{Prefix: "", Files: 1, Allocated: 4096, SizeMissing: 2, NewestMissing: 2},
		{Prefix: "a/", Files: 1, Allocated: 4096, SizeMissing: 2, NewestMissing: 2},
	}

	one := tally(t, "", "a/")
	add(t, one, alias("a/x"), alias("a/y"))
	check(t, one.Result(), want...)

	two := tally(t, "", "a/")
	add(t, two, alias("a/x"))
	add(t, two, alias("a/y"))
	check(t, two.Result(), want...)
}

// TestSingleLinkWithoutIdentity: a file with one link needs no device or
// inode, and two of them count as two files.
func TestSingleLinkWithoutIdentity(t *testing.T) {
	tl := tally(t, "a/")
	for _, key := range []string{"a/x", "a/y"} {
		e := file(key, 1, 0)
		e.Device, e.Inode = nil, nil
		add(t, tl, e)
	}
	check(t, tl.Result(), counted("a/", 2))
	if len(tl.seen) != 0 {
		t.Errorf("remembers %d single-link files", len(tl.seen))
	}
}

// TestDirectoryBlocks: a directory's own blocks count in every prefix its
// key begins with, its own included, and the root's entry (key "") in ""
// alone; a directory without them counts in AllocatedMissing.
func TestDirectoryBlocks(t *testing.T) {
	tl := tally(t, "", "a/", "a/b/", "e/")
	add(t, tl, dir("", 100), dir("a/", 20), dir("a/b/", 3), dir("e/", 4096))
	add(t, tl, listing.Entry{Key: []byte("a/b/c/"), Kind: listing.KindDir})
	check(t, tl.Result(),
		Totals{Prefix: "", Allocated: 4219, AllocatedMissing: 1},
		Totals{Prefix: "a/", Allocated: 23, AllocatedMissing: 1},
		Totals{Prefix: "a/b/", Allocated: 3, AllocatedMissing: 1},
		Totals{Prefix: "e/", Allocated: 4096})
}

// TestLinkedSymlink: a symbolic link with two names counts its size and
// space once in a prefix and its names twice, and is no file.
func TestLinkedSymlink(t *testing.T) {
	tl := tally(t, "a/")
	add(t, tl, local(listing.KindSymlink, "a/l1", 7, 512, 2, 3), local(listing.KindSymlink, "a/l2", 7, 512, 2, 3))
	check(t, tl.Result(), Totals{Prefix: "a/", Symlinks: 2, Size: 7, Allocated: 512})
}

// TestBucket: objects count by literal key in every prefix the key begins
// with, an object at exactly a prefix's key in that prefix, and a key
// without the prefix's "/" in none of it.
func TestBucket(t *testing.T) {
	tl := tally(t, "", "logs/", "logs/2026/")
	add(t, tl,
		object("logs/", 1, 10),
		object("logs/2026/a", 10, 20),
		object("logs/2026/b", 100, 30),
		object("logs", 1000, 40),
		object("logsx/a", 10000, 50),
		listing.Entry{Key: []byte("logs/2026/"), Kind: listing.KindPrefix})
	check(t, tl.Result(),
		Totals{Prefix: "", Files: 5, Size: 11111, Newest: at(50)},
		Totals{Prefix: "logs/", Files: 3, Size: 111, Newest: at(30)},
		Totals{Prefix: "logs/2026/", Files: 2, Size: 110, Newest: at(30)})
}

// snapshot is a deep copy of the committed state: totals and memberships.
func snapshot(tl *Tally) ([]Totals, map[fileID]member) {
	seen := maps.Clone(tl.seen)
	for id, m := range seen {
		m.more = slices.Clone(m.more)
		seen[id] = m
	}
	return tl.Result(), seen
}

func sameMembers(a, b map[fileID]member) bool {
	return maps.EqualFunc(a, b, func(x, y member) bool {
		return x.first == y.first && slices.Equal(x.more, y.more)
	})
}

// TestInodeLimit: a slice that would remember more hard-linked files than
// MaxInodes is not taken, the membership it extended for a file already
// remembered and the two names it held of one file included, and every
// later Add fails; the same slice without the new file is taken.
func TestInodeLimit(t *testing.T) {
	setup := func() *Tally {
		tl, err := New(Options{Prefixes: []string{"a/", "a/b/", "b/"}, MaxInodes: 2})
		if err != nil {
			t.Fatal(err)
		}
		add(t, tl, file("a/x", 4, 1))
		add(t, tl, file("b/y", 2, 2))
		return tl
	}
	extend := []listing.Entry{file("a/b/p", 4, 1), file("a/b/q", 4, 1)}

	tl := setup()
	totals, seen := snapshot(tl)
	err := tl.Add(append(slices.Clone(extend), file("a/z", 2, 3)))
	if !errors.Is(err, ErrInodeLimit) {
		t.Fatalf("Add: %v, want ErrInodeLimit", err)
	}
	if again := tl.Add(nil); again != err {
		t.Errorf("a later Add: %v, want %v", again, err)
	}
	check(t, tl.Result(), totals...)
	if !sameMembers(tl.seen, seen) {
		t.Errorf("memberships %v, want %v", tl.seen, seen)
	}

	tl = setup()
	add(t, tl, extend...)
	check(t, tl.Result(), counted("a/", 1), counted("a/b/", 1), counted("b/", 1))
	if m := tl.seen[fileID{1, 1}]; m.first != 0 || !slices.Equal(m.more, []int32{1}) {
		t.Errorf("membership %v, want a/ and a/b/", m)
	}
}

// TestOverflow: a slice whose sums would overflow is not taken, the
// membership it extended for a file already remembered and the two names
// it held of one file included, and every later Add fails.
func TestOverflow(t *testing.T) {
	tl := tally(t, "", "a/", "b/")
	add(t, tl, file("b/y", 3, 1), object("a/big", math.MaxInt64-5000, 10))
	totals, seen := snapshot(tl)

	err := tl.Add([]listing.Entry{file("a/x", 3, 1), file("a/x2", 3, 1), object("a/o", 2000, 20)})
	if !errors.Is(err, ErrOverflow) {
		t.Fatalf("Add: %v, want ErrOverflow", err)
	}
	if again := tl.Add([]listing.Entry{object("b/o", 1, 1)}); again != err {
		t.Errorf("a later Add: %v, want %v", again, err)
	}
	check(t, tl.Result(), totals...)
	if !sameMembers(tl.seen, seen) {
		t.Errorf("memberships %v, want %v", tl.seen, seen)
	}

	alloc := tally(t, "")
	add(t, alloc, dir("", math.MaxInt64))
	if err := alloc.Add([]listing.Entry{dir("d/", 1)}); !errors.Is(err, ErrOverflow) {
		t.Errorf("space on disk: %v, want ErrOverflow", err)
	}
}

// TestResult: Result gives every prefix in Options.Prefixes' order as
// new values, and reading it changes nothing.
func TestResult(t *testing.T) {
	tl := tally(t, "b/", "", "a/")
	add(t, tl, object("a/x", 1, 10), object("b/y", 2, 20))
	want := []Totals{
		{Prefix: "b/", Files: 1, Size: 2, Newest: at(20)},
		{Prefix: "", Files: 2, Size: 3, Newest: at(20)},
		{Prefix: "a/", Files: 1, Size: 1, Newest: at(10)},
	}
	first := tl.Result()
	check(t, first, want...)

	*first[0].Newest = time.Time{}
	first[1].Files = 99
	check(t, tl.Result(), want...)

	add(t, tl, object("a/z", 4, 30))
	want[1] = Totals{Prefix: "", Files: 3, Size: 7, Newest: at(30)}
	want[2] = Totals{Prefix: "a/", Files: 2, Size: 5, Newest: at(30)}
	check(t, tl.Result(), want...)
}

// TestNew: New refuses a malformed or repeated prefix, a prefix past the
// largest position and a negative limit, and keeps its own copy of the
// prefixes.
func TestNew(t *testing.T) {
	for _, opt := range []Options{
		{Prefixes: []string{"a"}},
		{Prefixes: []string{"a/b"}},
		{Prefixes: []string{"a/", "b/", "a/"}},
		{Prefixes: []string{"", ""}},
		{Prefixes: []string{"a\x00/"}},
		{MaxInodes: -1},
	} {
		if _, err := New(opt); !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("New(%q, %d): %v, want listing.ErrInvalidRequest", opt.Prefixes, opt.MaxInodes, err)
		}
	}

	prefixes := []string{"a/", ""}
	tl, err := New(Options{Prefixes: prefixes})
	if err != nil {
		t.Fatal(err)
	}
	prefixes[0] = "b/"
	add(t, tl, object("a/x", 1, 1))
	check(t, tl.Result(),
		Totals{Prefix: "a/", Files: 1, Size: 1, Newest: at(1)},
		Totals{Prefix: "", Files: 1, Size: 1, Newest: at(1)})

	defer func(old int) { maxIndex = old }(maxIndex)
	maxIndex = 2
	if _, err := New(Options{Prefixes: []string{"a/", "b/", "c/", "d/"}}); !errors.Is(err, listing.ErrInvalidRequest) {
		t.Errorf("New with a prefix past the largest position: %v, want listing.ErrInvalidRequest", err)
	}
	if _, err := New(Options{Prefixes: []string{"a/", "b/", "c/"}}); err != nil {
		t.Errorf("New with a prefix at the largest position: %v", err)
	}

	if tl, err := New(Options{}); err != nil || len(tl.Result()) != 0 || tl.maxInodes != defaultMaxInodes {
		t.Errorf("New(Options{}): %v", err)
	}
}
