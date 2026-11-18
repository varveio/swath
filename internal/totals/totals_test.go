// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package totals

import (
	"errors"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

// bruteWith is the oracle: every prefix of every key, summed one row at a
// time, under opt's depth and origin, where an object counts (as an
// object, with its figures and its time) only if counts says so. A directory counts in its parents' Dirs, and its own allocated
// bytes in its own prefix and its parents', as du -s; a key outside the
// origin in the total alone. alloc: the source has allocated bytes.
func bruteWith(rows []Row, opt Options, alloc bool, counts func(Row) bool) []Prefix {
	m := map[string]*Prefix{}
	allocKnown := map[string]int64{} // the entries with allocated bytes, by prefix
	for _, r := range rows {
		key := r.Key
		if !strings.HasPrefix(key, opt.Origin) {
			key = opt.Origin
		}
		for k := 0; k <= opt.Depth; k++ {
			end, ok := len(opt.Origin), true
			for range k {
				i := strings.IndexByte(key[end:], '/')
				if i < 0 {
					ok = false
					break
				}
				end += i + 1
			}
			if !ok {
				break
			}
			p := m[key[:end]]
			if p == nil {
				p = &Prefix{Prefix: key[:end], Depth: k}
				m[key[:end]] = p
			}
			if r.Dir && r.HasAllocated {
				p.Allocated += r.Allocated
				allocKnown[p.Prefix]++
			} else if r.Dir {
				p.AllocatedMissing++
			}
			if r.Dir && key[:end] == r.Key {
				break
			}
			if r.Dir {
				p.Dirs++
			}
			if !r.Object {
				continue
			}
			if counts(r) {
				p.Objects++
				if r.HasSize {
					p.Size += r.Size
				} else {
					p.SizeMissing++
				}
				if r.HasAllocated {
					p.Allocated += r.Allocated
					allocKnown[p.Prefix]++
				} else {
					p.AllocatedMissing++
				}
				if r.HasMtime && (!p.HasNewest || r.Mtime > p.Newest) {
					p.Newest, p.HasNewest = r.Mtime, true
				}
			}
		}
	}
	if m[opt.Origin] == nil {
		m[opt.Origin] = &Prefix{Prefix: opt.Origin}
	}
	var out []Prefix
	for _, p := range m {
		p.HasSize = p.Objects == 0 || p.SizeMissing < p.Objects
		p.HasAllocated = alloc && (p.AllocatedMissing == 0 || allocKnown[p.Prefix] > 0)
		out = append(out, *p)
	}
	slices.SortFunc(out, func(a, b Prefix) int { return strings.Compare(a.Prefix, b.Prefix) })
	return out
}

// randomRows is a tree of keys up to 5 levels deep over a small alphabet
// that includes bytes above 0x7f, invalid UTF-8 and '/' runs ("a//b": an
// empty component), sorted as a range's rows are, some not objects: a
// directory or a delete marker.
func randomRows(rng *rand.Rand, n int) []Row {
	names := []string{"a", "b", "\xff", "\xc3\xa9", "a-b", "a.b", "0", "", "zz\x80"}
	seen := map[string]bool{}
	var rows []Row
	for len(rows) < n {
		var parts []string
		for range 1 + rng.IntN(5) {
			parts = append(parts, names[rng.IntN(len(names))])
		}
		k := strings.Join(parts, "/")
		if rng.IntN(5) == 0 {
			k += "/"
		}
		if seen[k] {
			continue
		}
		seen[k] = true
		r := Row{Key: k, Object: rng.IntN(6) != 0, Size: rng.Int64N(1 << 40), HasSize: rng.IntN(10) != 0,
			Allocated: rng.Int64N(1 << 40), HasAllocated: rng.IntN(4) != 0}
		if !r.Object { // a directory keeps its own blocks
			r.Dir, r.Size, r.HasSize = rng.IntN(2) == 0, 0, false
			if !r.Dir {
				r.Allocated, r.HasAllocated = 0, false
			}
		}
		if rng.IntN(8) != 0 {
			r.Mtime, r.HasMtime = rng.Int64N(1<<50)-1<<49, true // negative times too
		}
		rows = append(rows, r)
	}
	slices.SortFunc(rows, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
	return rows
}

// TestTallyMatchesBruteForce: rows split into random batches, flushed by
// concurrent goroutines in any order (sorted within a batch, as a range's,
// or shuffled), give the brute-force totals at every depth and origin, in
// byte order: both figures, each with its missing objects, and a figure
// unknown only where every object lacked it (the allocated bytes
// throughout for a source without them).
func TestTallyMatchesBruteForce(t *testing.T) {
	for seed := range uint64(20) {
		rng := rand.New(rand.NewPCG(seed, 7))
		rows := randomRows(rng, 1+rng.IntN(600))
		for _, depth := range []int{0, 1, 2, 3, 7} {
			origin := []string{"", "a/", "a/b/", "\xff/"}[rng.IntN(4)]
			alloc := seed%3 != 0
			want := bruteWith(rows, Options{Depth: depth, Origin: origin}, alloc, func(Row) bool { return true })
			tl := New(Options{Depth: depth, Origin: origin})
			if alloc {
				tl.SetAllocated()
			}
			var batches [][]Row
			for rest := rows; len(rest) > 0; {
				n := min(len(rest), 1+rng.IntN(40))
				b := slices.Clone(rest[:n])
				if seed%2 == 1 {
					rng.Shuffle(len(b), func(i, j int) { b[i], b[j] = b[j], b[i] })
				}
				batches, rest = append(batches, b), rest[n:]
			}
			var wg sync.WaitGroup
			ch := make(chan []Row)
			for range 8 {
				wg.Go(func() {
					for rs := range ch {
						b := tl.Batch()
						for i := range rs {
							b.Add(&rs[i])
						}
						if err := b.Flush(); err != nil {
							t.Error(err)
						}
					}
				})
			}
			for _, b := range batches {
				ch <- b
			}
			close(ch)
			wg.Wait()
			got := tl.Result()
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d depth %d origin %q: %d prefixes, want %d\ngot  %+v\nwant %+v", seed, depth, origin, len(got), len(want), got, want)
			}
			st, w := tl.Stats(), want[0]
			if st.Prefixes != int64(len(want)) || st.Objects != w.Objects || (st.Size != nil) != w.HasSize || st.Size != nil && *st.Size != w.Size ||
				(st.AllocatedBytes != nil) != w.HasAllocated || st.AllocatedBytes != nil && *st.AllocatedBytes != w.Allocated || st.Dirs != w.Dirs ||
				st.SizeMissing != w.SizeMissing || st.AllocatedBytesMissing != w.AllocatedMissing || st.Depth != depth || st.Primary != Size ||
				(st.MaxLastModified != nil) != w.HasNewest || st.MaxLastModified != nil && st.MaxLastModified.UnixMicro() != w.Newest {
				t.Fatalf("seed %d depth %d: stats %+v", seed, depth, st)
			}
		}
	}
}

// TestDirBlocks: a directory's own allocated bytes count in its own
// prefix and its parents', as du -s counts a directory's blocks, in the
// tally and in the view that leads with them, ordered or not; its size,
// and an empty directory's own prefix's Dirs, count nowhere.
func TestDirBlocks(t *testing.T) {
	rows := []Row{
		{Key: "a/", Dir: true, Size: 96, HasSize: true, Allocated: 4096, HasAllocated: true},
		{Key: "a/b/", Dir: true, Size: 64, HasSize: true, Allocated: 4096, HasAllocated: true},
		{Key: "a/f", Object: true, Size: 5, HasSize: true, Allocated: 8192, HasAllocated: true},
		{Key: "e/", Dir: true, Allocated: 1024, HasAllocated: true},
		{Key: "s/", Dir: true, Allocated: 4096, HasAllocated: true}, // one child: its line is shared
		{Key: "s/g", Object: true, Size: 100, HasSize: true, Allocated: 4096, HasAllocated: true},
		{Key: "x", Object: true, Size: 1, HasSize: true, Allocated: 0, HasAllocated: true},
	}
	want := map[string][4]int64{ // objects, dirs, size, allocated
		"": {3, 4, 106, 25600}, "a/": {1, 1, 5, 16384}, "a/b/": {0, 0, 0, 4096}, "e/": {0, 0, 0, 1024}, "s/": {1, 0, 100, 8192},
	}
	for _, ordered := range []bool{true, false} {
		tl := New(Options{Depth: 2, Primary: AllocatedBytes, View: true})
		tl.SetAllocated()
		r, b := tl.ViewRun(ordered), tl.Batch()
		for i := range rows {
			b.Add(&rows[i])
			r.Add(&rows[i])
		}
		r.Flush()
		if err := b.Flush(); err != nil {
			t.Fatal(err)
		}
		r.End()
		got := tl.Result()
		if len(got) != len(want) {
			t.Fatalf("ordered %v: %+v", ordered, got)
		}
		for _, p := range got {
			if w := want[p.Prefix]; [4]int64{p.Objects, p.Dirs, p.Size, p.Allocated} != w || !p.HasAllocated {
				t.Errorf("ordered %v: %q %+v, want %v", ordered, p.Prefix, p, w)
			}
		}
		v := tl.View()
		if v.Total.Bytes != 25600 || v.Total.Objects != 3 {
			t.Errorf("ordered %v: view total %+v", ordered, v.Total)
		}
		for _, n := range v.Nodes {
			if w, ok := want[n.Key]; ok && n.Bytes != w[3] {
				t.Errorf("ordered %v: view node %q %d bytes, want %d", ordered, n.Key, n.Bytes, w[3])
			}
		}
		// The drawn lines: s/ shares its line with s/g, and the line gives
		// s/'s bytes, its own block too; the total's lines add up to it.
		var top, shared int64
		for _, r := range v.Select(20) {
			if r.Level == 1 {
				top += r.Bytes
			}
			if r.Name == "s/g" {
				shared = r.Bytes
			}
		}
		if shared != 8192 || top != 25600 {
			t.Errorf("ordered %v: shared line %d bytes, the total's lines %d: %+v", ordered, shared, top, v.Select(20))
		}
	}
}

// TestNoDirBlocks: with NoDirBlocks (a size or time filter) directories'
// own blocks count nowhere, neither as bytes nor as missing, in the tally
// and the view; they still count in Dirs.
func TestNoDirBlocks(t *testing.T) {
	rows := []Row{
		{Key: "a/", Dir: true, Allocated: 4096, HasAllocated: true},
		{Key: "a/b/", Dir: true}, // without blocks: not missing either
		{Key: "a/f", Object: true, Size: 5, HasSize: true, Allocated: 8192, HasAllocated: true},
	}
	tl := New(Options{Depth: 2, Primary: AllocatedBytes, View: true, NoDirBlocks: true})
	tl.SetAllocated()
	r, b := tl.ViewRun(true), tl.Batch()
	for i := range rows {
		b.Add(&rows[i])
		r.Add(&rows[i])
	}
	r.Flush()
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	r.End()
	for _, p := range tl.Result() {
		want := map[string]int64{"": 8192, "a/": 8192, "a/b/": 0}[p.Prefix]
		if p.Allocated != want || p.AllocatedMissing != 0 || !p.HasAllocated {
			t.Errorf("%q: %+v, want %d allocated", p.Prefix, p, want)
		}
	}
	if st := tl.Stats(); st.Dirs != 2 {
		t.Errorf("dirs %d", st.Dirs)
	}
	if v := tl.View(); v.Total.Bytes != 8192 || v.Total.Missing != 0 {
		t.Errorf("view total %+v", v.Total)
	}
}

// TestDirBlocksSharedFold: under a shared line the fold holds the rest of
// the line's bytes, the joined prefixes' own blocks too: a/ shares its line
// with a/b/, whose f has a line and whose empty file folds, so the line's
// children add up to its bytes.
func TestDirBlocksSharedFold(t *testing.T) {
	rows := []Row{
		{Key: "a/", Dir: true, Allocated: 4096, HasAllocated: true},
		{Key: "a/b/", Dir: true, Allocated: 4096, HasAllocated: true},
		{Key: "a/b/f", Object: true, Size: 1, HasSize: true, Allocated: 4096, HasAllocated: true},
		{Key: "a/b/zero", Object: true, Size: 0, HasSize: true, Allocated: 0, HasAllocated: true},
	}
	for _, ordered := range []bool{true, false} {
		tl := New(Options{Primary: AllocatedBytes, View: true})
		tl.SetAllocated()
		r, b := tl.ViewRun(ordered), tl.Batch()
		for i := range rows {
			b.Add(&rows[i])
			r.Add(&rows[i])
		}
		r.Flush()
		if err := b.Flush(); err != nil {
			t.Fatal(err)
		}
		r.End()
		lines := tl.View().Select(20)
		var shared, under int64
		for _, l := range lines {
			switch {
			case l.Name == "a/b/":
				shared = l.Bytes
			case l.Level == 2:
				under += l.Bytes
			}
		}
		if shared != 12288 || under != shared {
			t.Errorf("ordered %v: shared line %d bytes, its lines %d: %+v", ordered, shared, under, lines)
		}
	}
}

// TestShortKeysAndDepth: a key counts in the prefixes it has; a key's own
// trailing '/' is its deepest prefix; nothing below the depth is held.
func TestShortKeysAndDepth(t *testing.T) {
	tl := New(Options{Depth: 3})
	b := tl.Batch()
	for _, r := range []Row{
		{Key: "top", Object: true, Size: 1, HasSize: true},
		{Key: "a/", Object: false}, // a delete marker
		{Key: "a/b/", Dir: true},   // a directory: in a/'s Dirs, not its own; without blocks, missing in both
		{Key: "a/x", Object: true, Size: 2, HasSize: true, Mtime: 5, HasMtime: true},
		{Key: "a/b/c/d/e", Object: true, Size: 4, HasSize: true, Mtime: 9, HasMtime: true},
		{Key: "a/b/y", Object: true}, // no bytes, no time
	} {
		b.Add(&r)
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []Prefix{
		{Prefix: "", Objects: 4, Dirs: 1, Size: 7, SizeMissing: 1, HasSize: true, AllocatedMissing: 5, Newest: 9, HasNewest: true},
		{Prefix: "a/", Depth: 1, Objects: 3, Dirs: 1, Size: 6, SizeMissing: 1, HasSize: true, AllocatedMissing: 4, Newest: 9, HasNewest: true},
		{Prefix: "a/b/", Depth: 2, Objects: 2, Size: 4, SizeMissing: 1, HasSize: true, AllocatedMissing: 3, Newest: 9, HasNewest: true},
		{Prefix: "a/b/c/", Depth: 3, Objects: 1, Size: 4, HasSize: true, AllocatedMissing: 1, Newest: 9, HasNewest: true},
	}
	if got := tl.Result(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	if st := tl.Stats(); st.NotObjects != 2 || st.Dirs != 1 || st.SizeMissing != 1 || st.Prefixes != 4 || st.AllocatedBytes != nil {
		t.Fatalf("stats %+v", st)
	}
	// An empty tally still has its total: a size of 0, and allocated
	// bytes of 0 only for a source that has them.
	if got := New(Options{Depth: 2}).Result(); !reflect.DeepEqual(got, []Prefix{{HasSize: true}}) {
		t.Fatalf("empty: %+v", got)
	}
	withAlloc := New(Options{Depth: 2, Origin: "logs/"})
	withAlloc.SetAllocated()
	if got := withAlloc.Result(); !reflect.DeepEqual(got, []Prefix{{Prefix: "logs/", HasSize: true, HasAllocated: true}}) {
		t.Fatalf("empty under an origin: %+v", got)
	}
}

// TestOrigin: depth counts from the origin, the source's own prefix, and
// the rows keep the keys' spelling; the total is the origin's row.
func TestOrigin(t *testing.T) {
	tl := New(Options{Depth: 1, Origin: "logs/2026/"})
	b := tl.Batch()
	for _, k := range []string{"logs/2026/", "logs/2026/01/a", "logs/2026/01/b/c", "logs/2026/02/d", "logs/2026/top"} {
		b.Add(&Row{Key: k, Object: true, Size: 1, HasSize: true})
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []Prefix{
		{Prefix: "logs/2026/", Objects: 5, Size: 5, HasSize: true, AllocatedMissing: 5},
		{Prefix: "logs/2026/01/", Depth: 1, Objects: 2, Size: 2, HasSize: true, AllocatedMissing: 2},
		{Prefix: "logs/2026/02/", Depth: 1, Objects: 1, Size: 1, HasSize: true, AllocatedMissing: 1},
	}
	if got := tl.Result(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
}

// TestUnreadable: an unreadable directory's prefix marks its row; one
// deeper than the rows, or outside the origin, marks nothing.
func TestUnreadable(t *testing.T) {
	tl := New(Options{Depth: 1, Origin: "logs/"})
	b := tl.Batch()
	for _, k := range []string{"logs/a/", "logs/b/", "logs/b/c/", "logs/b/f"} {
		b.Add(&Row{Key: k, Dir: strings.HasSuffix(k, "/"), Object: !strings.HasSuffix(k, "/"), Size: 1, HasSize: true})
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"logs/a/", "logs/b/c/", "other/"} {
		tl.Unreadable(p)
	}
	var marked []string
	for _, r := range tl.Result() {
		if r.Unreadable {
			marked = append(marked, r.Prefix)
		}
	}
	if !slices.Equal(marked, []string{"logs/a/"}) {
		t.Fatalf("marked %q", marked)
	}
}

// TestHugeDepth: a depth far past the keys' costs what the keys' own depth
// does: the per-level scratch grows with the levels reached, never with
// the depth asked for.
func TestHugeDepth(t *testing.T) {
	tl := New(Options{Depth: 1 << 40})
	b := tl.Batch()
	b.Add(&Row{Key: "a/b/c", Object: true})
	if len(b.cur) != 3 {
		t.Fatalf("%d levels of scratch for a key of 3", len(b.cur))
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := len(tl.Result()); got != 3 {
		t.Fatalf("%d prefixes", got)
	}
}

// TestByteOrder: prefixes sort by their bytes, unsigned, as keys do: not
// as UTF-8 text, and invalid UTF-8 after everything valid below it.
func TestByteOrder(t *testing.T) {
	tl := New(Options{Depth: 1})
	b := tl.Batch()
	for _, k := range []string{"\xff/x", "a/x", "\xc3\xa9/x", "A/x", "a-b/x", "a.b/x", "\x01/x", "\x80/x"} {
		b.Add(&Row{Key: k, Object: true})
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range tl.Result() {
		got = append(got, p.Prefix)
	}
	want := []string{"", "\x01/", "A/", "a-b/", "a.b/", "a/", "\x80/", "\xc3\xa9/", "\xff/"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q", got)
	}
}

// TestPrefixCap: the merge that would pass MaxPrefixes fails with a
// LimitError, every later merge with the same one, and the tally holds no
// more than the cap.
func TestPrefixCap(t *testing.T) {
	tl := New(Options{Depth: 1, MaxPrefixes: 3})
	add := func(keys ...string) error {
		b := tl.Batch()
		for _, k := range keys {
			b.Add(&Row{Key: k, Object: true})
		}
		return b.Flush()
	}
	if err := add("a/1", "b/1"); err != nil { // "", a/, b/: at the cap
		t.Fatal(err)
	}
	if err := add("a/2", "b/2"); err != nil { // nothing new
		t.Fatal(err)
	}
	err := add("c/1")
	var le *LimitError
	if !errors.As(err, &le) || le.Max != 3 || le.Depth != 1 || le.Class() != "prefix_limit" {
		t.Fatalf("over the cap: %v", err)
	}
	if err := add("a/3"); !errors.As(err, &le) {
		t.Fatalf("after the cap: %v", err)
	}
	if st := tl.Stats(); st.Prefixes != 3 || st.MaxPrefixes != 3 {
		t.Fatalf("stats %+v", st)
	}
	// Keys without a '/' add no prefix below the total: a flat listing
	// holds one cell whatever the depth.
	flat := New(Options{Depth: 5, MaxPrefixes: 1})
	b := flat.Batch()
	for _, k := range []string{"x", "y", "z"} {
		b.Add(&Row{Key: k, Object: true})
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
}

// TestBatchKeepsNoKeys: a flushed batch holds no view of a key, and the
// tally holds copies, so the page under the rows may be reused at once.
func TestBatchKeepsNoKeys(t *testing.T) {
	tl := New(Options{Depth: 2})
	buf := []byte("a/b/c")
	b := tl.Batch()
	b.Add(&Row{Key: unsafe.String(&buf[0], len(buf)), Object: true}) // a view, as a page's keys are
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(b.ents) != 0 || cap(b.ents) > 0 && b.ents[:1][0].prefix != "" {
		t.Fatal("the batch keeps a view of its keys")
	}
	copy(buf, "z/z/z") // the page reused
	var got []string
	for _, p := range tl.Result() {
		got = append(got, p.Prefix)
	}
	if !slices.Equal(got, []string{"", "a/", "a/b/"}) {
		t.Fatalf("%q", got)
	}
}

// TestLinksOnceMatchesBruteForce: with LinksOnce, an object of several
// hard links counts once, its object and both figures (or their missing
// values) under the smallest of its keys and its time under every link,
// whichever batch or goroutine saw which link first, whether its links are
// inside the origin or not; without LinksOnce every link counts.
func TestLinksOnceMatchesBruteForce(t *testing.T) {
	for seed := range uint64(20) {
		rng := rand.New(rand.NewPCG(seed, 11))
		rows := randomRows(rng, 1+rng.IntN(400))
		inodes := 1 + rng.IntN(20)
		for i := range rows {
			if rows[i].Object && rng.IntN(3) == 0 {
				id := int64(rng.IntN(inodes))
				rows[i].Link, rows[i].Dev, rows[i].Inode = true, id%2, id
				rows[i].Size, rows[i].HasSize = 1000+id, id%4 != 0 // one inode, one size, some unknown
				rows[i].Allocated, rows[i].HasAllocated = 4096*id, id%3 != 0
			}
		}
		// The oracle: each inode counts under its smallest key; its other
		// links add nothing, not even their time.
		owner := map[[2]int64]string{}
		for _, r := range rows {
			id := [2]int64{r.Dev, r.Inode}
			if k, ok := owner[id]; r.Link && (!ok || r.Key < k) {
				owner[id] = r.Key
			}
		}
		links := int64(0)
		for _, r := range rows {
			if r.Link {
				links++
			}
		}
		counted := func(r Row) bool { return !r.Link || owner[[2]int64{r.Dev, r.Inode}] == r.Key }
		for _, depth := range []int{0, 2, 5} {
			for _, linksOnce := range []bool{false, true} {
				opt := Options{Depth: depth, LinksOnce: linksOnce, Origin: []string{"", "a/", "\xff/"}[rng.IntN(3)]}
				tl := New(opt)
				tl.SetAllocated()
				shuffled := slices.Clone(rows)
				rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
				var wg sync.WaitGroup
				for w := range 4 {
					wg.Go(func() {
						for lo := w * 13; lo < len(shuffled); lo += 4 * 13 {
							b := tl.Batch()
							for i := lo; i < min(lo+13, len(shuffled)); i++ {
								b.Add(&shuffled[i])
							}
							if err := b.Flush(); err != nil {
								t.Error(err)
							}
						}
					})
				}
				wg.Wait()
				want := bruteWith(rows, opt, true, func(Row) bool { return true })
				if linksOnce {
					want = bruteWith(rows, opt, true, counted)
				}
				got := tl.Result()
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("seed %d depth %d once %v origin %q:\ngot  %+v\nwant %+v", seed, depth, linksOnce, opt.Origin, got, want)
				}
				st := tl.Stats()
				if linksOnce && (st.LinkedRows != links || st.Inodes != int64(len(owner)) || !st.LinksOnce || !tl.LinksOnce()) ||
					!linksOnce && (st.LinkedRows != 0 || st.Inodes != 0 || st.LinksOnce || tl.LinksOnce()) {
					t.Fatalf("seed %d: stats %+v, %d links over %d inodes", seed, st, links, len(owner))
				}
			}
		}
	}
}

// TestLinkAliasTime: a name of a hard-linked file that is not counted
// adds no time to its prefix; with every name counted, every name's time
// counts.
func TestLinkAliasTime(t *testing.T) {
	for _, once := range []bool{true, false} {
		tl := New(Options{Depth: 1, LinksOnce: once})
		b := tl.Batch()
		for _, k := range []string{"a/f", "b/alias"} {
			b.Add(&Row{Key: k, Object: true, Size: 5, HasSize: true, Mtime: 100, HasMtime: true, Link: true, Dev: 1, Inode: 7})
		}
		if err := b.Flush(); err != nil {
			t.Fatal(err)
		}
		r := tl.Result()
		if len(r) != 3 || r[2].Prefix != "b/" || r[1].Objects != 1 || !r[1].HasNewest {
			t.Fatalf("once %v: %+v", once, r)
		}
		if alias := r[2]; once && (alias.Objects != 0 || alias.HasNewest) || !once && (alias.Objects != 1 || !alias.HasNewest || alias.Newest != 100) {
			t.Fatalf("once %v: b/ %+v", once, alias)
		}
	}
}

// TestInodeCap: the inodes LinksOnce remembers have their own cap and
// class, apart from the prefixes'.
func TestInodeCap(t *testing.T) {
	tl := New(Options{Depth: 0, MaxPrefixes: 1, MaxInodes: 2, LinksOnce: true})
	b := tl.Batch()
	for i := range int64(3) {
		b.Add(&Row{Key: "k" + string(rune('a'+i)), Object: true, Link: true, Inode: i})
	}
	var le *LimitError
	if err := b.Flush(); !errors.As(err, &le) || !le.Inodes || le.Max != 2 || le.Class() != "inode_limit" {
		t.Fatalf("%v", err)
	}
}

// TestSortBySize: below each parent the largest first by the primary
// figure, ties by prefix, unknown figures last and a partial sum by its
// bytes; a row's own rows stay under it and the total first.
func TestSortBySize(t *testing.T) {
	sized := func(p string, depth int, size, alloc int64) Prefix {
		return Prefix{Prefix: p, Depth: depth, Objects: 1, Size: size, HasSize: true, Allocated: alloc, HasAllocated: true}
	}
	rows := []Prefix{sized("", 0, 100, 100), sized("a/", 1, 10, 60), sized("a/x/", 2, 1, 1), sized("a/y/", 2, 9, 9),
		sized("b/", 1, 50, 50), sized("c/", 1, 10, 10), {Prefix: "d/", Depth: 1, Objects: 1, SizeMissing: 1, AllocatedMissing: 1},
		{Prefix: "e/", Depth: 1, HasSize: true, HasAllocated: true}, {Prefix: "f/", Depth: 1, Objects: 3, Size: 20, SizeMissing: 2, HasSize: true, AllocatedMissing: 3}}
	for primary, want := range map[string][]string{
		Size:           {"", "b/", "f/", "a/", "a/y/", "a/x/", "c/", "e/", "d/"},
		AllocatedBytes: {"", "a/", "a/y/", "a/x/", "b/", "c/", "e/", "d/", "f/"},
	} {
		rs := slices.Clone(rows)
		SortBySize(rs, primary)
		var got []string
		for _, r := range rs {
			got = append(got, r.Prefix)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%s: order %q, want %q", primary, got, want)
		}
	}
}

// TestUnidentifiedLinks: an object whose links cannot be told apart counts
// under its name, and the tally no longer claims to count links once.
func TestUnidentifiedLinks(t *testing.T) {
	tl := New(Options{LinksOnce: true})
	b := tl.Batch()
	b.Add(&Row{Key: "a", Object: true, Size: 1, HasSize: true, Unidentified: true})
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if tl.LinksOnce() || tl.Stats().LinksOnce || tl.Result()[0].Objects != 1 {
		t.Fatalf("links once %v, %+v", tl.LinksOnce(), tl.Result())
	}
	if empty := New(Options{LinksOnce: true}); !empty.LinksOnce() {
		t.Fatal("an empty tally keeps its option")
	}
}
