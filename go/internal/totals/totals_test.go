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

// brute is the oracle: every prefix of every key, summed one row at a time.
func brute(rows []Row, depth int) []Prefix {
	return bruteWith(rows, Options{Depth: depth}, func(Row) bool { return true })
}

// bruteWith is brute under opt's depth and origin, where an object counts
// (as an object, with its bytes) only if counts says so; its time counts in
// any case. A directory counts in its parents' Dirs, a key outside the
// origin in the total alone.
func bruteWith(rows []Row, opt Options, counts func(Row) bool) []Prefix {
	m := map[string]*Prefix{}
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
				p = &Prefix{Prefix: key[:end]}
				m[key[:end]] = p
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
				if r.HasBytes {
					p.Bytes += r.Bytes
				} else {
					p.Unsized++
				}
			}
			if r.HasMtime && (!p.HasNewest || r.Mtime > p.Newest) {
				p.Newest, p.HasNewest = r.Mtime, true
			}
		}
	}
	if m[opt.Origin] == nil {
		m[opt.Origin] = &Prefix{Prefix: opt.Origin}
	}
	var out []Prefix
	for _, p := range m {
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
		r := Row{Key: k, Object: rng.IntN(6) != 0, Bytes: rng.Int64N(1 << 40), HasBytes: rng.IntN(10) != 0}
		if !r.Object {
			r.Dir, r.Bytes, r.HasBytes = rng.IntN(2) == 0, 0, false
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
// byte order.
func TestTallyMatchesBruteForce(t *testing.T) {
	for seed := range uint64(20) {
		rng := rand.New(rand.NewPCG(seed, 7))
		rows := randomRows(rng, 1+rng.IntN(600))
		for _, depth := range []int{0, 1, 2, 3, 7} {
			origin := []string{"", "a/", "a/b/", "\xff/"}[rng.IntN(4)]
			want := bruteWith(rows, Options{Depth: depth, Origin: origin}, func(Row) bool { return true })
			tl := New(Options{Depth: depth, Origin: origin})
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
			st := tl.Stats()
			if st.Prefixes != int64(len(want)) || st.Objects != want[0].Objects || st.Bytes != want[0].Bytes || st.Dirs != want[0].Dirs ||
				st.Unsized != want[0].Unsized || st.Depth != depth {
				t.Fatalf("seed %d depth %d: stats %+v", seed, depth, st)
			}
		}
	}
}

// TestShortKeysAndDepth: a key counts in the prefixes it has; a key's own
// trailing '/' is its deepest prefix; nothing below the depth is held.
func TestShortKeysAndDepth(t *testing.T) {
	tl := New(Options{Depth: 3})
	b := tl.Batch()
	for _, r := range []Row{
		{Key: "top", Object: true, Bytes: 1, HasBytes: true},
		{Key: "a/", Object: false}, // a delete marker
		{Key: "a/b/", Dir: true},   // a directory: in a/'s Dirs, not its own
		{Key: "a/x", Object: true, Bytes: 2, HasBytes: true, Mtime: 5, HasMtime: true},
		{Key: "a/b/c/d/e", Object: true, Bytes: 4, HasBytes: true, Mtime: 9, HasMtime: true},
		{Key: "a/b/y", Object: true}, // no bytes, no time
	} {
		b.Add(&r)
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []Prefix{
		{Prefix: "", Objects: 4, Dirs: 1, Bytes: 7, Unsized: 1, Newest: 9, HasNewest: true},
		{Prefix: "a/", Objects: 3, Dirs: 1, Bytes: 6, Unsized: 1, Newest: 9, HasNewest: true},
		{Prefix: "a/b/", Objects: 2, Bytes: 4, Unsized: 1, Newest: 9, HasNewest: true},
		{Prefix: "a/b/c/", Objects: 1, Bytes: 4, Newest: 9, HasNewest: true},
	}
	if got := tl.Result(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	if st := tl.Stats(); st.NotObjects != 2 || st.Dirs != 1 || st.Unsized != 1 || st.Prefixes != 4 {
		t.Fatalf("stats %+v", st)
	}
	// An empty tally still has its total.
	if got := New(Options{Depth: 2}).Result(); !reflect.DeepEqual(got, []Prefix{{}}) {
		t.Fatalf("empty: %+v", got)
	}
	if got := New(Options{Depth: 2, Origin: "logs/"}).Result(); !reflect.DeepEqual(got, []Prefix{{Prefix: "logs/"}}) {
		t.Fatalf("empty under an origin: %+v", got)
	}
}

// TestOrigin: depth counts from the origin, the source's own prefix, and
// the rows keep the keys' spelling; the total is the origin's row.
func TestOrigin(t *testing.T) {
	tl := New(Options{Depth: 1, Origin: "logs/2026/"})
	b := tl.Batch()
	for _, k := range []string{"logs/2026/", "logs/2026/01/a", "logs/2026/01/b/c", "logs/2026/02/d", "logs/2026/top"} {
		b.Add(&Row{Key: k, Object: true, Bytes: 1, HasBytes: true})
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	want := []Prefix{
		{Prefix: "logs/2026/", Objects: 5, Bytes: 5},
		{Prefix: "logs/2026/01/", Objects: 2, Bytes: 2},
		{Prefix: "logs/2026/02/", Objects: 1, Bytes: 1},
	}
	if got := tl.Result(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
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
// hard links counts once, its object and bytes (or its unknown size) under
// the smallest of its keys and its time under every link, whichever batch
// or goroutine saw which link first, whether its links are inside the
// origin or not; without LinksOnce every link counts.
func TestLinksOnceMatchesBruteForce(t *testing.T) {
	for seed := range uint64(20) {
		rng := rand.New(rand.NewPCG(seed, 11))
		rows := randomRows(rng, 1+rng.IntN(400))
		inodes := 1 + rng.IntN(20)
		for i := range rows {
			if rows[i].Object && rng.IntN(3) == 0 {
				id := int64(rng.IntN(inodes))
				rows[i].Link, rows[i].Dev, rows[i].Inode = true, id%2, id
				rows[i].Bytes, rows[i].HasBytes = 1000+id, id%4 != 0 // one inode, one size, some unknown
			}
		}
		// The oracle: each inode counts under its smallest key; its other
		// links add their time only.
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
				want := bruteWith(rows, opt, func(Row) bool { return true })
				if linksOnce {
					want = bruteWith(rows, opt, counted)
				}
				got := tl.Result()
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("seed %d depth %d once %v origin %q:\ngot  %+v\nwant %+v", seed, depth, linksOnce, opt.Origin, got, want)
				}
				st := tl.Stats()
				if linksOnce && (st.LinkedRows != links || st.Inodes != int64(len(owner)) || !st.LinksOnce) || !linksOnce && (st.LinkedRows != 0 || st.Inodes != 0) {
					t.Fatalf("seed %d: stats %+v, %d links over %d inodes", seed, st, links, len(owner))
				}
			}
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
