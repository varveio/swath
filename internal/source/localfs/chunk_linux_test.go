// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/order"
)

// kids collects one Expand's child ranges.
type kids struct{ r []Range }

func (k *kids) Rows([]Row) error    { return nil }
func (k *kids) Child(r Range) error { k.r = append(k.r, r); return nil }

// TestCancelledChunkReleasesItsFD: a chunk a worker claimed just before the
// run was cancelled is not queued, so the order stage does not dispose it;
// its Expand releases its share of the directory descriptor even when it
// returns at once on the cancelled context.
func TestCancelledChunkReleasesItsFD(t *testing.T) {
	if !nativeEngine {
		t.Skip("chunks are a getdents-engine feature")
	}
	root := t.TempDir()
	for i := range chunkThreshold + 1 {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%05d", i)), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	p, err := New(root, Options{Fields: Stat})
	if err != nil {
		t.Fatal(err)
	}
	k := &kids{}
	if err := p.Expand(context.Background(), p.Root(), k); err != nil {
		t.Fatal(err)
	}
	if len(k.r) < 2 || k.r[0].chunk == nil {
		t.Fatalf("%d children: want the directory's chunks", len(k.r))
	}
	d := k.r[0].chunk.dir
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, r := range k.r {
		if err := p.Expand(ctx, r, &kids{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled chunk: %v", err)
		}
	}
	if n := d.refs.Load(); n != 0 {
		t.Fatalf("%d references left on the directory descriptor after every chunk's cancelled Expand", n)
	}
}

// TestStatCountRaisesProcsAndRestores: Begin starts a count with
// countProcsFactor processors per CPU at a stat tier and
// namesCountProcsFactor at the names tier (at most the workers) and records
// it, and the end it returns restores GOMAXPROCS (the command's local
// source ends it when its listing returns: cli TestLocalCountRestoresProcs).
func TestStatCountRaisesProcsAndRestores(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)
	for _, fields := range []Fields{Stat, Names} {
		src, err := Open(root, Options{Fields: fields}, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := int64(2 * countProcsFactor)
		if fields == Names {
			want = 2 * namesCountProcsFactor
		}

		env := bind.Env{Order: order.Options{Workers: 64}}
		end, err := src.Begin(nil, nil, true, &env.Order)
		if err != nil {
			t.Fatal(err)
		}
		if n := runtime.GOMAXPROCS(0); int64(n) != want {
			t.Fatalf("%s: GOMAXPROCS %d during the count, want %d", fields, n, want)
		}
		l, err := src.Bound().Prepare(env)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := l.Count(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		end()

		if n := runtime.GOMAXPROCS(0); n != 2 {
			t.Fatalf("%s: GOMAXPROCS %d after the end, want 2 restored", fields, n)
		}
		if got := src.p.Stats().CountProcs; got != want {
			t.Fatalf("%s: count_procs %d, want %d", fields, got, want)
		}
	}
}

// slabTree is a directory of several slabs (files, subdirectories with a
// file each, symlinks) and its expected file count.
func slabTree(t *testing.T) (string, int) {
	t.Helper()
	root := t.TempDir()
	files := 0
	for i := range 3*slabEntries + 123 {
		name := filepath.Join(root, fmt.Sprintf("f%05d", i))
		switch {
		case i%500 == 0:
			if err := os.Mkdir(name, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(name, "x"), []byte("abc"), 0o644); err != nil {
				t.Fatal(err)
			}
			files++
		case i%700 == 0:
			if err := os.Symlink("target", name); err != nil {
				t.Fatal(err)
			}
			files++
		default:
			if err := os.WriteFile(name, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			files++
		}
	}
	return root, files
}

// TestCountSlabs: a count reads a directory of several slabs in slabs, hands
// each slab's entries out as chunks with a continuation after them, and sees
// what a sorted listing sees, at one worker and many, at the names and stat
// tiers; every descriptor is closed when it returns.
func TestCountSlabs(t *testing.T) {
	if !nativeEngine {
		t.Skip("slabs are a getdents-engine feature")
	}
	root, _ := slabTree(t)
	for _, fields := range []Fields{Names, Stat} {
		for _, workers := range []int{1, 4, 16} {
			t.Run(fmt.Sprintf("%s/w%d", fields, workers), func(t *testing.T) {
				opt := Options{Fields: fields, LinkTarget: true}
				sorted, want := list(t, root, opt, workers, 7)
				fds := countFDs(t)
				src, err := Open(root, opt, nil)
				if err != nil {
					t.Fatal(err)
				}
				st, err := listOpened(context.Background(), src, nil, true, bind.Env{Order: order.Options{Workers: workers}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				got := src.p.Stats()
				if st.Rows != int64(len(sorted)) || got.Files != want.Files || got.Dirs != want.Dirs || got.Bytes != want.Bytes {
					t.Fatalf("count: rows %d files %d dirs %d bytes %d; listing: rows %d files %d dirs %d bytes %d",
						st.Rows, got.Files, got.Dirs, got.Bytes, len(sorted), want.Files, want.Dirs, want.Bytes)
				}
				// The root (3*slabEntries+123 entries, read in whole getdents batches) takes at least
				// 3 slabs, each subdirectory one.
				if got.Slabs < got.DirsRead+2 {
					t.Fatalf("count_slabs %d for %d directories: the large one was not read in slabs", got.Slabs, got.DirsRead)
				}
				if fields == Stat && got.Chunks < 3*slabEntries/chunkEntries-1 {
					t.Fatalf("chunks %d: the slabs were not handed out", got.Chunks)
				}
				if n := countFDs(t); n != fds {
					t.Fatalf("%d descriptors open after the count, %d before", n, fds)
				}
			})
		}
	}
}

// TestCountSlabReleasesFD: the chunks and the continuation a count's first
// slab hands out each hold a share of the directory descriptor, released
// whether they are expanded on a cancelled context or disposed unexpanded;
// the continuation holds the directory's name set (Options.MaxHeldBytes),
// released either way too.
func TestCountSlabReleasesFD(t *testing.T) {
	if !nativeEngine {
		t.Skip("slabs are a getdents-engine feature")
	}
	root, _ := slabTree(t)
	for _, dispose := range []bool{false, true} {
		p, err := New(root, Options{Fields: Stat, MaxHeldBytes: 1 << 30})
		if err != nil {
			t.Fatal(err)
		}
		p.unsorted = true
		k := &kids{}
		if err := p.Expand(context.Background(), p.Root(), k); err != nil {
			t.Fatal(err)
		}
		var d *dirFD
		cont := 0
		for _, r := range k.r {
			if r.chunk != nil {
				d = r.chunk.dir
				if r.chunk.next {
					cont++
				}
			}
		}
		if d == nil || cont != 1 || !k.r[len(k.r)-1].chunk.next {
			t.Fatalf("%d children, %d continuations: want the slab's chunks, then one continuation", len(k.r), cont)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, r := range k.r {
			if dispose {
				p.Dispose(r)
			} else if err := p.Expand(ctx, r, &kids{}); r.chunk != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled range: %v", err)
			}
		}
		if n := d.refs.Load(); n != 0 {
			t.Fatalf("dispose=%v: %d references left on the directory descriptor", dispose, n)
		}
		if n := p.heldNames.Load(); n != 0 || d.seen != nil {
			t.Fatalf("dispose=%v: %d bytes of names held", dispose, n)
		}
	}
}

// nestedLarge builds a/b/c below a new root, each directory past
// chunkThreshold entries and a past two slabs, so a kept directory is
// chunked and a count reads a in several slabs. It returns the root and
// each level's file count.
func nestedLarge(t *testing.T) (string, []int) {
	t.Helper()
	root := t.TempDir()
	files := []int{2*slabEntries + 100, chunkThreshold + 100, chunkThreshold + 100}
	dir := root
	for i, n := range files {
		dir = filepath.Join(dir, string(rune('a'+i)))
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for j := range n {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%05d", j)), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	return root, files
}

// TestRefusedDirectoryIsNotShared: a large directory the descriptor limit
// did not keep is read whole by the Expand that opened it, as a listing
// and as a count: it hands out no chunk or continuation, so no queued range
// holds its descriptor once that Expand returns, and nested large
// directories past the limit hold none. Each Expand is stepped by hand.
func TestRefusedDirectoryIsNotShared(t *testing.T) {
	root, files := nestedLarge(t)
	for _, count := range []bool{false, true} {
		t.Run(fmt.Sprintf("count=%v", count), func(t *testing.T) {
			before := countFDs(t)
			p, err := New(root, Options{Fields: Stat})
			if err != nil {
				t.Fatal(err)
			}
			p.keep.limit = 0
			p.unsorted = count
			var x expansion
			if err := p.Expand(context.Background(), p.Root(), &x); err != nil {
				t.Fatal(err)
			}
			if len(x.kids) != 1 {
				t.Fatalf("root: %d children, want a's range", len(x.kids))
			}
			r := x.kids[0]
			held := countFDs(t) // the root's, held by a's range
			if held != before+1 {
				t.Fatalf("%d descriptors open with the root held, %d before", held, before)
			}

			for i, n := range files {
				var x expansion
				if err := p.Expand(context.Background(), r, &x); err != nil {
					t.Fatal(err)
				}
				dirRows := min(1, len(files)-1-i)
				if open := countFDs(t); open != before+dirRows { // the root's, held by the next level's range
					t.Fatalf("level %d: %d descriptors open after its Expand, want %d", i+1, open, before+dirRows)
				}
				if len(x.rows) != n+dirRows || len(x.kids) != dirRows {
					t.Fatalf("level %d: %d rows and %d children, want %d and %d", i+1, len(x.rows), len(x.kids), n+dirRows, dirRows)
				}
				for _, k := range x.kids {
					if k.chunk != nil || !k.at.kept() || k.below != i+2 {
						t.Fatalf("level %d: child %q chunk %v, below %d: want the next level, from the root", i+1, k.prefix, k.chunk != nil, k.below)
					}
					r = k
				}
			}

			st := p.Stats()
			if st.Errors() != 0 || st.Chunks != 0 || st.DirOpens.Refused != int64(len(files)) || st.DirOpens.Redescents != int64(len(files)-1) {
				t.Fatalf("errors %d %q, chunks %d, dir_opens %+v", st.Errors(), st.Samples, st.Chunks, st.DirOpens)
			}
			if count && st.Slabs < int64(len(files))+2 {
				t.Fatalf("count_slabs %d: a was not read in several slabs", st.Slabs)
			}
			if open := countFDs(t); open != before {
				t.Fatalf("%d descriptors open after the walk, %d before", open, before)
			}
		})
	}
}

// TestRefusedDirectoriesListWhole: with no directory kept but the root, a
// parallel listing and count of nested large directories see every key,
// hand out no chunk, and close every descriptor.
func TestRefusedDirectoriesListWhole(t *testing.T) {
	root, _ := nestedLarge(t)
	want := oracle(t, root, true)
	for _, count := range []bool{false, true} {
		t.Run(fmt.Sprintf("count=%v", count), func(t *testing.T) {
			before := countFDs(t)
			p, err := New(root, Options{Fields: Stat})
			if err != nil {
				t.Fatal(err)
			}
			p.keep.limit = 0
			var rows []Row
			if count {
				p.unsorted = true
				c := &collect{}
				if _, err := order.CountWithSink[Range, Entry](context.Background(), p, nil, order.Options{Workers: 8, Unordered: true}, c); err != nil {
					t.Fatal(err)
				}
				rows = c.rows
			} else {
				rows, _ = walk(t, p, 8, 1<<20)
			}

			keys := make([]string, len(rows))
			for i := range rows {
				keys[i] = rows[i].Key
			}
			if count {
				slices.Sort(keys)
			}
			if !slices.Equal(keys, want) {
				t.Fatalf("%d keys, want %d", len(keys), len(want))
			}
			st := p.Stats()
			if st.Errors() != 0 || st.Chunks != 0 || st.DirOpens.Refused != 3 {
				t.Fatalf("errors %d %q, chunks %d, dir_opens %+v", st.Errors(), st.Samples, st.Chunks, st.DirOpens)
			}
			if open := countFDs(t); open != before {
				t.Fatalf("%d descriptors open after the walk, %d before", open, before)
			}
		})
	}
}

// TestAdmitSlabDropsRepeats: a name a directory's enumeration returns
// again, in the same slab or a later one, is dropped and counted.
func TestAdmitSlabDropsRepeats(t *testing.T) {
	p := &Provider{opt: Options{MaxHeldBytes: 1 << 20}, unsorted: true}
	n := p.newNameSet()
	slab := func(names ...string) *scratch {
		sc := &scratch{}
		for _, name := range names {
			sc.ents = append(sc.ents, entry{nameOff: uint32(len(sc.names)), nameLen: uint32(len(name))})
			sc.names = append(sc.names, name...)
		}
		return sc
	}
	got := func(sc *scratch) []string {
		var s []string
		for i := range sc.ents {
			s = append(s, nameOf(sc.names, &sc.ents[i]))
		}
		return s
	}
	var st Stats
	first, second := slab("a", "b", "a", "c"), slab("b", "d")
	if !p.admitSlab(n, Range{}, first, &st) || !p.admitSlab(n, Range{}, second, &st) {
		t.Fatal("the bound stopped the enumeration")
	}
	if !slices.Equal(got(first), []string{"a", "b", "c"}) || !slices.Equal(got(second), []string{"d"}) || st.RepeatedNames != 2 {
		t.Fatalf("%q %q, repeated_names %d", got(first), got(second), st.RepeatedNames)
	}
	n.drop()
	if p.heldNames.Load() != 0 {
		t.Fatalf("%d bytes held after the enumeration ended", p.heldNames.Load())
	}
}
