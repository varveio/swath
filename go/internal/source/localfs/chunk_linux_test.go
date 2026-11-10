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
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
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

// TestStatCountRaisesProcsAndRestores: a count runs with countProcsFactor
// processors per CPU at a stat tier and namesCountProcsFactor at the names
// tier (at most the workers), records it, and restores GOMAXPROCS.
func TestStatCountRaisesProcsAndRestores(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)
	for _, fields := range []Fields{Stat, Names} {
		src, err := open(context.Background(), registry.Request{Root: root, Options: &Options{Fields: fields}, Workers: 64})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 64}}); err != nil {
			t.Fatal(err)
		}
		if n := runtime.GOMAXPROCS(0); n != 2 {
			t.Fatalf("%s: GOMAXPROCS %d after the run, want 2 restored", fields, n)
		}
		want := int64(2 * countProcsFactor)
		if fields == Names {
			want = 2 * namesCountProcsFactor
		}
		if got := src.(*opened).p.Stats().CountProcs; got != want {
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

func openFDs(t *testing.T) int {
	t.Helper()
	des, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(des)
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
				fds := openFDs(t)
				src, err := open(context.Background(), registry.Request{Root: root, Options: &opt, Workers: workers})
				if err != nil {
					t.Fatal(err)
				}
				st, _, err := src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: workers}})
				if err != nil {
					t.Fatal(err)
				}
				got := src.(*opened).p.Stats()
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
				if n := openFDs(t); n != fds {
					t.Fatalf("%d descriptors open after the count, %d before", n, fds)
				}
			})
		}
	}
}

// TestCountSlabReleasesFD: the chunks and the continuation a count's first
// slab hands out each hold a share of the directory descriptor, released
// whether they are expanded on a cancelled context or disposed unexpanded.
func TestCountSlabReleasesFD(t *testing.T) {
	if !nativeEngine {
		t.Skip("slabs are a getdents-engine feature")
	}
	root, _ := slabTree(t)
	for _, dispose := range []bool{false, true} {
		p, err := New(root, Options{Fields: Stat})
		if err != nil {
			t.Fatal(err)
		}
		p.unsorted = true
		k := &kids{}
		if err := p.Expand(context.Background(), p.Root(), k); err != nil {
			t.Fatal(err)
		}
		var d *sharedFD
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
	}
}
