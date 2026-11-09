// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package main

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// duTree is a small tree with what du's flags are about: hard links within
// a directory, across directories and at the top, a sparse file, an empty
// file, a symbolic link, an empty directory, a name that is not UTF-8, and
// keys of every depth from 1 to 4.
func duTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write := func(rel string, n int) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, bytes.Repeat([]byte{'x'}, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	link := func(from, to string) {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, to)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(filepath.Join(root, from), filepath.Join(root, to)); err != nil {
			t.Fatal(err)
		}
	}
	write("a/b/c/deep", 70000)
	write("a/b/f1", 5000)
	write("a/g", 12)
	write("m/\xffbin/h", 300)
	write("m/empty", 0)
	write("top", 9000)
	link("a/b/f1", "z/f1-again")  // across directories: counted once under a/b/
	link("a/b/f1", "a/f1-up")     // the smallest key, a/b/f1 > a/f1-up: a/ owns it
	link("top", "a/b/c/top-link") // a link from the top into depth 3
	link("m/empty", "m/empty2")   // a link within one directory
	sparse := filepath.Join(root, "s", "sparse")
	if err := os.MkdirAll(filepath.Dir(sparse), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(sparse)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("tail"), 8<<20); err != nil { // 8 MiB apparent, a block or two allocated
		t.Fatal(err)
	}
	f.Close()
	if err := os.Symlink("a/g", filepath.Join(root, "m", "sym")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "e", "mpty"), 0o755); err != nil {
		t.Fatal(err)
	}
	return root
}

type duSums struct {
	objects, bytes, newest int64
	hasNewest              bool
}

// duOracle walks root with lstat and sums every prefix down to depth by
// brute force: each non-directory is an object (its size, or its blocks ×
// 512 when allocated) under every prefix of its key; with once, a file of
// several links counts under its smallest key only, its time under all;
// directories add their own prefixes and nothing else.
func duOracle(t *testing.T, root string, depth int, allocated, once bool) map[string]duSums {
	t.Helper()
	type obj struct {
		key         string
		size, mtime int64
		dir, multi  bool
		dev, ino    uint64
	}
	var objs []obj
	owner := map[[2]uint64]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == root {
			return err
		}
		var st syscall.Stat_t
		if err := syscall.Lstat(p, &st); err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		o := obj{key: filepath.ToSlash(rel), size: st.Size, mtime: st.Mtim.Sec*1_000_000 + st.Mtim.Nsec/1000, dir: d.IsDir(),
			multi: !d.IsDir() && st.Nlink > 1, dev: uint64(st.Dev), ino: st.Ino}
		if allocated {
			o.size = st.Blocks * 512
		}
		if o.dir {
			o.key += "/"
		}
		if id := [2]uint64{o.dev, o.ino}; o.multi && (owner[id] == "" || o.key < owner[id]) {
			owner[id] = o.key
		}
		objs = append(objs, o)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sums := map[string]duSums{"": {}}
	for _, o := range objs {
		counts := !o.dir && (!once || !o.multi || owner[[2]uint64{o.dev, o.ino}] == o.key)
		prefixes := []string{""}
		for end := 0; len(prefixes) <= depth; {
			i := strings.IndexByte(o.key[end:], '/')
			if i < 0 {
				break
			}
			end += i + 1
			prefixes = append(prefixes, o.key[:end])
		}
		for _, p := range prefixes {
			s := sums[p]
			if counts {
				s.objects++
				s.bytes += o.size
			}
			if !o.dir && (!s.hasNewest || o.mtime > s.newest) {
				s.newest, s.hasNewest = o.mtime, true
			}
			sums[p] = s
		}
	}
	return sums
}

// TestCountDepthMatchesDu: swath count --depth over a real tree, with each
// combination of --allocated and --count-links-once, at several depths,
// equals the brute-force sums of lstat, prefix for prefix, in byte order.
func TestCountDepthMatchesDu(t *testing.T) {
	root := duTree(t)
	for _, depth := range []int{0, 1, 2, 4} {
		for _, allocated := range []bool{false, true} {
			for _, once := range []bool{false, true} {
				t.Run(fmt.Sprintf("depth%d/allocated=%v/once=%v", depth, allocated, once), func(t *testing.T) {
					args := []string{root, "--depth", strconv.Itoa(depth), "--format", "tsv", "--no-stats", "--workers", "3"}
					if allocated {
						args = append(args, "--allocated")
					}
					if once {
						args = append(args, "--count-links-once")
					}
					var out bytes.Buffer
					if err := runCount(context.Background(), args, &out, false); err != nil {
						t.Fatal(err)
					}
					want := duOracle(t, root, depth, allocated, once)
					lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
					size := "Size"
					if allocated {
						size = "AllocatedSize"
					}
					if lines[0] != "Prefix\tObjects\t"+size+"\tLastModified" || len(lines)-1 != len(want) {
						t.Fatalf("%d rows for %d prefixes:\n%s", len(lines)-1, len(want), out.String())
					}
					prev := ""
					for i, l := range lines[1:] {
						f := strings.Split(l, "\t")
						if len(f) != 4 || i > 0 && f[0] <= prev {
							t.Fatalf("row %q after %q", l, prev)
						}
						prev = f[0]
						w, ok := want[f[0]]
						objects, _ := strconv.ParseInt(f[1], 10, 64)
						bytes, _ := strconv.ParseInt(f[2], 10, 64)
						var newest int64
						if f[3] != "" {
							at, err := time.Parse("2006-01-02T15:04:05.999999Z07:00", f[3])
							if err != nil {
								t.Fatal(err)
							}
							newest = at.UnixMicro()
						}
						if !ok || objects != w.objects || bytes != w.bytes || (f[3] != "") != w.hasNewest || newest != w.newest {
							t.Errorf("%q: got %d objects, %d bytes, newest %q; want %+v", f[0], objects, bytes, f[3], w)
						}
					}
				})
			}
		}
	}
	// The sparse file is where the two byte counts part.
	var app, alloc bytes.Buffer
	if err := runCount(context.Background(), []string{filepath.Join(root, "s"), "--depth=0", "--format=tsv", "--no-stats"}, &app, false); err != nil {
		t.Fatal(err)
	}
	if err := runCount(context.Background(), []string{filepath.Join(root, "s"), "--allocated", "--format=tsv", "--no-stats"}, &alloc, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(app.String(), "\t1\t8388612\t") || strings.Contains(alloc.String(), "\t1\t8388612\t") {
		t.Fatalf("sparse: apparent %q, allocated %q", app.String(), alloc.String())
	}
}

// TestCountDuRefusals: du's flags are a local source's and a count's, need
// each entry's stat, and an object store refuses them as it refuses the
// local provider's own flags; --one-filesystem is the local provider's.
func TestCountDuRefusals(t *testing.T) {
	root := duTree(t)
	for _, tc := range []struct {
		count bool
		args  []string
		want  string
	}{
		{true, []string{"s3://bkt", "--allocated"}, "--allocated applies to local sources, not to s3://"},
		{true, []string{"gs://bkt", "--count-links-once", "--depth", "1"}, "--count-links-once applies to local sources, not to gs://"},
		{true, []string{"s3://bkt", "--one-filesystem"}, "--one-filesystem applies to local sources, not to s3://"},
		{false, []string{"az://acct/ctr", "--one-filesystem"}, "--one-filesystem applies to local sources, not to az://"},
		{false, []string{root, "--allocated"}, "--allocated has no meaning for swath list"},
		{false, []string{root, "--count-links-once"}, "--count-links-once has no meaning for swath list"},
		{true, []string{root, "--allocated", "--metadata", "names"}, "not with --metadata names"},
		{true, []string{root, "--one-filesystem", "--metadata", "names"}, "names metadata tier"},
	} {
		var out bytes.Buffer
		var err error
		if tc.count {
			err = runCount(context.Background(), append(tc.args, "--no-stats"), &out, false)
		} else {
			err = run(context.Background(), append(tc.args, "-o", filepath.Join(t.TempDir(), "ds")), &out, false)
		}
		if exitOf(err) == exitCompleted || !strings.Contains(fmt.Sprint(err), tc.want) {
			t.Errorf("count=%v %v: exit %d (%v), want %q", tc.count, tc.args, exitOf(err), err, tc.want)
		}
	}
	// --allocated alone is the total, as du -s; --one-filesystem lists the
	// tree it stays on, and the manifest says the listing stopped there.
	var out bytes.Buffer
	if err := runCount(context.Background(), []string{root, "--allocated", "--one-filesystem", "--format", "tsv", "--no-stats"}, &out, false); err != nil ||
		!strings.HasPrefix(out.String(), "Prefix\tObjects\tAllocatedSize\tLastModified\n\t12\t") || strings.Count(out.String(), "\n") != 2 {
		t.Fatalf("%v %q", err, out.String())
	}
	ds := filepath.Join(t.TempDir(), "ds")
	out.Reset()
	if err := run(context.Background(), []string{root, "-o", ds, "--one-filesystem", "--no-stats"}, &out, false); err != nil {
		t.Fatal(err)
	}
	var m struct {
		Extensions struct {
			Swath struct{ Coverage struct{ Filters []string } }
		}
	}
	readJSON(t, filepath.Join(ds, "manifest.json"), &m)
	if f := m.Extensions.Swath.Coverage.Filters; len(f) != 1 || f[0] != "one_filesystem" {
		t.Fatalf("coverage.filters %q", f)
	}
}
