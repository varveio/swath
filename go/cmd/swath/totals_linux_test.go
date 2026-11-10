// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"
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
	objects, dirs, bytes, newest int64
	hasNewest                    bool
}

// duOracle walks root with lstat and sums every prefix down to depth by
// brute force: each non-directory is an object (its size, or its blocks ×
// 512 when allocated) under every prefix of its key; with once, a file of
// several links counts under its smallest key only, its time under all;
// a directory adds its own prefix and counts in its parents' dirs.
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
			if o.dir && p != o.key {
				s.dirs++
			}
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

// TestDuMatchesLstat: swath du over a real tree, with each combination of
// --allocated and --count-links-once, at several depths, equals the
// brute-force sums of lstat, prefix for prefix, in byte order, in TSV and
// in JSONL (a name that is not UTF-8 as PrefixBase64).
func TestDuMatchesLstat(t *testing.T) {
	root := duTree(t)
	for _, depth := range []int{0, 1, 2, 4} {
		for _, allocated := range []bool{false, true} {
			for _, once := range []bool{false, true} {
				t.Run(fmt.Sprintf("depth%d/allocated=%v/once=%v", depth, allocated, once), func(t *testing.T) {
					args := []string{root, "-d", strconv.Itoa(depth), "--no-stats", "--workers", "3"}
					if allocated {
						args = append(args, "--allocated")
					}
					if once {
						args = append(args, "--count-links-once")
					}
					want := duOracle(t, root, depth, allocated, once)
					size := "Size"
					if allocated {
						size = "AllocatedSize"
					}
					check := func(prefix string, objects, dirs, bytes int64, modified string, prev *string, first bool) {
						t.Helper()
						if !first && prefix <= *prev {
							t.Fatalf("row %q after %q", prefix, *prev)
						}
						*prev = prefix
						var newest int64
						if modified != "" {
							at, err := time.Parse("2006-01-02T15:04:05.999999Z07:00", modified)
							if err != nil {
								t.Fatal(err)
							}
							newest = at.UnixMicro()
						}
						w, ok := want[prefix]
						if !ok || objects != w.objects || dirs != w.dirs || bytes != w.bytes || (modified != "") != w.hasNewest || newest != w.newest {
							t.Errorf("%q: got %d objects, %d dirs, %d bytes, newest %q; want %+v", prefix, objects, dirs, bytes, modified, w)
						}
					}
					var out bytes.Buffer
					if err := runDu(context.Background(), append(args, "--format", "tsv"), &out); err != nil {
						t.Fatal(err)
					}
					lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
					if lines[0] != "Depth\tObjects\tDirs\t"+size+"\tUnsized\tLastModified\tPrefix" || len(lines)-1 != len(want) {
						t.Fatalf("%d rows for %d prefixes:\n%s", len(lines)-1, len(want), out.String())
					}
					prev := ""
					for i, l := range lines[1:] {
						f := strings.Split(l, "\t")
						if len(f) != 7 || f[4] != "0" || f[0] != strconv.Itoa(strings.Count(f[6], "/")) {
							t.Fatalf("row %q", l)
						}
						n := func(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
						check(f[6], n(f[1]), n(f[2]), n(f[3]), f[5], &prev, i == 0)
					}
					// JSONL: the same rows, each line JSON; the prefix as
					// text when it is UTF-8, else its exact bytes in base64.
					out.Reset()
					if err := runDu(context.Background(), append(args, "--format", "jsonl"), &out); err != nil {
						t.Fatal(err)
					}
					rows := 0
					prev = ""
					for l := range strings.Lines(out.String()) {
						var r map[string]any
						if err := json.Unmarshal([]byte(l), &r); err != nil {
							t.Fatalf("%q: %v", l, err)
						}
						prefix, text := r["Prefix"].(string)
						if b64, ok := r["PrefixBase64"].(string); ok == text {
							t.Fatalf("%q: one of Prefix and PrefixBase64", l)
						} else if ok {
							raw, err := base64.StdEncoding.DecodeString(b64)
							if err != nil || utf8.Valid(raw) {
								t.Fatalf("%q: PrefixBase64 of %q (%v)", l, raw, err)
							}
							prefix = string(raw)
						}
						modified, _ := r["LastModified"].(string)
						if r["Unsized"] != 0.0 || r["Depth"] != float64(strings.Count(prefix, "/")) {
							t.Fatalf("%q: Unsized or Depth", l)
						}
						check(prefix, int64(r["Objects"].(float64)), int64(r["Dirs"].(float64)), int64(r[size].(float64)), modified, &prev, rows == 0)
						rows++
					}
					if rows != len(want) {
						t.Fatalf("%d JSONL rows for %d prefixes", rows, len(want))
					}
				})
			}
		}
	}
	// The sparse file is where the two byte counts part.
	var app, alloc bytes.Buffer
	if err := runDu(context.Background(), []string{filepath.Join(root, "s"), "--format=tsv", "--no-stats"}, &app); err != nil {
		t.Fatal(err)
	}
	if err := runDu(context.Background(), []string{filepath.Join(root, "s"), "--allocated", "--format=tsv", "--no-stats"}, &alloc); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(app.String(), "\t1\t0\t8388612\t") || strings.Contains(alloc.String(), "\t1\t0\t8388612\t") {
		t.Fatalf("sparse: apparent %q, allocated %q", app.String(), alloc.String())
	}
}

// TestDuLocalRefusals: du's local flags need each entry's stat, and an
// object store refuses them as it refuses the local provider's own flags;
// --one-file-system is the local provider's.
func TestDuLocalRefusals(t *testing.T) {
	root := duTree(t)
	for _, tc := range []struct {
		du   bool
		args []string
		want string
	}{
		{true, []string{"s3://bkt", "--allocated"}, "--allocated applies to Local sources, not to S3 sources"},
		{true, []string{"gs://bkt", "--count-links-once", "--depth", "1"}, "--count-links-once applies to Local sources, not to GCS sources"},
		{true, []string{"az://acct/ctr", "--max-inodes", "5"}, "--max-inodes applies to Local sources, not to Azure Blob sources"},
		{true, []string{"s3://bkt", "--one-file-system"}, "--one-file-system applies to Local sources, not to S3 sources"},
		{false, []string{"az://acct/ctr", "--one-file-system"}, "--one-file-system applies to Local sources, not to Azure Blob sources"},
		{false, []string{root, "--allocated"}, "--allocated has no meaning for swath list"},
		{false, []string{root, "--count-links-once"}, "--count-links-once has no meaning for swath list"},
		{true, []string{root, "--allocated", "--metadata", "names"}, "not with --metadata names"},
		{true, []string{root, "--one-file-system", "--metadata", "names"}, "names metadata tier"},
	} {
		var out bytes.Buffer
		var err error
		if tc.du {
			err = runDu(context.Background(), append(tc.args, "--no-stats"), &out)
		} else {
			err = run(context.Background(), append(tc.args, "-o", filepath.Join(t.TempDir(), "ds")), &out, false)
		}
		if exitOf(err) == exitCompleted || !strings.Contains(fmt.Sprint(err), tc.want) {
			t.Errorf("du=%v %v: exit %d (%v), want %q", tc.du, tc.args, exitOf(err), err, tc.want)
		}
	}
	// --one-file-system lists the tree it stays on, and the manifest says the
	// listing stopped there.
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "--allocated", "--one-file-system", "--format", "tsv", "--no-stats"}, &out); err != nil ||
		!strings.HasPrefix(out.String(), "Depth\tObjects\tDirs\tAllocatedSize\tUnsized\tLastModified\tPrefix\n0\t12\t9\t") || strings.Count(out.String(), "\n") != 2 {
		t.Fatalf("%v %q", err, out.String())
	}
	ds := filepath.Join(t.TempDir(), "ds")
	out.Reset()
	if err := run(context.Background(), []string{root, "-o", ds, "--one-file-system", "--no-stats"}, &out, false); err != nil {
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

// TestDuWarnsOnUnreadable: an entry the listing could not read is not in
// the totals; the run still exits 0, as a listing does, and says so on
// stderr without --stats, but not with -q (no warnings).
func TestDuWarnsOnUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a directory of mode 000")
	}
	root := t.TempDir()
	locked := filepath.Join(root, "locked")
	if err := os.MkdirAll(filepath.Join(locked, "inner"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "f"), []byte("12"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	stderr := captureStderr(t)
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "--format=tsv", "--no-stats", "--no-progress"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "Depth\tObjects\tDirs\tSize\tUnsized\tLastModified\tPrefix\n0\t1\t1\t2\t0\t") ||
		!strings.Contains(stderr.String(), "swath: warning: 1 entry could not be read") {
		t.Fatalf("stdout %q stderr %q", out.String(), stderr.String())
	}
	for _, q := range []string{"-q", "-qq"} {
		stderr.Reset()
		if err := runDu(context.Background(), []string{root, "--format=tsv", q}, &out); err != nil || stderr.Len() != 0 {
			t.Fatalf("%s: %v, stderr %q", q, err, stderr.String())
		}
	}
}
