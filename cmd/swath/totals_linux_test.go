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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/textout"
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

// TestDuMatchesLstat: swath du over a real tree, with and without
// --allocated, counting a hard-linked file once (the default, as du) or
// under every name (-l, --count-links), at several depths, equals the
// brute-force sums of lstat, prefix for prefix, in byte order, in TSV and
// in JSONL (a name that is not UTF-8 as prefix_base64). Before, every name
// counted by default. With --metadata names, which
// reads no links, every name counts.
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
					if !once {
						args = append(args, []string{"--count-links", "-l"}[depth%2])
					}
					want := duOracle(t, root, depth, allocated, once)
					size := "size"
					if allocated {
						size = "allocated_size"
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
					if lines[0] != "depth\tobjects\tdirs\t"+size+"\tunsized\tlast_modified\tprefix" || len(lines)-1 != len(want) {
						t.Fatalf("%d rows for %d prefixes:\n%s", len(lines)-1, len(want), out.String())
					}
					prev := ""
					for i, l := range lines[1:] {
						f := strings.Split(l, "\t")
						if len(f) != 7 || f[4] != "0" || f[0] != strconv.Itoa(strings.Count(f[6], "/")) {
							t.Fatalf("row %q", l)
						}
						n := func(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
						prefix, _, err := textout.UnescapeTSV(f[6])
						if err != nil {
							t.Fatal(err)
						}
						modified, _, _ := textout.UnescapeTSV(f[5]) // "" for a null
						check(prefix, n(f[1]), n(f[2]), n(f[3]), modified, &prev, i == 0)
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
						prefix, text := r["prefix"].(string)
						if b64, ok := r["prefix_base64"].(string); ok == text {
							t.Fatalf("%q: one of prefix and prefix_base64", l)
						} else if ok {
							raw, err := base64.StdEncoding.DecodeString(b64)
							if err != nil || utf8.Valid(raw) {
								t.Fatalf("%q: prefix_base64 of %q (%v)", l, raw, err)
							}
							prefix = string(raw)
						}
						modified, _ := r["last_modified"].(string)
						if r["unsized"] != 0.0 || r["depth"] != float64(strings.Count(prefix, "/")) {
							t.Fatalf("%q: unsized or depth", l)
						}
						check(prefix, int64(r["objects"].(float64)), int64(r["dirs"].(float64)), int64(r[size].(float64)), modified, &prev, rows == 0)
						rows++
					}
					if rows != len(want) {
						t.Fatalf("%d JSONL rows for %d prefixes", rows, len(want))
					}
				})
			}
		}
	}
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "--metadata", "names", "--format=tsv", "--no-stats"}, &out); err != nil {
		t.Fatal(err)
	}
	if w := duOracle(t, root, 0, false, false)[""]; !strings.Contains(out.String(), fmt.Sprintf("\n0\t%d\t%d\t0\t%d\t", w.objects, w.dirs, w.objects)) {
		t.Fatalf("--metadata names: %q, want every name of %+v", out.String(), w)
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

// TestDuInodeLimit: past --max-inodes hard-linked files (counted once by
// default) swath du fails, exit 1, cause inode_limit, with no totals, and
// its error line says what to do: -l, or a larger --max-inodes and what it
// costs.
func TestDuInodeLimit(t *testing.T) {
	stderr := captureStderr(t)
	t.Cleanup(func() { logs.Store(nil) })
	root := duTree(t)
	var out bytes.Buffer
	code := conclude(runDu(context.Background(), []string{root, "--max-inodes", "1", "--format=tsv", "--no-stats"}, &out))
	const want = "swath: the tree has more than 1 file with several hard links, which swath du counts once each: run it again with -l to count every name, or with --max-inodes N to remember more of them (about 100 MB a million)\n"
	if code != exitFatal || out.Len() != 0 || !strings.HasSuffix(stderr.String(), want) {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, out.String(), stderr.String())
	}
	if err := runDu(context.Background(), []string{root, "--max-inodes", "1", "-l", "--format=tsv", "--no-stats"}, &out); err != nil {
		t.Fatalf("-l: %v", err)
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
		{true, []string{"gs://bkt", "--count-links", "--depth", "1"}, "--count-links applies to Local sources, not to GCS sources"},
		{true, []string{"az://acct/ctr", "--max-inodes", "5"}, "--max-inodes applies to Local sources, not to Azure Blob sources"},
		{true, []string{"s3://bkt", "--one-file-system"}, "--one-file-system applies to Local sources, not to S3 sources"},
		{false, []string{"az://acct/ctr", "--one-file-system"}, "--one-file-system applies to Local sources, not to Azure Blob sources"},
		{false, []string{root, "--allocated"}, "--allocated has no meaning for swath list"},
		{false, []string{root, "-l"}, "--count-links has no meaning for swath list"},
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
		!strings.HasPrefix(out.String(), "depth\tobjects\tdirs\tallocated_size\tunsized\tlast_modified\tprefix\n0\t8\t9\t") || strings.Count(out.String(), "\n") != 2 { // hard links once
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

// TestUnreadableIsIncomplete: an entry the listing could not read is not in
// the totals, the text rows or the dataset; each run finishes with exit 3
// (class incomplete), names the entry with its cause as a warning and ends
// with the count as its error line, which -q keeps and -qq drops. The
// committed dataset's coverage counts it (unreadable 1) beside complete
// true, its listing having reached the end of the tree. Before, each exited
// 0, and -q left no trace of the missing entry.
func TestUnreadableIsIncomplete(t *testing.T) {
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
	t.Cleanup(func() { logs.Store(nil) })
	const end = "swath: 1 entry could not be read and is missing from the output\n"
	warning := "swath: warning: unreadable entries:\n  " + locked + ": permission denied\n"
	var out bytes.Buffer
	report := filepath.Join(t.TempDir(), "report.json")
	err := runDu(context.Background(), []string{root, "--format=tsv", "--no-stats", "--no-progress", "--report", report}, &out)
	// D7: the entry is named with its cause, and the report says the
	// totals are short although the listing completed.
	if code := conclude(err); code != exitIncomplete ||
		!strings.HasPrefix(out.String(), "depth\tobjects\tdirs\tsize\tunsized\tlast_modified\tprefix\n0\t1\t1\t2\t0\t") ||
		!strings.HasSuffix(stderr.String(), warning+end) {
		t.Fatalf("du: exit %d, stdout %q stderr %q", code, out.String(), stderr.String())
	}
	var rep listing.Report
	readJSON(t, report, &rep)
	if o := rep.Outcome; o.Unreadable != 1 || !o.Complete || o.ExitCode != exitIncomplete || o.Class != listing.ClassIncomplete || o.Resumable {
		t.Fatalf("report outcome %+v", o)
	}
	// A listing, text or a dataset, ends the same way.
	stderr.Reset()
	if code := conclude(run(context.Background(), []string{root, "--format=tsv", "--no-stats", "--no-progress"}, io.Discard, false)); code != exitIncomplete ||
		!strings.HasSuffix(stderr.String(), warning+end) {
		t.Fatalf("list: exit %d, stderr %q", code, stderr.String())
	}
	stderr.Reset()
	dir := filepath.Join(t.TempDir(), "ds")
	if code := conclude(run(context.Background(), []string{root, "-o", dir, "--no-stats", "--no-progress"}, io.Discard, false)); code != exitIncomplete ||
		!strings.HasSuffix(stderr.String(), warning+end) {
		t.Fatalf("list -o: exit %d, stderr %q", code, stderr.String())
	}
	var m dataset.Manifest
	var o outcome
	readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
	readJSON(t, filepath.Join(dir, errorName), &o)
	if c := m.Coverage; c.Unreadable != 1 || !c.Complete || c.StopReason != "completed" || o.Class != listing.ClassIncomplete || o.ExitCode != exitIncomplete {
		t.Fatalf("dataset: coverage %+v, error.json %+v", c, o)
	}
	out.Reset()
	if err := show([]string{dir}, &out); err != nil || !strings.Contains(out.String(), "coverage complete=true stop=completed unreadable=1 ") {
		t.Fatalf("show: %v %q", err, out.String())
	}
	// swath resume of it exits 3, and its report keeps the count and says
	// why (it said unreadable 0 and "already complete").
	stderr.Reset()
	resumed := filepath.Join(t.TempDir(), "resume.json")
	err = resume(context.Background(), []string{dir, "--report", resumed}, io.Discard)
	want := dir + " left out an entry it could not read (coverage unreadable=1): it cannot be resumed; list the tree again once it can be read"
	if code := conclude(err); code != exitIncomplete || err.Error() != want {
		t.Fatalf("resume: exit %d (%v)", code, err)
	}
	readJSON(t, resumed, &rep)
	if o := rep.Outcome; o.Unreadable != 1 || !o.Complete || o.StopReason != "completed" || o.ExitCode != exitIncomplete || o.Error != want {
		t.Fatalf("resume report outcome %+v", o)
	}
	// swath diag count prints its partial counts, then exits 3 (it printed
	// nothing).
	stderr.Reset()
	out.Reset()
	if code := conclude(runCount(context.Background(), []string{root, "--no-stats"}, &out)); code != exitIncomplete ||
		!strings.Contains(out.String(), " rows=") || !strings.HasSuffix(stderr.String(), end) {
		t.Fatalf("diag count: exit %d, stdout %q, stderr %q", code, out.String(), stderr.String())
	}
	// A reader that closed the stream took what it wanted: exit 0, as
	// documented for a closed stdout, the unreadable entry notwithstanding
	// (it exited 3).
	if err := runDu(context.Background(), []string{root, "--format=tsv", "--no-stats", "--report", report}, failingWriter{syscall.EPIPE}); err != nil {
		t.Fatalf("du into a closed pipe: %v", err)
	}
	readJSON(t, report, &rep)
	if o := rep.Outcome; o.ExitCode != exitCompleted || o.StopReason != listing.StopDownstreamClosed || o.Unreadable != 1 {
		t.Fatalf("du into a closed pipe: outcome %+v", o)
	}
	// -q keeps the error line, -qq drops it; the exit code stays.
	for q, want := range map[string]string{"-q": end, "-qq": ""} {
		stderr.Reset()
		if code := conclude(runDu(context.Background(), []string{root, "--format=tsv", q}, &out)); code != exitIncomplete || stderr.String() != want {
			t.Fatalf("%s: exit %d, stderr %q", q, code, stderr.String())
		}
	}
}

// gatedWriter is a stderr pipe that is full: writes wait until open
// closes, then go through.
type gatedWriter struct {
	open chan struct{}
	mu   sync.Mutex
	b    bytes.Buffer
}

func (g *gatedWriter) Write(p []byte) (int, error) {
	<-g.open
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.b.Write(p)
}

// TestWarningOutlastsBacklog: with stderr full while a du run with an
// unreadable entry ends, and drained once its report is written, the
// unreadable entries' warning and the outcome both arrive, in that order:
// the outcome's priority drops the disposable lines only.
func TestWarningOutlastsBacklog(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a directory of mode 000")
	}
	root := t.TempDir()
	locked := filepath.Join(root, "locked")
	if err := os.MkdirAll(filepath.Join(locked, "inner"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	g := &gatedWriter{open: make(chan struct{})}
	errOut = g
	t.Cleanup(func() { errOut = os.Stderr; logs.Store(nil) })
	report := filepath.Join(t.TempDir(), "report.json")
	go func() {
		for {
			if _, err := os.Stat(report); err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(10 * time.Millisecond)
		close(g.open)
	}()
	conclude(runDu(context.Background(), []string{root, "--format=tsv", "--no-stats", "--no-progress", "-v", "--log-format=json", "--report", report}, io.Discard))
	g.mu.Lock()
	got := g.b.String()
	g.mu.Unlock()
	w, o := strings.Index(got, `"msg":"unreadable entries"`), strings.Index(got, `"event":"outcome"`)
	if w < 0 || o < 0 || w > o {
		t.Fatalf("stderr %q", got)
	}
}
