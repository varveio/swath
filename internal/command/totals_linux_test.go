// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package command

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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/inventory"
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
	objects, dirs, size, alloc, newest int64
	hasNewest                          bool
}

// duOracle walks root with lstat and sums every prefix down to depth by
// brute force: each non-directory is an object (its size and its blocks ×
// 512) under every prefix of its key; with once, a file of several links
// counts under its smallest key only, its time too; a directory adds
// its own prefix and counts in its parents' dirs.
func duOracle(t *testing.T, root string, depth int, once bool) map[string]duSums {
	t.Helper()
	type obj struct {
		key                string
		size, alloc, mtime int64
		dir, multi         bool
		dev, ino           uint64
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
		o := obj{key: filepath.ToSlash(rel), size: st.Size, alloc: st.Blocks * 512, mtime: st.Mtim.Sec*1_000_000 + st.Mtim.Nsec/1000, dir: d.IsDir(),
			multi: !d.IsDir() && st.Nlink > 1, dev: uint64(st.Dev), ino: st.Ino}
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
			if o.dir { // its own blocks, in its prefix and its parents', as du -s
				s.alloc += o.alloc
			}
			if counts {
				s.objects++
				s.size += o.size
				s.alloc += o.alloc
				if !s.hasNewest || o.mtime > s.newest {
					s.newest, s.hasNewest = o.mtime, true
				}
			}
			sums[p] = s
		}
	}
	return sums
}

// TestDuMatchesLstat: swath du over a real tree with a sparse file and hard
// links, counting a hard-linked file once (the default, as du) or under
// every name (-l, --count-links), at several depths, equals the
// brute-force sums of lstat, prefix for prefix, in byte order, in TSV and
// in JSONL (a name that is not UTF-8 as prefix_base64): both figures in
// every row, the apparent size (the files') and the allocated bytes (the
// files' and the directories' own, as du -s), whichever is the primary
// (the allocated bytes by default and with --allocated, the size with
// --apparent-size or -A), and the row says which, and whether links
// counted once. With --metadata names, which reads no links, every name
// counts.
func TestDuMatchesLstat(t *testing.T) {
	root := duTree(t)
	for _, depth := range []int{0, 1, 2, 4} {
		for _, figure := range []string{"", "--allocated", "--apparent-size"} {
			for _, once := range []bool{false, true} {
				t.Run(fmt.Sprintf("depth%d/figure=%s/once=%v", depth, figure, once), func(t *testing.T) {
					args := []string{root, "-d", strconv.Itoa(depth), "--no-stats", "--workers", "3"}
					primary := "allocated_bytes"
					switch {
					case figure == "--apparent-size" && depth%2 == 1:
						args, primary = append(args, "-A"), "size"
					case figure == "--apparent-size":
						args, primary = append(args, figure), "size"
					case figure != "":
						args = append(args, figure)
					}
					if !once {
						args = append(args, []string{"--count-links", "-l"}[depth%2])
					}
					want := duOracle(t, root, depth, once)
					check := func(prefix string, objects, dirs, size, alloc int64, modified string, prev *string, first bool) {
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
						if !ok || objects != w.objects || dirs != w.dirs || size != w.size || alloc != w.alloc || (modified != "") != w.hasNewest || newest != w.newest {
							t.Errorf("%q: got %d objects, %d dirs, %d bytes, %d allocated, newest %q; want %+v", prefix, objects, dirs, size, alloc, modified, w)
						}
					}
					var out bytes.Buffer
					if err := runDu(context.Background(), append(args, "--format", "tsv"), &out); err != nil {
						t.Fatal(err)
					}
					rows := duTSV(t, out.String())
					if !strings.HasPrefix(out.String(), "row_type\tdepth\tobjects\tdirs\tsize\tsize_missing\tallocated_bytes\tallocated_bytes_missing\tmax_last_modified\tprimary\tsort\tlinks_once\tprefix\n") ||
						len(rows) != len(want) {
						t.Fatalf("%d rows for %d prefixes:\n%s", len(rows), len(want), out.String())
					}
					prev := ""
					for i, r := range rows {
						prefix, _, err := textout.UnescapeTSV(r["prefix"])
						if err != nil {
							t.Fatal(err)
						}
						if r["size_missing"] != "0" || r["allocated_bytes_missing"] != "0" || r["depth"] != strconv.Itoa(strings.Count(prefix, "/")) ||
							r["row_type"] != map[bool]string{true: "total", false: "prefix"}[i == 0] || r["primary"] != primary || r["sort"] != "name" ||
							r["links_once"] != strconv.FormatBool(once) {
							t.Fatalf("row %v", r)
						}
						n := func(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
						modified, _, _ := textout.UnescapeTSV(r["max_last_modified"]) // "" for a null
						check(prefix, n(r["objects"]), n(r["dirs"]), n(r["size"]), n(r["allocated_bytes"]), modified, &prev, i == 0)
					}
					// JSONL: the same rows, each line JSON; the prefix as
					// text when it is UTF-8, else its exact bytes in base64.
					out.Reset()
					if err := runDu(context.Background(), append(args, "--format", "jsonl"), &out); err != nil {
						t.Fatal(err)
					}
					n := 0
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
						modified, _ := r["max_last_modified"].(string)
						if r["size_missing"] != 0.0 || r["allocated_bytes_missing"] != 0.0 || r["depth"] != float64(strings.Count(prefix, "/")) ||
							r["primary"] != primary || r["links_once"] != once {
							t.Fatalf("%q: missing values, depth or context", l)
						}
						check(prefix, int64(r["objects"].(float64)), int64(r["dirs"].(float64)), int64(r["size"].(float64)), int64(r["allocated_bytes"].(float64)), modified, &prev, n == 0)
						n++
					}
					if n != len(want) {
						t.Fatalf("%d JSONL rows for %d prefixes", n, len(want))
					}
				})
			}
		}
	}
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "--metadata", "names", "--format=tsv", "--no-stats"}, &out); err != nil {
		t.Fatal(err)
	}
	// Every name counts, and every entry lacks blocks: the files and the
	// directories.
	if w := duOracle(t, root, 0, false)[""]; !strings.Contains(out.String(), fmt.Sprintf("\ntotal\t0\t%d\t%d\t\\N\t%d\t\\N\t%d\t", w.objects, w.dirs, w.objects, w.objects+w.dirs)) {
		t.Fatalf("--metadata names: %q, want every name of %+v", out.String(), w)
	}
	// The sparse file is where the two figures part: both in one row,
	// whichever comes first.
	for _, primary := range []string{"--apparent-size", "--allocated"} {
		out.Reset()
		if err := runDu(context.Background(), []string{filepath.Join(root, "s"), primary, "--format=tsv", "--no-stats"}, &out); err != nil {
			t.Fatal(err)
		}
		r := duTSV(t, out.String())[0]
		if alloc, _ := strconv.ParseInt(r["allocated_bytes"], 10, 64); r["size"] != "8388612" || alloc >= 8388612 {
			t.Fatalf("sparse %s: %v", primary, r)
		}
	}
	// --sort size orders by the primary figure: the sparse file's s/ is the
	// largest by length, a/ by allocated bytes.
	for primary, first := range map[string]string{"--apparent-size": "s/", "--allocated": "a/"} {
		out.Reset()
		if err := runDu(context.Background(), []string{root, "-d", "1", "--sort", "size", primary, "--format=tsv", "--no-stats"}, &out); err != nil {
			t.Fatal(err)
		}
		if r := duTSV(t, out.String()); r[1]["prefix"] != first || r[1]["sort"] != "size" {
			t.Fatalf("%s: by size %v", primary, r)
		}
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
		if exitOf(err) == exitOK || !strings.Contains(fmt.Sprint(err), tc.want) {
			t.Errorf("du=%v %v: exit %d (%v), want %q", tc.du, tc.args, exitOf(err), err, tc.want)
		}
	}
	// --one-file-system lists the tree it stays on, and the manifest says the
	// listing stopped there.
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "--allocated", "--one-file-system", "--format", "tsv", "--no-stats"}, &out); err != nil ||
		!strings.HasPrefix(out.String(), "row_type\tdepth\tobjects\tdirs\tsize\tsize_missing\tallocated_bytes\tallocated_bytes_missing\tmax_last_modified\tprimary\tsort\tlinks_once\tprefix\ntotal\t0\t8\t9\t") ||
		!strings.Contains(out.String(), "\tallocated_bytes\tname\ttrue\t\n") || strings.Count(out.String(), "\n") != 2 { // hard links once
		t.Fatalf("%v %q", err, out.String())
	}
	ds := filepath.Join(t.TempDir(), "ds")
	out.Reset()
	if err := run(context.Background(), []string{root, "-o", ds, "--one-file-system", "--no-stats"}, &out, false); err != nil {
		t.Fatal(err)
	}
	var m struct {
		Extensions struct {
			Swath struct {
				Coverage struct{ Filters []map[string]any }
			}
		}
	}
	readJSON(t, filepath.Join(ds, "manifest.json"), &m)
	if f := m.Extensions.Swath.Coverage.Filters; len(f) != 1 || len(f[0]) != 2 || f[0]["filter"] != "one_file_system" || f[0]["value"] != nil {
		t.Fatalf("coverage.filters %q", f)
	}
}

// TestUnreadableIsIncomplete: an entry the listing could not read is not in
// the totals, the text rows or the dataset; each run finishes with exit 3
// (class incomplete), names the entry with its cause as a warning and ends
// with the count as its error line, which -q keeps and -qq drops. The
// committed dataset's coverage counts it (unreadable 1) with complete false
// and stop_reason completed (its listing reached the end of the tree, and
// an entry is missing). The closing block's verdict carries the cause
// after the warning that names it, and the outcome event says partial.
// Before, each exited 0, and -q left no trace of the missing entry.
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
	warning := "swath: warning: unreadable entries:\n  locked: permission denied\n" // relative to SOURCE
	var out bytes.Buffer
	report := filepath.Join(t.TempDir(), "report.json")
	err := runDu(context.Background(), []string{root, "--format=tsv", "--no-stats", "--no-progress", "--report", report}, &out)
	// D7: the entry is named with its cause, and the report says the
	// totals are short although the listing completed.
	if code := conclude(err); code != exitIncomplete ||
		!strings.Contains(out.String(), "\ntotal\t0\t1\t1\t2\t0\t") ||
		!strings.HasSuffix(stderr.String(), warning+end) {
		t.Fatalf("du: exit %d, stdout %q stderr %q", code, out.String(), stderr.String())
	}
	var rep inventory.Report
	readJSON(t, report, &rep)
	if o := rep.Outcome; o.Unreadables() != 1 || o.Complete || o.StopReason != "end_of_scope" || o.ExitCode != exitIncomplete || o.Class != inventory.ClassIncomplete || o.Resumable {
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
	// The dataset is committed and partial: its coverage says so, and no
	// error.json stands beside it.
	var m dataset.Manifest
	readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
	_, eerr := os.Stat(filepath.Join(dir, errorName))
	if c := m.Coverage; c.Unreadable != 1 || c.Complete || c.StopReason != "end_of_scope" || !os.IsNotExist(eerr) {
		t.Fatalf("dataset: coverage %+v, error.json %v", c, eerr)
	}
	out.Reset()
	if err := show([]string{dir}, &out); err != nil || !strings.HasPrefix(out.String(), dir+": partial: 1 unreadable entry\n") ||
		!strings.Contains(out.String(), "\n  coverage  1 entry unreadable; ") {
		t.Fatalf("show: %v %q", err, out.String())
	}
	// swath resume of it exits 3, and its report keeps the count and says
	// why (it said unreadable 0 and "already complete").
	stderr.Reset()
	resumed := filepath.Join(t.TempDir(), "resume.json")
	err = resume(context.Background(), []string{dir, "--report", resumed}, io.Discard)
	want := dir + " is partial: 1 unreadable entry: it cannot be resumed; list the tree again once it can be read"
	if code := conclude(err); code != exitIncomplete || err.Error() != want {
		t.Fatalf("resume: exit %d (%v)", code, err)
	}
	rep = inventory.Report{}
	readJSON(t, resumed, &rep)
	if o := rep.Outcome; o.Unreadables() != 1 || o.Complete || o.StopReason != "end_of_scope" || o.ExitCode != exitIncomplete || o.Error != want {
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
	rep = inventory.Report{}
	readJSON(t, report, &rep)
	if o := rep.Outcome; o.ExitCode != exitOK || o.Class != inventory.ClassOK || o.Complete || o.StopReason != inventory.StopDownstreamClosed || o.Unreadables() != 1 {
		t.Fatalf("du into a closed pipe: outcome %+v", o)
	}
	// -q keeps the error line, -qq drops it; the exit code stays.
	for q, want := range map[string]string{"-q": end, "-qq": ""} {
		stderr.Reset()
		if code := conclude(runDu(context.Background(), []string{root, "--format=tsv", q}, &out)); code != exitIncomplete || stderr.String() != want {
			t.Fatalf("%s: exit %d, stderr %q", q, code, stderr.String())
		}
	}
	// The closing lines: the warning, then the verdict naming what is
	// missing with the total row's counts, and no error line saying it
	// again.
	stderr.Reset()
	if code := conclude(runDu(context.Background(), []string{root, "--format=tsv", "--stats", "--no-progress", "-A"}, &out)); code != exitIncomplete ||
		!strings.HasPrefix(stderr.String(), warning+"  partial: 1 unreadable entry; 1 file, 1 directory, 2 B in ") || strings.Contains(stderr.String(), end) {
		t.Fatalf("--stats: exit %d, stderr %q", code, stderr.String())
	}
	// The table marks the directory it could not read, not as empty.
	out.Reset()
	if code := conclude(runDu(context.Background(), []string{root, "-d", "1", "--format=table", "-q"}, &out)); code != exitIncomplete ||
		!regexp.MustCompile(`(?m)^ +- +- +- +- +locked/$`).MatchString(out.String()) {
		t.Fatalf("table: exit %d, stdout %q", code, out.String())
	}
	// The outcome event tells partial from complete without parsing msg.
	stderr.Reset()
	if code := conclude(run(context.Background(), []string{root, "-o", filepath.Join(t.TempDir(), "ds"), "--log-format", "json"}, io.Discard, false)); code != exitIncomplete {
		t.Fatalf("json: exit %d", code)
	}
	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	var ev map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &ev); err != nil || ev["event"] != "outcome" || ev["class"] != "incomplete" || ev["status"] != "committed" ||
		ev["complete"] != false || ev["stop_reason"] != "end_of_scope" || ev["unreadable"] != 1.0 || ev["msg"] != strings.TrimSuffix(strings.TrimPrefix(end, "swath: "), "\n") || ev["message"] != nil {
		t.Fatalf("outcome event %v (%v): %s", ev, err, stderr.String())
	}
	stderr.Reset()
	if code := conclude(runDu(context.Background(), []string{root, "--format=tsv", "--exclude", "locked/**", "--exclude", "locked", "--log-format", "json"}, &out)); code != exitOK {
		t.Fatalf("json, nothing missing: exit %d: %s", code, stderr.String())
	}
	lines = strings.Split(strings.TrimSpace(stderr.String()), "\n")
	ev = nil
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &ev); err != nil || ev["class"] != "ok" || ev["status"] != "listed" || ev["complete"] != true ||
		ev["stop_reason"] != "end_of_scope" || ev["unreadable"] != 0.0 {
		t.Fatalf("outcome event %v (%v): %s", ev, err, stderr.String())
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

// TestDuViewUnreadable: on a short terminal with unreadable directories
// the view leaves room for everything printed after it, so its heading and
// total stay on screen: it marks with "!" each line whose sums leave out an
// unreadable directory (a fold too), one line under it says so (-v names
// them relative to SOURCE), and the closing line's verdict is not
// repeated as an error line.
func TestDuViewUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads every directory")
	}
	root := t.TempDir()
	files := map[string]int{"big/f": 3_000_000, "sub/a/x": 500_000}
	for i := 1; i <= 12; i++ {
		files[fmt.Sprintf("d%02d/f", i)] = 1000 * i
	}
	for i := 1; i <= 5; i++ {
		files[fmt.Sprintf("locked%d/inner/f", i)] = 3
	}
	for name, n := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 5; i++ {
		locked := filepath.Join(root, fmt.Sprintf("locked%d", i))
		if err := os.Chmod(locked, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	}
	t.Setenv("NO_COLOR", "1")
	viewTerminal(t, 100, 22)
	stderr := captureStderr(t)
	t.Cleanup(func() { logs.Store(nil) })
	var out bytes.Buffer
	if code := conclude(runDu(context.Background(), []string{root, "--no-progress", "-A"}, &out)); code != exitIncomplete { // the lengths, which the test sets
		t.Fatalf("exit %d", code)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	used := len(lines) + 1 // and the prompt
	for _, l := range strings.Split(strings.TrimSuffix(stderr.String(), "\n"), "\n") {
		used += max(1, (utf8.RuneCountInString(l)+99)/100)
	}
	marked := regexp.MustCompile(`(?m)^ *[0-9.]+ [KMG]?i?B !`)
	if used > 22 || !strings.HasPrefix(lines[0], "    size") || !marked.MatchString(lines[1]) ||
		!regexp.MustCompile(`(?m)^ *[0-9.]+ [KMG]?i?B ! .*\(\d+ more\)$`).MatchString(out.String()) ||
		!strings.Contains(stderr.String(), "\n! 5 directories could not be read (marked ! above); -v lists them\n") ||
		!strings.Contains(stderr.String(), "  partial: 5 unreadable entries;") || strings.Contains(stderr.String(), "could not be read and are missing") {
		t.Fatalf("%d rows of 22\nstdout\n%s\nstderr\n%s", used, out.String(), stderr.String())
	}
	// -v names them, relative to SOURCE.
	stderr.Reset()
	if code := conclude(runDu(context.Background(), []string{root, "--no-progress", "-v"}, io.Discard)); code != exitIncomplete ||
		!strings.Contains(stderr.String(), "\n  locked1: permission denied\n") || strings.Contains(stderr.String(), root+"/locked") {
		t.Fatalf("-v: exit %d\n%s", code, stderr.String())
	}
}
