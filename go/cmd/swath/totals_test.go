// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// depthTree is a small local tree with known sizes and times: a/b/f (5
// bytes), a/g (2), c/h (1), x (1) at the top, and an empty directory e.
func depthTree(t *testing.T) (root string, at time.Time) {
	t.Helper()
	root = t.TempDir()
	at = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, d := range []string{"a/b", "c", "e"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, body := range map[string]string{"a/b/f": "12345", "a/g": "12", "c/h": "1", "x": "1"} {
		p := filepath.Join(root, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		mt := at
		if name == "a/b/f" {
			mt = at.Add(time.Hour) // the newest, under a/ and a/b/
		}
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	return root, at
}

// TestDu: swath du -d N prints each prefix's files, directories, bytes and
// newest time, du-style (a parent includes its children; a directory
// counts in its parents' Dirs, an empty one is listed), sorted by prefix
// with the total first; the run report says the path engaged.
func TestDu(t *testing.T) {
	root, _ := depthTree(t)
	report := filepath.Join(t.TempDir(), "r.json")
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "--depth", "2", "--format", "tsv", "--no-stats", "--report", report}, &out); err != nil {
		t.Fatal(err)
	}
	const newest, older = "2026-01-02T04:04:05.000000Z", "2026-01-02T03:04:05.000000Z"
	want := "depth\tobjects\tdirs\tsize\tunsized\tlast_modified\tprefix\n" +
		"0\t4\t4\t9\t0\t" + newest + "\t\n" +
		"1\t2\t1\t7\t0\t" + newest + "\ta/\n" +
		"2\t1\t0\t5\t0\t" + newest + "\ta/b/\n" +
		"1\t1\t0\t1\t0\t" + older + "\tc/\n" +
		"1\t0\t0\t0\t0\t\\N\te/\n"
	if out.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", out.String(), want)
	}
	// A row includes its descendants: the depth-1 rows leave out x, the
	// file at the top, and all rows together count a/b/f three times; only
	// Depth 0 is the total.
	sums := map[string]int{}
	for l := range strings.Lines(out.String()) {
		f := strings.Split(l, "\t")
		if n, err := strconv.Atoi(f[3]); err == nil {
			sums[f[0]] += n
			sums["all"] += n
		}
	}
	if sums["0"] != 9 || sums["1"] != 8 || sums["all"] != 22 {
		t.Fatalf("sums %v", sums)
	}
	var r listing.Report
	readJSON(t, report, &r)
	tt := r.Output.Totals
	if tt == nil || tt.Depth != 2 || tt.Prefixes != 5 || tt.Objects != 4 || tt.Dirs != 4 || tt.Bytes != 9 || tt.NotObjects != 4 || tt.BytesColumn != "size" ||
		r.Output.RowsEmitted != 8 || r.Invocation.Argv[0] != "du" {
		t.Fatalf("report totals %+v, rows %d, argv %q", tt, r.Output.RowsEmitted, r.Invocation.Argv)
	}
	// The table: a heading, sizes in binary units, the newest time to the
	// second in local time, the prefix last, and the total named and last,
	// as du's; -d is --depth.
	out.Reset()
	if err := runDu(context.Background(), []string{root, "-d", "1", "--format=table", "--no-stats"}, &out); err != nil {
		t.Fatal(err)
	}
	local := func(s string) string {
		at, _ := time.Parse(time.RFC3339, s)
		return at.In(time.Local).Format(time.DateTime)
	}
	wantTable := "" +
		"Size  Files  Dirs  LastModified (local)  Path\n" +
		" 7 B      2     1  " + local(newest) + "   a/\n" +
		" 1 B      1     0  " + local(older) + "   c/\n" +
		" 0 B      0     0                        e/\n" +
		" 9 B      4     4  " + local(newest) + "   (total)\n"
	if out.String() != wantTable {
		t.Fatalf("table\n%s\nwant\n%s", out.String(), wantTable)
	}
	// Without -d, the total alone; swath diag count keeps its one line.
	out.Reset()
	if err := runDu(context.Background(), []string{root, "--no-stats", "--format=tsv"}, &out); err != nil ||
		out.String() != "depth\tobjects\tdirs\tsize\tunsized\tlast_modified\tprefix\n0\t4\t4\t9\t0\t"+newest+"\t\n" {
		t.Fatalf("%v %q", err, out.String())
	}
	out.Reset()
	if err := runCount(context.Background(), []string{root, "--no-stats"}, &out); err != nil || !strings.HasPrefix(out.String(), "files=4 dirs=4 ") ||
		!strings.Contains(out.String(), "rows=8 object_bytes=9 ") { // the files' bytes: a directory's own size is no object's
		t.Fatalf("%v %q", err, out.String())
	}
}

// TestObjectBytesLeaveOutDirs: a dataset run's object_bytes is the apparent
// bytes of the files it accepted after the filters: a directory's own size
// is no object's.
func TestObjectBytesLeaveOutDirs(t *testing.T) {
	root, _ := depthTree(t)
	report := filepath.Join(t.TempDir(), "r.json")
	var out bytes.Buffer
	if err := run(context.Background(), []string{root, "-o", filepath.Join(t.TempDir(), "ds"), "--exclude", "^c/", "--no-stats", "--report", report}, &out, false); err != nil {
		t.Fatal(err)
	}
	var r listing.Report
	readJSON(t, report, &r)
	if b := r.Output.ObjectBytes; b == nil || *b != 8 || r.Output.RowsEmitted != 6 { // a/b/f, a/g, x; a/, a/b/, e/
		t.Fatalf("object_bytes %v, rows %d", b, r.Output.RowsEmitted)
	}
}

// TestDuUnsized: without sizes (the names metadata tier) the files a
// prefix's Size leaves out are its Unsized, as rclone size's sizeless; the
// table shows such a size as "-", unknown; an empty directory has 0 bytes.
func TestDuUnsized(t *testing.T) {
	root, _ := depthTree(t)
	for format, want := range map[string]string{
		"tsv":   "depth\tobjects\tdirs\tsize\tunsized\tlast_modified\tprefix\n0\t4\t4\t0\t4\t\\N\t\n1\t2\t1\t0\t2\t\\N\ta/\n1\t1\t0\t0\t1\t\\N\tc/\n1\t0\t0\t0\t0\t\\N\te/\n",
		"jsonl": `{"depth":0,"prefix":"","objects":4,"dirs":4,"size":0,"unsized":4,"last_modified":null}` + "\n" + `{"depth":1,"prefix":"a/","objects":2,"dirs":1,"size":0,"unsized":2,"last_modified":null}` + "\n" + `{"depth":1,"prefix":"c/","objects":1,"dirs":0,"size":0,"unsized":1,"last_modified":null}` + "\n" + `{"depth":1,"prefix":"e/","objects":0,"dirs":0,"size":0,"unsized":0,"last_modified":null}` + "\n",
		"table": "Size  Files  Dirs  LastModified (local)  Path\n   -      2     1                        a/\n   -      1     0                        c/\n 0 B      0     0                        e/\n   -      4     4                        (total)\n",
	} {
		var out bytes.Buffer
		if err := runDu(context.Background(), []string{root, "-d", "1", "--metadata", "names", "--format", format, "--no-stats"}, &out); err != nil {
			t.Fatal(err)
		}
		if out.String() != want {
			t.Errorf("%s:\n%s\nwant\n%s", format, out.String(), want)
		}
	}
}

// TestDuJSONL: an object store's rows carry no Dirs, and the table calls
// its objects objects; depth counts from the prefix the source names.
func TestDuJSONL(t *testing.T) {
	src, keys := textBucket(t, 50)
	var out bytes.Buffer
	if err := runDu(context.Background(), append(src, "--depth", "1", "--format", "jsonl", "--no-stats"), &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	var size int
	for _, k := range keys {
		size += len(k) // the fake's object bodies are their keys
	}
	if !strings.HasPrefix(lines[0], `{"depth":0,"prefix":"","objects":50,"size":`+strconv.Itoa(size)+`,"last_modified":"`) || len(lines) < 2 || !strings.HasPrefix(lines[1], `{"depth":1,"prefix":"`) {
		t.Fatalf("%q", lines)
	}
	out.Reset()
	if err := runDu(context.Background(), append(src, "--format", "table", "--no-stats"), &out); err != nil || !strings.Contains(out.String(), "  Objects  LastModified (local)") {
		t.Fatalf("%v %q", err, out.String())
	}
}

// TestDuOrigin: depth counts from the prefix the source names, up to its
// last '/', and the rows keep the keys' spelling; an --include that
// narrows the listed prefix does not move it.
func TestDuOrigin(t *testing.T) {
	keys := []string{"logs/2026/01/a", "logs/2026/01/b/c", "logs/2026/02/d", "logs/2026/top", "logs/2027/x", "other/y"}
	end := s3test.Bucket(t, keys)
	du := func(args ...string) string {
		t.Helper()
		var out bytes.Buffer
		if err := runDu(context.Background(), append(args, "--endpoint", end, "--anonymous", "--format", "tsv", "--no-stats"), &out); err != nil {
			t.Fatal(err)
		}
		var rows []string
		for l := range strings.Lines(out.String()) {
			f := strings.Split(l, "\t")
			rows = append(rows, f[0]+":"+strings.TrimSuffix(f[len(f)-1], "\n")+" "+f[1]) // depth:prefix (last) objects
		}
		return strings.Join(rows[1:], ",")
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"s3://bkt/logs/2026/", "-d", "1"}, "0:logs/2026/ 4,1:logs/2026/01/ 2,1:logs/2026/02/ 1"},
		{[]string{"s3://bkt/logs/2026/0", "-d", "1"}, "0:logs/2026/ 3,1:logs/2026/01/ 2,1:logs/2026/02/ 1"},
		{[]string{"s3://bkt/logs", "-d", "1"}, "0: 5,1:logs/ 5"},
		{[]string{"s3://bkt/logs/", "-d", "1", "--include", "^logs/2026/"}, "0:logs/ 4,1:logs/2026/ 4"},
		{[]string{"s3://bkt", "-d", "2"}, "0: 6,1:logs/ 5,2:logs/2026/ 4,2:logs/2027/ 1,1:other/ 1"},
	} {
		if got := du(tc.args...); got != tc.want {
			t.Errorf("%v: %q, want %q", tc.args, got, tc.want)
		}
	}
}

// TestDuRefusals: the totals' flags are swath du's, --format names a
// format, and stdout carries the rows, so the report goes to a file.
func TestDuRefusals(t *testing.T) {
	root, _ := depthTree(t)
	for _, tc := range []struct {
		du   bool
		args []string
		want string
	}{
		{false, []string{"--depth", "1"}, "--depth has no meaning for swath list"},
		{false, []string{"--max-prefixes", "9"}, "--max-prefixes has no meaning for swath list"},
		{true, []string{"--depth", "-1"}, "--depth -1: negative"},
		{true, []string{"--max-prefixes", "0"}, "--max-prefixes 0"},
		{true, []string{"--max-inodes", "0"}, "--max-inodes 0"},
		{true, []string{"--format", "parquet"}, "want auto, table, tsv or jsonl"},
		{true, []string{"--report-format", "json"}, "use --report FILE"},
		{true, []string{"--seed=false"}, "give it to swath diag count or swath diag list"},
	} {
		var out bytes.Buffer
		var err error
		if tc.du {
			err = runDu(context.Background(), append([]string{root}, tc.args...), &out)
		} else {
			err = run(context.Background(), append([]string{root, "-o", filepath.Join(t.TempDir(), "ds")}, tc.args...), &out, false)
		}
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("du=%v %v: exit %d (%v), want %q", tc.du, tc.args, exitOf(err), err, tc.want)
		}
	}
	var out bytes.Buffer
	if err := dispatch(context.Background(), []string{"count", root}, &out); exitOf(err) != exitUsage || !strings.Contains(err.Error(), `unknown command "count"`) {
		t.Errorf("swath count: exit %d (%v)", exitOf(err), err)
	}
}

// TestDuCap: past --max-prefixes swath du fails (exit 1, cause
// prefix_limit) with advice, prints no partial rows, and the report says
// how far it got.
func TestDuCap(t *testing.T) {
	root, _ := depthTree(t)
	report := filepath.Join(t.TempDir(), "r.json")
	var out bytes.Buffer
	err := runDu(context.Background(), []string{root, "--depth", "1", "--max-prefixes", "2", "--no-stats", "--report", report}, &out)
	if exitOf(err) != exitFatal || !strings.Contains(err.Error(), "more than 2 distinct prefixes at depth 1") || !strings.Contains(err.Error(), "lower --depth") {
		t.Fatalf("exit %d: %v", exitOf(err), err)
	}
	if out.Len() != 0 {
		t.Fatalf("rows printed after a failure: %q", out.String())
	}
	var r listing.Report
	readJSON(t, report, &r)
	if r.Outcome.Cause != "prefix_limit" || r.Output.Totals == nil || r.Output.Totals.MaxPrefixes != 2 || r.Output.Totals.Prefixes > 2 {
		t.Fatalf("outcome %+v totals %+v", r.Outcome, r.Output.Totals)
	}
}

// failingWriter fails every write with err.
type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

// slowWriter takes delay over its first write.
type slowWriter struct {
	delay time.Duration
	once  sync.Once
	bytes.Buffer
}

func (w *slowWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { time.Sleep(w.delay) })
	return w.Buffer.Write(p)
}

// TestDuWriteFailure: the rows are the run's output, written before its
// end and outcome are settled: a failed write fails the run (status
// failed; a full disk is disk_full), and the report says so; a closed
// reader stops it as a listing to stdout does (exit 0, downstream_closed).
func TestDuWriteFailure(t *testing.T) {
	root, _ := depthTree(t)
	report := filepath.Join(t.TempDir(), "r.json")
	for _, tc := range []struct {
		err   error
		class string
		code  int
	}{
		{errors.New("disk on fire"), "fatal", exitFatal},
		{syscall.ENOSPC, "disk_full", exitDiskFull},
	} {
		err := runDu(context.Background(), []string{root, "--format=tsv", "--no-stats", "--report", report}, failingWriter{tc.err})
		if exitOf(err) != tc.code || !strings.Contains(fmt.Sprint(err), tc.err.Error()) {
			t.Fatalf("%v: exit %d: %v", tc.err, exitOf(err), err)
		}
		var r listing.Report
		readJSON(t, report, &r)
		if r.Outcome.Complete || r.Outcome.ExitCode != tc.code || r.Outcome.Status != listing.StatusFailed || r.Outcome.Class != tc.class {
			t.Fatalf("%v: outcome %+v", tc.err, r.Outcome)
		}
	}
	var r listing.Report
	pr, pw, perr := os.Pipe()
	if perr != nil {
		t.Fatal(perr)
	}
	pr.Close()
	defer pw.Close()
	err := runDu(context.Background(), []string{root, "--format=tsv", "--no-stats", "--report", report}, pw)
	readJSON(t, report, &r)
	if err != nil || r.Outcome.StopReason != listing.StopDownstreamClosed || r.Outcome.Status != listing.StatusStopped {
		t.Fatalf("closed reader: %v, outcome %+v", err, r.Outcome)
	}
}

// TestDuStopDuringOutput: a stop that comes while the rows are written
// (here --max-duration, behind a slow reader) is the run's: it exits as a
// stopped run does, never 0, and its report agrees; the report's times
// include the writing.
func TestDuStopDuringOutput(t *testing.T) {
	root, _ := depthTree(t)
	report := filepath.Join(t.TempDir(), "r.json")
	w := &slowWriter{delay: 600 * time.Millisecond}
	err := runDu(context.Background(), []string{root, "--format=tsv", "--no-stats", "--max-duration=300ms", "--report", report}, w)
	var r listing.Report
	readJSON(t, report, &r)
	if exitOf(err) != exitDeadline || r.Outcome.ExitCode != exitDeadline || r.Outcome.Complete {
		t.Fatalf("exit %d (%v), outcome %+v", exitOf(err), err, r.Outcome)
	}
	if r.Timings.WallMs < 600 {
		t.Fatalf("wall %d ms leaves out the writing", r.Timings.WallMs)
	}
	// A slow reader within the deadline: complete, and timed.
	w = &slowWriter{delay: 300 * time.Millisecond}
	if err := runDu(context.Background(), []string{root, "--format=tsv", "--no-stats", "--report", report}, w); err != nil {
		t.Fatal(err)
	}
	readJSON(t, report, &r)
	if !r.Outcome.Complete || r.Timings.WallMs < 300 || !strings.HasPrefix(w.String(), "depth\t") {
		t.Fatalf("outcome %+v, wall %d ms, rows %q", r.Outcome, r.Timings.WallMs, w.String())
	}
}
