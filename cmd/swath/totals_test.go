// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/internal/totals"
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

// duTSV is swath du's TSV rows, each by the header's names.
func duTSV(t *testing.T, out string) []map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	header := strings.Split(lines[0], "\t")
	var rows []map[string]string
	for _, l := range lines[1:] {
		f := strings.Split(l, "\t")
		if len(f) != len(header) {
			t.Fatalf("row %q under %q", l, lines[0])
		}
		r := map[string]string{}
		for i, n := range header {
			r[n] = f[i]
		}
		rows = append(rows, r)
	}
	return rows
}

// dropColumns is TSV out without the named columns: the ones whose values
// depend on the platform or the filesystem.
func dropColumns(out string, names ...string) string {
	var b strings.Builder
	var keep []bool
	for l := range strings.Lines(out) {
		f := strings.Split(strings.TrimSuffix(l, "\n"), "\t")
		if keep == nil {
			for _, n := range f {
				keep = append(keep, !slices.Contains(names, n))
			}
		}
		var kept []string
		for i, v := range f {
			if keep[i] {
				kept = append(kept, v)
			}
		}
		b.WriteString(strings.Join(kept, "\t") + "\n")
	}
	return b.String()
}

// tableCells is a table line's cells: they are two or more blanks apart.
func tableCells(line string) []string {
	return regexp.MustCompile(`  +`).Split(strings.TrimSpace(line), -1)
}

// duTimeHeading is the du table's time heading, the local zone's, padded
// to a time's width.
func duTimeHeading() string {
	zone, _ := time.Now().Zone()
	return fmt.Sprintf("%-*s", len(time.DateTime), "last_modified ("+zone+")")
}

// linksOnce is the links_once a local tree's rows carry by default: every
// platform but Windows reads the links.
var linksOnce = strconv.FormatBool(runtime.GOOS != "windows")

// TestDu: swath du -d N prints each prefix's files, directories, both byte
// figures and newest time, du-style (a parent includes its children; a
// directory counts in its parents' Dirs, an empty one is listed), sorted by
// prefix with the total first, each row with its type and the run's
// primary figure, order and hard-link policy; the run report says the
// path engaged, under the rows' names.
func TestDu(t *testing.T) {
	root, at := depthTree(t)
	report := filepath.Join(t.TempDir(), "r.json")
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "--depth", "2", "--format", "tsv", "--no-stats", "--report", report}, &out); err != nil {
		t.Fatal(err)
	}
	const newest, older = "2026-01-02T04:04:05.000000Z", "2026-01-02T03:04:05.000000Z"
	tail := "\tsize\tname\t" + linksOnce + "\t"
	want := "row_type\tdepth\tobjects\tdirs\tsize\tsize_missing\tmax_last_modified\tprimary\tsort\tlinks_once\tprefix\n" +
		"total\t0\t4\t4\t9\t0\t" + newest + tail + "\n" +
		"prefix\t1\t2\t1\t7\t0\t" + newest + tail + "a/\n" +
		"prefix\t2\t1\t0\t5\t0\t" + newest + tail + "a/b/\n" +
		"prefix\t1\t1\t0\t1\t0\t" + older + tail + "c/\n" +
		"prefix\t1\t0\t0\t0\t0\t\\N" + tail + "e/\n"
	if got := dropColumns(out.String(), "allocated_bytes", "allocated_bytes_missing"); got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	// The allocated bytes are every row's too: the files' blocks, known
	// for each file, but on Windows, which reads none; 0 for an empty
	// directory.
	for _, r := range duTSV(t, out.String()) {
		switch {
		case r["objects"] == "0" && (r["allocated_bytes"] != "0" || r["allocated_bytes_missing"] != "0"),
			r["objects"] != "0" && runtime.GOOS == "windows" && (r["allocated_bytes"] != `\N` || r["allocated_bytes_missing"] != r["objects"]),
			r["objects"] != "0" && runtime.GOOS != "windows" && (r["allocated_bytes"] == `\N` || r["allocated_bytes_missing"] != "0"):
			t.Errorf("allocated bytes %+v", r)
		}
	}
	// A row includes its descendants: the depth-1 rows leave out x, the
	// file at the top, and all rows together count a/b/f three times; only
	// the total, depth 0, is the whole.
	sums := map[string]int{}
	for _, r := range duTSV(t, out.String()) {
		n, _ := strconv.Atoi(r["size"])
		sums[r["depth"]] += n
		sums["all"] += n
	}
	if sums["0"] != 9 || sums["1"] != 8 || sums["all"] != 22 {
		t.Fatalf("sums %v", sums)
	}
	var r listing.Report
	readJSON(t, report, &r)
	tt := r.Output.Totals
	if tt == nil || tt.Depth != 2 || tt.Prefixes != 5 || tt.Objects != 4 || tt.Dirs != 4 || tt.Size == nil || *tt.Size != 9 || tt.SizeMissing != 0 || tt.NotObjects != 4 ||
		tt.Primary != "size" || tt.MaxLastModified == nil || tt.MaxLastModified.Format("2006-01-02T15:04:05.000000Z") != newest || (tt.AllocatedBytes == nil) != (runtime.GOOS == "windows") ||
		strconv.FormatBool(tt.LinksOnce) != linksOnce || r.Output.RowsEmitted != 8 || r.Invocation.Argv[0] != "du" {
		t.Fatalf("report totals %+v, rows %d, argv %q", tt, r.Output.RowsEmitted, r.Invocation.Argv)
	}
	// The table: one figure, the size, in binary units, the files and
	// directories, the newest time in local time to the second under a
	// heading naming the zone ("-" for the empty directory), the path
	// last, and the total named and last, as du's; -d is --depth. A deeper path is indented under its parent.
	// The line under it on stderr gives both figures' totals.
	stderr := captureStderr(t)
	out.Reset()
	if err := runDu(context.Background(), []string{root, "-d", "2", "--format=table", "--no-stats"}, &out); err != nil {
		t.Fatal(err)
	}
	heading := duTimeHeading()
	local := func(t time.Time) string { return fmt.Sprintf("%-*s", len(heading), t.Local().Format(time.DateTime)) }
	newestLocal, olderLocal, none := local(at.Add(time.Hour)), local(at), fmt.Sprintf("%-*s", len(heading), "-")
	wantTable := "" +
		"size  files  dirs  " + heading + "  path\n" +
		" 7 B      2     1  " + newestLocal + "  a/\n" +
		" 5 B      1     0  " + newestLocal + "    a/b/\n" +
		" 1 B      1     0  " + olderLocal + "  c/\n" +
		" 0 B      0     0  " + none + "  e/\n" +
		" 9 B      4     4  " + newestLocal + "  " + root + " (total)\n"
	onDisk := regexp.MustCompile(`^  9 B in 4 files · [0-9.]+ [KMG]?i?B on disk\n$`)
	if runtime.GOOS == "windows" { // no blocks read
		onDisk = regexp.MustCompile(`^  9 B in 4 files · on disk unknown\n$`)
	}
	if out.String() != wantTable || !onDisk.MatchString(stderr.String()) {
		t.Fatalf("table\n%s\nwant\n%s\nstderr %q", out.String(), wantTable, stderr.String())
	}
	// --allocated: the one figure is on disk, the line gives the size.
	stderr.Reset()
	out.Reset()
	if err := runDu(context.Background(), []string{root, "-d", "1", "--format=table", "--no-stats", "--allocated"}, &out); runtime.GOOS != "windows" &&
		(err != nil || !strings.HasPrefix(tableCells(out.String())[0], "on disk") ||
			!regexp.MustCompile(`^  [0-9.]+ [KMG]?i?B on disk in 4 files · 9 B of data\n$`).MatchString(stderr.String())) {
		t.Fatalf("--allocated: %v\n%s\nstderr %q", err, out.String(), stderr.String())
	}
	// -q, and TSV, print no such line.
	for _, args := range [][]string{{"--format=table", "-q"}, {"--format=tsv"}} {
		stderr.Reset()
		if err := runDu(context.Background(), append([]string{root, "--no-stats"}, args...), io.Discard); err != nil || stderr.Len() != 0 {
			t.Fatalf("%q: %v, stderr %q", args, err, stderr.String())
		}
	}
	// Without -d, the total alone, here without the header line; swath
	// diag count keeps its one line.
	out.Reset()
	if err := runDu(context.Background(), []string{root, "--no-stats", "--format=tsv", "--no-header"}, &out); err != nil {
		t.Fatal(err)
	}
	if f := strings.Split(out.String(), "\t"); len(f) != 13 || strings.Join(f[:6], "\t") != "total\t0\t4\t4\t9\t0" ||
		"\t"+strings.Join(f[8:], "\t") != "\t"+newest+tail+"\n" {
		t.Fatalf("total alone: %q", out.String())
	}
	out.Reset()
	if err := runCount(context.Background(), []string{root, "--no-stats"}, &out); err != nil || !strings.HasPrefix(out.String(), "files=4 dirs=4 ") ||
		!strings.Contains(out.String(), "rows=8 object_bytes=9 ") { // the files' bytes: a directory's own size is no object's
		t.Fatalf("%v %q", err, out.String())
	}
}

// TestDuFile: a file is a local source, as du and ls take one: du gives its
// one total row, list its one row, keyed by its name.
func TestDuFile(t *testing.T) {
	root, at := depthTree(t)
	file := filepath.Join(root, "a", "g")
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{file, "-d", "1", "--format", "tsv", "--no-stats"}, &out); err != nil ||
		dropColumns(out.String(), "allocated_bytes", "allocated_bytes_missing", "links_once") != "row_type\tdepth\tobjects\tdirs\tsize\tsize_missing\tmax_last_modified\tprimary\tsort\tprefix\n"+
			"total\t0\t1\t0\t2\t0\t"+at.Format("2006-01-02T15:04:05.000000Z")+"\tsize\tname\t\n" {
		t.Fatalf("du: %v %q", err, out.String())
	}
	out.Reset()
	if err := run(context.Background(), []string{file, "--columns", "key,size,kind", "--no-stats"}, &out, false); err != nil || out.String() != "key\tsize\tkind\ng\t2\tfile\n" {
		t.Fatalf("list: %v %q", err, out.String())
	}
}

// TestDuSortSize: --sort size orders the prefixes below each parent by
// size, the largest first, each prefix's own below it and the total in its
// place (last in the table, first in TSV); another order is refused.
func TestDuSortSize(t *testing.T) {
	root, _ := depthTree(t)
	if err := os.WriteFile(filepath.Join(root, "c", "big"), make([]byte, 20), 0o644); err != nil { // c/ the largest, after a/ by name
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "-d", "2", "--sort", "size", "--format=table", "--no-stats"}, &out); err != nil {
		t.Fatal(err)
	}
	var order []string
	for l := range strings.Lines(out.String()) {
		f := strings.Fields(l)
		order = append(order, f[len(f)-1])
	}
	if got := strings.Join(order, " "); got != "path c/ a/ a/b/ e/ (total)" {
		t.Fatalf("table by size: %s", got)
	}
	out.Reset()
	if err := runDu(context.Background(), []string{root, "-d", "1", "--sort=size", "--format=tsv", "--no-stats"}, &out); err != nil {
		t.Fatal(err)
	}
	var prefixes []string
	for l := range strings.Lines(out.String()) {
		f := strings.Split(strings.TrimSuffix(l, "\n"), "\t")
		prefixes = append(prefixes, f[1]+":"+f[len(f)-1]+":"+f[len(f)-3])
	}
	if got := strings.Join(prefixes, " "); got != "depth:prefix:sort 0::size 1:c/:size 1:a/:size 1:e/:size" {
		t.Fatalf("tsv by size: %s", got)
	}
	var u usageError
	if err := runDu(context.Background(), []string{root, "--sort", "big"}, &out); !errors.As(err, &u) {
		t.Fatalf("--sort big: %v", err)
	}
}

// TestObjectBytesLeaveOutDirs: a dataset run's object_bytes is the apparent
// bytes of the files it accepted after the filters: a directory's own size
// is no object's.
func TestObjectBytesLeaveOutDirs(t *testing.T) {
	root, _ := depthTree(t)
	report := filepath.Join(t.TempDir(), "r.json")
	var out bytes.Buffer
	if err := run(context.Background(), []string{root, "-o", filepath.Join(t.TempDir(), "ds"), "--exclude", "c/", "--exclude", "c/**", "--no-stats", "--report", report}, &out, false); err != nil {
		t.Fatal(err)
	}
	var r listing.Report
	readJSON(t, report, &r)
	if b := r.Output.ObjectBytes; b == nil || *b != 8 || r.Output.RowsEmitted != 6 { // a/b/f, a/g, x; a/, a/b/, e/
		t.Fatalf("object_bytes %v, rows %d", b, r.Output.RowsEmitted)
	}
}

// TestDuNamesOnly: without sizes or blocks (the names metadata tier) both
// figures of a prefix with files are null, beside the files without them,
// as rclone size's sizeless; the table shows them, and the time the tier
// does not read, as "-", unknown; an empty directory has 0 bytes. Nothing tells hard links apart, so every
// name counts and the rows say so.
func TestDuNamesOnly(t *testing.T) {
	root, _ := depthTree(t)
	none := fmt.Sprintf("%-*s", len(duTimeHeading()), "-")
	const tail = `,"max_last_modified":null,"primary":"size","sort":"name","links_once":false}` + "\n"
	for format, want := range map[string]string{
		"tsv": "row_type\tdepth\tobjects\tdirs\tsize\tsize_missing\tallocated_bytes\tallocated_bytes_missing\tmax_last_modified\tprimary\tsort\tlinks_once\tprefix\n" +
			"total\t0\t4\t4\t\\N\t4\t\\N\t4\t\\N\tsize\tname\tfalse\t\n" +
			"prefix\t1\t2\t1\t\\N\t2\t\\N\t2\t\\N\tsize\tname\tfalse\ta/\n" +
			"prefix\t1\t1\t0\t\\N\t1\t\\N\t1\t\\N\tsize\tname\tfalse\tc/\n" +
			"prefix\t1\t0\t0\t0\t0\t0\t0\t\\N\tsize\tname\tfalse\te/\n",
		"jsonl": `{"row_type":"total","depth":0,"prefix":"","objects":4,"dirs":4,"size":null,"size_missing":4,"allocated_bytes":null,"allocated_bytes_missing":4` + tail +
			`{"row_type":"prefix","depth":1,"prefix":"a/","objects":2,"dirs":1,"size":null,"size_missing":2,"allocated_bytes":null,"allocated_bytes_missing":2` + tail +
			`{"row_type":"prefix","depth":1,"prefix":"c/","objects":1,"dirs":0,"size":null,"size_missing":1,"allocated_bytes":null,"allocated_bytes_missing":1` + tail +
			`{"row_type":"prefix","depth":1,"prefix":"e/","objects":0,"dirs":0,"size":0,"size_missing":0,"allocated_bytes":0,"allocated_bytes_missing":0` + tail,
		"table": "" +
			"size  files  dirs  " + duTimeHeading() + "  path\n" +
			"   -      2     1  " + none + "  a/\n" +
			"   -      1     0  " + none + "  c/\n" +
			" 0 B      0     0  " + none + "  e/\n" +
			"   -      4     4  " + none + "  " + root + " (total)\n",
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

// TestDuJSONL: an object store's rows carry no Dirs, and a null allocated
// bytes figure beside every object without it; its table has one figure,
// plainly size, calls its objects objects and gives their newest time;
// depth counts from the
// prefix the source names. No line under its table speaks of on disk.
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
	if !strings.HasPrefix(lines[0], `{"row_type":"total","depth":0,"prefix":"","objects":50,"size":`+strconv.Itoa(size)+
		`,"size_missing":0,"allocated_bytes":null,"allocated_bytes_missing":50,"max_last_modified":"`) ||
		!strings.HasSuffix(lines[0], `Z","primary":"size","sort":"name","links_once":false}`) ||
		len(lines) < 2 || !strings.HasPrefix(lines[1], `{"row_type":"prefix","depth":1,"prefix":"`) {
		t.Fatalf("%q", lines)
	}
	out.Reset()
	stderr := captureStderr(t)
	if err := runDu(context.Background(), append(src, "--depth", "1", "--format", "table", "--no-stats"), &out); err != nil || strings.Contains(stderr.String(), "on disk") ||
		!regexp.MustCompile(`^ *size  objects  last_modified \([^)]+\)  prefix\n`).MatchString(out.String()) || strings.Contains(out.String(), "on disk") ||
		!regexp.MustCompile(`\n *[0-9.]+ [KMG]?i?B +50  \d{4}-\d\d-\d\d \d\d:\d\d:\d\d  s3://bkt \(total\)\n$`).MatchString(out.String()) {
		t.Fatalf("%v %q", err, out.String())
	}
}

// TestDuOrigin: depth counts from the prefix the source names, up to its
// last '/', and the rows keep the keys' spelling; the total's prefix is
// the one the source names whole; an --include that narrows the listed
// prefix does not move them, on S3 and on Azure Blob, nor the table's
// total label.
func TestDuOrigin(t *testing.T) {
	keys := []string{"logs/2026/01/a", "logs/2026/01/b/c", "logs/2026/02/d", "logs/2026/top", "logs/2027/x", "other/y"}
	end := s3test.Bucket(t, keys)
	az := azEndpoint(azblobtest.Serve(t, keys))
	du := func(args ...string) string {
		t.Helper()
		ep := end
		if strings.HasPrefix(args[0], "az://") {
			ep = az
		}
		var out bytes.Buffer
		if err := runDu(context.Background(), append(args, "--endpoint-url", ep, "--anonymous", "--format", "tsv", "--no-stats"), &out); err != nil {
			t.Fatal(err)
		}
		var rows []string
		for l := range strings.Lines(out.String()) {
			f := strings.Split(l, "\t")
			rows = append(rows, f[1]+":"+strings.TrimSuffix(f[len(f)-1], "\n")+" "+f[2]) // depth:prefix (last) objects
		}
		return strings.Join(rows[1:], ",")
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"s3://bkt/logs/2026/", "-d", "1"}, "0:logs/2026/ 4,1:logs/2026/01/ 2,1:logs/2026/02/ 1"},
		{[]string{"s3://bkt/logs/2026/0", "-d", "1"}, "0:logs/2026/0 3,1:logs/2026/01/ 2,1:logs/2026/02/ 1"},
		{[]string{"s3://bkt/logs", "-d", "1"}, "0:logs 5,1:logs/ 5"},
		{[]string{"s3://bkt/logs/", "-d", "1", "--include", "2026/**"}, "0:logs/ 4,1:logs/2026/ 4"},
		{[]string{"az://replay/corpus/logs/", "-d", "1", "--include", "2026/**"}, "0:logs/ 4,1:logs/2026/ 4"},
		{[]string{"az://replay/corpus", "-d", "2", "--include", "logs/2026/0*/**"}, "0: 3,1:logs/ 3,2:logs/2026/ 3"},
		{[]string{"s3://bkt", "-d", "2"}, "0: 6,1:logs/ 5,2:logs/2026/ 4,2:logs/2027/ 1,1:other/ 1"},
	} {
		if got := du(tc.args...); got != tc.want {
			t.Errorf("%v: %q, want %q", tc.args, got, tc.want)
		}
	}
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{"az://replay/corpus/logs/", "--include", "2026/**", "--endpoint-url", az, "--anonymous", "--format", "table", "--no-stats"}, &out); err != nil ||
		!strings.HasSuffix(out.String(), "  az://replay/corpus/logs/ (total)\n") {
		t.Fatalf("%v %q", err, out.String())
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
		{true, []string{"--depth", "-1"}, `--depth "-1": want a whole number, 0 or more`},
		{true, []string{"--max-prefixes", "0"}, `--max-prefixes "0"`},
		{true, []string{"--max-inodes", "0"}, `--max-inodes "0"`},
		{true, []string{"--format", "parquet"}, "want auto, table, tsv or jsonl"},
		{true, []string{"--report", "-"}, "give --report FILE"},
		{true, []string{"--no-seed"}, "give it to swath diag count or swath diag list"},
		{true, []string{"--allocated", "--apparent-size"}, "--allocated and --apparent-size each choose the figure that comes first: give one"},
		{true, []string{"--no-header", "--format", "jsonl"}, "--no-header leaves out TSV's header line: not with JSONL"},
		{true, []string{"--no-header", "--format", "table"}, "not with the table"},
		{false, []string{"--no-header"}, "--no-header: -o "},
		{false, []string{"--apparent-size"}, "--apparent-size has no meaning for swath list"},
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
	if !r.Outcome.Complete || r.Timings.WallMs < 300 || !strings.HasPrefix(w.String(), "row_type\t") {
		t.Fatalf("outcome %+v, wall %d ms, rows %q", r.Outcome, r.Timings.WallMs, w.String())
	}
}

// TestFiguresLine: the line under a du table gives both totals, the
// primary first; an unknown figure says so and a partial one is a lower
// bound.
func TestFiguresLine(t *testing.T) {
	const mib = 1 << 20
	whole := totals.Prefix{Objects: 331, Size: 127 * mib, HasSize: true, Allocated: 64*mib + mib/5, HasAllocated: true}
	for _, c := range []struct {
		total   totals.Prefix
		primary string
		want    string
	}{
		{whole, totals.Size, "127 MiB in 331 files · 64.2 MiB on disk"},
		{whole, totals.AllocatedBytes, "64.2 MiB on disk in 331 files · 127 MiB of data"},
		{totals.Prefix{Objects: 1, Size: 3, HasSize: true, AllocatedMissing: 1}, totals.Size, "3 B in 1 file · on disk unknown"},
		{totals.Prefix{Objects: 4, SizeMissing: 4, AllocatedMissing: 4}, totals.Size, "size unknown in 4 files · on disk unknown"},
		{totals.Prefix{Objects: 3, Size: 12 * 1024, HasSize: true, Allocated: 8192, AllocatedMissing: 1, HasAllocated: true}, totals.Size, "12.0 KiB in 3 files · >= 8.00 KiB on disk"},
		{totals.Prefix{Objects: 1000, Size: 1, HasSize: true, Allocated: 1, HasAllocated: true}, totals.AllocatedBytes, "1 B on disk in 1,000 files · 1 B of data"},
	} {
		if got := figuresLine(&c.total, c.primary); got != c.want {
			t.Errorf("%+v %s: %q, want %q", c.total, c.primary, got, c.want)
		}
	}
}
