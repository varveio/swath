// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/listing"
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

// TestCountDepth: swath count --depth N prints each prefix's objects,
// bytes and newest time, du-style (a parent includes its children; a
// directory row counts as nothing, an empty directory is listed), sorted
// by prefix with the total first; the run report says the path engaged.
func TestCountDepth(t *testing.T) {
	root, _ := depthTree(t)
	report := filepath.Join(t.TempDir(), "r.json")
	var out bytes.Buffer
	if err := runCount(context.Background(), []string{root, "--depth", "2", "--format", "tsv", "--no-stats", "--report", report}, &out, false); err != nil {
		t.Fatal(err)
	}
	const newest, older = "2026-01-02T04:04:05+00:00", "2026-01-02T03:04:05+00:00"
	want := "Prefix\tObjects\tSize\tLastModified\n" +
		"\t4\t9\t" + newest + "\n" +
		"a/\t2\t7\t" + newest + "\n" +
		"a/b/\t1\t5\t" + newest + "\n" +
		"c/\t1\t1\t" + older + "\n" +
		"e/\t0\t0\t\n"
	if out.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", out.String(), want)
	}
	var r listing.Report
	readJSON(t, report, &r)
	tt := r.Output.Totals
	if tt == nil || tt.Depth != 2 || tt.Prefixes != 5 || tt.Objects != 4 || tt.Bytes != 9 || tt.NotObjects != 4 || tt.BytesColumn != "size" || r.Output.RowsEmitted != 8 {
		t.Fatalf("report totals %+v, rows %d", tt, r.Output.RowsEmitted)
	}
	// The table: the newest time to the second, objects and bytes aligned,
	// the prefix; depth 0 is the total alone.
	out.Reset()
	if err := runCount(context.Background(), []string{root, "--depth=0", "--format=table", "--no-stats"}, &out, false); err != nil {
		t.Fatal(err)
	}
	if want := "2026-01-02 04:04:05          4               9 \n"; out.String() != want {
		t.Fatalf("table %q, want %q", out.String(), want)
	}
	// Without --depth a count prints its one line, unchanged.
	out.Reset()
	if err := runCount(context.Background(), []string{root, "--no-stats"}, &out, false); err != nil || !strings.HasPrefix(out.String(), "files=4 dirs=4 ") ||
		!strings.Contains(out.String(), "rows=8 object_bytes=") { // object_bytes holds the directories' sizes too
		t.Fatalf("%v %q", err, out.String())
	}
}

// TestCountDepthJSONL: an object store's keys are text, so its prefix rows
// may be JSON, one object per line.
func TestCountDepthJSONL(t *testing.T) {
	src, keys := textBucket(t, 50)
	var out bytes.Buffer
	if err := runCount(context.Background(), append(src, "--depth", "1", "--format", "jsonl", "--no-stats"), &out, false); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	var size int
	for _, k := range keys {
		size += len(k) // the fake's object bodies are their keys
	}
	if !strings.HasPrefix(lines[0], `{"Prefix":"","Objects":50,"Size":`+strconv.Itoa(size)+`,"LastModified":"`) || len(lines) < 2 || !strings.HasPrefix(lines[1], `{"Prefix":"`) {
		t.Fatalf("%q", lines)
	}
}

// TestCountDepthRefusals: the totals' flags are swath count's, --format
// goes with --depth, a local tree's byte names are not JSON, and stdout
// carries the rows, so the report goes to a file.
func TestCountDepthRefusals(t *testing.T) {
	root, _ := depthTree(t)
	for _, tc := range []struct {
		count bool
		args  []string
		want  string
	}{
		{false, []string{"--depth", "1"}, "--depth has no meaning for swath list"},
		{false, []string{"--max-prefixes", "9"}, "--max-prefixes has no meaning for swath list"},
		{true, []string{"--format", "tsv"}, "swath count with --depth"},
		{true, []string{"--max-prefixes", "9"}, "give --depth too"},
		{true, []string{"--depth", "-1"}, "--depth -1: negative"},
		{true, []string{"--depth", "1", "--max-prefixes", "0"}, "--max-prefixes 0"},
		{true, []string{"--depth", "1", "--format", "jsonl"}, "JSON cannot carry"},
		{true, []string{"--depth", "1", "--format", "parquet"}, "writes auto, table, tsv or jsonl"},
		{true, []string{"--depth", "1", "--report-format", "json"}, "use --report FILE"},
	} {
		var out bytes.Buffer
		var err error
		if tc.count {
			err = runCount(context.Background(), append([]string{root}, tc.args...), &out, false)
		} else {
			err = run(context.Background(), append([]string{root, "-o", filepath.Join(t.TempDir(), "ds")}, tc.args...), &out, false)
		}
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("count=%v %v: exit %d (%v), want %q", tc.count, tc.args, exitOf(err), err, tc.want)
		}
	}
}

// TestCountDepthCap: past --max-prefixes the count fails (exit 1, cause
// prefix_limit) with advice, prints no partial rows, and the report says
// how far it got.
func TestCountDepthCap(t *testing.T) {
	root, _ := depthTree(t)
	report := filepath.Join(t.TempDir(), "r.json")
	var out bytes.Buffer
	err := runCount(context.Background(), []string{root, "--depth", "1", "--max-prefixes", "2", "--no-stats", "--report", report}, &out, false)
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
