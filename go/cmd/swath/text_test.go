// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

func textBucket(t *testing.T, n int) (args []string, keys []string) {
	t.Helper()
	keys = gcstest.Keys(n)
	slices.Sort(keys)
	return []string{"s3://bkt", "--endpoint-url", s3test.Bucket(t, keys), "--anonymous"}, keys
}

// TestTextOutput: rows on stdout by default (TSV off a terminal, the
// header first, keys sorted), a .jsonl.gz file published whole, -o -,
// no summary line on a stdout that carries rows, and a text destination
// that swath resume refuses.
func TestTextOutput(t *testing.T) {
	src, keys := textBucket(t, 2500)
	var out bytes.Buffer
	if err := run(context.Background(), src, &out, false); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	const header = "key\tsize\tlast_modified\tetag\tstorage_class\towner_id\towner_display_name\tchecksum_algorithm\tchecksum_type"
	if lines[0] != header || len(lines) != len(keys)+1 {
		t.Fatalf("%d lines, first %q", len(lines), lines[0])
	}
	for i, k := range keys {
		if f := strings.Split(lines[i+1], "\t"); f[0] != k || f[1] != strconv.Itoa(len(k)) {
			t.Fatalf("row %d: %q", i, lines[i+1])
		}
	}
	out.Reset()
	// The ETag as the dataset stores it: the body's MD5, its quotes removed.
	etag := fmt.Sprintf(`"%x"`, md5.Sum([]byte(keys[0])))
	if err := run(context.Background(), append(src, "-o", "-", "--format", "jsonl"), &out, false); err != nil ||
		!strings.HasPrefix(out.String(), `{"key":"`+keys[0]+`","size":`) || !strings.Contains(out.String(), `Z","etag":`+etag+`,"storage_class":`) {
		t.Fatalf("%v %.200s", err, out.String())
	}
	file := filepath.Join(t.TempDir(), "list.jsonl.gz")
	out.Reset()
	if err := run(context.Background(), append(src, "-o", file), &out, false); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatalf("summary %q", out.String())
	}
	raw, _ := os.ReadFile(file)
	if len(raw) < 2 || raw[0] != 0x1f || raw[1] != 0x8b {
		t.Fatal("not gzip")
	}
	err := dispatch(context.Background(), []string{"resume", file}, io.Discard)
	if exitCode(t, err) != exitUsage || !strings.Contains(err.Error(), "not resumable") {
		t.Fatalf("resume of a text file: %v", err)
	}
	var u usageError
	if err := run(context.Background(), append(src, "--report", "-"), &out, false); !errors.As(err, &u) {
		t.Fatalf("json report on a stdout of rows: %v", err)
	}
}

// TestClosedReaderExitsZero: a reader that closes the stream ends the run
// with exit 0, the listing cancelled early, and the report says so.
func TestClosedReaderExitsZero(t *testing.T) {
	src, keys := textBucket(t, 20000)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		br := bufio.NewReader(r)
		for range 3 {
			_, _ = br.ReadString('\n')
		}
		r.Close()
	}()
	report := filepath.Join(t.TempDir(), "r.json")
	err = run(context.Background(), append(src, "--report", report, "--workers", "2"), w, false)
	w.Close()
	if err != nil {
		t.Fatalf("closed reader: %v", err)
	}
	var rep struct {
		Outcome struct {
			Status     string
			StopReason string `json:"stop_reason"`
			Complete   bool
			ExitCode   int `json:"exit_code"`
		}
		Output struct{ Text struct{ Rows int64 } }
	}
	readJSON(t, report, &rep)
	if rep.Outcome.Status != "stopped" || rep.Outcome.ExitCode != 0 || rep.Outcome.Complete || rep.Outcome.StopReason != "downstream_closed" || rep.Output.Text.Rows >= int64(len(keys)) {
		t.Fatalf("report %+v", rep)
	}
}

// TestColumns: --columns prints the named columns in the order given, in
// TSV and JSONL, and on a terminal's stdout TSV rather than the table; a
// name the schema lacks is a usage error listing the source's columns,
// before any request; the table and a dataset refuse it.
func TestColumns(t *testing.T) {
	keys := []string{"a", "bb"}
	var requests atomic.Int64
	h := s3test.Handler(s3test.Backend(t, keys))
	src := []string{"s3://bkt", "--anonymous", "--endpoint-url", s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		h.ServeHTTP(w, r)
	}))}
	var out bytes.Buffer
	if err := run(context.Background(), append(src, "--columns", "size,key"), &out, false); err != nil || out.String() != "size\tkey\n1\ta\n2\tbb\n" {
		t.Fatalf("tsv: %v %q", err, out.String())
	}
	out.Reset()
	if err := run(context.Background(), append(src, "--columns=key", "--columns", "size", "--format", "jsonl"), &out, false); err != nil ||
		out.String() != `{"key":"a","size":1}`+"\n"+`{"key":"bb","size":2}`+"\n" {
		t.Fatalf("jsonl: %v %q", err, out.String())
	}
	terminal := stdoutTerminal
	stdoutTerminal = func() bool { return true }
	t.Cleanup(func() { stdoutTerminal = terminal })
	out.Reset()
	if err := run(context.Background(), append(src, "--columns", "key"), &out, false); err != nil || out.String() != "key\na\nbb\n" {
		t.Fatalf("on a terminal: %v %q", err, out.String())
	}
	before := requests.Load()
	err := run(context.Background(), append(src, "--columns", "key,Size", "--no-stats"), &out, false)
	if exitCode(t, err) != exitUsage || !strings.Contains(err.Error(), `--columns: no column "Size" in swath/s3-objects/v1 (its columns: key, size, last_modified, etag`) {
		t.Fatalf("unknown column: %v", err)
	}
	if n := requests.Load() - before; n != 0 {
		t.Fatalf("an unknown column sent %d requests", n)
	}
	var u usageError
	for _, args := range [][]string{{"--columns", "key", "--format", "table"}, {"--columns", "key", "-o", filepath.Join(t.TempDir(), "ds")}} {
		if err := run(context.Background(), append(src, args...), &out, false); !errors.As(err, &u) || !strings.HasPrefix(err.Error(), "--columns: ") {
			t.Errorf("%q: %v", args, err)
		}
	}
}

// TestLocalTextEndToEnd: a local tree prints as text rows, to stdout and to a
// .tsv or .jsonl file, under the local schema's names; with --metadata
// names the fields it does not read are null in TSV and JSONL and blank in
// the table. A run with -o prints nothing on stdout.
func TestLocalTextEndToEnd(t *testing.T) {
	root, _ := depthTree(t)
	var out bytes.Buffer
	if err := run(context.Background(), []string{root, "--no-stats"}, &out, false); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if !strings.HasPrefix(lines[0], "key\tsize\tlast_modified\tkind\t") || len(lines) != 9 || !strings.HasPrefix(lines[1], "a/\t") ||
		!strings.HasPrefix(lines[3], "a/b/f\t5\t2026-01-02T04:04:05.000000Z\tFILE\t") {
		t.Fatalf("stdout %q", lines)
	}
	dir := t.TempDir()
	for _, name := range []string{"rows.tsv", "rows.jsonl"} {
		out.Reset()
		file := filepath.Join(dir, name)
		if err := run(context.Background(), []string{root, "-o", file, "--no-stats"}, &out, false); err != nil || out.Len() != 0 {
			t.Fatalf("%s: %v, stdout %q", name, err, out.String())
		}
		b, _ := os.ReadFile(file)
		if want := map[string]string{"rows.tsv": "key\tsize\t", "rows.jsonl": `{"key":"a/","size":`}[name]; !strings.HasPrefix(string(b), want) || strings.Count(string(b), "\n") < 8 {
			t.Fatalf("%s: %.120q", name, b)
		}
	}
	for format, want := range map[string]string{
		"tsv":   "a/b/f\t\\N\t\\N\tFILE\t",
		"jsonl": `{"key":"a/b/f","size":null,"last_modified":null,"kind":"FILE",`,
		"table": "                                 FILE          a/b/f\n", // kind: BLOCK_DEVICE wide
	} {
		out.Reset()
		if err := run(context.Background(), []string{root, "--metadata", "names", "--format", format, "--no-stats"}, &out, false); err != nil || !strings.Contains(out.String(), want) {
			t.Errorf("%s names: %v\n%s", format, err, out.String())
		}
	}
	out.Reset()
	if err := run(context.Background(), []string{root, "-o", filepath.Join(dir, "ds"), "--stats"}, &out, false); err != nil || out.Len() != 0 {
		t.Fatalf("dataset: %v, stdout %q", err, out.String())
	}
}

// TestNoProgressAmongRows: with stdout a terminal that carries rows, the
// automatic progress line and -v's appended records stay off; --progress
// still draws them. Where a record is expected the fake holds its answers
// until one is on stderr, so a slow runner cannot end the run first; where
// none is, it holds them past the first progress samples (2 s apart).
func TestNoProgressAmongRows(t *testing.T) {
	h := s3test.Handler(s3test.Backend(t, []string{"a", "b"}))
	stdout, stderr := stdoutTerminal, stderrTerminal
	stdoutTerminal, stderrTerminal = func() bool { return true }, func() (bool, int) { return true, 120 }
	t.Cleanup(func() { stdoutTerminal, stderrTerminal = stdout, stderr })
	const record = "listing · "
	for _, tc := range []struct {
		extra  []string
		forced bool
	}{{nil, false}, {[]string{"--progress"}, true}, {[]string{"-v"}, false}, {[]string{"-v", "--progress"}, true}} {
		errs := captureSyncStderr(t)
		hold := func() {
			if !tc.forced {
				time.Sleep(3 * time.Second)
				return
			}
			for end := time.Now().Add(time.Minute); !strings.Contains(errs.String(), record) && time.Now().Before(end); {
				time.Sleep(10 * time.Millisecond)
			}
		}
		endpoint := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hold(); h.ServeHTTP(w, r) }))
		args := append([]string{"s3://bkt", "--anonymous", "--no-stats", "--color", "never", "--endpoint-url", endpoint}, tc.extra...)
		var out bytes.Buffer
		if err := run(context.Background(), args, &out, false); err != nil {
			t.Fatal(err)
		}
		if drawn := strings.Contains(errs.String(), record); drawn != tc.forced || !strings.Contains(out.String(), "  a\n") {
			t.Errorf("%v: stderr %q, stdout %q", tc.extra, errs.String(), out.String())
		}
	}
}

func TestResolveOutput(t *testing.T) {
	for i, tc := range []struct {
		o             options
		tty           bool
		format, path  string
		comp, failure string
	}{
		{o: options{}, format: "tsv"},
		{o: options{}, tty: true, format: "table"},
		{o: options{out: "-", format: "jsonl"}, format: "jsonl"},
		{o: options{format: "table"}, format: "table"},
		{o: options{out: "x.parquet.tsv"}, format: "tsv", path: "x.parquet.tsv"},
		{o: options{out: "x.parquet.jsonl.zst"}, format: "jsonl", path: "x.parquet.jsonl.zst", comp: "zstd"},
		{o: options{out: "x.TSV.zst"}, format: "tsv", path: "x.TSV.zst", comp: "zstd"},
		{o: options{out: "x.jsonl.gz"}, format: "jsonl", path: "x.jsonl.gz", comp: "gzip"},
		{o: options{out: "x.jsonl"}, format: "jsonl", path: "x.jsonl"},
		{o: options{out: "ds"}},
		{o: options{out: "ds", format: "tsv"}, failure: "is a dataset directory"},
		{o: options{out: "x.tsv", format: "tsv"}, format: "tsv", path: "x.tsv"},
		{o: options{out: "x.tsv", format: "jsonl"}, failure: "names tsv"},
		{o: options{format: "parquet"}, failure: "want auto, table, tsv or jsonl"},
		{o: options{countOnly: true}},
	} {
		c, err := resolveOutput(&tc.o, tc.tty)
		switch {
		case tc.failure != "":
			if err == nil || !strings.Contains(err.Error(), tc.failure) {
				t.Errorf("%d: %v, want %q", i, err, tc.failure)
			}
		case err != nil:
			t.Errorf("%d: %v", i, err)
		case tc.format == "" && c != nil:
			t.Errorf("%d: text %+v, want the dataset", i, c)
		case tc.format != "" && (c == nil || c.Format != tc.format || c.Path != tc.path || c.Compression != tc.comp):
			t.Errorf("%d: %+v", i, c)
		}
	}
}
