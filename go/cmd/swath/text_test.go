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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

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
	const header = "Key\tLastModified\tETag\tChecksumAlgorithm\tChecksumType\tSize\tStorageClass\tOwner.DisplayName\tOwner.ID"
	if lines[0] != header || len(lines) != len(keys)+1 {
		t.Fatalf("%d lines, first %q", len(lines), lines[0])
	}
	for i, k := range keys {
		if f := strings.Split(lines[i+1], "\t"); f[0] != k || f[5] != strconv.Itoa(len(k)) {
			t.Fatalf("row %d: %q", i, lines[i+1])
		}
	}
	out.Reset()
	// The ETag as the API returns it: the body's MD5 in quotes.
	etag := fmt.Sprintf(`"\"%x\""`, md5.Sum([]byte(keys[0])))
	if err := run(context.Background(), append(src, "-o", "-", "--format", "jsonl"), &out, false); err != nil ||
		!strings.HasPrefix(out.String(), `{"Key":"`+keys[0]+`","LastModified":"`) || !strings.Contains(out.String(), `+00:00","ETag":`+etag+`,"Size":`) {
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
	if err := run(context.Background(), []string{t.TempDir()}, &out, false); exitOf(err) != exitUsage || !strings.Contains(err.Error(), "or swath du") {
		t.Fatalf("local source as text: %v", err)
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
