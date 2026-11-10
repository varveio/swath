// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/filter"
	"github.com/varveio/swath/go/internal/journal"
)

// TestFiltersThroughTheCommand: the filters drop rows before the order
// stage; repeated includes add up and an exclude vetoes; the manifest's
// coverage.filters names them, the report counts the dropped rows, includes sharing a
// literal prefix narrow the listed prefix (the report says whether and
// why not), and a resume keeps the filters (they are part of the
// identity: another value is not accepted), a relative time as the time
// the run recorded.
func TestFiltersThroughTheCommand(t *testing.T) {
	src, keys := textBucket(t, 3000)
	var want []string
	for _, k := range keys {
		if (strings.HasPrefix(k, "d01/") || strings.HasPrefix(k, "d02/")) && !strings.Contains(k, "/sub1/") {
			want = append(want, k)
		}
	}
	if len(want) == 0 {
		t.Fatal("no keys to keep")
	}
	var out bytes.Buffer
	args := append(src, "--include", "d01/**", "--include-regex", "^d02/", "--exclude", "*/sub1/*", "--format", "tsv")
	if err := run(context.Background(), args, &out, false); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, l := range strings.Split(strings.TrimSpace(out.String()), "\n")[1:] {
		got = append(got, strings.SplitN(l, "\t", 2)[0])
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%d rows, want %d", len(got), len(want))
	}
	dir := filepath.Join(t.TempDir(), "ds")
	report := filepath.Join(t.TempDir(), "r.json")
	if err := run(context.Background(), append(append(src, "-o", dir, "--exclude-regex", "sub1", "--min-size", "1", "--report", report), unjournaled...), io.Discard, diagList); err != nil {
		t.Fatal(err)
	}
	var m dataset.Manifest
	readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
	if !slices.Equal(m.Coverage.Filters, []string{"exclude_regex=sub1", "min_size=1"}) {
		t.Fatalf("coverage.filters %q", m.Coverage.Filters)
	}
	type rep struct {
		Output struct {
			RowsEmitted  int64 `json:"rows_emitted"`
			RowsFiltered int64 `json:"rows_filtered"`
		}
		Invocation struct {
			Source, Narrowing string
			Argv              []string
		}
	}
	var r rep
	readJSON(t, report, &r)
	if r.Output.RowsEmitted+r.Output.RowsFiltered != int64(len(keys)) || r.Output.RowsFiltered == 0 || r.Invocation.Narrowing != "" {
		t.Fatalf("rows %+v of %d", r, len(keys))
	}
	r0 := r.Output.RowsEmitted
	for _, tc := range []struct {
		include        []string
		source, reason string
	}{
		{[]string{"--include-regex", "^d01/", "--include", "d01/sub2/*"}, "s3://bkt/d01/", filter.Narrowed},
		{[]string{"--include", "d01/*/*", "--include", "*.x"}, "s3://bkt", filter.NarrowUnanchored},
		{[]string{"--include", "d01/*", "--include", "e/*"}, "s3://bkt", filter.NarrowNoLonger},
	} {
		r = rep{}
		if err := runCount(context.Background(), append(append(src, "--report", report), tc.include...), io.Discard); err != nil {
			t.Fatal(err)
		}
		readJSON(t, report, &r)
		if r.Invocation.Source != tc.source || r.Invocation.Narrowing != tc.reason {
			t.Fatalf("%v: %+v", tc.include, r.Invocation)
		}
	}
	var u usageError
	if !dataset.DirSyncEnforced {
		return // the resume cases: a resume is refused here (Windows)
	}
	if err := dispatch(context.Background(), []string{"resume", dir, "--exclude", "x"}, io.Discard); !errors.As(err, &u) {
		t.Fatalf("a filter given to resume: %v", err)
	}
	// An interrupted filtered run resumes with its filters: the same rows,
	// and the relative time as the run recorded it.
	crashed := filepath.Join(t.TempDir(), "ds")
	before := time.Now().Add(-100000 * 24 * time.Hour).Truncate(time.Second)
	crashRun(t, append(src, "-o", crashed, "--exclude-regex", "sub1", "--min-size", "1", "--modified-since", "100000d", "--part-size", "4KiB", "--part-interval", "0", "--workers", "2"), 2)
	st, err := journal.Read(crashed)
	if err != nil {
		t.Fatal(err)
	}
	var since string
	for _, a := range st.Header.Argv {
		if v, ok := strings.CutPrefix(a, "--modified-since="); ok {
			since = v
		}
	}
	if at, err := time.Parse(time.RFC3339, since); err != nil || at.Before(before) || at.After(before.Add(time.Minute)) {
		t.Fatalf("recorded --modified-since %q (%v), want about %v", since, err, before)
	}
	if err := dispatch(context.Background(), []string{"resume", crashed}, io.Discard); err != nil {
		t.Fatal(err)
	}
	var m2 dataset.Manifest
	readJSON(t, filepath.Join(crashed, dataset.ManifestName), &m2)
	var rows int64
	for _, p := range m2.Parts {
		rows += p.Rows
	}
	if rows != r0 || !slices.Equal(m2.Coverage.Filters, append(slices.Clone(m.Coverage.Filters), "modified_after="+since)) {
		t.Fatalf("resumed: %d rows (want %d), filters %q", rows, r0, m2.Coverage.Filters)
	}
}

// TestFilterRefusals: a malformed or empty pattern, and a filter on a
// column the source does not read, are usage errors before the run (the
// latter: sizes and times at a local tree's names tier, storage classes on
// a local tree; at the stat tier they run).
func TestFilterRefusals(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	var u usageError
	for _, f := range [][]string{{"--metadata", "names", "--min-size", "1"}, {"--metadata", "names", "--max-size", "1M"},
		{"--metadata", "names", "--modified-since", "7d"}, {"--metadata", "names", "--modified-until", "2026-01-01"}, {"--storage-class", "STANDARD"},
		{"--include", "a**b"}, {"--exclude", ""}, {"--include", "{a,{b}}"}, {"--include-regex", "(a)\\1"}} {
		if err := runDu(context.Background(), append([]string{root, "--no-stats"}, f...), io.Discard); !errors.As(err, &u) {
			t.Errorf("%v: %v", f, err)
		}
	}
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "--no-stats", "--format", "tsv", "--min-size", "1", "--include", "f"}, &out); err != nil || !strings.Contains(out.String(), "\t1\t") {
		t.Fatalf("%v %q", err, out.String())
	}
}
