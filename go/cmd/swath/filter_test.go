// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
)

// TestFiltersThroughTheCommand: the filters drop rows before the order
// stage; the manifest's coverage.filters names them (a reader that needs a
// whole source refuses such a dataset), the report counts the dropped
// rows, an anchored --include narrows the listed prefix, and a resume
// keeps the filters (they are part of the identity: another value is not
// accepted).
func TestFiltersThroughTheCommand(t *testing.T) {
	src, keys := textBucket(t, 3000)
	var want []string
	for _, k := range keys {
		if strings.HasPrefix(k, "d01/") && !strings.Contains(k, "sub1") {
			want = append(want, k)
		}
	}
	if len(want) == 0 {
		t.Fatal("no keys to keep")
	}
	var out bytes.Buffer
	args := append(src, "--include", "^d01/", "--exclude", "sub1", "--format", "tsv")
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
	if err := run(context.Background(), append(append(src, "-o", dir, "--exclude", "sub1", "--min-size", "1", "--report", report), unjournaled...), io.Discard, diagList); err != nil {
		t.Fatal(err)
	}
	var m dataset.Manifest
	readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
	if !slices.Equal(m.Coverage.Filters, []string{"exclude=sub1", "min_size=1"}) {
		t.Fatalf("coverage.filters %q", m.Coverage.Filters)
	}
	var r struct {
		Output struct {
			RowsEmitted  int64 `json:"rows_emitted"`
			RowsFiltered int64 `json:"rows_filtered"`
		}
		Invocation struct{ Source string }
	}
	readJSON(t, report, &r)
	if r.Output.RowsEmitted+r.Output.RowsFiltered != int64(len(keys)) || r.Output.RowsFiltered == 0 {
		t.Fatalf("rows %+v of %d", r.Output, len(keys))
	}
	r0 := r.Output.RowsEmitted
	if err := runCount(context.Background(), append(src, "--include", "^d01/", "--report", report), io.Discard); err != nil {
		t.Fatal(err)
	}
	readJSON(t, report, &r)
	if r.Invocation.Source != "s3://bkt/d01/" || r.Output.RowsFiltered != 0 {
		t.Fatalf("narrowed run: %+v", r)
	}
	var u usageError
	if !dataset.DirSyncEnforced {
		return // the resume cases: a resume is refused here (Windows)
	}
	if err := dispatch(context.Background(), []string{"resume", dir, "--exclude", "x"}, io.Discard); !errors.As(err, &u) {
		t.Fatalf("a filter given to resume: %v", err)
	}
	// An interrupted filtered run resumes with its filters: the same rows.
	crashed := filepath.Join(t.TempDir(), "ds")
	crashRun(t, append(src, "-o", crashed, "--exclude", "sub1", "--min-size", "1", "--part-size", "4KiB", "--part-interval", "0", "--workers", "2"), 2)
	if err := dispatch(context.Background(), []string{"resume", crashed}, io.Discard); err != nil {
		t.Fatal(err)
	}
	var m2 dataset.Manifest
	readJSON(t, filepath.Join(crashed, dataset.ManifestName), &m2)
	var rows int64
	for _, p := range m2.Parts {
		rows += p.Rows
	}
	if rows != r0 || !slices.Equal(m2.Coverage.Filters, m.Coverage.Filters) {
		t.Fatalf("resumed: %d rows (want %d), filters %q", rows, r0, m2.Coverage.Filters)
	}
}
