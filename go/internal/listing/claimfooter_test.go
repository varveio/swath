// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	sourcegcs "github.com/varveio/swath/go/internal/source/gcs"
)

// hasColumn reports whether the Parquet file at path has a top-level
// column of that name.
func hasColumn(t *testing.T, path, name string) bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	pf, err := pq.OpenFile(f, st.Size())
	if err != nil {
		t.Fatal(err)
	}
	_, ok := pf.Schema().Lookup(name)
	return ok
}

// TestClaimedPartKeepsItsColumns: a GCS run by a build that wrote no
// time_created column (the schema without its last column, a row's
// logical size 8 bytes less) stops with
// part 1 claimed and unrecorded: at journal-append (the record never
// written) and as a torn record (written, then cut short before the part
// was renamed). A resume by a build that writes the column must reproduce
// that suspect byte for byte, so it writes part 1 with the columns of the
// claimed part's own footer and only the parts after it with the column;
// the source is unchanged, so the resume completes with a clean run's
// rows. Before, the resume wrote part 1 with the column, its bytes
// differed and publish refused the unchanged source with
// ErrAmbiguousTail.
func TestClaimedPartKeepsItsColumns(t *testing.T) {
	datasettest.RequireJournal(t)
	ep := gcsBucket(t, 4000)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), gcsConfig(ep, ref, false)); err != nil {
		t.Fatal(err)
	}
	want := checkContract(t, ref)
	columns, size := sourcegcs.Schema.Columns, sourcegcs.Schema.Bytes
	older := func() {
		sourcegcs.Schema.Columns = columns[:len(columns)-1]
		sourcegcs.Schema.Bytes = func(o *sourcegcs.Object) int64 { return size(o) - 8 }
	}
	current := func() { sourcegcs.Schema.Columns, sourcegcs.Schema.Bytes = columns, size }
	t.Cleanup(current)
	if columns[len(columns)-1].Name != "time_created" {
		t.Fatalf("last column %s", columns[len(columns)-1].Name)
	}
	for _, tc := range []struct {
		name  string
		point string // the crash: its second record is part 1's
		torn  bool   // the record written, then cut short
	}{
		{"claimed, never recorded", "journal-append", false},
		{"torn record", "recorded", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "ds")
			nth := int64(2)
			if tc.point == "journal-append" {
				nth = 3 // the header's append comes first
			}
			var seen atomic.Int64
			older()
			restore := faults.Set(func(p string) error {
				if p == tc.point && seen.Add(1) == nth {
					return faults.ErrCrash
				}
				return nil
			})
			_, err := Run(context.Background(), gcsConfig(ep, out, false))
			restore()
			current()
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("crash not injected: %v", err)
			}
			if tc.torn {
				path := journal.Path(out)
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, b[:len(b)-5], 0o644); err != nil {
					t.Fatal(err)
				}
			}
			claim := dataset.ClaimPath(out, 1)
			claimed, err := measure(claim)
			if err != nil {
				t.Fatalf("part 1 is not claimed: %v", err)
			}
			if hasColumn(t, claim, "time_created") {
				t.Fatal("the claimed part holds time_created")
			}
			if _, err := Run(context.Background(), gcsConfig(ep, out, true)); err != nil {
				t.Fatalf("resume: %v", err)
			}
			if got := checkContract(t, out); !slices.Equal(got, want) {
				t.Fatalf("%d rows after the resume, want %d", len(got), len(want))
			}
			var m dataset.Manifest
			readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
			if len(m.Parts) < 4 {
				t.Fatalf("%d parts", len(m.Parts))
			}
			if p := m.Parts[1]; p.MD5 != claimed.MD5 || p.Bytes != claimed.Bytes {
				t.Fatalf("part 1 is %d bytes, md5 %s; the claimed part %d bytes, md5 %s", p.Bytes, p.MD5, claimed.Bytes, claimed.MD5)
			}
			for i, p := range m.Parts {
				if got := hasColumn(t, filepath.Join(out, filepath.FromSlash(p.Path)), "time_created"); got != (i > 1) {
					t.Fatalf("part %d: time_created %v", i, got)
				}
			}
			if _, err := os.Stat(filepath.Join(out, dataset.Workspace, dataset.SuspectDir, dataset.PartName(1))); !os.IsNotExist(err) {
				t.Fatalf("the reproduced suspect part was kept: %v", err)
			}
		})
	}
}
