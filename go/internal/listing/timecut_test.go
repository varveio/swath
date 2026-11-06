// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
)

// footerRows is the row count in a part file's footer.
func footerRows(t *testing.T, path string) int64 {
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
	return pf.NumRows()
}

// TestTimeCutClaimResumes: a trickling run closes its first part by the
// part interval, claims it and stops at journal-append (the claim is
// durable, its record never written). The part is set aside as the
// suspect. A resume over the unchanged source, faster and with the
// interval off, changed or the same, reproduces that part at the claim's
// boundary and completes with exactly the rows of a clean run (before,
// the faster resume put more rows into part 0 and the byte comparison
// refused the unchanged source with ErrAmbiguousTail). A source changed
// inside the claimed part is still refused, the part kept.
func TestTimeCutClaimResumes(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("k%04d", i))
	}
	run := func(out string, resume bool, ks []string, slow, interval time.Duration) (Report, error) {
		scriptMu.Lock()
		scriptNext = &scripted{keys: ks, chunk: 1, slow: slow}
		scriptMu.Unlock()
		return Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, Resume: resume},
			Limits: Limits{PartSize: 2 << 10, Workers: 1, Memory: 256 << 10, PartInterval: interval}})
	}
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := run(ref, false, keys, 0, -1); err != nil {
		t.Fatal(err)
	}
	want := checkContract(t, ref)
	for _, tc := range []struct {
		name     string
		interval time.Duration
		keys     []string
	}{
		{"interval off", -1, keys},
		{"interval changed", time.Hour, keys},
		{"interval same", 100 * time.Millisecond, keys},
		{"row deleted in the claim", -1, slices.Delete(slices.Clone(keys), 1, 2)},
		{"row added in the claim", -1, slices.Insert(slices.Clone(keys), 1, "k0000a")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "ds")
			var seen atomic.Int64
			restore := faults.Set(func(p string) error {
				if p == "journal-append" && seen.Add(1) == 2 { // 1: the header
					return faults.ErrCrash
				}
				return nil
			})
			_, err := run(out, false, keys, 5*time.Millisecond, 100*time.Millisecond)
			restore()
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("crash not injected: %v", err)
			}
			claimed := footerRows(t, dataset.ClaimPath(out, 0))
			if claimed < 2 || claimed >= int64(len(keys)) {
				t.Fatalf("part 0 claimed with %d rows: not a time cut", claimed)
			}
			sum, err := run(out, true, tc.keys, 0, tc.interval)
			kept := filepath.Join(out, dataset.Workspace, dataset.SuspectDir, dataset.PartName(0))
			if len(tc.keys) != len(keys) || tc.keys[1] != keys[1] {
				if !errors.Is(err, ErrAmbiguousTail) {
					t.Fatalf("changed source: resume did not refuse: %v", err)
				}
				if footerRows(t, kept) != claimed {
					t.Fatal("changed source: the claimed part is not kept")
				}
				return
			}
			if err != nil {
				t.Fatalf("resume (part 0 claimed with %d rows): %v", claimed, err)
			}
			if got := checkContract(t, out); !slices.Equal(got, want) {
				t.Fatalf("%d rows after the resume, want %d", len(got), len(want))
			}
			var m dataset.Manifest
			readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
			if m.Parts[0].Rows != claimed {
				t.Fatalf("part 0 holds %d rows, the claim %d", m.Parts[0].Rows, claimed)
			}
			if _, err := os.Stat(kept); !os.IsNotExist(err) {
				t.Fatal("the reproduced suspect part was kept")
			}
			t.Logf("part 0 claimed with %d rows; resumed into %d parts, %d rows listed", claimed, len(m.Parts), sum.Output.RowsListed)
		})
	}
}
