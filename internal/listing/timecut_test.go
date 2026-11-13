// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/journal"
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
	datasettest.RequireJournal(t)
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("k%04d", i))
	}
	run := func(out string, resume bool, ks []string, slow, interval time.Duration) (Report, error) {
		scriptMu.Lock()
		scriptNext = &scripted{keys: ks, chunk: 1, slow: slow}
		scriptMu.Unlock()
		return Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, Resume: resume},
			Limits: Limits{PartSize: 2 << 10, Workers: 1, RowBudget: 128 << 10, PartInterval: interval}})
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
			t.Logf("part 0 claimed with %d rows; resumed into %d parts, %d rows listed", claimed, len(m.Parts), sum.Output.RowsEmitted)
		})
	}
}

// TestTimeCutPartDoesNotCalibrate: a source that trickles at first (part
// 0 closed by the part interval with a few rows) and then speeds up must
// cut the rest by size as a clean run does: the ratio is calibrated on the
// first part closed by size, never on a small time-cut part (mostly
// footer). Also across a resume: a run stopped after recording the
// time-cut part 0 (its record holds no frozen limit) and one stopped
// after recording the calibrating part (its limit restored) resume to the
// same part count. Before, part 0's ratio froze a limit about a fifteenth
// of the part size for the whole run and every resume of it.
func TestTimeCutPartDoesNotCalibrate(t *testing.T) {
	datasettest.RequireJournal(t)
	var keys []string
	for i := range 6000 {
		keys = append(keys, fmt.Sprintf("k%05d", i))
	}
	// run lists keys; with slow, the source holds before key 10 until a
	// part is closed (with 10 rows, far below the part size, only the
	// interval closes one), then lists fast: a slow start that does not
	// depend on the host's timing. The 10 rows reach the sink before the
	// hold whichever goroutine lists key 10 (the drainer hands on what it
	// batched before it lists a range itself), so the cut comes one
	// interval into the hold; 30 s without it is a failure, not a pass
	// without a time cut. hook, if set, sees every fault point too.
	run := func(out string, resume bool, slow bool, interval time.Duration, hook func(string) error) (Report, error) {
		p := &scripted{keys: keys, chunk: 1}
		cut := make(chan struct{})
		var once sync.Once
		if slow {
			p.slowAt = func(i int) time.Duration {
				if i == 10 {
					select {
					case <-cut:
					case <-time.After(30 * time.Second):
						t.Error("no part closed during the hold: the rows before it never reached the sink")
					}
				}
				return 0
			}
		}
		restore := faults.Set(func(pt string) error {
			if pt == "part-closed" {
				once.Do(func() { close(cut) })
			}
			if hook != nil {
				return hook(pt)
			}
			return nil
		})
		defer restore()
		scriptMu.Lock()
		scriptNext = p
		scriptMu.Unlock()
		return Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, Resume: resume},
			Limits: Limits{PartSize: 2 << 10, Workers: 1, RowBudget: 128 << 10, PartInterval: interval}})
	}
	ref := filepath.Join(t.TempDir(), "ref")
	refRep, err := run(ref, false, false, -1, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := checkContract(t, ref)
	parts := func(t *testing.T, out string) int {
		t.Helper()
		if got := checkContract(t, out); !slices.Equal(got, want) {
			t.Fatalf("%d rows, want %d", len(got), len(want))
		}
		var m dataset.Manifest
		readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
		return len(m.Parts)
	}
	// Each part the interval closed, plus the clean run's parts, plus the
	// calibrating part's slack (a frozen tiny limit gives hundreds).
	ref0 := refRep.Output.PartsWritten
	out := filepath.Join(t.TempDir(), "slow")
	rep, err := run(out, false, true, 50*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Resources.PartTimeCuts == 0 {
		t.Fatal("no part cut by time")
	}
	if n := parts(t, out); n > ref0+int(rep.Resources.PartTimeCuts)+2 {
		t.Fatalf("%d parts (%d by time) after a slow start, a clean run %d", n, rep.Resources.PartTimeCuts, ref0)
	}
	for _, nth := range []int64{1, 2} {
		t.Run(fmt.Sprintf("resume after record %d", nth), func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "ds")
			var seen atomic.Int64
			_, err := run(out, false, true, 50*time.Millisecond, func(p string) error {
				if p == "recorded" && seen.Add(1) == nth {
					return faults.ErrCrash
				}
				return nil
			})
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("crash not injected: %v", err)
			}
			if _, err := run(out, true, false, -1, nil); err != nil {
				t.Fatalf("resume: %v", err)
			}
			// nth parts kept, the rest listed by size only.
			if n := parts(t, out); n > ref0+int(nth)+2 {
				t.Fatalf("%d parts after the resume, a clean run %d", n, ref0)
			}
		})
	}
}

// TestTimeCutLastPartEmptyTail: the part interval closes the last part
// that will hold a row while the listing goes on through a tail the
// filters drop entirely (--include '*.csv', no .csv key in the last stretch
// of the bucket), so Close finds no open part and no record is marked
// final. A run stopped there, every part recorded and published and no
// manifest, is not complete to its resume: the resume restarts after the
// last recorded key, lists the tail again, routes nothing, and commits the
// retained part alone, the dataset of a clean run. Deterministic: the
// tail's first range waits until part 0, cut by time, is being recorded.
func TestTimeCutLastPartEmptyTail(t *testing.T) {
	datasettest.RequireJournal(t)
	keys := []string{"a.csv"}
	for i := range 20 {
		keys = append(keys, fmt.Sprintf("b%03d.txt", i))
	}
	tail := int64(len(keys) - 1)
	run := func(out string, resume bool, p *scripted) (Report, error) {
		scriptMu.Lock()
		scriptNext = p
		scriptMu.Unlock()
		return Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Filter: &filter.Spec{Include: []string{"*.csv"}},
			Output: OutputConfig{Dir: out, Resume: resume}, Limits: Limits{PartSize: 1 << 20, Workers: 1, RowBudget: 128 << 10, PartInterval: 20 * time.Millisecond}})
	}
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := run(ref, false, &scripted{keys: keys, chunk: 1}); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	recording := make(chan struct{})
	var appends atomic.Int64
	restore := faults.Set(func(p string) error {
		switch {
		case p == "journal-append" && appends.Add(1) == 2: // 1: the header; 2: part 0's record
			close(recording)
		case p == "published": // every part recorded and published, no manifest
			return faults.ErrCrash
		}
		return nil
	})
	_, err := run(out, false, &scripted{keys: keys, chunk: 1, hold: recording, holdAt: 1})
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	st, err := journal.Read(out)
	if err != nil || len(st.Records) != 1 || st.Last().Final || st.Last().RowsTotal != 1 {
		t.Fatalf("journal: %v, want one record of one row, not final", err)
	}
	sum, err := run(out, true, &scripted{keys: keys, chunk: 1})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if o := sum.Output; o.RowsRetained != 1 || o.RowsEmitted != 0 || o.RowsFiltered != tail {
		t.Fatalf("resume: %d rows retained, %d emitted, %d filtered; want 1, 0, %d (the tail listed again)", o.RowsRetained, o.RowsEmitted, o.RowsFiltered, tail)
	}
	if got, want := checkContract(t, out), checkContract(t, ref); !slices.Equal(got, want) {
		t.Fatalf("keys %q, a clean run %q", got, want)
	}
	for _, dir := range []string{ref, out} {
		if parts, _ := filepath.Glob(filepath.Join(dir, dataset.DataDir, "*")); len(parts) != 1 {
			t.Fatalf("%s: %d parts, want 1", dir, len(parts))
		}
	}
	a, _ := os.ReadFile(filepath.Join(ref, dataset.DataDir, dataset.PartName(0)))
	b, _ := os.ReadFile(filepath.Join(out, dataset.DataDir, dataset.PartName(0)))
	if !bytes.Equal(a, b) {
		t.Fatal("part 0 differs from a clean run's")
	}
}
