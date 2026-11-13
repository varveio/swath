// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/journal"
)

// TestRecoveryVerificationIsCancellableAndReported (review item
// 37): a resume reads every retained part in full (size and MD5) before it
// lists. A stop during that read ends the resume with the cause, before
// recovery renames or removes anything, and the next resume completes;
// the bytes read are progress (progress.json verified_bytes, part of
// advance), so the watchdog does not take a long verification for a
// stall. Before, the read could not be stopped and was invisible.
func TestRecoveryVerificationIsCancellableAndReported(t *testing.T) {
	datasettest.RequireJournal(t)
	var keys []string
	for i := range 20000 {
		keys = append(keys, fmt.Sprintf("k%06d", i))
	}
	base := t.TempDir()
	clean, out := filepath.Join(base, "clean"), filepath.Join(base, "out")
	if _, err := runScripted(t, &scripted{keys: keys, chunk: 1000}, clean, false); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "renamed" && n.Add(1) == 5 {
			return faults.ErrCrash
		}
		return nil
	})
	_, err := runScripted(t, &scripted{keys: keys, chunk: 1000}, out, false)
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	st, err := journal.Read(out)
	if err != nil {
		t.Fatal(err)
	}
	var retained int64
	for _, r := range st.Records {
		retained += r.Part.Bytes
	}
	journalBefore := readFile(t, journal.Path(out))

	// A stop before verification ends: nothing in the output changes.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scriptMu.Lock()
	scriptNext = &scripted{keys: keys, chunk: 1000}
	scriptMu.Unlock()
	_, err = Run(ctx, Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, Resume: true},
		Limits: Limits{PartSize: 2 << 10, Workers: 4, RowBudget: 128 << 10, PartInterval: -1}})
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "verifying") {
		t.Fatalf("a stopped verification: %v", err)
	}
	if string(readFile(t, journal.Path(out))) != string(journalBefore) {
		t.Fatal("the stopped resume changed the journal")
	}
	// The resume that completes reports what it read.
	pr := &Progress{}
	scriptMu.Lock()
	scriptNext = &scripted{keys: keys, chunk: 1000}
	scriptMu.Unlock()
	start := time.Now()
	if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, Resume: true},
		Limits: Limits{PartSize: 2 << 10, Workers: 4, RowBudget: 128 << 10, PartInterval: -1}, Progress: pr}); err != nil {
		t.Fatal(err)
	}
	s := pr.Snapshot()
	if s.Verified != retained || s.Advance < retained {
		t.Fatalf("verified %d bytes (advance %d), retained parts hold %d", s.Verified, s.Advance, retained)
	}
	t.Logf("verified %d retained parts, %d bytes; resume took %v", len(st.Records), retained, time.Since(start))
	sameDataset(t, "verify", clean, out)
}

// TestVerifyThroughput measures verification (read + MD5) over the files
// in SWATH_VERIFY_DIR (opt-in; evict them from the page cache first for a
// cold read).
func TestVerifyThroughput(t *testing.T) {
	dir := os.Getenv("SWATH_VERIFY_DIR")
	if dir == "" {
		t.Skip("set SWATH_VERIFY_DIR")
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	v := verifier{ctx: context.Background(), pr: &Progress{}}
	start := time.Now()
	for _, e := range ents {
		if _, err := v.measure(filepath.Join(dir, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	d := time.Since(start)
	b := v.pr.verified.Load()
	t.Logf("verified %d files, %d MiB in %v: %.0f MiB/s", len(ents), b>>20, d, float64(b)/(1<<20)/d.Seconds())
}
