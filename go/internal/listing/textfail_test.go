// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/textout"
)

// TestTextWriteFailureStopsListing: the text file's first write fails in
// the sequencer while the source holds its next page. The failure cancels
// the listing at once and is what the run reports; before, the sequencer
// only stored it, nothing sent again, and the run waited for its deadline,
// which Abort then reported instead.
func TestTextWriteFailureStopsListing(t *testing.T) {
	var keys []string
	for i := range 3000 { // about 0.7 MB of TSV before the held range: past the 256 KiB buffer
		keys = append(keys, fmt.Sprintf("k%05d/%s", i, strings.Repeat("x", 200)))
	}
	hold := make(chan struct{})
	defer close(hold)
	scriptMu.Lock()
	scriptNext = &scripted{keys: keys, chunk: 100, hold: hold, holdAt: 2500}
	scriptMu.Unlock()
	dir := t.TempDir()
	out := filepath.Join(dir, "keys.tsv")
	defer faults.SetIO(func(op, name string, b []byte) (int, error) {
		if op == "write" && strings.HasPrefix(filepath.Base(name), ".keys.tsv.") && strings.HasSuffix(name, ".tmp") {
			return 0, syscall.EIO
		}
		return 0, nil
	})()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	_, err := Run(ctx, Config{Source: SourceConfig{URI: "scripted://x"},
		Output: OutputConfig{Text: &textout.Config{Path: out, Format: textout.TSV}},
		Limits: Limits{Workers: 4, Encoders: 3, RowBudget: 64 << 20}})
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("err %v after %v, want the write's EIO", err, time.Since(start))
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the failure stopped the run after %v", d)
	}
}

// TestTextCompressorFailureStopsListing: as above with a .zst file and
// parallel compression jobs, the disk stalling until the source holds its
// next range (the sequencer idle) and then failing in the compressor's
// background flusher. The disk's error stops the run at once and is what
// it reports; before, nothing saw it and the run reported its deadline.
func TestTextCompressorFailureStopsListing(t *testing.T) {
	var keys []string
	for i := range 22_000 { // about 5 MB of TSV before the held range: one 4 MiB section to a job
		keys = append(keys, fmt.Sprintf("k%05d/%s", i, strings.Repeat("x", 200)))
	}
	hold, held, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(hold)
	scriptMu.Lock()
	scriptNext = &scripted{keys: keys, chunk: 1000, hold: hold, holdAt: 20_000, held: held}
	scriptMu.Unlock()
	go func() {
		<-held
		time.Sleep(500 * time.Millisecond) // the rows before the held range handed on
		close(release)
	}()
	out := filepath.Join(t.TempDir(), "keys.tsv.zst")
	var writes atomic.Int64
	defer faults.SetIO(func(op, name string, b []byte) (int, error) {
		// The frame header (the sequencer's write) passes; the jobs'
		// output (the compressor's flusher) stalls, then fails.
		if op == "write" && strings.HasPrefix(filepath.Base(name), ".keys.tsv.zst.") && strings.HasSuffix(name, ".tmp") && writes.Add(1) > 1 {
			<-release
			return 0, syscall.EIO
		}
		return 0, nil
	})()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	sum, err := Run(ctx, Config{Source: SourceConfig{URI: "scripted://x"},
		Output: OutputConfig{Text: &textout.Config{Path: out, Format: textout.TSV, Compression: textout.Zstd}},
		Limits: Limits{Workers: 4, Encoders: 3, RowBudget: 64 << 20}})
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("err %v after %v, want the disk's EIO", err, time.Since(start))
	}
	if c := sum.Output.Text.Compressor; c == nil || c.Jobs < 2 {
		t.Fatalf("compressor %+v: the test needs parallel jobs", c)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the failure stopped the run after %v", d)
	}
}
