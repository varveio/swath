// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
)

// TestPartCountGrowth measures what a run keeps per part: the journal's
// and manifest's bytes, the peak Go heap and the wall, for runs of 1,000
// and 10,000 one-row parts (a GCS source of that many names, PartSize 1,
// eight workers).
// Opt-in measurement (SWATH_GROWTH=1), not a gate: it logs the numbers.
func TestPartCountGrowth(t *testing.T) {
	if os.Getenv("SWATH_GROWTH") == "" {
		t.Skip("measurement; set SWATH_GROWTH=1")
	}
	for _, n := range []int{1000, 10000} {
		ep := gcsBucket(t, n)
		out := filepath.Join(t.TempDir(), "ds")
		runtime.GC()
		var base runtime.MemStats
		runtime.ReadMemStats(&base)
		var peak atomic.Uint64
		stop := make(chan struct{})
		go func() {
			var m runtime.MemStats
			for {
				select {
				case <-stop:
					return
				case <-time.After(20 * time.Millisecond):
				}
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > peak.Load() {
					peak.Store(m.HeapAlloc)
				}
			}
		}()
		var jbytes atomic.Int64 // the journal's size when the last part is published
		restore := faults.Set(func(p string) error {
			if p == "published" {
				if fi, err := os.Stat(filepath.Join(out, dataset.JournalDir, journal.Name)); err == nil {
					jbytes.Store(fi.Size())
				}
			}
			return nil
		})
		start := time.Now()
		cfg := gcsConfig(ep, out, false)
		cfg.Limits.PartSize = 1
		sum, err := Run(context.Background(), cfg)
		wall := time.Since(start)
		restore()
		close(stop)
		if err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		size := func(p string) int64 {
			fi, err := os.Stat(p)
			if err != nil {
				return -1
			}
			return fi.Size()
		}
		parts, _ := filepath.Glob(filepath.Join(out, "data", "*.parquet"))
		t.Logf("parts=%d rows=%d wall=%s journal=%d B manifest=%d B peak heap=%.1f MiB (base %.1f) retained after run=%.1f MiB",
			len(parts), sum.Output.RowsListed, wall.Round(time.Millisecond), jbytes.Load(), size(filepath.Join(out, "manifest.json")),
			float64(peak.Load())/(1<<20), float64(base.HeapAlloc)/(1<<20), float64(after.HeapAlloc)/(1<<20))
	}
}
