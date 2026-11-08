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

// Ceilings on what a run keeps per part (one-row parts; keys of about 20
// bytes): the journal and the manifest at most growthBase plus growthBytes
// per part, and the live heap when the last part is published (after a
// collection: the router's and the committer's per-part records) at most
// growthHeap more per part between the two sizes. Measured (docs/measurements.md,
// "Recovery and part count"): about 233 and 253 bytes per part, and 1.2 KB of
// live heap per part (2.3 KB under the race detector).
const (
	growthBase  = 1 << 10
	growthBytes = 320
	growthHeap  = 8 << 10
)

// TestPartCountGrowth: a run of n one-row parts (a GCS source of n names,
// PartSize 1, eight workers) keeps the journal, the manifest and the live
// heap within linear ceilings in n, for 200 and 800 parts;
// SWATH_GROWTH=1 adds 10,000 parts and logs the peak heap and the wall.
func TestPartCountGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("lists 1,000 parts")
	}
	sizes := []int{200, 800}
	if os.Getenv("SWATH_GROWTH") != "" {
		sizes = append(sizes, 10000)
	}
	type point struct {
		parts                   int
		journal, manifest, live int64
	}
	var got []point
	for _, n := range sizes {
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
		var jbytes, live atomic.Int64 // at the last part's publication
		var published atomic.Int64
		restore := faults.Set(func(p string) error {
			if p == "renamed" && published.Add(1) == int64(n) {
				if fi, err := os.Stat(filepath.Join(out, dataset.JournalDir, journal.Name)); err == nil {
					jbytes.Store(fi.Size())
				}
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				live.Store(int64(m.HeapAlloc))
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
		fi, err := os.Stat(filepath.Join(out, dataset.ManifestName))
		if err != nil {
			t.Fatal(err)
		}
		parts, _ := filepath.Glob(filepath.Join(out, "data", "*.parquet"))
		if len(parts) != n || published.Load() != int64(n) {
			t.Fatalf("%d parts, %d published, want %d", len(parts), published.Load(), n)
		}
		p := point{n, jbytes.Load(), fi.Size(), live.Load()}
		t.Logf("parts=%d rows=%d wall=%s journal=%d B manifest=%d B live at the last publication=%.1f MiB peak heap=%.1f MiB (base %.1f)",
			n, sum.Output.RowsEmitted, wall.Round(time.Millisecond), p.journal, p.manifest, float64(p.live)/(1<<20),
			float64(peak.Load())/(1<<20), float64(base.HeapAlloc)/(1<<20))
		if limit := int64(growthBase + growthBytes*n); p.journal > limit || p.manifest > limit {
			t.Fatalf("%d parts: journal %d B, manifest %d B, ceiling %d B", n, p.journal, p.manifest, limit)
		}
		got = append(got, p)
	}
	for i := 1; i < len(got); i++ {
		a, b := got[i-1], got[i]
		d := int64(b.parts - a.parts)
		if (b.journal-a.journal)/d > growthBytes || (b.manifest-a.manifest)/d > growthBytes || (b.live-a.live)/d > growthHeap {
			t.Fatalf("%d -> %d parts: per part journal %d B, manifest %d B, live heap %d B (ceilings %d, %d)",
				a.parts, b.parts, (b.journal-a.journal)/d, (b.manifest-a.manifest)/d, (b.live-a.live)/d, growthBytes, growthHeap)
		}
	}
}
