// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/s3"
)

// frontierTree is a synthetic provider (generator: the constants below) of
// a delayed dense frontier: the root's first range sleeps, then lists;
// its siblings list at once, each with fanout children of rows each.
type frontierTree struct {
	siblings, fanout, rows int
	delay                  time.Duration
	listed                 atomic.Int64
}

type ftRange struct{ level, i, j int }

func (f *frontierTree) Root() ftRange { return ftRange{} }

func (f *frontierTree) Expand(ctx context.Context, r ftRange, e source.Emitter[ftRange, s3.Object]) error {
	rows := func(prefix string) error {
		out := make([]schema.Row[s3.Object], f.rows)
		for k := range out {
			key := fmt.Sprintf("%s/%05d", prefix, k)
			out[k] = schema.Row[s3.Object]{Key: key, Data: s3.Object{Size: int64(k), ETag: "etag-" + key}}
		}
		f.listed.Add(int64(len(out)))
		return e.Rows(out)
	}
	switch r.level {
	case 0:
		for i := range f.siblings {
			if err := e.Child(ftRange{level: 1, i: i}); err != nil {
				return err
			}
		}
		return nil
	case 1:
		if r.i == 0 {
			select {
			case <-time.After(f.delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := rows(fmt.Sprintf("s%04d", r.i)); err != nil {
			return err
		}
		for j := range f.fanout {
			if err := e.Child(ftRange{level: 2, i: r.i, j: j}); err != nil {
				return err
			}
		}
		return nil
	}
	return rows(fmt.Sprintf("s%04d/c%03d", r.i, r.j))
}

type frontierSource struct {
	registry.Bound[s3.Object]
	p *frontierTree
}

func (o *frontierSource) Locator() source.Locator {
	return source.Locator{Kind: "frontier", Root: "frontier://x"}
}
func (o *frontierSource) Listed() int64 { return o.p.listed.Load() }

var (
	frontierMu   sync.Mutex
	frontierNext *frontierTree
)

func init() {
	registry.Register(registry.Factory{Scheme: "frontier", Defaults: func() any { return nil },
		Open: func(ctx context.Context, req registry.Request) (registry.Source, error) {
			frontierMu.Lock()
			defer frontierMu.Unlock()
			return &frontierSource{Bound: registry.Bind[ftRange, s3.Object](frontierNext, &s3.Schema), p: frontierNext}, nil
		}})
}

// TestDelayedFrontierPlateaus (synthetic, labelled: generator
// frontierTree{siblings: 200, fanout: 10, rows: 100, delay: 1 s}, 2,201
// ranges and 220,000 rows; the smallest row budget, 128 KiB;
// one real encoder; spill on; byte accounting as in the command): hundreds
// of fast siblings finish while the first range sleeps. The order stage's
// metadata stays bounded: outstanding nodes plateau below the tree's size
// (children are charged to the budget), open spill readers stay within
// order.MaxSpillReaders, live spill within its cap, and the heap growth is
// logged against a loose ceiling. Before child nodes were charged, every
// node of the speculative tree was held at once.
func TestDelayedFrontierPlateaus(t *testing.T) {
	ft := &frontierTree{siblings: 200, fanout: 10, rows: 100, delay: time.Second}
	frontierMu.Lock()
	frontierNext = ft
	frontierMu.Unlock()
	out := filepath.Join(t.TempDir(), "ds")
	pr := &Progress{}
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peakHeap atomic.Uint64
	var peak order.GaugeSnapshot
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		tk := time.NewTicker(5 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			peakHeap.Store(max(peakHeap.Load(), m.HeapInuse))
			if s := pr.Snapshot(); s.Order != nil {
				peak = *s.Order
			}
		}
	}()
	sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: "frontier://x"}, Output: OutputConfig{Dir: out},
		Limits: Limits{RowBudget: minRowBudget, Encoders: 1, Workers: 16, PartSize: 256 << 10, Spill: SpillLimits{Max: -1}}, Progress: pr})
	close(stop)
	<-sampled
	if err != nil {
		t.Fatal(err)
	}
	nodes := int64(1 + ft.siblings + ft.siblings*ft.fanout)
	res := sum.Resources
	t.Logf("synthetic: %d ranges, %d rows; peak nodes %d, markers %d, spill readers %d, spill live %d bytes (sampled %+v); heap +%d MiB; spill %+v",
		nodes, sum.Output.RowsEmitted, res.PeakNodes, res.PeakMarkers, res.Spill.PeakReaders, res.Spill.PeakBytes, peak, (peakHeap.Load()-base.HeapInuse)>>20, *res.Spill)
	if res.Spill.Rows == 0 {
		t.Fatal("nothing spilled: the frontier was not delayed long enough")
	}
	if d := res.Drain; d == nil || d.ReadAhead == nil || sum.Timings.ListingDoneAt.IsZero() {
		t.Fatalf("no drain account: %+v", d)
	} else {
		t.Logf("drain: tail %v, read-ahead %+v", time.Duration(d.TailNs), *d.ReadAhead)
	}
	if res.PeakNodes >= nodes*3/4 {
		t.Fatalf("outstanding nodes peaked at %d of %d: no plateau", res.PeakNodes, nodes)
	}
	if res.Spill.PeakReaders > order.MaxSpillReaders {
		t.Fatalf("%d spill readers open, cap %d", res.Spill.PeakReaders, order.MaxSpillReaders)
	}
	if peak.PeakNodes != res.PeakNodes || peak.Nodes > nodes {
		t.Logf("progress gauges: %+v", peak)
	}
	if grew := int64(peakHeap.Load()) - int64(base.HeapInuse); grew > 256<<20 {
		t.Fatalf("heap grew by %d MiB", grew>>20)
	}
}
