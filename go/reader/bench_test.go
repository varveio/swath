// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

// The reader's benchmarks (BenchmarkReader*) measure Scan, Stream and the
// planning and splitting Scan begins with over one dataset:
// SWATH_READER_BENCH_DIR, an existing s3-objects/v1 dataset, or else a
// fixture of SWATH_READER_BENCH_ROWS generated rows (default 4,000,000;
// writeFixture), written once per process before any timing and removed
// at exit. Each reports ns/row, allocs/row and B/row (from
// runtime.MemStats over the timed loop, every goroutine's), GC cycles per
// operation and the peak live heap sampled during it (peak-heap-MiB).
// "warm" reuses one Dataset that has scanned once untimed; "fresh" opens
// a Dataset inside each operation. testing.md has the commands.

func benchRows(tb testing.TB) int64 {
	s := os.Getenv("SWATH_READER_BENCH_ROWS")
	if s == "" {
		return 4_000_000
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		tb.Fatalf("SWATH_READER_BENCH_ROWS=%q: want a positive row count", s)
	}
	return n
}

var benchFixture struct {
	once sync.Once
	dir  string
	tmp  string // the generated fixture's directory, removed by TestMain
	rows int64
	err  error
}

// benchDataset returns the benchmarks' dataset and its row count.
func benchDataset(b *testing.B) (string, int64) {
	f := &benchFixture
	f.once.Do(func() {
		if f.dir = os.Getenv("SWATH_READER_BENCH_DIR"); f.dir == "" {
			if f.tmp, f.err = os.MkdirTemp("", "swath-reader-bench-"); f.err != nil {
				return
			}
			f.dir = filepath.Join(f.tmp, "ds")
			start := time.Now()
			if f.err = writeFixture(context.Background(), f.dir, benchRows(b), 1); f.err != nil {
				return
			}
			fmt.Fprintf(os.Stderr, "generated %s in %v\n", f.dir, time.Since(start).Round(time.Millisecond))
		}
		var g fixtureGeometry
		if g, f.err = inspectFixture(f.dir); f.err != nil {
			return
		}
		f.rows = g.Rows
		fmt.Fprintf(os.Stderr, "bench dataset %s: %s\n", f.dir, g)
	})
	if f.err != nil {
		b.Fatal(f.err)
	}
	return f.dir, f.rows
}

func TestMain(m *testing.M) {
	code := m.Run()
	if benchFixture.tmp != "" {
		_ = os.RemoveAll(benchFixture.tmp)
	}
	os.Exit(code)
}

func benchOpen(b *testing.B, dir string) *Dataset {
	d, err := OpenDir(context.Background(), dir, seekparquet.Options{})
	if err != nil {
		b.Fatal(err)
	}
	return d
}

// heapPeak samples the live heap (heap objects, including those not yet
// swept) every millisecond until stopped.
type heapPeak struct {
	stop chan struct{}
	done chan uint64
}

func startHeapPeak() *heapPeak {
	h := &heapPeak{stop: make(chan struct{}), done: make(chan uint64)}
	go func() {
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		var peak uint64
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			metrics.Read(s)
			peak = max(peak, s[0].Value.Uint64())
			select {
			case <-h.stop:
				h.done <- peak
				return
			case <-t.C:
			}
		}
	}()
	return h
}

func (h *heapPeak) Stop() uint64 {
	close(h.stop)
	return <-h.done
}

// measure runs op b.N times (b.Loop) and reports the per-row metrics for
// rows rows an operation.
func measure(b *testing.B, rows int64, op func()) {
	b.ReportAllocs()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	peak := startHeapPeak()
	start := time.Now()
	n := 0
	b.ResetTimer()
	for b.Loop() {
		op()
		n++
	}
	b.StopTimer()
	elapsed := time.Since(start)
	p := peak.Stop()
	runtime.ReadMemStats(&after)
	total := float64(rows) * float64(n)
	b.ReportMetric(float64(elapsed.Nanoseconds())/total, "ns/row")
	b.ReportMetric(float64(after.Mallocs-before.Mallocs)/total, "allocs/row")
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/total, "B/row")
	b.ReportMetric(float64(after.NumGC-before.NumGC)/float64(n), "gc/op")
	b.ReportMetric(float64(p)/(1<<20), "peak-heap-MiB")
}

// The callbacks: noop does nothing with an object; touch reads its key,
// size, modification time and storage class, as a consumer tallying
// classes and bytes does.
func touch(o *Object) uint64 {
	h := uint64(len(o.Key))
	if len(o.Key) > 0 {
		h += uint64(o.Key[len(o.Key)-1])
	}
	if o.Size != nil {
		h += uint64(*o.Size)
	}
	if o.LastModified != nil {
		h += uint64(o.LastModified.UnixMicro())
	}
	if o.StorageClass != nil {
		h += uint64(len(*o.StorageClass))
	}
	return h
}

var benchSink atomic.Uint64

// BenchmarkReaderScan: Scan of the whole dataset at Workers 1, 4 and 8
// (GOMAXPROCS unchanged), end to end: planning, splitting and scanning.
func BenchmarkReaderScan(b *testing.B) {
	dir, rows := benchDataset(b)
	callbacks := []struct {
		name string
		fn   func(batch []Object) error
	}{
		{"noop", func([]Object) error { return nil }},
		{"touch", func(batch []Object) error {
			var h uint64
			for i := range batch {
				h += touch(&batch[i])
			}
			benchSink.Add(h)
			return nil
		}},
	}
	for _, workers := range []int{1, 4, 8} {
		for _, cb := range callbacks {
			for _, fresh := range []bool{false, true} {
				if fresh && cb.name != "noop" {
					continue
				}
				name := fmt.Sprintf("workers=%d/%s/%s", workers, cb.name, map[bool]string{false: "warm", true: "fresh"}[fresh])
				b.Run(name, func(b *testing.B) {
					var counted atomic.Int64
					fn := func(batch []Object) error {
						counted.Add(int64(len(batch)))
						return cb.fn(batch)
					}
					scan := func(d *Dataset) {
						if err := d.Scan(context.Background(), ScanRequest{Workers: workers}, fn); err != nil {
							b.Fatal(err)
						}
					}
					var d *Dataset
					if !fresh {
						d = benchOpen(b, dir)
						defer d.Close()
						scan(d) // warm: parts checked, indexes loaded
					}
					counted.Store(0)
					measure(b, rows, func() {
						if fresh {
							d = benchOpen(b, dir)
							defer d.Close()
						}
						scan(d)
					})
					if c := counted.Load(); c%rows != 0 {
						b.Fatalf("%d objects scanned, not a multiple of %d", c, rows)
					}
				})
			}
		}
	}
}

// BenchmarkReaderStream: Stream of the whole dataset, ordered and owning.
func BenchmarkReaderStream(b *testing.B) {
	dir, rows := benchDataset(b)
	for _, cb := range []string{"noop", "touch"} {
		b.Run(cb+"/warm", func(b *testing.B) {
			d := benchOpen(b, dir)
			defer d.Close()
			var count int64
			var h uint64
			fn := func(o Object) error {
				count++
				if cb == "touch" {
					h += touch(&o)
				}
				return nil
			}
			if err := d.Stream(context.Background(), fn); err != nil {
				b.Fatal(err)
			}
			count = 0
			measure(b, rows, func() {
				if err := d.Stream(context.Background(), fn); err != nil {
					b.Fatal(err)
				}
			})
			benchSink.Add(h)
			if count%rows != 0 {
				b.Fatalf("%d objects streamed, not a multiple of %d", count, rows)
			}
		})
	}
}

// BenchmarkReaderPlan: the planning and splitting a Scan begins with
// (PlanRanges over the whole dataset projecting Object's columns, then
// Split into Workers shards), alone; per-row metrics are over the
// dataset's rows. It also reports the shards Split made.
func BenchmarkReaderPlan(b *testing.B) {
	dir, rows := benchDataset(b)
	for _, workers := range []int{1, 4, 8, 18} {
		for _, fresh := range []bool{false, true} {
			b.Run(fmt.Sprintf("workers=%d/%s", workers, map[bool]string{false: "warm", true: "fresh"}[fresh]), func(b *testing.B) {
				var d *Dataset
				plan := func() int {
					columns, err := d.currentColumns()
					if err != nil {
						b.Fatal(err)
					}
					p, err := d.sp.PlanRanges(context.Background(), []seekparquet.Range{prefixSpan(nil).rng()}, columns)
					if err != nil {
						b.Fatal(err)
					}
					shards, err := p.Split(context.Background(), workers)
					if err != nil {
						b.Fatal(err)
					}
					return len(shards)
				}
				if !fresh {
					d = benchOpen(b, dir)
					defer d.Close()
					if err := d.Scan(context.Background(), ScanRequest{}, func([]Object) error { return nil }); err != nil {
						b.Fatal(err)
					}
				}
				var shards int
				measure(b, rows, func() {
					if fresh {
						d = benchOpen(b, dir)
						defer d.Close()
					}
					shards = plan()
				})
				b.ReportMetric(float64(shards), "shards")
			})
		}
	}
}

// allocFixture is a small generated dataset for the allocation budget,
// written once per process.
var allocFixture struct {
	once sync.Once
	dir  string
	err  error
}

const allocRows = 20_000

// The allocation budgets per object of Scan (Workers 1, no-op callback)
// and Stream over allocFixture, on a warm Dataset: generous ceilings set
// from the reader before the batch-scan work (Scan 13.1, Stream 13.2
// allocations an object at 5220c636, with or without -race), about 1.5
// times them, to be tightened as allocations are removed. A change that
// raises allocations past them fails here.
const (
	scanAllocBudget   = 20
	streamAllocBudget = 20
)

// TestScanAllocations: Scan and Stream stay within their allocation
// budgets per object, and every object is handed on.
func TestScanAllocations(t *testing.T) {
	a := &allocFixture
	a.once.Do(func() {
		var tmp string
		if tmp, a.err = os.MkdirTemp("", "swath-reader-alloc-"); a.err != nil {
			return
		}
		a.dir = filepath.Join(tmp, "ds")
		a.err = writeFixture(context.Background(), a.dir, allocRows, 2)
	})
	if a.err != nil {
		t.Fatal(a.err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(a.dir)) })
	d := openDir(t, a.dir)
	ctx := context.Background()
	var n int64
	scan := func() {
		n = 0
		if err := d.Scan(ctx, ScanRequest{Workers: 1}, func(batch []Object) error { n += int64(len(batch)); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	stream := func() {
		n = 0
		if err := d.Stream(ctx, func(Object) error { n++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		name   string
		run    func()
		budget float64
	}{{"Scan", scan, scanAllocBudget}, {"Stream", stream, streamAllocBudget}} {
		perObject := testing.AllocsPerRun(3, c.run) / allocRows
		if n != allocRows {
			t.Fatalf("%s: %d objects, want %d", c.name, n, allocRows)
		}
		t.Logf("%s: %.2f allocations an object (budget %v)", c.name, perObject, c.budget)
		if perObject > c.budget {
			t.Errorf("%s: %.2f allocations an object, over its budget of %v", c.name, perObject, c.budget)
		}
	}
}
