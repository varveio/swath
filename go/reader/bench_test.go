// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

// The reader's benchmarks (BenchmarkReader*) measure Scan and Stream over
// a fixture of SWATH_READER_BENCH_ROWS generated rows (default 4,000,000;
// writeFixture), written once per process before any timing and removed
// at exit. Each operation reads the whole dataset with a Dataset opened
// and scanned once before the timed loop (warm) and a callback that does
// nothing with the objects. Besides the standard ns/op, B/op and
// allocs/op, each reports rows/op, the denominator of the per-row costs,
// and ns/row from the benchmark's own elapsed time.

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
	tmp  string // removed by TestMain
	dir  string
	rows int64
	err  error
}

// benchDataset returns the benchmarks' generated dataset and its rows.
func benchDataset(b *testing.B) (string, int64) {
	f := &benchFixture
	f.once.Do(func() {
		if f.tmp, f.err = os.MkdirTemp("", "swath-reader-bench-"); f.err != nil {
			return
		}
		f.dir, f.rows = filepath.Join(f.tmp, "ds"), benchRows(b)
		start := time.Now()
		if f.err = writeFixture(context.Background(), f.dir, f.rows, 1); f.err != nil {
			return
		}
		g, err := inspectFixture(f.dir)
		if f.err = err; err == nil && g.Rows != f.rows {
			f.err = fmt.Errorf("fixture has %d rows, want %d", g.Rows, f.rows)
		}
		fmt.Fprintf(os.Stderr, "generated %s in %v: %s\n", f.dir, time.Since(start).Round(time.Millisecond), g)
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

// benchWarm opens the dataset and reads it once through read, untimed.
func benchWarm(b *testing.B, dir string, read func(*Dataset)) *Dataset {
	d, err := OpenDir(context.Background(), dir, seekparquet.Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Close() })
	read(d)
	return d
}

// benchLoop times read over b.Loop, checks that the callbacks saw exactly
// rows objects an operation, and reports rows/op and ns/row.
func benchLoop(b *testing.B, rows int64, counted *atomic.Int64, read func()) {
	b.ReportAllocs()
	counted.Store(0)
	n := int64(0)
	for b.Loop() {
		read()
		n++
	}
	if got := counted.Load(); rows <= 0 || got != rows*n {
		b.Fatalf("%d objects in %d operations, want %d each", got, n, rows)
	}
	b.ReportMetric(float64(rows), "rows/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(rows*n), "ns/row")
}

// BenchmarkReaderScan: Scan of the whole dataset, end to end (planning,
// splitting and scanning), at Workers 1, 4 and 8 with GOMAXPROCS
// unchanged.
func BenchmarkReaderScan(b *testing.B) {
	dir, rows := benchDataset(b)
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			var counted atomic.Int64
			scan := func(d *Dataset) {
				err := d.Scan(context.Background(), ScanRequest{Workers: workers}, func(batch []Object) error {
					counted.Add(int64(len(batch)))
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			d := benchWarm(b, dir, scan)
			benchLoop(b, rows, &counted, func() { scan(d) })
		})
	}
}

// BenchmarkReaderStream: Stream of the whole dataset, ordered and owning.
func BenchmarkReaderStream(b *testing.B) {
	dir, rows := benchDataset(b)
	var counted atomic.Int64
	stream := func(d *Dataset) {
		if err := d.Stream(context.Background(), func(Object) error { counted.Add(1); return nil }); err != nil {
			b.Fatal(err)
		}
	}
	d := benchWarm(b, dir, stream)
	benchLoop(b, rows, &counted, func() { stream(d) })
}

// scanAllocBudget is Scan's allocation budget per object (Workers 1,
// no-op callback, warm Dataset, a 20,000-row fixture): a generous ceiling,
// about 1.5 times the 13.1 measured before the batch-scan work (the same
// under -race), to be tightened as allocations are removed. A change that
// raises Scan's allocations past it fails here.
const scanAllocBudget = 20

// TestScanAllocations: Scan stays within its allocation budget per
// object, and hands on every object.
func TestScanAllocations(t *testing.T) {
	const rows = 20_000
	dir := filepath.Join(t.TempDir(), "ds")
	if err := writeFixture(context.Background(), dir, rows, 2); err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	var n int64
	perObject := testing.AllocsPerRun(3, func() {
		n = 0
		if err := d.Scan(context.Background(), ScanRequest{Workers: 1}, func(batch []Object) error { n += int64(len(batch)); return nil }); err != nil {
			t.Fatal(err)
		}
	}) / rows
	if n != rows {
		t.Fatalf("%d objects, want %d", n, rows)
	}
	t.Logf("%.2f allocations an object (budget %v)", perObject, scanAllocBudget)
	if perObject > scanAllocBudget {
		t.Errorf("%.2f allocations an object, over the budget of %v", perObject, scanAllocBudget)
	}
}
