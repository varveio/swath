// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
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
// and scanned once before the timed loop (warm). Callbacks either count
// rows only or touch every overview field. Besides the standard ns/op,
// B/op and allocs/op, each reports rows/op, the denominator of the per-row costs,
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
	d, err := OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
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
			for _, mode := range []string{"Scan", "Batch-noop", "Batch-touch", "Batch-all", "Batch-size"} {
				b.Run(mode, func(b *testing.B) {
					var counted, checksum atomic.Int64
					scan := func(d *Dataset) {
						var err error
						if mode == "Scan" {
							err = d.Scan(context.Background(), ScanRequest{Workers: workers}, func(batch []Object) error {
								counted.Add(int64(len(batch)))
								return nil
							})
						} else {
							fields := FieldLastModified | FieldStorageClass
							if mode == "Batch-all" {
								fields = AllObjectFields
							}
							if mode == "Batch-size" {
								fields = 0
							}
							err = d.ScanBatches(context.Background(), BatchScanRequest{Workers: workers, Fields: fields}, func(batch *ObjectBatch) error {
								counted.Add(int64(batch.Len()))
								if mode == "Batch-touch" {
									var sum int64
									for i := 0; i < batch.Len(); i++ {
										sum += batch.Size(i)
										for _, v := range batch.Key(i) {
											sum += int64(v)
										}
										if us, ok := batch.LastModified(i); ok {
											sum += us
										}
										if class, ok := batch.StorageClass(i); ok {
											for _, v := range class {
												sum += int64(v)
											}
										}
									}
									checksum.Add(sum)
								}
								return nil
							})
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					d := benchWarm(b, dir, scan)
					benchLoop(b, rows, &counted, func() { scan(d) })
				})
			}
		})
	}
}

// BenchmarkReaderScanSkewed: ScanBatches (last_modified and storage class)
// at Workers 4 and 8 with a callback whose work per object is 32 times
// higher in the first eighth of the keys than in the rest, as a consumer's
// work per key can vary. Equal shards then take unequal time, and the scan
// lasts as long as its slowest worker.
func BenchmarkReaderScanSkewed(b *testing.B) {
	dir, rows := benchDataset(b)
	var hot []byte // the last key of the costly eighth
	d := benchWarm(b, dir, func(d *Dataset) {
		var seen int64
		if err := d.ScanBatches(context.Background(), BatchScanRequest{}, func(batch *ObjectBatch) error {
			if seen < rows/8 && seen+int64(batch.Len()) >= rows/8 {
				hot = bytes.Clone(batch.Key(int(rows/8 - seen - 1)))
			}
			seen += int64(batch.Len())
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	})
	for _, workers := range []int{4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			var counted, checksum atomic.Int64
			benchLoop(b, rows, &counted, func() {
				err := d.ScanBatches(context.Background(), BatchScanRequest{Workers: workers, Fields: FieldLastModified | FieldStorageClass}, func(batch *ObjectBatch) error {
					var sum int64
					for i := range batch.Len() {
						key, rounds := batch.Key(i), 1
						if bytes.Compare(key, hot) <= 0 {
							rounds = 32
						}
						for range rounds {
							for _, v := range key {
								sum = sum*31 + int64(v)
							}
						}
					}
					counted.Add(int64(batch.Len()))
					checksum.Add(sum)
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			})
		})
	}
}

// BenchmarkReaderStream: Stream of the whole dataset, ordered and owning,
// in one pass (Stream) and as it read before, in Rows requests of 1,024
// rows each after the last (Range-pages, oldStream in stream_test.go).
// Workers do not apply: a stream is one ordered pass.
func BenchmarkReaderStream(b *testing.B) {
	dir, rows := benchDataset(b)
	for _, mode := range []struct {
		name string
		read func(context.Context, *Dataset, func(Object) error) error
	}{{"Stream", newStream}, {"Range-pages", oldStream}} {
		b.Run(mode.name, func(b *testing.B) {
			var counted atomic.Int64
			stream := func(d *Dataset) {
				if err := mode.read(context.Background(), d, func(Object) error { counted.Add(1); return nil }); err != nil {
					b.Fatal(err)
				}
			}
			d := benchWarm(b, dir, stream)
			benchLoop(b, rows, &counted, func() { stream(d) })
		})
	}
}

// BenchmarkReaderTotals: Totals of about 600 nested paths, root included
// (benchTotalsPaths), over the whole dataset, end to end: the serial
// oracle (serialTotals, Totals before it read in shards) and the sharded
// totals at 1, 4 and 8 workers, with GOMAXPROCS unchanged. Besides ns/row
// it reports B/row and allocs/row over every operation.
func BenchmarkReaderTotals(b *testing.B) {
	dir, rows := benchDataset(b)
	ctx := context.Background()
	var paths []string
	d := benchWarm(b, dir, func(d *Dataset) { paths = benchTotalsPaths(b, d) })
	want, err := serialTotals(ctx, d, paths)
	if err != nil || want[0].Count != rows {
		b.Fatalf("serial totals of %d rows: %v", rows, err)
	}
	for _, workers := range []int{0, 1, 4, 8} {
		name := fmt.Sprintf("workers=%d", workers)
		if workers == 0 {
			name = "serial"
		}
		b.Run(name, func(b *testing.B) {
			total := func() ([]PathTotals, error) {
				if workers == 0 {
					return serialTotals(ctx, d, paths)
				}
				return d.totals(ctx, paths, workers)
			}
			if got, err := total(); err != nil || !reflect.DeepEqual(got, want) {
				b.Fatalf("totals differ from the serial oracle: %v", err)
			}
			b.ReportAllocs()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			n := int64(0)
			for b.Loop() {
				if _, err := total(); err != nil {
					b.Fatal(err)
				}
				n++
			}
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(len(paths)), "paths/op")
			b.ReportMetric(float64(rows), "rows/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(rows*n), "ns/row")
			b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(rows*n), "B/row")
			b.ReportMetric(float64(after.Mallocs-before.Mallocs)/float64(rows*n), "allocs/row")
		})
	}
}

// benchTotalsPaths returns about 600 paths of the generated dataset, as a
// registry of prefixes nested up to eight levels holds them: root, every
// prefix above the days (tops, years, months), days and shard directories
// evenly spaced, and a few exact keys.
func benchTotalsPaths(b *testing.B, d *Dataset) []string {
	dirs := map[string]bool{}
	var keys []string
	var last []byte
	n := 0
	err := d.ScanBatches(context.Background(), BatchScanRequest{Workers: 1}, func(batch *ObjectBatch) error {
		for i := range batch.Len() {
			key := batch.Key(i)
			if n++; n%500_000 == 0 {
				keys = append(keys, string(key))
			}
			dir := key[:bytes.LastIndexByte(key, '/')+1]
			if bytes.Equal(dir, last) {
				continue
			}
			last = append(last[:0], dir...)
			for j, c := range dir {
				if c == '/' {
					dirs[string(dir[:j+1])] = true
				}
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	var upper, days, shards []string
	for _, p := range slices.Sorted(maps.Keys(dirs)) {
		switch {
		case strings.Contains(p, "/shard="):
			shards = append(shards, p)
		case strings.Contains(p, "/20") && strings.Count(p[strings.Index(p, "/20"):], "/") == 4: // /yyyy/mm/dd/
			days = append(days, p)
		default:
			upper = append(upper, p)
		}
	}
	paths := append([]string{""}, upper...)
	paths = append(paths, keys...)
	spread := func(from []string, n int) {
		for i := range min(n, len(from)) {
			paths = append(paths, from[i*len(from)/min(n, len(from))])
		}
	}
	room := 600 - len(paths)
	spread(days, room/2)
	spread(shards, room-room/2)
	return paths
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

// TestScanBatchesAllocations bounds the entire warm scan, including planning
// and splitting. The callback only counts; there are no owning row copies.
func TestScanBatchesAllocations(t *testing.T) {
	const rows = 300_000
	const budget = 0.06 // allocations/row; observed 0.044 with all fields, including page decoding
	dir := filepath.Join(t.TempDir(), "ds")
	if err := writeFixture(context.Background(), dir, rows, 2); err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	for _, fields := range []ObjectFields{0, FieldLastModified | FieldStorageClass, AllObjectFields} {
		var n int64
		perRow := testing.AllocsPerRun(3, func() {
			n = 0
			err := d.ScanBatches(context.Background(), BatchScanRequest{Fields: fields}, func(b *ObjectBatch) error {
				n += int64(b.Len())
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}) / rows
		if n != rows {
			t.Fatalf("%d objects, want %d", n, rows)
		}
		t.Logf("fields=%d: %.5f allocations/row (budget %.3f)", fields, perRow, budget)
		if perRow > budget {
			t.Errorf("%.5f allocations/row exceeds %.3f", perRow, budget)
		}
	}
}

// streamAllocBudget is Stream's allocation budget per object (no-op
// callback, warm Dataset, a 20,000-row fixture). The one-pass Stream
// measured 9.0 (the Object, its key, size, time, ETag and class with their
// pointers, and the decoder's row); the paged Stream it replaced made 12.1.
const streamAllocBudget = 10.5

// TestStreamAllocations: Stream stays within its allocation budget per
// object, and hands on every object.
func TestStreamAllocations(t *testing.T) {
	const rows = 20_000
	dir := filepath.Join(t.TempDir(), "ds")
	if err := writeFixture(context.Background(), dir, rows, 2); err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	var n int64
	perObject := testing.AllocsPerRun(3, func() {
		n = 0
		if err := d.Stream(context.Background(), func(Object) error { n++; return nil }); err != nil {
			t.Fatal(err)
		}
	}) / rows
	if n != rows {
		t.Fatalf("%d objects, want %d", n, rows)
	}
	t.Logf("%.2f allocations an object (budget %v)", perObject, streamAllocBudget)
	if perObject > streamAllocBudget {
		t.Errorf("%.2f allocations an object, over the budget of %v", perObject, streamAllocBudget)
	}
}
