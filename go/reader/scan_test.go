// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
)

// scanDir writes n objects under three top-level prefixes over three
// parts, in row groups of groupRows, so that a scan splits into many
// shards.
func scanDir(t testing.TB, n int, groupRows int64) string {
	t.Helper()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%c/%07d", "abc"[i*3/n], i)
	}
	return writeParts(t, s3Spec(), 100, groupRows, cut(keyRows(keys), 0, n/3, 2*n/3, n)...)
}

// scanAll scans d and returns its objects in key order, each batch's
// keys checked ascending.
func scanAll(t *testing.T, d *Dataset, req ScanRequest) ([]Object, error) {
	t.Helper()
	var mu sync.Mutex
	var out []Object
	err := d.Scan(context.Background(), req, func(batch []Object) error {
		if len(batch) == 0 || len(batch) > scanBatchObjects {
			return fmt.Errorf("a batch of %d objects", len(batch))
		}
		for i := 1; i < len(batch); i++ {
			if bytes.Compare(batch[i-1].Key, batch[i].Key) >= 0 {
				return fmt.Errorf("batch out of order at %q", batch[i].Key)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		out = append(out, batch...)
		return nil
	})
	slices.SortFunc(out, func(a, b Object) int { return bytes.Compare(a.Key, b.Key) })
	return out, err
}

// TestScanMatchesStream: for any number of workers, up to math.MaxInt,
// a scan hands fn exactly Stream's objects, each once, under every prefix.
func TestScanMatchesStream(t *testing.T) {
	dir := scanDir(t, 5000, 300)
	all, err := stream(t, openDir(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	for _, prefix := range []string{"", "b/", "a/00001", "zz"} {
		want := slices.DeleteFunc(slices.Clone(all), func(o Object) bool { return !bytes.HasPrefix(o.Key, []byte(prefix)) })
		for _, workers := range []int{-1, 0, 1, 2, 7, 64, math.MaxInt} {
			got, err := scanAll(t, d, ScanRequest{Prefix: []byte(prefix), Workers: workers})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) && (len(got) != 0 || len(want) != 0) {
				t.Fatalf("prefix %q, %d workers: %d objects, want %d", prefix, workers, len(got), len(want))
			}
		}
	}
}

// TestScanStats: a scan's statistics are its plan's, its split's and its
// shards' scans', summed, whatever the number of workers and of shards per
// worker.
func TestScanStats(t *testing.T) {
	dir := scanDir(t, 3000, 200)
	for _, k := range []int{1, 8, 16} {
		withShardsPerWorker(t, k)
		for _, workers := range []int{1, 7} {
			ctx := context.Background()
			sp := openDir(t, dir).SeekParquet()
			var want, st ReadStats
			plan, err := sp.Plan(ctx, []seekparquet.Selection{{}}, objectColumns(S3ObjectsV1), &st)
			addStats(&want, st)
			if err != nil {
				t.Fatal(err)
			}
			shards, err := plan.Split(ctx, scanShards(workers), &st)
			addStats(&want, st)
			if err != nil {
				t.Fatal(err)
			}
			for _, shard := range shards {
				if err := shard.Scan(ctx, &st, func(*seekparquet.Batch) error { return nil }); err != nil {
					t.Fatal(err)
				}
				addStats(&want, st)
			}
			got := ReadStats{PartsOpened: -1}
			if _, err := scanAll(t, openDir(t, dir), ScanRequest{Workers: workers, Stats: &got}); err != nil {
				t.Fatal(err)
			}
			if got.KeysDecoded != want.KeysDecoded || got.ValuePagesDecoded != want.ValuePagesDecoded ||
				got.PartsOpened != want.PartsOpened || got.KeysDecoded < 3000 {
				t.Fatalf("%d workers, %d shards a worker: stats %+v, want %+v", workers, k, got, want)
			}
		}
	}
}

// TestScanErrors: a row that breaks Stream's checks fails the scan with
// its SemanticError; fn's error stops every shard at its first batch and
// is returned unchanged; a cancelled context stops the scan with its
// error; versions are refused before any read; the statistics hold the
// work done.
func TestScanErrors(t *testing.T) {
	const n = 20000
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%07d", i)
	}
	rows := keyRows(keys)
	rows[n/2].Size = ptr(int64(-1))
	bad := writeParts(t, s3Spec(), 100, 500, cut(rows, 0, n/3, n)...)
	var st ReadStats
	err := openDir(t, bad).Scan(context.Background(), ScanRequest{Workers: 7, Stats: &st}, func([]Object) error { return nil })
	if se := semanticAt(t, err, KindRow, "size"); string(se.Row.Key) != keys[n/2] || st.KeysDecoded == 0 {
		t.Fatalf("row error %v, stats %+v", se, st)
	}

	dir := scanDir(t, n, 500)
	stop := errors.New("stop")
	var calls atomic.Int64
	err = openDir(t, dir).Scan(context.Background(), ScanRequest{Workers: 7}, func([]Object) error {
		calls.Add(1)
		return stop
	})
	if err != stop || calls.Load() == 0 || calls.Load() > 7 {
		t.Fatalf("fn's error: %v after %d calls", err, calls.Load())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var seen atomic.Int64
	err = openDir(t, dir).Scan(ctx, ScanRequest{Workers: 4}, func(batch []Object) error {
		seen.Add(int64(len(batch)))
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || seen.Load() >= n {
		t.Fatalf("cancelled: %v after %d objects", err, seen.Load())
	}

	s := s3Spec()
	s.id, s.key.Unique = S3VersionsV1, false
	versions, src := openSource(t, writeDataset(t, s, s3Rows(3)))
	src.reset()
	if err := versions.Scan(context.Background(), ScanRequest{}, func([]Object) error { return nil }); !errors.Is(err, ErrSchema) {
		t.Fatalf("scan versions: %v", err)
	}
	if _, reads := src.taken(); len(reads) != 0 {
		t.Fatalf("%d reads before refusing", len(reads))
	}
}

// BenchmarkScan scans 300,000 objects over three parts with one worker
// and with GOMAXPROCS, tallying their sizes as an overview would.
func BenchmarkScan(b *testing.B) {
	dir := scanDir(b, 300_000, 10_000)
	for _, workers := range []int{1, runtime.GOMAXPROCS(0)} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			for b.Loop() {
				d, err := OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
				if err != nil {
					b.Fatal(err)
				}
				var total atomic.Int64
				if err := d.Scan(context.Background(), ScanRequest{Workers: workers}, func(batch []Object) error {
					var sum int64
					for i := range batch {
						sum += *batch[i].Size
					}
					total.Add(sum)
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				_ = d.Close()
			}
		})
	}
}

// TestScanContextEnded: a scan whose context ends never succeeds: one
// cancelled by fn on the last batch, after which no read checks it, and
// one cancelled before it starts, over a dataset with no rows, whose plan
// and split read nothing.
func TestScanContextEnded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	one := openDir(t, writeDataset(t, s3Spec(), s3Rows(1)))
	err := one.Scan(ctx, ScanRequest{Workers: 1}, func([]Object) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled by fn: %v", err)
	}
	empty := openDir(t, writeDataset[s3Row](t, s3Spec(), nil))
	if err := empty.Scan(ctx, ScanRequest{}, func([]Object) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled before an empty scan: %v", err)
	}
}

// TestScanPlanFailureStats: a Source failure while planning is returned
// unchanged, and, as documented, the failed plan's own reads are in Stats
// (the part it opened to check and the read that failed), which holds no
// other work; its open and read count as unreported I/O.
func TestScanPlanFailureStats(t *testing.T) {
	d, src := openSource(t, scanDir(t, 3000, 200))
	failure := errors.New("failing read")
	src.mu.Lock()
	src.failAt, src.failErr = len(src.reads)+1, failure
	opened := len(src.reads)
	src.mu.Unlock()
	st := ReadStats{PartsOpened: 7}
	err := d.Scan(context.Background(), ScanRequest{Workers: 4, Stats: &st}, func([]Object) error {
		t.Fatal("fn called")
		return nil
	})
	_, reads := src.taken()
	if len(reads) != opened+1 {
		t.Fatalf("planning made %d reads, want the one that failed", len(reads)-opened)
	}
	if want := (ReadStats{PartsOpened: 1, SourceReads: 1, SourceBytes: reads[opened].len, UnreportedIOCalls: 2}); !errors.Is(err, failure) || st != want {
		t.Fatalf("plan failure: %v, stats %+v, want %+v", err, st, want)
	}
}

// withShardsPerWorker sets shardsPerWorker to k until the test ends.
func withShardsPerWorker(t *testing.T, k int) {
	t.Helper()
	old := shardsPerWorker
	shardsPerWorker = k
	t.Cleanup(func() { shardsPerWorker = old })
}

// concurrency counts fn's calls in flight and the most at once.
type concurrency struct{ now, most atomic.Int64 }

func (c *concurrency) enter() {
	n := c.now.Add(1)
	for m := c.most.Load(); n > m && !c.most.CompareAndSwap(m, n); m = c.most.Load() {
	}
	runtime.Gosched() // let other calls overlap this one
}

func (c *concurrency) leave() { c.now.Add(-1) }

// TestScanShardsPerWorker: whatever the shards per worker, Scan and
// ScanBatches hand on exactly Stream's objects, each once, each batch in
// ascending order, with no more calls of fn at once than Workers; a row
// that breaks the checks fails both with its position; fn's error stops
// each worker at its first batch.
func TestScanShardsPerWorker(t *testing.T) {
	const n = 6000
	dir := scanDir(t, n, 150) // 40 row groups: up to 40 shards
	want, err := stream(t, openDir(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%07d", i)
	}
	rows := keyRows(keys)
	rows[n*2/3].Size = ptr(int64(-1))
	bad := openDir(t, writeParts(t, s3Spec(), 100, 150, cut(rows, 0, n/3, n)...))
	d := openDir(t, dir)
	stop := errors.New("stop")
	for _, k := range []int{1, 2, 4, 8, 16} {
		withShardsPerWorker(t, k)
		for _, workers := range []int{1, 2, 7} {
			var c concurrency
			var mu sync.Mutex
			var scanned, batched []Object
			err := d.Scan(context.Background(), ScanRequest{Workers: workers}, func(batch []Object) error {
				c.enter()
				defer c.leave()
				for i := 1; i < len(batch); i++ {
					if bytes.Compare(batch[i-1].Key, batch[i].Key) >= 0 {
						return fmt.Errorf("batch out of order at %q", batch[i].Key)
					}
				}
				mu.Lock()
				defer mu.Unlock()
				scanned = append(scanned, batch...)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			err = d.ScanBatches(context.Background(), BatchScanRequest{Workers: workers, Fields: AllObjectFields}, func(b *ObjectBatch) error {
				c.enter()
				defer c.leave()
				mu.Lock()
				defer mu.Unlock()
				for i := range b.Len() {
					if i > 0 && bytes.Compare(b.Key(i-1), b.Key(i)) >= 0 {
						return fmt.Errorf("batch out of order at %q", b.Key(i))
					}
					batched = append(batched, copyBatchObject(b, i))
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, got := range [][]Object{scanned, batched} {
				slices.SortFunc(got, func(a, b Object) int { return bytes.Compare(a.Key, b.Key) })
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%d workers, %d shards a worker: %d objects, want %d", workers, k, len(got), len(want))
				}
			}
			if most := c.most.Load(); most > int64(workers) {
				t.Fatalf("%d workers, %d shards a worker: %d calls at once", workers, k, most)
			}

			err = bad.Scan(context.Background(), ScanRequest{Workers: workers}, func([]Object) error { return nil })
			if se := semanticAt(t, err, KindRow, "size"); string(se.Row.Key) != keys[n*2/3] {
				t.Fatalf("Scan row error %v", se)
			}
			err = bad.ScanBatches(context.Background(), BatchScanRequest{Workers: workers}, func(*ObjectBatch) error { return nil })
			if se := semanticAt(t, err, KindRow, "size"); string(se.Row.Key) != keys[n*2/3] || se.Row.Offset != 0 {
				t.Fatalf("ScanBatches row error %v", se)
			}

			var calls atomic.Int64
			err = d.ScanBatches(context.Background(), BatchScanRequest{Workers: workers}, func(*ObjectBatch) error {
				calls.Add(1)
				return stop
			})
			if err != stop || calls.Load() == 0 || calls.Load() > int64(workers) {
				t.Fatalf("%d workers, %d shards a worker: fn's error %v after %d calls", workers, k, err, calls.Load())
			}
		}
	}
}

// TestScanSlowShardDoesNotHoldTheRest: while fn holds the batch of the
// first key, the other worker goes on to the other shards and hands on
// three quarters of the objects, which one shard a worker, half of them
// waiting behind the held batch, could not.
func TestScanSlowShardDoesNotHoldTheRest(t *testing.T) {
	const n, workers = 8000, 2
	withShardsPerWorker(t, 4)
	d := openDir(t, scanDir(t, n, 500)) // 18 row groups: 8 shards
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, batches := range []bool{false, true} {
		var others atomic.Int64
		done := make(chan struct{})
		var once sync.Once
		hold := func(key []byte, objects int) error {
			if !bytes.Equal(key, []byte("a/0000000")) {
				if others.Add(int64(objects)) >= n*3/4 {
					once.Do(func() { close(done) })
				}
				return nil
			}
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("%d objects handed on while the first batch was held", others.Load())
			}
		}
		var err error
		if batches {
			err = d.ScanBatches(ctx, BatchScanRequest{Workers: workers}, func(b *ObjectBatch) error { return hold(b.Key(0), b.Len()) })
		} else {
			err = d.Scan(ctx, ScanRequest{Workers: workers}, func(batch []Object) error { return hold(batch[0].Key, len(batch)) })
		}
		if err != nil {
			t.Fatalf("batches %v: %v", batches, err)
		}
	}
}

// writeUnsorted writes keys, in the order given, as one part of row groups
// of groupRows rows and pages of about one row with the writer of
// parquet-go, which does not check key order, stamped and listed in a manifest as the module's writer would,
// keys declared unique or not. It is how a dataset whose key order is
// broken between two row groups looks.
func writeUnsorted(t *testing.T, keys []string, groupRows int64, unique bool) string {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[s3Row](&buf, parquet.MaxRowsPerRowGroup(groupRows), parquet.PageBufferSize(1),
		parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("key"))),
		parquet.KeyValueMetadata(seekparquet.StampFormatVersion, "0"),
		parquet.KeyValueMetadata(seekparquet.StampDraftRevision, "1"),
		parquet.KeyValueMetadata(seekparquet.StampSchemaID, string(S3ObjectsV1)),
		parquet.KeyValueMetadata(seekparquet.StampKeyColumn, "key"),
		parquet.KeyValueMetadata(seekparquet.StampKeyUnique, strconv.FormatBool(unique)))
	for _, r := range keyRows(keys) { // a row at a time: a page each
		if _, err := w.Write([]s3Row{r}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	ext, err := json.Marshal(map[string]any{Extension: swathExtension()})
	if err != nil {
		t.Fatal(err)
	}
	m, err := seekparquet.MarshalManifest(seekparquet.Manifest{Key: seekparquet.Key{Column: "key", Unique: unique},
		Version: seekparquet.Version{FormatVersion: 0, DraftRevision: 1}, SchemaID: string(S3ObjectsV1), Extensions: ext,
		Parts: []seekparquet.PartInfo{{Path: "data/part.parquet", Rows: int64(len(keys)), Bytes: int64(buf.Len()),
			MinKey: []byte(slices.Min(keys)), MaxKey: []byte(slices.Max(keys))}}})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string][]byte{seekparquet.ManifestName: m, "data/part.parquet": buf.Bytes()} {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// TestShardBoundaryOrder: a key below the one before it, or equal to it
// under unique keys, where a row group begins, below the last key page of
// the row group before it, passes CheckSchema, which decodes no such page;
// read in one pass it is a key_order refusal. Scan,
// ScanBatches and Totals refuse it at every worker count with the same
// code and key as Stream and the serial Totals, although at more than one
// worker each shard's own scan sees only keys in order (shardOrder).
func TestShardBoundaryOrder(t *testing.T) {
	for _, keys := range [][]string{
		{"a", "b", "c", "z", "d", "e", "f", "zz"},
		{"a", "b", "c", "d", "d", "e", "f", "g"},
		{"a", "b", "c", "d", "e", "f", "g", "h", "g", "i", "j", "k"},
	} {
		t.Run(strings.Join(keys, ","), func(t *testing.T) {
			d := openDir(t, writeUnsorted(t, keys, 4, true))
			if err := d.CheckSchema(context.Background()); err != nil {
				t.Fatal(err)
			}
			refusal := func(name string, err error) *seekparquet.RefusalError {
				t.Helper()
				var r *seekparquet.RefusalError
				if !errors.As(err, &r) || r.Code != seekparquet.CodeKeyOrder {
					t.Fatalf("%s: %v, want a key_order refusal", name, err)
				}
				return r
			}
			want := refusal("Stream", d.Stream(context.Background(), func(Object) error { return nil })).Key
			serial, err := serialTotals(context.Background(), d, []string{"", "b", "z"})
			if r := refusal("serial Totals", err); !bytes.Equal(r.Key, want) || serial != nil {
				t.Fatalf("serial Totals refused %q, want %q", r.Key, want)
			}
			for _, workers := range []int{1, 2, 4, 8} {
				_, err := scanAll(t, d, ScanRequest{Workers: workers})
				if r := refusal(fmt.Sprintf("Scan workers=%d", workers), err); !bytes.Equal(r.Key, want) {
					t.Errorf("Scan workers=%d refused %q, want %q", workers, r.Key, want)
				}
				err = d.ScanBatches(context.Background(), BatchScanRequest{Workers: workers, Fields: AllObjectFields}, func(*ObjectBatch) error { return nil })
				if r := refusal(fmt.Sprintf("ScanBatches workers=%d", workers), err); !bytes.Equal(r.Key, want) {
					t.Errorf("ScanBatches workers=%d refused %q, want %q", workers, r.Key, want)
				}
				got, err := d.totals(context.Background(), []string{"", "b", "z"}, workers)
				if r := refusal(fmt.Sprintf("Totals workers=%d", workers), err); !bytes.Equal(r.Key, want) || got != nil {
					t.Errorf("Totals workers=%d refused %q with %+v, want %q and nothing", workers, r.Key, got, want)
				}
			}
		})
	}
}

// TestShardBoundaryRepeatedKeys: under keys declared to repeat, a group
// of equal keys crossing a row group inside a part is in order; Split
// keeps it in one shard, and Scan and Totals read it at every worker count
// as Stream and the serial Totals do.
func TestShardBoundaryRepeatedKeys(t *testing.T) {
	d := openDir(t, writeUnsorted(t, []string{"a", "b", "c", "c", "c", "c", "d", "e"}, 3, false))
	want, err := stream(t, d)
	if err != nil || len(want) != 8 {
		t.Fatalf("Stream: %d objects, %v", len(want), err)
	}
	checkTotalsSerial(t, d, []string{"", "b", "c", "c/", "d"})
	for _, workers := range []int{1, 2, 4, 8} {
		var mu sync.Mutex
		var got []Object
		err := d.Scan(context.Background(), ScanRequest{Workers: workers}, func(batch []Object) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, batch...)
			return nil
		})
		slices.SortStableFunc(got, func(a, b Object) int { return bytes.Compare(a.Key, b.Key) })
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("Scan workers=%d: %d objects, %v; want Stream's %d", workers, len(got), err, len(want))
		}
	}
}

// TestShardOrder checks shardOrder alone: shards that handed on nothing
// are skipped, and only a first key above the last before it passes.
func TestShardOrder(t *testing.T) {
	k := func(first, last string) shardKeys {
		return shardKeys{first: []byte(first), last: []byte(last), seen: true}
	}
	for _, c := range []struct {
		keys []shardKeys
		bad  string
	}{
		{nil, ""},
		{[]shardKeys{{}, k("a", "c"), {}, k("d", "f"), k("g", "g")}, ""},
		{[]shardKeys{k("a", "c"), {}, k("c", "f")}, "c"},
		{[]shardKeys{k("a", "c"), k("b", "f")}, "b"},
		{[]shardKeys{k("a", "c"), k("d", "f"), k("e", "z")}, "e"},
	} {
		err := shardOrder(c.keys)
		var r *seekparquet.RefusalError
		switch {
		case c.bad == "" && err != nil:
			t.Errorf("%v: %v", c.keys, err)
		case c.bad != "" && (!errors.As(err, &r) || r.Code != seekparquet.CodeKeyOrder || string(r.Key) != c.bad):
			t.Errorf("%v: %v, want key_order at %q", c.keys, err, c.bad)
		}
	}
}

// TestScanCallbackStopIsAnError: a Scan or ScanBatches callback returning
// seekparquet.Stop, which ends a module scan successfully, is an error
// like any other to swath: returned unchanged after the callback's first
// batch of each worker at most, stopping the rest of its shard and every
// other shard, with one worker (one shard) and with several shards.
func TestScanCallbackStopIsAnError(t *testing.T) {
	dir := scanDir(t, 5000, 300)
	ctx := context.Background()
	for _, workers := range []int{1, 4} {
		var calls atomic.Int64
		err := openDir(t, dir).Scan(ctx, ScanRequest{Workers: workers}, func([]Object) error {
			calls.Add(1)
			return seekparquet.Stop
		})
		if err != seekparquet.Stop || calls.Load() < 1 || calls.Load() > int64(workers) {
			t.Errorf("Scan, %d workers: %v after %d calls, want seekparquet.Stop after at most %d", workers, err, calls.Load(), workers)
		}
		calls.Store(0)
		err = openDir(t, dir).ScanBatches(ctx, BatchScanRequest{Workers: workers}, func(*ObjectBatch) error {
			calls.Add(1)
			return seekparquet.Stop
		})
		if err != seekparquet.Stop || calls.Load() < 1 || calls.Load() > int64(workers) {
			t.Errorf("ScanBatches, %d workers: %v after %d calls, want seekparquet.Stop after at most %d", workers, err, calls.Load(), workers)
		}
	}
	// Stream already kept its callback's error apart from the module's.
	if err := openDir(t, dir).Stream(ctx, func(Object) error { return seekparquet.Stop }); err != seekparquet.Stop {
		t.Errorf("Stream: %v, want seekparquet.Stop", err)
	}
}
