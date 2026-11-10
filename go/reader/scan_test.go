// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

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

// TestScanMatchesStream: for any number of workers, a scan hands fn
// exactly Stream's objects, each once, under every prefix.
func TestScanMatchesStream(t *testing.T) {
	dir := scanDir(t, 5000, 300)
	all, err := stream(t, openDir(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	for _, prefix := range []string{"", "b/", "a/00001", "zz"} {
		want := slices.DeleteFunc(slices.Clone(all), func(o Object) bool { return !bytes.HasPrefix(o.Key, []byte(prefix)) })
		for _, workers := range []int{-1, 0, 1, 2, 7, 64} {
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
// shards' scans', summed, whatever the number of workers.
func TestScanStats(t *testing.T) {
	dir := scanDir(t, 3000, 200)
	for _, workers := range []int{1, 7} {
		ctx := context.Background()
		sp := openDir(t, dir).SeekParquet()
		plan, err := sp.PlanRanges(ctx, []seekparquet.Range{{}}, objectColumns(S3ObjectsV1))
		if err != nil {
			t.Fatal(err)
		}
		want := plan.Stats()
		shards, err := plan.Split(ctx, workers)
		if err != nil {
			t.Fatal(err)
		}
		for _, shard := range shards {
			var st ReadStats
			if err := shard.Scan(ctx, &st, func(seekparquet.Row) error { return nil }); err != nil {
				t.Fatal(err)
			}
			addStats(&want, shard.Stats())
			addStats(&want, st)
		}
		got := ReadStats{PartsOpened: -1}
		if _, err := scanAll(t, openDir(t, dir), ScanRequest{Workers: workers, Stats: &got}); err != nil {
			t.Fatal(err)
		}
		if got.KeysDecoded != want.KeysDecoded || got.ValuePagesDecoded != want.ValuePagesDecoded ||
			got.PartsOpened != want.PartsOpened || got.KeysDecoded < 3000 {
			t.Fatalf("%d workers: stats %+v, want %+v", workers, got, want)
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
				d, err := OpenDir(context.Background(), dir, seekparquet.Options{})
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
// unchanged, and, as documented, the failed plan's own reads are not in
// Stats, which holds no other work.
func TestScanPlanFailureStats(t *testing.T) {
	d, src := openSource(t, scanDir(t, 3000, 200))
	failure := errors.New("failing read")
	src.mu.Lock()
	src.failAt, src.failErr = len(src.reads)+1, failure
	src.mu.Unlock()
	st := ReadStats{PartsOpened: 7}
	err := d.Scan(context.Background(), ScanRequest{Workers: 4, Stats: &st}, func([]Object) error {
		t.Fatal("fn called")
		return nil
	})
	if !errors.Is(err, failure) || st != (ReadStats{}) {
		t.Fatalf("plan failure: %v, stats %+v", err, st)
	}
	if _, reads := src.taken(); len(reads) == 0 {
		t.Fatal("planning read nothing")
	}
}
