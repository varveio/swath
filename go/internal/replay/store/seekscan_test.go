// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/write"

	"github.com/varveio/swath/go/internal/replay/legacy"
)

// ownedScan is seekReader.Scan as it was before borrowed batches: Read's
// owning rows (a column a part lacks is a null value), handed to sink as
// one run. The window-cache fill must pack exactly what it packs.
func ownedScan(ctx context.Context, r *seekReader, rng legacy.Range, stats *legacy.Stats, sink columnSink) error {
	got, err := r.Read(ctx, rng, stats)
	if err != nil || len(got) == 0 {
		return err
	}
	keys := make([][]byte, len(got))
	for i := range got {
		keys[i] = got[i].Key
	}
	sink.Keys(keys)
	values := make([]parquet.Value, len(got))
	for c := range rng.Columns {
		for i := range got {
			values[i] = got[i].Values[c]
		}
		sink.Values(c, values)
	}
	return nil
}

// scanRow has nullable, dictionary and plain value columns.
type scanRow struct {
	Key               string  `parquet:"key"`
	Size              *int64  `parquet:"size,optional"`
	LastModified      int64   `parquet:"last_modified,timestamp(microsecond)"`
	ETag              string  `parquet:"etag,dict"`
	StorageClass      *string `parquet:"storage_class,optional,dict"`
	OwnerID           *string `parquet:"owner_id,optional"`
	ChecksumAlgorithm *string `parquet:"checksum_algorithm,optional,dict"`
}

// scanRowLean lacks etag, owner_id and checksum_algorithm (the reader's
// Missing) and stores storage_class plain.
type scanRowLean struct {
	Key          string  `parquet:"key"`
	Size         *int64  `parquet:"size,optional"`
	LastModified int64   `parquet:"last_modified,timestamp(microsecond)"`
	StorageClass *string `parquet:"storage_class,optional"`
}

// writeScanFixture writes 800 rows in three parts (the middle one lean),
// in pages of 7 rows and row groups of 40, and returns the dataset and its
// distinct keys. With repeat, keys repeat in runs of 1 to 9 rows, which
// cross pages and row groups.
func writeScanFixture(t testing.TB, repeat bool) (string, []string) {
	var all []string
	for id := 0; len(all) < 800; id++ {
		n := 1
		if repeat {
			n = 1 + id*5%9
		}
		for range n {
			all = append(all, fmt.Sprintf("k/%05d", id))
		}
	}
	all = all[:800]
	// Parts split at about rows 300 and 500, never inside a key's run.
	cut := func(i int) int {
		for i < len(all) && all[i] == all[i-1] {
			i++
		}
		return i
	}
	lo, hi := cut(300), cut(500)
	ptr := func(i, every int, s string) *string {
		if i%every == 0 {
			return nil
		}
		return &s
	}
	var full1, full2 []scanRow
	var lean []scanRowLean
	for i, key := range all {
		size := int64(i * 3)
		sizep := &size
		if i%3 == 0 {
			sizep = nil
		}
		switch {
		case i >= lo && i < hi:
			lean = append(lean, scanRowLean{Key: key, Size: sizep, LastModified: int64(i), StorageClass: ptr(i, 4, fmt.Sprintf("sc%d", i%3))})
		default:
			row := scanRow{Key: key, Size: sizep, LastModified: int64(i) << 20, ETag: fmt.Sprintf("etag-%d", i%5),
				StorageClass: ptr(i, 4, fmt.Sprintf("sc%d", i%3)), OwnerID: ptr(i, 5, fmt.Sprintf("owner-%0*d", 1+i%17, i)),
				ChecksumAlgorithm: ptr(i, 2, "CRC32")}
			if i < lo {
				full1 = append(full1, row)
			} else {
				full2 = append(full2, row)
			}
		}
	}
	dir := writeSeekWith(t, seekparquet.Key{Column: "key", Unique: !repeat}, write.PartOptions{PageRows: 7, RowGroupRows: 40},
		"swath/s3-objects/v1", partOf(full1), partOf(lean), partOf(full2))
	return dir, slices.Compact(all)
}

// openScanReader opens dir through a faultSource, as the store's reader
// (whose Scan uses only the dataset).
func openScanReader(t testing.TB, dir string) (*seekReader, *faultSource) {
	src := &faultSource{dir: dir}
	// A small tail window and no speculation keep the fixture's pages out of
	// Verify's reads, so a scan reads storage and an armed failure reaches it.
	d, err := seekparquet.OpenDataset(context.Background(), src, seekparquet.OpenOptions{Read: seekparquet.ReadPolicy{MetadataPrefetchBytes: 4 << 10, DisableSpeculation: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Verify(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	return &seekReader{d: d}, src
}

// runSink counts a scan's runs and Missing calls, and runs hook at the
// start of each run.
type runSink struct {
	*packer
	runs, missing int
	hook          func(run int)
}

func (s *runSink) Keys(keys [][]byte) {
	s.runs++
	if s.hook != nil {
		s.hook(s.runs)
	}
	s.packer.Keys(keys)
}

func (s *runSink) Missing(c int) { s.missing++; s.packer.Missing(c) }

// slotSink records a scan's keys and each projection slot's values as
// text, a null or missing value as "<null>": what any sink can observe,
// for projections the packer does not take (a duplicate column).
type slotSink struct {
	keys  []string
	slots [][]string
	run   int
}

func (s *slotSink) Keys(keys [][]byte) {
	for _, k := range keys {
		s.keys = append(s.keys, string(k))
	}
	s.run = len(keys)
}

func (s *slotSink) Values(c int, values []parquet.Value) {
	for _, v := range values {
		text := "<null>"
		if !v.IsNull() {
			text = v.Kind().String() + ":" + v.String()
		}
		s.slots[c] = append(s.slots[c], text)
	}
}

// View records a view's values as Values records page values, from the
// module's own kinds and text, which the legacy core's must match.
func (s *slotSink) View(c int, values seekparquet.ColumnView) {
	for i := range values.Len() {
		text := "<null>"
		if v := values.Value(i); !v.IsNull() {
			text = v.Kind().String() + ":" + v.String()
		}
		s.slots[c] = append(s.slots[c], text)
	}
}

func (s *slotSink) Missing(c int) {
	for range s.run {
		s.slots[c] = append(s.slots[c], "<null>")
	}
}

type scanFunc func(context.Context, *seekReader, legacy.Range, *legacy.Stats, columnSink) error

// batchScan is the borrowed-batch seekReader.Scan, as a scanFunc.
func batchScan(ctx context.Context, r *seekReader, rng legacy.Range, stats *legacy.Stats, sink columnSink) error {
	return r.Scan(ctx, rng, stats, sink)
}

// fill packs rng through scan.
func fill(ctx context.Context, r *seekReader, scan scanFunc, rng legacy.Range) (packed, legacy.Stats, *runSink, error) {
	sink := &runSink{packer: s3Objects.packer(rng.Columns, Projection{}, 16, 0)}
	var stats legacy.Stats
	if err := scan(ctx, r, rng, &stats, sink); err != nil {
		return packed{}, stats, sink, err
	}
	p, err := sink.packed()
	return p, stats, sink, err
}

// TestSeekScanPacksAsOwnedRows fills the window cache's packer through the
// borrowed-batch Scan and through the owning rows it replaced, over nulls,
// dictionary and plain pages, many pages, row groups and parts, a part
// lacking projected columns, repeated keys, and limits and bounds at and
// around page, row group and part boundaries: the packed windows and the
// read statistics are identical, and so are the rows a sink sees under a
// duplicate projection (which the packer never takes).
func TestSeekScanPacksAsOwnedRows(t *testing.T) {
	ctx := context.Background()
	projections := [][]string{nil, s3Objects.columns(Projection{Owner: true})}
	duplicate := []string{"etag", "size", "etag", "owner_id", "last_modified", "storage_class", "checksum_algorithm"}
	limits := []int{1, 2, 6, 7, 8, 13, 14, 39, 40, 41, 80, 200, 299, 300, 301, 500, 799, 800, 801, 5000}
	for _, repeat := range []bool{false, true} {
		t.Run(fmt.Sprintf("repeat=%v", repeat), func(t *testing.T) {
			dir, keys := writeScanFixture(t, repeat)
			r, _ := openScanReader(t, dir)
			rnd := rand.New(rand.NewPCG(7, 11))
			pick := func() []byte {
				switch n := rnd.IntN(10); {
				case n == 0:
					return nil
				case n < 4: // a row group or part start
					return []byte(keys[min(len(keys)-1, 40*rnd.IntN(21))])
				case n < 6: // between keys
					return []byte(keys[rnd.IntN(len(keys))] + "!")
				default:
					return []byte(keys[rnd.IntN(len(keys))])
				}
			}
			multi, missing := false, false
			trials := 600
			if raceEnabled || testing.Short() {
				trials = 200
			}
			for trial := range trials {
				rng := legacy.Range{From: pick(), FromInclusive: rnd.IntN(2) == 0, Limit: limits[trial%len(limits)],
					Columns: projections[trial%len(projections)]}
				if to := pick(); to != nil && rnd.IntN(3) == 0 && bytes.Compare(to, rng.From) > 0 {
					rng.To = to
				}
				// The owned fill runs before and after, so the batch fill's
				// statistics compare with a read over the same warm caches.
				if _, _, _, err := fill(ctx, r, ownedScan, rng); err != nil {
					t.Fatalf("%+v: owned: %v", rng, err)
				}
				got, gotStats, sink, err := fill(ctx, r, batchScan, rng)
				if err != nil {
					t.Fatalf("%+v: batches: %v", rng, err)
				}
				want, wantStats, _, err := fill(ctx, r, ownedScan, rng)
				if err != nil {
					t.Fatalf("%+v: owned: %v", rng, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%+v: packed\n%+v\nwant\n%+v", rng, got, want)
				}
				if gotStats != wantStats {
					t.Fatalf("%+v: stats %+v, want %+v", rng, gotStats, wantStats)
				}
				rng.Columns = duplicate
				var wantSlots, gotSlots slotSink
				wantSlots.slots, gotSlots.slots = make([][]string, len(duplicate)), make([][]string, len(duplicate))
				if err := ownedScan(ctx, r, rng, nil, &wantSlots); err != nil {
					t.Fatal(err)
				}
				if err := batchScan(ctx, r, rng, nil, &gotSlots); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(gotSlots.keys, wantSlots.keys) || !reflect.DeepEqual(gotSlots.slots, wantSlots.slots) {
					t.Fatalf("%+v: slots\n%v %v\nwant\n%v %v", rng, gotSlots.keys, gotSlots.slots, wantSlots.keys, wantSlots.slots)
				}
				multi = multi || sink.runs > 1
				missing = missing || sink.missing > 0
			}
			if !multi || !missing {
				t.Fatalf("no fill crossed a batch (%v) or a lean part (%v)", multi, missing)
			}
		})
	}
}

// TestSeekScanFailureAndCancellation: a storage failure or a cancellation
// before the scan fails both fills alike; one after the first batch ends
// the borrowed-batch scan with that error, after one run (the fill is then
// discarded by packedRows).
func TestSeekScanFailureAndCancellation(t *testing.T) {
	dir, _ := writeScanFixture(t, false)
	r, src := openScanReader(t, dir)
	rng := legacy.Range{Limit: 800, Columns: s3Objects.columns(Projection{Owner: true})}
	for _, scan := range []scanFunc{ownedScan, batchScan} {
		src.fail.Store(true)
		if _, _, _, err := fill(context.Background(), r, scan, rng); !errors.Is(err, errInjected) {
			t.Fatalf("armed failure: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, _, err := fill(ctx, r, scan, rng); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled: %v", err)
		}
	}
	for _, cancelled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		sink := &runSink{packer: s3Objects.packer(rng.Columns, Projection{}, 16, 0), hook: func(int) {
			if cancelled {
				cancel()
			} else {
				src.fail.Store(true)
			}
		}}
		var stats legacy.Stats
		err := r.Scan(ctx, rng, &stats, sink)
		cancel()
		src.fail.Store(false)
		if want := map[bool]error{false: errInjected, true: context.Canceled}[cancelled]; !errors.Is(err, want) || sink.runs != 1 {
			t.Fatalf("cancelled %v: %v after %d runs, want %v after 1", cancelled, err, sink.runs, want)
		}
		if stats.PagesDecoded == 0 {
			t.Fatalf("cancelled %v: no read counted", cancelled)
		}
	}
}

// TestPackedOverflowStopsTheBatchScan: an arena overflow in a batch stops
// the scan (packer.stop cancels it before the next batch), so a fill that
// overflows early decodes few keys, and packedRows reports
// errPackedOverflow, not the cancellation.
func TestPackedOverflowStopsTheBatchScan(t *testing.T) {
	dir, _ := writeScanFixture(t, false)
	opened, _, err := Open(context.Background(), dir, Options{Readers: 1, Prefetch: Prefetch{Off: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	s := opened.(*Sorted)
	s.arenaLimit = 100
	before := s.Counters().RowsDecoded.Load()
	if _, err := s.packedRows(context.Background(), nil, true, nil, 800, Projection{Owner: true}); !errors.Is(err, errPackedOverflow) {
		t.Fatalf("packedRows: %v, want errPackedOverflow", err)
	}
	if n := s.Counters().RowsDecoded.Load() - before; n == 0 || n > 80 {
		t.Fatalf("an overflowing fill decoded %d keys, want the first batches' only", n)
	}
}

// BenchmarkWindowFill packs a 10,000-row window-cache fill of the S3
// projection from a 200,000-key sorted dataset, through the owning rows
// and through borrowed batches.
func BenchmarkWindowFill(b *testing.B) {
	path := writeSorted(b, filepath.Join(b.TempDir(), "f"), fixtureKeys(200_000), 0)
	st, _, err := Open(context.Background(), path, Options{Readers: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	s := st.(*Sorted)
	r := s.reader.(*seekReader)
	columns := s.objects.columns(Projection{Owner: true})
	ctx := context.Background()
	for _, from := range []string{"", "d05/"} {
		rng := legacy.Range{From: []byte(from), FromInclusive: true, Limit: 10_000, Columns: columns}
		if from == "" {
			rng.From = nil
		}
		for _, v := range []struct {
			name string
			scan scanFunc
		}{{"owned", ownedScan}, {"batches", batchScan}} {
			b.Run(fmt.Sprintf("from=%q/%s", from, v.name), func(b *testing.B) {
				for b.Loop() {
					sink := s.objects.packer(columns, Projection{Owner: true}, 10_000, 0)
					if err := v.scan(ctx, r, rng, nil, sink); err != nil {
						b.Fatal(err)
					}
					if _, err := sink.packed(); err != nil || sink.out.n != 10_000 {
						b.Fatalf("%d rows, %v", sink.out.n, err)
					}
				}
			})
		}
	}
}
