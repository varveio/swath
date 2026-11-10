// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

// gcsBatchRow is the GCS schema before the absence-tolerant time_created
// addition. It exercises current-object reads of both old and new datasets.
type gcsBatchRow struct {
	Key            string  `parquet:"key"`
	Size           *int64  `parquet:"size,optional"`
	LastModified   *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	ETag           *string `parquet:"etag,optional"`
	Generation     *int64  `parquet:"generation,optional"`
	Metageneration *int64  `parquet:"metageneration,optional"`
	StorageClass   *string `parquet:"storage_class,optional"`
	MD5            *string `parquet:"md5,optional"`
	CRC32C         *string `parquet:"crc32c,optional"`
	ContentType    *string `parquet:"content_type,optional"`
}

// These fixtures deliberately vary the columns' null patterns and use
// small pages, row groups and multiple parts to exercise borrowed storage.
func batchFixture(t *testing.T, id SchemaID) string {
	s := s3Spec()
	s.id = id
	rows := s3Rows(1200)
	for i := range rows {
		if i%3 == 0 {
			rows[i].LastModified = nil
		}
		if i%5 == 0 {
			rows[i].ETag = nil
		}
		if i%7 == 0 {
			rows[i].StorageClass = nil
		}
		if i%11 == 0 {
			rows[i].ETag, rows[i].StorageClass = ptr(""), ptr("")
		}
	}
	if id == AzblobObjectsV1 {
		az := make([]azblobRow, len(rows))
		for i, r := range rows {
			az[i] = azblobRow{Key: r.Key, Size: r.Size, LastModified: r.LastModified, ETag: r.ETag, AccessTier: r.StorageClass}
		}
		dir := writeGeometry(t, s, az[:600], 31, 127)
		appendPart(t, dir, writeGeometry(t, s, az[600:], 31, 127))
		return dir
	}
	if id == GCSObjectsV1 {
		gcs := make([]gcsBatchRow, len(rows))
		for i, r := range rows {
			gcs[i] = gcsBatchRow{Key: r.Key, Size: r.Size, LastModified: r.LastModified, ETag: r.ETag, StorageClass: r.StorageClass}
		}
		dir := writeGeometry(t, s, gcs[:600], 31, 127)
		appendPart(t, dir, writeGeometry(t, s, gcs[600:], 31, 127))
		return dir
	}
	return writeParts(t, s, 31, 127, rows[:600], rows[600:])
}

func copyBatchObject(b *ObjectBatch, i int) Object {
	o := Object{Position: Position{Key: bytes.Clone(b.Key(i)), Offset: b.raw.Offset(i)}, Size: ptr(b.Size(i))}
	if b.Fields()&FieldLastModified != 0 {
		if us, ok := b.LastModified(i); ok {
			o.LastModified = ptr(time.UnixMicro(us).UTC())
		}
	}
	if b.Fields()&FieldETag != 0 {
		if v, ok := b.ETag(i); ok {
			o.ETag = ptr(string(v))
		}
	}
	if b.Fields()&FieldStorageClass != 0 {
		if v, ok := b.StorageClass(i); ok {
			o.StorageClass = ptr(string(v))
		}
	}
	return o
}

func TestScanBatchesMatchesScan(t *testing.T) {
	for _, id := range []SchemaID{S3ObjectsV1, GCSObjectsV1, AzblobObjectsV1} {
		t.Run(string(id), func(t *testing.T) {
			d := openDir(t, batchFixture(t, id))
			if err := d.CheckSchema(context.Background()); err != nil {
				t.Fatal(err)
			}
			all, err := scanAll(t, d, ScanRequest{})
			if err != nil {
				t.Fatal(err)
			}
			for fields := ObjectFields(0); fields <= AllObjectFields; fields++ {
				for _, workers := range []int{0, 1, 4, 8} {
					for _, prefix := range []string{"", "k0001", "missing"} {
						want := slices.Clone(all)
						want = slices.DeleteFunc(want, func(o Object) bool { return !bytes.HasPrefix(o.Key, []byte(prefix)) })
						for i := range want {
							if fields&FieldLastModified == 0 {
								want[i].LastModified = nil
							}
							if fields&FieldETag == 0 {
								want[i].ETag = nil
							}
							if fields&FieldStorageClass == 0 {
								want[i].StorageClass = nil
							}
						}
						var mu sync.Mutex
						var got []Object
						err := d.ScanBatches(context.Background(), BatchScanRequest{Prefix: []byte(prefix), Workers: workers, Fields: fields}, func(b *ObjectBatch) error {
							if b.Len() == 0 || b.Fields() != fields {
								return errors.New("empty batch or wrong fields")
							}
							mu.Lock()
							defer mu.Unlock()
							for i := 0; i < b.Len(); i++ {
								if i > 0 && bytes.Compare(b.Key(i-1), b.Key(i)) >= 0 {
									return errors.New("unordered keys")
								}
								got = append(got, copyBatchObject(b, i))
							}
							return nil
						})
						if err != nil {
							t.Fatal(err)
						}
						slices.SortFunc(got, func(a, b Object) int { return bytes.Compare(a.Key, b.Key) })
						if !reflect.DeepEqual(got, want) && (len(got) != 0 || len(want) != 0) {
							t.Fatalf("fields %d workers %d prefix %q: %d objects differ from %d", fields, workers, prefix, len(got), len(want))
						}
					}
				}
			}
		})
	}
}

func TestScanBatchesRowChecks(t *testing.T) {
	if minObjectMicros != time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro() ||
		endObjectMicros != time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro() {
		t.Fatal("batch timestamp bounds differ from the UTC year test")
	}
	for _, tc := range []struct {
		name, column string
		size, micros *int64
	}{
		{"null size", "size", nil, nil},
		{"negative size", "size", ptr(int64(-1)), nil},
		{"year zero", "last_modified", ptr(int64(0)), ptr(minObjectMicros - 1)},
		{"year 10000", "last_modified", ptr(int64(0)), ptr(endObjectMicros)},
		{"min int64", "last_modified", ptr(int64(0)), ptr(int64(math.MinInt64))},
		{"max int64", "last_modified", ptr(int64(0)), ptr(int64(math.MaxInt64))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := s3Rows(500)
			rows[333].Size, rows[333].LastModified = tc.size, tc.micros
			d := openDir(t, writeGeometry(t, s3Spec(), rows, 31, 127))
			_, oldErr := scanAll(t, d, ScanRequest{})
			old := semanticAt(t, oldErr, KindRow, tc.column)
			for _, workers := range []int{1, 4, 8} {
				var st ReadStats
				err := d.ScanBatches(context.Background(), BatchScanRequest{Workers: workers, Fields: AllObjectFields, Stats: &st}, func(*ObjectBatch) error { return nil })
				se := semanticAt(t, err, KindRow, tc.column)
				if !reflect.DeepEqual(se.Row, old.Row) || st.KeysDecoded == 0 {
					t.Fatalf("error %v, want %v; stats %+v", se, old, st)
				}
			}
			if tc.column == "last_modified" {
				if err := d.ScanBatches(context.Background(), BatchScanRequest{}, func(*ObjectBatch) error { return nil }); err != nil {
					t.Fatalf("unselected time checked: %v", err)
				}
			}
		})
	}
	rows := s3Rows(5)
	for i, v := range []int64{minObjectMicros, minObjectMicros + 1, -1, 0, endObjectMicros - 1} {
		rows[i].LastModified = ptr(v)
	}
	d := openDir(t, writeDataset(t, s3Spec(), rows))
	if err := d.ScanBatches(context.Background(), BatchScanRequest{Fields: FieldLastModified}, func(*ObjectBatch) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestScanBatchesSelectionAndTypes(t *testing.T) {
	type wrongTypes struct {
		Key          string `parquet:"key"`
		Size         *int64 `parquet:"size,optional"`
		LastModified *int64 `parquet:"last_modified,optional"` // no timestamp annotation, all null
		ETag         *int64 `parquet:"etag,optional"`          // wrong physical type, all null
		StorageClass *int64 `parquet:"storage_class,optional"`
	}
	d := openDir(t, writeDataset(t, s3Spec(), []wrongTypes{{Key: "a", Size: ptr(int64(1))}}))
	var st ReadStats
	if err := d.ScanBatches(context.Background(), BatchScanRequest{Stats: &st}, func(b *ObjectBatch) error {
		for _, accessor := range []func(){func() { b.LastModified(0) }, func() { b.ETag(0) }, func() { b.StorageClass(0) }, func() { b.Key(1) }, func() { b.Size(1) }, func() { b.Cursor(1) }} {
			func() {
				defer func() {
					if recover() == nil {
						t.Error("accessor did not panic")
					}
				}()
				accessor()
			}()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if st.ValuePagesDecoded != 1 {
		t.Fatalf("key+size read %d value pages", st.ValuePagesDecoded)
	}
	for field, column := range map[ObjectFields]string{FieldLastModified: "last_modified", FieldETag: "etag", FieldStorageClass: "storage_class"} {
		err := d.ScanBatches(context.Background(), BatchScanRequest{Fields: field}, func(*ObjectBatch) error { t.Error("called on bad schema"); return nil })
		se := semanticAt(t, err, KindSchema, column)
		if se.Row != nil || !strings.Contains(se.Error(), d.meta.Manifest.Parts[0].Path) {
			t.Fatalf("position %v", se)
		}
	}
	missing := openDir(t, writeDataset(t, s3Spec(), []s3RowNoETag{{Key: "a", Size: ptr(int64(1))}}))
	if err := missing.ScanBatches(context.Background(), BatchScanRequest{Fields: FieldLastModified | FieldStorageClass}, func(b *ObjectBatch) error {
		if _, ok := b.LastModified(0); ok {
			t.Error("null time present")
		}
		if _, ok := b.StorageClass(0); ok {
			t.Error("null class present")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err := missing.ScanBatches(context.Background(), BatchScanRequest{Fields: FieldETag}, func(*ObjectBatch) error { return nil })
	semanticAt(t, err, KindSchema, "etag")
	// Each part's logical type is checked, even when its physical type is
	// unchanged and an earlier part passed. An unselected malformed part
	// must not fail a prefix scan.
	first := writeDataset(t, s3Spec(), []s3Row{{Key: "a", Size: ptr(int64(1))}})
	appendPart(t, first, writeDataset(t, s3Spec(), []s3RowUntimed{{Key: "z", Size: ptr(int64(1))}}))
	parts := openDir(t, first)
	err = parts.ScanBatches(context.Background(), BatchScanRequest{Fields: FieldLastModified}, func(*ObjectBatch) error { return nil })
	if se := semanticAt(t, err, KindSchema, "last_modified"); se.Row != nil || !strings.Contains(se.Error(), parts.meta.Manifest.Parts[1].Path) {
		t.Fatal(se)
	}
	if err := parts.ScanBatches(context.Background(), BatchScanRequest{Prefix: []byte("a"), Fields: FieldLastModified}, func(*ObjectBatch) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestScanBatchesRefusalBeforeReads(t *testing.T) {
	for _, id := range []SchemaID{S3ObjectsV1, S3VersionsV1, GCSGenerationsV1, LocalV1} {
		s := s3Spec()
		s.id = id
		if id == S3VersionsV1 || id == GCSGenerationsV1 {
			s.key.Unique = false
		}
		d, src := openSource(t, writeDataset(t, s, s3Rows(1)))
		src.reset()
		st := ReadStats{PartsOpened: 99}
		req := BatchScanRequest{Stats: &st}
		want := error(ErrSchema)
		if id == S3ObjectsV1 {
			req.Fields = ObjectFields(128)
			want = ErrInvalidRequest
		}
		err := d.ScanBatches(context.Background(), req, func(*ObjectBatch) error { t.Error("callback"); return nil })
		if !errors.Is(err, want) || st != (ReadStats{}) {
			t.Fatalf("%s: %v stats %+v", id, err, st)
		}
		if _, reads := src.taken(); len(reads) != 0 {
			t.Fatalf("%s: %d reads", id, len(reads))
		}
	}
	// The key declaration cannot reinterpret repeated keys as current objects.
	s := s3Spec()
	s.key.Unique = false
	d := openDir(t, writeDataset(t, s, s3Rows(1)))
	semanticAt(t, d.ScanBatches(context.Background(), BatchScanRequest{}, func(*ObjectBatch) error { return nil }), KindSchema, "key")
}

func TestScanBatchesLifetime(t *testing.T) {
	d := openDir(t, batchFixture(t, S3ObjectsV1))
	want, err := scanAll(t, d, ScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var got []Object
	var cursors []*Cursor
	var wrapper *ObjectBatch
	err = d.ScanBatches(context.Background(), BatchScanRequest{Fields: AllObjectFields}, func(b *ObjectBatch) error {
		wrapper = b
		for i := 0; i < b.Len(); i++ {
			got = append(got, copyBatchObject(b, i))
			cursors = append(cursors, b.Cursor(i))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Internal assertion: reader holds no borrowed buffers after callback.
	if wrapper.raw != nil || wrapper.size != nil || wrapper.modified != nil || wrapper.etag != nil || wrapper.class != nil {
		t.Fatal("borrowed data retained")
	}
	if err := d.ScanBatches(context.Background(), BatchScanRequest{Fields: AllObjectFields}, func(*ObjectBatch) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("explicit copies changed after callbacks")
	}
	for i, c := range cursors {
		if !reflect.DeepEqual(c, want[i].Cursor()) {
			t.Fatalf("cursor %d changed", i)
		}
	}
	stop := errors.New("stop")
	err = d.ScanBatches(context.Background(), BatchScanRequest{}, func(b *ObjectBatch) error { wrapper = b; return stop })
	if err != stop || wrapper.raw != nil || wrapper.size != nil {
		t.Fatalf("callback error retains data: %v", err)
	}
}

func TestScanBatchesCancellationAndWorkers(t *testing.T) {
	d := openDir(t, scanDir(t, 5000, 200))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Force four callbacks to overlap. Separate shard storage must remain
	// valid while other workers decode and consume their own batches.
	var entered atomic.Int64
	ready := make(chan struct{})
	var once sync.Once
	err := d.ScanBatches(ctx, BatchScanRequest{Workers: 4, Fields: AllObjectFields}, func(b *ObjectBatch) error {
		before := copyBatchObject(b, 0)
		if entered.Add(1) == 4 {
			once.Do(func() { close(ready) })
		}
		select {
		case <-ready:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !reflect.DeepEqual(before, copyBatchObject(b, 0)) {
			return errors.New("workers share borrowed data")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("stop")
	var calls atomic.Int64
	var st ReadStats
	err = d.ScanBatches(context.Background(), BatchScanRequest{Workers: 4, Stats: &st}, func(*ObjectBatch) error { calls.Add(1); return stop })
	if err != stop || calls.Load() < 1 || calls.Load() > 4 || st.KeysDecoded == 0 {
		t.Fatalf("%v calls %d stats %+v", err, calls.Load(), st)
	}
	for _, workers := range []int{1, 4, 8} {
		ctx, cancel := context.WithCancel(context.Background())
		err := d.ScanBatches(ctx, BatchScanRequest{Workers: workers}, func(*ObjectBatch) error { cancel(); return nil })
		cancel()
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	empty := openDir(t, writeDataset[s3Row](t, s3Spec(), nil))
	if err := empty.ScanBatches(ctx, BatchScanRequest{}, func(*ObjectBatch) error { t.Error("callback"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	one := openDir(t, writeDataset(t, s3Spec(), s3Rows(1)))
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	if err := one.ScanBatches(ctx, BatchScanRequest{}, func(*ObjectBatch) error { cancel(); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if d.StreamStats() != (StreamStats{}) {
		t.Fatal("batch scan changed StreamStats")
	}
}

func TestScanBatchesStatsAndSourceError(t *testing.T) {
	dir := scanDir(t, 3000, 200)
	ctx := context.Background()
	for _, workers := range []int{1, 4} {
		sp := openDir(t, dir).SeekParquet()
		plan, err := sp.PlanRanges(ctx, []seekparquet.Range{{}}, []string{"size", "last_modified", "storage_class"})
		if err != nil {
			t.Fatal(err)
		}
		want := plan.Stats()
		shards, err := plan.Split(ctx, scanShards(workers))
		if err != nil {
			t.Fatal(err)
		}
		for _, shard := range shards {
			var st ReadStats
			if err := shard.ScanBatches(ctx, &st, func(*seekparquet.Batch) error { return nil }); err != nil {
				t.Fatal(err)
			}
			addStats(&want, shard.Stats())
			addStats(&want, st)
		}
		var got ReadStats
		if err := openDir(t, dir).ScanBatches(ctx, BatchScanRequest{Workers: workers, Fields: FieldLastModified | FieldStorageClass, Stats: &got}, func(*ObjectBatch) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("stats %+v want %+v", got, want)
		}
	}
	d, src := openSource(t, dir)
	failure := fmt.Errorf("source failed")
	src.mu.Lock()
	src.failAt, src.failErr = len(src.reads)+1, failure
	src.mu.Unlock()
	st := ReadStats{PartsOpened: 99}
	err := d.ScanBatches(ctx, BatchScanRequest{Stats: &st}, func(*ObjectBatch) error { t.Error("callback"); return nil })
	if !errors.Is(err, failure) || st != (ReadStats{}) {
		t.Fatalf("%v %+v", err, st)
	}
}

func TestScanBatchesLargePageAndAccessAllocations(t *testing.T) {
	d := openDir(t, writeGeometry(t, s3Spec(), s3Rows(5000), 4096, 5000))
	var largest int
	var sum int64
	stop := errors.New("checked one batch")
	err := d.ScanBatches(context.Background(), BatchScanRequest{Fields: AllObjectFields}, func(b *ObjectBatch) error {
		largest = max(largest, b.Len())
		allocs := testing.AllocsPerRun(10, func() {
			if err := b.check(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < b.Len(); i++ {
				sum += b.Size(i) + int64(len(b.Key(i)))
				if us, ok := b.LastModified(i); ok {
					sum += us
				}
				if v, ok := b.ETag(i); ok {
					sum += int64(len(v))
				}
				if v, ok := b.StorageClass(i); ok {
					sum += int64(len(v))
				}
			}
		})
		if allocs != 0 {
			t.Errorf("validation and typed accessors allocate: %v per batch", allocs)
		}
		return stop
	})
	if err != stop || largest <= scanBatchObjects || sum == 0 {
		t.Fatalf("error %v, batch length %d, sum %d", err, largest, sum)
	}
	// The existing API still caps its owning batches at 1,024.
	if _, err := scanAll(t, d, ScanRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestScanBatchesMandatorySizeSchema(t *testing.T) {
	type noSize struct {
		Key string `parquet:"key"`
	}
	type wrongSize struct {
		Key  string  `parquet:"key"`
		Size *string `parquet:"size,optional"`
	}
	for _, dir := range []string{
		writeDataset(t, s3Spec(), []noSize{{Key: "a"}}),
		writeDataset(t, s3Spec(), []wrongSize{{Key: "a"}}),
	} {
		d := openDir(t, dir)
		err := d.ScanBatches(context.Background(), BatchScanRequest{}, func(*ObjectBatch) error { t.Error("called on invalid size"); return nil })
		se := semanticAt(t, err, KindSchema, "size")
		if se.Row != nil || !strings.Contains(se.Error(), d.meta.Manifest.Parts[0].Path) {
			t.Fatal(se)
		}
	}
}

func TestScanBatchesKeyBytes(t *testing.T) {
	keys := []string{"a\x00", "a/", "é", "\ue000", "\U00010000"}
	d := openDir(t, writeGeometry(t, s3Spec(), keyRows(keys), 2, 3))
	var got []string
	if err := d.ScanBatches(context.Background(), BatchScanRequest{}, func(b *ObjectBatch) error {
		for i := 0; i < b.Len(); i++ {
			got = append(got, string(b.Key(i)))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, keys) {
		t.Fatalf("keys %q want %q", got, keys)
	}
}

// A part schema fault has the same position regardless of which shard
// enters the part first. There are 32 row groups, enough for eight shards.
func TestScanBatchesSchemaErrorStableAcrossShards(t *testing.T) {
	rows := make([]s3RowUntimed, 2048)
	for i := range rows {
		rows[i] = s3RowUntimed{Key: fmt.Sprintf("z%05d", i), Size: ptr(int64(i))}
	}
	d := openDir(t, writeGeometry(t, s3Spec(), rows, 31, 64))
	var want string
	for _, workers := range []int{1, 8} {
		for range 5 {
			err := d.ScanBatches(context.Background(), BatchScanRequest{Workers: workers, Fields: FieldLastModified}, func(*ObjectBatch) error {
				t.Error("callback on a part with an invalid schema")
				return nil
			})
			se := semanticAt(t, err, KindSchema, "last_modified")
			if se.Row != nil || !strings.Contains(se.Error(), "part "+d.meta.Manifest.Parts[0].Path+":") {
				t.Fatalf("part schema position: %v", se)
			}
			if want == "" {
				want = se.Error()
			}
			if se.Error() != want {
				t.Fatalf("workers=%d: %v, want %s", workers, se, want)
			}
		}
	}
}

// Scan retains its historical row decoder; batch scans also check the
// manifest key declaration and the selected columns' logical/physical types.
func TestScanBatchesStricterThanScan(t *testing.T) {
	type integerETag struct {
		Key          string  `parquet:"key"`
		Size         *int64  `parquet:"size,optional"`
		LastModified *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
		ETag         *int64  `parquet:"etag,optional"`
		StorageClass *string `parquet:"storage_class,optional"`
	}
	type binaryKey struct {
		Key          []byte  `parquet:"key"`
		Size         *int64  `parquet:"size,optional"`
		LastModified *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
		ETag         *string `parquet:"etag,optional"`
		StorageClass *string `parquet:"storage_class,optional"`
	}
	for _, tc := range []struct {
		name, column string
		write        func(*testing.T) string
	}{
		{"all-null integer etag", "etag", func(t *testing.T) string {
			return writeDataset(t, s3Spec(), []integerETag{{Key: "a", Size: ptr(int64(1))}})
		}},
		{"unannotated time", "last_modified", func(t *testing.T) string {
			return writeDataset(t, s3Spec(), []s3RowUntimed{{Key: "a", Size: ptr(int64(1)), LastModified: ptr(int64(0))}})
		}},
		{"binary key", "key", func(t *testing.T) string {
			return writeDataset(t, s3Spec(), []binaryKey{{Key: []byte("a"), Size: ptr(int64(1))}})
		}},
		{"nonunique key declaration", "key", func(t *testing.T) string {
			s := s3Spec()
			s.key.Unique = false
			return writeDataset(t, s, []s3Row{{Key: "a", Size: ptr(int64(1))}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := openDir(t, tc.write(t))
			objects, err := scanAll(t, d, ScanRequest{})
			if err != nil || len(objects) != 1 || string(objects[0].Key) != "a" || *objects[0].Size != 1 {
				t.Fatalf("historical Scan result: %+v, %v", objects, err)
			}
			err = d.ScanBatches(context.Background(), BatchScanRequest{Fields: AllObjectFields}, func(*ObjectBatch) error {
				t.Error("batch callback on invalid schema")
				return nil
			})
			if se := semanticAt(t, err, KindSchema, tc.column); se.Row != nil {
				t.Fatal(se)
			}
		})
	}
}

func TestScanBatchesSeveralBadRows(t *testing.T) {
	rows := s3Rows(16)
	rows[1].LastModified = ptr(endObjectMicros)
	rows[2].Size = ptr(int64(-1))
	d := openDir(t, writeGeometry(t, s3Spec(), rows, 16, 16))
	_, err := scanAll(t, d, ScanRequest{})
	se := semanticAt(t, err, KindRow, "last_modified")
	if string(se.Row.Key) != rows[1].Key || se.Row.Offset != 0 {
		t.Fatal(se)
	}
	err = d.ScanBatches(context.Background(), BatchScanRequest{Fields: FieldLastModified}, func(*ObjectBatch) error {
		t.Error("callback on invalid rows")
		return nil
	})
	se = semanticAt(t, err, KindRow, "size")
	if string(se.Row.Key) != rows[2].Key || se.Row.Offset != 0 {
		t.Fatal(se)
	}
}

// Closing from outside a callback waits for the active scan, while new
// requests (including the next part's Columns check) return ErrClosed.
func TestScanBatchesConcurrentClose(t *testing.T) {
	dir := writeParts(t, s3Spec(), 1, 1, s3Rows(1), []s3Row{{Key: "z", Size: ptr(int64(1))}})
	d := openDir(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{}, 1)
	defer close(release) // release on assertion failure too
	scanDone, closeDone := make(chan error, 1), make(chan error, 1)
	go func() {
		scanDone <- d.ScanBatches(ctx, BatchScanRequest{}, func(*ObjectBatch) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("scan never entered callback")
	}
	go func() { closeDone <- d.Close() }()
	for {
		_, err := d.sp.Columns(ctx, 0)
		if errors.Is(err, ErrClosed) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal("Close did not begin")
		}
		runtime.Gosched()
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before callback: %v", err)
	default:
	}
	// The callback's context select allows a release without closing the
	// channel twice when cleanup runs.
	release <- struct{}{}
	select {
	case err := <-scanDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("scan during Close: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("scan stuck during Close")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close stuck after scan")
	}
}

func TestScanBatchesAzureSchemaError(t *testing.T) {
	type wrongTier struct {
		Key        string `parquet:"key"`
		Size       *int64 `parquet:"size,optional"`
		AccessTier *int64 `parquet:"access_tier,optional"`
	}
	s := s3Spec()
	s.id = AzblobObjectsV1
	d := openDir(t, writeDataset(t, s, []wrongTier{{Key: "a", Size: ptr(int64(1))}}))
	err := d.ScanBatches(context.Background(), BatchScanRequest{Fields: FieldStorageClass}, func(*ObjectBatch) error {
		t.Error("callback on wrong access_tier type")
		return nil
	})
	se := semanticAt(t, err, KindSchema, "access_tier")
	if se.Row != nil || !strings.Contains(se.Error(), d.meta.Manifest.Parts[0].Path) {
		t.Fatal(se)
	}
}
