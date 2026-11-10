// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

// streamPages is Stream as it was before it read in one pass: Range
// requests of 1,024 rows, each after the last row of the one before, every
// row decoded and handed to fn in order. It is the oracle of the one-pass
// Stream's objects, order and row errors.
func streamPages[T any](ctx context.Context, d *Dataset, columns []string, decode func(seekparquet.Row) (T, error), fn func(T) error) error {
	var cursor *seekparquet.Cursor
	for {
		rows, err := d.sp.Range(ctx, seekparquet.Range{Cursor: cursor, Limit: 1024, Columns: columns})
		if err != nil || len(rows) == 0 {
			return err
		}
		cursor = rows[len(rows)-1].Cursor()
		for _, r := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			v, err := decode(r)
			if err != nil {
				return err
			}
			if err := fn(v); err != nil {
				return err
			}
		}
	}
}

// oldStream streams d's objects as Stream did before it read in one pass.
func oldStream(ctx context.Context, d *Dataset, fn func(Object) error) error {
	columns, err := d.currentColumns()
	if err != nil {
		return err
	}
	return streamPages(ctx, d, columns, func(r seekparquet.Row) (Object, error) { return decodeObject(d.spec.ID, r) }, fn)
}

// collect streams d with read, returning the objects handed on and the
// error it ended with.
func collect(d *Dataset, read func(context.Context, *Dataset, func(Object) error) error) ([]Object, error) {
	var out []Object
	err := read(context.Background(), d, func(o Object) error {
		out = append(out, o)
		return nil
	})
	return out, err
}

func newStream(ctx context.Context, d *Dataset, fn func(Object) error) error {
	return d.Stream(ctx, fn)
}

// streamFixtures are multi-part, multi-row-group datasets of every current
// object schema: the batch scan's (31-row pages, 127-row groups, two parts,
// varied nulls and empty strings), the scan's (three parts, 100-row pages
// cut by 300-row groups) and a generated one in swath's own geometry
// (1,024-row pages, dictionary and plain columns, Zstd).
func streamFixtures(t *testing.T) map[string]string {
	gen := filepath.Join(t.TempDir(), "gen")
	if err := writeFixture(context.Background(), gen, 20_000, 3); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"s3":     batchFixture(t, S3ObjectsV1),
		"gcs":    batchFixture(t, GCSObjectsV1),
		"azblob": batchFixture(t, AzblobObjectsV1),
		"scan":   scanDir(t, 5000, 300),
		"gen":    gen,
	}
}

// withStreamLimit runs f with Stream's request limit set to limit.
func withStreamLimit(limit int, f func()) {
	saved := streamLimit
	streamLimit = limit
	defer func() { streamLimit = saved }()
	f()
}

// TestStreamMatchesPages: Stream hands on exactly the paged Stream's
// objects, in the same order, with the same values, over parts, row groups
// and pages of every current-object schema, in one request; and, when a
// request stops at its limit, in several, each continuing after the last.
func TestStreamMatchesPages(t *testing.T) {
	for name, dir := range streamFixtures(t) {
		t.Run(name, func(t *testing.T) {
			want, err := collect(openDir(t, dir), oldStream)
			if err != nil || len(want) == 0 {
				t.Fatalf("oracle: %d objects, %v", len(want), err)
			}
			for _, limit := range []int{math.MaxInt, 1, 7, 1024, len(want), len(want) - 1} {
				d := openDir(t, dir)
				var got []Object
				withStreamLimit(limit, func() { got, err = collect(d, newStream) })
				if err != nil {
					t.Fatalf("limit %d: %v", limit, err)
				}
				if len(got) != len(want) {
					t.Fatalf("limit %d: %d objects, want %d", limit, len(got), len(want))
				}
				for i := range want {
					if !reflect.DeepEqual(got[i], want[i]) {
						t.Fatalf("limit %d: object %d %+v, want %+v", limit, i, got[i], want[i])
					}
				}
				// Within a later request, StreamStats holds nothing of the
				// stream: its fields are added together when it returns.
				if limit < len(want) {
					n := 0
					later := openDir(t, dir)
					withStreamLimit(limit, func() {
						err = later.Stream(context.Background(), func(Object) error {
							if n++; n == limit+1 {
								if st := later.StreamStats(); st != (StreamStats{}) {
									return fmt.Errorf("stats %+v during the stream's second request", st)
								}
							}
							return nil
						})
					})
					if err != nil {
						t.Fatalf("limit %d: %v", limit, err)
					}
				}
				st := d.StreamStats()
				if requests := int64(len(want)/limit + 1); st.Requests != requests || st.Objects != int64(len(want)) || st.KeysDecoded < int64(len(want)) {
					t.Fatalf("limit %d: stats %+v, want %d requests", limit, st, requests)
				}
			}
		})
	}
}

// TestStreamReadStats: a whole Stream is one request whose ReadStats are
// those of one Range of every row on a fresh Dataset, projecting the same
// columns; StreamStats adds each Stream's.
func TestStreamReadStats(t *testing.T) {
	dir := batchFixture(t, S3ObjectsV1)
	ref := openDir(t, dir)
	var want seekparquet.ReadStats
	rows, err := ref.sp.Range(context.Background(), seekparquet.Range{Limit: math.MaxInt, Columns: objectColumns(S3ObjectsV1), Stats: &want})
	if err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	got, err := collect(d, newStream)
	if err != nil {
		t.Fatal(err)
	}
	st := d.StreamStats()
	if st != (StreamStats{Requests: 1, Objects: int64(len(rows)), ReadStats: want}) || len(got) != len(rows) {
		t.Fatalf("stats %+v, want one request of %d objects and %+v", st, len(rows), want)
	}
	if st.PartsOpened != 2 || st.RowGroupsOpened == 0 || st.ValuePagesDecoded == 0 {
		t.Fatalf("stats %+v: want two parts, their row groups and value pages", st)
	}
	if _, err := collect(d, newStream); err != nil {
		t.Fatal(err)
	}
	if again := d.StreamStats(); again.Requests != 2 || again.Objects != 2*int64(len(rows)) || again.KeysDecoded <= st.KeysDecoded {
		t.Fatalf("after a second stream: %+v", again)
	}
}

// TestStreamEmptyAndCancelled: an empty dataset streams nothing in one
// request; a context ended before Stream starts calls no fn and is the
// error, the request still counted, with no objects; on an empty dataset,
// which has no object to check it before, it is a success, as before.
func TestStreamEmptyAndCancelled(t *testing.T) {
	empty := openDir(t, writeDataset[s3Row](t, s3Spec(), nil))
	got, err := collect(empty, newStream)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty: %d objects, %v", len(got), err)
	}
	if st := empty.StreamStats(); st != (StreamStats{Requests: 1}) {
		t.Fatalf("empty: stats %+v", st)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, read := range []func(context.Context, *Dataset, func(Object) error) error{oldStream, newStream} {
		if err := read(ctx, openDir(t, writeDataset[s3Row](t, s3Spec(), nil)), func(Object) error { return nil }); err != nil {
			t.Fatalf("cancelled before an empty stream: %v", err)
		}
	}
	d := openDir(t, writeDataset(t, s3Spec(), s3Rows(10)))
	called := false
	for _, read := range []func(context.Context, *Dataset, func(Object) error) error{oldStream, newStream} {
		if err := read(ctx, d, func(Object) error { called = true; return nil }); !errors.Is(err, context.Canceled) || called {
			t.Fatalf("cancelled before the stream: %v, fn called %t", err, called)
		}
	}
	if st := d.StreamStats(); st.Requests != 1 || st.Objects != 0 {
		t.Fatalf("cancelled before the stream: stats %+v", st)
	}
}

// TestStreamCancelEachObject: cancelling in fn at any object, the first
// and last of a page, row group or part included, ends the stream before
// the next object with the context's error, as the paged Stream did, even
// at the last object (where the pass, moving past it, sees the context
// ended), StreamStats counting the objects handed on.
func TestStreamCancelEachObject(t *testing.T) {
	dir := batchFixture(t, S3ObjectsV1)
	for _, at := range []int{1, 30, 31, 32, 126, 127, 128, 599, 600, 601, 1199, 1200} {
		for i, read := range []func(context.Context, *Dataset, func(Object) error) error{oldStream, newStream} {
			d := openDir(t, dir)
			ctx, cancel := context.WithCancel(context.Background())
			n := 0
			err := read(ctx, d, func(Object) error {
				if n++; n == at {
					cancel()
				}
				return nil
			})
			cancel()
			if !errors.Is(err, context.Canceled) || n != at {
				t.Fatalf("stream %d cancelled at %d: %v after %d objects", i, at, err, n)
			}
			if st := d.StreamStats(); i == 1 && (st.Objects != int64(at) || st.Requests != 1) {
				t.Fatalf("cancelled at %d: stats %+v", at, st)
			}
		}
	}
}

// TestStreamRowErrorParity: a row that fails a check or the decoder, deep
// in a later page, row group or part, ends the stream with the paged
// Stream's error (kind, column, row) after the same objects, the error
// owning its key.
func TestStreamRowErrorParity(t *testing.T) {
	year10000 := int64(253402300800000000)
	type bad struct {
		name   string
		at     int
		mutate func(*s3Row)
		kind   Kind
		column string
	}
	for _, tc := range []bad{
		{"null size in part 1", 77, func(r *s3Row) { r.Size = nil }, KindRow, "size"},
		{"negative size at a page start", 31 * 5, func(r *s3Row) { r.Size = ptr(int64(-1)) }, KindRow, "size"},
		{"time at a row group's last row", 126, func(r *s3Row) { r.LastModified = ptr(year10000) }, KindRow, "last_modified"},
		{"time in part 2", 1030, func(r *s3Row) { r.LastModified = ptr(year10000) }, KindRow, "last_modified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := s3Rows(1200)
			tc.mutate(&rows[tc.at])
			dir := writeParts(t, s3Spec(), 31, 127, rows[:600], rows[600:])
			want, wantErr := collect(openDir(t, dir), oldStream)
			d := openDir(t, dir)
			got, err := collect(d, newStream)
			se := semanticAt(t, err, tc.kind, tc.column)
			if !reflect.DeepEqual(err, wantErr) || len(got) != tc.at || len(want) != tc.at || string(se.Row.Key) != rows[tc.at].Key {
				t.Fatalf("%v after %d objects; the paged stream %v after %d", err, len(got), wantErr, len(want))
			}
			if d.StreamStats().Objects != int64(tc.at) {
				t.Fatalf("stats %+v", d.StreamStats())
			}
			// The error's key is its own: rereading the pages does not change it.
			if _, err := collect(d, newStream); err == nil {
				t.Fatal("second stream succeeded")
			}
			if string(se.Row.Key) != rows[tc.at].Key {
				t.Fatalf("error key changed to %q", se.Row.Key)
			}
		})
	}
	// A part lacking a column every part has: the decoder's KindSchema at
	// its first row, after every object of the part before.
	dir := writeDataset(t, s3Spec(), s3Rows(40))
	appendPart(t, dir, writeDataset(t, s3Spec(), []s3RowNoETag{{Key: "z1", Size: ptr(int64(1))}, {Key: "z2", Size: ptr(int64(2))}}))
	want, wantErr := collect(openDir(t, dir), oldStream)
	got, err := collect(openDir(t, dir), newStream)
	se := semanticAt(t, err, KindSchema, "etag")
	if !reflect.DeepEqual(err, wantErr) || len(got) != 40 || len(want) != 40 || string(se.Row.Key) != "z1" {
		t.Fatalf("absent column: %v after %d objects; the paged stream %v after %d", err, len(got), wantErr, len(want))
	}
}

// TestStreamErrorOwnsKey: a row that fails, decoded from a real batch,
// gives an error owning its key: overwriting the batch's borrowed keys
// after decoding changes none of the error's kind, column, key or offset.
func TestStreamErrorOwnsKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*s3Row)
		column string
	}{
		{"null size", func(r *s3Row) { r.Size = nil }, "size"},
		{"time after 9999", func(r *s3Row) { r.LastModified = ptr(int64(253402300800000000)) }, "last_modified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := s3Rows(100)
			tc.mutate(&rows[40])
			d := openDir(t, writeParts(t, s3Spec(), 31, 127, rows))
			columns := objectColumns(S3ObjectsV1)
			values := make([]seekparquet.Value, len(columns))
			var failed error
			err := d.sp.RangeBatches(context.Background(), seekparquet.Range{Limit: math.MaxInt, Columns: columns}, func(b *seekparquet.Batch) error {
				for i := range b.Len() {
					if _, err := d.batchObject(b, i, columns, values); err != nil {
						failed = err
						for j := range b.Len() { // poison every borrowed key of the batch
							k := b.Key(j)
							for x := range k {
								k[x] = '!'
							}
						}
						return err
					}
				}
				return nil
			})
			if err != failed {
				t.Fatalf("scan: %v, want the row's error %v", err, failed)
			}
			se := semanticAt(t, failed, KindRow, tc.column)
			if se.Row == nil || string(se.Row.Key) != rows[40].Key || se.Row.Offset != 0 {
				t.Fatalf("error row %+v after poisoning the batch, want key %q offset 0", se.Row, rows[40].Key)
			}
		})
	}
}

// TestStreamPartRefusalOnReach: a part the module refuses fails the stream
// when the pass reaches it, unchanged, after every object of the parts
// before it. The paged Stream, which read 1,024 keys before handing on any
// of them, handed on none of the five here: the documented change.
func TestStreamPartRefusalOnReach(t *testing.T) {
	dir := writeDataset(t, s3Spec(), s3Rows(5))
	appendPart(t, dir, writeDataset(t, s3Spec(), []s3Row{{Key: "z", Size: ptr(int64(1))}}))
	if err := removePart(dir, 1); err != nil {
		t.Fatal(err)
	}
	old, oldErr := collect(openDir(t, dir), oldStream)
	d := openDir(t, dir)
	got, err := collect(d, newStream)
	if seekparquet.CodeOf(err) != seekparquet.CodeMissingPart || seekparquet.CodeOf(oldErr) != seekparquet.CodeMissingPart {
		t.Fatalf("missing part: %v; the paged stream %v", err, oldErr)
	}
	if len(got) != 5 || len(old) != 0 {
		t.Fatalf("%d objects before the missing part (paged: %d), want 5 (0)", len(got), len(old))
	}
	if st := d.StreamStats(); st.Requests != 1 || st.Objects != 5 || st.PartsOpened != 1 || st.SourceReads == 0 {
		t.Fatalf("stats %+v", st)
	}
}

// TestStreamSourceFailure: a Source read failing at any point of the pass
// ends the stream with that error, unchanged, after a prefix of the
// dataset's objects in order, StreamStats counting the request, those
// objects and the reads made.
func TestStreamSourceFailure(t *testing.T) {
	dir := batchFixture(t, S3ObjectsV1)
	all, err := collect(openDir(t, dir), newStream)
	if err != nil {
		t.Fatal(err)
	}
	d0, src := openSource(t, dir)
	_, opened := src.taken()
	if _, err := collect(d0, newStream); err != nil {
		t.Fatal(err)
	}
	_, reads := src.taken()
	total := len(reads) - len(opened)
	failure := errors.New("failing read")
	for at := 1; at <= total; at += max(1, total/40) {
		d, src := openSource(t, dir)
		src.mu.Lock()
		src.failAt, src.failErr = len(src.reads)+at, failure
		src.mu.Unlock()
		got, err := collect(d, newStream)
		if !errors.Is(err, failure) {
			t.Fatalf("failing read %d of %d: %v", at, total, err)
		}
		if len(got) >= len(all) || len(got) > 0 && !reflect.DeepEqual(got, all[:len(got)]) {
			t.Fatalf("failing read %d: %d objects, not a proper prefix", at, len(got))
		}
		if st := d.StreamStats(); st.Requests != 1 || st.Objects != int64(len(got)) || st.SourceReads < int64(at) {
			t.Fatalf("failing read %d: stats %+v after %d objects", at, st, len(got))
		}
	}
}

// TestStreamLocalMatchesPages: StreamLocal hands on the paged StreamLocal's
// entries in order, in one request (several when the limit stops it), its
// work in StreamStats; Stream's refusal of the entries adds nothing.
func TestStreamLocalMatchesPages(t *testing.T) {
	for _, names := range []bool{false, true} {
		dir := localDir(t, names)
		ref := openDir(t, dir)
		var want []LocalEntry
		if err := streamPages(context.Background(), ref, ref.spec.ColumnNames(), decodeLocal, func(e LocalEntry) error {
			want = append(want, e)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int{math.MaxInt, 1, 2, 4} {
			d := openDir(t, dir)
			var got []LocalEntry
			withStreamLimit(limit, func() { got = streamLocal(t, d) })
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("names %t, limit %d: %+v, want %+v", names, limit, got, want)
			}
			if _, err := stream(t, d); !errors.Is(err, ErrSchema) {
				t.Fatalf("stream of entries: %v", err)
			}
			if st := d.StreamStats(); st.Requests != int64(len(want)/limit+1) || st.Objects != int64(len(want)) || st.PartsOpened != 2 {
				t.Fatalf("names %t, limit %d: stats %+v", names, limit, st)
			}
		}
	}
}

// TestStreamLocalOwnsEntries: an entry's key and link target are its own,
// changed by fn without changing the next entries or another stream's.
func TestStreamLocalOwnsEntries(t *testing.T) {
	d := openDir(t, localDir(t, false))
	var first []LocalEntry
	err := d.StreamLocal(context.Background(), func(e LocalEntry) error {
		first = append(first, e)
		for i := range e.Key {
			e.Key[i] = 0
		}
		if e.LinkTarget != nil {
			for i := range *e.LinkTarget {
				(*e.LinkTarget)[i] = 0
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range streamLocal(t, d) {
		if want := wantLocal(localTree[i].key, false); !reflect.DeepEqual(e, want) {
			t.Fatalf("entry %d after changes: %+v, want %+v", i, e, want)
		}
	}
	if len(first) != len(localTree) {
		t.Fatalf("%d entries", len(first))
	}
}

func removePart(dir string, part int) error {
	return os.Remove(filepath.Join(dir, "data", fmt.Sprintf("part-%05d.parquet", part)))
}

// TestStreamConcurrentClose: Close from another goroutine waits for the
// callback running when it begins. A callback error is then returned
// unchanged; a callback that returns nil lets the stream hand on the rest
// of its batch, and the pass then ends with ErrClosed.
func TestStreamConcurrentClose(t *testing.T) {
	stop := errors.New("stop")
	for _, fnErr := range []error{stop, nil} {
		d := openDir(t, writeParts(t, s3Spec(), 31, 127, s3Rows(100), s3Rows(200)[100:]))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		entered, release := make(chan struct{}), make(chan struct{})
		streamDone, closeDone := make(chan error, 1), make(chan error, 1)
		n := 0
		go func() {
			streamDone <- d.Stream(ctx, func(Object) error {
				if n++; n > 1 {
					return nil
				}
				close(entered)
				select {
				case <-release:
					return fnErr
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
		<-entered
		go func() { closeDone <- d.Close() }()
		for {
			if _, err := d.sp.Columns(ctx, 0); errors.Is(err, ErrClosed) {
				break
			} else if err != nil || ctx.Err() != nil {
				t.Fatalf("Close did not begin: %v", err)
			}
			runtime.Gosched()
		}
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned during the callback: %v", err)
		default:
		}
		close(release)
		err := <-streamDone
		if fnErr != nil && err != stop || fnErr == nil && (!errors.Is(err, ErrClosed) || n != 31) {
			t.Fatalf("callback returning %v during Close: %v after %d objects", fnErr, err, n)
		}
		if err := <-closeDone; err != nil {
			t.Fatal(err)
		}
		cancel()
	}
}
