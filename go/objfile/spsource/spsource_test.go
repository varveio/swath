// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package spsource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

// fake is an Opener and ObjectReader whose calls return the errors set,
// and whose Objects are RangeReaders failing as set.
type fake struct {
	err     error // of Open, OpenSized and ReadObject
	readErr error // of every ReadAt
	batch   error // of ReadRanges
	data    []byte

	ctxs   []context.Context // of the calls that take one, in order
	ranges []objfile.Range   // the last batch
}

func (o *fake) Open(ctx context.Context, name string) (objfile.Object, error) {
	o.ctxs = append(o.ctxs, ctx)
	if o.err != nil {
		return nil, o.err
	}
	return &object{o: o}, nil
}

func (o *fake) OpenSized(ctx context.Context, name string, size int64) (objfile.Object, error) {
	return o.Open(ctx, name)
}

func (o *fake) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	o.ctxs = append(o.ctxs, ctx)
	if o.err != nil {
		return nil, "", o.err
	}
	return o.data, "v1", nil
}

type object struct{ o *fake }

func (f *object) Size() int64      { return int64(len(f.o.data)) }
func (f *object) Identity() string { return "v1" }
func (f *object) Close() error     { return nil }
func (f *object) ReadAt(p []byte, off int64) (int, error) {
	return f.WithContext(context.Background()).ReadAt(p, off)
}

func (f *object) WithContext(ctx context.Context) io.ReaderAt {
	f.o.ctxs = append(f.o.ctxs, ctx)
	return readerFunc(func(p []byte, off int64) (int, error) {
		if f.o.readErr != nil {
			return 0, f.o.readErr
		}
		n := copy(p, f.o.data[off:])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	})
}

func (f *object) ReadRanges(ctx context.Context, ranges []objfile.Range) error {
	f.o.ctxs = append(f.o.ctxs, ctx)
	f.o.ranges = ranges
	if f.o.batch != nil {
		return f.o.batch
	}
	for _, r := range ranges {
		copy(r.Buf, f.o.data[r.Off:])
	}
	return nil
}

type readerFunc func(p []byte, off int64) (int, error)

func (r readerFunc) ReadAt(p []byte, off int64) (int, error) { return r(p, off) }

// want is what a mapped error must match: is, every error errors.Is must
// find, and mismatch, the *seekparquet.SizeMismatchError errors.As must
// find, if any.
type want struct {
	is       []error
	mismatch *seekparquet.SizeMismatchError
}

func (w want) check(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("nil error")
	}
	for _, target := range w.is {
		if !errors.Is(err, target) {
			t.Errorf("%v does not match %v", err, target)
		}
	}
	var got *seekparquet.SizeMismatchError
	switch ok := errors.As(err, &got); {
	case w.mismatch == nil && ok:
		t.Errorf("%v is a size mismatch", err)
	case w.mismatch != nil && (!ok || *got != *w.mismatch):
		t.Errorf("%v: size mismatch %+v, want %+v", err, got, w.mismatch)
	}
}

var errOther = errors.New("other")

// mappings are the errors objfile gives and what each must match after
// translation, the original included.
var mappings = []struct {
	name string
	err  error
	want want
}{
	{"object changed", fmt.Errorf("read: %w", objfile.ErrObjectChanged),
		want{is: []error{seekparquet.ErrObjectChanged, objfile.ErrObjectChanged}}},
	{"object changed with another size", &fs.PathError{Op: "read", Path: "p", Err: fmt.Errorf("%w: %w", objfile.ErrObjectChanged, &objfile.SizeMismatchError{Name: "p", Expected: 4, Actual: 5})},
		want{is: []error{seekparquet.ErrObjectChanged, objfile.ErrObjectChanged}}},
	{"size mismatch", &fs.PathError{Op: "read", Path: "p", Err: &objfile.SizeMismatchError{Name: "p", Expected: 4, Actual: 5}},
		want{mismatch: &seekparquet.SizeMismatchError{Path: "p", Expected: 4, Actual: 5}}},
	{"missing", &fs.PathError{Op: "open", Path: "p", Err: fs.ErrNotExist},
		want{is: []error{fs.ErrNotExist}}},
	{"too large", &fs.PathError{Op: "read", Path: "p", Err: fmt.Errorf("%w: 9 bytes, limit 4", objfile.ErrTooLarge)},
		want{is: []error{seekparquet.ErrObjectTooLarge, objfile.ErrTooLarge}}},
	{"other", errOther, want{is: []error{errOther}}},
	{"cancelled", context.Canceled, want{is: []error{context.Canceled}}},
}

// TestErrorMapping: every call translates every error objfile gives, on
// opens, the manifest read, scalar and context-bound reads, and batches.
func TestErrorMapping(t *testing.T) {
	ctx := context.Background()
	calls := []struct {
		name string
		call func(o *fake, err error) error
	}{
		{"Open", func(o *fake, err error) error {
			o.err = err
			_, err = New(o).Open(ctx, "p")
			return err
		}},
		{"OpenSized", func(o *fake, err error) error {
			o.err = err
			_, err = New(o).OpenSized(ctx, "p", 4)
			return err
		}},
		{"ReadManifest", func(o *fake, err error) error {
			o.err = err
			_, _, err = New(o).ReadManifest(ctx, 4)
			return err
		}},
		{"ReadAt", func(o *fake, err error) error {
			f, _ := New(o).Open(ctx, "p")
			o.readErr = err
			_, err = f.ReadAt(make([]byte, 2), 0)
			return err
		}},
		{"WithContext.ReadAt", func(o *fake, err error) error {
			f, _ := New(o).Open(ctx, "p")
			o.readErr = err
			_, err = f.(seekparquet.ContextBinder).WithContext(ctx).ReadAt(make([]byte, 2), 0)
			return err
		}},
		{"ReadRanges", func(o *fake, err error) error {
			f, _ := New(o).Open(ctx, "p")
			o.batch = err
			return f.(seekparquet.RangeReader).ReadRanges(ctx, []seekparquet.ReadRange{{Off: 0, Buf: make([]byte, 2)}})
		}},
	}
	for _, c := range calls {
		for _, m := range mappings {
			t.Run(c.name+"/"+m.name, func(t *testing.T) {
				m.want.check(t, c.call(&fake{data: []byte("abcd")}, m.err))
			})
		}
	}
}

// TestBatchErrorMembers: a BatchError is translated member by member, in
// position, a member without an error left nil.
func TestBatchErrorMembers(t *testing.T) {
	members := make([]error, 0, len(mappings)+1)
	for _, m := range mappings {
		members = append(members, m.err)
	}
	members = append(members, nil)
	o := &fake{data: []byte("abcdefgh"), batch: &objfile.BatchError{Errs: members}}
	f, _ := New(o).Open(context.Background(), "p")
	ranges := make([]seekparquet.ReadRange, len(members))
	for i := range ranges {
		ranges[i] = seekparquet.ReadRange{Off: int64(i % 8), Buf: make([]byte, 1)}
	}
	err := f.(seekparquet.RangeReader).ReadRanges(context.Background(), ranges)
	var be *seekparquet.BatchError
	if !errors.As(err, &be) || len(be.Errs) != len(members) {
		t.Fatalf("%v (%T), want a seekparquet.BatchError of %d", err, err, len(members))
	}
	for i, m := range mappings {
		t.Run(m.name, func(t *testing.T) { m.want.check(t, be.Errs[i]) })
	}
	if last := be.Errs[len(members)-1]; last != nil {
		t.Fatalf("the range read has error %v", last)
	}
}

// TestInvalidBatch: a batch objfile refuses before any request is an
// invalid request to SeekParquet.
func TestInvalidBatch(t *testing.T) {
	f := file{&noBatch{object{&fake{data: []byte("abcd")}}}}
	err := f.ReadRanges(context.Background(), []seekparquet.ReadRange{{Off: 3, Buf: make([]byte, 2)}})
	if !errors.Is(err, seekparquet.ErrInvalidRequest) || !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("%v, want ErrInvalidRequest and fs.ErrInvalid", err)
	}
}

// noBatch is an Object without ReadRanges, which objfile.ReadRanges reads
// by its own planner, validating the batch first.
type noBatch struct{ object }

func (*noBatch) ReadRanges() {} // hides object's

// TestReadRangesSharesBuffers: the batch objfile reads has the caller's
// offsets and buffers, not copies, and is filled in place.
func TestReadRangesSharesBuffers(t *testing.T) {
	o := &fake{data: []byte("abcdefgh")}
	f, _ := New(o).Open(context.Background(), "p")
	ranges := []seekparquet.ReadRange{{Off: 6, Buf: make([]byte, 2)}, {Off: 1, Buf: make([]byte, 3)}}
	if err := f.(seekparquet.RangeReader).ReadRanges(context.Background(), ranges); err != nil {
		t.Fatal(err)
	}
	for i, r := range ranges {
		got := o.ranges[i]
		if got.Off != r.Off || &got.Buf[0] != &r.Buf[0] || len(got.Buf) != len(r.Buf) {
			t.Fatalf("range %d: objfile read %d bytes at %d of another buffer", i, len(got.Buf), got.Off)
		}
	}
	if string(ranges[0].Buf) != "gh" || string(ranges[1].Buf) != "bcd" {
		t.Fatalf("read %q %q", ranges[0].Buf, ranges[1].Buf)
	}
}

// TestContextReachesObjfile: the operation's context, with the cache mode
// and observer it carries, is the one every objfile call receives.
func TestContextReachesObjfile(t *testing.T) {
	type key struct{}
	obs := &objfiletest.Observer{}
	ctx := objfile.WithObserver(objfile.WithCacheMode(context.WithValue(context.Background(), key{}, "op"), objfile.CacheBypass), obs)
	o := &fake{data: []byte("abcd")}
	s := New(o)
	opened, err := s.Open(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	f, err := s.OpenSized(ctx, "p", 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.(seekparquet.ContextBinder).WithContext(ctx).ReadAt(make([]byte, 4), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.(seekparquet.RangeReader).ReadRanges(ctx, []seekparquet.ReadRange{{Buf: make([]byte, 1)}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReadManifest(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if len(o.ctxs) != 5 {
		t.Fatalf("%d calls took a context, want 5", len(o.ctxs))
	}
	for i, c := range o.ctxs {
		if c != ctx || c.Value(key{}) != "op" || objfile.CacheModeFromContext(c) != objfile.CacheBypass || objfile.ObserverFromContext(c) != obs {
			t.Fatalf("call %d lost the operation's context", i)
		}
	}
}

func TestCacheMode(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%t", batch), func(t *testing.T) {
			o := &fake{data: []byte("abcdefgh")}
			cache, err := objfile.NewByteCache(1024)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			f, err := New(objfile.Cache(o, "test", cache)).Open(ctx, "p")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { f.Close() })
			bypass := objfile.WithCacheMode(ctx, objfile.CacheBypass)
			read := func(ctx context.Context, wantReads int) {
				t.Helper()
				o.ctxs = nil
				buf := make([]byte, 4)
				if batch {
					err = f.(seekparquet.RangeReader).ReadRanges(ctx, []seekparquet.ReadRange{{Off: 2, Buf: buf}})
				} else {
					var n int
					n, err = f.(seekparquet.ContextBinder).WithContext(ctx).ReadAt(buf, 2)
					if n != len(buf) {
						t.Fatalf("read %d bytes, want %d", n, len(buf))
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if string(buf) != "cdef" || len(o.ctxs) != wantReads {
					t.Fatalf("read %q with %d underlying reads, want cdef with %d", buf, len(o.ctxs), wantReads)
				}
			}
			read(bypass, 1)
			read(bypass, 1)
			read(ctx, 1)    // bypass did not admit; default now admits
			read(ctx, 0)    // default looks up the admitted bytes
			read(bypass, 1) // bypass does not look up the warm entry
			read(ctx, 0)
		})
	}
}

// TestEOFUnchanged: a read cut short by the object's end returns io.EOF
// itself, as io.ReaderAt says.
func TestEOFUnchanged(t *testing.T) {
	f, _ := New(&fake{data: []byte("abcd")}).Open(context.Background(), "p")
	if n, err := f.ReadAt(make([]byte, 4), 2); n != 2 || err != io.EOF {
		t.Fatalf("%d, %v", n, err)
	}
}
