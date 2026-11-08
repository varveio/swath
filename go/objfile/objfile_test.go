// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/objfile"
)

// An Object is a SeekParquet SourceFile and ContextBinder. Source's Open returns
// SeekParquet's own interface type, so an Opener needs a one-method adapter
// (README).
var (
	_ seekparquet.SourceFile    = objfile.Object(nil)
	_ seekparquet.ContextBinder = objfile.Object(nil)
)

func TestCheckPath(t *testing.T) {
	for _, ok := range []string{"manifest.json", "data/part-00000.parquet", "a b/c..d/.e"} {
		if err := objfile.CheckPath(ok); err != nil {
			t.Errorf("CheckPath(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"", "/abs", "a//b", "a/", ".", "..", "a/./b", "../x", "a/..", `a\b`, "a\x00b", "a\x7fb"} {
		if err := objfile.CheckPath(bad); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("CheckPath(%q) = %v, want fs.ErrInvalid", bad, err)
		}
	}
}

// fakeOpener records the names it opens; its files block each read until
// release is closed, counting the reads in flight.
type fakeOpener struct {
	mu       sync.Mutex
	names    []string
	release  chan struct{}
	inFlight atomic.Int32
	peak     atomic.Int32
}

func (o *fakeOpener) Open(_ context.Context, name string) (objfile.Object, error) {
	o.mu.Lock()
	o.names = append(o.names, name)
	o.mu.Unlock()
	return fakeObject{o}, nil
}

func (o *fakeOpener) OpenSized(ctx context.Context, name string, _ int64) (objfile.Object, error) {
	return o.Open(ctx, name)
}

type fakeObject struct{ o *fakeOpener }

func (f fakeObject) ReadAt(p []byte, off int64) (int, error) {
	return f.WithContext(context.Background()).ReadAt(p, off)
}
func (f fakeObject) Size() int64      { return 1 }
func (f fakeObject) Identity() string { return "v" }
func (f fakeObject) Close() error     { return nil }
func (f fakeObject) WithContext(context.Context) io.ReaderAt {
	return readerFunc(func(p []byte, _ int64) (int, error) {
		n := f.o.inFlight.Add(1)
		for {
			peak := f.o.peak.Load()
			if n <= peak || f.o.peak.CompareAndSwap(peak, n) {
				break
			}
		}
		<-f.o.release
		f.o.inFlight.Add(-1)
		return len(p), nil
	})
}

type readerFunc func(p []byte, off int64) (int, error)

func (r readerFunc) ReadAt(p []byte, off int64) (int, error) { return r(p, off) }

func TestRoot(t *testing.T) {
	ctx := context.Background()
	for _, prefix := range []string{"datasets/ds", "datasets/ds/"} {
		o := &fakeOpener{}
		r := objfile.Root(o, prefix)
		if _, err := r.Open(ctx, "data/part-00000.parquet"); err != nil {
			t.Fatal(err)
		}
		if _, err := r.OpenSized(ctx, "manifest.json", 1); err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{"../other/manifest.json", "/etc/passwd", "data/../../x"} {
			if _, err := r.Open(ctx, bad); !errors.Is(err, fs.ErrInvalid) {
				t.Errorf("Root(%q).Open(%q) = %v, want fs.ErrInvalid", prefix, bad, err)
			}
			if _, err := r.OpenSized(ctx, bad, 1); !errors.Is(err, fs.ErrInvalid) {
				t.Errorf("Root(%q).OpenSized(%q) = %v, want fs.ErrInvalid", prefix, bad, err)
			}
		}
		want := []string{"datasets/ds/data/part-00000.parquet", "datasets/ds/manifest.json"}
		if len(o.names) != 2 || o.names[0] != want[0] || o.names[1] != want[1] {
			t.Errorf("Root(%q) opened %q, want %q", prefix, o.names, want)
		}
	}
	o := &fakeOpener{}
	if _, err := objfile.Root(o, "").Open(ctx, "manifest.json"); err != nil || o.names[0] != "manifest.json" {
		t.Errorf("Root with no prefix opened %q, %v", o.names, err)
	}
}

func TestLimit(t *testing.T) {
	o := &fakeOpener{release: make(chan struct{})}
	l := objfile.Limit(o, 3)
	f, err := l.Open(context.Background(), "obj")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { _, _ = f.ReadAt(make([]byte, 1), 0) })
	}
	for o.inFlight.Load() < 3 {
		time.Sleep(time.Millisecond)
	}
	// With every slot held, a read waits only as long as its context.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := f.WithContext(ctx).ReadAt(make([]byte, 1), 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("read waiting for a slot: %v, want context.DeadlineExceeded", err)
	}
	close(o.release)
	wg.Wait()
	if peak := o.peak.Load(); peak != 3 {
		t.Errorf("peak reads in flight = %d, want 3", peak)
	}
	if f.Identity() != "v" || f.Size() != 1 {
		t.Errorf("Limit hides the file's Identity or Size")
	}
}
