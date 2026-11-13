// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package objfiletest is the behaviour every store's Opener is tested for,
// run against an in-process fake of the store's HTTP API.
package objfiletest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/objfile"
)

// Store is a fake store reached through an Opener of the package under test.
type Store struct {
	Opener objfile.Opener
	// Put creates or replaces an object.
	Put func(t testing.TB, name string, data []byte)
	// Delete removes an object.
	Delete func(t testing.TB, name string)
}

// NewStore starts a fake store whose HTTP handler is wrapped by wrap.
type NewStore func(t *testing.T, wrap func(http.Handler) http.Handler) Store

// Data returns n bytes that differ by seed and position.
func Data(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7) ^ seed
	}
	return b
}

// opener opens name, of size bytes, one way.
type opener func(t *testing.T, s Store, name string, size int) objfile.Object

// Run tests the Opener newStore builds: each case through Open and through
// OpenSized, then what is particular to each.
func Run(t *testing.T, newStore NewStore) {
	t.Run("Open", func(t *testing.T) {
		run(t, newStore, func(t *testing.T, s Store, name string, _ int) objfile.Object {
			t.Helper()
			f, err := s.Opener.Open(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			if f.Identity() == "" {
				t.Fatal("Open pinned no identity")
			}
			return f
		})
	})
	t.Run("OpenSized", func(t *testing.T) {
		run(t, newStore, func(t *testing.T, s Store, name string, size int) objfile.Object {
			t.Helper()
			f, err := s.Opener.OpenSized(context.Background(), name, int64(size))
			if err != nil {
				t.Fatal(err)
			}
			return f
		})
	})
	runOpen(t, newStore)
	runSized(t, newStore)
}

func plain(t *testing.T, newStore NewStore) Store {
	return newStore(t, func(h http.Handler) http.Handler { return h })
}

func run(t *testing.T, newStore NewStore, open opener) {
	ctx := context.Background()

	t.Run("SizeAndReads", func(t *testing.T) {
		s := plain(t, newStore)
		data := Data(1000, 1)
		s.Put(t, "dir/obj", data)
		f := open(t, s, "dir/obj", len(data))
		if f.Size() != 1000 {
			t.Fatalf("Size = %d, want 1000", f.Size())
		}
		for _, c := range []struct {
			off, len, want int
			eof            bool
		}{{0, 1000, 1000, false}, {17, 100, 100, false}, {999, 1, 1, false}, {990, 20, 10, true},
			{1000, 5, 0, true}, {5000, 5, 0, true}, {10, 0, 0, false}} {
			p := make([]byte, c.len)
			n, err := f.ReadAt(p, int64(c.off))
			if n != c.want || (c.eof != (err == io.EOF)) || (!c.eof && err != nil) {
				t.Fatalf("ReadAt(len %d, off %d) = %d, %v; want %d, eof %v", c.len, c.off, n, err, c.want, c.eof)
			}
			if !bytes.Equal(p[:n], data[min(c.off, len(data)):min(c.off, len(data))+n]) {
				t.Fatalf("ReadAt(len %d, off %d) read the wrong bytes", c.len, c.off)
			}
		}
		s.Put(t, "empty", nil)
		e := open(t, s, "empty", 0)
		if n, err := e.ReadAt(make([]byte, 4), 0); e.Size() != 0 || n != 0 || err != io.EOF {
			t.Fatalf("empty object: Size %d, ReadAt %d, %v", e.Size(), n, err)
		}
	})

	t.Run("Replaced", func(t *testing.T) {
		s := plain(t, newStore)
		s.Put(t, "obj", Data(512, 1))
		f := open(t, s, "obj", 512)
		p := make([]byte, 512)
		if _, err := f.ReadAt(p, 0); err != nil {
			t.Fatal(err)
		}
		s.Put(t, "obj", Data(512, 2))
		clear(p)
		n, err := f.ReadAt(p, 100)
		if !errors.Is(err, objfile.ErrObjectChanged) || n != 0 || !bytes.Equal(p, make([]byte, 512)) {
			t.Fatalf("ReadAt after replace = %d, %v; want 0, ErrObjectChanged and no bytes", n, err)
		}
	})

	t.Run("Deleted", func(t *testing.T) {
		s := plain(t, newStore)
		s.Put(t, "obj", Data(64, 1))
		f := open(t, s, "obj", 64)
		if _, err := f.ReadAt(make([]byte, 8), 0); err != nil {
			t.Fatal(err)
		}
		s.Delete(t, "obj")
		if _, err := f.ReadAt(make([]byte, 8), 0); !errors.Is(err, objfile.ErrObjectChanged) {
			t.Fatalf("ReadAt after delete: %v, want ErrObjectChanged", err)
		}
	})

	t.Run("CancelMidRead", func(t *testing.T) {
		stalled := make(chan struct{}, 1)
		s := newStore(t, func(h http.Handler) http.Handler { return stall(h, stalled) })
		s.Put(t, "obj", Data(1<<16, 1))
		f := open(t, s, "obj", 1<<16)
		rctx, cancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-stalled:
			case <-time.After(10 * time.Second):
			}
			cancel()
		}()
		done := make(chan error, 1)
		go func() {
			_, err := f.WithContext(rctx).ReadAt(make([]byte, 1<<15), 100)
			done <- err
		}()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled ReadAt: %v, want context.Canceled", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("cancelled ReadAt did not return")
		}
	})

	t.Run("Concurrent", func(t *testing.T) {
		s := plain(t, newStore)
		data := Data(1<<16, 3)
		s.Put(t, "obj", data)
		f := open(t, s, "obj", len(data))
		var wg sync.WaitGroup
		errs := make(chan error, 16)
		for i := range 16 {
			wg.Go(func() {
				off, n := i*4000, 3000+i*10
				p := make([]byte, n)
				if _, err := f.ReadAt(p, int64(off)); err != nil {
					errs <- err
				} else if !bytes.Equal(p, data[off:off+n]) {
					errs <- fmt.Errorf("reader %d read the wrong bytes", i)
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})

	t.Run("Closed", func(t *testing.T) {
		s := plain(t, newStore)
		s.Put(t, "obj", Data(64, 1))
		f := open(t, s, "obj", 64)
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := f.ReadAt(make([]byte, 8), 0); !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("ReadAt after Close: %v, want fs.ErrClosed", err)
		}
	})

}

// runOpen tests what is particular to Open.
func runOpen(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	t.Run("Missing", func(t *testing.T) {
		s := plain(t, newStore)
		if _, err := s.Opener.Open(ctx, "nope"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Open of a missing object: %v, want fs.ErrNotExist", err)
		}
	})

	t.Run("OpenCancelled", func(t *testing.T) {
		s := plain(t, newStore)
		s.Put(t, "obj", Data(64, 1))
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.Opener.Open(cctx, "obj"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Open with a cancelled context: %v, want context.Canceled", err)
		}
	})
}

// runSized tests what is particular to OpenSized: the first read pins, and
// a missing object or another size fails it.
func runSized(t *testing.T, newStore NewStore) {
	ctx := context.Background()
	t.Run("SizedPins", func(t *testing.T) {
		s := plain(t, newStore)
		s.Put(t, "obj", Data(100, 1))
		opened, err := s.Opener.Open(ctx, "obj")
		if err != nil {
			t.Fatal(err)
		}
		f, err := s.Opener.OpenSized(ctx, "obj", 100)
		if err != nil {
			t.Fatal(err)
		}
		if f.Identity() != "" {
			t.Fatalf("Identity before the first read = %q", f.Identity())
		}
		if _, err := f.ReadAt(make([]byte, 10), 0); err != nil {
			t.Fatal(err)
		}
		if f.Identity() != opened.Identity() {
			t.Fatalf("Identity = %q after the first read, Open pinned %q", f.Identity(), opened.Identity())
		}
	})
	t.Run("SizedZero", func(t *testing.T) {
		// No read proves an empty object: OpenSized(..., 0) asks for
		// metadata, and compares.
		s := plain(t, newStore)
		s.Put(t, "empty", nil)
		s.Put(t, "full", Data(10, 1))
		if f, err := s.Opener.OpenSized(ctx, "empty", 0); err != nil || f.Identity() == "" {
			t.Fatalf("OpenSized(0) of an empty object: %v", err)
		}
		var sm *objfile.SizeMismatchError
		if _, err := s.Opener.OpenSized(ctx, "full", 0); !errors.As(err, &sm) || sm.Actual != 10 {
			t.Fatalf("OpenSized(0) of a 10-byte object: %v, want a SizeMismatchError of 10", err)
		}
		if _, err := s.Opener.OpenSized(ctx, "nope", 0); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("OpenSized(0) of a missing object: %v, want fs.ErrNotExist", err)
		}
	})
	t.Run("SizedNegative", func(t *testing.T) {
		s := plain(t, newStore)
		if _, err := s.Opener.OpenSized(ctx, "obj", -1); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("OpenSized of a negative size: %v, want fs.ErrInvalid", err)
		}
	})
	t.Run("SizedMissing", func(t *testing.T) {
		s := plain(t, newStore)
		f, err := s.Opener.OpenSized(ctx, "nope", 10)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.ReadAt(make([]byte, 4), 0); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("first read of a missing object: %v, want fs.ErrNotExist", err)
		}
	})
	t.Run("SizedWrongSize", func(t *testing.T) {
		s := plain(t, newStore)
		s.Put(t, "obj", Data(100, 1))
		f, err := s.Opener.OpenSized(ctx, "obj", 99)
		if err != nil {
			t.Fatal(err)
		}
		var sm *objfile.SizeMismatchError
		if _, err := f.ReadAt(make([]byte, 4), 0); !errors.As(err, &sm) || errors.Is(err, objfile.ErrObjectChanged) || sm.Actual != 100 {
			t.Fatalf("read of an object of another size: %v, want a SizeMismatchError of 100 bytes", err)
		}
	})
	t.Run("SizedShort", func(t *testing.T) {
		// The store clips the range at the object's end: the response is
		// short, and its total names the real size.
		s := plain(t, newStore)
		s.Put(t, "obj", Data(90, 1))
		f, err := s.Opener.OpenSized(ctx, "obj", 100)
		if err != nil {
			t.Fatal(err)
		}
		var sm *objfile.SizeMismatchError
		if n, err := f.ReadAt(make([]byte, 20), 80); !errors.As(err, &sm) || sm.Actual != 90 || n != 0 {
			t.Fatalf("read past a shorter object's end = %d, %v; want 0, a SizeMismatchError of 90 bytes", n, err)
		}
	})
}

// stall serves a ranged GET's headers and half its body, signals stalled,
// and holds the response until the client goes away.
func stall(h http.Handler, stalled chan<- struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || (r.Header.Get("Range") == "" && r.Header.Get("x-ms-range") == "") {
			h.ServeHTTP(w, r)
			return
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		// The body is object bytes; never let a client take it for a page.
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(rec.Code)
		body := rec.Body.Bytes()
		_, _ = w.Write(body[:len(body)/2])
		w.(http.Flusher).Flush()
		select {
		case stalled <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	})
}
