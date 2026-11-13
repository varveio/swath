// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/internal/objfiletest"
)

func newCache(t *testing.T, n int64) *objfile.ByteCache {
	t.Helper()
	c, err := objfile.NewByteCache(n)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// reads returns how many reads the store has served, and forgets them.
func (s *mstore) takeReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.reads)
	s.reads = nil
	return n
}

func TestNewByteCache(t *testing.T) {
	if _, err := objfile.NewByteCache(-1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("a negative limit: %v, want fs.ErrInvalid", err)
	}
	st := newMstore()
	for _, c := range []*objfile.ByteCache{nil, newCache(t, 0), {}} {
		if o := objfile.Cache(st, scope, c); o != objfile.Opener(st) {
			t.Fatalf("a disabled cache %v wraps the Opener", c)
		}
	}
}

// TestCacheRevisit: a pinned Object's ReadAt and batch are served from the
// cache on a revisit, with no request, and reported as cache hits.
func TestCacheRevisit(t *testing.T) {
	st := newMstore()
	data := objfiletest.Data(10000, 1)
	st.put("obj", data)
	o := objfile.Cache(st, scope, newCache(t, 1<<20))
	obs := &objfiletest.Observer{}
	ctx := objfile.WithObserver(context.Background(), obs)
	f, err := o.Open(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	read := func(rs []objfile.Range) {
		t.Helper()
		if err := objfile.ReadRanges(ctx, f, rs); err != nil {
			t.Fatal(err)
		}
		for i, r := range rs {
			if !bytes.Equal(r.Buf, data[r.Off:r.Off+int64(len(r.Buf))]) {
				t.Fatalf("range %d holds the wrong bytes", i)
			}
		}
	}
	read([]objfile.Range{{Off: 0, Buf: make([]byte, 4000)}, {Off: 8000, Buf: make([]byte, 100)}})
	if n := st.takeReads(); n != 2 || obs.Hits() != 0 {
		t.Fatalf("cold batch: %d reads, %d hit bytes; want 2, 0", n, obs.Hits())
	}
	// Within cached extents, in any order and with an empty range.
	read([]objfile.Range{{Off: 8050, Buf: make([]byte, 50)}, {Off: 100, Buf: make([]byte, 3000)}, {Off: 9000, Buf: nil}})
	if n := st.takeReads(); n != 0 || obs.Hits() != 3050 {
		t.Fatalf("warm batch: %d reads, %d hit bytes; want 0, 3050", n, obs.Hits())
	}
	// A mixture: the miss alone is sent, in one batch, and then cached.
	obs.Reset()
	read([]objfile.Range{{Off: 10, Buf: make([]byte, 10)}, {Off: 5000, Buf: make([]byte, 10)}})
	if n := st.takeReads(); n != 1 || obs.Hits() != 10 || len(obs.Plans()) != 1 || obs.Plans()[0][0] != 1 {
		t.Fatalf("mixed batch: %d reads, %d hit bytes, plans %v; want 1, 10, one plan of the one miss", n, obs.Hits(), obs.Plans())
	}
	p := make([]byte, 10)
	if n, err := f.WithContext(ctx).ReadAt(p, 5000); err != nil || n != 10 || !bytes.Equal(p, data[5000:5010]) || st.takeReads() != 0 {
		t.Fatalf("ReadAt of a cached range = %d, %v, or made a request", n, err)
	}
	// A ReadAt past the end returns what the cache holds and io.EOF.
	p = make([]byte, 200)
	if n, err := f.ReadAt(p, 9950); n != 50 || err != io.EOF || st.takeReads() != 1 {
		t.Fatalf("cold ReadAt at the end = %d, %v", n, err)
	}
	if n, err := f.ReadAt(p, 9950); n != 50 || err != io.EOF || !bytes.Equal(p[:50], data[9950:]) || st.takeReads() != 0 {
		t.Fatalf("warm ReadAt at the end = %d, %v", n, err)
	}
	// Closed: the read goes to the Object (which fails it, the store
	// packages' with fs.ErrClosed), never to the cache.
	f.Close()
	if _, _ = f.ReadAt(make([]byte, 10), 0); st.takeReads() != 1 {
		t.Fatal("a closed Object served cached bytes")
	}
}

func TestCacheAlreadyCancelled(t *testing.T) {
	st := newMstore()
	st.put("obj", objfiletest.Data(1000, 1))
	f, err := objfile.Cache(st, scope, newCache(t, 1000)).Open(context.Background(), "obj")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.ReadAt(make([]byte, 100), 0); err != nil {
		t.Fatal(err)
	}
	st.takeReads()
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			obs := &objfiletest.Observer{}
			ctx, cancel := context.WithCancel(objfile.WithObserver(context.Background(), obs))
			cancel()
			p := []byte("untouched!")
			if batch {
				err = objfile.ReadRanges(ctx, f, []objfile.Range{{Off: 0, Buf: p}})
			} else {
				var n int
				n, err = f.WithContext(ctx).ReadAt(p, 0)
				if n != 0 {
					t.Errorf("ReadAt = %d bytes, want 0", n)
				}
			}
			if !errors.Is(err, context.Canceled) || string(p) != "untouched!" {
				t.Errorf("already cancelled read = %v, buffer %q; want context.Canceled and untouched buffer", err, p)
			}
			if n := st.takeReads(); n != 0 || obs.Hits() != 0 {
				t.Errorf("already cancelled read: %d reads, %d hit bytes; want 0, 0", n, obs.Hits())
			}
		})
	}
}

// TestCacheIdentity: a cache never serves bytes across versions, never
// stands in for a first read's proof, and never hides Pin's check.
func TestCacheIdentity(t *testing.T) {
	ctx := context.Background()
	old, repl := objfiletest.Data(1000, 1), objfiletest.Data(1000, 2)
	warm := func(t *testing.T, o objfile.Opener) {
		t.Helper()
		f, err := o.Open(ctx, "obj")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.ReadAt(make([]byte, 1000), 0); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("ReplacedThenOpen", func(t *testing.T) {
		st := newMstore()
		st.put("obj", old)
		o := objfile.Cache(st, scope, newCache(t, 1<<20))
		warm(t, o)
		st.put("obj", repl) // same size, another version
		f, err := o.Open(ctx, "obj")
		if err != nil {
			t.Fatal(err)
		}
		p := make([]byte, 100)
		if _, err := f.ReadAt(p, 0); err != nil || !bytes.Equal(p, repl[:100]) {
			t.Fatalf("a reopened replaced object read %v from the cache", err)
		}
	})
	t.Run("SizedFirstReadGoesToStore", func(t *testing.T) {
		st := newMstore()
		st.put("obj", old)
		o := objfile.Cache(st, scope, newCache(t, 1<<20))
		warm(t, o)
		st.takeReads()
		f, err := o.OpenSized(ctx, "obj", 1000)
		if err != nil {
			t.Fatal(err)
		}
		// Even an extent the cache holds is proved by the store first.
		p := make([]byte, 100)
		if _, err := f.ReadAt(p, 0); err != nil || st.takeReads() != 1 {
			t.Fatalf("the first read of a sized Object: %v, or it was served from the cache", err)
		}
		if _, err := f.ReadAt(p, 0); err != nil || st.takeReads() != 0 {
			t.Fatalf("a read after the pin: %v, or it was not served from the cache", err)
		}
		// Replaced before a sized open's first read: the new version.
		st.put("obj", repl)
		g, _ := o.OpenSized(ctx, "obj", 1000)
		if _, err := g.ReadAt(p, 0); err != nil || !bytes.Equal(p, repl[:100]) {
			t.Fatalf("a sized open of a replaced object read %v from the cache", err)
		}
		// Of another size: the store says so, not the cache.
		g, _ = o.OpenSized(ctx, "obj", 999)
		var sm *objfile.SizeMismatchError
		if _, err := g.ReadAt(p, 0); !errors.As(err, &sm) {
			t.Fatalf("a wrong size over a warm cache: %v", err)
		}
	})
	t.Run("FirstReadNotAdmitted", func(t *testing.T) {
		st := newMstore()
		st.put("obj", old)
		o := objfile.Cache(st, scope, newCache(t, 1<<20))
		f, _ := o.OpenSized(ctx, "obj", 1000)
		if err := objfile.ReadRanges(ctx, f, []objfile.Range{{Off: 0, Buf: make([]byte, 10)}, {Off: 500, Buf: make([]byte, 10)}}); err != nil {
			t.Fatal(err)
		}
		st.takeReads()
		if _, err := f.ReadAt(make([]byte, 10), 0); err != nil || st.takeReads() != 1 {
			t.Fatalf("a range of the batch that pinned the Object was cached: %v", err)
		}
	})
	t.Run("PinRecordedMismatch", func(t *testing.T) {
		for _, cacheBelow := range []bool{false, true} {
			st := newMstore()
			st.put("obj", old)
			c := newCache(t, 1<<20)
			var o objfile.Opener
			if cacheBelow {
				o = objfile.Pin(objfile.Cache(st, scope, c), scope)
			} else {
				o = objfile.Cache(objfile.Pin(st, scope), scope, c)
			}
			warm(t, o)
			st.put("obj", repl)
			if _, err := o.Open(ctx, "obj"); !errors.Is(err, objfile.ErrObjectChanged) {
				t.Fatalf("cache below Pin %v: Open of a replaced object = %v", cacheBelow, err)
			}
			f, err := o.OpenSized(ctx, "obj", 1000)
			if err != nil {
				t.Fatal(err)
			}
			if err := objfile.ReadRanges(ctx, f, []objfile.Range{{Off: 0, Buf: make([]byte, 100)}}); !errors.Is(err, objfile.ErrObjectChanged) {
				t.Fatalf("cache below Pin %v: sized read of a replaced object = %v", cacheBelow, err)
			}
		}
	})
	t.Run("ScopeAndName", func(t *testing.T) {
		st := newMstore()
		st.put("a", old)
		st.put("b", repl)
		c := newCache(t, 1<<20)
		warm := func(o objfile.Opener, name string) {
			f, _ := o.Open(ctx, name)
			if _, err := f.ReadAt(make([]byte, 1000), 0); err != nil {
				t.Fatal(err)
			}
		}
		warm(objfile.Cache(st, "one", c), "a")
		f, _ := objfile.Cache(st, "two", c).Open(ctx, "a")
		st.takeReads()
		if _, err := f.ReadAt(make([]byte, 10), 0); err != nil || st.takeReads() != 1 {
			t.Fatal("another scope's bytes were served")
		}
		g, _ := objfile.Cache(st, "one", c).Open(ctx, "b")
		p := make([]byte, 10)
		if _, err := g.ReadAt(p, 0); err != nil || !bytes.Equal(p, repl[:10]) {
			t.Fatal("another name's bytes were served")
		}
	})
}

// TestCacheBypass: CacheBypass neither looks up nor admits, for ReadAt,
// batches and whole reads; the pin checks still run; an unknown mode
// fails before I/O.
func TestCacheBypass(t *testing.T) {
	st := newMstore()
	data := objfiletest.Data(1000, 3)
	st.put("obj", data)
	c := newCache(t, 1<<20)
	o := objfile.Pin(objfile.Cache(st, scope, c), scope)
	obs := &objfiletest.Observer{}
	bypass := objfile.WithCacheMode(objfile.WithObserver(context.Background(), obs), objfile.CacheBypass)
	f, err := o.Open(bypass, "obj")
	if err != nil {
		t.Fatal(err)
	}
	rs := []objfile.Range{{Off: 0, Buf: make([]byte, 100)}}
	for range 2 {
		if _, err := f.WithContext(bypass).ReadAt(make([]byte, 100), 0); err != nil {
			t.Fatal(err)
		}
		if err := objfile.ReadRanges(bypass, f, rs); err != nil {
			t.Fatal(err)
		}
		if _, _, err := objfile.ReadObject(bypass, o, "obj", 1000); err != nil {
			t.Fatal(err)
		}
	}
	if n := st.takeReads(); n != 6 || obs.Hits() != 0 {
		t.Fatalf("bypassed: %d reads, %d hit bytes; want 6, 0", n, obs.Hits())
	}
	// Nothing was admitted: a default read misses, then is admitted.
	if _, err := f.ReadAt(make([]byte, 100), 0); err != nil || st.takeReads() != 1 {
		t.Fatal("a bypassed read was admitted")
	}
	if _, err := f.ReadAt(make([]byte, 100), 0); err != nil || st.takeReads() != 0 {
		t.Fatal("a default read was not admitted")
	}
	// And a warm entry is not used under bypass.
	if _, err := f.WithContext(bypass).ReadAt(make([]byte, 100), 0); err != nil || st.takeReads() != 1 {
		t.Fatal("a bypassed read was served from the cache")
	}
	// Pin still checks under bypass.
	st.put("obj", objfiletest.Data(1000, 4))
	if _, err := o.Open(bypass, "obj"); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("bypassed Open of a replaced object = %v", err)
	}
	// An unknown mode: refused before any request.
	bad := objfile.WithCacheMode(context.Background(), 9)
	for what, err := range map[string]error{
		"Open":       func() error { _, err := o.Open(bad, "obj"); return err }(),
		"OpenSized":  func() error { _, err := o.OpenSized(bad, "obj", 1000); return err }(),
		"ReadAt":     func() error { _, err := f.WithContext(bad).ReadAt(make([]byte, 10), 0); return err }(),
		"ReadRanges": objfile.ReadRanges(bad, f, rs),
		"ReadObject": func() error { _, _, err := objfile.ReadObject(bad, o, "obj", 1000); return err }(),
	} {
		if !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("%s with an unknown mode = %v, want fs.ErrInvalid", what, err)
		}
	}
	if n := st.takeReads(); n != 0 {
		t.Fatalf("%d reads with an unknown mode", n)
	}
}

// TestCacheBatchErrors: a batch of hits and misses keeps each range's
// outcome at its own position, and a failed range is not admitted.
func TestCacheBatchErrors(t *testing.T) {
	ctx := context.Background()
	obj := newCounting(100000)
	boom := errors.New("boom")
	obj.fail = func(off, _ int64) error {
		if off >= 50000 && off < 60000 {
			return boom
		}
		return nil
	}
	o := objfile.Cache(sliceOpener{"obj": obj}, scope, newCache(t, 1<<20))
	f, err := o.Open(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if err := objfile.ReadRanges(ctx, f, ranges(0, 100, 20000, 100)); err != nil {
		t.Fatal(err)
	}
	obj.mu.Lock()
	obj.reqs = nil
	obj.mu.Unlock()
	rs := ranges(55000, 10, 0, 100, 90000, 10, 20000, 100, 55000, 10)
	err = objfile.ReadRanges(ctx, f, rs)
	var be *objfile.BatchError
	if !errors.As(err, &be) || len(be.Errs) != 5 || !errors.Is(be.Errs[0], boom) || be.Errs[1] != nil || be.Errs[2] != nil ||
		be.Errs[3] != nil || !errors.Is(be.Errs[4], boom) {
		t.Fatalf("mixed batch = %v; want ranges 0 and 4 failed at their positions", err)
	}
	checkFilled(t, obj, []objfile.Range{rs[1], rs[2], rs[3]})
	if got := obj.requests(); len(got) != 2 {
		t.Fatalf("requests %v; want the two misses' spans", got)
	}
	obj.fail = nil
	if err := objfile.ReadRanges(ctx, f, ranges(55000, 10)); err != nil || len(obj.requests()) != 3 {
		t.Fatalf("a failed range was admitted: %v, %d requests", err, len(obj.requests()))
	}
	// Invalid batches fail whole before any copy.
	buf := []byte("untouched!")
	if err := objfile.ReadRanges(ctx, f, []objfile.Range{{Off: 0, Buf: buf}, {Off: 99999, Buf: make([]byte, 2)}}); !errors.Is(err, fs.ErrInvalid) || string(buf) != "untouched!" {
		t.Fatalf("an invalid batch = %v, buffer %q", err, buf)
	}
	// A cancelled batch of hits and misses: the hits stand.
	cctx, cancel := context.WithCancel(ctx)
	obj.block = make(chan struct{})
	obj.started = cancel
	rs = ranges(0, 100, 70000, 10)
	err = objfile.ReadRanges(cctx, f, rs)
	obj.block, obj.started = nil, nil
	if !errors.As(err, &be) || be.Errs[0] != nil || !errors.Is(be.Errs[1], context.Canceled) {
		t.Fatalf("cancelled mixed batch = %v", err)
	}
}

// TestCacheConcurrent: readers sharing a small cache, which evicts while
// they copy, always read the right bytes (run with -race).
func TestCacheConcurrent(t *testing.T) {
	ctx := context.Background()
	st := newMstore()
	objs := map[string][]byte{}
	for i := range 4 {
		name := fmt.Sprintf("obj%d", i)
		objs[name] = objfiletest.Data(64<<10, byte(i))
		st.put(name, objs[name])
	}
	c := newCache(t, 48<<10)
	o := objfile.Limit(objfile.Cache(st, scope, c), 8)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := range 16 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(g), 1))
			for range 200 {
				name := fmt.Sprintf("obj%d", r.IntN(4))
				f, err := o.Open(ctx, name)
				if err != nil {
					errs <- err
					return
				}
				off, n := r.IntN(60<<10), 1+r.IntN(4<<10)
				if r.IntN(2) == 0 {
					p := make([]byte, n)
					if _, err := f.ReadAt(p, int64(off)); err != nil && err != io.EOF {
						errs <- err
						return
					}
					if !bytes.Equal(p[:min(n, len(objs[name])-off)], objs[name][off:min(off+n, len(objs[name]))]) {
						errs <- fmt.Errorf("%s: ReadAt read the wrong bytes", name)
						return
					}
					continue
				}
				rs := []objfile.Range{{Off: int64(off), Buf: make([]byte, min(n, len(objs[name])-off))}, {Off: int64(r.IntN(1000)), Buf: make([]byte, 100)}}
				if err := objfile.ReadRanges(ctx, f, rs); err != nil {
					errs <- err
					return
				}
				for _, rg := range rs {
					if !bytes.Equal(rg.Buf, objs[name][rg.Off:rg.Off+int64(len(rg.Buf))]) {
						errs <- fmt.Errorf("%s: a batch read the wrong bytes", name)
						return
					}
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestCacheIOReporter: Cache reports what it wraps.
func TestCacheIOReporter(t *testing.T) {
	for _, complete := range []bool{false, true} {
		st := newMstore()
		st.put("obj", []byte("x"))
		o := objfile.Cache(reporting{st, complete}, scope, newCache(t, 100))
		f, err := o.Open(context.Background(), "obj")
		if err != nil {
			t.Fatal(err)
		}
		if o.(objfile.IOReporter).ReportsIO() != complete || f.(objfile.IOReporter).ReportsIO() != complete {
			t.Fatalf("Cache over a store reporting %v reports otherwise", complete)
		}
	}
}

// TestCacheOuterBatch keeps cache decisions at the caller's batch boundary.
func TestCacheOuterBatch(t *testing.T) {
	for _, order := range []string{"Batch", "Limit", "Pin", "BatchLimitPin", "CacheLimitBatch"} {
		t.Run(order, func(t *testing.T) {
			st := newMstore()
			st.put("obj", objfiletest.Data(1000, 1))
			c := newCache(t, 1<<20)
			var o objfile.Opener = objfile.Cache(st, scope, c)
			switch order {
			case "Batch":
				o = mustBatch(t, o, objfile.BatchPolicy{})
			case "Limit":
				o = objfile.Limit(o, 1)
			case "Pin":
				o = objfile.Pin(o, scope)
			case "BatchLimitPin":
				o = mustBatch(t, objfile.Limit(objfile.Pin(o, scope), 1), objfile.BatchPolicy{})
			case "CacheLimitBatch":
				o = objfile.Cache(objfile.Limit(mustBatch(t, o, objfile.BatchPolicy{}), 1), "outer", newCache(t, 1<<20))
			}
			ctx := context.Background()
			warm, _ := objfile.Cache(st, scope, c).Open(ctx, "obj")
			if _, err := warm.ReadAt(make([]byte, 100), 0); err != nil {
				t.Fatal(err)
			}
			// A fresh sized handle must not use or populate cache, even after its first request pins it.
			f, err := o.OpenSized(ctx, "obj", 1000)
			if err != nil {
				t.Fatal(err)
			}
			obs := &objfiletest.Observer{}
			ctx = objfile.WithObserver(ctx, obs)
			rs := []objfile.Range{{Off: 0, Buf: make([]byte, 100)}, {Off: 800, Buf: make([]byte, 100)}}
			if err := objfile.ReadRanges(ctx, f, rs); err != nil {
				t.Fatal(err)
			}
			if obs.Hits() != 0 {
				t.Fatalf("unconfirmed batch hit %d bytes", obs.Hits())
			}
			// The second span of that batch must not have been admitted.
			obs.Reset()
			st.takeReads()
			if _, err := f.WithContext(ctx).ReadAt(make([]byte, 100), 800); err != nil {
				t.Fatal(err)
			}
			if obs.Hits() != 0 || st.takeReads() != 1 {
				t.Fatal("unconfirmed batch admitted its later span")
			}
			// A cached range next to a failing miss survives; only the miss is planned.
			obs.Reset()
			st.put("obj", objfiletest.Data(1000, 2))
			rs = []objfile.Range{{Off: 0, Buf: make([]byte, 100)}, {Off: 101, Buf: make([]byte, 100)}}
			err = objfile.ReadRanges(ctx, f, rs)
			var be *objfile.BatchError
			if !errors.As(err, &be) || be.Errs[0] != nil || be.Errs[1] == nil {
				t.Fatalf("mixed results: %v", err)
			}
			if obs.Hits() != 100 || len(obs.Plans()) != 1 || obs.Plans()[0] != [3]int64{1, 1, 0} {
				t.Fatalf("hits %d, plans %v", obs.Hits(), obs.Plans())
			}
		})
	}
}
