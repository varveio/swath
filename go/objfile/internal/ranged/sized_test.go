// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/objfile"
)

// store is an in-memory object for a Getter and a Stat: its data and pin,
// which a test may change, with the requests made and, per request, the pin
// it carried. A response states the total unless noTotal; cut, if set,
// limits a body.
type store struct {
	mu      sync.Mutex
	data    []byte
	pin     Pin
	noTotal bool
	cut     func(req int) int // a body's bytes before it fails, or -1
	gets    []Pin             // the pin each ranged request carried
	wholes  int
	stats   atomic.Int32
}

func (s *store) get(_ context.Context, off, n int64, pin Pin) (Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 0 {
		s.wholes++
		n = int64(len(s.data))
	} else {
		s.gets = append(s.gets, pin)
	}
	if !pin.IsZero() && pin != s.pin {
		return Response{}, objfile.ErrObjectChanged
	}
	if off+n > int64(len(s.data)) {
		n = int64(len(s.data)) - off
	}
	res := Response{Pin: s.pin, Start: off, Total: int64(len(s.data)), Length: n}
	if s.noTotal {
		res.Total = -1
	}
	var body io.Reader = bytes.NewReader(bytes.Clone(s.data[off : off+n]))
	if s.cut != nil {
		if k := s.cut(len(s.gets) + s.wholes); k >= 0 {
			body = &cut{r: body, n: k}
		}
	}
	res.Body = io.NopCloser(body)
	return res, nil
}

func (s *store) stat(context.Context) (int64, Pin, error) {
	s.stats.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.data)), s.pin, nil
}

func newStore(n int) *store {
	return &store{data: bytes.Repeat([]byte("abcdefghij"), n/10), pin: Pin{ETag: "a"}}
}

// TestSizedPinsOnStatedTotal: a sized Object pins on a response stating its
// size, with no metadata request; a stated total of another size fails
// with ErrObjectChanged and pins nothing.
func TestSizedPinsOnStatedTotal(t *testing.T) {
	s := newStore(100)
	f := NewSized("obj", 100, s.get, s.stat)
	if _, err := f.ReadAt(make([]byte, 10), 0); err != nil || f.Identity() != "a" || s.stats.Load() != 0 {
		t.Fatalf("read: %v, Identity %q, %d metadata requests", err, f.Identity(), s.stats.Load())
	}
	g := NewSized("obj", 99, s.get, s.stat)
	if _, err := g.ReadAt(make([]byte, 10), 0); !errors.Is(err, objfile.ErrObjectChanged) || g.Identity() != "" {
		t.Fatalf("read of another size: %v, Identity %q", err, g.Identity())
	}
}

// TestSizedNoTotalFallsBack: a response that states no total is followed
// by one metadata request, which must give the size and the version
// served; the Object is not pinned, and returns no bytes, before.
func TestSizedNoTotalFallsBack(t *testing.T) {
	s := newStore(100)
	s.noTotal = true
	f := NewSized("obj", 100, s.get, s.stat)
	p := make([]byte, 10)
	if _, err := f.ReadAt(p, 5); err != nil || string(p) != "fghijabcde" || s.stats.Load() != 1 {
		t.Fatalf("read: %v, %q, %d metadata requests", err, p, s.stats.Load())
	}
	if _, err := f.ReadAt(p, 0); err != nil || s.stats.Load() != 1 {
		t.Fatalf("second read: %v, %d metadata requests", err, s.stats.Load())
	}

	// Wrong size: no pin, no bytes.
	g := NewSized("obj", 90, s.get, s.stat)
	clear(p)
	if n, err := g.ReadAt(p, 0); !errors.Is(err, objfile.ErrObjectChanged) || n != 0 || g.Identity() != "" || p[0] != 0 {
		t.Fatalf("read of another size: %d, %v, Identity %q", n, err, g.Identity())
	}

	// The object replaced between the read and the metadata request.
	h := NewSized("obj", 100, func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
		res, err := s.get(ctx, off, n, pin)
		s.mu.Lock()
		s.pin = Pin{ETag: "b"}
		s.mu.Unlock()
		return res, err
	}, s.stat)
	if _, err := h.ReadAt(p, 0); !errors.Is(err, objfile.ErrObjectChanged) || h.Identity() != "" {
		t.Fatalf("read across a replacement: %v, Identity %q", err, h.Identity())
	}

	// No metadata request to fall back on: the read fails.
	s.pin = Pin{ETag: "a"}
	if _, err := New("obj", 100, Pin{}, s.get).ReadAt(p, 0); err == nil {
		t.Fatal("an unpinned Object without Stat trusted a response with no total")
	}
}

// TestSizedConcurrentFirstReads: of concurrent first responses one pins
// and the others are checked against it; a metadata request is made once.
func TestSizedConcurrentFirstReads(t *testing.T) {
	s := newStore(1000)
	s.noTotal = true
	f := NewSized("obj", 1000, s.get, s.stat)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			p := make([]byte, 50)
			if _, err := f.ReadAt(p, int64(i*50)); err != nil || !bytes.Equal(p, s.data[i*50:i*50+50]) {
				t.Errorf("read %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	if n := s.stats.Load(); n != 1 {
		t.Fatalf("%d metadata requests, want 1", n)
	}
}

// TestSizedStatCancelled: a cancelled metadata request leaves the Object
// unpinned, and a later read pins it.
func TestSizedStatCancelled(t *testing.T) {
	s := newStore(100)
	s.noTotal = true
	ctx, cancel := context.WithCancel(context.Background())
	f := NewSized("obj", 100, s.get, func(ctx context.Context) (int64, Pin, error) {
		cancel()
		return 0, Pin{}, ctx.Err()
	})
	if _, err := f.WithContext(ctx).ReadAt(make([]byte, 10), 0); !errors.Is(err, context.Canceled) || f.Identity() != "" {
		t.Fatalf("cancelled read: %v, Identity %q", err, f.Identity())
	}
	f.(*file).stat = s.stat
	if _, err := f.ReadAt(make([]byte, 10), 0); err != nil || f.Identity() != "a" {
		t.Fatalf("read after a cancelled one: %v, Identity %q", err, f.Identity())
	}
}

// TestBatchRequestsCarryPin: every request of a batch on a sized Object
// after the first carries the pin the first took; a body cut short is
// resumed with that pin, from where it stopped.
func TestBatchRequestsCarryPin(t *testing.T) {
	s := newStore(100000)
	s.cut = func(req int) int {
		if req == 3 {
			return 7 // the third request's body fails after 7 bytes
		}
		return -1
	}
	f := NewSized("obj", 100000, s.get, s.stat)
	var rs []objfile.Range
	for i := range 10 {
		rs = append(rs, objfile.Range{Off: int64(i * 10000), Buf: make([]byte, 100)})
	}
	if err := f.(objfile.RangeReader).ReadRanges(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	for i, r := range rs {
		if !bytes.Equal(r.Buf, s.data[r.Off:r.Off+100]) {
			t.Fatalf("range %d holds the wrong bytes", i)
		}
	}
	if len(s.gets) != 11 || !s.gets[0].IsZero() {
		t.Fatalf("%d requests (want 10 and one resumption), the first pinned %+v", len(s.gets), s.gets[0])
	}
	for i, p := range s.gets[1:] {
		if p != s.pin {
			t.Fatalf("request %d carried %+v, want the pin %+v", i+1, p, s.pin)
		}
	}
}

// TestBatchIdentityChange: an object replaced after a batch's first request
// fails the ranges of the requests that find the new version with
// ErrObjectChanged, and only those.
func TestBatchIdentityChange(t *testing.T) {
	s := newStore(100000)
	var once sync.Once
	f := NewSized("obj", 100000, func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
		res, err := s.get(ctx, off, n, pin)
		once.Do(func() { s.mu.Lock(); s.pin = Pin{ETag: "b"}; s.mu.Unlock() })
		return res, err
	}, s.stat)
	rs := []objfile.Range{{Off: 0, Buf: make([]byte, 100)}, {Off: 50000, Buf: make([]byte, 100)}}
	err := f.(objfile.RangeReader).ReadRanges(context.Background(), rs)
	var be *objfile.BatchError
	if !errors.As(err, &be) || be.Errs[0] != nil || !errors.Is(be.Errs[1], objfile.ErrObjectChanged) {
		t.Fatalf("batch across a replacement: %v", err)
	}
	if !bytes.Equal(rs[0].Buf, s.data[:100]) {
		t.Fatal("the first range holds the wrong bytes")
	}
}

// TestReadObject: one request without a range; a limit below the size
// fails with ErrTooLarge and the identity, without reading the body; a
// body cut short is resumed as a pinned ranged read.
func TestReadObject(t *testing.T) {
	s := newStore(100)
	b, id, err := ReadObject(context.Background(), "obj", 100, s.get)
	if err != nil || id != "a" || !bytes.Equal(b, s.data) || s.wholes != 1 || len(s.gets) != 0 || s.stats.Load() != 0 {
		t.Fatalf("ReadObject = %d bytes, %q, %v after %d whole, %d ranged, %d metadata requests",
			len(b), id, err, s.wholes, len(s.gets), s.stats.Load())
	}
	b, id, err = ReadObject(context.Background(), "obj", 99, s.get)
	if !errors.Is(err, objfile.ErrTooLarge) || id != "a" || b != nil {
		t.Fatalf("ReadObject over the limit = %d bytes, %q, %v", len(b), id, err)
	}

	s.cut = func(req int) int {
		if req == 1 {
			return 30
		}
		return -1
	}
	s.wholes = 0
	b, _, err = ReadObject(context.Background(), "obj", 100, s.get)
	if err != nil || !bytes.Equal(b, s.data) || s.wholes != 1 || len(s.gets) != 1 || s.gets[0] != s.pin {
		t.Fatalf("ReadObject with a cut body = %v; %d whole, ranged %v", err, s.wholes, s.gets)
	}

	// No length stated: read up to one byte past the limit.
	get := func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
		res, err := s.get(ctx, off, n, pin)
		res.Length, res.Total = -1, -1
		return res, err
	}
	s.cut = nil
	if b, _, err := ReadObject(context.Background(), "obj", 100, get); err != nil || !bytes.Equal(b, s.data) {
		t.Fatalf("ReadObject of no stated length = %v", err)
	}
	if _, _, err := ReadObject(context.Background(), "obj", 50, get); !errors.Is(err, objfile.ErrTooLarge) {
		t.Fatalf("ReadObject of no stated length over the limit = %v", err)
	}

	missing := func(context.Context, int64, int64, Pin) (Response, error) { return Response{}, fs.ErrNotExist }
	if _, _, err := ReadObject(context.Background(), "obj", 100, missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadObject of a missing object = %v", err)
	}
}

// TestStated: an unknown total ("*") still states the start.
func TestStated(t *testing.T) {
	for _, c := range []struct {
		v            string
		start, total int64
	}{{"bytes 5-9/10", 5, 10}, {"bytes 5-9/*", 5, -1}, {"bytes x-9/*", -1, -1}, {"", -1, -1}} {
		if start, total := Stated(c.v); start != c.start || total != c.total {
			t.Errorf("Stated(%q) = %d, %d; want %d, %d", c.v, start, total, c.start, c.total)
		}
	}
}
