// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	if _, err := g.ReadAt(make([]byte, 10), 0); !isMismatch(err, 99, 100) || g.Identity() != "" {
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
	if n, err := g.ReadAt(p, 0); !isMismatch(err, 90, 100) || n != 0 || g.Identity() != "" || p[0] != 0 {
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

// tracked is a body that counts itself open in *open until closed.
type tracked struct {
	io.Reader
	open *atomic.Int32
	once sync.Once
}

func (b *tracked) Close() error { b.once.Do(func() { b.open.Add(-1) }); return nil }

// TestNoBodyHeldAcrossRequests: no response body is open while another
// request is made: not the metadata request after a response without a
// total, nor the ranged request for the rest of a whole-object read.
func TestNoBodyHeldAcrossRequests(t *testing.T) {
	s := newStore(100)
	s.noTotal = true
	var open atomic.Int32
	held := func() error {
		if n := open.Load(); n != 0 {
			return errors.New("a request was made while a response body was open")
		}
		return nil
	}
	get := func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
		if err := held(); err != nil {
			return Response{}, err
		}
		res, err := s.get(ctx, off, n, pin)
		if err == nil {
			open.Add(1)
			res.Body = &tracked{Reader: res.Body, open: &open}
		}
		return res, err
	}
	stat := func(ctx context.Context) (int64, Pin, error) {
		if err := held(); err != nil {
			return 0, Pin{}, err
		}
		return s.stat(ctx)
	}
	p := make([]byte, 10)
	if _, err := NewSized("obj", 100, get, stat).ReadAt(p, 5); err != nil || string(p) != "fghijabcde" {
		t.Fatalf("sized read without a total: %v, %q", err, p)
	}
	s.mu.Lock()
	s.noTotal, s.gets, s.wholes = false, nil, 0
	s.mu.Unlock()
	var cut atomic.Bool
	s.cut = func(req int) int {
		if req == 1 { // the whole-object response
			cut.Store(true)
			return 30
		}
		return -1
	}
	if b, _, err := ReadObject(context.Background(), "obj", 100, get); err != nil || !bytes.Equal(b, s.data) || !cut.Load() || len(s.gets) != 1 {
		t.Fatalf("ReadObject with a cut body: %v (cut %v, %d ranged requests after it)", err, cut.Load(), len(s.gets))
	}
}

// within waits for ch, failing the test instead of hanging when it does not
// come.
func within[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(20 * time.Second):
		t.Fatalf("%s did not happen", what)
		panic("unreachable")
	}
}

// doneWatch is a context whose Done, once asked for, signals asked: a read
// asks for it only when it waits (for the pinning lock).
type doneWatch struct {
	context.Context
	asked chan struct{}
	once  sync.Once
}

func (c *doneWatch) Done() <-chan struct{} {
	c.once.Do(func() { close(c.asked) })
	return c.Context.Done()
}

// TestSizedWaiterCancelled: a read waiting for another to pin the Object
// gives up when its own context ends, while the other's metadata request
// is still in flight.
func TestSizedWaiterCancelled(t *testing.T) {
	s := newStore(1000)
	entered, release := make(chan struct{}), make(chan struct{})
	var gets atomic.Int32
	f := NewSized("obj", 1000, func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
		gets.Add(1)
		res, err := s.get(ctx, off, n, pin)
		if pin.IsZero() {
			res.Total = -1 // the owner's response: proved by a metadata request
		}
		return res, err
	}, func(ctx context.Context) (int64, Pin, error) {
		close(entered)
		<-release
		return s.stat(ctx)
	})
	owner := make(chan error, 1)
	go func() { _, err := f.ReadAt(make([]byte, 10), 0); owner <- err }()
	within(t, entered, "the owner's metadata request") // it holds the pinning lock
	base, cancel := context.WithCancel(context.Background())
	ctx := &doneWatch{Context: base, asked: make(chan struct{})}
	waiter := make(chan error, 1)
	go func() { _, err := f.WithContext(ctx).ReadAt(make([]byte, 10), 500); waiter <- err }()
	within(t, ctx.asked, "the waiter's wait for the lock")
	cancel()
	if err := within(t, waiter, "the cancelled waiter's return"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter: %v", err)
	}
	if n := gets.Load(); n != 1 {
		t.Fatalf("%d requests; the waiter sent one while unpinned", n)
	}
	close(release)
	if err := within(t, owner, "the owner's return"); err != nil || f.Identity() != "a" {
		t.Fatalf("owner: %v, Identity %q", err, f.Identity())
	}
}

// TestReadObjectLimitMax: an unknown-length object read under the largest
// limit is read, not cut to nothing.
func TestReadObjectLimitMax(t *testing.T) {
	s := newStore(100)
	get := func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
		res, err := s.get(ctx, off, n, pin)
		res.Length, res.Total = -1, -1
		return res, err
	}
	if b, _, err := ReadObject(context.Background(), "obj", math.MaxInt64, get); err != nil || !bytes.Equal(b, s.data) {
		t.Fatalf("ReadObject under the largest limit = %d bytes, %v", len(b), err)
	}
}

// isMismatch reports whether err is a *SizeMismatchError of expected and
// actual, and not ErrObjectChanged.
func isMismatch(err error, expected, actual int64) bool {
	var sm *objfile.SizeMismatchError
	return errors.As(err, &sm) && sm.Expected == expected && sm.Actual == actual && !errors.Is(err, objfile.ErrObjectChanged)
}

// TestSizedClassification: a first read's evidence of another size is a
// SizeMismatchError, an unsatisfiable range is checked by a metadata
// request, and a size other than a pinned file's is ErrObjectChanged.
func TestSizedClassification(t *testing.T) {
	s := newStore(100)
	unsatisfiable := func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
		if off+n > int64(len(s.data)) {
			return Response{}, fmt.Errorf("%w: 416", ErrUnsatisfiable)
		}
		return s.get(ctx, off, n, pin)
	}
	f := NewSized("obj", 200, unsatisfiable, s.stat)
	if _, err := f.ReadAt(make([]byte, 10), 150); !isMismatch(err, 200, 100) || s.stats.Load() != 1 {
		t.Fatalf("unsatisfiable first read = %v after %d metadata requests; want a mismatch after 1", err, s.stats.Load())
	}
	// A 416 of an object of the size given is not a mismatch: the
	// store's error stands.
	g := NewSized("obj", 100, func(context.Context, int64, int64, Pin) (Response, error) {
		return Response{}, fmt.Errorf("%w: 416", ErrUnsatisfiable)
	}, s.stat)
	if _, err := g.ReadAt(make([]byte, 10), 0); !errors.Is(err, ErrUnsatisfiable) || isMismatch(err, 100, 100) {
		t.Fatalf("416 of an object of the size given = %v", err)
	}
	// Pinned, then replaced by an object of another size.
	h := NewSized("obj", 100, s.get, s.stat)
	if _, err := h.ReadAt(make([]byte, 10), 0); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.data = append(s.data, 'x')
	s.mu.Unlock()
	if _, err := h.ReadAt(make([]byte, 10), 0); !errors.Is(err, objfile.ErrObjectChanged) || isMismatch(err, 100, 101) {
		t.Fatalf("read of a pinned file, now of another size = %v; want ErrObjectChanged", err)
	}
}

// TestConcurrentFirstIdentities: concurrent first reads served two
// versions: one pins, and the reads of the other fail with ErrObjectChanged
// and return no bytes.
func TestConcurrentFirstIdentities(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 100)
	var n atomic.Int32
	start := make(chan struct{})
	get := func(_ context.Context, off, l int64, pin Pin) (Response, error) {
		served := Pin{ETag: "a"}
		if n.Add(1)%2 == 0 {
			served = Pin{ETag: "b"}
		}
		<-start
		return Response{Body: io.NopCloser(bytes.NewReader(data[off : off+l])), Pin: served, Start: off, Total: 1000, Length: l}, nil
	}
	f := NewSized("obj", 1000, get, nil)
	var wg sync.WaitGroup
	var ok, changed atomic.Int32
	for i := range 8 {
		wg.Go(func() {
			p := make([]byte, 10)
			_, err := f.ReadAt(p, int64(i*100))
			switch {
			case err == nil && bytes.Equal(p, data[i*100:i*100+10]):
				ok.Add(1)
			case errors.Is(err, objfile.ErrObjectChanged) && p[0] == 0:
				changed.Add(1)
			default:
				t.Errorf("read %d: %v", i, err)
			}
		})
	}
	close(start)
	wg.Wait()
	if ok.Load() != 4 || changed.Load() != 4 {
		t.Fatalf("%d reads served, %d changed; want 4 of each", ok.Load(), changed.Load())
	}
}

// TestOneConnectionInterleaving forces the interleaving that deadlocked on
// one connection per host: read A's response states no total, so A needs a
// metadata request; read B, also first, is started and has reached the
// point where it would send its request (or has sent it) before A's
// metadata request asks for the connection. Each request takes the one
// connection, a response holding it until its body is closed. B must not
// hold the connection while A proves the size.
func TestOneConnectionInterleaving(t *testing.T) {
	s := newStore(1000)
	conn := make(chan struct{}, 1)                                           // the one connection
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // a deadlock fails, not hangs
	defer cancel()
	take := func(ctx context.Context) error {
		select {
		case conn <- struct{}{}:
			return nil
		case <-ctx.Done():
			return errors.New("no connection: deadlock")
		}
	}
	bReady := make(chan struct{})
	var bOnce sync.Once
	markB := func() { bOnce.Do(func() { close(bReady) }) }
	get := func(rctx context.Context, off, n int64, pin Pin) (Response, error) {
		if off == 500 {
			markB() // B sends a request: it takes the connection
		}
		if err := take(rctx); err != nil {
			return Response{}, err
		}
		res, err := s.get(rctx, off, n, pin)
		if err != nil {
			<-conn
			return res, err
		}
		if off == 0 && pin.IsZero() {
			res.Total = -1 // A's response: a metadata request must prove it
		}
		res.Body = &releasing{ReadCloser: res.Body, conn: conn}
		return res, nil
	}
	aInStat := make(chan struct{})
	stat := func(sctx context.Context) (int64, Pin, error) {
		close(aInStat) // A proves the size; only then does B start
		select {       // B has started, waiting for the pin or holding a response
		case <-bReady:
		case <-sctx.Done():
			return 0, Pin{}, sctx.Err()
		}
		if err := take(sctx); err != nil {
			return 0, Pin{}, err
		}
		defer func() { <-conn }()
		return s.stat(sctx)
	}
	f := NewSized("obj", 1000, get, stat)
	a := make(chan error, 1)
	go func() { _, err := f.WithContext(ctx).ReadAt(make([]byte, 10), 0); a <- err }()
	within(t, aInStat, "read A's metadata request")
	bctx := &doneWatch{Context: ctx, asked: make(chan struct{})}
	go func() {
		<-bctx.asked // B waits for the pin without a request
		markB()
	}()
	b := make(chan error, 1)
	go func() { _, err := f.WithContext(bctx).ReadAt(make([]byte, 10), 500); b <- err }()
	if err := within(t, a, "read A"); err != nil {
		t.Fatalf("read A: %v", err)
	}
	if err := within(t, b, "read B"); err != nil {
		t.Fatalf("read B: %v", err)
	}
}

// releasing is a body that gives back its connection when closed.
type releasing struct {
	io.ReadCloser
	conn chan struct{}
	once sync.Once
}

func (r *releasing) Close() error {
	r.once.Do(func() { <-r.conn })
	return r.ReadCloser.Close()
}
