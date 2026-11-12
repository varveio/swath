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
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/objfile"
)

// span is one request a batch made: bytes [off, off+n).
type span struct{ off, n int64 }

// countingObject is an Object over data that records each ReadAt as a
// request: its range, how many were in flight at once, and how many are in
// flight now. With pinFirst, its Identity is "" until a read succeeds. With
// barrier above 1, each read waits until that many are in flight (or all
// reads of the batch have started), so that a test sees concurrency without
// timing. fail, if set, fails a request before it reads.
type countingObject struct {
	data     []byte
	pinFirst bool
	barrier  int
	fail     func(off, n int64) error
	block    chan struct{} // if set, a read waits for it to close or its context
	started  func()        // if set, called as each read starts

	mu       sync.Mutex
	reqs     []span
	inFlight int
	maxIn    int
	maxBytes int64
	inBytes  int64
	pinned   bool
	arrived  chan struct{} // closed when barrier reads are in flight
	expected int           // reads the barrier waits for at most
}

func newCounting(n int) *countingObject {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i*31 + i>>8)
	}
	return &countingObject{data: data}
}

func (o *countingObject) Size() int64 { return int64(len(o.data)) }
func (o *countingObject) Close() error {
	return nil
}

func (o *countingObject) Identity() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.pinFirst && !o.pinned {
		return ""
	}
	return "v1"
}

func (o *countingObject) ReadAt(p []byte, off int64) (int, error) {
	return o.read(context.Background(), p, off)
}

func (o *countingObject) WithContext(ctx context.Context) io.ReaderAt {
	return readerFunc(func(p []byte, off int64) (int, error) { return o.read(ctx, p, off) })
}

func (o *countingObject) read(ctx context.Context, p []byte, off int64) (int, error) {
	o.mu.Lock()
	o.reqs = append(o.reqs, span{off, int64(len(p))})
	o.inFlight++
	o.inBytes += int64(len(p))
	o.maxIn = max(o.maxIn, o.inFlight)
	o.maxBytes = max(o.maxBytes, o.inBytes)
	arrived := o.arrived
	if o.pinFirst && !o.pinned {
		arrived = nil // the read that pins is alone: it waits for no others
	} else if o.barrier > 1 && arrived != nil && (o.inFlight >= o.barrier || len(o.reqs) >= o.expected) {
		close(arrived)
		o.arrived = nil
	}
	started := o.started
	o.mu.Unlock()
	if started != nil {
		started()
	}
	defer func() {
		o.mu.Lock()
		o.inFlight--
		o.inBytes -= int64(len(p))
		o.mu.Unlock()
	}()
	if o.barrier > 1 && arrived != nil {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			return 0, errors.New("the barrier was never reached")
		}
	}
	if o.block != nil {
		select {
		case <-o.block:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if o.fail != nil {
		if err := o.fail(off, int64(len(p))); err != nil {
			return 0, err
		}
	}
	if off >= int64(len(o.data)) {
		return 0, io.EOF
	}
	n := copy(p, o.data[off:])
	o.mu.Lock()
	o.pinned = true
	o.mu.Unlock()
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// expect makes the barrier wait for at most n reads.
func (o *countingObject) expect(barrier, n int) {
	o.barrier, o.expected, o.arrived = barrier, n, make(chan struct{})
}

func (o *countingObject) requests() []span {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := slices.Clone(o.reqs)
	slices.SortFunc(out, func(a, b span) int { return int(a.off - b.off) })
	return out
}

// ranges returns ranges of o for (off, n) pairs, with fresh buffers.
func ranges(pairs ...int64) []objfile.Range {
	var out []objfile.Range
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, objfile.Range{Off: pairs[i], Buf: make([]byte, pairs[i+1])})
	}
	return out
}

// checkFilled fails unless every range holds o's bytes.
func checkFilled(t *testing.T, o *countingObject, rs []objfile.Range) {
	t.Helper()
	for i, r := range rs {
		if !bytes.Equal(r.Buf, o.data[r.Off:r.Off+int64(len(r.Buf))]) {
			t.Fatalf("range %d (%d bytes at %d) holds the wrong bytes", i, len(r.Buf), r.Off)
		}
	}
}

// policy is a small BatchPolicy for tests: requests of at most 1000 bytes,
// gaps of at most 100, over-read at most half.
func policy() objfile.BatchPolicy {
	return objfile.BatchPolicy{MaxGap: 100, MaxRequest: 1000, MaxOverRead: 0.5, MaxConcurrent: 4, MaxInFlightBytes: 1 << 20}
}

// TestBatchMerge: the requests a batch makes under each bound of the policy.
func TestBatchMerge(t *testing.T) {
	for _, c := range []struct {
		name   string
		ranges []objfile.Range
		want   []span
	}{
		{"one range is one request", ranges(10, 50), []span{{10, 50}}},
		{"adjacent ranges in any order join", ranges(200, 100, 0, 100, 100, 100), []span{{0, 300}}},
		{"overlapping and repeated ranges join", ranges(0, 100, 50, 100, 50, 100, 120, 10), []span{{0, 150}}},
		{"a gap within the bound is bridged", ranges(0, 200, 300, 200), []span{{0, 500}}},
		{"a gap beyond the bound is not", ranges(0, 200, 301, 200), []span{{0, 200}, {301, 200}}},
		{"over-read beyond the ratio is not bridged", ranges(0, 50, 150, 50), []span{{0, 50}, {150, 50}}},
		{"over-read within the ratio is", ranges(0, 100, 200, 100), []span{{0, 300}}},
		{"a join past the request size is not made", ranges(0, 600, 650, 400), []span{{0, 600}, {650, 400}}},
		{"a long range is split evenly", ranges(0, 2500), []span{{0, 834}, {834, 833}, {1667, 833}}},
		{"empty ranges make no request", ranges(5, 0, 100, 0), nil},
		{"a range at the end, empty, is valid", ranges(4096, 0), nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newCounting(4096)
			if err := objfile.ReadRangesWith(context.Background(), o, c.ranges, policy()); err != nil {
				t.Fatal(err)
			}
			checkFilled(t, o, c.ranges)
			if got := o.requests(); !slices.Equal(got, c.want) {
				t.Fatalf("requests %v, want %v", got, c.want)
			}
		})
	}
}

// TestBatchDirect: a request that is exactly one range reads into its
// buffer; any other reads into its own and copies out.
func TestBatchDirect(t *testing.T) {
	o := newCounting(4096)
	var bufs [][]byte
	r := readerFunc(func(p []byte, off int64) (int, error) {
		bufs = append(bufs, p)
		return o.ReadAt(p, off)
	})
	obj := &wrapObject{Object: o, r: r}
	rs := ranges(0, 100, 3000, 200, 3200, 50)
	if err := objfile.ReadRangesWith(context.Background(), obj, rs, objfile.BatchPolicy{MaxRequest: 1000, MaxConcurrent: 1}); err != nil {
		t.Fatal(err)
	}
	checkFilled(t, o, rs)
	if len(bufs) != 2 || &bufs[0][0] != &rs[0].Buf[0] || &bufs[1][0] == &rs[1].Buf[0] {
		t.Fatalf("buffers read into: %d requests; the first should be range 0's own, the second not", len(bufs))
	}
}

// wrapObject reads an Object through r.
type wrapObject struct {
	objfile.Object
	r io.ReaderAt
}

func (w *wrapObject) WithContext(context.Context) io.ReaderAt { return w.r }

// TestBatchMatchesReadAt: random batches read the bytes ReadAt reads, under
// the default policy and a small one, and every request keeps the policy's
// bounds.
func TestBatchMatchesReadAt(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, p := range []objfile.BatchPolicy{objfile.DefaultBatchPolicy(), policy(), {MaxRequest: 1}} {
		for iter := range 200 {
			size := 1 + rng.IntN(1<<14)
			if p.MaxRequest == 1 {
				size = 1 + rng.IntN(256) // a request a byte
			}
			o := newCounting(size)
			n := rng.IntN(40)
			rs := make([]objfile.Range, n)
			requested := 0
			for i := range rs {
				off := rng.IntN(size + 1)
				l := rng.IntN(min(size-off, 1+rng.IntN(3000)) + 1)
				rs[i] = objfile.Range{Off: int64(off), Buf: make([]byte, l)}
				requested += l
			}
			if err := objfile.ReadRangesWith(context.Background(), o, rs, p); err != nil {
				t.Fatalf("iteration %d: %v", iter, err)
			}
			reqs := o.requests()
			for i, r := range rs {
				want := make([]byte, len(r.Buf))
				if _, err := o.ReadAt(want, r.Off); err != nil && err != io.EOF {
					t.Fatal(err)
				}
				if !bytes.Equal(r.Buf, want) {
					t.Fatalf("iteration %d: range %d differs from ReadAt", iter, i)
				}
			}
			norm := p
			norm.MaxRequest = max(norm.MaxRequest, 1)
			var prevEnd int64 = -1
			for _, s := range reqs {
				if s.n > norm.MaxRequest {
					t.Fatalf("iteration %d: request of %d bytes, max %d", iter, s.n, norm.MaxRequest)
				}
				if s.off < prevEnd {
					t.Fatalf("iteration %d: requests overlap at %d", iter, s.off)
				}
				prevEnd = s.off + s.n
			}
		}
	}
}

// TestBatchConcurrency: scattered ranges are read concurrently, at most
// MaxConcurrent at once and within MaxInFlightBytes.
func TestBatchConcurrency(t *testing.T) {
	scattered := func() []objfile.Range {
		var rs []objfile.Range
		for i := range 20 {
			rs = append(rs, objfile.Range{Off: int64(i * 1000), Buf: make([]byte, 100)})
		}
		return rs
	}
	t.Run("MaxConcurrent", func(t *testing.T) {
		o := newCounting(20000)
		o.expect(4, 20)
		rs := scattered()
		if err := objfile.ReadRangesWith(context.Background(), o, rs, policy()); err != nil {
			t.Fatal(err)
		}
		checkFilled(t, o, rs)
		if len(o.reqs) != 20 || o.maxIn != 4 {
			t.Fatalf("%d requests, %d at most in flight; want 20 and 4", len(o.reqs), o.maxIn)
		}
	})
	t.Run("MaxInFlightBytes", func(t *testing.T) {
		o := newCounting(20000)
		o.expect(2, 20)
		p := policy()
		p.MaxInFlightBytes = 250
		p.MaxRequest = 100
		rs := scattered()
		if err := objfile.ReadRangesWith(context.Background(), o, rs, p); err != nil {
			t.Fatal(err)
		}
		checkFilled(t, o, rs)
		if o.maxIn != 2 || o.maxBytes > 250 {
			t.Fatalf("%d in flight and %d bytes at most; want 2 and at most 250", o.maxIn, o.maxBytes)
		}
	})
	t.Run("FirstAloneWhileUnpinned", func(t *testing.T) {
		o := newCounting(20000)
		o.pinFirst = true
		var first atomic.Int32
		o.fail = func(off, n int64) error {
			if first.Add(1) == 1 && o.inFlightNow() != 1 {
				return errors.New("the first request was not alone")
			}
			return nil
		}
		o.expect(4, 20)
		rs := scattered()
		if err := objfile.ReadRangesWith(context.Background(), o, rs, policy()); err != nil {
			t.Fatal(err)
		}
		if o.maxIn != 4 {
			t.Fatalf("%d in flight at most after the first; want 4", o.maxIn)
		}
	})
}

func (o *countingObject) inFlightNow() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.inFlight
}

// TestBatchPartialFailure: a failed request fails exactly the ranges it
// covers; the others are filled, and errors.Is sees the failure.
func TestBatchPartialFailure(t *testing.T) {
	o := newCounting(20000)
	boom := errors.New("boom")
	o.fail = func(off, n int64) error {
		if off <= 5000 && 5000 < off+n {
			return boom
		}
		return nil
	}
	// Ranges 1 and 2 overlap in one request; range 3 is split, and only
	// its second half's request fails.
	rs := ranges(0, 100, 4900, 150, 5000, 10, 3500, 1600)
	err := objfile.ReadRangesWith(context.Background(), o, rs, objfile.BatchPolicy{MaxRequest: 800, MaxConcurrent: 2})
	var be *objfile.BatchError
	if !errors.As(err, &be) || !errors.Is(err, boom) {
		t.Fatalf("ReadRanges = %v, want a BatchError of boom", err)
	}
	for i, want := range []bool{false, true, true, true} {
		if (be.Errs[i] != nil) != want {
			t.Fatalf("range %d: error %v, want failed=%v", i, be.Errs[i], want)
		}
	}
	checkFilled(t, o, rs[:1])
}

// TestBatchInvalid: a range outside the object, or one whose end
// overflows, fails the batch before any request.
func TestBatchInvalid(t *testing.T) {
	for _, rs := range [][]objfile.Range{
		ranges(-1, 10),
		ranges(0, 10, 4090, 7),
		ranges(4097, 0),
		{{Off: math.MaxInt64 - 2, Buf: make([]byte, 5)}},
	} {
		o := newCounting(4096)
		err := objfile.ReadRangesWith(context.Background(), o, rs, policy())
		if !errors.Is(err, fs.ErrInvalid) || len(o.reqs) != 0 {
			t.Fatalf("ReadRanges(%d ranges) = %v after %d requests; want fs.ErrInvalid and none", len(rs), err, len(o.reqs))
		}
	}
	o := newCounting(4096)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := objfile.ReadRangesWith(ctx, o, ranges(0, 10), policy()); !errors.Is(err, context.Canceled) || len(o.reqs) != 0 {
		t.Fatalf("ReadRanges with a done context = %v after %d requests", err, len(o.reqs))
	}
}

// TestBatchCancel: cancelling a batch cancels its requests in flight, sends
// no more, and returns once they have ended (none is left in flight); ranges
// it never read fail with the cancellation.
func TestBatchCancel(t *testing.T) {
	o := newCounting(20000)
	o.block = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	var rs []objfile.Range
	for i := range 20 {
		rs = append(rs, objfile.Range{Off: int64(i * 1000), Buf: make([]byte, 100)})
	}
	started := make(chan struct{})
	var n atomic.Int32
	o.started = func() {
		if n.Add(1) == 4 {
			close(started)
		}
	}
	done := make(chan error, 1)
	go func() { done <- objfile.ReadRangesWith(ctx, o, rs, policy()) }()
	waitFor(t, started, "the batch's requests") // MaxConcurrent are in flight, blocked
	cancel()
	err := waitFor(t, done, "the cancelled batch's return")
	var be *objfile.BatchError
	if !errors.As(err, &be) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ReadRanges = %v", err)
	}
	for i, e := range be.Errs {
		if e == nil {
			t.Fatalf("range %d reported filled", i)
		}
	}
	if n, in := len(o.reqs), o.inFlightNow(); n != 4 || in != 0 {
		t.Fatalf("%d requests sent, %d still in flight; want 4 and 0", n, in)
	}
}

// sliceOpener opens countingObjects by name.
type sliceOpener map[string]*countingObject

// Open returns the object pinned (an Identity at once); OpenSized returns
// it unpinned until its first read, as a store's OpenSized does.
func (s sliceOpener) Open(_ context.Context, name string) (objfile.Object, error) {
	o, ok := s[name]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	o.pinFirst = false
	return o, nil
}

func (s sliceOpener) OpenSized(ctx context.Context, name string, _ int64) (objfile.Object, error) {
	f, err := s.Open(ctx, name)
	if err == nil {
		s[name].pinFirst = true
	}
	return f, err
}

// TestLimitBatch: through Limit, each request of a batch holds a slot, so a
// batch never has more requests in flight than the limit, and a batch holds
// no slot once it returns, cancelled or not. Batch's policy reaches through
// Limit, whichever wraps the other.
func TestLimitBatch(t *testing.T) {
	ctx := context.Background()
	o := newCounting(20000)
	o.expect(3, 20)
	l := objfile.Limit(sliceOpener{"obj": o}, 3)
	f, err := l.Open(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	var rs []objfile.Range
	for i := range 20 {
		rs = append(rs, objfile.Range{Off: int64(i * 1000), Buf: make([]byte, 100)})
	}
	if err := objfile.ReadRanges(ctx, f, rs); err != nil {
		t.Fatal(err)
	}
	checkFilled(t, o, rs)
	if len(o.reqs) != 20 || o.maxIn != 3 {
		t.Fatalf("%d requests, %d at most in flight; want 20 and 3", len(o.reqs), o.maxIn)
	}

	// Cancelled mid-batch: every slot is free again.
	o2 := newCounting(20000)
	o2.block = make(chan struct{})
	l = objfile.Limit(sliceOpener{"obj": o2}, 3)
	f, _ = l.Open(ctx, "obj")
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- objfile.ReadRanges(cctx, f, rs) }()
	for o2.inFlightNow() < 3 {
		runtime.Gosched()
	}
	cancel()
	if err := waitFor(t, done, "the cancelled batch's return"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled batch: %v", err)
	}
	// Three requests at once fit only if no slot is still held.
	o2.block = nil
	o2.expect(3, 3)
	rs = ranges(0, 100, 5000, 100, 10000, 100)
	if err := objfile.ReadRanges(ctx, f, rs); err != nil {
		t.Fatal(err)
	}
	if o2.maxIn != 3 {
		t.Fatalf("%d requests at most in flight after a cancelled batch; want 3", o2.maxIn)
	}
}

func mustBatch(t *testing.T, o objfile.Opener, p objfile.BatchPolicy) objfile.Opener {
	t.Helper()
	b, err := objfile.Batch(o, p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// waitFor waits for ch, failing the test instead of hanging.
func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(20 * time.Second):
		t.Fatalf("%s did not happen", what)
		panic("unreachable")
	}
}

// TestComposition: Batch, Limit, Pin, Root and KnownSizes in the orders
// callers stack them (varve: Pin or Restore over Limit over a store whose
// Root is inside it) keep batching under Batch's policy, the first read's
// verification (alone, then the rest at once up to the limit), one slot
// per request (a limit of one never deadlocks against the batch's own
// requests) and cancellation; opened plainly (pinned at open) and sized
// (pinned by the first read).
func TestComposition(t *testing.T) {
	ctx := context.Background()
	small := objfile.BatchPolicy{MaxRequest: 10, MaxConcurrent: 4}
	for _, limit := range []int{1, 3} {
		for _, c := range []struct {
			name  string
			stack func(t *testing.T, o objfile.Opener) objfile.Opener
		}{
			{"Pin(Limit(Batch))", func(t *testing.T, o objfile.Opener) objfile.Opener {
				return objfile.Pin(objfile.Limit(mustBatch(t, o, small), limit), scope)
			}},
			{"Pin(Root(Limit(Batch)))", func(t *testing.T, o objfile.Opener) objfile.Opener {
				return objfile.Pin(objfile.Root(objfile.Limit(mustBatch(t, o, small), limit), ""), scope)
			}},
			{"KnownSizes(Pin(Limit(Batch)))", func(t *testing.T, o objfile.Opener) objfile.Opener {
				return objfile.KnownSizes(objfile.Pin(objfile.Limit(mustBatch(t, o, small), limit), scope), map[string]int64{"other": 1})
			}},
			{"Batch(Pin(Limit))", func(t *testing.T, o objfile.Opener) objfile.Opener {
				return mustBatch(t, objfile.Pin(objfile.Limit(o, limit), scope), small)
			}},
			{"Batch(Limit)", func(t *testing.T, o objfile.Opener) objfile.Opener {
				return mustBatch(t, objfile.Limit(o, limit), small)
			}},
			{"Limit(Batch)", func(t *testing.T, o objfile.Opener) objfile.Opener {
				return objfile.Limit(mustBatch(t, o, small), limit)
			}},
		} {
			for _, sized := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/limit=%d/sized=%v", c.name, limit, sized), func(t *testing.T) {
					o := newCounting(4096)
					stack := c.stack(t, sliceOpener{"obj": o})
					var f objfile.Object
					var err error
					if sized {
						f, err = stack.OpenSized(ctx, "obj", 4096)
					} else {
						f, err = stack.Open(ctx, "obj")
					}
					if err != nil {
						t.Fatal(err)
					}
					if _, ok := f.(objfile.RangeReader); !ok {
						t.Fatal("the Object is not a RangeReader")
					}
					var first atomic.Int32
					o.fail = func(int64, int64) error {
						if first.Add(1) == 1 && sized && o.inFlightNow() != 1 {
							return errors.New("the first request of an unpinned Object was not alone")
						}
						return nil
					}
					o.expect(limit, 10)                                      // the requests after the first wait until limit are in flight
					rctx, cancel := context.WithTimeout(ctx, 20*time.Second) // a deadlock fails, not hangs
					defer cancel()
					rs := ranges(0, 100)
					if err := objfile.ReadRanges(rctx, f, rs); err != nil {
						t.Fatal(err)
					}
					checkFilled(t, o, rs)
					if len(o.reqs) != 10 || o.maxIn != limit {
						t.Fatalf("%d requests, %d in flight at most; want 10 under the policy, %d under the limit", len(o.reqs), o.maxIn, limit)
					}

					// Cancelled while its requests wait: it returns, and no
					// slot is left held (another batch goes through).
					o.block = make(chan struct{})
					started := make(chan struct{})
					var once sync.Once
					o.started = func() { once.Do(func() { close(started) }) }
					cctx, ccancel := context.WithCancel(ctx)
					done := make(chan error, 1)
					go func() { done <- objfile.ReadRanges(cctx, f, ranges(1000, 100)) }()
					waitFor(t, started, "a request of the batch")
					ccancel()
					if err := waitFor(t, done, "the cancelled batch's return"); !errors.Is(err, context.Canceled) {
						t.Fatalf("cancelled batch: %v", err)
					}
					o.block, o.started = nil, nil
					o.expect(limit, 10)
					if err := objfile.ReadRanges(rctx, f, ranges(2000, 100)); err != nil {
						t.Fatalf("a batch after a cancelled one: %v", err)
					}
				})
			}
		}
	}
}

// TestCompositionRestoredMismatch: a restored pin whose object has been
// replaced fails a sized batch at its first request, through Pin, Limit and
// Batch: the rest of the batch is never sent.
func TestCompositionRestoredMismatch(t *testing.T) {
	ctx := context.Background()
	o := newCounting(4096)
	snap := []byte(`{"version":1,"scope":"` + scope + `","objects":{"obj":{"size":4096,"identity":"v0"}}}`)
	p, err := objfile.Restore(objfile.Limit(mustBatch(t, sliceOpener{"obj": o}, objfile.BatchPolicy{MaxRequest: 10}), 3), scope, snap)
	if err != nil {
		t.Fatal(err)
	}
	f, err := p.OpenSized(ctx, "obj", 4096)
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	err = objfile.ReadRanges(rctx, f, ranges(0, 100))
	var be *objfile.BatchError
	if !errors.Is(err, objfile.ErrObjectChanged) || !errors.As(err, &be) || len(be.Unwrap()) != 1 || len(o.reqs) != 1 {
		t.Fatalf("batch of a replaced pinned object = %v after %d requests; want ErrObjectChanged after 1", err, len(o.reqs))
	}
}

func TestBatchErrorText(t *testing.T) {
	err := &objfile.BatchError{Errs: []error{nil, io.ErrUnexpectedEOF, io.ErrUnexpectedEOF}}
	if got := err.Error(); got != fmt.Sprintf("objfile: 2 of 3 ranges failed; range 1: %v", io.ErrUnexpectedEOF) {
		t.Fatalf("Error() = %q", got)
	}
	if u := err.Unwrap(); len(u) != 2 {
		t.Fatalf("Unwrap = %v, want the error of each failed range", u)
	}
}

// TestBatchInFlightBytesBelowRequest: a byte bound below the request size is
// kept, not raised to it: one request is let through alone, and no more
// bytes are in flight than the bound otherwise.
func TestBatchInFlightBytesBelowRequest(t *testing.T) {
	o := newCounting(20000)
	o.expect(2, 20)
	p := objfile.DefaultBatchPolicy() // 4 MiB requests
	p.MaxInFlightBytes = 250
	var rs []objfile.Range
	for i := range 20 {
		rs = append(rs, objfile.Range{Off: int64(i * 1000), Buf: make([]byte, 100)})
	}
	if err := objfile.ReadRangesWith(context.Background(), o, rs, p); err != nil {
		t.Fatal(err)
	}
	if o.maxIn != 2 || o.maxBytes > 250 {
		t.Fatalf("%d in flight and %d bytes at most; want 2 and at most 250", o.maxIn, o.maxBytes)
	}
	// Ranges over the byte bound are split to fit it; none is let through
	// over it.
	o = newCounting(20000)
	rs = ranges(0, 1000, 5000, 1000)
	if err := objfile.ReadRangesWith(context.Background(), o, rs, p); err != nil || o.maxBytes > 250 {
		t.Fatalf("ranges over the byte bound: %v, %d bytes in flight at most; want at most 250", err, o.maxBytes)
	}
	checkFilled(t, o, rs)
	for _, r := range o.requests() {
		if r.n > 250 {
			t.Fatalf("a request of %d bytes, over the in-flight bound of 250", r.n)
		}
	}
}

// TestBatchExtremeBounds: bounds at the integer limits plan correct
// requests; nothing overflows into an empty plan.
func TestBatchExtremeBounds(t *testing.T) {
	for _, p := range []objfile.BatchPolicy{
		{MaxRequest: math.MaxInt64, MaxGap: math.MaxInt64, MaxOverRead: math.MaxFloat64, MaxConcurrent: math.MaxInt, MaxInFlightBytes: math.MaxInt64},
		{MaxRequest: math.MaxInt64 - 1},
	} {
		o := newCounting(4096)
		rs := ranges(10, 2, 4000, 5)
		if err := objfile.ReadRangesWith(context.Background(), o, rs, p); err != nil {
			t.Fatal(err)
		}
		checkFilled(t, o, rs)
		if len(o.reqs) == 0 {
			t.Fatal("no request was made")
		}
	}
}

// TestBatchZeroPolicy: the zero BatchPolicy is DefaultBatchPolicy, and a
// negative bound is refused before any request, by ReadRangesWith and by
// Batch.
func TestBatchZeroPolicy(t *testing.T) {
	var got [2][]span
	for i, p := range []objfile.BatchPolicy{{}, objfile.DefaultBatchPolicy()} {
		o := newCounting(9 << 20)
		rs := ranges(0, 5<<20, 5<<20+1, 10, 6<<20, 10)
		if err := objfile.ReadRangesWith(context.Background(), o, rs, p); err != nil {
			t.Fatal(err)
		}
		checkFilled(t, o, rs)
		got[i] = o.requests()
	}
	if !slices.Equal(got[0], got[1]) || len(got[0]) != 3 {
		t.Fatalf("zero policy requests %v, default %v", got[0], got[1])
	}
	for _, p := range []objfile.BatchPolicy{{MaxGap: -1}, {MaxRequest: -1}, {MaxOverRead: -0.5}, {MaxOverRead: math.NaN()},
		{MaxConcurrent: -1}, {MaxInFlightBytes: -1}} {
		o := newCounting(100)
		if err := objfile.ReadRangesWith(context.Background(), o, ranges(0, 10), p); !errors.Is(err, fs.ErrInvalid) || len(o.reqs) != 0 {
			t.Fatalf("policy %+v: %v after %d requests; want fs.ErrInvalid before any", p, err, len(o.reqs))
		}
		if _, err := objfile.Batch(sliceOpener{}, p); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("Batch with policy %+v: %v; want fs.ErrInvalid", p, err)
		}
	}
}

// sliceError is an error that is not comparable: == on two of them panics.
type sliceError []string

func (e sliceError) Error() string { return "slice error" }

// TestBatchUncomparableErrors: errors that cannot be compared are reported,
// not compared.
func TestBatchUncomparableErrors(t *testing.T) {
	o := newCounting(20000)
	o.fail = func(off, n int64) error {
		if off >= 5000 {
			return sliceError{"x"}
		}
		return nil
	}
	err := objfile.ReadRangesWith(context.Background(), o, ranges(0, 10, 5000, 10, 10000, 10), policy())
	var be *objfile.BatchError
	if !errors.As(err, &be) || be.Errs[0] != nil || be.Errs[1] == nil || be.Errs[2] == nil {
		t.Fatalf("batch = %v", err)
	}
	var se sliceError
	if len(be.Unwrap()) != 2 || !errors.As(err, &se) || err.Error() == "" {
		t.Fatalf("Unwrap = %v", be.Unwrap())
	}
}

// shortObject is an object of size bytes (100 if 0) whose reads return 50.
type shortObject struct {
	countingObject
	size int64
}

func (o *shortObject) Size() int64 {
	if o.size == 0 {
		return 100
	}
	return o.size
}
func (o *shortObject) ReadAt(p []byte, off int64) (int, error) {
	return copy(p, make([]byte, 50)), io.EOF
}
func (o *shortObject) WithContext(context.Context) io.ReaderAt { return o }

type shortOpener struct{}

func (shortOpener) Open(context.Context, string) (objfile.Object, error) { return &shortObject{}, nil }
func (shortOpener) OpenSized(ctx context.Context, n string, _ int64) (objfile.Object, error) {
	return shortOpener{}.Open(ctx, n)
}

// TestReadObjectFallbackShort: ReadObject over an Opener that is not an
// ObjectReader fails when the object's read comes up short.
func TestReadObjectFallbackShort(t *testing.T) {
	b, _, err := objfile.ReadObject(context.Background(), shortOpener{}, "obj", 1000)
	if !errors.Is(err, io.ErrUnexpectedEOF) || b != nil {
		t.Fatalf("ReadObject of a short object = %d bytes, %v; want io.ErrUnexpectedEOF", len(b), err)
	}
	if b, _, err := objfile.ReadObject(context.Background(), negativeOpener{}, "obj", 1000); err == nil || b != nil {
		t.Fatalf("ReadObject of an object of negative size = %d bytes, %v", len(b), err)
	}
}

type negativeOpener struct{ shortOpener }

func (negativeOpener) Open(context.Context, string) (objfile.Object, error) {
	return &shortObject{size: -1}, nil
}

// TestBatchBounds: a batch over MaxBatchRanges ranges or MaxBatchBytes
// bytes, or one whose pieces at the request size are too many to plan, fails
// whole before any request; zero-length ranges, through the object's end,
// need none.
func TestBatchBounds(t *testing.T) {
	ctx := context.Background()
	o := newCounting(1 << 20)
	many := make([]objfile.Range, objfile.MaxBatchRanges+1)
	if err := objfile.ReadRangesWith(ctx, o, many, policy()); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("%d ranges: %v; want fs.ErrInvalid", len(many), err)
	}
	if err := objfile.ReadRangesWith(ctx, o, many[:objfile.MaxBatchRanges], policy()); err != nil || len(o.reqs) != 0 {
		t.Fatalf("%d empty ranges: %v after %d requests", objfile.MaxBatchRanges, err, len(o.reqs))
	}
	if err := objfile.ReadRangesWith(ctx, o, ranges(1<<20, 0), policy()); err != nil || len(o.reqs) != 0 {
		t.Fatalf("an empty range at the end: %v after %d requests", err, len(o.reqs))
	}
	// 129 ranges of the same MiB: 129 MiB requested, over MaxBatchBytes.
	big := make([]byte, 1<<20)
	var over []objfile.Range
	for range objfile.MaxBatchBytes>>20 + 1 {
		over = append(over, objfile.Range{Off: 0, Buf: big})
	}
	if err := objfile.ReadRangesWith(ctx, o, over, policy()); !errors.Is(err, fs.ErrInvalid) || len(o.reqs) != 0 {
		t.Fatalf("%d MiB: %v after %d requests; want fs.ErrInvalid before any", len(over), err, len(o.reqs))
	}
	// 4096 ranges of the same 16 KiB at one byte a request: within the
	// byte bound, but planning would be quadratic.
	var dup []objfile.Range
	buf := make([]byte, 16<<10)
	for range objfile.MaxBatchRanges {
		dup = append(dup, objfile.Range{Off: 0, Buf: buf})
	}
	if err := objfile.ReadRangesWith(ctx, o, dup, objfile.BatchPolicy{MaxRequest: 1}); !errors.Is(err, fs.ErrInvalid) || len(o.reqs) != 0 {
		t.Fatalf("a batch of %d one-byte pieces: %v; want fs.ErrInvalid", len(dup)*len(buf), err)
	}
}

// TestBatchFirstReadBarrier: while the Object is not pinned, a batch's
// requests go one at a time; a failure that leaves it unpinned does not
// let the rest go at once, and one that shows it is not the object
// expected stops the batch.
func TestBatchFirstReadBarrier(t *testing.T) {
	ctx := context.Background()
	scattered := func() []objfile.Range {
		var rs []objfile.Range
		for i := range 8 {
			rs = append(rs, objfile.Range{Off: int64(i * 1000), Buf: make([]byte, 100)})
		}
		return rs
	}
	t.Run("TransientFirst", func(t *testing.T) {
		o := newCounting(10000)
		o.pinFirst = true
		var n atomic.Int32
		o.fail = func(int64, int64) error {
			k := n.Add(1)
			if k <= 2 && o.inFlightNow() != 1 {
				return errors.New("a request went with another before the Object was pinned")
			}
			if k <= 2 {
				return errors.New("transient")
			}
			return nil
		}
		o.expect(4, 8)
		rs := scattered()
		err := objfile.ReadRangesWith(ctx, o, rs, policy())
		var be *objfile.BatchError
		if !errors.As(err, &be) || be.Errs[0] == nil || be.Errs[1] == nil || be.Errs[2] != nil || o.maxIn != 4 {
			t.Fatalf("batch whose first two requests failed: %v, %d in flight at most; want ranges 0 and 1 failed, then 4 at once", err, o.maxIn)
		}
		checkFilled(t, o, rs[2:])
	})
	for _, c := range []struct {
		name string
		err  error
	}{
		{"SizeMismatch", &objfile.SizeMismatchError{Name: "obj", Expected: 10000, Actual: 9000}},
		{"Changed", objfile.ErrObjectChanged},
		{"Missing", fs.ErrNotExist},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newCounting(10000)
			o.pinFirst = true
			o.fail = func(int64, int64) error { return c.err }
			err := objfile.ReadRangesWith(ctx, o, scattered(), policy())
			var be *objfile.BatchError
			if !errors.As(err, &be) || len(be.Unwrap()) != 8 || len(o.reqs) != 1 {
				t.Fatalf("batch whose first request failed with %v: %v after %d requests; want every range failed after 1", c.err, err, len(o.reqs))
			}
			for i, e := range be.Errs {
				if !errors.Is(e, c.err) {
					t.Fatalf("range %d failed with %v, want %v", i, e, c.err)
				}
			}
		})
	}
}

// TestBatchFirstReadPinsThenFails: a first request that pins the Object and
// then fails with a terminal error (a body retry finding the object
// changed) still ends the batch.
func TestBatchFirstReadPinsThenFails(t *testing.T) {
	o := newCounting(10000)
	o.pinFirst = true
	o.fail = func(int64, int64) error {
		o.mu.Lock()
		o.pinned = true // the response pinned it; its body's retry then failed
		o.mu.Unlock()
		return objfile.ErrObjectChanged
	}
	rs := ranges(0, 100, 3000, 100, 6000, 100, 9000, 100)
	err := objfile.ReadRangesWith(context.Background(), o, rs, policy())
	var be *objfile.BatchError
	if !errors.As(err, &be) || len(be.Unwrap()) != 4 || len(o.reqs) != 1 {
		t.Fatalf("batch whose pinning request failed terminally: %v after %d requests; want every range failed after 1", err, len(o.reqs))
	}
}

// TestBatchDisableGapBridging: with gap bridging off no gap is read, small
// or large; touching and overlapping ranges are still merged.
func TestBatchDisableGapBridging(t *testing.T) {
	o := newCounting(4096)
	rs := ranges(0, 100, 101, 100, 201, 50, 220, 100, 1000, 10)
	if err := objfile.ReadRangesWith(context.Background(), o, rs, objfile.BatchPolicy{DisableGapBridging: true}); err != nil {
		t.Fatal(err)
	}
	checkFilled(t, o, rs)
	if got, want := o.requests(), []span{{0, 100}, {101, 219}, {1000, 10}}; !slices.Equal(got, want) {
		t.Fatalf("requests %v, want %v", got, want)
	}
}
