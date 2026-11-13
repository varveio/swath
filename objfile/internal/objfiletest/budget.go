// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfiletest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/objfile"
)

// Kind is what a request to a store is.
type Kind int

const (
	Other    Kind = iota
	Metadata      // a request for size and version (HEAD, GCS attributes)
	Ranged        // a read of one byte range
	Whole         // a read of the whole object, without a range
)

// Request is one request a Recorder saw: its kind, its Range header (or
// x-ms-range), and whether it carried the identity condition of a pinned
// read (If-Match, a GCS generation).
type Request struct {
	Kind   Kind
	Range  string
	Pinned bool
}

// Classify tells a store's request kind and whether it carries the pin.
type Classify func(r *http.Request) (Kind, bool)

// Recorder is a counting fake around a store's handler: it records each
// request, and how many reads (Ranged or Whole) were in flight at once. With
// Hold(n), a read after the first waits until n are in flight, so a test
// sees concurrency without timing. Before, if set, runs before the fake
// answers a read, with that read's number from 1.
type Recorder struct {
	classify Classify

	mu       sync.Mutex
	reqs     []Request
	inFlight int
	maxIn    int
	hold     int
	held     chan struct{}
	reads    int
	Before   func(n int)
	// Cut, if set, is given each read's number: a positive result ends that
	// response's body after so many bytes.
	Cut func(n int) int
	// Fail, if set, is given each read's number: a positive result is the
	// status that read is answered with instead, with no body.
	Fail func(n int) int
}

// NewRecorder returns a Recorder that classifies with c.
func NewRecorder(c Classify) *Recorder { return &Recorder{classify: c} }

// Wrap wraps a store's handler; pass it as NewStore's wrap.
func (rec *Recorder) Wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind, pinned := rec.classify(r)
		rng := r.Header.Get("x-ms-range")
		if rng == "" {
			rng = r.Header.Get("Range")
		}
		read := kind == Ranged || kind == Whole
		rec.mu.Lock()
		rec.reqs = append(rec.reqs, Request{Kind: kind, Range: rng, Pinned: pinned})
		var held chan struct{}
		n := 0
		if read {
			rec.reads++
			n = rec.reads
			rec.inFlight++
			rec.maxIn = max(rec.maxIn, rec.inFlight)
			if rec.held != nil && n > 1 {
				held = rec.held
				if rec.inFlight >= rec.hold {
					close(rec.held)
					rec.held = nil
				}
			}
		}
		before, cut, fail := rec.Before, rec.Cut, rec.Fail
		rec.mu.Unlock()
		if held != nil {
			select {
			case <-held:
			case <-r.Context().Done():
			case <-time.After(10 * time.Second):
			}
		}
		if read && fail != nil {
			if code := fail(n); code > 0 {
				rec.mu.Lock()
				rec.inFlight--
				rec.mu.Unlock()
				w.WriteHeader(code)
				return
			}
		}
		if read && cut != nil {
			if k := cut(n); k > 0 {
				w = &cutWriter{ResponseWriter: w, left: k}
			}
		}
		if read && before != nil {
			before(n)
		}
		h.ServeHTTP(w, r)
		if read {
			rec.mu.Lock()
			rec.inFlight--
			rec.mu.Unlock()
		}
	})
}

// cutWriter writes at most left bytes of a body, then nothing: the client
// sees the body end short of its Content-Length. The body is typed as a
// fake's passed-through body is (bodyType), set on the writer before its
// headers are sent.
type cutWriter struct {
	http.ResponseWriter
	left        int
	wroteHeader bool
}

func (c *cutWriter) WriteHeader(code int) {
	w := c.ResponseWriter
	if !c.wroteHeader {
		c.wroteHeader = true
		w.Header().Set("Content-Type", bodyType(w.Header()))
		w.Header().Set("X-Content-Type-Options", "nosniff")
	}
	w.WriteHeader(code)
}

func (c *cutWriter) Write(p []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	if c.left <= 0 {
		return 0, errors.New("cut")
	}
	n := min(len(p), c.left)
	c.left -= n
	w := c.ResponseWriter
	// Already set by WriteHeader, before the headers were sent; set again
	// on the writer the body goes to (a no-op once they are sent).
	w.Header().Set("Content-Type", bodyType(w.Header()))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	m, err := w.Write(p[:n])
	if err == nil && n < len(p) {
		err = errors.New("cut")
	}
	return m, err
}

// Hold makes the reads after the first wait until n are in flight at once.
func (rec *Recorder) Hold(n int) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.hold, rec.held = n, make(chan struct{})
}

// Requests returns the requests recorded so far.
func (rec *Recorder) Requests() []Request {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]Request(nil), rec.reqs...)
}

// Count returns how many requests of kind k were recorded.
func (rec *Recorder) Count(k Kind) int {
	n := 0
	for _, r := range rec.Requests() {
		if r.Kind == k {
			n++
		}
	}
	return n
}

// MaxInFlight returns the most reads that were in flight at once.
func (rec *Recorder) MaxInFlight() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.maxIn
}

// Reset forgets the requests recorded so far.
func (rec *Recorder) Reset() {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.reqs, rec.maxIn, rec.reads = nil, 0, 0
}

// Budget is a store for RunBudget: how to start it, how to classify its
// requests, and whether its SDK reads a Content-Range with an unknown
// total ("*").
type Budget struct {
	New            NewStore
	Classify       Classify
	UnknownTotalOK bool
	// Observed, if set, starts the store with a client whose transport is
	// ranged.Observe, recorded as observing (ranged.SetObserving).
	Observed NewStore
}

// RunBudget checks the requests the Opener of b makes: by kind, the ranges
// they ask for, how many are in flight, and that every read after the
// first of a sized Object carries the pin.
func RunBudget(t *testing.T, b Budget) {
	ctx := context.Background()
	start := func(t *testing.T) (Store, *Recorder) {
		rec := NewRecorder(b.Classify)
		return b.New(t, rec.Wrap), rec
	}
	read := func(t *testing.T, f objfile.Object, data []byte, rs []objfile.Range) {
		t.Helper()
		if err := objfile.ReadRanges(ctx, f, rs); err != nil {
			t.Fatal(err)
		}
		for i, r := range rs {
			if !bytes.Equal(r.Buf, data[r.Off:r.Off+int64(len(r.Buf))]) {
				t.Fatalf("range %d (%d bytes at %d) holds the wrong bytes", i, len(r.Buf), r.Off)
			}
		}
	}
	sized := func(t *testing.T, o objfile.Opener, name string, size int) objfile.Object {
		t.Helper()
		f, err := o.OpenSized(ctx, name, int64(size))
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	t.Run("CoalescedSizedBatch", func(t *testing.T) {
		s, rec := start(t)
		data := Data(1<<20, 5)
		s.Put(t, "obj", data)
		rec.Reset()
		f := sized(t, s.Opener, "obj", len(data))
		// Adjacent, overlapping, out of order and across a 1 KiB gap.
		rs := []objfile.Range{{Off: 20000, Buf: make([]byte, 5000)}, {Off: 10000, Buf: make([]byte, 10000)},
			{Off: 12000, Buf: make([]byte, 100)}, {Off: 26000, Buf: make([]byte, 4000)}}
		read(t, f, data, rs)
		if got := rec.Requests(); len(got) != 1 || got[0].Kind != Ranged || got[0].Range != "bytes=10000-29999" {
			t.Fatalf("requests %+v, want one ranged GET of bytes=10000-29999", got)
		}
	})

	t.Run("ConcurrentWithinLimit", func(t *testing.T) {
		s, rec := start(t)
		data := Data(4<<20, 6)
		s.Put(t, "obj", data)
		rec.Reset()
		f := sized(t, objfile.Limit(s.Opener, 3), "obj", len(data))
		var rs []objfile.Range
		for i := range 12 { // 300 KiB apart: never bridged
			rs = append(rs, objfile.Range{Off: int64(i) * 300 << 10, Buf: make([]byte, 1000)})
		}
		// The first request pins alone; then three at a time.
		rec.Hold(3)
		read(t, f, data, rs)
		got := rec.Requests()
		if len(got) != 12 || rec.Count(Metadata) != 0 || rec.MaxInFlight() != 3 {
			t.Fatalf("%d requests (%d metadata), %d in flight at most; want 12, 0, 3", len(got), rec.Count(Metadata), rec.MaxInFlight())
		}
		for i, r := range got {
			if r.Pinned != (i > 0) {
				t.Fatalf("request %d (%s) pinned=%v; only the first may be unpinned", i, r.Range, r.Pinned)
			}
		}
	})

	t.Run("SplitOversized", func(t *testing.T) {
		s, rec := start(t)
		data := Data(1<<20, 7)
		s.Put(t, "obj", data)
		o, err := objfile.Batch(s.Opener, objfile.BatchPolicy{MaxRequest: 64 << 10, MaxConcurrent: 4, MaxInFlightBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		f := sized(t, o, "obj", len(data))
		rec.Reset()
		read(t, f, data, []objfile.Range{{Off: 1000, Buf: make([]byte, 200<<10)}})
		if n := rec.Count(Ranged); n != 4 || len(rec.Requests()) != 4 {
			t.Fatalf("%d ranged requests of %d, want 4 of at most 64 KiB", n, len(rec.Requests()))
		}
	})

	t.Run("IdentityChangeMidBatch", func(t *testing.T) {
		s, rec := start(t)
		data := Data(1<<20, 8)
		s.Put(t, "obj", data)
		f := sized(t, s.Opener, "obj", len(data))
		var once sync.Once
		rec.Before = func(n int) {
			if n > 1 { // after the request that pins: same size, another version
				once.Do(func() { s.Put(t, "obj", Data(1<<20, 9)) })
			}
		}
		rs := []objfile.Range{{Off: 0, Buf: make([]byte, 100)}, {Off: 500000, Buf: make([]byte, 100)}, {Off: 900000, Buf: make([]byte, 100)}}
		err := objfile.ReadRanges(ctx, f, rs)
		var be *objfile.BatchError
		if !errors.As(err, &be) || be.Errs[0] != nil || !errors.Is(be.Errs[1], objfile.ErrObjectChanged) || !errors.Is(be.Errs[2], objfile.ErrObjectChanged) {
			t.Fatalf("batch across a replacement = %v; want range 0 filled and the others ErrObjectChanged", err)
		}
		if !bytes.Equal(rs[0].Buf, data[:100]) {
			t.Fatal("range 0 holds the wrong bytes")
		}
	})

	t.Run("SizeMismatch", func(t *testing.T) {
		// Each way a first read finds the size given wrong: a stated total
		// (larger or smaller object), a range the store clips, a range past
		// the object's end (416, then a metadata request for the size).
		for _, c := range []struct {
			name               string
			expected, off, gap int64
			n, metadata        int
		}{{"larger object", 999, 0, 200, 10, 0}, {"smaller object", 1001, 0, 200, 10, 0},
			{"clipped", 1 << 20, 995, 300000, 10, 0}, {"past the end", 1100, 1050, 0, 10, 1}} {
			s, rec := start(t)
			s.Put(t, "obj", Data(1000, 1))
			f := sized(t, s.Opener, "obj", int(c.expected))
			rs := []objfile.Range{{Off: c.off, Buf: make([]byte, c.n)}}
			if c.gap > 0 {
				// After the first range in object order, so the first
				// request is the one that shows it; the rest are never sent.
				for i := range 3 {
					rs = append(rs, objfile.Range{Off: c.off + int64(i+1)*c.gap, Buf: make([]byte, c.n)})
				}
			}
			err := objfile.ReadRanges(ctx, f, rs)
			var sm *objfile.SizeMismatchError
			if !errors.As(err, &sm) || errors.Is(err, objfile.ErrObjectChanged) || sm.Expected != c.expected || sm.Actual != 1000 {
				t.Fatalf("%s: batch = %v; want a SizeMismatchError of %d, actual 1000, not ErrObjectChanged", c.name, err, c.expected)
			}
			var be *objfile.BatchError
			if !errors.As(err, &be) || len(be.Unwrap()) != len(rs) {
				t.Fatalf("%s: %v; want every range failed", c.name, err)
			}
			if r := rec.Requests(); len(r) == 0 || r[0].Kind != Ranged || !strings.HasPrefix(r[0].Range, fmt.Sprintf("bytes=%d-", c.off)) {
				t.Fatalf("%s: requests %+v; want the range at %d sent first", c.name, r, c.off)
			}
			if rec.Count(Metadata) != c.metadata || rec.Count(Ranged) != 1 {
				t.Fatalf("%s: %d metadata and %d ranged requests; want %d and 1", c.name, rec.Count(Metadata), rec.Count(Ranged), c.metadata)
			}
		}
	})

	t.Run("MissingOnFirstRead", func(t *testing.T) {
		s, rec := start(t)
		f := sized(t, s.Opener, "nope", 1000)
		err := objfile.ReadRanges(ctx, f, []objfile.Range{{Off: 0, Buf: make([]byte, 10)}, {Off: 500, Buf: make([]byte, 10)}})
		if !errors.Is(err, fs.ErrNotExist) || errors.Is(err, objfile.ErrObjectChanged) || rec.Count(Ranged) != 1 {
			t.Fatalf("batch of a missing object = %v after %d requests; want fs.ErrNotExist after 1", err, rec.Count(Ranged))
		}
	})

	t.Run("UnknownTotalFallsBack", func(t *testing.T) {
		if !b.UnknownTotalOK {
			t.Skip("the SDK refuses a Content-Range with an unknown total")
		}
		rec := NewRecorder(b.Classify)
		s := b.New(t, func(h http.Handler) http.Handler { return rec.Wrap(UnknownTotal(h)) })
		data := Data(1000, 2)
		s.Put(t, "obj", data)
		f := sized(t, s.Opener, "obj", len(data))
		read(t, f, data, []objfile.Range{{Off: 0, Buf: make([]byte, 10)}, {Off: 500, Buf: make([]byte, 10)}})
		// The first response is let go before the metadata request (so it
		// holds no connection meanwhile) and its range asked again, pinned.
		if rec.Count(Metadata) != 1 || rec.Count(Ranged) != 3 {
			t.Fatalf("%d metadata and %d ranged requests; want 1 and 3", rec.Count(Metadata), rec.Count(Ranged))
		}
		g := sized(t, s.Opener, "obj", 1001)
		var sm *objfile.SizeMismatchError
		if _, err := g.ReadAt(make([]byte, 10), 0); !errors.As(err, &sm) || sm.Actual != 1000 || g.Identity() != "" {
			t.Fatalf("read of another size, total unknown = %v, Identity %q", err, g.Identity())
		}
	})

	t.Run("BodyCutResumedWithPin", func(t *testing.T) {
		s, rec := start(t)
		data := Data(1<<20, 3)
		s.Put(t, "obj", data)
		f := sized(t, s.Opener, "obj", len(data))
		rec.Cut = func(n int) int {
			if n == 2 {
				return 1000
			}
			return 0
		}
		read(t, f, data, []objfile.Range{{Off: 0, Buf: make([]byte, 100)}, {Off: 500000, Buf: make([]byte, 5000)}})
		got := rec.Requests()
		if len(got) != 3 || got[2].Range != "bytes=501000-504999" || !got[2].Pinned {
			t.Fatalf("requests %+v; want the cut one resumed at 501000, pinned", got)
		}
	})

	t.Run("Observed", func(t *testing.T) {
		runObserved(t, b.Classify, b.New, false)
	})
	t.Run("ObservedTransport", func(t *testing.T) {
		if b.Observed == nil {
			t.Skip("no client with an observing transport")
		}
		runObserved(t, b.Classify, b.Observed, true)
	})

	t.Run("CacheRevisit", func(t *testing.T) {
		// A pinned Object's revisit is served by the cache with no request;
		// a sized open's first read, and a bypassed read, go to the store.
		s, rec := start(t)
		data := Data(1<<20, 12)
		s.Put(t, "obj", data)
		c, err := objfile.NewByteCache(4 << 20)
		if err != nil {
			t.Fatal(err)
		}
		o := objfile.Cache(s.Opener, "budget", c)
		f, err := o.Open(ctx, "obj")
		if err != nil {
			t.Fatal(err)
		}
		rs := func() []objfile.Range {
			return []objfile.Range{{Off: 1000, Buf: make([]byte, 5000)}, {Off: 400000, Buf: make([]byte, 5000)}, {Off: 900000, Buf: make([]byte, 5000)}}
		}
		read(t, f, data, rs())
		rec.Reset()
		read(t, f, data, rs())
		if n := len(rec.Requests()); n != 0 {
			t.Fatalf("a warm revisit made %d requests, want 0", n)
		}
		g, err := o.OpenSized(ctx, "obj", int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		read(t, g, data, rs())
		if n := rec.Count(Ranged); n != 3 {
			t.Fatalf("a sized open's first batch made %d ranged requests over a warm cache, want 3", n)
		}
		rec.Reset()
		bypass := objfile.WithCacheMode(ctx, objfile.CacheBypass)
		if err := objfile.ReadRanges(bypass, f, rs()); err != nil {
			t.Fatal(err)
		}
		if n := rec.Count(Ranged); n != 3 {
			t.Fatalf("a bypassed batch made %d ranged requests, want 3", n)
		}
	})

	t.Run("ReadObject", func(t *testing.T) {
		s, rec := start(t)
		data := Data(5000, 4)
		s.Put(t, "manifest.json", data)
		opened, err := s.Opener.Open(ctx, "manifest.json")
		if err != nil {
			t.Fatal(err)
		}
		rec.Reset()
		got, id, err := objfile.ReadObject(ctx, objfile.Limit(objfile.Root(s.Opener, ""), 1), "manifest.json", 5000)
		if err != nil || !bytes.Equal(got, data) || id != opened.Identity() {
			t.Fatalf("ReadObject = %d bytes, %q, %v; want the object at %q", len(got), id, err, opened.Identity())
		}
		if r := rec.Requests(); len(r) != 1 || r[0].Kind != Whole {
			t.Fatalf("requests %+v, want one whole-object GET", r)
		}
		rec.Reset()
		if _, id, err := objfile.ReadObject(ctx, s.Opener, "manifest.json", 4999); !errors.Is(err, objfile.ErrTooLarge) || id != opened.Identity() || len(rec.Requests()) != 1 {
			t.Fatalf("ReadObject over the limit = %q, %v after %d requests", id, err, len(rec.Requests()))
		}
		s.Put(t, "empty", nil)
		if got, _, err := objfile.ReadObject(ctx, s.Opener, "empty", 10); err != nil || len(got) != 0 {
			t.Fatalf("ReadObject of an empty object = %d bytes, %v", len(got), err)
		}
		if _, _, err := objfile.ReadObject(ctx, s.Opener, "nope", 10); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("ReadObject of a missing object = %v", err)
		}
	})
}

// runObserved checks that every request a store's Opener makes is
// observed once, with its kind, extent, retry flag and the bytes it
// delivered, every one ended before the operation returns. With complete
// (the store's client sends through an Observe transport), ReportsIO is
// true and the requests an SDK sends again, after a 503 or a body cut short,
// are observed as retries; otherwise ReportsIO is false.
func runObserved(t *testing.T, classify Classify, newStore NewStore, complete bool) {
	ctx := context.Background()
	rec := NewRecorder(classify)
	s := newStore(t, rec.Wrap)
	if r, ok := s.Opener.(objfile.IOReporter); (complete && !ok) || (ok && r.ReportsIO() != complete) {
		t.Fatalf("the Opener is an IOReporter %v; want ReportsIO %v", ok, complete)
	}
	data := Data(1<<20, 11)
	s.Put(t, "obj", data)
	s.Put(t, "manifest.json", data[:3000])
	rec.Reset()
	obs := &Observer{}
	octx := objfile.WithObserver(ctx, obs)
	check := func(t *testing.T, what string) []objfile.RequestResult {
		t.Helper()
		reqs, ends := obs.Events()
		if len(ends) != len(reqs) {
			t.Fatalf("%s: %d requests began, %d ended", what, len(reqs), len(ends))
		}
		for k, ok := range map[Kind]objfile.RequestKind{Metadata: objfile.MetadataRequest, Ranged: objfile.RangeRequest, Whole: objfile.WholeRequest} {
			if got, want := obs.Count(ok), rec.Count(k); got != want {
				t.Fatalf("%s: %v requests: %d observed, %d made", what, ok, got, want)
			}
		}
		return ends
	}
	opened, err := s.Opener.Open(octx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if r, ok := opened.(objfile.IOReporter); (complete && !ok) || (ok && r.ReportsIO() != complete) {
		t.Fatalf("the Object is an IOReporter %v; want ReportsIO %v", ok, complete)
	}
	f, err := s.Opener.OpenSized(octx, "obj", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rs := []objfile.Range{{Off: 0, Buf: make([]byte, 5000)}, {Off: 300000, Buf: make([]byte, 5000)}, {Off: 600000, Buf: make([]byte, 5000)}}
	if err := objfile.ReadRanges(octx, f, rs); err != nil {
		t.Fatal(err)
	}
	if _, _, err := objfile.ReadObject(octx, s.Opener, "manifest.json", 1<<20); err != nil {
		t.Fatal(err)
	}
	ends := check(t, "fault-free")
	reqs, _ := obs.Events()
	var bytes int64
	for i, q := range reqs {
		if q.Retry {
			t.Fatalf("request %+v is a retry, with no fault", q)
		}
		if q.Kind == objfile.RangeRequest {
			bytes += ends[i].Bytes
		}
	}
	if bytes != 15000 {
		t.Fatalf("%d range bytes observed, want 15000", bytes)
	}
	if p := obs.Plans(); len(p) != 1 || p[0] != [3]int64{3, 3, 0} {
		t.Fatalf("plans %v, want one of 3 ranges, 3 requests, no gap", p)
	}
	if !complete {
		return
	}

	// A 503 the SDK retries, and a body cut short (which the GCS SDK
	// reopens itself, the others leave to objfile): each attempt observed.
	rec.Reset()
	obs.Reset()
	rec.Fail = func(n int) int {
		if n == 1 {
			return http.StatusServiceUnavailable
		}
		return 0
	}
	rec.Cut = func(n int) int {
		if n == 3 {
			return 1000
		}
		return 0
	}
	g, err := s.Opener.Open(octx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	rs = []objfile.Range{{Off: 0, Buf: make([]byte, 5000)}, {Off: 300000, Buf: make([]byte, 5000)}}
	if err := objfile.ReadRanges(octx, g, rs); err != nil {
		t.Fatal(err)
	}
	rec.Fail, rec.Cut = nil, nil
	ends = check(t, "faults")
	reqs, _ = obs.Events()
	retries := 0
	bytes = 0
	for i, q := range reqs {
		if q.Retry {
			retries++
		}
		if q.Kind == objfile.RangeRequest {
			bytes += ends[i].Bytes
		}
	}
	if retries != 2 || bytes != 10000 {
		t.Fatalf("%d retries and %d range bytes observed in %+v; want 2 (the 503's, the cut body's) and 10000", retries, bytes, reqs)
	}
}

// Observer is an objfile.Observer that keeps every event, each request's
// result at the index of its beginning.
type Observer struct {
	mu    sync.Mutex
	reqs  []objfile.Request
	ends  []objfile.RequestResult
	ended []bool
	plans [][3]int64
	hits  int64
	gen   int // counts Resets
}

func (o *Observer) Begin(q objfile.Request) func(objfile.RequestResult) {
	o.mu.Lock()
	defer o.mu.Unlock()
	i := len(o.reqs)
	o.reqs = append(o.reqs, q)
	o.ends = append(o.ends, objfile.RequestResult{})
	o.ended = append(o.ended, false)
	gen := o.gen
	return func(e objfile.RequestResult) {
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.gen != gen {
			return // reset since
		}
		if o.ended[i] {
			panic(fmt.Sprintf("the end of request %+v reported twice", o.reqs[i]))
		}
		o.ended[i], o.ends[i] = true, e
	}
}

func (o *Observer) BatchPlanned(ranges, requests, overRead int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.plans = append(o.plans, [3]int64{ranges, requests, overRead})
}

func (o *Observer) CacheHit(bytes int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.hits += bytes
}

// Reset forgets the events so far.
func (o *Observer) Reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reqs, o.ends, o.ended, o.plans, o.hits = nil, nil, nil, nil, 0
	o.gen++
}

// Count returns how many requests of kind k began.
func (o *Observer) Count(k objfile.RequestKind) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, q := range o.reqs {
		if q.Kind == k {
			n++
		}
	}
	return n
}

// Events returns the requests that began and the results of those that
// ended (in the order of their requests).
func (o *Observer) Events() ([]objfile.Request, []objfile.RequestResult) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var ends []objfile.RequestResult
	for i, e := range o.ends {
		if o.ended[i] {
			ends = append(ends, e)
		}
	}
	return append([]objfile.Request(nil), o.reqs...), ends
}

// Plans returns the batch plans observed: ranges, requests, over-read.
func (o *Observer) Plans() [][3]int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([][3]int64(nil), o.plans...)
}

// Hits returns the cache-hit bytes observed.
func (o *Observer) Hits() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.hits
}

// UnknownTotal rewrites every Content-Range total to "*".
func UnknownTotal(h http.Handler) http.Handler {
	total := regexp.MustCompile(`/[0-9]+$`)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			h.ServeHTTP(w, r)
			return
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		if cr := rec.Header().Get("Content-Range"); cr != "" {
			w.Header().Set("Content-Range", total.ReplaceAllString(cr, "/*"))
		}
		w.Header().Set("Content-Length", strconv.Itoa(rec.Body.Len()))
		w.Header().Set("Content-Type", bodyType(rec.Header()))
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	})
}

// bodyType is the Content-Type of a response whose body a wrapper passes
// through from a fake, with header h: the fake's own type when it is one of
// the non-HTML types the fakes send (kept, as the SDKs read it), else
// application/octet-stream (net/http would otherwise sniff one from the
// bytes, text/html included). The clients are SDKs, never browsers, but the
// bytes are a request's (an object's), so the response, also sent with
// X-Content-Type-Options: nosniff, is never one a browser could take for a
// page; go/bench/faultproxy types the bodies it passes through alike.
func bodyType(h http.Header) string {
	switch mt, _, _ := mime.ParseMediaType(h.Get("Content-Type")); mt {
	case "application/json", "application/xml", "text/xml":
		return mt
	}
	return "application/octet-stream"
}
