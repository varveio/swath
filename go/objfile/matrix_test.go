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
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/objfile"
)

// mstore is an in-memory store with the providers' semantics: Open pins by
// a metadata request; OpenSized pins at its first read, which finds a
// missing object (fs.ErrNotExist) or another size (*SizeMismatchError)
// first, and OpenSized(..., 0) is OpenEmpty over Open; a pinned read of a
// replaced or deleted object is ErrObjectChanged; ReadObject is one
// request. It records each read's extent and how many were in flight; with
// hold set, the reads after a pin wait until hold are in flight (so that
// concurrency shows without timing); with gate set, the next read signals
// first and waits for gate.
type mstore struct {
	mu       sync.Mutex
	objs     map[string]mobj
	gen      int
	reads    []span
	inFlight int
	maxIn    int
	hold     int
	held     chan struct{}
	notAlone bool
	gate     chan struct{}
	first    chan struct{}
}

type mobj struct {
	data []byte
	gen  int
}

func newMstore() *mstore { return &mstore{objs: map[string]mobj{}} }

func (s *mstore) put(name string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen++
	s.objs[name] = mobj{data: data, gen: s.gen}
}

func (s *mstore) remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objs, name)
}

func (s *mstore) Open(_ context.Context, name string) (objfile.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objs[name]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &mfile{s: s, name: name, size: int64(len(o.data)), gen: o.gen}, nil
}

func (s *mstore) OpenSized(ctx context.Context, name string, size int64) (objfile.Object, error) {
	if size == 0 {
		return objfile.OpenEmpty(ctx, name, s.Open)
	}
	return &mfile{s: s, name: name, size: size}, nil
}

func (s *mstore) ReadObject(_ context.Context, name string, limit int64) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, span{0, -1})
	o, ok := s.objs[name]
	if !ok {
		return nil, "", &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
	}
	if int64(len(o.data)) > limit {
		return nil, fmt.Sprint(o.gen), objfile.ErrTooLarge
	}
	return bytes.Clone(o.data), fmt.Sprint(o.gen), nil
}

type mfile struct {
	s    *mstore
	name string
	size int64
	mu   sync.Mutex
	gen  int // 0 until pinned
}

func (f *mfile) Size() int64  { return f.size }
func (f *mfile) Close() error { return nil }
func (f *mfile) Identity() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gen == 0 {
		return ""
	}
	return fmt.Sprint(f.gen)
}
func (f *mfile) WithContext(context.Context) io.ReaderAt { return f }

func (f *mfile) ReadAt(p []byte, off int64) (int, error) {
	if off >= f.size {
		return 0, io.EOF // no request
	}
	s := f.s
	f.mu.Lock()
	pinned := f.gen != 0
	f.mu.Unlock()
	s.mu.Lock()
	s.reads = append(s.reads, span{off, int64(len(p))})
	s.inFlight++
	s.maxIn = max(s.maxIn, s.inFlight)
	if !pinned && s.inFlight > 1 {
		s.notAlone = true
	}
	held, gate, first := s.held, s.gate, s.first
	if pinned && held != nil && s.inFlight >= s.hold {
		close(held)
		s.held = nil
	}
	if gate != nil {
		s.gate, s.first = nil, nil // this is the batch's first request
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()
	if gate != nil {
		close(first)
		<-gate
	}
	if pinned && held != nil {
		select {
		case <-held:
		case <-time.After(20 * time.Second):
			return 0, errors.New("the reads never reached the limit in flight")
		}
	}
	s.mu.Lock()
	o, ok := s.objs[f.name]
	s.mu.Unlock()
	f.mu.Lock()
	switch {
	case !pinned && !ok:
		f.mu.Unlock()
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrNotExist}
	case !pinned && int64(len(o.data)) != f.size:
		f.mu.Unlock()
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: &objfile.SizeMismatchError{Name: f.name, Expected: f.size, Actual: int64(len(o.data))}}
	case !pinned:
		f.gen = o.gen
	case !ok || o.gen != f.gen:
		f.mu.Unlock()
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: objfile.ErrObjectChanged}
	}
	f.mu.Unlock()
	n := copy(p, o.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// fixture is a matrix case's object and sizes: data (nil: missing), the
// size KnownSizes maps the name to and the size OpenSized is given, whether
// a pin is recorded (or restored) first, and then the change: replaced by
// with (or a clone, same size), or deleted.
type fixture struct {
	name          string
	data          []byte
	mapped, given int64
	record        bool
	change        string // "", "replace", "delete"
	with          []byte
}

// outcome is what an operation must give: nil, ErrObjectChanged,
// fs.ErrNotExist, or a *SizeMismatchError of expected (actual).
type outcome struct {
	err              error
	expected, actual int64
}

func (fx fixture) want(op string) outcome {
	after := fx.data
	switch fx.change {
	case "replace":
		after = fx.with
		if after == nil {
			after = bytes.Clone(fx.data)
		}
	case "delete":
		after = nil
	}
	if fx.record && fx.change != "" {
		return outcome{err: objfile.ErrObjectChanged}
	}
	if op == "ReadObject" {
		if after == nil {
			return outcome{err: fs.ErrNotExist}
		}
		return outcome{}
	}
	expected := fx.mapped
	if strings.HasPrefix(op, "OpenSized") {
		expected = fx.given
	}
	switch {
	case fx.record && expected != int64(len(fx.data)):
		return outcome{err: objfile.ErrObjectChanged}
	case after == nil:
		return outcome{err: fs.ErrNotExist}
	case expected != int64(len(after)):
		return outcome{err: &objfile.SizeMismatchError{}, expected: expected, actual: int64(len(after))}
	}
	return outcome{}
}

// TestCompositionMatrix runs every order of Root, Limit, Batch, KnownSizes
// and Pin (or Restore) over one store (all are supported), for Open,
// OpenSized, ReadAt, ReadRanges (also on a handle a rejected read has used)
// and ReadObject, over fixtures of an unchanged, replaced, deleted and
// missing object, wrong sizes given or mapped (each way, zero on either
// side), and empty objects. It checks the classification, the bytes, every
// batch request's extent under Batch's policy, that exactly the limit of
// requests is reached in flight and never passed, that a failing batch
// sends one request, and, holding a sized Object's first request open, that
// nothing else is sent until it is checked.
func TestCompositionMatrix(t *testing.T) {
	const limit, s = 2, 4000
	data := bytes.Repeat([]byte("0123456789"), s/10)
	fixtures := []fixture{
		{name: "unchanged", data: data, mapped: s, given: s},
		{name: "replaced", data: data, mapped: s, given: s, record: true, change: "replace"},
		{name: "deleted", data: data, mapped: s, given: s, record: true, change: "delete"},
		{name: "missing", mapped: s, given: s},
		{name: "wrongsize", data: data, mapped: s + 1, given: s + 1},
		{name: "map wrong", data: data, mapped: s + 1, given: s},
		{name: "given wrong", data: data, mapped: s, given: s + 1},
		{name: "empty, 0", data: []byte{}, mapped: 0, given: 0},
		{name: "full, 0", data: data, mapped: 0, given: 0},
		{name: "missing, 0", mapped: 0, given: 0},
		{name: "empty, map 4, given 0", data: []byte{}, mapped: 4, given: 0},
		{name: "full, map 0, given full", data: data, mapped: 0, given: s},
		{name: "pinned empty, map 4, given 0", data: []byte{}, mapped: 4, given: 0, record: true},
		{name: "pinned empty, replaced full", data: []byte{}, mapped: 0, given: 0, record: true, change: "replace", with: data},
	}
	ops := []string{"Open+ReadAt", "OpenSized+ReadAt", "Open+ReadRanges", "OpenSized+ReadRanges", "OpenSized+ReuseReadRanges", "ReadObject"}
	layers := []string{"Root", "Limit", "Batch", "KnownSizes", "Pin"}
	var perms [][]string
	var permute func(prefix, rest []string)
	permute = func(prefix, rest []string) {
		if len(rest) == 0 {
			perms = append(perms, prefix)
			return
		}
		for i := range rest {
			next := append(append([]string(nil), rest[:i]...), rest[i+1:]...)
			permute(append(append([]string(nil), prefix...), rest[i]), next)
		}
	}
	permute(nil, layers)
	for _, perm := range perms {
		for _, restored := range []bool{false, true} {
			for _, fx := range fixtures {
				if restored && !fx.record {
					continue // a restored pin is a recorded one
				}
				for _, op := range ops {
					t.Run(fmt.Sprintf("%s/restored=%v/%s/%s", strings.Join(perm, "<"), restored, fx.name, op), func(t *testing.T) {
						runMatrixCase(t, perm, restored, fx, op, limit)
					})
				}
			}
		}
	}
}

func runMatrixCase(t *testing.T, perm []string, restored bool, fx fixture, op string, limit int) {
	ctx := context.Background()
	st := newMstore()
	if fx.data != nil {
		st.put("ds/obj", fx.data)
	}
	// Built innermost first; layers above Root see root-relative names.
	rootBelow := false
	var o objfile.Opener = st
	local := func() string {
		if rootBelow {
			return "obj"
		}
		return "ds/obj"
	}
	for _, l := range perm {
		switch l {
		case "Root":
			o, rootBelow = objfile.Root(o, "ds"), true
		case "Limit":
			o = objfile.Limit(o, limit)
		case "Batch":
			o = mustBatch(t, o, objfile.BatchPolicy{MaxRequest: 10, MaxConcurrent: 4})
		case "KnownSizes":
			o = objfile.KnownSizes(o, map[string]int64{local(): fx.mapped})
		case "Pin":
			if restored {
				snap := fmt.Sprintf(`{"version":1,"scope":%q,"objects":{%q:{"size":%d,"identity":"1"}}}`, scope, local(), len(fx.data))
				p, err := objfile.Restore(o, scope, []byte(snap))
				if err != nil {
					t.Fatal(err)
				}
				o = p
			} else {
				o = objfile.Pin(o, scope)
			}
		}
	}
	const name = "obj" // Root is in every order

	if fx.record && !restored {
		if _, _, err := objfile.ReadObject(ctx, o, name, 1<<20); err != nil {
			t.Fatalf("recording the pin: %v", err)
		}
	}
	switch fx.change {
	case "replace":
		with := fx.with
		if with == nil {
			with = bytes.Clone(fx.data)
		}
		st.put("ds/obj", with)
	case "delete":
		st.remove("ds/obj")
	}
	want := fx.want(op)
	expected := fx.mapped
	if strings.HasPrefix(op, "OpenSized") {
		expected = fx.given
	}
	n := min(int64(100), expected)                           // the bytes read
	rctx, cancel := context.WithTimeout(ctx, 20*time.Second) // a deadlock fails, not hangs
	defer cancel()

	var err error
	var rs []objfile.Range
	var batched bool
	switch op {
	case "ReadObject":
		var b []byte
		b, _, err = objfile.ReadObject(rctx, o, name, 1<<20)
		if err == nil && !bytes.Equal(b, fx.data) {
			t.Fatal("ReadObject read the wrong bytes")
		}
	default:
		var f objfile.Object
		if strings.HasPrefix(op, "OpenSized") {
			f, err = o.OpenSized(rctx, name, fx.given)
		} else {
			f, err = o.Open(rctx, name)
		}
		if err != nil {
			break
		}
		if strings.Contains(op, "Reuse") {
			// A read first, whose rejection (if any) leaves the handle
			// unconfirmed: the batch after it keeps its barrier.
			_, _ = f.WithContext(rctx).ReadAt(make([]byte, min(10, n)), 0)
		}
		st.mu.Lock()
		st.reads, st.maxIn = nil, 0
		lazy := f.Identity() == "" && n > 0
		batched = strings.HasSuffix(op, "ReadRanges")
		if batched && want.err == nil && n >= 20 {
			st.hold, st.held = limit, make(chan struct{})
		}
		if batched && lazy {
			st.gate, st.first = make(chan struct{}), make(chan struct{})
		}
		gate, first := st.gate, st.first
		st.mu.Unlock()
		if batched {
			rs = []objfile.Range{{Off: 0, Buf: make([]byte, n)}}
			done := make(chan error, 1)
			go func() { done <- objfile.ReadRanges(rctx, f, rs) }()
			if gate != nil {
				// The first request is held open: nothing else is sent.
				select {
				case <-first:
				case <-rctx.Done():
					t.Fatal("the first request of a sized Object was not sent")
				}
				for range 2000 {
					runtime.Gosched()
				}
				st.mu.Lock()
				sent := len(st.reads)
				st.mu.Unlock()
				if sent != 1 {
					t.Fatalf("%d requests sent while the first of a sized Object was unchecked", sent)
				}
				close(gate)
			}
			err = <-done
		} else {
			p := make([]byte, min(10, n))
			var m int
			if m, err = f.WithContext(rctx).ReadAt(p, 0); err == nil && !bytes.Equal(p, fx.data[:m]) {
				t.Fatal("ReadAt read the wrong bytes")
			}
			if err == io.EOF && expected == 0 {
				err = nil // an empty object's read
			}
		}
	}

	var sm *objfile.SizeMismatchError
	switch {
	case want.err == nil:
		if err != nil {
			t.Fatalf("want success: %v", err)
		}
	case errors.Is(want.err, objfile.ErrObjectChanged):
		if !errors.Is(err, objfile.ErrObjectChanged) || errors.As(err, &sm) {
			t.Fatalf("want ErrObjectChanged: %v", err)
		}
	case errors.Is(want.err, fs.ErrNotExist):
		if !errors.Is(err, fs.ErrNotExist) || errors.Is(err, objfile.ErrObjectChanged) {
			t.Fatalf("want fs.ErrNotExist alone: %v", err)
		}
	default:
		if !errors.As(err, &sm) || sm.Expected != want.expected || sm.Actual != want.actual || errors.Is(err, objfile.ErrObjectChanged) {
			t.Fatalf("want a SizeMismatchError of %d (actual %d): %v", want.expected, want.actual, err)
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.notAlone {
		t.Fatal("a read of an unpinned Object was made beside another")
	}
	if st.maxIn > limit {
		t.Fatalf("%d requests in flight, over the limit of %d", st.maxIn, limit)
	}
	if !batched {
		return
	}
	if err != nil {
		if len(st.reads) > 1 {
			t.Fatalf("a failing batch sent %d requests: %v", len(st.reads), st.reads)
		}
		return
	}
	if !bytes.Equal(rs[0].Buf, fx.data[:n]) {
		t.Fatal("the batch read the wrong bytes")
	}
	var wantSpans []span
	for off := int64(0); off < n; off += 10 {
		wantSpans = append(wantSpans, span{off, min(10, n-off)})
	}
	got := slices.Clone(st.reads)
	slices.SortFunc(got, func(a, b span) int { return int(a.off - b.off) })
	if !slices.Equal(got, wantSpans) {
		t.Fatalf("batch requests %v, want %v", got, wantSpans)
	}
	if n >= 20 && st.maxIn != limit {
		t.Fatalf("%d requests at most in flight, want the limit %d", st.maxIn, limit)
	}
}
