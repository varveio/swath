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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/objfile"
)

// mstore is an in-memory store with the providers' semantics: Open pins by
// a metadata request; OpenSized pins at its first read, which finds a
// missing object (fs.ErrNotExist) or another size (*SizeMismatchError)
// first; a pinned read of a replaced or deleted object is
// ErrObjectChanged; ReadObject is one request. It counts reads and how many
// are in flight, holds the reads after a pin until hold are in flight (so
// that concurrency shows without timing), and records a read made beside
// another while its Object was unpinned.
type mstore struct {
	mu        sync.Mutex
	objs      map[string]mobj
	gen       int
	reads     int
	inFlight  int
	maxIn     int
	hold      int
	held      chan struct{}
	notAlone  bool
	sizedRead bool // a read of an unpinned Object was made
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

func (s *mstore) OpenSized(_ context.Context, name string, size int64) (objfile.Object, error) {
	return &mfile{s: s, name: name, size: size}, nil
}

func (s *mstore) ReadObject(_ context.Context, name string, limit int64) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
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
	s := f.s
	f.mu.Lock()
	pinned := f.gen != 0
	f.mu.Unlock()
	s.mu.Lock()
	s.reads++
	s.inFlight++
	s.maxIn = max(s.maxIn, s.inFlight)
	if !pinned {
		s.sizedRead = true
		if s.inFlight > 1 {
			s.notAlone = true
		}
	}
	held := s.held
	if pinned && held != nil && s.inFlight >= s.hold {
		close(held)
		s.held = nil
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()
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
	if off >= int64(len(o.data)) {
		return 0, io.EOF
	}
	n := copy(p, o.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// TestCompositionMatrix runs every order of Root, Limit, Batch, KnownSizes
// and Pin (or Restore) over one store, for Open, OpenSized, ReadAt,
// ReadRanges and ReadObject on an unchanged object, a replaced one (same
// size), a deleted one and one opened at a wrong size. Every order is
// supported. It checks the classification (ErrObjectChanged once a pin is
// recorded or restored, *SizeMismatchError for a wrong size before one,
// success otherwise), that a batch follows Batch's policy (10 requests of
// 10 bytes), that a sized Object's first request is alone, and that no
// more requests than the limit are in flight while exactly that many are
// reached.
func TestCompositionMatrix(t *testing.T) {
	const limit = 2
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
	data := bytes.Repeat([]byte("0123456789"), 400) // 4000 bytes
	ctx := context.Background()
	for _, perm := range perms {
		for _, restored := range []bool{false, true} {
			for _, scenario := range []string{"unchanged", "replaced", "deleted", "wrongsize"} {
				if restored && scenario == "wrongsize" {
					continue // a restored pin is recorded: covered as a change below
				}
				for _, op := range []string{"Open+ReadAt", "OpenSized+ReadAt", "Open+ReadRanges", "OpenSized+ReadRanges", "ReadObject"} {
					name := fmt.Sprintf("%s/restored=%v/%s/%s", strings.Join(perm, "<"), restored, scenario, op)
					t.Run(name, func(t *testing.T) {
						runMatrixCase(t, ctx, perm, restored, scenario, op, data, limit)
					})
				}
			}
		}
	}
}

func runMatrixCase(t *testing.T, ctx context.Context, perm []string, restored bool, scenario, op string, data []byte, limit int) {
	s := newMstore()
	s.put("ds/obj", data)
	size := int64(len(data))
	given := size // the size the reader holds
	if scenario == "wrongsize" {
		given = size + 1
	}
	// Built innermost first; names below Root carry its prefix.
	rootBelow := false
	var o objfile.Opener = s
	var pinned *objfile.Pinned
	pinName := func() string {
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
			o = objfile.KnownSizes(o, map[string]int64{pinName(): given})
		case "Pin":
			if restored {
				snap := fmt.Sprintf(`{"version":1,"scope":%q,"objects":{%q:{"size":%d,"identity":"1"}}}`, scope, pinName(), size)
				var err error
				if pinned, err = objfile.Restore(o, scope, []byte(snap)); err != nil {
					t.Fatal(err)
				}
			} else {
				pinned = objfile.Pin(o, scope)
			}
			o = pinned
		}
	}
	name := "obj" // Root is in every order: the outermost names are root-relative

	// A recorded pin, then the change.
	if scenario == "replaced" || scenario == "deleted" {
		if !restored {
			if _, _, err := objfile.ReadObject(ctx, o, name, 1<<20); err != nil {
				t.Fatalf("recording the pin: %v", err)
			}
		}
		if scenario == "replaced" {
			s.put("ds/obj", bytes.Clone(data))
		} else {
			s.remove("ds/obj")
		}
	}

	var err error
	var rs []objfile.Range
	switch op {
	case "ReadObject":
		var b []byte
		b, _, err = objfile.ReadObject(ctx, o, name, 1<<20)
		if err == nil && !bytes.Equal(b, data) {
			t.Fatal("ReadObject read the wrong bytes")
		}
	default:
		var f objfile.Object
		if strings.HasPrefix(op, "OpenSized") {
			f, err = o.OpenSized(ctx, name, given)
		} else {
			f, err = o.Open(ctx, name)
		}
		if err != nil {
			break
		}
		s.mu.Lock()
		s.reads, s.maxIn = 0, 0
		if strings.HasSuffix(op, "ReadRanges") { // the batch's pinned requests wait until limit are in flight
			s.hold, s.held = limit, make(chan struct{})
		}
		s.mu.Unlock()
		rctx, cancel := context.WithTimeout(ctx, 20*time.Second) // a deadlock fails, not hangs
		defer cancel()
		if strings.HasSuffix(op, "ReadRanges") {
			rs = []objfile.Range{{Off: 0, Buf: make([]byte, 100)}}
			err = objfile.ReadRanges(rctx, f, rs)
		} else {
			p := make([]byte, 10)
			if _, err = f.WithContext(rctx).ReadAt(p, 100); err == nil && !bytes.Equal(p, data[100:110]) {
				t.Fatal("ReadAt read the wrong bytes")
			}
		}
	}

	var sm *objfile.SizeMismatchError
	switch {
	case scenario == "unchanged":
		if err != nil {
			t.Fatalf("unchanged object: %v", err)
		}
	case scenario == "replaced" || scenario == "deleted":
		if !errors.Is(err, objfile.ErrObjectChanged) || errors.As(err, &sm) {
			t.Fatalf("%s object: %v; want ErrObjectChanged", scenario, err)
		}
	case scenario == "wrongsize":
		wrong := op != "ReadObject" // Open reaches KnownSizes, which opens at the size it holds
		if wrong && (!errors.As(err, &sm) || sm.Expected != given || sm.Actual != int64(len(data)) || errors.Is(err, objfile.ErrObjectChanged)) {
			t.Fatalf("object of another size than given: %v; want a SizeMismatchError of %d, not %d", err, given, len(data))
		}
		if !wrong && err != nil {
			t.Fatalf("object opened without the wrong size: %v", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.notAlone {
		t.Fatal("a read of an unpinned Object was made beside another")
	}
	if strings.HasSuffix(op, "ReadRanges") && err == nil {
		if s.reads != 10 || s.maxIn != limit {
			t.Fatalf("batch: %d requests, %d in flight at most; want 10 under Batch's policy and %d under Limit", s.reads, s.maxIn, limit)
		}
	}
	if s.maxIn > limit {
		t.Fatalf("%d requests in flight, over the limit of %d", s.maxIn, limit)
	}
}
