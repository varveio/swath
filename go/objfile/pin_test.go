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
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/objfile"
)

// memStore is an Opener of objects held in memory, each at a generation that
// is its Identity. With churn set, every Open replaces the object first.
type memStore struct {
	mu      sync.Mutex
	objects map[string]memObject
	gen     int
	churn   bool
	// With gate set, Open sends on entered, then waits for gate to close
	// before it reads the object.
	gate    chan struct{}
	entered chan struct{}
	opens   atomic.Int32
	closes  atomic.Int32
}

type memObject struct {
	data []byte
	gen  int
}

func newMemStore() *memStore { return &memStore{objects: map[string]memObject{}} }

func (s *memStore) put(name string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen++
	s.objects[name] = memObject{data: data, gen: s.gen}
}

func (s *memStore) Open(_ context.Context, name string) (objfile.Object, error) {
	s.opens.Add(1)
	if s.gate != nil {
		s.entered <- struct{}{}
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[name]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	if s.churn {
		s.gen++
		o.gen = s.gen
		s.objects[name] = o
	}
	return &memFile{Reader: bytes.NewReader(o.data), id: fmt.Sprint(o.gen), s: s}, nil
}

func (s *memStore) OpenSized(ctx context.Context, name string, _ int64) (objfile.Object, error) {
	return s.Open(ctx, name)
}

type memFile struct {
	*bytes.Reader
	id string
	s  *memStore
}

func (f *memFile) Identity() string                        { return f.id }
func (f *memFile) Close() error                            { f.s.closes.Add(1); return nil }
func (f *memFile) WithContext(context.Context) io.ReaderAt { return f.Reader }

const scope = "s3://bkt/datasets/ds/"

func TestPinRecordsFirstOpen(t *testing.T) {
	ctx := context.Background()
	s := newMemStore()
	s.put("datasets/ds/manifest.json", []byte("m1"))
	s.put("datasets/ds/data/p", []byte("part"))
	p := objfile.Pin(objfile.Root(objfile.Limit(s, 2), "datasets/ds"), scope)
	for range 2 { // a reopen of the same version passes
		for _, name := range []string{"manifest.json", "data/p"} {
			f, err := p.Open(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			f.Close()
		}
	}
	snap, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"version":1,"scope":"s3://bkt/datasets/ds/","objects":{"data/p":{"size":4,"identity":"2"},"manifest.json":{"size":2,"identity":"1"}}}`
	if string(snap) != want {
		t.Fatalf("Snapshot = %s\nwant       %s", snap, want)
	}

	// A replaced object fails, of the same size too, and its pin stays.
	s.put("datasets/ds/manifest.json", []byte("m2"))
	if _, err := p.Open(ctx, "manifest.json"); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("Open of a replaced object: %v, want ErrObjectChanged", err)
	}
	if again, _ := p.Snapshot(); !bytes.Equal(again, snap) {
		t.Fatalf("a failed Open replaced a pin: %s", again)
	}
	// A deleted one is changed too, and still not-exist.
	s.mu.Lock()
	delete(s.objects, "datasets/ds/data/p")
	s.mu.Unlock()
	if _, err := p.Open(ctx, "data/p"); !errors.Is(err, objfile.ErrObjectChanged) || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open of a deleted object: %v, want ErrObjectChanged and fs.ErrNotExist", err)
	}
	if _, err := p.Open(ctx, "../x"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Open(../x) = %v, want fs.ErrInvalid", err)
	}
}

// TestPinConcurrentFirstOpens: first opens that all reach the store before
// any returns, each finding another version: one records its pin, and the
// others fail with ErrObjectChanged and close their objects.
func TestPinConcurrentFirstOpens(t *testing.T) {
	const n = 16
	s := newMemStore()
	s.put("obj", []byte("data"))
	s.churn = true
	s.gate, s.entered = make(chan struct{}), make(chan struct{}, n)
	p := objfile.Pin(s, scope)
	var ok, changed atomic.Int32
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			_, err := p.Open(context.Background(), "obj")
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, objfile.ErrObjectChanged):
				changed.Add(1)
			default:
				t.Error(err)
			}
		})
	}
	for range n {
		<-s.entered
	}
	if snap, _ := p.Snapshot(); !bytes.Contains(snap, []byte(`"objects":{}`)) {
		t.Fatalf("a pin was recorded before any open returned: %s", snap)
	}
	close(s.gate)
	wg.Wait()
	if ok.Load() != 1 || changed.Load() != n-1 || s.closes.Load() != n-1 {
		t.Fatalf("%d opened, %d changed, %d closed; want 1, %d, %d", ok.Load(), changed.Load(), s.closes.Load(), n-1, n-1)
	}
}

func TestRestore(t *testing.T) {
	ctx := context.Background()
	s := newMemStore()
	s.put("ds/manifest.json", []byte("m1"))
	s.put("ds/other", []byte("x"))
	o := objfile.Root(s, "ds")
	p := objfile.Pin(o, scope)
	if _, err := p.Open(ctx, "manifest.json"); err != nil {
		t.Fatal(err)
	}
	snap, _ := p.Snapshot()

	r, err := objfile.Restore(o, scope, snap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Open(ctx, "manifest.json"); err != nil {
		t.Fatalf("restored Open of the unchanged object: %v", err)
	}
	// Sealed: a name the snapshot lacks fails before any request.
	before := s.opens.Load()
	if _, err := r.Open(ctx, "other"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("restored Open of an unpinned name: %v, want fs.ErrNotExist", err)
	}
	if s.opens.Load() != before {
		t.Fatal("an unpinned name reached the store")
	}
	if again, _ := r.Snapshot(); !bytes.Equal(again, snap) {
		t.Fatalf("restored Snapshot = %s, want %s", again, snap)
	}
	// A restart after the object was replaced.
	s.put("ds/manifest.json", []byte("m1"))
	r, err = objfile.Restore(o, scope, snap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Open(ctx, "manifest.json"); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("restored Open of a replaced object: %v, want ErrObjectChanged", err)
	}

	if _, err := objfile.Restore(o, "gs://bkt/datasets/ds/", snap); !errors.Is(err, objfile.ErrInvalidSnapshot) {
		t.Fatalf("Restore for another scope: %v, want ErrInvalidSnapshot", err)
	}
}

func TestRestoreRefusesMalformed(t *testing.T) {
	const pin = `{"size":2,"identity":"1"}`
	obj := func(objects string) string {
		return `{"version":1,"scope":"` + scope + `","objects":{` + objects + `}}`
	}
	valid := []string{
		obj(``),
		obj(`"a":` + pin),
		` {"objects":{"a/b":{"identity":"x","size":0}},"scope":"` + scope + `","version":1} `,
	}
	for _, snap := range valid {
		if _, err := objfile.Restore(newMemStore(), scope, []byte(snap)); err != nil {
			t.Errorf("Restore(%s) = %v", snap, err)
		}
	}
	invalid := []string{
		``,
		`[]`,
		`{"version":1,"scope":"` + scope + `"}`,
		`{"version":2,"scope":"` + scope + `","objects":{}}`,
		`{"version":"1","scope":"` + scope + `","objects":{}}`,
		`{"version":1.0,"scope":"` + scope + `","objects":{}}`,
		`{"version":1,"version":1,"scope":"` + scope + `","objects":{}}`,
		`{"Version":1,"scope":"` + scope + `","objects":{}}`,
		`{"version":1,"scope":"` + scope + `","objects":{},"extra":0}`,
		obj(``) + `{}`,
		obj(`"a":` + pin + `,"a":` + pin),
		obj(`"../a":` + pin),
		obj(`"/a":` + pin),
		obj(`"a":{"size":-1,"identity":"1"}`),
		obj(`"a":{"size":1}`),
		obj(`"a":{"identity":"1"}`),
		obj(`"a":{"size":1,"identity":""}`),
		obj(`"a":{"size":1,"identity":"a\nb"}`),
		obj(`"a":{"size":1,"identity":"1","size":1}`),
		obj(`"a":{"size":1,"identity":"1","etag":"x"}`),
		obj(`"a":null`),
		strings.TrimSuffix(obj(`"a":`+pin), "}"),
	}
	for _, snap := range invalid {
		if _, err := objfile.Restore(newMemStore(), scope, []byte(snap)); !errors.Is(err, objfile.ErrInvalidSnapshot) {
			t.Errorf("Restore(%s) = %v, want ErrInvalidSnapshot", snap, err)
		}
	}
}

// TestSnapshotDeterministic: the same pins, recorded in another order, give
// the same bytes.
func TestSnapshotDeterministic(t *testing.T) {
	names := []string{"z", "a/b", "m<&>", "a"}
	var snaps [][]byte
	for _, order := range [][]int{{0, 1, 2, 3}, {3, 2, 1, 0}} {
		s := newMemStore()
		for _, name := range names {
			s.put(name, []byte(name))
		}
		p := objfile.Pin(s, scope)
		for _, i := range order {
			if _, err := p.Open(context.Background(), names[i]); err != nil {
				t.Fatal(err)
			}
		}
		snap, err := p.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		snaps = append(snaps, snap)
	}
	if !bytes.Equal(snaps[0], snaps[1]) {
		t.Fatalf("snapshots differ:\n%s\n%s", snaps[0], snaps[1])
	}
	if _, err := objfile.Restore(newMemStore(), scope, snaps[0]); err != nil {
		t.Fatalf("Restore of a snapshot: %v", err)
	}
}

// sizedStore is memStore with OpenSized, ReadObject and counting: an
// OpenSized Object pins at its first read, and reports that version's
// size; opens counts metadata requests (Open), reads every other request.
type sizedStore struct {
	*memStore
	reads atomic.Int32
}

func (s *sizedStore) OpenSized(_ context.Context, name string, size int64) (objfile.Object, error) {
	return &lazyFile{s: s, name: name, size: size}, nil
}

func (s *sizedStore) ReadObject(_ context.Context, name string, limit int64) ([]byte, string, error) {
	s.reads.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[name]
	if !ok {
		return nil, "", &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
	}
	if int64(len(o.data)) > limit {
		return nil, fmt.Sprint(o.gen), objfile.ErrTooLarge
	}
	return bytes.Clone(o.data), fmt.Sprint(o.gen), nil
}

// lazyFile reads the live object; its first read pins it (a size other
// than the one given is ErrObjectChanged), later ones must find it.
type lazyFile struct {
	s    *sizedStore
	name string
	size int64
	mu   sync.Mutex
	id   string
}

func (f *lazyFile) ReadAt(p []byte, off int64) (int, error) {
	f.s.reads.Add(1)
	f.s.mu.Lock()
	o, ok := f.s.objects[f.name]
	f.s.mu.Unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case !ok && f.id == "":
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrNotExist}
	case !ok, f.id != "" && f.id != fmt.Sprint(o.gen), int64(len(o.data)) != f.size:
		return 0, objfile.ErrObjectChanged
	}
	f.id = fmt.Sprint(o.gen)
	return bytes.NewReader(o.data).ReadAt(p, off)
}

func (f *lazyFile) Identity() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.id
}
func (f *lazyFile) Size() int64                             { return f.size }
func (f *lazyFile) Close() error                            { return nil }
func (f *lazyFile) WithContext(context.Context) io.ReaderAt { return f }

// TestPinOpenSized: Pinned.OpenSized makes no request; the first read
// records the pin, or must find the recorded one, before any byte is
// returned; a recorded pin of another size fails at once.
func TestPinOpenSized(t *testing.T) {
	ctx := context.Background()
	s := &sizedStore{memStore: newMemStore()}
	s.put("data/p", []byte("part"))
	p := objfile.Pin(s, scope)
	f, err := p.OpenSized(ctx, "data/p", 4)
	if err != nil || s.opens.Load() != 0 || s.reads.Load() != 0 {
		t.Fatalf("OpenSized: %v after %d opens, %d reads", err, s.opens.Load(), s.reads.Load())
	}
	b := make([]byte, 4)
	if _, err := f.ReadAt(b, 0); err != nil || string(b) != "part" {
		t.Fatalf("first read: %v, %q", err, b)
	}
	if snap, _ := p.Snapshot(); !strings.Contains(string(snap), `"data/p":{"size":4,"identity":"1"}`) {
		t.Fatalf("Snapshot after a sized read: %s", snap)
	}

	// A reopen re-proves the current object: a replacement of the same
	// size fails its first read, and returns no bytes.
	s.put("data/p", []byte("PART"))
	g, err := p.OpenSized(ctx, "data/p", 4)
	if err != nil {
		t.Fatal(err)
	}
	clear(b)
	if n, err := g.ReadAt(b, 0); !errors.Is(err, objfile.ErrObjectChanged) || n != 0 {
		t.Fatalf("read of a replaced object = %d, %v; want 0, ErrObjectChanged", n, err)
	}
	rs := []objfile.Range{{Off: 0, Buf: make([]byte, 2)}}
	if err := objfile.ReadRanges(ctx, g, rs); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("batch of a replaced object = %v; want ErrObjectChanged", err)
	}
	if _, err := p.OpenSized(ctx, "data/p", 5); !errors.Is(err, objfile.ErrObjectChanged) || s.reads.Load() != 3 {
		t.Fatalf("OpenSized of a pinned name at another size = %v", err)
	}

	// Deleted: changed, and not-exist.
	s.mu.Lock()
	delete(s.objects, "data/p")
	s.mu.Unlock()
	h, _ := p.OpenSized(ctx, "data/p", 4)
	if _, err := h.ReadAt(b, 0); !errors.Is(err, objfile.ErrObjectChanged) || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("read of a deleted pinned object = %v", err)
	}

	// Sealed: an unrecorded name fails without a request.
	snap, _ := p.Snapshot()
	r, err := objfile.Restore(s, scope, snap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.OpenSized(ctx, "data/q", 4); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("OpenSized of a name a sealed snapshot lacks = %v", err)
	}
}

// TestPinReadObject: a whole-object read through Pinned records the pin,
// re-proves it, and keeps ErrTooLarge distinct from a change.
func TestPinReadObject(t *testing.T) {
	ctx := context.Background()
	s := &sizedStore{memStore: newMemStore()}
	s.put("manifest.json", []byte("m1"))
	p := objfile.Pin(objfile.Root(objfile.Limit(s, 2), ""), scope)
	for range 2 {
		b, id, err := objfile.ReadObject(ctx, p, "manifest.json", 10)
		if err != nil || string(b) != "m1" || id != "1" {
			t.Fatalf("ReadObject = %q, %q, %v", b, id, err)
		}
	}
	if s.opens.Load() != 0 || s.reads.Load() != 2 {
		t.Fatalf("%d metadata requests and %d reads, want 0 and 2", s.opens.Load(), s.reads.Load())
	}
	if _, id, err := objfile.ReadObject(ctx, p, "manifest.json", 1); !errors.Is(err, objfile.ErrTooLarge) || id != "1" {
		t.Fatalf("ReadObject over the limit of a pinned object = %q, %v", id, err)
	}
	s.put("manifest.json", []byte("m2"))
	if _, _, err := objfile.ReadObject(ctx, p, "manifest.json", 10); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("ReadObject of a replaced object = %v", err)
	}
}

// TestPinOpenSizedZero: a size of 0 is checked against the object found.
func TestPinOpenSizedZero(t *testing.T) {
	ctx := context.Background()
	s := &sizedStore{memStore: newMemStore()}
	s.put("full", []byte("data"))
	s.put("empty", nil)
	p := objfile.Pin(s, scope)
	var sm *objfile.SizeMismatchError
	if _, err := p.OpenSized(ctx, "full", 0); !errors.As(err, &sm) || sm.Actual != 4 || errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("OpenSized(0) of a non-empty object = %v, want a SizeMismatchError of 4 bytes", err)
	}
	// That Open recorded the object's pin: now 0 is a change.
	if _, err := p.OpenSized(ctx, "full", 0); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("OpenSized(0) of a pinned non-empty object = %v, want ErrObjectChanged", err)
	}
	if f, err := p.OpenSized(ctx, "empty", 0); err != nil || f.Size() != 0 {
		t.Fatalf("OpenSized(0) of an empty object = %v", err)
	}
	if _, err := p.OpenSized(ctx, "full", -1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("OpenSized(-1) = %v, want fs.ErrInvalid", err)
	}
}

// failingStore opens objects whose reads fail with an error that is not
// comparable.
type failingStore struct{ *memStore }

func (s failingStore) OpenSized(context.Context, string, int64) (objfile.Object, error) {
	return failingFile{}, nil
}

type failingFile struct{}

func (failingFile) ReadAt([]byte, int64) (int, error)         { return 0, uncomparable{"x"} }
func (failingFile) Size() int64                               { return 4 }
func (failingFile) Identity() string                          { return "" }
func (failingFile) Close() error                              { return nil }
func (f failingFile) WithContext(context.Context) io.ReaderAt { return f }

type uncomparable []string

func (uncomparable) Error() string { return "uncomparable" }

// TestPinUncomparableError: a read error that cannot be compared passes
// through a Pinned object as it is.
func TestPinUncomparableError(t *testing.T) {
	p := objfile.Pin(failingStore{newMemStore()}, scope)
	f, err := p.OpenSized(context.Background(), "obj", 4)
	if err != nil {
		t.Fatal(err)
	}
	var u uncomparable
	if _, err := f.ReadAt(make([]byte, 4), 0); !errors.As(err, &u) {
		t.Fatalf("ReadAt = %v", err)
	}
	if err := objfile.ReadRanges(context.Background(), f, []objfile.Range{{Off: 0, Buf: make([]byte, 4)}}); !errors.As(err, &u) {
		t.Fatalf("ReadRanges = %v", err)
	}
}
