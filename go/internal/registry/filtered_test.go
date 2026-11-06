// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// retPayload has the S3 payload's layout: two int64s and six strings.
type retPayload struct {
	Size, Mtime      int64
	A, B, C, D, E, F string
}

type retCodec struct{}

func (retCodec) Append(dst []byte, p *retPayload) []byte {
	dst = binary.AppendVarint(binary.AppendVarint(dst, p.Size), p.Mtime)
	for _, s := range [...]string{p.A, p.B, p.C, p.D, p.E, p.F} {
		dst = schema.AppendString(dst, s)
	}
	return dst
}

func (retCodec) Decode(d *schema.Decoder, p *retPayload) {
	p.Size, p.Mtime = d.Varint(), d.Varint()
	p.A, p.B, p.C, p.D, p.E, p.F = d.Str(), d.Str(), d.Str(), d.Str(), d.Str(), d.Str()
}

var retSchema = schema.Schema[retPayload]{
	Heap: func(p *retPayload) int64 {
		return int64(len(p.A) + len(p.B) + len(p.C) + len(p.D) + len(p.E) + len(p.F))
	},
	Codec: retCodec{},
}

// retPage is one 1,000-row page whose keys and etags (A) view one shared
// arena, as an S3 page's do.
func retPage(prefix string) []schema.Row[retPayload] {
	unit := prefix + strings.Repeat("k", 64-len(prefix)) + strings.Repeat("e", 32)
	arena := strings.Repeat(unit, 1000)
	rows := make([]schema.Row[retPayload], 1000)
	for i := range rows {
		u := arena[i*96 : (i+1)*96]
		rows[i].Key, rows[i].Data.A = u[:64], u[64:]
		rows[i].Data.Size = int64(i)
	}
	return rows
}

func onePerPage(r *schema.Row[retPayload]) bool { return r.Data.Size == 0 }

type retSink struct{ pages [][]schema.Row[retPayload] }

func (s *retSink) Rows(r []schema.Row[retPayload]) error {
	s.pages = append(s.pages, r)
	return nil
}
func (s *retSink) Child(any) error { return nil }

func heapNow() int64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// plateau is the retained-heap bound for rows charged bytes: a small
// multiple of the charge plus fixed slack for the runtime and the test's
// own bookkeeping. The pages themselves are 400 × ~200 KiB = ~78 MiB.
func plateau(charged int64) int64 { return 3*charged + 256<<10 }

// TestSparseFilterReleasesPages: one survivor per 1,000-row page, 400
// pages, every output retained downstream. The survivors are detached, so
// the retained heap is a small multiple of what the budget is charged, not
// the 400 pages (R6: 75 MiB retained for 75 KiB charged).
func TestSparseFilterReleasesPages(t *testing.T) {
	before := heapNow()
	s := new(retSink)
	n := new(atomic.Int64)
	e := filterEmitter[retPayload]{s, onePerPage, &retSchema, n}
	for page := 0; page < 400; page++ {
		if err := e.Rows(retPage(fmt.Sprintf("p%06d", page))); err != nil {
			t.Fatal(err)
		}
	}
	growth := heapNow() - before
	var charged int64
	for _, rows := range s.pages {
		charged += retSchema.RowsBytes(rows)
	}
	t.Logf("kept=%d dropped=%d charged=%d retained_heap=%d first_cap=%d", len(s.pages), n.Load(), charged, growth, cap(s.pages[0]))
	if len(s.pages) != 400 || n.Load() != 399600 {
		t.Fatalf("kept %d pages, dropped %d", len(s.pages), n.Load())
	}
	if cap(s.pages[0]) != 1 {
		t.Errorf("survivor slice capacity %d, want 1", cap(s.pages[0]))
	}
	for i, rows := range s.pages {
		want := fmt.Sprintf("p%06d", i)
		if r := rows[0]; !strings.HasPrefix(r.Key, want) || len(r.Key) != 64 || r.Data.A != strings.Repeat("e", 32) || r.Data.Size != 0 {
			t.Fatalf("page %d: detached row changed: %+v", i, r)
		}
	}
	if growth > plateau(charged) {
		t.Errorf("retained heap %d B for %d B charged: the pages are still held", growth, charged)
	}
	runtime.KeepAlive(s)
}

// TestDenseFilterIsZeroCopy: when most rows survive the page is handed on
// in place: no row array or string is copied.
func TestDenseFilterIsZeroCopy(t *testing.T) {
	for _, tc := range []struct {
		name string
		keep func(*schema.Row[retPayload]) bool
		want int
	}{
		{"all", func(*schema.Row[retPayload]) bool { return true }, 1000},
		{"most", func(r *schema.Row[retPayload]) bool { return r.Data.Size%4 != 0 }, 750},
	} {
		s := new(retSink)
		e := filterEmitter[retPayload]{s, tc.keep, &retSchema, new(atomic.Int64)}
		rows := retPage("dense")
		keys := make([]string, len(rows))
		for i := range rows {
			keys[i] = rows[i].Key
		}
		if err := e.Rows(rows); err != nil {
			t.Fatal(err)
		}
		got := s.pages[0]
		if len(got) != tc.want {
			t.Fatalf("%s: %d rows, want %d", tc.name, len(got), tc.want)
		}
		if &got[0] != &rows[0] {
			t.Errorf("%s: the row array was copied", tc.name)
		}
		if unsafe.StringData(got[len(got)-1].Key) != unsafe.StringData(keys[len(keys)-1]) {
			t.Errorf("%s: a key string was cloned", tc.name)
		}
	}
}

// retProvider: range 1 (the frontier) blocks until released; range 2
// emits 400 pages, each with one survivor, which queue behind it.
type retProvider struct{ release, ready chan struct{} }

func (p *retProvider) Root() int { return 0 }
func (p *retProvider) Expand(ctx context.Context, r int, e source.Emitter[int, retPayload]) error {
	switch r {
	case 0:
		if err := e.Child(1); err != nil {
			return err
		}
		return e.Child(2)
	case 1:
		select {
		case <-p.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return e.Rows([]schema.Row[retPayload]{{Key: "a"}})
	}
	for page := 0; page < 400; page++ {
		if err := e.Rows(retPage(fmt.Sprintf("z%06d", page))); err != nil {
			return err
		}
	}
	close(p.ready)
	return nil
}

type retDiscard struct{}

func (retDiscard) Write(context.Context, [][]schema.Row[retPayload]) error { return nil }

// TestSparseFilterPipelinePlateau runs the production order stage with a
// blocked frontier: 400 sparse pages wait behind it. Their retained heap
// must stay within a small multiple of the bytes the budget is charged.
func TestSparseFilterPipelinePlateau(t *testing.T) {
	before := heapNow()
	p := &retProvider{make(chan struct{}), make(chan struct{})}
	n := new(atomic.Int64)
	f := newFiltered(source.Erase[int, retPayload](p), onePerPage, &retSchema, n)
	type result struct {
		st  order.Stats
		err error
	}
	done := make(chan result, 1)
	go func() {
		st, err := order.Run(context.Background(), f, retDiscard{}, &retSchema, order.Options{Workers: 2, Budget: 128 << 10})
		done <- result{st, err}
	}()
	<-p.ready
	growth := heapNow() - before
	close(p.release)
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Logf("rows=%d dropped=%d peak_accounted=%d retained_heap_while_frontier_blocked=%d", r.st.Rows, n.Load(), r.st.MaxBuffered, growth)
	if r.st.Rows != 401 || n.Load() != 399600 {
		t.Fatalf("rows %d dropped %d", r.st.Rows, n.Load())
	}
	if growth > plateau(r.st.MaxBuffered) {
		t.Errorf("retained heap %d B while %d B were charged: the sparse pages are still held", growth, r.st.MaxBuffered)
	}
}

// TestDetachRoundTrips: detached rows equal the originals and share no
// memory with them.
func TestDetachRoundTrips(t *testing.T) {
	rows := retPage("rt")[:3]
	rows[1].Data = retPayload{Size: -5, Mtime: 1 << 50, A: "a", B: "", C: "c", D: "dd", E: "\x00\xff", F: "f"}
	got := retSchema.Detach(rows)
	for i := range rows {
		if got[i] != rows[i] {
			t.Fatalf("row %d: %+v != %+v", i, got[i], rows[i])
		}
		if unsafe.StringData(got[i].Key) == unsafe.StringData(rows[i].Key) {
			t.Fatalf("row %d: key shares memory", i)
		}
	}
}
