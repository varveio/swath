// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/schema"
)

// withHandles sets the handle cache's cap for a test.
func withHandles(t *testing.T, n int) {
	prev := maxHandles
	maxHandles = n
	t.Cleanup(func() { maxHandles = prev })
}

// handleFile writes a spill file of frames frames of rows rows each (keys
// name/frame-row, payloads derived from them) and its trailer, and returns
// it with its frames' places.
func handleFile(t *testing.T, s *spiller[tp], name string, frames, rows int) (*spillFile, []spillAt) {
	t.Helper()
	f := s.file()
	var at []spillAt
	for i := range frames {
		if err := s.write(f, handleRows(name, i, rows), false); err != nil {
			t.Fatal(err)
		}
		at = append(at, spillAt{off: f.lastOff, size: f.lastSize, rows: f.lastRows})
	}
	if err := s.write(f, nil, true); err != nil {
		t.Fatal(err)
	}
	return f, at
}

func handleRows(name string, frame, n int) []schema.Row[tp] {
	rows := make([]schema.Row[tp], n)
	for j := range rows {
		k := fmt.Sprintf("%s/%03d-%04d", name, frame, j)
		rows[j] = schema.Row[tp]{Key: k, Data: tp{Size: int64(frame*1000 + j), ETag: "e-" + k}}
	}
	return rows
}

// sameRows requires got to be want, key and payload.
func sameRows(t *testing.T, what string, got, want []schema.Row[tp]) {
	t.Helper()
	if !slices.EqualFunc(got, want, func(a, b schema.Row[tp]) bool { return a.Key == b.Key && a.Data == b.Data }) {
		t.Fatalf("%s: %d rows differ from the %d written", what, len(got), len(want))
	}
}

// TestHandlesShareOneDescriptor: every frame read at an offset and the
// trailer read of one file use one descriptor: one open, then hits; the
// file drained retires it (closed, the file removed).
func TestHandlesShareOneDescriptor(t *testing.T) {
	s, err := newSpiller(&testSchema, t.TempDir(), false, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	f, at := handleFile(t, s, "a", 3, 50)
	writes := s.opens.Load()
	for i, a := range at {
		rows, _, err := s.readAt(f, a.off, a.size)
		if err != nil {
			t.Fatal(err)
		}
		sameRows(t, fmt.Sprintf("frame %d", i), rows, handleRows("a", i, 50))
	}
	if n, err := s.trailerRows(f); err != nil || n != 150 {
		t.Fatalf("trailer: %d %v", n, err)
	}
	st := s.h.stats()
	if opens := s.opens.Load() - writes; opens != 1 || st.Misses != 1 || st.Hits != 3 || s.h.openFDs() != 1 {
		t.Fatalf("%d read opens, %+v, %d open", opens, st, s.h.openFDs())
	}
	if err := s.drained(f); err != nil {
		t.Fatal(err)
	}
	if st := s.h.stats(); st.Evictions.Retired != 1 || s.h.openFDs() != 0 {
		t.Fatalf("after the drain: %+v, %d open", st, s.h.openFDs())
	}
	if _, err := os.Stat(f.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drained file: %v", err)
	}
}

// TestHandlesConcurrentReadsAndEviction: 16 goroutines read random frames
// and trailers of 24 files through a cache of 2 descriptors while another
// retires random files' handles (as a truncation does) and a third drops
// and reopens a sequential reader: leases race their opens, evictions and
// retirements, and every read is byte-exact (no descriptor is read after
// its close: a closed one fails the read). With every lease released at
// most the cap is open, and none once the cache stops.
func TestHandlesConcurrentReadsAndEviction(t *testing.T) {
	withHandles(t, 2)
	s, err := newSpiller(&testSchema, t.TempDir(), false, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	type file struct {
		f  *spillFile
		at []spillAt
	}
	var files []file
	for i := range 24 {
		f, at := handleFile(t, s, fmt.Sprintf("f%02d", i), 4, 20)
		files = append(files, file{f, at})
	}
	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(1)
	go func() { // retire handles under the readers
		defer bg.Done()
		r := rand.New(rand.NewPCG(1, 2))
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.h.retire(files[r.IntN(len(files))].f, evictTruncated)
		}
	}()
	var wg sync.WaitGroup
	errc := make(chan error, 16)
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 9))
			for range 400 {
				i := r.IntN(len(files))
				fl := files[i]
				if r.IntN(5) == 0 {
					if n, err := s.trailerRows(fl.f); err != nil || n != 80 {
						errc <- fmt.Errorf("file %d trailer: %d %v", i, n, err)
						return
					}
					continue
				}
				k := r.IntN(len(fl.at))
				rows, _, err := s.readAt(fl.f, fl.at[k].off, fl.at[k].size)
				if err != nil {
					errc <- fmt.Errorf("file %d frame %d: %v", i, k, err)
					return
				}
				want := handleRows(fmt.Sprintf("f%02d", i), k, 20)
				if !slices.EqualFunc(rows, want, func(a, b schema.Row[tp]) bool { return a.Key == b.Key && a.Data == b.Data }) {
					errc <- fmt.Errorf("file %d frame %d: rows differ", i, k)
					return
				}
			}
		}()
	}
	// The drainer's sequential reader on one file, dropped and reopened
	// at its position between frames, beside the others' leases.
	seq := files[0].f
	for k := range 4 {
		rows, err := s.next(seq)
		if err != nil {
			t.Fatal(err)
		}
		sameRows(t, fmt.Sprintf("sequential frame %d", k), rows, handleRows("f00", k, 20))
		s.closeFile(seq)
	}
	wg.Wait()
	close(stop)
	bg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	st := s.h.stats()
	if n := s.h.openFDs(); n > maxHandles {
		t.Fatalf("%d descriptors open with no lease, cap %d (%+v)", n, maxHandles, st)
	}
	if st.Hits == 0 || st.Evictions.Capacity == 0 || st.Evictions.Truncated == 0 {
		t.Fatalf("the cache was not exercised: %+v", st)
	}
	s.close()
	if n := s.h.openFDs(); n != 0 {
		t.Fatalf("%d descriptors open after close", n)
	}
	t.Logf("%+v", st)
}

// TestHandlesLowFD: an open refused for want of descriptors (EMFILE)
// closes every idle handle and is retried once; from then on the cache
// keeps none (one open per read, as before it), and reads go on exact.
func TestHandlesLowFD(t *testing.T) {
	s, err := newSpiller(&testSchema, t.TempDir(), false, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	var files []*spillFile
	var ats [][]spillAt
	for i := range 6 {
		f, at := handleFile(t, s, fmt.Sprintf("g%d", i), 2, 5)
		files, ats = append(files, f), append(ats, at)
	}
	for i := range 5 { // five idle handles
		if _, _, err := s.readAt(files[i], ats[i][0].off, ats[i][0].size); err != nil {
			t.Fatal(err)
		}
	}
	if n := s.h.openFDs(); n != 5 {
		t.Fatalf("%d open, want 5 idle", n)
	}
	var refused atomic.Bool
	hookOpen = func(*spillFile) error {
		if refused.CompareAndSwap(false, true) {
			return &os.PathError{Op: "open", Path: "x", Err: syscall.EMFILE}
		}
		return nil
	}
	t.Cleanup(func() { hookOpen = nil })
	rows, _, err := s.readAt(files[5], ats[5][1].off, ats[5][1].size)
	if err != nil {
		t.Fatal(err)
	}
	sameRows(t, "after the refusal", rows, handleRows("g5", 1, 5))
	st := s.h.stats()
	// The peak counts descriptors open, not the refused open's attempt.
	if st.Evictions.LowFD != 5 || s.h.openFDs() != 0 || st.PeakOpen != 5 {
		t.Fatalf("after EMFILE: %+v, %d open", st, s.h.openFDs())
	}
	for i := range 6 {
		if _, _, err := s.readAt(files[i], ats[i][1].off, ats[i][1].size); err != nil {
			t.Fatal(err)
		}
		if n := s.h.openFDs(); n != 0 {
			t.Fatalf("the fallback kept %d idle descriptors", n)
		}
	}
}

// TestHandlesStop: a failed output identity check (a replaced output)
// retires every handle at once, an idle one closed and a leased one at
// its release; a lease after it is closed at its release, never kept.
func TestHandlesStop(t *testing.T) {
	s, err := newSpiller(&testSchema, t.TempDir(), false, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	a, aat := handleFile(t, s, "a", 1, 3)
	b, bat := handleFile(t, s, "b", 1, 3)
	if _, _, err := s.readAt(a, aat[0].off, aat[0].size); err != nil {
		t.Fatal(err)
	}
	if _, err := s.next(b); err != nil { // b's reader holds a lease
		t.Fatal(err)
	}
	replaced := errors.New("the output was replaced")
	s.verify = func() error { return replaced }
	if err := s.check(); !errors.Is(err, replaced) {
		t.Fatal(err)
	}
	if st := s.h.stats(); st.Evictions.Closed != 2 || s.h.openFDs() != 1 {
		t.Fatalf("after the replacement: %+v, %d open", st, s.h.openFDs())
	}
	s.closeFile(b)
	if n := s.h.openFDs(); n != 0 {
		t.Fatalf("%d open after the last release", n)
	}
	if _, _, err := s.readAt(b, bat[0].off, bat[0].size); err != nil {
		t.Fatal(err)
	}
	if n := s.h.openFDs(); n != 0 {
		t.Fatalf("a lease after the stop kept its descriptor: %d open", n)
	}
}

// TestHandlesTruncation: a frame torn by ENOSPC is truncated away; the
// file's read handle, opened before, is retired with it, and the frames
// written after it read back exact through a new one.
func TestHandlesTruncation(t *testing.T) {
	s, err := newSpiller(&testSchema, t.TempDir(), false, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	f := s.file()
	if err := s.write(f, handleRows("t", 0, 30), false); err != nil {
		t.Fatal(err)
	}
	first := spillAt{off: f.lastOff, size: f.lastSize}
	if _, _, err := s.readAt(f, first.off, first.size); err != nil {
		t.Fatal(err)
	}
	restore := faults.SetIO(func(op, _ string, b []byte) (int, error) {
		if op == "write" {
			return len(b) / 2, syscall.ENOSPC
		}
		return 0, nil
	})
	err = s.write(f, handleRows("t", 1, 30), false)
	restore()
	if !errors.Is(err, errSpillFull) {
		t.Fatalf("torn frame: %v", err)
	}
	if st := s.h.stats(); st.Evictions.Truncated != 1 || s.h.openFDs() != 0 {
		t.Fatalf("after the truncation: %+v, %d open", st, s.h.openFDs())
	}
	if err := s.write(f, handleRows("t", 2, 30), false); err != nil {
		t.Fatal(err)
	}
	second := spillAt{off: f.lastOff, size: f.lastSize}
	if err := s.write(f, nil, true); err != nil {
		t.Fatal(err)
	}
	for i, a := range []spillAt{first, second} {
		rows, _, err := s.readAt(f, a.off, a.size)
		if err != nil {
			t.Fatal(err)
		}
		sameRows(t, fmt.Sprint("frame ", i), rows, handleRows("t", 2*i, 30))
	}
	if n, err := s.trailerRows(f); err != nil || n != 60 {
		t.Fatalf("trailer: %d %v", n, err)
	}
}

// TestHandlesConserveRows: random trees whose frontier is held until the
// rest has spilled, with handle caps from none to the default, the
// read-ahead on, and every frame format: the output is the oracle's, key
// and payload, row for row; every frame is replayed once; every handle is
// closed when the run returns; and with a cache the trailers share the
// frames' descriptors (fewer read opens than frames plus files).
func TestHandlesConserveRows(t *testing.T) {
	withAhead(t)
	for _, n := range []int{0, 1, 3, MaxSpillReaders} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			withHandles(t, n)
			var hits int64
			for seed := range uint64(8) {
				r := rand.New(rand.NewPCG(seed, 11))
				tr := genTree(r)
				g := &Gauges{}
				opt := Options{Workers: 2 + r.IntN(15), Budget: int64(50 + r.IntN(3000)), SpillDir: t.TempDir(),
					SpillCompress: r.IntN(2) == 0, SpillPlain: r.IntN(3) == 0, Gauges: g}
				c := &collect{}
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				st, err := Run[*rnode, tp](ctx, tr, c, &testSchema, opt)
				cancel()
				if err != nil {
					t.Fatalf("seed %d: %v", seed, err)
				}
				want := tr.keys()
				if !slices.Equal(c.keys, want) {
					t.Fatalf("seed %d: %d keys, want %d", seed, len(c.keys), len(want))
				}
				for i, k := range want { // the payload rtree wrote for k
					j := 0
					fmt.Sscanf(k[len(k)-5:], "%d", &j)
					if d := c.data[i]; d.Size != int64(j) || d.ETag != k[:len(k)-8] {
						t.Fatalf("seed %d: row %d (%s) carries %+v", seed, i, k, d)
					}
				}
				replayed(t, st)
				if n := g.readers.n.Load(); n != 0 {
					t.Fatalf("seed %d: %d descriptors open after the run", seed, n)
				}
				h := st.SpillHandles
				if p := h.PeakOpen; p > int64(MaxSpillReaders) {
					t.Fatalf("seed %d: %d descriptors at once", seed, p)
				}
				hits += h.Hits
			}
			if n > 0 && hits == 0 {
				t.Fatal("no lease ever hit")
			}
		})
	}
}

// TestHandlesConserveGroups: in group mode (consecutive rows sharing a
// key, as versions do) behind a held frontier, with a one-handle cache,
// the output is the oracle's, key and payload.
func TestHandlesConserveGroups(t *testing.T) {
	withAhead(t)
	withHandles(t, 1)
	f := &frontier{slowPages: 3, siblings: 40, sibPages: 6, rows: 60, group: 4}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	c := &collect{}
	st, err := Run[fr, tp](context.Background(), f, c, &testSchema, Options{Workers: 8, Budget: 400, SpillDir: t.TempDir(), Groups: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := f.keys(); !slices.Equal(c.keys, want) {
		t.Fatalf("%d keys, want %d", len(c.keys), len(want))
	}
	// The frontier's payload: Size is the row's index on its page.
	for i, d := range c.data {
		if d.ETag != "e" || d.Size < 0 || d.Size >= int64(f.rows) {
			t.Fatalf("row %d carries %+v", i, d)
		}
	}
	if st.SpillFrames == 0 {
		t.Fatal("nothing spilled")
	}
	replayed(t, st)
}

// TestHandlesRetiredDuringOpen: a file retired while a lease's open of it
// is under way: the read is served, and its descriptor closed at the
// release, never kept.
func TestHandlesRetiredDuringOpen(t *testing.T) {
	s, err := newSpiller(&testSchema, t.TempDir(), false, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	f, at := handleFile(t, s, "r", 1, 5)
	hookOpen = func(g *spillFile) error {
		s.h.retire(g, evictRetired)
		return nil
	}
	t.Cleanup(func() { hookOpen = nil })
	rows, _, err := s.readAt(f, at[0].off, at[0].size)
	if err != nil {
		t.Fatal(err)
	}
	sameRows(t, "retired during the open", rows, handleRows("r", 0, 5))
	if n := s.h.openFDs(); n != 0 {
		t.Fatalf("%d descriptors kept for a retired file", n)
	}
}

// TestHandlesWriteRetryOnce: a frame's write open refused for want of
// descriptors is retried once after the idle read handles are closed; a
// second refusal fails the frame (nothing written, nothing retried again).
func TestHandlesWriteRetryOnce(t *testing.T) {
	s, err := newSpiller(&testSchema, t.TempDir(), false, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	g, at := handleFile(t, s, "idle", 1, 3)
	if _, _, err := s.readAt(g, at[0].off, at[0].size); err != nil {
		t.Fatal(err)
	}
	var tries atomic.Int64
	refuse := int64(1)
	hookOpenWrite = func(*spillFile) error {
		if tries.Add(1) <= refuse {
			return &os.PathError{Op: "open", Path: "x", Err: syscall.EMFILE}
		}
		return nil
	}
	t.Cleanup(func() { hookOpenWrite = nil })
	f := s.file()
	if err := s.write(f, handleRows("w", 0, 3), false); err != nil || tries.Load() != 2 {
		t.Fatalf("one refusal: %v after %d opens", err, tries.Load())
	}
	if st := s.h.stats(); st.Evictions.LowFD != 1 || s.h.openFDs() != 0 {
		t.Fatalf("%+v, %d open", st, s.h.openFDs())
	}
	tries.Store(0)
	refuse = 2
	size := f.size
	if err := s.write(f, handleRows("w", 1, 3), false); !errors.Is(err, syscall.EMFILE) || tries.Load() != 2 || f.size != size {
		t.Fatalf("two refusals: %v after %d opens, size %d -> %d", err, tries.Load(), size, f.size)
	}
}
