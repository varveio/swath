// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// frontier is the step-3 adversarial shape: a slow, dense first range (the
// frontier) and many fast siblings after it, with long keys.
type frontier struct {
	slowPages, siblings, sibPages, rows int
	slowDelay, sibDelay                 time.Duration
	// hold, if set, holds the slow range after its first page until every
	// sibling has emitted all its rows (left counts them down).
	hold chan struct{}
	left *atomic.Int64
}

type fr struct{ i int } // -1: root, 0: the slow range, 1..: siblings

func (f *frontier) Root() fr { return fr{-1} }

func (f *frontier) key(i, page, j int) string {
	return fmt.Sprintf("r%04d/%0200d-%04d-%04d", i, 0, page, j) // about 220-byte keys
}

func (f *frontier) Expand(ctx context.Context, r fr, e source.Emitter[fr, tp]) error {
	if r.i < 0 {
		for i := 0; i <= f.siblings; i++ {
			if err := e.Child(fr{i}); err != nil {
				return err
			}
		}
		return nil
	}
	pages, delay := f.sibPages, f.sibDelay
	if r.i == 0 {
		pages, delay = f.slowPages, f.slowDelay
	}
	for p := range pages {
		time.Sleep(delay)
		rows := make([]schema.Row[tp], f.rows)
		for j := range rows {
			rows[j] = schema.Row[tp]{Key: f.key(r.i, p, j), Data: tp{Size: int64(j), ETag: "e"}}
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
		if f.hold != nil && r.i == 0 && p == 0 {
			select {
			case <-f.hold:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if f.hold != nil && r.i > 0 && f.left.Add(-1) == 0 {
		close(f.hold)
	}
	return nil
}

func (f *frontier) keys() []string {
	var out []string
	for i := 0; i <= f.siblings; i++ {
		pages := f.sibPages
		if i == 0 {
			pages = f.slowPages
		}
		for p := range pages {
			for j := range f.rows {
				out = append(out, f.key(i, p, j))
			}
		}
	}
	return out
}

// TestSpillKeepsWorkersBusy is the step-3 gate: with a reorder budget far
// below one sibling's output, spill lets the siblings list while the slow
// frontier holds the output back. The frontier is held after its first
// page until every sibling has listed all its rows (a barrier), so the run
// finishes only if the workers proceed while the frontier waits, which
// without spill they cannot (they wait on the budget); the output is
// identical, and rows in memory stay within the budget plus one batch per
// worker.
func TestSpillKeepsWorkersBusy(t *testing.T) {
	// A sibling's last page fills a publish batch (11 × 100 ≥ publishRows),
	// so it ends with nothing left to publish.
	f := &frontier{slowPages: 40, siblings: 56, sibPages: 11, rows: 100, slowDelay: 10 * time.Millisecond, sibDelay: 5 * time.Millisecond}
	want := f.keys()
	const workers, budget = 8, 500
	run := func(opt Options, held bool) Stats {
		t.Helper()
		c := &collect{}
		f.hold, f.left = nil, nil
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if held {
			f.hold, f.left = make(chan struct{}), &atomic.Int64{}
			f.left.Store(int64(f.siblings))
		}
		st, err := Run[fr, tp](ctx, f, c, &testSchema, opt)
		if held && errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%+v: the siblings did not finish while the frontier waited (spill did not keep workers busy)", opt)
		}
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(c.keys, want) {
			t.Fatalf("%+v: %d keys, want %d", opt, len(c.keys), len(want))
		}
		for i, d := range c.data { // payloads survive the spill codec
			if d != (tp{Size: int64(i % f.rows), ETag: "e"}) {
				t.Fatalf("%+v: row %d payload %+v", opt, i, d)
			}
		}
		return st
	}
	pst := run(Options{Workers: workers, Budget: budget}, false)
	dir := t.TempDir()
	for _, compress := range []bool{false, true} {
		st := run(Options{Workers: workers, Budget: budget, SpillDir: dir, SpillCompress: compress}, true)
		t.Logf("compress=%v: no spill (max buffered %d), spill (max buffered %d, %d rows in %d frames, %d bytes, %d files)",
			compress, pst.MaxBuffered, st.MaxBuffered, st.SpillRows, st.SpillFrames, st.SpillBytes, st.SpillFiles)
		if st.SpillRows == 0 {
			t.Fatal("nothing spilled")
		}
		// With spill and no disk refusal nothing waits on the budget: not
		// an emitter with rows (it spills), nor a range that ends with
		// nothing to publish.
		if st.BudgetWait != 0 {
			t.Fatalf("compress=%v: budget wait %v with spill", compress, st.BudgetWait)
		}
		// D1: the budget, plus one publish batch per worker, plus the
		// frontier path (here one range).
		if limit := int64(budget + workers*publishRows + f.rows); st.MaxBuffered > limit {
			t.Fatalf("max buffered %d > %d", st.MaxBuffered, limit)
		}
		if ents, _ := os.ReadDir(dir); len(ents) != 0 {
			t.Fatalf("spill files left: %d", len(ents))
		}
	}
	if limit := int64(budget + workers*publishRows + f.rows); pst.MaxBuffered > limit {
		t.Fatalf("no spill: max buffered %d > %d", pst.MaxBuffered, limit)
	}
	// The slow frontier makes the drainer wait for it, and without spill
	// the siblings wait on the full budget.
	t.Logf("no spill: frontier wait %v, budget wait %v", pst.FrontierWait, pst.BudgetWait)
	if pst.FrontierWait <= 0 || pst.BudgetWait <= 0 {
		t.Fatalf("no spill: frontier wait %v, budget wait %v", pst.FrontierWait, pst.BudgetWait)
	}
}

// TestSpillCleanedOnFailure: a failing run removes its spill directory.
func TestSpillCleanedOnFailure(t *testing.T) {
	f := &frontier{slowPages: 20, siblings: 20, sibPages: 5, rows: 100, slowDelay: 5 * time.Millisecond}
	dir := t.TempDir()
	c := &collect{fail: 2500}
	if _, err := Run[fr, tp](context.Background(), f, c, &testSchema, Options{Workers: 8, Budget: 300, SpillDir: dir}); err == nil {
		t.Fatal("sink failure not reported")
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("spill directory left after failure: %d entries", len(ents))
	}
}

// TestSpillDiskBudget: spill stays within its disk budget and backs off to
// waiting when the budget is used, with identical output and memory still
// bounded. A cap well below the spill a free run produces must be hit; a
// free-space reserve larger than any disk refuses every frame, so the run
// behaves as without spill.
func TestSpillDiskBudget(t *testing.T) {
	f := &frontier{slowPages: 40, siblings: 56, sibPages: 10, rows: 100, slowDelay: 5 * time.Millisecond}
	want := f.keys()
	const workers, budget = 8, 500
	run := func(opt Options) Stats {
		t.Helper()
		c := &collect{}
		st, err := Run[fr, tp](context.Background(), f, c, &testSchema, opt)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(c.keys, want) {
			t.Fatalf("%d keys, want %d", len(c.keys), len(want))
		}
		if limit := int64(budget + workers*publishRows + f.rows); st.MaxBuffered > limit {
			t.Fatalf("max buffered %d > %d", st.MaxBuffered, limit)
		}
		return st
	}
	dir := t.TempDir()
	free := run(Options{Workers: workers, Budget: budget, SpillDir: dir})
	if free.SpillRefused != 0 || free.SpillPeak == 0 {
		t.Fatalf("no budget: peak %d, refused %d", free.SpillPeak, free.SpillRefused)
	}
	limit := free.SpillPeak / 4
	capped := run(Options{Workers: workers, Budget: budget, SpillDir: dir, SpillMax: limit})
	t.Logf("peak without a cap %d bytes; with cap %d: peak %d, refused %d, %d rows spilled", free.SpillPeak, limit, capped.SpillPeak, capped.SpillRefused, capped.SpillRows)
	// Trailers are always admitted (a few bytes per file).
	if capped.SpillRefused == 0 || capped.SpillPeak > limit+16*capped.SpillFiles {
		t.Fatalf("cap %d: peak %d, refused %d", limit, capped.SpillPeak, capped.SpillRefused)
	}
	none := run(Options{Workers: workers, Budget: budget, SpillDir: dir, SpillReserve: 1 << 62})
	if none.SpillRows != 0 || none.SpillRefused == 0 {
		t.Fatalf("reserve: %d rows spilled, %d refused", none.SpillRows, none.SpillRefused)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("spill files left: %d", len(ents))
	}
}

// TestSpillReadFailures: a frame whose length exceeds the limit, and a
// compressed frame that decodes past the decode limit, are refused before
// their memory is allocated; a reader left open by a failure downstream
// of a replay is closed with the spiller.
func TestSpillReadFailures(t *testing.T) {
	newSp := func(compress bool) *spiller[tp] {
		s, err := newSpiller(testSchema.Codec, t.TempDir(), compress, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := newSp(false)
	f := s.file()
	if err := os.WriteFile(f.path, []byte{0xff, 0xff, 0xff, 0x7f, 0, 0, 0, 0}, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.next(f); !errors.Is(err, errSpillFrame) || f.f != nil {
		t.Fatalf("oversized length: %v (reader open %v)", err, f.f != nil)
	}
	s.close()

	old := maxSpillDecoded
	maxSpillDecoded = 1 << 20
	s = newSp(true)
	maxSpillDecoded = old
	enc, _ := zstd.NewWriter(nil)
	bomb := enc.EncodeAll(make([]byte, 4<<20), nil) // a few hundred bytes that expand to 4 MiB
	f = s.file()
	if err := os.WriteFile(f.path, s.frame('Z', bomb), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.next(f); !errors.Is(err, errSpillFrame) || f.f != nil {
		t.Fatalf("decoded past the limit: %v (reader open %v)", err, f.f != nil)
	}
	s.close()

	// A checksum-valid frame that claims more rows than any frame its file
	// was given (here 2; the frame claims 2,000,000 over 2 MB) is refused
	// before the rows are allocated (they would take about 80 MB).
	s = newSp(false)
	f = s.file()
	if err := s.write(f, []schema.Row[tp]{{Key: "a"}, {Key: "b"}}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.next(f); err != nil {
		t.Fatal(err)
	}
	inflated := append(binary.AppendUvarint(nil, 2_000_000), make([]byte, 2_000_000)...)
	if err := os.WriteFile(f.path, s.frame('R', inflated), 0o644); err != nil {
		t.Fatal(err)
	}
	f.close()
	f.r, f.pos = nil, 0
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := s.next(f)
	runtime.ReadMemStats(&after)
	if alloc := after.TotalAlloc - before.TotalAlloc; !errors.Is(err, schema.ErrCodec) || alloc > 16<<20 {
		t.Fatalf("an inflated row count: %v, %d bytes allocated", err, alloc)
	}
	s.close()

	s = newSp(false)
	f = s.file()
	rows := []schema.Row[tp]{{Key: "a"}, {Key: "b"}}
	if err := s.write(f, rows[:1], false); err != nil {
		t.Fatal(err)
	}
	if err := s.write(f, rows[1:], true); err != nil {
		t.Fatal(err)
	}
	if got, err := s.next(f); err != nil || len(got) != 1 {
		t.Fatalf("replay: %v %v", got, err)
	}
	// The sink fails here; the run ends without reading the trailer.
	if f.f == nil {
		t.Fatal("the reader is not open mid-file")
	}
	s.close()
	if f.f != nil {
		t.Fatal("close left a spill reader open")
	}
}

// TestSpillMarkersInByteMode: with the budget in bytes (the CLI's mode;
// every other order test counts rows, where a marker costs nothing), spill
// markers behind a stalled frontier pass admission (their bytes never
// exceed the budget by more than one publish batch) and every marker's
// bytes are released when its frame is replayed, on the finished-node and
// the expanding-node drain paths alike: the drain ends with nothing
// counted.
func TestSpillMarkersInByteMode(t *testing.T) {
	countRows = false
	defer func() { countRows = true }()
	f := &frontier{slowPages: 30, siblings: 40, sibPages: 11, rows: 100, slowDelay: 5 * time.Millisecond, sibDelay: time.Millisecond}
	want := f.keys()
	budget := 24 * markerUnits() // a few publish batches of markers, far less than one page of rows
	var mu sync.Mutex
	var peak, leftBytes, leftMarkers int64 = 0, -1, -1
	hookMarkers = func(m int64) { mu.Lock(); peak = max(peak, m); mu.Unlock() }
	hookDrained = func(b, m int64) { mu.Lock(); leftBytes, leftMarkers = b, m; mu.Unlock() }
	defer func() { hookMarkers, hookDrained = nil, nil }()
	c := &collect{}
	st, err := Run[fr, tp](context.Background(), f, c, &testSchema, Options{Workers: 8, Budget: budget, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.keys, want) {
		t.Fatalf("%d keys, want %d", len(c.keys), len(want))
	}
	mu.Lock()
	defer mu.Unlock()
	if st.SpillFrames == 0 {
		t.Fatal("nothing spilled")
	}
	if leftBytes != 0 || leftMarkers != 0 {
		t.Fatalf("the drain ended with %d bytes, %d of markers, still counted", leftBytes, leftMarkers)
	}
	// Metadata also counts child nodes: the root's siblings are published
	// at once by the awaited root, past the budget.
	if limit := budget + 2*publishRows/int64(f.rows)*markerUnits() + int64(f.siblings+1)*childUnits(); peak > limit {
		t.Fatalf("marker bytes peaked at %d, budget %d (limit %d)", peak, budget, limit)
	}
	t.Logf("%d frames, marker peak %d of budget %d, budget wait %v", st.SpillFrames, peak, budget, st.BudgetWait)
}

// longRange is a slow empty range, then a long range of pages one row
// each: a worker claims the long one while the drainer waits on the slow
// one, so the drainer then consumes the long range while it grows.
type longRange struct{ pages int }

func (l *longRange) Root() fr { return fr{-1} }

func (l *longRange) Expand(ctx context.Context, r fr, e source.Emitter[fr, tp]) error {
	switch r.i {
	case -1:
		if err := e.Child(fr{0}); err != nil {
			return err
		}
		return e.Child(fr{1})
	case 0:
		time.Sleep(50 * time.Millisecond)
		return nil
	}
	for p := range l.pages {
		if err := e.Rows([]schema.Row[tp]{{Key: fmt.Sprintf("k%08d", p), Data: tp{Size: 1}}}); err != nil {
			return err
		}
		runtime.Gosched() // a page at a time, also with one P: the drainer keeps up
	}
	return nil
}

// TestRunningNodeStorageIsBounded: a long streaming range on the drain
// path holds its consumed items only until they are half of its items
// (and at least 256), so the storage for what was already drained never
// exceeds that for what still waits, however many pages it has streamed
// (byte-mode budget; durability review, P2-11). What waits is the awaited
// range's own lead over the drainer, which the budget exempts. The old
// drainer zeroed consumed slots but kept them, one per page streamed.
func TestRunningNodeStorageIsBounded(t *testing.T) {
	countRows = false
	defer func() { countRows = true }()
	const pages = 60000
	var mu sync.Mutex
	peak, seen, worst := 0, 0, ""
	hookItems = func(consumed, waiting int) {
		mu.Lock()
		defer mu.Unlock()
		seen++
		peak = max(peak, consumed)
		if consumed >= compactAt && consumed > waiting && worst == "" {
			worst = fmt.Sprintf("%d consumed items held with %d waiting", consumed, waiting)
		}
	}
	defer func() { hookItems = nil }()
	c := &collect{}
	if _, err := Run[fr, tp](context.Background(), &longRange{pages: pages}, c, &testSchema, Options{Workers: 4, Budget: 64 << 10}); err != nil {
		t.Fatal(err)
	}
	if len(c.keys) != pages {
		t.Fatalf("%d keys, want %d", len(c.keys), pages)
	}
	mu.Lock()
	defer mu.Unlock()
	if seen < pages/2 {
		t.Fatalf("the drainer consumed only %d of %d items from a running node: the test did not stream", seen, pages)
	}
	if worst != "" {
		t.Fatalf("a running node retained storage for consumed items after %d pages: %s", pages, worst)
	}
	t.Logf("at most %d consumed items held over %d pages (%d drained while running)", peak, pages, seen)
}

// TestSpillReadersAreCapped: a deep walk leaves one partly replayed spill
// file per spilled ancestor. With 3 x maxReaders files each read part way,
// at most maxReaders readers (descriptors and 64 KiB buffers) are open at
// once; an evicted reader reopens at its position and every file replays
// exactly, trailer checks included. Before, every partly read file kept
// its reader open.
func TestSpillReadersAreCapped(t *testing.T) {
	s, err := newSpiller(testSchema.Codec, t.TempDir(), false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	g := &Gauges{}
	s.g = g
	defer s.close()
	var files []*spillFile
	for i := range 3 * maxReaders {
		f := s.file()
		for j := range 3 {
			if err := s.write(f, []schema.Row[tp]{{Key: fmt.Sprintf("%02d-%d", i, j), Data: tp{Size: int64(j)}}}, false); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.write(f, nil, true); err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	open := func() int {
		n := 0
		for _, f := range files {
			if f.f != nil {
				n++
			}
		}
		return n
	}
	// Descend: the first frame of every file, as a deep path would.
	for i, f := range files {
		rows, err := s.next(f)
		if err != nil || len(rows) != 1 || rows[0].Key != fmt.Sprintf("%02d-0", i) {
			t.Fatalf("file %d: %v %v", i, rows, err)
		}
		if n := open(); n > maxReaders {
			t.Fatalf("%d readers open, cap %d", n, maxReaders)
		}
	}
	// Climb back: the rest of each, deepest first.
	for i := len(files) - 1; i >= 0; i-- {
		for j := 1; j < 3; j++ {
			rows, err := s.next(files[i])
			if err != nil || len(rows) != 1 || rows[0].Key != fmt.Sprintf("%02d-%d", i, j) {
				t.Fatalf("file %d frame %d: %v %v", i, j, rows, err)
			}
		}
		if rows, err := s.next(files[i]); err != nil || rows != nil {
			t.Fatalf("file %d trailer: %v %v", i, rows, err)
		}
		if n := open(); n > maxReaders {
			t.Fatalf("%d readers open, cap %d", n, maxReaders)
		}
	}
	if p := g.readers.peak.Load(); p != maxReaders || g.readers.n.Load() != 0 {
		t.Fatalf("readers: peak %d, open %d", p, g.readers.n.Load())
	}
	if s.live.Load() != 0 || g.spill.n.Load() != 0 {
		t.Fatalf("live spill bytes %d after every file was replayed", s.live.Load())
	}
}
