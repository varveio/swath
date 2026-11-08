// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// partSink is a Segmenter with the Parquet router's cut rule, in memory: a
// part closes after the row whose logical bytes reach limit, or else at
// maxRows rows. The rule is stated (Plan) once its first part has closed,
// as the router's is once part 0 has frozen its ratio. Parts record their
// keys, whether they were closed, final, and in which order they opened.
type partSink struct {
	limit, maxRows int64
	encoders       int
	queue          int // the plan's queue depth (SegmentPlan.Queue)
	failAt         int // the segment (1-based, in opening order) whose Write fails (0: none)
	opened         int
	failed         *fakePart // the failing segment's part

	mu        sync.Mutex
	parts     []*fakePart
	cur       *fakePart
	full      bool
	segmented bool
	rowCuts   int
	keyBytes  int64 // the keys' bytes the parts keep (mu)
}

type fakePart struct {
	keys    []string
	logical int64
	closed  bool
	final   bool
	seg     bool
}

func (s *partSink) Write(_ context.Context, segs [][]schema.Row[tp]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.segmented {
		return errors.New("written after the segments began")
	}
	for _, seg := range segs {
		for i := range seg {
			if s.full {
				s.cur.closed = true
				s.cur, s.full = nil, false
			}
			if s.cur == nil {
				s.cur = &fakePart{}
				s.parts = append(s.parts, s.cur)
			}
			p := s.cur
			p.keys = append(p.keys, strings.Clone(seg[i].Key)) // not the rows' storage
			s.keyBytes += int64(len(seg[i].Key))
			p.logical += testSchema.LogicalBytes(&seg[i])
			if p.logical >= s.limit {
				s.full = true
			} else if s.maxRows > 0 && int64(len(p.keys)) >= s.maxRows {
				s.full = true
				s.rowCuts++
			}
		}
	}
	return nil
}

func (s *partSink) Plan() (SegmentPlan, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.parts) < 2 || s.segmented {
		return SegmentPlan{}, false
	}
	pl := SegmentPlan{Limit: s.limit, MaxRows: s.maxRows, Encoders: s.encoders, Queue: s.queue}
	if s.cur != nil {
		pl.Open, pl.Full, pl.Logical, pl.Rows = true, s.full, s.cur.logical, int64(len(s.cur.keys))
	}
	return pl, true
}

func (s *partSink) BeginSegments() (SegmentPlan, bool) {
	pl, ok := s.Plan()
	if ok {
		s.mu.Lock()
		s.segmented = true
		s.mu.Unlock()
	}
	return pl, ok
}

func (s *partSink) AbandonSegments() {
	s.mu.Lock()
	s.segmented = false
	s.mu.Unlock()
}

func (s *partSink) OpenSegment(_ context.Context, cont bool) (SegmentSink[tp], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opened++
	if cont {
		p := s.cur
		s.cur = nil
		return &fakeSeg{s: s, p: p, n: s.opened}, nil
	}
	p := &fakePart{seg: true}
	s.parts = append(s.parts, p)
	return &fakeSeg{s: s, p: p, n: s.opened}, nil
}

// close is the end of the stream on the serial path (the router's Close).
func (s *partSink) close() {
	if s.cur != nil {
		s.cur.closed, s.cur.final = true, true
	}
}

type fakeSeg struct {
	s *partSink
	p *fakePart
	n int // its segment, 1-based in opening order
}

func (f *fakeSeg) Write(_ context.Context, segs [][]schema.Row[tp]) error {
	if f.s.failAt > 0 && f.n == f.s.failAt {
		f.s.mu.Lock()
		f.s.failed = f.p
		f.s.mu.Unlock()
		return errors.New("part writer failed")
	}
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	for _, seg := range segs {
		for i := range seg {
			f.p.keys = append(f.p.keys, strings.Clone(seg[i].Key))
			f.s.keyBytes += int64(len(seg[i].Key))
		}
	}
	return nil
}

func (f *fakeSeg) Close(_ context.Context, rowCut bool) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	f.p.closed = true
	if rowCut {
		f.s.rowCuts++
	}
}

// Hold hands the last part back: the sink's close marks it final.
func (f *fakeSeg) Hold(rowCut bool) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	f.s.cur = f.p
	if rowCut {
		f.s.rowCuts++
	}
}

// boundaries is the parts' keys, closed and final flags.
func (s *partSink) boundaries() [][]string {
	var out [][]string
	for _, p := range s.parts {
		out = append(out, p.keys)
	}
	return out
}

// withProcs sets the processors the read-ahead and the segments may use.
// anyShare plans the segment workers as if each held one byte
// (hookShare): a test of the tail's cuts, seams or failures at a budget too
// small to hold two real shares, the safety gate holding the bound alone.
func anyShare(t *testing.T) {
	hookShare = func(int64) int64 { return 1 }
	t.Cleanup(func() { hookShare = nil })
}

func withProcs(t *testing.T, n int) {
	prev := aheadProcs
	aheadProcs = func() int { return n }
	t.Cleanup(func() { aheadProcs = prev })
}

// TestSegmentsCutWhereTheSinkWould: random trees whose frontier is held
// until the rest has listed and spilled, replayed by segments after
// listing; the parts (their rows, order, count, the last marked final) are
// exactly the serial drain's, with a byte limit alone and with a row cap,
// for 2, 3 and 8 encoders admitted (the tiny budgets hold what the workers
// read to the row budget: segReserve); every row passes the guard once (the oracle).
// Without spill (a budget that holds everything), the rest held in memory
// is cut and written as segments too, the parts the serial drain's.
func TestSegmentsCutWhereTheSinkWould(t *testing.T) {
	anyShare(t)
	engaged, fromMemory := 0, 0
	for seed := range uint64(16) {
		r := rand.New(rand.NewPCG(seed, 11))
		want := genTree(rand.New(rand.NewPCG(seed, 11))).keys()
		limit := int64(200 + r.IntN(20000))
		var maxRows int64
		if seed%3 == 0 {
			maxRows = int64(50 + r.IntN(2000))
		}
		budget := int64(50 + r.IntN(3000))
		run := func(procs, encoders int, memory bool) (*partSink, Stats) {
			t.Helper()
			withProcs(t, procs)
			fresh := genTree(rand.New(rand.NewPCG(seed, 11))) // the same shape, a fresh barrier
			s := &partSink{limit: limit, maxRows: maxRows, encoders: encoders}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			opt := Options{Workers: 2 + r.IntN(8), Budget: budget, SpillDir: t.TempDir()}
			if memory {
				opt.Budget, opt.SpillDir = 1<<30, ""
			}
			st, err := Run[*rnode, tp](ctx, fresh, s, &testSchema, opt)
			if err != nil {
				t.Fatalf("seed %d procs %d: %v", seed, procs, err)
			}
			s.close()
			return s, st
		}
		serial, sst := run(1, 1, false)
		if sst.Segments.Reason != SegmentOneProcessor && sst.Segments.Reason != SegmentNotPlanned && sst.Segments.Reason != SegmentSinglePart {
			t.Fatalf("seed %d: serial run's segments %+v", seed, sst.Segments)
		}
		if got := slices.Concat(serial.boundaries()...); !slices.Equal(got, want) {
			t.Fatalf("seed %d: serial run's rows differ", seed)
		}
		if s, st := run(4, 3, true); st.SpillRows != 0 || !slices.EqualFunc(s.boundaries(), serial.boundaries(), slices.Equal[[]string]) {
			t.Fatalf("seed %d in memory (%+v, %d rows spilled): %d parts, serial %d; or their rows differ", seed, st.Segments, st.SpillRows, len(s.parts), len(serial.parts))
		} else if st.Segments.Reason == SegmentEngaged {
			fromMemory++
		}
		for _, k := range []int{2, 3, 8} {
			s, st := run(k+1, k, false)
			if !slices.EqualFunc(s.boundaries(), serial.boundaries(), slices.Equal[[]string]) {
				t.Fatalf("seed %d k %d (%+v): %d parts, serial %d; or their rows differ", seed, k, st.Segments, len(s.parts), len(serial.parts))
			}
			if s.rowCuts != serial.rowCuts {
				t.Fatalf("seed %d k %d: %d row cuts, serial %d", seed, k, s.rowCuts, serial.rowCuts)
			}
			for i, p := range s.parts {
				if !p.closed || p.final != (i == len(s.parts)-1) {
					t.Fatalf("seed %d k %d: part %d closed %v final %v of %d", seed, k, i, p.closed, p.final, len(s.parts))
				}
			}
			if st.Rows != int64(len(want)) || st.Received != st.Rows {
				t.Fatalf("seed %d k %d: %d rows handed on, %d received, want %d", seed, k, st.Rows, st.Received, len(want))
			}
			if st.Segments.Reason == SegmentEngaged {
				engaged++
				if st.Segments.Rows == 0 || st.Segments.Seams == 0 && st.Segments.Segments > 2 {
					t.Fatalf("seed %d k %d: segments %+v", seed, k, st.Segments)
				}
			}
		}
	}
	if engaged == 0 || fromMemory == 0 {
		t.Fatalf("the segments engaged %d times spilled, %d in memory", engaged, fromMemory)
	}
}

// TestSegmentsFailureClosesNoPart: a segment's part writer fails mid-run.
// The run fails with it, that part is never closed (so never published),
// and every part closed holds an exact run of the oracle's rows.
func TestSegmentsFailureClosesNoPart(t *testing.T) {
	anyShare(t)
	withProcs(t, 5)
	f := &frontier{slowPages: 2, siblings: 40, sibPages: 6, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	s := &partSink{limit: 20_000, encoders: 4, failAt: 3}
	st, err := Run[fr, tp](context.Background(), f, s, &testSchema, Options{Workers: 8, Budget: 500, SpillDir: t.TempDir()})
	if err == nil || err.Error() != "part writer failed" {
		t.Fatalf("err %v, want the part writer's (segments %+v)", err, st.Segments)
	}
	if s.failed == nil || s.failed.closed {
		t.Fatal("the failed segment's part was closed: it would be published")
	}
	want := f.keys()
	pos := 0
	for i, p := range s.parts {
		if !p.closed {
			continue
		}
		at := slices.Index(want, p.keys[0])
		if at < 0 || !slices.Equal(want[at:at+len(p.keys)], p.keys) || at < pos {
			t.Fatalf("part %d is not a run of the output", i)
		}
		pos = at
	}
}

// badFrontier lists sibling bad with sibling bad-1's keys: each range is
// ascending, the stream repeats a range (a broken provider).
type badFrontier struct {
	*frontier
	bad int
}

func (b badFrontier) Expand(ctx context.Context, r fr, e source.Emitter[fr, tp]) error {
	if r.i == b.bad {
		return b.frontier.Expand(ctx, fr{b.bad - 1}, e)
	}
	return b.frontier.Expand(ctx, r, e)
}

// TestSegmentsSeamError: the order guard refuses the repeated range with
// segments as without: within a segment, or at a seam between two. The
// segments must engage where parts are cut (anyShare: the tiny budget
// otherwise plans one worker, the serial drain, and the test passed
// without them).
func TestSegmentsSeamError(t *testing.T) {
	anyShare(t)
	for _, procs := range []int{1, 5} {
		withProcs(t, procs)
		for _, limit := range []int64{2000, 60_000, 1 << 30} {
			f := &frontier{slowPages: 2, siblings: 40, sibPages: 6, rows: 100}
			f.hold, f.left = make(chan struct{}), &atomic.Int64{}
			f.left.Store(int64(f.siblings))
			s := &partSink{limit: limit, encoders: 4}
			st, err := Run[fr, tp](context.Background(), badFrontier{f, 30}, s, &testSchema, Options{Workers: 8, Budget: 500, SpillDir: t.TempDir()})
			var inv *dataset.InvariantError
			if !errors.As(err, &inv) {
				t.Fatalf("procs %d limit %d: err %v, want the order guard's (segments %+v)", procs, limit, err, st.Segments)
			}
			if procs > 1 && limit < 1<<30 && st.Segments.Reason != SegmentEngaged {
				t.Fatalf("procs %d limit %d: segments %+v, want engaged", procs, limit, st.Segments)
			}
		}
	}
}

// TestSegmentsMemoryIsBounded: the segments hold no more than the serial
// drain: the stage's bound with the read-ahead's share it had when they
// began (heldBoundRun).
func TestSegmentsMemoryIsBounded(t *testing.T) {
	anyShare(t)
	withProcs(t, 5)
	s := &partSink{limit: 5000, encoders: 4}
	st, f := heldBoundRun(t, s)
	if got := slices.Concat(s.boundaries()...); !slices.Equal(got, f.keys()) {
		t.Fatal("rows differ")
	}
	if st.Segments.Reason != SegmentEngaged {
		t.Fatalf("segments %+v", st.Segments)
	}
}

// TestSegmentWorkersCap: Options.SegmentWorkers caps the workers; and
// ListingDone is called once, when the last range is expanded, before the
// drain ends.
func TestSegmentWorkersCap(t *testing.T) {
	withProcs(t, 9)
	f := &frontier{slowPages: 2, siblings: 30, sibPages: 4, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	s := &partSink{limit: 3000, encoders: 8}
	var calls atomic.Int64
	var at time.Time
	st, err := Run[fr, tp](context.Background(), f, s, &testSchema, Options{Workers: 8, Budget: 1 << 30, SegmentWorkers: 3,
		ListingDone: func() {
			if calls.Add(1) == 1 {
				at = time.Now()
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	if st.Segments.Reason != SegmentEngaged || st.Segments.Workers != 3 {
		t.Fatalf("segments %+v, want 3 workers", st.Segments)
	}
	if calls.Load() != 1 || st.ListingDone.IsZero() || at.Before(st.ListingDone) || st.DrainEnd.Before(st.ListingDone) {
		t.Fatalf("calls %d, listing done %v (called at %v), drain end %v", calls.Load(), st.ListingDone, at, st.DrainEnd)
	}
}

// TestSegmenterCalledUnlocked: the drainer calls its Segmenter without
// the stage's lock. Holding it there deadlocked: the router's interval cut
// holds the router's lock while part 0's encoder, accounting its bytes,
// wakes the stage (Shared.Add takes the stage's lock).
func TestSegmenterCalledUnlocked(t *testing.T) {
	anyShare(t)
	withProcs(t, 5)
	var calls, held atomic.Int64
	hookSegmenter = func(mu *sync.Mutex) {
		// The caller is stopped here: if it held the lock, nobody could
		// take it now (a worker holding it briefly lets go).
		calls.Add(1)
		got := make(chan struct{})
		go func() {
			mu.Lock()
			mu.Unlock()
			close(got)
		}()
		select {
		case <-got:
		case <-time.After(2 * time.Second):
			held.Add(1)
		}
	}
	t.Cleanup(func() { hookSegmenter = nil })
	f := &frontier{slowPages: 2, siblings: 30, sibPages: 4, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	s := &partSink{limit: 3000, encoders: 4}
	st, err := Run[fr, tp](context.Background(), f, s, &testSchema, Options{Workers: 8, Budget: 500, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if st.Segments.Reason != SegmentEngaged || calls.Load() < 2 || held.Load() != 0 {
		t.Fatalf("segments %+v: %d calls, %d with the stage's lock held", st.Segments, calls.Load(), held.Load())
	}
}

// damageTrailer rewrites the trailer of the spill file at path to count
// one row more, its CRC valid: only the count check can refuse it.
func damageTrailer(t *testing.T, path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off+8 <= len(b); {
		n := int(binary.LittleEndian.Uint32(b[off:]))
		payload := b[off+8 : off+8+n]
		if payload[0] == frameTrailer {
			rows, _ := binary.Uvarint(payload[1:])
			tr := (&spiller[tp]{}).frame(frameTrailer, binary.AppendUvarint(nil, rows+1))
			if err := os.WriteFile(path, append(b[:off:off], tr...), 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
		off += 8 + n
	}
	t.Fatalf("%s: no trailer", path)
}

// TestSegmentsTrailerFailureClosesNoPart: a spill file whose trailer
// disagrees with its rows is found before any segment closes a part: the
// run fails, no segment was opened, and no part is final (a part closed
// could be journaled; the final one would let a resume commit).
func TestSegmentsTrailerFailureClosesNoPart(t *testing.T) {
	withProcs(t, 5)
	f := &frontier{slowPages: 2, siblings: 30, sibPages: 4, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	dir := t.TempDir()
	s := &partSink{limit: 3000, encoders: 4}
	opt := Options{Workers: 8, Budget: 500, SpillDir: dir, ListingDone: func() {
		files, _ := filepath.Glob(filepath.Join(dir, "spill-*", "*.spill"))
		if len(files) == 0 {
			t.Error("no spill file to damage")
			return
		}
		slices.Sort(files)
		damageTrailer(t, files[len(files)-1]) // a late sibling's: replayed by the segments
	}}
	_, err := Run[fr, tp](context.Background(), f, s, &testSchema, opt)
	var inv *dataset.InvariantError
	if !errors.As(err, &inv) {
		t.Fatalf("err %v, want the trailer's invariant error", err)
	}
	if s.opened != 0 {
		t.Fatalf("%d segments opened before the trailers were checked", s.opened)
	}
	for i, p := range s.parts {
		if p.final {
			t.Fatalf("part %d final", i)
		}
	}
}

// queuedSink is partSink whose segments queue their batches to stalled
// encoders, as the Parquet router's do: each batch's bytes are counted
// downstream (Shared) from Write until its encoder has taken it, two
// batches queued a part. peak is the most counted downstream at once.
type queuedSink struct {
	*partSink
	shared  *Shared
	stall   time.Duration
	hold    chan struct{} // if set, every encoder waits on it before its first batch
	queued  atomic.Int64  // batches written to segments
	closed  atomic.Int64  // segment parts closed (their batches taken)
	closing atomic.Int64  // Close calls begun
	// atHold, if set, runs when the last segment hands its part back,
	// once every other segment's part is closed: every segment but the
	// last has finished.
	atHold func()
	wg     sync.WaitGroup
	peak   atomic.Int64
	held   *queuedSeg
}

type queuedSeg struct {
	*fakeSeg
	q       *queuedSink
	c       chan [][]schema.Row[tp]
	drained chan struct{}
}

func (s *queuedSink) OpenSegment(ctx context.Context, cont bool) (SegmentSink[tp], error) {
	out, err := s.partSink.OpenSegment(ctx, cont)
	if err != nil {
		return nil, err
	}
	// Two batches queued, as the router's segment parts (the part a
	// segment continues is held to as many by its tokens).
	g := &queuedSeg{fakeSeg: out.(*fakeSeg), q: s, c: make(chan [][]schema.Row[tp], 2), drained: make(chan struct{})}
	s.wg.Go(func() {
		if s.hold != nil {
			<-s.hold
		}
		for b := range g.c {
			time.Sleep(s.stall)
			_ = g.fakeSeg.Write(ctx, b)
			var n int64
			for _, seg := range b {
				n += testSchema.RowsBytes(seg)
			}
			s.shared.Add(-n)
		}
		close(g.drained)
	})
	return g, nil
}

func (g *queuedSeg) Write(ctx context.Context, b [][]schema.Row[tp]) error {
	var n int64
	for _, seg := range b {
		n += testSchema.RowsBytes(seg)
	}
	g.q.shared.Add(n)
	g.q.queued.Add(1)
	for v := g.q.shared.Load(); v > g.q.peak.Load(); v = g.q.shared.Load() {
		g.q.peak.Store(v)
	}
	select {
	case g.c <- b:
		return nil
	case <-ctx.Done():
		g.q.shared.Add(-n)
		return context.Cause(ctx)
	}
}

func (g *queuedSeg) Close(ctx context.Context, rowCut bool) {
	close(g.c)
	<-g.drained // as the router's: the part's batches taken first
	g.fakeSeg.Close(ctx, rowCut)
	g.q.closed.Add(1)
}

func (g *queuedSeg) Hold(rowCut bool) {
	g.q.held = g
	g.fakeSeg.Hold(rowCut)
	if g.q.atHold != nil {
		for g.q.closed.Load() < int64(g.n-1) { // the parts opened before it (it opened last)
			time.Sleep(time.Millisecond)
		}
		g.q.atHold()
	}
}

// close ends the held part's queue and waits for every encoder.
func (s *queuedSink) close() {
	if s.held != nil {
		close(s.held.c)
	}
	s.wg.Wait()
	s.partSink.close()
}

// budgetRun runs the long-key fixture of the segment budget tests: ranges
// of 1,000-byte keys among 220-byte ones, spilled under the budget, the
// tail replayed into stalled encoders whose queues are the router's (two
// batches a segment, sixteen the continued part). It returns the stats,
// the most counted downstream at once and the segment waits seen.
func budgetRun(t *testing.T, budget int64, siblings int) (Stats, int64, int64) {
	t.Helper()
	withProcs(t, 5)
	countRows = false // the budget in bytes, as the encoders' queues count them
	defer func() { countRows = true }()
	var waits atomic.Int64
	hookWait = func(site string) {
		if site == waitSegment {
			waits.Add(1)
		}
	}
	defer func() { hookWait = nil }()
	f := &frontier{slowPages: 4, siblings: siblings, sibPages: 6, rows: 100, long: 3}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	shared := &Shared{}
	s := &queuedSink{partSink: &partSink{limit: 2_000_000, encoders: 4, queue: 2}, shared: shared, stall: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, err := Run[fr, tp](ctx, f, s, &testSchema, Options{Workers: 8, Budget: budget, SpillDir: t.TempDir(), Shared: shared})
	if err != nil {
		t.Fatal(err)
	}
	s.close()
	if got := slices.Concat(s.boundaries()...); !slices.Equal(got, f.keys()) {
		t.Fatal("rows differ")
	}
	t.Logf("budget %d: segments %+v; downstream peak %d; %d waits; spilled %d rows", budget, st.Segments, s.peak.Load(), waits.Load(), st.SpillRows)
	return st, s.peak.Load(), waits.Load()
}

// TestSegmentsSizedToBudget: the segment workers are sized to the row
// budget when planned (segmentShare, budgetWorkers). A long-key fixture at
// a budget that holds exactly two plans two (the encoders allow four),
// and they hold their shares: the safety gate waits fewer than 16 times
// and the segments hold within the budget beside the tree. Before
// (2026-11-07), the gate counted the tree too and admitted every worker,
// which then waited tens of thousands of times on Batch.
func TestSegmentsSizedToBudget(t *testing.T) {
	const budget = 28 << 20
	st, peak, waits := budgetRun(t, budget, 200)
	sg := st.Segments
	if sg.Reason != SegmentEngaged || sg.Workers != 2 || sg.WorkersBy != WorkersByRowBudget || st.SpillRows == 0 {
		t.Fatalf("segments %+v, %d rows spilled: want two workers set by the row budget, spill read", sg, st.SpillRows)
	}
	if waits >= 16 || sg.BudgetWaits >= 16 || sg.MaxHeld > budget || peak > budget {
		t.Fatalf("%d waits (%d counted), %d held at most, %d downstream, budget %d", waits, sg.BudgetWaits, sg.MaxHeld, peak, budget)
	}

}

// TestSegmentsStayWithinBudget: with the plan's share understated
// (hookShare), every worker is admitted at a 4 MiB budget and the safety
// gate must hold the bound alone: the encoders' queues stay within it and
// what the segments hold within it but for one unit, the workers waiting
// at the segment site. Before (sol review 2, P2) every worker queued
// four-thousand-row batches into its part's queue (16 deep for the
// continued part) whatever the budget: 19 MB downstream.
func TestSegmentsStayWithinBudget(t *testing.T) {
	const budget = 4 << 20
	anyShare(t)
	st, peak, waits := budgetRun(t, budget, 60)
	sg := st.Segments
	if sg.Reason != SegmentEngaged || sg.Workers < 2 || st.SpillRows == 0 {
		t.Fatalf("segments %+v", sg)
	}
	unit := int64(publishRows) * (1100 + int64(unsafe.Sizeof(schema.Row[tp]{}))) // the largest: a frame of long keys
	if peak > budget || sg.MaxHeld > budget+unit || sg.BudgetWaits == 0 || waits == 0 {
		t.Fatalf("%d downstream, %d held at most (budget %d), %d budget waits (%d seen)", peak, sg.MaxHeld, budget, sg.BudgetWaits, waits)
	}
}

// TestRunBytes: the heaviest run of consecutive units reaching a row
// count, over every start.
func TestRunBytes(t *testing.T) {
	us := []unit[tp]{{n: 10, units: 100}, {n: 10, units: 900}, {n: 10, units: 100}, {n: 5, units: 50}}
	for _, tc := range []struct{ rows, want int64 }{{10, 900}, {15, 1000}, {20, 1000}, {30, 1100}, {100, 1150}} { // 15: from the second unit, 900+100
		if got := runBytes(us, tc.rows); got != tc.want {
			t.Errorf("runBytes(%d) = %d, want %d", tc.rows, got, tc.want)
		}
	}
}

// segmentRun is a stage stopped after planning, for one worker's segments
// alone (segment): a spiller and the segments' state, nothing listing.
func segmentRun(t *testing.T, units []unit[tp], budget int64) *run[fr, tp] {
	t.Helper()
	o := &run[fr, tp]{ctx: context.Background(), schema: &testSchema, seg: &segState[tp]{}, g: &Gauges{}}
	o.opt.Budget = budget
	o.drain = sync.NewCond(&o.mu)
	o.seg.uses = make([]atomic.Int32, len(units))
	return o
}

// keepSink is a segment's part that keeps every batch it is given.
type keepSink struct{ batches [][][]schema.Row[tp] }

func (k *keepSink) Write(_ context.Context, b [][]schema.Row[tp]) error {
	k.batches = append(k.batches, b)
	return nil
}
func (k *keepSink) Close(context.Context, bool) {}
func (k *keepSink) Hold(bool)                   {}

// longRows is n rows of about 1,000-byte keys, ascending.
func longRows(n int) []schema.Row[tp] {
	rows := make([]schema.Row[tp], n)
	for i := range rows {
		rows[i] = schema.Row[tp]{Key: fmt.Sprintf("k%06d/%01000d", i, 0), Data: tp{Size: int64(i), ETag: "e"}}
	}
	return rows
}

// TestSegmentsReleaseSpanStorage: a segment that hands on a span of a
// spilled frame (decoded whole, by each segment that reads it) copies the
// span, so the batch an encoder's queue holds keeps only the span. Eight
// segments read 28-row spans of one 1,000-row frame of 1,000-byte keys,
// each decoding its own copy, and their batches are all kept (a stalled
// encoder): the live heap grows by about the spans (well within 2 MiB), not
// by eight frames (about 9 MiB before, sol review 3, P2). No goroutines, no
// timing: the segments run one after another on the test's goroutine.
func TestSegmentsReleaseSpanStorage(t *testing.T) {
	countRows = false
	defer func() { countRows = true }()
	sp, err := newSpiller(&testSchema, t.TempDir(), false, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.close()
	f := sp.file()
	rows := longRows(1000)
	if err := sp.write(f, rows, true); err != nil {
		t.Fatal(err)
	}
	at := spillAt{off: f.lastOff, size: f.lastSize, units: testSchema.RowsBytes(rows), rows: f.lastRows, logical: f.lastLogical}
	rows = nil
	units := []unit[tp]{{f: f, at: at, n: at.rows, logical: at.logical, units: at.units}}
	o := segmentRun(t, units, 1<<30)
	o.sp = sp
	const spans, span = 8, 28
	o.seg.uses[0].Store(spans)
	keep := &keepSink{}
	base := gcLive()
	for i := range spans {
		s := segment{u0: 0, r0: i * span, u1: 0, r1: (i + 1) * span}
		if _, _, _, _, err := o.segment(units, s, false, keep, &segTimes{}); err != nil {
			t.Fatal(err)
		}
	}
	held := gcLive() - base
	runtime.KeepAlive(keep)
	t.Logf("%d spans of %d rows kept: live heap %.2f MiB", spans, span, float64(held)/(1<<20))
	if len(keep.batches) != spans || held > 2<<20 {
		t.Fatalf("%d batches, %d bytes live", len(keep.batches), held)
	}
}

// TestSegmentUnitsDroppedByLastReader: an in-memory unit read by three
// segments (its rows split at two part boundaries) keeps its rows until the
// last of them has read it, then the stage drops its reference and returns
// its tree charge, once; each segment's span is a copy, so nothing the
// queues hold keeps the unit's storage.
func TestSegmentUnitsDroppedByLastReader(t *testing.T) {
	countRows = false
	defer func() { countRows = true }()
	rows := longRows(90)
	units := []unit[tp]{{rows: rows, n: 90, units: testSchema.RowsBytes(rows)}}
	o := segmentRun(t, units, 1<<30)
	o.buffered = units[0].units // the tree's charge for it
	o.seg.uses[0].Store(3)
	keep := &keepSink{}
	for i, s := range []segment{{u0: 0, r0: 0, u1: 0, r1: 30}, {u0: 0, r0: 30, u1: 0, r1: 60}, {u0: 0, r0: 60, u1: 1, r1: 0}} {
		if _, _, _, _, err := o.segment(units, s, false, keep, &segTimes{}); err != nil {
			t.Fatal(err)
		}
		last := i == 2
		if (units[0].rows == nil) != last || (o.buffered == 0) != last {
			t.Fatalf("after segment %d: rows dropped %v, tree charge %d", i, units[0].rows == nil, o.buffered)
		}
	}
	for _, b := range keep.batches {
		for _, seg := range b {
			if &seg[0] == &rows[0] || &seg[0] == &rows[30] || &seg[0] == &rows[60] {
				t.Fatal("a span shares the unit's row array")
			}
		}
	}
}

// TestSegReserveIgnoresTree: what the segments may take is the budget
// beside the tree, which holds about the budget once listing ends and
// which the serial drain holds too. With the tree at the full budget, a
// reservation that fits beside what the segments hold is granted at once;
// one past the budget is not. Counting the tree too (2026-11-07) made every
// worker wait at every frame on Batch: tails up to 3x longer.
func TestSegReserveIgnoresTree(t *testing.T) {
	const budget = 1 << 20
	o := segmentRun(t, nil, budget)
	o.buffered = budget // the tree, full
	tm := &segTimes{}
	for _, tc := range []struct {
		c    int64
		want bool
	}{{600 << 10, true}, {300 << 10, true}, {200 << 10, false}} {
		if ok, err := o.segReserve(tc.c, false, tm); err != nil || ok != tc.want {
			t.Fatalf("reserve %d with %d held beside a full tree: %v (%v), want %v", tc.c, o.seg.held, ok, err, tc.want)
		}
	}
}

// gcLive is the live heap after a collection.
func gcLive() int64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// TestSegmentsCompletedPartsReleaseStorage: once a segment has finished,
// nothing of it keeps row storage alive. Long keys in 1,000-row frames and
// chunks, parts of about 28 rows (up to about 1,400 segments; fewer at
// GOMAXPROCS 1, where the serial drain gets further), measured when the
// last segment hands its part back and every other part is closed:
//   - spilled: the seam keys each finished segment leaves for the seam
//     check are copies, not views of the span or frame they came from
//     (before, sol review 4, P2: every finished part kept its boundary
//     span, tens of megabytes here);
//   - in memory: the units every finished segment read are dropped once
//     their last reader is done, and the tree's items with them (before
//     7a6ff48e: every row stayed until the drain ended).
//
// The live heap, less the keys the fake parts keep, stays within two
// budgets.
func TestSegmentsCompletedPartsReleaseStorage(t *testing.T) {
	withProcs(t, 5)
	anyShare(t)
	countRows = false
	defer func() { countRows = true }()
	for _, arm := range []struct {
		name   string
		budget int64
		spill  bool
	}{{"spilled", 2 << 20, true}, {"memory", 64 << 20, false}} {
		f := &frontier{slowPages: 1, siblings: 40, sibPages: 1, rows: 1000, long: 1}
		f.hold, f.left = make(chan struct{}), &atomic.Int64{}
		f.left.Store(int64(f.siblings))
		shared := &Shared{}
		s := &queuedSink{partSink: &partSink{limit: 30_000, encoders: 4, queue: 2}, shared: shared}
		runtime.GC()
		base := gcLive()
		var held int64 = -1
		s.atHold = func() {
			s.mu.Lock()
			kept := s.keyBytes
			s.mu.Unlock()
			held = gcLive() - base - kept
		}
		opt := Options{Workers: 8, Budget: arm.budget, Shared: shared}
		if arm.spill {
			opt.SpillDir = t.TempDir()
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		st, err := Run[fr, tp](ctx, f, s, &testSchema, opt)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		s.close()
		if got := slices.Concat(s.boundaries()...); !slices.Equal(got, f.keys()) {
			t.Fatalf("%s: rows differ", arm.name)
		}
		sg := st.Segments
		t.Logf("%s: %d segments, %d workers, %d spilled; live heap %.1f MiB when the last segment finished", arm.name, sg.Segments, sg.Workers, st.SpillRows, float64(held)/(1<<20))
		if sg.Reason != SegmentEngaged || sg.Segments < 150 || (st.SpillRows > 0) != arm.spill || held < 0 {
			t.Fatalf("%s: segments %+v, %d spilled, held %d", arm.name, sg, st.SpillRows, held)
		}
		if held > 2*min(arm.budget, 4<<20) {
			t.Fatalf("%s: %d bytes live when the last segment finished", arm.name, held)
		}
	}
}
