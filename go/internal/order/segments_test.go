// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	failAt         int // the segment (1-based, in opening order) whose Write fails (0: none)
	opened         int
	failed         *fakePart // the failing segment's part

	mu        sync.Mutex
	parts     []*fakePart
	cur       *fakePart
	full      bool
	segmented bool
	rowCuts   int
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
			p.keys = append(p.keys, seg[i].Key)
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
	pl := SegmentPlan{Limit: s.limit, MaxRows: s.maxRows, Encoders: s.encoders}
	if s.cur != nil {
		pl.Open, pl.Full, pl.Logical, pl.Rows = true, s.full, s.cur.logical, int64(len(s.cur.keys))
	}
	return pl, true
}

func (s *partSink) BeginSegments() {
	s.mu.Lock()
	s.segmented = true
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
			f.p.keys = append(f.p.keys, seg[i].Key)
		}
	}
	return nil
}

func (f *fakeSeg) Close(final, rowCut bool) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	f.p.closed, f.p.final = true, final
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
func withProcs(t *testing.T, n int) {
	prev := aheadProcs
	aheadProcs = func() int { return n }
	t.Cleanup(func() { aheadProcs = prev })
}

// TestSegmentsCutWhereTheSinkWould: random trees whose frontier is held
// until the rest has listed and spilled, replayed by segments after
// listing; the parts (their rows, order, count, the last marked final) are
// exactly the serial drain's, with a byte limit alone and with a row cap,
// for 2, 3 and 8 workers; every row passes the guard once (the oracle).
func TestSegmentsCutWhereTheSinkWould(t *testing.T) {
	engaged := 0
	for seed := range uint64(16) {
		r := rand.New(rand.NewPCG(seed, 11))
		want := genTree(rand.New(rand.NewPCG(seed, 11))).keys()
		limit := int64(200 + r.IntN(20000))
		var maxRows int64
		if seed%3 == 0 {
			maxRows = int64(50 + r.IntN(2000))
		}
		budget := int64(50 + r.IntN(3000))
		run := func(procs, encoders int) (*partSink, Stats) {
			t.Helper()
			withProcs(t, procs)
			fresh := genTree(rand.New(rand.NewPCG(seed, 11))) // the same shape, a fresh barrier
			s := &partSink{limit: limit, maxRows: maxRows, encoders: encoders}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			st, err := Run[*rnode, tp](ctx, fresh, s, &testSchema, Options{Workers: 2 + r.IntN(8), Budget: budget, SpillDir: t.TempDir()})
			if err != nil {
				t.Fatalf("seed %d procs %d: %v", seed, procs, err)
			}
			s.close()
			return s, st
		}
		serial, sst := run(1, 1)
		if sst.Segments.Reason != SegmentOneProcessor && sst.Segments.Reason != SegmentNotPlanned && sst.Segments.Reason != SegmentSinglePart {
			t.Fatalf("seed %d: serial run's segments %+v", seed, sst.Segments)
		}
		if got := slices.Concat(serial.boundaries()...); !slices.Equal(got, want) {
			t.Fatalf("seed %d: serial run's rows differ", seed)
		}
		for _, k := range []int{2, 3, 8} {
			s, st := run(k+1, k)
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
	if engaged == 0 {
		t.Fatal("the segments never engaged")
	}
}

// TestSegmentsFailureClosesNoPart: a segment's part writer fails mid-run.
// The run fails with it, that part is never closed (so never published),
// and every part closed holds an exact run of the oracle's rows.
func TestSegmentsFailureClosesNoPart(t *testing.T) {
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
// segments as without: within a segment, or at a seam between two.
func TestSegmentsSeamError(t *testing.T) {
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
		}
	}
}

// TestSegmentsMemoryIsBounded: under a tiny budget the segments hold no
// more than the serial drain: the budget, one publish batch per worker, the
// frontier path, and the read-ahead's share it had when they began.
func TestSegmentsMemoryIsBounded(t *testing.T) {
	withProcs(t, 5)
	f := &frontier{slowPages: 4, siblings: 60, sibPages: 6, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	const workers, budget = 16, 500
	s := &partSink{limit: 5000, encoders: 4}
	st, err := Run[fr, tp](context.Background(), f, s, &testSchema, Options{Workers: workers, Budget: budget, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if got := slices.Concat(s.boundaries()...); !slices.Equal(got, f.keys()) {
		t.Fatal("rows differ")
	}
	t.Logf("max buffered %d; segments %+v", st.MaxBuffered, st.Segments)
	if st.Segments.Reason != SegmentEngaged {
		t.Fatalf("segments %+v", st.Segments)
	}
	if limit := int64(budget+workers*publishRows+f.rows) + max(budget/4, publishRows); st.MaxBuffered > limit {
		t.Fatalf("max buffered %d > %d", st.MaxBuffered, limit)
	}
}

// TestSegmentsFromMemory: without spill, the rest held in memory is cut
// and written as segments too, the parts the serial drain's.
func TestSegmentsFromMemory(t *testing.T) {
	run := func(procs int) (*partSink, Stats) {
		withProcs(t, procs)
		f := &frontier{slowPages: 2, siblings: 30, sibPages: 4, rows: 100}
		f.hold, f.left = make(chan struct{}), &atomic.Int64{}
		f.left.Store(int64(f.siblings))
		s := &partSink{limit: 20_000, encoders: 4}
		st, err := Run[fr, tp](context.Background(), f, s, &testSchema, Options{Workers: 8, Budget: 1 << 30})
		if err != nil {
			t.Fatal(err)
		}
		s.close()
		return s, st
	}
	serial, _ := run(1)
	s, st := run(5)
	if st.Segments.Reason != SegmentEngaged || st.SpillRows != 0 {
		t.Fatalf("segments %+v, %d rows spilled", st.Segments, st.SpillRows)
	}
	if !slices.EqualFunc(s.boundaries(), serial.boundaries(), slices.Equal[[]string]) {
		t.Fatal("parts differ from the serial drain's")
	}
}

// TestSegmentWorkersCap: Options.SegmentWorkers caps the workers.
func TestSegmentWorkersCap(t *testing.T) {
	withProcs(t, 9)
	f := &frontier{slowPages: 2, siblings: 30, sibPages: 4, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	s := &partSink{limit: 3000, encoders: 8}
	st, err := Run[fr, tp](context.Background(), f, s, &testSchema, Options{Workers: 8, Budget: 500, SpillDir: t.TempDir(), SegmentWorkers: 3})
	if err != nil {
		t.Fatal(err)
	}
	if st.Segments.Reason != SegmentEngaged || st.Segments.Workers != 3 {
		t.Fatalf("segments %+v, want 3 workers", st.Segments)
	}
}
