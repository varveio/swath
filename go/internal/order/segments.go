// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/schema"
)

// Segment-parallel replay. Once listing is done every range is finished
// and immutable, and the rows the drainer has not handed on yet are known:
// in memory, or in spill frames whose markers record their rows and
// logical bytes. A sink that cuts its stream into parts by a stated rule
// (Segmenter: the Parquet router, once part 0 has frozen its size ratio)
// lets the drainer cut the rest itself, exactly where the sink would, and
// hand the parts to several workers at once: each segment is one part,
// read (spill frames on its own descriptor, readAt), order-checked and
// written by one worker, so parts encode in parallel instead of one after
// another. The sink publishes them in ordinal order and checks each
// against the previous (adjacency) before publishing it; the seams between
// segments are checked again here. Before listing is done nothing
// changes: the drainer walks the tree as always.

// Segmenter is a sink that can take the rest of its stream as consecutive
// segments (the Parquet router through registry).
type Segmenter[P any] interface {
	// Plan is the sink's cut rule now (its encoders, its readiness),
	// false while it cannot be stated.
	Plan() (SegmentPlan, bool)
	// BeginSegments ends Sink.Write and stops whatever could still close
	// the open part (the interval cut), returning the rule in the same
	// step; false: it cannot be stated, and nothing changed.
	BeginSegments() (SegmentPlan, bool)
	// AbandonSegments undoes BeginSegments: Sink.Write goes on.
	AbandonSegments()
	// OpenSegment opens the next segment's part, in order: the open part
	// when continueOpen, else a new one.
	OpenSegment(ctx context.Context, continueOpen bool) (SegmentSink[P], error)
}

// SegmentPlan is a Segmenter's cut rule (parquetout.SegmentPlan): a part
// closes after the row whose logical bytes bring it to Limit, or else after
// MaxRows rows (0: none). Logical and Rows are the open part's (Open); Full:
// it closes before the next row. Encoders is the encoders admitted; Queue
// the batches a segment queues to its part.
type SegmentPlan struct {
	Limit, MaxRows int64
	Logical, Rows  int64
	Open, Full     bool
	Encoders       int
	Queue          int
}

// SegmentSink is one segment's part. Close closes it (not the last) and
// returns once its batches have been taken (or ctx is done), so a worker
// holds one part's queue at a time;
// Hold hands the last back to the sink as its open part, which the sink
// closes, final, only once the whole run has checked out (the Parquet
// router's Close, after the stage's conservation and the pipeline's
// reconciliation). rowCut: the row cap closed the part.
type SegmentSink[P any] interface {
	Write(ctx context.Context, segs [][]schema.Row[P]) error
	Close(ctx context.Context, rowCut bool)
	Hold(rowCut bool)
}

// SegmentStats describe the segment-parallel replay. Reason is why it ran
// or not (Segment*). Rows are the rows the segments handed on, MaxRows the
// largest segment's; Time sums the segments' wall time (open to close),
// MaxTime the longest; Plan is the planning time (the boundary frames
// decoded); Seams the seams checked. Time splits into Read (spill frames
// read, Decode of it decoding), Write (handing batches to the part, its
// encoder's queue wait included) and the rest (opening the part, the order
// guard). BudgetWaits are the times a worker waited for the row budget
// before reading or gathering a unit (segReserve), BudgetWait their time;
// MaxHeld the most the segments held at once beside the tree: the
// encoders' queues and the units the workers read or gathered.
type SegmentStats struct {
	Reason              string
	Segments, Workers   int
	Rows, MaxRows       int64
	Seams               int64
	Time, MaxTime       time.Duration
	Plan                time.Duration
	Read, Decode, Write time.Duration
	BudgetWaits         int64
	BudgetWait          time.Duration
	MaxHeld             int64
	// WorkersBy names what set Workers (Workers*); PerWorker is the
	// budget bytes one worker holds at most (segmentShare), Free the
	// budget the segments could take when they were planned (beside the
	// tree, less what the encoders' queues held).
	WorkersBy       string
	PerWorker, Free int64
	// Tree is the budget bytes the tree held when the segments were
	// planned: what the segments' budget does not count (the serial
	// drain holds it too).
	Tree int64
	// DurableFiles is the durable spill files (order.Cache) the segments
	// read frames of; DurableBoundaries the segment boundaries that fall
	// inside one of their frames (a span of it on either side).
	DurableFiles, DurableBoundaries int
}

// What set the segment workers' count (SegmentStats.WorkersBy).
const (
	WorkersByEncoders    = "encoders"    // the encoders admitted after listing
	WorkersByProcessors  = "processors"  // one per spare processor, at most one per two
	WorkersByDescriptors = "descriptors" // Options.SegmentWorkers
	WorkersByRowBudget   = "row_budget"  // what the row budget leaves holds no more (segmentShare)
	WorkersBySegments    = "segments"    // fewer segments than workers
)

// Why the segment-parallel replay ran, or not (SegmentStats.Reason).
const (
	SegmentEngaged       = "engaged"
	SegmentOneProcessor  = "one_processor"  // at most one processor beside the drainer (GOMAXPROCS 2 or less)
	SegmentOneEncoder    = "one_encoder"    // one encoder admitted: parts encode one at a time anyway
	SegmentSinglePart    = "single_part"    // what was left after listing fits one part
	SegmentNotPlanned    = "not_planned"    // the sink could not state its rule before the drain ended (part 0 not measured, a recovered boundary ahead)
	SegmentNotListedDone = "listing_active" // the drain ended before listing did
	SegmentRowBudget     = "row_budget"     // the row budget holds one segment worker only
)

// minSegmentWorkers and a worker per two processors cap the segment
// workers: each drives its part's encoder as well as reading and decoding,
// so two processors per worker are about what one keeps busy. Measured
// (sorted Parquet, 2026-11-07): at 16 vCPU, 8 workers beat 15 (0.75× against
// 0.81× of the serial build's wall on 64 lists); at 32 cores 31 workers
// bought 10 s of a 140 s tail over 8 and cost 3 GB of RSS (6.3 against
// 3.4 GB), the tail then CPU-bound on encoding and spill read-back.
const minSegmentWorkers = 8

// The segment workers are at most the encoders admitted (each feeds one
// part's writer, admitted by its working set, parquetout.EncoderBytes),
// one per spare processor (spareProcs: each reads and decodes, with its own
// zstd decoder), and Options.SegmentWorkers (the descriptors: one spill
// read each, at a time). What they hold is within the row budget
// (segReserve): a worker reserves a spilled frame's bytes before it reads
// it, and hands them to the budget's downstream share (Shared, the
// encoders' queues) when it writes the frame's rows; rows held in memory
// move from the tree's share to the downstream one as they are written.

// segState is the drainer's account of the segment-parallel replay.
type segState[P any] struct {
	sg    Segmenter[P]
	tried bool
	// waitRows: the sink could not state its rule when the drainer had
	// handed on this many rows; it asks again once it has handed on more.
	waitRows int64
	st       SegmentStats
	// held: the budget bytes of the spilled frames the workers have
	// reserved and not yet written (mu); freed: the tree's bytes the
	// workers released, written rows held in memory (mu).
	held, freed int64
	// uses counts, per unit, the segments that have yet to read it: the
	// last drops the stage's reference to its rows (segment).
	uses []atomic.Int32
}

// unit is a run of rows still to hand on: an in-memory item's rows, or a
// spilled frame.
type unit[P any] struct {
	rows       []schema.Row[P]
	f          *spillFile
	at         spillAt
	n, logical int64
	units      int64 // what its rows count against the row budget
}

// segment is one part's rows: from row r0 of unit u0 to row r1 of unit u1
// (exclusive; r1 0 ends at the end of unit u1-1).
type segment struct {
	u0, r0, u1, r1 int
	cont           bool // continues the sink's open part
	rowCut         bool // closed by the row cap
	final          bool
}

// segmentsDue reports whether the drainer should try the segments now.
func (o *run[R, P]) segmentsDue() bool {
	return o.seg != nil && !o.seg.tried && o.listedNotified.Load() && o.handed() > o.seg.waitRows
}

// handed is the rows the drainer has handed on, batched ones included.
func (o *run[R, P]) handed() int64 { return o.rows + int64(o.segRows) }

// trySegments runs the rest of the drain as segments when it can (mu
// held, and on return); done: the drain is complete.
func (o *run[R, P]) trySegments() (bool, error) {
	sg := o.seg.sg
	if spareProcs() < 1 { // one processor: nothing to run beside the drainer
		o.seg.tried, o.seg.st.Reason = true, SegmentOneProcessor
		return false, nil
	}
	// The sink is called without the stage's lock: the router's lock is
	// held by its interval cut while part 0's encoder, accounting its
	// bytes, wakes the stage (Shared.Add takes o.mu).
	o.mu.Unlock()
	if hookSegmenter != nil {
		hookSegmenter(&o.mu)
	}
	pl, ok := sg.Plan()
	o.mu.Lock()
	if !ok {
		o.seg.st.Reason, o.seg.waitRows = SegmentNotPlanned, o.handed()
		return false, nil
	}
	k, by := pl.Encoders, WorkersByEncoders
	if p := min(spareProcs(), max(minSegmentWorkers, aheadProcs()/2)); p < k {
		k, by = p, WorkersByProcessors
	}
	if c := o.opt.SegmentWorkers; c > 0 && c < k {
		k, by = c, WorkersByDescriptors
	}
	if k < 2 {
		o.seg.tried, o.seg.st.Reason = true, SegmentOneEncoder
		if pl.Encoders >= 2 {
			o.seg.st.Reason = SegmentOneProcessor // one spare processor: one segment at a time is the serial drain
		}
		return false, nil
	}
	o.mu.Unlock()
	err := o.flush() // the sink's open part must hold every row handed on
	o.mu.Lock()
	if err != nil {
		return false, err
	}
	t := time.Now()
	units, files, released, releasedMeta, nodes, markers, mem := o.flatten()
	tree := o.buffered
	// What the segments may hold is the budget beside the tree: the tree
	// holds about the budget once listing has ended (spill keeps it
	// there), and the serial drain adds its frames and its part's queue on
	// top as well; the segments' queues and frames together stay within
	// one budget more (segReserve).
	free := o.opt.Budget
	if sh := o.opt.Shared; sh != nil {
		free -= sh.Load()
	}
	o.mu.Unlock()
	per := segmentShare(units, pl)
	if hookShare != nil {
		per = hookShare(per)
	}
	if kb := budgetWorkers(free, per); kb < k {
		k, by = kb, WorkersByRowBudget
	}
	// Every spill file's trailer must count the rows replayed from it
	// before any segment's part is closed: a part closed is a part the
	// router may journal.
	err = o.checkTrailers(files)
	if err == nil && hookSegmenter != nil {
		hookSegmenter(&o.mu)
	}
	var segs []segment
	began := false
	if err == nil {
		// The definitive rule: the router stops its interval cut and
		// states the open part at once, so no cut can close it after.
		if pl, began = sg.BeginSegments(); began {
			segs, err = o.cut(units, pl)
		}
	}
	o.seg.st.Plan = time.Since(t)
	real := 0
	for _, s := range segs {
		if s.u0 < s.u1 || s.r0 < s.r1 && s.u0 == s.u1 {
			real++
		}
	}
	tight := err == nil && began && real >= 2 && k < 2
	if err != nil || !began || real < 2 || tight {
		if began {
			sg.AbandonSegments() // the drainer writes on, the interval cut resumes
		}
		o.mu.Lock()
		switch {
		case err != nil:
			return false, err
		case !began:
			o.seg.st.Reason, o.seg.waitRows = SegmentNotPlanned, o.handed()
		case tight:
			o.seg.tried, o.seg.st.Reason = true, SegmentRowBudget
			o.seg.st.WorkersBy, o.seg.st.PerWorker, o.seg.st.Free = by, per, free
		default:
			o.seg.tried, o.seg.st.Reason = true, SegmentSinglePart
		}
		return false, nil
	}
	o.seg.tried = true
	// Committed: the tree's remaining items are the segments'.
	o.mu.Lock()
	o.seg.st.Reason, o.seg.st.Segments, o.seg.st.Workers = SegmentEngaged, len(segs), min(k, len(segs))
	if len(segs) < k {
		by = WorkersBySegments
	}
	o.seg.st.WorkersBy, o.seg.st.PerWorker, o.seg.st.Free, o.seg.st.Tree = by, per, free, tree
	o.seg.st.DurableFiles, o.seg.st.DurableBoundaries = durableUse(units, segs)
	// The units are the rows' only reference now: the tree's items drop
	// theirs, and the last segment to read a unit drops it (segment).
	for _, it := range mem {
		it.rows = nil
	}
	o.seg.uses = make([]atomic.Int32, len(units))
	for _, sg := range segs {
		for ui := sg.u0; ui < sg.u1 || (ui == sg.u1 && sg.r1 > 0); ui++ {
			o.seg.uses[ui].Add(1)
		}
	}
	if ra := o.ra; ra != nil {
		// The read-ahead stops, and its reads in flight end (their
		// descriptors and decoded rows) before the segments read.
		ra.off = true
		for ra.reads > 0 {
			ra.waiting = true
			o.waitAt(o.drain, waitFetch)
		}
		ra.waiting = false
		o.buffered -= ra.held
		ra.held, ra.q = 0, nil
	}
	o.mu.Unlock()
	if o.sp != nil {
		o.sp.closeReaders() // the drainer reads no more frames
	}
	o.stalled()
	err = faults.At("segments-begun") // tests: killed with the segments planned, none opened
	if err == nil {
		err = o.runSegments(units, segs, o.seg.st.Workers)
	}
	if err == nil {
		err = faults.At("segments-written") // tests: killed with every row handed on, the last part held
	}
	if err == nil {
		// Every spill file the segments finished is done as the drainer's
		// trailer read leaves it: a run-private file removed and its credit
		// returned, a durable one handed back to its cache (drainedFile).
		for f := range files {
			if e := o.sp.drained(f); e != nil && err == nil {
				err = e
			}
		}
	}
	o.advanced = time.Now()
	o.mu.Lock()
	if err != nil {
		return false, err
	}
	o.released += released - o.seg.freed // what the workers released is gone already
	o.releasedMeta += releasedMeta
	o.g.nodes.add(-nodes)
	o.g.markers.add(-markers)
	o.path = nil
	return true, nil
}

// segmentShare is the budget bytes one segment worker holds at most. Its
// rows in hand are consecutive: the batch it gathers, its part's queue
// (pl.Queue batches; the part the first segment continues is held to as
// many, parquetout's tokens) and the batch its encoder writes, at most
// Queue+2 batches of batchRows rows and one unit's, and one batch more:
// the batch being handed on is counted twice while its Write waits for
// room (its reservation, and the queue's charge, taken first). They are
// priced at the most any
// run of that many consecutive rows weighs (runBytes: a cluster of long
// keys is priced as such, not at the mean); beside them the largest unit
// (a spilled frame, or rows in memory) it reads or gathers next.
func segmentShare[P any](units []unit[P], pl SegmentPlan) int64 {
	var frame, rows int64 // the unit it reads or gathers next, beside them; the most rows in one
	for i := range units {
		frame = max(frame, units[i].units)
		rows = max(rows, units[i].n)
	}
	batch := batchRows + rows // a batch closes at the unit that brings it to batchRows
	return frame + runBytes(units, int64(pl.Queue+3)*batch)
}

// durableUse counts the durable spill files among units and the segment
// boundaries inside one of their frames (SegmentStats).
func durableUse[P any](units []unit[P], segs []segment) (files, boundaries int) {
	seen := map[*spillFile]bool{}
	for i := range units {
		if f := units[i].f; f != nil && f.d != nil && !seen[f] {
			seen[f] = true
			files++
		}
	}
	for _, s := range segs[1:] {
		if s.r0 > 0 && s.u0 < len(units) && units[s.u0].f != nil && units[s.u0].f.d != nil {
			boundaries++
		}
	}
	return files, boundaries
}

// runBytes is the most budget bytes any run of consecutive units holding
// rows rows weighs: over every unit it starts at, the units up to the
// first that brings the run to rows (all of them when fewer remain).
func runBytes[P any](units []unit[P], rows int64) int64 {
	var most, n, bytes int64
	j := 0
	for i := range units {
		for j < len(units) && n < rows {
			n += units[j].n
			bytes += units[j].units
			j++
		}
		most = max(most, bytes)
		n -= units[i].n
		bytes -= units[i].units
	}
	return most
}

// budgetWorkers is how many segment workers of per bytes each the budget
// the segments may take (free) holds: at least one.
func budgetWorkers(free, per int64) int {
	if per <= 0 {
		return 1
	}
	return int(max(1, free/per))
}

// segReserve reserves c budget bytes for a unit a segment worker is about
// to read or gather (mu not held): the encoders' queues (Shared) and the
// frames reserved already, with c, stay within the budget beside the
// tree; past it the worker waits (waitSegment) for the encoders or
// another worker to release, unless nothing is held, when it goes on alone
// (a frame larger than the budget still passes, one at a time, as on the
// serial drain). The workers are sized to the budget when planned
// (budgetWorkers), so this is a safety: it waits only when the plan's
// estimate (segmentShare) is short. wait false: false at once instead of
// waiting.
func (o *run[R, P]) segReserve(c int64, wait bool, tm *segTimes) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var t time.Time
	for {
		if o.stopped.Load() {
			return false, context.Cause(o.ctx)
		}
		held := o.seg.held
		if s := o.opt.Shared; s != nil {
			held += s.Load()
		}
		if held == 0 || held+c <= o.opt.Budget {
			o.seg.held += c
			o.seg.st.MaxHeld = max(o.seg.st.MaxHeld, held+c)
			break
		}
		if !wait {
			return false, nil
		}
		if t.IsZero() {
			t = time.Now()
			o.seg.st.BudgetWaits++
		}
		o.waitAt(o.drain, waitSegment)
	}
	if !t.IsZero() {
		tm.budget += time.Since(t)
	}
	return true, nil
}

// segRelease returns c reserved bytes, written now (counted downstream),
// and the tree's bytes tree of in-memory rows written (mu not held).
func (o *run[R, P]) segRelease(c, tree int64) {
	if c == 0 && tree == 0 {
		return
	}
	o.mu.Lock()
	o.seg.held -= c
	o.buffered -= tree
	o.seg.freed += tree
	o.drain.Broadcast()
	o.mu.Unlock()
}

// hookShare, if set (tests), replaces segmentShare's estimate.
var hookShare func(per int64) int64

// hookSegmenter runs (tests) just before the drainer calls its Segmenter;
// mu is the stage's lock, which must not be held then.
var hookSegmenter func(mu *sync.Mutex)

// flatten lists the rows still to hand on, in drain order, from the
// drainer's walk (mu held; every node is done), with the rows each spill
// file must account for at its trailer, the budget units the items hold,
// the nodes and markers they release, and the items holding rows in memory
// (whose rows the segments, once committed, own instead).
func (o *run[R, P]) flatten() (units []unit[P], files map[*spillFile]int64, released, meta, nodes, markers int64, mem []*item[R, P]) {
	files = map[*spillFile]int64{}
	path := append([]drainFrame[R, P](nil), o.path...)
	for len(path) > 0 {
		f := &path[len(path)-1]
		if f.i >= len(f.n.items) {
			if sf := f.n.spill; sf != nil {
				files[sf] += sf.read // what the drainer replayed itself
			}
			path = path[:len(path)-1]
			nodes++
			continue
		}
		it := f.n.items[f.i]
		f.i++
		released += it.units
		switch {
		case it.child != nil:
			meta += it.units
			path = append(path, drainFrame[R, P]{n: it.child})
		case it.spill != nil:
			meta += it.units
			markers++
			files[it.spill] += it.at.rows
			units = append(units, unit[P]{f: it.spill, at: it.at, n: it.at.rows, logical: it.at.logical, units: it.at.units})
		case len(it.rows) > 0:
			mem = append(mem, &f.n.items[f.i-1])
			var l int64
			for i := range it.rows {
				l += o.schema.LogicalBytes(&it.rows[i])
			}
			units = append(units, unit[P]{rows: it.rows, n: int64(len(it.rows)), logical: l, units: it.units})
		}
	}
	return units, files, released, meta, nodes, markers, mem
}

// rowsTimed is a unit's rows, with the frame's decoding time: in memory,
// or its frame read and decoded on its own descriptor; a frame holding
// other than the rows its marker recorded is refused.
func (o *run[R, P]) rowsTimed(u *unit[P]) ([]schema.Row[P], time.Duration, error) {
	if u.f == nil {
		return u.rows, 0, nil
	}
	rows, dec, err := o.sp.readAt(u.f, u.at.off, u.at.size)
	if err != nil {
		return nil, 0, err
	}
	if int64(len(rows)) != u.n {
		return nil, 0, fmt.Errorf("spill: %s frame at %d holds %d rows, its marker %d", u.f.path, u.at.off, len(rows), u.n)
	}
	return rows, dec, nil
}

// segTimes is one segment's time by what it did (SegmentStats).
type segTimes struct{ read, decode, write, budget time.Duration }

// cut cuts units into segments where the sink would close its parts: a
// unit that cannot hold a boundary (its logical bytes and rows do not reach
// the rule) is passed whole; one that can is walked row by row, a spilled
// one decoded for it.
func (o *run[R, P]) cut(units []unit[P], pl SegmentPlan) ([]segment, error) {
	segs := []segment{{cont: pl.Open}}
	logical, rows := pl.Logical, pl.Rows
	next := pl.Open && pl.Full // the next row opens a new part
	split := func(u, r int) {
		cur := &segs[len(segs)-1]
		cur.u1, cur.r1 = u, r
		segs = append(segs, segment{u0: u, r0: r})
		logical, rows, next = 0, 0, false
	}
	for ui := range units {
		u := &units[ui]
		if next {
			split(ui, 0)
		}
		if logical+u.logical < pl.Limit && (pl.MaxRows == 0 || rows+u.n < pl.MaxRows) {
			logical += u.logical
			rows += u.n
			continue
		}
		rs, _, err := o.rowsTimed(u)
		if err != nil {
			return nil, err
		}
		for i := range rs {
			if next {
				split(ui, i)
			}
			rows++
			logical += o.schema.LogicalBytes(&rs[i])
			if logical >= pl.Limit {
				next = true
			} else if pl.MaxRows > 0 && rows >= pl.MaxRows {
				next = true
				segs[len(segs)-1].rowCut = true
			}
		}
	}
	last := &segs[len(segs)-1]
	last.u1, last.r1, last.final = len(units), 0, true
	return segs, nil
}

// runSegments writes the segments with k workers: a worker takes the next
// segment, opens its part (in order: parts are opened in ordinal order),
// hands its rows on, and closes it. Any failure stops every worker; a
// segment that did not finish leaves its part unclosed, so it is never
// published (the router's abort discards it).
func (o *run[R, P]) runSegments(units []unit[P], segs []segment, k int) error {
	var mu sync.Mutex // hands segments out and opens their parts, in order
	next := 0
	first := make([]string, len(segs))
	last := make([]string, len(segs))
	has := make([]bool, len(segs))
	times := make([]time.Duration, len(segs))
	parts := make([]segTimes, len(segs))
	counts := make([]int64, len(segs))
	var wg sync.WaitGroup
	var failed atomic.Bool
	fail := func(err error) {
		if failed.CompareAndSwap(false, true) {
			o.cancel(err)
		}
		o.stopped.Store(true)
	}
	for range k {
		wg.Go(func() {
			for !o.stopped.Load() {
				mu.Lock()
				j := next
				if j >= len(segs) {
					mu.Unlock()
					return
				}
				next++
				t := time.Now()
				out, err := o.seg.sg.OpenSegment(o.ctx, segs[j].cont)
				mu.Unlock()
				if err != nil {
					fail(err)
					return
				}
				n, f, l, ok, err := o.segment(units, segs[j], j == 0, out, &parts[j])
				if err != nil {
					fail(err)
					return
				}
				if segs[j].final {
					out.Hold(segs[j].rowCut)
				} else {
					out.Close(o.ctx, segs[j].rowCut)
				}
				first[j], last[j], has[j], counts[j], times[j] = f, l, ok, n, time.Since(t)
			}
		})
	}
	wg.Wait()
	if failed.Load() || o.stopped.Load() {
		return context.Cause(o.ctx)
	}
	st := &o.seg.st
	prev, havePrev := o.last, o.hasLast
	for j := range segs {
		st.Rows += counts[j]
		st.MaxRows = max(st.MaxRows, counts[j])
		st.Time += times[j]
		st.MaxTime = max(st.MaxTime, times[j])
		st.Read += parts[j].read
		st.Decode += parts[j].decode
		st.Write += parts[j].write
		st.BudgetWait += parts[j].budget
		if !has[j] {
			continue
		}
		// The seam check is the stage's own: the row guard runs within a
		// segment only, and Segmenter does not promise to compare its parts.
		// The Parquet router does (adjacent, before publishing); any other
		// Segmenter would hand on a descending seam without this.
		if havePrev && j > 0 { // segment 0's first row was checked against the drainer's last
			st.Seams++
			if first[j] <= prev {
				return orderError(prev, first[j], o.rows, false)
			}
		}
		prev, havePrev = last[j], true
	}
	o.rows += st.Rows
	o.last, o.hasLast = prev, havePrev
	return nil
}

// segment hands one segment's rows to its part in batches, through the
// order guard (the first segment also after the drainer's last key). It
// returns the rows, the first and last keys, and whether there were any.
func (o *run[R, P]) segment(units []unit[P], s segment, first bool, out SegmentSink[P], tm *segTimes) (n int64, lo, hi string, any bool, err error) {
	// The batch's spilled frames' reserved bytes (held) and the tree bytes
	// of the in-memory units it finishes (tree): written, the batch's rows
	// are counted downstream instead.
	var batch [][]schema.Row[P]
	var batched int
	var held, tree int64
	write := func() error {
		o.segRelease(0, tree)
		tree = 0
		if len(batch) == 0 {
			return nil
		}
		t := time.Now()
		err := out.Write(o.ctx, batch)
		tm.write += time.Since(t)
		o.segRelease(held, 0)
		batch, batched, held, tree = nil, 0, 0, 0
		return err
	}
	defer func() { o.segRelease(held, 0) }() // on failure
	prev, have := "", false
	if first {
		prev, have = o.last, o.hasLast
	}
	for ui := s.u0; ui < s.u1 || (ui == s.u1 && s.r1 > 0); ui++ {
		if o.stopped.Load() {
			return 0, "", "", false, context.Cause(o.ctx)
		}
		u := &units[ui]
		{
			// The unit's bytes within the budget before it is read (a
			// frame) or gathered (rows in memory, which its part's queue
			// then counts): a batch gathered so far is written first if
			// they are not.
			ok, err := o.segReserve(u.units, false, tm)
			if err == nil && !ok {
				if err = write(); err == nil {
					_, err = o.segReserve(u.units, true, tm)
				}
			}
			if err != nil {
				return 0, "", "", false, err
			}
			held += u.units
		}
		t := time.Now()
		rs, dec, err := o.rowsTimed(u)
		if u.f != nil {
			tm.read += time.Since(t)
			tm.decode += dec
		}
		if err != nil {
			return 0, "", "", false, err
		}
		a, b := 0, len(rs)
		if ui == s.u0 {
			a = s.r0
		}
		if ui == s.u1 {
			b = s.r1
		}
		whole := a == 0 && b == len(rs)
		rs = rs[a:b]
		if !whole && len(rs) > 0 {
			// A span of the unit: copied, so the batch does not keep the
			// whole frame (or the whole in-memory chunk) alive while its
			// queue counts the span alone.
			rs = o.schema.Detach(rs)
		}
		if o.seg.uses[ui].Add(-1) == 0 && u.f == nil {
			// The last segment to read the unit: its rows are referenced
			// only by this batch now (whole: their charge moves to the
			// queue as it is written), or by nothing (spans were copied).
			u.rows = nil
			if whole {
				tree += u.units
			} else {
				o.segRelease(0, u.units)
			}
		}
		if len(rs) == 0 {
			continue
		}
		for i := range rs {
			if k := rs[i].Key; have && k <= prev {
				return 0, "", "", false, orderError(prev, k, o.rows+n+int64(i), first && n == 0 && i == 0 && o.opt.After != nil)
			} else {
				prev, have = k, true
			}
		}
		if !any {
			lo, any = strings.Clone(rs[0].Key), true // kept for the seam check: not the rows' storage
		}
		hi = rs[len(rs)-1].Key
		n += int64(len(rs))
		o.g.ahead.add(-int64(len(rs)))
		batch = append(batch, rs)
		if batched += len(rs); batched >= batchRows {
			if err := write(); err != nil {
				return 0, "", "", false, err
			}
		}
	}
	if err := write(); err != nil {
		return 0, "", "", false, err
	}
	// The last key is kept until every segment has finished (the seam
	// check): a copy, so it does not keep its frame or span alive.
	return n, lo, strings.Clone(hi), any, nil
}

// checkTrailers checks, for every spill file the segments finished, that
// its trailer counts the rows replayed from it.
func (o *run[R, P]) checkTrailers(files map[*spillFile]int64) error {
	for f, rows := range files {
		n, err := o.sp.trailerRows(f)
		if err != nil {
			return err
		}
		if n != rows {
			return &dataset.InvariantError{Reason: fmt.Sprintf("spill: %s holds %d rows, trailer says %d", f.path, rows, n)}
		}
	}
	return nil
}
