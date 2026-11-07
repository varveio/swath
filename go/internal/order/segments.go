// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
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
// it closes before the next row. Encoders is the encoders admitted.
type SegmentPlan struct {
	Limit, MaxRows int64
	Logical, Rows  int64
	Open, Full     bool
	Encoders       int
}

// SegmentSink is one segment's part. Close closes it (not the last);
// Hold hands the last back to the sink as its open part, which the sink
// closes, final, only once the whole run has checked out (the Parquet
// router's Close, after the stage's conservation and the pipeline's
// reconciliation). rowCut: the row cap closed the part.
type SegmentSink[P any] interface {
	Write(ctx context.Context, segs [][]schema.Row[P]) error
	Close(rowCut bool)
	Hold(rowCut bool)
}

// SegmentStats describe the segment-parallel replay. Reason is why it ran
// or not (Segment*). Rows are the rows the segments handed on, MaxRows the
// largest segment's; Time sums the segments' wall time (open to close),
// MaxTime the longest; Plan is the planning time (the boundary frames
// decoded); Seams the seams checked. Time splits into Read (spill frames
// read, Decode of it decoding), Write (handing batches to the part, its
// encoder's queue wait included) and the rest (opening the part, the order
// guard).
type SegmentStats struct {
	Reason              string
	Segments, Workers   int
	Rows, MaxRows       int64
	Seams               int64
	Time, MaxTime       time.Duration
	Plan                time.Duration
	Read, Decode, Write time.Duration
}

// Why the segment-parallel replay ran, or not (SegmentStats.Reason).
const (
	SegmentEngaged       = "engaged"
	SegmentOneProcessor  = "one_processor"  // at most one processor beside the drainer (GOMAXPROCS 2 or less)
	SegmentOneEncoder    = "one_encoder"    // one encoder admitted: parts encode one at a time anyway
	SegmentSinglePart    = "single_part"    // what was left after listing fits one part
	SegmentNotPlanned    = "not_planned"    // the sink could not state its rule before the drain ended (part 0 not measured, a recovered boundary ahead)
	SegmentNotListedDone = "listing_active" // the drain ended before listing did
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
// read each, at a time). Each holds one decoded frame, one batch and its
// part's short queue (parquetout.segmentQueue).

// segState is the drainer's account of the segment-parallel replay.
type segState[P any] struct {
	sg    Segmenter[P]
	tried bool
	// waitRows: the sink could not state its rule when the drainer had
	// handed on this many rows; it asks again once it has handed on more.
	waitRows int64
	st       SegmentStats
}

// unit is a run of rows still to hand on: an in-memory item's rows, or a
// spilled frame.
type unit[P any] struct {
	rows       []schema.Row[P]
	f          *spillFile
	at         spillAt
	n, logical int64
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
	k := min(pl.Encoders, spareProcs(), max(minSegmentWorkers, aheadProcs()/2))
	if c := o.opt.SegmentWorkers; c > 0 {
		k = min(k, c)
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
	units, files, released, releasedMeta, nodes, markers := o.flatten()
	o.mu.Unlock()
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
	if err != nil || !began || real < 2 {
		if began {
			sg.AbandonSegments() // the drainer writes on, the interval cut resumes
		}
		o.mu.Lock()
		switch {
		case err != nil:
			return false, err
		case !began:
			o.seg.st.Reason, o.seg.waitRows = SegmentNotPlanned, o.handed()
		default:
			o.seg.tried, o.seg.st.Reason = true, SegmentSinglePart
		}
		return false, nil
	}
	o.seg.tried = true
	// Committed: the tree's remaining items are the segments'.
	o.mu.Lock()
	o.seg.st.Reason, o.seg.st.Segments, o.seg.st.Workers = SegmentEngaged, len(segs), min(k, len(segs))
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
	err = o.runSegments(units, segs, o.seg.st.Workers)
	o.advanced = time.Now()
	o.mu.Lock()
	if err != nil {
		return false, err
	}
	o.released += released
	o.releasedMeta += releasedMeta
	o.g.nodes.add(-nodes)
	o.g.markers.add(-markers)
	o.path = nil
	return true, nil
}

// hookSegmenter runs (tests) just before the drainer calls its Segmenter;
// mu is the stage's lock, which must not be held then.
var hookSegmenter func(mu *sync.Mutex)

// flatten lists the rows still to hand on, in drain order, from the
// drainer's walk (mu held; every node is done), with the rows each spill
// file must account for at its trailer, the budget units the items hold,
// and the nodes and markers they release.
func (o *run[R, P]) flatten() (units []unit[P], files map[*spillFile]int64, released, meta, nodes, markers int64) {
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
			units = append(units, unit[P]{f: it.spill, at: it.at, n: it.at.rows, logical: it.at.logical})
		case len(it.rows) > 0:
			var l int64
			for i := range it.rows {
				l += o.schema.LogicalBytes(&it.rows[i])
			}
			units = append(units, unit[P]{rows: it.rows, n: int64(len(it.rows)), logical: l})
		}
	}
	return units, files, released, meta, nodes, markers
}

// rowsOf is a unit's rows: in memory, or its frame read and decoded on its
// own descriptor; a frame holding other than the rows its marker recorded
// is refused.
func (o *run[R, P]) rowsOf(u *unit[P]) ([]schema.Row[P], error) {
	rows, _, err := o.rowsTimed(u)
	return rows, err
}

// rowsTimed is rowsOf with the frame's decoding time.
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
type segTimes struct{ read, decode, write time.Duration }

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
		rs, err := o.rowsOf(u)
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
					out.Close(segs[j].rowCut)
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
		if !has[j] {
			continue
		}
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
	write := func(batch [][]schema.Row[P]) error {
		t := time.Now()
		err := out.Write(o.ctx, batch)
		tm.write += time.Since(t)
		return err
	}
	prev, have := "", false
	if first {
		prev, have = o.last, o.hasLast
	}
	var batch [][]schema.Row[P]
	var batched int
	for ui := s.u0; ui < s.u1 || (ui == s.u1 && s.r1 > 0); ui++ {
		if o.stopped.Load() {
			return 0, "", "", false, context.Cause(o.ctx)
		}
		t := time.Now()
		rs, dec, err := o.rowsTimed(&units[ui])
		if units[ui].f != nil {
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
		rs = rs[a:b]
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
			lo, any = rs[0].Key, true
		}
		hi = rs[len(rs)-1].Key
		n += int64(len(rs))
		o.g.ahead.add(-int64(len(rs)))
		batch = append(batch, rs)
		if batched += len(rs); batched >= batchRows {
			if err := write(batch); err != nil {
				return 0, "", "", false, err
			}
			batch, batched = nil, 0
		}
	}
	if len(batch) > 0 {
		if err := write(batch); err != nil {
			return 0, "", "", false, err
		}
	}
	return n, lo, hi, any, nil
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
