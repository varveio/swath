// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
)

// The spill read-ahead. Once the provider has listed what is queued, the
// workers are idle while one drainer replays the spill: read, decode,
// order check and hand-on, one frame after another. A finished node's items
// never change, so the frames the drainer will replay next are known
// before it reaches them. The drainer plans them (planAhead: a cursor
// walks the finished nodes ahead of it, in drain order, and queues each
// spill marker it meets); idle workers claim the queued frames, read them
// on their own descriptors (spiller.readAt) and decode them; the drainer
// takes them from the head of the queue, in the order it planned them,
// and only checks their order and hands them on. The tree order is the
// queue order: the head of the queue must be the marker the drainer is
// at, or the run fails with an invariant error.
//
// Memory: a claimed frame's decoded rows are charged to the row budget
// (the units they were published with) until the drainer takes them, so
// producers still listing see them. The read-ahead holds at most
// aheadShare of the budget, or one frame when it holds nothing, past a
// full budget: the rows are the drainer's next, as the awaited node's
// lead is (leadLimit). Past its share a worker waits (waitAhead).

const (
	// aheadReads caps the frames being read at once: each holds a
	// descriptor for the read (MaxSpillReaders counts them for descriptor
	// admission) and a zstd decoder; eight decoders outrun one drainer
	// (measurements: go/docs/measurements.md).
	aheadReads = 8
	// aheadFrames caps the frames planned and not yet taken: enough to
	// keep aheadReads reads going while the drainer takes the frames read
	// before; the planner refills at half (one wake-up per aheadFrames/2
	// frames, not one per frame, for hundreds of idle workers).
	aheadFrames = 64
	// aheadScan caps the items one planning step walks (rows and children
	// between markers), so planning costs the drainer a bounded step.
	aheadScan = 1024
)

// aheadProcs is the processors the read-ahead may use (a variable for
// tests). Its reads are capped one below it, so the drainer keeps a
// processor; with one there is no read-ahead: the drainer replays alone,
// as an idle worker's read would only take turns with it.
var aheadProcs = func() int { return runtime.GOMAXPROCS(0) }

type fetchState uint8

const (
	pending fetchState = iota
	reading
	ready
)

// fetch is one spilled frame planned ahead of the drainer (mu).
type fetch[P any] struct {
	f     *spillFile
	at    spillAt
	state fetchState
	rows  []schema.Row[P]
	err   error
}

// readAhead is the planned frames and the cursor that plans them (mu).
type readAhead[R, P any] struct {
	cursor []drainFrame[R, P] // the planner's walk: finished nodes only
	q      []*fetch[P]        // planned and not yet taken by the drainer, in drain order
	next   int                // q's first fetch that may still be pending
	reads  int                // frames workers are reading
	// maxReads caps reads: aheadReads, at most aheadProcs()-1.
	maxReads int
	held     int64 // units charged for claimed frames not yet taken
	// starved: a worker waits for the read-ahead to drop below its share;
	// waiting: the drainer waits for a frame being read.
	starved, waiting bool
	off              bool // stopped for good (the segments read for themselves)
	st               ReadAheadStats
}

// aheadShare is the budget the read-ahead may hold past a full budget.
func (o *run[R, P]) aheadShare() int64 { return max(1, o.opt.Budget/4) }

// claimAhead takes the next pending planned frame for a worker (mu held),
// charging its rows to the budget; nil when there is none or reads are at
// their cap, with capped set when the read-ahead holds its share.
func (o *run[R, P]) claimAhead() (*fetch[P], bool) {
	ra := o.ra
	if ra == nil || ra.off || ra.reads >= ra.maxReads {
		return nil, false
	}
	for ra.next < len(ra.q) && ra.q[ra.next].state != pending {
		ra.next++
	}
	if ra.next == len(ra.q) {
		return nil, false
	}
	ft := ra.q[ra.next]
	if ra.held > 0 && ra.held+ft.at.units > o.aheadShare() {
		ra.starved = true
		return nil, true
	}
	ra.next++
	ft.state = reading
	ra.reads++
	ra.held += ft.at.units
	o.buffered += ft.at.units
	o.max = max(o.max, o.buffered)
	return ft, false
}

// fetchAhead reads and decodes a claimed frame; called with mu held, which
// it releases for the read.
func (o *run[R, P]) fetchAhead(ft *fetch[P]) {
	o.mu.Unlock()
	t := time.Now()
	rows, dec, err := o.readFrame(ft, false)
	d := time.Since(t)
	o.mu.Lock()
	ra := o.ra
	ft.rows, ft.err, ft.state = rows, err, ready
	ra.reads--
	ra.st.Read += d
	ra.st.Decode += dec
	if ra.waiting {
		o.drain.Broadcast()
	}
}

// hookReadAt sees every planned frame just before it is read, by a worker
// or by the drainer (tests).
var hookReadAt func(f *spillFile, at spillAt, drainer bool)

// readFrame reads a planned frame (without mu).
func (o *run[R, P]) readFrame(ft *fetch[P], drainer bool) ([]schema.Row[P], time.Duration, error) {
	if hookReadAt != nil {
		hookReadAt(ft.f, ft.at, drainer)
	}
	return o.sp.readAt(ft.f, ft.at.off, ft.at.size)
}

// idle accounts the part of a worker's idle wait, begun at t (mu held),
// after the listing finished.
func (o *run[R, P]) idle(t time.Time) {
	if o.listedAt.IsZero() {
		return
	}
	if o.listedAt.After(t) {
		t = o.listedAt
	}
	o.idleAfter += time.Since(t)
}

// planAhead queues the spill markers ahead of the drainer (mu held, on the
// drainer). reset restarts the cursor at the drainer's position: required
// whenever nothing is planned, since the drainer may then have passed the
// cursor, or the cursor have stopped at a node the drainer has since
// compacted; otherwise planning resumes where the cursor stopped, once
// the queue is down to half. The cursor enters finished nodes only.
func (o *run[R, P]) planAhead(reset bool) {
	ra := o.ra
	if reset {
		ra.cursor = append(ra.cursor[:0], o.path...)
	} else if len(ra.q) > aheadFrames/2 {
		return
	}
	planned := 0
	for scanned := 0; len(ra.q) < aheadFrames && scanned < aheadScan && len(ra.cursor) > 0; {
		c := &ra.cursor[len(ra.cursor)-1]
		if c.n.state != done {
			break
		}
		if c.i >= len(c.n.items) {
			ra.cursor = ra.cursor[:len(ra.cursor)-1]
			continue
		}
		it := &c.n.items[c.i]
		c.i++
		scanned++
		switch {
		case it.child != nil:
			ra.cursor = append(ra.cursor, drainFrame[R, P]{n: it.child})
		case it.spill != nil:
			ra.q = append(ra.q, &fetch[P]{f: it.spill, at: it.at})
			planned++
		}
	}
	// Wake as many workers as can start a read, not every idle one:
	// hundreds of wake-ups per refill would cost more than the reads. A
	// signal that reaches a worker held at admission is not lost work:
	// the drainer reads any frame still queued when it gets there.
	for range min(planned, ra.maxReads-ra.reads) {
		o.work.Signal()
	}
}

// replayMarker hands on the frame behind spill marker it, the drainer's
// next item (called without mu): read ahead if it was planned, else read
// here. Its marker's units are already counted as released.
func (o *run[R, P]) replayMarker(it *item[R, P]) error {
	if o.ra == nil {
		return o.replay(it.spill)
	}
	o.mu.Lock()
	ra := o.ra
	if len(ra.q) == 0 {
		ra.st.Unplanned++
		o.planAhead(true)
		o.mu.Unlock()
		t := time.Now()
		err := o.replay(it.spill)
		o.dt.ReplayRead += time.Since(t)
		return err
	}
	ft := ra.q[0]
	if ft.f != it.spill || ft.at.off != it.at.off {
		o.mu.Unlock()
		return &dataset.InvariantError{Reason: fmt.Sprintf("order: read-ahead out of order: planned %s@%d, drained %s@%d", ft.f.path, ft.at.off, it.spill.path, it.at.off)}
	}
	ra.q[0] = nil
	ra.q = ra.q[1:]
	ra.next = max(0, ra.next-1)
	var rows []schema.Row[P]
	var err error
	switch ft.state {
	case pending: // not claimed yet: read it here, uncharged
		ft.state = reading
		ra.st.DrainerFrames++
		o.mu.Unlock()
		t := time.Now()
		var dec time.Duration
		rows, dec, err = o.readFrame(ft, true)
		o.dt.ReplayRead += time.Since(t)
		o.dt.ReplayDecode += dec
		o.mu.Lock()
	default:
		if ft.state == reading {
			ra.st.Waits++
			t := time.Now()
			for ft.state == reading && !o.stopped.Load() {
				ra.waiting = true
				o.waitAt(o.drain, waitFetch)
			}
			ra.waiting = false
			o.dt.AheadWait += time.Since(t)
			if ft.state == reading { // stopped
				o.mu.Unlock()
				return context.Cause(o.ctx)
			}
		}
		rows, err = ft.rows, ft.err
		ra.held -= ft.at.units
		o.released += ft.at.units
		ra.st.Frames++
		ra.st.Rows += int64(len(rows))
		if ra.starved && ra.held <= o.aheadShare()/2 {
			ra.starved = false
			o.work.Broadcast()
		}
	}
	o.planAhead(len(ra.q) == 0)
	if o.released >= o.releaseEvery {
		o.release()
	}
	o.mu.Unlock()
	if err != nil {
		return err
	}
	o.sp.skip(it.spill, ft.at.off+ft.at.size, int64(len(rows)))
	return o.send(rows)
}

// spareReaders is how many read-ahead reads may run beside the drainer's:
// one per spare processor, at most aheadReads; 0 on one processor.
func spareReaders() int { return min(aheadReads, spareProcs()) }

// spareProcs is the processors beside the drainer's: the segment workers'
// cap, and the zstd decoders beside the drainer's (one per concurrent
// reader); 0 on one processor.
func spareProcs() int { return max(0, aheadProcs()-1) }
