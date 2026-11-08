// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
)

// Segments. Once the order stage knows the rest of the stream (listing is
// done and every range is immutable), it can cut it into parts itself,
// with the router's own rule (Plan), and write consecutive parts at once
// from several goroutines (OpenSegment): each segment is one part, opened
// in ordinal order, encoded and published as any other (publication stays
// in ordinal order, adjacency checked before each). The interval cut stops
// when segments begin: the rest is cut by size and row count only.

// Plan is the cut rule for the rows still to come, or false while it
// cannot be stated: before the size ratio is frozen (part 0 measured),
// with a recovered boundary (Options.Cut) still ahead, after a failure,
// or once segments began.
func (r *Router[P]) Plan() (dataset.SegmentPlan, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.plan()
}

// plan is Plan with mu held.
func (r *Router[P]) plan() (dataset.SegmentPlan, bool) {
	if r.closed || r.segmented || r.limit == 0 || r.firstErr() != nil {
		return dataset.SegmentPlan{}, false
	}
	if c := r.opt.Cut; c != nil {
		first := len(r.done) + len(r.parts)
		if r.cur != nil {
			first = r.cur.ordinal
		}
		if c.Ordinal >= first {
			return dataset.SegmentPlan{}, false
		}
	}
	// Queue: to the open part a segment continues, whose queue is deeper,
	// as many batches as to a new one (Segment.Write's tokens).
	pl := dataset.SegmentPlan{Limit: r.limit, MaxRows: r.opt.MaxRows, Encoders: r.Encoders(), Queue: segmentQueue}
	if r.cur != nil {
		pl.Open, pl.Full, pl.Logical, pl.Rows = true, r.full, r.cur.logical, r.cur.n
	}
	return pl, true
}

// BeginSegments ends Write: the rest of the stream goes to segments. The
// interval cut stops first, and the rule is stated in the same step, so no
// cut can close the open part after the caller has planned against it.
// False: the rule cannot be stated, and nothing changed (the interval cut
// is restarted).
func (r *Router[P]) BeginSegments() (dataset.SegmentPlan, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.plan(); !ok {
		return dataset.SegmentPlan{}, false
	}
	r.stop() // releases mu while the ticker returns
	pl, ok := r.plan()
	if !ok {
		r.startTick()
		return dataset.SegmentPlan{}, false
	}
	r.segmented = true
	return pl, true
}

// AbandonSegments undoes BeginSegments: Write goes on, and the interval
// cut with it.
func (r *Router[P]) AbandonSegments() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.segmented && !r.closed {
		r.segmented = false
		r.startTick()
	}
}

// segmentQueue bounds the batches (4,096 rows or more each) queued to a
// segment's part: its one producer only needs to stay a batch ahead of the
// encoder, and with tens of parts open at once a deep queue (partQueue)
// would hold tens of batches each.
const segmentQueue = 2

// Segment is one part written by one goroutine (OpenSegment).
type Segment[P any] struct {
	r *Router[P]
	p *part[P]
}

// OpenSegment opens the part for the next segment: the open part when
// continueOpen, else a new one, waiting for an encoder slot. Calls must
// come in the segments' order (ordinals are dense and in order).
func (r *Router[P]) OpenSegment(ctx context.Context, continueOpen bool) (*Segment[P], error) {
	if continueOpen {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.cur == nil {
			return nil, errors.New("parquet router: no open part to continue")
		}
		p := r.cur
		p.segment, p.tokens = true, make(chan struct{}, segmentQueue+1)
		r.cur, r.full = nil, false
		return &Segment[P]{r: r, p: p}, nil
	}
	if err := r.firstErr(); err != nil {
		return nil, err
	}
	// The slot is taken without the lock: a segment closing its part
	// (Close) takes it, and that is what frees a slot.
	select {
	case r.slots <- struct{}{}:
	default:
		t := time.Now()
		select {
		case r.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
		r.waitNs.Add(int64(time.Since(t)))
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		<-r.slots
		return nil, errors.New("parquet router: closed")
	}
	o := len(r.done) + len(r.parts)
	if o >= dataset.MaxParts {
		var last *part[P]
		if n := len(r.parts); n > 0 {
			last = r.parts[n-1]
		}
		rows := r.rows + r.segRows.Load()
		r.mu.Unlock()
		defer func() { <-r.slots }()
		if last != nil {
			// The refusal waits for the durable prefix, but not past a
			// cancellation: a segment that failed leaves its part open
			// (never published) until the pipeline's Abort, which runs
			// only once every segment worker has returned.
			select {
			case <-last.published:
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
		}
		return nil, fmt.Errorf("%w: part %d cannot be opened (%d rows so far; part size %d bytes, row cap %d)",
			dataset.ErrPartLimit, o, rows, r.opt.PartSize, r.opt.MaxRows)
	}
	p := &part[P]{ordinal: o, opened: time.Now(), segs: make(chan batch[P], segmentQueue), done: make(chan struct{}), drained: make(chan struct{}),
		published: make(chan struct{}), limit: r.limit, segment: true}
	if n := len(r.parts); n > 0 {
		p.prev = r.parts[n-1]
	}
	r.parts = append(r.parts, p)
	r.active.Add(1)
	r.mu.Unlock()
	go r.encode(p)
	return &Segment[P]{r: r, p: p}, nil
}

// Write queues ascending rows to the segment's part.
func (s *Segment[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	var n int64
	for _, seg := range segs {
		n += int64(len(seg))
	}
	s.r.segRows.Add(n)
	if t := s.p.tokens; t != nil {
		select {
		case t <- struct{}{}:
		case <-s.p.done:
			return s.p.err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return s.r.sendTo(ctx, s.p, segs, s.p.tokens)
}

// Close closes the segment's part (never the last: Hold); rowCut says the
// row cap closed it (RowCuts). It returns once the part's encoder has
// taken every batch queued (or ctx is done): a worker then opens its next
// part holding no batches of the last, so it holds one part's queue at a
// time, as its share of the row budget assumes (order.segmentShare).
func (s *Segment[P]) Close(ctx context.Context, rowCut bool) {
	s.r.mu.Lock()
	if s.p.closed {
		s.r.mu.Unlock()
		return
	}
	s.p.closed = true
	close(s.p.segs)
	if rowCut {
		s.r.rowCuts.Add(1)
	}
	s.r.mu.Unlock()
	select {
	case <-s.p.drained:
	case <-ctx.Done():
	}
}

// Hold hands the last segment's part back as the open part: Close marks
// it final and closes it, once the run has checked out, as for a part
// written by Write; Abort discards it.
func (s *Segment[P]) Hold(rowCut bool) {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	s.r.cur = s.p
	if rowCut {
		s.r.rowCuts.Add(1)
	}
}
