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

// SegmentPlan is the router's cut rule for the rows still to come: a part
// closes after the row whose logical bytes (schema.LogicalBytes) bring it
// to Limit, or else after MaxRows rows (0: no row cap). Logical and Rows
// are the open part's so far (Open); Full: the open part is closed before
// the next row. Encoders is the encoders admitted now.
type SegmentPlan struct {
	Limit, MaxRows int64
	Logical, Rows  int64
	Open, Full     bool
	Encoders       int
}

// Plan is the cut rule for the rows still to come, or false while it
// cannot be stated: before the size ratio is frozen (part 0 measured),
// with a recovered boundary (Options.Cut) still ahead, after a failure,
// or once segments began.
func (r *Router[P]) Plan() (SegmentPlan, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.segmented || r.limit == 0 || r.firstErr() != nil {
		return SegmentPlan{}, false
	}
	if c := r.opt.Cut; c != nil {
		first := len(r.done) + len(r.parts)
		if r.cur != nil {
			first = r.cur.ordinal
		}
		if c.Ordinal >= first {
			return SegmentPlan{}, false
		}
	}
	pl := SegmentPlan{Limit: r.limit, MaxRows: r.opt.MaxRows, Encoders: r.Encoders()}
	if r.cur != nil {
		pl.Open, pl.Full, pl.Logical, pl.Rows = true, r.full, r.cur.logical, r.cur.n
	}
	return pl, true
}

// BeginSegments ends Write: the rest of the stream goes to segments. The
// interval cut stops.
func (r *Router[P]) BeginSegments() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stop()
	r.segmented = true
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
		if last != nil {
			<-last.published
		}
		<-r.slots
		return nil, fmt.Errorf("%w: part %d cannot be opened (%d rows so far; part size %d bytes, row cap %d)",
			dataset.ErrPartLimit, o, rows, r.opt.PartSize, r.opt.MaxRows)
	}
	p := &part[P]{ordinal: o, opened: time.Now(), segs: make(chan batch[P], segmentQueue), done: make(chan struct{}),
		published: make(chan struct{}), limit: r.limit}
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
	return s.r.sendTo(ctx, s.p, segs)
}

// Close closes the segment's part, the dataset's last when final; rowCut
// says the row cap closed it (RowCuts).
func (s *Segment[P]) Close(final, rowCut bool) {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	if s.p.closed {
		return
	}
	s.p.final, s.p.closed = final, true
	close(s.p.segs)
	if rowCut {
		s.r.rowCuts.Add(1)
	}
}
