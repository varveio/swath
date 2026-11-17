// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package bind

import (
	"context"
	"sync/atomic"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/reader/listing"
)

// Sizes is the object bytes a listing emitted. Measured says the schema
// has a size column (Prepare sets it before listing); Unknown counts the
// objects whose size was null (the names metadata tier), which leave Bytes
// short.
type Sizes struct {
	Bytes    atomic.Int64
	Unknown  atomic.Int64
	Measured atomic.Bool
}

// Known is the bytes summed, false when the schema has no size column or
// an object's size was unknown.
func (s *Sizes) Known() (int64, bool) {
	return s.Bytes.Load(), s.Measured.Load() && s.Unknown.Load() == 0
}

// sized sums the object sizes of the rows the order stage accepts from a
// provider (after the filters, which it wraps), one atomic add per page.
// On a committed run that is exactly the rows emitted; on a run that
// stopped it also holds rows accepted and never handed on.
type sized[P any] struct {
	p    source.Provider[any, P]
	size func(*P) (int64, bool)
	n    *Sizes
}

func newSized[P any](p source.Provider[any, P], size func(*P) (int64, bool), n *Sizes) source.Provider[any, P] {
	s := sized[P]{p, size, n}
	if d, ok := p.(source.Disposer[any]); ok {
		return sizedDisposer[P]{s, d}
	}
	return s
}

func (s sized[P]) Root() any { return s.p.Root() }

func (s sized[P]) Expand(ctx context.Context, r any, e source.Emitter[any, P]) error {
	return s.p.Expand(ctx, r, sizeEmitter[P]{e, s.size, s.n})
}

// Fences and Replay forward the provider's (source.Fencing,
// source.Replaying): durable spill engages only when the whole chain from
// the provider to the order stage forwards them.
func (s sized[P]) Fences() bool {
	fc, ok := s.p.(source.Fencing)
	return ok && fc.Fences()
}

func (s sized[P]) Replay(r any) (int, bool) {
	if rp, ok := s.p.(source.Replaying[any]); ok {
		return rp.Replay(r)
	}
	return 0, false
}

// Planner forwards the provider's (source.Planning): the order stage
// admits a plan's tiles only when every adapter forwards it.
func (s sized[P]) Planner() source.Planner[any] {
	if pl, ok := s.p.(source.Planning[any]); ok {
		return pl.Planner()
	}
	return nil
}

// Dropped forwards the provider's (source.Dropper).
func (s sized[P]) Dropped(r any) (listing.Unread, bool) {
	if d, ok := s.p.(source.Dropper[any]); ok {
		return d.Dropped(r)
	}
	return listing.Unread{}, false
}

type sizedDisposer[P any] struct {
	sized[P]
	d source.Disposer[any]
}

func (s sizedDisposer[P]) Dispose(r any) { s.d.Dispose(r) }

type sizeEmitter[P any] struct {
	source.Emitter[any, P]
	size func(*P) (int64, bool)
	n    *Sizes
}

// Subtree forwards to a Subtreer emitter: the filters drop rows, not
// ranges, so a range still owns its prefix.
func (e sizeEmitter[P]) Subtree(prefix string) {
	if t, ok := e.Emitter.(source.Subtreer); ok {
		t.Subtree(prefix)
	}
}

// Span and Fence forward to a Fencer emitter.
func (e sizeEmitter[P]) Span(lo, hi string, open bool) {
	if f, ok := e.Emitter.(source.Fencer); ok {
		f.Span(lo, hi, open)
	}
}

func (e sizeEmitter[P]) Fence(x string, top bool) {
	if f, ok := e.Emitter.(source.Fencer); ok {
		f.Fence(x, top)
	}
}

// Unread forwards to an Unreader emitter.
func (e sizeEmitter[P]) Unread(u listing.Unread) {
	if ur, ok := e.Emitter.(source.Unreader); ok {
		ur.Unread(u)
	}
}

// Awaited forwards an Awaiter emitter's (false without one).
func (e sizeEmitter[P]) Awaited() bool {
	a, ok := e.Emitter.(source.Awaiter)
	return ok && a.Awaited()
}

// Rows hands the rows on and adds their sizes, and counts those unknown,
// once the order stage has accepted them: a page it refuses (the run
// stopping) is not counted. The sizes are summed first, because the slice
// passes to the order stage, which may reuse it.
func (e sizeEmitter[P]) Rows(rows []schema.Row[P]) error {
	var sum, unknown int64
	for i := range rows {
		if v, ok := e.size(&rows[i].Data); ok {
			sum += v
		} else {
			unknown++
		}
	}
	if err := e.Emitter.Rows(rows); err != nil {
		return err
	}
	e.n.Bytes.Add(sum)
	if unknown > 0 {
		e.n.Unknown.Add(unknown)
	}
	return nil
}
