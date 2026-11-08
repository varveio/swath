// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// filtered applies the row filters (package filter) between the provider
// and the order stage: each page's rows are compacted in place, so the
// filter costs its predicate per row and nothing else; the rows dropped are
// counted (the run reconciles listed = kept + dropped). A page where few
// rows survive hands on detached copies instead (see filterEmitter.Rows).
type filtered[P any] struct {
	p    source.Provider[any, P]
	keep func(*schema.Row[P]) bool
	s    *schema.Schema[P]
	n    *atomic.Int64
}

func newFiltered[P any](p source.Provider[any, P], keep func(*schema.Row[P]) bool, s *schema.Schema[P], n *atomic.Int64) source.Provider[any, P] {
	f := filtered[P]{p, keep, s, n}
	if d, ok := p.(source.Disposer[any]); ok {
		return filteredDisposer[P]{f, d}
	}
	return f
}

func (f filtered[P]) Root() any { return f.p.Root() }

func (f filtered[P]) Expand(ctx context.Context, r any, e source.Emitter[any, P]) error {
	return f.p.Expand(ctx, r, filterEmitter[P]{e, f.keep, f.s, f.n})
}

// Fences and Replay forward the provider's: a fence holds after filtering
// (it is about the rows the filters keep), and a replay range's rows were
// filtered when they were listed, so the filters never see them again.
func (f filtered[P]) Fences() bool {
	fc, ok := f.p.(source.Fencing)
	return ok && fc.Fences()
}

func (f filtered[P]) Replay(r any) (int, bool) {
	if rp, ok := f.p.(source.Replaying[any]); ok {
		return rp.Replay(r)
	}
	return 0, false
}

type filteredDisposer[P any] struct {
	filtered[P]
	d source.Disposer[any]
}

func (f filteredDisposer[P]) Dispose(r any) { f.d.Dispose(r) }

type filterEmitter[P any] struct {
	source.Emitter[any, P]
	keep func(*schema.Row[P]) bool
	s    *schema.Schema[P]
	n    *atomic.Int64
}

// Span and Fence forward to a Fencer emitter.
func (e filterEmitter[P]) Span(lo, hi string, open bool) {
	if f, ok := e.Emitter.(source.Fencer); ok {
		f.Span(lo, hi, open)
	}
}

func (e filterEmitter[P]) Fence(x string, top bool) {
	if f, ok := e.Emitter.(source.Fencer); ok {
		f.Fence(x, top)
	}
}

// Rows keeps the rows the filters accept; a page with none emits nothing.
//
// Downstream memory accounting (schema.RowsBytes) charges only the rows
// handed on, but a compacted page still holds its whole row array, and a
// provider's strings may view one arena for the whole page (S3 does). So
// the survivors are detached (schema.Detach) when they are at most half the
// page's rows or at most half its charged bytes: copied into a slice and a
// string buffer sized to them, and the page becomes garbage. A page handed
// on whole has more than half its rows and more than half its charged bytes
// surviving, so it keeps alive less than about twice what it is charged.
// The copy is of the survivors: under the OR only one of their rows and
// their bytes is bounded by half the page, and the other may be nearly all
// of it (half the rows can hold almost all the bytes, and most rows can
// survive while few bytes do). A page where more than half of both survive
// keeps the zero-copy path.
func (e filterEmitter[P]) Rows(rows []schema.Row[P]) error {
	kept := rows[:0]
	var all, survived int64
	for i := range rows {
		b := e.s.RowBytes(&rows[i])
		all += b
		if e.keep(&rows[i]) {
			kept = append(kept, rows[i])
			survived += b
		}
	}
	if d := len(rows) - len(kept); d > 0 {
		e.n.Add(int64(d))
		clear(rows[len(kept):]) // release the dropped rows' strings
	}
	if len(kept) == 0 {
		return nil
	}
	if 2*len(kept) <= len(rows) || 2*survived <= all {
		kept = e.s.Detach(kept)
	}
	return e.Emitter.Rows(kept)
}
