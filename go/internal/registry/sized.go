// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// objectSize reads a row's object size from the schema's size column, nil
// without one. A row that is no object (schema.Schema.Container: a local
// directory) has none: its own size is not an object's, so the run's
// object_bytes is the files' bytes, as swath du's total and the local
// provider's bytes are.
func objectSize[P any](s *schema.Schema[P]) func(*P) (int64, bool) {
	for _, c := range s.Columns {
		if c.Name != "size" {
			continue
		}
		if box := s.Container; box != nil {
			return func(p *P) (int64, bool) {
				if box(p) {
					return 0, false
				}
				return c.Int(p)
			}
		}
		return c.Int
	}
	return nil
}

// sized sums the object sizes of the rows the order stage accepts from a
// provider (after the filters, which it wraps), one atomic add per page.
// On a committed run that is exactly the rows emitted; on a run that
// stopped it also holds rows accepted and never handed on.
type sized[P any] struct {
	p    source.Provider[any, P]
	size func(*P) (int64, bool)
	n    *atomic.Int64
}

func newSized[P any](p source.Provider[any, P], size func(*P) (int64, bool), n *atomic.Int64) source.Provider[any, P] {
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

type sizedDisposer[P any] struct {
	sized[P]
	d source.Disposer[any]
}

func (s sizedDisposer[P]) Dispose(r any) { s.d.Dispose(r) }

type sizeEmitter[P any] struct {
	source.Emitter[any, P]
	size func(*P) (int64, bool)
	n    *atomic.Int64
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

// Rows hands the rows on and adds their sizes once the order stage has
// accepted them: a page it refuses (the run stopping) is not counted. The
// sizes are summed first, because the slice passes to the order stage,
// which may reuse it.
func (e sizeEmitter[P]) Rows(rows []schema.Row[P]) error {
	var sum int64
	for i := range rows {
		if v, ok := e.size(&rows[i].Data); ok {
			sum += v
		}
	}
	if err := e.Emitter.Rows(rows); err != nil {
		return err
	}
	e.n.Add(sum)
	return nil
}
