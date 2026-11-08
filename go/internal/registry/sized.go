// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// sizeColumn is the schema's size column reader, nil without one.
func sizeColumn[P any](s *schema.Schema[P]) func(*P) (int64, bool) {
	for _, c := range s.Columns {
		if c.Name == "size" {
			return c.Int
		}
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
