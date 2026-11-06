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

// Rows keeps the rows the filters accept; a page with none emits nothing.
//
// Downstream memory accounting (schema.RowsBytes) charges only the rows
// handed on, but a compacted page still holds its whole row array, and a
// provider's strings may view one arena for the whole page (S3 does). So
// unless the survivors hold more than half of the page, both by rows and by
// charged bytes, they are detached (schema.Detach): copied into a slice and
// a string buffer sized to them, and the page becomes garbage. The rule
// bounds what a page handed on keeps alive to about twice what it is
// charged; the copy costs at most half a page, less than parsing it did.
// A page where most rows survive keeps the zero-copy path.
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
