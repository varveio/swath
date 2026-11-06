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
// counted (the run reconciles listed = kept + dropped).
type filtered[P any] struct {
	p    source.Provider[any, P]
	keep func(*schema.Row[P]) bool
	n    *atomic.Int64
}

func newFiltered[P any](p source.Provider[any, P], keep func(*schema.Row[P]) bool, n *atomic.Int64) source.Provider[any, P] {
	f := filtered[P]{p, keep, n}
	if d, ok := p.(source.Disposer[any]); ok {
		return filteredDisposer[P]{f, d}
	}
	return f
}

func (f filtered[P]) Root() any { return f.p.Root() }

func (f filtered[P]) Expand(ctx context.Context, r any, e source.Emitter[any, P]) error {
	return f.p.Expand(ctx, r, filterEmitter[P]{e, f.keep, f.n})
}

type filteredDisposer[P any] struct {
	filtered[P]
	d source.Disposer[any]
}

func (f filteredDisposer[P]) Dispose(r any) { f.d.Dispose(r) }

type filterEmitter[P any] struct {
	source.Emitter[any, P]
	keep func(*schema.Row[P]) bool
	n    *atomic.Int64
}

// Rows keeps the rows the filters accept; a page with none emits nothing.
func (e filterEmitter[P]) Rows(rows []schema.Row[P]) error {
	kept := rows[:0]
	for i := range rows {
		if e.keep(&rows[i]) {
			kept = append(kept, rows[i])
		}
	}
	if d := len(rows) - len(kept); d > 0 {
		e.n.Add(int64(d))
		clear(rows[len(kept):]) // release the dropped rows' strings
	}
	if len(kept) == 0 {
		return nil
	}
	return e.Emitter.Rows(kept)
}
