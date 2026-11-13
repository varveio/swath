// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"

	"github.com/varveio/swath/internal/schema"
)

// Erase adapts a provider of ranges R to Provider[any, P], so the order
// stage is not generic over the range type. Ranges are boxed once each;
// rows pass through untouched. A Disposer is carried over.
func Erase[R, P any](p Provider[R, P]) Provider[any, P] {
	if d, ok := p.(Disposer[R]); ok {
		return erasedDisposer[R, P]{erased[R, P]{p}, d}
	}
	return erased[R, P]{p}
}

type erased[R, P any] struct{ p Provider[R, P] }

func (e erased[R, P]) Root() any { return e.p.Root() }

func (e erased[R, P]) Expand(ctx context.Context, r any, em Emitter[any, P]) error {
	return e.p.Expand(ctx, r.(R), typed[R, P]{em})
}

type erasedDisposer[R, P any] struct {
	erased[R, P]
	d Disposer[R]
}

func (e erasedDisposer[R, P]) Dispose(r any) { e.d.Dispose(r.(R)) }

// Fences forwards the provider's Fencing (false without it).
func (e erased[R, P]) Fences() bool {
	f, ok := e.p.(Fencing)
	return ok && f.Fences()
}

// Replay forwards the provider's Replaying (none without it).
func (e erased[R, P]) Replay(r any) (int, bool) {
	rp, ok := e.p.(Replaying[R])
	rr, isR := r.(R)
	if !ok || !isR {
		return 0, false
	}
	return rp.Replay(rr)
}

type typed[R, P any] struct{ e Emitter[any, P] }

func (t typed[R, P]) Rows(rows []schema.Row[P]) error { return t.e.Rows(rows) }
func (t typed[R, P]) Child(r R) error                 { return t.e.Child(r) }

// Span and Fence forward to a Fencer emitter.
func (t typed[R, P]) Span(lo, hi string, open bool) {
	if f, ok := t.e.(Fencer); ok {
		f.Span(lo, hi, open)
	}
}

func (t typed[R, P]) Fence(x string, top bool) {
	if f, ok := t.e.(Fencer); ok {
		f.Fence(x, top)
	}
}

// Empty is a source position with nothing left to list: a resume whose
// final part is already durable.
type Empty[P any] struct{}

func (Empty[P]) Root() any                                          { return nil }
func (Empty[P]) Expand(context.Context, any, Emitter[any, P]) error { return nil }
