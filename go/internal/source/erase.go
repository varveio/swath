// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"

	"github.com/varveio/swath/go/internal/schema"
)

// Erase adapts a provider of ranges R to Provider[any], so the pipeline
// is not generic over the provider. Ranges are boxed once each; rows pass
// through untouched. A Disposer is carried over.
func Erase[R any](p Provider[R]) Provider[any] {
	if d, ok := p.(Disposer[R]); ok {
		return erasedDisposer[R]{erased[R]{p}, d}
	}
	return erased[R]{p}
}

type erased[R any] struct{ p Provider[R] }

func (e erased[R]) Root() any { return e.p.Root() }

func (e erased[R]) Expand(ctx context.Context, r any, em Emitter[any]) error {
	return e.p.Expand(ctx, r.(R), typed[R]{em})
}

type erasedDisposer[R any] struct {
	erased[R]
	d Disposer[R]
}

func (e erasedDisposer[R]) Dispose(r any) { e.d.Dispose(r.(R)) }

type typed[R any] struct{ e Emitter[any] }

func (t typed[R]) Rows(rows []schema.Row) error { return t.e.Rows(rows) }
func (t typed[R]) Child(r R) error              { return t.e.Child(r) }

// Empty is a source position with nothing left to list: a resume whose
// final part is already durable.
type Empty struct{}

func (Empty) Root() any                                       { return nil }
func (Empty) Expand(context.Context, any, Emitter[any]) error { return nil }
