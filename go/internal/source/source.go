// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package source defines the contract between listing providers and the
// ordering stage. A provider describes its namespace as a tree of ranges;
// each range yields rows and child ranges in ascending key order.
package source

import (
	"context"

	"github.com/varveio/swath/go/internal/schema"
)

// Emitter receives the content of one range in ascending key order.
type Emitter[R any] interface {
	// Rows appends rows that sort after everything emitted so far in this
	// range. Ownership of the slice passes to the emitter; the provider must
	// not modify it afterwards. A non-nil error means stop.
	Rows(rows []schema.Row) error
	// Child appends a range whose entire subtree sorts at this position.
	Child(r R) error
}

// Provider produces a tree of ranges. Expand is called concurrently for
// different ranges. It returns an error only when the listing must stop;
// per-entry problems are the provider's to count.
type Provider[R any] interface {
	Root() R
	Expand(ctx context.Context, r R, e Emitter[R]) error
}

// Disposer is implemented by providers whose ranges hold resources (open
// directory descriptors): the order stage calls Dispose for every range it
// will never expand, when a run is cancelled or fails.
type Disposer[R any] interface {
	Dispose(r R)
}
