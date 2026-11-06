// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package source defines the contract between listing providers and the
// ordering stage. A provider describes its namespace as a tree of ranges R;
// each range yields rows (a key and the provider's payload P) and child
// ranges in ascending key order.
package source

import (
	"context"
	"errors"

	"github.com/varveio/swath/go/internal/schema"
)

// ErrProtocol marks a service answer that breaks the listing contract in a
// way the engine cannot recover from (keys out of order or repeated, a
// repeated continuation token, a page outside its range): the run fails,
// and its journal marks it unresumable, since a resume would walk back into
// the same service.
var ErrProtocol = errors.New("protocol violation")

// ErrUnsupported marks a source the tool refuses before any request (an
// S3 directory bucket): the caller's to change.
var ErrUnsupported = errors.New("unsupported source")

// Violation marks err as a protocol violation (errors.Is ErrProtocol),
// keeping its message and its own chain.
func Violation(err error) error { return marked{err, ErrProtocol} }

// Unsupported marks err as an unsupported source (errors.Is
// ErrUnsupported), keeping its message.
func Unsupported(err error) error { return marked{err, ErrUnsupported} }

// marked is an error that is also its mark.
type marked struct {
	error
	mark error
}

func (m marked) Unwrap() error        { return m.error }
func (m marked) Is(target error) bool { return target == m.mark }

// Emitter receives the content of one range in ascending key order.
type Emitter[R, P any] interface {
	// Rows appends rows that sort after everything emitted so far in this
	// range. Ownership of the slice passes to the emitter; the provider must
	// not modify it afterwards. A non-nil error means stop.
	Rows(rows []schema.Row[P]) error
	// Child appends a range whose entire subtree sorts at this position.
	Child(r R) error
}

// Provider produces a tree of ranges. Expand is called concurrently for
// different ranges. It returns an error only when the listing must stop;
// per-entry problems are the provider's to count.
type Provider[R, P any] interface {
	Root() R
	Expand(ctx context.Context, r R, e Emitter[R, P]) error
}

// Disposer is implemented by providers whose ranges hold resources (open
// directory descriptors): the order stage calls Dispose for every range it
// will never expand, when a run is cancelled or fails.
type Disposer[R any] interface {
	Dispose(r R)
}
