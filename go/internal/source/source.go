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

// ErrProtocol marks a violation of the listing contract that the engine
// cannot recover from (keys out of order or repeated, a
// repeated continuation token, a page outside its range): the run fails,
// and its journal marks it unresumable, since a resume would walk back into
// the same service.
var ErrProtocol = errors.New("protocol violation")

// ErrUnsupported marks a source the tool refuses before listing (an S3
// directory bucket or an Azure namespace that is not positively flat).
// Admission may make requests to establish whether the source is supported.
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
	// Ownership of r passes to the emitter even when Child returns an
	// error: the order stage disposes it (Disposer), the provider must not.
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

// Fencer is an Emitter that accepts coverage fences, the declarations a
// durable spill certifies coverage from (go/docs/internals/ordering.md,
// "Durable spill"). An emitter that is not one ignores them; an adapter
// between a provider and the order stage forwards both calls.
type Fencer interface {
	// Span declares the range's logical interval (lo, hi] (open: up to ⊤),
	// before its first row: lo is max(request bound, resume key), never a
	// request bound below the resume key. It is also the fence at lo.
	Span(lo, hi string, open bool)
	// Fence declares that every row of the range's logical interval up to
	// x (top: up to ⊤) that the provider will ever emit has been emitted.
	// x is a bound the provider accepts or the run's resume key, never a
	// key below that key.
	Fence(x string, top bool)
}

// Fencing is a provider whose ranges declare fences through a Fencer
// emitter: durable spill engages only for one (and only when every adapter
// between it and the order stage forwards them).
type Fencing interface {
	Fences() bool
}

// Replaying is a provider whose tree may hold replay ranges: positions the
// order stage fills from a reused coverage certificate (its index) instead
// of expanding the range.
type Replaying[R any] interface {
	Replay(r R) (cert int, ok bool)
}

// Span is a coverage certificate's interval (Lo, Hi] (HiOpen: up to ⊤), as
// a resume hands it to the provider to list around.
type Span struct {
	Lo, Hi string
	HiOpen bool
}

// Reusing is a resumable source that lists around reused certificates:
// Reuse is called before Root, with the certificates' spans in ascending,
// disjoint order; the provider emits a replay range (Replaying) at each one
// and lists only the gaps between them.
type Reusing interface {
	Reuse(spans []Span)
}
