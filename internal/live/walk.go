// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"context"
	"errors"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/reader/listing"
)

// Walker is a Store that can also walk: list everything under a prefix,
// flat and in parallel, through its provider's range engine.
type Walker interface {
	// Walk lists every entry under w.Prefix into sink, in no order, each
	// once: a provider of its own for this call, over the store's
	// client, credentials and limiter, bound to its row schema and counted
	// (Count). It returns when every worker has, with an error that
	// matches a class of package listing, or with ctx's own once ctx has
	// ended. What the walk did not read reaches sink.Unread.
	Walk(ctx context.Context, w Walk, sink Sink) error
}

// Walk is what one walk lists.
type Walk struct {
	// Prefix is the scope: the keys that begin with it.
	Prefix []byte
	// Workers is how many ranges are listed at once: zero for the store's
	// default, one for its sequential mode (one request chain, never
	// split).
	Workers int
}

// Sink takes a walk's entries and the extents it did not read. Its
// methods are called concurrently, from the walk's workers.
type Sink interface {
	// Entries takes a batch of entries of one range, which belong to the
	// walk until Entries returns. An error stops the walk: it is the
	// count's, and the batch counts as not emitted, so its range reports
	// it unread.
	Entries(ctx context.Context, entries []listing.Entry) error
	// Unread takes an extent the walk did not read, and keeps its keys.
	Unread(u listing.Unread)
}

// Count lists b's scope without ordering it across ranges
// (bind.Prepared.Count, order.CountWithSink) into sink, with opt as the
// order stage's options: each range's rows checked as opt says, made
// entries by entries, and handed to sink.Entries; each extent the
// provider reports through its emitter, and that of each range the count
// drops at a stop, handed to sink.Unread. A broken order is
// source.ErrProtocol, which objhttp.ListingClass reads as
// listing.ErrInvalidPage.
func Count[P any](ctx context.Context, b bind.Bound[P], opt order.Options, entries func([]schema.Row[P]) []listing.Entry, sink Sink) error {
	l, err := b.Prepare(bind.Env{Order: opt})
	if err != nil {
		return err
	}

	_, err = l.Count(ctx, rows[P]{sink: sink, entries: entries})
	var broken *dataset.InvariantError
	if errors.As(err, &broken) {
		return source.Violation(err)
	}
	return err
}

// rows is a count's sink (order.Sink, order.UnreadSink) that hands a
// walk's rows to its Sink as entries.
type rows[P any] struct {
	sink    Sink
	entries func([]schema.Row[P]) []listing.Entry
}

func (r rows[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	for _, seg := range segs {
		if len(seg) == 0 {
			continue
		}
		if err := r.sink.Entries(ctx, r.entries(seg)); err != nil {
			return err
		}
	}
	return nil
}

func (r rows[P]) Unread(u listing.Unread) { r.sink.Unread(u) }
