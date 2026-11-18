// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/reader/listing"
)

// walkWorkers is a walk's workers when the caller sets none: the
// command's default for an object store (--workers).
const walkWorkers = 64

var _ live.Walker = (*Live)(nil)

// Walk lists w.Prefix for one walk (live.Walker): a provider of its own
// over the source's client (New), admitted as the command's listing is,
// so a hierarchical namespace, or one that cannot be read, is
// listing.ErrUnsupported before any listing request. The provider's
// adapter serves each range in byte order, so each range's keys are
// checked strictly ascending and apart from every other range's
// (live.Count). One worker lists one continuation chain (Sequential). Its
// errors are classed as the source's other calls' are.
func (l *Live) Walk(ctx context.Context, w live.Walk, sink live.Sink) error {
	workers := w.Workers
	if workers == 0 {
		workers = walkWorkers
	}

	p, err := New(ctx, Options{Account: l.account, Container: l.container, Prefix: string(w.Prefix), Workers: workers, Client: l.client,
		Policy: keyrange.Policy{Sequential: workers == 1}})
	if err != nil {
		return failure(ctx, err)
	}
	b, err := bind.Provider[Range, Object](p, &Schema)
	if err != nil {
		return err
	}

	if err := live.Count(ctx, b, order.Options{Workers: workers}, entries, sink); err != nil {
		return failure(ctx, err)
	}
	return nil
}

// entries are a range's rows as the contract's entries: a blob each, its
// key copied.
func entries(rows []Row) []listing.Entry {
	out := make([]listing.Entry, len(rows))
	for i := range rows {
		o := &rows[i].Data
		out[i] = object([]byte(rows[i].Key), o.Size, o.Mtime, o.AccessTier)
	}
	return out
}
