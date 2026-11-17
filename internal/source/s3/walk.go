// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"time"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/reader/listing"
)

// walkWorkers is a walk's workers when the caller sets none: the
// command's default for an object store (--workers).
const walkWorkers = 64

// Walk lists w.Prefix for one walk (live.Walker): a provider of its own
// over the source's client, on the range engine with no seed and no
// probes, so that a prefix that fits in one page is one request and only
// a larger one spreads, as idle workers ask for splits; one worker lists
// one continuation chain (Sequential). Each range's keys are checked
// strictly ascending. A range a stop cuts short is reported unread by its
// key interval (keyrange). Its errors are classed as the source's other
// calls' are.
func (l *Live) Walk(ctx context.Context, w live.Walk, sink live.Sink) error {
	workers := w.Workers
	if workers == 0 {
		workers = walkWorkers
	}

	p, err := New(ctx, Options{Bucket: l.bucket, Prefix: string(w.Prefix), Workers: workers, Client: l.client,
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

// entries are a range's rows as the contract's entries: an object each,
// its key copied, the values the row's own, which share the page's
// strings until the walk hands the batch back.
func entries(rows []Row) []listing.Entry {
	n := 0
	for i := range rows {
		n += len(rows[i].Key)
	}
	keys := make([]byte, 0, n)
	out := make([]listing.Entry, len(rows))
	sizes := make([]int64, len(rows))
	times := make([]time.Time, len(rows))
	classes := make([]string, len(rows))

	for i := range rows {
		o := &rows[i].Data
		at := len(keys)
		keys = append(keys, rows[i].Key...)
		sizes[i] = o.Size
		out[i] = listing.Entry{Key: keys[at:len(keys):len(keys)], Kind: listing.KindObject, Size: &sizes[i]}
		if o.Mtime != 0 {
			times[i] = time.UnixMicro(o.Mtime).UTC()
			out[i].LastModified = &times[i]
		}
		if o.StorageClass != "" {
			classes[i] = o.StorageClass
			out[i].StorageClass = &classes[i]
		}
	}
	return out
}
