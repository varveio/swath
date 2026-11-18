// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"runtime"
	"sync"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader/listing"
)

// Walk hands emit every entry at or under req.Prefix, as package listing
// states. With req.Workers above 1 (zero selects GOMAXPROCS) SeekParquet
// scans shards of the range in parallel, and the walk hands their batches
// to emit one at a time, from the goroutine that called Walk.
//
// A local prefix the inventory has as another kind, or below a symbolic
// link, is ErrNotDirectory; one it lacks, ErrNotFound when it proves the
// absence, and an empty walk otherwise.
func (inv *Inventory) Walk(ctx context.Context, req listing.WalkRequest, emit func(listing.WalkBatch) error) (listing.WalkSummary, error) {
	if err := inv.checkWalk(req, emit); err != nil {
		return listing.WalkSummary{}, err
	}
	if !inv.inScope(req.Prefix) {
		return listing.WalkSummary{}, errOutOfScope
	}

	w := walker{inv: inv, emit: emit}
	err := w.run(ctx, req)
	if err == nil && w.entries == 0 && inv.rows.local && len(req.Prefix) > 0 {
		err = inv.missingDirectory(ctx, req.Prefix)
	}

	res := listing.WalkSummary{Entries: w.entries, Evidence: inv.evidence()}
	switch {
	case w.emitErr != nil:
		return res, w.emitErr
	case ctx.Err() != nil:
		return res, ctx.Err()
	case err != nil:
		return res, failed(ctx, err)
	}
	res.Complete = inv.provable
	return res, nil
}

func (inv *Inventory) checkWalk(req listing.WalkRequest, emit func(listing.WalkBatch) error) error {
	if err := inv.checkPrefix(req.Prefix); err != nil {
		return err
	}
	switch {
	case emit == nil:
		return invalid("no emit callback")
	case req.Workers < 0:
		return invalid("a negative number of workers")
	}
	return nil
}

// walker is one Walk: what it handed emit.
type walker struct {
	inv  *Inventory
	emit func(listing.WalkBatch) error
	// entries counts the entries handed to emit.
	entries int64
	// emitErr is the error emit returned.
	emitErr error
}

// run walks req.Prefix as req asks: in parallel shards when it sets
// workers, else in key order.
func (w *walker) run(ctx context.Context, req listing.WalkRequest) error {
	workers := req.Workers
	if workers == 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers > 1 {
		return w.parallel(ctx, req.Prefix, workers)
	}
	return w.scan(ctx, req.Prefix)
}

// hand gives emit a batch of entries.
func (w *walker) hand(entries []listing.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := w.emit(listing.WalkBatch{Entries: entries}); err != nil {
		w.emitErr = err
		return err
	}
	w.entries += int64(len(entries))
	return nil
}

// scan hands emit the rows under prefix, in key order.
func (w *walker) scan(ctx context.Context, prefix []byte) error {
	d := decoder{rows: w.inv.rows, slab: &slab{}}
	var batch []listing.Entry
	q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan(prefix)}, Columns: w.inv.rows.columns}
	return w.inv.sp.Scan(ctx, q, nil, func(b *seekparquet.Batch) error {
		var err error
		if batch, err = d.batchEntries(b, batch); err != nil {
			return err
		}
		return w.hand(batch)
	})
}

// parallel hands emit the rows under prefix as workers scan them: each
// worker decodes its batch and waits while the walk's goroutine hands it
// to emit, so that the batch stays valid until emit returns.
func (w *walker) parallel(ctx context.Context, prefix []byte, workers int) error {
	plan, err := w.inv.sp.Plan(ctx, []seekparquet.Selection{{Span: seekparquet.PrefixSpan(prefix)}}, w.inv.rows.columns, nil)
	if err != nil {
		return err
	}

	type handoff struct {
		entries []listing.Entry
		done    chan error
	}
	type buffer struct {
		d       decoder
		entries []listing.Entry
		done    chan error
	}
	buffers := sync.Pool{New: func() any {
		return &buffer{d: decoder{rows: w.inv.rows, slab: &slab{}}, done: make(chan error, 1)}
	}}
	queue := make(chan handoff)
	scanned := make(chan error, 1)
	go func() {
		defer close(queue)
		scanned <- plan.ScanParallel(ctx, workers, nil, func(b *seekparquet.Batch) error {
			buf := buffers.Get().(*buffer)
			defer buffers.Put(buf)
			var err error
			if buf.entries, err = buf.d.batchEntries(b, buf.entries); err != nil {
				return err
			}
			queue <- handoff{entries: buf.entries, done: buf.done}
			return <-buf.done
		})
	}()

	var failed error
	for h := range queue {
		if failed == nil {
			failed = w.hand(h.entries)
		}
		h.done <- failed
	}
	return <-scanned
}
