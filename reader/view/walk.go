// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"cmp"
	"context"
	"errors"
	"runtime"
	"sync"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader/listing"
)

// listBatch is the entries one listing request of a walk with a depth
// returns.
const listBatch = 1024

// Walk hands emit every entry at or under req.Prefix, the prefix's own
// included when the inventory has it, each once. A walk of every level is
// a range scan; with req.Workers above 1 (zero selects GOMAXPROCS) and
// neither MaxEntries nor MaxDuration, SeekParquet scans shards of it in
// parallel and the
// walk hands their batches to emit one at a time, from the goroutine that
// called Walk, in no order. A budgeted walk scans in key order, so that
// where it stops is one key: it hands emit a last batch holding the
// interval after the last key it handed, of class UnreadBudget, and
// Evidence.Stop names the bound. It stops exactly at MaxEntries.
// MaxValueBytes and MaxHeldBytes do not bound it.
//
// With req.MaxDepth, the walk lists level by level in key order
// (SeekParquet's List): an object store's keys below the depth come back
// as one KindPrefix entry for each prefix at the depth, which the object
// at exactly its key, if any, precedes; a local walk hands only the
// inventory's rows. A local prefix the inventory has as another kind is
// ErrNotDirectory; one it lacks, ErrNotFound when it proves the absence,
// and an empty walk otherwise.
//
// A batch, its entries and their values belong to the walk and are valid
// until emit returns. An error from emit stops the walk and is returned
// unchanged. When ctx ends, Walk returns its error.
func (inv *Inventory) Walk(ctx context.Context, req listing.WalkRequest, emit func(listing.WalkBatch) error) (listing.WalkSummary, error) {
	if err := inv.checkWalk(req, emit); err != nil {
		return listing.WalkSummary{}, err
	}
	if !inv.inScope(req.Prefix) {
		return listing.WalkSummary{}, errOutOfScope
	}

	op, cancel := withBudget(ctx, req.Budget.MaxDuration)
	defer cancel()
	w := walker{inv: inv, emit: emit, max: req.Budget.MaxEntries}
	err := w.run(op, req)
	if err == nil && w.entries == 0 && inv.rows.local && len(req.Prefix) > 0 {
		err = inv.missingDirectory(op, req.Prefix)
	}

	stop := listing.StopCompleted
	switch {
	case w.emitErr != nil:
		return listing.WalkSummary{}, w.emitErr
	case ctx.Err() != nil:
		return listing.WalkSummary{}, ctx.Err()
	case errors.Is(err, errEntryBudget):
		stop = listing.StopEntryBudget
	case err != nil && timeUp(ctx, op):
		stop = listing.StopTimeBudget
	case err != nil:
		return listing.WalkSummary{}, failed(ctx, err)
	}
	if stop != listing.StopCompleted {
		unread := listing.WalkBatch{Unread: []listing.Unread{{Key: w.last, Scope: listing.UnreadInterval, Class: listing.UnreadBudget}}}
		if err := emit(unread); err != nil {
			return listing.WalkSummary{}, err
		}
	}
	return listing.WalkSummary{Entries: w.entries, Evidence: inv.evidence(stop)}, nil
}

func (inv *Inventory) checkWalk(req listing.WalkRequest, emit func(listing.WalkBatch) error) error {
	if err := inv.checkPrefix(req.Prefix); err != nil {
		return err
	}
	switch {
	case emit == nil:
		return invalid("no emit callback")
	case req.MaxDepth < 0 || req.Workers < 0:
		return invalid("a negative depth or number of workers")
	}
	return checkBudget(req.Budget)
}

// errEntryBudget ends a walk that reached its MaxEntries with entries
// left to hand.
var errEntryBudget = errors.New("view: the walk's entry budget is reached")

// walker is one Walk: what it handed emit, and its entry budget.
type walker struct {
	inv  *Inventory
	emit func(listing.WalkBatch) error
	// max is the budget's MaxEntries, zero for none, and counted the
	// entries it counts.
	max     int64
	counted int64
	// entries counts the entries handed to emit, and last is the key of
	// the last one, in key order unless the walk is parallel.
	entries int64
	last    []byte
	// emitErr is the error emit returned.
	emitErr error
}

// run walks req.Prefix as req asks: level by level down to its MaxDepth,
// in parallel shards when it sets workers and no bound, or in key order.
func (w *walker) run(ctx context.Context, req listing.WalkRequest) error {
	workers := req.Workers
	if workers == 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	switch {
	case req.MaxDepth > 0:
		return w.level(ctx, req.Prefix, req.MaxDepth)
	case workers > 1 && req.Budget.MaxEntries == 0 && req.Budget.MaxDuration == 0:
		return w.parallel(ctx, req.Prefix, workers)
	}
	return w.scan(ctx, req.Prefix)
}

// hand gives emit a batch of entries, cut where the entry budget ends:
// errEntryBudget when it cut one.
func (w *walker) hand(entries []listing.Entry) error {
	n, over := len(entries), false
	for i := range entries {
		if w.max == 0 {
			break
		}
		if entries[i].Kind == listing.KindPrefix {
			continue
		}
		if w.counted == w.max {
			n, over = i, true
			break
		}
		w.counted++
	}

	if n > 0 {
		if err := w.emit(listing.WalkBatch{Entries: entries[:n]}); err != nil {
			w.emitErr = err
			return err
		}
		w.entries += int64(n)
		w.last = append(w.last[:0], entries[n-1].Key...)
	}
	if over {
		return errEntryBudget
	}
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

// level hands emit the entries of prefix's level, in key order, and those
// of the levels below it down to depth: each listed row, and for each
// common prefix its own level, or at the depth its exact row and, on an
// object store, a KindPrefix entry.
func (w *walker) level(ctx context.Context, prefix []byte, depth int) error {
	d := decoder{rows: w.inv.rows}
	q := seekparquet.ListQuery{Prefix: prefix, Delimiter: '/', Limit: listBatch, Columns: w.inv.rows.columns}
	var pending []listing.Entry
	for {
		listed, err := w.inv.sp.List(ctx, q, nil)
		if err != nil {
			return err
		}
		pending = pending[:0]
		for _, le := range listed {
			if le.CommonPrefix != nil && depth > 1 {
				if err := w.hand(pending); err != nil {
					return err
				}
				pending = pending[:0]
				if err := w.level(ctx, le.CommonPrefix, depth-1); err != nil {
					return err
				}
				continue
			}
			if row := cmp.Or(le.Row, le.Exact); row != nil {
				e, err := d.entry(ownedRow(row))
				if err != nil {
					return err
				}
				pending = append(pending, e)
			}
			if le.CommonPrefix != nil && !w.inv.rows.local {
				e, err := prefixEntry(le.CommonPrefix, listing.KindPrefix)
				if err != nil {
					return err
				}
				pending = append(pending, *e)
			}
		}
		if err := w.hand(pending); err != nil {
			return err
		}
		if len(listed) < q.Limit {
			return nil
		}
		q.Cursor = listed[len(listed)-1].Cursor()
	}
}
