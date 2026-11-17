// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package swath

import (
	"bytes"
	"context"
	"time"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// depthPage is the most entries a depth walk asks the store for at once:
// S3's most keys in one answer.
const depthPage = 1000

// depthLevel is one level a depth walk reads: the keys one "/" below
// prefix, at depth levels below the walk's prefix.
type depthLevel struct {
	prefix []byte
	depth  int
}

// walkDepth is Walk with req.MaxDepth: delimiter listings by "/", one
// level after another, from the goroutine that called Walk, one request
// at a time, whatever req.Workers says. Each answer is one batch: the
// objects it lists, and, at depth req.MaxDepth, its common prefixes as
// KindPrefix entries, which stand for the keys below the depth; a common
// prefix above it is read as a level of its own, the object at exactly
// its key included. A walk with a depth proves nothing below it.
//
// The bounds stop it between answers. Every level not finished at the
// stop is handed to emit unread, of class UnreadBudget: an UnreadListing
// at the level's prefix when it was begun, whose emitted children are
// read, else an UnreadSubtree. Answers are checked as Page checks them,
// and as Walk checks its entries.
func (s *Source) walkDepth(ctx context.Context, req listing.WalkRequest, emit func(listing.WalkBatch) error) (listing.WalkSummary, error) {
	op, cancel := withBudget(ctx, req.Budget.MaxDuration)
	defer cancel()

	d := &depthWalk{src: s, req: req, emit: emit, queue: []depthLevel{{prefix: req.Prefix, depth: 1}}}
	started := time.Now()
	reason, err := d.run(ctx, op)
	finished := time.Now()
	if err != nil {
		return listing.WalkSummary{}, err
	}

	if err := emitUnread(ctx, emit, d.unread()); err != nil {
		return listing.WalkSummary{}, err // ctx's too, when it ended during the last emit
	}
	return listing.WalkSummary{Entries: d.entries, Evidence: evidence(started, finished, reason)}, nil
}

// depthWalk is one walk with a depth: the levels left to read, the first
// of them begun when begun is set, and the entries emitted, counted those
// that count toward the entry budget.
type depthWalk struct {
	src   *Source
	req   listing.WalkRequest
	emit  func(listing.WalkBatch) error
	queue []depthLevel
	begun bool

	entries int64
	counted int64
}

// run reads the levels until none is left, or a bound stops it: the
// bound, or the walk's error.
func (d *depthWalk) run(ctx, op context.Context) (listing.StopReason, error) {
	for len(d.queue) > 0 {
		lv := d.queue[0]
		var last []byte
		token := ""
		for {
			if reason, err := d.bound(ctx, op); reason != listing.StopCompleted || err != nil {
				return reason, err
			}
			more, reason, err := d.read(ctx, op, lv, &last, &token)
			if reason != listing.StopCompleted || err != nil {
				return reason, err
			}
			if !more {
				break
			}
		}
		d.queue, d.begun = d.queue[1:], false
	}
	return listing.StopCompleted, nil
}

// bound is the bound that stops the walk before its next answer, or ctx's
// error once ctx has ended.
func (d *depthWalk) bound(ctx, op context.Context) (listing.StopReason, error) {
	switch {
	case ctx.Err() != nil:
		return listing.StopCompleted, ctx.Err()
	case timeUp(ctx, op):
		return listing.StopTimeBudget, nil
	case d.req.Budget.MaxEntries > 0 && d.counted >= d.req.Budget.MaxEntries:
		return listing.StopEntryBudget, nil
	}
	return listing.StopCompleted, nil
}

// read reads one answer of lv after *last, from *token, emits it, and
// queues its common prefixes above the depth; more says the level goes
// on. A request the time budget ended is StopTimeBudget; emit's error is
// returned unchanged.
func (d *depthWalk) read(ctx, op context.Context, lv depthLevel, last *[]byte, token *string) (more bool, reason listing.StopReason, err error) {
	reason = listing.StopCompleted
	got, err := d.src.store.List(op, lv.prefix, *token, depthPage)
	switch {
	case err != nil && timeUp(ctx, op):
		return false, listing.StopTimeBudget, nil
	case err != nil:
		return false, reason, failure(ctx, err)
	}
	page, next, err := level(lv.prefix, *last, got, depthPage)
	if err != nil {
		return false, reason, objfile.RedactError(err) // it quotes the store's keys
	}
	if got.Truncated {
		if err := checkContinuation(*token, got.Next); err != nil {
			return false, reason, err
		}
	}
	d.begun = true

	batch := make([]listing.Entry, 0, len(page.Entries)+1)
	if page.Self != nil {
		batch = append(batch, *page.Self)
	}
	for _, e := range page.Entries {
		if e.Kind == listing.KindPrefix && lv.depth < d.req.MaxDepth {
			d.queue = append(d.queue, depthLevel{prefix: bytes.Clone(e.Key), depth: lv.depth + 1})
			continue
		}
		batch = append(batch, e)
	}
	if err := checkEntries(d.req.Prefix, batch); err != nil {
		return false, reason, objfile.RedactError(err)
	}

	if len(batch) > 0 {
		if err := d.emit(listing.WalkBatch{Entries: batch}); err != nil {
			return false, reason, err
		}
	}
	d.entries += int64(len(batch))
	for i := range batch {
		if batch[i].Kind != listing.KindPrefix {
			d.counted++
		}
	}
	*last, *token = next, got.Next
	return got.Truncated, reason, nil
}

// unread is every level the walk did not finish, of class UnreadBudget:
// the one it had begun a listing, the rest subtrees; none once it has
// completed.
func (d *depthWalk) unread() []listing.Unread {
	out := make([]listing.Unread, len(d.queue))
	for i, lv := range d.queue {
		out[i] = listing.Unread{Key: lv.prefix, Scope: listing.UnreadSubtree, Class: listing.UnreadBudget}
		if i == 0 && d.begun {
			out[i].Scope = listing.UnreadListing
		}
	}
	return out
}
