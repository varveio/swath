// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

const (
	// maxPendingUnread bounds the extents not read that a walk holds
	// before it hands them to emit; past it an extent is dropped, and the
	// walk's Evidence.Unlocated is nil.
	maxPendingUnread = 1 << 16
	// unreadBatch is the most extents a batch holds once the workers have
	// returned.
	unreadBatch = 1024
)

var (
	// errEntryBudget is the cause of a walk stopped at its MaxEntries.
	errEntryBudget = errors.New("swath: the walk's entry budget is reached")
	// errStopped refuses a batch once the walk has stopped: it is not
	// emitted, and its range reports it unread.
	errStopped = errors.New("swath: the walk has stopped")
)

// Walk hands emit every entry at or under req.Prefix, the object at
// exactly the prefix included, each once, in no order, as package listing
// states. The prefix is any key prefix, taken literally. Workers list key
// ranges at once and split them at page boundaries while workers are idle,
// so a prefix that fits in one page is one request; req.Workers 1 lists
// one continuation chain, never split; zero selects the source's default,
// 64 for an object store. Each walk has a range engine of its own; concurrent walks
// share the source's client and request limits only.
//
// emit is called from the goroutine that called Walk, one batch at a time.
// A batch and what it reaches belong to the walk until emit returns, and
// a worker whose batch emit has not taken waits: that is the backpressure,
// and it counts toward the time budget. Every entry is checked before it
// reaches emit: inside the prefix, no NUL in its key, a size on every
// object, else ErrInvalidPage. An error from emit stops the walk, and
// Walk returns it unchanged once the workers have returned; nothing is
// emitted after Walk returns.
//
// req.Budget.MaxEntries stops the walk after the batch that reaches it,
// MaxDuration once that time has passed: Evidence.Stop names the bound,
// with no error, and every key interval left unread is handed to emit as
// an UnreadInterval of class UnreadBudget, before Walk returns. A walk
// that meets more unread extents than it holds at once (65,536) drops the
// rest and says so with a nil Evidence.Unlocated. When ctx ends,
// Walk returns its error, even when emit has accepted every batch. A panic
// in the walk's workers is returned as an error of no class.
func (s *Source) Walk(ctx context.Context, req listing.WalkRequest, emit func(listing.WalkBatch) error) (listing.WalkSummary, error) {
	if err := checkWalk(req, emit); err != nil {
		return listing.WalkSummary{}, err
	}
	walker, ok := s.store.(Walker)
	if !ok {
		return listing.WalkSummary{}, fmt.Errorf("%w: this source does not walk", listing.ErrUnsupported)
	}

	timed, cancel := withBudget(ctx, req.Budget.MaxDuration)
	defer cancel()
	op, stop := context.WithCancelCause(timed)
	defer stop(nil)
	w := &walk{op: op, stop: stop, prefix: req.Prefix, emit: emit, max: req.Budget.MaxEntries, queue: make(chan handoff)}
	started := time.Now()
	err := w.run(walker, Walk{Prefix: req.Prefix, Workers: req.Workers})
	finished := time.Now()

	switch {
	case w.emitErr != nil:
		return listing.WalkSummary{}, w.emitErr
	case ctx.Err() != nil:
		return listing.WalkSummary{}, ctx.Err()
	case w.invalid != nil:
		return listing.WalkSummary{}, objfile.RedactError(w.invalid) // it quotes the store's keys
	}
	reason := listing.StopCompleted
	switch {
	case err == nil:
	case errors.Is(context.Cause(op), errEntryBudget):
		reason = listing.StopEntryBudget
	case timeUp(ctx, op):
		reason = listing.StopTimeBudget
	default:
		return listing.WalkSummary{}, failure(ctx, err)
	}

	if err := emitUnread(ctx, emit, w.takeUnread()); err != nil {
		return listing.WalkSummary{}, err
	}
	ev := evidence(started, finished, reason)
	if w.lost {
		ev.Unlocated = nil
	}
	return listing.WalkSummary{Entries: w.entries, Evidence: ev}, nil
}

// checkWalk refuses a request of the wrong form, before any request.
func checkWalk(req listing.WalkRequest, emit func(listing.WalkBatch) error) error {
	b := req.Budget
	switch {
	case emit == nil:
		return invalid("no emit callback")
	case bytes.IndexByte(req.Prefix, 0) >= 0:
		return invalid("the prefix holds a NUL byte")
	case !utf8.Valid(req.Prefix):
		return invalid("the prefix is not UTF-8")
	case req.Workers < 0:
		return invalid("a negative number of workers")
	case b.MaxEntries < 0 || b.MaxDuration < 0:
		return invalid("a negative bound")
	}
	return nil
}

// walk is one Walk: the batches its workers hand over (Entries, Unread,
// called concurrently: a Sink), and what the goroutine that called
// Walk has emitted of them.
type walk struct {
	op     context.Context
	stop   context.CancelCauseFunc // ends op, and so the workers, with a cause
	prefix []byte
	emit   func(listing.WalkBatch) error
	queue  chan handoff

	// The emitting goroutine's: the entries handed to emit, which max
	// bounds (MaxEntries, zero for none), emit's error, and whether the
	// walk emits no more entries.
	max     int64
	entries int64
	emitErr error
	halted  bool

	// mu guards the extents not read that wait for a batch, whether one
	// was dropped (lost), and the first entry that failed its check.
	mu      sync.Mutex
	pending []listing.Unread
	lost    bool
	invalid error
}

// handoff is a worker's batch, and where its emitter answers: nil once
// emitted, else errStopped.
type handoff struct {
	entries []listing.Entry
	done    chan error
}

// run lists through walker on a goroutine of its own and emits the
// batches its workers hand over from this one, until walker returns: its
// error. A panic of emit's ends the walk, waits for its workers, and goes
// on.
func (w *walk) run(walker Walker, lw Walk) error {
	listed := make(chan error, 1)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				listed <- fmt.Errorf("swath: the walk panicked: %v\n%s", v, debug.Stack())
			}
		}()
		listed <- walker.Walk(w.op, lw, w)
	}()

	ended := false
	defer func() {
		if ended {
			return
		}
		w.stop(errStopped)
		for {
			select {
			case h := <-w.queue:
				h.done <- errStopped
			case <-listed:
				return
			}
		}
	}()
	for {
		select {
		case h := <-w.queue:
			w.serve(h)
		case err := <-listed:
			ended = true
			return err
		}
	}
}

// serve answers one handoff, refused unless emitted, even when emit
// panics.
func (w *walk) serve(h handoff) {
	err := errStopped
	defer func() { h.done <- err }()
	err = w.deliver(h.entries)
}

// deliver hands entries to emit, with the extents not read waiting so
// far, and stops the walk at its entry budget or at emit's error. Once the
// walk has stopped it emits nothing more: errStopped.
func (w *walk) deliver(entries []listing.Entry) error {
	if w.halted || w.op.Err() != nil {
		return errStopped
	}
	if err := w.emit(listing.WalkBatch{Entries: entries, Unread: w.takeUnread()}); err != nil {
		w.emitErr, w.halted = err, true
		w.stop(err)
		return errStopped
	}

	w.entries += int64(len(entries))
	if w.max > 0 && w.entries >= w.max {
		w.halted = true
		w.stop(errEntryBudget)
	}
	return nil
}

// Entries checks a worker's batch and waits for the walk's goroutine to
// emit it (Sink): nil once emitted; an error otherwise, which stops
// the batch's range.
func (w *walk) Entries(ctx context.Context, entries []listing.Entry) error {
	if err := w.check(entries); err != nil {
		w.mu.Lock()
		if w.invalid == nil {
			w.invalid = err
		}
		w.mu.Unlock()
		w.stop(err)
		return err
	}

	h := handoff{entries: entries, done: make(chan error, 1)}
	select {
	case w.queue <- h:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	return <-h.done
}

// check refuses an entry the walk must not emit (checkEntries).
func (w *walk) check(entries []listing.Entry) error {
	return checkEntries(w.prefix, entries)
}

// checkEntries refuses an entry a walk of prefix must not emit: outside
// the prefix, a key holding a NUL, or an object or file without a size
// (ErrInvalidPage).
func checkEntries(prefix []byte, entries []listing.Entry) error {
	for i := range entries {
		e := &entries[i]
		switch {
		case !bytes.HasPrefix(e.Key, prefix):
			return badPage("a key outside the prefix: %q", e.Key)
		case bytes.IndexByte(e.Key, 0) >= 0:
			return badPage("a key holding a NUL byte: %q", e.Key)
		case e.Size == nil && (e.Kind == listing.KindObject || e.Kind == listing.KindFile):
			return badPage("an entry of kind %v without a size: %q", e.Kind, e.Key)
		}
	}
	return nil
}

// Unread keeps an extent not read for the next batch (Sink), or
// drops it once maxPendingUnread wait.
func (w *walk) Unread(u listing.Unread) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == maxPendingUnread {
		w.lost = true
		return
	}
	w.pending = append(w.pending, u)
}

// takeUnread takes the extents waiting.
func (w *walk) takeUnread() []listing.Unread {
	w.mu.Lock()
	defer w.mu.Unlock()
	u := w.pending
	w.pending = nil
	return u
}

// emitUnread hands emit the extents still waiting once a walk has stopped
// reading, unreadBatch at a time: emit's error, else ctx's once it has
// ended, whether before a batch or while emit took it.
func emitUnread(ctx context.Context, emit func(listing.WalkBatch) error, pending []listing.Unread) error {
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(len(pending), unreadBatch)
		if err := emit(listing.WalkBatch{Unread: pending[:n:n]}); err != nil {
			return err
		}
		pending = pending[n:]
	}
	return ctx.Err()
}
