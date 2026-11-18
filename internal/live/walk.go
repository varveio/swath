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

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// errStopped refuses a batch once the walk has stopped: it is not
// emitted.
var errStopped = errors.New("swath: the walk has stopped")

// Walk hands emit every entry at or under req.Prefix, each once, in no
// order, as package listing states, and every place the walk could not
// read and went on past (an Unread), each in a batch of its own as the
// walk meets it. On an object store the prefix is any key prefix, taken
// literally, and the object at exactly it is included; workers list key
// ranges at once and split them at page boundaries while workers are idle,
// so a prefix that fits in one page is one request; req.Workers 1 lists
// one continuation chain, never split; zero selects the source's default,
// 64 for an object store. Each walk has a range engine of its own;
// concurrent walks share the source's client and request limits only.
//
// emit is called from the goroutine that called Walk, one batch at a time.
// A batch and what it reaches belong to the walk until emit returns, and
// a worker whose batch emit has not taken waits: that is the backpressure,
// and the walk holds no entry and no Unread it has not handed on. Every
// entry is checked before it reaches emit: inside the prefix, no NUL in
// its key, a size on every object, else ErrInvalidPage. An error from emit
// stops the walk, and Walk returns it unchanged once the workers have
// returned; nothing is emitted after Walk returns. The walk sets no limit:
// when ctx ends, Walk returns its error, even when emit has accepted every
// batch. A panic in the walk's workers is returned as an error of no class.
// Whatever it returns, the WalkSummary counts what emit accepted, and its
// Evidence.Stop is StopCompleted only when the walk returns no error.
func (s *Source) Walk(ctx context.Context, req listing.WalkRequest, emit func(listing.WalkBatch) error) (listing.WalkSummary, error) {
	if err := s.checkWalk(req, emit); err != nil {
		return listing.WalkSummary{}, err
	}
	walker, ok := s.store.(Walker)
	if !ok {
		return listing.WalkSummary{}, fmt.Errorf("%w: this source does not walk", listing.ErrUnsupported)
	}

	op, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	w := &walk{op: op, stop: stop, prefix: req.Prefix, emit: emit, queue: make(chan handoff)}
	started := time.Now()
	err := w.run(walker, Walk{Prefix: req.Prefix, Workers: req.Workers})
	finished := time.Now()

	res := listing.WalkSummary{Entries: w.entries, Unread: w.unread, Evidence: evidence(started, finished, "")}
	switch {
	case w.emitErr != nil:
		return res, w.emitErr
	case ctx.Err() != nil:
		return res, ctx.Err()
	case w.invalid != nil:
		return res, objfile.RedactError(w.invalid) // it quotes the store's keys
	case err != nil:
		return res, failure(ctx, err)
	}
	res.Evidence = evidence(started, finished, listing.StopCompleted)
	return res, nil
}

// checkWalk refuses a request of the wrong form, before any request.
func (s *Source) checkWalk(req listing.WalkRequest, emit func(listing.WalkBatch) error) error {
	if err := s.checkName("prefix", req.Prefix); err != nil {
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

// walk is one Walk: the batches its workers hand over (Entries, Unread,
// called concurrently: a Sink), and what the goroutine that called
// Walk has emitted of them.
type walk struct {
	op     context.Context
	stop   context.CancelCauseFunc // ends op, and so the workers, with a cause
	prefix []byte
	emit   func(listing.WalkBatch) error
	queue  chan handoff

	// The emitting goroutine's: the entries and Unread emit accepted, and
	// emit's error.
	entries int64
	unread  int64
	emitErr error

	// mu guards the first entry that failed its check.
	mu      sync.Mutex
	invalid error
}

// handoff is a worker's batch, and where its emitter answers: nil once
// emitted, else errStopped.
type handoff struct {
	batch listing.WalkBatch
	done  chan error
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
	err = w.deliver(h.batch)
}

// deliver hands a batch to emit, and stops the walk at emit's error. Once
// the walk has stopped it emits nothing more: errStopped.
func (w *walk) deliver(b listing.WalkBatch) error {
	if w.op.Err() != nil {
		return errStopped
	}
	if err := w.emit(b); err != nil {
		w.emitErr = err
		w.stop(err)
		return errStopped
	}
	w.entries += int64(len(b.Entries))
	w.unread += int64(len(b.Unread))
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
	return w.hand(ctx, listing.WalkBatch{Entries: entries})
}

// Unread waits for the walk's goroutine to emit a place a worker could
// not read, in a batch of its own (Sink); once the walk has stopped it is
// dropped, as the walk did not finish.
func (w *walk) Unread(u listing.Unread) {
	_ = w.hand(w.op, listing.WalkBatch{Unread: []listing.Unread{u}})
}

// hand passes b to the walk's goroutine and waits until it is emitted:
// nil, else errStopped, or ctx's cause once ctx has ended first.
func (w *walk) hand(ctx context.Context, b listing.WalkBatch) error {
	h := handoff{batch: b, done: make(chan error, 1)}
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
