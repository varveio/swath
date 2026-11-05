// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package order turns a provider's tree of sorted ranges into one ascending
// stream of rows with bounded memory.
//
// Workers expand ranges taken from a LIFO stack; children are pushed in
// reverse order so the range the drain needs next is on top. One drainer
// (the caller of Run) walks the tree depth-first and hands contiguous row
// segments to the sink, consuming the range it is on as rows arrive.
//
// Memory: rows of ranges off the drain path are bounded by Options.Budget.
// Workers wait before claiming a range, and inside Rows for any node other
// than the drainer's current one, while the budget is used up. The current
// node never blocks, and the drainer expands a needed range itself if no
// worker has claimed it, so a full buffer cannot deadlock the walk. Rows on
// the drain path itself are held on top of the budget: at most one range's
// content per level of the current root-to-frontier path (the O(depth ×
// fan-out) of a depth-first walk), plus one publish batch per worker (at most
// publishRows rows plus one Rows call).
package order

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/tools/swath-local-spike/internal/schema"
	"github.com/varveio/swath/tools/swath-local-spike/internal/source"
)

// Sink receives ascending row segments from a single goroutine. The segments
// are owned by the sink once Write is called.
type Sink interface {
	Write(ctx context.Context, segs [][]schema.Row) error
}

// Options configure Run.
type Options struct {
	Workers int // expanding goroutines (>= 1)
	Budget  int // rows read but not yet drained (>= 1)
}

// Stats describe a run.
type Stats struct {
	Rows        int64 // rows handed to the sink
	MaxBuffered int64 // peak rows read but not yet drained
}

// batchRows is the minimum number of rows per sink call (except the last).
const batchRows = 4096

type state uint8

const (
	queued state = iota
	expanding
	done
)

type item[R any] struct {
	rows  []schema.Row
	child *node[R]
}

type node[R any] struct {
	r       R
	state   state
	items   []item[R]
	pending []*node[R]  // children not yet pushed to the stack
	onPath  atomic.Bool // the drainer's current node: its emitter never blocks (written under mu)
}

type run[R any] struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	p        source.Provider[R]
	opt      Options
	mu       sync.Mutex
	work     *sync.Cond // workers: stack empty or budget used
	drain    *sync.Cond // drainer and blocked emitters
	stack    []*node[R]
	buffered int
	max      int
	finished bool
	stopped  atomic.Bool // ctx is done; checked instead of ctx.Err(), which takes a mutex
}

// Run drains p in ascending key order into sink. It returns the first error
// from the provider or the sink, or the context's error on cancellation.
func Run[R any](ctx context.Context, p source.Provider[R], sink Sink, opt Options) (Stats, error) {
	if opt.Workers < 1 || opt.Budget < 1 {
		return Stats{}, errors.New("order: workers and budget must be positive")
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	o := &run[R]{ctx: ctx, cancel: cancel, p: p, opt: opt}
	o.work = sync.NewCond(&o.mu)
	o.drain = sync.NewCond(&o.mu)
	stop := context.AfterFunc(ctx, func() {
		o.stopped.Store(true)
		o.mu.Lock()
		o.work.Broadcast()
		o.drain.Broadcast()
		o.mu.Unlock()
	})
	defer stop()
	root := &node[R]{r: p.Root()}
	o.stack = []*node[R]{root}

	var wg sync.WaitGroup
	for range opt.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.worker()
		}()
	}
	st, err := o.drainTree(root, sink)
	o.mu.Lock()
	o.finished = true
	o.work.Broadcast()
	o.drain.Broadcast()
	o.mu.Unlock()
	if err != nil {
		cancel(err)
	}
	wg.Wait()
	st.MaxBuffered = int64(o.max)
	return st, err
}

func (o *run[R]) worker() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for {
		for !o.finished && !o.stopped.Load() && (len(o.stack) == 0 || o.buffered >= o.opt.Budget) {
			o.work.Wait()
		}
		if o.finished || o.stopped.Load() {
			return
		}
		n := o.stack[len(o.stack)-1]
		o.stack = o.stack[:len(o.stack)-1]
		if n.state != queued { // the drainer took it
			continue
		}
		n.state = expanding
		o.mu.Unlock()
		err := o.expand(n)
		o.mu.Lock()
		if err != nil {
			o.cancel(err)
			return
		}
	}
}

// expand runs the provider on n and publishes its children; called without mu.
func (o *run[R]) expand(n *node[R]) error {
	e := &emitter[R]{o: o, n: n}
	err := o.p.Expand(o.ctx, n.r, e)
	o.mu.Lock()
	defer o.mu.Unlock()
	if err == nil {
		err = e.publish()
	}
	n.state = done
	for i := len(n.pending) - 1; i >= 0; i-- {
		o.stack = append(o.stack, n.pending[i])
	}
	if len(n.pending) > 0 {
		o.work.Broadcast()
	}
	n.pending = nil
	if n.onPath.Load() {
		o.drain.Broadcast()
	}
	return err
}

// emitter collects a range's items locally and publishes them under the
// lock in batches: at once when the drainer is on this node, otherwise
// when publishRows rows have accumulated or the range ends. One directory
// therefore costs one lock round trip, while a long streaming range still
// becomes visible to the drainer as it grows.
type emitter[R any] struct {
	o     *run[R]
	n     *node[R]
	items []item[R]
	kids  []*node[R]
	rows  int
}

const publishRows = 1024

func (e *emitter[R]) Rows(rows []schema.Row) error {
	if len(rows) == 0 {
		return nil
	}
	e.items = append(e.items, item[R]{rows: rows})
	e.rows += len(rows)
	return e.maybePublish()
}

func (e *emitter[R]) Child(r R) error {
	c := &node[R]{r: r}
	e.items = append(e.items, item[R]{child: c})
	e.kids = append(e.kids, c)
	return e.maybePublish()
}

func (e *emitter[R]) maybePublish() error {
	if e.rows < publishRows && !e.n.onPath.Load() {
		return nil
	}
	e.o.mu.Lock()
	defer e.o.mu.Unlock()
	return e.publish()
}

// publish appends the collected items to the node; mu held. Unless the node
// is on the drain path, it first waits while the budget is used up.
func (e *emitter[R]) publish() error {
	o := e.o
	for !e.n.onPath.Load() && o.buffered >= o.opt.Budget && !o.stopped.Load() {
		o.drain.Wait()
	}
	if o.stopped.Load() {
		return context.Cause(o.ctx)
	}
	if len(e.items) == 0 {
		return nil
	}
	e.n.items = append(e.n.items, e.items...)
	e.n.pending = append(e.n.pending, e.kids...)
	o.buffered += e.rows
	o.max = max(o.max, o.buffered)
	e.items, e.kids, e.rows = nil, nil, 0
	if e.n.onPath.Load() {
		o.drain.Broadcast()
	}
	return nil
}

// drainTree walks the tree depth-first from root, streaming rows to sink.
func (o *run[R]) drainTree(root *node[R], sink Sink) (Stats, error) {
	var st Stats
	var segs [][]schema.Row
	segRows, released := 0, 0
	flush := func() error {
		if len(segs) == 0 {
			return nil
		}
		st.Rows += int64(segRows)
		err := sink.Write(o.ctx, segs)
		segs, segRows = nil, 0
		return err
	}
	release := func() { // mu held
		if released == 0 {
			return
		}
		full := o.buffered >= o.opt.Budget
		o.buffered -= released
		released = 0
		if full && o.buffered < o.opt.Budget {
			o.work.Broadcast()
			o.drain.Broadcast()
		}
	}
	releaseEvery := max(1, min(batchRows, o.opt.Budget/16))

	type frame struct {
		n *node[R]
		i int
	}
	frames := []frame{{n: root}}
	o.mu.Lock()
	root.onPath.Store(true)
	for len(frames) > 0 {
		if o.stopped.Load() {
			o.mu.Unlock()
			return st, context.Cause(o.ctx)
		}
		f := &frames[len(frames)-1]
		n := f.n
		if n.state == done && f.i < len(n.items) {
			// A finished node's items are immutable: drain them without the lock.
			items := n.items
			o.mu.Unlock()
			var child *node[R]
			var err error
			for f.i < len(items) && err == nil {
				it := items[f.i]
				items[f.i] = item[R]{}
				f.i++
				if it.child != nil {
					child = it.child
					break
				}
				released += len(it.rows)
				segs = append(segs, it.rows)
				segRows += len(it.rows)
				if segRows >= batchRows {
					err = flush()
				}
				if released >= releaseEvery {
					o.mu.Lock()
					release()
					o.mu.Unlock()
				}
			}
			o.mu.Lock()
			if err != nil {
				o.mu.Unlock()
				return st, err
			}
			if child != nil {
				n.onPath.Store(false)
				child.onPath.Store(true)
				frames = append(frames, frame{n: child})
			}
			continue
		}
		if f.i < len(n.items) {
			it := n.items[f.i]
			n.items[f.i] = item[R]{}
			f.i++
			if it.child != nil {
				n.onPath.Store(false)
				it.child.onPath.Store(true)
				frames = append(frames, frame{n: it.child})
				continue
			}
			released += len(it.rows)
			if released >= releaseEvery {
				release()
			}
			segs = append(segs, it.rows)
			segRows += len(it.rows)
			if segRows >= batchRows {
				o.mu.Unlock()
				err := flush()
				o.mu.Lock()
				if err != nil {
					o.mu.Unlock()
					return st, err
				}
			}
			continue
		}
		switch n.state {
		case done:
			n.items = nil
			n.onPath.Store(false)
			frames = frames[:len(frames)-1]
			if len(frames) > 0 {
				frames[len(frames)-1].n.onPath.Store(true)
				o.drain.Broadcast() // its emitter may be blocked on the budget
			}
		case queued: // nobody has it: expand it here
			n.state = expanding
			release()
			o.mu.Unlock()
			err := o.expand(n)
			o.mu.Lock()
			if err != nil {
				o.mu.Unlock()
				return st, err
			}
		case expanding:
			release()
			o.drain.Broadcast() // its emitter may be blocked from before n joined the path
			o.drain.Wait()
		}
	}
	release()
	o.mu.Unlock()
	return st, flush()
}
