// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
)

// Count expands every range of p and counts its rows without ordering them:
// a run whose rows nothing consumes (swath du, swath diag count). Workers pop ranges from
// a LIFO stack as Run's do and push a range's children when its Expand
// returns; rows are counted and dropped as they arrive. Nothing is
// buffered: no tree, no budget, no spill (Options.Budget, Shared and the
// spill fields are ignored), so memory is the pages in flight and the
// queued ranges.
//
// What it checks: every range's rows strictly ascending (nondecreasing
// with Options.Groups), and after Options.After; with Options.Unordered,
// nothing about order. What it does not: the stream-wide guard across ranges
// (Run's send), which catches a provider whose ranges overlap. Coverage
// and disjointness are the provider's tiling, as in Run. If counted is set
// it is advanced with every row (the progress file's "ordered").
func Count[R, P any](ctx context.Context, p source.Provider[R, P], counted *atomic.Int64, opt Options) (Stats, error) {
	return CountWithSink(ctx, p, counted, opt, nil)
}

// CountWithSink is Count with an optional observer of the counted batches.
// The sink receives validated rows concurrently from independent ranges;
// it must be safe for concurrent use and must not retain or modify rows.
// A RangeSink receives them through runs instead.
func CountWithSink[R, P any](ctx context.Context, p source.Provider[R, P], counted *atomic.Int64, opt Options, sink Sink[P]) (Stats, error) {
	if opt.Workers < 1 {
		return Stats{}, errors.New("order: workers must be positive")
	}
	if opt.Unordered && (opt.After != nil || opt.Groups) {
		return Stats{}, errors.New("order: an unordered count takes no resume key and no groups")
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	c := &counter[R, P]{ctx: ctx, cancel: cancel, p: p, opt: opt, counted: counted, g: opt.Gauges, sink: sink}
	c.runs, _ = sink.(RangeSink[P])
	if c.trees, _ = sink.(SubtreeSink[P]); c.trees != nil {
		c.subs = []*subtree{{pending: 1}}
	}
	if c.g == nil {
		c.g = &Gauges{}
	}
	c.cond = sync.NewCond(&c.mu)
	c.done = ctx.Done()
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.stopped = true
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	defer stop()
	c.stack = []R{p.Root()}
	c.g.nodes.add(1)
	var wg sync.WaitGroup
	for range opt.Workers {
		wg.Go(c.worker)
	}
	wg.Wait()
	c.dispose()
	n := c.rows.Load()
	st := Stats{Rows: n, Received: n, DistinctKeys: c.names.Load(), PeakNodes: c.g.nodes.peak.Load()}
	if ctx.Err() != nil {
		return st, context.Cause(ctx)
	}
	return st, nil
}

// RangeSink is a count's sink that is told which rows came from one range:
// Count opens a Run for a range's first rows and ends it when the range
// emits a child or its Expand returns. A provider emits a range's rows and
// children in key order (the range engine's rows (lo, c] before the
// children that split (c, hi], a directory's entries with each
// subdirectory after its own row), and in an ordered count those rows
// ascend, so a run is a contiguous stretch of the stream: no row of
// another range sorts between two of its rows. With Options.Unordered
// only the range is known, not the order.
type RangeSink[P any] interface {
	Sink[P]
	// Run opens a run; it is called concurrently, once a run per range
	// segment, and each Run is used by one goroutine.
	Run() RangeRun[P]
}

// RangeRun takes one range's consecutive rows (RangeSink); like Sink.Write it
// must not retain or modify them.
type RangeRun[P any] interface {
	Write(ctx context.Context, rows []schema.Row[P]) error
	// End closes the run: its range emitted a child, or returned. A count
	// that fails does not use what its runs held.
	End()
}

// SubtreeSink is a RangeSink also told when a range that declared the
// prefix it owns (source.Subtreer: a local directory's) has ended together
// with every range it emitted, recursively: each run of their rows has
// ended, so every row under the prefix has reached the sink. A failed
// count tells nothing more.
type SubtreeSink[P any] interface {
	RangeSink[P]
	SubtreeEnd(prefix string)
}

// subtree is a range in the tree a count expands, for a SubtreeSink.
type subtree struct {
	parent  *subtree
	pending int    // its Expand until it returns, and its children's subtrees until they end (counter.mu)
	prefix  string // what the range declared (owns)
	owns    bool
}

type counter[R, P any] struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	p       source.Provider[R, P]
	sink    Sink[P]
	runs    RangeSink[P]   // the sink, if it takes runs
	trees   SubtreeSink[P] // the sink, if it is told of subtrees ending; subs then parallels stack
	subs    []*subtree
	opt     Options
	counted *atomic.Int64
	g       *Gauges
	rows    atomic.Int64
	names   atomic.Int64 // distinct keys, summed over ranges (key-disjoint in group mode too)
	// done is the run context's Done channel, closed by a failure (its
	// cancel) or the caller's stop: emitters poll it without a lock (ctx.Err
	// takes the context's mutex).
	done <-chan struct{}

	mu      sync.Mutex
	cond    *sync.Cond // a range pushed, the last range finished, or a stop
	stack   []R
	busy    int // ranges being expanded
	stopped bool
}

func (c *counter[R, P]) worker() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		for !c.stopped && len(c.stack) == 0 && c.busy > 0 {
			c.cond.Wait()
		}
		if c.stopped || len(c.stack) == 0 { // stopped, or nothing queued and nothing running: done
			c.cond.Broadcast()
			return
		}
		r := c.stack[len(c.stack)-1]
		var zero R // the spare capacity must not keep an expanded range alive (a directory's names)
		c.stack[len(c.stack)-1] = zero
		c.stack = c.stack[:len(c.stack)-1]
		var node *subtree
		if c.trees != nil {
			node = c.subs[len(c.subs)-1]
			c.subs[len(c.subs)-1] = nil
			c.subs = c.subs[:len(c.subs)-1]
		}
		c.busy++
		c.mu.Unlock()
		e := &countEmitter[R, P]{c: c, node: node}
		err := c.p.Expand(c.ctx, r, e)
		e.end()
		c.mu.Lock()
		c.busy--
		c.g.nodes.add(-1)
		if err == nil && c.stopped {
			err = context.Cause(c.ctx)
		}
		if err != nil {
			c.stopped = true
			c.cancel(err)
			c.stack = append(c.stack, e.kids...) // disposed with the rest
			c.cond.Broadcast()
			return
		}
		for i := len(e.kids) - 1; i >= 0; i-- {
			c.stack = append(c.stack, e.kids[i])
		}
		if node != nil {
			for range e.kids {
				c.subs = append(c.subs, &subtree{parent: node, pending: 1})
			}
			node.pending += len(e.kids)
			if ended := c.settle(node); len(ended) > 0 {
				c.mu.Unlock()
				for _, prefix := range ended {
					c.trees.SubtreeEnd(prefix)
				}
				c.mu.Lock()
			}
		}
		c.cond.Broadcast()
	}
}

// settle counts node's Expand done and returns the prefixes of the
// subtrees that ended with it, the deepest first (c.mu held).
func (c *counter[R, P]) settle(node *subtree) []string {
	var ended []string
	for n := node; n != nil; n = n.parent {
		if n.pending--; n.pending > 0 {
			break
		}
		if n.owns {
			ended = append(ended, n.prefix)
		}
	}
	return ended
}

// dispose releases the ranges queued and never expanded (a stopped run);
// called after the workers have returned.
func (c *counter[R, P]) dispose() {
	if d, ok := c.p.(source.Disposer[R]); ok {
		for _, r := range c.stack {
			d.Dispose(r)
		}
	}
	c.g.nodes.add(-int64(len(c.stack)))
	c.stack = nil
}

// countEmitter counts one range's rows and collects its children.
type countEmitter[R, P any] struct {
	c       *counter[R, P]
	kids    []R
	node    *subtree    // the range's place in the tree (counter.trees)
	run     RangeRun[P] // the open run (counter.runs), nil until the range's next rows
	last    string      // the range's last key so far (a view of its page; dropped with the emitter)
	hasLast bool
	sent    bool // last is a row's key (not Options.After): in group mode the next may repeat it
}

func (e *countEmitter[R, P]) Rows(rows []schema.Row[P]) error {
	if e.c.halted() { // before the empty check: a provider emitting only empty batches still stops
		return context.Cause(e.c.ctx)
	}
	if len(rows) == 0 {
		return nil
	}
	if e.c.opt.Unordered {
		return e.add(rows, int64(len(rows)))
	}
	if !e.hasLast && e.c.opt.After != nil {
		e.last, e.hasLast = *e.c.opt.After, true
	}
	var names int64
	for i := range rows {
		k := rows[i].Key
		if e.hasLast && (k < e.last || k == e.last && !(e.c.opt.Groups && e.sent)) {
			return &dataset.InvariantError{Reason: fmt.Sprintf("count: a range's key %q is not after %q", k, e.last)}
		}
		if !e.sent || k != e.last {
			names++
		}
		e.last, e.hasLast, e.sent = k, true, true
	}
	return e.add(rows, names)
}

// add hands a range's checked rows to the sink, or its run, and adds
// them, names of them distinct keys, to the totals.
func (e *countEmitter[R, P]) add(rows []schema.Row[P], names int64) error {
	c := e.c
	switch {
	case c.runs != nil:
		if e.run == nil {
			e.run = c.runs.Run()
		}
		if err := e.run.Write(c.ctx, rows); err != nil {
			return err
		}
	case c.sink != nil:
		if err := c.sink.Write(c.ctx, [][]schema.Row[P]{rows}); err != nil {
			return err
		}
	}
	c.names.Add(names)
	n := int64(len(rows))
	c.rows.Add(n)
	if c.counted != nil {
		c.counted.Add(n)
	}
	return nil
}

// Child takes r (its own from here: queued when the range returns, or
// disposed with the range's other children if it fails) and returns the
// stop, if any, so a provider emitting only children (a wide directory
// with --no-dirs) stops at its next child.
func (e *countEmitter[R, P]) Child(r R) error {
	e.end() // the child's rows sort after the range's so far, before its next
	e.kids = append(e.kids, r)
	e.c.g.nodes.add(1)
	if e.c.halted() {
		return context.Cause(e.c.ctx)
	}
	return nil
}

// Subtree takes the prefix the range owns (source.Subtreer).
func (e *countEmitter[R, P]) Subtree(prefix string) {
	if e.node != nil {
		e.node.prefix, e.node.owns = strings.Clone(prefix), true
	}
}

// end ends the range's open run, if any.
func (e *countEmitter[R, P]) end() {
	if e.run != nil {
		e.run.End()
		e.run = nil
	}
}

// halted reports whether the run has stopped.
func (c *counter[R, P]) halted() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
