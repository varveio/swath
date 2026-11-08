// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// Count expands every range of p and counts its rows without ordering them:
// a run whose rows nothing consumes (swath count). Workers pop ranges from
// a LIFO stack as Run's do and push a range's children when its Expand
// returns; rows are counted and dropped as they arrive. Nothing is
// buffered: no tree, no budget, no spill (Options.Budget, Shared and the
// spill fields are ignored), so memory is the pages in flight and the
// queued ranges.
//
// What it checks: every range's rows strictly ascending (and after
// Options.After). What it does not: the stream-wide guard across ranges
// (Run's send), which catches a provider whose ranges overlap. Coverage
// and disjointness are the provider's tiling, as in Run. If counted is set
// it is advanced with every row (the progress file's "ordered").
func Count[R, P any](ctx context.Context, p source.Provider[R, P], counted *atomic.Int64, opt Options) (Stats, error) {
	if opt.Workers < 1 {
		return Stats{}, errors.New("order: workers must be positive")
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	c := &counter[R, P]{ctx: ctx, cancel: cancel, p: p, opt: opt, counted: counted, g: opt.Gauges}
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
	st := Stats{Rows: n, Received: n, PeakNodes: c.g.nodes.peak.Load()}
	if ctx.Err() != nil {
		return st, context.Cause(ctx)
	}
	return st, nil
}

type counter[R, P any] struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	p       source.Provider[R, P]
	opt     Options
	counted *atomic.Int64
	g       *Gauges
	rows    atomic.Int64
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
		c.stack = c.stack[:len(c.stack)-1]
		c.busy++
		c.mu.Unlock()
		e := &countEmitter[R, P]{c: c}
		err := c.p.Expand(c.ctx, r, e)
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
		c.cond.Broadcast()
	}
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
	last    string // the range's last key so far (a view of its page; dropped with the emitter)
	hasLast bool
}

func (e *countEmitter[R, P]) Rows(rows []schema.Row[P]) error {
	if e.c.halted() { // before the empty check: a provider emitting only empty batches still stops
		return context.Cause(e.c.ctx)
	}
	if len(rows) == 0 {
		return nil
	}
	if !e.hasLast && e.c.opt.After != nil {
		e.last, e.hasLast = *e.c.opt.After, true
	}
	for i := range rows {
		k := rows[i].Key
		if e.hasLast && k <= e.last {
			return &dataset.InvariantError{Reason: fmt.Sprintf("count: a range's key %q is not after %q", k, e.last)}
		}
		e.last, e.hasLast = k, true
	}
	n := int64(len(rows))
	e.c.rows.Add(n)
	if e.c.counted != nil {
		e.c.counted.Add(n)
	}
	return nil
}

// Child takes r (its own from here: queued when the range returns, or
// disposed with the range's other children if it fails) and returns the
// stop, if any, so a provider emitting only children (a wide directory
// with --no-dirs) stops at its next child.
func (e *countEmitter[R, P]) Child(r R) error {
	e.kids = append(e.kids, r)
	e.c.g.nodes.add(1)
	if e.c.halted() {
		return context.Cause(e.c.ctx)
	}
	return nil
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
