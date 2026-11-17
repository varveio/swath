// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package bind

import (
	"context"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
)

// observed is a count's sink that hands each batch to see, then to next
// (nil: none), keeping next's place in the count: a next that takes runs
// (order.RangeSink) is handed them, each run told its interval when next's
// run is an order.CoveredRun, and a next told of subtrees ending
// (order.SubtreeSink) is told. An observer between the count and a
// caller's sink would otherwise hide those callbacks, and the sink's
// coverage with them.
func observed[P any](see func(context.Context, [][]schema.Row[P]) error, next order.Sink[P]) order.Sink[P] {
	o := observer[P]{see, next}
	runs, ok := next.(order.RangeSink[P])
	if !ok {
		return o
	}
	r := rangeObserver[P]{o, runs}
	if trees, ok := next.(order.SubtreeSink[P]); ok {
		return subtreeObserver[P]{r, trees}
	}
	return r
}

type observer[P any] struct {
	see  func(context.Context, [][]schema.Row[P]) error
	next order.Sink[P]
}

func (o observer[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	if err := o.see(ctx, segs); err != nil {
		return err
	}
	return handOn(ctx, o.next, segs)
}

type rangeObserver[P any] struct {
	observer[P]
	runs order.RangeSink[P]
}

func (o rangeObserver[P]) Run() order.RangeRun[P] {
	return observedRun(o.see, o.runs.Run())
}

type subtreeObserver[P any] struct {
	rangeObserver[P]
	trees order.SubtreeSink[P]
}

func (o subtreeObserver[P]) SubtreeEnd(prefix string) { o.trees.SubtreeEnd(prefix) }

// observedRun is next's run with see before it, told its interval when
// next is.
func observedRun[P any](see func(context.Context, [][]schema.Row[P]) error, next order.RangeRun[P]) order.RangeRun[P] {
	r := runObserver[P]{see, next}
	if c, ok := next.(order.CoveredRun); ok {
		return coveredObserver[P]{r, c}
	}
	return r
}

type runObserver[P any] struct {
	see  func(context.Context, [][]schema.Row[P]) error
	next order.RangeRun[P]
}

func (r runObserver[P]) Write(ctx context.Context, rows []schema.Row[P]) error {
	if err := r.see(ctx, [][]schema.Row[P]{rows}); err != nil {
		return err
	}
	return r.next.Write(ctx, rows)
}

func (r runObserver[P]) End() { r.next.End() }

type coveredObserver[P any] struct {
	runObserver[P]
	c order.CoveredRun
}

func (r coveredObserver[P]) From(lo string)              { r.c.From(lo) }
func (r coveredObserver[P]) Through(hi string, top bool) { r.c.Through(hi, top) }

// handOn hands segs to next, if any.
func handOn[P any](ctx context.Context, next order.Sink[P], segs [][]schema.Row[P]) error {
	if next == nil {
		return nil
	}
	return next.Write(ctx, segs)
}
