// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package bind binds a provider to its row schema and lists it through the
// order stage into a sink its caller supplies. The provider's range type is
// erased and its row type kept; between the provider and the sink stand the
// row filters, the object-size and version counters and the per-prefix
// totals. It chooses no sink: the Parquet router and the text stream are
// the command's (package registry), and an unordered listing (Count) takes
// any order.Sink, so code inside swath can walk a provider with its own.
package bind

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/totals"
)

// Bound is a provider bound to its row schema and the schema's version
// roles.
type Bound[P any] struct {
	p source.Provider[any, P]
	s *schema.Schema[P]
	// v is s's version roles (schema.BindVersions), nil for none.
	v *schema.Versions[P]
}

// Provider erases p's range type (source.Erase: ranges boxed once each,
// rows untouched) and binds it to its row schema s and the schema's
// version roles, which it refuses when they do not define the rows
// (schema.BindVersions).
func Provider[R, P any](p source.Provider[R, P], s *schema.Schema[P]) (Bound[P], error) {
	v, err := schema.BindVersions(s)
	if err != nil {
		return Bound[P]{}, err
	}
	return Bound[P]{source.Erase[R, P](p), s, v}, nil
}

// Schema is the bound row schema.
func (b Bound[P]) Schema() *schema.Schema[P] { return b.s }

// Env is a listing's settings apart from where its rows go.
type Env struct {
	// Order is the order stage's options; its grouping policy is the
	// schema's (Prepare sets Groups).
	Order order.Options
	// Empty lists nothing: a resume whose final part is durable.
	Empty bool
	// Ordered, if set, counts the rows a count (Count) hands on; the sink
	// of an ordered listing (Run) counts its own.
	Ordered *atomic.Int64
	// Filter, if set, drops the rows it does not accept before the order
	// stage, counting them in Filtered.
	Filter   *filter.Spec
	Filtered *atomic.Int64
	// Sizes, if set, sums the object sizes of the rows handed to the order
	// stage (after the filters).
	Sizes *Sizes
	// Versions, if set, counts the version rows handed on (after the
	// filters), for a schema with version roles.
	Versions *VersionCounts
	// Totals, if set, sums the rows per key prefix (swath du); only a count
	// takes it.
	Totals *totals.Tally
}

// Prepared is a bound provider with a listing's settings, checked against
// its schema. It lists once, by Count or Run.
type Prepared[P any] struct {
	b   Bound[P]
	env Env
	// p is the provider behind the filters and the size counter.
	p source.Provider[any, P]
}

// Prepare checks env against the schema, compiling the filters, before
// anything is listed or any sink is made: a sink with side effects (the
// Parquet router makes its directories) is then made only for a listing
// that can run.
func (b Bound[P]) Prepare(env Env) (*Prepared[P], error) {
	env.Order.Groups = b.s.Info.Groups
	keep, err := filter.Compile(env.Filter, b.s)
	if err != nil {
		return nil, err
	}

	if env.Versions != nil && b.v != nil {
		env.Versions.Enabled, env.Versions.HasDeleteMarkers = true, b.v.HasDeleteMarkers()
	}
	p := b.p
	if env.Empty {
		p = source.Empty[P]{}
	} else if keep != nil {
		p = newFiltered(p, keep, b.s, env.Filtered)
	}
	if env.Sizes != nil && !env.Empty {
		if size := schema.ObjectSize(b.s); size != nil {
			env.Sizes.Measured.Store(true)
			p = newSized(p, size, env.Sizes)
		}
	}
	return &Prepared[P]{b, env, p}, nil
}

// Count lists without ordering the rows (order.CountWithSink): an
// unordered listing, whose rows reach sink (nil: none) in validated
// batches, concurrently from independent ranges. The version counter and
// the per-prefix totals see the rows before sink does.
func (l *Prepared[P]) Count(ctx context.Context, sink order.Sink[P]) (order.Stats, error) {
	observe := sink
	if l.env.Versions != nil && l.b.v != nil {
		next := sink
		if next == nil {
			next = discard[P]{}
		}
		observe = versionSink[P]{Sink: next, counts: l.env.Versions, v: l.b.v}
	}
	if l.env.Totals != nil {
		var err error
		observe, err = Totals(l.env.Totals, l.b.s, l.b.v, observe, !l.env.Order.Unordered)
		if err != nil {
			return order.Stats{}, err
		}
	}
	return order.CountWithSink(ctx, l.p, l.env.Ordered, l.env.Order, observe)
}

// Run lists through the order stage (order.Run) into sink, in key order;
// once listing is done the order stage hands the rest on as parallel
// segments when sink is an order.Segmenter. The version counter sees the
// rows handed to sink, its segments' included.
func (l *Prepared[P]) Run(ctx context.Context, sink order.Sink[P]) (order.Stats, error) {
	if l.env.Totals != nil {
		return order.Stats{}, errors.New("bind: per-prefix totals need a count")
	}
	if l.env.Versions != nil && l.b.v != nil {
		observer := versionSink[P]{Sink: sink, counts: l.env.Versions, v: l.b.v}
		if sg, ok := sink.(order.Segmenter[P]); ok {
			sink = versionSegmenter[P]{observer, sg}
		} else {
			sink = observer
		}
	}
	return order.Run(ctx, l.p, sink, l.b.s, l.env.Order)
}

// discard is a sink that keeps nothing.
type discard[P any] struct{}

func (discard[P]) Write(context.Context, [][]schema.Row[P]) error { return nil }
