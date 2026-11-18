// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/reader/listing"
)

// Walker is a Store that can also walk: list everything under a prefix,
// flat and in parallel, through its provider's range engine.
type Walker interface {
	// Walk lists every entry under w.Prefix into sink, in no order, each
	// once: a provider of its own for this call, over the store's
	// client, credentials and limiter, bound to its row schema and counted
	// (Count). It returns when every worker has, with an error that
	// matches a class of package listing, or with ctx's own once ctx has
	// ended. Each place the walk could not read and went on past reaches
	// sink.Unread.
	Walk(ctx context.Context, w Walk, sink Sink) error
}

// Walk is what one walk lists.
type Walk struct {
	// Prefix is the scope: the keys that begin with it.
	Prefix []byte
	// Workers is how many ranges are listed at once: zero for the store's
	// default, one for its sequential mode (one request chain, never
	// split).
	Workers int
}

// Sink takes a walk's entries and the places it could not read. Its
// methods are called concurrently, from the walk's workers.
type Sink interface {
	// Entries takes a batch of entries of one range, which belong to the
	// walk until Entries returns. An error stops the walk: it is the
	// count's, and the batch counts as not emitted.
	Entries(ctx context.Context, entries []listing.Entry) error
	// Unread takes a place the walk could not read and went on past, and
	// keeps its key.
	Unread(u listing.Unread)
}

// Count lists b's scope without ordering it across ranges
// (bind.Prepared.Count, order.CountWithSink) into sink, with opt as the
// order stage's options: each range's rows checked as opt says, made
// entries by entries, and handed to sink.Entries; each place the
// provider reports through its emitter that it could not read handed to
// sink.Unread. Unless opt.Unordered, the
// ranges are also checked disjoint before their rows reach sink: each
// run of a range's rows spans the keys from its first to its last, and
// no two spans meet, so with each range strictly ascending no key reaches
// sink twice. A broken order is source.ErrProtocol, which
// objhttp.ListingClass reads as listing.ErrInvalidPage. A panic in the
// provider is the count's error.
func Count[P any](ctx context.Context, b bind.Bound[P], opt order.Options, entries func([]schema.Row[P]) []listing.Entry, sink Sink) error {
	l, err := b.Prepare(bind.Env{Order: opt})
	if err != nil {
		return err
	}

	r := rows[P]{sink: sink, entries: entries}
	if !opt.Unordered {
		r.spans = &spans{}
	}
	_, err = l.Count(ctx, r)
	var broken *dataset.InvariantError
	if errors.As(err, &broken) {
		return source.Violation(err)
	}
	return err
}

// rows is a count's sink (order.RangeSink, order.UnreadSink,
// order.Recovering) that hands a walk's rows to its Sink as entries, each
// run's checked against the others' spans when spans is set.
type rows[P any] struct {
	sink    Sink
	entries func([]schema.Row[P]) []listing.Entry
	spans   *spans
}

func (r rows[P]) Run() order.RangeRun[P] { return &run[P]{rows: r} }

// Write takes rows outside any run, unchecked: a count whose sink is a
// RangeSink sends none.
func (r rows[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	for _, seg := range segs {
		if err := r.write(ctx, nil, seg); err != nil {
			return err
		}
	}
	return nil
}

func (r rows[P]) Unread(u listing.Unread) { r.sink.Unread(u) }

func (r rows[P]) Recovers() bool { return true }

// write checks seg against the other runs' spans, extending s, its run's
// (nil for none), and hands it to the sink.
func (r rows[P]) write(ctx context.Context, s **span, seg []schema.Row[P]) error {
	if len(seg) == 0 {
		return nil
	}
	if r.spans != nil && s != nil {
		if err := r.spans.extend(s, seg[0].Key, seg[len(seg)-1].Key); err != nil {
			return err
		}
	}
	return r.sink.Entries(ctx, r.entries(seg))
}

// run is one range's consecutive rows (order.RangeRun), and its span.
type run[P any] struct {
	rows[P]
	span *span
}

func (r *run[P]) Write(ctx context.Context, seg []schema.Row[P]) error {
	return r.write(ctx, &r.span, seg)
}

func (r *run[P]) End() {}

// spans are a walk's runs' spans, by their first key, none meeting
// another: the keys a walk has handed on, as intervals.
type spans struct {
	mu  sync.Mutex
	all []*span
}

// span is one run's keys so far: from lo to hi, both included.
type span struct {
	lo, hi string
}

// extend makes *s, nil before its run's first rows, reach rows from lo
// to hi, the run's next ascending rows: an InvariantError if another run's
// span meets it. Keys are compared, not quoted: they are the store's.
func (sp *spans) extend(s **span, lo, hi string) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if *s != nil {
		lo = (*s).lo
	}
	i, _ := slices.BinarySearchFunc(sp.all, lo, func(x *span, k string) int { return strings.Compare(x.lo, k) })
	j := i
	if *s != nil {
		j++ // its own span is at i
	}
	switch {
	case *s == nil && i > 0 && sp.all[i-1].hi >= lo,
		j < len(sp.all) && sp.all[j].lo <= hi:
		return &dataset.InvariantError{Reason: "walk: two ranges' keys overlap"}
	}

	if *s != nil {
		(*s).hi = strings.Clone(hi)
		return nil
	}
	*s = &span{lo: strings.Clone(lo), hi: strings.Clone(hi)}
	sp.all = slices.Insert(sp.all, i, *s)
	return nil
}
