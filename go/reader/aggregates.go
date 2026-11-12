// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	seekparquet "github.com/varveio/seekparquet/go"
)

// SumMode selects which prefixes of an Aggregates request get a byte sum.
type SumMode int

const (
	// SumsOff computes no byte sums.
	SumsOff SumMode = iota
	// SumsWithinBudget sums prefixes in ascending key order while the size
	// pages they need fit AggregateOptions.SumBudgetBytes, skipping any
	// that does not fit and trying the next.
	SumsWithinBudget
	// SumsAlways sums every prefix with a known count, whatever its cost.
	SumsAlways
)

// DefaultSumBudget is the size-page byte budget of SumsWithinBudget when
// AggregateOptions.SumBudgetBytes is zero.
const DefaultSumBudget = 4 << 20

// minPageBytes is fewer bytes than any page holds: a data page header
// alone takes at least 17 bytes of Thrift compact protocol.
const minPageBytes = 16

// AggregateOptions requests the recursive totals of prefixes from
// Aggregates.
type AggregateOptions struct {
	// Counts requests each prefix's object count. It plans each prefix's
	// range without columns, which decodes the key pages at its two ends
	// and no value page.
	Counts bool
	// Sums selects the prefixes whose object sizes are summed. A sum
	// decodes the size pages of the prefix's rows, so its cost grows with
	// the number of objects summed. Only a prefix with a known count is
	// summed.
	Sums SumMode
	// SumBudgetBytes bounds, for SumsWithinBudget, the compressed bytes of
	// size pages, and of their dictionaries, the sums read
	// (seekparquet.Plan.ProjectedBytes), an extent that several
	// prefixes share counted once; zero means DefaultSumBudget. It must be
	// zero for any other mode. Opening a part and reading its footer, key
	// pages and page indexes to plan the sums are not charged. A budget
	// that cannot hold one page plans nothing.
	SumBudgetBytes int64
}

// PrefixAggregate is the recursive totals of one prefix: every object
// whose key begins with it, an object whose key is the prefix included. A
// nil field was not requested or not computed; it is never a partial or
// estimated total. Over a filesystem's entries they are Totals': entries
// of every kind, a directory's own row "d/" included for "d/", and their
// entry bytes (PathTotals).
type PrefixAggregate struct {
	// Count is the number of objects under the prefix. It is nil when its
	// rows span a part no read has checked, whose row count is the
	// manifest's alone (seekparquet.Plan.Count).
	Count *int64
	// Bytes is the sum of their sizes. It is nil when the count is
	// unknown, the prefix was not summed or did not fit the budget, the
	// budget could not be proven (a size chunk without an OffsetIndex), a
	// filesystem entry under the prefix has no size, or the sum does not
	// fit an int64.
	Bytes *int64
}

func (o AggregateOptions) validate() error {
	switch o.Sums {
	case SumsOff, SumsAlways:
		if o.SumBudgetBytes != 0 {
			return fmt.Errorf("%w: a sum budget requires SumsWithinBudget", seekparquet.ErrInvalidRequest)
		}
	case SumsWithinBudget:
		if o.SumBudgetBytes < 0 {
			return fmt.Errorf("%w: a sum budget must not be negative", seekparquet.ErrInvalidRequest)
		}
	default:
		return fmt.Errorf("%w: unknown sum mode %d", seekparquet.ErrInvalidRequest, o.Sums)
	}
	return nil
}

func (o AggregateOptions) budget() int64 {
	if o.SumBudgetBytes == 0 {
		return DefaultSumBudget
	}
	return o.SumBudgetBytes
}

// Aggregates returns one PrefixAggregate per prefix, in input order.
// Prefixes may repeat, nest and overlap; each total is independent. With
// neither Counts nor Sums it reads nothing.
//
// Counts come from each prefix's planned range
// (seekparquet.Dataset.Plan without columns): exact when every part
// holding its rows has been checked by an earlier read, else nil, so a
// count never relies on a manifest row count alone and never opens a part
// between its ends. Sums plan the summed prefixes' rows projecting size
// alone and scan them once, a row several prefixes share decoded once;
// planning checks every part holding those rows. Under SumsWithinBudget a
// prefix is summed when the projected bytes it adds to those already
// selected fit what remains of the budget; a prefix inside one already
// // selected, or whose pages are all charged already, adds none. Every
// summed object is checked as Stream checks its size (a filesystem entry
// may lack one, which leaves its prefixes' sums nil), failing with a
// SemanticError of KindRow.
//
// A context already ended or a closed Dataset fails it first, with the
// context's error or seekparquet.ErrClosed. An invalid AggregateOptions is
// an error wrapping
// seekparquet.ErrInvalidRequest, and a versions schema a SemanticError of
// KindSchema, both before any read. The module's refusals and the Source's
// errors are returned unchanged, and a context that ends fails Aggregates
// with its error. No aggregates are returned with an error.
func (d *Dataset) Aggregates(ctx context.Context, prefixes [][]byte, opt AggregateOptions) ([]PrefixAggregate, error) {
	return d.aggregates(ctx, prefixes, opt, nil)
}

// aggregates is Aggregates, adding the statistics of its plans and scans
// to st when it is not nil.
func (d *Dataset) aggregates(ctx context.Context, prefixes [][]byte, opt AggregateOptions, st *ReadStats) ([]PrefixAggregate, error) {
	if err := d.usable(ctx); err != nil {
		return nil, err
	}
	if err := opt.validate(); err != nil {
		return nil, err
	}
	if objectColumns(d.spec.ID) == nil {
		return nil, notCurrent(d.spec.ID)
	}
	out := make([]PrefixAggregate, len(prefixes))
	if !opt.Counts && opt.Sums == SumsOff {
		return out, nil
	}
	unique, of := distinct(prefixes)
	spans := make([]keySpan, len(unique))
	counts := make([]*int64, len(unique))
	for i, p := range unique {
		spans[i] = prefixSpan(p)
		var err error
		if counts[i], err = d.count(ctx, spans[i], st); err != nil {
			return nil, err
		}
	}
	var sums []*int64
	if opt.Sums != SumsOff {
		var err error
		if sums, err = d.prefixSums(ctx, spans, counts, opt, st); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i, u := range of {
		if opt.Counts && counts[u] != nil {
			out[i].Count = ptrTo(*counts[u])
		}
		if sums != nil && sums[u] != nil {
			out[i].Bytes = ptrTo(*sums[u])
		}
	}
	return out, nil
}

// prefixSums sums the sizes of the prefixes the mode selects among those
// with a known count: nil for any other, or whose sum overflows.
func (d *Dataset) prefixSums(ctx context.Context, spans []keySpan, counts []*int64, opt AggregateOptions, st *ReadStats) ([]*int64, error) {
	sums := make([]*int64, len(spans))
	var candidates []int
	for i, n := range counts {
		switch {
		case n == nil:
		case *n == 0:
			sums[i] = ptrTo(int64(0))
		default:
			candidates = append(candidates, i)
		}
	}
	selected := candidates
	if opt.Sums == SumsWithinBudget {
		var err error
		if selected, err = d.withinBudget(ctx, spans, candidates, opt.budget(), st); err != nil {
			return nil, err
		}
	}
	chosen := make([]keySpan, len(selected))
	for k, i := range selected {
		chosen[k] = spans[i]
	}
	found, err := d.sumSpans(ctx, chosen, []string{"size"}, st, 1)
	if err != nil {
		return nil, err
	}
	for k, i := range selected {
		sums[i] = found.sums[k].total()
	}
	return sums, nil
}

// withinBudget returns the candidates whose size pages fit budget, taken
// in ascending key order. Prefixes either nest or are disjoint, so a
// candidate is either inside the last one selected, adding nothing, or
// after every one selected: an extent it shares with any of them, a page
// or a chunk's dictionary, holds rows between that one and the candidate,
// and so rows of the last one selected. What a candidate adds is then the
// projected bytes of it and the last one selected, less those of the last
// one alone, zero when its pages are all charged already. Until one is
// selected every candidate adds at least a page, so a budget below the
// smallest page plans none.
func (d *Dataset) withinBudget(ctx context.Context, spans []keySpan, candidates []int, budget int64, st *ReadStats) ([]int, error) {
	order := slices.Clone(candidates)
	slices.SortFunc(order, func(a, b int) int { return bytes.Compare(spans[a].lo, spans[b].lo) })
	var selected []int
	var used, lastBytes int64
	last := -1
	for _, i := range order {
		if last >= 0 && spans[last].contains(spans[i]) {
			selected = append(selected, i)
			continue
		}
		if last < 0 && budget < minPageBytes {
			continue
		}
		alone, ok, err := d.projectedSize(ctx, st, spans[i])
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		added := alone
		if last >= 0 {
			both, ok, err := d.projectedSize(ctx, st, spans[last], spans[i])
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			added = both - lastBytes
		}
		if used+added > budget {
			continue
		}
		used += added
		selected = append(selected, i)
		last, lastBytes = i, alone
	}
	slices.Sort(selected)
	return selected, nil
}

// projectedSize returns the compressed bytes a scan of spans' size column
// reads, and false when a size chunk has no OffsetIndex to prove it.
func (d *Dataset) projectedSize(ctx context.Context, st *ReadStats, spans ...keySpan) (int64, bool, error) {
	var planned ReadStats
	plan, err := d.sp.Plan(ctx, selections(spans), []string{"size"}, &planned)
	addStats(st, planned)
	if err != nil {
		return 0, false, err
	}
	n, exact := plan.ProjectedBytes()
	return n, exact, nil
}

func ptrTo[T any](v T) *T { return &v }
