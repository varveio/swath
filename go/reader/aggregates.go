// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"

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
// One plan of every prefix's keys (seekparquet.Dataset.Plan without
// columns) locates the rows of each, decoding the key pages at their ends
// in one request; the count of a prefix is the count of its rows
// (seekparquet.Dataset.PlanRows, which reads nothing): exact when every
// part holding them has been checked by an earlier read, else nil, so a
// count never relies on a manifest row count alone and never opens a part
// between its ends. The count of every key (the prefix "") after
// CheckSchema has succeeded is the manifest's row count, which it checked
// against every part's footer, and reads nothing.
//
// Sums plan the rows of the prefixes with a known count, projecting size
// (PlanRows: the parts holding them are checked and the size column's
// OffsetIndexes read), and read their size values alone, by row
// (seekparquet.Plan.ScanValues): no key page between a prefix's ends, and
// a row several prefixes share read once. Under SumsWithinBudget the
// prefixes are taken in ascending key order, each summed when the
// compressed size pages and dictionaries of it and those already selected
// (seekparquet.Plan.SelectRows, ProjectedBytes), each shared extent
// counted once, fit the budget: a prefix inside one already selected adds
// nothing, and one whose bytes cannot be proven (a size chunk without an
// OffsetIndex) is not summed. Every summed size is checked as Stream checks
// it (a filesystem entry may lack one, which leaves its prefixes' sums nil),
// failing with a SemanticError of KindRow that names the column and the
// row's dataset ordinal, not its key, which a sum does not read.
//
// A context already ended or a closed Dataset fails it first, with the
// context's error or seekparquet.ErrClosed. An invalid AggregateOptions is
// an error wrapping
// seekparquet.ErrInvalidRequest, and a versions schema a SemanticError of
// KindSchema, both before any read. The module's refusals and the Source's
// errors are returned unchanged, and a context that ends fails Aggregates
// with its error. No aggregates are returned with an error.
func (d *Dataset) Aggregates(ctx context.Context, prefixes [][]byte, opt AggregateOptions) ([]PrefixAggregate, error) {
	if err := d.checkAggregates(ctx, opt); err != nil {
		return nil, err
	}
	out := make([]PrefixAggregate, len(prefixes))
	if !opt.Counts && opt.Sums == SumsOff {
		return out, nil
	}
	unique, of := distinct(prefixes)
	targets := make([]target, len(unique))
	for i, p := range unique {
		targets[i].span = prefixSpan(p)
	}
	aggs, err := d.aggregate(ctx, targets, opt, nil)
	if err != nil {
		return nil, err
	}
	for i, u := range of {
		out[i] = aggs[u]
	}
	return out, nil
}

// checkAggregates refuses, before any read, an ended context, a closed
// Dataset, invalid options and a versions schema.
func (d *Dataset) checkAggregates(ctx context.Context, opt AggregateOptions) error {
	if err := d.usable(ctx); err != nil {
		return err
	}
	if err := opt.validate(); err != nil {
		return err
	}
	if objectColumns(d.spec.ID) == nil {
		return notCurrent(d.spec.ID)
	}
	return nil
}

// target is a prefix to total: its keys, and its rows when a listing
// located them (known).
type target struct {
	span  keySpan
	rows  seekparquet.RowSpan
	known bool
}

// aggregate returns the totals opt requests of each target, adding the
// statistics of its plans and scans to st when it is not nil. A target's
// rows not known are located by one plan of their keys.
func (d *Dataset) aggregate(ctx context.Context, targets []target, opt AggregateOptions, st *ReadStats) ([]PrefixAggregate, error) {
	out := make([]PrefixAggregate, len(targets))
	counts := make([]*int64, len(targets))
	var locate []int
	for i, t := range targets {
		if !t.known && !(t.span.whole() && d.checked.Load() && opt.Sums == SumsOff) {
			locate = append(locate, i)
		}
	}
	if err := d.locate(ctx, targets, locate, st); err != nil {
		return nil, err
	}
	for i, t := range targets {
		if !t.known { // every key, after CheckSchema
			counts[i] = ptrTo(d.rows())
			continue
		}
		plan, err := d.sp.PlanRows(ctx, []seekparquet.RowSpan{t.rows}, nil, nil)
		if err != nil {
			return nil, err
		}
		if n, exact := plan.Count(); exact {
			counts[i] = &n
		}
	}
	if opt.Sums != SumsOff {
		if err := d.sum(ctx, targets, counts, opt, out, st); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opt.Counts {
		for i, n := range counts {
			out[i].Count = n
		}
	}
	return out, nil
}

// whole reports whether s is every key.
func (s keySpan) whole() bool { return len(s.lo) == 0 && s.hi == nil }

// locate sets the rows of targets[i] for each i of which, by one plan of
// their keys.
func (d *Dataset) locate(ctx context.Context, targets []target, which []int, st *ReadStats) error {
	if len(which) == 0 {
		return nil
	}
	spans := make([]keySpan, len(which))
	for k, i := range which {
		spans[k] = targets[i].span
	}
	var planned ReadStats
	plan, err := d.sp.Plan(ctx, selections(spans), nil, &planned)
	addStats(st, planned)
	if err != nil {
		return err
	}
	for k, rows := range plan.RowSpans() {
		targets[which[k]].rows, targets[which[k]].known = rows, true
	}
	return nil
}

// sum sets the Bytes of out[i] for the targets the mode selects among
// those with a known count: nil for any other, or whose sum overflows.
func (d *Dataset) sum(ctx context.Context, targets []target, counts []*int64, opt AggregateOptions, out []PrefixAggregate, st *ReadStats) error {
	var candidates []int
	for i, n := range counts {
		switch {
		case n == nil:
		case *n == 0:
			out[i].Bytes = ptrTo(int64(0))
		default:
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	slices.SortStableFunc(candidates, func(a, b int) int { return bytes.Compare(targets[a].span.lo, targets[b].span.lo) })
	rows := make([]seekparquet.RowSpan, len(candidates))
	for k, i := range candidates {
		rows[k] = targets[i].rows
	}
	var planned ReadStats
	plan, err := d.sp.PlanRows(ctx, rows, []string{"size"}, &planned)
	addStats(st, planned)
	if err != nil {
		return err
	}
	selected := candidates
	if opt.Sums == SumsWithinBudget {
		selected, rows = nil, rows[:0:0]
		for _, i := range candidates {
			p, err := plan.SelectRows(append(rows, targets[i].rows))
			if err != nil {
				return err
			}
			if n, exact := p.ProjectedBytes(); exact && n <= opt.budget() {
				selected, rows = append(selected, i), append(rows, targets[i].rows)
				plan = p
			}
		}
		if len(selected) == 0 {
			return nil
		}
	}
	sums, err := d.sumRows(ctx, plan, st)
	if err != nil {
		return err
	}
	for k, i := range selected {
		out[i].Bytes = sums[k].total()
	}
	return nil
}

// sumRows reads the size values of plan's rows (ScanValues) and returns
// the sum of each of its row spans (RowSpans): the sizes of the rows
// between two consecutive span boundaries are added up once, and that sum
// added to every span holding them.
func (d *Dataset) sumRows(ctx context.Context, plan *seekparquet.Plan, st *ReadStats) ([]objectSum, error) {
	spans := plan.RowSpans()
	type bounds struct{ start, end int64 }
	of := make([]bounds, len(spans))
	var cuts []int64
	for i, sp := range spans {
		start, _ := sp.Start.Ordinal()
		end, _ := sp.End.Ordinal()
		of[i] = bounds{start, end}
		cuts = append(cuts, start, end)
	}
	slices.Sort(cuts)
	cuts = slices.Compact(cuts)
	segments := make([]objectSum, len(cuts))
	sizeSpec, _ := d.spec.Column("size")
	check := sizeCheck{required: sizeSpec.Required, mayBeAbsent: sizeSpec.MayBeAbsent, unsized: d.spec.ID == LocalV1}
	k := 0 // the segment [cuts[k], cuts[k+1]) of the row last read
	var scanned ReadStats
	err := plan.ScanValues(ctx, &scanned, func(b *seekparquet.ValueBatch) error {
		start, _ := b.Start().Ordinal()
		sizes := b.Column(0)
		for r := range b.Len() {
			ordinal := start + int64(r)
			for k+1 < len(cuts) && cuts[k+1] <= ordinal {
				k++
			}
			v, err := check.value(sizes, r, ordinal)
			if err != nil {
				return err
			}
			segments[k].add(v)
		}
		return nil
	})
	addStats(st, scanned)
	if err != nil {
		return nil, err
	}
	sums := make([]objectSum, len(spans))
	for i, b := range of {
		for k := sort.Search(len(cuts), func(k int) bool { return cuts[k] >= b.start }); k < len(cuts) && cuts[k] < b.end; k++ {
			sums[i].merge(&segments[k])
		}
	}
	return sums, nil
}

// sizeCheck checks a size value as Stream checks an object's: present
// unless the entry may lack one (unsized, a filesystem entry), and not
// negative. required and mayBeAbsent are the size column's spec.
type sizeCheck struct {
	required, mayBeAbsent, unsized bool
}

// value returns row r's size of sizes, the row at ordinal in the dataset.
func (c sizeCheck) value(sizes seekparquet.ColumnView, r int, ordinal int64) (rowValue, error) {
	switch {
	case !sizes.Present() && !c.mayBeAbsent:
		return rowValue{}, semantic(KindSchema, "size", nil, "absent from the part of dataset row %d", ordinal)
	case sizes.Present() && sizes.Kind() != seekparquet.KindInt64:
		return rowValue{}, semantic(KindSchema, "size", nil, "%v values, not INT64, at dataset row %d", sizes.Kind(), ordinal)
	case !sizes.Present() || sizes.IsNull(r):
		if c.required || !c.unsized {
			return rowValue{}, semantic(KindRow, "size", nil, "dataset row %d: null: an object has a size", ordinal)
		}
		return rowValue{}, nil
	}
	size := sizes.Int64(r)
	if size < 0 {
		return rowValue{}, semantic(KindRow, "size", nil, "dataset row %d: %d is negative", ordinal, size)
	}
	return rowValue{size: size, sized: true}, nil
}

func ptrTo[T any](v T) *T { return &v }
