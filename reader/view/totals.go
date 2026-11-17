// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// lastKind is the greatest Kind.
const lastKind = listing.KindUnknown

// selection is the kinds a summary counts: files and objects unless the
// options name others. KindPrefix is never counted.
type selection struct {
	kinds [lastKind + 1]bool
}

func selectionOf(kinds []listing.Kind) (selection, error) {
	var s selection
	if len(kinds) == 0 {
		kinds = []listing.Kind{listing.KindFile, listing.KindObject}
	}
	for _, k := range kinds {
		if k == 0 || k > lastKind {
			return selection{}, invalid("%v is no kind", k)
		}
		s.kinds[k] = true
	}
	return s, nil
}

func (s selection) counts(k listing.Kind) bool {
	return k != listing.KindPrefix && s.kinds[k]
}

// countsAny reports whether the selection counts any row of the shape.
func (s selection) countsAny(rows rowShape) bool {
	if !rows.local {
		return s.counts(listing.KindObject)
	}
	return s.counts(listing.KindFile) || s.counts(listing.KindDir) || s.counts(listing.KindSymlink) || s.counts(listing.KindOther) ||
		s.counts(listing.KindUnknown)
}

// The reductions of a set of rows, in result order: an object's, then a
// local entry's space on disk.
var (
	objectReductions = []seekparquet.Reduction{
		{Op: seekparquet.CountRows},
		{Op: seekparquet.Sum, Column: "size"},
		{Op: seekparquet.CountNulls, Column: "size"},
		{Op: seekparquet.Max, Column: "last_modified"},
		{Op: seekparquet.CountNulls, Column: "last_modified"},
	}
	localReductions = append(slices.Clip(objectReductions),
		seekparquet.Reduction{Op: seekparquet.Sum, Column: "allocated_bytes"},
		seekparquet.Reduction{Op: seekparquet.CountNulls, Column: "allocated_bytes"},
	)
)

const (
	reduceCount = iota
	reduceBytes
	reduceUnsized
	reduceNewest
	reduceUndated
	reduceAllocated
	reduceUnallocated
)

// errUnknownTotal reports a reduction SeekParquet left unknown with no
// budget in force, which reads every value it needs.
var errUnknownTotal = errors.New("view: an aggregation left a total unknown without a budget")

// aggregate counts and sums the rows of the selection's kinds in each
// span, in one aggregation: an element is nil where the value budget
// (SeekParquet's: -1 for none) did not reach the span. checkKeys checks
// the order of every key it counts.
func (inv *Inventory) aggregate(ctx context.Context, spans []seekparquet.RowSpan, sel selection, budget int64, checkKeys bool) ([]*listing.Totals, error) {
	out := make([]*listing.Totals, len(spans))
	if len(spans) == 0 {
		return out, nil
	}
	if !sel.countsAny(inv.rows) {
		for i := range out {
			out[i] = zeroTotals()
		}
		return out, nil
	}

	reductions := objectReductions
	if inv.rows.local {
		reductions = localReductions
	}
	opt := seekparquet.AggregateOptions{ValueBudgetBytes: budget, Workers: runtime.GOMAXPROCS(0), CheckKeys: checkKeys, Filter: inv.filter(sel)}
	results, err := inv.sp.AggregateRows(ctx, spans, reductions, opt, nil)
	if err != nil {
		return nil, err
	}

	for i, r := range results {
		if out[i], err = inv.totals(r, budget >= 0); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// totals reads one span's reductions: nil when a value budget did not
// reach them. A sum that overflows, or that a counted row cannot supply,
// is nil.
func (inv *Inventory) totals(r []seekparquet.AggregateResult, bounded bool) (*listing.Totals, error) {
	for _, x := range r {
		switch x.Status {
		case seekparquet.AggregateExact, seekparquet.AggregateOverflow:
		case seekparquet.AggregateBudgetExceeded, seekparquet.AggregateUnpriced:
			return nil, nil
		default:
			if bounded {
				return nil, nil
			}
			return nil, errUnknownTotal
		}
	}
	if r[reduceCount].Status != seekparquet.AggregateExact {
		return nil, errors.New("view: a count of rows overflowed")
	}

	t := &listing.Totals{Entries: r[reduceCount].Value.Int64()}
	t.Bytes = sum(r[reduceBytes], r[reduceUnsized])
	if v := r[reduceNewest]; v.Status == seekparquet.AggregateExact && !v.Value.IsNull() && none(r[reduceUndated]) {
		newest := time.UnixMicro(v.Value.Int64()).UTC()
		t.Newest = &newest
	}
	switch {
	case inv.rows.local:
		t.AllocatedBytes = sum(r[reduceAllocated], r[reduceUnallocated])
	case t.Entries == 0:
		t.AllocatedBytes = new(int64)
	}
	return t, nil
}

// sum is an exact sum over rows none of which lacks the value.
func sum(total, nulls seekparquet.AggregateResult) *int64 {
	if total.Status != seekparquet.AggregateExact || !none(nulls) {
		return nil
	}
	v := total.Value.Int64()
	return &v
}

func none(count seekparquet.AggregateResult) bool {
	return count.Status == seekparquet.AggregateExact && count.Value.Int64() == 0
}

// zeroTotals are the totals of no counted entry.
func zeroTotals() *listing.Totals {
	return &listing.Totals{Bytes: new(int64), AllocatedBytes: new(int64)}
}

// entryTotals are the totals of one entry, or of none.
func entryTotals(e *listing.Entry, sel selection) *listing.Totals {
	if e == nil || !sel.counts(e.Kind) {
		return zeroTotals()
	}
	return &listing.Totals{Entries: 1, Bytes: clone(e.Size), AllocatedBytes: clone(e.AllocatedBytes), Newest: clone(e.LastModified)}
}

// filter checks every row an aggregation reads, as the view checks an
// entry, and keeps those of the selection's kinds. It reads the columns
// the reductions read, so it adds no page.
func (inv *Inventory) filter(sel selection) *seekparquet.BatchFilter {
	if !inv.rows.local {
		return &seekparquet.BatchFilter{
			Columns: []string{"size", "last_modified"},
			Apply: func(b *seekparquet.ValueBatch, _ []bool) error {
				if err := present(b.Column(1), "last_modified", seekparquet.KindInt64); err != nil {
					return err
				}
				return sizes(b.Column(0), "size")
			},
		}
	}
	return &seekparquet.BatchFilter{
		Columns: []string{"kind", "size", "allocated_bytes", "last_modified"},
		Apply: func(b *seekparquet.ValueBatch, keep []bool) error {
			kind := b.Column(0)
			if err := present(kind, "kind", seekparquet.KindByteArray); err != nil {
				return err
			}
			if err := present(b.Column(3), "last_modified", seekparquet.KindInt64); err != nil {
				return err
			}
			for i := range b.Len() {
				k, ok := localKinds[string(kind.Bytes(i))]
				if kind.IsNull(i) || !ok {
					return &reader.SemanticError{Kind: reader.KindRow, Column: "kind", Err: errors.New("null, or not a kind swath writes")}
				}
				keep[i] = sel.counts(k)
			}
			if err := sizes(b.Column(1), "size"); err != nil {
				return err
			}
			return sizes(b.Column(2), "allocated_bytes")
		},
	}
}

// present refuses a column a part lacks or holds with another physical
// type.
func present(c seekparquet.ColumnView, name string, want seekparquet.Kind) error {
	switch {
	case !c.Present():
		return &reader.SemanticError{Kind: reader.KindSchema, Column: name, Err: errors.New("absent from a part")}
	case c.Kind() != want:
		return &reader.SemanticError{Kind: reader.KindSchema, Column: name, Err: fmt.Errorf("a %s column, the schema's type is %s", c.Kind(), want)}
	}
	return nil
}

// sizes refuses a byte count that is negative.
func sizes(c seekparquet.ColumnView, name string) error {
	if err := present(c, name, seekparquet.KindInt64); err != nil {
		return err
	}
	for i := range c.Len() {
		if !c.IsNull(i) && c.Int64(i) < 0 {
			return &reader.SemanticError{Kind: reader.KindRow, Column: name, Err: errors.New("negative")}
		}
	}
	return nil
}

// rowBudget admits sets of row spans, in order, while the rows of the
// union of those admitted fit max, so a row several spans hold counts
// once; a zero max admits every set.
type rowBudget struct {
	max int64
	// held is the union admitted: disjoint [lo, hi) ordinals, sorted.
	held [][2]int64
}

// admit reports whether every span of spans fits, and then holds them
// all; a set that does not fit holds none. A span whose rows are not
// established is errUnknownSpan.
func (b *rowBudget) admit(spans []seekparquet.RowSpan) (bool, error) {
	if b.max == 0 {
		return true, nil
	}
	held := slices.Clone(b.held)
	for _, span := range spans {
		lo, okLo := span.Start.Ordinal()
		hi, okHi := span.End.Ordinal()
		if !okLo || !okHi {
			return false, errUnknownSpan
		}
		held = append(held, [2]int64{lo, hi})
	}
	held = merge(held)

	var rows int64
	for _, h := range held {
		rows += h[1] - h[0]
	}
	if rows > b.max {
		return false, nil
	}
	b.held = held
	return true, nil
}

// established reports whether both of span's boundaries are known.
func established(span seekparquet.RowSpan) bool {
	_, start := span.Start.Ordinal()
	_, end := span.End.Ordinal()
	return start && end
}

// errUnknownSpan reports a span SeekParquet did not establish.
var errUnknownSpan = errors.New("view: a selection's rows are not established")

// merge sorts intervals and joins those that overlap or touch.
func merge(intervals [][2]int64) [][2]int64 {
	slices.SortFunc(intervals, func(a, b [2]int64) int { return cmp.Compare(a[0], b[0]) })
	out := intervals[:0]
	for _, iv := range intervals {
		if n := len(out); n > 0 && iv[0] <= out[n-1][1] {
			out[n-1][1] = max(out[n-1][1], iv[1])
			continue
		}
		out = append(out, iv)
	}
	return out
}
