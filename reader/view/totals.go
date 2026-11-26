// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"errors"
	"fmt"
	"runtime"

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

// errUnknownTotal reports a span whose fold SeekParquet left inexact,
// which with no value budget it reads every value to give.
var errUnknownTotal = wrap(listing.ErrUnsupported, "an aggregation left a total unknown")

// fold counts the rows of the selection's kinds in each span into their
// groups, in one fold that reads every value it needs and checks the
// order of every key it counts.
func (inv *Inventory) fold(ctx context.Context, spans []seekparquet.RowSpan, sel selection, split listing.Split) ([]tallies, error) {
	f := objectFold(split)
	if inv.rows.local {
		f = localFold(sel, split)
	}
	opt := seekparquet.FoldOptions{Workers: runtime.GOMAXPROCS(0), CheckKeys: true}
	results, err := seekparquet.FoldRows(ctx, inv.sp, spans, f, opt, nil)
	if err != nil {
		return nil, err
	}

	out := make([]tallies, len(results))
	for i, r := range results {
		if r.Status != seekparquet.FoldExact {
			return nil, errUnknownTotal
		}
		out[i] = r.Value
	}
	return out, nil
}

// The slots of the folds' projections.
const (
	foldObjectSize = iota
	foldObjectModified
	foldObjectClass // projected only when the split asks for it
)

const (
	foldLocalSize = iota
	foldLocalModified
	foldLocalAllocated
	foldLocalKind
)

// objectFold counts an object inventory's rows, checking every row as the
// view checks an entry. Its rows have no space on disk.
func objectFold(split listing.Split) seekparquet.Fold[tallies] {
	columns := []string{"size", "last_modified"}
	if split.StorageClass {
		columns = append(columns, "storage_class")
	}
	return seekparquet.Fold[tallies]{
		Columns: columns,
		Add: func(acc *tallies, b *seekparquet.ValueBatch) error {
			size := b.Column(foldObjectSize)
			modified := b.Column(foldObjectModified)
			if err := present(modified, "last_modified", seekparquet.KindInt64); err != nil {
				return err
			}
			if err := sizes(size, "size"); err != nil {
				return err
			}
			var class seekparquet.ColumnView
			if split.StorageClass {
				class = b.Column(foldObjectClass)
				if err := present(class, "storage_class", seekparquet.KindByteArray); err != nil {
					return err
				}
			}

			for i := range b.Len() {
				it := item{size: int64Of(size, i), modified: int64Of(modified, i)}
				if split.StorageClass && !class.IsNull(i) {
					it.class, it.classed = class.Bytes(i), true
				}
				acc.add(it, split.SizeBounds)
			}
			return nil
		},
		Merge: merge,
	}
}

// localFold counts a local inventory's rows of the selection's kinds,
// checking every row as the view checks an entry. Its rows have no
// storage class.
func localFold(sel selection, split listing.Split) seekparquet.Fold[tallies] {
	return seekparquet.Fold[tallies]{
		Columns: []string{"size", "last_modified", "allocated_bytes", "kind"},
		Add: func(acc *tallies, b *seekparquet.ValueBatch) error {
			kind := b.Column(foldLocalKind)
			size := b.Column(foldLocalSize)
			allocated := b.Column(foldLocalAllocated)
			modified := b.Column(foldLocalModified)
			if err := checkLocal(kind, size, allocated, modified); err != nil {
				return err
			}

			for i := range b.Len() {
				if !sel.counts(localKinds[string(kind.Bytes(i))]) {
					continue
				}
				it := item{size: int64Of(size, i), allocated: int64Of(allocated, i), modified: int64Of(modified, i)}
				acc.add(it, split.SizeBounds)
			}
			return nil
		},
		Merge: merge,
	}
}

// checkLocal refuses a batch of local rows the view would refuse as
// entries: a column absent or of another type, a kind swath does not
// write, a negative size.
func checkLocal(kind, size, allocated, modified seekparquet.ColumnView) error {
	if err := present(kind, "kind", seekparquet.KindByteArray); err != nil {
		return err
	}
	if err := present(modified, "last_modified", seekparquet.KindInt64); err != nil {
		return err
	}
	for i := range kind.Len() {
		if _, ok := localKinds[string(kind.Bytes(i))]; kind.IsNull(i) || !ok {
			return &reader.SemanticError{Kind: reader.KindRow, Column: "kind", Err: errors.New("null, or not a kind swath writes")}
		}
	}
	if err := sizes(size, "size"); err != nil {
		return err
	}
	return sizes(allocated, "allocated_bytes")
}

// int64Of is row i's value of a checked int64 column, absent for a null.
func int64Of(c seekparquet.ColumnView, i int) optional {
	if c.IsNull(i) {
		return optional{}
	}
	return optional{v: c.Int64(i), ok: true}
}

// zeroTotals are the totals of no counted entry.
func zeroTotals() *listing.Totals {
	return &listing.Totals{Bytes: new(int64), AllocatedBytes: new(int64)}
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

// established reports whether both of span's boundaries are known.
func established(span seekparquet.RowSpan) bool {
	_, start := span.Start.Ordinal()
	_, end := span.End.Ordinal()
	return start && end
}

// errUnknownSpan reports a span SeekParquet did not establish.
var errUnknownSpan = wrap(listing.ErrUnsupported, "a selection's rows are not established")
