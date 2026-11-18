// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"errors"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader/listing"
)

// errRoot is the Reason of a subtree target at the root, which is no
// entry: an inventory has no row for it.
var errRoot = errors.New("view: the root has no entry")

// Summarize counts the entries of opts.Kinds at or under each target and
// sums their sizes, space on disk and newest change, one TargetSummary per
// target, in order. An entry counts toward every target that holds it. A
// target's State and Self are what Lookup answers for its key; the root's,
// a subtree target with an empty key, is unknown. A target outside the
// inventory's scope is unknown with ErrOutOfScope as its Reason, and
// counts nothing. A target is Complete when the inventory proves absence
// (its Unlocated is 0) and its State is known, the root's excepted; its
// Unread is always 0, since an inventory's gaps are unlocated.
//
// The call reads in two phases. First the entries at the targets' keys,
// with those on the way to a local key when the inventory proves absence:
// an exact target's totals are those of its own entry. Then the subtrees'
// rows, in one aggregation through SeekParquet, every counted key's order
// checked. A counted entry without a size or a time, and a sum that
// overflows, make that total nil.
func (inv *Inventory) Summarize(ctx context.Context, targets []listing.Target, opts listing.SummaryOptions) ([]listing.TargetSummary, listing.Evidence, error) {
	sel, err := inv.checkSummary(targets, opts)
	if err != nil {
		return nil, listing.Evidence{}, err
	}

	s := newSummary(inv, sel, targets)
	s.outside()
	if err := s.locate(ctx); err != nil {
		return nil, listing.Evidence{}, failed(ctx, err)
	}
	if err := s.readOwn(ctx); err != nil {
		return nil, listing.Evidence{}, failed(ctx, err)
	}
	if err := s.count(ctx); err != nil {
		return nil, listing.Evidence{}, failed(ctx, err)
	}
	s.complete()
	return s.out, inv.evidence(listing.StopCompleted), nil
}

// checkSummary refuses targets of the wrong form, and returns the kinds
// the options count.
func (inv *Inventory) checkSummary(targets []listing.Target, opts listing.SummaryOptions) (selection, error) {
	for _, t := range targets {
		check := inv.checkKey
		if t.Subtree {
			check = inv.checkPrefix
		}
		if err := check(t.Key); err != nil {
			return selection{}, err
		}
	}
	return selectionOf(opts.Kinds)
}

// summary is one Summarize call: its targets, where their rows lie, and
// their summaries.
type summary struct {
	inv     *Inventory
	sel     selection
	targets []listing.Target
	out     []listing.TargetSummary

	// own holds the row spans of the names of each target's key that hold
	// a row, and under each counted subtree's rows.
	own   [][]seekparquet.RowSpan
	under []seekparquet.RowSpan

	// known marks the targets whose State is set, and done those whose
	// totals are.
	known []bool
	done  []bool
}

func newSummary(inv *Inventory, sel selection, targets []listing.Target) *summary {
	n := len(targets)
	return &summary{
		inv:     inv,
		sel:     sel,
		targets: targets,
		out:     make([]listing.TargetSummary, n),
		own:     make([][]seekparquet.RowSpan, n),
		under:   make([]seekparquet.RowSpan, n),
		known:   make([]bool, n),
		done:    make([]bool, n),
	}
}

// outside answers what needs no read: the targets outside the scope, and
// the root's state.
func (s *summary) outside() {
	for i, t := range s.targets {
		switch {
		case !s.inv.inScope(t.Key):
			s.out[i].Reason, s.out[i].Totals = errOutOfScope, *zeroTotals()
			s.known[i], s.done[i] = true, true
		case len(t.Key) == 0:
			s.out[i].Reason, s.known[i] = errRoot, true
		}
	}
}

// counted reports whether target i counts a subtree's rows.
func (s *summary) counted(i int) bool {
	return s.targets[i].Subtree && s.sel.countsAny(s.inv.rows)
}

// locate finds the rows of every target still open, by their keys, in
// one plan that reads no value: the names of its key, and a counted
// subtree's rows.
func (s *summary) locate(ctx context.Context) error {
	type slot struct {
		target int
		under  bool
	}
	var selections []seekparquet.Selection
	var slots []slot
	for i, t := range s.targets {
		if s.done[i] {
			continue
		}
		if !s.known[i] {
			for _, name := range s.inv.keys(t.Key) {
				selections = append(selections, seekparquet.Selection{Span: seekparquet.KeySpan(name)})
				slots = append(slots, slot{target: i})
			}
		}
		if s.counted(i) {
			selections = append(selections, seekparquet.Selection{Span: seekparquet.PrefixSpan(t.Key)})
			slots = append(slots, slot{target: i, under: true})
		}
	}
	if len(selections) == 0 {
		return nil
	}

	plan, err := s.inv.sp.Plan(ctx, selections, nil, nil)
	if err != nil {
		return err
	}
	for j, span := range plan.InputRowSpans() {
		at := slots[j]
		switch {
		case !established(span):
			return errUnknownSpan
		case at.under:
			s.under[at.target] = span
		case !empty(span):
			s.own[at.target] = append(s.own[at.target], span)
		}
	}
	return nil
}

// empty reports whether an established span holds no row.
func empty(span seekparquet.RowSpan) bool {
	lo, _ := span.Start.Ordinal()
	hi, _ := span.End.Ordinal()
	return lo == hi
}

// readOwn reads the entries at the targets' keys, and sets the states of
// those it reads and an exact target's totals.
func (s *summary) readOwn(ctx context.Context) error {
	var admitted []int
	var spans []seekparquet.RowSpan
	for i := range s.targets {
		if s.known[i] {
			continue
		}
		admitted, spans = append(admitted, i), append(spans, s.own[i]...)
	}

	found, err := s.read(ctx, spans)
	if err != nil {
		return err
	}
	for _, i := range admitted {
		t := s.targets[i]
		s.out[i].State, s.out[i].Self, s.out[i].Reason = s.inv.resolve(t.Key, found)
		s.known[i] = true
		if !t.Subtree {
			s.out[i].Totals, s.done[i] = *entryTotals(s.out[i].Self, s.sel), true
		}
	}
	return nil
}

// read reads the entries in spans.
func (s *summary) read(ctx context.Context, spans []seekparquet.RowSpan) (map[string]*listing.Entry, error) {
	if len(spans) == 0 {
		return map[string]*listing.Entry{}, nil
	}
	plan, err := s.inv.sp.PlanRows(ctx, spans, s.inv.rows.columns, nil)
	if err != nil {
		return nil, err
	}
	return s.inv.read(ctx, plan)
}

// count sets the totals of the subtrees still open, counted in one
// aggregation.
func (s *summary) count(ctx context.Context) error {
	var subtrees []int
	var spans []seekparquet.RowSpan
	for i := range s.targets {
		switch {
		case s.done[i]:
		case !s.counted(i):
			s.out[i].Totals, s.done[i] = *zeroTotals(), true
		default:
			subtrees, spans = append(subtrees, i), append(spans, s.under[i])
		}
	}
	if len(spans) == 0 {
		return nil
	}

	totals, err := s.inv.aggregate(ctx, spans, s.sel, true)
	if err != nil {
		return err
	}
	for j, i := range subtrees {
		s.out[i].Totals, s.done[i] = *totals[j], true
	}
	return nil
}

// complete marks the targets whose totals cover everything at or under
// them: the inventory proves absence, the target is in its scope, and its
// state is known, the root's excepted.
func (s *summary) complete() {
	for i, t := range s.targets {
		known := s.out[i].State != listing.LookupUnknown || len(t.Key) == 0
		s.out[i].Complete = s.inv.provable && s.inv.inScope(t.Key) && known
	}
}
