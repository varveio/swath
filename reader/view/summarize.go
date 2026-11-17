// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"errors"
	"slices"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader/listing"
)

// errRoot is the Reason of a subtree target at the root, which is no
// entry: an inventory has no row for it.
var errRoot = errors.New("view: the root has no entry")

// errUnreached is the Reason of a target whose own entry a bound of the
// request kept the call from reading.
var errUnreached = errors.New("view: a bound of the request kept the call from reading the target's entry")

// Summarize counts the entries of opts.Kinds at or under each target and
// sums their sizes, space on disk and newest change, one TargetSummary per
// target, in order. An entry counts toward every target that holds it. A
// target's State and Self are what Lookup answers for its key; the root's,
// a subtree target with an empty key, is unknown. A target outside the
// inventory's scope is unknown with ErrOutOfScope as its Reason, and
// counts nothing.
//
// The call reads in two phases, both within opts.Budget. First the
// entries at the targets' keys, in target order, with those on the way to
// a local key when the inventory proves absence: an exact target's totals
// are those of its own entry. Then the subtrees' rows, in one aggregation
// through SeekParquet, every counted key's order checked. MaxEntries
// bounds the rows of both phases together, each counted once however many
// targets hold it; MaxValueBytes the value pages both read, the first
// phase's priced before it reads them and the aggregation given what
// remains; MaxDuration the whole call. A target whose own entry the
// budget does not reach is unknown, with a Reason that says so, and
// counts nothing; a subtree whose rows it does not reach counts nothing.
// Either has one Unread, the whole target's key interval, of class
// UnreadBudget, and Evidence.Stop names the bound. A counted entry
// without a size or a time, and a sum that overflows, make that total
// nil.
func (inv *Inventory) Summarize(ctx context.Context, targets []listing.Target, opts listing.SummaryOptions) ([]listing.TargetSummary, listing.Evidence, error) {
	sel, err := inv.checkSummary(targets, opts)
	if err != nil {
		return nil, listing.Evidence{}, err
	}

	op, cancel := withBudget(ctx, opts.Budget.MaxDuration)
	defer cancel()
	s := newSummary(inv, sel, targets, opts.Budget)
	s.outside()
	if err := s.locate(op); err != nil {
		return s.fail(ctx, op, err)
	}
	if err := s.readOwn(op); err != nil {
		return s.fail(ctx, op, err)
	}
	if err := s.count(op); err != nil {
		return s.fail(ctx, op, err)
	}
	return s.out, inv.evidence(s.stop), nil
}

// checkSummary refuses targets and options of the wrong form, and returns
// the kinds the options count.
func (inv *Inventory) checkSummary(targets []listing.Target, opts listing.SummaryOptions) (selection, error) {
	if err := checkBudget(opts.Budget); err != nil {
		return selection{}, err
	}
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

// summary is one Summarize call: its targets, where their rows lie, what
// the budget has admitted, their summaries, and the bound that stopped
// it.
type summary struct {
	inv     *Inventory
	sel     selection
	targets []listing.Target
	budget  listing.Budget
	out     []listing.TargetSummary

	// own holds the row spans of the names of each target's key that hold
	// a row, and under each counted subtree's rows.
	own   [][]seekparquet.RowSpan
	under []seekparquet.RowSpan

	// rows is the entry budget both phases share, and spent the value
	// bytes of the first phase's read.
	rows  rowBudget
	spent int64

	// known marks the targets whose State is set, and done those whose
	// totals are.
	known []bool
	done  []bool
	stop  listing.StopReason
}

func newSummary(inv *Inventory, sel selection, targets []listing.Target, budget listing.Budget) *summary {
	n := len(targets)
	return &summary{
		inv:     inv,
		sel:     sel,
		targets: targets,
		budget:  budget,
		out:     make([]listing.TargetSummary, n),
		own:     make([][]seekparquet.RowSpan, n),
		under:   make([]seekparquet.RowSpan, n),
		rows:    rowBudget{max: budget.MaxEntries},
		known:   make([]bool, n),
		done:    make([]bool, n),
		stop:    listing.StopCompleted,
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

// readOwn reads the entries at the targets' keys, admitting each
// target's in order while the entry and value budgets hold them, and sets
// the states of those it reads and an exact target's totals. A target it
// does not admit is unknown and unread.
func (s *summary) readOwn(ctx context.Context) error {
	var admitted []int
	var spans []seekparquet.RowSpan
	var priced *seekparquet.Plan
	for i := range s.targets {
		if s.known[i] {
			continue
		}
		trial := append(slices.Clip(spans), s.own[i]...)
		plan, stop, err := s.price(ctx, trial, len(s.own[i]) > 0)
		if err != nil {
			return err
		}
		if stop == listing.StopCompleted {
			fits, err := s.rows.admit(s.own[i])
			if err != nil {
				return err
			}
			if !fits {
				stop = listing.StopEntryBudget
			}
		}
		if stop != listing.StopCompleted {
			s.unreached(i, stop)
			continue
		}
		admitted, spans = append(admitted, i), trial
		if plan != nil {
			priced = plan
		}
	}

	found, err := s.read(ctx, spans, priced)
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

// price plans the first phase's read of spans when the value budget is
// set and the target adds rows to it: StopValueBudget when their
// projected pages, each counted once, do not fit the budget or cannot be
// priced.
func (s *summary) price(ctx context.Context, spans []seekparquet.RowSpan, adds bool) (*seekparquet.Plan, listing.StopReason, error) {
	if s.budget.MaxValueBytes == 0 || !adds {
		return nil, listing.StopCompleted, nil
	}
	plan, err := s.inv.sp.PlanRows(ctx, spans, s.inv.rows.columns, nil)
	if err != nil {
		return nil, "", err
	}
	if cost, exact := plan.ProjectedBytes(); !exact || cost > s.budget.MaxValueBytes {
		return nil, listing.StopValueBudget, nil
	}
	return plan, listing.StopCompleted, nil
}

// read reads the entries in spans, through plan when price planned them,
// and records the value bytes the read is charged.
func (s *summary) read(ctx context.Context, spans []seekparquet.RowSpan, plan *seekparquet.Plan) (map[string]*listing.Entry, error) {
	if len(spans) == 0 {
		return map[string]*listing.Entry{}, nil
	}
	if plan == nil {
		var err error
		if plan, err = s.inv.sp.PlanRows(ctx, spans, s.inv.rows.columns, nil); err != nil {
			return nil, err
		}
	}
	s.spent, _ = plan.ProjectedBytes()
	return s.inv.read(ctx, plan)
}

// count sets the totals of the subtrees still open: admitted in order
// while the entry budget holds their rows, then counted in one
// aggregation within the value budget the first phase left.
func (s *summary) count(ctx context.Context) error {
	var subtrees []int
	var spans []seekparquet.RowSpan
	for i := range s.targets {
		switch {
		case s.done[i]:
		case !s.counted(i):
			s.out[i].Totals, s.done[i] = *zeroTotals(), true
		default:
			fits, err := s.rows.admit([]seekparquet.RowSpan{s.under[i]})
			if err != nil {
				return err
			}
			if !fits {
				s.unread(i, listing.StopEntryBudget)
				continue
			}
			subtrees, spans = append(subtrees, i), append(spans, s.under[i])
		}
	}
	if len(spans) == 0 {
		return nil
	}

	values := int64(-1)
	if s.budget.MaxValueBytes > 0 {
		values = max(0, s.budget.MaxValueBytes-s.spent)
	}
	totals, err := s.inv.aggregate(ctx, spans, s.sel, values, true)
	if err != nil {
		return err
	}
	for j, i := range subtrees {
		if totals[j] == nil {
			s.unread(i, listing.StopValueBudget)
			continue
		}
		s.out[i].Totals, s.done[i] = *totals[j], true
	}
	return nil
}

// unreached ends target i unknown and unread, by the bound stop: the call
// did not read its own entry.
func (s *summary) unreached(i int, stop listing.StopReason) {
	s.out[i].State, s.out[i].Self, s.out[i].Reason = listing.LookupUnknown, nil, errUnreached
	s.known[i] = true
	s.unread(i, stop)
}

// unread ends target i unread, by the bound stop, which becomes the
// call's unless a bound applied before it stopped the call too.
func (s *summary) unread(i int, stop listing.StopReason) {
	s.out[i].Totals, s.done[i] = *zeroTotals(), true
	s.out[i].Unread = []listing.Unread{{Scope: listing.UnreadInterval, Class: listing.UnreadBudget}}
	if stopRank(stop) > stopRank(s.stop) {
		s.stop = stop
	}
}

// stopRank orders the bounds that stop a Summarize by when they apply:
// the time budget ends it, the entry budget admits each read, and the
// value budget prices it.
func stopRank(stop listing.StopReason) int {
	switch stop {
	case listing.StopTimeBudget:
		return 3
	case listing.StopEntryBudget:
		return 2
	case listing.StopValueBudget:
		return 1
	}
	return 0
}

// fail ends the call on err: with ctx's error when ctx ended; at the time
// budget, with every target not yet answered unread, and unknown when its
// own entry was not read; else with err.
func (s *summary) fail(ctx, op context.Context, err error) ([]listing.TargetSummary, listing.Evidence, error) {
	if ctx.Err() != nil {
		return nil, listing.Evidence{}, ctx.Err()
	}
	if !timeUp(ctx, op) {
		return nil, listing.Evidence{}, err
	}
	for i := range s.out {
		switch {
		case !s.known[i]:
			s.unreached(i, listing.StopTimeBudget)
		case !s.done[i]:
			s.unread(i, listing.StopTimeBudget)
		}
	}
	return s.out, s.inv.evidence(s.stop), nil
}
