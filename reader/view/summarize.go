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
// counts nothing.
//
// An exact target's totals are those of its own entry. A subtree's are
// aggregated through SeekParquet, every counted key's order checked, all
// in one aggregation: opts.Budget.MaxEntries bounds the rows it counts,
// each counted once however many targets hold it, MaxValueBytes the value
// pages it reads, and MaxDuration the whole call. A subtree the budget
// does not reach counts nothing and has one Unread, the whole target's
// key interval, of class UnreadBudget; Evidence.Stop names the bound. A
// counted entry without a size or a time, and a sum that overflows, make
// that total nil.
func (inv *Inventory) Summarize(ctx context.Context, targets []listing.Target, opts listing.SummaryOptions) ([]listing.TargetSummary, listing.Evidence, error) {
	sel, err := inv.checkSummary(targets, opts)
	if err != nil {
		return nil, listing.Evidence{}, err
	}

	op, cancel := withBudget(ctx, opts.Budget.MaxDuration)
	defer cancel()
	s := summary{inv: inv, sel: sel, targets: targets, out: make([]listing.TargetSummary, len(targets)), stop: listing.StopCompleted}
	if err := s.resolve(op); err != nil {
		return s.fail(ctx, op, err)
	}
	if err := s.count(op, opts.Budget); err != nil {
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

// summary is one Summarize call: its targets, their summaries, and the
// bound that stopped it.
type summary struct {
	inv     *Inventory
	sel     selection
	targets []listing.Target
	out     []listing.TargetSummary
	// done marks the targets whose totals are set.
	done []bool
	stop listing.StopReason
}

// resolve answers the targets outside the scope, and sets every other
// target's state and entry from one read of the entries at their keys.
func (s *summary) resolve(ctx context.Context) error {
	s.done = make([]bool, len(s.targets))
	var keys [][]byte
	for i, t := range s.targets {
		switch {
		case !s.inv.inScope(t.Key):
			s.out[i].Reason, s.out[i].Totals, s.done[i] = errOutOfScope, *zeroTotals(), true
		case len(t.Key) == 0:
			s.out[i].Reason = errRoot
		default:
			keys = append(keys, s.inv.names(t.Key)...)
		}
	}
	found, err := s.inv.entries(ctx, keys)
	if err != nil {
		return err
	}

	for i, t := range s.targets {
		if !s.done[i] && len(t.Key) > 0 {
			s.out[i].State, s.out[i].Self = s.inv.resolve(t.Key, found)
		}
	}
	return nil
}

// count sets the totals of the targets inside the scope: an exact
// target's from its entry, the subtrees' from one aggregation within the
// budget.
func (s *summary) count(ctx context.Context, budget listing.Budget) error {
	var subtrees []int
	var selections []seekparquet.Selection
	for i, t := range s.targets {
		switch {
		case s.done[i]:
		case !t.Subtree:
			s.out[i].Totals, s.done[i] = *entryTotals(s.out[i].Self, s.sel), true
		case !s.sel.countsAny(s.inv.rows):
			s.out[i].Totals, s.done[i] = *zeroTotals(), true
		default:
			subtrees = append(subtrees, i)
			selections = append(selections, seekparquet.Selection{Span: seekparquet.PrefixSpan(t.Key)})
		}
	}
	if len(subtrees) == 0 {
		return nil
	}

	plan, err := s.inv.sp.Plan(ctx, selections, nil, nil)
	if err != nil {
		return err
	}
	rows := rowBudget{max: budget.MaxEntries}
	var admitted []int
	var spans []seekparquet.RowSpan
	for j, span := range plan.InputRowSpans() {
		fits, err := rows.admit(span)
		if err != nil {
			return err
		}
		if !fits {
			s.unread(subtrees[j], listing.StopEntryBudget)
			continue
		}
		admitted = append(admitted, subtrees[j])
		spans = append(spans, span)
	}

	totals, err := s.inv.aggregate(ctx, spans, s.sel, valueBudget(budget), true)
	if err != nil {
		return err
	}
	for j, i := range admitted {
		if totals[j] == nil {
			s.unread(i, listing.StopValueBudget)
			continue
		}
		s.out[i].Totals, s.done[i] = *totals[j], true
	}
	return nil
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
// the time budget ends it, the entry budget admits the subtrees, and the
// value budget reads them.
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
// budget, with every target not yet counted unread; else with err.
func (s *summary) fail(ctx, op context.Context, err error) ([]listing.TargetSummary, listing.Evidence, error) {
	if ctx.Err() != nil {
		return nil, listing.Evidence{}, ctx.Err()
	}
	if !timeUp(ctx, op) {
		return nil, listing.Evidence{}, err
	}
	for i := range s.out {
		if !s.done[i] {
			s.unread(i, listing.StopTimeBudget)
		}
	}
	return s.out, s.inv.evidence(s.stop), nil
}
