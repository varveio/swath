// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/varveio/swath/reader/listing"
)

var (
	// errRoot is the Reason of a subtree target at the root, which is no
	// entry: no object has an empty key.
	errRoot = errors.New("swath: the root has no entry")
	// errUnreached is the Reason of a target whose own entry a bound of
	// the request kept the call from reading.
	errUnreached = fmt.Errorf("%w: a bound of the request kept the call from reading the target's entry", listing.ErrBudgetExceeded)
)

// Summarize counts the entries of opts.Kinds at or under each target and
// sums their sizes and newest change, one TargetSummary per target, in
// order, as package listing states. Each target is answered on its own,
// within the whole of opts.Budget: an exact target by a Lookup of its key,
// whose State, Self and Reason it takes and whose entry is its totals; a
// subtree target by one Walk of its key, whose entries it counts, so
// nested and repeated targets are each walked and counted in full.
//
// A subtree target is present when its walk handed on an entry at exactly
// its key, absent when it did not and the walk read everything, and
// unknown otherwise; the root's, the empty key's, is unknown. Its Unread
// are its walk's, and its totals cover only what the walk read. A
// target's Stop is the bound that stopped its walk or lookup, empty for an
// exact target the lookup left unknown, and Evidence.Stop the first such
// bound. A counted entry without a size or a
// time, and a sum that overflows, make that total nil. A walk's or a
// lookup's failure is Summarize's error; when ctx ends, Summarize returns
// its error.
func (s *Source) Summarize(ctx context.Context, targets []listing.Target, opts listing.SummaryOptions) ([]listing.TargetSummary, listing.Evidence, error) {
	counted, err := s.checkSummary(targets, opts)
	if err != nil {
		return nil, listing.Evidence{}, err
	}

	started := time.Now()
	out := make([]listing.TargetSummary, len(targets))
	stop, lostAny := listing.StopCompleted, false
	for i, t := range targets {
		lost := false
		if t.Subtree {
			out[i], lost, err = s.subtree(ctx, t.Key, opts.Budget, counted)
		} else {
			out[i], err = s.exact(ctx, t.Key, opts.Budget, counted)
		}
		if err != nil {
			return nil, listing.Evidence{}, err
		}
		if stop == listing.StopCompleted && out[i].Stop != "" {
			stop = out[i].Stop
		}
		lostAny = lostAny || lost
	}

	ev := evidence(started, time.Now(), stop)
	if lostAny {
		ev.Unlocated = nil
	}
	return out, ev, nil
}

// checkSummary refuses targets and options of the wrong form, before any
// request, and returns the kinds counted: counted[k] for each.
func (s *Source) checkSummary(targets []listing.Target, opts listing.SummaryOptions) ([]bool, error) {
	b := opts.Budget
	if b.MaxEntries < 0 || b.MaxDuration < 0 {
		return nil, invalid("a negative bound")
	}
	for _, t := range targets {
		check := s.checkKey
		if t.Subtree {
			check = func(key []byte) error { return s.checkName("target's key", key) }
		}
		if err := check(t.Key); err != nil {
			return nil, err
		}
	}

	kinds := opts.Kinds
	if len(kinds) == 0 {
		kinds = []listing.Kind{listing.KindFile, listing.KindObject}
	}
	counted := make([]bool, listing.KindUnknown+1)
	for _, k := range kinds {
		if k == 0 || k > listing.KindUnknown {
			return nil, invalid("%v is no kind", k)
		}
		counted[k] = true
	}
	counted[listing.KindPrefix] = false // never counted
	return counted, nil
}

// exact answers an exact target by a Lookup of key within budget's time:
// unknown and unread whole when the time ends first; an unknown answer's
// Stop is empty.
func (s *Source) exact(ctx context.Context, key []byte, budget listing.Budget, counted []bool) (listing.TargetSummary, error) {
	op, cancel := withBudget(ctx, budget.MaxDuration)
	defer cancel()
	r, err := s.Lookup(op, key)
	switch {
	case err != nil && timeUp(ctx, op):
		return listing.TargetSummary{State: listing.LookupUnknown, Reason: errUnreached, Totals: zeroTotals(),
			Unread: []listing.Unread{{Scope: listing.UnreadInterval, Class: listing.UnreadBudget}}, Stop: listing.StopTimeBudget}, nil
	case err != nil:
		return listing.TargetSummary{}, err
	}

	var tl tally
	stop := listing.StopCompleted
	switch r.State {
	case listing.LookupPresent:
		tl.add(r.Entry, counted)
	case listing.LookupUnknown:
		stop = "" // not read, and no bound is why: never complete
	}
	return listing.TargetSummary{State: r.State, Self: r.Entry, Reason: r.Reason, Totals: tl.totals(), Stop: stop}, nil
}

// subtree answers a subtree target by one Walk of key within budget, and
// reports whether the walk dropped extents it did not read.
func (s *Source) subtree(ctx context.Context, key []byte, budget listing.Budget, counted []bool) (listing.TargetSummary, bool, error) {
	var tl tally
	var self *listing.Entry
	var unread []listing.Unread
	res, err := s.Walk(ctx, listing.WalkRequest{Prefix: key, Budget: budget}, func(b listing.WalkBatch) error {
		for i := range b.Entries {
			e := &b.Entries[i]
			tl.add(e, counted)
			if len(e.Key) == len(key) && e.Kind != listing.KindPrefix {
				self = cloneEntry(e)
			}
		}
		for _, u := range b.Unread {
			unread = append(unread, listing.Unread{Key: bytes.Clone(u.Key), Hi: bytes.Clone(u.Hi), Scope: u.Scope, Class: u.Class})
		}
		return nil
	})
	if err != nil {
		return listing.TargetSummary{}, false, err
	}

	lost := res.Evidence.Unlocated == nil
	ts := listing.TargetSummary{Self: self, Totals: tl.totals(), Unread: unread, Stop: res.Evidence.Stop}
	switch {
	case len(key) == 0:
		ts.State, ts.Reason = listing.LookupUnknown, errRoot
	case self != nil:
		ts.State = listing.LookupPresent
	case len(unread) > 0: // a walk that dropped extents handed on those it kept
		ts.State, ts.Reason = listing.LookupUnknown, unreadReason(unread[0].Class)
	default:
		ts.State = listing.LookupAbsent
	}
	return ts, lost, nil
}

// unreadReason is the Reason of a subtree target whose walk left an
// extent of class c unread, which may hold the target's own entry.
func unreadReason(c listing.UnreadClass) error {
	switch c {
	case listing.UnreadBudget:
		return errUnreached
	case listing.UnreadPermission:
		return fmt.Errorf("%w: the target was not read: access was refused", listing.ErrAuth)
	}
	return fmt.Errorf("swath: the target was not read: %s", c)
}

// tally sums the counted entries of a set. A sum is unknown once an entry
// lacks its value or the sum overflows.
type tally struct {
	entries              int64
	bytes, allocated     int64
	noBytes, noAllocated bool
	newest               time.Time
	noTime               bool
}

// add counts e when its kind is counted.
func (t *tally) add(e *listing.Entry, counts []bool) {
	if int(e.Kind) >= len(counts) || !counts[e.Kind] {
		return
	}
	t.entries++
	t.bytes, t.noBytes = plus(t.bytes, t.noBytes, e.Size)
	t.allocated, t.noAllocated = plus(t.allocated, t.noAllocated, e.AllocatedBytes)
	switch {
	case e.LastModified == nil:
		t.noTime = true
	case e.LastModified.After(t.newest):
		t.newest = *e.LastModified
	}
}

// plus is sum + v, unknown when the sum is, when v is nil, or when the
// addition overflows.
func plus(sum int64, unknown bool, v *int64) (int64, bool) {
	if unknown || v == nil {
		return 0, true
	}
	s := sum + *v
	if *v > 0 && s < sum || *v < 0 && s > sum {
		return 0, true
	}
	return s, false
}

func (t *tally) totals() listing.Totals {
	out := listing.Totals{Entries: t.entries}
	if !t.noBytes {
		out.Bytes = &t.bytes
	}
	if !t.noAllocated {
		out.AllocatedBytes = &t.allocated
	}
	if t.entries > 0 && !t.noTime {
		out.Newest = &t.newest
	}
	return out
}

// zeroTotals are the totals of no counted entry.
func zeroTotals() listing.Totals {
	var t tally
	return t.totals()
}

// cloneEntry is a copy of e that shares nothing with it.
func cloneEntry(e *listing.Entry) *listing.Entry {
	c := *e
	c.Key = bytes.Clone(e.Key)
	c.Size = clonePtr(e.Size)
	c.AllocatedBytes = clonePtr(e.AllocatedBytes)
	c.LastModified = clonePtr(e.LastModified)
	c.StorageClass = clonePtr(e.StorageClass)
	c.Device = clonePtr(e.Device)
	c.Inode = clonePtr(e.Inode)
	c.Links = clonePtr(e.Links)
	return &c
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
