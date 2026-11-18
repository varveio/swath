// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"context"
	"errors"
	"time"

	"github.com/varveio/swath/reader/listing"
)

// errRoot is the Reason of a subtree target at the root, which is no
// entry: no object has an empty key, and a local source's root is the
// directory it holds.
var errRoot = errors.New("swath: the root has no entry")

// Summarize counts the entries of opts.Kinds at or under each target and
// sums their sizes and newest change, one TargetSummary per target, in
// order, as package listing states. Each target is answered on its own:
// its State, Self and Reason by a Lookup of its key (the root's, the empty
// key's, is unknown: no entry), whose Self an exact target counts; a
// subtree target's entries at and under its key by one Walk of it, so
// nested and repeated targets are each walked and counted in full.
//
// A subtree target's Unread counts what its walk handed on as Unread, and
// it is Complete when its State is known (the root's excepted) and its
// walk handed on none; an exact target is Complete when its State is
// known. A subtree whose Lookup left it unknown is still walked, and when
// that walk fails the target counts what it read, incomplete, rather than
// failing the call; so is one shown absent whose walk finds no local
// directory (ErrNotFound), which holds nothing. A walk refused its
// subtree (ErrAuth) or stopped at another device (ErrOtherDevice) counts
// one Unread, as a walk above the target hands it on, and the call goes
// on. Any other walk's failure, and a lookup's, is Summarize's error;
// when ctx ends, Summarize returns its error. A counted entry without a
// size or a time, and a sum that overflows, make that total nil.
func (s *Source) Summarize(ctx context.Context, targets []listing.Target, opts listing.SummaryOptions) ([]listing.TargetSummary, listing.Evidence, error) {
	counted, err := s.checkSummary(targets, opts)
	if err != nil {
		return nil, listing.Evidence{}, err
	}

	started := time.Now()
	out := make([]listing.TargetSummary, len(targets))
	for i, t := range targets {
		if out[i], err = s.target(ctx, t, counted); err != nil {
			return nil, listing.Evidence{}, err
		}
	}
	return out, evidence(started, time.Now(), listing.StopCompleted), nil
}

// checkSummary refuses targets and options of the wrong form, before any
// request, and returns the kinds counted: counted[k] for each.
func (s *Source) checkSummary(targets []listing.Target, opts listing.SummaryOptions) ([]bool, error) {
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

// target answers one target: its own entry by a Lookup, and a subtree's
// entries under its key by a Walk.
func (s *Source) target(ctx context.Context, t listing.Target, counted []bool) (listing.TargetSummary, error) {
	var ts listing.TargetSummary
	if len(t.Key) == 0 {
		ts.State, ts.Reason = listing.LookupUnknown, errRoot
	} else {
		r, err := s.Lookup(ctx, t.Key)
		if err != nil {
			return listing.TargetSummary{}, err
		}
		ts.State, ts.Self, ts.Reason = r.State, r.Entry, r.Reason
	}

	var tl tally
	ts.Complete = ts.State != listing.LookupUnknown || len(t.Key) == 0
	if !t.Subtree {
		if ts.Self != nil {
			tl.add(ts.Self, counted)
		}
		ts.Totals = tl.totals()
		return ts, nil
	}

	res, err := s.Walk(ctx, listing.WalkRequest{Prefix: t.Key}, func(b listing.WalkBatch) error {
		for i := range b.Entries {
			tl.add(&b.Entries[i], counted)
		}
		return nil
	})
	switch {
	case err == nil:
	case ctx.Err() != nil:
		return listing.TargetSummary{}, ctx.Err()
	case ts.State == listing.LookupUnknown && len(t.Key) > 0: // incomplete, and the Lookup's Reason says why
	case ts.State == listing.LookupAbsent && errors.Is(err, listing.ErrNotFound): // nothing there
	case errors.Is(err, listing.ErrAuth) || errors.Is(err, listing.ErrOtherDevice): // a place it could not read, as a walk above it hands on
		res.Unread++
	default:
		return listing.TargetSummary{}, err
	}
	ts.Totals, ts.Unread = tl.totals(), res.Unread
	ts.Complete = ts.Complete && res.Unread == 0
	return ts, nil
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
