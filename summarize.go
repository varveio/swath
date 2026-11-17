// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package swath

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/reader/listing"
)

var (
	// errRoot is the Reason of a subtree target at the root, which is no
	// entry: no object has an empty key.
	errRoot = errors.New("swath: the root has no entry")
	// errUnreached is the Reason of a target whose own entry a bound of
	// the request kept the call from reading.
	errUnreached = fmt.Errorf("%w: a bound of the request kept the call from reading the target's entry", listing.ErrBudgetExceeded)
	// errUnlocated is the Reason of a target whose own entry a walk did
	// not read, after it dropped extents it did not read: absence is not
	// shown.
	errUnlocated = fmt.Errorf("%w: the walk did not keep every extent it did not read", listing.ErrBudgetExceeded)
)

// Summarize counts the entries of opts.Kinds at or under each target and
// sums their sizes and newest change, one TargetSummary per target, in
// order, as package listing states. An entry counts toward every target
// that holds it, so nested targets are each counted in full.
//
// An exact target is a Lookup of its key: its State, Self and Reason are
// the lookup's, its totals those of its own entry. The subtree targets are
// walked (Walk): one walk for each that lies under no other, whose
// entries count toward it and toward every target nested in it. A subtree
// target's State is present when the walk read an object at exactly its
// key, absent when the walk read where it would be and found none, and
// unknown when an extent the walk did not read holds it, or when the walk
// dropped extents it did not read (a nil Evidence.Unlocated); the root's,
// the empty key's, is unknown.
//
// opts.Budget bounds the whole call: MaxEntries the entries the walks
// read and the objects the lookups find, MaxDuration its time. Every walk
// stops at what is left of them. A target the call stopped before
// reading is unknown, with listing.ErrBudgetExceeded as its Reason,
// counts nothing, and has one Unread, its whole key interval; a target a
// walk stopped inside counts what the walk read and has, as its Unread,
// the extents within it the walk did not read. Evidence.Stop names the
// first bound that stopped a walk or a lookup. A counted entry without a
// size or a time, and a sum that overflows, make that total nil. A walk's
// or a lookup's failure is Summarize's error; when ctx ends, Summarize
// returns its error.
func (s *Source) Summarize(ctx context.Context, targets []listing.Target, opts listing.SummaryOptions) ([]listing.TargetSummary, listing.Evidence, error) {
	counted, err := checkSummary(targets, opts)
	if err != nil {
		return nil, listing.Evidence{}, err
	}

	sum := &summary{src: s, targets: targets, counts: counted, budget: opts.Budget, started: time.Now(),
		out: make([]listing.TargetSummary, len(targets)), reason: listing.StopCompleted}
	if d := opts.Budget.MaxDuration; d > 0 {
		sum.deadline = sum.started.Add(d)
	}
	if err := sum.lookups(ctx); err != nil {
		return nil, listing.Evidence{}, err
	}
	if err := sum.walks(ctx); err != nil {
		return nil, listing.Evidence{}, err
	}

	ev := evidence(sum.started, time.Now(), sum.reason)
	if sum.lost {
		ev.Unlocated = nil
	}
	return sum.out, ev, nil
}

// checkSummary refuses targets and options of the wrong form, before any
// request, and returns the kinds counted: counted[k] for each.
func checkSummary(targets []listing.Target, opts listing.SummaryOptions) ([]bool, error) {
	b := opts.Budget
	if b.MaxEntries < 0 || b.MaxDuration < 0 {
		return nil, invalid("a negative bound")
	}
	for _, t := range targets {
		if !t.Subtree {
			if err := checkKey(t.Key); err != nil {
				return nil, err
			}
			continue
		}
		switch {
		case bytes.IndexByte(t.Key, 0) >= 0:
			return nil, invalid("a target's key holds a NUL byte")
		case !utf8.Valid(t.Key):
			return nil, invalid("a target's key is not UTF-8")
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

// summary is one Summarize call: its targets, what the budget has spent,
// and their answers.
type summary struct {
	src     *Source
	targets []listing.Target
	counts  []bool
	budget  listing.Budget
	out     []listing.TargetSummary

	// started is when the call began, deadline when its MaxDuration ends
	// it (zero for none), and used the entries its budget has counted.
	started  time.Time
	deadline time.Time
	used     int64

	// reason is the first bound that stopped a part of the call, and lost
	// says a walk dropped extents it did not read (Evidence.Unlocated).
	reason listing.StopReason
	lost   bool
}

// left is the budget what the call has spent leaves: StopCompleted while
// it holds, else the bound reached.
func (s *summary) left() (listing.Budget, listing.StopReason) {
	var b listing.Budget
	if s.budget.MaxEntries > 0 {
		if b.MaxEntries = s.budget.MaxEntries - s.used; b.MaxEntries <= 0 {
			return b, listing.StopEntryBudget
		}
	}
	if !s.deadline.IsZero() {
		if b.MaxDuration = time.Until(s.deadline); b.MaxDuration <= 0 {
			return b, listing.StopTimeBudget
		}
	}
	return b, listing.StopCompleted
}

// stopped records the bound that stopped a part of the call.
func (s *summary) stopped(reason listing.StopReason) {
	if s.reason == listing.StopCompleted {
		s.reason = reason
	}
}

// unreached ends target i unknown, unread whole and counting nothing: the
// bound reason kept the call from it.
func (s *summary) unreached(i int, reason listing.StopReason) {
	s.out[i] = listing.TargetSummary{State: listing.LookupUnknown, Reason: errUnreached, Totals: zeroTotals(),
		Unread: []listing.Unread{{Scope: listing.UnreadInterval, Class: listing.UnreadBudget}}}
	s.stopped(reason)
}

// lookups answers the exact targets, in order, each by a Lookup of its
// key within the budget.
func (s *summary) lookups(ctx context.Context) error {
	for i, t := range s.targets {
		if t.Subtree {
			continue
		}
		if _, reason := s.left(); reason != listing.StopCompleted {
			s.unreached(i, reason)
			continue
		}

		op, cancel := ctx, context.CancelFunc(func() {})
		if !s.deadline.IsZero() {
			op, cancel = context.WithDeadlineCause(ctx, s.deadline, errTimeBudget)
		}
		r, err := s.src.Lookup(op, t.Key)
		cancel()
		switch {
		case err != nil && timeUp(ctx, op):
			s.unreached(i, listing.StopTimeBudget)
			continue
		case err != nil:
			return err
		}

		var tl tally
		if r.State == listing.LookupPresent {
			s.used++
			tl.add(r.Entry, s.counts)
		}
		s.out[i] = listing.TargetSummary{State: r.State, Self: r.Entry, Reason: r.Reason, Totals: tl.totals()}
	}
	return nil
}

// walks answers the subtree targets: one walk for each that lies under no
// other, its entries counted toward it and toward every target nested in
// it.
func (s *summary) walks(ctx context.Context) error {
	var keys [][]byte
	for _, t := range s.targets {
		if t.Subtree {
			keys = append(keys, t.Key)
		}
	}
	slices.SortFunc(keys, bytes.Compare)
	keys = slices.CompactFunc(keys, bytes.Equal)

	for i, k := range keys {
		if i > 0 && outer(keys[:i], k) {
			continue // nested: counted by the walk of the target it lies under
		}
		if err := s.walk(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

// outer reports whether a key in sorted, the keys before k, is a prefix of
// k: k is nested in that target.
func outer(sorted [][]byte, k []byte) bool {
	for _, o := range sorted {
		if bytes.HasPrefix(k, o) {
			return true
		}
	}
	return false
}

// walk answers the subtree targets at or under prefix with one walk of it.
func (s *summary) walk(ctx context.Context, prefix []byte) error {
	in := s.nested(prefix)
	budget, reason := s.left()
	if reason != listing.StopCompleted {
		for _, i := range in.targets {
			s.unreached(i, reason)
		}
		return nil
	}

	var unread []listing.Unread
	res, err := s.src.Walk(ctx, listing.WalkRequest{Prefix: prefix, Budget: budget}, func(b listing.WalkBatch) error {
		for i := range b.Entries {
			in.add(&b.Entries[i], s.counts)
		}
		for _, u := range b.Unread {
			unread = append(unread, listing.Unread{Key: bytes.Clone(u.Key), Hi: bytes.Clone(u.Hi), Scope: u.Scope, Class: u.Class})
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.used += res.Entries
	if res.Evidence.Stop != listing.StopCompleted {
		s.stopped(res.Evidence.Stop)
	}
	lost := res.Evidence.Unlocated == nil
	if lost {
		s.lost = true
	}

	for j, i := range in.targets {
		s.out[i] = in.summary(j, s.targets[i].Key, unread, lost)
	}
	return nil
}

// nested is the subtree targets at or under one walk's prefix, matched to
// the entries it reads by the targets' keys: a target holds an entry when
// its key is the first n bytes of the entry's, for one of the lengths.
type nested struct {
	targets []int
	byKey   map[string][]int // the positions in targets of the targets with that key
	lengths []int            // the targets' key lengths, ascending, each once
	tallies []tally
	selves  []*listing.Entry
}

func (s *summary) nested(prefix []byte) *nested {
	n := &nested{byKey: map[string][]int{}}
	for i, t := range s.targets {
		if !t.Subtree || !bytes.HasPrefix(t.Key, prefix) {
			continue
		}
		n.byKey[string(t.Key)] = append(n.byKey[string(t.Key)], len(n.targets))
		n.targets = append(n.targets, i)
		n.lengths = append(n.lengths, len(t.Key))
	}
	slices.Sort(n.lengths)
	n.lengths = slices.Compact(n.lengths)
	n.tallies = make([]tally, len(n.targets))
	n.selves = make([]*listing.Entry, len(n.targets))
	return n
}

// add counts e toward every target that holds it, and keeps it as the
// Self of a target at exactly its key.
func (n *nested) add(e *listing.Entry, counts []bool) {
	for _, l := range n.lengths {
		if l > len(e.Key) {
			return
		}
		for _, j := range n.byKey[string(e.Key[:l])] {
			n.tallies[j].add(e, counts)
			if l == len(e.Key) && e.Kind != listing.KindPrefix {
				n.selves[j] = cloneEntry(e)
			}
		}
	}
}

// summary is target j's answer, key its key, from what the walk read and
// the extents it did not read, of which it dropped some when lost is set.
func (n *nested) summary(j int, key []byte, unread []listing.Unread, lost bool) listing.TargetSummary {
	ts := listing.TargetSummary{Totals: n.tallies[j].totals()}
	var own *listing.Unread
	for k := range unread {
		u, ok := clip(unread[k], key)
		if !ok {
			continue
		}
		ts.Unread = append(ts.Unread, u)
		if own == nil && holdsOwn(unread[k], key) {
			own = &unread[k]
		}
	}

	switch {
	case len(key) == 0:
		ts.State, ts.Reason = listing.LookupUnknown, errRoot
	case n.selves[j] != nil:
		ts.State, ts.Self = listing.LookupPresent, n.selves[j]
	case own != nil:
		ts.State, ts.Reason = listing.LookupUnknown, unreadReason(own.Class)
	case lost:
		ts.State, ts.Reason = listing.LookupUnknown, errUnlocated
	default:
		ts.State = listing.LookupAbsent
	}
	return ts
}

// clip is u within the target at or under key: ok false when u holds none
// of its keys. An interval's ends outside it become the target's own (an
// empty Key or Hi); an extent the target lies under becomes the target's
// own key, of the same scope for a subtree, and a subtree for a listing,
// whose children the call may have missed whole.
func clip(u listing.Unread, key []byte) (listing.Unread, bool) {
	c := listing.Unread{Scope: u.Scope, Class: u.Class}
	switch {
	case u.Scope == listing.UnreadInterval:
		end := prefixEnd(key)
		if len(u.Hi) > 0 && bytes.Compare(key, u.Hi) > 0 || end != nil && bytes.Compare(u.Key, end) >= 0 {
			return listing.Unread{}, false
		}
		if bytes.HasPrefix(u.Key, key) {
			c.Key = u.Key
		}
		if len(u.Hi) > 0 && (end == nil || bytes.Compare(u.Hi, end) < 0) {
			c.Hi = u.Hi
		}
	case bytes.HasPrefix(u.Key, key):
		c.Key = u.Key
	case bytes.HasPrefix(key, u.Key):
		c.Key, c.Scope = key, listing.UnreadSubtree
	default:
		return listing.Unread{}, false
	}
	return c, true
}

// holdsOwn reports whether u holds the entry at exactly key.
func holdsOwn(u listing.Unread, key []byte) bool {
	switch u.Scope {
	case listing.UnreadInterval:
		return bytes.Compare(u.Key, key) < 0 && (len(u.Hi) == 0 || bytes.Compare(key, u.Hi) <= 0)
	case listing.UnreadSubtree:
		return bytes.HasPrefix(key, u.Key)
	}
	return bytes.HasPrefix(key, u.Key) && len(key) > len(u.Key)
}

// prefixEnd is the least key above every key that begins with p: nil when
// there is none (p empty, or every byte 0xff).
func prefixEnd(p []byte) []byte {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] < 0xff {
			end := bytes.Clone(p[:i+1])
			end[i]++
			return end
		}
	}
	return nil
}

// unreadReason is the Reason of a target whose own entry an extent of
// class c kept the call from reading.
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
