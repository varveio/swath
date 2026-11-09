// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/source"
)

// TestCountCountsEveryRow: Count expands the whole tree and counts each
// row once, at one worker and at many, with the progress counter and the
// node gauge in step; it buffers nothing.
func TestCountCountsEveryRow(t *testing.T) {
	for _, workers := range []int{1, 8} {
		p := &tree{depth: 3, fanout: 4, rowsPer: 5}
		var counted atomic.Int64
		g := &Gauges{}
		st, err := Count(context.Background(), p, &counted, Options{Workers: workers, Budget: 1, Gauges: g})
		if err != nil {
			t.Fatal(err)
		}
		want := int64(len(p.keys()))
		if st.Rows != want || st.Received != want || counted.Load() != want {
			t.Fatalf("workers %d: rows %d, received %d, counted %d; want %d", workers, st.Rows, st.Received, counted.Load(), want)
		}
		if st.MaxBuffered != 0 || st.SpillRows != 0 || st.PeakAhead != 0 || st.PeakNodes < 2 || g.Snapshot().Nodes != 0 {
			t.Fatalf("workers %d: %+v, gauges %+v", workers, st, g.Snapshot())
		}
		if n := p.expanded.Load(); n != 1+4+16+64 {
			t.Fatalf("expanded %d ranges", n)
		}
	}
}

// TestCountGuardsEachRange: a range whose own keys repeat or descend, or a
// key at or below a resume's key, fails the count. Two ranges that overlap
// (each ascending) are not detected: that is Run's stream-wide guard,
// which Count does not keep (the provider's tiling owns disjointness).
func TestCountGuardsEachRange(t *testing.T) {
	k := func(s string) *string { return &s }
	for _, tc := range []struct {
		name  string
		p     script
		after *string
		fails bool
	}{
		{name: "descending in a range", p: script{root: []string{"b", "a"}}, fails: true},
		{name: "repeated across Rows calls", p: script{root: []string{"a"}, child: []string{"x"}, after: []string{"a"}}, fails: true},
		{name: "at the resume key", p: script{root: []string{"k"}}, after: k("k"), fails: true},
		{name: "overlapping ranges", p: script{root: []string{"m"}, child: []string{"a"}, after: []string{"n"}}},
	} {
		st, err := Count(context.Background(), tc.p, nil, Options{Workers: 2, After: tc.after})
		var inv *dataset.InvariantError
		switch {
		case tc.fails && !errors.As(err, &inv):
			t.Errorf("%s: %v, want an invariant error", tc.name, err)
		case !tc.fails && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		case !tc.fails && st.Rows != 3:
			t.Errorf("%s: %d rows", tc.name, st.Rows)
		}
	}
}

// disposing is a tree whose ranges are disposed when never expanded.
type disposing struct {
	*tree
	disposed atomic.Int64
}

func (d *disposing) Dispose(rng) { d.disposed.Add(1) }

// TestCountStops: a provider error is the count's error and a cancelled
// context its cause; the ranges queued and never expanded are disposed, and
// no worker is left behind.
func TestCountStops(t *testing.T) {
	p := &disposing{tree: &tree{depth: 2, fanout: 4, rowsPer: 2, failAt: "01/"}}
	if _, err := Count(context.Background(), p, nil, Options{Workers: 1}); err == nil || err.Error() != "provider failed" {
		t.Fatalf("err %v", err)
	}
	if p.expanded.Load()+p.disposed.Load() > 1+4+16 || p.disposed.Load() == 0 {
		t.Fatalf("expanded %d, disposed %d", p.expanded.Load(), p.disposed.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	slow := &tree{depth: 4, fanout: 4, rowsPer: 1, slow: 5 * time.Millisecond}
	time.AfterFunc(20*time.Millisecond, cancel)
	if _, err := Count(ctx, slow, nil, Options{Workers: 4}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

var _ source.Disposer[rng] = (*disposing)(nil)

// wide is one range that emits only children, never checking its context
// (a local directory of n subdirectories with --no-dirs), with an empty
// Rows batch between them; the first child cancels the run.
type wide struct {
	n         int
	childOnly bool // no Rows batches at all: only Child can see the stop
	cancel    context.CancelFunc
	emitted   atomic.Int64
	disposed  atomic.Int64
	expanded  atomic.Int64
}

func (w *wide) Root() int { return -1 }

func (w *wide) Expand(_ context.Context, r int, e source.Emitter[int, tp]) error {
	w.expanded.Add(1)
	if r >= 0 {
		return nil
	}
	for i := range w.n {
		if i == 1 {
			w.cancel()
		}
		if !w.childOnly {
			if err := e.Rows(nil); err != nil {
				return err
			}
		}
		w.emitted.Add(1)
		if err := e.Child(i); err != nil {
			return err
		}
	}
	return nil
}

func (w *wide) Dispose(int) { w.disposed.Add(1) }

// TestCountStopsAChildOnlyRange: after a stop, the next Child (or empty
// Rows batch) returns the cause, so a range emitting only children stops at
// once instead of after its whole width; every child it handed over is
// disposed, none expanded.
// Two variants: with an empty Rows batch between children (the stop is
// seen there first) and with children only, where Child alone sees it
// (the child accepted at the stop is disposed, not expanded).
func TestCountStopsAChildOnlyRange(t *testing.T) {
	for _, childOnly := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		w := &wide{n: 1_000_000, childOnly: childOnly, cancel: cancel}
		_, err := Count(ctx, w, nil, Options{Workers: 1})
		cancel()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("child only %v: err %v", childOnly, err)
		}
		if n := w.emitted.Load(); n > 2 {
			t.Fatalf("child only %v: %d children emitted after the stop", childOnly, n)
		}
		if w.disposed.Load() != w.emitted.Load() || w.expanded.Load() != 1 {
			t.Fatalf("child only %v: emitted %d, disposed %d, expanded %d", childOnly, w.emitted.Load(), w.disposed.Load(), w.expanded.Load())
		}
	}
}

// empties is one range that emits only empty Rows batches, never checking
// its context, until an emit fails; the first batch cancels the run.
type empties struct {
	cancel  context.CancelFunc
	batches atomic.Int64
}

func (e *empties) Root() int { return 0 }

func (e *empties) Expand(_ context.Context, _ int, em source.Emitter[int, tp]) error {
	for i := 0; i < 1_000_000; i++ {
		if i == 1 {
			e.cancel()
		}
		if err := em.Rows(nil); err != nil {
			return err
		}
		e.batches.Add(1)
	}
	return errors.New("empty batches never stopped")
}

// TestCountStopsOnEmptyBatches: after a stop an empty Rows batch returns
// the cause, so a range that only sends empty batches stops at its next
// one.
func TestCountStopsOnEmptyBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &empties{cancel: cancel}
	if _, err := Count(ctx, p, nil, Options{Workers: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	if n := p.batches.Load(); n > 1 {
		t.Fatalf("%d empty batches accepted, after the stop %d", n, n-1)
	}
}

// TestCountUnordered: with Unordered a range's rows may come in any order
// and every row is a distinct key; a resume key or groups are refused.
func TestCountUnordered(t *testing.T) {
	p := script{root: []string{"b", "a"}, child: []string{"z", "c"}, after: []string{"x"}}
	st, err := Count(context.Background(), p, nil, Options{Workers: 2, Unordered: true})
	if err != nil {
		t.Fatal(err)
	}
	if st.Rows != 5 || st.DistinctKeys != 5 {
		t.Fatalf("rows %d, distinct keys %d; want 5 and 5", st.Rows, st.DistinctKeys)
	}
	k := "a"
	for _, opt := range []Options{{Workers: 1, Unordered: true, After: &k}, {Workers: 1, Unordered: true, Groups: true}} {
		if _, err := Count(context.Background(), p, nil, opt); err == nil {
			t.Errorf("%+v: an unordered count was accepted", opt)
		}
	}
}
