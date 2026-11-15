// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
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

// runScript emits its range's rows one per Rows call, a child between
// before and after: the child's own rows are child.
type runScript struct{ before, child, after []string }

func (s runScript) Root() fr { return fr{-1} }

func (s runScript) Expand(_ context.Context, r fr, e source.Emitter[fr, tp]) error {
	rows := func(keys []string) error {
		for _, k := range keys {
			if err := e.Rows([]schema.Row[tp]{{Key: k}}); err != nil {
				return err
			}
		}
		return nil
	}
	if r.i == 0 {
		return rows(s.child)
	}
	if err := rows(s.before); err != nil {
		return err
	}
	if err := e.Child(fr{0}); err != nil {
		return err
	}
	return rows(s.after)
}

// runSink records each run's keys and checks that a run takes no row once
// ended and ends once.
type runSink struct {
	mu   sync.Mutex
	runs [][]string
	bad  []string
}

func (s *runSink) Write(context.Context, [][]schema.Row[tp]) error {
	s.mu.Lock()
	s.bad = append(s.bad, "Write without a run")
	s.mu.Unlock()
	return nil
}

func (s *runSink) Run() RangeRun[tp] { return &recordedRun{s: s} }

type recordedRun struct {
	s     *runSink
	keys  []string
	ended bool
}

func (r *recordedRun) Write(_ context.Context, rows []schema.Row[tp]) error {
	if r.ended {
		r.s.mu.Lock()
		r.s.bad = append(r.s.bad, "a row after End")
		r.s.mu.Unlock()
	}
	for _, row := range rows {
		r.keys = append(r.keys, row.Key)
	}
	return nil
}

func (r *recordedRun) End() {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if r.ended {
		r.s.bad = append(r.s.bad, "ended twice")
	}
	r.ended = true
	r.s.runs = append(r.s.runs, r.keys)
}

// TestCountRuns: a RangeSink takes each range's rows in runs, one per
// stretch between the range's children, every row of a stretch in one run
// and in order, each run ended once; a range with no rows opens none.
func TestCountRuns(t *testing.T) {
	for _, unordered := range []bool{false, true} {
		s := &runSink{}
		p := runScript{before: []string{"a", "b"}, child: []string{"c", "d"}, after: []string{"e"}}
		if _, err := CountWithSink(context.Background(), p, nil, Options{Workers: 2, Unordered: unordered}, s); err != nil {
			t.Fatal(err)
		}
		slices.SortFunc(s.runs, func(a, b []string) int { return strings.Compare(a[0], b[0]) })
		if want := [][]string{{"a", "b"}, {"c", "d"}, {"e"}}; len(s.bad) > 0 || !slices.EqualFunc(s.runs, want, slices.Equal) {
			t.Fatalf("unordered %v: runs %q (%q), want %q", unordered, s.runs, s.bad, want)
		}
		s = &runSink{}
		if _, err := CountWithSink(context.Background(), runScript{child: []string{"c"}}, nil, Options{Workers: 1}, s); err != nil || len(s.runs) != 1 {
			t.Fatalf("a range without rows: %v, runs %q", err, s.runs)
		}
	}
}

// treeScript is a three-level tree whose ranges own prefixes, as a local
// directory's do: the root "" (row a, child d/), d/ (row d/x, child
// d/e/), d/e/ (row d/e/y).
type treeScript struct{}

func (treeScript) Root() fr { return fr{0} }

func (treeScript) Expand(_ context.Context, r fr, e source.Emitter[fr, tp]) error {
	prefix, row := [...]string{"", "d/", "d/e/"}[r.i], [...]string{"a", "d/x", "d/e/y"}[r.i]
	e.(source.Subtreer).Subtree(prefix)
	if err := e.Rows([]schema.Row[tp]{{Key: row}}); err != nil {
		return err
	}
	if r.i < 2 {
		return e.Child(fr{r.i + 1})
	}
	return nil
}

// treeSink records runs ending and subtrees ending, in order.
type treeSink struct {
	runSink
	events []string
}

func (s *treeSink) Run() RangeRun[tp] { return &treeRun{recordedRun{s: &s.runSink}, s} }

func (s *treeSink) SubtreeEnd(prefix string) {
	s.mu.Lock()
	s.events = append(s.events, "subtree "+prefix)
	s.mu.Unlock()
}

type treeRun struct {
	recordedRun
	t *treeSink
}

func (r *treeRun) End() {
	r.recordedRun.End()
	r.t.mu.Lock()
	r.t.events = append(r.t.events, "run "+strings.Join(r.keys, ","))
	r.t.mu.Unlock()
}

// TestCountSubtrees: a SubtreeSink is told that a range owning a prefix
// ended once its runs and its descendants' have ended, the deepest first,
// each once.
func TestCountSubtrees(t *testing.T) {
	for _, workers := range []int{1, 4} {
		s := &treeSink{}
		if _, err := CountWithSink(context.Background(), treeScript{}, nil, Options{Workers: workers, Unordered: true}, s); err != nil {
			t.Fatal(err)
		}
		want := []string{"run a", "run d/x", "run d/e/y", "subtree d/e/", "subtree d/", "subtree "}
		if !slices.Equal(s.events, want) || len(s.bad) > 0 {
			t.Fatalf("workers %d: %q (%q), want %q", workers, s.events, s.bad, want)
		}
	}
}

// spanScript is two ranges that declare their intervals, as the range
// engine's do: the root (⊥, ⊤] lists a and b and splits after b; its
// child (b, ⊤] lists c and d and returns.
type spanScript struct{}

func (spanScript) Root() fr { return fr{0} }

func (spanScript) Expand(_ context.Context, r fr, e source.Emitter[fr, tp]) error {
	f := e.(source.Fencer)
	if r.i == 0 {
		f.Span("", "", true)
		if err := e.Rows([]schema.Row[tp]{{Key: "a"}, {Key: "b"}}); err != nil {
			return err
		}
		f.Fence("b", false)
		return e.Child(fr{1})
	}
	f.Span("b", "", true)
	return e.Rows([]schema.Row[tp]{{Key: "c"}, {Key: "d"}})
}

// coverSink records each run's interval.
type coverSink struct {
	runSink
	got []string
}

func (s *coverSink) Run() RangeRun[tp] {
	return &coverRun{recordedRun: recordedRun{s: &s.runSink}, c: s}
}

type coverRun struct {
	recordedRun
	c        *coverSink
	from, to string
}

func (r *coverRun) From(lo string) { r.from = "(" + lo }

func (r *coverRun) Through(hi string, top bool) {
	r.to = hi + "]"
	if top {
		r.to = "⊤]"
	}
}

func (r *coverRun) End() {
	r.recordedRun.End()
	r.c.mu.Lock()
	r.c.got = append(r.c.got, strings.Join(r.keys, ",")+" "+r.from+","+r.to)
	r.c.mu.Unlock()
}

// TestCountCoveredRuns: in an ordered count a CoveredRun is told the
// interval its range's rows cover: from the range's span, through its last
// row at a split or the span's end when the range returns. An unordered
// count tells nothing.
func TestCountCoveredRuns(t *testing.T) {
	s := &coverSink{}
	if _, err := CountWithSink(context.Background(), spanScript{}, nil, Options{Workers: 2}, s); err != nil {
		t.Fatal(err)
	}
	slices.Sort(s.got)
	if want := []string{"a,b (,b]", "c,d (b,⊤]"}; !slices.Equal(s.got, want) {
		t.Fatalf("runs %q, want %q", s.got, want)
	}
	s = &coverSink{}
	if _, err := CountWithSink(context.Background(), spanScript{}, nil, Options{Workers: 2, Unordered: true}, s); err != nil {
		t.Fatal(err)
	}
	slices.Sort(s.got)
	if want := []string{"a,b ,", "c,d ,"}; !slices.Equal(s.got, want) {
		t.Fatalf("unordered runs %q, want %q", s.got, want)
	}
}

// emptyScript is ranges that declare their intervals and list nothing in
// some: the root (⊥, b] lists a, b and splits; its first child (b, m] is
// empty and splits at its fence x; the next child (x, m] is empty and
// returns; the last (m, ⊤] lists z.
type emptyScript struct{}

func (emptyScript) Root() fr { return fr{0} }

func (emptyScript) Expand(_ context.Context, r fr, e source.Emitter[fr, tp]) error {
	f := e.(source.Fencer)
	switch r.i {
	case 0:
		f.Span("", "", true)
		if err := e.Rows([]schema.Row[tp]{{Key: "a"}, {Key: "b"}}); err != nil {
			return err
		}
		if err := e.Child(fr{1}); err != nil {
			return err
		}
		return e.Child(fr{3})
	case 1:
		f.Span("b", "m", false)
		f.Fence("x", false)
		return e.Child(fr{2})
	case 2:
		f.Span("x", "m", false)
		return nil
	}
	f.Span("m", "", true)
	return e.Rows([]schema.Row[tp]{{Key: "z"}})
}

// TestCountCoveredEmpty: a range that listed no row still tells its
// interval, through an empty run: up to its fence at a split, up to its end
// when it returns; so the intervals of a count tile its keys.
func TestCountCoveredEmpty(t *testing.T) {
	s := &coverSink{}
	if _, err := CountWithSink(context.Background(), emptyScript{}, nil, Options{Workers: 2}, s); err != nil {
		t.Fatal(err)
	}
	slices.Sort(s.got)
	if want := []string{" (b,x]", " (x,m]", "a,b (,b]", "z (m,⊤]"}; !slices.Equal(s.got, want) {
		t.Fatalf("runs %q, want %q", s.got, want)
	}
}
