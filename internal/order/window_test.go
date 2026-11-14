// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
)

// planned is a provider whose root is a plan (source.Planning): tile i
// lists rows[i] keys "t%04d-%05d" in pages of page rows, and the plan
// tells the window est[i] (rows[i] when unset). ready tiles are readable
// (-1: all; a test raises it and calls wake), failAt fails Next at that
// tile (-1: never); hold[i], when set, holds tile i after its first page
// until closed.
type planned struct {
	tiles, page int
	rows, est   []int
	failAt      int
	failErr     error
	hold        map[int]chan struct{}
	sizes       source.PlanSizes

	mu       sync.Mutex
	next     int
	ready    int
	wake     func()
	listing  atomic.Int64 // tiles in Expand
	finished atomic.Int64 // tiles listed through
}

func newPlanned(tiles, rows, page int) *planned {
	p := &planned{tiles: tiles, page: page, failAt: -1, ready: -1, hold: map[int]chan struct{}{}}
	for range tiles {
		p.rows = append(p.rows, rows)
		p.est = append(p.est, rows)
	}
	return p
}

func (p *planned) Planner() source.Planner[int] { return p }
func (p *planned) Root() int                    { return -1 }
func (p *planned) Sizes() source.PlanSizes      { return p.sizes }
func (p *planned) Bound(bool)                   {}

func (p *planned) Start(_ context.Context, wake func()) func() {
	p.mu.Lock()
	p.wake = wake
	p.mu.Unlock()
	return func() {}
}

func (p *planned) Next(admit func(int64) bool) (int, bool, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.next == p.failAt:
		return 0, false, false, p.failErr
	case p.next == p.tiles:
		return 0, false, true, nil
	case p.ready >= 0 && p.next >= p.ready:
		return 0, false, false, nil
	case !admit(int64(p.est[p.next])):
		return 0, false, false, nil
	}
	p.next++
	return p.next - 1, true, false, nil
}

// setReady makes n tiles readable and wakes the run, as a prior's reader.
func (p *planned) setReady(n int) {
	p.mu.Lock()
	p.ready = n
	w := p.wake
	p.mu.Unlock()
	w()
}

func (p *planned) admitted() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.next
}

func (p *planned) key(i, j int) string { return fmt.Sprintf("t%04d-%05d", i, j) }

func (p *planned) Expand(ctx context.Context, i int, e source.Emitter[int, tp]) error {
	if i < 0 {
		return errors.New("the plan root expanded")
	}
	p.listing.Add(1)
	defer p.listing.Add(-1)
	for j := 0; j < p.rows[i]; j += p.page {
		rows := make([]schema.Row[tp], 0, p.page)
		for k := j; k < min(j+p.page, p.rows[i]); k++ {
			rows = append(rows, schema.Row[tp]{Key: p.key(i, k)})
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
		if h := p.hold[i]; h != nil && j == 0 {
			select {
			case <-h:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	}
	p.finished.Add(1)
	return nil
}

func (p *planned) keys() []string {
	var out []string
	for i := range p.tiles {
		for k := range p.rows[i] {
			out = append(out, p.key(i, k))
		}
	}
	return out
}

// runPlanned runs p to its end within a minute (a hold never released is
// a failure of the fixture, not a release by time).
func runPlanned(t *testing.T, p *planned, opt Options) (Stats, []string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := &collect{}
	st, err := Run[int, tp](ctx, p, c, &testSchema, opt)
	return st, c.keys, err
}

// TestWindowBoundsRowsAhead: with the first tile held after its first page
// (the frontier stalled), workers admit tiles only while the tiles' charges
// fit the window (the budget, in rows, without spill), then wait at the
// window holding no range; released, the run lists every key once, in
// order. No more tiles are held at once than the window holds, and the
// rows charged to them never pass it.
func TestWindowBoundsRowsAhead(t *testing.T) {
	const tiles, rows, page, budget = 40, 1000, 100, 3000
	p := newPlanned(tiles, rows, page)
	release := make(chan struct{})
	p.hold[0] = release
	var once sync.Once
	var admittedAtRelease, listingAtRelease int64
	hookWait = func(site string) {
		// Every tile the window holds besides the stalled one is listed
		// and its worker waits at the window: release the frontier.
		if site == waitWindow && p.finished.Load() == budget/rows-1 {
			once.Do(func() {
				admittedAtRelease, listingAtRelease = int64(p.admitted()), p.listing.Load()
				close(release)
			})
		}
	}
	defer func() { hookWait = nil }()
	st, got, err := runPlanned(t, p, Options{Workers: 4, Budget: budget})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, p.keys()) {
		t.Fatalf("%d keys, want %d", len(got), len(p.keys()))
	}
	w := st.Window
	if admittedAtRelease != budget/rows || listingAtRelease != 1 {
		t.Fatalf("at the release: %d tiles admitted (want %d), %d listing (want only the stalled one)", admittedAtRelease, budget/rows, listingAtRelease)
	}
	if w == nil || w.Tiles != tiles || w.Waits[WindowRows] == 0 || w.PeakWaiters == 0 || w.PeakTiles > budget/rows || w.PeakCreditRows > budget || w.BoundBy != WindowBudget {
		t.Fatalf("window %+v", w)
	}
}

// TestWindowSmallerThanATile: a window of fewer rows than any tile admits
// none by itself; the drainer, having drained every admitted tile, admits
// the next regardless, so the run lists one tile at a time and completes.
func TestWindowSmallerThanATile(t *testing.T) {
	p := newPlanned(12, 500, 100)
	st, got, err := runPlanned(t, p, Options{Workers: 4, Budget: 50})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, p.keys()) {
		t.Fatalf("%d keys, want %d", len(got), len(p.keys()))
	}
	if w := st.Window; w == nil || w.Tiles != 12 || w.Forced != 12 || w.PeakTiles != 1 {
		t.Fatalf("window %+v", w)
	}
}

// TestPlanWaitWakes: the drainer, every admitted tile drained and the
// plan's next tile not yet read, waits at the plan, and listing is not done
// (the plan root stays open until the plan ends, though every range
// admitted is); the reader's wake releases it, and the run completes, its
// listing done once.
func TestPlanWaitWakes(t *testing.T) {
	p := newPlanned(6, 300, 100)
	p.ready = 2
	ch := waits(t)
	done := make(chan error, 1)
	var got []string
	var listed atomic.Int64
	go func() {
		var err error
		_, got, err = runPlanned(t, p, Options{Workers: 2, Budget: 1 << 20, ListingDone: func() { listed.Add(1) }})
		done <- err
	}()
	expectWait(t, ch, waitPlan)
	if p.admitted() != 2 || p.finished.Load() != 2 || listed.Load() != 0 {
		t.Fatalf("at the plan's wait: %d tiles admitted, %d listed (want 2 each), listing done %d times (want none)", p.admitted(), p.finished.Load(), listed.Load())
	}
	p.setReady(-1)
	if err := expect(t, done, "plan"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, p.keys()) || listed.Load() != 1 {
		t.Fatalf("%d keys, want %d; listing done %d times", len(got), len(p.keys()), listed.Load())
	}
}

// TestPlanFailureStopsTheStream: the plan failing after some tiles fails
// the run with its error, never ends it as complete: what the sink
// received is a prefix of the keyspace, and no tile after the failure was
// admitted.
func TestPlanFailureStopsTheStream(t *testing.T) {
	for _, w := range []int{1, 4} {
		p := newPlanned(10, 300, 100)
		p.failAt, p.failErr = 5, errors.New("the prior's reader failed")
		st, got, err := runPlanned(t, p, Options{Workers: w, Budget: 1 << 20})
		if !errors.Is(err, p.failErr) {
			t.Fatalf("w%d: %v, want the plan's error", w, err)
		}
		if want := p.keys(); len(got) > 5*300 || !slices.Equal(got, want[:len(got)]) {
			t.Fatalf("w%d: %d keys handed on, not a prefix of the first 5 tiles", w, len(got))
		}
		if !st.ListingDone.IsZero() {
			t.Fatalf("w%d: a failed plan finished the listing", w)
		}
	}
}

// TestWindowCancelWhileWaiting: a run whose workers wait at the window
// behind a stalled frontier, and whose drainer waits on it, returns
// promptly when cancelled.
func TestWindowCancelWhileWaiting(t *testing.T) {
	p := newPlanned(20, 1000, 100)
	p.hold[0] = make(chan struct{}) // never released
	ch := waits(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Run[int, tp](ctx, p, &collect{}, &testSchema, Options{Workers: 4, Budget: 2000})
		done <- err
	}()
	expectWait(t, ch, waitWindow)
	cancel()
	if err := expect(t, done, "window"); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v, want the cancellation", err)
	}
}

// TestWindowGrowthGuard: a tile that lists past growthFactor times its
// expectation stops admission until the frontier passes it (the window
// waits as "growth"), whatever room the rows leave; the run completes.
func TestWindowGrowthGuard(t *testing.T) {
	p := newPlanned(30, 300, 50)
	p.rows[1], p.est[1] = 3000, 300 // grown tenfold since the prior
	release := make(chan struct{})
	for i := range p.tiles { // every tile but the grown one held until the first wait
		if i != 1 {
			p.hold[i] = release
		}
	}
	var once sync.Once
	var admitted int
	hookWait = func(site string) {
		// The grown tile listed through and its worker refused for it
		// (every other tile is held): release the rest.
		if site == waitWindow && p.finished.Load() == 1 {
			once.Do(func() { admitted = p.admitted(); close(release) })
		}
	}
	defer func() { hookWait = nil }()
	st, got, err := runPlanned(t, p, Options{Workers: 4, Budget: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, p.keys()) {
		t.Fatalf("%d keys, want %d", len(got), len(p.keys()))
	}
	if w := st.Window; w == nil || w.Grown == 0 || w.Waits[WindowGrowth] == 0 || admitted >= p.tiles {
		t.Fatalf("window %+v, %d tiles admitted at the first wait", w, admitted)
	}
}

// TestWindowSpillCapHeld: with spill capped, the window takes half the cap
// over the spilled rows' bytes (bound_by spill_cap) and refuses tiles once
// live spill passes half the cap, so a stalled frontier never makes the
// disk budget refuse a frame; released, the run lists every key.
func TestWindowSpillCapHeld(t *testing.T) {
	p := newPlanned(300, 100, 50)
	release := make(chan struct{})
	p.hold[0] = release
	var once sync.Once
	hookWait = func(site string) {
		if (site == waitWindow || site == waitIdle || site == waitPublish) && p.finished.Load() > 2 {
			once.Do(func() { close(release) })
		}
	}
	defer func() { hookWait = nil }()
	st, got, err := runPlanned(t, p, Options{Workers: 4, Budget: 300, SpillDir: t.TempDir(), SpillMax: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, p.keys()) {
		t.Fatalf("%d keys, want %d", len(got), len(p.keys()))
	}
	w := st.Window
	if w == nil || w.BoundBy != WindowSpillCap || w.Waits[WindowRows]+w.Waits[WindowSpill] == 0 || st.SpillRows == 0 || st.SpillRefused != 0 {
		t.Fatalf("window %+v, spilled %d rows, refused %d frames", w, st.SpillRows, st.SpillRefused)
	}
}

// TestWindowTarget: with a disk and a budget that hold far more, the
// window is its target (windowTarget: bound_by rows), or the rows the plan
// sets (the diag knob: bound_by set), the tiles held never charging more;
// under a small spill cap the cap's bound applies first; the run
// completes.
func TestWindowTarget(t *testing.T) {
	for _, tc := range []struct {
		sizes source.PlanSizes
		by    string
		rows  int64
	}{
		{source.PlanSizes{}, WindowTarget, windowTarget},
		{source.PlanSizes{WindowRows: 500}, WindowSet, 500},
	} {
		p := newPlanned(40, 100, 50)
		p.sizes = tc.sizes
		st, got, err := runPlanned(t, p, Options{Workers: 4, Budget: 1 << 20, SpillDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, p.keys()) {
			t.Fatalf("%s: %d keys, want %d", tc.by, len(got), len(p.keys()))
		}
		if w := st.Window; w == nil || w.BoundBy != tc.by || w.Rows != tc.rows || w.PeakCreditRows > tc.rows {
			t.Fatalf("%s: window %+v, want %d rows", tc.by, w, tc.rows)
		}
	}
	p := newPlanned(40, 100, 50)
	p.sizes = source.PlanSizes{WindowRows: 1 << 30}
	st, _, err := runPlanned(t, p, Options{Workers: 4, Budget: 1 << 20, SpillDir: t.TempDir(), SpillMax: 4 << 10})
	if err != nil || st.Window.BoundBy != WindowSpillCap {
		t.Fatalf("a small cap under a large target: %v, window %+v", err, st.Window)
	}
}

// fencedPlanned is planned with durable spill: each tile declares its span
// (t%04d of its index, of the next] and a fence after every page and at
// its end, so a tile that spills seals a certificate.
type fencedPlanned struct{ *planned }

func (f fencedPlanned) Fences() bool { return true }

func (f fencedPlanned) Expand(ctx context.Context, i int, e source.Emitter[int, tp]) error {
	if i < 0 {
		return errors.New("the plan root expanded")
	}
	p := f.planned
	fence := e.(source.Fencer)
	fence.Span(fmt.Sprintf("t%04d", i), fmt.Sprintf("t%04d", i+1), false)
	p.listing.Add(1)
	defer p.listing.Add(-1)
	for j := 0; j < p.rows[i]; j += p.page {
		end := min(j+p.page, p.rows[i])
		rows := make([]schema.Row[tp], 0, end-j)
		for k := j; k < end; k++ {
			rows = append(rows, schema.Row[tp]{Key: p.key(i, k), Data: tp{Size: int64(k), ETag: "etag"}})
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
		fence.Fence(p.key(i, end-1), false)
		if h := p.hold[i]; h != nil && j == 0 {
			select {
			case <-h:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	}
	fence.Fence(fmt.Sprintf("t%04d", i+1), false)
	p.finished.Add(1)
	return nil
}

// pacedSink is collect behind a writer the durable spill's unlinks keep up
// with, as a part writer is beside a flusher cycling every 100 ms: a batch
// waits for the unlinks queued before it.
type pacedSink struct {
	collect
	c        *Cache
	unlinked chan struct{}
}

func (s *pacedSink) Write(ctx context.Context, segs [][]schema.Row[tp]) error {
	for behind, waiting := s.c.Behind(); behind > waiting; behind, waiting = s.c.Behind() {
		select {
		case <-s.unlinked:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return s.collect.Write(ctx, segs)
}

// TestWindowShrinksToTheSpillCap: far more workers than a small spill cap
// can serve, tiles finishing out of order (every eighth tile stalls after
// its first page until a worker waits), a fresh counted prior, durable
// spill whose drained certificates wait for a K that never comes (no part
// is published), a writer the unlinks keep up with. The spill behind the
// output does not hold the window: it is evicted while live spill is past
// half the cap, so the spill ahead holds the window, which shrinks to what
// the cap serves (spill_rows under its rows) instead of closing on spill
// the writer has passed and crawling on the drainer's forced admissions
// (counted against the gate, 295 of 320 tiles were forced). No frame is
// refused and every key is handed on once, in order.
func TestWindowShrinksToTheSpillCap(t *testing.T) {
	const tiles, rows, page = 320, 120, 30
	p := newPlanned(tiles, rows, page)
	var holds []chan struct{}
	for i := 8; i < tiles; i += 8 {
		h := make(chan struct{})
		p.hold[i], holds = h, append(holds, h)
	}
	var released int
	hookWait = func(site string) {
		// A worker waits: the oldest stalled tile goes on (mu held).
		if (site == waitWindow || site == waitIdle || site == waitPublish) && released < len(holds) {
			close(holds[released])
			released++
		}
	}
	defer func() { hookWait = nil }()
	interval := flushInterval
	flushInterval = 0 // a cycle whenever one is queued
	defer func() { flushInterval = interval }()
	unlinked := make(chan struct{}, 1)
	defer SetDurableEvents(func(e string) {
		if e == "unlinked" {
			select {
			case unlinked <- struct{}{}:
			default:
			}
		}
	})()
	c, _, _ := newDurable(t)
	if err := c.Start(slots(4096), 0, nil); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sink := &pacedSink{c: c, unlinked: unlinked}
	st, err := Run[int, tp](ctx, fencedPlanned{p}, sink, &testSchema, Options{Workers: 64, Budget: 240, SpillDir: t.TempDir(), SpillMax: 48 << 10, Durable: c})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sink.keys, p.keys()) {
		t.Fatalf("%d keys, want %d", len(sink.keys), len(p.keys()))
	}
	w := st.Window
	t.Logf("window %+v; spilled %d rows, peak %d, refused %d; durable %+v", w, st.SpillRows, st.SpillPeak, st.SpillRefused, c.Report())
	if !st.Durable || c.st.SealedFiles.Load() == 0 || w.PeakBehindBytes == 0 {
		t.Fatalf("no certificate drained behind the output: the arm proves nothing")
	}
	if st.SpillRefused != 0 || w.Forced > tiles/20 {
		t.Fatalf("refused %d frames, %d of %d tiles forced: the window crawled", st.SpillRefused, w.Forced, w.Tiles)
	}
	if w.Waits[WindowSpill] == 0 || w.Evicted == 0 || w.SpillRows == 0 || w.SpillRows >= w.Rows {
		t.Fatalf("spill did not bind the window below its rows: %+v", w)
	}
}
