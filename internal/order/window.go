// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/internal/source"
)

// The frontier window (docs/internals/ordering.md, "The frontier window").
// A provider whose root is a plan (source.Planning: a counted prior's
// tiles) has its tiles admitted one at a time, in key order, while the
// rows ahead of the output stay within the window; the root itself is
// never expanded. Each admitted tile holds a credit: the rows the prior
// expects in it (est) and the rows its ranges have received and the
// drainer has not yet handed to the sink (held). Its charge is the larger
// of the two until it is released: closed by the drainer (every range in
// it drained) with nothing held. A worker with nothing queued admits the
// next tile when the charges, with the tile's expected rows, fit the
// window's rows, live spill is at most half its cap and no admitted tile
// has grown to growthFactor times its expectation; otherwise it waits at
// the window, holding no range. The drainer, having drained every admitted
// tile, admits one regardless (forced), so a window smaller than a tile
// still lists. The window is an admission heuristic, not a storage bound:
// a tile already listing keeps listing whatever it holds.

// growthFactor is how far past its expected rows a tile's held rows go
// before it counts as grown: no tile is admitted while a grown one is held.
const growthFactor = 4

// spillRowBytes is a spilled row's bytes before the run has spilled any,
// when the prior gives none: about a plain frame's row (delta frames
// measure 18 to 49 bytes a row, plain ones 67).
const spillRowBytes = 64

// What bound the window's rows (WindowStats.BoundBy).
const (
	WindowSpillCap  = "spill_cap" // half the spill cap over a spilled row's bytes, with what the budget holds
	WindowBudget    = "budget"    // spill off: what the budget holds
	WindowUnbounded = "unbounded" // spill without a cap: no limit
	WindowRows      = "rows"      // a refusal: the charges and the tile past the window's rows
	WindowSpill     = "spill"     // a refusal: live spill past half its cap, or a frame refused
	WindowGrowth    = "growth"    // a refusal: a held tile grown past growthFactor
)

// WindowStats is the frontier window's account (Stats.Window; nil without
// a plan).
type WindowStats struct {
	// Tiles admitted; Forced of them by the drainer past a refusal (it
	// had drained every other); Grown tiles whose held rows reached
	// growthFactor times their expectation.
	Tiles, Forced, Grown int64
	// Rows is the window's rows when it was last evaluated, BoundBy the
	// term that set it.
	Rows    int64
	BoundBy string
	// Waits are workers' waits at the window by the refusal that held
	// them (WindowRows, WindowSpill, WindowGrowth); WaitTime their time,
	// summed over workers, PeakWaiters the most at once.
	Waits       map[string]int64
	WaitTime    time.Duration
	PeakWaiters int64
	// PlanWait is the drainer's time waiting for the plan to read ahead.
	PlanWait time.Duration
	// PeakTiles is the most admitted tiles held at once; PeakCreditRows
	// the most rows they were charged (the larger of each one's expected
	// and held rows); PeakRetainedBytes the most bytes the order stage
	// held at an admission, in memory (Options.Budget's units) and on
	// disk (live spill).
	PeakTiles, PeakCreditRows, PeakRetainedBytes int64
}

// The waiters at the window wait on their own condition (run.winCond),
// woken one at a time as room comes back: a tile released wakes one per
// tile, the refused tile fitting one, a worker that admits a tile the next
// (so waking follows the room, not every step of the drainer), children
// queued one per child, spill credit one; a stop, the plan's end or
// failure all of them.

// tileCredit is an admitted tile's account.
type tileCredit struct {
	est    int64
	grow   int64        // held rows at which the tile counts as grown
	held   atomic.Int64 // rows received and not yet handed to the sink
	grown  atomic.Bool
	closed bool // the drainer closed the tile's node (mu)
	// open counts the tile's ranges not yet done (o.open's, per tile):
	// at 0 under mu the tile is listed, every range in it done, its items
	// final and its spill files complete (segments.go: rolling).
	open atomic.Int64
}

// window is the run's frontier window (nil without a plan).
type window[R, P any] struct {
	plan     source.Planner[R]
	root     *node[R, P]
	rowBytes float64 // the prior's bytes per row (0: unknown)
	// tiles are the admitted tiles not yet released, in key order; est
	// their expected rows (mu); excess the rows held past their
	// expectations, grown how many are grown (atomic: emitters charge
	// rows without mu).
	tiles  []*tileCredit
	est    int64
	excess atomic.Int64
	grown  atomic.Int64
	ever   atomic.Int64 // tiles that grew (WindowStats.Grown)
	listed atomic.Int64 // tiles whose every range is done (tileCredit.open reached 0)
	sumEst int64        // every admitted tile's expected rows, for the mean (growth's floor)
	done   bool         // the plan ended (mu)
	why    string
	// need is the expected rows of the tile the window last refused for
	// its rows (0: none since an admission): settle wakes the workers at
	// the window once that many fit (mu).
	need int64
	st   WindowStats
	// units and rows: the budget units and rows measured as emitters
	// received them, the budget's bytes per row.
	units, rows atomic.Int64
}

// tileRun is a run of rows of one tile in the drainer's pending batch.
type tileRun struct {
	t *tileCredit
	n int64
}

// charge adds d rows to t's held rows (negative: handed on).
func (w *window[R, P]) charge(t *tileCredit, d int64) {
	h := t.held.Add(d)
	old := h - d
	if dx := max(0, h-t.est) - max(0, old-t.est); dx != 0 {
		w.excess.Add(dx)
	}
	if d > 0 && old < t.grow && h >= t.grow && t.grown.CompareAndSwap(false, true) {
		w.grown.Add(1)
		w.ever.Add(1)
	}
}

// Admission outcomes (admitTile).
const (
	admitted = iota
	admitNone
	admitRefused
	admitDone
	admitFailed
)

// windowRows is the window's rows and the term that set it (mu held).
func (o *run[R, P]) windowRows() (float64, string) {
	w := o.win
	mem := 0.0
	if u := w.units.Load(); u > 0 {
		mem = float64(o.opt.Budget) * float64(w.rows.Load()) / float64(u)
	}
	switch {
	case o.sp == nil:
		return mem, WindowBudget
	case o.sp.max <= 0:
		return math.Inf(1), WindowUnbounded
	}
	beta := w.rowBytes
	if n := o.sp.rows.Load(); n > 0 {
		beta = float64(o.sp.bytes.Load()) / float64(n)
	}
	if beta <= 0 {
		beta = spillRowBytes
	}
	return float64(o.sp.max)/2/beta + mem, WindowSpillCap
}

// refuses is why the window refuses a tile of e expected rows, "" when it
// admits it (mu held).
func (o *run[R, P]) refuses(e int64) string {
	w := o.win
	if w.grown.Load() > 0 {
		return WindowGrowth
	}
	if o.sp != nil && (o.sp.full.Load() || o.sp.max > 0 && o.sp.live.Load() > o.sp.max/2) {
		return WindowSpill
	}
	rows, by := o.windowRows()
	w.st.Rows, w.st.BoundBy = int64(min(rows, math.MaxInt64)), by
	if float64(w.est+w.excess.Load()+e) > rows {
		w.need = max(e, 1)
		return WindowRows
	}
	return ""
}

// admitTile takes the plan's next tile if the window admits it, or
// regardless with force: the tile becomes the root's next child and is
// queued (mu held).
func (o *run[R, P]) admitTile(force bool) (int, error) {
	w := o.win
	if w.done {
		return admitDone, nil
	}
	var e int64
	why := ""
	r, ok, done, err := w.plan.Next(func(rows int64) bool {
		e, why = rows, o.refuses(rows)
		return force || why == ""
	})
	switch {
	case err != nil:
		return admitFailed, err
	case done:
		return admitDone, nil
	case !ok && why != "":
		w.why = why
		return admitRefused, nil
	case !ok:
		return admitNone, nil
	}
	w.st.Tiles++
	w.need = 0
	if why != "" {
		w.st.Forced++
	}
	w.sumEst += e
	t := &tileCredit{est: e, grow: growthFactor * max(e, w.sumEst/w.st.Tiles, 1)}
	t.open.Store(1) // its node
	w.tiles = append(w.tiles, t)
	w.est += e
	n := &node[R, P]{r: r, tile: t}
	u := childUnits()
	w.root.items = append(w.root.items, item[R, P]{child: n, units: u})
	o.meta += u
	o.buffered += u
	o.max = max(o.max, o.buffered)
	o.g.nodes.add(1)
	o.open.Add(1)
	o.stack = append(o.stack, n)
	w.st.PeakTiles = max(w.st.PeakTiles, int64(len(w.tiles)))
	w.st.PeakCreditRows = max(w.st.PeakCreditRows, w.est+w.excess.Load())
	held := o.buffered
	if o.sp != nil {
		held += o.sp.live.Load()
	}
	w.st.PeakRetainedBytes = max(w.st.PeakRetainedBytes, held)
	o.drain.Broadcast() // the drainer may wait on the plan
	return admitted, nil
}

// settle releases the tiles at the front that are closed with nothing
// held, and wakes the workers waiting at the window (drainer, mu held).
func (o *run[R, P]) settle() {
	w := o.win
	n := 0
	for n < len(w.tiles) && w.tiles[n].closed && w.tiles[n].held.Load() == 0 {
		t := w.tiles[n]
		w.est -= t.est
		if t.grown.Load() {
			w.grown.Add(-1)
		}
		w.tiles[n] = nil
		n++
	}
	if n > 0 {
		w.tiles = w.tiles[n:]
		for range n { // a waiter per tile's room
			o.winCond.Signal()
		}
		return
	}
	o.roomCheck()
}

// roomCheck wakes the workers the window refused for its rows once the
// refused tile fits: rows handed on, or the window grown with what the run
// measured (a row's bytes in memory or spilled). Once per refusal, not at
// every step: the drainer's steps (settle) and every publish ask (mu held).
func (o *run[R, P]) roomCheck() {
	w := o.win
	if w.need > 0 && o.g.windowWait.n.Load() > 0 {
		if rows, _ := o.windowRows(); float64(w.est+w.excess.Load()+w.need) <= rows {
			w.need = 0
			o.winCond.Signal()
		}
	}
}

// measured adds rows received and their budget units to the window's
// measure of a row; the first measure wakes every waiter, since until then
// the window counted no memory at all (without mu).
func (o *run[R, P]) measured(units int64, rows int) {
	w := o.win
	w.units.Add(units)
	if w.rows.Add(int64(rows)) == int64(rows) {
		o.mu.Lock()
		o.winCond.Signal()
		o.mu.Unlock()
	}
}

// endPlan ends the plan root: done, no longer counted open; it reports
// whether that finished the listing (ListingDone is the caller's to call,
// without mu; mu held).
func (o *run[R, P]) endPlan() bool {
	o.win.done = true
	o.win.root.state = done
	o.work.Broadcast()
	o.winCond.Broadcast()
	o.drain.Broadcast()
	if o.open.Add(-1) == 0 && !o.stopped.Load() {
		o.listedAt = time.Now()
		return true
	}
	return false
}

// planFailed stops the run with the plan's failure, latched before
// anything is exposed as done (as a failed range is; mu held).
func (o *run[R, P]) planFailed(err error) error {
	o.cancel(err)
	o.stopped.Store(true)
	o.work.Broadcast()
	o.winCond.Broadcast()
	o.drain.Broadcast()
	return err
}

// notifyListed tells the pipeline the listing is done (without mu).
func (o *run[R, P]) notifyListed() {
	if o.opt.ListingDone != nil {
		o.opt.ListingDone()
	}
	o.listedNotified.Store(true)
}

// awaitPlan is the drainer at the plan root with every admitted tile
// drained: it hands on what is batched, releases the tiles, and admits the
// next tile regardless of the window, ends the plan, fails with it, or
// waits for it to read ahead (mu held).
func (o *run[R, P]) awaitPlan() error {
	if len(o.segs) > 0 {
		o.mu.Unlock()
		err := o.flush()
		o.mu.Lock()
		return err
	}
	o.settle()
	switch st, err := o.admitTile(true); st {
	case admitted:
		o.work.Signal()
		o.winCond.Signal() // a worker at the window may take it
		return nil
	case admitDone:
		if o.endPlan() {
			o.mu.Unlock()
			o.notifyListed()
			o.mu.Lock()
		}
		return nil
	case admitFailed:
		return o.planFailed(err)
	}
	o.release()
	o.win.st.PlanWait += o.waitAt(o.drain, waitPlan)
	return nil
}

// stats is the window's account (mu held, after the run).
func (w *window[R, P]) stats() *WindowStats {
	st := w.st
	st.Grown = w.ever.Load()
	return &st
}
