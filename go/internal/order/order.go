// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package order turns a provider's tree of sorted ranges into one ascending
// stream of rows with bounded memory.
//
// Workers expand ranges taken from a LIFO stack; children are pushed in
// reverse order so the range the drain needs next is on top. One drainer
// (the caller of Run) walks the tree depth-first and hands contiguous row
// segments to the sink, consuming the range it is on as rows arrive.
//
// Memory: buffered rows are bounded by Options.Budget. Workers wait before
// claiming a range, and when publishing rows, while the budget is used up,
// unless the drainer is waiting for that very range (or expanding it
// itself): then the rows it needs are always admitted, and the drainer
// expands a needed range itself if no worker has claimed it, so a full
// buffer cannot deadlock the walk. A streaming range on the drain path (an
// S3 prefix) therefore also blocks while the sink is behind. Child nodes
// and spill markers count against the budget as metadata. Rows still
// pending on the root-to-frontier path count against the budget and can
// exceed it by at most one range's content per level (the O(depth ×
// fan-out) of a depth-first walk), plus one publish batch per worker (at
// most 2 × publishRows rows or publishItems items).
package order

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// Sink receives ascending row segments from a single goroutine. The segments
// are owned by the sink once Write is called.
type Sink[P any] interface {
	Write(ctx context.Context, segs [][]schema.Row[P]) error
}

// Options configure Run.
type Options struct {
	Workers int   // expanding goroutines (>= 1)
	Budget  int64 // bytes of rows read but not yet drained (schema.RowBytes), shared with Shared (>= 1)
	// Shared, if set, holds bytes owned downstream (the encoders' queues);
	// they count against the same budget, so a slow encoder slows producers.
	Shared *Shared
	// SpillDir, if set, enables spill: when the budget is used up, ranges
	// that are not being drained write their rows to files there instead
	// of waiting (see spill.go). SpillCompress compresses frames (zstd 1).
	SpillDir      string
	SpillCompress bool
	// SpillMax caps the live spill bytes (0: no cap); SpillReserve is the
	// free space spill leaves on its filesystem (0: none). Past either, an
	// emitter waits on the budget instead of spilling.
	SpillMax, SpillReserve int64
	// SpillVerify, if set, is called before the spill directory is created
	// and before each spill file is created, appended to or removed: a
	// spill directory inside the output must still be in the directory
	// the run owns (journal.Lock.Verify).
	SpillVerify func() error
	// Gauges, if set, receives the stage's live resource counts.
	Gauges *Gauges
	// After, if set, is a resume's last published key: every row must sort
	// after it. The stream-wide order guard checks it with every other row.
	After *string
}

// Stats describe a run.
type Stats struct {
	Rows        int64 // rows handed to the sink
	Received    int64 // rows the provider's emitters received (== Rows after a successful run)
	MaxBuffered int64 // peak bytes read but not yet drained (in memory, excluding Shared)
	SpillRows   int64 // rows written to spill files and replayed
	SpillFrames int64
	SpillBytes  int64
	SpillFiles  int64 // ranges that spilled
	SpillPeak   int64 // peak live spill bytes
	// SpillRefused counts frames the disk budget refused; each made its
	// emitter wait on the reorder budget instead.
	SpillRefused int64
	// FrontierWait is the time the drainer waited for the range it needs
	// next while a worker was still listing it (the slow-frontier cost).
	// BudgetWait is the time emitters and workers with ranges queued waited
	// on a full budget (the reorder cost). Both sum over goroutines.
	FrontierWait, BudgetWait time.Duration
	// Peaks of the stage's metadata (Gauges): outstanding range nodes,
	// spill markers in memory, open spill readers.
	PeakNodes, PeakMarkers, PeakReaders int64
	// PeakAhead is the most rows received and not yet handed on at once:
	// rows ahead of the publish frontier, in memory or spilled (Gauges).
	PeakAhead int64
	// FrontierStuck is the longest interval in which the drainer handed
	// nothing on: from the drain's start to the first row, between two
	// hand-ons, or from the last row to the drain's end.
	FrontierStuck time.Duration
}

// Gauges are the order stage's live resource counts, read while it runs
// (the progress file): outstanding range nodes (created and not yet
// drained), spill markers in memory, open spill readers and live spill
// bytes, each with its peak.
type Gauges struct {
	nodes, markers, readers, spill gauge
	ahead                          gauge // rows received and not yet handed on
}

// gauge is a count and its peak.
type gauge struct{ n, peak atomic.Int64 }

func (g *gauge) add(d int64) { g.raise(g.n.Add(d)) }

// set records an absolute value (a level read elsewhere).
func (g *gauge) set(n int64) {
	g.n.Store(n)
	g.raise(n)
}

func (g *gauge) raise(n int64) {
	for p := g.peak.Load(); n > p && !g.peak.CompareAndSwap(p, n); p = g.peak.Load() {
	}
}

// GaugeSnapshot is Gauges at one moment.
type GaugeSnapshot struct {
	Nodes            int64 `json:"nodes"`
	PeakNodes        int64 `json:"peak_nodes"`
	Markers          int64 `json:"markers"`
	PeakMarkers      int64 `json:"peak_markers"`
	SpillReaders     int64 `json:"spill_readers"`
	PeakSpillReaders int64 `json:"peak_spill_readers"`
	SpillLiveBytes   int64 `json:"spill_live_bytes"`
	PeakSpillBytes   int64 `json:"peak_spill_live_bytes"`
	// AheadRows are rows received and not yet handed on: ahead of the
	// publish frontier, in memory or spilled.
	AheadRows     int64 `json:"ahead_rows"`
	PeakAheadRows int64 `json:"peak_ahead_rows"`
}

// Snapshot reads g; a nil Gauges reads as zero.
func (g *Gauges) Snapshot() GaugeSnapshot {
	if g == nil {
		return GaugeSnapshot{}
	}
	return GaugeSnapshot{Nodes: g.nodes.n.Load(), PeakNodes: g.nodes.peak.Load(), Markers: g.markers.n.Load(), PeakMarkers: g.markers.peak.Load(),
		SpillReaders: g.readers.n.Load(), PeakSpillReaders: g.readers.peak.Load(), SpillLiveBytes: g.spill.n.Load(), PeakSpillBytes: g.spill.peak.Load(),
		AheadRows: g.ahead.n.Load(), PeakAheadRows: g.ahead.peak.Load()}
}

// batchRows is the minimum number of rows per sink call (except the last).
const batchRows = 4096

type state uint8

const (
	queued state = iota
	expanding
	done
)

type item[R, P any] struct {
	rows  []schema.Row[P]
	units int64 // budget bytes of rows
	child *node[R, P]
	spill *spillFile // rows of this item are the next frame of the node's spill file
}

type node[R, P any] struct {
	r       R
	state   state
	items   []item[R, P]
	pending []*node[R, P] // children not yet pushed to the stack
	spill   *spillFile    // the node's spill file, once it has spilled
	queued  int64         // units published and not yet drained (mu; kept for running nodes only)
	onPath  atomic.Bool   // the drainer's current node: its emitter publishes at once (written under mu)
	awaited atomic.Bool   // the drainer waits for, or itself expands, this node: its emitter never blocks
}

type run[R, P any] struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	p        source.Provider[R, P]
	opt      Options
	mu       sync.Mutex
	work     *sync.Cond // workers: stack empty or budget used
	drain    *sync.Cond // drainer and blocked emitters
	stack    []*node[R, P]
	buffered int64
	meta     int64 // the part of buffered held by metadata: spill markers and child nodes
	max      int64
	frontier time.Duration // Stats.FrontierWait (under mu)
	budget   time.Duration // Stats.BudgetWait (under mu)
	finished bool
	g        *Gauges
	stopped  atomic.Bool // ctx is done; checked instead of ctx.Err(), which takes a mutex

	// Drainer-owned output batch (only the drainer goroutine touches it).
	sp      *spiller[P]                 // nil: no spill
	measure func([]schema.Row[P]) int64 // budget bytes of rows
	sink    Sink[P]
	segs    [][]schema.Row[P]
	segRows int
	rows    int64
	// released: bytes drained but still counted; releasedMeta: the part
	// of it held by metadata (drainer only; returned under mu).
	released, releasedMeta int64
	// The stream-wide order guard: the last key handed on (drainer only).
	last    string
	hasLast bool
	// advanced is when the drainer last handed rows on (or began), stuck
	// the longest interval between two such moments (Stats.FrontierStuck).
	advanced time.Time
	stuck    time.Duration

	received atomic.Int64 // rows the emitters received
}

// dispose releases the ranges that were queued but never expanded (a
// cancelled or failed run); called after the workers have stopped.
func (o *run[R, P]) dispose() {
	d, ok := o.p.(source.Disposer[R])
	if !ok {
		return
	}
	for _, n := range o.stack {
		if n.state == queued {
			n.state = done
			d.Dispose(n.r)
		}
	}
	o.stack = nil
}

// orderError is a broken ordering contract seen by the guard: key, the
// row at index (0-based, in the order handed on), is not after prev.
func orderError(prev, key string, index int64, seam bool) error {
	what := "the previous row"
	if seam && index == 0 {
		what = "the last published key (resume)"
	}
	rel := "descends below"
	if key == prev {
		rel = "repeats"
	}
	return &dataset.InvariantError{Reason: fmt.Sprintf("order: row %d key %q %s %s %q", index, key, rel, what, prev)}
}

// The order stage's waits, by site (hookWait).
const (
	waitIdle      = "idle"      // a worker: no queued range (work.Wait)
	waitAdmission = "admission" // a worker: ranges queued, budget used and spill unavailable (work.Wait)
	waitPublish   = "publish"   // an emitter: row or marker capacity (drain.Wait)
	waitFrontier  = "frontier"  // the drainer: the range it needs is being listed (drain.Wait)
)

// hookWait sees every wait just before it starts (tests; mu held).
var hookWait func(site string)

// waitAt waits on c (mu held), telling hookWait first.
func (o *run[R, P]) waitAt(c *sync.Cond, site string) {
	if hookWait != nil {
		hookWait(site)
	}
	c.Wait()
}

// Test hooks that widen race windows; nil in production.
var hookBeforeDrain, hookAfterExpand func()

// hookStopCallback runs (tests) in the context's stop callback before it
// sets stopped: delaying it opens the window in which only a latch taken
// with the failure keeps the drainer from passing a failed node.
var hookStopCallback func()

// hookMarkers sees the marker bytes held after each publish, hookDrained
// the bytes and marker bytes still counted when the drain ends (tests;
// mu held).
var hookMarkers func(markers int64)
var hookDrained func(buffered, markers int64)

// hookItems sees, after the drainer consumes an item of a running node, the
// consumed items the node still holds and those not yet drained (tests;
// mu held).
var hookItems func(consumed, waiting int)

// compactAt is the number of consumed items of a running node from which
// the drainer drops them, once they are at least half of its items.
const compactAt = 256

// countRows makes the budget count rows instead of bytes (tests).
var countRows bool

// markerUnits is what a spill marker (the item left in a node for a frame
// on disk) counts against the budget: its own size, so a range that spills
// many frames still holds the bound in memory. Nothing when the budget
// counts rows (tests).
func markerUnits() int64 {
	if countRows {
		return 0
	}
	return int64(unsafe.Sizeof(item[struct{}, struct{}]{}))
}

// childUnits is what a child node counts against the budget, as
// metadata: its node and its item. Nothing when the budget counts rows.
func childUnits() int64 {
	if countRows {
		return 0
	}
	return int64(unsafe.Sizeof(node[struct{}, struct{}]{}) + unsafe.Sizeof(item[struct{}, struct{}]{}))
}

// leadLimit is how far the awaited node may run ahead of the drainer past a
// full budget: the units it has published and the drainer not yet taken.
func (o *run[R, P]) leadLimit() int64 { return max(1, o.opt.Budget/4) }

// full reports whether the budget is used up (mu held): bytes buffered here
// plus bytes held downstream.
func (o *run[R, P]) full() bool {
	n := o.buffered
	if o.opt.Shared != nil {
		n += o.opt.Shared.n.Load()
	}
	return n >= o.opt.Budget
}

// Shared counts bytes held downstream of the order stage against its
// budget. Add with a negative delta wakes producers waiting for room.
type Shared struct {
	n    atomic.Int64
	mu   sync.Mutex
	wake func()
}

// Add records d bytes taken (d > 0) or released (d < 0).
func (s *Shared) Add(d int64) {
	s.n.Add(d)
	if d < 0 {
		s.mu.Lock()
		w := s.wake
		s.mu.Unlock()
		if w != nil {
			w()
		}
	}
}

// Load is the bytes held now.
func (s *Shared) Load() int64 { return s.n.Load() }

func (s *Shared) setWake(f func()) {
	s.mu.Lock()
	s.wake = f
	s.mu.Unlock()
}

// send appends rows to the drainer's output batch, writing full batches.
// Every row handed on passes the stream-wide order guard first: keys
// strictly ascending across the whole run (and after Options.After), so no
// sink, Parquet or other, ever receives a descending or repeated key from
// a provider that broke its ordering contract.
func (o *run[R, P]) send(rows []schema.Row[P]) error {
	if len(rows) == 0 {
		return nil
	}
	for i := range rows {
		k := rows[i].Key
		if o.hasLast && k <= o.last {
			return orderError(o.last, k, o.rows+int64(o.segRows+i), o.opt.After != nil)
		}
		o.last, o.hasLast = k, true
	}
	o.stalled()
	o.advanced = time.Now()
	o.g.ahead.add(-int64(len(rows)))
	o.segs = append(o.segs, rows)
	o.segRows += len(rows)
	if o.segRows >= batchRows {
		return o.flush()
	}
	return nil
}

func (o *run[R, P]) flush() error {
	if len(o.segs) == 0 {
		return nil
	}
	o.rows += int64(o.segRows)
	err := o.sink.Write(o.ctx, o.segs)
	o.segs, o.segRows = nil, 0
	return err
}

// Run drains p in ascending key order into sink. It returns the first error
// from the provider or the sink, or the context's error on cancellation.
func Run[R, P any](ctx context.Context, p source.Provider[R, P], sink Sink[P], s *schema.Schema[P], opt Options) (Stats, error) {
	if opt.Workers < 1 || opt.Budget < 1 {
		return Stats{}, errors.New("order: workers and budget must be positive")
	}
	o, closeRun, err := newRun(ctx, p, sink, s, opt)
	if err != nil {
		return Stats{}, err
	}
	defer closeRun()
	ctx, cancel := o.ctx, o.cancel
	root := &node[R, P]{r: p.Root()}
	o.stack = []*node[R, P]{root}
	o.g.nodes.add(1)

	var wg sync.WaitGroup
	for range opt.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.worker()
		}()
	}
	if hookBeforeDrain != nil {
		hookBeforeDrain()
	}
	st, err := o.drainTree(root, sink)
	if err != nil && ctx.Err() != nil {
		// A worker failed first and cancelled the run; report its error,
		// not the cancellation the drainer saw because of it.
		err = context.Cause(ctx)
	}
	o.mu.Lock()
	o.finished = true
	o.work.Broadcast()
	o.drain.Broadcast()
	o.mu.Unlock()
	if err != nil {
		cancel(err)
	}
	wg.Wait()
	if err == nil && ctx.Err() != nil {
		// A worker failed (or the run was cancelled) after the drainer had
		// finished: never report success for a failed listing.
		err = context.Cause(ctx)
	}
	o.dispose()
	st.Rows, st.MaxBuffered, st.FrontierWait, st.BudgetWait = o.rows, o.max, o.frontier, o.budget
	st.Received = o.received.Load()
	st.PeakNodes, st.PeakMarkers, st.PeakReaders = o.g.nodes.peak.Load(), o.g.markers.peak.Load(), o.g.readers.peak.Load()
	st.PeakAhead, st.FrontierStuck = o.g.ahead.peak.Load(), o.stuck
	if err == nil && st.Received != st.Rows {
		// Conservation inside the stage (spill, compaction, replay): a row
		// received and never handed on is a lost row, never a success.
		err = &dataset.InvariantError{Reason: fmt.Sprintf("order stage: received %d rows, handed on %d", st.Received, st.Rows)}
	}
	if o.sp != nil {
		st.SpillRows, st.SpillFrames, st.SpillBytes, st.SpillFiles = o.sp.rows.Load(), o.sp.frames.Load(), o.sp.bytes.Load(), o.sp.files.Load()
		st.SpillPeak, st.SpillRefused = o.sp.peak.Load(), o.sp.refused.Load()
	}
	return st, err
}

// newRun builds a run's state (Run's set-up without the goroutines); the
// returned function releases it.
func newRun[R, P any](ctx context.Context, p source.Provider[R, P], sink Sink[P], s *schema.Schema[P], opt Options) (*run[R, P], func(), error) {
	ctx, cancel := context.WithCancelCause(ctx)
	o := &run[R, P]{ctx: ctx, cancel: cancel, p: p, opt: opt, sink: sink, measure: s.RowsBytes}
	o.work = sync.NewCond(&o.mu)
	o.drain = sync.NewCond(&o.mu)
	o.g = opt.Gauges
	if o.g == nil {
		o.g = &Gauges{}
	}
	if opt.After != nil {
		o.last, o.hasLast = *opt.After, true
	}
	if countRows {
		o.measure = func(rows []schema.Row[P]) int64 { return int64(len(rows)) }
	}
	// wake rouses every waiter: capacity came back (bytes released
	// downstream, spill credit returned) or the run stopped. Each waiter
	// rechecks its own condition.
	wake := func() {
		o.mu.Lock()
		o.work.Broadcast()
		o.drain.Broadcast()
		o.mu.Unlock()
	}
	if opt.SpillDir != "" {
		var sp *spiller[P]
		err := error(nil)
		if opt.SpillVerify != nil {
			err = opt.SpillVerify()
		}
		if err == nil {
			sp, err = newSpiller(s.Codec, opt.SpillDir, opt.SpillCompress, opt.SpillMax, opt.SpillReserve)
		}
		if err != nil {
			cancel(nil)
			return nil, nil, err
		}
		sp.wake, sp.verify = wake, opt.SpillVerify
		sp.g = o.g
		o.sp = sp
	}
	if opt.Shared != nil {
		opt.Shared.setWake(wake)
	}
	hook := hookStopCallback // read now: the callback may outlive the run
	stop := context.AfterFunc(ctx, func() {
		if hook != nil {
			hook()
		}
		o.stopped.Store(true)
		wake()
	})
	return o, func() {
		stop()
		if opt.Shared != nil {
			opt.Shared.setWake(nil)
		}
		if o.sp != nil {
			o.sp.close()
		}
		cancel(nil)
	}, nil
}

func (o *run[R, P]) worker() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for {
		for !o.finished && !o.stopped.Load() && (len(o.stack) == 0 || ((o.sp == nil || o.sp.full.Load()) && o.full())) {
			if len(o.stack) == 0 {
				o.waitAt(o.work, waitIdle)
				continue
			}
			t := time.Now()
			o.waitAt(o.work, waitAdmission)
			o.budget += time.Since(t)
		}
		if o.finished || o.stopped.Load() {
			return
		}
		n := o.stack[len(o.stack)-1]
		o.stack = o.stack[:len(o.stack)-1]
		if n.state != queued { // the drainer took it
			continue
		}
		n.state = expanding
		o.mu.Unlock()
		err := o.expand(n, false)
		if hookAfterExpand != nil {
			hookAfterExpand()
		}
		o.mu.Lock()
		if err != nil {
			o.cancel(err)
			return
		}
	}
}

// expand runs the provider on n and publishes its children; called without
// mu. direct is set when the drainer expands the node it is draining: rows
// up to the first child are then next in global order and go straight to the
// sink, so a long streaming range is not buffered while the drainer is busy.
func (o *run[R, P]) expand(n *node[R, P], direct bool) error {
	return o.expandBy(n, direct, false)
}

// expandBy is expand; drainer is set when the drainer runs it.
func (o *run[R, P]) expandBy(n *node[R, P], direct, drainer bool) error {
	e := &emitter[R, P]{o: o, n: n, direct: direct, drainer: drainer}
	err := o.p.Expand(o.ctx, n.r, e)
	o.mu.Lock()
	defer o.mu.Unlock()
	if err == nil {
		err = e.publish()
	}
	if err != nil {
		// Latch the failure before the node is exposed as done: otherwise
		// the drainer could see a finished tree and succeed first, or pass
		// the failed node and hand on its siblings' rows (a gap: the rows
		// it emitted are dropped, those after it are not). stopped is set
		// here, under mu, not only by the context's AfterFunc, which runs
		// later on its own goroutine.
		o.cancel(err)
		o.stopped.Store(true)
	}
	if err != nil { // children emitted but never published will not be expanded
		if d, ok := o.p.(source.Disposer[R]); ok {
			for _, k := range e.kids {
				d.Dispose(k.r)
			}
		}
		e.kids = nil
	}
	if err == nil && n.spill != nil { // the trailer, before the node is done
		o.mu.Unlock()
		err = o.sp.write(n.spill, nil, true)
		o.mu.Lock()
	}
	n.state = done
	for i := len(n.pending) - 1; i >= 0; i-- {
		o.stack = append(o.stack, n.pending[i])
	}
	if len(n.pending) > 0 {
		o.work.Broadcast()
	}
	n.pending = nil
	if n.onPath.Load() {
		o.drain.Broadcast()
	}
	return err
}

// emitter collects a range's items locally and publishes them under the
// lock in batches: at once when the drainer is on this node, otherwise
// when publishRows rows have accumulated or the range ends. One directory
// therefore costs one lock round trip, while a long streaming range still
// becomes visible to the drainer as it grows.
type emitter[R, P any] struct {
	drainer bool // run by the drainer itself: it can never wait for the drainer
	o       *run[R, P]
	n       *node[R, P]
	items   []item[R, P]
	kids    []*node[R, P]
	rows    int
	units   int64
	direct  bool // drainer-expanded, no child yet: rows go straight to the sink
}

const publishRows = 1024

func (e *emitter[R, P]) Rows(rows []schema.Row[P]) error {
	if len(rows) == 0 {
		return nil
	}
	e.o.received.Add(int64(len(rows)))
	e.o.g.ahead.add(int64(len(rows)))
	if e.direct {
		if e.o.stopped.Load() {
			return context.Cause(e.o.ctx)
		}
		return e.o.send(rows)
	}
	// A batch is published in pieces of at most publishRows rows, so one
	// large Rows call is admitted (and spilled) as several bounded ones.
	for len(rows) > 0 {
		chunk := rows[:min(len(rows), publishRows)]
		rows = rows[len(chunk):]
		u := e.o.measure(chunk)
		e.items = append(e.items, item[R, P]{rows: chunk, units: u})
		e.rows += len(chunk)
		e.units += u
		if err := e.maybePublish(); err != nil {
			return err
		}
	}
	return nil
}

func (e *emitter[R, P]) Child(r R) error {
	e.direct = false
	c := &node[R, P]{r: r}
	u := childUnits()
	e.items = append(e.items, item[R, P]{child: c, units: u})
	e.kids = append(e.kids, c)
	e.units += u
	e.o.g.nodes.add(1)
	return e.maybePublish()
}

// publishItems bounds the items an emitter collects before it publishes:
// a range emitting children without rows is admitted (and charged) in
// bounded batches.
const publishItems = 1024

func (e *emitter[R, P]) maybePublish() error {
	if e.rows < publishRows && len(e.items) < publishItems && !e.n.onPath.Load() {
		return nil
	}
	e.o.mu.Lock()
	defer e.o.mu.Unlock()
	return e.publish()
}

// publish appends the collected items to the node; mu held. Unless the node
// is on the drain path, it first waits while the budget is used up.
func (e *emitter[R, P]) publish() error {
	o := e.o
	if len(e.items) == 0 {
		// Nothing to publish (a range that ends right after a batch): a
		// wait would free nothing and only keep the node from being done,
		// so a worker never waits here. Waiting at every range end while
		// rows far ahead of the frontier fill the budget left the drainer
		// listing the frontier alone.
		if o.stopped.Load() {
			return context.Cause(o.ctx)
		}
		return nil
	}
	refused := false // the disk budget refused the last spill: wait before retrying
	for o.full() && !o.stopped.Load() {
		awaited := e.n.awaited.Load()
		// The awaited node publishes past a full budget (the drainer needs
		// it), except a node the drainer expands itself once its published
		// lead passes leadLimit: nothing drains it until Expand returns,
		// so its rows spill when they can.
		if awaited && !(e.drainer && e.n.queued >= o.leadLimit()) {
			break
		}
		if o.sp != nil && e.rows > 0 && !refused {
			// Spill instead of waiting: write the rows (without the lock),
			// then publish markers. Frames precede their markers, so the
			// drainer never reads ahead of the writer.
			o.mu.Unlock()
			err := e.spill()
			o.mu.Lock()
			if err == nil {
				continue // the markers left pass admission too (below)
			}
			if err != errSpillFull {
				return err
			}
			// The disk budget is used: wait, as without spill. The lock
			// was released, so recheck the condition first (the drainer
			// may have marked this node awaited meanwhile).
			refused = true
			continue
		} else if awaited {
			// The drainer past its lead without spill (off, or refused):
			// it cannot wait for itself. The documented exception.
			break
		} else if e.rows == 0 {
			// Only children and spill markers (metadata): admitted without
			// a wait unless the metadata in memory would by itself fill the
			// budget, so spilling keeps workers busy, a range that ends
			// after its last split is not held back, and the metadata (the
			// speculative tree included) stays bounded.
			if o.meta+e.units < o.opt.Budget {
				break
			}
		}
		t := time.Now()
		o.waitAt(o.drain, waitPublish)
		o.budget += time.Since(t)
		refused = false
	}
	if o.stopped.Load() {
		return context.Cause(o.ctx)
	}
	if len(e.items) == 0 {
		return nil
	}
	for i := range e.items {
		if it := &e.items[i]; it.spill != nil || it.child != nil {
			o.meta += it.units
			if it.spill != nil {
				o.g.markers.add(1)
			}
		}
	}
	e.n.queued += e.units
	if hookMarkers != nil {
		hookMarkers(o.meta)
	}
	e.n.items = append(e.n.items, e.items...)
	e.n.pending = append(e.n.pending, e.kids...)
	o.buffered += e.units
	o.max = max(o.max, o.buffered)
	e.items, e.kids, e.rows, e.units = nil, nil, 0, 0
	if e.n.onPath.Load() {
		o.drain.Broadcast()
	}
	return nil
}

// spill writes the collected row items to the node's spill file and
// replaces them with markers; called without mu (the node's own emitter is
// the only writer of its file).
func (e *emitter[R, P]) spill() error {
	if e.n.spill == nil {
		e.n.spill = e.o.sp.file()
	}
	for i := range e.items {
		if e.items[i].rows == nil {
			continue
		}
		if err := e.o.sp.write(e.n.spill, e.items[i].rows, false); err != nil {
			return err // refused: the items not yet written keep their rows
		}
		e.rows -= len(e.items[i].rows)
		e.units -= e.items[i].units - markerUnits()
		e.items[i] = item[R, P]{spill: e.n.spill, units: markerUnits()}
	}
	return nil
}

// replay reads one spilled frame and sends it; called without mu.
func (o *run[R, P]) replay(f *spillFile) error {
	rows, err := o.sp.next(f)
	if err != nil {
		return err
	}
	if rows == nil {
		return errors.New("spill: trailer before the last frame")
	}
	return o.send(rows)
}

// release returns the bytes drained since the last release to the budget
// (mu held) and wakes every waiter: any release can make a waiter eligible,
// not only one that ends a full budget. A worker held at admission and an
// emitter of rows wait for the total to drop below the budget, but an
// emitter of markers and children waits for the marker bytes alone, which
// can fall while the total stays full.
func (o *run[R, P]) release() {
	if o.released == 0 {
		return
	}
	o.buffered -= o.released
	o.meta -= o.releasedMeta
	o.released, o.releasedMeta = 0, 0
	o.work.Broadcast()
	o.drain.Broadcast()
}

// drainFrame is a node on the drain path and the index of its next item.
type drainFrame[R, P any] struct {
	n *node[R, P]
	i int
}

// drainTree walks the tree depth-first from root, streaming rows to sink.
// Each step takes the top frame's next item (drainFinished without the
// lock for a finished node, drainNext for one still being listed), or,
// with its items consumed, closes a finished node (closeNode), expands a
// queued one here (expandHere) or waits on the frontier (awaitFrontier).
// The steps run with o.mu held and return with it held.
func (o *run[R, P]) drainTree(root *node[R, P], sink Sink[P]) (Stats, error) {
	var st Stats
	releaseEvery := max(1, o.opt.Budget/16)
	frames := []drainFrame[R, P]{{n: root}}
	o.advanced = time.Now()
	o.mu.Lock()
	root.onPath.Store(true)
	for len(frames) > 0 {
		if o.stopped.Load() {
			o.mu.Unlock()
			o.stalled()
			return st, context.Cause(o.ctx)
		}
		f := &frames[len(frames)-1]
		n := f.n
		var child *node[R, P]
		var err error
		switch {
		case n.state == done && f.i < len(n.items):
			child, err = o.drainFinished(f, releaseEvery)
		case f.i < len(n.items):
			child, err = o.drainNext(f, releaseEvery)
		case n.state == done:
			if err = o.closeNode(n); err == nil {
				frames = frames[:len(frames)-1]
				o.g.nodes.add(-1)
				if len(frames) > 0 {
					frames[len(frames)-1].n.onPath.Store(true)
					o.drain.Broadcast() // its emitter may be blocked on the budget
				}
			}
		case n.state == queued:
			err = o.expandHere(n, f.i == 0)
		case n.state == expanding:
			err = o.awaitFrontier(n)
		}
		if err != nil {
			o.mu.Unlock()
			o.stalled()
			return st, err
		}
		if child != nil {
			n.onPath.Store(false)
			child.onPath.Store(true)
			frames = append(frames, drainFrame[R, P]{n: child})
		}
	}
	o.release()
	if hookDrained != nil {
		hookDrained(o.buffered, o.meta)
	}
	o.mu.Unlock()
	o.stalled()
	return st, o.flush()
}

// stalled closes the interval since the drainer last handed rows on into
// stuck. Every drain exit calls it, cancellation and errors included: an
// interrupted run's last stall (a frontier that never answered before the
// stop) is the one its report most needs.
func (o *run[R, P]) stalled() { o.stuck = max(o.stuck, time.Since(o.advanced)) }

// drainFinished drains a finished node's items up to its next child,
// which it returns. A finished node's items are immutable: they are
// drained without the lock, which is retaken to release budget.
func (o *run[R, P]) drainFinished(f *drainFrame[R, P], releaseEvery int64) (*node[R, P], error) {
	items := f.n.items
	o.mu.Unlock()
	var child *node[R, P]
	var err error
	for f.i < len(items) && err == nil {
		it := items[f.i]
		items[f.i] = item[R, P]{}
		f.i++
		if it.child != nil {
			o.released += it.units
			o.releasedMeta += it.units
			child = it.child
			break
		}
		if it.spill != nil {
			o.released += it.units
			o.releasedMeta += it.units
			o.g.markers.add(-1)
			err = o.replay(it.spill)
			if o.released >= releaseEvery {
				o.mu.Lock()
				o.release()
				o.mu.Unlock()
			}
			continue
		}
		o.released += it.units
		err = o.send(it.rows)
		if o.released >= releaseEvery {
			o.mu.Lock()
			o.release()
			o.mu.Unlock()
		}
	}
	o.mu.Lock()
	return child, err
}

// drainNext takes the next item of a node still being listed: a child is
// returned to descend into; rows and spilled frames are sent without the
// lock.
func (o *run[R, P]) drainNext(f *drainFrame[R, P], releaseEvery int64) (*node[R, P], error) {
	n := f.n
	it := n.items[f.i]
	n.items[f.i] = item[R, P]{}
	f.i++
	// A running node keeps growing at the end while the drainer
	// consumes it from the front: drop the consumed slots (with
	// their backing storage) once they are half of it, so a long
	// streaming range retains only what is not yet drained.
	if f.i >= compactAt && 2*f.i >= len(n.items) {
		n.items = append([]item[R, P](nil), n.items[f.i:]...)
		f.i = 0
	}
	if hookItems != nil {
		hookItems(f.i, len(n.items)-f.i)
	}
	if it.child != nil {
		o.released += it.units
		o.releasedMeta += it.units
		return it.child, nil
	}
	if it.spill != nil {
		o.released += it.units
		o.releasedMeta += it.units
		o.g.markers.add(-1)
		if o.released >= releaseEvery {
			o.release()
		}
		o.mu.Unlock()
		err := o.replay(it.spill)
		o.mu.Lock()
		return nil, err
	}
	o.released += it.units
	if o.released >= releaseEvery {
		o.release()
	}
	o.mu.Unlock()
	err := o.send(it.rows)
	o.mu.Lock()
	return nil, err
}

// closeNode finishes a drained node: a spilled node's trailer is read
// (the count checked, the file deleted) and its items dropped.
func (o *run[R, P]) closeNode(n *node[R, P]) error {
	if n.spill != nil {
		f := n.spill
		n.spill = nil
		o.mu.Unlock()
		rows, err := o.sp.next(f)
		o.mu.Lock()
		if err == nil && rows != nil {
			err = errors.New("spill: frames after the last marker")
		}
		if err != nil {
			return err
		}
	}
	n.items = nil
	n.onPath.Store(false)
	return nil
}

// expandHere expands a queued node nobody has claimed, on the drainer.
func (o *run[R, P]) expandHere(n *node[R, P], direct bool) error {
	if len(o.segs) > 0 {
		// Hand on what is batched before listing here, as before a wait on
		// the frontier: the expansion is a request that may take long, and
		// the sink (a part's age bound, its durability) must see every row
		// the walk has reached meanwhile. The caller rechecks after (a
		// worker may have taken the node).
		o.mu.Unlock()
		err := o.flush()
		o.mu.Lock()
		return err
	}
	n.state = expanding
	n.awaited.Store(true)
	o.release()
	o.mu.Unlock()
	err := o.expandBy(n, direct, true)
	o.mu.Lock()
	return err
}

// awaitFrontier waits for the node being expanded elsewhere to publish,
// after handing on what is batched.
func (o *run[R, P]) awaitFrontier(n *node[R, P]) error {
	if len(o.segs) > 0 {
		// Hand on what is batched before waiting on the frontier:
		// the sink (a part's age bound, its durability) sees every
		// row the walk has reached. The caller rechecks after.
		o.mu.Unlock()
		err := o.flush()
		o.mu.Lock()
		return err
	}
	o.release()
	n.awaited.Store(true)
	o.drain.Broadcast() // its emitter may be blocked on the budget
	t := time.Now()
	o.waitAt(o.drain, waitFrontier)
	o.frontier += time.Since(t)
	n.awaited.Store(false)
	return nil
}
