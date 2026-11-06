// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// The order stage's waits, each driven to its wait with a deterministic
// barrier (hookWait) and then released by exactly one event: the event
// must wake it promptly. Sort consult (Astra), section 3, lists them.

// op is one step of a scripted range: rows, a child, or the end.
type op struct {
	rows  []string
	child string
	end   bool
}

// ctl is a provider whose ranges (named by string) follow ops the test
// sends; started reports each range's first step.
type ctl struct {
	mu      sync.Mutex
	ops     map[string]chan op
	started chan string
}

func newCtl() *ctl { return &ctl{ops: map[string]chan op{}, started: make(chan string, 16)} }

func (c *ctl) ch(r string) chan op {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ops[r] == nil {
		c.ops[r] = make(chan op)
	}
	return c.ops[r]
}

func (c *ctl) Root() string { return "root" }

func (c *ctl) Expand(ctx context.Context, r string, e source.Emitter[string, tp]) error {
	c.started <- r
	ch := c.ch(r)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case o := <-ch:
			switch {
			case o.end:
				return nil
			case o.child != "":
				if err := e.Child(o.child); err != nil {
					return err
				}
			default:
				rows := make([]schema.Row[tp], len(o.rows))
				for i, k := range o.rows {
					rows[i].Key = k
				}
				if err := e.Rows(rows); err != nil {
					return err
				}
			}
		}
	}
}

// waits records the wait sites as they start.
func waits(t *testing.T) chan string {
	ch := make(chan string, 256)
	hookWait = func(site string) {
		select {
		case ch <- site:
		default:
		}
	}
	t.Cleanup(func() { hookWait = nil })
	return ch
}

func expectWait(t *testing.T, ch chan string, site string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case s := <-ch:
			if s == site {
				return
			}
		case <-deadline:
			t.Fatalf("never waited at %s", site)
		}
	}
}

// prompt is how long a released wait may take to proceed.
const prompt = 2 * time.Second

func expect[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(prompt):
		t.Fatalf("%s: the wait was not released", what)
	}
	var zero T
	return zero
}

// bg runs f as one of the test's goroutines: testRun's cleanup stops the
// run and waits for them before the hooks are reset.
func bg(o *run[string, tp], f func()) {
	wg, _ := testWGs.LoadOrStore(o, &sync.WaitGroup{})
	wg.(*sync.WaitGroup).Go(f)
}

var testWGs sync.Map // *run -> *sync.WaitGroup

func testRun(t *testing.T, p *ctl, opt Options) *run[string, tp] {
	t.Helper()
	if opt.Workers == 0 {
		opt.Workers = 1
	}
	o, closeRun, err := newRun[string, tp](context.Background(), p, &collect{}, &testSchema, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		o.cancel(errors.New("test over"))
		if wg, ok := testWGs.LoadAndDelete(o); ok {
			wg.(*sync.WaitGroup).Wait()
		}
		closeRun()
	})
	return o
}

// TestAdmissionWaitWakes: a worker with a range queued and the budget used
// (spill off, or the disk budget refusing) waits at admission; each event
// that makes it eligible wakes it: drained bytes released, bytes released
// downstream, spill credit returned (the file of a drained range deleted),
// and cancellation. Before, the spill credit cleared the refusal without a
// broadcast, and the worker slept on.
func TestAdmissionWaitWakes(t *testing.T) {
	for _, event := range []string{"release", "shared", "spill credit", "cancel"} {
		t.Run(event, func(t *testing.T) {
			ws := waits(t)
			p := newCtl()
			opt := Options{Budget: 1}
			if event == "shared" {
				opt.Shared = &Shared{}
			}
			if event == "spill credit" {
				opt.SpillDir = t.TempDir()
			}
			o := testRun(t, p, opt)
			o.mu.Lock()
			o.stack = []*node[string, tp]{{r: "x"}}
			switch event {
			case "shared":
				opt.Shared.n.Store(1)
			case "spill credit":
				o.buffered = 1
				o.sp.full.Store(true) // the disk budget refused the last frame
			default:
				o.buffered = 1
			}
			o.mu.Unlock()
			done := make(chan struct{})
			bg(o, func() { o.worker(); close(done) })
			expectWait(t, ws, waitAdmission)
			switch event {
			case "release":
				o.mu.Lock()
				o.released = 1
				o.release()
				o.mu.Unlock()
			case "shared":
				opt.Shared.Add(-1)
			case "spill credit":
				o.sp.release(&spillFile{}) // the budget stays full: spill takes the range
			case "cancel":
				o.cancel(errors.New("stop"))
				expect(t, done, event)
				return
			}
			if r := expect(t, p.started, event); r != "x" {
				t.Fatalf("claimed %q", r)
			}
			p.ch("x") <- op{end: true}
		})
	}
}

// TestIdleWorkerWakesOnChildren: an idle worker (nothing queued) is woken
// when a range completes with only children (control-only completion).
func TestIdleWorkerWakesOnChildren(t *testing.T) {
	ws := waits(t)
	p := newCtl()
	o := testRun(t, p, Options{Budget: 1})
	n := &node[string, tp]{r: "x", state: expanding}
	bg(o, func() { _ = o.expand(n, false) })
	expect(t, p.started, "x")
	bg(o, o.worker)
	expectWait(t, ws, waitIdle)
	p.ch("x") <- op{child: "a"}
	p.ch("x") <- op{child: "b"}
	p.ch("x") <- op{end: true}
	if r := expect(t, p.started, "children"); r != "a" {
		t.Fatalf("claimed %q, want the first child", r)
	}
}

// publishing starts an emitter of node n publishing items (mu taken here);
// its result arrives on the channel.
func publishing(o *run[string, tp], n *node[string, tp], items []item[string, tp], rows int) chan error {
	done := make(chan error, 1)
	bg(o, func() {
		e := &emitter[string, tp]{o: o, n: n, items: items, rows: rows}
		for _, it := range items {
			e.units += it.units
		}
		o.mu.Lock()
		err := e.publish()
		o.mu.Unlock()
		done <- err
	})
	return done
}

// TestPublishWaitWakes: an emitter of rows facing a full budget (spill
// off) waits at publish; drained bytes released, bytes released
// downstream, the drainer awaiting its node, and cancellation each wake
// it.
func TestPublishWaitWakes(t *testing.T) {
	for _, event := range []string{"release", "shared", "awaited", "cancel"} {
		t.Run(event, func(t *testing.T) {
			ws := waits(t)
			opt := Options{Budget: 1}
			if event == "shared" {
				opt.Shared = &Shared{}
				opt.Shared.n.Store(1)
			}
			o := testRun(t, newCtl(), opt)
			if event != "shared" {
				o.buffered = 1
			}
			n := &node[string, tp]{r: "x", state: expanding}
			done := publishing(o, n, []item[string, tp]{{rows: []schema.Row[tp]{{Key: "k"}}, units: 1}}, 1)
			expectWait(t, ws, waitPublish)
			switch event {
			case "release":
				o.mu.Lock()
				o.released = 1
				o.release()
				o.mu.Unlock()
			case "shared":
				opt.Shared.Add(-1)
			case "awaited": // what the drainer does when it reaches the node
				o.mu.Lock()
				n.awaited.Store(true)
				o.drain.Broadcast()
				o.mu.Unlock()
			case "cancel":
				o.cancel(errors.New("stop"))
				if err := expect(t, done, event); err == nil {
					t.Fatal("a cancelled publish succeeded")
				}
				return
			}
			if err := expect(t, done, event); err != nil {
				t.Fatal(err)
			}
			if len(n.items) != 1 {
				t.Fatalf("published %d items", len(n.items))
			}
		})
	}
}

// TestMarkerSaturationWakes: an emitter holding only spill markers waits
// while the markers in memory would fill the budget by themselves. The
// drainer replaying markers lowers them while the total stays full (rows
// held downstream): that release must wake it. Before, a release woke
// waiters only when it ended a full budget, and the emitter slept on.
func TestMarkerSaturationWakes(t *testing.T) {
	countRows = false
	defer func() { countRows = true }()
	ws := waits(t)
	mu := markerUnits()
	opt := Options{Budget: 3 * mu, SpillDir: t.TempDir(), Shared: &Shared{}}
	o := testRun(t, newCtl(), opt)
	opt.Shared.n.Store(10 * mu) // the encoders hold the budget: it stays full
	o.meta, o.buffered = 2*mu, 2*mu
	n := &node[string, tp]{r: "x", state: expanding}
	f := o.sp.file()
	done := publishing(o, n, []item[string, tp]{{spill: f, units: mu}}, 0)
	expectWait(t, ws, waitPublish)
	o.mu.Lock()
	o.released, o.releasedMeta = mu, mu // one marker replayed
	o.release()
	full := o.full()
	o.mu.Unlock()
	if !full {
		t.Fatal("the budget must stay full for this test")
	}
	if err := expect(t, done, "marker release"); err != nil {
		t.Fatal(err)
	}
}

// TestRefusedSpillWakesOnCredit: an emitter whose frame the disk budget
// refused waits at publish; the credit of a deleted spill file must wake
// it to spill again (the reorder budget stays full). Before, the credit
// was returned without a broadcast.
func TestRefusedSpillWakesOnCredit(t *testing.T) {
	ws := waits(t)
	o := testRun(t, newCtl(), Options{Budget: 1, SpillDir: t.TempDir(), SpillMax: 1})
	o.buffered = 1
	n := &node[string, tp]{r: "x", state: expanding}
	done := publishing(o, n, []item[string, tp]{{rows: []schema.Row[tp]{{Key: "k"}}, units: 1}}, 1)
	expectWait(t, ws, waitPublish)
	if o.sp.refused.Load() == 0 {
		t.Fatal("the frame was not refused")
	}
	o.mu.Lock()
	o.sp.max = 1 << 30 // room on disk again ...
	o.mu.Unlock()
	o.sp.release(&spillFile{}) // ... returned by a drained range's file
	if err := expect(t, done, "spill credit"); err != nil {
		t.Fatal(err)
	}
	if len(n.items) != 1 || n.items[0].spill == nil {
		t.Fatalf("the rows did not spill: %+v", n.items)
	}
}

// TestFrontierWaitWakes: the drainer waits for the range it needs while a
// worker lists it; the worker publishing rows, completing with nothing
// (control-only), completing with only a child, and cancellation each
// wake it.
func TestFrontierWaitWakes(t *testing.T) {
	for _, event := range []string{"rows", "control-only", "child-only", "cancel"} {
		t.Run(event, func(t *testing.T) {
			ws := waits(t)
			p := newCtl()
			o := testRun(t, p, Options{Budget: 1 << 20})
			root := &node[string, tp]{r: "root", state: expanding} // a worker claimed it
			bg(o, func() { _ = o.expand(root, false) })
			expect(t, p.started, "root")
			done := make(chan error, 1)
			bg(o, func() {
				_, err := o.drainTree(root, o.sink)
				if err == nil && o.ctx.Err() != nil {
					err = context.Cause(o.ctx) // as Run does after the drain
				}
				done <- err
			})
			expectWait(t, ws, waitFrontier)
			switch event {
			case "rows":
				p.ch("root") <- op{rows: []string{"a", "b"}}
				expectWait(t, ws, waitFrontier) // consumed, waiting again
				p.ch("root") <- op{end: true}
			case "control-only":
				p.ch("root") <- op{end: true}
			case "child-only":
				p.ch("root") <- op{child: "c"}
				p.ch("root") <- op{end: true}
				expect(t, p.started, "the child") // the drainer expands it itself
				p.ch("c") <- op{rows: []string{"c1"}}
				p.ch("c") <- op{end: true}
			case "cancel":
				o.cancel(errors.New("stop"))
				if err := expect(t, done, event); err == nil {
					t.Fatal("a cancelled drain succeeded")
				}
				return
			}
			if err := expect(t, done, event); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestChildOnlyPublishIsAdmitted: a publish of children alone (a range
// that ends after its last split) never waits on a full budget, spill on
// or off: waiting would free nothing and hold the subtree back from the
// drainer. Before, with spill off, it waited for the budget.
func TestChildOnlyPublishIsAdmitted(t *testing.T) {
	for _, spill := range []bool{false, true} {
		opt := Options{Budget: 1}
		if spill {
			opt.SpillDir = t.TempDir()
		}
		o := testRun(t, newCtl(), opt)
		o.buffered = 1
		n := &node[string, tp]{r: "x", state: expanding}
		done := publishing(o, n, []item[string, tp]{{child: &node[string, tp]{r: "c"}}}, 0)
		if err := expect(t, done, "children"); err != nil {
			t.Fatal(err)
		}
	}
}

// leadP: the root emits a child, then many rows after it (a generic
// provider; object-store splits return right after their children).
type leadP struct{ batches int }

func (leadP) Root() string { return "root" }
func (l leadP) Expand(_ context.Context, r string, e source.Emitter[string, tp]) error {
	if r == "c" {
		return e.Rows([]schema.Row[tp]{{Key: "a"}})
	}
	if err := e.Child("c"); err != nil {
		return err
	}
	for b := range l.batches {
		rows := make([]schema.Row[tp], 1000)
		for i := range rows {
			rows[i].Key = fmt.Sprintf("b%03d-%04d", b, i)
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
	}
	return nil
}

// TestDrainerExpandedLeadIsBounded: a range the drainer expands itself
// cannot be drained until its Expand returns, so past a full budget its
// published lead is capped (leadLimit) and its rows spill. Before, every
// row after its first child was held in memory.
func TestDrainerExpandedLeadIsBounded(t *testing.T) {
	const budget = 1000
	o, closeRun, err := newRun[string, tp](context.Background(), leadP{batches: 50}, &collect{}, &testSchema, Options{Workers: 1, Budget: budget, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer closeRun()
	root := &node[string, tp]{r: "root"}
	if _, err := o.drainTree(root, o.sink); err != nil { // no workers: the drainer expands everything
		t.Fatal(err)
	}
	if limit := int64(budget) + o.leadLimit() + 2*publishRows; o.max > limit || o.sp.rows.Load() == 0 {
		t.Fatalf("buffered peaked at %d rows (limit %d), %d spilled", o.max, limit, o.sp.rows.Load())
	}
	if o.rows != 50_001 {
		t.Fatalf("%d rows", o.rows)
	}
}
