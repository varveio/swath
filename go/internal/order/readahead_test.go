// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// rnode is a range of a random tree: rows batches and children, in key
// order.
type rnode struct {
	prefix string
	items  []ritem
}

type ritem struct {
	rows  int
	child *rnode
}

// rtree is a random tree whose first child of the root is held until
// every range outside its subtree has finished: the drainer is stuck on
// it while everything else lists and spills, then replays it all.
type rtree struct {
	root, first *rnode
	hold        chan struct{}
	left        atomic.Int64
	once        sync.Once
}

func genTree(r *rand.Rand) *rtree {
	var gen func(prefix string, depth int) *rnode
	gen = func(prefix string, depth int) *rnode {
		n := &rnode{prefix: prefix}
		for i := range 1 + r.IntN(6) {
			if depth < 4 && r.IntN(3) == 0 {
				n.items = append(n.items, ritem{child: gen(fmt.Sprintf("%s%02d/", prefix, i), depth+1)})
				continue
			}
			// Batches from one row to past a publish batch, so frames of
			// every size, a large Rows call split in pieces.
			sizes := []int{1, 7, 100, 999, 1024, 1500}
			n.items = append(n.items, ritem{rows: sizes[r.IntN(len(sizes))]})
		}
		return n
	}
	root := &rnode{}
	t := &rtree{root: root, hold: make(chan struct{})}
	t.first = gen("00/", 1)
	root.items = append(root.items, ritem{child: t.first})
	for i := 1; i < 3+r.IntN(20); i++ {
		root.items = append(root.items, ritem{child: gen(fmt.Sprintf("%02d/", i), 1)})
	}
	var count func(n *rnode) int64
	count = func(n *rnode) int64 {
		c := int64(1)
		for _, it := range n.items {
			if it.child != nil {
				c += count(it.child)
			}
		}
		return c
	}
	t.left.Store(count(root) - 1 - count(t.first))
	if t.left.Load() == 0 {
		close(t.hold)
	}
	return t
}

func (t *rtree) Root() *rnode { return t.root }

func (t *rtree) Expand(ctx context.Context, n *rnode, e source.Emitter[*rnode, tp]) error {
	if n == t.first {
		select {
		case <-t.hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i, it := range n.items {
		if it.child != nil {
			if err := e.Child(it.child); err != nil {
				return err
			}
			continue
		}
		rows := make([]schema.Row[tp], it.rows)
		for j := range rows {
			rows[j] = schema.Row[tp]{Key: fmt.Sprintf("%s%02d-%05d", n.prefix, i, j), Data: tp{Size: int64(j), ETag: n.prefix}}
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
	}
	if n != t.root && !t.inFirst(n) && t.left.Add(-1) == 0 {
		t.once.Do(func() { close(t.hold) })
	}
	return nil
}

func (t *rtree) inFirst(n *rnode) bool {
	p := t.first.prefix
	return len(n.prefix) >= len(p) && n.prefix[:len(p)] == p
}

func (t *rtree) keys() []string {
	var out []string
	var walk func(n *rnode)
	walk = func(n *rnode) {
		for i, it := range n.items {
			if it.child != nil {
				walk(it.child)
				continue
			}
			for j := range it.rows {
				out = append(out, fmt.Sprintf("%s%02d-%05d", n.prefix, i, j))
			}
		}
	}
	walk(t.root)
	return out
}

// replayed checks that every spilled frame was replayed through exactly
// one path: read ahead, taken over by the drainer, or unplanned.
func replayed(t *testing.T, st Stats) {
	t.Helper()
	ra := st.ReadAhead
	if got := ra.Frames + ra.DrainerFrames + ra.Unplanned; got != st.SpillFrames {
		t.Fatalf("%d frames replayed (%+v), %d spilled", got, ra, st.SpillFrames)
	}
}

// withAhead turns the read-ahead on whatever the processor count, so these
// tests exercise it under GOMAXPROCS=1 too.
func withAhead(t *testing.T) {
	prev := aheadProcs
	aheadProcs = func() int { return 1 + aheadReads }
	t.Cleanup(func() { aheadProcs = prev })
}

// TestReadAheadOffOnOneProcessor: with one processor there is no
// read-ahead (its reads would only take turns with the drainer): every
// frame is the drainer's, the output the oracle's.
func TestReadAheadOffOnOneProcessor(t *testing.T) {
	prev := aheadProcs
	aheadProcs = func() int { return 1 }
	t.Cleanup(func() { aheadProcs = prev })
	f := &frontier{slowPages: 2, siblings: 30, sibPages: 4, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	c := &collect{}
	st, err := Run[fr, tp](context.Background(), f, c, &testSchema, Options{Workers: 8, Budget: 500, SpillDir: t.TempDir()})
	if err != nil || !slices.Equal(c.keys, f.keys()) {
		t.Fatalf("err %v, %d keys", err, len(c.keys))
	}
	if ra := st.ReadAhead; ra.MaxReads != 0 || ra.Frames != 0 || ra.DrainerFrames != 0 || st.SpillFrames == 0 {
		t.Fatalf("read-ahead %+v with one processor (%d frames spilled)", ra, st.SpillFrames)
	}
}

// TestReadAheadKeepsOrder: random trees (node shapes, frame sizes, budgets,
// compression) whose frontier is held until the rest has listed and
// spilled (one worker holds it, so at least two), so the drain is a
// replay the idle workers read ahead of. The
// output is the oracle's, row for row; every frame is replayed once; and
// the read-ahead engaged.
func TestReadAheadKeepsOrder(t *testing.T) {
	withAhead(t)
	var ahead int64
	for seed := range uint64(24) {
		r := rand.New(rand.NewPCG(seed, 7))
		tr := genTree(r)
		want := tr.keys()
		opt := Options{Workers: 2 + r.IntN(15), Budget: int64(50 + r.IntN(3000)), SpillDir: t.TempDir(), SpillCompress: r.IntN(2) == 0}
		c := &collect{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		st, err := Run[*rnode, tp](ctx, tr, c, &testSchema, opt)
		cancel()
		if err != nil {
			t.Fatalf("seed %d %+v: %v", seed, opt, err)
		}
		if !slices.Equal(c.keys, want) {
			t.Fatalf("seed %d %+v: %d keys, want %d", seed, opt, len(c.keys), len(want))
		}
		replayed(t, st)
		ahead += st.ReadAhead.Frames
	}
	if ahead == 0 && runtime.GOMAXPROCS(0) > 1 {
		t.Fatal("the read-ahead never engaged")
	}
}

// heldBoundRun runs the held-frontier fixture of the memory bound tests:
// a tiny budget, 16 workers, everything ahead of a held frontier spilled.
// It requires the stage's bound: the budget, one publish batch per worker,
// the frontier path, and a share of the budget (or one batch) for the
// read-ahead or the segments.
func heldBoundRun(t *testing.T, sink Sink[tp]) (Stats, *frontier) {
	t.Helper()
	f := &frontier{slowPages: 4, siblings: 60, sibPages: 6, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	const workers, budget = 16, 500
	st, err := Run[fr, tp](context.Background(), f, sink, &testSchema, Options{Workers: workers, Budget: budget, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("max buffered %d; read-ahead %+v; drainer %+v; segments %+v", st.MaxBuffered, st.ReadAhead, st.Drainer, st.Segments)
	if limit := int64(budget+workers*publishRows+f.rows) + max(budget/4, publishRows); st.MaxBuffered > limit {
		t.Fatalf("max buffered %d > %d", st.MaxBuffered, limit)
	}
	return st, f
}

// TestReadAheadMemoryIsBounded: the read-ahead holds at most its share of
// the budget (or one frame) on top of the stage's bound (heldBoundRun).
func TestReadAheadMemoryIsBounded(t *testing.T) {
	withAhead(t)
	c := &collect{}
	st, f := heldBoundRun(t, c)
	if !slices.Equal(c.keys, f.keys()) {
		t.Fatalf("%d keys, want %d", len(c.keys), len(f.keys()))
	}
	replayed(t, st)
	// On one processor the drainer may replay every frame before an idle
	// worker is scheduled (DrainerFrames): correct, and nothing to bound.
	if st.ReadAhead.Frames == 0 && runtime.GOMAXPROCS(0) > 1 {
		t.Fatal("the read-ahead never engaged")
	}
}

// TestReadAheadCancelMidReplay: a run cancelled while the workers read
// ahead returns the cause, closes every read descriptor, removes its spill
// files and leaves no goroutine behind.
func TestReadAheadCancelMidReplay(t *testing.T) {
	withAhead(t)
	base := runtime.NumGoroutine()
	f := &frontier{slowPages: 2, siblings: 60, sibPages: 6, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	ctx, cancel := context.WithCancelCause(context.Background())
	var reads atomic.Int64
	hookReadAt = func(*spillFile, spillAt, bool) {
		if reads.Add(1) == 20 {
			cancel(errors.New("stop mid-replay"))
		}
	}
	t.Cleanup(func() { hookReadAt = nil })
	dir := t.TempDir()
	g := &Gauges{}
	_, err := Run[fr, tp](ctx, f, &collect{}, &testSchema, Options{Workers: 16, Budget: 500, SpillDir: dir, Gauges: g})
	if err == nil || err.Error() != "stop mid-replay" {
		t.Fatalf("err %v, want the cause", err)
	}
	if reads.Load() < 20 {
		t.Fatalf("only %d frames read ahead: the cancellation was not mid-replay", reads.Load())
	}
	if n := g.readers.n.Load(); n != 0 {
		t.Fatalf("%d spill descriptors open after the run", n)
	}
	if ents := readDir(t, dir); len(ents) != 0 {
		t.Fatalf("spill files left: %d", len(ents))
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base {
		t.Fatalf("%d goroutines after the run, %d before", n, base)
	}
}

// TestReadAheadCorruptFrame: a planned frame fails its CRC.
// The run fails with the frame error, and the stream stops at that frame:
// the sink received an exact prefix of the output that ends before the
// frame's first row.
func TestReadAheadCorruptFrame(t *testing.T) {
	withAhead(t)
	f := &frontier{slowPages: 2, siblings: 40, sibPages: 6, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	var sp *spiller[tp]
	var first string
	var n atomic.Int64
	var mu sync.Mutex
	hookReadAt = func(file *spillFile, at spillAt, drainer bool) {
		if n.Add(1) != 10 { // a worker's read, or the drainer's of a planned frame
			return
		}
		mu.Lock()
		defer mu.Unlock()
		rows, _, err := sp.readAt(file, at.off, at.size)
		if err != nil || len(rows) == 0 {
			panic(fmt.Sprintf("the frame to corrupt: %v", err))
		}
		first = rows[0].Key
		fh, err := os.OpenFile(file.path, os.O_WRONLY, 0)
		if err != nil {
			panic(err)
		}
		defer fh.Close()
		if _, err := fh.WriteAt([]byte{0xff, 0xee}, at.off+at.size-2); err != nil { // the payload's tail
			panic(err)
		}
	}
	t.Cleanup(func() { hookReadAt = nil })
	c := &collect{}
	o, closeRun, err := newRun[fr, tp](context.Background(), f, c, &testSchema, Options{Workers: 16, Budget: 500, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	closeRun()
	sp = o.sp // a spiller of the same codec reads the frame before it is damaged
	_, err = Run[fr, tp](context.Background(), f, c, &testSchema, Options{Workers: 16, Budget: 500, SpillDir: t.TempDir()})
	if !errors.Is(err, errSpillFrame) {
		t.Fatalf("err %v, want a corrupt frame", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if first == "" {
		t.Fatal("no frame was corrupted")
	}
	want := f.keys()
	if !slices.Equal(c.keys, want[:len(c.keys)]) {
		t.Fatal("the sink received rows out of order")
	}
	if len(c.keys) > 0 && c.keys[len(c.keys)-1] >= first {
		t.Fatalf("rows from the corrupt frame on were handed on: last %q, frame starts at %q", c.keys[len(c.keys)-1], first)
	}
}

// waitsAt records the waits at one site: hundreds of idle waits must not
// crowd it out of the buffer.
func waitsAt(t *testing.T, site string) chan string {
	ch := make(chan string, 16)
	hookWait = func(s string) {
		if s == site {
			select {
			case ch <- s:
			default:
			}
		}
	}
	t.Cleanup(func() { hookWait = nil })
	return ch
}

// TestFetchWaitWakes: the drainer reaches a frame a worker is still reading
// and waits (waitFetch); the read completing wakes it, and so does a stop.
func TestFetchWaitWakes(t *testing.T) {
	withAhead(t)
	for _, event := range []string{"read", "cancel"} {
		t.Run(event, func(t *testing.T) {
			ws := waitsAt(t, waitFetch)
			gate := make(chan struct{})
			var once sync.Once
			hookReadAt = func(_ *spillFile, _ spillAt, drainer bool) {
				if drainer {
					// Slow the drainer's own reads, so workers claim frames
					// even on one processor.
					time.Sleep(time.Millisecond)
					return
				}
				once.Do(func() { <-gate }) // the first worker read stalls
			}
			t.Cleanup(func() { hookReadAt = nil })
			f := &frontier{slowPages: 1, siblings: 20, sibPages: 3, rows: 100}
			f.hold, f.left = make(chan struct{}), &atomic.Int64{}
			f.left.Store(int64(f.siblings))
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			done := make(chan error, 1)
			c := &collect{}
			go func() {
				_, err := Run[fr, tp](ctx, f, c, &testSchema, Options{Workers: 8, Budget: 500, SpillDir: t.TempDir()})
				done <- err
			}()
			expectWait(t, ws, waitFetch)
			if event == "cancel" {
				cancel(errors.New("stop"))
			}
			close(gate)
			err := expect(t, done, event)
			if event == "cancel" {
				if err == nil {
					t.Fatal("a cancelled drain succeeded")
				}
				return
			}
			if err != nil || !slices.Equal(c.keys, f.keys()) {
				t.Fatalf("err %v, %d keys", err, len(c.keys))
			}
		})
	}
}

// blockingSink holds every write once it has taken after rows, until
// opened: the drainer has begun the replay (planned frames) by then.
type blockingSink struct {
	collect
	after int
	open  chan struct{}
}

func (b *blockingSink) Write(ctx context.Context, segs [][]schema.Row[tp]) error {
	b.mu.Lock()
	n := len(b.keys)
	b.mu.Unlock()
	if n >= b.after {
		<-b.open
	}
	return b.collect.Write(ctx, segs)
}

// TestAheadWaitWakes: with the drainer held by its sink, workers read ahead
// up to the read-ahead's share of the budget and then wait (waitAhead); the
// drainer taking the frames read wakes them, and so does a stop.
//
// The slow range is held until every other range is done (Run's
// "one-range-open"), not only listed: the read-ahead plans frames only in
// finished nodes, so a sibling still completing when the drainer passed
// the slow range stopped the plan at it, the workers had nothing to read
// and idled instead of waiting at the share (#325). Released this way,
// every sibling's frames can be planned once the drainer is past it.
func TestAheadWaitWakes(t *testing.T) {
	withAhead(t)
	for _, event := range []string{"drained", "cancel"} {
		t.Run(event, func(t *testing.T) {
			ws := waitsAt(t, waitAhead)
			f := &frontier{slowPages: 1, siblings: 40, sibPages: 6, rows: 100}
			f.hold = make(chan struct{})
			var once sync.Once
			defer SetDurableEvents(func(e string) {
				if e == "one-range-open" {
					once.Do(func() { close(f.hold) })
				}
			})()
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			sink := &blockingSink{after: 300, open: make(chan struct{})}
			done := make(chan Stats, 1)
			errs := make(chan error, 1)
			go func() {
				st, err := Run[fr, tp](ctx, f, sink, &testSchema, Options{Workers: 8, Budget: 500, SpillDir: t.TempDir()})
				done <- st
				errs <- err
			}()
			expectWait(t, ws, waitAhead)
			if event == "cancel" {
				cancel(errors.New("stop"))
				close(sink.open)
				if err := expect(t, errs, event); err == nil {
					t.Fatal("a cancelled run succeeded")
				}
				return
			}
			close(sink.open)
			st := expect(t, done, event)
			if err := <-errs; err != nil || !slices.Equal(sink.keys, f.keys()) {
				t.Fatalf("err %v, %d keys", err, len(sink.keys))
			}
			if st.ReadAhead.Capped == 0 {
				t.Fatal("no worker was held at the read-ahead's share")
			}
		})
	}
}
